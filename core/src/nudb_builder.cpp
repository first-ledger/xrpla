// Builds an xrpld NuDB node database directly (nudb_builder.hpp).
#include "nudb_builder.hpp"
#include <xrpla/archive.hpp>
#include <xrpl/nodestore/NodeObject.h>
#include <xrpl/nodestore/detail/EncodedBlob.h>
#include <xrpl/nodestore/detail/codec.h>
#include <nudb/create.hpp>
#include <nudb/detail/bucket.hpp>
#include <nudb/detail/bulkio.hpp>
#include <nudb/detail/format.hpp>
#include <nudb/native_file.hpp>
#include <nudb/xxhasher.hpp>
#include <fcntl.h>
#include <sys/resource.h>
#include <cerrno>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <array>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fstream>
#include <future>
#include <mutex>
#include <optional>
#include <thread>

namespace xrpla::core {
namespace {
using File = nudb::native_file;
namespace d = nudb::detail;
void check(nudb::error_code const& ec, char const* what) {
    if (ec) throw Error(std::string("NuDB build (") + what + "): " + ec.message());
}
constexpr std::size_t key_size = 32;
// An entry in the log: a record's 48-bit key hash (NuDB's, with the build's salt), its offset
// in the data file, its value size, and the key's first 8 bytes. Hash and key bytes together
// tell two records of one key from records of different keys whose hashes collide (48 bits
// do, among billions of keys) without reading the keys back.
constexpr std::size_t entry_bytes = 6 + 6 + 4 + 8;
struct Entry {
    std::uint64_t bucket, hash, offset, key8;
    std::uint32_t size;
    bool operator<(Entry const& o) const {
        if (bucket != o.bucket) return bucket < o.bucket;
        if (hash != o.hash) return hash < o.hash;
        if (key8 != o.key8) return key8 < o.key8;
        return offset < o.offset;
    }
};
void pack(char* out, std::uint64_t hash, std::uint64_t offset, std::uint32_t size, char const* key) {
    std::memcpy(out, &hash, 6); std::memcpy(out + 6, &offset, 6); std::memcpy(out + 12, &size, 4); std::memcpy(out + 16, key, 8);
}
Entry unpack(char const* in) {
    Entry e{};
    std::memcpy(&e.hash, in, 6); std::memcpy(&e.offset, in + 6, 6); std::memcpy(&e.size, in + 12, 4); std::memcpy(&e.key8, in + 16, 8);
    return e;
}
// A record in xrpld's NuDB encoding: the EncodedBlob, compressed as its backend compresses it.
struct Encoded { char key[key_size]; std::string value; };
Encoded encode(std::shared_ptr<xrpl::NodeObject> const& object) {
    xrpl::node_store::EncodedBlob blob(object);
    nudb::detail::buffer buffer;
    auto [data, size] = xrpl::node_store::nodeobjectCompress(blob.getData(), blob.getSize(), buffer);
    Encoded out;
    std::memcpy(out.key, blob.getKey(), key_size);
    out.value.assign(static_cast<char const*>(data), size);
    return out;
}
// The kernel's estimate of memory available without swapping (MemAvailable), if it gives one.
std::optional<std::uint64_t> available_memory() {
    std::ifstream meminfo("/proc/meminfo");
    for (std::string line; std::getline(meminfo, line);)
        if (line.starts_with("MemAvailable:")) return std::strtoull(line.c_str() + 13, nullptr, 10) * 1024;  // "MemAvailable:  N kB"
    return std::nullopt;
}
fs::path find(fs::path const& directory, char const* extension) {
    for (auto const& entry : fs::directory_iterator(directory))
        if (entry.path().extension() == extension) return entry.path();
    return {};
}
// Runs work(0..count-1) on `threads` threads.
template <class Work> void parallel(std::size_t count, unsigned threads, Work const& work) {
    std::atomic<std::size_t> next{0};
    std::vector<std::future<void>> tasks;
    for (unsigned t = 0; t < std::max(1u, threads); ++t)
        tasks.push_back(std::async(std::launch::async, [&] { for (std::size_t i; (i = next++) < count;) work(i); }));
    std::exception_ptr error;
    for (auto& task : tasks) try { task.get(); } catch (...) { if (!error) error = std::current_exception(); }
    if (error) std::rethrow_exception(error);
}
}

struct NudbBuilder::Impl {
    fs::path directory, dat_path, key_path, log_path, entries_path;
    std::size_t block_size;
    double load_factor;
    Tuning tuning;
    nudb::detail::dat_file_header header;
    std::uint64_t salt = 0;  // the key file's; entries are hashed with it as they are written
    File data, entries;
    std::uint64_t data_size = 0, entry_count = 0;  // what the writer has written (under write_mutex)
    // Where the files ended at the last flush cue, everything before them in the file itself: batches
    // written after a cue may still be in the direct-I/O staging buffer, so a checkpoint records these.
    std::uint64_t flushed_data = 0, flushed_entries = 0;
    // Nodes are gathered in shards, each thread in its own (mostly), so adding one takes a
    // lock few other threads want. A full batch is then encoded by a task, or by the adding
    // thread itself when enough tasks are running, and handed to the writer thread, which
    // appends batches to the data file and the entry log in the order they come. One thread
    // writes because ext4 lets one write into a file at a time (its inode lock): when every
    // encoding task wrote its own batch, they spent a quarter of a restore's CPU time spinning
    // for that lock. Records land in no particular order: the key file is built from the entry
    // log at the end.
    struct Shard { std::mutex mutex; std::vector<std::shared_ptr<xrpl::NodeObject>> batch; };
    std::array<Shard, 16> shards;
    std::uint64_t nodes = 0, value_bytes = 0;  // written (under write_mutex, as data_size and entry_count)
    // An encoded batch: its records as the data file holds them, and their entries, whose
    // offsets count from the batch's start until the writer places it. Or, with `flush`, the
    // writer's cue to have everything before it in the files (a checkpoint's).
    struct Batch { std::string records, log; std::uint64_t count = 0, values = 0; bool flush = false; };
    std::mutex write_mutex;
    std::condition_variable write_changed;
    std::deque<Batch> to_write;
    std::size_t queued_bytes = 0;
    std::uint64_t handed = 0, done = 0;  // batches handed to the writer, and written
    bool writer_stop = false;
    std::exception_ptr write_error;  // the writer's first failure: every later hand-off or drain throws it
    std::thread writer;
    // The files again, for starting their writeback as batches land (sync_file_range): a
    // checkpoint's sync then waits for little more than the last batches.
    int data_fd = -1, entries_fd = -1;
    // The data file is appended to around the page cache (O_DIRECT) where the file system
    // allows: batches gather in an aligned buffer, written a block-multiple at a time, the
    // last partial block kept until more follows or a checkpoint writes it (padded, the file
    // then cut back to its end). Through the page cache, copying into it and handing its pages
    // to writeback kept the writer thread three quarters busy (200,000 recent ledgers:
    // 122 GB). The buffer holds the file's bytes [staged_at, staged_at + held).
    static constexpr std::size_t kBlock = 4096, kStaging = 16u << 20;
    int direct_fd = -1;  // -1: written through data (the page cache)
    char* staging = nullptr;
    std::uint64_t staged_at = 0;
    std::size_t held = 0;
    // Starts the buffer at the data file's end (its last partial block read back).
    void stage_from(std::uint64_t end) {
        if (direct_fd < 0) return;
        staged_at = end / kBlock * kBlock;
        held = static_cast<std::size_t>(end - staged_at);
        nudb::error_code ec;
        if (held) { data.read(staged_at, staging, held, ec); check(ec, "data read"); }
    }
    // Writes `size` bytes of the buffer at `at`; the first write the file system refuses
    // (EINVAL: no O_DIRECT there) turns direct writing off, what is held going through data.
    void write_direct(std::size_t size, std::uint64_t at) {
        for (std::size_t done = 0; done < size;) {
            auto n = ::pwrite(direct_fd, staging + done, size - done, static_cast<off_t>(at + done));
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && errno == EINVAL && !direct_written) {
                ::close(direct_fd); direct_fd = -1;
                nudb::error_code ec;
                data.write(staged_at, staging, held, ec); check(ec, "data write");
                held = 0;
                return;
            }
            if (n <= 0) throw Error("NuDB build (data write): " + std::string(std::strerror(errno)));
            done += static_cast<std::size_t>(n);
            direct_written = true;
        }
    }
    bool direct_written = false;
    void flush_blocks() {
        auto full = held / kBlock * kBlock;
        if (!full) return;
        write_direct(full, staged_at);
        if (direct_fd < 0) return;  // turned off: everything went through data
        std::memmove(staging, staging + full, held - full);
        held -= full; staged_at += full;
    }
    void append(std::uint64_t at, std::string const& bytes) {
        if (direct_fd < 0) {
            nudb::error_code ec;
            data.write(at, bytes.data(), bytes.size(), ec); check(ec, "data write");
            write_back(data_fd, at, bytes.size());
            return;
        }
        for (std::size_t done = 0; done < bytes.size();) {
            auto n = std::min(kStaging - held, bytes.size() - done);
            std::memcpy(staging + held, bytes.data() + done, n);
            held += n; done += n;
            if (held == kStaging) flush_blocks();
            if (direct_fd < 0 && done < bytes.size()) { append(at + done, bytes.substr(done)); return; }
        }
    }
    // Everything appended is in the file: the partial block too, padded, and the file cut back.
    void flush_all() {
        if (direct_fd < 0) return;
        flush_blocks();
        if (direct_fd < 0 || !held) return;
        std::memset(staging + held, 0, kBlock - held);
        write_direct(kBlock, staged_at);
        if (direct_fd < 0) return;
        if (::ftruncate(direct_fd, static_cast<off_t>(staged_at + held)) != 0) throw Error("NuDB build (data truncate): " + std::string(std::strerror(errno)));
    }
    void write_back(int fd, std::uint64_t at, std::uint64_t size) {
#ifdef SYNC_FILE_RANGE_WRITE
        if (fd >= 0) (void)::sync_file_range(fd, static_cast<off64_t>(at), static_cast<off64_t>(size), SYNC_FILE_RANGE_WRITE);
#else
        (void)fd; (void)at; (void)size;
#endif
    }
    void write_loop() {
        std::unique_lock lock(write_mutex);
        for (;;) {
            write_changed.wait(lock, [&] { return !to_write.empty() || writer_stop; });
            if (to_write.empty()) return;
            auto batch = std::move(to_write.front());
            to_write.pop_front();
            auto at = data_size, first = entry_count;
            bool failed_before = bool(write_error);
            lock.unlock();
            std::exception_ptr failed;
            if (!failed_before) try {
                if (batch.flush) flush_all();
                for (std::uint64_t n = 0; n < batch.count; ++n) {  // offsets from the batch's place in the file
                    auto* entry = batch.log.data() + n * entry_bytes;
                    std::uint64_t offset = 0;
                    std::memcpy(&offset, entry + 6, 6);
                    offset += at;
                    std::memcpy(entry + 6, &offset, 6);
                }
                if (!batch.records.empty()) append(at, batch.records);
                nudb::error_code ec;
                if (!batch.log.empty()) {
                    entries.write(first * entry_bytes, batch.log.data(), batch.log.size(), ec); check(ec, "entry log write");
                    write_back(entries_fd, first * entry_bytes, batch.log.size());
                }
            } catch (...) { failed = std::current_exception(); }
            lock.lock();
            if (!failed_before && !failed) {
                data_size = at + batch.records.size(); entry_count = first + batch.count;
                nodes += batch.count; value_bytes += batch.values;
                if (batch.flush) { flushed_data = data_size; flushed_entries = entry_count; }
            }
            if (failed && !write_error) write_error = failed;
            queued_bytes -= batch.records.size() + batch.log.size();
            ++done;
            write_changed.notify_all();
        }
    }
    // Queues a batch for the writer, waiting while 64 MB are queued already. Once the builder
    // is going away (tasks still encoding as an error unwinds it), batches are dropped.
    void hand_off(Batch batch) {
        std::unique_lock lock(write_mutex);
        write_changed.wait(lock, [&] { return queued_bytes < (64u << 20) || write_error || writer_stop; });
        if (write_error) std::rethrow_exception(write_error);
        if (writer_stop) return;
        queued_bytes += batch.records.size() + batch.log.size();
        to_write.push_back(std::move(batch));
        ++handed;
        write_changed.notify_all();
    }
    // Queues the cue to write everything before it (see Batch); the batches handed off so far.
    std::uint64_t hand_off_flush() {
        Batch cue;
        cue.flush = true;
        hand_off(std::move(cue));
        std::lock_guard lock(write_mutex);
        return handed;
    }
    // Waits until the first `batches` handed off (the last a flush cue) are written; where the files
    // ended at the newest cue written, all of it in the files. Batches written after that cue are left
    // out: they may still be in the staging buffer, and a checkpoint must not claim them.
    std::pair<std::uint64_t, std::uint64_t> written(std::uint64_t batches) {
        std::unique_lock lock(write_mutex);
        write_changed.wait(lock, [&] { return done >= batches || write_error; });
        if (write_error) std::rethrow_exception(write_error);
        return {flushed_data, flushed_entries};
    }
    // Written data is synced in the background about once a second, so a checkpoint's sync only
    // has the last moments' data left (the kernel would otherwise hold gigabytes dirty, and every
    // checkpoint would wait for them).
    std::mutex flush_mutex;
    std::condition_variable flush_wake;
    bool flush_stop = false;
    std::thread flusher;
    void flush_loop() {
        std::unique_lock lock(flush_mutex);
        while (!flush_wake.wait_for(lock, std::chrono::seconds(1), [&] { return flush_stop; })) {
            lock.unlock();
            nudb::error_code ec;
            data.sync(ec); entries.sync(ec);  // failures surface at the next checkpoint's own sync
            lock.lock();
        }
    }
    ~Impl() {
        { std::lock_guard lock(write_mutex); writer_stop = true; }
        write_changed.notify_all();
        if (writer.joinable()) writer.join();
        if (direct_fd >= 0) ::close(direct_fd);
        std::free(staging);
        if (data_fd >= 0) ::close(data_fd);
        if (entries_fd >= 0) ::close(entries_fd);
        { std::lock_guard lock(flush_mutex); flush_stop = true; }
        flush_wake.notify_all();
        if (flusher.joinable()) flusher.join();
    }
    unsigned threads = std::clamp(std::thread::hardware_concurrency() / 2, 1u, 16u);
    std::mutex mutex;  // the encoding tasks
    std::condition_variable idle;
    std::deque<std::future<void>> writing;  // batches tasks are encoding
    unsigned writing_here = 0;              // batches adding threads are encoding themselves

    std::uint64_t hash(char const* key) const { return d::hash<nudb::xxhasher>(key, key_size, salt); }
    // Encodes a batch and hands it to the writer.
    void encode_batch(std::vector<std::shared_ptr<xrpl::NodeObject>> const& objects) {
        std::vector<Encoded> records;
        records.reserve(objects.size());
        std::uint64_t bytes = 0;
        Batch batch;
        for (auto const& object : objects) {
            records.push_back(encode(object));
            bytes += 6 + key_size + records.back().value.size(); batch.values += records.back().value.size();
        }
        batch.count = records.size();
        batch.log.assign(records.size() * entry_bytes, '\0');
        batch.records.reserve(bytes);
        std::uint64_t offset = 0;
        for (std::size_t n = 0; n < records.size(); ++n) {
            auto const& record = records[n];
            char size[6];
            {
                nudb::detail::ostream os{size, 6};
                nudb::detail::write<nudb::detail::uint48_t>(os, record.value.size());
            }
            batch.records.append(size, 6); batch.records.append(record.key, key_size); batch.records += record.value;
            pack(batch.log.data() + n * entry_bytes, hash(record.key), offset, static_cast<std::uint32_t>(record.value.size()), record.key);
            offset += 6 + key_size + record.value.size();
        }
        hand_off(std::move(batch));
    }
    // Hands a full batch to a task to encode (`lock` held), or encodes it on this thread
    // (unlocking `lock` meanwhile) when enough tasks are running.
    void dispatch(std::vector<std::shared_ptr<xrpl::NodeObject>> objects, std::unique_lock<std::mutex>& lock) {
        if (objects.empty()) return;
        for (auto it = writing.begin(); it != writing.end();)  // finished tasks (their errors surface here)
            if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) { it->get(); it = writing.erase(it); }
            else ++it;
        if (writing.size() < threads * 2) {
            writing.push_back(std::async(std::launch::async, [this, objects = std::move(objects)] { encode_batch(objects); }));
            return;
        }
        ++writing_here;
        lock.unlock();
        try { encode_batch(objects); } catch (...) { lock.lock(); --writing_here; idle.notify_all(); throw; }
        lock.lock();
        --writing_here;
        idle.notify_all();
    }
    // Everything added so far is written (`lock` held on return); where the data file and the
    // entry log then end. Batches are encoded with `lock` held, so none starts meanwhile, but
    // their writing is waited for without it: nodes added meanwhile go on to be encoded.
    std::pair<std::uint64_t, std::uint64_t> drain(std::unique_lock<std::mutex>& lock) {
        for (auto& shard : shards) {
            std::vector<std::shared_ptr<xrpl::NodeObject>> objects;
            { std::lock_guard held(shard.mutex); objects = std::move(shard.batch); shard.batch.clear(); }
            dispatch(std::move(objects), lock);
        }
        idle.wait(lock, [&] { return writing_here == 0; });
        std::exception_ptr error;
        for (auto& task : writing) try { task.get(); } catch (...) { if (!error) error = std::current_exception(); }
        writing.clear();
        if (error) std::rethrow_exception(error);
        auto batches = hand_off_flush();
        lock.unlock();
        auto ends = written(batches);
        lock.lock();
        return ends;
    }
    // A log written by an earlier release held each entry's whole key: rewritten in this form.
    void convert_old_log(fs::path const& old) {
        nudb::error_code ec;
        File in;
        in.open(nudb::file_mode::scan, old.string(), ec); check(ec, "old entry log open");
        auto size = in.size(ec); check(ec, "old entry log size");
        constexpr std::size_t old_bytes = key_size + 8 + 4;
        constexpr std::uint64_t chunk = 1u << 20;
        std::string raw, out;
        for (std::uint64_t first = 0; first * old_bytes < size; first += chunk) {
            auto count = std::min<std::uint64_t>(chunk, size / old_bytes - first);
            raw.resize(count * old_bytes); out.assign(count * entry_bytes, '\0');
            in.read(first * old_bytes, raw.data(), raw.size(), ec); check(ec, "old entry log read");
            for (std::uint64_t i = 0; i < count; ++i) {
                char const* e = raw.data() + i * old_bytes;
                std::uint64_t offset; std::uint32_t length;
                std::memcpy(&offset, e + key_size, 8); std::memcpy(&length, e + key_size + 8, 4);
                pack(out.data() + i * entry_bytes, hash(e), offset, length, e);
            }
            entries.write(first * entry_bytes, out.data(), out.size(), ec); check(ec, "entry log write");
        }
        entries.sync(ec); check(ec, "entry log sync");
        in.close();
        fs::remove(old);
    }
    // Where a full bucket's contents go: a spill record at the data file's end, reserved
    // atomically so regions spill from several threads.
    std::atomic<std::uint64_t> data_end{0};
    void spill(d::bucket& b) {
        std::string record(6 + 2 + b.actual_size(), '\0');
        d::ostream os{record.data(), record.size()};
        d::write<d::uint48_t>(os, 0ULL);
        d::write<std::uint16_t>(os, static_cast<std::uint16_t>(b.actual_size()));
        b.write(os);
        auto offset = data_end.fetch_add(record.size());
        nudb::error_code ec;
        data.write(offset, record.data(), record.size(), ec); check(ec, "spill");
        b.clear();
        b.spill(offset + 8);
    }
    // Fills the key file's buckets [first, last) from entries sorted by bucket, dropping repeated
    // records of a key; returns (keys, repeats).
    std::pair<std::uint64_t, std::uint64_t> fill(File& key, d::key_file_header const& kh, std::uint64_t first, std::uint64_t last,
                                                 std::vector<Entry> const& sorted) {
        std::uint64_t const chunk = std::max<std::uint64_t>(1, (16u << 20) / kh.block_size);
        d::buffer buffer{chunk * kh.block_size};
        std::uint64_t keys = 0, repeats = 0;
        std::size_t next = 0;
        nudb::error_code ec;
        for (auto b0 = first; b0 < last; b0 += chunk) {
            auto b1 = std::min(b0 + chunk, last);
            for (std::uint64_t i = 0; i < b1 - b0; ++i) d::bucket{kh.block_size, buffer.get() + i * kh.block_size, d::empty};
            for (; next < sorted.size() && sorted[next].bucket < b1; ++next) {
                auto const& e = sorted[next];
                if (next && sorted[next - 1].bucket == e.bucket && sorted[next - 1].hash == e.hash && sorted[next - 1].key8 == e.key8) { ++repeats; continue; }
                d::bucket bucket{kh.block_size, buffer.get() + (e.bucket - b0) * kh.block_size};
                if (bucket.full()) spill(bucket);
                bucket.insert(e.offset, e.size, e.hash);
                ++keys;
            }
            key.write((b0 + 1) * kh.block_size, buffer.get(), (b1 - b0) * kh.block_size, ec); check(ec, "key write");
        }
        return {keys, repeats};
    }
};

NudbBuilder::NudbBuilder(fs::path const& directory, std::size_t block_size, double load_factor, Tuning tuning) : impl_(std::make_unique<Impl>()) {
    auto& m = *impl_;
    m.directory = directory; m.block_size = block_size; m.load_factor = load_factor; m.tuning = tuning;
    m.dat_path = find(directory, ".dat");
    if (m.dat_path.empty()) throw Error("No NuDB data file in " + directory.string());
    m.key_path = m.dat_path; m.key_path.replace_extension(".key");
    m.log_path = m.dat_path; m.log_path.replace_extension(".log");
    m.entries_path = directory.parent_path() / "xrpla-nudb-keys.bin";
    auto salt_path = directory.parent_path() / "xrpla-nudb-salt";
    nudb::error_code ec;
    m.data.open(nudb::file_mode::write, m.dat_path.string(), ec); check(ec, "data open");
    nudb::detail::read(m.data, m.header, ec); check(ec, "data header");
    nudb::detail::verify(m.header, ec); check(ec, "data header");
    if (m.header.key_size != key_size) throw Error("NuDB key size is not 32");
    // The salt is fixed when the build starts: entries are hashed with it as they are written.
    if (fs::exists(salt_path)) {
        auto text = read_file(salt_path, 64);
        m.salt = std::stoull(text);
    } else {
        m.salt = nudb::make_salt();
        write_file(salt_path.string() + ".part", std::to_string(m.salt)); durable_rename(salt_path.string() + ".part", salt_path);
    }
    if (!fs::exists(m.entries_path)) { File f; f.create(nudb::file_mode::write, m.entries_path.string(), ec); check(ec, "entry log create"); }
    m.entries.open(nudb::file_mode::write, m.entries_path.string(), ec); check(ec, "entry log open");
    if (auto old = directory.parent_path() / "xrpla-nudb-entries.bin"; fs::exists(old)) m.convert_old_log(old);
    m.data_size = m.data.size(ec); check(ec, "data size");
    m.entry_count = m.entries.size(ec) / entry_bytes; check(ec, "entry log size");
    m.flushed_data = m.data_size; m.flushed_entries = m.entry_count;
    m.flusher = std::thread([&m] { m.flush_loop(); });
    m.data_fd = ::open(m.dat_path.c_str(), O_RDONLY | O_CLOEXEC);  // -1: no early writeback, which is no error
    m.entries_fd = ::open(m.entries_path.c_str(), O_RDONLY | O_CLOEXEC);
#ifdef O_DIRECT
    void* buffer = nullptr;
    if (::posix_memalign(&buffer, Impl::kBlock, Impl::kStaging) == 0) {
        m.staging = static_cast<char*>(buffer);
        m.direct_fd = ::open(m.dat_path.c_str(), O_WRONLY | O_DIRECT | O_CLOEXEC);  // -1: through the page cache
        m.stage_from(m.data_size);
    }
#endif
    m.writer = std::thread([&m] { m.write_loop(); });
}
NudbBuilder::~NudbBuilder() = default;

void NudbBuilder::resume(Checkpoint const& checkpoint) {
    auto& m = *impl_;
    nudb::error_code ec;
    // A key file (from a finished build) no longer covers the data; a log file would make
    // NuDB replay an old recovery.
    fs::remove(m.key_path); fs::remove(m.log_path);
    auto data_size = std::max<std::uint64_t>(checkpoint.data_size, nudb::detail::dat_file_header::size);
    if (data_size > m.data_size || checkpoint.entries > m.entry_count) throw Error("NuDB build checkpoint is ahead of its files");
    m.data.trunc(data_size, ec); check(ec, "data truncate");
    m.entries.trunc(checkpoint.entries * entry_bytes, ec); check(ec, "entry log truncate");
    m.data_size = data_size;
    m.entry_count = checkpoint.entries;
    m.flushed_data = m.data_size; m.flushed_entries = m.entry_count;
    m.stage_from(data_size);
}
void NudbBuilder::add(std::shared_ptr<xrpl::NodeObject> const& object) {
    auto& m = *impl_;
    auto& shard = m.shards[std::hash<std::thread::id>{}(std::this_thread::get_id()) % m.shards.size()];
    std::vector<std::shared_ptr<xrpl::NodeObject>> full;
    {
        std::lock_guard held(shard.mutex);
        shard.batch.push_back(object);
        if (shard.batch.size() < 4096) return;
        full = std::move(shard.batch); shard.batch.clear();
    }
    std::unique_lock lock(m.mutex);
    m.dispatch(std::move(full), lock);
}
NudbBuilder::Checkpoint NudbBuilder::checkpoint() {
    auto& m = *impl_;
    Checkpoint made;
    {
        std::unique_lock lock(m.mutex);
        auto [size, entries] = m.drain(lock);
        made = {size, entries};
    }
    // Synced without the lock, so other threads keep adding meanwhile (their later writes
    // may be synced too, which does no harm).
    nudb::error_code ec;
    m.data.sync(ec); check(ec, "data sync");
    m.entries.sync(ec); check(ec, "entry log sync");
    return made;
}
// The key file from the entry log. Its buckets are cut into regions of contiguous buckets, each
// small enough to sort in memory (`region_entries`). With more than one region, the log is
// first distributed into a file per region (as many at once as may be open), then every
// region is sorted and written on a thread of its own: no sorted run is written and nothing
// is merged, so the temporary space is one more log's worth, whatever the database's size.
Json NudbBuilder::finish() {
    auto& m = *impl_;
    using clock = std::chrono::steady_clock;
    auto seconds = [](clock::duration dt) { return std::chrono::duration<double>(dt).count(); };
    auto started = clock::now();
    checkpoint();
    auto synced = clock::now();
    nudb::error_code ec;
    d::key_file_header kh;
    kh.version = d::currentVersion;
    kh.uid = m.header.uid; kh.appnum = m.header.appnum; kh.key_size = m.header.key_size;
    kh.salt = m.salt;
    kh.pepper = d::pepper<nudb::xxhasher>(kh.salt);
    kh.block_size = m.block_size;
    kh.load_factor = std::min<std::size_t>(static_cast<std::size_t>(65536.0 * m.load_factor), 65535);
    kh.buckets = std::max<std::size_t>(1, static_cast<std::size_t>(std::ceil(m.entry_count / (d::bucket_capacity(kh.block_size) * m.load_factor))));
    kh.modulus = d::ceil_pow2(kh.buckets);
    fs::remove(m.key_path);
    File key;
    key.create(nudb::file_mode::write, m.key_path.string(), ec); check(ec, "key create");
    {
        d::buffer block{kh.block_size};
        std::memset(block.get(), 0, kh.block_size);
        d::ostream os{block.get(), kh.block_size};
        d::write(os, kh);
        key.write(0, block.get(), kh.block_size, ec); check(ec, "key header");
        std::uint8_t zero = 0;  // the whole file, empty buckets included
        key.write(static_cast<nudb::noff_t>(kh.buckets + 1) * kh.block_size - 1, &zero, 1, ec); check(ec, "key allocate");
    }
    m.data_end = m.data_size;
    auto const count = m.entry_count;
    auto const regions = std::max<std::uint64_t>(1, (count + m.tuning.region_entries - 1) / m.tuning.region_entries);
    auto region_start = [&](std::uint64_t g) { return (g * kh.buckets + regions - 1) / regions; };  // first bucket of region g
    auto region_of = [&](std::uint64_t bucket) { return bucket * regions / kh.buckets; };
    auto bucket_of = [&](Entry& e) { e.bucket = d::bucket_index(e.hash, kh.buckets, kh.modulus); };
    unsigned const cores = std::clamp(std::thread::hardware_concurrency(), 1u, 32u);
    std::uint64_t keys = 0, repeats = 0;
    std::mutex totals;
    double distribute_seconds = 0;
    auto sort_and_fill = [&](std::vector<Entry>& region, std::uint64_t g) {
        std::sort(region.begin(), region.end());
        auto [k, r] = m.fill(key, kh, region_start(g), region_start(g + 1), region);
        std::lock_guard held(totals); keys += k; repeats += r;
    };
    if (regions == 1) {
        // Everything fits in memory: read and bucket on every core, sort in bucket ranges.
        std::vector<Entry> all(count);
        std::uint64_t const slice = 1u << 20;
        parallel((count + slice - 1) / slice, cores, [&](std::size_t s) {
            auto first = s * slice, n = std::min<std::uint64_t>(slice, count - first);
            std::string raw(n * entry_bytes, '\0');
            nudb::error_code read_error;
            m.entries.read(first * entry_bytes, raw.data(), raw.size(), read_error); check(read_error, "entry log read");
            for (std::uint64_t i = 0; i < n; ++i) { all[first + i] = unpack(raw.data() + i * entry_bytes); bucket_of(all[first + i]); }
        });
        // Scattered to `cores * 8` bucket ranges (counted per slice, then placed) and each range
        // sorted on its own: the ranges in order are the whole log in order.
        std::uint64_t const ranges = cores * 8ull, slices = cores;
        auto range_of = [&](std::uint64_t bucket) { return bucket * ranges / kh.buckets; };
        auto slice_of = [&](std::size_t t) { return std::pair{count * t / slices, count * (t + 1) / slices}; };
        std::vector<std::vector<std::uint64_t>> at(slices, std::vector<std::uint64_t>(ranges, 0));
        parallel(slices, cores, [&](std::size_t t) {
            auto [lo, hi] = slice_of(t);
            for (auto i = lo; i < hi; ++i) ++at[t][range_of(all[i].bucket)];
        });
        std::vector<std::uint64_t> bounds(ranges + 1, 0);
        for (std::uint64_t g = 0, next = 0; g < ranges; ++g) {
            bounds[g] = next;
            for (std::size_t t = 0; t < slices; ++t) { auto n = at[t][g]; at[t][g] = next; next += n; }
        }
        bounds[ranges] = count;
        std::vector<Entry> sorted(count);
        parallel(slices, cores, [&](std::size_t t) {
            auto [lo, hi] = slice_of(t);
            for (auto i = lo; i < hi; ++i) sorted[at[t][range_of(all[i].bucket)]++] = all[i];
        });
        std::vector<Entry>().swap(all);
        parallel(ranges, cores, [&](std::size_t g) { std::sort(sorted.begin() + bounds[g], sorted.begin() + bounds[g + 1]); });
        auto [k, r] = m.fill(key, kh, 0, kh.buckets, sorted);
        keys = k; repeats = r;
    } else {
        // Distribute: every open region file takes its entries, a pass over the log per group
        // of regions that may be open at once.
        auto distributing = clock::now();
        std::uint64_t open_limit = m.tuning.open_regions;
        if (!open_limit) {
            rlimit limit{};
            getrlimit(RLIMIT_NOFILE, &limit);
            if (limit.rlim_cur < limit.rlim_max) { limit.rlim_cur = limit.rlim_max; setrlimit(RLIMIT_NOFILE, &limit); getrlimit(RLIMIT_NOFILE, &limit); }
            open_limit = limit.rlim_cur > 256 ? limit.rlim_cur - 128 : 128;
        }
        auto region_path = [&](std::uint64_t g) { auto p = m.entries_path; p += ".region" + std::to_string(g); return p; };
        // Region files an interrupted build left (a crash, a full disk) go first: they are
        // created exclusively, and would otherwise fail every later build.
        {
            std::error_code ignored;
            auto prefix = m.entries_path.filename().string() + ".region";
            for (auto const& entry : fs::directory_iterator(m.entries_path.parent_path(), ignored))
                if (entry.path().filename().string().starts_with(prefix)) fs::remove(entry.path(), ignored);
        }
        for (std::uint64_t g0 = 0; g0 < regions; g0 += open_limit) {
            auto g1 = std::min(regions, g0 + open_limit);
            std::vector<File> files(g1 - g0);
            std::vector<std::atomic<std::uint64_t>> sizes(g1 - g0);
            for (auto g = g0; g < g1; ++g) { files[g - g0].create(nudb::file_mode::write, region_path(g).string(), ec); check(ec, "region create"); }
            std::uint64_t const slice = 4u << 20;
            parallel((count + slice - 1) / slice, cores, [&](std::size_t s) {
                auto first = s * slice, n = std::min<std::uint64_t>(slice, count - first);
                std::string raw(n * entry_bytes, '\0');
                nudb::error_code io;
                m.entries.read(first * entry_bytes, raw.data(), raw.size(), io); check(io, "entry log read");
                // This slice's entries grouped by region (a counting sort), then one write each.
                std::vector<std::uint64_t> at(g1 - g0 + 1, 0);
                std::vector<std::uint32_t> region(n);
                for (std::uint64_t i = 0; i < n; ++i) {
                    Entry e = unpack(raw.data() + i * entry_bytes); bucket_of(e);
                    auto g = region_of(e.bucket);
                    region[i] = static_cast<std::uint32_t>(g);
                    if (g >= g0 && g < g1) ++at[g - g0 + 1];
                }
                for (std::size_t g = 1; g < at.size(); ++g) at[g] += at[g - 1];
                std::string grouped(at.back() * entry_bytes, '\0');
                auto cursor = at;
                for (std::uint64_t i = 0; i < n; ++i)
                    if (region[i] >= g0 && region[i] < g1) std::memcpy(grouped.data() + (cursor[region[i] - g0]++) * entry_bytes, raw.data() + i * entry_bytes, entry_bytes);
                for (auto g = g0; g < g1; ++g) {
                    auto bytes = (at[g - g0 + 1] - at[g - g0]) * entry_bytes;
                    if (!bytes) continue;
                    auto where = sizes[g - g0].fetch_add(bytes);
                    files[g - g0].write(where, grouped.data() + at[g - g0] * entry_bytes, bytes, io); check(io, "region write");
                }
            });
            for (auto& f : files) f.close();
        }
        distribute_seconds = seconds(clock::now() - distributing);
        // Every region sorted and written on its own; a few at a time, each holding its entries
        // (about 64 bytes each, and its raw bytes as read): as many as the machine's free memory
        // holds, so a small machine sorts fewer at once instead of running out. Half of it: a
        // restore builds transaction.db's indexes meanwhile (sqlite_index::build, beside finish()
        // in both of its writers), which allocates its sort buffers after this is read.
        unsigned sorting = std::min<unsigned>(cores, m.tuning.sorting_regions);
        if (auto available = available_memory()) {
            auto each = m.tuning.region_entries * (64 + entry_bytes);
            sorting = std::clamp<unsigned>(static_cast<unsigned>(std::min<std::uint64_t>(*available / 2 / std::max<std::uint64_t>(each, 1), sorting)), 1, sorting);
        }
        parallel(regions, sorting, [&](std::size_t g) {
            File in;
            nudb::error_code io;
            in.open(nudb::file_mode::scan, region_path(g).string(), io); check(io, "region open");
            auto bytes = in.size(io); check(io, "region size");
            std::string raw(bytes, '\0');
            if (bytes) { in.read(0, raw.data(), bytes, io); check(io, "region read"); }
            in.close();
            std::vector<Entry> region(bytes / entry_bytes);
            for (std::size_t i = 0; i < region.size(); ++i) { region[i] = unpack(raw.data() + i * entry_bytes); bucket_of(region[i]); }
            std::string().swap(raw);
            sort_and_fill(region, g);
            fs::remove(region_path(g));
        });
    }
    auto placed = clock::now();
    m.data_size = m.data_end;
    // The spill records went past the end the writer knows (straight to the file, not through
    // its block buffer): its buffer starts again at the new end, or the next checkpoint's flush
    // would cut the file back to the old one and lose them.
    m.stage_from(m.data_end);
    m.data.sync(ec); check(ec, "data sync");
    if (m.data.size(ec) != m.data_end || ec) throw Error("NuDB build: the data file does not end where its spill records do");
    key.sync(ec); check(ec, "key sync");
    key.close();
    return {{"keys", keys}, {"repeated", repeats}, {"buckets", kh.buckets}, {"data_bytes", m.data_size}, {"regions", regions},
            {"key_seconds", seconds(clock::now() - started)},
            {"key_phase_seconds", {{"data_sync", seconds(synced - started)}, {"distribute", distribute_seconds},
                                   {"sort_and_fill", seconds(placed - synced) - distribute_seconds}, {"final_sync", seconds(clock::now() - placed)}}}};
}
std::uint64_t NudbBuilder::data_file_size() const {
    nudb::error_code ec;
    auto size = impl_->data.size(ec); check(ec, "data size");
    return size;
}
Json NudbBuilder::stats() const {
    auto& m = *impl_;
    std::lock_guard lock(m.write_mutex);
    return {{"nodes", m.nodes}, {"value_bytes", m.value_bytes}, {"data_bytes", m.data_size}, {"entries", m.entry_count}};
}
}
