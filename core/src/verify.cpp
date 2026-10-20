#include <xrpla/core/verify.hpp>
#include <sodium.h>
#include <xrpl/basics/TaggedCache.ipp>
#include <xrpl/basics/chrono.h>
#include <xrpl/config/BasicConfig.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <xrpl/protocol/STTx.h>
#include "wire.hpp"
#include "replay.hpp"
#include <xrpl/protocol/TxFormats.h>
#include <xrpl/protocol/TxMeta.h>
#include <xrpl/shamap/Family.h>
#include <xrpl/shamap/SHAMap.h>
#include <xrpl/shamap/SHAMapNodeID.h>
#include <xrpl/nodestore/Database.h>
#include <xrpl/nodestore/DummyScheduler.h>
#include <rocksdb/db.h>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#ifdef __GLIBC__
#include <malloc.h>
#endif
#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <unordered_map>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <future>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <thread>
#ifdef __linux__
#include <pthread.h>
#endif

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
r::Slice slice(Bytes const& b) { return {b.data(), b.size()}; }
Bytes bytes(r::Slice s) { return {reinterpret_cast<char const*>(s.data()), s.size()}; }
template<class T> Bytes raw(T const& value) {
    return {reinterpret_cast<char const*>(value.data()), value.size()};
}
r::uint256 key256(Bytes const& key) {
    if (key.size() != 32) throw Error("Object key must contain 32 bytes");
    return r::uint256::fromVoid(key.data());
}
// Content-addressed scratch storage. This is deliberately separate from the
// historical query database; unpublished verification nodes are safe to retain.
class PersistentNodes final : public r::node_store::Database {
    std::unique_ptr<rocksdb::DB> db_;
    // Readers on other threads (a peer server) share the store; a compaction swaps db_ for
    // its copy while holding this exclusively.
    mutable std::shared_mutex swap_;
    LedgerVerifier::NodeSink sink_;
    static void check(rocksdb::Status const& s) { if (!s.ok()) throw Error("SHAMap storage: " + s.ToString()); }
    // The open store. It is closed only after a compaction's swap failed and the old
    // store could not be opened again (swap_in): then every use is an error.
    rocksdb::DB& opened() const {
        if (!db_) throw Error("SHAMap storage is closed: a compaction's swap failed");
        return *db_;
    }
    std::shared_ptr<r::NodeObject> fetchNodeObject(r::uint256 const& hash, std::uint32_t,
        r::node_store::FetchReport& report, bool) override {
        Bytes value;
        {
            std::lock_guard lock(pending_mutex_);
            if (auto it = pending_.find(raw(hash)); it != pending_.end()) value = it->second;
            else if (auto it = writing_.find(raw(hash)); it != writing_.end()) value = it->second;
        }
        if (!value.empty()) {
            report.wasFound = true;
            r::Blob data(value.begin() + 1, value.end());
            return r::NodeObject::createObject(static_cast<r::NodeObjectType>(static_cast<unsigned char>(value[0])), std::move(data), hash);
        }
        std::shared_lock swapping(swap_);
        auto s = opened().Get({}, raw(hash), &value);
        if (s.IsNotFound()) return {};
        check(s);
        if (value.empty()) throw Error("Empty persisted SHAMap node");
        report.wasFound = true;
        r::Blob data(value.begin() + 1, value.end());
        return r::NodeObject::createObject(static_cast<r::NodeObjectType>(static_cast<unsigned char>(value[0])), std::move(data), hash);
    }
    void forEach(std::function<void(std::shared_ptr<r::NodeObject>)> visit) override {
        std::shared_lock swapping(swap_);
        std::unique_ptr<rocksdb::Iterator> it(opened().NewIterator({}));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            if (it->key().size() != 32 || it->value().empty()) throw Error("Corrupt persisted node");
            auto value = it->value().ToString();
            r::Blob data(value.begin() + 1, value.end());
            visit(r::NodeObject::createObject(static_cast<r::NodeObjectType>(static_cast<unsigned char>(value[0])),
                std::move(data), r::uint256::fromVoid(it->key().data())));
        }
        check(it->status());
    }
    fs::path path_;
    static std::unique_ptr<rocksdb::DB> open(fs::path const& path) {
        rocksdb::Options options; options.create_if_missing = true; options.max_open_files = 512;
        // A bulk-loaded snapshot is compacted once, as one manual compaction: split it.
        options.max_subcompactions = std::clamp(std::thread::hardware_concurrency() / 2, 1u, 16u);
        // Bulk loads write from many threads: flush several memtables at once rather than stall.
        options.max_background_jobs = 16;
        options.max_background_flushes = 8;
        // Writers insert into the memtable without waiting on each other's order: nothing
        // reads this store through snapshots, and every read follows its node's write.
        options.unordered_write = true;
        options.write_buffer_size = 16 * 1024 * 1024; options.compression = rocksdb::kLZ4Compression;
        // Hash-keyed SHAMap traversal makes random point reads across hundreds
        // of SSTs. Keep table readers and their indexes hot in a bounded cache;
        // the default small cache plus 64 open files repeatedly reloads them.
        rocksdb::BlockBasedTableOptions table;
        table.block_cache = rocksdb::NewLRUCache(256ULL * 1024 * 1024);
        table.cache_index_and_filter_blocks = true;
        table.cache_index_and_filter_blocks_with_high_priority = true;
        table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
        rocksdb::DB* ptr = nullptr; check(rocksdb::DB::Open(options, path.string(), &ptr));
        return std::unique_ptr<rocksdb::DB>(ptr);
    }
    static fs::path beside(fs::path const& path, char const* suffix) { return fs::path(path.string() + suffix); }
    // The copy's memtables and the old store's caches, freed on the walk's threads, stay in
    // their malloc arenas unless handed back.
    static void release_freed_memory() {
#ifdef __GLIBC__
        malloc_trim(0);
#endif
    }
public:
    PersistentNodes(fs::path const& path, r::node_store::Scheduler& scheduler, beast::Journal j, LedgerVerifier::NodeSink sink)
        : Database(scheduler, 0, [] { r::Section s; s.set("earliest_seq", "1"); return s; }(), j), sink_(std::move(sink)), path_(path) {
        // Finish or undo an interrupted compaction: the swap renames the old store aside first.
        if (!fs::exists(path) && fs::exists(beside(path, ".old"))) fs::rename(beside(path, ".old"), path);
        fs::remove_all(beside(path, ".old")); fs::remove_all(beside(path, ".compact"));
        // Stores compactions replaced. One may still be going on the thread swap_in started for
        // it, its files vanishing under this; whatever is left goes at a later open.
        std::error_code ignored;
        if (fs::exists(path.parent_path()))
            for (auto const& entry : fs::directory_iterator(path.parent_path()))
                if (entry.path().filename().string().starts_with(path.filename().string() + ".trash-")) fs::remove_all(entry.path(), ignored);
        db_ = open(path);
    }
    // Keep only the nodes reachable from `roots`: copy them into a fresh store, then swap it in.
    // Stored inner nodes carry all 16 child hashes, so the walk needs no in-memory tree. The
    // subtrees under each root's children are walked on their own threads, and the fresh store
    // is loaded as a bulk load is (no write-ahead log, compaction afterwards, in the
    // background): walking mainnet's 28M nodes on one thread into a plain store held a
    // following publisher still for nine minutes. The first root is the newest ledger's;
    // later ones are earlier ledgers kept for readers, and their walks stop at nodes already
    // copied (a copied node's whole subtree was copied with it), so each costs about the nodes
    // its ledger changed. An earlier root no longer stored is skipped.
    std::uint64_t compact(std::vector<r::uint256> const& roots) {
        if (compacting()) (void)finish_compaction(true);
        finish_writing();
        auto target = open_target();
        std::atomic<std::uint64_t> copied{0};
        std::atomic<bool> cancel{false};
        walk(*target, roots, 16, copied, cancel);
        rocksdb::FlushOptions flush; flush.wait = true; check(target->Flush(flush));
        swap_in(std::move(target));
        live_ = copied.load(); written_ = 0;
        return copied;
    }
    // A compaction on a thread of its own, for a follower that keeps applying ledgers: the
    // nodes reachable from `roots` (the state when it starts) are copied as above, while every
    // node written meanwhile goes to both stores, so the copy holds everything reachable from
    // the roots at the end (a tree written meanwhile shares its unchanged subtrees with the
    // one it started from). finish_compaction then swaps it in. `threads` walk at once (fewer
    // than a stopped follower uses, leaving it the disk); the walk waits for `gate` first
    // (tests hold it to apply ledgers meanwhile). False: one is already running.
    bool start_compaction(std::vector<r::uint256> roots, unsigned threads, std::shared_future<void> gate) {
        finish_writing();  // nodes written from now on reach the copy too (mirror)
        std::lock_guard lock(background_mutex_);
        if (background_) return false;
        auto background = std::make_unique<Background>();
        background->target = open_target();
        auto* b = background.get();
        background->walker = std::thread([this, b, roots = std::move(roots), threads, gate] {
            try {
                while (gate.valid() && gate.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready)
                    if (b->cancel) throw Error("Compaction cancelled");
                walk(*b->target, roots, threads, b->copied, b->cancel);
            } catch (...) { b->failed = std::current_exception(); }
            b->done = true;
        });
        background_ = std::move(background);
        return true;
    }
    bool compacting() const { std::lock_guard lock(background_mutex_); return bool(background_); }
    struct Compacted { std::uint64_t nodes; double seconds; };
    // Swaps a finished background compaction in (waiting for it with `wait`); nullopt when none
    // is running or it has not finished. A failed one is dropped and its error thrown.
    std::optional<Compacted> finish_compaction(bool wait) {
        Background* b;
        {
            std::lock_guard lock(background_mutex_);
            if (!background_ || (!wait && !background_->done)) return std::nullopt;
            b = background_.get();
        }
        b->walker.join();
        if (b->failed) { auto failed = b->failed; abandon_compaction(); std::rethrow_exception(failed); }
        write_pending();  // nodes still waiting go to both stores
        std::unique_ptr<Background> finished;
        {
            std::lock_guard lock(background_mutex_);
            finished = std::move(background_);
        }
        rocksdb::FlushOptions flush; flush.wait = true; check(finished->target->Flush(flush));
        auto nodes = finished->copied.load() + finished->mirrored.load();
        auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - finished->started).count();
        swap_in(std::move(finished->target));
        live_ = nodes; written_ = 0;
        release_freed_memory();
        return Compacted{nodes, seconds};
    }
    // Stops a background compaction (waiting for its walk) and drops its copy.
    void abandon_compaction() {
        std::unique_ptr<Background> b;
        {
            std::lock_guard lock(background_mutex_);
            b = std::move(background_);
        }
        if (!b) return;
        b->cancel = true;
        if (b->walker.joinable()) b->walker.join();
        b->target.reset();
        std::error_code ignored; fs::remove_all(beside(path_, ".compact"), ignored);
    }
private:
    struct Background {
        std::shared_ptr<rocksdb::DB> target;
        std::thread walker;
        std::atomic<bool> done{false}, cancel{false};
        std::exception_ptr failed;
        std::atomic<std::uint64_t> copied{0}, mirrored{0};
        std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
    };
    mutable std::mutex background_mutex_;
    std::unique_ptr<Background> background_;
    // Writes reach a background compaction's copy too (before the store itself).
    void mirror(rocksdb::WriteBatch const& batch) {
        std::lock_guard lock(background_mutex_);
        if (!background_) return;
        rocksdb::WriteOptions unlogged; unlogged.disableWAL = true;
        rocksdb::WriteBatch copy(batch);
        check(background_->target->Write(unlogged, &copy));
        background_->mirrored += batch.Count();
    }
    // The fresh store a compaction fills, set up as for a bulk load.
    std::shared_ptr<rocksdb::DB> open_target() {
        auto next = beside(path_, ".compact");
        fs::remove_all(next);
        std::shared_ptr<rocksdb::DB> target(open(next).release());
        // Up to 1 GB of memtables (8 had let a follower grow by 2.6 GB, kept after the swap).
        check(target->SetOptions({{"disable_auto_compactions", "true"}, {"write_buffer_size", std::to_string(256 << 20)}, {"max_write_buffer_number", "4"},
                                  {"compression", "kNoCompression"}, {"level0_slowdown_writes_trigger", "4096"}, {"level0_stop_writes_trigger", "4096"}}));
        return target;
    }
    // The flushed copy replaces the store; the old one is renamed aside, then deleted on a
    // thread of its own (tens of GB take a while). Should a step fail (a rename, or opening the
    // copy), the old store goes back in place and is opened again (it holds every node the copy
    // does: nodes written meanwhile went to both), and the error is thrown. Should even that
    // fail, the store stays closed and every later use throws (opened) instead of reaching a
    // null store.
    void swap_in(std::shared_ptr<rocksdb::DB> target) {
        auto next = beside(path_, ".compact"), old = beside(path_, ".old");
        target.reset();
        {
            std::unique_lock swapping(swap_);
            db_.reset();
            int moved = 0;  // 1: the old store is aside; 2: the copy is in its place
            try {
                fs::rename(path_, old); moved = 1;
                fs::rename(next, path_); moved = 2;
                db_ = open(path_);
            } catch (...) {
                try {
                    if (moved == 2) fs::rename(path_, next);  // the copy aside again; the next compaction clears it
                    if (moved >= 1) fs::rename(old, path_);
                    db_ = open(path_);
                } catch (...) { db_.reset(); }
                throw;
            }
            // As after a bulk load: the copy's level-0 files merge in the background.
            check(db_->SetOptions({{"level0_slowdown_writes_trigger", "256"}, {"level0_stop_writes_trigger", "512"}}));
        }
        static std::atomic<std::uint64_t> trashed{0};
        auto trash = beside(path_, (".trash-" + std::to_string(++trashed) + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())).c_str());
        std::error_code error;
        fs::rename(old, trash, error);
        if (error) { fs::remove_all(old, error); return; }  // deleted here then (the next start clears any leftover)
        std::thread([trash] { std::error_code ignored; fs::remove_all(trash, ignored); }).detach();
    }
    // Copies every node reachable from `roots` into `target`, `threads` subtrees at a time.
    void walk(rocksdb::DB& target, std::vector<r::uint256> const& roots, unsigned threads,
              std::atomic<std::uint64_t>& copied, std::atomic<bool> const& cancel) {
        rocksdb::WriteOptions unlogged; unlogged.disableWAL = true;
        Bytes const zero(32, '\0');
        auto children = [&](Bytes const& value) {
            std::vector<Bytes> out;
            if (value.size() >= 5 && value.compare(1, 4, Bytes("MIN\0", 4)) == 0) {
                if (value.size() != 5 + 16 * 32) throw Error("Corrupt persisted inner node");
                for (unsigned b = 0; b < 16; ++b)
                    if (auto child = value.substr(5 + 32 * b, 32); child != zero) out.push_back(std::move(child));
            }
            return out;
        };
        auto copied_already = [&](Bytes const& hash) {
            Bytes ignored;
            auto s = target.Get({}, hash, &ignored);
            if (s.IsNotFound()) return false;
            check(s);
            return true;
        };
        for (std::size_t n = 0; n < roots.size(); ++n) {
            if (cancel) throw Error("Compaction cancelled");
            bool earlier = n > 0;
            auto root = raw(roots[n]);
            if (earlier && copied_already(root)) continue;
            Bytes top;
            auto found = opened().Get({}, root, &top);
            if (earlier && found.IsNotFound()) continue;
            check(found);
            check(target.Put(unlogged, root, top)); ++copied;
            auto starts = children(top);
            std::atomic<std::size_t> next{0};
            std::vector<std::future<void>> walks;
            for (std::size_t t = 0; t < std::min<std::size_t>(std::max(threads, 1u), starts.size()); ++t)
                walks.push_back(std::async(std::launch::async, [&, earlier] {
                    rocksdb::WriteBatch batch;
                    Bytes value;
                    for (std::size_t i; (i = next++) < starts.size();) {
                        std::vector<Bytes> pending{starts[i]};
                        while (!pending.empty()) {
                            if (cancel) throw Error("Compaction cancelled");
                            auto hash = std::move(pending.back()); pending.pop_back();
                            if (earlier && copied_already(hash)) continue;
                            check(opened().Get({}, hash, &value));
                            for (auto& child : children(value)) pending.push_back(std::move(child));
                            batch.Put(hash, value); ++copied;
                            if (batch.Count() >= 16384) { check(target.Write(unlogged, &batch)); batch.Clear(); }
                        }
                    }
                    check(target.Write(unlogged, &batch));
                }));
            std::exception_ptr failed;
            for (auto& walk : walks) try { walk.get(); } catch (...) { if (!failed) failed = std::current_exception(); }
            if (failed) std::rethrow_exception(failed);
        }
    }
public:
    // Bulk loading (sorted snapshots): batched writes without the write-ahead log, made
    // durable by one memtable flush at the end; nodes also go to the sink as they are added.
    // Hash keys arrive in random order, so leveled compaction would rewrite a 20M-node
    // tree several times while it loads; instead compaction pauses until the load is done.
    rocksdb::WriteBatch bulk_;
    bool bulk_started_ = false;
    // Nodes written since the last compaction, and the nodes it kept (or bulk-loaded).
    std::atomic<std::uint64_t> written_{0}, live_{0};
    std::uint64_t floor_ = 1u << 22;
    // Superseded nodes are never deleted, so a long run compacts once writes since the last
    // compaction reach twice the live set: copying then costs at most half the writing.
    bool wants_compaction() const { return written_ >= 2 * std::max(live_.load(), floor_); }
    void compaction_floor(std::uint64_t nodes) { floor_ = nodes; }
    bool bulk_capable() const { return true; }
    void begin_bulk() {
        if (bulk_started_) return;
        bulk_started_ = true;
        // Memtables flush uncompressed while loading (lz4 held flushes behind the writers).
        check(opened().SetOptions({{"disable_auto_compactions", "true"}, {"write_buffer_size", std::to_string(256 << 20)}, {"max_write_buffer_number", "8"}, {"compression", "kNoCompression"},
                               {"level0_slowdown_writes_trigger", "4096"}, {"level0_stop_writes_trigger", "4096"}}));
    }
    // Many nodes at once, from any thread (after begin_bulk).
    // Seconds bulk writes spent in the sink and in RocksDB, summed over threads.
    std::atomic<std::int64_t> sink_ns{0}, store_ns{0};
    void put_bulk_many(std::vector<std::array<char, 32>> const& hashes, std::vector<Bytes> const& nodes) {
        using clock = std::chrono::steady_clock;
        std::shared_lock swapping(swap_);
        rocksdb::WriteBatch batch;
        Bytes value;
        for (std::size_t n = 0; n < hashes.size(); ++n) {
            value.assign(1, static_cast<char>(r::NodeObjectType::AccountNode)); value += nodes[n];
            batch.Put(rocksdb::Slice(hashes[n].data(), hashes[n].size()), value);
        }
        auto t = clock::now();
        if (sink_) for (std::size_t n = 0; n < hashes.size(); ++n) sink_(static_cast<std::uint8_t>(r::NodeObjectType::AccountNode), Bytes(hashes[n].data(), hashes[n].size()), nodes[n]);
        auto u = clock::now();
        live_ += hashes.size();
        mirror(batch);
        rocksdb::WriteOptions options; options.disableWAL = true; check(opened().Write(options, &batch));
        sink_ns += (u - t).count(); store_ns += (clock::now() - u).count();
    }
    void put_bulk(r::uint256 const& hash, Bytes const& serialized) {
        begin_bulk();
        Bytes value(1, static_cast<char>(r::NodeObjectType::AccountNode)); value += serialized;
        bulk_.Put(raw(hash), value); ++live_;
        if (sink_) sink_(static_cast<std::uint8_t>(r::NodeObjectType::AccountNode), raw(hash), serialized);
        if (bulk_.Count() >= 16384) write_bulk();
    }
    void write_bulk() { mirror(bulk_); std::shared_lock swapping(swap_); rocksdb::WriteOptions options; options.disableWAL = true; check(opened().Write(options, &bulk_)); bulk_.Clear(); }
    void finish_bulk() {
        write_bulk();
        std::shared_lock swapping(swap_);
        rocksdb::FlushOptions flush; flush.wait = true; check(opened().Flush(flush));
        if (!bulk_started_) return;
        bulk_started_ = false;
        // The loaded tree sits in a few dozen overlapping level-0 files; compaction merges them
        // in the background while ledgers apply (reads meanwhile check each file's bloom
        // filter), with limits high enough that writes never wait for it.
        check(opened().SetOptions({{"disable_auto_compactions", "false"}, {"write_buffer_size", std::to_string(16 << 20)}, {"max_write_buffer_number", "2"}, {"compression", "kLZ4Compression"},
                               {"level0_slowdown_writes_trigger", "256"}, {"level0_stop_writes_trigger", "512"}}));
    }
    // A stored node (type byte + serialized node); thread-safe.
    Bytes get(Bytes const& hash) const {
        std::shared_lock swapping(swap_);
        Bytes value; check(opened().Get({}, hash, &value));
        if (value.size() < 5) throw Error("Corrupt persisted SHAMap node");
        return value;
    }
    ~PersistentNodes() override {
        try { finish_writing(); } catch (...) {}
        abandon_compaction(); stop();
    }
    std::string getName() const override { return "xrpla verification nodes"; }
    void importDatabase(r::node_store::Database&) override { throw Error("Scratch node import is unsupported"); }
    std::int32_t getWriteLoad() const override { return 0; }
    // Nodes are written in batches: a ledger's new nodes arrive one by one, and single puts
    // (each through the write-ahead log) cost more than hashing them. Until a batch is
    // written, reads find its nodes here. A full batch is written on a thread of its own while
    // the next one fills (a follower's ledgers no longer wait on the store; it was a fifth of
    // their verification); write_pending writes everything waiting and returns once it is stored.
    std::mutex pending_mutex_;
    std::unordered_map<Bytes, Bytes> pending_, writing_;
    rocksdb::WriteBatch pending_batch_, writing_batch_;
    std::mutex writer_mutex_;
    std::future<void> writer_;
    std::uint64_t batch_nodes_ = 65536;
    void write_batches(std::uint64_t nodes) { batch_nodes_ = std::max<std::uint64_t>(1, nodes); }
    void write_batch(rocksdb::WriteBatch& batch) {
        mirror(batch);
        std::shared_lock swapping(swap_);
        check(opened().Write({}, &batch));
    }
    // Waits for the batch being written in the background. Should it have failed, its nodes go
    // back to wait with the others (written by the next write) and the error is thrown.
    void finish_writing() {
        std::lock_guard writing(writer_mutex_);
        if (!writer_.valid()) return;
        try { writer_.get(); }
        catch (...) {
            std::lock_guard lock(pending_mutex_);
            for (auto& [hash, value] : writing_) { pending_batch_.Put(hash, value); pending_.insert_or_assign(hash, std::move(value)); }
            writing_.clear(); writing_batch_.Clear();
            throw;
        }
    }
    void write_pending() {
        finish_writing();
        std::lock_guard lock(pending_mutex_);
        if (!pending_batch_.Count()) return;
        write_batch(pending_batch_);
        pending_batch_.Clear(); pending_.clear();
    }
    void write_pending_in_background() {
        finish_writing();  // one batch at a time in flight
        {
            std::lock_guard lock(pending_mutex_);
            if (!pending_batch_.Count()) return;
            std::swap(writing_batch_, pending_batch_); std::swap(writing_, pending_);
        }
        std::lock_guard writing(writer_mutex_);
        writer_ = std::async(std::launch::async, [this] {
            write_batch(writing_batch_);  // only this thread touches the batch until finish_writing
            std::lock_guard lock(pending_mutex_);
            writing_batch_.Clear(); writing_.clear();
        });
    }
    void store(r::NodeObjectType type, r::Blob&& data, r::uint256 const& hash, std::uint32_t) override {
        Bytes value(1, static_cast<char>(type)); value.append(reinterpret_cast<char const*>(data.data()), data.size());
        if (sink_) sink_(static_cast<std::uint8_t>(type), raw(hash), value.substr(1));
        bool full;
        {
            std::lock_guard lock(pending_mutex_);
            pending_batch_.Put(raw(hash), value);
            pending_.insert_or_assign(raw(hash), std::move(value));
            full = pending_batch_.Count() >= batch_nodes_;
        }
        ++written_;
        if (full) write_pending_in_background();
    }
    bool isSameDB(std::uint32_t, std::uint32_t) override { return true; }
    void sync() override { write_pending(); std::shared_lock swapping(swap_); check(opened().FlushWAL(true)); }
    void sweep() override {}
};
class ArchiveFamily : public r::Family {
    beast::Journal journal_{beast::Journal::getNullSink()};
    std::shared_ptr<r::FullBelowCache> below_ = std::make_shared<r::FullBelowCache>("xrpla", r::stopwatch(), journal_);
    std::shared_ptr<r::TreeNodeCache> tree_ = std::make_shared<r::TreeNodeCache>("xrpla", 1024, std::chrono::minutes{1}, r::stopwatch(), journal_);
    r::node_store::DummyScheduler scheduler_;
    std::shared_ptr<r::node_store::Database> database_;
    bool export_nodes_ = false;
public:
    explicit ArchiveFamily(fs::path const& path, LedgerVerifier::NodeSink sink) : export_nodes_(bool(sink)) {
        if (path.empty()) return;
        safe_cache_path(path);
        database_ = std::make_unique<PersistentNodes>(path, scheduler_, journal_, std::move(sink));
    }
    explicit ArchiveFamily(std::shared_ptr<r::node_store::Database> database)
        : database_(std::move(database)), export_nodes_(true) {
        if (!database_) throw Error("Native verifier requires a NodeStore");
    }
    bool backed() const { return bool(database_); }
    bool exports() const { return export_nodes_; }
    std::shared_ptr<r::node_store::Database> const& database() const { return database_; }
    r::node_store::Database& db() override { if (!database_) throw Error("Unexpected backed SHAMap access"); return *database_; }
    r::node_store::Database const& db() const override { if (!database_) throw Error("Unexpected backed SHAMap access"); return *database_; }
    beast::Journal const& journal() override { return journal_; }
    std::shared_ptr<r::FullBelowCache> getFullBelowCache() override { return below_; }
    std::shared_ptr<r::TreeNodeCache> getTreeNodeCache() override { return tree_; }
    void sweep() override { below_->sweep(); tree_->sweep(); }
    void reset() override { below_->reset(); tree_->reset(); }
    void missingNodeAcquireBySeq(std::uint32_t, r::uint256 const&) override { throw Error("Missing SHAMap node"); }
    void missingNodeAcquireByHash(r::uint256 const&, std::uint32_t) override { throw Error("Missing SHAMap node"); }
};
r::STLedgerEntry decode_object(ObjectRecord const& item) {
    if (item.object.empty() || item.object.size() > 16 * 1024 * 1024) throw Error("Invalid state object size");
    r::SerialIter input(slice(item.object));
    r::STLedgerEntry object(input, key256(item.key));
    if (!input.empty() || bytes(object.getSerializer().slice()) != item.object)
        throw Error("Noncanonical ledger object");
    return object;
}
r::STTx decode_tx(Bytes const& tx) {
    r::SerialIter input(slice(tx));
    r::STTx object(input);
    if (!input.empty() || bytes(object.getSerializer().slice()) != tx) throw Error("Noncanonical transaction");
    return object;
}
r::STObject decode_meta(Bytes const& meta) {
    r::SerialIter input(slice(meta));
    r::STObject object(input, r::sfMetadata);
    if (!input.empty() || bytes(object.getSerializer().slice()) != meta) throw Error("Noncanonical transaction metadata");
    return object;
}
// Data this build's protocol tables cannot parse (a newer amendment's fields or transaction
// types) is accepted when it is well-formed binary: the tree hashes, not parsing, prove it is
// the ledger's. The first few are reported.
void unrecognized(char const* what, std::exception const& error) {
    static std::atomic<unsigned> reported{0};
    if (reported++ < 20) std::cerr << Json{{"event", "unrecognized_protocol_data"}, {"what", what}, {"error", error.what()}}.dump() << std::endl;
}
// Accepts an object the wire reader parses, or failing that one libxrpl decodes canonically:
// the same objects a libxrpl decode first would accept (it fell back to the wire reader), at a
// fraction of the cost (the decode was most of a snapshot load's CPU time). Objects are
// authenticated by the state tree's hash, not by this check. An object type libxrpl does not
// know (a newer amendment's) is reported, as a failed decode was.
void check_object(ObjectRecord const& item) {
    if (item.object.empty() || item.object.size() > 16 * 1024 * 1024) throw Error("Invalid state object size");
    std::optional<std::string_view> type;
    try { type = wire::top_level(item.object, 1, 1); }  // UInt16 LedgerEntryType
    catch (std::exception const&) { (void)decode_object(item); return; }
    if (!type || type->size() != 2) { unrecognized("ledger object", Error("No LedgerEntryType")); return; }
    auto code = static_cast<std::uint16_t>((static_cast<unsigned char>((*type)[0]) << 8) | static_cast<unsigned char>((*type)[1]));
    if (!r::LedgerFormats::getInstance().findByType(static_cast<r::LedgerEntryType>(code)))
        unrecognized("ledger object", Error("Unknown LedgerEntryType " + std::to_string(code)));
}
// As check_object: the wire reader first, libxrpl only when it fails, which accepts the same
// transactions a libxrpl decode first did (at a fraction of its cost, on every ledger's path).
// A transaction type libxrpl does not know is reported, as a failed decode was.
void check_transaction(TransactionRecord const& record) {
    std::optional<std::string_view> type;
    try { type = wire::top_level(record.transaction, 1, 2); wire::validate(record.metadata); }  // UInt16 TransactionType
    catch (std::exception const&) { (void)decode_tx(record.transaction); (void)decode_meta(record.metadata); return; }
    if (!type || type->size() != 2) { unrecognized("transaction", Error("No TransactionType")); return; }
    auto code = static_cast<std::uint16_t>((static_cast<unsigned char>((*type)[0]) << 8) | static_cast<unsigned char>((*type)[1]));
    if (!r::TxFormats::getInstance().findByType(static_cast<r::TxType>(code)))
        unrecognized("transaction", Error("Unknown TransactionType " + std::to_string(code)));
}
} // namespace
namespace checks {
void object(ObjectRecord const& item) { check_object(item); }
void transaction(TransactionRecord const& record) { check_transaction(record); }
}
Bytes from_hex(std::string const& text) {
    if (text.size() % 2 || text.find_first_not_of("0123456789abcdefABCDEF") != std::string::npos)
        throw Error("Invalid hex bytes");
    Bytes out(text.size() / 2, '\0');
    if (sodium_hex2bin(reinterpret_cast<unsigned char*>(out.data()), out.size(), text.data(), text.size(),
                      nullptr, nullptr, nullptr) != 0) throw Error("Invalid hex bytes");
    return out;
}
std::string to_hex(std::string_view bytes) {
    std::string text(bytes.size() * 2 + 1, '\0');
    sodium_bin2hex(text.data(), text.size(), reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size());
    text.pop_back(); return text;
}
Header decode_header(Bytes const& bytes_in) {
    auto h = r::deserializeHeader(slice(bytes_in));
    r::Serializer output;
    r::addRaw(h, output);
    if (!h.seq || bytes(output.slice()) != bytes_in) throw Error("Noncanonical ledger header");
    return {h.seq, to_hex(raw(r::calculateLedgerHash(h))), to_hex(raw(h.parentHash)),
            to_hex(raw(h.accountHash)), to_hex(raw(h.txHash)), bytes_in, static_cast<std::uint32_t>(h.closeTime.time_since_epoch().count())};
}
Json header_json(Header const& h) {
    return {{"ledger_index", h.sequence}, {"ledger_hash", h.hash}, {"parent_hash", h.parent_hash},
            {"account_hash", h.account_hash}, {"transaction_hash", h.transaction_hash}, {"header", to_hex(h.raw)}};
}
Json to_json(::json::Value const& value) {
    switch (value.type()) {
    case ::json::ValueType::Null: return nullptr;
    case ::json::ValueType::Int: return value.asInt();
    case ::json::ValueType::UInt: return value.asUInt();
    // As libxrpl writes a number, so a reply shows the digits xrpld's does.
    case ::json::ValueType::Real: return Json::parse(value.toStyledString());
    case ::json::ValueType::String: return value.asString();
    case ::json::ValueType::Boolean: return value.asBool();
    case ::json::ValueType::Array: {
        Json out = Json::array();
        for (auto const& item : value) out.push_back(to_json(item));
        return out;
    }
    case ::json::ValueType::Object: {
        Json out = Json::object();
        for (auto it = value.begin(); it != value.end(); ++it) out[it.memberName()] = to_json(*it);
        return out;
    }
    }
    return nullptr;
}
Json object_json(ObjectRecord const& item) {
    return to_json(decode_object(item).getJson(r::JsonOptions::Values::None));
}
bool api_decodable(TransactionRecord const& tx) {
    try { (void)decode_tx(tx.transaction); (void)decode_meta(tx.metadata); return true; }
    catch (std::exception const&) { return false; }
}
Json transaction_json(TransactionRecord const& tx) {
    return {{"tx", to_json(decode_tx(tx.transaction).getJson(r::JsonOptions::Values::None))},
            {"meta", to_json(decode_meta(tx.metadata).getJson())}};
}
std::vector<TransactionIndex> index_transactions(LedgerRecord const& ledger) {
    // Read by type code (wire.hpp), so transactions of types this build does not know are
    // indexed like any other.
    std::vector<TransactionIndex> result;
    for (auto const& record : ledger.transactions) {
        auto summary = wire::summarize(record);
        TransactionIndex index{summary.hash, summary.index, std::move(summary.accounts)};
        auto const* format = r::TxFormats::getInstance().findByType(static_cast<r::TxType>(summary.type));
        index.type = format ? format->getName() : std::to_string(summary.type);
        index.account = std::move(summary.account);
        index.sequence = summary.sequence;
        result.push_back(std::move(index));
    }
    return result;
}
std::string transaction_root(std::vector<TransactionRecord> const& transactions) {
    ArchiveFamily family(fs::path{}, {});
    r::SHAMap map(r::SHAMapType::TRANSACTION, family);
    map.setUnbacked();
    for (auto const& record : transactions) {
        r::Serializer payload; payload.addVL(slice(record.transaction)); payload.addVL(slice(record.metadata));
        if (!map.addItem(r::SHAMapNodeType::TnTransactionMd, r::makeShamapitem(key256(wire::transaction_id(record.transaction)), payload.slice())))
            throw Error("Duplicate transaction");
    }
    return to_hex(raw(map.getHash().asUInt256()));
}
// Builds a disk-backed state tree from objects in strictly increasing key order in one
// pass, writing each node as SHAMap::writeNode would (serializeWithPrefix, hotACCOUNT_NODE).
// A SHAMap is canonical for its key set: a leaf hangs from the inner node at the depth of
// its longest shared nibble prefix with a neighbour, and every level along a path has an
// inner node. So each leaf can be placed once the next key is known, and each inner node
// hashed as soon as the keys leave its prefix, without random reads or an in-memory tree.
// A 32-byte key or hash, kept inline: the tree builder handles tens of millions of them.
using Hash32 = std::array<char, 32>;
Hash32 hash32(Bytes const& bytes) {
    if (bytes.size() != 32) throw Error("Expected 32 bytes");
    Hash32 out; std::memcpy(out.data(), bytes.data(), 32); return out;
}
class SortedStateBuilder {
    r::node_store::Database& db_;
    PersistentNodes* bulk_ = nullptr;  // batched writes when the store allows them
    std::uint32_t seq_ = 0;
    unsigned base_ = 0;  // the depth of the node this builder builds (0: the root)
    std::vector<std::array<Hash32, 16>> frames_;  // inner nodes along the current path, by depth (zero: no child)
    Hash32 path_key_{}, pending_key_{}, pending_hash_{};
    unsigned pending_shared_ = 0;
    bool has_pending_ = false;
    Bytes inner_;  // the inner node being closed
    static unsigned nibble(Hash32 const& key, unsigned depth) {
        auto byte = static_cast<unsigned char>(key[depth / 2]);
        return depth % 2 ? byte & 15 : byte >> 4;
    }
    // Leading nibbles two keys share.
    static unsigned shared(Hash32 const& a, Hash32 const& b) {
        unsigned byte = 0;
        while (byte < 32 && a[byte] == b[byte]) ++byte;
        if (byte == 32) throw Error("Duplicate snapshot key");
        auto differ = static_cast<unsigned char>(a[byte]) ^ static_cast<unsigned char>(b[byte]);
        return byte * 2 + ((differ & 0xF0) ? 0 : 1);
    }
    // Inner nodes written by background tasks (after begin()), so the thread placing leaves
    // never waits on the node store or the sink.
    bool deferred_ = false;
    std::vector<Hash32> deferred_hashes_;
    std::vector<std::uint32_t> deferred_sizes_;
    Bytes deferred_nodes_;  // their serialized nodes, one after another
    std::deque<std::future<void>> writes_;
    std::int64_t write_wait_ns = 0;  // the placing thread waiting on those writes
    void write_deferred() {
        if (deferred_hashes_.empty()) return;
        writes_.push_back(std::async(std::launch::async, [this, hashes = std::move(deferred_hashes_), sizes = std::move(deferred_sizes_), joined = std::move(deferred_nodes_)] {
            std::vector<Bytes> nodes;
            nodes.reserve(sizes.size());
            std::size_t at = 0;
            for (auto size : sizes) { nodes.push_back(joined.substr(at, size)); at += size; }
            write_leaves(hashes, nodes);
        }));
        deferred_hashes_.clear(); deferred_sizes_.clear(); deferred_nodes_.clear();
        auto waiting = std::chrono::steady_clock::now();
        while (writes_.size() > 4) { writes_.front().get(); writes_.pop_front(); }
        write_wait_ns += (std::chrono::steady_clock::now() - waiting).count();
    }
    void wait_for_writes() {
        write_deferred();
        std::exception_ptr error;
        for (auto& w : writes_) try { w.get(); } catch (...) { if (!error) error = std::current_exception(); }
        writes_.clear();
        if (error) std::rethrow_exception(error);
    }
    void write(Bytes const& serialized, Hash32 const& hash) {
        if (deferred_) {
            deferred_hashes_.push_back(hash); deferred_sizes_.push_back(static_cast<std::uint32_t>(serialized.size())); deferred_nodes_ += serialized;
            if (deferred_hashes_.size() >= 4096) write_deferred();
            return;
        }
        if (bulk_) bulk_->put_bulk(r::uint256::fromVoid(hash.data()), serialized);
        else db_.store(r::NodeObjectType::AccountNode, r::Blob(serialized.begin(), serialized.end()), r::uint256::fromVoid(hash.data()), seq_);
    }
    Hash32 close_top() {
        inner_.assign("MIN\0", 4);
        for (auto const& child : frames_.back()) inner_.append(child.data(), child.size());
        frames_.pop_back();
        auto hash = half(inner_);
        write(inner_, hash);
        return hash;
    }
    void place(unsigned next_shared) {
        while (base_ + frames_.size() > pending_shared_ + 1) {
            auto depth = static_cast<unsigned>(base_ + frames_.size() - 1);
            auto hash = close_top();
            frames_[depth - 1 - base_][nibble(path_key_, depth - 1)] = hash;
        }
        auto depth = std::max(pending_shared_, next_shared);
        while (base_ + frames_.size() < depth + 1) frames_.emplace_back();
        frames_[depth - base_][nibble(pending_key_, depth)] = pending_hash_;
        path_key_ = pending_key_;
    }
    // Places the last leaf and closes every open node; returns the hash of the node at base_.
    Hash32 close_all() {
        place(base_);
        while (frames_.size() > 1) {
            auto depth = static_cast<unsigned>(base_ + frames_.size() - 1);
            auto hash = close_top();
            frames_[depth - 1 - base_][nibble(path_key_, depth - 1)] = hash;
        }
        return close_top();
    }
    Hash32 write_inner(std::array<Hash32, 16> const& children) {
        frames_.push_back(children);
        return close_top();
    }
public:
    static Hash32 half(Bytes const& data) {
        unsigned char digest[64];
        wire::sha512(data.data(), data.size(), digest);
        Hash32 out; std::memcpy(out.data(), digest, 32); return out;
    }
    explicit SortedStateBuilder(r::node_store::Database& db, unsigned base = 0) : db_(db), base_(base) {
        frames_.emplace_back();
        if (auto* nodes = dynamic_cast<PersistentNodes*>(&db); nodes && nodes->bulk_capable()) bulk_ = nodes;
    }
    ~SortedStateBuilder() { for (auto& w : writes_) if (w.valid()) w.wait(); }
    static Bytes leaf(Bytes const& key, Bytes const& object) { return Bytes("MLN\0", 4) + object + key; }
    // Leaves are stored in any order, from any thread (begin() first); only placing them, and
    // writing the inner nodes that follow from it, is ordered.
    void begin() { if (bulk_) bulk_->begin_bulk(); deferred_ = true; }
    Json write_seconds() const {
        if (!bulk_) return Json::object();
        return {{"sink", bulk_->sink_ns / 1e9}, {"store", bulk_->store_ns / 1e9}, {"placer_waiting_on_inner_writes", write_wait_ns / 1e9}};
    }
    void write_leaves(std::vector<Hash32> const& hashes, std::vector<Bytes> const& nodes) {
        if (bulk_) { bulk_->put_bulk_many(hashes, nodes); return; }
        for (std::size_t n = 0; n < hashes.size(); ++n)
            db_.store(r::NodeObjectType::AccountNode, r::Blob(nodes[n].begin(), nodes[n].end()), r::uint256::fromVoid(hashes[n].data()), seq_);
    }
    // A builder for the subtree under one first byte (the node at depth 2), whose inner nodes
    // are written in the background like this one's; any thread may use it.
    std::unique_ptr<SortedStateBuilder> subtree() const {
        auto builder = std::make_unique<SortedStateBuilder>(db_, 2);
        builder->deferred_ = true;
        return builder;
    }
    // A subtree built from two or more leaves: its node's hash, once its inner nodes are written.
    Hash32 finish_subtree() {
        auto hash = close_all();
        wait_for_writes();
        return hash;
    }
    // What lies under one first byte: nothing, one leaf (its hash), or a depth-2 inner node.
    struct Subtree { std::size_t leaves = 0; Hash32 hash{}; };
    // The tree's root from its 256 subtrees, as SHAMap places them: a leaf alone under its first
    // nibble hangs from the root, one alone under its first byte from the depth-1 node.
    r::uint256 finish_from(std::array<Subtree, 256> const& subtrees, std::uint32_t seq) {
        seq_ = seq;
        std::array<Hash32, 16> root{};
        bool empty = true;
        for (unsigned high = 0; high < 16; ++high) {
            std::array<Hash32, 16> children{};
            std::size_t leaves = 0;
            for (unsigned low = 0; low < 16; ++low)
                if (auto const& subtree = subtrees[high * 16 + low]; subtree.leaves) { children[low] = subtree.hash; leaves += subtree.leaves; }
            if (!leaves) continue;
            empty = false;
            if (leaves == 1) { for (auto const& child : children) if (child != Hash32{}) root[high] = child; }
            else root[high] = write_inner(children);
        }
        auto top = empty ? Hash32{} : write_inner(root);
        wait_for_writes();
        if (bulk_) bulk_->finish_bulk();
        return r::uint256::fromVoid(top.data());
    }
    void place_leaf(Hash32 const& key, Hash32 const& hash) {
        auto with_previous = has_pending_ ? shared(pending_key_, key) : base_;
        if (has_pending_) place(with_previous);
        pending_key_ = key; pending_hash_ = hash; pending_shared_ = with_previous; has_pending_ = true;
    }
    // Root hash of the finished tree (zero for an empty tree, which SHAMap does not store).
    r::uint256 finish(std::uint32_t seq) {
        seq_ = seq;
        if (!has_pending_) return {};
        auto root = close_all();
        wait_for_writes();
        if (bulk_) bulk_->finish_bulk();
        return r::uint256::fromVoid(root.data());
    }
};
// Loads a sorted snapshot as a pipeline: batches are checked and their leaves hashed and
// stored on their own threads, several at once, while one thread takes finished batches in
// order and gathers the leaves under each first byte. Each first byte's subtree (a 256th of
// the tree) is then built on its own thread, and the root from the 256 subtrees at the end. The caller only waits when too many batches are in flight, so reading, checking and
// placing overlap (placing alone once took as long as checking on every core).
class SnapshotLoader {
    struct Batch { std::vector<Hash32> keys, hashes; };
    SortedStateBuilder& builder_;
    std::size_t limit_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<std::future<Batch>> queue_;
    bool closing_ = false, abandon_ = false;
    std::exception_ptr error_;
    std::thread placer_;
    // The first byte being gathered, its leaves, and every first byte's subtree (or its build).
    int byte_ = -1;
    std::vector<Hash32> byte_keys_, byte_hashes_;
    std::array<SortedStateBuilder::Subtree, 256> subtrees_;
    std::array<std::future<Hash32>, 256> building_;
    void gathered() {
        if (byte_ < 0 || byte_keys_.empty()) return;
        auto& subtree = subtrees_[byte_];
        subtree.leaves = byte_keys_.size();
        if (byte_keys_.size() == 1) subtree.hash = byte_hashes_.front();
        else building_[byte_] = std::async(std::launch::async, [builder = builder_.subtree(), keys = std::move(byte_keys_), hashes = std::move(byte_hashes_)] {
            for (std::size_t n = 0; n < keys.size(); ++n) builder->place_leaf(keys[n], hashes[n]);
            return builder->finish_subtree();
        });
        byte_keys_.clear(); byte_hashes_.clear();
    }
    using clock = std::chrono::steady_clock;
    static double seconds(clock::duration d) { return std::chrono::duration<double>(d).count(); }
public:
    // Where the load spent its time: summed over worker threads (checks, leaf writes), the
    // placer's own (placing, waiting for the next batch) and the caller's (waiting for room).
    std::atomic<std::int64_t> check_ns{0}, write_ns{0}, place_ns{0}, place_wait_ns{0}, submit_wait_ns{0};
    Json timings() const {
        auto s = [](std::int64_t ns) { return ns / 1e9; };
        return {{"checks", s(check_ns)}, {"leaf_writes", s(write_ns)}, {"placing", s(place_ns)}, {"placer_waiting", s(place_wait_ns)}, {"reader_waiting", s(submit_wait_ns)},
                {"node_writes", builder_.write_seconds()}};
    }
private:
    void place() {
        for (;;) {
            std::future<Batch> next;
            {
                std::unique_lock lock(mutex_);
                changed_.wait(lock, [&] { return !queue_.empty() || closing_; });
                if (abandon_) return;
                if (queue_.empty()) break;
                next = std::move(queue_.front()); queue_.pop_front();
            }
            changed_.notify_all();
            try {
                auto waited = clock::now();
                auto batch = next.get();
                auto placing = clock::now();
                for (std::size_t n = 0; n < batch.keys.size(); ++n) {
                    auto byte = static_cast<unsigned char>(batch.keys[n][0]);
                    if (byte != byte_) { gathered(); byte_ = byte; }
                    byte_keys_.push_back(batch.keys[n]); byte_hashes_.push_back(batch.hashes[n]);
                }
                place_wait_ns += (placing - waited).count(); place_ns += (clock::now() - placing).count();
            } catch (...) {
                { std::lock_guard lock(mutex_); error_ = std::current_exception(); }
                changed_.notify_all();
                return;
            }
        }
        gathered();
    }
public:
    SnapshotLoader(SortedStateBuilder& builder, unsigned threads) : builder_(builder), limit_(threads + threads / 2) {
        builder_.begin();
        placer_ = std::thread([this] {
#ifdef __linux__
            pthread_setname_np(pthread_self(), "xrpla-placer");
#endif
            place();
        });
    }
    ~SnapshotLoader() {
        { std::lock_guard lock(mutex_); closing_ = abandon_ = true; }
        changed_.notify_all();
        if (placer_.joinable()) placer_.join();
    }
    void submit(std::vector<ObjectRecord> items) {
        auto work = std::async(std::launch::async, [this, items = std::move(items)] {
            Batch batch;
            std::vector<Bytes> nodes;
            batch.keys.reserve(items.size()); batch.hashes.reserve(items.size()); nodes.reserve(items.size());
            auto began = clock::now();
            for (auto const& item : items) {
                check_object(item);
                nodes.push_back(SortedStateBuilder::leaf(item.key, item.object));
                batch.hashes.push_back(SortedStateBuilder::half(nodes.back()));
                batch.keys.push_back(hash32(item.key));
            }
            auto writing = clock::now();
            builder_.write_leaves(batch.hashes, nodes);
            check_ns += (writing - began).count(); write_ns += (clock::now() - writing).count();
            return batch;
        });
        auto waiting = clock::now();
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return queue_.size() < limit_ || error_; });
        submit_wait_ns += (clock::now() - waiting).count();
        if (error_) std::rethrow_exception(error_);
        queue_.push_back(std::move(work));
        lock.unlock();
        changed_.notify_all();
    }
    // Waits until every submitted batch is placed; the state tree's root.
    r::uint256 finish(std::uint32_t seq) {
        { std::lock_guard lock(mutex_); closing_ = true; }
        changed_.notify_all();
        placer_.join();
        std::exception_ptr error = error_;
        for (std::size_t b = 0; b < building_.size(); ++b)
            if (building_[b].valid()) try { subtrees_[b].hash = building_[b].get(); } catch (...) { if (!error) error = std::current_exception(); }
        if (error) std::rethrow_exception(error);
        return builder_.finish_from(subtrees_, seq);
    }
};
// Reads the tree nodes on the paths to keys coming ledgers change, on threads of its own, from
// the newest verified state. Its nodes are shared (libxrpl hooks children up under per-node
// locks, as xrpld's readers rely on), so these reads run beside the ledger being applied, which
// then finds them in memory. A hint only: a failed read or a stale state costs only the read.
class StatePrefetcher {
public:
    using View = std::function<std::shared_ptr<r::SHAMap const>()>;
    StatePrefetcher(View view, unsigned threads) : view_(std::move(view)) {
        for (unsigned n = 0; n < threads; ++n) threads_.emplace_back([this] { run(); });
    }
    ~StatePrefetcher() {
        { std::lock_guard lock(mutex_); stopping_ = true; }
        changed_.notify_all();
        for (auto& thread : threads_) thread.join();
    }
    void add(std::vector<Bytes> keys) {
        {
            std::lock_guard lock(mutex_);
            if (queue_.size() >= 64) return;  // this far behind, those ledgers apply first anyway
            queue_.push_back(std::move(keys));
        }
        changed_.notify_one();
    }
    std::uint64_t keys() const { return keys_; }
private:
    void run() {
        for (;;) {
            std::vector<Bytes> keys;
            {
                std::unique_lock lock(mutex_);
                changed_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
                if (stopping_) return;
                keys = std::move(queue_.front()); queue_.pop_front();
            }
            auto state = view_();
            if (!state) continue;
            for (auto const& key : keys) {
                if (key.size() != 32) continue;
                try { (void)state->peekItem(r::uint256::fromVoid(key.data())); } catch (...) {}
                ++keys_;
            }
        }
    }
    View view_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<std::vector<Bytes>> queue_;
    bool stopping_ = false;
    std::atomic<std::uint64_t> keys_{0};
    std::vector<std::thread> threads_;
};
namespace {
// Parts 0 to count - 1 of one ledger's work, run on the process's task pool (`helpers` tasks)
// and by the thread that waits for them: apply once started threads of its own for every
// ledger. Parts are taken from a counter, and wait() takes whatever no pool thread has yet, so
// they finish even while every pool thread is busy (with work that applies a ledger itself,
// say); a pool task arriving after the parts are gone touches nothing of the caller's. Whoever
// abandons them (an exception passing) waits for the parts under way.
class LedgerParts {
    struct State {
        std::function<void(std::size_t)> work;
        std::size_t count = 0;
        std::atomic<std::size_t> next{0};
        std::mutex mutex;
        std::condition_variable finished;
        std::size_t done = 0;
        std::exception_ptr failure;
        void take() {
            for (std::size_t n; (n = next++) < count;) {
                std::exception_ptr failed;
                try { work(n); } catch (...) { failed = std::current_exception(); }
                std::lock_guard lock(mutex);
                if (failed && !failure) failure = failed;
                if (++done == count) finished.notify_all();
            }
        }
        void wait_done() { std::unique_lock lock(mutex); finished.wait(lock, [&] { return done == count; }); }
    };
    std::shared_ptr<State> state_ = std::make_shared<State>();
    bool waited_ = false;
public:
    LedgerParts(std::size_t count, std::size_t helpers, std::function<void(std::size_t)> work) {
        state_->work = std::move(work);
        state_->count = count;
        for (std::size_t n = 0; n < std::min(helpers, count); ++n) TaskPool::shared().post([state = state_] { state->take(); });
    }
    LedgerParts(LedgerParts const&) = delete;
    LedgerParts& operator=(LedgerParts const&) = delete;
    // Runs the parts left, waits for the rest, and rethrows the first failure.
    void wait() {
        waited_ = true;
        state_->take();
        state_->wait_done();
        if (state_->failure) std::rethrow_exception(state_->failure);
    }
    ~LedgerParts() {
        if (waited_) return;
        // The parts not taken yet are dropped; those under way finish first.
        auto taken = std::min(state_->next.exchange(state_->count), state_->count);
        { std::lock_guard lock(state_->mutex); state_->done += state_->count - taken; }
        state_->wait_done();
    }
};
}
struct LedgerVerifier::Impl {
    ArchiveFamily family;
    std::shared_ptr<r::SHAMap> state;
    std::unique_ptr<SortedStateBuilder> builder;
    std::unique_ptr<SnapshotLoader> loader;  // destroyed before the builder it places into
    Json load_seconds = Json::object();       // the finished loader's timings
    std::vector<ObjectRecord> unsent;         // single snapshot objects, sent to the loader in batches
    void send(std::vector<ObjectRecord> items) {
        if (items.empty()) return;
        if (!builder) {
            builder = std::make_unique<SortedStateBuilder>(family.db());
            loader = std::make_unique<SnapshotLoader>(*builder, std::clamp(std::thread::hardware_concurrency(), 1u, 32u));
        }
        loader->submit(std::move(items));
    }
    Header current{};
    Bytes last_snapshot_key;
    bool ready = false;
    std::uint64_t pending = 0;
    std::uint64_t touched = 0;
    std::uint64_t retain_ledgers = 64, retain_objects = 4096;
    // The state and transaction tree roots of the newest ledgers kept for readers
    // (keep_ledgers), oldest first. While a window is kept, transaction trees are stored too.
    std::deque<std::pair<r::uint256, r::uint256>> kept_roots;
    std::size_t keep = 0;
    void remember(Header const& h) {
        if (!keep) return;
        std::pair roots{key256(from_hex(h.account_hash)), key256(from_hex(h.transaction_hash))};
        if (kept_roots.empty() || kept_roots.back() != roots) kept_roots.push_back(roots);
        while (kept_roots.size() > keep) kept_roots.pop_front();
    }
    // Seconds spent in each part of apply (timings()).
    double checks_seconds = 0, transactions_seconds = 0, state_seconds = 0, flush_seconds = 0, persist_seconds = 0;
    explicit Impl(fs::path const& backing, NodeSink sink) : family(backing, std::move(sink)), state(std::make_shared<r::SHAMap>(r::SHAMapType::STATE, family)) {
        if (!family.backed()) state->setUnbacked();
    }
    explicit Impl(std::shared_ptr<r::node_store::Database> database)
        : family(std::move(database)), state(std::make_shared<r::SHAMap>(r::SHAMapType::STATE, family)) {}
    void flush() {
        if (!family.backed()) return;
        state->flushDirty(r::NodeObjectType::AccountNode);
        auto hash = state->getHash();
        family.db().sync();
        reload(hash);
    }
    // Drop the old root and its cached descendants before fetchRoot. A
    // cache hit here would otherwise reattach the entire visited tree,
    // retaining every snapshot object despite the periodic disk flush.
    void reload(r::SHAMapHash const& hash) {
        state.reset();
        family.reset();
        state = std::make_shared<r::SHAMap>(r::SHAMapType::STATE, hash.asUInt256(), family);
        state->setLedgerSeq(std::max<std::uint32_t>(1, current.sequence));
        if (hash.asUInt256() != r::uint256{} && !state->fetchRoot(hash, nullptr)) throw Error("Cannot reload persisted state root");
        state->clearSynching();
        pending = 0;
        touched = 0;
        publish();
    }
    PersistentNodes* nodes() { return family.backed() ? dynamic_cast<PersistentNodes*>(&family.db()) : nullptr; }
    // Automatic compaction runs on its own thread too (a follower's; see start_compaction).
    bool background = false;
    std::uint32_t compaction_ledger = 0;
    // A compaction swapped in (by whichever call found it ready), until finish_compaction
    // reports it.
    std::optional<LedgerVerifier::Compaction> finished;
    void flush_and_maybe_compact() {
        flush();
        auto* store = nodes();
        if (!store) return;
        if (!background) { if (store->wants_compaction()) compact(true); return; }
        // In the background only a compaction already running is swapped in: a new one starts
        // where a follower starts it, after a commit, so its roots include the committed state
        // (a failed batch rewinds to it).
        if (store->compacting()) swap_in_ready(false);
    }
    // The state root of the ledger a failed batch goes back to (commit_point, resume, rewind;
    // zero: none named). Automatic compaction can run mid-batch, from apply or a persist,
    // before the batch commits: it keeps this tree too, or a one-off export failing after it
    // could not resume at its archive's tip and would replay the archive from its snapshot.
    r::uint256 committed{};
    // The newest state tree whole, then (`with_committed`) what the committed ledger's tree
    // adds, then what the earlier kept ledgers' trees add, newest first.
    std::vector<r::uint256> compaction_roots(r::SHAMapHash const& hash, bool with_committed) const {
        std::vector<r::uint256> roots{hash.asUInt256()};
        if (with_committed && committed != r::uint256{} && committed != hash.asUInt256()) roots.push_back(committed);
        for (auto it = kept_roots.rbegin(); it != kept_roots.rend(); ++it)
            if (it->first != hash.asUInt256() && it->first != r::uint256{}) roots.push_back(it->first);
        for (auto it = kept_roots.rbegin(); it != kept_roots.rend(); ++it)
            if (it->second != r::uint256{}) roots.push_back(it->second);
        return roots;
    }
    // `automatic`: one the store's growth started (it keeps the committed tree too).
    std::uint64_t compact(bool automatic) {
        auto* nodes = this->nodes();
        if (!nodes) throw Error("Only disk-backed verification scratch nodes can be compacted");
        flush();
        auto hash = state->getHash();
        state.reset();
        family.reset();
        // A compaction that fails leaves the store as it was (swap_in puts the old one back), so
        // the state is read from it again either way. Should that reload fail too, the
        // compaction's error is still the one thrown (the cause), the reload's noted beside it.
        std::uint64_t copied = 0;
        try { copied = hash.asUInt256() == r::uint256{} ? 0 : nodes->compact(compaction_roots(hash, automatic)); }
        catch (...) {
            auto failure = std::current_exception();
            try { reload(hash); }
            catch (std::exception const& reloading) {
                std::cerr << Json{{"event", "compaction_reload_failed"}, {"error", reloading.what()}}.dump() << std::endl;
            } catch (...) {
                std::cerr << Json{{"event", "compaction_reload_failed"}, {"error", "unknown"}}.dump() << std::endl;
            }
            std::rethrow_exception(failure);
        }
        reload(hash);
        return copied;
    }
    bool start_compaction(std::shared_future<void> gate) {
        auto* nodes = this->nodes();
        if (!nodes || nodes->compacting()) return false;
        flush();  // everything reachable from the roots is on disk
        auto hash = state->getHash();
        if (hash.asUInt256() == r::uint256{}) return false;
        compaction_ledger = current.sequence;
        return nodes->start_compaction(compaction_roots(hash, true), std::clamp(std::thread::hardware_concurrency() / 8, 2u, 8u), std::move(gate));
    }
    void swap_in_ready(bool wait) {
        auto* nodes = this->nodes();
        if (!nodes) return;
        if (auto done = nodes->finish_compaction(wait)) finished = LedgerVerifier::Compaction{done->nodes, done->seconds, compaction_ledger};
    }
    std::optional<LedgerVerifier::Compaction> finish_compaction(bool wait) {
        swap_in_ready(wait);
        return std::exchange(finished, std::nullopt);
    }
    // The newest verified state, for readers on other threads (the prefetcher): set as each
    // ledger applies and as the state is reloaded.
    std::mutex published_mutex;
    std::shared_ptr<r::SHAMap const> published;
    void publish() { std::lock_guard lock(published_mutex); published = state; }
    std::shared_ptr<r::SHAMap const> view() { std::lock_guard lock(published_mutex); return published; }
    std::once_flag prefetching;
    std::unique_ptr<StatePrefetcher> prefetcher;  // last: its threads stop before the rest is destroyed
};
LedgerVerifier::LedgerVerifier(fs::path const& backing, NodeSink sink) : impl_(std::make_unique<Impl>(backing, std::move(sink))) {}
LedgerVerifier::LedgerVerifier(std::shared_ptr<r::node_store::Database> database) : impl_(std::make_unique<Impl>(std::move(database))) {}
LedgerVerifier::~LedgerVerifier() = default;
void LedgerVerifier::add_snapshot_object(ObjectRecord const& item) {
    auto& i = *impl_;
    if (i.ready) throw Error("Snapshot is already sealed");
    if (!i.last_snapshot_key.empty() && item.key <= i.last_snapshot_key) throw Error("Snapshot keys must be strictly increasing across parts");
    // Disk-backed verifiers stream sorted snapshots straight into their node store.
    if (i.family.backed()) {
        i.unsent.push_back(item);
        i.last_snapshot_key = item.key;
        if (i.unsent.size() == 4096) i.send(std::exchange(i.unsent, {}));
        return;
    }
    check_object(item);
    if (!i.state->addItem(r::SHAMapNodeType::TnAccountState, r::makeShamapitem(key256(item.key), slice(item.object))))
        throw Error("Duplicate snapshot key");
    i.last_snapshot_key = item.key;
}
void LedgerVerifier::add_snapshot_objects(std::vector<ObjectRecord> const& items) {
    if (!impl_->family.backed()) { for (auto const& item : items) add_snapshot_object(item); return; }
    add_snapshot_objects(std::vector<ObjectRecord>(items));
}
void LedgerVerifier::add_snapshot_objects(std::vector<ObjectRecord>&& items) {
    auto& i = *impl_;
    if (!i.family.backed()) { for (auto const& item : items) add_snapshot_object(item); return; }
    if (i.ready) throw Error("Snapshot is already sealed");
    for (std::size_t n = 0; n < items.size(); ++n) {
        auto const& previous = n ? items[n - 1].key : i.last_snapshot_key;
        if (!previous.empty() && items[n].key <= previous) throw Error("Snapshot keys must be strictly increasing across parts");
    }
    if (items.empty()) return;
    // Canonical checks and leaf hashes are independent per object; placement is ordered.
    i.send(std::exchange(i.unsent, {}));
    auto last = items.back().key;
    i.send(std::move(items));
    i.last_snapshot_key = std::move(last);
}
Header LedgerVerifier::finish_snapshot(Bytes const& header, std::uint32_t sequence, std::string const& account_hash) {
    auto& i = *impl_;
    if (i.ready) throw Error("Snapshot is already sealed");
    auto h = decode_header(header);
    if (h.sequence != sequence || h.account_hash != account_hash) throw Error("Snapshot state root mismatch");
    if (i.family.backed()) {
        i.send(std::exchange(i.unsent, {}));
        r::uint256 root{};
        if (i.loader) { root = i.loader->finish(sequence); i.load_seconds = i.loader->timings(); i.loader.reset(); }
        else if (i.builder) root = i.builder->finish(sequence);
        i.builder.reset();
        if (to_hex(raw(root)) != h.account_hash) throw Error("Snapshot state root mismatch");
        i.family.db().sync();
        i.current = h;
        i.remember(h);
        i.reload(r::SHAMapHash{root});
        i.ready = true;
        return h;
    }
    if (to_hex(raw(i.state->getHash().asUInt256())) != h.account_hash) throw Error("Snapshot state root mismatch");
    i.current = h;
    i.ready = true;
    return h;
}
std::pair<Header, std::vector<TransactionIndex>> LedgerVerifier::verify_and_apply(LedgerRecord const& ledger) {
    auto& i = *impl_;
    if (!i.ready) throw Error("A verified snapshot is required");
    auto h = decode_header(ledger.header);
    if (h.sequence != ledger.sequence || static_cast<std::uint64_t>(i.current.sequence) + 1 != h.sequence ||
        h.parent_hash != i.current.hash) throw Error("Ledger parent chain or sequence mismatch");
    auto clock = [] { return std::chrono::steady_clock::now(); };
    auto since = [&](auto start) { return std::chrono::duration<double>(clock() - start).count(); };
    // Parsing checks and summaries are independent per transaction and object: they run as
    // several parts at once (LedgerParts); only the tree updates below are sequential.
    auto start = clock();
    std::vector<wire::Summary> summaries(ledger.transactions.size());
    {
        auto items = ledger.transactions.size() + ledger.patches.size();
        unsigned threads = items < 64 ? 1 : std::clamp(std::thread::hardware_concurrency(), 1u, 8u);
        LedgerParts checks(threads, threads - 1, [&](std::size_t t) {
            for (std::size_t n = t; n < ledger.transactions.size(); n += threads) {
                check_transaction(ledger.transactions[n]);
                summaries[n] = wire::summarize(ledger.transactions[n]);
            }
            for (std::size_t n = t; n < ledger.patches.size(); n += threads)
                if (!ledger.patches[n].object.empty()) check_object(ledger.patches[n]);
        });
        checks.wait();
    }
    i.checks_seconds += since(start); start = clock();
    // The transaction tree is independent of the state: it is built on a pool thread while the
    // state changes apply (or after them, by this one, should none be free), and both roots
    // are checked before the ledger is accepted.
    std::pair<std::vector<TransactionIndex>, double> built;
    LedgerParts transactions(1, ledger.transactions.empty() ? 0 : 1, [&](std::size_t) {
        auto began = clock();
        r::SHAMap tx_map(r::SHAMapType::TRANSACTION, i.family);
        bool stored = i.family.exports() || (i.keep && i.family.backed());
        if (!stored) tx_map.setUnbacked();
        Bytes last_hash;
        std::vector<bool> positions(ledger.transactions.size());
        std::vector<TransactionIndex> indexes;
        for (std::size_t n = 0; n < ledger.transactions.size(); ++n) {
            auto const& record = ledger.transactions[n];
            auto& summary = summaries[n];
            auto hash = summary.hash;
            if (!last_hash.empty() && hash <= last_hash) throw Error("Transactions must be sorted by unique hash");
            if (summary.index >= positions.size() || positions[summary.index]) throw Error("Invalid transaction index permutation");
            positions[summary.index] = true;
            indexes.push_back(TransactionIndex{hash, summary.index, std::move(summary.accounts)});
            r::Serializer payload;
            payload.addVL(slice(record.transaction));
            payload.addVL(slice(record.metadata));
            if (!tx_map.addItem(r::SHAMapNodeType::TnTransactionMd, r::makeShamapitem(key256(hash), payload.slice())))
                throw Error("Duplicate transaction hash");
            last_hash = std::move(hash);
        }
        if (stored) tx_map.flushDirty(r::NodeObjectType::TransactionNode);
        if (to_hex(raw(tx_map.getHash().asUInt256())) != h.transaction_hash) throw Error("Transaction SHAMap root mismatch");
        built = {std::move(indexes), since(began)};
    });
    auto next = i.state->snapShot(true);
    Bytes previous;
    for (auto const& patch : ledger.patches) {
        auto key = key256(patch.key);
        if (!previous.empty() && patch.key <= previous) throw Error("Patch keys must be strictly increasing");
        if (patch.object.empty()) {
            if (!next->delItem(key)) throw Error("Patch deletes a missing object");
        } else {
            auto item = r::makeShamapitem(key, slice(patch.object));
            bool ok = next->peekItem(key) ? next->updateGiveItem(r::SHAMapNodeType::TnAccountState, item)
                                          : next->addItem(r::SHAMapNodeType::TnAccountState, item);
            if (!ok) throw Error("Object patch failed");
        }
        previous = patch.key;
    }
    i.state_seconds += since(start); start = clock();
    if (i.family.backed()) next->flushDirty(r::NodeObjectType::AccountNode);
    if (to_hex(raw(next->getHash().asUInt256())) != h.account_hash) throw Error("Patched state SHAMap root mismatch");
    i.flush_seconds += since(start); start = clock();
    transactions.wait();
    auto& [indexes, transaction_seconds] = built;
    i.transactions_seconds += transaction_seconds;  // alongside the state's, not added to it
    start = clock();
    i.state = std::move(next);
    i.publish();
    i.current = h;
    i.remember(h);
    i.touched += ledger.patches.size();
    if (++i.pending >= i.retain_ledgers || i.touched >= i.retain_objects) i.flush_and_maybe_compact();
    i.persist_seconds += since(start);
    return {std::move(h), std::move(indexes)};
}
VerifiedLedger LedgerVerifier::apply(LedgerRecord const& ledger) {
    auto [h, indexes] = verify_and_apply(ledger);
    return VerifiedLedger(std::move(h), ledger, std::move(indexes));
}
VerifiedLedger LedgerVerifier::apply(LedgerRecord&& ledger) {
    auto [h, indexes] = verify_and_apply(ledger);
    return VerifiedLedger(std::move(h), std::move(ledger), std::move(indexes));
}
void LedgerVerifier::advance(LedgerRecord const& ledger) { (void)verify_and_apply(ledger); }
void LedgerVerifier::write_batches(std::uint64_t nodes) {
    if (auto* store = impl_->nodes()) store->write_batches(nodes);
}
void LedgerVerifier::prefetch(std::vector<Bytes> keys) {
    auto& i = *impl_;
    if (!i.family.backed() || keys.empty()) return;
    // Each read mostly waits on the store: a few threads keep ahead of the one applying ledgers.
    std::call_once(i.prefetching, [&i] {
        i.prefetcher = std::make_unique<StatePrefetcher>([&i] { return i.view(); }, std::clamp(std::thread::hardware_concurrency() / 8, 2u, 8u));
    });
    i.prefetcher->add(std::move(keys));
}
Header const& LedgerVerifier::header() const {
    if (!impl_->ready) throw Error("Snapshot is not verified");
    return impl_->current;
}
namespace {
// Walk a persisted state tree in key order with several threads. Subtrees two levels down
// are independent, so each is read straight from the node store (no SHAMap objects) by a
// worker, and results are handed to `visit` strictly in subtree order: the same stream,
// in the same order, as a SHAMap iteration.
void walk_state(PersistentNodes const& nodes, r::uint256 const& root, std::function<void(ObjectRecord const&)> const& visit) {
    Bytes const zero(32, '\0');
    auto children = [&](Bytes const& value) {
        std::vector<Bytes> out;
        if (value.compare(1, 4, Bytes("MIN\0", 4)) != 0) return out;
        if (value.size() != 5 + 16 * 32) throw Error("Corrupt persisted inner node");
        for (unsigned b = 0; b < 16; ++b) if (auto child = value.substr(5 + 32 * b, 32); child != zero) out.push_back(std::move(child));
        return out;
    };
    auto leaf = [](Bytes const& value) {
        if (value.compare(1, 4, Bytes("MLN\0", 4)) != 0 || value.size() < 5 + 32) throw Error("Unexpected node in state tree");
        return ObjectRecord{value.substr(value.size() - 32), value.substr(5, value.size() - 5 - 32)};
    };
    // Subtree roots at depth <= 2, in key order.
    std::vector<Bytes> tasks;
    for (auto const& a : children(nodes.get(raw(root)))) {
        auto value = nodes.get(a);
        auto next = children(value);
        if (next.empty()) tasks.push_back(a); else tasks.insert(tasks.end(), next.begin(), next.end());
    }
    auto collect = [&](Bytes const& hash) {
        std::vector<ObjectRecord> out;
        std::vector<Bytes> stack{hash};
        while (!stack.empty()) {
            auto value = nodes.get(stack.back()); stack.pop_back();
            auto below = children(value);
            if (below.empty()) out.push_back(leaf(value));
            else for (auto it = below.rbegin(); it != below.rend(); ++it) stack.push_back(std::move(*it));
        }
        return out;
    };
    unsigned threads = std::clamp(std::thread::hardware_concurrency(), 1u, 16u);
    std::deque<std::future<std::vector<ObjectRecord>>> running;
    std::size_t next = 0;
    while (next < tasks.size() || !running.empty()) {
        while (next < tasks.size() && running.size() < 2 * threads)
            running.push_back(std::async(std::launch::async, collect, std::cref(tasks[next++])));
        for (auto const& item : running.front().get()) visit(item);
        running.pop_front();
    }
}
}
void LedgerVerifier::state_objects(std::function<void(ObjectRecord const&)> const& visit) const {
    (void)header();
    if (auto* nodes = impl_->family.backed() ? dynamic_cast<PersistentNodes*>(&impl_->family.db()) : nullptr) {
        impl_->flush();
        auto root = impl_->state->getHash().asUInt256();
        if (root != r::uint256{}) walk_state(*nodes, root, visit);
        return;
    }
    Bytes last;
    for (;;) {
        bool finished;
        {
            auto iterator = last.empty() ? impl_->state->begin() : impl_->state->upperBound(key256(last));
            for (unsigned count = 0; iterator != impl_->state->end() && count < 4096; ++iterator, ++count) {
                auto const& item = *iterator; last = raw(item.key()); visit({last, bytes(item.slice())});
            }
            finished = iterator == impl_->state->end();
        }
        if (finished) break;
        // Release nodes loaded by traversal; retaining one iterator for the
        // entire map would otherwise pin every visited subtree in memory.
        impl_->flush();
    }
}
Bytes LedgerVerifier::state_object(Bytes const& key) const {
    (void)header();
    auto item = impl_->state->peekItem(key256(key));
    return item ? bytes(item->slice()) : Bytes{};
}
Bytes LedgerVerifier::state_successor(Bytes const& key) const {
    (void)header();
    auto it = impl_->state->upperBound(key256(key));
    return it == impl_->state->end() ? Bytes(32, '\xff') : raw(it->key());
}
Bytes LedgerVerifier::state_predecessor(Bytes const& key) const {
    (void)header();
    auto it = impl_->state->lowerBound(key256(key));
    return it == impl_->state->end() ? Bytes(32, '\0') : raw(it->key());
}
void LedgerVerifier::persist() { (void)header(); impl_->flush_and_maybe_compact(); }
std::uint64_t LedgerVerifier::compact_nodes() { (void)header(); return impl_->compact(false); }
void LedgerVerifier::commit_point() { (void)header(); impl_->committed = key256(from_hex(impl_->current.account_hash)); }
bool LedgerVerifier::start_compaction(std::shared_future<void> gate) { (void)header(); return impl_->start_compaction(std::move(gate)); }
std::optional<LedgerVerifier::Compaction> LedgerVerifier::finish_compaction(bool wait) { return impl_->finish_compaction(wait); }
bool LedgerVerifier::compacting() const { auto* nodes = impl_->nodes(); return nodes && nodes->compacting(); }
void LedgerVerifier::compact_in_background(bool background) { impl_->background = background; }
void LedgerVerifier::compaction_floor(std::uint64_t nodes) { if (auto* store = impl_->nodes()) store->compaction_floor(nodes); }
Json LedgerVerifier::timings() const {
    auto const& i = *impl_;
    return {{"checks", i.checks_seconds}, {"transaction_tree", i.transactions_seconds}, {"state_updates", i.state_seconds},
            {"state_hash_and_node_writes", i.flush_seconds}, {"reloads", i.persist_seconds}, {"snapshot_load", i.load_seconds},
            {"prefetched_keys", i.prefetcher ? i.prefetcher->keys() : 0}};
}
void LedgerVerifier::retain(std::uint64_t ledgers, std::uint64_t objects) {
    impl_->retain_ledgers = std::max<std::uint64_t>(1, ledgers);
    impl_->retain_objects = std::max<std::uint64_t>(1, objects);
}
void LedgerVerifier::persist_transactions(LedgerRecord const& ledger) {
    // The record must be the verified ledger itself (a baseline that already holds its state
    // changes): its transaction tree is checked against the record's own header, so a record of
    // another ledger would pass as this one's otherwise.
    auto const& current = header();
    auto h = decode_header(ledger.header);
    if (h.sequence != ledger.sequence || h.sequence != current.sequence || h.hash != current.hash)
        throw Error("Transactions are not the verified ledger's");
    r::SHAMap map(r::SHAMapType::TRANSACTION, impl_->family);
    if (!impl_->family.exports()) map.setUnbacked();
    for (auto const& record : ledger.transactions) {
        check_transaction(record);
        r::Serializer payload; payload.addVL(slice(record.transaction)); payload.addVL(slice(record.metadata));
        if (!map.addItem(r::SHAMapNodeType::TnTransactionMd, r::makeShamapitem(key256(wire::transaction_id(record.transaction)), payload.slice()))) throw Error("Duplicate transaction");
    }
    if (impl_->family.exports()) map.flushDirty(r::NodeObjectType::TransactionNode);
    if (to_hex(raw(map.getHash().asUInt256())) != h.transaction_hash) throw Error("Transaction SHAMap root mismatch");
    if (impl_->family.backed()) impl_->family.db().sync();
}
void LedgerVerifier::resume(Bytes const& verified_header) {
    auto& i = *impl_;
    if (i.ready || !i.family.backed()) throw Error("Resume requires an unused disk-backed verifier");
    auto h = decode_header(verified_header);
    auto hash = r::SHAMapHash{key256(from_hex(h.account_hash))};
    i.state = std::make_shared<r::SHAMap>(r::SHAMapType::STATE, hash.asUInt256(), i.family);
    i.state->setLedgerSeq(h.sequence);
    if (hash.asUInt256() != r::uint256{} && !i.state->fetchRoot(hash, nullptr)) throw Error("Verification checkpoint nodes are missing");
    i.state->clearSynching(); i.current = h; i.ready = true;
    i.committed = hash.asUInt256();
    i.publish();
    i.remember(h);
}
void LedgerVerifier::rewind(Bytes const& verified_header) {
    auto& i = *impl_;
    if (!i.ready || !i.family.backed()) throw Error("Rewind requires a disk-backed verifier in use");
    auto h = decode_header(verified_header);
    // The ledgers applied since leave the window; the rewound one is its newest again.
    std::pair roots{key256(from_hex(h.account_hash)), key256(from_hex(h.transaction_hash))};
    while (!i.kept_roots.empty() && i.kept_roots.back() != roots) i.kept_roots.pop_back();
    i.current = h;
    i.committed = roots.first;
    i.reload(r::SHAMapHash{roots.first});
    i.remember(h);
}
void LedgerVerifier::keep_ledgers(std::size_t ledgers) {
    impl_->keep = ledgers;
    while (impl_->kept_roots.size() > ledgers) impl_->kept_roots.pop_front();
}
void LedgerVerifier::keep_ledger(Header const& header) { impl_->remember(header); }
std::shared_ptr<xrpl::node_store::Database> LedgerVerifier::node_store() const { return impl_->family.database(); }
std::vector<std::pair<Bytes, Bytes>> LedgerVerifier::state_nodes(Bytes const& position, unsigned depth) const {
    (void)header();
    auto id = r::deserializeSHAMapNodeID(position);
    if (!id) throw Error("Invalid state tree position");
    std::vector<r::SHAMapNodeData> data;
    std::vector<std::pair<Bytes, Bytes>> nodes;
    if (!impl_->state->getNodeFat(*id, data, true, depth)) return nodes;
    for (auto& node : data) nodes.emplace_back(node.nodeID.getRawString(), Bytes(reinterpret_cast<char const*>(node.data.data()), node.data.size()));
    return nodes;
}
std::vector<Bytes> closing_keys(std::uint32_t sequence) {
    std::vector<Bytes> keys{raw(r::keylet::skip().key), raw(r::keylet::skip(sequence - 1).key)};
    if (sequence % 256 == 0) keys.push_back(raw(r::keylet::negativeUNL().key));
    return keys;
}
Json write_state_snapshot(LedgerVerifier& verifier, fs::path const& out, int level, unsigned jobs, Json* timings) {
    using Clock = std::chrono::steady_clock;
    auto seconds = [](Clock::time_point since) { return std::chrono::duration<double>(Clock::now() - since).count(); };
    auto started = Clock::now();
    verifier.persist();
    auto const& header = verifier.header();
    auto prefix = "snapshots/" + std::to_string(header.sequence) + "." + header.hash.substr(0, 16);
    fs::create_directories(out / prefix);
    auto describe = [&out](std::string const& key) {
        sync_file(out / key);
        return Json{{"object", key}, {"sha256", file_sha256(out / key)}, {"size", fs::file_size(out / key)}};
    };
    Json parts = Json::array();
    std::vector<ObjectRecord> objects;
    std::uint64_t pending = 0, total_size = 0, count = 0, submitted = 0;
    double waited = 0;
    // Up to `jobs` parts compress at once; descriptors are collected in part order.
    std::deque<std::future<Json>> compressing;
    auto collect = [&](std::size_t keep) {
        auto wait_started = Clock::now();
        while (compressing.size() > keep) {
            auto part = compressing.front().get(); compressing.pop_front();
            total_size += part.at("size").get<std::uint64_t>(); parts.push_back(std::move(part));
        }
        waited += seconds(wait_started);
    };
    auto flush_part = [&] {
        if (objects.empty() && submitted) return;
        auto key = prefix + "/part-" + std::to_string(submitted++) + ".xrps";
        compressing.push_back(std::async(std::launch::async, [&out, &describe, key, level, batch = std::move(objects)] {
            write_snapshot_part(out / key, batch, {}, level);
            auto part = describe(key); part["records"] = batch.size();
            return part;
        }));
        objects = {}; pending = 0;
        collect(std::max(jobs, 1u));
    };
    try {
        verifier.state_objects([&](ObjectRecord const& item) {
            objects.push_back(item); ++count; pending += item.object.size() + 36;
            if (pending >= snapshot_part_bytes) flush_part();
        });
        flush_part();
    } catch (...) {
        for (auto& part : compressing) if (part.valid()) part.wait();
        throw;
    }
    collect(0);
    Json snapshot{{"format_version", 2}, {"record_format", "keyed-stobject-v2"}, {"seq", header.sequence},
                  {"account_hash", header.account_hash}, {"ledger_header", to_hex(header.raw)},
                  {"dictionary_sha256", std::string(64, '0')}, {"parts", parts}};
    // The parts' directory entries are durable before the index that names them is, and the
    // index goes in under its name whole, its own entry and the directories up from it synced:
    // a crash leaves either no index or one whose every part is there.
    sync_file(out / prefix);
    auto index = prefix + "/index.json";
    auto temp = out / index; temp += ".part";
    write_file(temp, snapshot.dump());
    durable_rename(temp, out / index);
    if (timings) *timings = {{"seconds", seconds(started)}, {"waiting_for_compression", waited}, {"jobs", jobs}};
    return {{"seq", header.sequence}, {"prefix", prefix}, {"account_hash", header.account_hash}, {"ledger_hash", header.hash},
            {"index", describe(index)}, {"parts", parts.size()}, {"total_size", total_size}, {"objects", count}};
}
std::optional<Json> existing_state_snapshot(Header const& header, fs::path const& out) {
    auto prefix = "snapshots/" + std::to_string(header.sequence) + "." + header.hash.substr(0, 16);
    auto index = prefix + "/index.json";
    auto path = out / index;
    // Anything short of an index of exactly this ledger (its whole header, not the directory's
    // 16-character hash prefix) whose every part is present, named as write_state_snapshot names
    // them and of the size and digest it lists, is not taken: the snapshot is written again.
    try {
        if (fs::is_symlink(fs::symlink_status(path)) || !fs::is_regular_file(path)) return std::nullopt;
        auto bytes = read_file(path, 16 * 1024 * 1024);
        if (fs::file_size(path) != bytes.size()) return std::nullopt;
        auto snapshot = Json::parse(bytes);
        if (snapshot.at("format_version") != 2 || snapshot.at("record_format") != "keyed-stobject-v2" ||
            snapshot.at("dictionary_sha256") != std::string(64, '0') || snapshot.at("seq").get<std::uint32_t>() != header.sequence ||
            snapshot.at("account_hash").get<std::string>() != header.account_hash ||
            snapshot.at("ledger_header").get<std::string>() != to_hex(header.raw))
            return std::nullopt;
        auto const& parts = snapshot.at("parts");
        if (!parts.is_array() || parts.empty()) return std::nullopt;
        std::uint64_t total_size = 0, count = 0;
        for (std::size_t n = 0; n < parts.size(); ++n) {
            auto part = parse_object(parts[n]);
            if (part.object != prefix + "/part-" + std::to_string(n) + ".xrps" || !parts[n].at("records").is_number_unsigned() ||
                !matches(out / part.object, part))
                return std::nullopt;
            total_size += part.size; count += parts[n].at("records").get<std::uint64_t>();
        }
        // As durable as write_state_snapshot leaves it (a batch that wrote it may have failed
        // before): the index, and the directory entries of the parts and the index up to `out`.
        sync_file(path);
        sync_file(out / prefix);
        sync_file(out / "snapshots");
        sync_file(out);
        return Json{{"seq", header.sequence}, {"prefix", prefix}, {"account_hash", header.account_hash}, {"ledger_hash", header.hash},
                    {"index", {{"object", index}, {"sha256", sha256(bytes)}, {"size", bytes.size()}}}, {"parts", parts.size()},
                    {"total_size", total_size}, {"objects", count}};
    } catch (std::exception const&) {
        return std::nullopt;  // unreadable or malformed: written again
    }
}
std::optional<Json> existing_state_snapshot(LedgerVerifier const& verifier, fs::path const& out) {
    auto existing = existing_state_snapshot(verifier.header(), out);
    if (!existing) return existing;
    // The parts are read back in order beside the verified state, object for object: one that
    // differs, is missing or is left over means the snapshot is written again. The walk is the
    // one write_state_snapshot makes, without its compression.
    try {
        auto const& described = existing->at("index");
        auto bytes = read_file(out / described.at("object").get<std::string>(), 16 * 1024 * 1024);
        if (sha256(bytes) != described.at("sha256").get<std::string>()) return std::nullopt;
        auto parts = Json::parse(bytes).at("parts");
        std::size_t part = 0, next = 0;
        std::vector<ObjectRecord> objects;
        auto stored = [&]() -> ObjectRecord const* {
            while (next == objects.size()) {
                if (part == parts.size()) return nullptr;
                auto const& listed = parts.at(part++);
                objects.clear(); next = 0;
                read_snapshot_part(out / listed.at("object").get<std::string>(), {}, listed.at("records").get<std::uint64_t>(),
                                   [&](ObjectRecord&& object) { objects.push_back(std::move(object)); });
            }
            return &objects[next++];
        };
        verifier.state_objects([&](ObjectRecord const& object) {
            auto const* found = stored();
            if (!found || found->key != object.key || found->object != object.object) throw Error("Snapshot parts differ from the verified state");
        });
        if (stored()) return std::nullopt;
        return existing;
    } catch (std::exception const&) {
        return std::nullopt;
    }
}
} // namespace xrpla::core
