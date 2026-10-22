#include <xrpla/core/operations.hpp>
#include "validators.hpp"
#include "ws_client.hpp"
#include "peer_ledgers.hpp"
#include "piece_feed.hpp"
#include "peer_serving.hpp"
#include "wire.hpp"
#include <sodium.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <iostream>
#include <iterator>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <csignal>
#include <cstdlib>
#include <thread>
#include <fcntl.h>
#include <unistd.h>

namespace xrpla::core {
namespace {
template<class T> Bytes raw(T const& value) { return {reinterpret_cast<char const*>(value.data()), value.size()}; }
std::string normalized(std::string const& text) { return to_hex(from_hex(text)); }
void success(Json const& response) {
    if (response.contains("error")) throw Error("Source RPC: " + response.dump());
}
void pinned(Json const& response, Header const& header) {
    if (response.contains("ledger_hash") && normalized(response.at("ledger_hash").get<std::string>()) != header.hash)
        throw Error("Source returned a different ledger hash");
    if (response.contains("ledger_index") && response.at("ledger_index").get<std::uint32_t>() != header.sequence)
        throw Error("Source returned a different ledger sequence");
}
std::pair<Header, Json> get_ledger(RpcClient const& rpc, std::uint32_t sequence) {
    auto response = rpc.call("ledger", {{"ledger_index", sequence}, {"binary", true}, {"transactions", true}, {"expand", true}, {"diff", true}});
    success(response);
    if (!response.value("validated", false)) throw Error("Source ledger is not validated");
    auto header = decode_header(from_hex(response.at("ledger").at("ledger_data").get<std::string>()));
    if (header.sequence != sequence || normalized(response.at("ledger_hash").get<std::string>()) != header.hash)
        throw Error("Source header hash or sequence mismatch");
    return {header, response.at("ledger")};
}
// The keys a ledger holds (from its diff) nearest each even division of the key space, to
// split a state download at: Clio pages only from a marker the ledger holds, and each page
// starts after its marker.
std::vector<Bytes> split_keys(Json const& document, unsigned parts) {
    std::vector<Bytes> held, splits;
    if (document.contains("diff"))
        for (auto const& item : document.at("diff"))
            if (!item.at("object").get<std::string>().empty()) held.push_back(from_hex(item.at("object_id").get<std::string>()));
    std::sort(held.begin(), held.end());
    auto position = [](Bytes const& key) {
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < 8 && i < key.size(); ++i) value = value << 8 | static_cast<unsigned char>(key[i]);
        return value;
    };
    for (unsigned n = 1; n < parts && !held.empty(); ++n) {
        auto division = std::numeric_limits<std::uint64_t>::max() / parts * n;
        auto it = std::lower_bound(held.begin(), held.end(), division, [&](Bytes const& key, std::uint64_t at) { return position(key) < at; });
        if (it == held.end() || (it != held.begin() && division - position(*std::prev(it)) < position(*it) - division)) --it;
        if (splits.empty() || *it > splits.back()) splits.push_back(*it);
    }
    return splits;
}
// State pages a range reads ahead of its turn past the memory budget (ordered_ranges), kept in
// order in a file of the range's own: each page as its length (8 bytes), then each object's key
// and object, each after its length (4 bytes). The file has no name (O_TMPFILE, or unlinked as
// soon as it is created where the file system lacks that), so its space returns when it is
// closed, however the export ends; nothing in it is trusted beyond what the state's objects are
// anyway (the verifier checks them against the account hash).
struct Spill {
    int fd = -1;
    std::uint64_t written = 0, taken = 0;  // bytes the reader appended, and the consumer read back
    Spill() = default;
    Spill(Spill const&) = delete;
    Spill& operator=(Spill const&) = delete;
    ~Spill() { if (fd >= 0) ::close(fd); }
};
int open_spill(fs::path const& directory) {
#ifdef O_TMPFILE
    if (int fd = ::open(directory.c_str(), O_TMPFILE | O_RDWR | O_CLOEXEC, 0600); fd >= 0) return fd;
#endif
    auto name = (directory / ".state-ahead-XXXXXX").string();
    int fd = ::mkostemp(name.data(), O_CLOEXEC);
    if (fd < 0) throw Error("Cannot create a file for state pages read ahead in " + directory.string() + ": " + std::strerror(errno));
    if (::unlink(name.c_str()) != 0) {
        auto why = errno;
        ::close(fd);
        throw Error("Cannot unlink " + name + ": " + std::strerror(why));
    }
    return fd;
}
// The length of `objects` as a page in the file.
std::uint64_t spilled_size(std::vector<ObjectRecord> const& objects) {
    std::uint64_t size = 8;
    for (auto const& object : objects) size += 8 + object.key.size() + object.object.size();
    return size;
}
// Writes `objects` as a page at offset `at` (spilled_size(objects) bytes).
void spill_page(int fd, std::uint64_t at, std::vector<ObjectRecord> const& objects) {
    Bytes page;
    auto length = [&](std::uint64_t value, int bytes) { for (int shift = 8 * (bytes - 1); shift >= 0; shift -= 8) page += static_cast<char>(value >> shift); };
    length(0, 8);
    for (auto const& object : objects) {
        if (object.key.size() > std::numeric_limits<std::uint32_t>::max() || object.object.size() > std::numeric_limits<std::uint32_t>::max())
            throw Error("A state object read ahead is too large for its page");
        length(object.key.size(), 4); page += object.key; length(object.object.size(), 4); page += object.object;
    }
    for (int n = 0; n < 8; ++n) page[n] = static_cast<char>((page.size() - 8) >> (56 - 8 * n));
    for (std::size_t done = 0; done < page.size();) {
        auto wrote = ::pwrite(fd, page.data() + done, page.size() - done, static_cast<off_t>(at + done));
        if (wrote < 0 && errno == EINTR) continue;
        if (wrote < 0) throw Error(std::string("Cannot write state pages read ahead: ") + std::strerror(errno));
        if (wrote == 0) throw Error("Cannot write state pages read ahead: nothing was written");
        done += static_cast<std::size_t>(wrote);
    }
}
void spill_read(int fd, std::uint64_t at, char* into, std::size_t size) {
    for (std::size_t done = 0; done < size;) {
        auto got = ::pread(fd, into + done, size - done, static_cast<off_t>(at + done));
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) throw Error("Cannot read back state pages read ahead");
        done += static_cast<std::size_t>(got);
    }
}
// The page spill_page wrote at `at`; `at` moves past it.
std::vector<ObjectRecord> unspill_page(int fd, std::uint64_t& at) {
    std::array<unsigned char, 8> head{};
    spill_read(fd, at, reinterpret_cast<char*>(head.data()), head.size());
    std::uint64_t size = 0;
    for (auto byte : head) size = size << 8 | byte;
    Bytes page(size, '\0');
    spill_read(fd, at + head.size(), page.data(), page.size());
    at += head.size() + size;
    std::size_t position = 0;
    auto field = [&] {
        if (page.size() - position < 4) throw Error("State pages read ahead are damaged");
        std::uint32_t length = 0;
        for (int n = 0; n < 4; ++n) length = length << 8 | static_cast<unsigned char>(page[position++]);
        if (page.size() - position < length) throw Error("State pages read ahead are damaged");
        auto value = page.substr(position, length);
        position += length;
        return value;
    };
    std::vector<ObjectRecord> objects;
    while (position < page.size()) {
        auto key = field();
        objects.push_back({std::move(key), field()});
    }
    return objects;
}
// A ledger's state read as `count` key ranges at once, each on its own thread and connection:
// `read(n, emit)` reads range n in key order, handing each page to `emit` (false: stop). The
// objects reach `visit` in key order (checked strictly increasing), so the ranges after the one
// being taken are read ahead of their turn. Their pages wait in memory, all ranges' together
// up to `kBytesAhead`, and past that in each range's spill file under `spill` (a range's pages
// in memory always come before those in its file: a page goes to memory only while the file
// holds none still to be read back). The range being taken may hold `kPagesAhead` pages beyond
// the budget, goes on appending to its file while that is still being read back, and otherwise
// waits for the consumer; no other reader waits while files are written, so all of them
// keep reading (bounded in memory alone, the later ranges filled the budget and waited, and
// only the range being taken progressed: about a tenth of the readers' pace on mainnet). Memory
// stays within the budget plus those pages (and one page a reader, each checking for room
// before it adds its page); the spill files hold at most the state itself (its objects as read,
// less those already taken), and in practice what the readers got ahead of the consumer. That
// is disk the export needs beside the archive's own files, and their space returns only once
// the state is read: so the files take at most half the space free on their file system when
// the download starts, and once that is reached or a file cannot be created or written (a full
// disk, a quota) no page is written to one again for the download. The readers then wait for
// room in the budget as the range being taken does (bounded in memory alone, slower but within
// the disk), and pages already in files are read back in turn; a page not written whole is never
// read (`written` moves past a page only once all of it is in the file).
using PageSink = std::function<bool(std::vector<ObjectRecord>)>;
// A number of bytes set in the environment variable `name` for tests, or `otherwise`.
std::uint64_t test_bytes(char const* name, std::uint64_t otherwise) {
    auto const* text = std::getenv(name);
    if (!text) return otherwise;
    std::uint64_t value = 0;
    auto [end, ec] = std::from_chars(text, text + std::strlen(text), value);
    if (ec != std::errc{} || *end || end == text) throw Error(std::string(name) + " must be a number of bytes");
    return value;
}
void ordered_ranges(std::size_t count, fs::path const& spill, std::function<void(std::size_t, PageSink const&)> const& read,
                    std::function<void(ObjectRecord const&)> const& visit) {
    constexpr std::size_t kPagesAhead = 4;
    // XRPLA_TEST_STATE_AHEAD_BYTES and XRPLA_TEST_STATE_SPILL_BYTES set another budget and
    // another room for the files for tests, whose states are far smaller (read at each state
    // download, so one process can try several).
    std::uint64_t const kBytesAhead = test_bytes("XRPLA_TEST_STATE_AHEAD_BYTES", 256ULL * 1024 * 1024);
    std::uint64_t const kSpillRoom = test_bytes("XRPLA_TEST_STATE_SPILL_BYTES", [&] {
        std::error_code ec;
        auto space = fs::space(spill, ec);
        return ec ? std::numeric_limits<std::uint64_t>::max() : space.available / 2;
    }());
    struct Page { std::vector<ObjectRecord> objects; std::uint64_t bytes = 0; };
    struct Range { std::deque<Page> pages; Spill spilled; bool done = false; };
    std::vector<Range> ranges(count);
    std::mutex mutex;
    std::condition_variable changed;
    std::exception_ptr failure;
    std::atomic<bool> stopping{false};
    std::uint64_t buffered = 0;  // bytes of every range's pages in memory
    std::uint64_t spilled = 0;   // bytes of every range's file, written or being written
    bool spilling = true;        // pages past the budget may still go to files
    std::size_t taking = 0;      // the range the consumer is taking pages from
    auto run = [&](std::size_t n) {
        auto& range = ranges[n];
        try {
            read(n, [&](std::vector<ObjectRecord> objects) {
                Page page{std::move(objects)};
                for (auto const& object : page.objects) page.bytes += object.key.size() + object.object.size() + sizeof(ObjectRecord);
                std::unique_lock lock(mutex);
                auto unread = [&] { return range.spilled.taken < range.spilled.written; };
                auto in_memory = [&] {
                    return !unread() && (buffered < kBytesAhead || (n == taking && range.pages.size() < kPagesAhead));
                };
                for (;;) {
                    // While pages go to files only the range being taken waits, and only once its
                    // file is read back (until then it goes on appending to it): each page taken
                    // wakes it. Once they do not, every reader waits so.
                    changed.wait(lock, [&] { return stopping || in_memory() || (spilling && (n != taking || unread())); });
                    if (stopping) return false;
                    if (in_memory()) {
                        buffered += page.bytes;
                        range.pages.push_back(std::move(page));
                        break;
                    }
                    auto size = spilled_size(page.objects);
                    std::string why;
                    if (size > kSpillRoom - std::min(spilled, kSpillRoom)) {
                        why = "the files of state pages read ahead would take more than " + std::to_string(kSpillRoom) + " bytes";
                    } else {
                        // Written with the lock released: only this reader appends, and the
                        // consumer reads no further than `written`, moved once the page is whole.
                        spilled += size;
                        auto at = range.spilled.written;
                        lock.unlock();
                        try {
                            if (range.spilled.fd < 0) range.spilled.fd = open_spill(spill);
                            spill_page(range.spilled.fd, at, page.objects);
                        } catch (std::exception const& error) {
                            why = error.what();
                        }
                        lock.lock();
                        if (why.empty()) {
                            range.spilled.written = at + size;
                            break;
                        }
                    }
                    if (spilling) {
                        spilling = false;
                        std::cerr << Json{{"event", "state_spill_stopped"}, {"spilled", spilled}, {"error", why}}.dump() << std::endl;
                    }
                }
                changed.notify_all();
                return true;
            });
        } catch (...) {
            std::lock_guard lock(mutex);
            if (!failure) failure = std::current_exception();
            stopping = true;
        }
        std::lock_guard lock(mutex);
        ranges[n].done = true;
        changed.notify_all();
    };
    std::vector<std::jthread> threads;
    // However the consumer leaves (done, a failure, a key out of order), readers waiting for
    // room are woken to stop before they are joined.
    struct Join {
        std::mutex& mutex; std::condition_variable& changed; std::atomic<bool>& stopping; std::vector<std::jthread>& threads;
        ~Join() {
            { std::lock_guard lock(mutex); stopping = true; }
            changed.notify_all();
            threads.clear();
        }
    } join{mutex, changed, stopping, threads};
    for (std::size_t n = 0; n < ranges.size(); ++n) threads.emplace_back(run, n);
    Bytes previous;
    for (std::size_t n = 0; n < ranges.size(); ++n) {
        auto& range = ranges[n];
        {
            // The next range is taken from now: its reader may go past the budget.
            std::lock_guard lock(mutex);
            taking = n;
        }
        changed.notify_all();
        for (;;) {
            std::vector<ObjectRecord> objects;
            {
                std::unique_lock lock(mutex);
                auto unread = [&] { return range.spilled.taken < range.spilled.written; };
                changed.wait(lock, [&] { return failure || !range.pages.empty() || unread() || range.done; });
                if (failure) std::rethrow_exception(failure);
                if (!range.pages.empty()) {
                    objects = std::move(range.pages.front().objects);
                    buffered -= range.pages.front().bytes;
                    range.pages.pop_front();
                } else if (unread()) {
                    // Read with the lock released: the reader only appends past `written`.
                    auto fd = range.spilled.fd;
                    auto at = range.spilled.taken;
                    lock.unlock();
                    objects = unspill_page(fd, at);
                    lock.lock();
                    range.spilled.taken = at;
                } else {
                    break;
                }
            }
            changed.notify_all();  // room for the range's reader, and in the budget
            for (auto const& object : objects) {
                if (object.key.size() != 32 || (!previous.empty() && object.key <= previous)) throw Error("Source state keys are not strictly increasing");
                visit(object); previous = object.key;
            }
        }
    }
}
void state_progress(Header const& header, std::size_t range, std::uint64_t pages, std::vector<ObjectRecord> const& page) {
    if (pages % 128 == 0 && !page.empty())
        std::cerr << Json{{"event", "state_pages"}, {"ledger", header.sequence}, {"range", range}, {"pages", pages},
                          {"at", to_hex(page.back().key.substr(0, 4))}}.dump() << std::endl;
}
// Over JSON-RPC (ledger_data, 2,048 objects a page). `splits` (keys the ledger holds, ascending)
// divide the key space: the first range through splits[0], the next after splits[0] through
// splits[1], and the last to the end (Clio pages only from a marker the ledger holds).
void rpc_state(RpcClient const& rpc, Header const& header, fs::path const& spill, std::function<void(ObjectRecord const&)> const& visit,
               std::vector<Bytes> const& splits = {}) {
    ordered_ranges(splits.size() + 1, spill, [&](std::size_t n, PageSink const& emit) {
        Json params{{"ledger_hash", header.hash}, {"binary", true}, {"limit", 2048}};
        if (n) params["marker"] = to_hex(splits[n - 1]);
        auto const* end = n < splits.size() ? &splits[n] : nullptr;
        std::set<std::string> markers;
        std::uint64_t pages = 0;
        for (bool more = true; more;) {
            auto page = rpc.call("ledger_data", params); success(page); pinned(page, header);
            std::vector<ObjectRecord> objects;
            for (auto const& item : page.at("state")) {
                ObjectRecord object{from_hex(item.at("index").get<std::string>()), from_hex(item.at("data").get<std::string>())};
                if (end && object.key > *end) { more = false; break; }
                objects.push_back(std::move(object));
            }
            if (more && !page.contains("marker")) more = false;
            if (more && end && page.at("marker").is_string() && from_hex(page.at("marker").get<std::string>()) >= *end) more = false;
            if (more) {
                if (!markers.insert(page.at("marker").dump()).second) throw Error("Source pagination marker repeated");
                params["marker"] = page.at("marker");
            }
            if (splits.size()) state_progress(header, n, ++pages, objects);
            if (!emit(std::move(objects))) return;
        }
    }, visit);
}
// Over gRPC (GetLedgerData returns the keys after `marker` and before `end_marker`): the key
// space in `jobs` equal ranges.
void grpc_state(GrpcLedgerClient const& grpc, Header const& header, unsigned jobs, fs::path const& spill,
                std::function<void(ObjectRecord const&)> const& visit) {
    auto parts = std::clamp(jobs, 1u, 256u);
    std::vector<Bytes> bounds;  // each range's first possible key
    for (unsigned n = 1; n < parts; ++n) {
        Bytes bound(32, '\0');
        bound[0] = static_cast<char>(256 * n / parts);
        if (bounds.empty() || bound > bounds.back()) bounds.push_back(bound);
    }
    ordered_ranges(bounds.size() + 1, spill, [&](std::size_t n, PageSink const& emit) {
        // After the key just before this range's first (markers are exclusive).
        Bytes marker;
        if (n) { marker = Bytes(32, '\xff'); marker[0] = static_cast<char>(static_cast<unsigned char>(bounds[n - 1][0]) - 1); }
        auto end = n < bounds.size() ? bounds[n] : Bytes{};
        std::uint64_t pages = 0;
        for (;;) {
            auto [objects, next] = grpc.state_page(header.sequence, marker, end);
            if (parts > 1) state_progress(header, n, ++pages, objects);
            if (!emit(std::move(objects)) || next.empty()) return;
            marker = std::move(next);
        }
    }, visit);
}
// Fetches a range's ledgers ahead of their verification on `jobs` threads, each keeping its
// own connection to the source, at most `jobs` * 4 ledgers ahead; take() hands them over in
// order and rethrows the first failure at or before the ledger asked for. Each fetch is told
// which thread (0 to jobs - 1) asks, so threads can be spread over sources.
class LedgerPrefetch {
public:
    using Fetch = std::function<std::pair<Header, Json>(std::uint32_t, unsigned)>;
    LedgerPrefetch(Fetch fetch, std::uint64_t first, std::uint64_t last, unsigned jobs)
        : fetch_(std::move(fetch)), next_(first), last_(last), taken_(first), ahead_(std::uint64_t(jobs) * 4) {
        for (unsigned n = 0; n < jobs; ++n) threads_.emplace_back([this, n] { run(n); });
    }
    ~LedgerPrefetch() {
        { std::lock_guard lock(mutex_); stopping_ = true; }
        changed_.notify_all();
        for (auto& thread : threads_) thread.join();
    }
    std::pair<Header, Json> take(std::uint64_t sequence) {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return ready_.contains(sequence) || (failure_ && failed_at_ <= sequence); });
        auto found = ready_.find(sequence);
        if (found == ready_.end()) std::rethrow_exception(failure_);
        auto result = std::move(found->second);
        ready_.erase(found);
        taken_ = sequence + 1;
        changed_.notify_all();
        return result;
    }
private:
    void run(unsigned worker) {
        for (;;) {
            std::uint64_t sequence;
            {
                std::unique_lock lock(mutex_);
                changed_.wait(lock, [&] { return stopping_ || failure_ || next_ > last_ || next_ < taken_ + ahead_; });
                if (stopping_ || failure_ || next_ > last_) return;
                sequence = next_++;
            }
            try {
                auto result = fetch_(static_cast<std::uint32_t>(sequence), worker);
                std::lock_guard lock(mutex_);
                ready_.emplace(sequence, std::move(result));
            } catch (...) {
                std::lock_guard lock(mutex_);
                if (!failure_ || sequence < failed_at_) { failure_ = std::current_exception(); failed_at_ = sequence; }
            }
            changed_.notify_all();
        }
    }
    Fetch fetch_;
    std::uint64_t next_, last_, taken_, ahead_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::map<std::uint64_t, std::pair<Header, Json>> ready_;
    std::exception_ptr failure_;
    std::uint64_t failed_at_ = 0;
    bool stopping_ = false;
    std::vector<std::thread> threads_;
};
// The node a publisher reads ledgers from (docs/SETUP.md): over JSON-RPC (http[s]://: xrpld,
// or Clio, which adds each ledger's object diff), over gRPC (grpc[s]://: an xrpld's GetLedger,
// with each ledger's diff, and GetLedgerData for state, so one port serves everything), or both
// (--grpc-source beside a JSON-RPC source: gRPC then serves everything it can).
class NodeSource {
    std::optional<RpcClient> rpc_, state_rpc_;
    std::unique_ptr<GrpcLedgerClient> grpc_;
    // Transaction trees the node's API omits, read raw from history peers (opened when needed).
    std::vector<std::string> history_peers_;
    fs::path history_identity_;
    std::uint32_t network_id_ = 0;
    mutable std::mutex history_mutex_;
    mutable std::unique_ptr<PeerLedgers> history_;
    // Runs `request` against the history peers (opened on first use: those given, or the peers
    // the network's hubs list as holding `sequence`); a failure drops them, so the next
    // attempt looks for peers again.
    template<class Request> auto with_history(std::uint32_t sequence, Request request) const {
        std::lock_guard lock(history_mutex_);
        try {
            if (!history_) {
                auto peers = history_peers_;
                if (peers == std::vector<std::string>{"default"}) peers = peers_holding(sequence, network_id_);
                if (peers.empty()) throw Error("No peer that holds ledger " + std::to_string(sequence) + " publishes its address");
                PeerLedgers::Options options;
                options.peers = peers; options.bootstrap = peers; options.private_mode = true;
                options.identity = history_identity_; options.network_id = network_id_;
                options.connections = static_cast<unsigned>(std::min<std::size_t>(peers.size(), 3));
                options.bootstrap_sessions = options.connections;
                history_ = std::make_unique<PeerLedgers>(options, [](Bytes const&) {}, [](Bytes const&) {});
            }
            return request(*history_);
        } catch (...) {
            history_.reset();
            throw;
        }
    }
    // In transaction tree (ID) order, as xrpld lists them.
    static Json listing(std::vector<TransactionRecord> const& transactions) {
        std::vector<std::pair<Bytes, std::size_t>> order;
        for (std::size_t i = 0; i < transactions.size(); ++i) order.push_back({wire::transaction_id(transactions[i].transaction), i});
        std::sort(order.begin(), order.end());
        Json listed = Json::array();
        for (auto const& [id, i] : order)
            listed.push_back({{"tx_blob", to_hex(transactions[i].transaction)}, {"meta", to_hex(transactions[i].metadata)}});
        return listed;
    }
    // xrpld decodes each transaction as it lists a ledger's, and on the first that fails to
    // decode (some 2013 transactions, or their metadata, on current xrpld) it drops that one and
    // all after it. A list whose tree root is not the header's is replaced by the ledger's
    // transaction tree read raw from peers that hold the ledger, each node checked against the
    // hash its parent (from the header's transaction root) commits to.
    void complete(Header const& header, Json& document) const {
        auto& listed = document["transactions"];
        if (!document.value("transactions_omitted", false)) {
            std::vector<TransactionRecord> records;
            for (auto const& tx : listed) records.push_back({from_hex(tx.at("tx_blob").get<std::string>()), from_hex(tx.value("meta", std::string()))});
            if (transaction_root(records) == header.transaction_hash) return;
        }
        document.erase("transactions_omitted");
        if (history_peers_.empty())
            throw Error("The source sent " + std::string(listed.empty() ? "none" : "only some") + " of the transactions of ledger " +
                        std::to_string(header.sequence) + " (xrpld cannot decode one of them): give history peers "
                        "(--history-peers default: peers that hold the ledger) to read its transaction tree raw");
        auto transactions = with_history(header.sequence, [&](PeerLedgers& peers) { return peers.transactions(header); });
        std::cerr << Json{{"event", "transactions_from_peers"}, {"ledger", header.sequence}, {"source_sent", listed.size()},
                          {"transactions", transactions.size()}}.dump() << std::endl;
        listed = listing(transactions);
    }
    // A ledger the node cannot serve: one it lacks (a hole in its history: ledgerNotFound), or
    // the one after it, whose diff xrpld computes against the missing parent. It is read from
    // peers that hold it, as when following the peer network: its header by hash (the next
    // ledger's parent hash, which the node still gives), its transaction tree, and the objects
    // its transactions and its close touched ("peer_objects": the export keeps those that
    // changed, and compares state trees should the ledger's state still not match).
    std::pair<Header, Json> from_peers(std::uint32_t sequence, std::string const& why) const {
        auto fetched = with_history(sequence, [&](PeerLedgers& peers) {
            Header header;
            try { header = grpc_->header(sequence); }
            catch (Error const&) {
                header = peers.header(from_hex(grpc_->header(sequence + 1).parent_hash), sequence);
                if (header.sequence != sequence) throw Error("The ledger before " + std::to_string(sequence + 1) + " is not ledger " + std::to_string(sequence));
            }
            auto transactions = peers.transactions(header);
            auto closing = closing_keys(header.sequence);
            std::set<Bytes> keys(closing.begin(), closing.end());
            for (auto const& record : transactions)
                for (auto& key : wire::summarize(record).keys) keys.insert(std::move(key));
            std::vector<Bytes> ordered(keys.begin(), keys.end());
            auto values = peers.objects(header, ordered);
            Json objects = Json::array();
            for (std::size_t i = 0; i < ordered.size(); ++i) objects.push_back({{"object_id", to_hex(ordered[i])}, {"object", to_hex(values[i])}});
            return std::pair{header, Json{{"transactions", listing(transactions)}, {"peer_objects", std::move(objects)}}};
        });
        std::cerr << Json{{"event", "ledger_from_peers"}, {"ledger", sequence}, {"source_error", why},
                          {"transactions", fetched.second.at("transactions").size()}}.dump() << std::endl;
        return fetched;
    }
public:
    bool has_history_peers() const { return !history_peers_.empty(); }
    // Every object a ledger read from peers changed, by comparing its state tree with the
    // verified parent's (for a change its transactions' metadata does not name).
    std::vector<ObjectRecord> peer_changes(Header const& header, LedgerVerifier const& parent) const {
        return with_history(header.sequence, [&](PeerLedgers& peers) { return peers.changes(header, parent); });
    }
    void history(std::vector<std::string> peers, fs::path identity, std::uint32_t network_id) {
        history_peers_ = std::move(peers); history_identity_ = std::move(identity); network_id_ = network_id;
    }
    // Ledgers are cached in `cache` and state pages in `state_cache` (JSON-RPC; empty: none).
    NodeSource(std::string const& source, std::string const& grpc_source, fs::path const& cache, fs::path const& state_cache, unsigned delay_ms) {
        if (source.starts_with("grpc://") || source.starts_with("grpcs://")) {
            if (!grpc_source.empty()) throw Error("A gRPC --source needs no --grpc-source");
            grpc_ = std::make_unique<GrpcLedgerClient>(source);
            return;
        }
        rpc_.emplace(RpcClient{source, cache, delay_ms});
        state_rpc_.emplace(RpcClient{source, state_cache, delay_ms});
        if (!grpc_source.empty()) grpc_ = std::make_unique<GrpcLedgerClient>(grpc_source);
    }
    std::string transport() const { return grpc_ ? "grpc" : "json-rpc"; }
    // For objects fetched one by one (a JSON-RPC source without diffs).
    RpcClient const& rpc() const {
        if (!rpc_) throw Error("Objects by key need a JSON-RPC source");
        return *rpc_;
    }
    // The newest ledger the source says is validated.
    Header tip() const {
        if (grpc_) return grpc_->validated();
        auto response = rpc_->call("ledger", {{"ledger_index", "validated"}, {"binary", true}});
        success(response);
        if (!response.value("validated", false)) throw Error("Source tip is not validated");
        auto header = decode_header(from_hex(response.at("ledger").at("ledger_data")));
        pinned(response, header);
        return header;
    }
    // A validated ledger: its header, transactions and (when the source gives it) object diff.
    std::pair<Header, Json> ledger(std::uint32_t sequence) const { return ledger(sequence, grpc_ != nullptr); }
    // From the gRPC node, or (`over_grpc` false, with --grpc-source beside a JSON-RPC source)
    // from the JSON-RPC one: a ledger is verified against its hashes whichever node sends it.
    std::pair<Header, Json> ledger(std::uint32_t sequence, bool over_grpc) const {
        if (!grpc_ || (!over_grpc && rpc_)) return get_ledger(*rpc_, sequence);
        std::pair<Header, Json> result;
        try { result = grpc_->ledger(sequence); }
        catch (Error const& error) {
            std::string what = error.what();
            if (history_peers_.empty() || (what.find("ledgerNotFound") == std::string::npos && what.find("parent ledger not validated") == std::string::npos)) throw;
            return from_peers(sequence, what);
        }
        complete(result.first, result.second);
        return result;
    }
    // The ledger a new archive starts after (its state is the base snapshot). Over gRPC only its
    // header is asked for: its state comes by pages, and the diff xrpld would add needs its parent,
    // which the first ledger a full-history node holds lacks.
    std::pair<Header, Json> base(std::uint32_t sequence) const {
        if (grpc_) return {grpc_->header(sequence), Json{{"transactions", Json::array()}, {"diff", Json::array()}}};
        return ledger(sequence);
    }
    // A ledger's state in key order on `jobs` connections; `document` (the ledger's, from
    // ledger()) names keys to split a JSON-RPC download at, and `spill` is the directory for the
    // pages read ahead of their turn past the memory budget (ordered_ranges: at most the state's
    // size, gone once the state is read).
    void state(Header const& header, Json const& document, unsigned jobs, fs::path const& spill,
               std::function<void(ObjectRecord const&)> const& visit) const {
        if (grpc_) grpc_state(*grpc_, header, jobs, spill, visit);
        else rpc_state(*state_rpc_, header, spill, visit, split_keys(document, jobs));
    }
};
// An object written under `root`, made durable: the file, and the directories from its own up
// to `root` (a new archive syncs root's ancestors once: sync_ancestors). `sha256` when the
// writer already hashed it.
Json descriptor(fs::path const& root, std::string const& key, std::string sha256 = {}) {
    sync_file(root / key);
    auto top = fs::absolute(root).lexically_normal(), parent = fs::absolute(root / key).lexically_normal().parent_path();
    if (!top.has_filename()) top = top.parent_path();  // a trailing separator
    for (;;) { sync_file(parent); if (parent == top) break; auto next = parent.parent_path(); if (next == parent) break; parent = next; }
    if (sha256.empty()) sha256 = file_sha256(root / key);
    return {{"object", key}, {"size", fs::file_size(root / key)}, {"sha256", sha256}};
}
void sync_ancestors(fs::path const& root) {
    auto parent = fs::absolute(root).lexically_normal();
    for (;;) { sync_file(parent); auto next = parent.parent_path(); if (next == parent) break; parent = next; }
}
Bytes signing_seed(fs::path const& path) {
    auto seed = read_file(path, 128);
    if (seed.size() != crypto_sign_SEEDBYTES) {
        while (!seed.empty() && (seed.back() == '\n' || seed.back() == '\r')) seed.pop_back();
        seed = from_hex(seed);
    }
    if (seed.size() != crypto_sign_SEEDBYTES) throw Error("Signing key must contain a 32-byte Ed25519 seed");
    return seed;
}
void checked(fs::path const& root, Object const& object) {
    safe_cache_path(root / object.object);
    if (!matches(root / object.object, object)) throw Error("Published archive object differs: " + object.object);
}
// The headers of the archive's ledgers `first`..`last` (from its chunks), in order.
std::vector<Header> archive_headers(fs::path const& root, Manifest const& manifest, std::uint64_t first, std::uint64_t last) {
    std::vector<Header> headers;
    for (auto const& chunk : manifest.chunks) {
        if (chunk.last < first || chunk.first > last) continue;
        checked(root, chunk.file);
        read_chunk(root / chunk.file.object, {}, [&](LedgerRecord&& ledger) {
            if (ledger.sequence >= first && ledger.sequence <= last) headers.push_back(decode_header(ledger.header));
        });
    }
    return headers;
}
std::unique_ptr<LedgerVerifier> restore(fs::path const& root, Manifest const& manifest, bool& resumed, std::size_t keep = 0) {
    if (manifest.version != 2 || !manifest.dicts.empty() || manifest.chunks.empty())
        throw Error("Publishing requires a v2 archive without dictionaries");
    auto const& last = manifest.chunks.back();
    if (last.last != manifest.tip) throw Error("Published chunks do not reach the tip");
    checked(root, last.file);
    Bytes header;
    read_chunk(root / last.file.object, {}, [&](LedgerRecord&& ledger) {
        if (ledger.sequence == manifest.tip) header = ledger.header;
    });
    if (header.empty() || decode_header(header).hash != manifest.tip_hash)
        throw Error("Published tip header differs from manifest");
    auto nodes = root / ".verification-nodes";
    auto verifier = std::make_unique<LedgerVerifier>(nodes);
    // A peer server's window: the ledgers before the tip keep their trees through compaction.
    auto window = [&](LedgerVerifier& v) {
        if (!keep) return;
        v.keep_ledgers(keep);
        auto first = manifest.tip >= keep ? static_cast<std::uint64_t>(manifest.tip) - keep + 1 : 0;
        for (auto const& h : archive_headers(root, manifest, std::max<std::uint64_t>(first, manifest.chunks.front().first), manifest.tip - 1)) v.keep_ledger(h);
    };
    try { window(*verifier); verifier->resume(header); resumed = true; return verifier; }
    catch (Error const&) { verifier.reset(); verifier = std::make_unique<LedgerVerifier>(nodes); if (keep) verifier->keep_ledgers(keep); }
    // The signed archive, not a private progress file, is the recovery authority.
    auto plan = make_plan(manifest, {manifest.chunks.front().first, manifest.tip});
    checked(root, plan.snapshot.index);
    auto bytes = read_file(root / plan.snapshot.index.object, 16 * 1024 * 1024);
    auto index = Json::parse(bytes);
    for (auto const& part : snapshot_parts(plan.snapshot, bytes)) {
        checked(root, part);
        auto found = std::find_if(index.at("parts").begin(), index.at("parts").end(), [&](auto const& p) { return p.at("object") == part.object; });
        std::vector<ObjectRecord> objects;
        read_snapshot_part(root / part.object, {}, found->at("records").template get<std::uint64_t>(),
            [&](ObjectRecord&& object) { objects.push_back(std::move(object)); });
        verifier->add_snapshot_objects(objects);
    }
    verifier->finish_snapshot(from_hex(index.at("ledger_header")), plan.snapshot.seq, plan.snapshot.account_hash);
    for (auto const& chunk : manifest.chunks) {
        if (chunk.last <= plan.snapshot.seq) continue;
        checked(root, chunk.file);
        read_chunk(root / chunk.file.object, {}, [&](LedgerRecord&& ledger) {
            if (ledger.sequence > plan.snapshot.seq) verifier->advance(ledger);
        });
        if (verifier->header().sequence != chunk.last || verifier->header().hash != chunk.last_ledger_hash)
            throw Error("Published chunk tip mismatch");
    }
    if (verifier->header().hash != manifest.tip_hash) throw Error("Published archive tip mismatch");
    verifier->commit_point();  // what a failed batch resumes at next time
    return verifier;
}
void immutable_signature(fs::path const& root, std::string const& bytes, Bytes const& signature) {
    auto path = root / "signatures" / (sha256(bytes) + ".sig");
    safe_cache_path(path); fs::create_directories(path.parent_path());
    if (fs::exists(path)) {
        if (read_file(path, 64) != signature) throw Error("Conflicting immutable manifest signature");
        return;
    }
    auto temp = path; temp += ".part"; write_file(temp, signature); durable_rename(temp, path);
}
// The state after an interval's boundary ledger, written as the follower passes it, so the
// piece publisher's snapshot tool copies it instead of replaying the interval. Its parts go
// under the ledger's sequence and hash (write_state_snapshot); its descriptor
// (interval-snapshots/SEQ.json) is only added to `descriptors`, written by
// write_interval_descriptors once the batch's tip is checked against the quorum, so a batch
// from a source off the quorum's chain leaves none behind. One already written for this same
// ledger is kept; one for another ledger at this sequence (an earlier version wrote them before
// that check) is replaced.
//
// A batch retried after failing past the boundary finds the snapshot it wrote already there,
// still named by no descriptor: that one is taken as it is (existing_state_snapshot: an index
// of exactly this ledger, every part present with its listed digest, and the parts' objects
// read back and found to be the verified state) rather than written again, a whole mainnet
// state at zstd 19. Parts of a ledger the quorum did not validate are
// removed: those this batch wrote when its tip turns out not to be the quorum's
// (abandon_interval_snapshots), and any left at the sequence by a batch that failed otherwise,
// once the quorum's ledger there is named (prune_interval_forks).
struct IntervalSnapshot {
    fs::path descriptor;  // interval-snapshots/SEQ.json
    Json result;          // what it will hold: write_state_snapshot's descriptor
    bool written;         // this batch wrote the parts (not taken from an earlier batch's)
};
using IntervalDescriptors = std::vector<IntervalSnapshot>;
void interval_snapshot(LedgerVerifier& verifier, fs::path const& root, IntervalDescriptors& descriptors) {
    auto dir = root / "interval-snapshots";
    auto seq = verifier.header().sequence;
    auto done = dir / (std::to_string(seq) + ".json");
    if (auto status = fs::symlink_status(done); fs::exists(status)) {
        // Only a file of the batch's own is read or removed: a link or a directory there was put
        // by someone else, and is left for them.
        if (!fs::is_regular_file(status))
            throw Error(done.string() + " is not a regular file: remove it to have the interval snapshot written again");
        try { if (Json::parse(read_file(done, 1 << 20)).value("ledger_hash", std::string()) == verifier.header().hash) return; }
        catch (std::exception const&) {
            // Unreadable: written again. What it names is not known, and may be the very parts
            // about to be written again in place, so it goes first: no descriptor names parts
            // while they are rewritten (the batch writes this sequence's again once its tip is
            // checked).
            fs::remove(done);
            sync_file(dir);
        }
    }
    if (auto existing = existing_state_snapshot(verifier, dir)) {
        std::cerr << Json{{"event", "interval_snapshot_reused"}, {"ledger", seq}, {"objects", existing->at("objects")},
                          {"bytes", existing->at("total_size")}}.dump() << std::endl;
        descriptors.push_back({done, std::move(*existing), false});
        return;
    }
    Json spent;
    auto result = write_state_snapshot(verifier, dir, 19, std::clamp(std::thread::hardware_concurrency(), 1u, 8u), &spent);
    std::cerr << Json{{"event", "interval_snapshot"}, {"ledger", seq}, {"objects", result.at("objects")}, {"bytes", result.at("total_size")},
                      {"seconds", spent.at("seconds")}}.dump() << std::endl;
    descriptors.push_back({done, std::move(result), true});
}
// Whether the descriptor at `done` names the parts under `prefix`. One that cannot be read
// counts as naming them: what it names is not known, so nothing is removed on its account.
bool interval_named(fs::path const& done, std::string const& prefix) {
    try { return fs::exists(done) && Json::parse(read_file(done, 1 << 20)).value("prefix", std::string()) == prefix; }
    catch (std::exception const&) { return true; }
}
// The batch is abandoned because its tip is not the ledger the quorum validated: the parts it
// wrote are of a chain the quorum has not validated, and no descriptor was written for them.
// Only those this batch wrote, and only while no descriptor names them, are removed (failing to
// remove one is reported, not thrown: the batch's own failure is what the caller hears).
void abandon_interval_snapshots(IntervalDescriptors const& descriptors) {
    for (auto const& snapshot : descriptors) {
        if (!snapshot.written) continue;
        auto prefix = snapshot.result.at("prefix").get<std::string>();
        if (interval_named(snapshot.descriptor, prefix)) continue;
        std::error_code error;
        fs::remove_all(snapshot.descriptor.parent_path() / prefix, error);
        std::cerr << Json{{"event", "interval_snapshot_abandoned"}, {"ledger", snapshot.result.at("seq")}, {"prefix", prefix},
                          {"removed", !error}}.dump() << std::endl;
    }
}
// Once the descriptor at `done` names the quorum's ledger at its sequence, any other parts
// directory at that sequence (snapshots/SEQ.<another 16-hex hash prefix>) is of a ledger the
// quorum did not validate, left by a batch that failed before its tip was checked: none names it,
// and none will (the descriptor at a sequence is only ever replaced by the quorum's ledger's).
void prune_interval_forks(fs::path const& done, Json const& result) {
    auto keep = fs::path(result.at("prefix").get<std::string>()).filename().string();
    auto lead = std::to_string(result.at("seq").get<std::uint32_t>()) + ".";
    std::error_code error;
    std::vector<fs::path> forks;
    for (fs::directory_iterator it(done.parent_path() / "snapshots", error), end; !error && it != end; it.increment(error)) {
        auto name = it->path().filename().string();
        if (name == keep || name.size() != lead.size() + 16 || name.compare(0, lead.size(), lead) != 0 ||
            name.find_first_not_of("0123456789abcdef", lead.size()) != std::string::npos)
            continue;
        std::error_code status;
        if (fs::is_directory(fs::symlink_status(it->path(), status))) forks.push_back(it->path());
    }
    for (auto const& fork : forks) {
        std::error_code removing;
        fs::remove_all(fork, removing);
        std::cerr << Json{{"event", "interval_snapshot_fork_removed"}, {"ledger", result.at("seq")}, {"prefix", "snapshots/" + fork.filename().string()},
                          {"removed", !removing}}.dump() << std::endl;
    }
}
void write_interval_descriptors(IntervalDescriptors const& descriptors) {
    for (auto const& snapshot : descriptors) {
        auto temp = snapshot.descriptor; temp += ".part";
        write_file(temp, snapshot.result.dump()); durable_rename(temp, snapshot.descriptor);
        prune_interval_forks(snapshot.descriptor, snapshot.result);
    }
}
volatile std::sig_atomic_t stopping = 0;
void stop(int) { stopping = 1; }
}
Json create_signing_key(fs::path const& path) {
    safe_cache_path(path);
    if (fs::exists(path)) throw Error("Signing key already exists");
    std::array<unsigned char, crypto_sign_SEEDBYTES> seed{};
    randombytes_buf(seed.data(), seed.size());
    // Created readable by its owner alone: written first and restricted after, it would be
    // readable by others for a moment.
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) throw Error("Cannot create signing key " + path.string());
    bool written = ::write(fd, seed.data(), seed.size()) == static_cast<ssize_t>(seed.size()) && ::fsync(fd) == 0;
    ::close(fd);
    sodium_memzero(seed.data(), seed.size());
    if (!written) { std::error_code ignored; fs::remove(path, ignored); throw Error("Cannot write signing key " + path.string()); }
    // Its directory entry too: synced only in its file, a crash could leave the directory
    // without it, and the archive signed by a key no longer there. The key is in place by now:
    // a failure says so, since creating it again finds it there.
    try { sync_file(fs::absolute(path).parent_path()); }
    catch (std::exception const& error) {
        throw Error("Signing key " + path.string() + " was created, but its directory could not be synced (" + error.what() +
                    "): a crash could still lose it; sync its directory before using it, or remove it and create it again");
    }
    // The public key too, hex: what users pin, known before the first publish.
    return {{"signing_key", path.string()}, {"public_key", public_key(path)}};
}
std::string public_key(fs::path const& signing_key) {
    auto seed = signing_seed(signing_key);
    std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
    std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
    crypto_sign_seed_keypair(pk.data(), sk.data(), reinterpret_cast<unsigned char const*>(seed.data()));
    sodium_memzero(seed.data(), seed.size()); sodium_memzero(sk.data(), sk.size());
    return to_hex(raw(pk));
}
namespace {
// A new archive's starting state given directly: its ledger's header, and its objects in key
// order (checked against the header's account hash as they load).
struct Baseline {
    Header header;
    std::function<void(std::function<void(ObjectRecord const&)> const&)> objects;
};
// A ledger read from peers before its verification: its header, its transactions, and the
// objects they and the ledger's close touched (each read along a path checked against the header).
struct PeerLedger {
    Header header;
    std::vector<TransactionRecord> transactions;
    std::vector<Bytes> keys, values;
};
PeerLedger fetch_peer_ledger(PeerLedgers& peers, Header header) {
    auto contents = peers.contents(header, [&](std::vector<TransactionRecord> const& transactions) {
        auto closing = closing_keys(header.sequence);
        std::set<Bytes> keys(closing.begin(), closing.end());
        for (auto const& record : transactions)
            for (auto& key : wire::summarize(record).keys) keys.insert(std::move(key));
        return std::vector<Bytes>(keys.begin(), keys.end());
    });
    return {std::move(header), std::move(contents.transactions), std::move(contents.keys), std::move(contents.values)};
}
// Near the network's tip, each ledger is fetched from peers as soon as a quorum validates it,
// while the ones before it are still being fetched or verified: a ledger takes longer to fetch
// than the network takes to close the next, so one at a time the follower would fall behind and
// publish in bursts. Batches then commit the fetched ledgers in order, usually one at a time.
// A fetch is of a ledger by its hash: the first trusted validations of a ledger may name a hash
// the quorum does not follow (a dissenting validator's), and peers do not answer for a ledger
// they lack, so such a fetch can take a minute to fail. Each runs on a thread of its own, and one
// no longer wanted is abandoned, never waited for (its thread is joined once it ends, or when
// the PeerAhead goes); the hash the quorum validated is fetched beside it.
class PeerAhead {
public:
    PeerAhead() = default;
    PeerAhead(PeerAhead const&) = delete;
    PeerAhead& operator=(PeerAhead const&) = delete;
    // Fetches still running then use the peers: they end before it does.
    ~PeerAhead() {
        std::vector<Worker> workers;
        { std::lock_guard lock(mutex_); workers.swap(workers_); }
        for (auto& worker : workers) worker.thread.join();
    }
    void start(PeerLedgers& peers, std::uint32_t seq, std::string const& hash) {
        std::lock_guard lock(mutex_);
        launch(peers, seq, hash, std::nullopt);
    }
    // A ledger whose header is already known (and checked): its contents only.
    std::shared_future<PeerLedger> start(PeerLedgers& peers, Header const& header) {
        std::lock_guard lock(mutex_);
        return launch(peers, header.sequence, header.hash, header);
    }
    // `seq` by the hash a quorum validated, the fetches started on any other abandoned.
    void settle(PeerLedgers& peers, std::uint32_t seq, std::string const& hash) {
        std::lock_guard lock(mutex_);
        std::erase_if(fetching_[seq], [&](auto const& fetch) { return fetch.first != hash; });
        launch(peers, seq, hash, std::nullopt);
    }
    bool started(std::uint32_t seq) {
        std::lock_guard lock(mutex_);
        auto it = fetching_.find(seq);
        return it != fetching_.end() && !it->second.empty();
    }
    bool started(std::uint32_t seq, std::string const& hash) { std::lock_guard lock(mutex_); return find(seq, hash) != nullptr; }
    std::optional<std::shared_future<PeerLedger>> get(std::uint32_t seq, std::string const& hash) {
        std::lock_guard lock(mutex_);
        auto found = find(seq, hash);
        return found ? std::optional(*found) : std::nullopt;
    }
    // The last of the ledgers after `first` (whose hash is `hash`) up to `limit` already fetched
    // as a chain from it, each the fetch whose header names the one before as its parent, and
    // that last one's hash. Only ledgers fetched join: `first` is the batch's to wait for.
    std::pair<std::uint32_t, std::string> ready_through(std::uint32_t first, std::string hash, std::uint32_t limit) {
        std::lock_guard lock(mutex_);
        auto last = first;
        for (auto seq = first + 1; seq <= limit; ++seq) {
            auto it = fetching_.find(seq);
            if (it == fetching_.end()) break;
            std::optional<std::string> child;
            for (auto const& [candidate, fetch] : it->second) {
                if (fetch.wait_for(std::chrono::seconds(0)) != std::future_status::ready) continue;
                try { if (fetch.get().header.parent_hash == hash) { child = candidate; break; } }
                catch (std::exception const&) {}  // a fetch that failed: started again later
            }
            if (!child) break;
            last = seq; hash = std::move(*child);
        }
        return {last, hash};
    }
    // Fetches no longer needed (committed), or all of them after a failed batch: started again.
    void forget_through(std::uint32_t seq) {
        std::lock_guard lock(mutex_);
        fetching_.erase(fetching_.begin(), fetching_.upper_bound(seq));
        reap();
    }
    void clear() { forget_through(std::numeric_limits<std::uint32_t>::max()); }
private:
    struct Worker { std::thread thread; std::shared_ptr<std::atomic<bool>> done; };
    std::shared_future<PeerLedger> const* find(std::uint32_t seq, std::string const& hash) const {
        auto it = fetching_.find(seq);
        if (it == fetching_.end()) return nullptr;
        auto fetch = it->second.find(hash);
        return fetch == it->second.end() ? nullptr : &fetch->second;
    }
    // Under the lock. Its result goes through a promise, so dropping the future never waits.
    // A fetch of the ledger already started is shared, unless it has failed: then it is started
    // again (the batch waiting on it would otherwise fail with it, and every batch after).
    std::shared_future<PeerLedger> launch(PeerLedgers& peers, std::uint32_t seq, std::string const& hash, std::optional<Header> known) {
        reap();
        auto& fetches = fetching_[seq];
        if (auto found = fetches.find(hash); found != fetches.end()) {
            if (!failed(found->second)) return found->second;
            fetches.erase(found);
        }
        auto promise = std::make_shared<std::promise<PeerLedger>>();
        auto done = std::make_shared<std::atomic<bool>>(false);
        std::thread thread([&peers, seq, hash, known = std::move(known), promise, done] {
            try {
                auto header = known ? *known : peers.header(from_hex(hash), seq);  // checked: it hashes to `hash`
                if (header.sequence != seq) throw Error("Validated ledger hash names another sequence");
                promise->set_value(fetch_peer_ledger(peers, std::move(header)));
            } catch (...) { promise->set_exception(std::current_exception()); }
            *done = true;
        });
        auto future = promise->get_future().share();
        fetches.emplace(hash, future);
        workers_.push_back({std::move(thread), std::move(done)});
        return future;
    }
    static bool failed(std::shared_future<PeerLedger> const& fetch) {
        if (fetch.wait_for(std::chrono::seconds(0)) != std::future_status::ready) return false;
        try { fetch.get(); return false; } catch (...) { return true; }
    }
    // Joins the threads of fetches that have ended (taken or abandoned).
    void reap() {
        for (auto it = workers_.begin(); it != workers_.end();) {
            if (!*it->done) { ++it; continue; }
            it->thread.join();
            it = workers_.erase(it);
        }
    }
    std::mutex mutex_;
    std::map<std::uint32_t, std::map<std::string, std::shared_future<PeerLedger>>> fetching_;  // seq -> hash -> fetch
    std::vector<Worker> workers_;
};
// Ledgers come from the source node (JSON-RPC or gRPC), from peers, or from a verified piece
// publication (`restore --to spool`: its snapshot and the ledgers after it).
// `records` supplies whole ledger records in order instead (an existing spool's).
// With `kept`, a publisher's verifier stays open from batch to batch: it is used again while
// it stands at the committed tip and handed back after a commit; a failed batch drops it, and
// the next restores from the signed archive. With `peer_ahead`, ledgers it has already started
// fetching from peers are taken from it. With `kept_source`, ledgers and state are read from
// it (a follower's, opened as the batch would open it) instead of a source opened for the batch.
// With `check_last`, the last ledger's header and the NegativeUNL object of its parent's verified
// state (serialized; empty when it has none) are checked before the commit: it throws to refuse.
Json export_locked(ExportOptions const& options, PeerLedgers* peers = nullptr, PieceFeed* feed = nullptr,
                   std::function<bool(LedgerRecord&)> records = {}, std::unique_ptr<LedgerVerifier>* kept = nullptr,
                   std::vector<Header>* committed = nullptr, Baseline const* baseline = nullptr, PeerAhead* peer_ahead = nullptr,
                   NodeSource* kept_source = nullptr, std::function<void(Header const&, Bytes const&)> const& check_last = {}) {
    if (feed && !records) records = [feed](LedgerRecord& out) { return feed->next(out); };
    if (options.range.first < 2 || options.range.last < options.range.first) throw Error("Export requires a range with a preceding snapshot ledger");
    if (!options.jobs || options.jobs > 32) throw Error("RPC jobs must be between 1 and 32");
    if (options.grpc_jobs && (options.grpc_source.empty() || options.grpc_jobs > 32))
        throw Error("--grpc-jobs needs --grpc-source beside a JSON-RPC --source, and at most 32");
    if (!options.compression_level || options.compression_level > 19) throw Error("Compression level must be 1..19");
    auto seed = signing_seed(options.signing_key);
    std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
    std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
    crypto_sign_seed_keypair(pk.data(), sk.data(), reinterpret_cast<unsigned char const*>(seed.data()));
    sodium_memzero(seed.data(), seed.size());
    struct EraseKey { decltype(sk)& key; ~EraseKey() { sodium_memzero(key.data(), key.size()); } } erase{sk};
    bool existing = fs::exists(options.output / "manifest.json"), resumed = false;
    if (existing && !options.append) throw Error("Archive already published; use publish to extend it");
    auto root = options.output;
    fs::create_directories(root / "chunks");
    fs::create_directories(root / "times");
    // Snapshot pages are cached, so a long state download resumes where it stopped. A publisher
    // commits its ledgers every batch, so only a one-off export caches them too.
    // A follower's source (`kept_source`) lasts from batch to batch: its connections (a gRPC
    // channel, history peers) are not opened again for every batch.
    auto cache = options.rpc_cache ? root / ".rpc-cache" : fs::path{};
    std::optional<NodeSource> own_source;
    if (!kept_source) {
        own_source.emplace(options.source, options.grpc_source, options.append ? fs::path{} : cache, cache, options.delay_ms);
        own_source->history(options.history_peers, options.history_identity, options.network_id);
    }
    auto& source = kept_source ? *kept_source : *own_source;
    Json manifest;
    std::unique_ptr<LedgerVerifier> owner;
    std::optional<Header> base_header;
    Json base_document;
    if (existing) {
        auto signed_manifest = read_manifest(Source(root.string()), raw(pk));
        if (static_cast<std::uint64_t>(signed_manifest.manifest.tip) + 1 != options.range.first)
            throw Error("Published extension must start immediately after the committed tip");
        immutable_signature(root, signed_manifest.bytes, signed_manifest.signature);
        manifest = Json::parse(signed_manifest.bytes);
        if (kept && *kept && (*kept)->header().hash == signed_manifest.manifest.tip_hash) { owner = std::move(*kept); resumed = true; }
        else {
            if (kept) kept->reset();  // its node store must close before it is opened again
            owner = restore(root, signed_manifest.manifest, resumed, options.keep_ledgers);
        }
    } else {
        if (peers) throw Error("An archive read from peers extends an existing one; export its first ledgers from a node");
        if (baseline) base_header = baseline->header;
        else if (feed) base_header = feed->snapshot();
        else {
            std::tie(base_header, base_document) = source.base(options.range.first - 1);
        }
        owner = std::make_unique<LedgerVerifier>(root / ".verification-nodes");
    }
    auto& verifier = *owner;
    if (options.keep_ledgers) verifier.keep_ledgers(options.keep_ledgers);
    // The tree stays in memory until the export persists (each batch of a follower's) or 1M
    // objects change, rather than reloading from the store every 64 ledgers or 4,096 objects
    // (a mainnet ledger changes several hundred): after each reload, every changed path was read
    // back from RocksDB a node at a time.
    verifier.retain(4096, 1u << 20);
    // A verifier kept from batch to batch (a follower's) compacts on a thread of its own.
    if (kept) verifier.compact_in_background(true);
    // A batch failing part-way (a peer that did not answer, most often) leaves a kept verifier
    // ahead of what was committed: it goes back to the committed ledger and stays open, so work
    // under way (a background compaction) carries on; the caller reopens it from disk if
    // batches keep failing.
    struct Rewind {
        std::unique_ptr<LedgerVerifier>& owner;
        std::unique_ptr<LedgerVerifier>* kept;
        Bytes header;
        ~Rewind() {
            if (!kept || !owner || header.empty() || !std::uncaught_exceptions()) return;
            try { owner->rewind(header); *kept = std::move(owner); } catch (...) {}
        }
    } rewind{owner, kept, existing && kept ? verifier.header().raw : Bytes{}};
    std::vector<Header> applied;
    IntervalDescriptors interval_descriptors;  // written once the batch's tip is checked
    std::uint64_t object_count = 0;
    if (!existing) {
    auto const& base = *base_header;
    std::string prefix = "snapshots/" + std::to_string(base.sequence) + "." + base.hash.substr(0, 16);
    fs::create_directories(root / prefix);
    sync_ancestors(root);  // a new archive's directory, wherever it was created
    Json parts = Json::array();
    std::vector<ObjectRecord> objects;
    std::uint64_t total_size = 0, pending_size = 0;
    auto flush_part = [&] {
        if (objects.empty()) return;
        auto key = prefix + "/part-" + std::to_string(parts.size()) + ".xrps";
        write_snapshot_part(root / key, objects, {}, options.compression_level);
        auto part = descriptor(root, key); part["records"] = objects.size();
        total_size += part.at("size").get<std::uint64_t>(); parts.push_back(part);
        objects.clear(); pending_size = 0;
    };
    auto add_object = [&](ObjectRecord const& item) {
        verifier.add_snapshot_object(item); objects.push_back(item); ++object_count;
        pending_size += item.object.size() + 36;
        if (pending_size >= snapshot_part_bytes) flush_part();
        if (object_count % 262144 == 0) std::cerr << "Snapshot " << base.sequence << ": " << object_count << " objects\n";
    };
    if (baseline) baseline->objects(add_object);
    else if (feed) feed->snapshot_objects(add_object);
    else source.state(base, base_document, options.jobs, root, add_object);
    flush_part();
    if (parts.empty()) {
        auto key = prefix + "/part-0.xrps";
        write_snapshot_part(root / key, {});
        auto part = descriptor(root, key); part["records"] = 0; total_size = part.at("size"); parts.push_back(part);
    }
    verifier.finish_snapshot(base.raw, base.sequence, base.account_hash);
    Json snapshot{{"format_version", 2}, {"record_format", "keyed-stobject-v2"}, {"seq", base.sequence},
        {"account_hash", base.account_hash}, {"ledger_header", to_hex(base.raw)},
        {"dictionary_sha256", std::string(64, '0')}, {"parts", parts}};
    write_file(root / prefix / "index.json", snapshot.dump());
    manifest = {{"format_version", 2}, {"chunk_width", 65536}, {"dicts", Json::array()}, {"chunks", Json::array()},
        {"snapshots", Json::array({{{"seq", base.sequence}, {"prefix", prefix}, {"account_hash", base.account_hash},
          {"index", descriptor(root, prefix + "/index.json")}, {"parts", parts.size()}, {"total_size", total_size}}})}};
    }
    auto& chunks = manifest["chunks"];
    std::vector<LedgerRecord> ledgers;
    std::uint64_t ledger_bytes = 0, patch_count = 0, transaction_count = 0;
    auto flush_chunk = [&] {
        if (ledgers.empty()) return;
        auto temp = root / "chunk.part";
        write_chunk(temp, ledgers, {}, 2, options.compression_level);
        auto hash = file_sha256(temp);
        auto key = "chunks/" + std::to_string(ledgers.front().sequence) + "-" + std::to_string(ledgers.back().sequence) + "." + hash.substr(0, 16) + ".xrpa";
        durable_rename(temp, root / key);
        auto chunk = descriptor(root, key, hash);
        chunk["first"] = ledgers.front().sequence; chunk["last"] = ledgers.back().sequence;
        chunk["ledger_hash_of_last"] = decode_header(ledgers.back().header).hash; chunk["amendments"] = Json::array();
        Bytes times;
        auto append = [&](std::uint32_t value) { for (int shift = 24; shift >= 0; shift -= 8) times += static_cast<char>(value >> shift); };
        for (auto const& ledger : ledgers) { append(ledger.sequence); append(decode_header(ledger.header).close_time); }
        auto time_key = "times/" + std::to_string(ledgers.front().sequence) + "-" + std::to_string(ledgers.back().sequence) + "." + sha256(times).substr(0, 16) + ".xrpt";
        write_file(root / time_key, times);
        chunk["time_index"] = descriptor(root, time_key);
        chunk["first_close_time"] = decode_header(ledgers.front().header).close_time;
        chunk["last_close_time"] = decode_header(ledgers.back().header).close_time;
        chunks.push_back(chunk); ledgers.clear(); ledger_bytes = 0;
    };
    double rpc_seconds = 0, decode_seconds = 0, verification_seconds = 0;
    auto elapsed = [](auto start) { return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count(); };
    // From peers, the batch's headers: back from the quorum-validated last ledger by parent hash.
    std::map<std::uint64_t, Header> peer_headers;
    if (peers && peer_ahead && !options.expected_last_hash.empty()) {
        // Ledgers already being fetched (PeerAhead), followed back from the quorum-validated last
        // ledger by parent hash: each fetch waited for is the one of the hash the ledger after it
        // names, never one started on a hash the quorum did not validate. Should one not have
        // started, the headers are read from peers below.
        auto rpc_start = std::chrono::steady_clock::now();
        auto hash = normalized(options.expected_last_hash);
        for (auto seq = options.range.last;; --seq) {
            auto fetch = peer_ahead->get(seq, hash);
            if (!fetch) { peer_headers.clear(); break; }
            auto const& h = fetch->get().header;
            if (h.sequence != seq || h.hash != hash) throw Error("Peer ledger headers do not form a chain");
            peer_headers[seq] = h;
            hash = h.parent_hash;
            if (seq == options.range.first) break;
        }
        rpc_seconds += elapsed(rpc_start);
    }
    if (peers && peer_headers.empty()) {
        if (options.expected_last_hash.empty()) throw Error("Ledgers read from peers must end at a quorum-validated ledger");
        auto rpc_start = std::chrono::steady_clock::now();
        auto h = peers->header(from_hex(options.expected_last_hash));
        for (;;) {
            if (!peer_headers.empty() && h.sequence + 1 != peer_headers.begin()->first) throw Error("Peer ledger headers do not form a chain");
            peer_headers[h.sequence] = h;
            if (h.sequence <= options.range.first) break;
            h = peers->header(from_hex(h.parent_hash));
        }
        if (peer_headers.begin()->first != options.range.first || peer_headers.rbegin()->first != options.range.last) throw Error("Peer headers do not span the batch");
        rpc_seconds += elapsed(rpc_start);
    }
    // A ledger's transactions and the objects they touched depend only on its header, so the
    // next few are fetched from peers concurrently while ledgers are verified in order. A
    // follower's go through its PeerAhead (those it started already are taken from it), so a
    // batch that fails leaves them running there rather than waiting for them as it unwinds.
    std::map<std::uint64_t, std::shared_future<PeerLedger>> prefetched;
    auto prefetch = [&](std::uint64_t seq) {
        if (!peers || seq > options.range.last || prefetched.contains(seq)) return;
        auto const& header = peer_headers.at(seq);
        if (peer_ahead) { prefetched[seq] = peer_ahead->start(*peers, header); return; }
        prefetched[seq] = std::async(std::launch::async, [peers, header] { return fetch_peer_ledger(*peers, header); }).share();
    };
    for (auto n = options.range.first; n < options.range.first + 8; ++n) prefetch(n);
    static bool const tree_changes = [] { auto v = std::getenv("XRPLA_PEER_TREE_CHANGES"); return v && std::string(v) == "1"; }();
    // From a node, ledgers are fetched ahead of their verification on `jobs` connections (and,
    // with --grpc-jobs, on that many more to the gRPC node beside a JSON-RPC one: the first
    // grpc_jobs threads ask it).
    std::unique_ptr<LedgerPrefetch> ahead;
    if (!records && !peers)
        ahead = std::make_unique<LedgerPrefetch>([&](std::uint32_t seq, unsigned worker) {
                auto fetched = options.grpc_jobs ? source.ledger(seq, worker < options.grpc_jobs) : source.ledger(seq);
                // The objects it changes are known now: the verifier reads their tree paths while
                // the ledgers before it apply.
                if (auto diff = fetched.second.find("diff"); diff != fetched.second.end() && diff->is_array()) {
                    std::vector<Bytes> keys;
                    keys.reserve(diff->size());
                    for (auto const& item : *diff) keys.push_back(from_hex(item.at("object_id").get<std::string>()));
                    verifier.prefetch(std::move(keys));
                }
                return fetched;
            }, options.range.first, options.range.last, options.jobs + options.grpc_jobs);
    // Records (a publication's, a spool's) are read a few ahead of their verification, and the
    // objects each changes go to the verifier to read their tree paths meanwhile, as ledgers
    // fetched from a node do. None past the range is read.
    std::deque<LedgerRecord> upcoming;
    auto read_ahead = [&](std::uint64_t seq) {
        while (upcoming.size() < 16 && (upcoming.empty() ? seq : upcoming.back().sequence + 1) <= options.range.last) {
            LedgerRecord next;
            if (!records(next)) break;
            std::vector<Bytes> keys;
            keys.reserve(next.patches.size());
            for (auto const& patch : next.patches) keys.push_back(patch.key);
            verifier.prefetch(std::move(keys));
            upcoming.push_back(std::move(next));
        }
    };
    Bytes parent_negative_unl;  // the last ledger's parent's NegativeUNL object, for `check_last`
    for (std::uint64_t seq = options.range.first; seq <= options.range.last; ++seq) {
        LedgerRecord ledger;
        auto decode_start = std::chrono::steady_clock::now();
        if (records) {
            read_ahead(seq);
            if (upcoming.empty() || upcoming.front().sequence != seq) throw Error("Source ledger records are out of order at " + std::to_string(seq));
            ledger = std::move(upcoming.front()); upcoming.pop_front();
        } else {
            auto rpc_start = std::chrono::steady_clock::now();
            Header header;
            Json document;
            std::vector<TransactionRecord> received;
            PeerLedger fetched;
            if (peers) {
                header = peer_headers.at(seq);
                fetched = prefetched.extract(seq).mapped().get();
                prefetch(seq + 8);
                received = std::move(fetched.transactions);
            }
            else {
                std::tie(header, document) = ahead->take(seq);
                for (auto const& tx : document.at("transactions")) {
                    auto txblob = tx.contains("tx_blob") ? tx.at("tx_blob") : tx.at("tx");
                    received.push_back({from_hex(txblob.get<std::string>()), from_hex(tx.at("meta").get<std::string>())});
                }
            }
            rpc_seconds += elapsed(rpc_start);
            decode_start = std::chrono::steady_clock::now();
            ledger = {header.sequence, header.raw, {}, {}};
            std::set<Bytes> keys;
            std::map<Bytes, TransactionRecord> transactions;
            for (auto& record : received) {
                // Read by type code, so transactions this build's protocol tables do not know
                // are exported like any other.
                auto summary = wire::summarize(record);
                auto hash = summary.hash;
                for (auto& key : summary.keys) keys.insert(std::move(key));
                if (!transactions.emplace(hash, std::move(record)).second) throw Error("Duplicate source transaction");
            }
            for (auto& [hash, record] : transactions) ledger.transactions.push_back(std::move(record));
            if (peers && tree_changes) {
                // Validation switch (XRPLA_PEER_TREE_CHANGES=1): every ledger's changes from
                // comparing its state tree with the verified parent's, the fallback's path.
                ledger.patches = peers->changes(header, verifier);
            } else if (peers) {
                for (std::size_t n = 0; n < fetched.keys.size(); ++n)
                    if (verifier.state_object(fetched.keys[n]) != fetched.values[n]) ledger.patches.push_back({fetched.keys[n], std::move(fetched.values[n])});
            } else if (options.full_state) {
                std::set<Bytes> seen;
                source.state(header, Json::object(), 1, root, [&](ObjectRecord const& item) {
                    seen.insert(item.key);
                    if (verifier.state_object(item.key) != item.object) ledger.patches.push_back(item);
                });
                verifier.state_objects([&](ObjectRecord const& item) { if (!seen.contains(item.key)) ledger.patches.push_back({item.key, {}}); });
            } else if (document.contains("peer_objects")) {
                // A ledger the node could not serve, read from peers: the objects that changed.
                for (auto const& item : document.at("peer_objects")) {
                    auto key = from_hex(item.at("object_id").get<std::string>()), object = from_hex(item.at("object").get<std::string>());
                    if (verifier.state_object(key) != object) ledger.patches.push_back({std::move(key), std::move(object)});
                }
            } else if (document.contains("diff")) {
                for (auto const& item : document.at("diff"))
                    ledger.patches.push_back({from_hex(item.at("object_id").get<std::string>()), from_hex(item.at("object").get<std::string>())});
            } else {
                // The ledger's close changes some objects outside transaction metadata.
                for (auto& key : closing_keys(header.sequence)) keys.insert(std::move(key));
                // Bound response buffering as well as network concurrency. Each key
                // occurs once, so concurrent workers never share a cache filename.
                std::vector<Bytes> ordered(keys.begin(), keys.end());
                for (std::size_t start = 0; start < ordered.size(); start += 256) {
                  std::vector<Json> requests;
                  for (auto n = start; n < std::min(start + 256, ordered.size()); ++n)
                    requests.push_back({{"ledger_hash", header.hash}, {"index", to_hex(ordered[n])}, {"binary", true}});
                  auto responses = source.rpc().call_many("ledger_entry", requests, options.jobs);
                  for (std::size_t n = 0; n < responses.size(); ++n) {
                    auto const& key = ordered[start + n]; auto const& response = responses[n];
                    pinned(response, header);
                    Bytes object;
                    if (response.value("error", "") != "entryNotFound") { success(response); object = from_hex(response.at("node_binary").get<std::string>()); }
                    if (verifier.state_object(key) != object) ledger.patches.push_back({key, std::move(object)});
                  }
                }
            }
        }
        std::sort(ledger.patches.begin(), ledger.patches.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
        decode_seconds += elapsed(decode_start);
        if (check_last && seq == options.range.last) {
            auto key = ::xrpl::keylet::negativeUNL().key;
            parent_negative_unl = verifier.state_object(Bytes(reinterpret_cast<char const*>(key.data()), key.size()));
        }
        auto verify_start = std::chrono::steady_clock::now();
        try { verifier.advance(ledger); }
        catch (std::exception const& error) {
            // From peers or a JSON-RPC node, a ledger's changes are what its transactions'
            // metadata and its close name. Should its state still not match (a change neither
            // names), its state tree is compared with the verified parent's instead (from peers
            // that hold the ledger), which finds every change.
            if ((!peers && !source.has_history_peers()) || std::string(error.what()).find("state SHAMap root mismatch") == std::string::npos)
                throw Error("Export ledger " + std::to_string(seq) + ": " + error.what() + "; retry with --full-state to compare all objects");
            std::set<Bytes> named;
            for (auto const& patch : ledger.patches) named.insert(patch.key);
            auto comparing = std::chrono::steady_clock::now();
            ledger.patches = peers ? peers->changes(decode_header(ledger.header), verifier) : source.peer_changes(decode_header(ledger.header), verifier);
            Json unnamed = Json::array();
            for (auto const& patch : ledger.patches)
                if (!named.contains(patch.key) && unnamed.size() < 16) unnamed.push_back({{"key", to_hex(patch.key)}, {"deleted", patch.object.empty()}});
            std::cerr << Json{{"event", "state_changes_from_tree"}, {"ledger", seq}, {"changes", ledger.patches.size()}, {"unnamed", unnamed},
                              {"seconds", elapsed(comparing)}}.dump() << std::endl;
            try { verifier.advance(ledger); }
            catch (std::exception const& again) { throw Error("Export ledger " + std::to_string(seq) + ": " + again.what() + " (also comparing its state tree)"); }
        }
        verification_seconds += elapsed(verify_start);
        if (options.interval_snapshots && seq % options.interval_snapshots == 0) interval_snapshot(verifier, root, interval_descriptors);
        if (committed) applied.push_back(verifier.header());
        patch_count += ledger.patches.size(); transaction_count += ledger.transactions.size();
        for (auto const& p : ledger.patches) ledger_bytes += 36 + p.object.size();
        for (auto const& t : ledger.transactions) ledger_bytes += 8 + t.transaction.size() + t.metadata.size();
        ledger_bytes += ledger.header.size() + 16;
        ledgers.push_back(std::move(ledger));
        if (ledgers.size() >= 256 || ledger_bytes >= 64 * 1024 * 1024) flush_chunk();
        if (!feed || seq % 1024 == 0 || seq == options.range.last) std::cerr << "Verified source ledger " << seq << " / " << options.range.last << '\n';
    }
    flush_chunk();
    std::vector<std::string> replaced;
    if (options.merge_chunks) {
        auto field = [](Json const& chunk, char const* name) { return chunk.at(name).get<std::uint64_t>(); };
        for (std::uint64_t width : {16u, 256u}) {
            for (std::size_t i = 0; i < chunks.size();) {
                auto block_end = (field(chunks[i], "first") / width + 1) * width - 1;
                auto j = i;
                std::uint64_t bytes = 0;
                while (j < chunks.size() && field(chunks[j], "last") <= block_end) bytes += field(chunks[j++], "size");
                // Only blocks this export completed, so an existing spool is never rewritten wholesale.
                if (j - i < 2 || block_end < options.range.first || block_end > options.range.last || bytes > 16 * 1024 * 1024) { i = std::max(j, i + 1); continue; }
                for (auto n = i; n < j; ++n) {
                    Object file{chunks[n].at("object"), field(chunks[n], "size"), chunks[n].at("sha256")};
                    checked(root, file);
                    read_chunk(root / file.object, {}, [&](LedgerRecord&& ledger) { ledgers.push_back(std::move(ledger)); });
                    replaced.push_back(file.object);
                    if (chunks[n].contains("time_index")) replaced.push_back(chunks[n].at("time_index").at("object"));
                }
                flush_chunk();
                auto merged = chunks.back();
                chunks.erase(chunks.size() - 1);
                chunks.erase(chunks.begin() + i, chunks.begin() + j);
                chunks.insert(chunks.begin() + i, merged);
                ++i;
            }
        }
        // A replaced file's time is when it was retired; it is removed an hour later (below).
        for (auto const& object : replaced) fs::last_write_time(root / object, fs::file_time_type::clock::now());
    }
    auto tip = verifier.header();
    if (!options.expected_last_hash.empty() && tip.hash != normalized(options.expected_last_hash)) {
        abandon_interval_snapshots(interval_descriptors);
        throw Error("Exported ledgers do not end at the quorum-validated ledger " + options.expected_last_hash);
    }
    if (check_last) {
        try { check_last(tip, parent_negative_unl); }
        catch (...) { abandon_interval_snapshots(interval_descriptors); throw; }
    }
    // The batch's ledgers are the quorum's: their interval snapshots can be named now.
    write_interval_descriptors(interval_descriptors);
    auto now = std::time(nullptr); char timestamp[32];
    std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&now));
    manifest["published_at"] = timestamp;
    manifest["tip"] = {{"seq", tip.sequence}, {"ledger_hash", tip.hash}};
    manifest["signature_format"] = "sha256-path-v1";
    auto bytes = manifest.dump();
    if (bytes.size() > 16 * 1024 * 1024) throw Error("Manifest exceeds the 16 MiB reader limit; start a new archive generation");
    Bytes signature(crypto_sign_BYTES, '\0');
    crypto_sign_detached(reinterpret_cast<unsigned char*>(signature.data()), nullptr,
        reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size(), sk.data());
    // Persist nodes and all immutable objects before publishing the commit point.
    verifier.persist();
    immutable_signature(root, bytes, signature);
    write_file(root / "publisher.pub", raw(pk));
    // The hour readers get with the manifest being replaced counts from now, not from when its
    // signature was written: after a quiet hour that signature would otherwise be pruned below
    // at once, under a reader that has just read the old manifest.json.
    if (fs::exists(root / "manifest.json")) {
        auto outgoing = root / "signatures" / (sha256(read_file(root / "manifest.json", 16 * 1024 * 1024)) + ".sig");
        if (fs::exists(outgoing)) fs::last_write_time(outgoing, fs::file_time_type::clock::now());
    }
    write_file(root / "manifest.json.part", bytes);
    durable_rename(root / "manifest.json.part", root / "manifest.json");
    // Committed: the verifier stands at the new tip, which a failure from here on (housekeeping)
    // must not rewind it from, and a follower serves the batch's ledgers whatever happens next.
    rewind.header.clear();
    verifier.commit_point();  // automatic compaction in the next batch keeps this tree
    Json result{{"source", options.source}, {"range", {{"first", options.range.first}, {"last", options.range.last}}},
        {"snapshot_objects", object_count}, {"transactions", transaction_count}, {"patches", patch_count},
        {"last_ledger_hash", tip.hash}, {"ledger_hashes_verified", true}, {"source_reports_validated", true},
        {"independent_consensus_validation", false}, {"extended", existing}, {"resumed_nodes", resumed}, {"rpc_jobs", options.jobs}, {"grpc_jobs", options.grpc_jobs},
        {"diff_transport", feed ? "publication" : peers ? "peers" : options.full_state ? "full-state" : source.transport()}};
    result["compression_level"] = options.compression_level;
    result["rpc_cache"] = options.rpc_cache;
    result["ledger_stage_seconds"] = {{"rpc_and_cache", rpc_seconds}, {"decode_and_patch_collection", decode_seconds},
                                      {"canonical_verification", verification_seconds}};
    try {
        // Legacy readers retry this pair. New readers use the immutable signature.
        write_file(root / "manifest.json.sig.part", signature);
        durable_rename(root / "manifest.json.sig.part", root / "manifest.json.sig");
        // Readers holding an earlier manifest get an hour to finish with the files it named: the
        // chunks merging replaced, and its immutable signature (one is written every batch).
        auto cutoff = fs::file_time_type::clock::now() - std::chrono::hours(1);
        if (!replaced.empty()) {
            std::set<fs::path> live;
            for (auto const& chunk : chunks) {
                live.insert(root / chunk.at("object").get<std::string>());
                if (chunk.contains("time_index")) live.insert(root / chunk.at("time_index").at("object").get<std::string>());
            }
            for (auto top : {"chunks", "times"})
                for (auto const& file : fs::directory_iterator(root / top))
                    if (file.is_regular_file() && !live.contains(file.path()) && file.last_write_time() < cutoff) fs::remove(file.path());
        }
        auto current = sha256(bytes) + ".sig";
        for (auto const& file : fs::directory_iterator(root / "signatures"))
            if (file.is_regular_file() && file.path().extension() == ".sig" && file.path().filename() != current && file.last_write_time() < cutoff)
                fs::remove(file.path());
        // A following publisher keeps one archive for a whole interval. Its verification nodes
        // would keep every ledger's superseded nodes, so they are rewritten to the current
        // state now and then; the marker records the last ledger compacted. A follower's verifier
        // compacts on a thread of its own while ledgers keep applying, the copy swapped in at the
        // end of a later batch (on a slow disk the walk took 12 minutes, and following waited).
        auto marker = root / ".verification-nodes.compacted";
        std::uint64_t compacted = fs::exists(marker) ? std::stoull(read_file(marker, 32)) : manifest.at("chunks").front().at("first").get<std::uint64_t>();
        if (kept) {
            if (auto done = verifier.finish_compaction()) {
                result["compacted_nodes"] = done->nodes;
                result["compaction_seconds"] = done->seconds;
                write_file(marker, std::to_string(done->ledger));
            } else if (!verifier.compacting() && tip.sequence >= compacted + 16384 && verifier.start_compaction()) {
                result["compaction_started"] = tip.sequence;
            }
        } else if (tip.sequence >= compacted + 16384) {
            auto started = std::chrono::steady_clock::now();
            result["compacted_nodes"] = verifier.compact_nodes();
            result["compaction_seconds"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
            write_file(marker, std::to_string(tip.sequence));
        }
        write_file(root / "export-report.json", result.dump(2));
    } catch (std::exception const& error) {
        // A one-off export reports it as before; a follower has committed the batch, so it keeps
        // its verifier and serves the ledgers, and the next batch does this housekeeping again.
        if (!kept) throw;
        result["after_commit_error"] = error.what();
        std::cerr << Json{{"event", "publish_after_commit_error"}, {"ledger", tip.sequence}, {"error", error.what()}}.dump() << std::endl;
    }
    if (kept) *kept = std::move(owner);
    if (committed) *committed = std::move(applied);
    return result;
}
}
Json check_keys(fs::path const& archive, fs::path const& publisher_key) {
    auto key = pinned_publisher_key(publisher_key);
    auto manifest = read_manifest(Source(archive.string()), key).manifest;
    std::uint64_t ledgers = 0, changes = 0;
    Json missing = Json::array();
    for (auto const& chunk : manifest.chunks) {
        checked(archive, chunk.file);
        read_chunk(archive / chunk.file.object, {}, [&](LedgerRecord&& ledger) {
            ++ledgers; changes += ledger.patches.size();
            auto closing = closing_keys(ledger.sequence);
            std::set<Bytes> derived(closing.begin(), closing.end());
            for (auto const& record : ledger.transactions)
                for (auto& touched : wire::summarize(record).keys) derived.insert(std::move(touched));
            Json keys = Json::array();
            for (auto const& patch : ledger.patches)
                if (!derived.contains(patch.key)) keys.push_back({{"key", to_hex(patch.key)}, {"deleted", patch.object.empty()}});
            if (!keys.empty()) missing.push_back({{"ledger", ledger.sequence}, {"keys", keys}});
        });
    }
    return {{"ledgers", ledgers}, {"changes", changes}, {"ledgers_missing_changes", missing.size()}, {"missing", missing}};
}
Json export_archive(ExportOptions const& options) {
    DirectoryLock lock(options.output);
    return export_locked(options);
}
namespace {
// The negative UNL in a verified ledger's state (its NegativeUNL object).
NegativeUnl negative_unl_in(LedgerVerifier& verifier) {
    auto key = ::xrpl::keylet::negativeUNL().key;
    return negative_unl(verifier.state_object(Bytes(reinterpret_cast<char const*>(key.data()), key.size())));
}
// Follows a node's validations stream (or those peers relay) and remembers each ledger a quorum
// of listed validators signed. Reconnects after errors, and fetches the list again on a thread
// of its own (refresh_in): a list site out of reach leaves the list in force, and the stream
// read; a list fetched that is not newer than the one in force changes nothing, and a pending
// list (a version 2 list's later blob) is put in force at its effective time from the list
// fetched before (ValidationQuorum::apply and rotate). The stream's thread holds the watcher
// only while it records a validation, and stops (closing its connection) once the watcher's
// last owner lets go: at the next message, or within the 30 s a silent stream is given; the
// list's thread stops then, or once a fetch under way ends.
//
// A ledger is judged under the negative UNL xrpld judges it with, that of its parent's state
// (ValidationQuorum explains the rule): known from the committed ledger's verified state for
// the ledgers after it up to the end of the flag period after it. The newest ledgers of a
// follower further behind are only provisionally validated: a batch from a node may end at
// one, as the commit check (confirms) judges it under its verified parent's negative UNL before
// it is committed; from peers, the parent's NegativeUNL object of the newest is read from them
// (learn_negative_unl), which judges that ledger alone. Each negative UNL given judges every
// ledger counted again. Once the list expires (and is not fetched again) nothing is committed.
//
// As xrpld, a list with a later sequence is put in force even if it names no validators or has
// expired, as is a revocation of the publisher's key (for good): nothing is committed then. The
// newest publisher manifest seen is kept by the list's loader (verify_validator_list), so an
// older one a list site serves changes nothing. Neither the list nor that manifest is kept
// across a restart, as xrpld keeps them (its cached list files, its manifests in the wallet
// database): the first list fetched then is put in force whatever its sequence, as for a
// publisher first heard from, so a list site (or one in its place, under the pinned key) serving
// an older list, still in force by its own times, has it trusted until a newer one is fetched.
struct QuorumWatcher {
    std::mutex mutex;
    std::map<std::uint32_t, std::string> validated;  // ledger sequence -> hash (lowercase hex)
    // Ledgers a quorum validated under a negative UNL not known yet (see above).
    std::map<std::uint32_t, std::string> provisional;
    std::string last_error;  // the stream's
    std::string list_error;  // the list's last fetch's, cleared by one that succeeds and puts a list validating in force
    // Notified on every validation counted (`changes` counts them), for those waiting for one.
    std::condition_variable changed;
    std::uint64_t changes = 0;
    // The newest committed ledger's negative UNL given (its sequence, the list in its verified
    // state), for the first quorum should it be given before the list is fetched (later ones
    // adopt it).
    std::optional<std::pair<std::uint32_t, NegativeUnl>> committed_unl;
    void set_negative_unl(std::uint32_t ledger, NegativeUnl unl) {
        {
            std::lock_guard lock(mutex);
            committed_unl.emplace(ledger, unl);
            if (!quorum) { forget_before(ledger); return; }
            quorum->negative_unl(ledger, std::move(unl));
            judge_again();
            forget_before(ledger);  // after judging again, which records the ledgers the quorum still holds
            ++changes;
        }
        changed.notify_all();
    }
    // The list fetched (again), judged against the one in force (ValidationQuorum::apply): one
    // with a later sequence replaces it, what the one before counted carrying over; one with the
    // same sequence changes nothing, and one with an earlier sequence is refused. When the list
    // is to be fetched again (refresh_in, for the list in force then; as a failed fetch is,
    // retry_in, while that validates nothing).
    std::chrono::seconds replace_quorum(ValidatorList fresh) {
        auto fetched = fresh.sequence;
        std::optional<std::uint32_t> stale;
        std::chrono::seconds wait;
        {
            std::lock_guard lock(mutex);
            list_error.clear();
            if (!quorum) {
                quorum = std::make_unique<ValidationQuorum>(std::move(fresh));
                if (committed_unl) quorum->negative_unl(committed_unl->first, committed_unl->second);
                ++lists;
            } else if (auto disposition = quorum->apply(std::move(fresh)); disposition == ValidationQuorum::Disposition::accepted) {
                ++lists;
            } else if (disposition == ValidationQuorum::Disposition::stale && fetched) {
                stale = quorum->list().sequence;
            }
            rotate_due();
            judge_again();
            ++changes;
            if (quorum->validating()) validated_once = true;
            else if (quorum->list().sequence == revoked_sequence) list_error = "List publisher key is revoked";
            else list_error = "The validator list in force validates nothing (none in force yet, expired, or naming no validators)";
            wait = quorum->validating() ? refresh_in(quorum->list()) : retry_in();
        }
        changed.notify_all();
        if (stale) std::cerr << Json{{"event", "validator_list_stale"}, {"sequence", fetched}, {"in_force", *stale}}.dump() << std::endl;
        return wait;
    }
    // A pending list whose effective time has come put in force (under the lock), as xrpld does
    // at the first ledger closed from then, whether or not the list site answers now: every
    // ledger counted is judged again. Whether one was.
    bool rotate_due() {
        if (!quorum || !quorum->rotate()) return false;
        ++lists;
        judge_again();
        return true;
    }
    // The same, from the list's thread at that time.
    void rotate_list() {
        {
            std::lock_guard lock(mutex);
            if (!rotate_due()) return;
            ++changes;
        }
        changed.notify_all();
    }
    // A fetch of the list failed: when it is tried again (retry_in).
    std::chrono::seconds list_failed(std::string const& error) {
        std::lock_guard lock(mutex);
        list_error = error;
        return retry_in();
    }
    // Whether a list in force validated anything yet, once fetched.
    bool validated_once = false;
    // When a list is fetched again after a failed fetch, or one that validates nothing (under the
    // lock): a minute later (5 s later while no list fetched validated anything yet), sooner
    // should a pending list take effect before.
    std::chrono::seconds retry_in() {
        auto retry = std::chrono::seconds(validated_once ? 60 : 5);
        return quorum ? std::min(retry, refresh_in(quorum->list())) : retry;
    }
    // Counts the lists put in force, for the stream to ask each one's validators' manifests.
    std::uint64_t lists = 0;
    std::uint64_t lists_in_force() { std::lock_guard lock(mutex); return lists; }
    // The listed validators' master keys: nullopt while no list was fetched.
    std::optional<std::vector<Bytes>> listed() {
        std::lock_guard lock(mutex);
        if (!quorum) return std::nullopt;
        std::vector<Bytes> masters;
        for (auto const& [master, signing] : quorum->list().signing_keys) masters.push_back(master);
        return masters;
    }
    // Records a ledger's verdict (under the lock). A validated ledger judged again short of its
    // quorum (a validator revoked, the list expired, or fetched again without enough of its
    // signers) is withdrawn, and with it the hashes found from validated ledgers' parents (and
    // whatever walks under way then find: `generation`). One only provisional again, its negative
    // UNL no longer kept (a ledger at or before a committed one), stays: its verdict is unchanged.
    void record(ValidationQuorum::Counted const& counted) {
        auto hash = to_hex(counted.ledger_hash);
        auto keep = [&](std::map<std::uint32_t, std::string>& ledgers) {
            ledgers.insert_or_assign(counted.ledger_index, hash);
            while (ledgers.size() > 100000) ledgers.erase(ledgers.begin());
        };
        auto drop = [&](std::map<std::uint32_t, std::string>& ledgers) {
            auto it = ledgers.find(counted.ledger_index);
            if (it == ledgers.end() || it->second != hash) return false;
            ledgers.erase(it);
            return true;
        };
        if (counted.verdict == ValidationQuorum::Verdict::validated) keep(validated);
        else if (counted.verdict == ValidationQuorum::Verdict::short_of_quorum && drop(validated)) { chain.clear(); ++generation; }
        if (counted.verdict == ValidationQuorum::Verdict::provisional) keep(provisional);
        else drop(provisional);
    }
    void judge_again() {
        for (auto const& counted : quorum->ledgers()) record(counted);
    }
    // The ledgers before the committed one `ledger` (under the lock): nothing asks for them again
    // (a batch starts after it, and ends, and finds the hashes leading to its end, later still),
    // and kept until the caps they filled some 40 MB in four days of following the tip. The
    // newest of each stays: newest() and tally() read it. The caps (100000, some four days) stay
    // for a follower stalled behind validations that still arrive: every ledger after its
    // committed one is then needed, as a batch ends at the first validated ledger it knows past
    // `limit` (end_for), so a smaller cap would make the batch that catches up larger.
    void forget_before(std::uint32_t ledger) {
        for (auto* ledgers : {&validated, &provisional, &chain}) {
            if (ledgers->empty()) continue;
            auto end = ledgers->lower_bound(ledger);
            if (end == ledgers->end()) end = std::prev(end);
            ledgers->erase(ledgers->begin(), end);
        }
    }
    static std::shared_ptr<QuorumWatcher> start(PublishOptions const& options) {
        auto watcher = std::make_shared<QuorumWatcher>();
        // The list is fetched on its own thread, as from peers, so a fetch (retried for minutes
        // while the list site is out of reach) never holds up the stream.
        std::thread(follow_list, std::weak_ptr(watcher), watcher->ended, loader(options.validator_list, options.validator_list_key), std::chrono::seconds(0)).detach();
        std::thread([weak = std::weak_ptr(watcher), node = options.validations_node] {
            while (!weak.expired()) {
                try {
                    // The stream connects once any list is in force, and only reads: a list put in
                    // force later is seen by its count (lists_in_force), read before the list so
                    // that one put in force meanwhile is at worst asked for twice.
                    std::uint64_t asked = 0;
                    std::optional<std::vector<Bytes>> masters;
                    if (auto watcher = weak.lock()) { asked = watcher->lists_in_force(); masters = watcher->listed(); }
                    else return;
                    if (!masters) { std::this_thread::sleep_for(std::chrono::seconds(1)); continue; }  // no list yet
                    WsClient client(node);
                    client.send({{"id", 1}, {"command", "subscribe"}, {"streams", {"validations", "manifests"}}});
                    // The node's manifest of each listed validator: the stream gives only those
                    // that arrive from now on, and the list site may not yet serve a rotation the
                    // node already knows. A revocation the node learned before is not had this
                    // way: its manifest request answers nothing for a revoked key
                    // (ManifestCache::getManifest, Manifest.cpp:360-368), and no other request
                    // gives the signed revocation (the stream gives a manifest once, as the node
                    // first accepts it: OverlayImpl::onManifests), so the revoked key stays trusted
                    // here until the list site drops the validator. From peers, every revocation
                    // they relay is applied (their TMManifests).
                    int id = 1;
                    auto ask = [&](std::vector<Bytes> const& keys) { for (auto const& master : keys) client.send(manifest_request(master, ++id)); };
                    ask(*masters);
                    // A list fetched again (refresh_in) has its validators' manifests asked for on
                    // the same connection; a validator's key rotation (a newer manifest) applies as
                    // it arrives. Mainnet's validators sign every few seconds, so a stream silent
                    // for 30 s (a node out of touch, or a connection gone half-open) is left for a
                    // new connection.
                    constexpr auto silence = std::chrono::seconds(30);
                    auto heard = std::chrono::steady_clock::now();
                    for (;;) {
                        std::optional<std::vector<Bytes>> keys;
                        if (auto watcher = weak.lock()) {
                            if (auto in_force = watcher->lists_in_force(); in_force != asked) { asked = in_force; keys = watcher->listed(); }
                        } else return;
                        if (keys) ask(*keys);
                        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(heard + silence - std::chrono::steady_clock::now());
                        if (left.count() <= 0) throw Error("No validations from " + node + " for 30 s");
                        auto message = client.receive(left);
                        auto manifest = manifest_from_stream(message);
                        if (!manifest) manifest = manifest_from_answer(message);
                        if (!manifest && (message.value("type", "") != "validationReceived" || !message.contains("data"))) continue;
                        auto watcher = weak.lock();
                        if (!watcher) return;
                        if (manifest) {
                            auto master = message.contains("master_key") ? message.at("master_key") : Json(message.at("result").value("requested", ""));
                            if (watcher->manifest(*manifest)) std::cerr << Json{{"event", "validator_key_rotated"}, {"master_key", master}}.dump() << std::endl;
                            continue;
                        }
                        heard = std::chrono::steady_clock::now();
                        watcher->validation(from_hex(message.at("data").get<std::string>()));
                    }
                } catch (std::exception const& error) {
                    auto watcher = weak.lock();
                    if (!watcher) return;
                    { std::lock_guard lock(watcher->mutex); watcher->last_error = error.what(); }
                    watcher.reset();  // not held through the wait
                    std::cerr << Json{{"event", "validations_retry"}, {"error", error.what()}}.dump() << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                }
            }
        }).detach();
        return watcher;
    }
    // The validations counted against the list (re-fetched hourly), from the stream or from peers.
    std::unique_ptr<ValidationQuorum> quorum;
    static std::shared_ptr<QuorumWatcher> for_peers(std::string const& list, std::string const& key) {
        auto watcher = std::make_shared<QuorumWatcher>();
        auto load = loader(list, key);
        auto wait = watcher->replace_quorum(load());
        std::thread(follow_list, std::weak_ptr(watcher), watcher->ended, std::move(load), wait).detach();
        return watcher;
    }
    // Fetches and verifies the list; the newest publisher manifest seen is kept from one fetch to
    // the next (one at a time: the list's thread's).
    static std::function<ValidatorList()> loader(std::string const& list, std::string const& key) {
        return [list, key, publisher = std::make_shared<std::optional<PublisherManifest>>()] {
            auto text = list.starts_with("https://") || list.starts_with("http://") ? fetch_url(list, 16 << 20) : read_file(list, 16 << 20);
            return verify_validator_list(parse_as_xrpld(text), key, xrpl_seconds_now(), *publisher);
        };
    }
    // Set as the watcher goes, for the list's thread waiting to fetch it again (which holds this,
    // not the watcher) to stop then instead of up to an hour later.
    struct Ended { std::mutex mutex; std::condition_variable changed; bool ended = false; };
    std::shared_ptr<Ended> ended = std::make_shared<Ended>();
    ~QuorumWatcher() {
        { std::lock_guard lock(ended->mutex); ended->ended = true; }
        ended->changed.notify_all();
    }
    // The list's thread: the list fetched after `wait`, then whenever refresh_in says. What the
    // validations counted so far, and newer manifests than the list's, carry over to a list
    // fetched again (replace_quorum); one that cannot be fetched leaves the one in force counting
    // (list_failed says when it is tried again), and once that expires nothing is validated until
    // another is fetched. A pending list is put in force at its effective time first, the list
    // site in reach or not. The watcher is held neither through a fetch nor through a wait.
    static void follow_list(std::weak_ptr<QuorumWatcher> weak, std::shared_ptr<Ended> ended, std::function<ValidatorList()> load, std::chrono::seconds wait) {
        for (;;) {
            {
                std::unique_lock lock(ended->mutex);
                if (ended->changed.wait_for(lock, wait, [&] { return ended->ended; })) return;
            }
            if (auto watcher = weak.lock()) watcher->rotate_list();
            else return;
            try {
                auto fresh = load();
                auto watcher = weak.lock();
                if (!watcher) return;
                wait = watcher->replace_quorum(std::move(fresh));
            } catch (std::exception const& error) {
                auto watcher = weak.lock();
                if (!watcher) return;
                wait = watcher->list_failed(error.what());
                std::cerr << Json{{"event", "validator_list_retry"}, {"error", error.what()}}.dump() << std::endl;
            }
        }
    }
    // When a list is fetched again: hourly and ten minutes before it expires, a minute apart at
    // least, and a second after a pending one (a version 2 list's next blob) takes effect, which
    // is put in force then from the list fetched before (rotate_list), as xrpld rotates to it at
    // the first ledger closed from then (ValidatorList.cpp:1813-1885, updateTrusted); validations
    // and commit checks put it in force as well should the list's thread be in a fetch then.
    static std::chrono::seconds refresh_in(ValidatorList const& list) {
        auto now = xrpl_seconds_now();
        std::uint64_t wait = 3600;
        if (list.expiration) wait = std::min<std::uint64_t>(wait, list.expiration > now + 600 ? list.expiration - now - 600 : 0);
        wait = std::max<std::uint64_t>(wait, 60);
        if (auto next = list.next_effective()) wait = std::min<std::uint64_t>(wait, next >= now ? next - now + 1 : 1);
        return std::chrono::seconds(wait);
    }
    // The newest ledgers' leading hash by trusted validations so far, before a quorum: where a
    // follower may start fetching (it commits only what a quorum validated).
    std::map<std::uint32_t, std::pair<std::string, std::size_t>> early;
    std::optional<std::string> early_hash(std::uint32_t seq) {
        std::lock_guard lock(mutex);
        auto it = early.find(seq);
        return it == early.end() ? std::nullopt : std::optional(it->second.first);
    }
    std::optional<std::uint32_t> newest_seen() {
        std::lock_guard lock(mutex);
        return early.empty() ? std::nullopt : std::optional(early.rbegin()->first);
    }
    void validation(Bytes const& serialized) {
        {
            std::lock_guard lock(mutex);
            // A pending list whose time has come judges the validation (rotate_due).
            auto rotated = rotate_due();
            auto counted = quorum ? quorum->add(serialized) : std::nullopt;
            if (!counted && !rotated) return;
            ++changes;
            if (counted) tally(*counted);
        }
        changed.notify_all();
    }
    // A validation counted (under the lock): the leading hash of its ledger, and its verdict.
    void tally(ValidationQuorum::Counted const& counted) {
        auto& leading = early[counted.ledger_index];
        if (counted.count > leading.second) leading = {to_hex(counted.ledger_hash), counted.count};
        record(counted);
        // Not those more than a flag period after the newest ledger a quorum signed (validated
        // or provisionally): one trusted validator signing far later sequences (a faulty one)
        // would push the network's newest ledgers out. Nor those within a flag period after the
        // ledger most trusted validators signed so far: while no quorum is reached (the newest
        // a quorum signed an older one) the network's newest stay, and a faulty validator's
        // ledgers, signed by it alone, still go.
        std::optional<std::uint32_t> reached;
        if (!validated.empty()) reached = validated.rbegin()->first;
        if (!provisional.empty()) reached = std::max(reached.value_or(0), provisional.rbegin()->first);
        if (reached) {
            auto most = std::max_element(early.begin(), early.end(), [](auto const& a, auto const& b) { return a.second.second < b.second.second; });
            reached = std::max(*reached, most->first);
        }
        if (reached && *reached <= std::numeric_limits<std::uint32_t>::max() - 256) early.erase(early.upper_bound(*reached + 256), early.end());
        while (early.size() > 64) early.erase(early.begin());
    }
    // Waits until a validation is counted after `seen` (a value of `changes`), for at most
    // `timeout`, or a stop; returns `changes` then.
    std::uint64_t wait_change(std::uint64_t seen, std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        // A stop signal only sets a flag, so it is looked at every 100 ms.
        while (changes == seen && !stopping && std::chrono::steady_clock::now() < deadline)
            changed.wait_until(lock, std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(100)));
        return changes;
    }
    std::uint64_t change_count() { std::lock_guard lock(mutex); return changes; }
    // Waits until a quorum has validated a ledger after `seq`, for at most `timeout`, or a stop.
    bool wait_validated_after(std::uint32_t seq, std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            if (!validated.empty() && validated.rbegin()->first > seq) return true;
            auto now = std::chrono::steady_clock::now();
            if (stopping || now >= deadline) return false;
            changed.wait_until(lock, std::min(deadline, now + std::chrono::milliseconds(100)));
        }
    }
    // The stream's last error, and the list's while its fetches fail.
    std::string error() {
        std::lock_guard lock(mutex);
        return last_error.empty() || list_error.empty() ? last_error + list_error : last_error + "; " + list_error;
    }
    // A newer manifest judges every ledger counted again: a revoked validator's validations no
    // longer count.
    bool manifest(Bytes const& serialized) {
        std::lock_guard lock(mutex);
        if (!quorum || !quorum->manifest(serialized)) return false;
        judge_again();
        return true;
    }
    bool trusted(Bytes const& signing_key) { std::lock_guard lock(mutex); return quorum && quorum->trusted(signing_key); }
    std::map<std::uint32_t, std::string> chain;  // earlier ledgers' hashes, from validated ledgers' parents
    // Advanced as `chain` is cleared (a validated ledger withdrawn): a walk begun before (its
    // peers asked without the lock) keeps none of the hashes it finds, and names none.
    std::uint64_t generation = 0;
    std::optional<std::string> hash_of(std::uint32_t seq, PeerLedgers& peers) {
        std::uint32_t at;
        std::string hash;
        std::uint64_t begun;
        {
            std::lock_guard lock(mutex);
            auto pick = [&](std::map<std::uint32_t, std::string> const& known) {
                auto it = known.lower_bound(seq);
                if (it != known.end() && (hash.empty() || it->first < at)) { at = it->first; hash = it->second; }
            };
            pick(validated); pick(chain);
            begun = generation;
        }
        if (hash.empty()) return std::nullopt;
        // Far back, as xrpld finds an older ledger's hash (hashOfSeq, walkHashBySeq): a ledger's
        // state lists its 256 predecessors' hashes (keylet::skip()) and, in pages, every 256th
        // ledger's (keylet::skip(n)), each read along a state-tree path checked against the
        // header. A few reads replace a header request per ledger, which peers that no longer
        // hold the ledgers left unanswered.
        while (at > seq + 1) {
            auto header = peers.header(from_hex(hash), at);
            if (header.sequence != at) throw Error("Peer header chain skips a ledger");
            // Within 256 ledgers the target itself; further, the next ledger whose number is a
            // multiple of 256, whose own list then reaches the target.
            auto target = at - seq <= 256 ? seq : (seq + 255) & ~0xffu;
            bool recent = at - target <= 256;
            std::optional<std::string> found;
            try { found = hashes_listed(peers, header, recent ? ::xrpl::keylet::skip() : ::xrpl::keylet::skip(target), recent, target); }
            catch (std::exception const&) {}
            if (!found) break;  // not listed (or not supplied): walked one header at a time below
            remember(at = target, hash = *found, begun);
        }
        while (at > seq) {
            auto header = peers.header(from_hex(hash), at);
            if (header.sequence != at) throw Error("Peer header chain skips a ledger");
            remember(at - 1, hash = header.parent_hash, begun); --at;
        }
        std::lock_guard lock(mutex);
        if (generation != begun) return std::nullopt;  // its anchor withdrawn meanwhile
        return hash;
    }
    // The hash of ledger `target` as `header`'s ledger lists it at `keylet` (a skip list).
    static std::optional<std::string> hashes_listed(PeerLedgers& peers, Header const& header, ::xrpl::Keylet const& keylet, bool recent, std::uint32_t target) {
        Bytes key(reinterpret_cast<char const*>(keylet.key.data()), keylet.key.size());
        auto value = peers.objects(header, {key}).at(0);
        if (value.empty()) return std::nullopt;
        ::xrpl::SerialIter input(value.data(), value.size());
        ::xrpl::STLedgerEntry list(input, keylet.key);
        if (list.getType() != ::xrpl::ltLEDGER_HASHES) return std::nullopt;
        auto last = list.getFieldU32(::xrpl::sfLastLedgerSequence);
        auto const& hashes = list.getFieldV256(::xrpl::sfHashes);
        // The 256-ledger list ends at the ledger's parent; a page at its newest multiple of 256.
        std::size_t back = recent ? header.sequence - target : ((last - target) >> 8) + 1;
        if (recent ? last != header.sequence - 1 : last < target || (last & 0xff) != 0) return std::nullopt;
        if (!back || back > hashes.size()) return std::nullopt;
        auto const& listed = hashes[hashes.size() - back];
        return to_hex(std::string_view(reinterpret_cast<char const*>(listed.data()), listed.size()));
    }
    void remember(std::uint32_t at, std::string const& hash, std::uint64_t begun) {
        std::lock_guard lock(mutex);
        if (generation != begun) return;
        chain[at] = hash;
        while (chain.size() > 100000) chain.erase(chain.begin());
    }
    std::optional<std::uint32_t> newest() {
        std::lock_guard lock(mutex);
        if (validated.empty()) return std::nullopt;
        return validated.rbegin()->first;
    }
    // A batch starting at `next` ends at a quorum-validated ledger no later than `tip`: the
    // newest one within `limit` ledgers, else beyond it (catching up) the first validated one,
    // or the provisionally validated one the most trusted validators signed (the first of those:
    // the oldest is often one whose validations arrived in part, as the watcher started). A
    // provisionally validated one too: the batch's commit check judges it under its parent's
    // negative UNL.
    std::optional<std::pair<std::uint32_t, std::string>> end_for(std::uint64_t next, std::uint64_t tip, std::uint64_t limit) {
        std::lock_guard lock(mutex);
        auto candidates = provisional;
        for (auto const& [seq, hash] : validated) candidates.insert_or_assign(seq, hash);
        std::optional<std::pair<std::uint32_t, std::string>> within, beyond;
        std::size_t most = 0;
        for (auto const& [seq, hash] : candidates) {
            if (seq < next || seq > tip) continue;
            if (seq <= next + limit - 1) { within = std::pair{seq, hash}; continue; }
            if (within) break;
            if (auto it = validated.find(seq); it != validated.end() && it->second == hash) { beyond = std::pair{seq, hash}; break; }
            auto count = quorum ? quorum->count(from_hex(hash)) : 0;
            if (!beyond || count > most) { beyond = std::pair{seq, hash}; most = count; }
        }
        auto chosen = within ? within : beyond;
        // Its validations kept however long the batch takes (one beyond `limit` may take longer
        // than the ledgers a list fetched again keeps), for its commit check (confirms).
        if (chosen && quorum) quorum->keep(from_hex(chosen->second), chosen->first);
        return chosen;
    }
    // The commit check, before a batch ending at `last` commits: the ledger must reach its
    // quorum under the negative UNL read from its parent's verified state (`parent`), judged on
    // its own, or be validated exactly already, or be one an exactly validated ledger names as
    // an ancestor (its hash found from their parents). A ledger is validated exactly under its
    // parent's list given for it alone, or under a committed ledger's, which is its parent's
    // for the batch's last ledger (it descends from the committed one); that verdict stands
    // once the quorum no longer holds the ledger's validations (a list fetched again keeps
    // those of the newest ledgers, of those not committed and of the batch's end, see adopt and
    // keep), and is withdrawn as
    // the quorum judges the ledger short of it (record). The list is kept for the ledger only
    // once it passes; a provisional ledger refused is forgotten, so the next batch ends at
    // another. Once the list has expired (or while it names no validators), none passes. A
    // pending list whose time has come is put in force first (rotate_due).
    bool confirms(Header const& last, NegativeUnl const& parent) {
        bool passed = false, judged = false;
        {
            std::lock_guard lock(mutex);
            judged = rotate_due();
            if (quorum && quorum->validating()) {
                auto hash = normalized(last.hash);
                if (quorum->validates(from_hex(hash), last.sequence, parent)) {
                    quorum->negative_unl(from_hex(hash), last.sequence, parent);
                    judge_again();
                    passed = judged = true;
                } else {
                    auto names = [&](std::map<std::uint32_t, std::string> const& ledgers) {
                        auto it = ledgers.find(last.sequence);
                        return it != ledgers.end() && it->second == hash;
                    };
                    passed = names(validated) || names(chain);
                    if (!passed && names(provisional)) provisional.erase(last.sequence);
                }
                // A validated ledger's close time, which xrpld judges the list's times against.
                if (passed) quorum->closed(last.close_time);
            }
            if (judged) ++changes;
        }
        if (judged) changed.notify_all();
        return passed;
    }
    // From peers, behind by more than a flag period: the negative UNL of the newest provisionally
    // validated ledger's parent, its NegativeUNL object read along a state-tree path checked
    // against the parent's header (named by the ledger's own), so the network's newest ledger is
    // judged exactly (it alone: another of its period may descend from another parent) and the
    // follower catches up through its parents' hashes.
    std::uint32_t learned = 0;
    void learn_negative_unl(PeerLedgers& peers) {
        std::uint32_t seq;
        std::string hash;
        {
            std::lock_guard lock(mutex);
            if (provisional.empty()) return;
            std::tie(seq, hash) = *provisional.rbegin();
            if (seq == learned || seq < 2 || (!validated.empty() && validated.rbegin()->first >= seq)) return;
        }
        auto header = peers.header(from_hex(hash), seq);
        auto parent = peers.header(from_hex(header.parent_hash), seq - 1);
        if (header.sequence != seq || parent.sequence != seq - 1) throw Error("Peer header chain skips a ledger");
        auto key = ::xrpl::keylet::negativeUNL().key;
        auto object = peers.objects(parent, {Bytes(reinterpret_cast<char const*>(key.data()), key.size())}).at(0);
        auto unl = negative_unl(object);
        {
            std::lock_guard lock(mutex);
            learned = seq;
            if (!quorum) return;
            quorum->negative_unl(from_hex(hash), seq, std::move(unl));
            judge_again();
            ++changes;
        }
        changed.notify_all();
    }
};
}
Json restore_spool(RestoreOptions const& options) {
    if (options.signing_key.empty()) throw Error("restore --to spool requires --signing-key (the spool's key)");
    // By default the state at the tip (or at the last ledger given); given a first ledger, the
    // snapshot before it and every ledger after (replayed: the spool holds them).
    auto tip = PieceFeed(options.source, options.publisher_key).last();
    auto from = options.first.value_or(options.last.value_or(tip));
    PieceFeed feed(options.source, options.publisher_key, from, options.last);
    safe_cache_path(options.output);
    // A spool already there is extended to the publication's tip, as a restore to a later
    // ledger extends an earlier one.
    if (fs::exists(options.output / "manifest.json")) return extend_spool(options);
    if (fs::exists(options.output) && !fs::is_empty(options.output)) throw Error(options.output.string() + " holds an unfinished spool; remove it and restore again");
    fs::create_directories(options.output);
    DirectoryLock lock(options.output);
    ExportOptions exporting;
    exporting.output = options.output; exporting.signing_key = options.signing_key;
    exporting.range = {feed.snapshot().sequence + 1, feed.last()};
    if (exporting.range.last < exporting.range.first) throw Error("The publication has no ledgers after its newest snapshot");
    auto replay = [&](PieceFeed& ledgers) {
        auto result = export_locked(exporting, nullptr, &ledgers);
        result["source"] = options.source; result["bytes_read"] = ledgers.bytes_read(); result["start"] = "replay";
        return result;
    };
    if (options.replay || options.first || exporting.range.last - exporting.range.first < 2) return replay(feed);
    // The state at the tip without replaying each ledger: the newest value of every object the
    // ledgers since the snapshot changed (empty: deleted), the headers checked as a chain. Those
    // values are held in memory, at most `fast_start_bytes` of them (about: each entry's node and
    // strings counted): late in a long interval they can come to several GB, and the spool is
    // then built by replaying the ledgers instead (slower, in bounded memory). Nothing has been
    // written to the spool by then.
    constexpr std::uint64_t fast_start_bytes = 1ull << 30;
    constexpr std::uint64_t entry_bytes = 128;  // a map node with its links and two strings, about
    auto started = std::chrono::steady_clock::now();
    std::map<Bytes, Bytes> newest;
    std::uint64_t held = 0;
    Header previous = feed.snapshot();
    LedgerRecord tip_ledger, record;
    std::uint64_t patches = 0;
    while (feed.next(record)) {
        auto header = decode_header(record.header);
        if (header.sequence != previous.sequence + 1 || header.parent_hash != previous.hash)
            throw Error("Published ledger " + std::to_string(header.sequence) + " does not follow its parent");
        if (record.sequence == exporting.range.last) { tip_ledger = std::move(record); break; }
        for (auto& patch : record.patches) {
            auto [entry, added] = newest.try_emplace(std::move(patch.key));
            held -= entry->second.size();
            held += patch.object.size() + (added ? entry->first.size() + entry_bytes : 0);
            entry->second = std::move(patch.object);
        }
        patches += record.patches.size();
        previous = header;
        if (held > fast_start_bytes) {
            std::map<Bytes, Bytes>().swap(newest);
            PieceFeed again(options.source, options.publisher_key, from, feed.last());
            exporting.range = {again.snapshot().sequence + 1, again.last()};
            auto result = replay(again);
            result["fast_start"] = {{"skipped_at", header.sequence}, {"reason", "the objects changed since the snapshot exceed " + std::to_string(fast_start_bytes >> 20) + " MiB"}};
            return result;
        }
    }
    if (tip_ledger.sequence != exporting.range.last) throw Error("The publication ends before its tip");
    auto scanned = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    // The ledger before the tip is the spool's base: the snapshot's objects merged in key order
    // with the newest values (the loader checks the result against its account hash); the tip is
    // then applied as any ledger is.
    Baseline baseline{previous, [&](std::function<void(ObjectRecord const&)> const& add) {
        auto changed = newest.begin();
        auto emit_changed_before = [&](Bytes const* key) {
            for (; changed != newest.end() && (!key || changed->first < *key); ++changed)
                if (!changed->second.empty()) add({changed->first, changed->second});
        };
        feed.snapshot_objects([&](ObjectRecord&& object) {
            emit_changed_before(&object.key);
            if (changed != newest.end() && changed->first == object.key) {
                if (!changed->second.empty()) add({changed->first, changed->second});
                ++changed;
            } else add(object);
        });
        emit_changed_before(nullptr);
    }};
    exporting.range = {exporting.range.last, exporting.range.last};
    bool served = false;
    auto result = export_locked(exporting, nullptr, nullptr, [&](LedgerRecord& out) {
        if (served) return false;
        served = true; out = std::move(tip_ledger); return true;
    }, nullptr, nullptr, &baseline);
    result["source"] = options.source; result["bytes_read"] = feed.bytes_read(); result["start"] = "state at the tip";
    result["scan"] = {{"ledgers", previous.sequence - feed.snapshot().sequence}, {"patches", patches}, {"objects_changed", newest.size()}, {"seconds", scanned}};
    return result;
}
Json extend_spool(RestoreOptions const& options) {
    if (options.signing_key.empty()) throw Error("Extending a spool requires its signing key");
    auto tip = Json::parse(read_file(options.output / "manifest.json", 64 << 20)).at("tip").at("seq").get<std::uint32_t>();
    if (PieceFeed(options.source, options.publisher_key).last() <= tip) return {{"extended", 0}, {"tip", tip}};
    PieceFeed feed(options.source, options.publisher_key, tip + 1);
    feed.skip_to(tip + 1);
    DirectoryLock lock(options.output);
    ExportOptions exporting;
    exporting.output = options.output; exporting.signing_key = options.signing_key; exporting.append = true;
    exporting.range = {tip + 1, feed.last()};
    auto result = export_locked(exporting, nullptr, &feed);
    result["extended"] = feed.last() - tip; result["source"] = options.source; result["bytes_read"] = feed.bytes_read();
    return result;
}
namespace {
// A spool's directory as given without a trailing separator ("spool/" is "spool"), so that its
// siblings (SPOOL.next, SPOOL.retired-TIME) are made beside it, never inside it.
fs::path spool_directory(fs::path const& dir) {
    auto spool = dir.lexically_normal();
    if (!spool.has_filename()) spool = spool.parent_path();
    if (spool.filename().empty() || spool.filename() == "." || spool.filename() == "..") throw Error("Not a spool directory: " + dir.string());
    return spool;
}
fs::path spool_sibling(fs::path const& spool, std::string const& suffix) { return spool.parent_path() / (spool.filename().string() + suffix); }
}
Json recover_spool(fs::path const& dir, fs::path const& signing_key) {
    auto spool = spool_directory(dir), next = spool_sibling(spool, ".next");
    if (fs::exists(spool / "manifest.json") || !fs::exists(next / "manifest.json")) return {{"recovered", false}};
    // rotate_spool renames the spool away only once SPOOL.next is complete and holds every
    // ledger the spool did, so SPOOL.next without a spool is the new spool, whole. Its signed
    // manifest is checked before it is put in place; a spool directory without a manifest (made
    // since, or a rebuild from the publication begun since and not finished) is replaced.
    auto manifest = read_manifest(Source(next.string()), from_hex(public_key(signing_key))).manifest;
    if (fs::exists(spool)) fs::remove_all(spool);
    durable_rename(next, spool);
    return {{"recovered", true}, {"spool", spool.string()}, {"tip", manifest.tip}};
}
Json rotate_spool(fs::path const& dir, fs::path const& signing_key, Mirrors const& source, fs::path const& publisher_key, bool apply,
                  std::function<void()> const& pause, std::optional<std::uint32_t> first) {
    auto spool = spool_directory(dir), next = spool_sibling(spool, ".next");
    if (apply) recover_spool(spool, signing_key);
    // Nothing else may write to the spool from the moment its last ledgers are carried over
    // until it is swapped: a batch committed meanwhile would name chunks only the retired spool
    // then holds. Without `pause` its lock is taken now (a follower writing to it has the
    // rotation refused); with it, once `pause` has stopped the writer.
    std::optional<DirectoryLock> held;
    auto hold = [&] {
        try { held.emplace(spool); }
        catch (DirectoryLocked const&) { throw Error("Spool " + spool.string() + " is in use (a follower writes to it): stop it to rotate the spool"); }
    };
    if (!fs::exists(spool / "manifest.json")) throw Error("No spool in " + spool.string());
    if (apply && !pause) hold();
    auto seed = signing_seed(signing_key);
    std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
    std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
    crypto_sign_seed_keypair(pk.data(), sk.data(), reinterpret_cast<unsigned char const*>(seed.data()));
    sodium_memzero(seed.data(), seed.size()); sodium_memzero(sk.data(), sk.size());
    auto old = read_manifest(Source(spool.string()), raw(pk)).manifest;
    auto current = Json::parse(read_file(spool / "manifest.json", 16 << 20)).at("snapshots").at(0).at("seq").get<std::uint32_t>();
    auto published = PieceFeed(source, publisher_key);
    auto newest = PieceFeed(source, publisher_key, published.last()).snapshot().sequence;
    // The new spool starts at the publication's tip, or at `first` if that is earlier; after
    // the publication's newest snapshot in either case, or the rotation is not due yet.
    auto start = std::min(first.value_or(published.last()), published.last());
    bool due = newest > current && start > newest;
    Json result{{"spool_snapshot", current}, {"publication_snapshot", newest}, {"start", start}, {"due", due}, {"rotated", false}};
    if (!due || !apply) return result;
    // SPOOL.next is one rotation's at a time (one run by hand, or the service's): the one
    // building there holds its lock, and another is refused rather than removing its work.
    std::optional<DirectoryLock> building;
    auto claim = [&] {
        try { building.emplace(next); }
        catch (DirectoryLocked const&) { throw Error(next.string() + " is in use (another rotation builds the new spool there)"); }
    };
    if (fs::exists(next)) { claim(); fs::remove_all(next); building.reset(); }
    // The new spool: the state at `start` (see restore_spool), then this spool's later ledgers,
    // verified again on the way in.
    RestoreOptions restore;
    restore.source = source; restore.publisher_key = publisher_key; restore.output = next; restore.signing_key = signing_key;
    restore.last = start;
    auto restored = restore_spool(restore);
    std::uint32_t tip = restored.at("range").at("last");
    // Held from restore_spool's end to the swap. Another rotation that took SPOOL.next between
    // the two (removing it, or building its own there) is found by its manifest: none, or
    // another tip.
    claim();
    if (!fs::exists(next / "manifest.json") || read_manifest(Source(next.string()), raw(pk)).manifest.tip != tip)
        throw Error(next.string() + " changed while it was built (another rotation?)");
    // Appends the old spool's committed ledgers after the new spool's tip.
    auto extend = [&] {
        auto committed = read_manifest(Source(spool.string()), raw(pk)).manifest;
        if (committed.tip <= tip) return;
        std::size_t chunk = 0;
        std::vector<LedgerRecord> pending;
        std::size_t at = 0;
        auto records = [&](LedgerRecord& out) {
            while (at == pending.size()) {
                if (chunk == committed.chunks.size()) return false;
                pending.clear(); at = 0;
                auto const& c = committed.chunks[chunk++];
                if (c.last <= tip) continue;
                checked(spool, c.file);
                read_chunk(spool / c.file.object, {}, [&](LedgerRecord&& record) { if (record.sequence > tip) pending.push_back(std::move(record)); });
            }
            out = std::move(pending[at++]);
            return true;
        };
        ExportOptions extending;
        extending.output = next; extending.signing_key = signing_key; extending.append = true; extending.merge_chunks = true;
        extending.range = {tip + 1, committed.tip};
        auto range = export_locked(extending, nullptr, nullptr, records).at("range");
        result["extended"].push_back(range);
        tip = committed.tip;
    };
    result["extended"] = Json::array();
    // Most of the old spool's later ledgers while its writer carries on; the last few once
    // `pause` has stopped it.
    extend();
    if (pause) { pause(); hold(); extend(); }
    // Each rename synced (their directory) before the next: a crash between them leaves
    // SPOOL.next, complete, and no spool, which recover_spool puts in place. Only that sync
    // separates them: a tail reading the spool meanwhile finds no manifest there, and tries
    // again (live_history.cpp's rotated_chunk).
    auto retired = spool_sibling(spool, ".retired-" + std::to_string(std::time(nullptr)));
    auto parent = fs::absolute(spool).parent_path();
#ifdef _WIN32
    building.reset();  // there the lock is a directory in SPOOL.next, which would move with it into the spool
#endif
    fs::rename(spool, retired);
    try {
        sync_file(parent);
        fs::rename(next, spool);
        sync_file(parent);
    } catch (...) {
        // Not left without a spool until the next start (a follower started meanwhile would
        // make an empty one): the swap is finished if it can be, else the old spool, which
        // nothing wrote to since, is put back; the error is reported either way.
        std::error_code ec;
        if (!fs::exists(spool / "manifest.json", ec)) {
            fs::rename(next, spool, ec);
            if (ec) fs::rename(retired, spool, ec);
            if (!ec) { try { sync_file(parent); } catch (std::exception const&) {} }
        }
        throw;
    }
    result["rotated"] = true; result["retired"] = retired.string(); result["tip"] = tip;
    return result;
}
Json publish_archive(PublishOptions const& options) {
    if (!options.interval_seconds || options.interval_seconds > 3600 || !options.batch_ledgers || options.batch_ledgers > 256)
        throw Error("Publish interval must be 1..3600 seconds and batch-ledgers 1..256");
    auto input = options.export_options;
    input.network_id = options.network_id;
    if (!input.jobs || input.jobs > 32) throw Error("RPC jobs must be between 1 and 32");
    DirectoryLock lock(input.output);
    auto seed = signing_seed(input.signing_key);
    std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
    std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
    crypto_sign_seed_keypair(pk.data(), sk.data(), reinterpret_cast<unsigned char const*>(seed.data()));
    sodium_memzero(seed.data(), seed.size()); sodium_memzero(sk.data(), sk.size());
    std::uint32_t committed = 0;
    if (fs::exists(input.output / "manifest.json")) {
        auto m = read_manifest(Source(input.output.string()), raw(pk)).manifest;
        if (m.version != 2 || m.chunks.empty()) throw Error("Publishing requires a v2 archive with ledger chunks");
        committed = m.tip;
        if (options.start && options.start != m.chunks.front().first) throw Error("--start differs from the existing archive start");
    } else if (options.start < 2) throw Error("A new publisher requires --start N (N >= 2)");
    if (options.until && options.until < (committed ? committed : options.start)) throw Error("--until precedes the archive range");
    auto old_int = std::signal(SIGINT, stop), old_term = std::signal(SIGTERM, stop);
    stopping = 0;
    struct Restore { decltype(old_int) a, b; ~Restore() { std::signal(SIGINT, a); std::signal(SIGTERM, b); } } signals{old_int, old_term};
    // The node (not when following peers): asked for its newest validated ledger, and the source
    // every batch reads, opened as a batch would open it.
    std::optional<NodeSource> node;
    auto open_node = [&] {
        if (node) return;
        node.emplace(input.source, input.grpc_source, fs::path{}, input.rpc_cache ? input.output / ".rpc-cache" : fs::path{}, input.delay_ms);
        node->history(input.history_peers, input.history_identity, input.network_id);
    };
    bool from_peers = !options.peers.empty();
    bool trusting_validators = from_peers || !options.validator_list.empty();
    std::shared_ptr<QuorumWatcher> watcher;
    // Validations and proposals relayed among this node's peers, outbound and inbound.
    std::shared_ptr<PeerRelay> relay;
    struct RelayTargets { std::atomic<PeerLedgers*> out{nullptr}; std::atomic<PeerOverlay*> in{nullptr}; };
    auto targets = std::make_shared<RelayTargets>();
    std::unique_ptr<PeerLedgers> peers;
    // Serving the peer network: the ledger server answers every session's requests; the
    // overlay accepts inbound peers and tells all of them each ledger served.
    std::shared_ptr<LedgerServer> server;
    std::unique_ptr<PeerOverlay> overlay;
    // The relay stops before the sessions it reaches go away.
    struct StopRelay { std::shared_ptr<PeerRelay>& relay; ~StopRelay() { if (relay) relay->stop(); } } stop_relay{relay};
    if (options.serve && !from_peers) throw Error("Serving the peer network requires --peers");
    if (options.serve) {
        server = std::make_shared<LedgerServer>(options.serve_ledgers);
        input.keep_ledgers = options.serve_ledgers;
    }
    if (from_peers) {
        if (!committed) throw Error("An archive read from peers extends an existing one; export its first ledgers from a node");
        // The network's own validator list unless one is given (vl.ripple.com on mainnet).
        auto network = network_defaults(options.network_id);
        if (!network && (options.validator_list.empty() || options.validator_list_key.empty()))
            throw Error("Network " + std::to_string(options.network_id) + " has no validator list known by name: give one ([validators] list and list_key)");
        auto list = options.validator_list.empty() ? network->list : options.validator_list;
        auto key = options.validator_list_key.empty() ? network->list_key : options.validator_list_key;
        watcher = QuorumWatcher::for_peers(list, key);
        PeerRelay::Options relay_options{options.relay, PeerRelay::policy(options.relay_validations), PeerRelay::policy(options.relay_proposals),
                                         options.squelch, options.squelch_sources};
        relay = std::make_shared<PeerRelay>(relay_options, [w = watcher.get()](Bytes const& key) { return w->trusted(key); },
                                            [targets](PeerRelay::Visit const& visit) {
                                                if (auto out = targets->out.load()) out->visit(visit);
                                                if (auto in = targets->in.load()) in->visit(visit);
                                            });
        PeerLedgers::Options peer_options;
        if (options.peers != std::vector<std::string>{"default"}) peer_options.peers = options.peers;
        peer_options.identity = options.peer_identity; peer_options.network_id = options.network_id;
        peer_options.connections = options.peer_connections;
        peer_options.relay = relay;
        peer_options.private_mode = options.peer_private;
        if (server) {
            peer_options.responder = server;
            peer_options.ledger_replay = true;
            peer_options.crawl = options.serve_crawl;
            peer_options.closed = [server]() -> std::optional<std::pair<Bytes, Bytes>> {
                auto newest = server->newest();
                if (!newest) return std::nullopt;
                return std::pair{from_hex(newest->hash), from_hex(newest->parent_hash)};
            };
            peer_options.connected = [server](PeerSession& session) { server->greet(session); };
        }
        peers = std::make_unique<PeerLedgers>(peer_options, [w = watcher.get()](Bytes const& v) { w->validation(v); },
                                              [w = watcher.get()](Bytes const& m) { w->manifest(m); });
        targets->out = peers.get();
        if (server) {
            overlay = std::make_unique<PeerOverlay>(PeerOverlay::Options{options.serve_listen, options.serve_max_inbound, options.serve_crawl, options.network_id,
                                                                         {options.serve_reserved.begin(), options.serve_reserved.end()},
                                                                         options.serve_ip_limit, options.peer_private, relay},
                                                    *peers, server, [w = watcher.get()](Bytes const& v) { w->validation(v); },
                                                    [w = watcher.get()](Bytes const& m) { w->manifest(m); });
            targets->in = overlay.get();
        }
    } else {
        if (trusting_validators && (options.validator_list_key.empty() || options.validations_node.empty()))
            throw Error("--validator-list needs --validator-list-key and --validations-node");
        if (trusting_validators) watcher = QuorumWatcher::start(options);
    }
    // From peers, near the tip: every newly validated ledger is fetched at once (PeerAhead), on a
    // thread of its own, so a fetch never waits for the batch before it.
    std::unique_ptr<PeerAhead> ahead;
    std::atomic<std::uint32_t> ahead_from{committed + 1};
    std::atomic<bool> ahead_stop{false};
    std::thread starter;
    struct StopStarter { std::atomic<bool>& stop; std::thread& thread; ~StopStarter() { stop = true; if (thread.joinable()) thread.join(); } } stop_starter{ahead_stop, starter};
    if (peers) {
        ahead = std::make_unique<PeerAhead>();
        starter = std::thread([&] {
            constexpr std::uint32_t near = 16;  // further behind, batches catch up as before
            while (!ahead_stop && !stopping) {
                auto seen = watcher->change_count();
                try {
                    // A ledger is fetched from its first trusted validations (or two peers
                    // announcing it), ahead of the quorum that lets it be committed. Should the
                    // leading hash change, or the quorum validate another, that one is fetched
                    // too; once a quorum has validated the ledger, only its hash is kept.
                    auto newest = watcher->newest();
                    auto from = ahead_from.load();
                    auto top = std::max(newest.value_or(0), watcher->newest_seen().value_or(0));
                    while (peers->announced(top + 1)) ++top;
                    if (top >= from && top - from < near)
                        for (auto seq = from; seq <= top && !ahead_stop; ++seq) {
                            if (newest && seq <= *newest) {
                                auto hash = watcher->hash_of(seq, *peers);
                                if (!hash) break;
                                ahead->settle(*peers, seq, *hash);
                                continue;
                            }
                            auto hash = watcher->early_hash(seq);
                            if (!hash)
                                if (auto heard = peers->announced(seq)) hash = to_hex(*heard);
                            if (!hash) break;
                            ahead->start(*peers, seq, *hash);
                        }
                } catch (std::exception const&) {}  // the batch fetches what this could not
                // Until the next validation counted (peers' announcements are looked at every 100 ms).
                watcher->wait_change(seen, std::chrono::milliseconds(100));
            }
        });
    }
    std::uint64_t added = 0, batches = 0;
    Json latest;
    Header tip{};  // the newest validated ledger, as last reported
    std::unique_ptr<LedgerVerifier> verifier;  // kept open from batch to batch
    // The committed ledger's negative UNL, from its verified state, before the first batch: the
    // ledgers after it are judged under it (the verifier is kept for that batch). Should the
    // archive not restore here, the first batch's commit check gives it instead.
    if (watcher && committed) {
        try {
            bool resumed = false;
            verifier = restore(input.output, read_manifest(Source(input.output.string()), raw(pk)).manifest, resumed, input.keep_ledgers);
            watcher->set_negative_unl(committed, negative_unl_in(*verifier));
        } catch (std::exception const& error) {
            verifier.reset();
            std::cerr << Json{{"event", "negative_unl_unread"}, {"ledger", committed}, {"error", error.what()}}.dump() << std::endl;
        }
    }
    // Before a batch commits, its last ledger is judged under the negative UNL of its parent's
    // verified state (QuorumWatcher::confirms).
    std::function<void(Header const&, Bytes const&)> confirm_last;
    if (watcher)
        confirm_last = [&](Header const& last, Bytes const& parent_negative_unl) {
            if (!watcher->confirms(last, negative_unl(parent_negative_unl)))
                throw Error("Ledger " + std::to_string(last.sequence) + " (" + last.hash + ") does not reach its validation quorum under its parent's negative UNL");
        };
    // Batches failing at the same ledger: reported once they keep failing, and when they pass.
    std::uint64_t stuck_at = 0, stuck_attempts = 0;
    auto stuck_since = std::chrono::steady_clock::now();
    // The ledgers served so far come from the archive (its newest, up to the window).
    auto serve = [&](std::vector<Header> const& headers) {
        if (!server || !verifier) return;
        server->node_store(verifier->node_store());
        if (!server->range() && committed) {
            auto m = read_manifest(Source(input.output.string()), raw(pk)).manifest;
            auto first = std::max<std::uint64_t>(m.chunks.front().first, m.tip >= options.serve_ledgers ? m.tip - options.serve_ledgers + 1 : 0);
            for (auto const& h : archive_headers(input.output, m, first, m.tip)) server->add(h);
        }
        for (auto const& h : headers) server->add(h);
        if (overlay) overlay->announce();
        // Serving statistics every five minutes.
        static auto reported = std::chrono::steady_clock::now();
        if (overlay && std::chrono::steady_clock::now() - reported >= std::chrono::minutes(5)) {
            reported = std::chrono::steady_clock::now();
            std::cerr << Json{{"event", "peer_serving"}, {"status", overlay->status()}}.dump() << std::endl;
        }
    };
    auto relay_reported = std::chrono::steady_clock::now();
    std::chrono::milliseconds retry_delay{0};  // after failed batches (below)
    // The newest ledger a quorum validated, and since when: validations that stop arriving (a
    // silent stream, peers no longer relaying them) leave the tip where it is, said once a minute.
    std::optional<std::uint32_t> newest_validated;
    auto newest_since = std::chrono::steady_clock::now(), stall_reported = newest_since;
    while (!stopping && (!options.until || committed < options.until)) {
        // Outbound peer (and relay) statistics every five minutes.
        if (peers && std::chrono::steady_clock::now() - relay_reported >= std::chrono::minutes(5)) {
            relay_reported = std::chrono::steady_clock::now();
            Json line{{"event", "peer_relay"}, {"outbound", peers->status()}};
            if (relay) line["status"] = relay->status();
            std::cerr << line.dump() << std::endl;
        }
        if (watcher) {
            auto newest = watcher->newest();
            auto now = std::chrono::steady_clock::now();
            if (newest != newest_validated) { newest_validated = newest; newest_since = now; }
            else if (now - newest_since >= std::chrono::minutes(1) && now - stall_reported >= std::chrono::minutes(1)) {
                stall_reported = now;
                std::cerr << Json{{"event", "validations_stalled"}, {"newest_validated", newest.value_or(0)}, {"committed", committed},
                                  {"seconds", std::chrono::duration<double>(now - newest_since).count()}, {"error", watcher->error()}}.dump() << std::endl;
            }
        }
        // Set once a batch ends before the known tip: the next one follows without a pause.
        bool behind = false;
        bool progressed = false, failed = false;  // a batch committed; one failed
        try {
            auto next = committed ? static_cast<std::uint64_t>(committed) + 1 : options.start;
            if (peers) {
                // More than a flag period behind, the newest ledgers' negative UNL comes from peers.
                try { watcher->learn_negative_unl(*peers); }
                catch (std::exception const& error) { std::cerr << Json{{"event", "negative_unl_retry"}, {"error", error.what()}}.dump() << std::endl; }
                // The network's tip is the newest ledger a quorum validated.
                tip.sequence = watcher->newest().value_or(committed);
                if (tip.sequence < committed) tip.sequence = committed;
            } else if (tip.sequence < next + options.batch_ledgers - 1) {
                // While catching up the source is asked only when a whole batch is not known
                // to be validated, so fetching ledgers keeps every connection to itself.
                open_node();
                tip = node->tip();
                if (tip.sequence < committed) throw Error("Source validated tip precedes the published archive");
                if (tip.sequence == committed && tip.hash != read_manifest(Source(input.output.string()), raw(pk)).manifest.tip_hash)
                    throw Error("Source validated tip conflicts with the published ledger hash");
            }
            auto end = std::min<std::uint64_t>(tip.sequence, next + options.batch_ledgers - 1);
            if (options.until) end = std::min<std::uint64_t>(end, options.until);
            input.expected_last_hash.clear();
            if (peers && next <= end && ahead->started(static_cast<std::uint32_t>(next))) {
                // Near the tip: the next ledger by the hash a quorum validated (waited for: a
                // fetch started on another hash, a ledger the validators did not agree on, is
                // abandoned and this one started), and those after it already fetched as a chain
                // from it, ending at a hash a quorum validated (else at the next ledger alone).
                auto first = static_cast<std::uint32_t>(next);
                if (auto hash = watcher->hash_of(first, *peers)) {
                    ahead->settle(*peers, first, *hash);
                    auto [last, last_hash] = ahead->ready_through(first, *hash, static_cast<std::uint32_t>(end));
                    if (last > first && watcher->hash_of(last, *peers) != last_hash) { last = first; last_hash = *hash; }
                    end = last; input.expected_last_hash = last_hash;
                } else end = next - 1;
            } else if (peers && next <= end) {
                // Behind the newest ledger seen validated, a batch may end anywhere: its hash comes
                // from the parent chain of a later ledger that a quorum validated.
                if (auto hash = watcher->hash_of(static_cast<std::uint32_t>(end), *peers)) input.expected_last_hash = *hash;
                else end = next - 1;
            } else if (watcher && next <= end) {
                // The source's "validated" flag is not trusted: end at a ledger validators signed.
                auto chosen = watcher->end_for(next, options.until ? std::min<std::uint64_t>(tip.sequence, options.until) : tip.sequence, options.batch_ledgers);
                if (!chosen) end = next - 1;  // nothing validated yet: wait
                else { end = chosen->first; input.expected_last_hash = chosen->second; }
            }
            if (next <= end) {
                input.range = {static_cast<std::uint32_t>(next), static_cast<std::uint32_t>(end)};
                input.append = true;
                // A verifier that must be reopened needs the server to let go of its store first.
                if (server && verifier && verifier->header().sequence != committed) server->node_store(nullptr);
                std::vector<Header> headers;
                if (!peers) open_node();
                latest = export_locked(input, peers.get(), nullptr, {}, &verifier, &headers, nullptr, ahead.get(), node ? &*node : nullptr, confirm_last);
                committed = input.range.last; added += end - next + 1; ++batches; progressed = true;
                // Committed: a list that cannot be read now is the next commit check's to give.
                if (watcher && verifier) {
                    try { watcher->set_negative_unl(committed, negative_unl_in(*verifier)); }
                    catch (std::exception const& error) { std::cerr << Json{{"event", "negative_unl_unread"}, {"ledger", committed}, {"error", error.what()}}.dump() << std::endl; }
                }
                if (ahead) { ahead_from = committed + 1; ahead->forget_through(committed); }
                if (stuck_attempts >= 10)
                    std::cerr << Json{{"event", "follower_recovered"}, {"ledger", stuck_at}, {"attempts", stuck_attempts},
                                      {"seconds", std::chrono::duration<double>(std::chrono::steady_clock::now() - stuck_since).count()}}.dump() << std::endl;
                stuck_attempts = 0;
                serve(headers);
                behind = end < tip.sequence;
                if (!options.once) std::cout << Json{{"event", "archive_published"}, {"result", latest}}.dump() << std::endl;
            }
        } catch (std::exception const& error) {
            if (options.once) { if (server) server->node_store(nullptr); throw; }
            // A crash/error after the manifest rename may have already committed.
            if (fs::exists(input.output / "manifest.json")) committed = read_manifest(Source(input.output.string()), raw(pk)).manifest.tip;
            // Fetches that failed (or ran ahead of a failure) are started again.
            if (ahead) { ahead_from = committed + 1; ahead->clear(); }
            std::cerr << Json{{"event", "publish_retry"}, {"error", error.what()}}.dump() << std::endl;
            // The same ledger failing again and again: said loudly, as it will not pass by itself.
            auto failing = committed ? static_cast<std::uint64_t>(committed) + 1 : options.start;
            if (failing != stuck_at) { stuck_at = failing; stuck_attempts = 0; stuck_since = std::chrono::steady_clock::now(); }
            if (++stuck_attempts == 10 || stuck_attempts % 100 == 0)
                std::cerr << Json{{"event", "follower_stuck"}, {"ledger", stuck_at}, {"attempts", stuck_attempts},
                                  {"seconds", std::chrono::duration<double>(std::chrono::steady_clock::now() - stuck_since).count()},
                                  {"error", error.what()}}.dump() << std::endl;
            // The failed batch rewound its verifier to the committed ledger and kept it open. One
            // it could not keep, or a third failure in a row at one ledger, and the store is
            // reopened from disk next time (released by the server first); once a ledger is
            // stuck, not again on every failure after (a restore from the archive takes long).
            if (!verifier || stuck_attempts == 3) {
                if (server) server->node_store(nullptr);
                verifier.reset();
            }
            failed = true;
        }
        if (options.once || (options.until && committed >= options.until)) break;
        if (behind) continue;
        if (failed) {
            // A failed batch is retried after a pause, not as soon as a quorum validates more:
            // from peers after 100 ms, then twice as long after each failure up to 8 s (a batch
            // fails most often on a peer that did not answer); from a node after its poll
            // interval, as before.
            retry_delay = std::min(retry_delay.count() ? retry_delay * 2 : std::chrono::milliseconds(100), std::chrono::milliseconds(8000));
            auto delay = peers ? retry_delay : std::chrono::milliseconds(options.interval_seconds * 1000ull);
            for (auto until = std::chrono::steady_clock::now() + delay; !stopping && std::chrono::steady_clock::now() < until;)
                std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(until - std::chrono::steady_clock::now(), std::chrono::milliseconds(100)));
            continue;
        }
        retry_delay = std::chrono::milliseconds(0);
        // Until the next poll, or a quorum validating a ledger past the committed one (past the
        // newest validated when this pass committed nothing, so a ledger not yet ready is not
        // asked for again at once). From a node, only for a follower that merges its chunks: one
        // that does not keeps a chunk per poll, so its manifest stays small.
        auto interval = std::chrono::milliseconds(options.interval_seconds * 1000ull);
        if (watcher && (peers || input.merge_chunks)) {
            // A minute at most, so a stall is reported while it lasts.
            auto past = progressed ? committed : std::max<std::uint32_t>(committed, watcher->newest().value_or(0));
            watcher->wait_validated_after(past, std::min(interval, std::chrono::milliseconds(60000)));
        } else {
            for (auto until = std::chrono::steady_clock::now() + interval; !stopping && std::chrono::steady_clock::now() < until;)
                std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(until - std::chrono::steady_clock::now(), std::chrono::milliseconds(100)));
        }
    }
    Json result{{"ledgers_published", added}, {"batches", batches}, {"tip", committed}, {"stopped", bool(stopping)}, {"last_batch", latest},
                {"trust", trusting_validators ? "validator-quorum" : "source-node"}};
    if (peers) result["peers"] = peers->status();
    if (overlay) result["serving"] = overlay->status();
    return result;
}
}
