// The restore's replay: ledgers applied to an in-memory hash tree (replay.hpp).
#include "replay.hpp"
#include "wire.hpp"
#include <xrpl/nodestore/NodeObject.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <thread>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
using Hash32 = std::array<char, 32>;
Hash32 hash32(Bytes const& bytes) {
    if (bytes.size() != 32) throw Error("Expected a 32-byte key");
    Hash32 out; std::memcpy(out.data(), bytes.data(), 32); return out;
}
// SHA-512 half: how every SHAMap node is named.
Hash32 half(Bytes const& data) {
    unsigned char digest[64];
    wire::sha512(data.data(), data.size(), digest);
    Hash32 out; std::memcpy(out.data(), digest, 32); return out;
}
// Keys in byte order (char may be signed).
bool before(Hash32 const& a, Hash32 const& b) { return std::memcmp(a.data(), b.data(), a.size()) < 0; }
unsigned nibble(Hash32 const& key, unsigned depth) {
    if (depth >= 64) throw Error("Duplicate key in a SHAMap");
    auto byte = static_cast<unsigned char>(key[depth / 2]);
    return depth % 2 ? byte & 15 : byte >> 4;
}
// A blob's length as the XRPL binary format prefixes it.
void add_vl(Bytes& out, Bytes const& blob) {
    auto n = blob.size();
    if (n <= 192) out += static_cast<char>(n);
    else if (n <= 12480) { n -= 193; out += static_cast<char>(193 + (n >> 8)); out += static_cast<char>(n & 0xff); }
    else if (n <= 918744) { n -= 12481; out += static_cast<char>(241 + (n >> 16)); out += static_cast<char>((n >> 8) & 0xff); out += static_cast<char>(n & 0xff); }
    else throw Error("Blob too long for the XRPL binary format");
    out += blob;
}
std::uint8_t const kAccountNode = static_cast<std::uint8_t>(r::NodeObjectType::AccountNode);
std::uint8_t const kTransactionNode = static_cast<std::uint8_t>(r::NodeObjectType::TransactionNode);

// The tree: an inner node holds its present children in branch order. A child is a leaf (its
// key and hash) or an inner node (its hash and the node). Every inner node but the root has
// at least two leaves below it, and every leaf sits as high as that allows: SHAMap's shape,
// which depends only on the keys.
struct Node;
struct Child {
    bool present = false;
    Hash32 hash{};
    Hash32 key{};                 // a leaf's
    std::unique_ptr<Node> inner;  // an inner node's
};
struct Node {
    std::uint16_t mask = 0;
    std::vector<Child> children;
};
struct Entry { Hash32 key, hash; };
// A state change: a leaf set (its new hash) or erased.
struct Op { Hash32 key, hash; bool erase = false; };

// Where built nodes go: the sink, while writing, with the tree's node type.
struct Writer {
    StateReplay::NodeSink const& sink;
    std::atomic<bool> const& writing;
    std::uint8_t type;
    void operator()(Hash32 const& hash, Bytes const& node) const {
        if (sink && writing.load(std::memory_order_relaxed)) sink(type, Bytes(hash.data(), hash.size()), node);
    }
};
Hash32 hash_inner(Node const& node, Writer const& write) {
    static char const zero[32] = {};
    Bytes serialized("MIN\0", 4);
    serialized.reserve(4 + 16 * 32);
    std::size_t i = 0;
    for (unsigned b = 0; b < 16; ++b)
        if (node.mask >> b & 1) serialized.append(node.children[i++].hash.data(), 32);
        else serialized.append(zero, 32);
    auto hash = half(serialized);
    write(hash, serialized);
    return hash;
}
Child build(unsigned depth, Entry const* first, Entry const* last, Writer const& write);
// Fills an inner node at `depth` with the subtrees of entries (sorted) sharing its prefix.
void fill(Node& node, unsigned depth, Entry const* first, Entry const* last, Writer const& write) {
    while (first != last) {
        auto b = nibble(first->key, depth);
        auto end = first;
        while (end != last && nibble(end->key, depth) == b) ++end;
        node.mask |= static_cast<std::uint16_t>(1u << b);
        node.children.push_back(build(depth + 1, first, end, write));
        first = end;
    }
}
// What sits at depth `depth` for entries (sorted, at least one) sharing its prefix.
Child build(unsigned depth, Entry const* first, Entry const* last, Writer const& write) {
    Child child;
    child.present = true;
    if (last - first == 1) { child.key = first->key; child.hash = first->hash; return child; }
    child.inner = std::make_unique<Node>();
    fill(*child.inner, depth, first, last, write);
    child.hash = hash_inner(*child.inner, write);
    return child;
}
// The same, the top `levels` levels' subtrees built on their own threads.
Child build_parallel(unsigned depth, Entry const* first, Entry const* last, Writer const& write, unsigned levels);
void fill_parallel(Node& node, unsigned depth, Entry const* first, Entry const* last, Writer const& write, unsigned levels) {
    std::vector<std::pair<unsigned, std::future<Child>>> parts;
    while (first != last) {
        auto b = nibble(first->key, depth);
        auto end = first;
        while (end != last && nibble(end->key, depth) == b) ++end;
        parts.emplace_back(b, std::async(std::launch::async, [=, &write] { return build_parallel(depth + 1, first, end, write, levels - 1); }));
        first = end;
    }
    for (auto& [b, part] : parts) { node.mask |= static_cast<std::uint16_t>(1u << b); node.children.push_back(part.get()); }
}
Child build_parallel(unsigned depth, Entry const* first, Entry const* last, Writer const& write, unsigned levels) {
    if (levels == 0 || last - first < 4096) return build(depth, first, last, write);
    Child child;
    child.present = true;
    child.inner = std::make_unique<Node>();
    fill_parallel(*child.inner, depth, first, last, write, levels);
    child.hash = hash_inner(*child.inner, write);
    return child;
}

void update(Child& slot, unsigned depth, Op const* first, Op const* last, Writer const& write);
// Applies changes (sorted, sharing the node's prefix) to an inner node's children.
void update_children(Node& node, unsigned depth, Op const* first, Op const* last, Writer const& write) {
    while (first != last) {
        auto b = nibble(first->key, depth);
        auto end = first;
        while (end != last && nibble(end->key, depth) == b) ++end;
        auto bit = static_cast<std::uint16_t>(1u << b);
        auto index = static_cast<std::size_t>(std::popcount(static_cast<unsigned>(node.mask & (bit - 1))));
        if (node.mask & bit) {
            update(node.children[index], depth + 1, first, end, write);
            if (!node.children[index].present) { node.children.erase(node.children.begin() + index); node.mask &= static_cast<std::uint16_t>(~bit); }
        } else {
            Child added;
            update(added, depth + 1, first, end, write);
            if (added.present) { node.children.insert(node.children.begin() + index, std::move(added)); node.mask |= bit; }
        }
        first = end;
    }
}
// Applies changes (sorted, sharing the prefix of depth `depth`) to what sits there, keeping
// SHAMap's shape: an inner node left with no leaves goes, one left with a single leaf gives
// way to it, and a leaf joined by others moves down into a new inner node.
void update(Child& slot, unsigned depth, Op const* first, Op const* last, Writer const& write) {
    if (slot.inner) {
        auto& node = *slot.inner;
        update_children(node, depth, first, last, write);
        if (node.children.empty()) { slot = Child{}; return; }
        if (node.children.size() == 1 && !node.children.front().inner) { Child leaf = std::move(node.children.front()); slot = std::move(leaf); return; }
        slot.hash = hash_inner(node, write);
        return;
    }
    // Nothing or a leaf here: the leaves here afterwards, rebuilt.
    std::vector<Entry> entries;
    bool kept = slot.present;
    for (auto op = first; op != last; ++op) {
        if (slot.present && op->key == slot.key) kept = false;
        else if (op->erase) throw Error("Patch deletes a missing object");
        if (!op->erase) entries.push_back({op->key, op->hash});
    }
    if (kept) {
        auto at = std::lower_bound(entries.begin(), entries.end(), slot.key, [](Entry const& e, Hash32 const& k) { return before(e.key, k); });
        entries.insert(at, Entry{slot.key, slot.hash});
    }
    if (entries.empty()) { slot = Child{}; return; }
    slot = build(depth, entries.data(), entries.data() + entries.size(), write);
}
Hash32 root_hash(Node const& root, Writer const& write) { return root.children.empty() ? Hash32{} : hash_inner(root, write); }

} // namespace

TaskPool::TaskPool(unsigned threads) {
    for (unsigned t = 0; t < std::max(1u, threads); ++t) threads_.emplace_back([this] { run(); });
}
TaskPool::~TaskPool() {
    { std::lock_guard lock(mutex_); stopping_ = true; }
    wake_.notify_all();
    for (auto& t : threads_) t.join();
}
TaskPool& TaskPool::shared() {
    static TaskPool pool(std::clamp(std::thread::hardware_concurrency(), 2u, 64u));
    return pool;
}
void TaskPool::post(std::function<void()> task) {
    { std::lock_guard lock(mutex_); tasks_.push_back(std::move(task)); }
    wake_.notify_one();
}
void TaskPool::run() {
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock lock(mutex_);
            wake_.wait(lock, [&] { return stopping_ || !tasks_.empty(); });
            if (tasks_.empty()) return;
            task = std::move(tasks_.front()); tasks_.pop_front();
        }
        try { task(); } catch (...) {}  // tasks report their own failures
    }
}

namespace {
using clock = std::chrono::steady_clock;
std::int64_t since(clock::time_point began) { return (clock::now() - began).count(); }
// The root's child under one first nibble, as the root's hash needs it.
struct Top { bool present = false; Hash32 hash{}; };
// A ledger in the pipeline.
struct InFlight {
    std::shared_ptr<LedgerRecord const> record;
    Header header;
    std::vector<Op> ops;                     // its state changes, in key order
    std::array<std::size_t, 17> bounds{};    // those under first nibble h: [bounds[h], bounds[h + 1])
    std::array<Top, 16> tops;                // the subtrees it changes, once changed
    bool prepared = false;
    std::atomic<int> pending{0};             // subtrees still to take it (set when it is dispatched)
};
// A ledger's transaction tree: its transactions checked, their leaves written, its root checked.
void verify_transactions(LedgerRecord const& ledger, Header const& header, Writer const& write) {
    auto const n = ledger.transactions.size();
    std::vector<Entry> entries(n);
    std::vector<bool> seen(n);
    for (std::size_t i = 0; i < n; ++i) {
        auto const& record = ledger.transactions[i];
        checks::transaction(record);
        auto id = hash32(wire::transaction_id(record.transaction));
        if (i && !before(entries[i - 1].key, id)) throw Error("Transactions must be sorted by unique hash");
        auto index = wire::top_level(record.metadata, 2, 28);  // UInt32 TransactionIndex
        if (!index || index->size() != 4) throw Error("Metadata has no TransactionIndex");
        auto const* v = reinterpret_cast<unsigned char const*>(index->data());
        std::uint32_t position = (std::uint32_t(v[0]) << 24) | (std::uint32_t(v[1]) << 16) | (std::uint32_t(v[2]) << 8) | v[3];
        if (position >= n || seen[position]) throw Error("Invalid transaction index permutation");
        seen[position] = true;
        Bytes node("SND\0", 4);
        add_vl(node, record.transaction);
        add_vl(node, record.metadata);
        node.append(id.data(), 32);
        entries[i] = {id, half(node)};
        write(entries[i].hash, node);
    }
    Node tree;
    fill(tree, 0, entries.data(), entries.data() + n, write);
    if (to_hex(std::string_view(root_hash(tree, write).data(), 32)) != header.transaction_hash) throw Error("Transaction SHAMap root mismatch");
}
} // namespace

struct StateReplay::Impl {
    NodeSink sink;
    std::atomic<bool> writing{true};
    TaskPool& pool = TaskPool::shared();
    // The snapshot while it loads: batches checked and hashed on their own threads.
    std::deque<std::future<std::vector<Entry>>> loading;
    std::vector<Entry> snapshot;
    Bytes last_key;
    bool ready = false;
    // The state: the root's child under each first nibble, each changed by one stream at a time.
    struct Stream { Child slot; std::deque<std::shared_ptr<InFlight>> queue; bool running = false; };
    std::array<Stream, 16> streams;
    std::array<Top, 16> tops;  // the root's children as of the last verified ledger (the checker's)
    // The pipeline, under `mutex`.
    mutable std::mutex mutex;
    std::condition_variable changed;
    std::map<std::uint32_t, std::shared_ptr<InFlight>> flight;
    std::uint32_t submitted = 0, dispatched = 0, verified = 0;
    Header last_submitted{}, current{};
    std::exception_ptr error;
    unsigned tasks = 0;  // posted to the pool and not finished
    bool stopping = false;
    std::thread checker;
    static constexpr std::size_t kInFlight = 1024;
    std::atomic<std::int64_t> prepare_ns{0}, stream_ns{0}, check_ns{0}, submit_wait_ns{0};
    std::uint64_t ledgers = 0, changes = 0;

    ~Impl() {
        { std::lock_guard lock(mutex); stopping = true; }
        changed.notify_all();
        if (checker.joinable()) checker.join();
        std::unique_lock lock(mutex);
        changed.wait(lock, [&] { return tasks == 0; });
    }
    Writer state_writer() const { return {sink, writing, kAccountNode}; }
    void collect_one() {
        auto entries = loading.front().get();
        loading.pop_front();
        snapshot.insert(snapshot.end(), entries.begin(), entries.end());
    }
    void fail(std::exception_ptr failure) {
        { std::lock_guard lock(mutex); if (!error) error = failure; }
        changed.notify_all();
    }
    // Runs work on the pool, counted so this replay outlives it (`mutex` held).
    void spawn(std::function<void()> work) {
        ++tasks;
        pool.post([this, work = std::move(work)] {
            try { work(); } catch (...) { fail(std::current_exception()); }
            std::lock_guard lock(mutex);
            --tasks;
            changed.notify_all();
        });
    }
    // A ledger's checks, transaction tree and new leaves, then its changes split by first nibble.
    void prepare(InFlight& f) {
        auto began = clock::now();
        auto const& ledger = *f.record;
        verify_transactions(ledger, f.header, Writer{sink, writing, kTransactionNode});
        auto write = state_writer();
        f.ops.resize(ledger.patches.size());
        for (std::size_t i = 0; i < ledger.patches.size(); ++i) {
            auto const& patch = ledger.patches[i];
            if (i && !(ledger.patches[i - 1].key < patch.key)) throw Error("Patch keys must be strictly increasing");
            auto& op = f.ops[i];
            op.key = hash32(patch.key);
            if (patch.object.empty()) { op.erase = true; continue; }
            checks::object(patch);
            auto node = Bytes("MLN\0", 4) + patch.object + patch.key;
            op.hash = half(node);
            write(op.hash, node);
        }
        std::size_t at = 0;
        for (unsigned b = 0; b < 16; ++b) {
            f.bounds[b] = at;
            while (at < f.ops.size() && nibble(f.ops[at].key, 0) == b) ++at;
        }
        f.bounds[16] = f.ops.size();
        prepare_ns += since(began);
    }
    // Hands prepared ledgers, in order, to the streams of the subtrees they change (`mutex` held).
    void dispatch() {
        for (;;) {
            auto it = flight.find(dispatched + 1);
            if (it == flight.end() || !it->second->prepared) return;
            auto f = it->second;
            int touched = 0;
            for (unsigned h = 0; h < 16; ++h) touched += f->bounds[h] != f->bounds[h + 1];
            f->pending = touched;
            for (unsigned h = 0; h < 16; ++h) {
                if (f->bounds[h] == f->bounds[h + 1]) continue;
                auto& stream = streams[h];
                stream.queue.push_back(f);
                if (!stream.running) { stream.running = true; spawn([this, h] { run_stream(h); }); }
            }
            dispatched = f->header.sequence;
            if (!touched) changed.notify_all();
        }
    }
    // One subtree taking its changes, ledger after ledger, for as long as it has any queued.
    void run_stream(unsigned h) {
        auto write = state_writer();
        auto& stream = streams[h];
        for (;;) {
            std::shared_ptr<InFlight> f;
            {
                std::lock_guard lock(mutex);
                if (stream.queue.empty() || error || stopping) { stream.running = false; return; }
                f = std::move(stream.queue.front());
                stream.queue.pop_front();
            }
            auto began = clock::now();
            update(stream.slot, 1, f->ops.data() + f->bounds[h], f->ops.data() + f->bounds[h + 1], write);
            f->tops[h] = {stream.slot.present, stream.slot.hash};
            stream_ns += since(began);
            if (f->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) { std::lock_guard lock(mutex); changed.notify_all(); }
        }
    }
    Hash32 root(Writer const& write) const {
        if (std::none_of(tops.begin(), tops.end(), [](Top const& t) { return t.present; })) return {};
        static char const zero[32] = {};
        Bytes serialized("MIN\0", 4);
        for (auto const& t : tops) serialized.append(t.present ? t.hash.data() : zero, 32);
        auto hash = half(serialized);
        write(hash, serialized);
        return hash;
    }
    // Checks each ledger's state root, in order, once every subtree it changes has taken it.
    void check_loop() {
        auto write = state_writer();
        for (;;) {
            std::shared_ptr<InFlight> f;
            {
                std::unique_lock lock(mutex);
                changed.wait(lock, [&] {
                    if (stopping || error) return true;
                    auto it = flight.find(verified + 1);
                    return it != flight.end() && dispatched >= it->first && it->second->pending.load(std::memory_order_acquire) == 0;
                });
                if (stopping || error) return;
                f = flight.at(verified + 1);
            }
            auto began = clock::now();
            for (unsigned h = 0; h < 16; ++h)
                if (f->bounds[h] != f->bounds[h + 1]) tops[h] = f->tops[h];
            if (to_hex(std::string_view(root(write).data(), 32)) != f->header.account_hash) {
                fail(std::make_exception_ptr(Error("Patched state SHAMap root mismatch at ledger " + std::to_string(f->header.sequence))));
                return;
            }
            check_ns += since(began);
            {
                std::lock_guard lock(mutex);
                verified = f->header.sequence;
                current = f->header;
                ++ledgers; changes += f->ops.size();
                flight.erase(verified);
            }
            changed.notify_all();
        }
    }
};

StateReplay::StateReplay(NodeSink sink) : impl_(std::make_unique<Impl>()) { impl_->sink = std::move(sink); }
StateReplay::~StateReplay() = default;
void StateReplay::writing(bool on) { impl_->writing = on; }
Header StateReplay::header() const { std::lock_guard lock(impl_->mutex); return impl_->current; }

void StateReplay::add_snapshot_objects(std::vector<ObjectRecord>&& items) {
    auto& i = *impl_;
    if (i.ready) throw Error("Snapshot is already sealed");
    if (items.empty()) return;
    for (std::size_t n = 0; n < items.size(); ++n) {
        auto const& previous = n ? items[n - 1].key : i.last_key;
        if (!previous.empty() && items[n].key <= previous) throw Error("Snapshot keys must be strictly increasing across parts");
    }
    i.last_key = items.back().key;
    // Each batch is checked, and its leaves hashed and written, on a thread of its own.
    i.loading.push_back(std::async(std::launch::async, [&i, items = std::move(items)] {
        Writer write = i.state_writer();
        std::vector<Entry> entries;
        entries.reserve(items.size());
        for (auto const& item : items) {
            checks::object(item);
            auto key = hash32(item.key);
            auto node = Bytes("MLN\0", 4) + item.object + item.key;
            entries.push_back({key, half(node)});
            write(entries.back().hash, node);
        }
        return entries;
    }));
    while (i.loading.size() > 2 * std::thread::hardware_concurrency()) i.collect_one();
}
Header StateReplay::finish_snapshot(Bytes const& header, std::uint32_t sequence, std::string const& account_hash) {
    auto& i = *impl_;
    if (i.ready) throw Error("Snapshot is already sealed");
    auto h = decode_header(header);
    if (h.sequence != sequence || h.account_hash != account_hash) throw Error("Snapshot state root mismatch");
    while (!i.loading.empty()) i.collect_one();
    // The tree's inner nodes: the 256 subtrees under each first byte on their own threads.
    auto write = i.state_writer();
    Node root;
    fill_parallel(root, 0, i.snapshot.data(), i.snapshot.data() + i.snapshot.size(), write, 2);
    std::vector<Entry>().swap(i.snapshot);
    if (to_hex(std::string_view(root_hash(root, write).data(), 32)) != h.account_hash) throw Error("Snapshot state root mismatch");
    // Each first nibble's subtree goes to its stream.
    for (unsigned b = 0, n = 0; b < 16; ++b)
        if (root.mask >> b & 1) {
            i.streams[b].slot = std::move(root.children[n++]);
            i.tops[b] = {true, i.streams[b].slot.hash};
        }
    std::lock_guard lock(i.mutex);
    i.current = i.last_submitted = h;
    i.submitted = i.dispatched = i.verified = h.sequence;
    i.ready = true;
    return h;
}

void StateReplay::submit(std::shared_ptr<LedgerRecord const> record) {
    auto& i = *impl_;
    auto h = decode_header(record->header);
    auto began = clock::now();
    std::unique_lock lock(i.mutex);
    if (i.error) std::rethrow_exception(i.error);
    if (!i.ready) throw Error("A verified snapshot is required");
    if (h.sequence != record->sequence || static_cast<std::uint64_t>(i.submitted) + 1 != h.sequence || h.parent_hash != i.last_submitted.hash)
        throw Error("Ledger parent chain or sequence mismatch");
    i.changed.wait(lock, [&] { return i.error || i.flight.size() < Impl::kInFlight; });
    if (i.error) std::rethrow_exception(i.error);
    auto f = std::make_shared<InFlight>();
    f->record = std::move(record);
    f->header = h;
    i.flight.emplace(h.sequence, f);
    i.submitted = h.sequence;
    i.last_submitted = h;
    if (!i.checker.joinable()) i.checker = std::thread([&i] { i.check_loop(); });
    i.spawn([&i, f] {
        i.prepare(*f);
        std::lock_guard held(i.mutex);
        f->prepared = true;
        i.dispatch();
    });
    i.submit_wait_ns += since(began);
}
void StateReplay::wait(std::uint32_t sequence) {
    auto& i = *impl_;
    std::unique_lock lock(i.mutex);
    i.changed.wait(lock, [&] { return i.error || i.verified >= sequence; });
    if (i.error) std::rethrow_exception(i.error);
}
bool StateReplay::verified(std::uint32_t sequence) const {
    auto& i = *impl_;
    std::lock_guard lock(i.mutex);
    if (i.error) std::rethrow_exception(i.error);
    return i.verified >= sequence;
}
void StateReplay::apply(LedgerRecord const& ledger) {
    submit(std::make_shared<LedgerRecord const>(ledger));
    wait(ledger.sequence);
}
void StateReplay::apply_transactions(LedgerRecord const& ledger) {
    auto& i = *impl_;
    std::uint32_t last;
    {
        std::lock_guard lock(i.mutex);
        if (!i.ready) throw Error("A verified snapshot is required");
        last = i.submitted;
    }
    wait(last);
    // The record must be the verified ledger itself: its transaction tree is checked against the
    // record's own header, so a record of another ledger would pass as this one's otherwise.
    auto h = decode_header(ledger.header);
    auto current = header();
    if (h.sequence != ledger.sequence || h.sequence != current.sequence || h.hash != current.hash)
        throw Error("Transactions are not the verified ledger's");
    verify_transactions(ledger, h, Writer{i.sink, i.writing, kTransactionNode});
}
Json StateReplay::timings() const {
    auto const& i = *impl_;
    auto s = [](std::int64_t ns) { return ns / 1e9; };
    std::lock_guard lock(i.mutex);
    return {{"prepare", s(i.prepare_ns)}, {"state_updates", s(i.stream_ns)}, {"root_checks", s(i.check_ns)},
            {"submit_waiting", s(i.submit_wait_ns)}, {"ledgers", i.ledgers}, {"changes", i.changes}};
}
}
