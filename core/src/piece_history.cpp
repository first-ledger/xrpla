// History over a piece cache: containers are memory-mapped and decoded one
// 64-ledger page at a time; state at any cached ledger is the newest object
// version at or before it (from the query index) or else the base snapshot.
#include "piece_history.hpp"
#include "piece_format.hpp"
#include "rpc.hpp"  // rpc::fail: a key walk past its budget answers tooBusy
#include <xrpl/protocol/AccountID.h>
#include <xrpl/protocol/nft.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace xrpla::core {
namespace r = ::xrpl;
using namespace pieces;
namespace {
// The threads a request's loads run on (segment pages, snapshot blocks, object reads), shared by
// every request: one per core, at most 16. The asking thread takes tasks too, so a request never
// waits on a busy pool and loads asked for from a pool thread still finish. Starting threads for
// each request instead made a busy server spend a third of its CPU creating them, serialized in
// the kernel.
class LoadPool {
    struct Batch {
        // The caller's tasks: read only while some are left to hand out (the caller waits for
        // them all, so they outlive that); `count` is what a late pool thread sees.
        std::vector<std::function<void()>> const* tasks;
        std::size_t count, helpers;                 // pool threads that may join, besides the caller
        std::atomic<std::size_t> next{0}, done{0}, joined{0};
        std::mutex mutex;
        std::condition_variable finished;
        std::exception_ptr failure;
        // Runs tasks until none is left to take.
        void work() {
            for (std::size_t i; (i = next++) < count;) {
                try { (*tasks)[i](); }
                catch (...) { std::lock_guard lock(mutex); if (!failure) failure = std::current_exception(); }
                if (++done == count) { std::lock_guard lock(mutex); finished.notify_all(); }
            }
        }
    };
    std::mutex mutex_;
    std::condition_variable waiting_;
    std::deque<std::shared_ptr<Batch>> batches_;
    std::vector<std::thread> threads_;
    bool stopping_ = false;
    void run() {
        for (;;) {
            std::shared_ptr<Batch> batch;
            {
                std::unique_lock lock(mutex_);
                waiting_.wait(lock, [&] { return stopping_ || !batches_.empty(); });
                if (stopping_) return;
                batch = batches_.front();
                // A batch leaves the queue once all its tasks are handed out, or enough threads joined.
                if (batch->next >= batch->count || ++batch->joined >= batch->helpers) batches_.pop_front();
            }
            batch->work();
        }
    }
public:
    explicit LoadPool(unsigned threads) { for (unsigned i = 0; i < threads; ++i) threads_.emplace_back([this] { run(); }); }
    ~LoadPool() {
        { std::lock_guard lock(mutex_); stopping_ = true; }
        waiting_.notify_all();
        for (auto& thread : threads_) thread.join();
    }
    static LoadPool& shared() {
        static LoadPool pool(std::clamp(std::thread::hardware_concurrency(), 2u, 16u));
        return pool;
    }
    // Runs `tasks` on the caller and up to `most` - 1 pool threads; returns the first failure.
    std::exception_ptr run(std::vector<std::function<void()>> const& tasks, std::size_t most) {
        if (tasks.empty()) return nullptr;
        auto batch = std::make_shared<Batch>();
        batch->tasks = &tasks; batch->count = tasks.size();
        batch->helpers = std::min({most, tasks.size(), threads_.size() + 1}) - 1;
        if (batch->helpers) {
            { std::lock_guard lock(mutex_); batches_.push_back(batch); }
            for (std::size_t i = 0; i < batch->helpers; ++i) waiting_.notify_one();
        }
        batch->work();
        std::unique_lock lock(batch->mutex);
        batch->finished.wait(lock, [&] { return batch->done == batch->count; });
        return batch->failure;
    }
};
// Memory shared by several caches. Each cache has a guaranteed share and may grow past it into
// room the others leave unused; while the total is over budget, a cache over its share evicts
// down to it (on its next insert or lookup), so a cache under its share gets its room back.
// One budget serves every PieceHistory of a cache directory (shared_budget): a sync that switches
// the selection has a new history opened while the one it replaces still answers the requests
// that hold it (and a live tail's, until follow_tail has rebuilt it). The replaced one is retired:
// its caches are emptied and their shares set to nothing, so what it caches meanwhile is evicted
// first whenever the total is over, and the two together stay within one --cache, not two.
// A retired cache trims itself only when it is used, so the budget also lists the retired caches:
// any of its caches finding the total over sheds theirs first (Retired::shed), then its own.
// Evicted values, let go once the locks are: a decoded page or segment frees its memory then.
using Freed = std::vector<std::shared_ptr<void const>>;
struct Retired {
    // Evicts entries while the budget is over, into `freed`; skips this cache if its lock is held
    // elsewhere.
    virtual void shed(Freed& freed) = 0;
    virtual ~Retired() = default;
};
// Lock order: a cache's own lock, then the budget's `mutex`, then a retired cache's lock taken
// only by try_lock (shed), so no order among caches can deadlock. A cache never holds its own lock
// while it takes the budget's to enlist or leave (Lru::retire, ~Lru), and a retired cache never
// sheds the others: a cache's lock is never asked for, blocking, under the budget's.
struct Budget {
    std::atomic<std::size_t> total{0}, used{0};
    std::mutex mutex;
    std::vector<Retired*> retired;  // under `mutex`
    // What the retired caches hold, read without `mutex`: nothing to shed is the common case, a
    // retired cache being emptied when it retires and again whenever the total is over.
    std::atomic<std::size_t> retired_bytes{0};
    Json stats() const { return {{"total", total.load()}, {"used", used.load()}}; }
    void enlist(Retired* cache) { std::lock_guard lock(mutex); retired.push_back(cache); }
    void leave(Retired* cache) { std::lock_guard lock(mutex); std::erase(retired, cache); }
    // The retired caches' entries, oldest retired first, while the total is over.
    void shed(Freed& freed) {
        if (!retired_bytes || used <= total) return;
        std::lock_guard lock(mutex);
        for (auto* cache : retired) { if (used <= total) break; cache->shed(freed); }
    }
};
// The budget of the histories open on `cache`, created by the first. Kept while one holds it:
// the map's weak pointers to budgets no history holds any more are dropped on the next call.
std::shared_ptr<Budget> shared_budget(fs::path const& cache) {
    static std::mutex mutex;
    static std::map<std::string, std::weak_ptr<Budget>> budgets;
    std::error_code ec;
    auto canonical = fs::weakly_canonical(cache, ec);  // the same directory however it is named
    auto name = (ec ? cache : canonical).string();
    std::lock_guard lock(mutex);
    std::erase_if(budgets, [](auto const& item) { return item.second.expired(); });
    auto& slot = budgets[name];
    auto budget = slot.lock();
    if (!budget) slot = budget = std::make_shared<Budget>();
    return budget;
}
template<class K, class T> class Lru final : Retired {
    using Item = std::pair<K, std::shared_ptr<T>>;
    std::mutex mutex_;
    std::list<Item> order_;
    std::map<K, typename std::list<Item>::iterator> index_;
    // Loads under way: a request for one waits for it rather than loading it again (a cold
    // segment's dictionary takes ~80 ms, and concurrent requests often want the same one).
    std::map<K, std::shared_future<std::shared_ptr<T>>> loading_;
    std::size_t bytes_ = 0, capacity_;
    std::size_t (*size_)(T const&);
    Budget* budget_ = nullptr;
    bool retired_ = false;  // enlisted in budget_'s retired caches (and counted in its retired_bytes)
    void charge(T const& value) {
        auto n = size_(value); bytes_ += n;
        if (budget_) { budget_->used += n; if (retired_) budget_->retired_bytes += n; }
    }
    void release(T const& value) {
        auto n = size_(value); bytes_ -= n;
        if (budget_) { budget_->used -= n; if (retired_) budget_->retired_bytes -= n; }
    }
    // The caller declares `freed` before taking mutex_, so what is evicted is freed after it is let go.
    void evict_oldest(Freed& freed) {
        release(*order_.back().second);
        freed.push_back(std::move(order_.back().second));
        index_.erase(order_.back().first); order_.pop_back();
    }
    // Over its share and the budget full (or, alone, over its capacity); keeps the newest entry.
    // With the budget full, the retired caches give back theirs first (the caller holds mutex_; a
    // retired cache does not shed the others: Budget's lock order).
    void trim(Freed& freed) {
        if (budget_ && !retired_) budget_->shed(freed);
        auto over = [&] { return bytes_ > capacity_ && (!budget_ || budget_->used > budget_->total); };
        while (order_.size() > 1 && over()) evict_oldest(freed);
    }
    // Retired::shed, from another cache of the budget (under the budget's lock): every entry may
    // go, as from retire(); a request holding one keeps it until it is done.
    void shed(Freed& freed) override {
        std::unique_lock lock(mutex_, std::try_to_lock);
        if (!lock) return;  // in use: it trims itself meanwhile, and the next shed tries it again
        while (!order_.empty() && budget_->used > budget_->total) evict_oldest(freed);
    }
public:
    U hits = 0, misses = 0;
    Lru(std::size_t capacity, std::size_t (*size)(T const&)) : capacity_(capacity), size_(size) {}
    // A shared budget outlives this cache (its history holds it): what this one charged is given
    // back, or a reopened history would find it full of entries that no longer exist. A retired
    // one leaves the budget's list first, so no other cache sheds it from here on.
    ~Lru() override {
        if (!budget_) return;
        if (retired_) { budget_->leave(this); budget_->retired_bytes -= bytes_; }
        budget_->used -= bytes_;
    }
    Lru(Lru const&) = delete;
    Lru& operator=(Lru const&) = delete;
    // The capacity, or with a shared budget the guaranteed share.
    void capacity(std::size_t value) { std::lock_guard lock(mutex_); capacity_ = value; }
    std::size_t capacity() { std::lock_guard lock(mutex_); return capacity_; }
    void share(Budget& budget) { std::lock_guard lock(mutex_); budget_ = &budget; budget.used += bytes_; }
    // Every entry evicted and no share kept: from now on this cache holds only room the shared
    // budget has left, and is the first to evict once it is over (it is always over its share).
    // A request holding an entry keeps it until it is done; a load under way still inserts its
    // value, as any insert, then trims. Hits and misses are kept. With a shared budget it is then
    // enlisted, so the budget's other caches evict what it caches later before their own entries
    // (taken after its own lock is let go: Budget's lock order).
    void retire() {
        bool enlist = false;
        {
            Freed freed;
            std::lock_guard lock(mutex_);
            capacity_ = 0;
            while (!order_.empty()) evict_oldest(freed);
            enlist = budget_ && !retired_;
            if (enlist) retired_ = true;
        }
        if (enlist) budget_->enlist(this);
    }
    template<class Load> std::shared_ptr<T> get(K const& key, Load load) {
        std::promise<std::shared_ptr<T>> loaded;
        std::shared_future<std::shared_ptr<T>> waiting;
        // Each locked block frees what it evicted as it lets go of the lock, not after the wait or
        // the load that follows.
        {
            Freed freed;
            std::lock_guard lock(mutex_);
            trim(freed);
            if (auto it = index_.find(key); it != index_.end()) { order_.splice(order_.begin(), order_, it->second); ++hits; return it->second->second; }
            if (auto it = loading_.find(key); it != loading_.end()) waiting = it->second;
            else { ++misses; loading_.emplace(key, loaded.get_future().share()); }
        }
        if (waiting.valid()) return waiting.get();  // the loader's value, or its failure
        std::shared_ptr<T> value;
        try { value = load(); }
        catch (...) {
            { std::lock_guard lock(mutex_); loading_.erase(key); }
            loaded.set_exception(std::current_exception());
            throw;
        }
        {
            Freed freed;
            std::lock_guard lock(mutex_);
            loading_.erase(key);
            if (auto it = index_.find(key); it != index_.end()) value = it->second->second;
            else { order_.emplace_front(key, value); index_[key] = order_.begin(); charge(*value); trim(freed); }
        }
        loaded.set_value(value);
        return value;
    }
    bool contains(K const& key) { std::lock_guard lock(mutex_); return index_.contains(key); }
    // Inserts or replaces (a longer decode of a partial page).
    void put(K const& key, std::shared_ptr<T> value) {
        Freed freed;
        std::lock_guard lock(mutex_);
        ++misses;
        if (auto it = index_.find(key); it != index_.end()) {
            release(*it->second->second); freed.push_back(std::move(it->second->second)); order_.erase(it->second); index_.erase(it);
        }
        order_.emplace_front(key, value); index_[key] = order_.begin(); charge(*value);
        trim(freed);
    }
    // `value`'s charge again after `update` changed what it holds, if it is still the entry for
    // `key` (an evicted one is no longer charged).
    template<class Update> void recharge(K const& key, T& value, Update update) {
        Freed freed;
        std::lock_guard lock(mutex_);
        auto it = index_.find(key);
        if (it == index_.end() || it->second->second.get() != &value) return;
        release(value); update(value); charge(value);
        trim(freed);
    }
    // Without counting a hit or refreshing the entry: for planning which loads are needed.
    std::shared_ptr<T> peek(K const& key) {
        std::lock_guard lock(mutex_);
        auto it = index_.find(key);
        return it == index_.end() ? nullptr : it->second->second;
    }
    std::shared_ptr<T> find(K const& key) {
        Freed freed;
        std::lock_guard lock(mutex_);
        trim(freed);
        auto it = index_.find(key);
        if (it == index_.end()) return nullptr;
        order_.splice(order_.begin(), order_, it->second); ++hits;
        return it->second->second;
    }
    Json stats() { std::lock_guard lock(mutex_); return {{"entries", order_.size()}, {"bytes", bytes_}, {"share", capacity_}, {"hits", hits}, {"misses", misses}}; }
    // `visit(value)` for each entry, outside the cache's lock (the entries are held meanwhile).
    template<class Visit> void each(Visit visit) {
        std::vector<std::shared_ptr<T>> values;
        { std::lock_guard lock(mutex_); for (auto const& item : order_) values.push_back(item.second); }
        for (auto const& value : values) visit(*value);
    }
};

struct OpenArchive {
    std::mutex mutex;
    hx::Archive archive;
    // Memory charged to the open-segment cache (held), measured again as it grows (under
    // the cache's lock and this mutex); read by cache_stats under this mutex alone.
    std::atomic<std::size_t> weight = 0;
    // Set while what it holds may differ from `weight`: by a read that grew it, as it lets go of
    // `mutex` (Hold; one that throws too), and by a measure that found `mutex` held (Impl::grew).
    // Cleared when it is measured; the next lookup of a stale segment measures it (Impl::archive).
    std::atomic<bool> stale = false;
    explicit OpenArchive(V bytes) : archive(bytes, true) {}
};
// What an open segment is charged: what it holds, and room for a transaction page being read.
std::size_t held(OpenArchive const& a) { return a.archive.heldBytes() + (std::size_t(1) << 20); }
// A read's hold of an open segment, which may grow it: one that did leaves it stale. Concurrent
// lookups of a hot segment whose reads grow nothing then measure nothing (Impl::archive, Impl::grew).
class Hold {
    OpenArchive& a_;
    std::unique_lock<std::mutex> lock_;
public:
    explicit Hold(OpenArchive& a) : a_(a), lock_(a.mutex) {}
    ~Hold() { if (held(a_) != a_.weight) a_.stale = true; }
    Hold(Hold const&) = delete;
    Hold& operator=(Hold const&) = delete;
};
Hold hold(OpenArchive& a) { return Hold(a); }
struct Ledger {
    U seq = 0;
    V header;
    std::vector<std::pair<V, V>> transactions, patches;
};
// A decoded ledger: its record, and the record parsed (views into it).
struct Page {
    Bytes raw;
    Ledger ledger;
    std::size_t bytes = 0;
    Ledger const& at(U seq) const {
        if (ledger.seq != seq) throw Error("Decoded ledger differs");
        return ledger;
    }
};

struct Range {
    U first = 0, last = 0;
    Bytes sha;
    std::unique_ptr<Mapped> file;
    Container directory;
    TxIndex tx;
    std::unique_ptr<QueryIndex> query;
    std::unique_ptr<OptionalIndex> holders, issuers;  // absent when turned off or not built yet
    std::unique_ptr<SeekIndex> seek;  // absent until index-pieces builds it: lists are read from their start
};
// A k-way merge of sorted runs of fixed-width entries: the entries after `after` (from the
// start when empty), each once, in order, at most `limit`.
std::vector<Bytes> merge_after(std::vector<V> runs, std::size_t width, V after, unsigned limit) {
    for (auto& run : runs) {
        std::size_t lo = 0, hi = run.size() / width;
        if (!after.empty())
            while (lo < hi) { auto mid = (lo + hi) / 2; if (run.substr(mid * width, width) <= after) lo = mid + 1; else hi = mid; }
        run.remove_prefix(lo * width);
    }
    std::vector<Bytes> out;
    while (out.size() < limit) {
        std::optional<V> least;
        for (auto const& run : runs) if (!run.empty() && (!least || run.substr(0, width) < *least)) least = run.substr(0, width);
        if (!least) break;
        auto entry = Bytes(*least);
        for (auto& run : runs) if (!run.empty() && run.substr(0, width) == V(entry)) run.remove_prefix(width);
        out.push_back(std::move(entry));
    }
    return out;
}
std::size_t archive_bytes(OpenArchive const& a) { return a.weight; }
std::size_t page_bytes(Page const& p) { return p.bytes; }
// What a decoded ledger holds: its record's buffer and the views parsed from it.
std::size_t page_charge(Page const& p) {
    return sizeof(Page) + p.raw.capacity() + (p.ledger.transactions.capacity() + p.ledger.patches.capacity()) * sizeof(std::pair<V, V>);
}
std::size_t block_bytes(StateBlock const& b) { return sizeof(StateBlock) + b.raw.capacity() + b.items.capacity() * sizeof(std::pair<V, V>); }

Ledger parse_ledger(V raw) {
    hx::Reader r{raw};
    Ledger l;
    l.seq = r.little(4);
    l.header = r.take(r.little(4));
    U n = r.little(4);
    l.transactions.reserve(std::min<U>(n, raw.size() / 8));
    for (U i = 0; i < n; ++i) { auto tx = r.take(r.little(4)); auto meta = r.take(r.little(4)); l.transactions.emplace_back(tx, meta); }
    n = r.little(4);
    l.patches.reserve(std::min<U>(n, raw.size() / 36));
    for (U i = 0; i < n; ++i) { auto key = r.take(32); auto value = r.take(r.little(4)); l.patches.emplace_back(key, value); }
    r.end();
    return l;
}
Bytes txid(V tx) { return hx::half(Bytes("TXN\0", 4) + Bytes(tx)); }
}

struct PieceHistory::Impl {
    fs::path cache;
    Json selection;
    U first = 0, last = 0, snapshot_seq = 0;
    // The base snapshot's ledger, served whole when it has no transactions: its header (from the
    // signed snapshot index) and its state (the snapshot) are all of it. On mainnet that is ledger
    // 32,570, the first the network kept; the published ranges start after it.
    std::optional<Header> baseline;
    std::vector<std::unique_ptr<Range>> ranges;
    std::unique_ptr<StateIndex> state;
    // Where a ledger's state is read from: a snapshot, and the ranges that changed it since
    // (from `first_range`, the one starting right after it). `tag` keeps each snapshot's blocks
    // apart in the block cache (keyed tag << 48 | block): 0 for the base snapshot, then 1, 2, ...
    // for the interval snapshots in ascending order.
    struct Base { StateIndex const* state; U seq; std::size_t first_range; U tag; };
    // The interval snapshots in the selection (when synced and indexed), ascending: the newest
    // (sync_pieces' `recent`) and each one [cache] snapshots_every keeps. A ledger reads its state
    // from the newest at or before it and the few ranges since, not from the base snapshot and
    // every range since (on mainnet, 2013's ledger 32,570 and over a hundred bundles). Any of them
    // gives the same answer: each holds the state its ledger's account hash commits to (checked
    // against the signed catalog by sync), and the ranges after it every change since.
    std::vector<std::unique_ptr<StateIndex>> interval_states;
    std::vector<Base> intervals;
    Base base_for(U seq) const {
        auto after = std::upper_bound(intervals.begin(), intervals.end(), seq, [](U n, Base const& b) { return n < b.seq; });
        if (after != intervals.begin()) return *(after - 1);
        return {state.get(), snapshot_seq, 0, 0};
    }
    std::unique_ptr<NftIndex> nfts;  // absent in caches indexed before nft_info was supported
    // The base snapshot's optional indexes (piece_format.hpp's XQOX); an optional index answers
    // only when the snapshot and every range have theirs.
    std::unique_ptr<OptionalIndex> holders, issuers;
    bool has_holders = false, has_issuers = false;
    // An open segment holds its codec dictionary (loaded by the first object or ledger decode;
    // header and transaction reads never need it), the values rendered from it and any
    // transaction pages read, charged by what it holds after each read: loading a dictionary is
    // most of a cold read (about 80 ms for a 2014 segment), so as many stay open as the budget holds.
    // Shared by the three caches below and by every other history of this cache directory
    // (shared_budget); declared before them, so it outlives them.
    std::shared_ptr<Budget> budget;
    Lru<std::pair<std::size_t, std::size_t>, OpenArchive> archives{std::size_t(1) << 30, archive_bytes};
    // Each segment's first ledger and ledgers per page, so a cached page is found without
    // opening its segment.
    // Time spent loading, in nanoseconds: segment opens, codec dictionaries, page decodes, snapshot blocks.
    std::atomic<std::uint64_t> open_ns{0}, dictionary_ns{0}, page_ns{0}, block_ns{0};
    std::atomic<std::uint64_t> dictionary_read_ns{0}, dictionary_parse_ns{0}, dictionary_bytes{0}, dictionaries{0};  // the dictionary loads' parts
    static std::uint64_t since(std::chrono::steady_clock::time_point t) {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t).count());
    }
    // Decoded ledgers, keyed by (range, segment, ledger, patches only): object lookups decode
    // without metadata, and a full ledger serves them too.
    using LedgerKey = std::tuple<std::size_t, std::size_t, U, bool>;
    using SegmentId = std::pair<std::size_t, std::size_t>;
    Lru<LedgerKey, Page> pages{512u << 20, page_bytes};
    Lru<U, StateBlock> blocks{128u << 20, block_bytes};
    std::mutex hint_mutex;
    std::map<Bytes, std::pair<U, U>> hints;  // transaction hash -> (ledger, record position)

    std::size_t range_of(U seq) const {
        auto it = std::upper_bound(ranges.begin(), ranges.end(), seq, [](U n, auto const& r) { return n < r->first; });
        if (it == ranges.begin() || seq > (*(it - 1))->last) throw Error("Ledger outside cached ranges");
        return static_cast<std::size_t>(it - ranges.begin() - 1);
    }
    // How many ranges hold changes at or before `seq` (none for the snapshot ledger itself,
    // whose state a live tail reads while no range is sealed yet).
    std::size_t through(U seq) const { return seq <= snapshot_seq ? 0 : range_of(seq) + 1; }
    // The newest of the first `below` ranges whose object table holds `key`, and its record. A
    // key last changed years ago is found only after every newer range's table was searched, so
    // the answer for a search of all ranges is remembered (ranges never change; the cache goes
    // with this history when the selection does): a busy account's trust lines are looked up
    // again and again. Ranges before `lowest` are not searched (a ledger whose state is read from
    // a newer snapshot needs none of them), and a search that stopped there is not remembered.
    // Keyed by the 32-byte object key (any other is not remembered), in shards with locks of
    // their own: object_page asks for thousands of keys at once, from many requests. Each shard
    // starts over past 65,536 keys (about 6 MB), so the whole is bounded whatever clients ask.
    // A key in no range (one unchanged since the base snapshot, or one that never existed) is
    // remembered apart, in a smaller set that starts over past 16,384: clients name any keys they
    // like, and a flood of keys that never existed then evicts none of the busy keys found.
    struct Newest { std::size_t range; U offset; };
    // Clients choose the keys they ask about, so neither a prefix nor a fixed hash may decide the
    // bucket or shard (SeededKeyHash).
    struct NewestShard {
        std::mutex mutex;
        std::unordered_map<std::array<char, 32>, Newest, SeededKeyHash> keys;
        std::unordered_set<std::array<char, 32>, SeededKeyHash> absent;  // in no range at all
    };
    std::array<NewestShard, 16> newest;
    // With `remember_absent` false a key in no range is not remembered: a lookup that need not be
    // of a real object (deleted_object, for any key a client names) would otherwise fill the
    // shards with keys that never existed and start them over, losing the busy keys they hold.
    std::optional<std::pair<std::size_t, Record>> newest_below(V key, std::size_t below, std::size_t lowest = 0, bool remember_absent = true) {
        std::array<char, 32> k{};
        bool keyed = key.size() == k.size();
        if (keyed) std::memcpy(k.data(), key.data(), k.size());
        auto& shard = newest[(SeededKeyHash{}(k) >> 56) % newest.size()];  // the hash's top bits: the map uses the low ones
        if (keyed) {
            std::lock_guard lock(shard.mutex);
            if (auto it = shard.keys.find(k); it != shard.keys.end()) {
                auto ri = it->second.range;
                if (ri < below) return std::pair{ri, *ranges[ri]->query->objects.at(it->second.offset)};
            } else if (shard.absent.contains(k)) return std::nullopt;
        }
        std::optional<std::pair<std::size_t, Record>> found;
        for (auto ri = below; ri-- > lowest;)
            if (auto record = ranges[ri]->query->objects.find(key)) { found = std::pair{ri, *record}; break; }
        if (keyed && below == ranges.size() && (found || (lowest == 0 && remember_absent))) {
            std::lock_guard lock(shard.mutex);
            if (found) {
                if (shard.keys.size() >= 65536) shard.keys.clear();
                shard.keys[k] = Newest{found->first, ranges[found->first]->query->objects.offset_of(found->second)};
            } else {
                if (shard.absent.size() >= 16384) shard.absent.clear();
                shard.absent.insert(k);
            }
        }
        return found;
    }
    // `key`'s version at `seq`: the newest range after `lowest` holding a version at or before
    // it, the record and that version; nullopt when none does (the base's snapshot holds it, or
    // nothing). `remember_absent` as newest_below.
    std::optional<std::tuple<std::size_t, Record, Version>> holding(V key, U seq, std::size_t lowest, bool remember_absent = true) {
        for (auto below = through(seq); below > lowest;) {
            auto found = newest_below(key, below, lowest, remember_absent);
            if (!found || found->first < lowest) return std::nullopt;
            auto const& [ri, record] = *found;
            if (seq >= ranges[ri]->first)
                if (auto version = version_at(record, seq - ranges[ri]->first)) return std::tuple{ri, record, *version};
            below = ri;
        }
        return std::nullopt;
    }
    std::shared_ptr<OpenArchive> archive(std::size_t ri, std::size_t si) {
        auto open = archives.get({ri, si}, [&] {
            auto t = std::chrono::steady_clock::now();
            auto const& s = ranges[ri]->directory.segments[si];
            auto a = std::make_shared<OpenArchive>(ranges[ri]->file->view().substr(s.offset, s.length));
            open_ns += since(t);
            a->weight = held(*a);
            return a;
        });
        if (open->stale) grew(ri, si, *open);  // a read that grew it left it uncharged (OpenArchive::stale)
        return open;
    }
    // Charges a segment again after a read grew it, measured under the cache's lock and its own
    // mutex, so two reads that grew it never leave the older size charged. When another thread
    // holds the segment (a decode can take a while; cache_stats) it is not waited for, as every
    // lookup would wait behind the cache's lock: the segment stays stale, and is measured by that
    // read once it is done or else by the next lookup of it.
    void grew(std::size_t ri, std::size_t si, OpenArchive& a) {
        if (!a.stale) return;  // the read left it as charged
        archives.recharge({ri, si}, a, [](OpenArchive& value) {
            if (std::unique_lock lock(value.mutex, std::try_to_lock); lock) { value.stale = false; value.weight = held(value); }
            else value.stale = true;
        });
    }
    // Loads a segment's dictionary unless loaded; the caller holds its mutex.
    void prepare(OpenArchive& a) {
        if (a.archive.dictionaryBytes()) return;
        auto t = std::chrono::steady_clock::now();
        a.archive.prepare();
        dictionary_ns += since(t);
        auto [read, parse] = a.archive.dictionarySeconds();
        dictionary_read_ns += static_cast<std::uint64_t>(read * 1e9); dictionary_parse_ns += static_cast<std::uint64_t>(parse * 1e9);
        dictionary_bytes += a.archive.dictionaryBytes(); ++dictionaries;
    }
    Bytes transaction_blob(U seq, U position) {
        auto ri = range_of(seq); auto si = ranges[ri]->directory.segment_of(seq);
        auto a = archive(ri, si);
        Bytes out;
        {
            auto lock = hold(*a);
            auto const& txs = a->archive.transactionsOf(seq);
            if (position >= txs.size()) throw Error("Transaction position outside ledger");
            out = Bytes(txs[position]);
        }
        grew(ri, si, *a);
        return out;
    }
    std::vector<Bytes> transaction_blobs(U seq) {
        auto ri = range_of(seq); auto si = ranges[ri]->directory.segment_of(seq);
        auto a = archive(ri, si);
        std::vector<Bytes> out;
        {
            auto lock = hold(*a);
            for (auto tx : a->archive.transactionsOf(seq)) out.emplace_back(tx);
        }
        grew(ri, si, *a);
        return out;
    }
    // With `patches_only` a ledger's metadata is empty. index-pieces checked every page's
    // canonical digest, so these decodes skip it.
    std::shared_ptr<Page> cached(std::size_t ri, std::size_t si, U seq, bool patches_only, bool peek = false) {
        auto look = [&](LedgerKey const& key) { return peek ? pages.peek(key) : pages.find(key); };
        if (auto p = look({ri, si, seq, false})) return p;
        return patches_only ? look({ri, si, seq, true}) : nullptr;
    }
    // Decodes and caches the ledgers of one segment not cached yet. Each is built alone; under one
    // hold of the segment and in ascending order, so a page's parse continues from the ledger
    // before (the reader keeps where it stopped) instead of starting again at the page's first.
    std::map<U, std::shared_ptr<Page>> decode(std::size_t ri, std::size_t si, std::set<U> const& ledgers, bool patches_only) {
        std::vector<U> missing;
        for (auto seq : ledgers) if (!cached(ri, si, seq, patches_only, true)) missing.push_back(seq);
        std::map<U, std::shared_ptr<Page>> decoded;
        if (missing.empty()) return decoded;
        auto a = archive(ri, si);
        {
            auto lock = hold(*a);
            prepare(*a);
            auto t = std::chrono::steady_clock::now();
            for (auto seq : missing) {
                // Another read of this segment may have decoded it while this one waited.
                if (cached(ri, si, seq, patches_only, true)) continue;
                auto p = std::make_shared<Page>();
                p->raw = a->archive.decodeLedger(seq, patches_only);
                // The record is built by appending (up to twice its size reserved): trimmed before it is
                // parsed (the parse's views point into it), and charged what it and the views hold.
                p->raw.shrink_to_fit();
                p->ledger = parse_ledger(p->raw);
                p->bytes = page_charge(*p);
                decoded.emplace(seq, std::move(p));
            }
            page_ns += since(t);
        }
        grew(ri, si, *a);
        for (auto const& [seq, p] : decoded) pages.put({ri, si, seq, patches_only}, p);
        return decoded;
    }
    // Ledger `seq`, decoded when not cached.
    std::shared_ptr<Page> page(U seq, bool patches_only = false) {
        auto [ri, si] = segment_id(seq);
        // Another read may decode it meanwhile, and a full cache evict it again before it is found.
        for (int tries = 0; tries < 4; ++tries) {
            if (auto p = cached(ri, si, seq, patches_only)) return p;
            auto decoded = decode(ri, si, {seq}, patches_only);
            if (auto it = decoded.find(seq); it != decoded.end()) return it->second;
        }
        throw Error("Decoded ledger evicted before it was read");
    }
    // Loads of the ledgers wanted, one task per segment.
    std::vector<std::function<void()>> load_tasks(std::map<SegmentId, std::set<U>> const& wanted, bool patches_only) {
        std::vector<std::function<void()>> tasks;
        for (auto const& [id, ledgers] : wanted)
            tasks.push_back([this, id = id, &ledgers = ledgers, patches_only] { decode(id.first, id.second, ledgers, patches_only); });
        return tasks;
    }
    // Runs loads on the shared pool (and the calling thread); a failure is left for the read that
    // needs the data.
    static void parallel(std::vector<std::function<void()>> const& tasks) {
        (void)LoadPool::shared().run(tasks, 16);
    }
    // (range, segment, page) holding a ledger.
    SegmentId segment_id(U seq) const {
        auto ri = range_of(seq);
        return {ri, ranges[ri]->directory.segment_of(seq)};
    }
    std::shared_ptr<StateBlock> block(U i) { return block(base_for(snapshot_seq), i); }
    std::shared_ptr<StateBlock> block(Base const& base, U i) {
        return blocks.get(base.tag << 48 | i, [&] { auto t = std::chrono::steady_clock::now(); auto b = base.state->load(i); block_ns += since(t); return b; });
    }
    // The first snapshot object whose key is > key.
    std::optional<std::pair<Bytes, Bytes>> snapshot_after(V key) {
        for (U i = state->count ? state->block_for(key) : 0; i < state->count; ++i) {
            auto p = block(i);
            auto it = std::upper_bound(p->items.begin(), p->items.end(), key, [](V k, auto const& item) { return k < item.first; });
            if (it != p->items.end()) return std::pair{Bytes(it->first), Bytes(it->second)};
        }
        return std::nullopt;
    }
    // A token's state in the base snapshot. As in rippled's locatePage, its page is the owner's
    // first NFTokenPage whose key is above owner + the token ID's low 96 bits.
    std::optional<NftState> snapshot_nft(Bytes const& id) {
        auto owner = nfts->owner(id);
        if (!owner) return std::nullopt;
        auto page = snapshot_after(Bytes(*owner) + id.substr(20));
        if (!page || V(page->first).substr(0, 20) != *owner) throw Error("Snapshot NFT page missing");
        for (auto const& [token, uri] : page_tokens(page->second))
            if (token == id) return NftState{toBase58(r::AccountID::fromVoid(owner->data())), uri, false};
        throw Error("Snapshot NFT page does not hold the token");
    }
    std::optional<Bytes> snapshot_object(Base const& base, V key) {
        if (!base.state->count) return std::nullopt;
        auto p = block(base, base.state->block_for(key));
        auto it = std::lower_bound(p->items.begin(), p->items.end(), key, [](auto const& item, V k) { return item.first < k; });
        if (it != p->items.end() && it->first == key) return Bytes(it->second);
        return std::nullopt;
    }
    // Newest version at or before seq within one range: nullopt when the range has none,
    // an empty value when the object was deleted.
    std::optional<Bytes> resolve(std::size_t ri, Record const& record, U seq) {
        auto const& r = *ranges[ri];
        if (seq < r.first) return std::nullopt;
        auto version = version_at(record, seq - r.first);
        if (!version) return std::nullopt;
        if (version->deleted) return Bytes();
        U ledger = r.first + version->ledger;
        auto p = page(ledger, true);
        auto const& patches = p->at(ledger).patches;
        auto found = std::lower_bound(patches.begin(), patches.end(), record.key, [](auto const& patch, V k) { return patch.first < k; });
        if (found == patches.end() || found->first != record.key || found->second.empty()) throw Error("Object version missing from ledger");
        return Bytes(found->second);
    }
    void remember(Bytes const& hash, U seq, U position) {
        std::lock_guard lock(hint_mutex);
        if (hints.size() > 200000) hints.clear();
        hints[hash] = {seq, position};
    }
    // A transaction's ledger and record position. A reused ID (a pseudo-transaction repeated in
    // two flag ledgers: Store::append) answers its earliest ledger, as xrpld: ranges are searched
    // oldest first, and within one the earliest match wins. Hints (from history pages) are never
    // a pseudo-transaction's.
    std::optional<std::pair<U, U>> locate(Bytes const& hash) {
        {
            std::lock_guard lock(hint_mutex);
            if (auto it = hints.find(hash); it != hints.end()) return it->second;
        }
        for (std::size_t ri = 0; ri < ranges.size(); ++ri) {
            std::optional<std::pair<U, U>> found;
            for (auto [seq, position] : ranges[ri]->tx.candidates(hash))
                if (seq >= first && seq <= last && (!found || seq < found->first) && txid(transaction_blob(seq, position)) == hash) found = std::pair{seq, position};
            if (found) return found;
        }
        return std::nullopt;
    }
};

namespace {
std::atomic<std::size_t> cache_megabytes{3072};
std::atomic<unsigned> warm_count{0};  // only `serve` warms: a one-shot command would exit mid-warm
}
void configure_piece_caches(std::size_t megabytes, unsigned warm_segments) {
    if (megabytes < 256) throw Error("Piece cache budget must be at least 256 MB");
    cache_megabytes = megabytes; warm_count = warm_segments;
}
PieceHistory::PieceHistory(fs::path const& cache) : impl_(std::make_unique<Impl>()) {
    auto& m = *impl_;
    // One budget: open segments are guaranteed 60% (a segment load costs several page decodes),
    // decoded pages 30% and snapshot blocks 10%, and each may use what the others leave free. The
    // budget is the directory's, shared with the history this one replaces (open_piece_history).
    std::size_t budget = cache_megabytes.load() << 20;
    m.budget = shared_budget(cache);
    m.budget->total = budget;
    m.pages.capacity(budget / 10 * 3);
    m.archives.capacity(budget / 10 * 6);
    m.blocks.capacity(budget / 10);
    m.archives.share(*m.budget); m.pages.share(*m.budget); m.blocks.share(*m.budget);
    m.cache = cache;
    m.selection = Json::parse(read_file(cache / "selection.json", 64 << 20));
    m.first = m.selection.at("first"); m.last = m.selection.at("last");
    auto const& snapshot = m.selection.at("snapshot");
    m.snapshot_seq = snapshot.at("seq");
    auto state = state_path(cache, m.snapshot_seq);
    if (!fs::exists(state)) throw Error("Missing snapshot state index; run `xrpla index-pieces --dir " + cache.string() + "`");
    m.state = std::make_unique<StateIndex>(state, from_hex(snapshot.at("index").at("sha256").get<std::string>()));
    if (m.state->ledger != m.snapshot_seq) throw Error("Snapshot state index ledger differs");
    // The interval snapshots: `snapshots` lists every one selected; a cache synced before that
    // list existed names its newest alone, as `recent`. One not indexed yet is not used.
    std::map<U, Json> selected;
    for (auto key : {"snapshots", "recent"}) {
        if (!m.selection.contains(key)) continue;
        auto const& listed = m.selection.at(key);
        for (auto const& s : listed.is_array() ? listed : Json::array({listed})) selected.emplace(s.at("seq").get<U>(), s);
    }
    std::vector<std::pair<U, std::unique_ptr<StateIndex>>> interval_states;
    for (auto const& [seq, s] : selected) {
        if (seq <= m.snapshot_seq) throw Error("Interval snapshot precedes the base snapshot");
        auto path = state_path(cache, seq);
        if (!fs::exists(path)) continue;
        auto index = std::make_unique<StateIndex>(path, from_hex(s.at("index").at("sha256").get<std::string>()));
        if (index->ledger != seq) throw Error("Interval snapshot state index ledger differs");
        interval_states.emplace_back(seq, std::move(index));
    }
    if (auto index_file = cache / snapshot.at("index").at("object").get<std::string>(); fs::exists(index_file)) {
        auto text = read_file(index_file, 16 << 20);
        if (hx::hex(hx::sha(text)) != snapshot.at("index").at("sha256").get<std::string>()) throw Error("Snapshot index differs from the catalog");
        auto index = Json::parse(text);
        if (index.contains("ledger_header")) {
            auto header = decode_header(from_hex(index.at("ledger_header").get<std::string>()));
            auto same = [](std::string a, std::string b) {
                std::transform(a.begin(), a.end(), a.begin(), ::tolower); std::transform(b.begin(), b.end(), b.begin(), ::tolower); return a == b;
            };
            if (header.sequence != m.snapshot_seq || !same(header.hash, snapshot.at("ledger_hash").get<std::string>()) ||
                !same(header.account_hash, snapshot.at("account_hash").get<std::string>()))
                throw Error("Snapshot header differs from the catalog");
            std::transform(header.hash.begin(), header.hash.end(), header.hash.begin(), ::tolower);
            if (header.transaction_hash == std::string(64, '0')) m.baseline = std::move(header);
        }
    }
    if (auto nfts = nft_path(cache, m.snapshot_seq); fs::exists(nfts))
        m.nfts = std::make_unique<NftIndex>(nfts, from_hex(snapshot.at("index").at("sha256").get<std::string>()));
    // Optional indexes: whatever index-pieces left, used only where the snapshot and every range have one.
    auto optional = [&](std::string const& stem, Optional kind, Bytes const& source) -> std::unique_ptr<OptionalIndex> {
        auto path = optional_path(cache, stem, kind);
        if (!fs::exists(path)) return nullptr;
        try { return std::make_unique<OptionalIndex>(path, kind, source); } catch (std::exception const&) { return nullptr; }
    };
    auto snapshot_sha = from_hex(snapshot.at("index").at("sha256").get<std::string>());
    m.holders = optional(snapshot_stem(m.snapshot_seq), Optional::mpt_holders, snapshot_sha);
    m.issuers = optional(snapshot_stem(m.snapshot_seq), Optional::nfts_by_issuer, snapshot_sha);
    m.has_holders = m.holders != nullptr; m.has_issuers = m.issuers != nullptr;
    U expected = m.snapshot_seq + 1;
    for (auto const& info : m.selection.at("ranges")) {
        auto r = std::make_unique<Range>();
        r->first = info.at("first"); r->last = info.at("last");
        r->sha = from_hex(info.at("sha256").get<std::string>());
        if (r->first != expected) throw Error("Cached ranges are not contiguous");
        expected = r->last + 1;
        r->file = std::make_unique<Mapped>(cache / info.at("object").get<std::string>());
        auto data = r->file->view();
        r->directory = Container::parse(data, info.at("directory_sha256").get<std::string>());
        if (r->directory.first != r->first || r->directory.last != r->last) throw Error("Container range differs from catalog");
        r->tx = TxIndex(data.substr(r->directory.index_offset, r->directory.index_length));
        auto query = query_path(cache, info.at("sha256").get<std::string>());
        if (!fs::exists(query)) throw Error("Missing query index for " + info.at("object").get<std::string>() + "; run `xrpla index-pieces --dir " + cache.string() + "`");
        r->query = std::make_unique<QueryIndex>(query, r->sha);
        if (r->query->first != r->first || r->query->count != r->last - r->first + 1) throw Error("Query index range differs");
        auto stem = container_stem(info.at("sha256").get<std::string>());
        r->holders = optional(stem, Optional::mpt_holders, r->sha);
        r->issuers = optional(stem, Optional::nfts_by_issuer, r->sha);
        // The seek index of this build of the query index, once index-pieces has made it (and
        // checked its trailer); one made for another build, or unreadable, is not used.
        if (auto seek = seek_path(cache, info.at("sha256").get<std::string>()); fs::exists(seek))
            try { r->seek = std::make_unique<SeekIndex>(seek, *r->query); } catch (std::exception const&) {}
        m.has_holders = m.has_holders && r->holders; m.has_issuers = m.has_issuers && r->issuers;
        // A query index of version 2 misses some burns (piece_format.hpp): until index-pieces has
        // rebuilt it, NFT state is not indexed rather than answered from it.
        if (r->query->version != query_version) m.nfts.reset();
        m.ranges.push_back(std::move(r));
    }
    // An interval snapshot serves the ledgers from its own on only when a range starts right
    // after it (the ranges before that one are then never searched); otherwise it is not used.
    for (auto& [seq, index] : interval_states) {
        auto after = std::find_if(m.ranges.begin(), m.ranges.end(), [&](auto const& r) { return r->first == seq + 1; });
        if (after == m.ranges.end()) continue;
        m.intervals.push_back({index.get(), seq, static_cast<std::size_t>(after - m.ranges.begin()), static_cast<U>(m.intervals.size() + 1)});
        m.interval_states.push_back(std::move(index));
    }
    // A selection without ranges holds a new publication's baseline snapshot alone.
    if (m.first != m.snapshot_seq + 1 || (m.ranges.empty() ? m.last != m.snapshot_seq : m.last > m.ranges.back()->last || m.last < m.first))
        throw Error("Invalid piece selection");
}
PieceHistory::~PieceHistory() = default;

Json PieceHistory::status() const {
    auto& m = *impl_;
    auto last_hash = m.ranges.empty() ? m.selection.at("snapshot").at("ledger_hash").get<std::string>() : decode_header(header(static_cast<std::uint32_t>(m.last))).hash;
    return {{"store_available", true}, {"schema", "xrpla-pieces-v1"}, {"verification", "signed-catalog-object-digests-and-section-checksums"},
            {"baseline_hash", m.selection.at("snapshot").at("ledger_hash")}, {"generation", m.selection.at("catalog").at("generation")},
            {"complete_ledgers", {{"first", m.first}, {"last", m.last}}}, {"last_ledger_hash", last_hash},
            {"state_ledgers", {{"first", m.first}, {"last", m.last}}}};
}
// Fixed for this history's selection, so answered from memory (the constructor checked that
// the first range follows the snapshot, so the baseline is the ledger just before `first`).
std::optional<std::pair<std::uint32_t, std::uint32_t>> PieceHistory::served_ledgers() const {
    auto& m = *impl_;
    return std::pair{static_cast<std::uint32_t>(m.baseline ? m.snapshot_seq : m.first), static_cast<std::uint32_t>(m.last)};
}
std::optional<std::uint32_t> PieceHistory::baseline_ledger() const {
    auto& m = *impl_;
    return m.baseline ? std::optional<std::uint32_t>(m.snapshot_seq) : std::nullopt;
}
Bytes PieceHistory::header(std::uint32_t seq) const {
    auto& m = *impl_;
    if (m.baseline && seq == m.snapshot_seq) return m.baseline->raw;
    if (seq < m.first || seq > m.last) return {};
    return Bytes(m.ranges[m.range_of(seq)]->query->header(seq));
}
std::uint32_t PieceHistory::ledger_sequence(Bytes const& hash) const {
    auto& m = *impl_;
    if (m.baseline && to_hex(hash) == m.baseline->hash) return m.snapshot_seq;
    for (auto const& r : m.ranges)
        if (auto seq = r->query->ledger_of(hash); seq && *seq >= m.first && *seq <= m.last) return static_cast<std::uint32_t>(*seq);
    return 0;
}
Bytes PieceHistory::object(Bytes const& key, std::uint32_t seq) const {
    auto& m = *impl_;
    if (key.size() != 32) throw Error("Invalid object key length");
    if (seq < m.snapshot_seq || seq > m.last) throw Error("Ledger state has been pruned");
    auto base = m.base_for(seq);
    if (auto held = m.holding(key, seq, base.first_range)) return m.resolve(std::get<0>(*held), std::get<1>(*held), seq).value_or(Bytes());
    return m.snapshot_object(base, key).value_or(Bytes());
}
// Keys grouped by where their version at `seq` is: a ledger's patches in some segment page, or
// the base snapshot. Each segment is read by one task (opened once, its pages decoded once each,
// up to the newest ledger wanted), each snapshot block once, on up to 16 threads. Segments and
// decoded pages go through the shared caches, so the same directory read again (a gateway's
// trust lines) is served from memory.
std::vector<Bytes> PieceHistory::objects(std::vector<Bytes> const& keys, std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq < m.snapshot_seq || seq > m.last) throw Error("Ledger state has been pruned");
    std::vector<Bytes> out(keys.size());
    struct Want { std::size_t index; U ledger; };
    // (range, segment) -> keys: each segment's ledgers decode together, in one task.
    std::map<std::pair<std::size_t, std::size_t>, std::vector<Want>> segments;
    std::map<U, std::vector<std::size_t>> blocks;
    auto base = m.base_for(seq);
    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto const& key = keys[i];
        if (key.size() != 32) throw Error("Invalid object key length");
        bool found = false;
        if (auto held = m.holding(key, seq, base.first_range)) {
            auto const& [ri, record, version] = *held;
            found = true;
            if (version.deleted) continue;  // deleted: stays empty
            U ledger = m.ranges[ri]->first + version.ledger;
            auto ri2 = m.range_of(ledger);
            segments[{ri2, m.ranges[ri2]->directory.segment_of(ledger)}].push_back({i, ledger});
        }
        if (!found && base.state->count) blocks[base.state->block_for(key)].push_back(i);
    }
    std::vector<std::function<void()>> tasks;
    for (auto const& [segment, wanted] : segments) tasks.push_back([&m, &keys, &out, segment = segment, &wanted = wanted] {
        auto [ri, si] = segment;
        std::set<U> ledgers;
        for (auto const& w : wanted) ledgers.insert(w.ledger);
        m.decode(ri, si, ledgers, true);
        for (auto const& w : wanted) {
            auto p = m.page(w.ledger, true);
            auto const& patches = p->at(w.ledger).patches;
            V key(keys[w.index]);
            auto found = std::lower_bound(patches.begin(), patches.end(), key, [](auto const& patch, V k) { return patch.first < k; });
            if (found == patches.end() || found->first != key || found->second.empty()) throw Error("Object version missing from ledger");
            out[w.index] = Bytes(found->second);
        }
    });
    for (auto const& [block, indexes] : blocks) tasks.push_back([&m, &keys, &out, base, block = block, &indexes = indexes] {
        auto p = m.block(base, block);
        for (auto i : indexes) {
            V key(keys[i]);
            auto it = std::lower_bound(p->items.begin(), p->items.end(), key, [](auto const& item, V k) { return item.first < k; });
            if (it != p->items.end() && it->first == key) out[i] = Bytes(it->second);
        }
    });
    // As Impl::parallel, but a failure is the caller's: every key must be read.
    if (auto failure = LoadPool::shared().run(tasks, 16)) std::rethrow_exception(failure);
    return out;
}
std::optional<std::pair<std::uint32_t, Bytes>> PieceHistory::deleted_object(Bytes const& key, std::uint32_t seq) const {
    auto& m = *impl_;
    if (key.size() != 32) throw Error("Invalid object key length");
    if (seq < m.snapshot_seq || seq > m.last) throw Error("Ledger state has been pruned");
    // Any key a client names (ledger_entry's include_deleted): one in no range is not remembered.
    if (auto held = m.holding(key, seq, 0, false)) {
        auto const& [ri, record, version] = *held;
        if (!version.deleted) return std::nullopt;
        auto deleted_at = static_cast<std::uint32_t>(m.ranges[ri]->first + version.ledger);
        auto before = object(key, deleted_at - 1);
        if (before.empty()) return std::nullopt;
        return std::pair{deleted_at, std::move(before)};
    }
    return std::nullopt;
}
Json PieceHistory::object_page(std::uint32_t seq, std::string const& marker, unsigned limit, bool binary) const {
    KeyWalk walk;
    return object_page(seq, marker, limit, binary, walk);
}
Json PieceHistory::object_page(std::uint32_t seq, std::string const& marker, unsigned limit, bool binary, KeyWalk& walk) const {
    auto& m = *impl_;
    if (!limit || limit > 4096) throw Error("Object page limit must be between 1 and 4096");
    if (seq < m.snapshot_seq || seq > m.last) throw Error("Ledger state has been pruned");
    Bytes after;
    if (!marker.empty()) { after = from_hex(marker); if (after.size() != 32) throw Error("Invalid object marker"); }
    // The page's keys from the merge of keys alone (one more, to know whether a marker is due),
    // then their values read together: grouped by segment and decoded concurrently.
    auto keys = keys_between(seq, after, {}, limit + 1, walk);
    bool more = keys.size() > limit;
    if (more) keys.pop_back();
    auto values = objects(keys, seq);
    Json items = Json::array();
    for (std::size_t i = 0; i < keys.size(); ++i) {
        if (values[i].empty()) throw Error("Listed object has no value");
        if (binary) items.push_back({{"index", to_hex(keys[i])}, {"data", to_hex(values[i])}});
        else { auto object = object_json({keys[i], values[i]}); object["index"] = to_hex(keys[i]); items.push_back(object); }
    }
    Json result{{"state", items}};
    if (more) result["marker"] = to_hex(keys.back());
    return result;
}
Json PieceHistory::history_page(std::string const& column, Bytes const& key, std::uint32_t first,
    std::uint32_t last, unsigned limit, bool forward, Json const& marker) const {
    auto& m = *impl_;
    if (!limit || limit > 1000 || first > last) throw Error("Invalid history page bounds");
    if (column == "mpt_tx") throw Error("MPT history is not indexed for pieces");
    if (column != "account_tx" && column != "nft_tx") throw Error("Invalid history index");
    std::optional<std::pair<U, U>> cursor;
    if (!marker.is_null()) {
        if (!marker.is_object() || !marker.contains("ledger") || !marker.contains("seq") ||
            !marker["ledger"].is_number_unsigned() || !marker["seq"].is_number_unsigned()) throw Error("Invalid history marker");
        auto ledger = marker.at("ledger").get<std::uint64_t>(), position = marker.at("seq").get<std::uint64_t>();
        if (ledger < first || ledger > last || position > UINT32_MAX) throw Error("History marker is outside range");
        cursor = std::pair{ledger, position};
    }
    U lo = std::max<U>(first, m.first), hi = std::min<U>(last, m.last);
    Json items = Json::array(), next = nullptr;
    std::vector<std::size_t> order;
    for (std::size_t ri = 0; ri < m.ranges.size(); ++ri) if (m.ranges[ri]->last >= lo && m.ranges[ri]->first <= hi) order.push_back(ri);
    if (!forward) std::reverse(order.begin(), order.end());
    for (auto ri : order) {
        auto const& r = *m.ranges[ri];
        // A range wholly before the marker (in page order) was listed by earlier pages.
        if (cursor && (forward ? r.last < cursor->first : r.first > cursor->first)) continue;
        bool accounts = column == "account_tx";
        auto const& table = accounts ? r.query->accounts : r.query->nfts;
        auto record = table.find(key);
        if (!record) continue;
        // Stream the ascending postings, keeping at most the next `wanted` in page order, so a
        // key with millions of postings costs bounded memory. With the range's seek index (XQSK)
        // a long list is read from the last checkpoint at or before the first posting the page
        // can want instead of from its start: a page deep in a busy key's list costs about a
        // page and a stride of postings, not every posting before it.
        std::size_t wanted = limit - items.size() + 1;
        SeekPoints points;
        if (r.seek) points = r.seek->points(accounts ? SeekIndex::accounts : SeekIndex::nfts, table.offset_of(*record), *record);
        // The ledger and TransactionIndex of checkpoint j's posting (j >= 1).
        auto key_at = [&](U j) {
            PostingReader reader(*record, points.state(j));
            Posting p;
            if (!reader.next(p)) throw Error("Seek index differs from its query index");
            return std::pair<U, U>{r.first + p.ledger, p.index};
        };
        // The last checkpoint whose posting is at or before `bound`; 0 (the list's start) when none is.
        auto checkpoint = [&](std::pair<U, U> bound) {
            U low = 1, high = points.size() + 1;
            while (low < high) { U mid = (low + high) / 2; if (key_at(mid) <= bound) low = mid + 1; else high = mid; }
            return low - 1;
        };
        // Reads from checkpoint `from` up to posting `until` (or the list's end) into `out`. A
        // read that reaches a later checkpoint must stand where that checkpoint says: a
        // checkpoint names where to start reading, never which postings there are.
        auto read = [&](U from, U until, std::deque<Posting>& out) {
            PostingReader reader(*record, points.state(from));
            for (U check = from + 1;;) {
                auto const& s = reader.state();
                if (check <= points.size() && s.read == check * points.stride()) {
                    auto expected = points.state(check++);
                    if (s.at != expected.at || s.ledger != expected.ledger || s.index != expected.index) throw Error("Seek index differs from its query index");
                }
                Posting p;
                if (s.read >= until || !reader.next(p)) break;
                U seq = r.first + p.ledger;
                if (seq < lo) continue;
                if (seq > hi) break;
                std::pair<U, U> here{seq, p.index};
                if (forward) {
                    if (cursor && here < *cursor) continue;
                    out.push_back(p);
                    if (out.size() == wanted) break;
                } else {
                    if (cursor && here > *cursor) break;
                    out.push_back(p);
                    if (out.size() > wanted) out.pop_front();
                }
            }
        };
        std::deque<Posting> list;
        if (forward) {
            // Every posting before a checkpoint at or before the cursor (or ledger `lo`) is skipped.
            auto from = points.size() ? checkpoint(std::max(std::pair<U, U>{lo, 0}, cursor.value_or(std::pair<U, U>{0, 0}))) : 0;
            read(from, record->entries, list);
        } else {
            // The postings up to the cursor (or ledger `hi`) from the last checkpoint at or before
            // it; then, while fewer than wanted, each stride before that, newest first, until
            // ledger `lo` is passed, keeping the newest `wanted` as one read from the start does.
            std::pair<U, U> upper{hi, UINT64_MAX};
            if (cursor) upper = std::min(upper, *cursor);
            auto from = points.size() ? checkpoint(upper) : 0;
            read(from, record->entries, list);
            for (; list.size() < wanted && from && key_at(from).first >= lo; --from) {
                std::deque<Posting> earlier;
                read(from - 1, from * points.stride(), earlier);
                earlier.insert(earlier.end(), list.begin(), list.end());
                list.swap(earlier);
                while (list.size() > wanted) list.pop_front();
            }
        }
        if (!forward) std::reverse(list.begin(), list.end());
        // The caller reads each listed transaction with its metadata next: decode their ledgers together.
        std::map<Impl::SegmentId, std::set<U>> ledgers;
        for (std::size_t i = 0; i < list.size() && i + items.size() < limit; ++i) {
            U seq = r.first + list[i].ledger;
            ledgers[m.segment_id(seq)].insert(seq);
        }
        Impl::parallel(m.load_tasks(ledgers, false));
        for (auto const& p : list) {
            U seq = r.first + p.ledger;
            if (items.size() == limit) { next = {{"ledger", seq}, {"seq", p.index}}; break; }
            auto blob = m.transaction_blob(seq, p.position);
            auto hash = txid(blob);
            if (!pseudo_transaction(blob)) m.remember(hash, seq, p.position);
            items.push_back({{"hash", to_hex(hash)}, {"ledger_index", seq}, {"tx_index", p.index}});
        }
        if (!next.is_null()) break;
    }
    Json result{{"transactions", items}, {"limit", limit}};
    if (!next.is_null()) result["marker"] = next;
    return result;
}
Json PieceHistory::transaction(Bytes const& hash, bool binary) const {
    auto& m = *impl_;
    if (hash.size() != 32) return nullptr;
    auto location = m.locate(hash);
    if (!location) return nullptr;
    auto [seq, position] = *location;
    auto p = m.page(seq);
    auto const& ledger = p->at(seq);
    if (position >= ledger.transactions.size()) throw Error("Transaction position outside ledger");
    auto [tx, meta] = ledger.transactions[position];
    if (txid(tx) != hash) throw Error("Transaction hash differs from index");
    Json result = binary ? Json{{"tx_blob", to_hex(tx)}, {"meta", to_hex(meta)}} : transaction_json({Bytes(tx), Bytes(meta)});
    result["ledger_index"] = seq; result["hash"] = to_hex(hash);
    return result;
}
std::vector<Bytes> PieceHistory::ledger_transactions(std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq < m.first || seq > m.last) return {};
    auto blobs = m.transaction_blobs(seq);
    std::vector<Bytes> result;
    for (auto position : m.ranges[m.range_of(seq)]->query->order(seq, blobs.size())) result.push_back(txid(blobs.at(position)));
    return result;
}
LedgerRecord PieceHistory::ledger_record(std::uint32_t seq) const {
    auto& m = *impl_;
    // The baseline ledger's changes from its parent are not published (its parent's state is not).
    if (m.baseline && seq == m.snapshot_seq) throw Error("Ledger changes before the first published ledger have been pruned");
    if (seq < m.first || seq > m.last) throw Error("Ledger is not stored");
    auto p = m.page(seq);
    auto const& ledger = p->at(seq);
    LedgerRecord record{seq, Bytes(ledger.header), {}, {}};
    std::vector<std::pair<Bytes, TransactionRecord>> sorted;
    for (auto const& [tx, meta] : ledger.transactions) sorted.push_back({txid(tx), {Bytes(tx), Bytes(meta)}});
    std::sort(sorted.begin(), sorted.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    for (auto& item : sorted) record.transactions.push_back(std::move(item.second));
    for (auto const& [key, value] : ledger.patches) record.patches.push_back({Bytes(key), Bytes(value)});
    return record;
}
std::vector<TransactionRecord> PieceHistory::ledger_transaction_records(std::uint32_t seq) const {
    if (impl_->baseline && seq == impl_->snapshot_seq) return {};  // the baseline ledger has none
    return ledger_record(seq).transactions;
}
bool PieceHistory::has_nft_state() const { return impl_->nfts != nullptr; }
void PieceHistory::warm(unsigned segments) const {
    auto& m = *impl_;
    if (m.ranges.empty()) return;
    auto ri = m.ranges.size() - 1;
    auto const& list = m.ranges[ri]->directory.segments;
    std::vector<std::function<void()>> tasks;
    for (std::size_t i = 0; i < std::min<std::size_t>(segments, list.size()); ++i) {
        auto si = list.size() - 1 - i;
        tasks.push_back([&m, ri, si] {
            auto a = m.archive(ri, si);
            {
                auto lock = hold(*a);
                m.prepare(*a);
            }
            m.grew(ri, si, *a);
        });
    }
    Impl::parallel(tasks);
}
// Loads, on several threads, what reading `keys` at `seq` will need and is not cached yet:
// the snapshot block of each key unchanged since the snapshot (random reads, which
// overlap), and the object patches of each page holding a newer version (pages of one
// segment still decode one at a time, as their reader is shared). Failures are left to
// the reads themselves.
void PieceHistory::prefetch(std::vector<Bytes> const& keys, std::uint32_t seq) const {
    auto& m = *impl_;
    if (keys.size() < 2 || seq < m.snapshot_seq || seq > m.last) return;
    std::map<Impl::SegmentId, std::set<U>> ledgers;  // the ledgers needed in each segment
    std::set<U> blocks;
    auto base = m.base_for(seq);
    try {
        for (auto const& key : keys) {
            if (key.size() != 32) continue;
            bool changed = false;
            if (auto held = m.holding(key, seq, base.first_range)) {
                auto const& [ri, record, version] = *held;
                changed = true;
                if (version.deleted) continue;
                U ledger = m.ranges[ri]->first + version.ledger;
                ledgers[m.segment_id(ledger)].insert(ledger);
            }
            if (!changed && base.state->count) {
                auto block = base.state->block_for(key);
                if (!m.blocks.contains(base.tag << 48 | block)) blocks.insert(block);
            }
        }
    } catch (std::exception const&) { return; }
    auto tasks = m.load_tasks(ledgers, true);
    for (auto block : blocks) tasks.push_back([&m, base, block] { m.block(base, block); });
    if (tasks.size() >= 2) Impl::parallel(tasks);
}
// The same ordered merge as object_page, deciding existence from version flags alone.
// Here `after` may also be empty (from the first key) and `end` empty (to the last): object_page
// lists the whole state this way.
//
// The walk looks at every key that the ranges since the base snapshot hold between the bounds,
// live or long gone, until `limit` exist; each record and snapshot key looked at is charged to
// `walk`, which fails the request with tooBusy once spent (KeyWalk) rather than answer a part.
// Nothing in the current index lets a dead run be skipped without reading it:
//  - an object table's directory holds only each 64-record block's first key and offset, and a
//    record only its own versions, so a block of dead keys is known dead only once read;
//  - a key a newer range deleted cannot be skipped in an older range wholesale either: the older
//    range's keys between two such keys may be ones the newer range never held, still live;
//  - and a dead record must be read even when it is alone, as it may hide the snapshot's key.
// The interval snapshots (base_for) already bound how many ranges a walk merges, and so which
// dead keys it can meet: those of the few ranges since the newest snapshot at or before `seq`.
// Skipping dead runs outright needs the index to say more (a format change, not made here): per
// object-table block, whether every key in it was absent at the range's start (created in the
// range), and the ledger offsets at which any of its keys exists (the union of their live
// spans, a few intervals). At `seq` a block of created keys none of which exists then can be
// passed whole in its range: none of its keys exists at `seq` (the range's version decides, and
// before a key's creation older sources say it did not exist either), and an older source's
// record for one of them still says so for itself. index-pieces would build it with the table.
std::vector<Bytes> PieceHistory::keys_between(std::uint32_t seq, Bytes const& after, Bytes const& end, unsigned limit) const {
    KeyWalk walk;
    return keys_between(seq, after, end, limit, walk);
}
std::vector<Bytes> PieceHistory::keys_between(std::uint32_t seq, Bytes const& after, Bytes const& end, unsigned limit, KeyWalk& walk) const {
    auto& m = *impl_;
    std::vector<Bytes> out;
    if (seq < m.snapshot_seq || seq > m.last || (!after.empty() && after.size() != 32) || (!end.empty() && end.size() != 32)) return out;
    auto count = m.through(seq);
    auto base = m.base_for(seq);
    std::vector<std::optional<Record>> cursors(count);
    for (std::size_t ri = base.first_range; ri < count; ++ri) {
        auto const& table = m.ranges[ri]->query->objects;
        cursors[ri] = table.at(after.empty() ? 0 : table.seek(after, true));
    }
    // The ranges' cursors ordered by their keys (the newest range first among equal keys): each
    // step costs the log of the number of ranges, not the number. A busy order book's range of
    // keys holds every price level it ever had, nearly all of them long gone.
    auto later = [&](std::size_t a, std::size_t b) { return cursors[a]->key != cursors[b]->key ? cursors[a]->key > cursors[b]->key : a < b; };
    std::vector<std::size_t> heap;
    for (std::size_t ri = base.first_range; ri < count; ++ri) if (cursors[ri]) heap.push_back(ri);
    std::make_heap(heap.begin(), heap.end(), later);
    U part = 0; std::size_t item = 0;
    std::shared_ptr<StateBlock> data;
    if (base.state->count) {
        part = after.empty() ? 0 : base.state->block_for(after); data = m.block(base, part);
        item = after.empty() ? 0 : static_cast<std::size_t>(std::upper_bound(data->items.begin(), data->items.end(), V(after), [](V k, auto const& i) { return k < i.first; }) - data->items.begin());
    }
    auto snapshot_valid = [&] {
        while (data && item >= data->items.size()) {
            if (++part >= base.state->count) { data.reset(); break; }
            data = m.block(base, part); item = 0;
        }
        return bool(data);
    };
    // Keys are compared as views: a range's into its mapped query index, the snapshot's into the block
    // `data` holds (it moves on only at the next turn, after the key is copied out). A busy book's range
    // holds mostly price levels long gone, and copying each key looked at was most of a walk's cost.
    V const stop(end);
    while (out.size() < limit) {
        std::optional<V> key;
        if (snapshot_valid()) key = data->items[item].first;
        if (!heap.empty() && (!key || cursors[heap.front()]->key < *key)) key = cursors[heap.front()]->key;
        if (!key || (!stop.empty() && *key >= stop)) break;
        // The ranges holding this key, newest first: the newest with a version at `seq` decides.
        std::optional<bool> exists;
        while (!heap.empty() && cursors[heap.front()]->key == *key) {
            walk.spend();  // before anything moves on: a spent walk throws, and nothing is kept
            std::pop_heap(heap.begin(), heap.end(), later);
            auto ri = heap.back();
            auto& c = cursors[ri];
            if (!exists)
                if (auto version = version_at(*c, seq - m.ranges[ri]->first)) exists = !version->deleted;
            c = m.ranges[ri]->query->objects.at(c->next);
            if (c) std::push_heap(heap.begin(), heap.end(), later); else heap.pop_back();
        }
        if (snapshot_valid() && data->items[item].first == *key) { walk.spend(); if (!exists) exists = true; ++item; }
        if (exists.value_or(false)) out.emplace_back(*key);
    }
    return out;
}
// The RPC layer's own failure (rpc.cpp's rpc_request answers it as given), not an Error, which
// it would answer as an internal error; nor a std::exception any fallback on the way catches.
void KeyWalk::exhausted() { rpc::fail(r::RpcTooBusy); }
// Within a book's key range the only objects are its quality directories' root pages (a directory's
// further pages and offers have hashed keys), so the keys there are the book's price levels: one
// ordered merge instead of book_offers' successor walk, one key at a time.
std::optional<std::vector<Bytes>> PieceHistory::book_directories(std::uint32_t seq, Bytes const& after, Bytes const& end, unsigned limit) const {
    if (seq < impl_->snapshot_seq || seq > impl_->last) return std::nullopt;
    return keys_between(seq, after, end, limit);
}
// The Store's nft index for the token at `seq`: its owner, URI and burned flag, and the
// ledger where its owner and burned flag became these (Clio's ledger_index: a URI change alone
// does not move it). index-pieces records, per token and ledger, the state its last page move
// left (as a fingerprint whose first half is of the owner and burned flag); only the newest
// one is decoded.
Json PieceHistory::nft(Bytes const& id, std::uint32_t seq) const {
    auto& m = *impl_;
    if (!m.nfts) throw Error("NFT state is not indexed; run `xrpla index-pieces --dir " + m.cache.string() + "`");
    if (id.size() != 32) return nullptr;
    if (seq < m.snapshot_seq || seq > m.last) throw Error("NFT state has been pruned");
    auto render = [](NftState const& s, U since) { return Json{{"owner", s.owner}, {"uri", s.uri}, {"is_burned", s.burned}, {"ledger_index", since}}; };
    std::optional<NftState> current;
    Bytes fingerprint;
    U since = 0;
    for (auto ri = m.through(seq); ri-- > 0;) {
        auto const& r = *m.ranges[ri];
        auto record = r.query->nft_states.find(id);
        if (!record) continue;
        auto events = nft_events(*record);
        for (auto e = events.rbegin(); e != events.rend(); ++e) {
            U ledger = r.first + e->ledger;
            if (ledger > seq) continue;
            if (!current) {
                auto page = m.page(ledger);
                auto const& transactions = page->at(ledger).transactions;
                if (e->position >= transactions.size()) throw Error("Transaction position outside ledger");
                auto [tx, meta] = transactions[e->position];
                current = nft_change({Bytes(tx), Bytes(meta)}, id);
                if (!current || (fingerprint = nft_fingerprint(*current)) != e->fingerprint) throw Error("NFT state differs from its index");
            } else if (e->fingerprint.substr(0, nft_holder_print) != V(fingerprint).substr(0, nft_holder_print)) return render(*current, since);
            since = ledger;
        }
    }
    auto base = m.snapshot_nft(id);
    if (!current) return base ? render(*base, m.snapshot_seq) : Json(nullptr);
    if (base && base->owner == current->owner && base->burned == current->burned) since = m.snapshot_seq;
    return render(*current, since);
}
std::optional<std::vector<Bytes>> PieceHistory::mpt_holders(Bytes const& issuance, Bytes const& after, unsigned limit) const {
    auto& m = *impl_;
    if (!m.has_holders) return std::nullopt;
    if (issuance.size() != 24 || (!after.empty() && after.size() != 20)) throw Error("Invalid MPT holder query");
    std::vector<V> runs{m.holders->entries(issuance)};
    for (auto const& r : m.ranges) runs.push_back(r->holders->entries(issuance));
    return merge_after(runs, 20, after, limit);
}
std::optional<std::vector<Bytes>> PieceHistory::nfts_by_issuer(Bytes const& issuer, std::optional<std::uint32_t> taxon, Bytes const& after, unsigned limit) const {
    auto& m = *impl_;
    if (!m.has_issuers) return std::nullopt;
    if (issuer.size() != 20 || (!after.empty() && after.size() != 32)) throw Error("Invalid NFT issuer query");
    std::vector<V> runs{m.issuers->entries(issuer)};
    for (auto const& r : m.ranges) runs.push_back(r->issuers->entries(issuer));
    if (taxon) {
        // Entries begin with the taxon: keep one taxon's run of each.
        char prefix[4]; put_be32(prefix, *taxon);
        for (auto& run : runs) {
            auto bound = [&](bool past) {
                std::size_t lo = 0, hi = run.size() / 16;
                while (lo < hi) { auto mid = (lo + hi) / 2; auto c = run.substr(mid * 16, 4).compare(V(prefix, 4)); if (c < 0 || (past && c == 0)) lo = mid + 1; else hi = mid; }
                return lo;
            };
            auto first = bound(false), end = bound(true);
            run = run.substr(first * 16, (end - first) * 16);
        }
    }
    // The cursor as an entry: its own taxon (Clio's order), or the one asked about (with a taxon,
    // Clio compares token IDs within it).
    Bytes cursor;
    if (!after.empty()) cursor = issuer_entry(after, taxon ? *taxon : r::nft::toUInt32(r::nft::getTaxon(r::uint256::fromVoid(after.data()))));
    std::vector<Bytes> ids;
    for (auto const& entry : merge_after(runs, 16, cursor, limit)) ids.push_back(token_of(entry, issuer));
    return ids;
}
// The pages holding each token's newest state change at `seq` (and the snapshot blocks of
// tokens unchanged since the snapshot) are decoded together first, on several threads.
std::vector<Json> PieceHistory::nfts(std::vector<Bytes> const& ids, std::uint32_t seq) const {
    auto& m = *impl_;
    if (m.nfts && seq >= m.snapshot_seq && seq <= m.last && ids.size() > 1) {
        std::map<Impl::SegmentId, std::set<U>> ledgers;
        std::set<U> blocks;
        try {
            for (auto const& id : ids) {
                if (id.size() != 32) continue;
                std::optional<U> newest;
                for (auto ri = m.through(seq); ri-- > 0 && !newest;) {
                    auto const& r = *m.ranges[ri];
                    auto record = r.query->nft_states.find(id);
                    if (!record) continue;
                    for (auto const& e : nft_events(*record)) if (r.first + e.ledger <= seq) newest = r.first + e.ledger;
                }
                if (newest) { ledgers[m.segment_id(*newest)].insert(*newest); continue; }
                if (auto owner = m.nfts->owner(id); owner && m.state->count) blocks.insert(m.state->block_for(Bytes(*owner) + id.substr(20)));
            }
        } catch (std::exception const&) {}
        auto tasks = m.load_tasks(ledgers, false);
        for (auto block : blocks) tasks.push_back([&m, block] { m.block(block); });
        Impl::parallel(tasks);
    }
    std::vector<Json> out;
    for (auto const& id : ids) out.push_back(nft(id, seq));
    return out;
}
// The process allocator's own figures, where it is jemalloc (linked on Linux; CMakeLists.txt): what the
// program has allocated, what the allocator keeps for it (active, resident) and has mapped or kept
// for reuse. Elsewhere, or without it, nothing.
extern "C" int mallctl(char const*, void*, std::size_t*, void*, std::size_t) __attribute__((weak));
Json allocator_stats() {
    if (!mallctl) return nullptr;
    std::uint64_t epoch = 1; std::size_t length = sizeof(epoch);
    mallctl("epoch", &epoch, &length, &epoch, length);  // refreshes the statistics below
    Json out = Json::object();
    for (auto name : {"allocated", "active", "resident", "mapped", "retained", "metadata"}) {
        std::size_t value = 0; length = sizeof(value);
        if (mallctl((std::string("stats.") + name).c_str(), &value, &length, nullptr, 0) == 0) out[name] = value;
    }
    return out;
}
Json PieceHistory::cache_stats() const {
    auto& m = *impl_;
    // What the open segments hold, summed by part (hx::Archive::memory).
    hx::Archive::Memory sum;
    std::size_t segments = 0, charged = 0;
    m.archives.each([&](OpenArchive& a) {
        std::lock_guard lock(a.mutex);
        auto part = a.archive.memory();
        sum.dictionary += part.dictionary; sum.tables += part.tables; sum.memos += part.memos; sum.memoCount += part.memoCount;
        sum.transactionPages += part.transactionPages; sum.cursors += part.cursors; charged += a.weight; ++segments;
        sum.headers += part.headers; sum.values += part.values; sum.hashes += part.hashes; sum.atoms += part.atoms;
        sum.shapes += part.shapes; sum.shapeItems += part.shapeItems; sum.shapeCount += part.shapeCount;
    });
    Json open{{"segments", segments}, {"charged_bytes", charged}, {"dictionary_bytes", sum.dictionary}, {"table_bytes", sum.tables},
              {"memo_bytes", sum.memos}, {"memos", sum.memoCount}, {"transaction_page_bytes", sum.transactionPages}, {"cursor_bytes", sum.cursors},
              {"tables", {{"header_bytes", sum.headers}, {"value_bytes", sum.values}, {"hash_bytes", sum.hashes}, {"atom_bytes", sum.atoms},
                          {"shape_bytes", sum.shapes}, {"shapes", sum.shapeCount}, {"shape_items", sum.shapeItems}}}};
    auto ms = [](std::uint64_t ns) { return static_cast<double>(ns) / 1e6; };
    return {{"budget", m.budget->stats()}, {"archives", m.archives.stats()}, {"pages", m.pages.stats()}, {"snapshot_blocks", m.blocks.stats()},
            {"load_ms", {{"segment_open", ms(m.open_ns)}, {"dictionary", ms(m.dictionary_ns)}, {"page_decode", ms(m.page_ns)}, {"snapshot_block", ms(m.block_ns)}}},
            {"dictionaries", {{"loaded", m.dictionaries.load()}, {"read_ms", ms(m.dictionary_read_ns)}, {"parse_ms", ms(m.dictionary_parse_ns)},
                              {"mean_bytes", m.dictionaries ? m.dictionary_bytes / m.dictionaries : 0}}},
            {"open_segments", open}, {"allocator", allocator_stats()}};
}

void PieceHistory::retire() const {
    auto& m = *impl_;
    m.archives.retire(); m.pages.retire(); m.blocks.retire();
}

bool is_piece_cache(fs::path const& dir) { return fs::exists(dir / "selection.json"); }
// A piece cache is opened once and kept between requests. A sync that switches selection.json
// (restore.cpp) has it opened again: the file's time is looked at most every 200 ms, and the new
// history is built on a thread of its own while requests go on with the current one, which
// they use until it is ready (one that fails is tried again 10 s later). Before, every
// request read the file's time and took one lock, under which a rebuild stalled all of them.
// A selection.json missing for a moment keeps the current history.
std::shared_ptr<PieceHistory const> open_piece_history(fs::path const& dir) {
    struct Entry {
        std::mutex mutex;
        std::shared_ptr<PieceHistory const> history;
        fs::file_time_type stamp;
        std::chrono::steady_clock::time_point next;  // selection.json's time is not looked at before
        bool building = false;
    };
    auto warm = [](std::shared_ptr<PieceHistory const> const& history) {
        // Tip queries read the newest segments first; open them without delaying the caller.
        if (auto count = warm_count.load()) std::thread([history, count] { try { history->warm(count); } catch (std::exception const&) {} }).detach();
    };
    static std::mutex mutex;
    static std::map<std::string, std::shared_ptr<Entry>> open;  // by the directory as given
    thread_local std::pair<std::string, std::shared_ptr<Entry>> last;  // this thread's last one, without the lock
    if (last.first != dir.native() || !last.second) {
        std::lock_guard lock(mutex);
        auto& entry = open[dir.string()];
        if (!entry) entry = std::make_shared<Entry>();
        last = {dir.native(), entry};
    }
    auto& entry = *last.second;
    auto now = std::chrono::steady_clock::now();
    std::unique_lock lock(entry.mutex);
    if (entry.history && (entry.building || now < entry.next)) return entry.history;
    if (!entry.history) {
        // The first open: nothing to answer with meanwhile, so the callers wait for it.
        auto stamp = fs::last_write_time(dir / "selection.json");
        entry.history = std::make_shared<PieceHistory const>(dir);
        entry.stamp = stamp; entry.next = now + std::chrono::milliseconds(200);
        warm(entry.history);
        return entry.history;
    }
    entry.next = now + std::chrono::milliseconds(200);
    std::error_code missing;
    auto stamp = fs::last_write_time(dir / "selection.json", missing);
    if (missing || stamp == entry.stamp) return entry.history;
    entry.building = true;
    std::thread([target = last.second, dir, stamp, warm] {
        std::shared_ptr<PieceHistory const> fresh;
        try { fresh = std::make_shared<PieceHistory const>(dir); }
        catch (std::exception const& error) { std::cerr << "piece cache " << dir.string() << ": reopening: " << error.what() << std::endl; }
        std::shared_ptr<PieceHistory const> replaced;
        {
            std::lock_guard lock(target->mutex);
            target->building = false;
            if (!fresh) { target->next = std::chrono::steady_clock::now() + std::chrono::seconds(10); return; }
            replaced = std::exchange(target->history, fresh); target->stamp = stamp;
        }
        // Both share the directory's budget (shared_budget): the replaced history gives back what
        // it holds, so the fresh one's caches and warm have it, rather than the two holding twice
        // --cache until the requests (and a live tail) holding the replaced one let it go.
        if (replaced && replaced != fresh) replaced->retire();
        warm(fresh);
    }).detach();
    return entry.history;
}
}
