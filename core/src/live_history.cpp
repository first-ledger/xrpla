// A live tail over a piece cache (live_history.hpp).
#include "live_history.hpp"
#include "piece_format.hpp"
#include <xrpla/core/format.hpp>
#include <xrpla/core/operations.hpp>
#include <algorithm>
#include <array>
#include <cstring>
#include <exception>
#include <future>
#include <limits>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <cctype>
#include <chrono>
#include <iostream>
#ifdef __linux__
#include <fcntl.h>  // a spool chunk opened once: Impl::OpenFile
#include <pthread.h>
#include <unistd.h>
#endif

namespace xrpla::core {
namespace {
// The tail is indexed in memory for as long as the publication takes to seal it into pieces, so
// its entries are kept small: keys and hashes inline as 32 bytes (an account zero-padded), each
// transaction hash once in one table, referred to elsewhere by its 4-byte id.
using Key = std::array<unsigned char, 32>;
Key key_of(Bytes const& b) {
    if (b.size() > 32) throw Error("Key longer than 32 bytes");
    Key k{}; std::memcpy(k.data(), b.data(), b.size()); return k;
}
std::optional<Key> find_key(Bytes const& b) { if (b.size() > 32) return std::nullopt; return key_of(b); }
Bytes bytes_of(Key const& k) { return Bytes(reinterpret_cast<char const*>(k.data()), k.size()); }
// Keys are hashes, except NFT IDs and padded accounts, and clients name the ones they look up:
// all four words count, mixed with a key drawn per process (SeededKeyHash).
using KeyHash = SeededKeyHash;
struct Posting { std::uint32_t ledger, index, tx; };  // TransactionIndex, transaction id
struct Event { std::uint32_t ledger, position; Bytes fingerprint; };
struct TailLedger {
    Bytes header;
    Key hash{};
    std::uint32_t first_tx = 0;           // its transactions' ids, in record order, from here
    std::vector<std::uint32_t> in_order;  // ... and in TransactionIndex order
};
// An object version: the ledger, with this bit set when the ledger deleted the object.
constexpr std::uint32_t deleted_bit = 1u << 31;
std::uint32_t version_ledger(std::uint32_t v) { return v & ~deleted_bit; }
using Records = std::map<std::uint32_t, LedgerRecord>;
Json render(NftState const& s, std::uint32_t since) { return {{"owner", s.owner}, {"uri", s.uri}, {"is_burned", s.burned}, {"ledger_index", since}}; }
}

// A shared mutex that lets a waiting writer in ahead of new readers (glibc's default lets
// readers keep it): refresh() adds the network's new ledgers within the time of the requests
// already running, however many more keep arriving.
class WriterFirstMutex {
#ifdef __linux__
    pthread_rwlock_t lock_;
public:
    WriterFirstMutex() {
        pthread_rwlockattr_t attributes;
        pthread_rwlockattr_init(&attributes);
        pthread_rwlockattr_setkind_np(&attributes, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
        pthread_rwlock_init(&lock_, &attributes);
        pthread_rwlockattr_destroy(&attributes);
    }
    ~WriterFirstMutex() { pthread_rwlock_destroy(&lock_); }
    void lock() { pthread_rwlock_wrlock(&lock_); }
    void unlock() { pthread_rwlock_unlock(&lock_); }
    void lock_shared() { pthread_rwlock_rdlock(&lock_); }
    void unlock_shared() { pthread_rwlock_unlock(&lock_); }
#else
    std::shared_mutex lock_;
public:
    void lock() { lock_.lock(); }
    void unlock() { lock_.unlock(); }
    void lock_shared() { lock_.lock_shared(); }
    void unlock_shared() { lock_.unlock_shared(); }
#endif
    WriterFirstMutex(WriterFirstMutex const&) = delete;
    WriterFirstMutex& operator=(WriterFirstMutex const&) = delete;
};

struct LiveHistory::Impl {
    std::shared_ptr<PieceHistory const> base;
    fs::path spool;
    std::string key;
    std::uint32_t base_last = 0;
    std::string base_last_hash;
    mutable WriterFirstMutex mutex;  // guards everything below; base_last never changes
    std::uint32_t last = 0;           // the tail is (base_last, last]
    std::map<std::uint32_t, TailLedger> ledgers;
    std::unordered_map<Key, std::uint32_t, KeyHash> by_hash;
    std::vector<Key> tx_hashes;                                       // by transaction id
    std::vector<std::pair<std::uint32_t, std::uint32_t>> tx_where;    // ... its (ledger, record position)
    std::unordered_map<Key, std::uint32_t, KeyHash> transactions;     // hash -> transaction id
    std::map<Key, std::vector<std::uint32_t>> versions;               // key -> versions, ascending (ordered for object_page)
    std::unordered_map<Key, std::vector<Posting>, KeyHash> accounts, nft_postings;
    std::map<Bytes, std::vector<Event>> nft_states;
    // The optional indexes' tail part: MPToken holders by issuance, NFTs-by-issuer entries by issuer.
    std::map<Bytes, std::set<Bytes>> holders, issuer_entries;
    std::vector<Chunk> chunks;
    // Decoded spool chunks by file, most recently used first (a follower merging chunks
    // writes a new file for ledgers an older one held), with their decoded bytes; and the
    // chunks being decoded, which a request for the same one waits for rather than decoding
    // its own copy.
    struct Cached { std::string path; std::shared_ptr<Records> records; std::uint64_t bytes; };
    mutable std::mutex cache_mutex;
    mutable std::list<Cached> cache;
    mutable std::uint64_t cache_bytes = 0;
    mutable std::map<std::string, std::shared_future<std::shared_ptr<Records>>> decoding;
    static constexpr std::size_t cache_chunks = 8;
    static constexpr std::uint64_t cache_limit = 512ull << 20;

    // The spools rotations retired beside this one (export.cpp's rotate_spool), newest first:
    // they are named by when they were retired. The service keeps one a day, for readers of its
    // files, and until the piece cache holds the ledgers before the spool's first.
    std::vector<fs::path> retired_spools() const {
        std::vector<fs::path> retired;
        std::error_code ignored;
        auto prefix = spool.filename().string() + ".retired-";
        for (auto const& entry : fs::directory_iterator(spool.parent_path(), ignored))
            if (entry.path().filename().string().starts_with(prefix)) retired.push_back(entry.path());
        std::sort(retired.rbegin(), retired.rend());
        return retired;
    }
    // Where a chunk's file is: in the spool, or in the newest retired spool holding it.
    fs::path chunk_path(Chunk const& chunk) const {
        auto here = spool / chunk.file.object;
        if (fs::exists(here)) return here;
        for (auto const& dir : retired_spools()) if (fs::exists(dir / chunk.file.object)) return dir / chunk.file.object;
        return here;
    }
    // The chunks before ledger `before` that continue the pieces, from the retired spools whose
    // signed manifests list them, newest first: each spool's chunks reaching the first of those
    // a newer one gave (a cache short of several rotations' spools needs each). For a tail built
    // after a rotation (a restart, or one rebuilt over a cache still short of the rotated spool's
    // first ledger), which knows no chunks of the spools it replaced. None unless they reach the
    // pieces; their files are then found as any retired chunk's.
    std::vector<Chunk> retired_chunks(std::uint32_t before) const {
        std::vector<Chunk> stitched;
        for (auto const& dir : retired_spools()) {
            if (before <= base_last + 1) break;
            std::vector<Chunk> found;
            try {
                for (auto const& chunk : read_manifest(Source(dir.string()), key).manifest.chunks)
                    if (chunk.last > base_last && chunk.first < before) found.push_back(chunk);
            } catch (std::exception const&) { continue; }  // one being removed, or not a spool
            if (found.empty() || found.back().last + 1 < before) continue;  // a gap before the newer ones
            stitched.insert(stitched.begin(), found.begin(), found.end());
            before = found.front().first;
        }
        if (before > base_last + 1) return {};
        return stitched;
    }
    std::shared_ptr<Records> chunk_records(Chunk const& chunk) const {
        auto path = chunk_path(chunk).string();
        std::promise<std::shared_ptr<Records>> promise;
        {
            std::unique_lock lock(cache_mutex);
            for (auto it = cache.begin(); it != cache.end(); ++it)
                if (it->path == path) { cache.splice(cache.begin(), cache, it); return cache.front().records; }
            if (auto it = decoding.find(path); it != decoding.end()) {
                auto pending = it->second;
                lock.unlock();
                return pending.get();
            }
            decoding.emplace(path, promise.get_future().share());
        }
        std::shared_ptr<Records> records;
        std::uint64_t bytes = 0;
        try {
            records = std::make_shared<Records>();
            load_chunk(chunk, path, [&](LedgerRecord&& record) {
                bytes += held_bytes(record);
                auto seq = record.sequence;
                records->emplace(seq, std::move(record));
            });
        } catch (...) {
            std::lock_guard lock(cache_mutex);
            decoding.erase(path);
            promise.set_exception(std::current_exception());
            throw;
        }
        std::lock_guard lock(cache_mutex);
        decoding.erase(path);
        promise.set_value(records);
        cache.push_front({path, records, bytes});
        cache_bytes += bytes;
        // A request at the tip reads a few recent chunks; one over older tail ledgers reads more.
        // The newest is kept whatever its size.
        while (cache.size() > 1 && (cache.size() > cache_chunks || cache_bytes > cache_limit)) { cache_bytes -= cache.back().bytes; cache.pop_back(); }
        return records;
    }
    // A chunk's file as the signed manifest names it (its SHA-256), checked before its ledgers
    // are read: the headers' parent links and last hash tie the chain, this its transactions
    // and objects. The file is opened once and both the hash and read_chunk read that opened
    // file (on Linux through /proc/self/fd), so a file replaced or moved meanwhile (a merge, a
    // rotation's renames) can never have one file's ledgers pass another's check. read_chunk
    // takes a path, not a buffer, so the file is still read for the hash and again to decode
    // (the second read from the page cache). Where /proc/self/fd cannot be read (not Linux, or
    // /proc not mounted) both read the path again, and that guarantee is lost: a file replaced
    // between the two reads can still be refused only by the chain's links and the manifest's
    // last hash (OpenFile says so once).
    // A failure names the chunk by its path, not by the opened file's /proc/self/fd name.
    static void load_chunk(Chunk const& chunk, fs::path const& path, std::function<void(LedgerRecord&&)> const& visit) {
        OpenFile file(path);
        try {
            if (!chunk.file.sha256.empty() && file_sha256(file.path) != chunk.file.sha256) throw Error("Spool chunk " + path.string() + " differs from its manifest");
            read_chunk(file.path, {}, visit);
        } catch (std::exception const& e) {
            auto const opened = file.path.string();
            std::string message = e.what();
            if (opened == path.string() || message.find(opened) == std::string::npos) throw;
            for (auto at = message.find(opened); at != std::string::npos; at = message.find(opened, at + path.string().size()))
                message.replace(at, opened.size(), path.string());
            throw Error(message);
        }
    }
    // A file held open for load_chunk: `path` names the opened file itself where the system
    // can (Linux's /proc/self/fd, when /proc is mounted), else the path it was opened by (said
    // once a process, as the chunks are then read twice by their paths).
    struct OpenFile {
        fs::path path;
        int fd = -1;
        explicit OpenFile(fs::path const& file) : path(file) {
#ifdef __linux__
            fd = ::open(file.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd < 0) throw Error("Cannot open spool chunk " + file.string());
            auto opened = "/proc/self/fd/" + std::to_string(fd);
            if (::access(opened.c_str(), R_OK) == 0) { path = opened; return; }
#endif
            static std::once_flag said;
            std::call_once(said, [] {
                std::cerr << Json{{"event", "spool_chunks_read_by_path"},
                                  {"reason", "/proc/self/fd cannot be read: a chunk's hash and its ledgers are read from its path separately"}}.dump() << std::endl;
            });
        }
        ~OpenFile() { if (fd >= 0) ::close(fd); }
        OpenFile(OpenFile const&) = delete;
        OpenFile& operator=(OpenFile const&) = delete;
    };
    // What a decoded ledger costs the chunk cache: its bytes and what holds them (the map node,
    // the vectors' slots, each string's buffer and the allocator's overhead per allocation). A
    // ledger of small transactions and objects holds about as much again in these as in its bytes.
    static std::uint64_t held_bytes(LedgerRecord const& record) {
        constexpr std::uint64_t allocation = 16;  // the allocator's header and rounding, about
        // A string's buffer, unless it is held inline (as long as an empty string's capacity).
        static std::size_t const inline_capacity = Bytes().capacity();
        auto buffer = [](Bytes const& s) -> std::uint64_t { return s.capacity() <= inline_capacity ? 0 : s.capacity() + 1 + allocation; };
        std::uint64_t bytes = sizeof(Records::value_type) + 32 + allocation  // the map's node and its links
            + buffer(record.header)
            + record.transactions.capacity() * sizeof(TransactionRecord) + allocation
            + record.patches.capacity() * sizeof(ObjectRecord) + allocation;
        for (auto const& tx : record.transactions) bytes += buffer(tx.transaction) + buffer(tx.metadata);
        for (auto const& patch : record.patches) bytes += buffer(patch.key) + buffer(patch.object);
        return bytes;
    }
    // The chunk holding a tail ledger; the caller holds `mutex` (shared).
    Chunk chunk_of(std::uint32_t seq) const {
        auto it = std::upper_bound(chunks.begin(), chunks.end(), seq, [](std::uint32_t n, Chunk const& c) { return n < c.first; });
        if (it == chunks.begin() || seq > (it - 1)->last) throw Error("Ledger missing from the spool");
        return *(it - 1);
    }
    // A tail ledger's record from its chunk (found under `mutex`), read without the lock: a
    // chunk's file keeps its ledgers while the follower merges or rotates (a retired spool is kept
    // for its readers), so decoding it never holds up refresh() or the requests behind it.
    std::pair<std::shared_ptr<Records>, LedgerRecord const*> record_in(Chunk const& chunk, std::uint32_t seq) const {
        std::shared_ptr<Records> records;
        try { records = chunk_records(chunk); }
        catch (std::exception const&) {
            if (fs::exists(chunk_path(chunk))) throw;
            records = chunk_records(rotated_chunk(seq));
        }
        auto found = records->find(seq);
        if (found == records->end()) throw Error("Ledger missing from its spool chunk");
        return {records, &found->second};
    }
    // An object's value in the tail ledger of `version` (not a deletion), read without the lock.
    Bytes value_in(Chunk const& chunk, Bytes const& key, std::uint32_t version) const {
        auto [holder, record] = record_in(chunk, version_ledger(version));
        auto patch = std::lower_bound(record->patches.begin(), record->patches.end(), key, [](ObjectRecord const& p, Bytes const& k) { return p.key < k; });
        if (patch == record->patches.end() || patch->key != key) throw Error("Object version missing from its spool ledger");
        return patch->object;
    }
    // A rotation moves a new spool, cut into other chunks, into this path (export.cpp's
    // rotate_spool): until refresh() adopts its chunk list, a ledger is found through its
    // manifest. Tried again for about half a second, as the path is empty between the rotation's
    // two renames (the directory is synced between them).
    Chunk rotated_chunk(std::uint32_t seq) const {
        for (unsigned attempt = 0;; ++attempt) {
            try {
                for (auto const& chunk : read_manifest(Source(spool.string()), key).manifest.chunks)
                    if (chunk.first <= seq && seq <= chunk.last) return chunk;
                throw Error("Ledger missing from the spool");
            } catch (std::exception const&) {
                if (attempt == 25) throw;
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
    }
    // A ledger's index entries, worked out from its record without the lock (prepare: every
    // transaction and object is parsed) and then added under it (add: the entries are only moved
    // in). Its transactions are numbered from its first here (ledger.first_tx 0, in_order, the
    // postings' ids): their ids are known once it is added.
    struct Indexed {
        std::uint32_t sequence = 0;
        TailLedger ledger;
        std::vector<Key> tx_hashes;  // by record position
        std::vector<std::pair<Key, std::vector<Posting>>> accounts, nfts;  // each by TransactionIndex
        std::vector<std::pair<Bytes, Event>> nft_states;
        std::vector<std::pair<Bytes, Bytes>> issuer_entries, holders;  // (issuer, entry), (issuance, holder)
        std::vector<std::pair<Key, std::uint32_t>> versions;
    };
    // Whatever can fail (a malformed key, transaction or object) fails here, before any map
    // changes, so a ledger that cannot be indexed leaves the tail as it was (refresh() continues
    // from the ledger before it, and no ledger's postings or versions are ever added twice).
    static Indexed prepare(LedgerRecord const& record) {
        auto seq = record.sequence;
        if (seq >= deleted_bit) throw Error("Ledger sequence out of range");
        Indexed out;
        out.sequence = seq;
        out.ledger.header = record.header;
        out.ledger.hash = key_of(from_hex(decode_header(record.header).hash));
        auto indexes = index_transactions(record);
        std::vector<std::set<Bytes>> named(indexes.size());
        std::vector<std::map<Bytes, NftState>> changes(indexes.size());
        for (std::uint32_t pos = 0; pos < indexes.size(); ++pos) {
            named[pos] = nft_ids(record.transactions[pos]);
            if (named[pos].empty()) continue;
            changes[pos] = nft_changes(record.transactions[pos]);
            for (auto const& token : named[pos]) out.issuer_entries.push_back(nft_issuer_entry(token));
        }
        std::vector<std::pair<std::uint32_t, std::uint32_t>> order;
        std::map<Key, std::vector<Posting>> ledger_accounts, ledger_nfts;
        std::map<Bytes, std::tuple<std::uint32_t, std::uint32_t, Bytes>> moved;  // token -> (TransactionIndex, position, fingerprint)
        for (std::uint32_t pos = 0; pos < indexes.size(); ++pos) {
            auto const& tx = indexes[pos];
            out.tx_hashes.push_back(key_of(tx.hash));
            order.emplace_back(tx.position, pos);
            for (auto const& account : tx.accounts) ledger_accounts[key_of(account)].push_back({seq, tx.position, pos});
            for (auto const& token : named[pos]) {
                ledger_nfts[key_of(token)].push_back({seq, tx.position, pos});
                if (auto state = changes[pos].find(token); state != changes[pos].end()) {
                    auto [slot, fresh] = moved.try_emplace(token, tx.position, pos, nft_fingerprint(state->second));
                    if (!fresh && std::get<0>(slot->second) < tx.position) slot->second = {tx.position, pos, nft_fingerprint(state->second)};
                }
            }
        }
        std::sort(order.begin(), order.end());
        out.ledger.in_order.reserve(order.size());
        for (auto const& [index, pos] : order) out.ledger.in_order.push_back(pos);
        auto by_index = [](Posting const& a, Posting const& b) { return a.index < b.index; };
        for (auto& [account, list] : ledger_accounts) { std::sort(list.begin(), list.end(), by_index); out.accounts.emplace_back(account, std::move(list)); }
        for (auto& [id, list] : ledger_nfts) { std::sort(list.begin(), list.end(), by_index); out.nfts.emplace_back(id, std::move(list)); }
        for (auto& [id, change] : moved) out.nft_states.emplace_back(id, Event{seq, std::get<1>(change), std::move(std::get<2>(change))});
        for (auto const& patch : record.patches) {
            out.versions.emplace_back(key_of(patch.key), seq | (patch.object.empty() ? deleted_bit : 0));
            if (!patch.object.empty()) if (auto holder = mptoken_holder(patch.object)) out.holders.push_back(std::move(*holder));
        }
        return out;
    }
    // A prepared ledger added to the tail's maps; the caller holds `mutex` (unique).
    void add(Indexed&& entries) {
        auto seq = entries.sequence;
        auto first_tx = static_cast<std::uint32_t>(tx_hashes.size());
        for (std::uint32_t pos = 0; pos < entries.tx_hashes.size(); ++pos) {
            tx_hashes.push_back(entries.tx_hashes[pos]);
            tx_where.emplace_back(seq, pos);
            // A reused ID (a pseudo-transaction repeated: Store::append) answers its earliest ledger, as xrpld.
            transactions.try_emplace(entries.tx_hashes[pos], first_tx + pos);
        }
        auto post = [first_tx](auto& table, auto& lists) {
            for (auto& [key, list] : lists) {
                auto& all = table[key];
                for (auto p : list) { p.tx += first_tx; all.push_back(p); }
            }
        };
        post(accounts, entries.accounts);
        post(nft_postings, entries.nfts);
        for (auto& [id, event] : entries.nft_states) nft_states[id].push_back(std::move(event));
        for (auto& [issuer, entry] : entries.issuer_entries) issuer_entries[issuer].insert(std::move(entry));
        for (auto const& [key, version] : entries.versions) versions[key].push_back(version);
        for (auto& [issuance, holder] : entries.holders) holders[issuance].insert(std::move(holder));
        auto& ledger = entries.ledger;
        ledger.first_tx = first_tx;
        for (auto& id : ledger.in_order) id += first_tx;
        by_hash[ledger.hash] = seq;
        ledgers.emplace(seq, std::move(ledger));
    }
    Bytes tx_hash(std::uint32_t id) const { return bytes_of(tx_hashes.at(id)); }
    // The newest tail version of `key` at or before `seq`, or none.
    std::optional<std::uint32_t> tail_version(Bytes const& key, std::uint32_t seq) const {
        auto k = find_key(key);
        if (!k) return std::nullopt;
        auto it = versions.find(*k);
        if (it == versions.end()) return std::nullopt;
        auto const& list = it->second;
        auto v = std::upper_bound(list.begin(), list.end(), seq, [](std::uint32_t n, std::uint32_t e) { return n < version_ledger(e); });
        if (v == list.begin()) return std::nullopt;
        return *(v - 1);
    }
    bool in_tail(std::uint32_t seq) const { return seq > base_last && seq <= last; }
    // `key`'s newest tail version at or before `seq` and where its value is, found under `mutex`
    // (shared); its value is read after the lock is released (value()). A tail ledger's versions
    // never change once indexed: refresh() only adds newer ones.
    struct Found { std::uint32_t version; std::optional<Chunk> chunk; };
    std::optional<Found> find(Bytes const& key, std::uint32_t seq) const {
        auto v = tail_version(key, seq);
        if (!v) return std::nullopt;
        if (*v & deleted_bit) return Found{*v, std::nullopt};
        return Found{*v, chunk_of(version_ledger(*v))};
    }
    Bytes value(Bytes const& key, Found const& found) const { return found.chunk ? value_in(*found.chunk, key, found.version) : Bytes(); }
};

LiveHistory::LiveHistory(std::shared_ptr<PieceHistory const> base, fs::path const& spool, fs::path const& spool_key) : impl_(std::make_unique<Impl>()) {
    auto& m = *impl_;
    m.base = std::move(base);
    // Without a trailing separator ("spool/" is "spool"): the retired spools beside it are
    // found by its name.
    m.spool = spool.lexically_normal();
    if (!m.spool.has_filename()) m.spool = m.spool.parent_path();
    m.key = read_file(spool_key, 128);
    auto status = m.base->status();
    m.base_last = status.at("complete_ledgers").at("last");
    m.base_last_hash = status.at("last_ledger_hash");
    m.last = m.base_last;
    refresh();
}
LiveHistory::~LiveHistory() = default;

std::uint32_t LiveHistory::refresh() {
    auto& m = *impl_;
    if (!fs::exists(m.spool / "manifest.json")) return m.last;
    auto manifest = read_manifest(Source(m.spool.string()), m.key).manifest;
    std::vector<Chunk> covering;
    for (auto const& chunk : manifest.chunks) if (chunk.last > m.base_last) covering.push_back(chunk);
    std::uint32_t known;
    bool same_chunks;
    {
        std::shared_lock lock(m.mutex);
        known = m.last;
        // A rotated spool starts at a newer snapshot: the tail's ledgers before it stay in the
        // chunks already known (their files now in the retired spool) until the tail is rebuilt
        // over pieces that hold them. A tail that knows none reads them from the retired spool's
        // manifest (retired_chunks).
        if (!covering.empty() && covering.front().first > m.base_last + 1) {
            std::vector<Chunk> kept;
            for (auto const& chunk : m.chunks) if (chunk.first < covering.front().first) kept.push_back(chunk);
            if (kept.empty()) kept = m.retired_chunks(covering.front().first);
            covering.insert(covering.begin(), kept.begin(), kept.end());
        }
        same_chunks = std::equal(covering.begin(), covering.end(), m.chunks.begin(), m.chunks.end(),
                                 [](Chunk const& a, Chunk const& b) { return a.file.object == b.file.object; });
    }
    if (manifest.tip <= known && same_chunks) return known;
    // The new ledgers are decoded outside the lock and indexed one chunk at a time: a tail built
    // over an old publication (a restart while it has not advanced) can span hundreds of
    // thousands of ledgers, far more than fit in memory decoded at once. A failure keeps what was
    // indexed; the next refresh continues from there.
    auto expected = known + 1;
    std::string parent;
    {
        std::shared_lock lock(m.mutex);
        parent = known == m.base_last ? m.base_last_hash : to_hex(bytes_of(m.ledgers.at(known).hash));
    }
    bool adopted = false;
    for (auto const& chunk : covering) {
        if (chunk.last < expected) continue;
        std::vector<LedgerRecord> fresh;
        Impl::load_chunk(chunk, m.chunk_path(chunk), [&](LedgerRecord&& record) { if (record.sequence >= expected) fresh.push_back(std::move(record)); });
        std::sort(fresh.begin(), fresh.end(), [](auto const& a, auto const& b) { return a.sequence < b.sequence; });
        for (auto const& record : fresh) {
            if (record.sequence != expected++) throw Error("Spool ledgers are not contiguous with the pieces");
            auto header = decode_header(record.header);
            if (!parent.empty() && header.parent_hash != parent) throw Error("Spool does not continue the sealed history");
            parent = header.hash;
        }
        // The parent links tie these ledgers back to the sealed history; the signed manifest's
        // hash of the chunk's last ledger ties them forward, so a chunk file that changed after
        // the follower wrote it is refused rather than served.
        if (!fresh.empty() && fresh.back().sequence == chunk.last) {
            auto signed_hash = chunk.last_ledger_hash;
            std::transform(signed_hash.begin(), signed_hash.end(), signed_hash.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (parent != signed_hash) throw Error("Spool chunk " + m.chunk_path(chunk).string() + " differs from its manifest");
        }
        // The ledgers' entries are worked out before the lock is taken (parsing every transaction
        // and object of a chunk would hold up every tail read), and only added under it. One that
        // fails to index stops the chunk there: the ones before it are added, and the next refresh
        // starts from it, not again from the chunk's first (which would post the ones before twice).
        std::vector<Impl::Indexed> indexed;
        indexed.reserve(fresh.size());
        std::exception_ptr failed;
        for (auto const& record : fresh) {
            try { indexed.push_back(Impl::prepare(record)); }
            catch (...) { failed = std::current_exception(); break; }
        }
        {
            std::unique_lock lock(m.mutex);
            if (!adopted) { m.chunks = covering; adopted = true; }
            for (auto& ledger : indexed) { auto seq = ledger.sequence; m.add(std::move(ledger)); m.last = seq; }
        }
        if (failed) std::rethrow_exception(failed);
    }
    std::unique_lock lock(m.mutex);
    m.chunks = std::move(covering);
    return m.last;
}

Json LiveHistory::status() const {
    auto& m = *impl_;
    auto result = m.base->status();
    std::shared_lock lock(m.mutex);
    if (m.last > m.base_last) {
        result["complete_ledgers"]["last"] = m.last;
        result["state_ledgers"]["last"] = m.last;
        result["last_ledger_hash"] = to_hex(bytes_of(m.ledgers.at(m.last).hash));
        result["tail"] = {{"first", m.base_last + 1}, {"last", m.last}, {"ledgers", m.last - m.base_last}};
    }
    return result;
}
std::optional<std::uint32_t> LiveHistory::baseline_ledger() const { return impl_->base->baseline_ledger(); }
// The base's first, and the tail's newest indexed ledger (raised only once a ledger is indexed).
std::optional<std::pair<std::uint32_t, std::uint32_t>> LiveHistory::served_ledgers() const {
    auto& m = *impl_;
    auto served = m.base->served_ledgers();
    if (!served) return served;
    std::shared_lock lock(m.mutex);
    if (m.last > m.base_last) served->second = static_cast<std::uint32_t>(m.last);
    return served;
}
// Ledgers up to base_last (fixed for a tail) are read from the base without the tail's lock:
// slow history reads never hold up refresh().
std::vector<TransactionRecord> LiveHistory::ledger_transaction_records(std::uint32_t seq) const {
    if (seq <= impl_->base_last) return impl_->base->ledger_transaction_records(seq);
    {
        std::shared_lock lock(impl_->mutex);
        if (!impl_->in_tail(seq)) return {};
    }
    return ledger_record(seq).transactions;
}
Bytes LiveHistory::header(std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->header(seq);
    std::shared_lock lock(m.mutex);
    if (!m.in_tail(seq)) return seq > m.last ? Bytes() : m.base->header(seq);
    return m.ledgers.at(seq).header;
}
std::uint32_t LiveHistory::ledger_sequence(Bytes const& hash) const {
    auto& m = *impl_;
    {
        std::shared_lock lock(m.mutex);
        if (auto k = find_key(hash))
            if (auto it = m.by_hash.find(*k); it != m.by_hash.end()) return it->second;
    }
    return m.base->ledger_sequence(hash);
}
Bytes LiveHistory::object(Bytes const& key, std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->object(key, seq);
    std::optional<Impl::Found> found;
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) throw Error("Ledger state has been pruned");
        found = m.find(key, seq);
    }
    return found ? m.value(key, *found) : m.base->object(key, m.base_last);
}
std::vector<Bytes> LiveHistory::objects(std::vector<Bytes> const& keys, std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->objects(keys, seq);
    // The tail's versions first; the rest from the base in one bulk read.
    std::vector<Bytes> out(keys.size()), rest;
    std::vector<std::size_t> where;
    std::vector<std::pair<std::size_t, Impl::Found>> tail;
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) throw Error("Ledger state has been pruned");
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (auto found = m.find(keys[i], seq)) tail.emplace_back(i, std::move(*found));
            else { rest.push_back(keys[i]); where.push_back(i); }
        }
    }
    // By ledger, so each chunk is decoded once while it is in the cache.
    std::sort(tail.begin(), tail.end(), [](auto const& a, auto const& b) { return version_ledger(a.second.version) < version_ledger(b.second.version); });
    for (auto const& [i, found] : tail) out[i] = m.value(keys[i], found);
    auto base = m.base->objects(rest, m.base_last);
    for (std::size_t j = 0; j < where.size(); ++j) out[where[j]] = std::move(base[j]);
    return out;
}
std::optional<std::pair<std::uint32_t, Bytes>> LiveHistory::deleted_object(Bytes const& key, std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->deleted_object(key, seq);
    std::optional<std::uint32_t> v;
    std::optional<Impl::Found> previous;  // the tail's version before the deletion, if any
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) throw Error("Ledger state has been pruned");
        v = m.tail_version(key, seq);
        if (v && (*v & deleted_bit) && version_ledger(*v) - 1 > m.base_last) previous = m.find(key, version_ledger(*v) - 1);
    }
    if (!v) return m.base->deleted_object(key, m.base_last);
    if (!(*v & deleted_bit)) return std::nullopt;
    auto deleted_at = version_ledger(*v);
    Bytes before;
    if (deleted_at - 1 <= m.base_last) before = m.base->object(key, deleted_at - 1);
    else if (previous) before = m.value(key, *previous);
    else before = m.base->object(key, m.base_last);
    if (before.empty()) return std::nullopt;
    return std::pair{deleted_at, std::move(before)};
}
Json LiveHistory::object_page(std::uint32_t seq, std::string const& marker, unsigned limit, bool binary) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->object_page(seq, marker, limit, binary);
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) throw Error("Ledger state has been pruned");
    }
    if (!limit || limit > 4096) throw Error("Object page limit must be between 1 and 4096");
    Bytes after;
    if (!marker.empty()) { after = from_hex(marker); if (after.size() != 32) throw Error("Invalid object marker"); }
    // An ordered merge of the base's keys at its last ledger and the tail's changed keys, the
    // whole of it bounded by one walk (KeyWalk, as keys_between below): the base's pages charge
    // their listings to it, and each tail change looked at is charged too.
    KeyWalk walk;
    std::vector<std::pair<Bytes, Bytes>> base_items;
    std::size_t next_base = 0;
    std::string base_marker = marker;
    bool base_done = false;
    auto refill = [&] {
        base_items.clear(); next_base = 0;
        if (base_done) return;
        auto page = m.base->object_page(m.base_last, base_marker, 256, true, walk);
        for (auto const& item : page.at("state")) base_items.emplace_back(from_hex(item.at("index").get<std::string>()), from_hex(item.at("data").get<std::string>()));
        if (page.contains("marker")) base_marker = page.at("marker"); else base_done = true;
    };
    refill();
    // The tail's changed keys after the marker, a batch at a time under the lock.
    std::vector<std::pair<Bytes, std::optional<Impl::Found>>> tail_items;
    std::size_t next_tail = 0;
    std::optional<Key> tail_after = after.empty() ? std::nullopt : std::optional<Key>(key_of(after));
    bool tail_done = false;
    auto more_tail = [&] {
        tail_items.clear(); next_tail = 0;
        if (tail_done) return;
        std::shared_lock lock(m.mutex);
        auto it = tail_after ? m.versions.upper_bound(*tail_after) : m.versions.begin();
        for (; it != m.versions.end() && tail_items.size() < 256; ++it) {
            auto key = bytes_of(it->first);
            tail_items.emplace_back(key, m.find(key, seq));
        }
        if (it == m.versions.end()) tail_done = true;
        if (!tail_items.empty()) tail_after = key_of(tail_items.back().first);
    };
    more_tail();
    Json items = Json::array();
    std::string last_key;
    bool more = false;
    for (;;) {
        if (next_base == base_items.size() && !base_done) refill();
        if (next_tail == tail_items.size() && !tail_done) more_tail();
        bool has_base = next_base < base_items.size(), has_tail = next_tail < tail_items.size();
        if (!has_base && !has_tail) break;
        Bytes tail_key = has_tail ? tail_items[next_tail].first : Bytes();
        Bytes key = !has_tail || (has_base && base_items[next_base].first < tail_key) ? base_items[next_base].first : tail_key;
        std::optional<Bytes> value;
        if (has_tail && tail_key == key) {
            walk.spend();
            if (auto const& found = tail_items[next_tail].second) value = m.value(key, *found);
            ++next_tail;
        }
        if (has_base && base_items[next_base].first == key) { if (!value) value = std::move(base_items[next_base].second); ++next_base; }
        if (!value || value->empty()) continue;
        if (items.size() == limit) { more = true; break; }
        if (binary) items.push_back({{"index", to_hex(key)}, {"data", to_hex(*value)}});
        else { auto object = object_json({key, *value}); object["index"] = to_hex(key); items.push_back(object); }
        last_key = to_hex(key);
    }
    Json result{{"state", items}};
    if (more) result["marker"] = last_key;
    return result;
}
Json LiveHistory::history_page(std::string const& column, Bytes const& key, std::uint32_t first,
    std::uint32_t last, unsigned limit, bool forward, Json const& marker) const {
    auto& m = *impl_;
    if (last <= m.base_last || (column != "account_tx" && column != "nft_tx")) return m.base->history_page(column, key, first, last, limit, forward, marker);
    if (!limit || limit > 1000 || first > last) throw Error("Invalid history page bounds");
    std::optional<std::pair<std::uint64_t, std::uint64_t>> cursor;
    if (!marker.is_null()) {
        if (!marker.is_object() || !marker.contains("ledger") || !marker.contains("seq") ||
            !marker["ledger"].is_number_unsigned() || !marker["seq"].is_number_unsigned()) throw Error("Invalid history marker");
        cursor = std::pair{marker.at("ledger").get<std::uint64_t>(), marker.at("seq").get<std::uint64_t>()};
        if (cursor->first < first || cursor->first > last) throw Error("History marker is outside range");
    }
    bool base_part = first <= m.base_last;
    Json items = Json::array(), next = nullptr;
    // The tail's postings for the page, in page order with their hashes, taken under the lock
    // (at most one more than the page holds); the base is read after it is released.
    struct Listed { std::uint32_t ledger, index; Bytes hash; };
    std::vector<Listed> listed;
    if (forward || !(cursor && cursor->first <= m.base_last)) {
        std::shared_lock lock(m.mutex);
        auto const& table = column == "account_tx" ? m.accounts : m.nft_postings;
        auto k = find_key(key);
        auto found = k ? table.find(*k) : table.end();
        std::uint32_t tail_first = std::max(first, m.base_last + 1), tail_last = std::min(last, m.last);
        auto take = [&](Posting const& p) {
            listed.push_back({p.ledger, p.index, listed.size() < limit ? m.tx_hash(p.tx) : Bytes()});
            return listed.size() <= limit;
        };
        // A key's postings are in (ledger, TransactionIndex) order: the page starts at the cursor
        // (itself included) or at the range's bound, found by a binary search, however long the tail.
        using Place = std::pair<std::uint64_t, std::uint64_t>;
        auto place = [](Posting const& p) { return Place{p.ledger, p.index}; };
        if (found != table.end()) {
            auto const& list = found->second;
            if (forward) {
                Place from{tail_first, 0};
                if (cursor) from = std::max(from, *cursor);
                auto p = std::lower_bound(list.begin(), list.end(), from, [&](Posting const& a, Place const& b) { return place(a) < b; });
                for (; p != list.end() && p->ledger <= tail_last; ++p) if (!take(*p)) break;
            } else {
                Place to{tail_last, std::numeric_limits<std::uint64_t>::max()};
                if (cursor) to = std::min(to, *cursor);
                auto p = std::upper_bound(list.begin(), list.end(), to, [&](Place const& a, Posting const& b) { return a < place(b); });
                while (p != list.begin() && (--p)->ledger >= tail_first) if (!take(*p)) break;
            }
        }
    }
    auto list_tail = [&] {
        for (auto const& p : listed) {
            if (items.size() == limit) { next = {{"ledger", p.ledger}, {"seq", p.index}}; break; }
            items.push_back({{"hash", to_hex(p.hash)}, {"ledger_index", p.ledger}, {"tx_index", p.index}});
        }
    };
    auto from_base = [&](unsigned count, Json const& base_marker) {
        auto page = m.base->history_page(column, key, first, std::min(last, m.base_last), count, forward, base_marker);
        for (auto const& t : page.at("transactions")) items.push_back(t);
        if (page.contains("marker")) next = page.at("marker");
    };
    if (forward) {
        if (base_part && (!cursor || cursor->first <= m.base_last)) {
            from_base(limit, marker);
            if (!next.is_null()) return {{"transactions", items}, {"limit", limit}, {"marker", next}};
        }
        list_tail();
    } else {
        bool in_base = cursor && cursor->first <= m.base_last;
        if (!in_base) list_tail();
        if (next.is_null() && base_part) {
            if (items.size() < limit) from_base(limit - static_cast<unsigned>(items.size()), in_base ? marker : Json(nullptr));
            else {
                // The page is full: the marker is the base's first entry, if any.
                auto page = m.base->history_page(column, key, first, std::min(last, m.base_last), 1, false, nullptr);
                if (!page.at("transactions").empty()) {
                    auto const& t = page.at("transactions")[0];
                    next = {{"ledger", t.at("ledger_index")}, {"seq", t.at("tx_index")}};
                }
            }
        }
    }
    Json result{{"transactions", items}, {"limit", limit}};
    if (!next.is_null()) result["marker"] = next;
    return result;
}
Json LiveHistory::transaction(Bytes const& hash, bool binary) const {
    auto& m = *impl_;
    std::shared_lock lock(m.mutex);
    auto k = find_key(hash);
    auto found = k ? m.transactions.find(*k) : m.transactions.end();
    if (found == m.transactions.end()) { lock.unlock(); return m.base->transaction(hash, binary); }
    auto [seq, position] = m.tx_where.at(found->second);
    auto chunk = m.chunk_of(seq);
    lock.unlock();
    auto [holder, record] = m.record_in(chunk, seq);
    auto const& tx = record->transactions.at(position);
    // A pseudo-transaction's ID can repeat (Store::append): the base's earlier ledger answers it, as xrpld.
    if (pseudo_transaction(tx.transaction))
        if (auto earlier = m.base->transaction(hash, binary); !earlier.is_null()) return earlier;
    Json result = binary ? Json{{"tx_blob", to_hex(tx.transaction)}, {"meta", to_hex(tx.metadata)}} : transaction_json(tx);
    result["ledger_index"] = seq; result["hash"] = to_hex(hash);
    return result;
}
std::vector<Bytes> LiveHistory::ledger_transactions(std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->ledger_transactions(seq);
    std::shared_lock lock(m.mutex);
    if (!m.in_tail(seq)) return seq > m.last ? std::vector<Bytes>{} : m.base->ledger_transactions(seq);
    std::vector<Bytes> out;
    for (auto id : m.ledgers.at(seq).in_order) out.push_back(m.tx_hash(id));
    return out;
}
LedgerRecord LiveHistory::ledger_record(std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->ledger_record(seq);
    std::vector<Bytes> hashes;  // the ledger's transaction hashes, by record position
    std::optional<Chunk> chunk;
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) throw Error("Ledger is not stored");
        if (m.in_tail(seq)) {
            auto const& ledger = m.ledgers.at(seq);
            for (std::size_t i = 0; i < ledger.in_order.size(); ++i) hashes.push_back(m.tx_hash(ledger.first_tx + static_cast<std::uint32_t>(i)));
            chunk = m.chunk_of(seq);
        }
    }
    if (!chunk) return m.base->ledger_record(seq);
    auto [holder, stored] = m.record_in(*chunk, seq);
    if (hashes.size() != stored->transactions.size()) throw Error("Tail ledger differs from its index");
    std::vector<std::pair<Bytes, TransactionRecord>> sorted;
    for (std::size_t i = 0; i < stored->transactions.size(); ++i) sorted.push_back({hashes[i], stored->transactions[i]});
    std::sort(sorted.begin(), sorted.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    LedgerRecord record{seq, stored->header, {}, stored->patches};
    for (auto& item : sorted) record.transactions.push_back(std::move(item.second));
    return record;
}
Json LiveHistory::nft(Bytes const& id, std::uint32_t seq) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->nft(id, seq);
    std::vector<Event> events;
    std::optional<Chunk> chunk;
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) throw Error("NFT state has been pruned");
        if (auto found = m.nft_states.find(id); found != m.nft_states.end())
            for (auto const& e : found->second) if (e.ledger <= seq) events.push_back(e);
        if (!events.empty()) chunk = m.chunk_of(events.back().ledger);
    }
    if (events.empty()) return m.base->nft(id, m.base_last);
    // The newest state, and the ledger where its owner and burned flag began (a URI change alone
    // does not move it, as in Clio): walk back while that half of the fingerprint holds, then
    // into the base when the whole tail agrees.
    auto const& newest = events.back();
    auto [holder, record] = m.record_in(*chunk, newest.ledger);
    auto state = nft_change(record->transactions.at(newest.position), id);
    if (!state) throw Error("NFT state differs from its index");
    std::uint32_t since = newest.ledger;
    bool whole_tail = true;
    for (auto e = events.rbegin(); e != events.rend(); ++e) {
        if (e->fingerprint.compare(0, nft_holder_print, newest.fingerprint, 0, nft_holder_print) != 0) { whole_tail = false; break; }
        since = e->ledger;
    }
    if (whole_tail) {
        auto before = m.base->nft(id, m.base_last);
        if (!before.is_null() && before.at("owner") == state->owner && before.at("is_burned") == state->burned)
            since = before.at("ledger_index").get<std::uint32_t>();
    }
    return render(*state, since);
}
namespace {
fs::path spool_in(fs::path const& tail) {
    auto pointer = tail / "current";
    if (!fs::exists(pointer)) return tail;
    auto name = read_file(pointer, 256);
    while (!name.empty() && std::isspace(static_cast<unsigned char>(name.back()))) name.pop_back();
    if (name.empty() || name.find('/') != std::string::npos || name.starts_with(".")) throw Error("Invalid spool pointer in " + pointer.string());
    return tail / "spools" / name;
}
}
void use_tail(fs::path const& dir, fs::path const& tail, fs::path const& tail_key) {
    use_live_history(std::make_shared<LiveHistory>(open_piece_history(dir), spool_in(tail), tail_key));
}
void follow_tail(fs::path const& dir, fs::path const& tail, fs::path const& tail_key) {
    auto base = open_piece_history(dir);
    auto spool = spool_in(tail);
    auto live = std::make_shared<LiveHistory>(base, spool, tail_key);
    use_live_history(live);
    std::thread([=]() mutable {
        // The tail lives in memory until the publication seals it into pieces; a publication that
        // stops advancing (a stalled publisher, a frozen source) leaves it growing, so say so.
        constexpr std::uint32_t long_tail = 100000;  // ledgers, about four and a half days
        auto warned = std::chrono::steady_clock::time_point{};
        // The spool's manifest is looked at four times a second and read only when it changed,
        // so a ledger the follower commits is served within a quarter of a second; the piece
        // cache and the spool's place are checked every 2 s.
        std::optional<fs::file_time_type> seen;
        // A tail over a new piece cache or spool, being indexed on its own thread: until it is
        // ready the current one keeps being refreshed, so the served tip does not stand still.
        std::future<std::shared_ptr<LiveHistory>> rebuilding;
        for (unsigned tick = 1;; ++tick) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            if (tick % 8) {
                std::error_code ec;
                auto changed = fs::last_write_time(spool_in(tail) / "manifest.json", ec);
                if (ec || changed == seen) continue;
                seen = changed;
                try { live->refresh(); }
                catch (std::exception const& error) { std::cerr << Json{{"event", "tail_refresh_failed"}, {"error", error.what()}}.dump() << std::endl; }
                continue;
            }
            if (rebuilding.valid() && rebuilding.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                try {
                    auto fresh = rebuilding.get();
                    fresh->refresh();  // built a while ago: never serve a tip older than the current one's
                    live = fresh;
                    use_live_history(live);
                    std::cerr << Json{{"event", "tail_rebuilt"}, {"status", live->status()}}.dump() << std::endl;
                } catch (std::exception const& error) {
                    base = nullptr;  // built again at the next check
                    std::cerr << Json{{"event", "tail_refresh_failed"}, {"error", error.what()}}.dump() << std::endl;
                }
            }
            try {
                auto status = live->status();
                if (status.contains("tail") && status.at("tail").at("ledgers").get<std::uint32_t>() > long_tail &&
                    std::chrono::steady_clock::now() - warned > std::chrono::hours(1)) {
                    warned = std::chrono::steady_clock::now();
                    auto const& tail = status.at("tail");
                    std::cerr << Json{{"event", "tail_growing"}, {"tail", tail}, {"publication_last", tail.at("first").get<std::uint32_t>() - 1},
                                      {"message", "the publication has not advanced: the served tail is held in memory until it does"}}.dump() << std::endl;
                }
                auto next_base = open_piece_history(dir);
                auto next_spool = spool_in(tail);
                if (!rebuilding.valid() && (next_base != base || next_spool != spool)) {
                    base = next_base; spool = next_spool;
                    rebuilding = std::async(std::launch::async, [next_base, next_spool, tail_key] { return std::make_shared<LiveHistory>(next_base, next_spool, tail_key); });
                }
                live->refresh();
            } catch (std::exception const& error) {
                std::cerr << Json{{"event", "tail_refresh_failed"}, {"error", error.what()}}.dump() << std::endl;
            }
        }
    }).detach();
}
bool LiveHistory::has_nft_state() const { return impl_->base->has_nft_state(); }
Json LiveHistory::cache_stats() const { return impl_->base->cache_stats(); }
std::vector<Json> LiveHistory::nfts(std::vector<Bytes> const& ids, std::uint32_t seq) const {
    if (seq <= impl_->base_last) return impl_->base->nfts(ids, seq);
    std::vector<Json> out;
    for (auto const& id : ids) out.push_back(nft(id, seq));
    return out;
}
// The base's candidates merged with the tail's.
std::optional<std::vector<Bytes>> LiveHistory::mpt_holders(Bytes const& issuance, Bytes const& after, unsigned limit) const {
    auto& m = *impl_;
    auto base = m.base->mpt_holders(issuance, after, limit);
    if (!base) return std::nullopt;
    std::set<Bytes> merged(base->begin(), base->end());
    std::shared_lock lock(m.mutex);
    if (auto found = m.holders.find(issuance); found != m.holders.end())
        for (auto it = after.empty() ? found->second.begin() : found->second.upper_bound(after); it != found->second.end() && merged.size() < 2 * limit; ++it) merged.insert(*it);
    std::vector<Bytes> out(merged.begin(), merged.end());
    if (out.size() > limit) out.resize(limit);
    return out;
}
std::optional<std::vector<Bytes>> LiveHistory::nfts_by_issuer(Bytes const& issuer, std::optional<std::uint32_t> taxon, Bytes const& after, unsigned limit) const {
    auto& m = *impl_;
    auto base = m.base->nfts_by_issuer(issuer, taxon, after, limit);
    if (!base) return std::nullopt;
    // Merged in Clio's order, by the tokens' entries (taxon, then token ID).
    std::map<Bytes, Bytes> merged;
    for (auto const& id : *base) merged.emplace(nft_issuer_entry(id).second, id);
    Bytes cursor;
    if (!after.empty()) cursor = nft_issuer_entry(after).second;
    if (!cursor.empty() && taxon) { char prefix[4]; pieces::put_be32(prefix, *taxon); cursor.replace(0, 4, prefix, 4); }
    std::shared_lock lock(m.mutex);
    if (auto found = m.issuer_entries.find(issuer); found != m.issuer_entries.end())
        for (auto it = cursor.empty() ? found->second.begin() : found->second.upper_bound(cursor); it != found->second.end(); ++it) {
            if (taxon && pieces::be32(it->data()) != *taxon) { if (pieces::be32(it->data()) > *taxon) break; continue; }
            merged.emplace(*it, pieces::token_of(*it, issuer));
            if (merged.size() >= 2 * limit) break;
        }
    std::vector<Bytes> out;
    for (auto const& [entry, id] : merged) { if (out.size() == limit) break; out.push_back(id); }
    return out;
}
void LiveHistory::prefetch(std::vector<Bytes> const& keys, std::uint32_t seq) const {
    impl_->base->prefetch(keys, std::min(seq, impl_->base_last));
}
// The keys after `after` and before `end` in `seq`'s state: the base's, with the tail's changes
// since its last ledger applied (a key the tail deleted is left out, one it created put in).
std::vector<Bytes> LiveHistory::keys_between(std::uint32_t seq, Bytes const& after, Bytes const& end, unsigned limit) const {
    auto& m = *impl_;
    if (seq <= m.base_last) return m.base->keys_between(seq, after, end, limit);
    // Empty bounds: from the first key, to the last (as PieceHistory reads them).
    if ((!after.empty() && after.size() != 32) || (!end.empty() && end.size() != 32)) return {};
    if (!after.empty() && !end.empty() && after >= end) return {};  // nothing between (as PieceHistory)
    {
        std::shared_lock lock(m.mutex);
        if (seq > m.last) return {};
    }
    // An ordered merge of two streams, each read a batch at a time: the base's keys at its last
    // ledger, and the tail's changed keys with their versions at `seq` (taken under the lock, so a
    // range the tail changed often costs only what the answer needs).
    // One walk for the whole merge (KeyWalk): the base's batches are charged to it, and each tail
    // change looked at. A tail that deleted many of the base's keys, or created and deleted many
    // of its own (a busy book's), has the merge read batch after batch for few keys; the bound
    // holds for all of them together, as for one PieceHistory listing.
    KeyWalk walk;
    std::vector<Bytes> base;
    std::size_t next_base = 0;
    Bytes base_after = after;
    bool base_done = false;
    auto more_base = [&] {
        auto wanted = std::max<unsigned>(limit, 64);
        base = m.base->keys_between(m.base_last, base_after, end, wanted, walk); next_base = 0;
        if (base.size() < wanted) base_done = true;
        if (!base.empty()) base_after = base.back();
    };
    std::vector<std::pair<Bytes, std::optional<std::uint32_t>>> changes;  // none: not changed by `seq`
    std::size_t next_change = 0;
    Key changes_after = key_of(after);
    bool started = !after.empty(), changes_done = false;
    auto more_changes = [&] {
        changes.clear(); next_change = 0;
        std::shared_lock lock(m.mutex);
        auto it = started ? m.versions.upper_bound(changes_after) : m.versions.begin();
        auto stop = end.empty() ? m.versions.end() : m.versions.lower_bound(key_of(end));
        for (; it != stop && it != m.versions.end() && changes.size() < 256; ++it) {
            auto key = bytes_of(it->first);
            changes.emplace_back(key, m.tail_version(key, seq));
        }
        if (it == stop || it == m.versions.end()) changes_done = true;
        if (!changes.empty()) { changes_after = key_of(changes.back().first); started = true; }
    };
    std::vector<Bytes> out;
    while (out.size() < limit) {
        if (next_base == base.size() && !base_done) more_base();
        if (next_change == changes.size() && !changes_done) more_changes();
        bool has_base = next_base < base.size(), has_change = next_change < changes.size();
        if (!has_base && !has_change) break;
        if (has_change && (!has_base || changes[next_change].first <= base[next_base])) {
            walk.spend();
            auto const& [key, version] = changes[next_change];
            // Both streams are read in order, so the base holds this key only if it is its next one.
            bool in_base = has_base && base[next_base] == key;
            if (in_base) ++next_base;
            if (version ? !(*version & deleted_bit) : in_base) out.push_back(key);
            ++next_change;
        } else out.push_back(base[next_base++]);  // a key the tail has not changed
    }
    return out;
}
std::optional<std::vector<Bytes>> LiveHistory::book_directories(std::uint32_t seq, Bytes const& after, Bytes const& end, unsigned limit) const {
    if (seq <= impl_->base_last) return impl_->base->book_directories(seq, after, end, limit);
    // A book's key range holds only its quality directories' root pages (PieceHistory).
    return keys_between(seq, after, end, limit);
}
}
