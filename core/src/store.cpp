#include "store.hpp"
#include "wire.hpp"
#include "piece_history.hpp"
#include <xrpl/protocol/AccountID.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <algorithm>
#include <charconv>
#include <iostream>
#include <set>
#include <thread>
#ifndef _WIN32
#include <sys/resource.h>
#endif

namespace xrpla::core {
void Store::compact() {
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    for (auto const& [name, handle] : columns_) handles.push_back(handle);
    rocksdb::FlushOptions flush; flush.wait = true;
    auto result = db_->Flush(flush, handles);
    if (!result.ok()) throw Error("Flush store: " + result.ToString());
    for (auto handle : handles) {
        result = db_->CompactRange(rocksdb::CompactRangeOptions{}, handle, nullptr, nullptr);
        if (!result.ok()) throw Error("Compact store: " + result.ToString());
    }
}
namespace {
void check(rocksdb::Status const& s) { if (!s.ok()) throw Error("RocksDB: " + s.ToString()); }
Bytes be(std::uint32_t n) {
    Bytes b(4, '\0');
    for (unsigned i = 0; i < 4; ++i) b[i] = static_cast<char>((n >> (24 - 8 * i)) & 255);
    return b;
}
std::uint32_t number(std::string_view bytes) {
    if (bytes.size() != 4) throw Error("Corrupt database integer");
    std::uint32_t n = 0;
    for (unsigned char c : bytes) n = (n << 8) | c;
    return n;
}
std::uint32_t decimal(std::string const& s) {
    std::uint32_t n = 0;
    auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
    if (ec != std::errc{} || end != s.data() + s.size() || !n) throw Error("Expected a positive ledger sequence");
    return n;
}
Bytes hash_key(std::string const& text) {
    auto key = from_hex(text);
    if (key.size() != 32) throw Error("Expected a 32-byte hash or object key");
    return key;
}
// Table files a store keeps open. A full-history store has tens of thousands, and with 128
// open a lookup kept reopening them. The process's own limit is raised to its hard limit (at
// most 65,536), and a store keeps up to half of it open, leaving room for a second store and
// the process's sockets.
int open_files_allowed() {
#ifdef _WIN32
    return 512;
#else
    rlimit limit{};
    if (getrlimit(RLIMIT_NOFILE, &limit) != 0) return 128;
    auto const wanted = std::min<rlim_t>(limit.rlim_max, 65536);
    if (limit.rlim_cur != RLIM_INFINITY && limit.rlim_cur < wanted) {
        limit.rlim_cur = wanted;
        setrlimit(RLIMIT_NOFILE, &limit);
        getrlimit(RLIMIT_NOFILE, &limit);
    }
    auto const current = limit.rlim_cur == RLIM_INFINITY ? rlim_t(65536) : std::min<rlim_t>(limit.rlim_cur, 65536);
    return static_cast<int>(std::max<rlim_t>(128, current / 2));
#endif
}
std::shared_ptr<rocksdb::Cache> const& shared_block_cache() {
    static auto const cache = rocksdb::NewLRUCache(std::size_t(512) << 20);
    return cache;
}
} // namespace
void Store::reserve_block_cache(std::size_t bytes) {
    auto const& cache = shared_block_cache();
    if (bytes > cache->GetCapacity()) cache->SetCapacity(bytes);
}
Store::Store(fs::path const& path, bool writable, bool bulk) : bulk_(bulk && writable), writable_(writable) {
    safe_cache_path(path);
    if (fs::exists(path)) {
        for (auto const& entry : fs::directory_iterator(path)) safe_cache_path(entry.path());
        if (!fs::is_empty(path) && !fs::exists(path / "CURRENT")) throw Error("Store path is not a RocksDB database");
    }
    rocksdb::Options options;
    options.create_if_missing = writable;
    options.create_missing_column_families = writable;
    options.compression = rocksdb::kLZ4Compression;
    options.max_open_files = open_files_allowed();
    // A full-history store is terabytes written in bulk, then a ledger every few seconds. The
    // defaults (two background jobs, 64 MB memtables per column) left a restore spending most
    // of its time in RocksDB's writes. Background flushes and compactions use up to 16 cores;
    // memtables are larger, within one budget for all columns; readers share a block cache.
    auto const cores = std::max(2u, std::thread::hardware_concurrency());
    options.IncreaseParallelism(static_cast<int>(std::min(16u, cores)));
    options.max_background_jobs = static_cast<int>(std::min(16u, cores));
    options.db_write_buffer_size = std::size_t(1) << 30;
    options.write_buffer_size = std::size_t(128) << 20;
    options.max_write_buffer_number = 4;
    options.target_file_size_base = std::uint64_t(128) << 20;
    options.max_bytes_for_level_base = std::uint64_t(1) << 30;
    options.bytes_per_sync = std::uint64_t(1) << 20;
    options.atomic_flush = bulk_;  // with no log, every flush covers all columns
    rocksdb::BlockBasedTableOptions table;
    // One block cache for the process: a server reopens its stores (rpc.cpp) and keeps their
    // blocks, and several stores share one memory budget (a batch job may raise it).
    table.block_cache = shared_block_cache();
    table.block_size = 16 * 1024;
    // Index and filter blocks live in the block cache, in partitions (their top level pinned),
    // so their memory stays within the cache however large the store grows.
    table.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
    table.partition_filters = true;
    table.cache_index_and_filter_blocks = true;
    table.pin_top_level_index_and_filter = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
    // Lookups by exact key (a transaction by hash, a ledger by hash or by sequence) skip table
    // files that cannot hold the key: a Ribbon filter with a 10-bit Bloom filter's 1% false
    // positives, at about 7 bits a key (some 4.5 GB for full history's 5 billion transactions).
    // Columns read by ranges or seeks (object versions, account and token histories) take none:
    // a whole-key filter cannot answer a seek, and a prefix filter would change how their
    // iterators cross from one key to the next.
    auto filtered = table;
    filtered.filter_policy.reset(rocksdb::NewRibbonFilterPolicy(10));
    std::shared_ptr<rocksdb::TableFactory> exact(rocksdb::NewBlockBasedTableFactory(filtered));
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptions;
    std::vector<std::string> names{"default", "ledgers", "ledger_hashes", "tx", "ledger_tx", "account_tx", "objects", "diff", "nft", "nft_tx", "mpt_tx", "mpt_holders", "nft_issuers", "book_dirs", "book_dir_live"};
    if (!writable) check(rocksdb::DB::ListColumnFamilies({}, path.string(), &names));
    for (auto const& name : names) {
        rocksdb::ColumnFamilyOptions column(options);
        if (name == "tx" || name == "ledger_hashes" || name == "ledgers" || name == "book_dir_live") column.table_factory = exact;
        descriptions.emplace_back(name, column);
    }
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* db = nullptr;
    auto result = writable ? rocksdb::DB::Open(options, path.string(), descriptions, &handles, &db)
                           : rocksdb::DB::OpenForReadOnly(options, path.string(), descriptions, &handles, &db);
    check(result);
    db_.reset(db);
    for (std::size_t i = 0; i < descriptions.size(); ++i) columns_[descriptions[i].name] = handles[i];
    // Stores written before nfts_by_issuer lack its index: it is filled once from the NFT index.
    if (writable && get("default", "nft_issuers_indexed").empty()) {
        rocksdb::WriteBatch batch;
        Bytes previous;
        visit("nft", [&](Bytes const& key, Bytes const&) {
            auto id = key.substr(0, 32);
            if (id.size() != 32 || id == previous) return;
            previous = id;
            auto [issuer, entry] = nft_issuer_entry(id);
            batch.Put(cf("nft_issuers"), issuer + entry, "");
            if (batch.Count() >= 10000) { write(batch); batch.Clear(); }
        });
        batch.Put(cf("default"), "nft_issuers_indexed", "1");
        write(batch);
    }
    // A store opened only for reading before then answers no NFT state rather than a stale one.
    if (writable && get("default", "nft_clio_indexed").empty()) reindex_nft();
    if (writable && get("default", "tx_first_kept").empty()) keep_first_transactions();
    // A book index marked "1" may be stale: before version 2, a build resumed after the store
    // took ledgers marked the baseline's directories live again, so one deleted since and created
    // again later got no "c" row and book_offers missed its price level. Rebuilding it would read
    // every object version, so book_offers reads those instead (book_directories answers
    // nothing), as for a store from before the index. A store built again indexes its books.
    if (writable && get("default", "book_dirs_indexed") == "1") {
        rocksdb::WriteBatch batch;
        batch.Delete(cf("default"), "book_dirs_indexed");
        write(batch);
        std::cerr << Json{{"event", "book_index_unused"}, {"store", path.string()},
            {"hint", "This store's order book index may lack price levels; book_offers reads the object versions instead. Build the store again for the faster index."}}.dump() << std::endl;
    }
    nft_state_ = !get("default", "nft_clio_indexed").empty();
}
Store::~Store() {
    { std::lock_guard lock(cursors_mutex_); cursors_.clear(); }  // iterators go before the database
    for (auto const& [name, handle] : columns_) db_->DestroyColumnFamilyHandle(handle); }
rocksdb::ColumnFamilyHandle* Store::cf(std::string const& name) const { return columns_.at(name); }
void Store::write(rocksdb::WriteBatch& batch) {
    rocksdb::WriteOptions options;
    options.disableWAL = bulk_;
    options.sync = sync_each_ && !bulk_;
    check(db_->Write(options, &batch));
}
// In bulk mode there is no log to sync: what is flushed is what a crash keeps.
void Store::sync() { if (!bulk_) check(db_->SyncWAL()); }
void Store::flush() {
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    for (auto const& [name, handle] : columns_) handles.push_back(handle);
    rocksdb::FlushOptions options; options.wait = true;
    check(db_->Flush(options, handles));
    if (!bulk_) sync();
}
std::string Store::get(std::string const& column, Bytes const& key) const {
    std::string result;
    auto s = db_->Get(rocksdb::ReadOptions{}, cf(column), key, &result);
    if (s.IsNotFound()) return {};
    check(s); return result;
}
void Store::initialize(LedgerVerifier const& verifier, Range range) {
    initialize(verifier.header(), range, [&](auto const& visit) { verifier.state_objects(visit); });
}
void Store::initialize(Header const& header, Range range, std::function<void(std::function<void(ObjectRecord const&)> const&)> const& objects) {
    if (static_cast<std::uint64_t>(header.sequence) + 1 != range.first) throw Error("Store baseline must precede requested range");
    Json identity{{"schema", 2}, {"first", range.first}, {"baseline_hash", header.hash}};
    auto existing = get("default", "identity");
    if (!existing.empty() && Json::parse(existing) != identity)
        throw Error("Store has a different baseline; extend with the same range start or choose a new data directory");
    bool const populated = !get("default", "last").empty();
    if (populated && !checkpoint_files().is_null()) return;
    rocksdb::WriteBatch batch;
    batch.Put(cf("default"), "identity", identity.dump());
    write(batch); batch.Clear();
    std::size_t pending = 0;
    objects([&](ObjectRecord const& item) {
        // A store holding ledgers already (a build stopped before finish_admission, or a store
        // from an older version) keeps its book index: book_dir_live follows the ledgers
        // appended, and the baseline's directories marked live again would include those
        // deleted since (one created again later would then get no "c" row).
        if (!populated) {
            batch.Put(cf("objects"), item.key + be(~header.sequence), item.object);
            index_book_directory(batch, item, header.sequence, true);
        }
        index_object(batch, item, header.sequence);
        if (++pending == 10000) { write(batch); batch.Clear(); pending = 0; }
    });
    // The order book index holds every directory from the baseline on: book_offers may use it.
    // "2": built by a version that keeps it as the ledgers left it when a build resumes ("1",
    // from before, is not used; see Store::Store).
    if (!populated) batch.Put(cf("default"), "book_dirs_indexed", "2");
    batch.Put(cf("default"), "baseline_header", header.raw);
    write(batch);
}
bool Store::append(VerifiedLedger const& ledger) {
    auto identity_bytes = get("default", "identity");
    if (identity_bytes.empty() || get("default", "baseline_header").empty()) throw Error("Store baseline is incomplete");
    auto identity = Json::parse(identity_bytes);
    auto const& h = ledger.header();
    auto last_bytes = get("default", "last");
    auto first = identity.at("first").get<std::uint32_t>();
    auto last = last_bytes.empty() ? first - 1 : number(last_bytes);
    if (h.sequence <= last) {
        if (get("ledgers", be(h.sequence)) != h.raw) throw Error("Stored ledger differs from verified archive");
        if (checkpoint_files().is_null()) {
            rocksdb::WriteBatch batch;
            for (auto const& patch : ledger.record().patches) batch.Put(cf("diff"), be(h.sequence) + patch.key, "");
            index_ledger(batch, ledger); write(batch);
        }
        return false;
    }
    auto parent = last_bytes.empty() ? identity.at("baseline_hash").get<std::string>()
                                   : decode_header(get("ledgers", be(last))).hash;
    if (static_cast<std::uint64_t>(last) + 1 != h.sequence || h.parent_hash != parent)
        throw Error("Store append is not consecutive");
    rocksdb::WriteBatch batch;
    batch.Put(cf("ledgers"), be(h.sequence), h.raw);
    batch.Put(cf("ledger_hashes"), from_hex(h.hash), be(h.sequence));
    auto const& records = ledger.record().transactions;
    for (std::size_t i = 0; i < ledger.transactions().size(); ++i) {
        auto const& index = ledger.transactions()[i];
        auto const& tx = records[i];
        // A pseudo-transaction can repeat (an identical vote in two flag ledgers: mainnet's SetFee
        // 1C15FEA3... in ledgers 3,715,073 and 3,721,729): its ID keeps the earliest ledger's
        // record, which xrpld's tx answers, and a later one goes under the ID and its ledger,
        // for that ledger (read_ledger).
        auto value = be(h.sequence) + be(static_cast<std::uint32_t>(tx.transaction.size())) + tx.transaction + tx.metadata;
        if (auto earlier = get("tx", index.hash); !earlier.empty()) {
            if (!pseudo_transaction(tx.transaction)) throw Error("Transaction already exists in another stored ledger");
            batch.Put(cf("tx"), index.hash + be(h.sequence), value);
        } else batch.Put(cf("tx"), index.hash, value);
        batch.Put(cf("ledger_tx"), be(h.sequence) + be(index.position), index.hash);
        for (auto const& account : index.accounts)
            batch.Put(cf("account_tx"), account + be(~h.sequence) + be(~index.position), index.hash);
    }
    for (auto const& patch : ledger.record().patches) {
        batch.Put(cf("objects"), patch.key + be(~h.sequence), patch.object);
        batch.Put(cf("diff"), be(h.sequence) + patch.key, "");
    }
    index_ledger(batch, ledger);
    batch.Put(cf("default"), "last", be(h.sequence));
    write(batch);
    return true;
}
bool Store::initialized() const { return !get("default", "identity").empty() && !get("default", "baseline_header").empty(); }
Json Store::checkpoint_files() const {
    auto value = get("default", "checkpoint_files_v3");
    return value.empty() ? Json(nullptr) : Json::parse(value);
}
void Store::finish_admission(Json const& files) {
    rocksdb::WriteBatch batch; batch.Put(cf("default"), "checkpoint_files_v3", files.dump()); write(batch);
}
Json Store::status() const {
    auto id = get("default", "identity"), last = get("default", "last");
    if (id.empty()) throw Error("Database is not an xrpla store");
    auto identity = Json::parse(id);
    Json result{{"store_available", true}, {"schema", identity.at("schema")}, {"verification", "canonical-ledger-roots-and-parent-chain"},
                {"baseline_hash", identity.at("baseline_hash")}, {"complete_ledgers", nullptr}};
    if (!last.empty()) {
        auto n = number(last);
        result["complete_ledgers"] = {{"first", identity.at("first")}, {"last", n}};
        result["last_ledger_hash"] = decode_header(get("ledgers", be(n))).hash;
        auto state_first = get("default", "state_first");
        result["state_ledgers"] = {{"first", state_first.empty() ? identity.at("first").get<std::uint32_t>() : number(state_first)}, {"last", n}};
    }
    return result;
}
std::optional<std::uint32_t> Store::baseline_ledger() const {
    if (get("default", "last").empty()) return std::nullopt;
    auto baseline = served_baseline();
    return baseline ? std::optional<std::uint32_t>(baseline->sequence) : std::nullopt;
}
std::optional<Header> Store::served_baseline() const {
    auto raw = get("default", "baseline_header");
    if (raw.empty()) return std::nullopt;
    auto header = decode_header(raw);
    auto state_first = get("default", "state_first");
    if (header.transaction_hash != std::string(64, '0') || (!state_first.empty() && number(state_first) > header.sequence)) return std::nullopt;
    return header;
}
Json Store::query(std::string const& kind, std::string const& key, std::uint32_t sequence) const {
    auto state = status();
    if (state.at("complete_ledgers").is_null()) throw Error("Store has no complete ledgers");
    auto first = state.at("complete_ledgers").at("first").get<std::uint32_t>();
    auto last = state.at("complete_ledgers").at("last").get<std::uint32_t>();
    if (!sequence) sequence = last;
    if (sequence < first || sequence > last) throw Error("Ledger is outside the stored range");
    if (kind == "ledger") {
        auto seq = key.size() == 64 ? get("ledger_hashes", hash_key(key)) : be(decimal(key));
        if (seq.empty()) return {{"found", false}};
        auto raw = get("ledgers", seq);
        if (raw.empty()) return {{"found", false}};
        auto result = header_json(decode_header(raw));
        result["found"] = true;
        result["transactions"] = Json::array();
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator({}, cf("ledger_tx")));
        for (it->Seek(seq); it->Valid() && it->key().starts_with(seq); it->Next())
            result["transactions"].push_back(to_hex(it->value().ToString()));
        check(it->status()); return result;
    }
    if (kind == "tx") {
        auto value = get("tx", hash_key(key));
        if (value.empty()) return {{"found", false}};
        if (value.size() < 8) throw Error("Corrupt transaction record");
        auto seq = number(std::string_view(value).substr(0, 4));
        auto length = number(std::string_view(value).substr(4, 4));
        if (!length || length > value.size() - 8) throw Error("Corrupt transaction record length");
        auto result = transaction_json({value.substr(8, length), value.substr(8 + length)});
        result["found"] = true; result["ledger_index"] = seq; result["hash"] = key;
        return result;
    }
    if (kind == "object") {
        auto id = hash_key(key);
        auto value = object(id, sequence);
        if (value.empty()) return {{"found", false}, {"ledger_index", sequence}};
        return {{"found", true}, {"ledger_index", sequence}, {"object", object_json({id, value})}};
    }
    if (kind == "account_tx") {
        Bytes account;
        if (key.size() == 40) account = from_hex(key);
        else {
            auto parsed = ::xrpl::parseBase58<::xrpl::AccountID>(key);
            if (!parsed) throw Error("Invalid account address");
            account.assign(reinterpret_cast<char const*>(parsed->data()), parsed->size());
        }
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator({}, cf("account_tx")));
        Json items = Json::array();
        it->Seek(account + be(~sequence));
        for (; it->Valid() && it->key().starts_with(account) && items.size() < 100; it->Next()) {
            auto k = it->key().ToString();
            if (k.size() != 28) throw Error("Corrupt account index");
            items.push_back({{"hash", to_hex(it->value().ToString())}, {"ledger_index", ~number(std::string_view(k).substr(20, 4))},
                             {"tx_index", ~number(std::string_view(k).substr(24, 4))}});
        }
        check(it->status());
        return {{"transactions", items}, {"limit", 100}, {"truncated", it->Valid() && it->key().starts_with(account)}};
    }
    throw Error("Query kind must be ledger, tx, object, or account_tx");
}
Json store_status(fs::path const& dir) { DirectoryLock lock(dir); return Store(dir / "store", false).status(); }
Json query_store(fs::path const& dir, std::string const& kind, std::string const& key, std::uint32_t seq) {
    DirectoryLock lock(dir); return Store(dir / "store", false).query(kind, key, seq);
}
} // namespace xrpla::core
