#include "store.hpp"
#include "piece_format.hpp"
#include "piece_history.hpp"
#include <xrpl/protocol/AccountID.h>
#include <algorithm>

namespace xrpla::core {
namespace {
void check(rocksdb::Status const& s) { if (!s.ok()) throw Error("RocksDB: " + s.ToString()); }
Bytes be(std::uint32_t n) { Bytes bytes(4, '\0'); for (unsigned i = 0; i < 4; ++i) bytes[i] = static_cast<char>(n >> (24 - 8 * i)); return bytes; }
std::uint32_t number(std::string_view bytes) { if (bytes.size() != 4) throw Error("Corrupt index integer"); std::uint32_t n = 0; for (unsigned char c : bytes) n = (n << 8) | c; return n; }
}
Store::Cursor::~Cursor() {
    if (!it || store->writable_ || !it->status().ok()) return;
    std::lock_guard lock(store->cursors_mutex_);
    auto& kept = store->cursors_[column];
    if (kept.size() < 32) kept.push_back(std::move(it));
}
Store::Cursor Store::cursor(std::string const& column) const {
    if (!writable_) {
        std::lock_guard lock(cursors_mutex_);
        if (auto& kept = cursors_[column]; !kept.empty()) { auto it = std::move(kept.back()); kept.pop_back(); return {this, column, std::move(it)}; }
    }
    return {this, column, std::unique_ptr<rocksdb::Iterator>(db_->NewIterator({}, cf(column)))};
}
// status()'s complete_ledgers without its header decode, from the baseline when that is served
// whole. A read-only open sees the store as it was when opened and never anything later, so its
// answer is read once and is exactly what that open holds; a writable store (an import, a
// restore) reads it each time, as its appends move it.
std::optional<std::pair<std::uint32_t, std::uint32_t>> Store::served_ledgers() const {
    auto read = [this]() -> std::optional<std::pair<std::uint32_t, std::uint32_t>> {
        auto id = get("default", "identity"), last = get("default", "last");
        if (id.empty()) throw Error("Database is not an xrpla store");
        if (last.empty()) return std::nullopt;
        std::pair<std::uint32_t, std::uint32_t> held{Json::parse(id).at("first").get<std::uint32_t>(), number(last)};
        if (auto baseline = baseline_ledger(); baseline && *baseline + 1 == held.first) held.first = *baseline;
        return held;
    };
    if (writable_) return read();
    // A read that throws leaves the flag unset: the next request reads again.
    std::call_once(served_once_, [&] { served_ = read(); });
    return served_;
}
Bytes Store::header(std::uint32_t sequence) const {
    auto bytes = get("ledgers", be(sequence));
    if (bytes.empty()) if (auto baseline = served_baseline(); baseline && baseline->sequence == sequence) return baseline->raw;
    return bytes;
}
std::uint32_t Store::ledger_sequence(Bytes const& hash) const {
    auto bytes = get("ledger_hashes", hash);
    if (!bytes.empty()) return number(bytes);
    auto baseline = served_baseline();
    return baseline && hash.size() == 32 && baseline->hash == to_hex(hash) ? baseline->sequence : 0;
}
Bytes Store::object(Bytes const& key, std::uint32_t seq) const {
    if (key.size() != 32) throw Error("Invalid object key length");
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("Ledger state has been pruned");
    auto it = cursor("objects");
    it->Seek(key + be(~seq));
    if (!it->Valid() || !it->key().starts_with(key)) { check(it->status()); return {}; }
    if (it->key().size() != 36) throw Error("Corrupt object version key");
    return it->value().ToString();
}
std::vector<Bytes> Store::objects(std::vector<Bytes> const& keys, std::uint32_t seq) const {
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("Ledger state has been pruned");
    // In key order on one iterator: each seek starts near the last.
    std::vector<std::size_t> order(keys.size());
    for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(), [&](auto a, auto b) { return keys[a] < keys[b]; });
    std::vector<Bytes> out(keys.size());
    auto it = cursor("objects");
    for (auto i : order) {
        auto const& key = keys[i];
        if (key.size() != 32) throw Error("Invalid object key length");
        it->Seek(key + be(~seq));
        if (!it->Valid() || !it->key().starts_with(key)) continue;
        if (it->key().size() != 36) throw Error("Corrupt object version key");
        out[i] = it->value().ToString();
    }
    check(it->status());
    return out;
}
std::optional<std::vector<Bytes>> Store::book_directories(std::uint32_t seq, Bytes const& after, Bytes const& end, unsigned limit) const {
    // Only an index marked "2" is complete (a "1" one may lack price levels: Store::Store).
    if (!columns_.contains("book_dirs") || get("default", "book_dirs_indexed") != "2") return std::nullopt;
    if (after.size() != 32 || end.size() != 32) throw Error("Invalid book directory bounds");
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("Ledger state has been pruned");
    std::vector<Bytes> out;
    auto it = cursor("book_dirs");
    auto version = [&] { return ~number(std::string_view(it->key().data(), it->key().size()).substr(32)); };
    constexpr unsigned steps = 8;  // a directory's few rows are stepped over; past those, sought past
    it->Seek(after + Bytes(4, '\xff'));
    while (it->Valid() && out.size() < limit) {
        if (it->key().size() != 36) throw Error("Corrupt book directory index");
        auto dir = it->key().ToString().substr(0, 32);
        if (dir >= end) break;
        unsigned n = 0;
        while (it->Valid() && it->key().starts_with(dir) && version() > seq && ++n <= steps) it->Next();
        if (n > steps) it->Seek(dir + be(~seq));
        if (it->Valid() && it->key().starts_with(dir) && it->value() == "c") out.push_back(dir);
        n = 0;
        while (it->Valid() && it->key().starts_with(dir) && ++n <= steps) it->Next();
        if (n > steps) it->Seek(dir + Bytes(4, '\xff'));
    }
    check(it->status());
    return out;
}
std::optional<std::pair<std::uint32_t, Bytes>> Store::deleted_object(Bytes const& key, std::uint32_t seq) const {
    if (key.size() != 32) throw Error("Invalid object key length");
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("Ledger state has been pruned");
    auto it = cursor("objects");
    it->Seek(key + be(~seq));
    if (!it->Valid() || !it->key().starts_with(key)) { check(it->status()); return std::nullopt; }
    if (it->key().size() != 36) throw Error("Corrupt object version key");
    if (!it->value().empty()) return std::nullopt;
    auto deleted_at = ~number(std::string_view(it->key().data(), it->key().size()).substr(32));
    auto before = object(key, deleted_at - 1);
    if (before.empty()) return std::nullopt;
    return std::pair{deleted_at, std::move(before)};
}
Json Store::object_page(std::uint32_t seq, std::string const& marker, unsigned limit, bool binary) const {
    if (!limit || limit > 4096) throw Error("Object page limit must be between 1 and 4096");
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("Ledger state has been pruned");
    Bytes after;
    if (!marker.empty()) { after = from_hex(marker); if (after.size() != 32) throw Error("Invalid object marker"); }
    Json items = Json::array();
    std::string last_key;
    bool more = false;
    // From past every version of the marker's key.
    visit_live(seq, after.empty() ? Bytes{} : after + Bytes(5, '\xff'), {}, [&](Bytes const& key, Bytes const& bytes) {
        if (items.size() == limit) { more = true; return false; }
        if (binary) items.push_back({{"index", to_hex(key)}, {"data", to_hex(bytes)}});
        else { auto item = object_json({key, bytes}); item["index"] = to_hex(key); items.push_back(item); }
        last_key = to_hex(key);
        return true;
    });
    Json result{{"state", items}};
    if (more) result["marker"] = last_key;
    return result;
}
void Store::visit_live(std::uint32_t seq, Bytes const& from, Bytes const& to, std::function<bool(Bytes const&, Bytes const&)> const& visitor) const {
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("Ledger state has been pruned");
    // The ordered version keyspace is also a successor index: each key's versions, newest
    // first, then the next key's. A key's few versions are stepped over (a seek costs many
    // steps; a store holds every key it ever had, so a walk crosses many deleted ones, as in an
    // order book's price levels); past a few, it seeks, so a key with thousands of versions
    // does not make the walk linear in them.
    auto it = cursor("objects");
    if (from.empty()) it->SeekToFirst(); else it->Seek(from);
    auto version = [&] { return ~number(std::string_view(it->key().data(), it->key().size()).substr(32)); };
    constexpr unsigned steps = 8;
    while (it->Valid()) {
        if (it->key().size() != 36) throw Error("Corrupt object index");
        auto key = it->key().ToString().substr(0, 32);
        if (!to.empty() && key >= to) break;
        unsigned n = 0;
        while (it->Valid() && it->key().starts_with(key) && version() > seq && ++n <= steps) it->Next();
        if (n > steps) it->Seek(key + be(~seq));
        if (it->Valid() && it->key().starts_with(key) && !it->value().empty() && !visitor(key, it->value().ToString())) break;
        n = 0;
        while (it->Valid() && it->key().starts_with(key) && ++n <= steps) it->Next();
        if (n > steps) it->Seek(key + Bytes(5, '\xff'));
    }
    check(it->status());
}
Json Store::history_page(std::string const& column, Bytes const& prefix, std::uint32_t first,
    std::uint32_t last, unsigned limit, bool forward, Json const& marker) const {
    if (!limit || limit > 1000 || first > last) throw Error("Invalid history page bounds");
    if (column != "account_tx" && column != "nft_tx" && column != "mpt_tx") throw Error("Invalid history index");
    Bytes cursor;
    if (!marker.is_null()) {
        if (!marker.is_object() || !marker.contains("ledger") || !marker.contains("seq") ||
            !marker["ledger"].is_number_unsigned() || !marker["seq"].is_number_unsigned()) throw Error("Invalid history marker");
        auto ledger = marker.at("ledger").get<std::uint64_t>(), position = marker.at("seq").get<std::uint64_t>();
        if (ledger < first || ledger > last || position > UINT32_MAX) throw Error("History marker is outside range");
        cursor = prefix + be(~static_cast<std::uint32_t>(ledger)) + be(~static_cast<std::uint32_t>(position));
    }
    auto it = this->cursor(column);
    if (forward) {
        it->SeekForPrev(cursor.empty() ? prefix + be(~first) + Bytes(4, '\xff') : cursor);
    } else it->Seek(cursor.empty() ? prefix + be(~last) : cursor);
    Json items = Json::array(), next = nullptr;
    while (it->Valid() && it->key().starts_with(prefix)) {
        auto key = it->key().ToString();
        if (key.size() != prefix.size() + 8) throw Error("Corrupt history index");
        auto seq = ~number(std::string_view(key).substr(prefix.size(), 4));
        auto pos = ~number(std::string_view(key).substr(prefix.size() + 4));
        if (seq < first || seq > last) break;
        if (items.size() == limit) { next = {{"ledger", seq}, {"seq", pos}}; break; }
        items.push_back({{"hash", to_hex(it->value().ToString())}, {"ledger_index", seq}, {"tx_index", pos}});
        if (forward) it->Prev(); else it->Next();
    }
    check(it->status());
    Json result{{"transactions", items}, {"limit", limit}};
    if (!next.is_null()) result["marker"] = next;
    return result;
}
Json Store::transaction(Bytes const& hash, bool binary) const {
    auto value = get("tx", hash);
    if (value.empty()) return nullptr;
    if (value.size() < 8) throw Error("Corrupt transaction");
    auto seq = number(std::string_view(value).substr(0, 4));
    auto length = number(std::string_view(value).substr(4, 4));
    if (!length || length > value.size() - 8) throw Error("Corrupt transaction length");
    TransactionRecord record{value.substr(8, length), value.substr(8 + length)};
    Json result = binary ? Json{{"tx_blob", to_hex(record.transaction)}, {"meta", to_hex(record.metadata)}} : transaction_json(record);
    result["ledger_index"] = seq; result["hash"] = to_hex(hash); return result;
}
std::vector<Bytes> Store::ledger_transactions(std::uint32_t sequence) const {
    auto prefix = be(sequence); std::vector<Bytes> result;
    auto it = cursor("ledger_tx");
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) result.push_back(it->value().ToString());
    check(it->status()); return result;
}
LedgerRecord Store::ledger_record(std::uint32_t seq) const { return read_ledger(seq, true); }
std::vector<TransactionRecord> Store::ledger_transaction_records(std::uint32_t seq) const { return read_ledger(seq, false).transactions; }
LedgerRecord Store::read_ledger(std::uint32_t seq, bool with_patches) const {
    LedgerRecord record{seq, header(seq), {}, {}};
    if (record.header.empty()) throw Error("Ledger is not stored");
    // The baseline ledger's changes from its parent are not stored (its parent's state is not).
    if (with_patches && get("ledgers", be(seq)).empty()) throw Error("Ledger changes before the first stored ledger have been pruned");
    auto ids = ledger_transactions(seq); std::sort(ids.begin(), ids.end());
    for (auto const& id : ids) {
        auto value = get("tx", id);
        // A repeated pseudo-transaction's earlier ledger has its record under the ID and the ledger (Store::append).
        if (value.size() >= 4 && number(std::string_view(value).substr(0, 4)) != seq) value = get("tx", id + be(seq));
        if (value.size() < 8) throw Error("Corrupt transaction");
        auto length = number(std::string_view(value).substr(4, 4));
        if (!length || length > value.size() - 8) throw Error("Corrupt transaction length");
        record.transactions.push_back({value.substr(8, length), value.substr(8 + length)});
    }
    if (!with_patches) return record;
    if (columns_.contains("diff")) {
        auto prefix = be(seq);
        auto it = cursor("diff");
        std::vector<Bytes> keys;
        for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) keys.push_back(it->key().ToString().substr(4));
        check(it->status());
        auto values = objects(keys, seq);  // in one pass over the object versions
        for (std::size_t i = 0; i < keys.size(); ++i) record.patches.push_back({std::move(keys[i]), std::move(values[i])});
    } else visit("objects", [&](Bytes const& key, Bytes const& value) {
        if (key.size() == 36 && ~number(std::string_view(key).substr(32)) == seq) record.patches.push_back({key.substr(0, 32), value});
    });
    return record;
}
Bytes Store::predecessor(Bytes const& key, std::uint32_t seq) const {
    auto it = cursor("objects");
    it->SeekForPrev(key);
    while (it->Valid()) {
        auto candidate = it->key().ToString().substr(0, 32);
        if (candidate.size() != 32) throw Error("Corrupt object index");
        if (!object(candidate, seq).empty()) return candidate;
        it->SeekForPrev(candidate);
    }
    check(it->status()); return Bytes(32, '\0');
}
void Store::visit(std::string const& column, std::function<void(Bytes const&, Bytes const&)> const& visitor) const {
    auto it = cursor(column);
    for (it->SeekToFirst(); it->Valid(); it->Next()) visitor(it->key().ToString(), it->value().ToString());
    check(it->status());
}
std::uint64_t Store::column_bytes(std::string const& column) const {
    std::uint64_t bytes = 0;
    return db_->GetIntProperty(cf(column), "rocksdb.total-sst-files-size", &bytes) ? bytes : 0;
}
void Store::visit(std::string const& column, Bytes const& from, Bytes const& to, std::function<void(Bytes const&, Bytes const&)> const& visitor) const {
    auto it = cursor(column);
    for (it->Seek(from); it->Valid() && (to.empty() || it->key().compare(to) < 0); it->Next()) visitor(it->key().ToString(), it->value().ToString());
    check(it->status());
}
std::optional<std::vector<Bytes>> Store::mpt_holders(Bytes const& issuance, Bytes const& after, unsigned limit) const {
    if (!columns_.contains("mpt_holders")) return std::nullopt;
    if (issuance.size() != 24 || (!after.empty() && after.size() != 20)) throw Error("Invalid MPT holder query");
    std::vector<Bytes> out;
    auto it = cursor("mpt_holders");
    for (it->Seek(issuance + after); it->Valid() && it->key().starts_with(issuance) && out.size() < limit; it->Next()) {
        auto holder = it->key().ToString().substr(24);
        if (holder.size() != 20) throw Error("Corrupt MPT holder index");
        if (holder != after) out.push_back(std::move(holder));
    }
    check(it->status());
    return out;
}
std::optional<std::vector<Bytes>> Store::nfts_by_issuer(Bytes const& issuer, std::optional<std::uint32_t> taxon, Bytes const& after, unsigned limit) const {
    if (!columns_.contains("nft_issuers") || get("default", "nft_issuers_indexed").empty()) return std::nullopt;
    if (issuer.size() != 20 || (!after.empty() && after.size() != 32)) throw Error("Invalid NFT issuer query");
    // Keys are the issuer, then its entries: taxon and token ID bytes (piece_format.hpp's XQOX).
    Bytes start = issuer;
    if (!after.empty()) {
        auto entry = nft_issuer_entry(after).second;
        if (taxon) entry.replace(0, 4, be(*taxon));
        start += entry;
    } else if (taxon) start += be(*taxon);
    auto prefix = taxon ? issuer + be(*taxon) : issuer;
    std::vector<Bytes> out;
    auto it = cursor("nft_issuers");
    for (it->Seek(start); it->Valid() && it->key().starts_with(prefix) && out.size() < limit; it->Next()) {
        auto key = it->key().ToString();
        if (key.size() != 36) throw Error("Corrupt NFT issuer index");
        if (!after.empty() && key == start) continue;
        out.push_back(pieces::token_of(std::string_view(key).substr(20), issuer));
    }
    check(it->status());
    return out;
}
Json Store::nft(Bytes const& id, std::uint32_t seq) const {
    auto floor = get("default", "state_first");
    if (!floor.empty() && seq < number(floor)) throw Error("NFT state has been pruned");
    auto it = cursor("nft");
    it->Seek(id + be(~seq));
    if (!it->Valid() || !it->key().starts_with(id)) { check(it->status()); return nullptr; }
    return Json::parse(it->value().ToString());
}
Json Store::prune(std::uint32_t first) {
    auto info = status().at("complete_ledgers");
    if (info.is_null() || first < info.at("first").get<std::uint32_t>() || first > info.at("last").get<std::uint32_t>())
        throw Error("State retention boundary is outside stored history");
    auto existing = get("default", "state_first");
    if (!existing.empty() && first < number(existing)) throw Error("Cannot restore pruned state");
    // Publish the retention floor before deleting: a crash may leave extra old
    // versions, but can never advertise history whose baseline was deleted.
    rocksdb::WriteBatch batch; batch.Put(cf("default"), "state_first", be(first)); write(batch); batch.Clear();
    std::uint64_t removed = 0;
    for (auto column : {"objects", "nft"}) {
        Bytes previous; bool baseline = false;
        visit(column, [&](Bytes const& key, Bytes const&) {
            if (key.size() != 36) throw Error("Corrupt versioned key during pruning");
            auto id = key.substr(0, 32); if (id != previous) { previous = id; baseline = false; }
            auto seq = ~number(std::string_view(key).substr(32));
            if (seq > first) return;
            if (!baseline) { baseline = true; return; }
            batch.Delete(cf(column), key); ++removed;
            if (batch.Count() >= 10000) { write(batch); batch.Clear(); }
        });
    }
    if (batch.Count()) write(batch);
    check(db_->CompactRange({}, cf("objects"), nullptr, nullptr));
    return {{"state_first", first}, {"versions_removed", removed}, {"transaction_history_preserved", true}};
}
Json prune_store(fs::path const& dir, std::uint32_t first) { DirectoryLock lock(dir); return Store(dir / "store", true).prune(first); }
}
