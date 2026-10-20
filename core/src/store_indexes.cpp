#include "store.hpp"
#include "piece_history.hpp"
#include "wire.hpp"
#include <xrpl/protocol/AccountID.h>
#include <algorithm>
#include <map>
#include <set>

namespace xrpla::core {
namespace {
Bytes be(std::uint32_t n) { Bytes b(4, '\0'); for (unsigned i = 0; i < 4; ++i) b[i] = static_cast<char>(n >> (24 - 8 * i)); return b; }
// MPTokenIssuanceIDs (UInt192 field 1) anywhere in a transaction or its metadata, read by type code.
void wire_mpt_ids(std::vector<wire::Field> const& fields, std::set<Bytes>& out) {
    for (auto const& field : fields) {
        if (field.type == 21 && field.code == 1 && field.value.size() == 24) out.insert(Bytes(field.value));
        else if (field.type == 14 || field.type == 15) wire_mpt_ids(field.fields, out);
    }
}
}
namespace {
// Whether a transaction or its metadata holds an NFTokenID (UInt256 field 10), an NFTokens array
// (Array field 10) or an MPTokenIssuanceID (UInt192 field 1), read by type code: only those can
// add to the NFT and MPT indexes, so only they are decoded (decoding every transaction to JSON
// was most of a restore's time in the store). Bytes this reader cannot walk are decoded anyway.
bool names_tokens(TransactionRecord const& record) {
    try {
        for (auto const* data : {&record.transaction, &record.metadata})
            if (wire::has_any(*data, {{5, 10}, {15, 10}, {21, 1}})) return true;
        return false;
    } catch (std::exception const&) { return true; }
}
// The nft column's value for a token's state at `seq`, or null when its entry before (`prior`)
// says it already. ledger_index is where the owner and burned flag became these, as Clio
// reports it: a URI change alone (NFTokenModify) keeps the ledger before.
Json nft_value(NftState const& state, std::uint32_t seq, Json const& prior) {
    std::uint32_t since = seq;
    if (!prior.is_null() && prior.at("owner") == state.owner && prior.at("is_burned").get<bool>() == state.burned) {
        if (prior.at("uri") == state.uri) return nullptr;
        since = prior.at("ledger_index").get<std::uint32_t>();
    }
    return Json{{"owner", state.owner}, {"uri", state.uri}, {"is_burned", state.burned}, {"ledger_index", since}};
}
std::uint32_t number(std::string_view b) {
    if (b.size() < 4) throw Error("Corrupt database integer");
    return (std::uint32_t(std::uint8_t(b[0])) << 24) | (std::uint32_t(std::uint8_t(b[1])) << 16) | (std::uint32_t(std::uint8_t(b[2])) << 8) | std::uint8_t(b[3]);
}
}
// An order book directory: a DirectoryNode (0x0064) with an ExchangeRate (UInt64 field 6), read
// by type code (history holds objects this libxrpl cannot decode).
bool book_directory(Bytes const& object) {
    if (object.empty()) return false;
    auto type = wire::top_level(object, 1, 1);
    return type && *type == std::string_view("\x00\x64", 2) && wire::top_level(object, 3, 6);
}
// book_dirs: (directory key, inverted ledger) -> "c" (exists from that ledger) or "d" (deleted
// in it), newest first per directory. A book's price levels at a ledger are then read from this
// small column in order, rather than by seeking through every object version under the book
// (a store keeps every price level the book ever had, most of them deleted). book_dir_live
// holds the directories that exist after the newest ledger, so a change is classified with one
// filtered key lookup (most deleted objects are not directories).
void Store::index_book_directory(rocksdb::WriteBatch& batch, ObjectRecord const& item, std::uint32_t seq, bool baseline) {
    bool directory = book_directory(item.object);
    if (baseline) {
        if (directory) { batch.Put(cf("book_dirs"), item.key + be(~seq), "c"); batch.Put(cf("book_dir_live"), item.key, "1"); }
        return;
    }
    if (!item.object.empty() && !directory) return;
    // A store checking a ledger it already holds again (Store::append) finds it live already.
    bool live = !get("book_dir_live", item.key).empty();
    if (directory && !live) { batch.Put(cf("book_dirs"), item.key + be(~seq), "c"); batch.Put(cf("book_dir_live"), item.key, "1"); }
    else if (!directory && live) { batch.Put(cf("book_dirs"), item.key + be(~seq), "d"); batch.Delete(cf("book_dir_live"), item.key); }
}
void Store::index_object(rocksdb::WriteBatch& batch, ObjectRecord const& item, std::uint32_t seq, bool nft_pages) {
    if (item.object.empty()) return;
    // Only NFTokenPage (0x0050) and MPToken (0x007F) objects are indexed, and only they are
    // decoded: history holds objects this libxrpl cannot decode (types and fields since removed).
    auto code = wire::top_level(item.object, 1, 1);
    if (!code || (*code != std::string_view("\x00\x50", 2) && *code != std::string_view("\x00\x7f", 2))) return;
    auto decoded = object_json(item);
    auto type = decoded.value("LedgerEntryType", "");
    if (type == "NFTokenPage" && nft_pages) {
        auto owner = ::xrpl::AccountID::fromVoid(item.key.data());
        for (auto const& token : decoded.at("NFTokens")) {
            auto const& fields = token.at("NFToken");
            auto id = from_hex(fields.at("NFTokenID").get<std::string>());
            auto [issuer, entry] = nft_issuer_entry(id);
            batch.Put(cf("nft_issuers"), issuer + entry, "");
            auto value = nft_value({::xrpl::toBase58(owner), fields.value("URI", ""), false}, seq, nft(id, seq - 1));
            if (!value.is_null()) batch.Put(cf("nft"), id + be(~seq), value.dump());
        }
    } else if (type == "MPToken") {
        auto issuer = from_hex(decoded.at("MPTokenIssuanceID").get<std::string>());
        auto account = ::xrpl::parseBase58<::xrpl::AccountID>(decoded.at("Account").get<std::string>());
        if (!account) throw Error("Invalid MPToken account");
        Bytes holder(reinterpret_cast<char const*>(account->data()), account->size());
        batch.Put(cf("mpt_holders"), issuer + holder, item.key);
    }
}
void Store::index_ledger(rocksdb::WriteBatch& batch, VerifiedLedger const& ledger) {
    auto seq = ledger.header().sequence;
    // NFT state follows the ledger's transactions in TransactionIndex order, as Clio records it:
    // a token's state is the one left by the last transaction that moved it between pages.
    auto const& txs = ledger.transactions();
    std::vector<std::size_t> order(txs.size());
    for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(), [&](auto x, auto y) { return txs[x].position < txs[y].position; });
    std::vector<bool> tokens(txs.size());
    for (std::size_t i = 0; i < txs.size(); ++i) tokens[i] = names_tokens(ledger.record().transactions[i]);
    std::map<Bytes, NftState> moved;
    for (auto i : order) if (tokens[i]) for (auto& [id, state] : nft_changes(ledger.record().transactions[i])) moved[id] = std::move(state);
    for (auto const& [id, state] : moved)
        if (auto value = nft_value(state, seq, nft(id, seq - 1)); !value.is_null()) batch.Put(cf("nft"), id + be(~seq), value.dump());
    // The book index follows ledgers as they are added: one the store holds already (checked again
    // by Store::append) leaves it as it is.
    auto last = get("default", "last");
    bool added = last.size() != 4 || ((std::uint32_t(std::uint8_t(last[0])) << 24) | (std::uint32_t(std::uint8_t(last[1])) << 16) | (std::uint32_t(std::uint8_t(last[2])) << 8) | std::uint8_t(last[3])) < seq;
    for (auto const& patch : ledger.record().patches) { index_object(batch, patch, seq, false); if (added) index_book_directory(batch, patch, seq, false); }
    auto const& records = ledger.record().transactions;
    for (std::size_t i = 0; i < ledger.transactions().size(); ++i) {
        if (!tokens[i]) continue;
        auto const& tx = ledger.transactions()[i];
        // nft_tx lists what Clio 2.8's nft_history lists (piece_history.hpp's nft_ids), and
        // nfts_by_issuer's candidates are those tokens (and the snapshot's: index_object).
        for (auto const& id : nft_ids(records[i])) {
            batch.Put(cf("nft_tx"), id + be(~seq) + be(~tx.position), tx.hash);
            auto [issuer, entry] = nft_issuer_entry(id);
            batch.Put(cf("nft_issuers"), issuer + entry, "");
        }
        std::set<Bytes> mpts;
        try {
            auto scan = [&](auto&& self, Json const& value) -> void {
                if (value.is_object()) for (auto const& [name, field] : value.items()) {
                    if (name == "MPTokenIssuanceID" && field.is_string()) mpts.insert(from_hex(field.get<std::string>()));
                    if (field.is_structured()) self(self, field);
                }
                else if (value.is_array()) for (auto const& field : value) self(self, field);
            };
            scan(scan, transaction_json(records[i]));
        } catch (std::exception const&) {
            // A transaction this libxrpl cannot decode (some of 2013): its MPTokenIssuanceIDs read by type code.
            wire_mpt_ids(wire::parse(records[i].transaction), mpts);
            wire_mpt_ids(wire::parse(records[i].metadata), mpts);
        }
        for (auto const& id : mpts) batch.Put(cf("mpt_tx"), id + be(~seq) + be(~tx.position), tx.hash);
    }
}
// Stores indexed before the NFT indexes followed Clio 2.8 (piece_history.hpp's nft_ids and
// nft_fingerprint) are brought to them once (Store::Store). The old nft_tx lists held every
// transaction naming a token, so each still holds every one that moved it between pages and
// every one Clio lists. Per token, read oldest first: entries Clio does not list (failed
// transactions, other types) are deleted; its states are derived again from those
// transactions as index_ledger derives them now (which also adds the burns a deleted page's
// PreviousFields alone showed) and replace its entries from its first listed ledger on,
// continuing the entry before it (the snapshot's, when it held the token). nft_issuers is
// then refilled from the tokens nft_tx still lists and the snapshot's (entries at or before
// the baseline). Cost: one transaction record read and decoded per nft_tx entry. In a pruned
// store whose entry before a token's first change was pruned, a URI change first dates the
// token from its own ledger.
void Store::reindex_nft() {
    auto identity = get("default", "identity");
    std::uint32_t baseline = identity.empty() ? 0 : Json::parse(identity).at("first").get<std::uint32_t>() - 1;
    auto record_of = [&](Bytes const& hash, std::uint32_t seq) {
        auto value = get("tx", hash);
        // A repeated pseudo-transaction's later ledgers have their records under the ID and the ledger (Store::append).
        if (value.size() >= 4 && number(value) != seq) value = get("tx", hash + be(seq));
        if (value.size() < 8) throw Error("Corrupt transaction");
        auto length = number(std::string_view(value).substr(4, 4));
        if (!length || length > value.size() - 8) throw Error("Corrupt transaction length");
        return TransactionRecord{value.substr(8, length), value.substr(8 + length)};
    };
    auto check_status = [](rocksdb::Iterator& it) { if (!it.status().ok()) throw Error("RocksDB: " + it.status().ToString()); };
    std::uint64_t dropped = 0, written = 0;
    // (nft_tx key, hash) of one token's transactions, newest first as the column keeps them.
    auto reindex = [&](Bytes const& id, std::vector<std::pair<Bytes, Bytes>> const& list) {
        rocksdb::WriteBatch batch;
        std::vector<std::pair<std::uint32_t, NftState>> states;  // per ledger, ascending
        for (auto at = list.rbegin(); at != list.rend();) {
            auto seq = ~number(std::string_view(at->first).substr(32, 4));
            std::optional<NftState> state;
            for (; at != list.rend() && ~number(std::string_view(at->first).substr(32, 4)) == seq; ++at) {
                auto record = record_of(at->second, seq);
                if (!nft_ids(record).contains(id)) { batch.Delete(cf("nft_tx"), at->first); ++dropped; }
                if (auto change = nft_change(record, id)) state = std::move(change);
            }
            if (state) states.emplace_back(seq, std::move(*state));
        }
        auto first = ~number(std::string_view(list.back().first).substr(32, 4));
        Json prior;
        auto it = cursor("nft");
        for (it->Seek(id); it->Valid() && it->key().starts_with(id); it->Next()) {
            auto seq = ~number(it->key().ToString().substr(32, 4));
            if (seq >= first) batch.Delete(cf("nft"), it->key());
            else { prior = Json::parse(it->value().ToString()); break; }  // the newest before the token's first listed ledger
        }
        check_status(*it.it);
        for (auto const& [seq, state] : states)
            if (auto value = nft_value(state, seq, prior); !value.is_null()) { batch.Put(cf("nft"), id + be(~seq), value.dump()); prior = std::move(value); ++written; }
        write(batch);
    };
    Bytes token;
    std::vector<std::pair<Bytes, Bytes>> list;
    visit("nft_tx", [&](Bytes const& key, Bytes const& hash) {
        if (key.size() != 40) throw Error("Corrupt NFT transaction index");
        if (key.compare(0, 32, token) != 0) { if (!list.empty()) reindex(token, list); token = key.substr(0, 32); list.clear(); }
        list.emplace_back(key, hash);
    });
    if (!list.empty()) reindex(token, list);
    // nfts_by_issuer's candidates: what nft_tx lists now, and the snapshot's tokens.
    {
        rocksdb::WriteBatch batch;
        auto flush = [&] { if (batch.Count() >= 10000) { write(batch); batch.Clear(); } };
        visit("nft_issuers", [&](Bytes const& key, Bytes const&) { batch.Delete(cf("nft_issuers"), key); flush(); });
        Bytes previous;
        auto add = [&](Bytes const& id) {
            if (id == previous) return;
            previous = id;
            auto [issuer, entry] = nft_issuer_entry(id);
            batch.Put(cf("nft_issuers"), issuer + entry, "");
            flush();
        };
        visit("nft_tx", [&](Bytes const& key, Bytes const&) { add(key.substr(0, 32)); });
        previous.clear();
        visit("nft", [&](Bytes const& key, Bytes const&) { if (key.size() == 36 && ~number(std::string_view(key).substr(32, 4)) <= baseline) add(key.substr(0, 32)); });
        batch.Put(cf("default"), "nft_clio_indexed", Json{{"nft_tx_dropped", dropped}, {"nft_written", written}}.dump());
        write(batch);
    }
}
// Stores written before reused transaction IDs answered their earliest ledger kept the later
// one's record under the ID (Store::append). Such IDs are pseudo-transactions, which only flag
// ledgers and the ones after them hold: those ledgers' transactions are checked, oldest first,
// and an ID holding a later ledger's record takes this one's, the later going under the ID and
// its ledger. Cost: two ledgers in every 256.
void Store::keep_first_transactions() {
    auto identity = get("default", "identity"), last_bytes = get("default", "last");
    rocksdb::WriteBatch batch;
    if (!identity.empty() && last_bytes.size() == 4) {
        auto first = Json::parse(identity).at("first").get<std::uint32_t>(), last = number(last_bytes);
        for (std::uint64_t flag = first - first % 256; flag <= last; flag += 256)
            for (std::uint64_t seq = flag; seq <= flag + 1; ++seq) {
                if (seq < first || seq > last) continue;
                auto prefix = be(static_cast<std::uint32_t>(seq));
                auto it = cursor("ledger_tx");
                for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
                    auto hash = it->value().ToString();
                    auto held = get("tx", hash);
                    if (held.size() < 4 || number(held) <= seq) continue;
                    auto own = get("tx", hash + prefix);
                    if (own.size() < 4 || number(own) != seq) throw Error("Repeated transaction record missing");
                    batch.Put(cf("tx"), hash, own); batch.Put(cf("tx"), hash + held.substr(0, 4), held); batch.Delete(cf("tx"), hash + prefix);
                    write(batch); batch.Clear();
                }
                if (!it->status().ok()) throw Error("RocksDB: " + it->status().ToString());
            }
    }
    batch.Put(cf("default"), "tx_first_kept", "1");
    write(batch);
}
}
