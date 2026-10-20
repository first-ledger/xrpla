#pragma once
#include <xrpla/core/verify.hpp>

namespace xrpla::core {
// The read surface the RPC layer needs. The RocksDB Store implements it over an
// imported database; PieceHistory implements it over published pieces and
// bundles in their compressed form, plus a small local query index.
class History {
public:
    virtual ~History() = default;
    virtual Json status() const = 0;
    // The ledger just before complete_ledgers when it is held whole all the same: a baseline
    // snapshot's ledger without transactions (its header and its state are all of it). On mainnet,
    // ledger 32,570, the first the network kept. The RPC layer serves it as a ledger; status() and
    // what builds on it (extension, imports, restores) keep to the ledgers with records.
    virtual std::optional<std::uint32_t> baseline_ledger() const { return std::nullopt; }
    // The ledgers the RPC layer serves, first and last: status()'s complete_ledgers, from
    // baseline_ledger() when that is the ledger just before them; nullopt while none is held.
    // Every request reads it first, so a backend overrides it to answer from what it keeps in
    // memory: status() reads and decodes the newest header (and the Store parses its identity)
    // for fields no request but server_info shows. A history whose newest ledger advances (a
    // live tail) answers with its newest as of the call, never one it does not yet hold.
    virtual std::optional<std::pair<std::uint32_t, std::uint32_t>> served_ledgers() const {
        auto const range = status().at("complete_ledgers");
        if (range.is_null()) return std::nullopt;
        std::pair<std::uint32_t, std::uint32_t> held{range.at("first").get<std::uint32_t>(), range.at("last").get<std::uint32_t>()};
        if (auto baseline = baseline_ledger(); baseline && *baseline + 1 == held.first) held.first = *baseline;
        return held;
    }
    virtual Bytes header(std::uint32_t sequence) const = 0;
    virtual std::uint32_t ledger_sequence(Bytes const& hash) const = 0;
    // The object's canonical bytes as of `sequence`; empty when it does not exist.
    virtual Bytes object(Bytes const& key, std::uint32_t sequence) const = 0;
    // object() for many keys at once, in their order. A backend may read them in bulk: each
    // compressed page or block once, however the keys are spread (an issuer's trust lines).
    virtual std::vector<Bytes> objects(std::vector<Bytes> const& keys, std::uint32_t sequence) const {
        std::vector<Bytes> out;
        out.reserve(keys.size());
        for (auto const& key : keys) out.push_back(object(key, sequence));
        return out;
    }
    // An object whose newest version at or before `sequence` deletes it: the ledger that
    // deleted it and its last version before then (Clio's ledger_entry include_deleted).
    // Nothing when it exists at `sequence`, or no deletion of it is held.
    virtual std::optional<std::pair<std::uint32_t, Bytes>> deleted_object(Bytes const&, std::uint32_t) const { return std::nullopt; }
    virtual Json object_page(std::uint32_t sequence, std::string const& marker, unsigned limit, bool binary = false) const = 0;
    virtual Json history_page(std::string const& column, Bytes const& key, std::uint32_t first,
        std::uint32_t last, unsigned limit, bool forward, Json const& marker) const = 0;
    virtual Json transaction(Bytes const& hash, bool binary = false) const = 0;
    // Transaction hashes of one ledger in TransactionIndex order.
    virtual std::vector<Bytes> ledger_transactions(std::uint32_t sequence) const = 0;
    virtual LedgerRecord ledger_record(std::uint32_t sequence) const = 0;
    // A ledger's transactions alone (book_changes), without reading the objects it changed.
    virtual std::vector<TransactionRecord> ledger_transaction_records(std::uint32_t sequence) const { return ledger_record(sequence).transactions; }
    virtual Json nft(Bytes const&, std::uint32_t) const = 0;
    // nft() for several tokens; a backend may load them concurrently.
    virtual std::vector<Json> nfts(std::vector<Bytes> const& ids, std::uint32_t sequence) const {
        std::vector<Json> out;
        for (auto const& id : ids) out.push_back(nft(id, sequence));
        return out;
    }
    virtual bool has_nft_state() const { return true; }
    // Candidates for Clio's mpt_holders and nfts_by_issuer: every holder (AccountID) of an
    // issuance (24-byte ID), or every token of an issuer (20-byte AccountID; only `taxon`'s when
    // given, in (taxon, token ID) order), that any held ledger has seen, after `after` (a holder,
    // or a token ID; empty: from the start), at most `limit`. Callers check each at the ledger
    // asked about. nullopt: this backend keeps no such index.
    virtual std::optional<std::vector<Bytes>> mpt_holders(Bytes const&, Bytes const&, unsigned) const { return std::nullopt; }
    virtual std::optional<std::vector<Bytes>> nfts_by_issuer(Bytes const&, std::optional<std::uint32_t>, Bytes const&, unsigned) const { return std::nullopt; }
    // A hint that these objects are about to be read at `sequence` (a directory page's
    // entries); a backend may load them concurrently.
    virtual void prefetch(std::vector<Bytes> const&, std::uint32_t) const {}
    // Whether prefetch() does anything, so callers can skip planning reads for it.
    virtual bool prefetches() const { return false; }
    // Up to `limit` keys of objects that exist at `sequence`, ascending, in (after, end). A
    // backend that does not list keys (lists_keys() false) returns none: then they are only
    // for planning prefetches.
    virtual std::vector<Bytes> keys_between(std::uint32_t, Bytes const&, Bytes const&, unsigned) const { return {}; }
    virtual bool lists_keys() const { return false; }
    // The order book directories (one per price level) that exist at `sequence` with keys in
    // (after, end), ascending, at most `limit`, from an index of when each was created and
    // deleted; nullopt when this backend keeps no such index (book_offers then walks the state).
    virtual std::optional<std::vector<Bytes>> book_directories(std::uint32_t, Bytes const&, Bytes const&, unsigned) const { return std::nullopt; }
    // Hit rates and sizes of the decode caches (xrpla_cache_stats); null for a backend without any.
    virtual Json cache_stats() const { return nullptr; }
};
}
