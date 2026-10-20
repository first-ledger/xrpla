#pragma once
#include "piece_history.hpp"
#include <memory>

namespace xrpla::core {
// The read surface over sealed history plus a live tail: a PieceHistory (the piece cache)
// and the ledgers after it that a follower has committed to its spool (a signed v2 archive,
// as `xrpla publish` writes). The tail keeps in-memory indexes (headers, transaction
// locations, object versions, account and NFT postings, NFT states) and reads ledger records
// from spool chunks through a small cache. Every History method merges the two, so answers
// span both as if they were one store.
class LiveHistory : public History {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    LiveHistory(std::shared_ptr<PieceHistory const> base, fs::path const& spool, fs::path const& spool_key);
    ~LiveHistory() override;
    // Indexes ledgers the spool has committed since the last call; returns the tail's tip
    // (the base's last ledger while the tail is empty).
    std::uint32_t refresh();
    Json status() const override;
    std::optional<std::uint32_t> baseline_ledger() const override;
    std::optional<std::pair<std::uint32_t, std::uint32_t>> served_ledgers() const override;
    std::vector<TransactionRecord> ledger_transaction_records(std::uint32_t sequence) const override;
    Bytes header(std::uint32_t sequence) const override;
    std::uint32_t ledger_sequence(Bytes const& hash) const override;
    Bytes object(Bytes const& key, std::uint32_t sequence) const override;
    std::optional<std::pair<std::uint32_t, Bytes>> deleted_object(Bytes const& key, std::uint32_t sequence) const override;
    Json object_page(std::uint32_t sequence, std::string const& marker, unsigned limit, bool binary = false) const override;
    Json history_page(std::string const& column, Bytes const& key, std::uint32_t first,
        std::uint32_t last, unsigned limit, bool forward, Json const& marker) const override;
    Json transaction(Bytes const& hash, bool binary = false) const override;
    std::vector<Bytes> ledger_transactions(std::uint32_t sequence) const override;
    LedgerRecord ledger_record(std::uint32_t sequence) const override;
    Json nft(Bytes const&, std::uint32_t) const override;
    bool has_nft_state() const override;
    std::vector<Json> nfts(std::vector<Bytes> const& ids, std::uint32_t sequence) const override;
    std::optional<std::vector<Bytes>> mpt_holders(Bytes const& issuance, Bytes const& after, unsigned limit) const override;
    std::optional<std::vector<Bytes>> nfts_by_issuer(Bytes const& issuer, std::optional<std::uint32_t> taxon, Bytes const& after, unsigned limit) const override;
    void prefetch(std::vector<Bytes> const&, std::uint32_t) const override;
    std::vector<Bytes> objects(std::vector<Bytes> const&, std::uint32_t) const override;
    bool prefetches() const override { return true; }
    bool lists_keys() const override { return true; }
    std::vector<Bytes> keys_between(std::uint32_t, Bytes const&, Bytes const&, unsigned) const override;
    std::optional<std::vector<Bytes>> book_directories(std::uint32_t, Bytes const&, Bytes const&, unsigned) const override;
    Json cache_stats() const override;  // the base's: the tail decodes its chunks without these caches
};
// While set, RPC requests are answered from this history instead of the served directory.
void use_live_history(std::shared_ptr<History const>);
std::shared_ptr<History const> live_history();
// Answers RPC requests from the piece cache in `dir` plus the tail in `tail`: a spool, or a
// live publisher's work directory whose `current` file names its spool. The tail is refreshed
// every two seconds and rebuilt when the cache switches selection or the spool is rotated.
void follow_tail(fs::path const& dir, fs::path const& tail, fs::path const& tail_key);
}
