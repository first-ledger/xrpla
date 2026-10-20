#pragma once
#include "history.hpp"
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <map>
#include <optional>
#include <mutex>

namespace xrpla::core {
class Store : public History {
    std::unique_ptr<rocksdb::DB> db_;
    std::map<std::string, rocksdb::ColumnFamilyHandle*> columns_;
    bool sync_each_ = true;
    bool bulk_ = false;
    bool writable_ = false;
    bool nft_state_ = false;  // the NFT indexes follow Clio 2.8 (Store::reindex_nft)
    // A read-only store never changes, so its iterators are kept by column and reused: creating
    // one (over every table file) cost more than most of the reads it served.
    mutable std::mutex cursors_mutex_;
    mutable std::map<std::string, std::vector<std::unique_ptr<rocksdb::Iterator>>> cursors_;
    // A read-only store's served_ledgers(), read once for the same reason: every RPC request
    // asks it, and the RPC layer reopens the store (open_store) to see newer ledgers.
    mutable std::once_flag served_once_;
    mutable std::optional<std::pair<std::uint32_t, std::uint32_t>> served_;
    struct Cursor {
        Store const* store;
        std::string column;
        std::unique_ptr<rocksdb::Iterator> it;
        Cursor(Store const* s, std::string c, std::unique_ptr<rocksdb::Iterator> i) : store(s), column(std::move(c)), it(std::move(i)) {}
        Cursor(Cursor&&) = default;
        ~Cursor();
        rocksdb::Iterator* operator->() const { return it.get(); }
    };
    Cursor cursor(std::string const& column) const;
    LedgerRecord read_ledger(std::uint32_t sequence, bool patches) const;
    // The baseline ledger, served whole when it has no transactions and its state is kept: its
    // header and its state are all of it (on mainnet, ledger 32,570, the first the network kept).
    std::optional<Header> served_baseline() const;
    rocksdb::ColumnFamilyHandle* cf(std::string const&) const;
    void write(rocksdb::WriteBatch&);
    std::string get(std::string const& column, Bytes const& key) const;
    // One-time upgrades of stores written by older builds, run by a writable open: the NFT
    // indexes as Clio 2.8 keeps them, and a reused transaction ID's earliest ledger under the ID.
    void reindex_nft();
    void keep_first_transactions();
public:
    // `bulk`: a batch job's load (a restore). Writes skip RocksDB's log, and every flush covers
    // all columns at once, so a crash leaves the store as of its last flush, consistent; the
    // job resumes from what the store holds. flush() at the end writes the rest.
    Store(fs::path const&, bool writable, bool bulk = false);
    ~Store();
    void initialize(LedgerVerifier const&, Range);
    void initialize(Header const&, Range, std::function<void(std::function<void(ObjectRecord const&)> const&)> const&);
    bool append(VerifiedLedger const&);
    // Each write is synced by default. A batch job that marks its own progress (a restore)
    // turns that off and calls sync() before each mark: a crash can then lose only ledgers
    // after the last mark, which a resume appends again.
    void sync_each_write(bool sync) { sync_each_ = sync; }
    void sync();
    void flush();
    Json status() const override;
    std::optional<std::uint32_t> baseline_ledger() const override;
    std::optional<std::pair<std::uint32_t, std::uint32_t>> served_ledgers() const override;
    bool initialized() const;
    Json checkpoint_files() const;
    void finish_admission(Json const& files);
    Json query(std::string const& kind, std::string const& key, std::uint32_t sequence) const;
    Bytes header(std::uint32_t sequence) const override;
    std::uint32_t ledger_sequence(Bytes const& hash) const override;
    Bytes object(Bytes const& key, std::uint32_t sequence) const override;
    std::vector<Bytes> objects(std::vector<Bytes> const& keys, std::uint32_t sequence) const override;
    std::vector<TransactionRecord> ledger_transaction_records(std::uint32_t sequence) const override;
    std::optional<std::vector<Bytes>> book_directories(std::uint32_t sequence, Bytes const& after, Bytes const& end, unsigned limit) const override;
    // book_dirs rows for a ledger's changed objects (or a snapshot's, `baseline`): an order book
    // directory created or deleted at `sequence`.
    void index_book_directory(rocksdb::WriteBatch&, ObjectRecord const&, std::uint32_t sequence, bool baseline);
    std::optional<std::pair<std::uint32_t, Bytes>> deleted_object(Bytes const& key, std::uint32_t sequence) const override;
    Json object_page(std::uint32_t sequence, std::string const& marker, unsigned limit, bool binary = false) const override;
    Json history_page(std::string const& column, Bytes const& account, std::uint32_t first,
        std::uint32_t last, unsigned limit, bool forward, Json const& marker) const override;
    Json transaction(Bytes const& hash, bool binary = false) const override;
    std::vector<Bytes> ledger_transactions(std::uint32_t sequence) const override;
    LedgerRecord ledger_record(std::uint32_t sequence) const override;
    Bytes predecessor(Bytes const& key, std::uint32_t sequence) const;
    void visit(std::string const& column, std::function<void(Bytes const&, Bytes const&)> const&) const;
    // The keys from `from` up to (not including) `to`, in order; an empty `to` is the column's end.
    // Ranges of one column can be visited at once, on several threads.
    void visit(std::string const& column, Bytes const& from, Bytes const& to, std::function<void(Bytes const&, Bytes const&)> const&) const;
    // The objects live at `sequence` (key and contents), in key order, from the version key
    // `from` (empty: the first) to the key `to` (excluded; empty: the last), until `visitor`
    // returns false. object_page reads its pages so.
    void visit_live(std::uint32_t sequence, Bytes const& from, Bytes const& to, std::function<bool(Bytes const&, Bytes const&)> const& visitor) const;
    // Raises the block cache every store in the process shares (512 MB by default) to `bytes`:
    // a batch job reading a store over and over (import_clio) keeps more of it uncompressed.
    static void reserve_block_cache(std::size_t bytes);
    // A column's table files, in bytes (as stored: compressed).
    std::uint64_t column_bytes(std::string const& column) const;
    // Snapshot objects seed NFT state from their pages; ledgers pass nft_pages = false (index_ledger derives it).
    void index_object(rocksdb::WriteBatch&, ObjectRecord const&, std::uint32_t, bool nft_pages = true);
    void index_ledger(rocksdb::WriteBatch&, VerifiedLedger const&);
    Json nft(Bytes const&, std::uint32_t) const override;
    bool has_nft_state() const override { return nft_state_; }
    std::optional<std::vector<Bytes>> mpt_holders(Bytes const& issuance, Bytes const& after, unsigned limit) const override;
    std::optional<std::vector<Bytes>> nfts_by_issuer(Bytes const& issuer, std::optional<std::uint32_t> taxon, Bytes const& after, unsigned limit) const override;
    Json prune(std::uint32_t state_first);
    void compact();
};
}
