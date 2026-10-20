#pragma once
#include <xrpla/core/format.hpp>
#include <future>
#include <memory>
#include <optional>

namespace xrpl::node_store { class Database; }
namespace json { class Value; }  // libxrpl's

namespace xrpla::core {
Bytes from_hex(std::string const&);
std::string to_hex(std::string_view);
struct Header {
    std::uint32_t sequence;
    std::string hash, parent_hash, account_hash, transaction_hash;
    Bytes raw;
    std::uint32_t close_time = 0;
};
Header decode_header(Bytes const&);
Json header_json(Header const&);
Json object_json(ObjectRecord const&);
// libxrpl's JSON as ours, converted directly: rendering it as text to parse again cost more than
// the decoding behind it for large listings.
Json to_json(::json::Value const&);
Json transaction_json(TransactionRecord const&);
// Whether libxrpl decodes the transaction and its metadata. xrpld's APIs cannot show one it does
// not (a few of 2013: txnNotFound, and left out of ledger and account_tx listings), though the
// ledger holds it. The archive, the History readers and every restore keep it; only the RPC
// server hides it, answering as xrpld.
bool api_decodable(TransactionRecord const&);

struct TransactionIndex {
    Bytes hash;
    std::uint32_t position;
    std::vector<Bytes> accounts;
    // As xrpld's Transactions table records them: type name, sending account, its Sequence.
    std::string type;
    Bytes account;
    std::uint32_t sequence = 0;
};
std::vector<TransactionIndex> index_transactions(LedgerRecord const&);
// The root hash (hex) of the transaction tree holding these transactions, as a ledger header
// commits to it (all zeros for none). Transactions are read raw: none needs to parse.
std::string transaction_root(std::vector<TransactionRecord> const&);
// The objects a ledger's close changes outside its transactions, which no metadata names (as
// xrpld's buildLedger makes them): the skip lists (the recent hashes every ledger, and the
// page holding the previous ledger's, updated when that one is a multiple of 256) and, on flag
// ledgers (sequences divisible by 256), the negative UNL, whose voted disables and re-enables
// take effect then (updateNegativeUNL changes or deletes it). A ledger's changed objects are
// these and the ones its transactions' metadata names.
std::vector<Bytes> closing_keys(std::uint32_t sequence);
// Only the canonical verifier can create a value accepted by the store.
class VerifiedLedger {
    friend class LedgerVerifier;
    friend class AdmissionJournal;
    friend struct StoreRestore;
    Header header_;
    LedgerRecord record_;
    std::vector<TransactionIndex> transactions_;
    VerifiedLedger(Header h, LedgerRecord r, std::vector<TransactionIndex> tx)
        : header_(std::move(h)), record_(std::move(r)), transactions_(std::move(tx)) {}
public:
    Header const& header() const { return header_; }
    LedgerRecord const& record() const { return record_; }
    std::vector<TransactionIndex> const& transactions() const { return transactions_; }
};
class LedgerVerifier {
    struct Impl;
    std::unique_ptr<Impl> impl_;
    // apply's checks and state change; the ledger's header and transaction indexes once it is in.
    std::pair<Header, std::vector<TransactionIndex>> verify_and_apply(LedgerRecord const&);
public:
    // Receives every node written (type, hash, serialized node); called from several threads.
    using NodeSink = std::function<void(std::uint8_t, Bytes const&, Bytes const&)>;
    explicit LedgerVerifier(fs::path const& backing = {}, NodeSink sink = {});
    // A native importer can persist and read the verified SHAMap in its final
    // NodeStore, avoiding a second copy of every historical node in scratch.
    explicit LedgerVerifier(std::shared_ptr<xrpl::node_store::Database> database);
    ~LedgerVerifier();
    void add_snapshot_object(ObjectRecord const&);
    // The same for a sorted batch (e.g. one snapshot part): checks and hashing run in parallel.
    void add_snapshot_objects(std::vector<ObjectRecord> const&);
    void add_snapshot_objects(std::vector<ObjectRecord>&&);
    Header finish_snapshot(Bytes const& header, std::uint32_t sequence, std::string const& account_hash);
    // Verifies the next ledger against the current one and makes it current. The verified
    // ledger holds the record: a copy of it, or the record itself when it is handed over.
    VerifiedLedger apply(LedgerRecord const&);
    VerifiedLedger apply(LedgerRecord&&);
    // The same checks and change, for callers that keep the record and need no VerifiedLedger
    // (an export, a replay): nothing is copied.
    void advance(LedgerRecord const&);
    // A hint for a disk-backed verifier: the tree nodes on the paths to these keys (objects a
    // coming ledger changes) are read from the node store on threads of their own, from the
    // newest verified state, so applying that ledger finds them in memory instead of waiting on
    // the store. Verifies and changes nothing; ignored in memory. Callable from any thread.
    void prefetch(std::vector<Bytes> keys);
    Header const& header() const;
    void state_objects(std::function<void(ObjectRecord const&)> const&) const;
    // The current state tree's node at a position (33 bytes: key prefix and depth, as a
    // SHAMapNodeID serializes it) and, to `depth` levels, the nodes below it, each as (position,
    // wire form); empty when the position holds nothing.
    std::vector<std::pair<Bytes, Bytes>> state_nodes(Bytes const& position, unsigned depth) const;
    Bytes state_object(Bytes const& key) const;
    // The current state's first key after `key` (32 0xff bytes when none) and its last key
    // before it (32 zero bytes when none); `key` need not be in the state.
    Bytes state_successor(Bytes const& key) const;
    Bytes state_predecessor(Bytes const& key) const;
    void resume(Bytes const& verified_header);
    // Back to a ledger this verifier verified and persisted (a follower's committed tip, after a
    // batch that failed part-way): its state is read again from the node store and the ledgers
    // applied since are forgotten (their nodes stay until a compaction). Work under way, a
    // background compaction, carries on.
    void rewind(Bytes const& verified_header);
    // The current ledger is the one a failed batch goes back to (by rewind, or a later run's
    // resume): automatic compaction keeps its state tree, as well as the current one, until the
    // next commit point. resume and rewind set it to the ledger they go to.
    void commit_point();
    void persist();
    // Rewrite the disk-backed node store to hold only the current state tree; returns the
    // nodes kept. Every applied ledger otherwise leaves its old nodes behind (~0.7 MB each).
    std::uint64_t compact_nodes();
    // The same on a thread of its own, for a follower that keeps applying ledgers: it copies
    // what the current state (and the kept ledgers' trees) reach while nodes written meanwhile
    // go to the copy too, and the copy is swapped in once it is ready (a pause of about a
    // second instead of the whole walk), by finish_compaction or a persist in background
    // mode. finish_compaction reports a compaction once, whichever call swapped it in (with
    // `wait`, waiting for a running one). The walk waits for `gate` when given (tests).
    // start_compaction: false when one is running, or the state is in memory.
    struct Compaction { std::uint64_t nodes; double seconds; std::uint32_t ledger; };
    bool start_compaction(std::shared_future<void> gate = {});
    std::optional<Compaction> finish_compaction(bool wait = false);
    bool compacting() const;
    // Automatic compaction (below) runs as start_compaction does, finishing on a later persist.
    void compact_in_background(bool);
    // Disk-backed verifiers compact on their own once superseded nodes outgrow the live set
    // (at least `nodes`, 4M by default); tests lower the floor.
    void compaction_floor(std::uint64_t nodes);
    // A disk-backed verifier writes new nodes in batches of `nodes` (65,536 by default), each
    // in the background while the next fills; tests lower it.
    void write_batches(std::uint64_t nodes);
    // A disk-backed verifier keeps the state tree nodes it has touched in memory and writes
    // them out (dropping them) every `ledgers` ledgers or `objects` changed objects: 64 and
    // 4,096 by default, for long-running followers. Batch jobs (restores) keep far more: every
    // drop means reading the tree's upper levels from disk again.
    void retain(std::uint64_t ledgers, std::uint64_t objects);
    // Seconds apply has spent in each of its parts.
    Json timings() const;
    // Keep the trees of the newest `ledgers` ledgers, not only the latest state, so readers (a
    // peer server) can walk them: their transaction trees are stored as they apply, and
    // compaction keeps every node reachable from their state and transaction roots. Ledgers
    // are recorded as they apply; keep_ledger adds an earlier one (after a restart, those
    // before the ledger resumed at), oldest first.
    void keep_ledgers(std::size_t ledgers);
    void keep_ledger(Header const& header);
    // The disk-backed node store, which readers on other threads may share; null in memory.
    std::shared_ptr<xrpl::node_store::Database> node_store() const;
    // Only the current (verified) ledger's transaction tree, checked against its header and
    // stored: a baseline that already holds its state changes. The record must be that ledger's.
    void persist_transactions(LedgerRecord const&);
};
// Writes the verifier's current state as a v2 snapshot under `out`: snapshots/SEQ.HASH16/
// holds parts of about 64 MiB (zstd `level`, `jobs` compressed at once) and index.json, each
// synced with its directory entries (index.json last, renamed in whole). Returns the catalog descriptor (seq, prefix, account_hash, ledger_hash, index,
// parts, total_size, objects); `timings`, when given, receives where the time went.
Json write_state_snapshot(LedgerVerifier& verifier, fs::path const& out, int level, unsigned jobs, Json* timings = nullptr);
// The descriptor write_state_snapshot returned for `header`'s ledger, rebuilt from what it left
// under `out` (snapshots/SEQ.HASH16/index.json and its parts), so a snapshot already written is
// not compressed again. Only when the index is of exactly this ledger (its whole header) and
// every part it lists is there with its size and SHA-256; nullopt otherwise (write it again).
// The parts' objects are not decoded: they are trusted as far as the digests their writer took
// (which the index beside them lists), so this alone only says what is on disk.
std::optional<Json> existing_state_snapshot(Header const& header, fs::path const& out);
// The same for the verifier's current ledger, taken only when the parts also hold exactly its
// verified state: every object decoded and compared with it in key order (no compression, so
// far cheaper than writing the snapshot again). What is reused is then trusted as far as the
// verifier, not as far as digests the index itself lists.
std::optional<Json> existing_state_snapshot(LedgerVerifier const& verifier, fs::path const& out);
struct VerifyOptions {
    fs::path dir, publisher_key;
    std::string trust_ledger_hash;
};
Json verify_full(VerifyOptions const&);
Json build_store(VerifyOptions const&);
Json store_status(fs::path const&);
Json query_store(fs::path const&, std::string const& kind, std::string const& key,
                 std::uint32_t sequence = 0);
} // namespace xrpla::core
