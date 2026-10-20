#pragma once
#include "peer.hpp"
#include "peer_relay.hpp"
#include <functional>

namespace xrpla::core {
// Validated ledgers read from the XRPL peer network instead of a node's API. Keeps a few peer
// sessions open (reconnecting, and following the peers full servers suggest) and fetches by
// ledger hash: headers, every transaction with its metadata (the transaction tree), and the
// state objects at given keys (their paths through the account-state tree). Every tree node
// is checked against the hash its parent (or the header) commits to, so a peer can withhold
// data but not alter it (one that sends data that does not check is asked no more than one
// that does not answer); the verifier then checks each ledger as a whole. Requests go out as
// xrpld's own (no request cookie, so each peer charges them) and are spread over the peers:
// a few out to each at once, and never more than keeps what each charges well below where
// xrpld warns.
class PeerLedgers {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    struct Options {
        std::vector<std::string> peers;  // kept connected (reconnected when dropped)
        fs::path identity;               // empty: a new node key
        std::uint32_t network_id = 0;
        unsigned connections = 10;  // as a default xrpld keeps (PeerFinder: 10 outbound)
        // This node's side of each session: what it serves, its crawl setting and newest ledger.
        std::shared_ptr<PeerResponder> responder;
        bool crawl = false, ledger_replay = false;
        std::function<std::optional<std::pair<Bytes, Bytes>>()> closed;
        // Called with each new session (to tell it this node's status).
        std::function<void(PeerSession&)> connected;
        // Where to join the network (empty: mainnet's public hubs, unless `peers` are given).
        // Sessions go first to `peers`, then to peers the network names; the bootstrap
        // endpoints serve only while too few others are known, and once others are, at most
        // `bootstrap_sessions` of them stay (one more gives way every `rebalance`).
        std::vector<std::string> bootstrap;
        unsigned bootstrap_sessions = 1;
        std::chrono::seconds rebalance{60};
        // Only `peers` (or the bootstrap endpoints), never ones other peers name; and this
        // node's address kept out of crawls.
        bool private_mode = false;
        // Validations, proposals and squelches go through it: relayed, and validations counted.
        std::shared_ptr<PeerRelay> relay;
    };
    // Validations and manifests peers relay (serialized STValidation and manifest objects).
    PeerLedgers(Options options, std::function<void(Bytes const&)> on_validation, std::function<void(Bytes const&)> on_manifest);
    ~PeerLedgers();
    // The header of the ledger with this hash (checked: it hashes to `hash`). Given the ledger's
    // sequence, peers that say they hold it are asked first.
    Header header(Bytes const& hash, std::uint32_t sequence = 0);
    // The ledger's transactions (tx, metadata) in transaction-tree order, checked against its
    // transaction root.
    std::vector<TransactionRecord> transactions(Header const& header);
    // The objects at `keys` in the ledger's state (empty where none), checked against its
    // account-state root.
    std::vector<Bytes> objects(Header const& header, std::vector<Bytes> const& keys);
    // The ledger's transactions and the objects at the keys `keys_of` names for them, with the
    // top of the state tree (which nearly every ledger changes) fetched while the transactions
    // are: one round of requests fewer than transactions() then objects().
    struct Contents { std::vector<TransactionRecord> transactions; std::vector<Bytes> keys, values; };
    Contents contents(Header const& header, std::function<std::vector<Bytes>(std::vector<TransactionRecord> const&)> const& keys_of);
    // Every object the ledger's state holds differently from `parent` (the verified state of its
    // parent), by comparing the two state trees: only subtrees whose hashes differ are fetched
    // (each node checked against its hash) and their leaves compared by key. Sorted by key; an
    // empty object is one the ledger deleted. Independent of what metadata names.
    std::vector<ObjectRecord> changes(Header const& header, LedgerVerifier const& parent);
    // The hash at least two open peers announced for a ledger they accepted (status messages,
    // sent as each finishes building it, before validations arrive): where fetching may start.
    // Never a reason to trust the ledger.
    std::optional<Bytes> announced(std::uint32_t sequence) const;
    Json status() const;
    // Sends a message to every open session (e.g. this node's status).
    void broadcast(int type, std::string const& payload);
    // Calls `visit` with every open session.
    void visit(std::function<void(PeerSession&)> const& visit) const;
    std::vector<PeerSession::Info> sessions() const;
    // Endpoints to try: configured, suggested by full servers, relayed by peers.
    std::vector<std::string> known() const;
    void learn(std::vector<std::string> const& endpoints);
    NodeIdentity const& identity() const;
};
}
