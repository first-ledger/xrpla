#pragma once
#include "peer.hpp"
#include "peer_relay.hpp"
#include <xrpla/core/verify.hpp>

namespace xrpla::core {
class PeerLedgers;
// What this node serves to peers (docs/PEER-SERVING.md): the newest `window` validated ledgers
// in full. Headers are kept here; state and transaction trees are read from the follower's
// node store, which keeps them for the window (LedgerVerifier::keep_ledgers). Requests are
// answered on worker threads as xrpld answers them (nothing for a ledger it lacks), and each
// peer is charged as xrpld charges, by address: one whose balance reaches 25,000 is dropped.
class LedgerServer final : public PeerResponder {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    explicit LedgerServer(std::size_t window, unsigned threads = 4);
    ~LedgerServer() override;
    std::size_t window() const;
    // The node store the trees are read from (replaced whenever the verifier is). Once it
    // returns, requests in progress have finished and no reference to the old store remains:
    // call with null before the store is opened again.
    void node_store(std::shared_ptr<xrpl::node_store::Database> store);
    // A validated ledger now served, newest last (added again or out of order: ignored). The
    // oldest beyond the window leaves; a ledger whose trees are not stored is not served.
    void add(Header const& header);
    // The ledgers served, (first, last).
    std::optional<std::pair<std::uint32_t, std::uint32_t>> range() const;
    std::optional<Header> newest() const;
    // A TMStatusChange naming the newest ledger and the range served (empty when none).
    std::string status_message() const;
    // Tells a new session this node's status.
    void greet(PeerSession& session) const;
    // Node keys (base58) whose requests are never charged (xrpld's cluster peers).
    void reserve(std::set<std::string> keys);
    void request(int type, std::string const& payload, PeerRequester const& from) override;
    // Charges a peer for something else it sent (dropping it as a request's charge would).
    void charge(PeerRequester const& from, double fee);
    // Whether a session from `address` may open: not while the balance of a peer dropped
    // from it is still at xrpld's drop level (xrpld refuses such a reconnect too).
    bool admits(std::string const& address) const;
    // Counts since start: requests, answered, unanswered (by kind in unanswered_kinds: tx_set,
    // transactions, ledger_newer, ledger_older, ledger_unknown, tree_nodes, fetch_pack,
    // proof_path, replay_delta, send_queue), nodes and bytes sent, peers dropped, busy (the
    // queue, or the peer's share of it, full); in
    // `reserved`, the part of those from reserved peers; and in `unserved_ledgers`, of the
    // ledgers other peers asked for by hash while not served, how many were served afterwards
    // (by how long after the first request), not served within five minutes, or still pending.
    Json status() const;
};

// A serving node's inbound side: accepts peers on `listen` (as xrpld does, up to
// `max_inbound`, two per address), tells every peer, inbound and outbound, of each new ledger
// served, advertises its own endpoint every 151 s while it has room, takes validations,
// manifests and endpoints from inbound peers as from outbound ones, and answers /crawl (when
// `crawl`) and /health on the peer port.
class PeerOverlay {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    struct Options {
        std::string listen;
        unsigned max_inbound = 11;
        bool crawl = false;
        std::uint32_t network_id = 0;
        // Node keys (base58) always admitted, beyond the inbound and per-address limits
        // (xrpld's [peer_reservations]).
        std::set<std::string> reserved;
        unsigned ip_limit = 2;       // inbound peers from one address (xrpld's [overlay] ip_limit)
        bool private_mode = false;   // never advertise this node's endpoint; crawls see no address
        // Validations, proposals and squelches from inbound peers go through it.
        std::shared_ptr<PeerRelay> relay;
    };
    PeerOverlay(Options options, PeerLedgers& outbound, std::shared_ptr<LedgerServer> server,
                std::function<void(Bytes const&)> on_validation, std::function<void(Bytes const&)> on_manifest);
    ~PeerOverlay();
    std::uint16_t port() const;
    // Calls `visit` with every open inbound session.
    void visit(std::function<void(PeerSession&)> const& visit) const;
    // Tells every peer the newest ledger served (call after LedgerServer::add).
    void announce();
    Json status() const;
    // The /crawl and /health documents (as served on the peer port).
    Json crawl() const;
    std::pair<int, Json> health() const;
};
}
