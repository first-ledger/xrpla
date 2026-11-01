#pragma once
#include <xrpla/core/verify.hpp>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

namespace google::protobuf { class MessageLite; }

namespace xrpla::core {
// This process's identity on the XRPL peer network: a secp256k1 node key pair. Kept in a file
// (the 32-byte secret key) so peers see the same node across restarts; created when missing.
struct NodeIdentity {
    Bytes public_key, secret_key;
    static NodeIdentity load_or_create(fs::path const& file);
    static NodeIdentity ephemeral();
    std::string base58() const;  // as xrpld prints node public keys (n...)
};

// A full server answered the handshake with 503 and suggested other peers.
struct PeerRedirect : Error {
    std::vector<std::string> peers;
    PeerRedirect(std::string const& what, std::vector<std::string> suggested) : Error(what), peers(std::move(suggested)) {}
};

// The peer that asked: `reply` sends it a message and `drop` ends its session (both from any
// thread, and ignored once the session has ended).
struct PeerRequester {
    std::function<void(int type, std::string const& payload)> reply;
    std::function<void(std::string const& why)> drop;
    std::string public_key, endpoint;
    bool ledger_replay = false;
    std::string address;        // its remote IP (charges are kept by its address_group, as xrpld keeps them by IP)
    std::uint64_t session = 0;  // its session's Info::id
    // Whether its send queue holds 192 messages or 16 MiB, as xrpld declines requests from a
    // peer it cannot send to (kDropSendQueue): replies would only wait behind it.
    std::function<bool()> backlogged;
};
// Answers what peers ask this node for (ledger data: TMGetLedger, TMGetObjectByHash queries,
// ledger replay requests). Called on the asking session's I/O thread, so anything slower
// than a lookup belongs on other threads, replying later through the requester.
struct PeerResponder {
    virtual ~PeerResponder() = default;
    virtual void request(int type, std::string const& payload, PeerRequester const& from) = 0;
};
// This node serves no ledger data and answers as xrpld does for data it lacks: no reply to a
// ledger request (xrpld charges the sender of an error-only TMLedgerData), and an object
// query answered with none of the objects.
std::shared_ptr<PeerResponder> serving_nothing();

// This node's side of a handshake.
struct PeerLocal {
    NodeIdentity identity;
    std::uint32_t network_id = 0;
    bool crawl = false;          // Crawl: public (crawlers may list this node's address)
    bool ledger_replay = false;  // X-Protocol-Ctl ledgerreplay=1: proof path and replay delta messages
    std::shared_ptr<PeerResponder> responder = serving_nothing();
    // The newest ledger this node has, (hash, parent hash), for Closed-Ledger/Previous-Ledger.
    std::function<std::optional<std::pair<Bytes, Bytes>>()> closed;
    bool squelch = false;        // X-Protocol-Ctl vprr=1: squelch messages (validator reduce-relay)
};

// One connection to an XRPL peer, as xrpld's overlay speaks it: TLS 1.2, an HTTP upgrade to
// XRPL/2.2 in which both sides sign the TLS session (the SHA-512 half of the XOR of the hashed
// Finished messages) with their node keys, then framed protobuf messages from xrpl.proto (a
// 4-byte length, a 2-byte type; no compression is offered). An internal thread owns the
// stream: it answers pings, hands peers' requests to the responder and queues the message
// types asked for (others are dropped as they arrive, unread; one larger than its type ever is
// ends the session). As xrpld's peer timer does, it pings the peer every minute and ends the
// session when a ping is still unanswered a minute later, or when 128 messages or more have
// waited to be sent at four checks in a row. Outbound sessions connect to an endpoint;
// inbound ones come from a PeerListener.
class PeerSession {
    struct Impl;
    std::shared_ptr<Impl> impl_;
    PeerSession();
    friend class PeerListener;
public:
    struct Info {
        std::uint64_t id = 0;  // unique among this process's sessions
        std::string endpoint, public_key, server, closed_ledger, network_id;
        std::string address;  // the remote IP
        bool inbound = false;
        bool crawl = false;          // the peer's Crawl: public
        bool ledger_replay = false;  // both sides enabled ledger replay
        bool squelch = false;        // both sides enabled squelch messages (vprr)
        std::chrono::steady_clock::time_point connected;
    };
    struct Message { int type; std::string payload; };
    // `wanted`: message types queued for receive() (others are read and dropped).
    PeerSession(std::string const& endpoint, NodeIdentity const& identity, std::uint32_t network_id, std::set<int> wanted);
    PeerSession(std::string const& endpoint, PeerLocal const& local, std::set<int> wanted);
    ~PeerSession();
    Info const& info() const;
    void send(int type, google::protobuf::MessageLite const& message);
    void send_raw(int type, std::string const& payload);
    // The next wanted message; nullopt after `timeout`. Throws once the connection has failed.
    std::optional<Message> receive(std::chrono::milliseconds timeout);
    bool open() const;
    // Why the session ended (empty while open).
    std::string failure() const;
    // Frames (and their bytes) received from and sent to the peer since the handshake.
    struct Traffic { std::uint64_t messages_in = 0, bytes_in = 0, messages_out = 0, bytes_out = 0; };
    Traffic traffic() const;
    // Bytes waiting to be sent to the peer.
    std::size_t queued_bytes() const;
    // Ends the session (receive() then throws); `why` goes into failure().
    void close(std::string const& why = {});
    // The session as a responder sees a peer that asks (to charge it for what it sends).
    PeerRequester requester() const;
};

// Accepts inbound peers on HOST:PORT. Each connection's TLS and HTTP exchange runs on a thread
// of its own (at most 32 at once, 2 from one address other than loopback, an IPv6 /64 counted
// as one address (address_group), 15 s per step). An upgrade that passes xrpld's checks and
// `admit` becomes a PeerSession handed to `accepted`; one `admit` refuses gets a 503 naming
// `redirect()`'s peers. A plain GET is answered by `http` (status and JSON body; 404 when it
// returns nothing).
class PeerListener {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    struct Handlers {
        std::function<bool(PeerSession::Info const&, std::string const& address)> admit;
        std::function<void(std::unique_ptr<PeerSession>, std::string const& address)> accepted;
        std::function<std::vector<std::string>()> redirect;
        std::function<std::optional<std::pair<int, std::string>>(std::string const& target)> http;
        // A connection that did not become a session (TLS, its request, a refused upgrade), and why.
        std::function<void(std::string const& why, std::string const& address)> failed;
        // One `admit` let in that then failed before it was `accepted` (its response not written).
        std::function<void(PeerSession::Info const&, std::string const& address)> withdrawn;
    };
    PeerListener(std::string const& endpoint, PeerLocal local, std::set<int> wanted, Handlers handlers);
    ~PeerListener();
    std::uint16_t port() const;
};

// The public networks known by name, with what a node needs to join one: its public hubs (for
// a first connection) and its validator list and the list publisher's key, as xrpl.org gives them.
// Any other network names its peers and validator list in the configuration.
struct NetworkDefaults {
    std::uint32_t id;
    std::string name;
    std::vector<std::string> hubs;
    std::string list, list_key;
};
std::optional<NetworkDefaults> network_defaults(std::uint32_t network_id);
// The network's public hubs; none for a network not known by name.
std::vector<std::string> default_peers(std::uint32_t network_id = 0);
// Peers that hold ledger `sequence` (their complete_ledgers cover it) and publish their
// address, from the /crawl pages of the network's hubs, in random order. They are whoever
// runs them: what they send is checked against ledger hashes, never trusted.
std::vector<std::string> peers_holding(std::uint32_t sequence, std::uint32_t network_id = 0);

// Whether a peer's gossiped endpoint (TMEndpoints) may be dialled: as xrpld's PeerFinder, a
// literal public IP address and a port other than 0, never a name, loopback, private or
// link-local address (else a peer could have this node connect to its own network), nor a
// special-purpose one (documentation, benchmarking, broadcast; NAT64, IPv4-compatible, and 6to4
// carrying a private IPv4 address).
bool public_endpoint(std::string const& endpoint);
// An endpoint that is an IP address and port as one spelling: the address as boost prints it
// (an IPv4 address mapped into IPv6 as itself; IPv6 bracketed) and the port in decimal, so a
// peer cannot have one address remembered many times ("[2600::1]:51235", "[2600:0::1]:051235").
// Anything else (a name) is returned unchanged.
std::string canonical_endpoint(std::string const& endpoint);
// The address a per-address limit counts a remote IP as: an IPv6 address by its /64 (one host
// or site is usually given a whole /64, so exact addresses would let it take every place), an
// IPv4 one (or one mapped into IPv6) as itself. Text that is no address is returned unchanged.
std::string address_group(std::string const& address);
// A failure message without the endpoint it names, so one failure from many peers is one reason.
std::string without_endpoint(std::string text, std::string const& endpoint);
// Counts by reason: at most 32 reasons of up to 160 characters, the rest counted as "other".
void count_reason(std::map<std::string, std::uint64_t>& counts, std::string reason);
}
