#pragma once
#include <xrpla/core/verify.hpp>
#include <atomic>
#include <functional>
#include <optional>
#include <utility>

namespace xrpla::core {
struct RpcClient {
    std::string url;
    fs::path cache;
    unsigned delay_ms = 200;
    // Tries on transport errors, HTTP 429/5xx and tooBusy/slowDown/rate_limited answers (1: none again).
    unsigned attempts = 5;
    // Sent as X-Forwarded-For when set: the client a request is relayed for (xrpld accounts
    // resources by it when this machine is one of its secure_gateway addresses).
    std::string forwarded_for;
    // When set, the addresses the URL's host is reached at, in place of looking it up: libcurl's
    // CURLOPT_RESOLVE entry, "HOST:PORT:ADDRESS[,ADDRESS]..." (an IPv6 address in brackets).
    // A connection to any other address is refused. A forwarding server pins its upstream to the
    // addresses it checked (server.cpp). No request goes through a proxy.
    std::string resolve;
    Json call(std::string const& method, Json params = Json::object()) const;
    std::vector<Json> call_many(std::string const& method, std::vector<Json> const& params, unsigned jobs) const;
    // Whether this thread's last call reached the server: true when it answered (whatever it
    // answered: an error, HTTP 429 or 5xx, a body that is not JSON), false when it could not be
    // reached or did not answer in time (no connection, one that broke, TLS, a timeout). A
    // forwarding server reads it after rpc_request, which answers notReady to a failure of any
    // kind, to have an upstream not reached checked again soon (server.cpp).
    static bool last_reached();
};
// Asks `urls` in order (http(s):// or ws(s)://, the same JSON methods either way). The next is
// asked when this one does not hold the ledger (lgrNotFound or ledgerNotFound) or cannot be
// reached. entryNotFound is the answer (the object is gone) and is not asked of the next.
// `attempts` and `delay_ms` are RpcClient's. A ledger served by a later url is logged.
Json call_sources(std::vector<std::string> const& urls, std::string const& method, Json params, unsigned delay_ms, unsigned attempts, fs::path const& cache);
std::vector<Json> call_sources_many(std::vector<std::string> const& urls, std::string const& method, std::vector<Json> const& params, unsigned jobs,
                                    unsigned delay_ms, unsigned attempts, fs::path const& cache);
// Lets a libcurl handle (a CURL*, which curl.h declares as void) connect only to the addresses in
// `allowed`, written as boost::asio writes them (an IPv4 address written as IPv6 is the IPv4
// address it is), whatever its name cache holds. Read at each connection: `allowed` must outlive
// the handle's transfers. RpcClient's pinned requests use it, and a forwarding server's probes.
void pin_connections(void* curl, std::vector<std::string> const& allowed);
struct ExportOptions {
    std::string source;
    Range range;
    fs::path output, signing_key;
    unsigned delay_ms = 200;
    bool full_state = false;
    unsigned jobs = 1;
    bool append = false;
    std::string grpc_source;
    // With grpc_source beside a JSON-RPC source: ledgers are fetched on this many connections
    // to the gRPC node as well as on `jobs` to the JSON-RPC one (0: gRPC serves every ledger).
    unsigned grpc_jobs = 0;
    unsigned compression_level = 3;
    bool rpc_cache = true;
    // When set, the last ledger exported must have this hash (a quorum-validated ledger); the
    // parent chain then authenticates every ledger before it.
    std::string expected_last_hash;
    // Merge the chunks inside each aligned block of 16 and 256 ledgers once the block is
    // complete, so a follower that commits a few ledgers at a time keeps about one chunk per
    // 256 ledgers. Replaced files are kept for an hour for readers of the previous manifest.
    bool merge_chunks = false;
    // When set, the state after every ledger divisible by it (a publication interval's
    // boundary) is written to interval-snapshots/, for xrpla_snapshot to take instead of
    // replaying the interval.
    std::uint32_t interval_snapshots = 0;
    // The verifier keeps the trees of the newest N ledgers (a peer server reads them).
    std::size_t keep_ledgers = 0;
    // Where a ledger's transaction tree comes from when a gRPC source's list is not the ledger's:
    // xrpld decodes each transaction as it lists them, and at the first it cannot decode (some
    // 2013 transactions, or their metadata, on current xrpld) drops that one and all after it.
    // The peer protocol sends the tree's nodes raw; each is checked against the hash its parent
    // (from the header's transaction root) commits to, so any peer may serve them. {"default"}:
    // peers the network's hubs list (/crawl) as holding the ledger; or HOST:PORT endpoints; none
    // (empty, the default): the export stops at that ledger, saying so. `history_identity`: this
    // node's key toward them.
    std::vector<std::string> history_peers;
    fs::path history_identity;
    std::uint32_t network_id = 0;
    // After `source`, in order. A ledger the source does not hold (lgrNotFound), or a source
    // that cannot be reached, is asked of the next. http(s):// or ws(s)://. The first source
    // that answers is the one followed for the newest ledger: a local xrpld's admin port, which
    // is not rate-limited. Empty: the source alone.
    std::vector<std::string> fallbacks;
};
class GrpcLedgerClient {
    struct Impl;
    std::unique_ptr<Impl> impl_;
    mutable std::atomic<bool> last_unlimited_{false};
public:
    // Whether the source's last answer said it was exempt from its resource limits (xrpld's
    // is_unlimited: a secure_gateway address that sends a user).
    bool unlimited() const { return last_unlimited_; }
    explicit GrpcLedgerClient(std::string const& endpoint);
    ~GrpcLedgerClient();
    // A validated ledger: its header, transactions and object diff. When xrpld sends no
    // transactions for a nonempty transaction root (it could not decode one of them), the
    // document says "transactions_omitted" and its transactions are to be read elsewhere.
    std::pair<Header, Json> ledger(std::uint32_t sequence) const;
    // The newest ledger the node has validated (its header alone).
    Header validated() const;
    // A validated ledger's header alone: no transactions, and no diff, which xrpld cannot give
    // for the first ledger it holds (32,570 on mainnet), whose parent it lacks.
    Header header(std::uint32_t sequence) const;
    // One page of a ledger's state (GetLedgerData): the objects after `marker` (and before
    // `end_marker` when set), and the marker to continue from (empty when done).
    std::pair<std::vector<ObjectRecord>, Bytes> state_page(std::uint32_t sequence, Bytes const& marker, Bytes const& end_marker = {}) const;
};
// How fast a gRPC source serves ledgers (with transactions and object diffs) and state pages,
// over one connection: `count` ledgers from `first`, then `pages` state pages of `first`.
Json probe_grpc(std::string const& source, std::uint32_t first, std::uint32_t count, unsigned pages);
Json export_archive(ExportOptions const&);
// For an archive exported with each ledger's true diff (from a gRPC or Clio source): the
// ledgers with changes that neither their transactions' metadata nor closing_keys name, i.e.
// what a follower deriving changes from metadata (peers, xrpld's JSON-RPC) would miss.
Json check_keys(fs::path const& archive, fs::path const& publisher_key);
struct PublishOptions {
    ExportOptions export_options;
    std::uint32_t start = 0, until = 0;
    unsigned interval_seconds = 3, batch_ledgers = 64;
    bool once = false;
    // Trust validators rather than the source's "validated" flag: a validator list (URL or file)
    // and its publisher key, and a node whose validations stream to follow (ws:// or wss://).
    // Each batch then ends at a ledger that a quorum of listed validators signed.
    std::string validator_list, validator_list_key, validations_node;
    // Read ledgers from the peer network instead of the source node (extending an existing
    // archive): these peers ("default": mainnet's public hubs), trusting validations the peers
    // relay from the validator list (vl.ripple.com unless given).
    std::vector<std::string> peers;
    fs::path peer_identity;
    std::uint32_t network_id = 0;
    unsigned peer_connections = 10;
    // With `peers` (docs/PEER-SERVING.md): relay validators' validations and proposals as
    // xrpld does (policies "all", "trusted" or "drop_untrusted" for unlisted validators),
    // squelching sources beyond `squelch_sources` per validator; `peer_private`: connect only
    // to `peers` and keep this node's address out of the network's view.
    bool relay = true;
    std::string relay_validations = "all", relay_proposals = "trusted";
    bool squelch = true;
    unsigned squelch_sources = 5;
    bool peer_private = false;
    // With `peers`: serve ledger data to the peer network — the newest `serve_ledgers`
    // ledgers in full — accepting peers on `serve_listen` when it is set.
    bool serve = false;
    std::size_t serve_ledgers = 256;
    std::string serve_listen;
    unsigned serve_max_inbound = 11;
    unsigned serve_ip_limit = 2;
    bool serve_crawl = false;
    // Node keys (base58) always admitted and never charged.
    std::vector<std::string> serve_reserved;
};
Json publish_archive(PublishOptions const&);
// The outbound and inbound peer limits: `max_peers` (0: no cap) caps both together, as xrpld's
// [peers_max]. Outbound peers come first (capped at `max_peers`); inbound slots are what
// `max_peers` leaves, or `max_inbound` if smaller (11 with neither, xrpld's default).
std::pair<unsigned, unsigned> peer_limits(unsigned max_peers, unsigned outbound, std::optional<unsigned> max_inbound);
Json create_signing_key(fs::path const&);
// The Ed25519 public key (hex) of a signing key file.
std::string public_key(fs::path const& signing_key);
// `client` is the address the request came from, passed on with a forwarded request; `resolve`,
// when set, the addresses `forward` is reached at (RpcClient::resolve).
Json rpc_request(fs::path const&, std::string const& method, Json const& params, std::string const& forward = {}, std::string const& client = {},
                 std::string const& resolve = {});
// Whether a request is one the server forwards upstream (transactions, fees, paths, the open
// ledger: what only a node following consensus knows).
bool forwarded_request(std::string const& method, Json const& params);
struct ServeOptions {
    fs::path dir;
    std::string http = "127.0.0.1:5005", ws = "127.0.0.1:6006", forward;
    // TLS listeners (each answers HTTPS and WSS, as each plain one answers HTTP and WebSocket),
    // with a certificate chain and its private key (PEM files). Both are read at start and again
    // when either changes, so a renewed certificate is used without a restart.
    std::string https, wss;
    fs::path tls_cert, tls_key;
    // The upstream's WebSocket (ws:// or wss://), for path_find: xrpld streams a path search
    // over WebSocket only. Empty: path_find answers notSupported.
    std::string forward_ws;
    unsigned threads = 4;
    // Piece caches: memory for decoded pages, open segments and snapshot blocks (0: an eighth of
    // the machine's memory, 3-32 GB; an open segment's dictionary is about 45 MB and loading one
    // is most of a cold read), and how many of the newest segments to open in the background at
    // start (off: tip reads still touch segments across the whole interval, so warming the
    // newest few measured no gain).
    std::size_t cache_mb = 0;
    unsigned warm_segments = 0;
    // A live tail: a follower's spool (and its public key) holding the ledgers after the piece
    // cache in `dir`, indexed as the spool grows.
    fs::path tail, tail_key;
    // The XRPL network served (0: mainnet), for ctid and network_id in responses and streams.
    std::uint32_t network_id = 0;
    // Open HTTP and WebSocket connections at once; more are closed on arrival. Of them, at most
    // max_connections_per_ip from one address (0: no limit); this machine's own addresses (a
    // local proxy or benchmark) are not limited.
    unsigned max_connections = 1024, max_connections_per_ip = 64;
    // Whether clients on this machine's own addresses (loopback) are its own tools: they may ask
    // the server's internal methods (those starting with `_`; others are answered unknownCmd)
    // and pass max_connections_per_ip. Behind a reverse proxy on this machine every client
    // arrives from loopback: false there refuses those methods to all and limits loopback too.
    bool loopback_trusted = true;
};
// The network ID for responses in this process (serve sets it from ServeOptions).
void configure_network(std::uint32_t network_id);
// A network given by name ("mainnet" or "main", "testnet", "devnet") or by its ID. Mainnet,
// testnet and devnet also bring their public hubs and validator lists as defaults.
std::uint32_t parse_network(std::string const& text);
// The validator list a network known by name (mainnet, testnet, devnet) uses, and its
// publisher's key; empty for any other network.
std::pair<std::string, std::string> network_validator_list(std::uint32_t network_id);
// For one-shot `xrpla rpc` requests over a piece cache plus a spool tail.
void use_tail(fs::path const& dir, fs::path const& tail, fs::path const& tail_key);
void serve(ServeOptions const&);
// A --dir holding a piece cache (piece_client.py sync) is served in place: build its
// query index once with index_pieces, then serve and rpc answer from the compressed data.
// The optional indexes answer Clio's mpt_holders and nfts_by_issuer; one turned off is not
// built, and its files are removed (the method then answers notSupported).
struct IndexOptions { bool mpt_holders = true, nfts_by_issuer = true; };
Json index_pieces(fs::path const& cache, unsigned jobs, IndexOptions const& = {});
// The same for a selection not yet written to selection.json (a sync indexes before switching).
Json index_pieces(fs::path const& cache, unsigned jobs, Json const& selection, IndexOptions const& = {});
// Keeps a piece cache in step with a publication: downloads what the chosen ledgers need
// (each object checked against the signed catalog), indexes it, then switches selection.json
// and removes files no longer selected. Serving processes pick up the new selection.
// Besides the base snapshot, the cache keeps the newest interval snapshot the ranges pass and,
// with snapshots_every N (ledgers; 0: none), every published one whose sequence is a multiple
// of N: a ledger's state is read from the newest kept snapshot at or before it.
// `source` is the publication's mirrors, tried in order (Source); `publisher_key` its key's file,
// or the key as 64 hexadecimal characters.
struct SyncOptions {
    Mirrors source;
    fs::path publisher_key, cache;
    std::optional<std::uint32_t> first, last;
    unsigned jobs = 4;
    IndexOptions indexes;
    std::uint32_t snapshots_every = 0;
};
Json sync_pieces(SyncOptions const&);
Json prune_store(fs::path const&, std::uint32_t);
Json compact_store(fs::path const&, bool native);
// backend applies to rippled imports: "nudb" (default, what most operators run) or "rocksdb".
struct ImportOptions { fs::path dir, output; std::string range; std::string backend = "nudb"; };
Json import_rippled(ImportOptions const&);
// jobs: threads preparing the plan (0: one a core, at most 32); the plan is the same for any.
// load_jobs: the loader's processes, each with its own connection (0: the loader's default).
struct ClioOptions { ImportOptions input; std::string host, keyspace = "xrpla", username; unsigned port = 9042; bool tls = false; unsigned jobs = 0, load_jobs = 0; };
Json import_clio(ClioOptions const&);
// Restores a piece publication (its bucket directory or HTTPS base URL, or its mirrors, checked
// against a pinned publisher key) into another store, streaming: nothing is staged but the target.
struct RestoreOptions {
    Mirrors source;
    fs::path publisher_key;
    std::string target;  // nudb or rocksdb (an xrpld node database), store (xrpla's RocksDB store), clio
    fs::path output;
    std::optional<std::uint32_t> first, last;  // default: from the earliest snapshot to the tip
    std::string clio_host, clio_keyspace = "xrpla", clio_username;
    unsigned clio_port = 9042;
    bool clio_tls = false;
    fs::path signing_key;  // spool: the key the new spool is signed with
    // spool: replay every ledger after the snapshot through the verifier (each state and
    // transaction root checked) instead of starting from the state at the tip (see restore_spool).
    bool replay = false;
    // nudb: publication intervals restored at once (0: about a third of the cores; 1: in order).
    // clio: threads preparing the import plan (ClioOptions::jobs).
    unsigned jobs = 0;
    // clio: the loader's processes (ClioOptions::load_jobs).
    unsigned load_jobs = 0;
};
Json restore(RestoreOptions const&);
// target "spool": a signed v2 archive for `xrpla publish` to extend, its verifier's nodes kept.
// It starts from the state at the publication's tip: the newest snapshot's objects updated by
// the newest value of every object the ledgers since changed (one decode pass, each header's
// parent hash checked), bulk-loaded and checked against the account hash of the ledger before
// the tip, which is then applied in full. That state is the one a replay of every ledger
// reaches (`replay`, or a given first ledger, does that instead and keeps the ledgers: hours
// on a busy interval). The newest values are held in memory, up to 1 GiB of them: past that
// (a long interval, restored late in it) the ledgers are replayed instead. Given only a last
// ledger, the state at that ledger. An existing spool is extended instead (extend_spool).
Json restore_spool(RestoreOptions const&);
// The ledgers the publication (with its tail) holds past an existing spool's tip, appended to it
// the same way: a node starting behind the network catches up from the publication rather than
// from peers, which keep only recent ledgers. Only the segments after the tip are fetched.
Json extend_spool(RestoreOptions const&);
// When the publication has a newer snapshot than the spool's, rebuilds the spool from it (its
// ledgers after the publication's are verified again) and swaps it in, keeping the old one as
// SPOOL.retired-TIME. The new spool starts at the publication's tip, or at `first` if earlier
// (a server's piece cache's last ledger + 1, so the tail never needs the retired spool for
// the ledgers in between); a rotation is due only once that is past the newest snapshot. A
// writer may keep extending the spool until `pause` is called (it must stop the writer), and
// the spool's lock is then held through the swap; without `pause` it is held throughout, so
// a rotation is refused while a follower writes to the spool. The two renames are each synced
// before the next. Without `apply`, only reports whether a rotation is due.
Json rotate_spool(fs::path const& spool, fs::path const& signing_key, Mirrors const& source, fs::path const& publisher_key,
                  bool apply = true, std::function<void()> const& pause = {}, std::optional<std::uint32_t> first = std::nullopt);
// A rotation a crash stopped between its two renames (no spool, or one without a manifest,
// and a complete SPOOL.next signed by the spool's key): SPOOL.next is put in place. Run before
// anything writes to the spool; rotate_spool runs it first.
Json recover_spool(fs::path const& spool, fs::path const& signing_key);
Json restore_rippled(RestoreOptions const&);
// Whether two xrpld node databases hold the same nodes (for tests and checks of restores).
Json compare_node_databases(fs::path const& a, fs::path const& b, std::string const& backend);
// Verifies a validator list (a list site URL or file) against the list publisher's key; with a
// node (ws:// or wss://), also follows its validations and reports each ledger that reaches a
// quorum of trusted validators, until `ledgers` have.
struct ValidatorsOptions { std::string list, list_key, node; unsigned ledgers = 5; };
Json watch_validators(ValidatorsOptions const&);
// Joins the XRPL peer network (default: mainnet's public hubs) with a node identity (a key file,
// or a new key each run), counts peers' validations from a verified validator list, and fetches
// each quorum-validated ledger's header from a peer, checking it hashes to the validated hash.
struct PeersOptions {
    std::vector<std::string> peers;
    std::string list, list_key;  // the network's validator list and its publisher's key unless set
    fs::path identity;
    std::uint32_t network_id = 0;
    unsigned connections = 3, ledgers = 5, seconds = 120;
};
Json watch_peers(PeersOptions const&);
// A ledger read by hash from peers (default: the network's hubs): its header and every
// transaction as the transaction tree holds it, with whether libxrpl can still parse it.
Json peer_ledger(std::vector<std::string> const& peers, std::string const& hash, std::uint32_t network_id, fs::path const& identity = {});
// The same peer-protocol requests sent to a reference xrpld peer and to an XRPLA peer serving
// ledgers, for recent ledgers both hold: headers by hash and by sequence, transaction and state
// tree nodes at several query depths, several nodes at once, nodes by hash (node-store
// format), and a ledger neither holds (xrpld sends no answer). Each answer is compared whole
// with the reference's (node order reported apart).
struct PeerCompareOptions {
    std::string reference, subject;  // HOST:PORT
    std::uint32_t network_id = 0;
    unsigned ledgers = 8, spacing = 7, timeout_ms = 10000, seed = 1;
    fs::path identity;
};
Json peer_compare(PeerCompareOptions const&);
struct TailOptions { FetchOptions fetch; unsigned interval_seconds = 15; std::uint32_t state_window = 0; bool once = false; };
Json tail(TailOptions const&);
}
