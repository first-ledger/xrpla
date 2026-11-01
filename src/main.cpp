#include <xrpla/archive.hpp>
#include "publications.hpp"
#include <toml++/toml.h>
#ifdef XRPLA_WITH_LIBXRPL
#include <xrpla/core/verify.hpp>
#include <xrpla/core/operations.hpp>
#include "setup.hpp"
#include <xrpl/protocol/BuildInfo.h>
#endif
#include <cstdlib>
#ifndef _WIN32
#include <sys/resource.h>
#endif
#include <charconv>
#include <iostream>
#include <map>
#include <set>
#include <thread>

namespace {
constexpr char help[] = "xrpla " XRPLA_VERSION R"( — authenticated and verified XRPL archives

  xrpla fetch --archive URL|PATH --range A-B|latest-N|Nd --publisher-key FILE
              [--dir ./data] [--jobs 8] [--config FILE]
  xrpla verify --dir ./data --publisher-key FILE [--full] [--trust-ledger-hash HASH]
  xrpla build --dir ./data --publisher-key FILE [--trust-ledger-hash HASH]
  xrpla query --dir ./data --kind ledger|tx|object|account_tx --key KEY [--ledger N]
  xrpla status [--dir ./data] [--config FILE]
  xrpla keygen --out publisher.seed
  xrpla export --source RPC_URL|grpc://HOST:PORT --range A-B --out ARCHIVE --signing-key FILE
               [--fallback URL,URL] [--delay-ms 200] [--jobs 1] [--grpc-source grpc[s]://HOST:PORT [--grpc-jobs N]] [--full-state]
               [--compression-level 3] [--no-rpc-cache] [--history-peers default|none|HOST:PORT,... [--history-identity FILE]]
  xrpla publish --source RPC_URL|grpc://HOST:PORT --out ARCHIVE --signing-key FILE
                [--fallback URL,URL] [--start N] [--until N] [--batch-ledgers 64] [--interval 3]
                [--delay-ms 200] [--jobs 4] [--grpc-source grpc[s]://HOST:PORT [--grpc-jobs N]] [--full-state] [--once] [--no-rpc-cache]
                [--compression-level 3] [--merge-chunks] [--interval-snapshots N]
                [--history-peers default|none|HOST:PORT,... [--history-identity FILE]]
                  (raw transaction trees of ledgers whose transactions a gRPC source omits because
                   current xrpld cannot decode one, a few of 2013; off unless given: `default` asks
                   peers the network's crawl lists as holding the ledger)
                [--validator-list https://vl.ripple.com --validator-list-key HEX --validations-node ws[s]://HOST:PORT]
                [--peers default|HOST:PORT,... [--peer-identity FILE] [--network-id mainnet] [--peer-connections 10] [--peer-max N]
                         [--peer-private] [--peer-no-relay] [--peer-relay-validations all|trusted|drop_untrusted]
                         [--peer-relay-proposals trusted|all|drop_untrusted] [--peer-no-squelch] [--peer-squelch-sources 5]]
                  (extends ARCHIVE from peers, relaying validations and proposals as xrpld does)
                [--peer-serve [--peer-ledgers 256] [--peer-listen HOST:PORT] [--peer-max-inbound 11] [--peer-ip-limit 2]
                              [--peer-crawl] [--peer-reserved NODE_KEY,...]]
                  (with --peers: serve the newest ledgers to the peer network; docs/PEER-SERVING.md)
  xrpla serve --dir ./data [--http 127.0.0.1:5005] [--ws 127.0.0.1:6006]
              [--https IP:PORT] [--wss IP:PORT] [--tls-cert CHAIN.pem --tls-key KEY.pem]  (reloaded when renewed)
              [--forward RPC_URL] [--forward-ws WS_URL] [--threads 4] [--config FILE]
              [--cache-mb N (default: memory / 4, 3072-65536)] [--tail SPOOL|FOLLOWER_DIR --tail-key FILE] [--network-id mainnet] [--max-connections 1024] [--max-connections-per-ip 64]
              [--no-loopback-trust]  (as [serve] loopback_trusted = false)
  xrpla rpc --dir ./data --request FILE|- [--forward RPC_URL] [--tail SPOOL|FOLLOWER_DIR --tail-key FILE] [--network-id mainnet]
  xrpla index-pieces --dir PIECE_CACHE [--jobs N] [--indexes all|none|mpt_holders,nfts_by_issuer]
  xrpla import rippled --dir ./data --out EMPTY_DIRECTORY [--range A-B] [--backend nudb|rocksdb]
  xrpla import clio --dir ./data --out PLAN_DIRECTORY [--range A-B] [--jobs N]
                    [--host HOST --port 9042 --keyspace xrpla --tls --username USER] [--load-jobs N]
  xrpla validators --list https://vl.ripple.com|FILE --list-key HEX [--node ws[s]://HOST:PORT] [--ledgers 5]
  xrpla peers [--peers HOST:PORT,...] [--list URL|FILE --list-key HEX] [--identity FILE]
              [--network-id mainnet] [--connections 3] [--ledgers 5] [--seconds 120]
  xrpla peer-ledger --hash LEDGER_HASH [--peers HOST:PORT,...] [--identity FILE] [--network-id mainnet]
  xrpla peer-compare --reference XRPLD_HOST:PORT --subject XRPLA_HOST:PORT [--ledgers 8] [--spacing 7] [--timeout-ms 10000]
              (the same peer requests to both; answers compared with the reference's)
              (a ledger's header and raw transactions from peers, and whether libxrpl parses each)
  xrpla setup [--config /etc/xrpla/xrpla.toml] [--unit-dir /etc/systemd/system]
  xrpla run --config /etc/xrpla/xrpla.toml
  xrpla publications  (the approved publications installed with xrpla: name, network, mirrors)
  xrpla sync --publication NAME | --source BUCKET_DIR|HTTPS_URL --publisher-key FILE
             --dir PIECE_CACHE [--range A-B|A-] [--jobs 4] [--indexes all|none|mpt_holders,nfts_by_issuer] [--snapshots-every N]
  xrpla restore --publication NAME | --source BUCKET_DIR|HTTPS_URL --publisher-key FILE
                --to nudb|rocksdb|store|clio|spool
                --out DIR [--range A-B|A-] [--host HOST --port 9042 --keyspace xrpla --tls --username USER]
                [--signing-key FILE] [--replay] [--jobs N] [--load-jobs N]  (spool: the state at the tip, for publish --peers;
                --replay or a range start: the snapshot and every later ledger, replayed;
                --jobs: nudb, restore each snapshot interval on its own thread; default cores/3;
                clio, threads writing the import plan, default one a core (the plan is the same for any))
  xrpla compare-nodes --dir NODE_DB --against NODE_DB [--backend nudb|rocksdb]
  xrpla check-keys --archive DIR --publisher-key FILE  (state changes no transaction in their ledger explains)
  xrpla grpc-probe --source grpc://HOST:PORT --ledger N [--count 200] [--pages 20]  (whether xrpld's client limits apply)
  xrpla rotate-spool --dir SPOOL --signing-key FILE --publication NAME | --source BUCKET_DIR|HTTPS_URL --publisher-key FILE [--check]
                     [--start N]  (the new spool starts at N if before the publication's tip: a piece cache's last + 1)
  xrpla tail --archive URL|PATH --range A-B|latest-N|Nd --publisher-key FILE
             [--dir ./data] [--interval 15] [--state-window N] [--once]
  xrpla prune --dir ./data --state-first N
  xrpla compact --dir ./data [--native]
  xrpla --version

Publisher key: 32 raw bytes or 64 hex characters, pinned independently.
--publication NAME: a publication from the approved list (`xrpla publications`), with its key and
mirrors; --source then replaces its mirrors, and --publisher-key, if given, must be its key.
fetch/verify authenticate the publisher and check object sizes and SHA-256.
--full and build require archive v2 and the libxrpl-enabled build.
They verify canonical ledger roots and parent chains; build writes RocksDB.
Day ranges require signed close-time indexes and are relative to the archive tip.
Clio without --host writes a reviewable import plan. Password: XRPLA_CQL_PASSWORD.
--load-jobs: the Clio loader's processes, each with its own connection (default 8, at most the cores).
--tail serves a follower's spool (or the one a live publisher's `current` names) after a piece cache.
--indexes picks the optional indexes a piece cache keeps (default all): mpt_holders and
nfts_by_issuer answer Clio's methods of those names; one left out is not built, or removed.
--snapshots-every N: sync also keeps each published state snapshot at a multiple of N ledgers
(0, the default: only the base and the newest), so older ledgers' state is read faster.
--network-id: mainnet (the default), testnet, devnet, or a network ID. The three named networks
bring their public hubs (--peers default, peers) and validator list (peers, publish --peers).
)";

// A flag's whole value as an integer from `low` to `high` ("-1" or "8GB" is refused, not wrapped
// or cut short), named in the error.
std::uint64_t number_value(std::string const& flag, std::string const& value, std::uint64_t low, std::uint64_t high) {
    std::uint64_t number = 0;
    auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), number);
    if (ec != std::errc{} || end != value.data() + value.size() || value.empty() || number < low || number > high)
        throw xrpla::Error(flag + " must be an integer between " + std::to_string(low) + " and " + std::to_string(high));
    return number;
}
unsigned jobs_value(std::string const& value, std::string const& flag = "--jobs") { return static_cast<unsigned>(number_value(flag, value, 1, 32)); }
#ifdef XRPLA_WITH_LIBXRPL
// --indexes: all, none, or a comma-separated list of the optional piece cache indexes.
xrpla::core::IndexOptions index_options(std::string const& value) {
    if (value == "all") return {true, true};
    xrpla::core::IndexOptions options{false, false};
    if (value == "none") return options;
    std::size_t at = 0;
    while (at <= value.size()) {
        auto comma = value.find(',', at);
        auto name = value.substr(at, comma == std::string::npos ? std::string::npos : comma - at);
        if (name == "mpt_holders") options.mpt_holders = true;
        else if (name == "nfts_by_issuer") options.nfts_by_issuer = true;
        else throw xrpla::Error("--indexes takes all, none, or a list of mpt_holders and nfts_by_issuer, not '" + name + "'");
        if (comma == std::string::npos) break;
        at = comma + 1;
    }
    return options;
}
#endif
int run(int argc, char** argv) {
    xrpla::initialize();
    if (argc == 1 || (argc == 2 && std::string(argv[1]) == "--help")) { std::cout << help; return 0; }
    std::string command = argv[1];
    if (command == "--version" && argc == 2) {
        std::cout << "xrpla " << XRPLA_VERSION;
#ifdef XRPLA_WITH_LIBXRPL
        std::cout << " (libxrpl " << xrpl::build_info::getVersionString() << ")\n";
#else
        std::cout << " (transport-only development build)\n";
#endif
        return 0;
    }
    if (command == "publications") {
        if (argc > 2) throw xrpla::Error("Unknown option: " + std::string(argv[2]));
        std::cout << xrpla::installed_publications().describe().dump(2) << '\n';
        return 0;
    }
#ifdef XRPLA_WITH_LIBXRPL
    // Profiles use the sectioned config written by `setup`, not the flat --config keys below.
    if (command == "setup" || command == "run") {
        std::map<std::string, std::string> profile_args;
        for (int i = 2; i < argc; ++i) {
            std::string name = argv[i];
            if ((name != "--config" && !(command == "setup" && name == "--unit-dir")) || i + 1 >= argc) throw xrpla::Error("Unknown option: " + name);
            profile_args[name] = argv[++i];
        }
        xrpla::fs::path config = profile_args.contains("--config") ? profile_args.at("--config") : "/etc/xrpla/xrpla.toml";
        // Requests a stop left running hold no locks the exit needs: leave without static destructors.
        if (command == "run") { auto code = xrpla::run_config(config); std::cout.flush(); std::cerr.flush(); std::_Exit(code); }
        return xrpla::setup(config, profile_args.contains("--unit-dir") ? profile_args.at("--unit-dir") : "/etc/systemd/system", std::cin, std::cout);
    }
#endif
    if (!std::set<std::string>{"fetch", "verify", "status", "build", "query", "export", "publish", "keygen", "serve", "rpc", "prune", "compact", "import", "tail", "index-pieces", "restore", "sync", "validators", "peers", "rotate-spool", "compare-nodes", "grpc-probe", "check-keys", "peer-ledger", "peer-compare"}.contains(command))
        throw xrpla::Error("Command not implemented: " + command + "; see --help");
    std::set<std::string> allowed{"--dir", "--config"};
    if (command == "fetch" || command == "verify" || command == "build" || command == "tail") allowed.insert("--publisher-key");
    if (command == "fetch" || command == "tail") for (auto const* name : {"--archive", "--range", "--jobs"}) allowed.insert(name);
    if (command == "index-pieces") for (auto name : {"--jobs", "--indexes"}) allowed.insert(name);
    if (command == "tail") for (auto name : {"--interval", "--state-window", "--once"}) allowed.insert(name);
    if (command == "verify") allowed.insert("--full");
    if (command == "verify" || command == "build") allowed.insert("--trust-ledger-hash");
    if (command == "query") for (auto const* name : {"--kind", "--key", "--ledger"}) allowed.insert(name);
    if (command == "export") for (auto const* name : {"--source", "--range", "--out", "--signing-key", "--delay-ms", "--full-state"}) allowed.insert(name);
    if (command == "export" || command == "publish") for (auto name : {"--jobs", "--grpc-source", "--grpc-jobs", "--compression-level", "--no-rpc-cache", "--interval-snapshots", "--history-peers", "--history-identity", "--fallback"}) allowed.insert(name);
    if (command == "publish") for (auto name : {"--source", "--out", "--signing-key", "--delay-ms", "--full-state", "--start", "--until", "--batch-ledgers", "--interval", "--once",
                                                 "--validator-list", "--validator-list-key", "--validations-node", "--merge-chunks",
                                                 "--peers", "--peer-identity", "--network-id", "--peer-connections", "--peer-serve", "--peer-ledgers",
                                                 "--peer-listen", "--peer-max-inbound", "--peer-crawl", "--peer-reserved", "--peer-max", "--peer-private",
                                                 "--peer-no-relay", "--peer-relay-validations", "--peer-relay-proposals", "--peer-no-squelch",
                                                 "--peer-squelch-sources", "--peer-ip-limit"}) allowed.insert(name);
    if (command == "keygen") allowed.insert("--out");
    if (command == "serve") for (auto name : {"--http", "--ws", "--https", "--wss", "--tls-cert", "--tls-key", "--forward", "--forward-ws", "--threads", "--cache-mb", "--warm-segments", "--tail", "--tail-key", "--network-id", "--max-connections", "--max-connections-per-ip", "--no-loopback-trust"}) allowed.insert(name);
    if (command == "rpc") for (auto name : {"--request", "--forward", "--tail", "--tail-key", "--network-id"}) allowed.insert(name);
    if (command == "prune") allowed.insert("--state-first");
    if (command == "compact") allowed.insert("--native");
    if (command == "import") for (auto name : {"--out", "--range", "--host", "--port", "--keyspace", "--username", "--tls", "--backend", "--jobs", "--load-jobs"}) allowed.insert(name);
    if (command == "restore") for (auto name : {"--publication", "--source", "--publisher-key", "--to", "--out", "--range", "--host", "--port", "--keyspace", "--username", "--tls", "--signing-key", "--jobs", "--load-jobs", "--replay"}) allowed.insert(name);
    if (command == "sync") for (auto name : {"--publication", "--source", "--publisher-key", "--range", "--jobs", "--indexes", "--snapshots-every"}) allowed.insert(name);
    if (command == "compare-nodes") for (auto name : {"--against", "--backend"}) allowed.insert(name);
    if (command == "grpc-probe") for (auto name : {"--source", "--ledger", "--count", "--pages"}) allowed.insert(name);
    if (command == "check-keys") for (auto name : {"--archive", "--publisher-key"}) allowed.insert(name);
    if (command == "rotate-spool") for (auto name : {"--publication", "--source", "--publisher-key", "--signing-key", "--check", "--start"}) allowed.insert(name);
    if (command == "validators") for (auto name : {"--list", "--list-key", "--node", "--ledgers"}) allowed.insert(name);
    if (command == "peers") for (auto name : {"--peers", "--list", "--list-key", "--identity", "--network-id", "--connections", "--ledgers", "--seconds"}) allowed.insert(name);
    if (command == "peer-ledger") for (auto name : {"--peers", "--hash", "--network-id", "--identity"}) allowed.insert(name);
    if (command == "peer-compare") for (auto name : {"--reference", "--subject", "--ledgers", "--spacing", "--timeout-ms", "--seed", "--network-id", "--identity"}) allowed.insert(name);
    std::map<std::string, std::string> args;
    std::string import_target;
    if (command == "import") { if (argc < 3) throw xrpla::Error("import requires rippled or clio"); import_target = argv[2]; }
    for (int i = command == "import" ? 3 : 2; i < argc; ++i) {
        std::string name = argv[i];
        if (!allowed.contains(name)) throw xrpla::Error("Unknown option: " + name);
        if (name == "--full" || name == "--full-state" || name == "--once" || name == "--tls" || name == "--replay" || name == "--native" || name == "--no-rpc-cache" || name == "--merge-chunks" || name == "--check" || name == "--peer-serve" || name == "--peer-crawl" ||
            name == "--peer-private" || name == "--peer-no-relay" || name == "--peer-no-squelch" || name == "--no-loopback-trust") {
            if (!args.emplace(name, "true").second) throw xrpla::Error("Duplicate option: " + name);
            continue;
        }
        if (i + 1 >= argc || std::string(argv[i + 1]).starts_with("--"))
            throw xrpla::Error("Missing value for " + name);
        if (!args.emplace(name, argv[++i]).second) throw xrpla::Error("Duplicate option: " + name);
    }
    xrpla::FetchOptions options;
    options.dir = "./data";
    // Whether --jobs or the config's `jobs` set options.jobs: commands with a default of their
    // own (sync 4, index-pieces every core, publish 4) take it only then.
    bool jobs_set = false;
    if (args.contains("--config")) {
        auto table = toml::parse_file(args.at("--config"));
        std::set<std::string> fields{"archive", "range", "dir", "publisher_key", "jobs", "http", "ws", "forward", "forward_ws", "threads", "interval", "state_window"};
        for (auto const& [key, value] : table) {
            auto name = std::string(key.str());
            if (!fields.contains(name)) throw xrpla::Error("Unknown config key: " + name);
            if (name == "jobs" || name == "threads" || name == "interval" || name == "state_window") {
                auto n = value.value<std::int64_t>();
                if (!n || !value.is_integer() || *n < 0) throw xrpla::Error("Config " + name + " must be a nonnegative integer");
                if (name == "jobs") { options.jobs = jobs_value(std::to_string(*n)); jobs_set = true; }
                else { auto flag = name; for (char& c : flag) if (c == '_') c = '-'; args.try_emplace("--" + flag, std::to_string(*n)); }
            } else {
                auto text = value.value<std::string>();
                if (!text) throw xrpla::Error("Config " + name + " must be a string");
                if (name == "archive") options.archive = *text;
                if (name == "range") options.range = *text;
                if (name == "dir") options.dir = *text;
                if (name == "publisher_key") options.publisher_key = *text;
                if (name == "http" || name == "ws" || name == "forward") args.try_emplace("--" + name, *text);
                if (name == "forward_ws") args.try_emplace("--forward-ws", *text);
            }
        }
    }
    if (args.contains("--archive")) options.archive = args.at("--archive");
    if (args.contains("--range")) options.range = args.at("--range");
    if (args.contains("--dir")) options.dir = args.at("--dir");
    if (args.contains("--publisher-key")) options.publisher_key = args.at("--publisher-key");
    if (args.contains("--jobs")) { options.jobs = jobs_value(args.at("--jobs")); jobs_set = true; }
    if (options.dir.empty()) throw xrpla::Error("--dir cannot be empty");
    if (args.contains("--tail-key") && !args.contains("--tail")) throw xrpla::Error("--tail-key applies with --tail");
    if ((command == "fetch" || command == "verify" || command == "build" || command == "tail") && options.publisher_key.empty()) throw xrpla::Error("--publisher-key is required; automatic trust is not supported");
    // sync, restore and rotate-spool: a publication from the approved list (--publication), or one
    // given by its --source and --publisher-key.
    auto publication = [&] {
        if (!args.contains("--publication") && !args.contains("--source")) throw xrpla::Error(command + " requires --publication NAME, or --source and --publisher-key");
        if (!args.contains("--publication") && options.publisher_key.empty()) throw xrpla::Error("--publisher-key is required; automatic trust is not supported");
        xrpla::Mirrors source;
        if (args.contains("--source")) source.push_back(args.at("--source"));
        return xrpla::choose_publication(args.contains("--publication") ? args.at("--publication") : "", source, options.publisher_key);
    };
    xrpla::Json result;
    if (command == "fetch") {
        if (options.archive.empty() || options.range.empty()) throw xrpla::Error("fetch requires --archive and --range");
        result = xrpla::fetch(options);
    } else if (command == "verify" && !args.contains("--full") && !args.contains("--trust-ledger-hash"))
        result = xrpla::verify(options.dir, options.publisher_key);
    else if (command == "status" && !xrpla::fs::exists(options.dir / "store")) result = xrpla::status(options.dir);
    else {
#ifdef XRPLA_WITH_LIBXRPL
        xrpla::core::VerifyOptions full{options.dir, options.publisher_key, args.contains("--trust-ledger-hash") ? args.at("--trust-ledger-hash") : ""};
        if (command == "verify") result = xrpla::core::verify_full(full);
        else if (command == "build") result = xrpla::core::build_store(full);
        else if (command == "status") result = xrpla::core::store_status(options.dir);
        else if (command == "compact") result = xrpla::core::compact_store(options.dir, args.contains("--native"));
        else if (command == "index-pieces") result = xrpla::core::index_pieces(options.dir, jobs_set ? options.jobs : std::max(1u, std::thread::hardware_concurrency()),
                                                                             index_options(args.contains("--indexes") ? args.at("--indexes") : "all"));
        else if (command == "tail") {
            xrpla::core::TailOptions tail{options};
            auto number = [&](char const* key, unsigned fallback, std::uint64_t low) { return args.contains(key) ? static_cast<unsigned>(number_value(key, args.at(key), low, UINT32_MAX)) : fallback; };
            tail.interval_seconds = number("--interval", 15, 1); tail.state_window = number("--state-window", 0, 0); tail.once = args.contains("--once");
            result = xrpla::core::tail(tail);
        }
        else if (command == "import") {
            if (!args.contains("--out")) throw xrpla::Error("import requires --out");
            xrpla::core::ImportOptions input{options.dir, args.at("--out"), options.range};
            if (args.contains("--backend")) {
                if (import_target != "rippled") throw xrpla::Error("--backend applies to rippled imports");
                input.backend = args.at("--backend");
            }
            if (import_target != "clio" && (args.contains("--jobs") || args.contains("--load-jobs"))) throw xrpla::Error("--jobs and --load-jobs apply to clio imports");
            if (import_target == "rippled") result = xrpla::core::import_rippled(input);
            else if (import_target == "clio") {
                xrpla::core::ClioOptions clio{input};
                if (jobs_set) clio.jobs = options.jobs;
                if (args.contains("--load-jobs")) clio.load_jobs = jobs_value(args.at("--load-jobs"), "--load-jobs");
                if (args.contains("--host")) clio.host = args.at("--host");
                if (args.contains("--keyspace")) clio.keyspace = args.at("--keyspace");
                if (args.contains("--username")) clio.username = args.at("--username");
                if (args.contains("--port")) {
                    auto value = args.at("--port"); unsigned port = 0;
                    auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), port);
                    if (ec != std::errc{} || end != value.data() + value.size() || !port || port > 65535) throw xrpla::Error("Invalid --port");
                    clio.port = static_cast<std::uint16_t>(port);
                }
                clio.tls = args.contains("--tls"); result = xrpla::core::import_clio(clio);
            } else throw xrpla::Error("Unknown import target");
        }
        else if (command == "validators") {
            if (!args.contains("--list") || !args.contains("--list-key")) throw xrpla::Error("validators requires --list and --list-key");
            xrpla::core::ValidatorsOptions validators{args.at("--list"), args.at("--list-key"), args.contains("--node") ? args.at("--node") : ""};
            if (args.contains("--ledgers")) validators.ledgers = static_cast<unsigned>(number_value("--ledgers", args.at("--ledgers"), 0, UINT32_MAX));
            result = xrpla::core::watch_validators(validators);
        }
        else if (command == "peers") {
            xrpla::core::PeersOptions peers;
            if (args.contains("--peers")) {
                std::stringstream list(args.at("--peers"));
                for (std::string peer; std::getline(list, peer, ',');) if (!peer.empty()) peers.peers.push_back(peer);
            }
            if (args.contains("--list")) peers.list = args.at("--list");
            if (args.contains("--list-key")) peers.list_key = args.at("--list-key");
            if (args.contains("--identity")) peers.identity = args.at("--identity");
            auto number = [&](char const* name, unsigned& value) { if (args.contains(name)) value = static_cast<unsigned>(number_value(name, args.at(name), 0, UINT32_MAX)); };
            if (args.contains("--network-id")) peers.network_id = xrpla::core::parse_network(args.at("--network-id"));
            number("--connections", peers.connections); number("--ledgers", peers.ledgers); number("--seconds", peers.seconds);
            result = xrpla::core::watch_peers(peers);
        }
        else if (command == "peer-compare") {
            xrpla::core::PeerCompareOptions compare;
            if (!args.contains("--reference") || !args.contains("--subject")) throw xrpla::Error("peer-compare requires --reference HOST:PORT --subject HOST:PORT");
            compare.reference = args.at("--reference"); compare.subject = args.at("--subject");
            if (args.contains("--network-id")) compare.network_id = xrpla::core::parse_network(args.at("--network-id"));
            if (args.contains("--ledgers")) compare.ledgers = static_cast<unsigned>(number_value("--ledgers", args.at("--ledgers"), 0, UINT32_MAX));
            if (args.contains("--spacing")) compare.spacing = static_cast<unsigned>(number_value("--spacing", args.at("--spacing"), 0, UINT32_MAX));
            if (args.contains("--timeout-ms")) compare.timeout_ms = static_cast<unsigned>(number_value("--timeout-ms", args.at("--timeout-ms"), 0, UINT32_MAX));
            if (args.contains("--seed")) compare.seed = static_cast<unsigned>(number_value("--seed", args.at("--seed"), 0, UINT32_MAX));
            if (args.contains("--identity")) compare.identity = args.at("--identity");
            result = xrpla::core::peer_compare(compare);
        }
        else if (command == "peer-ledger") {
            if (!args.contains("--hash")) throw xrpla::Error("peer-ledger requires --hash LEDGER_HASH");
            std::vector<std::string> peers;
            if (args.contains("--peers")) {
                std::stringstream list(args.at("--peers"));
                for (std::string peer; std::getline(list, peer, ',');) if (!peer.empty()) peers.push_back(peer);
            }
            result = xrpla::core::peer_ledger(peers, args.at("--hash"),
                                              args.contains("--network-id") ? xrpla::core::parse_network(args.at("--network-id")) : 0,
                                              args.contains("--identity") ? xrpla::fs::path(args.at("--identity")) : xrpla::fs::path{});
        }
        else if (command == "check-keys") {
            if (!args.contains("--archive") || !args.contains("--publisher-key")) throw xrpla::Error("check-keys requires --archive DIR and --publisher-key FILE");
            result = xrpla::core::check_keys(args.at("--archive"), args.at("--publisher-key"));
        }
        else if (command == "grpc-probe") {
            if (!args.contains("--source") || !args.contains("--ledger")) throw xrpla::Error("grpc-probe requires --source grpc://HOST:PORT and --ledger N");
            result = xrpla::core::probe_grpc(args.at("--source"), static_cast<std::uint32_t>(number_value("--ledger", args.at("--ledger"), 0, UINT32_MAX)),
                                             args.contains("--count") ? static_cast<std::uint32_t>(number_value("--count", args.at("--count"), 0, UINT32_MAX)) : 200,
                                             args.contains("--pages") ? static_cast<unsigned>(number_value("--pages", args.at("--pages"), 0, UINT32_MAX)) : 20);
        }
        else if (command == "compare-nodes") {
            if (!args.contains("--against")) throw xrpla::Error("compare-nodes requires --dir and --against (node database paths)");
            result = xrpla::core::compare_node_databases(options.dir, args.at("--against"), args.contains("--backend") ? args.at("--backend") : "nudb");
        }
        else if (command == "rotate-spool") {
            if (!args.contains("--signing-key")) throw xrpla::Error("rotate-spool requires --signing-key");
            auto chosen = publication();
            std::optional<std::uint32_t> start;
            if (args.contains("--start")) start = static_cast<std::uint32_t>(number_value("--start", args.at("--start"), 2, UINT32_MAX));
            result = xrpla::core::rotate_spool(options.dir, args.at("--signing-key"), chosen.source, chosen.publisher_key, !args.contains("--check"), {}, start);
        }
        else if (command == "sync") {
            auto chosen = publication();
            xrpla::core::SyncOptions sync{chosen.source, chosen.publisher_key, options.dir, std::nullopt, std::nullopt, options.jobs};
            if (!jobs_set) sync.jobs = 4;
            sync.indexes = index_options(args.contains("--indexes") ? args.at("--indexes") : "all");
            if (args.contains("--snapshots-every")) {
                auto const& value = args.at("--snapshots-every");
                auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), sync.snapshots_every);
                if (ec != std::errc{} || end != value.data() + value.size()) throw xrpla::Error("--snapshots-every must be a number of ledgers (0: none)");
            }
            if (!options.range.empty()) {  // --range, or the config's `range`
                auto const& text = options.range;
                if (text.ends_with("-")) sync.first = xrpla::parse_range(text.substr(0, text.size() - 1) + "-" + text.substr(0, text.size() - 1), UINT32_MAX).first;
                else { auto range = xrpla::parse_range(text, UINT32_MAX); sync.first = range.first; sync.last = range.last; }
            }
            result = xrpla::core::sync_pieces(sync);
        }
        else if (command == "restore") {
            xrpla::core::RestoreOptions restore;
            for (auto key : {"--to", "--out"}) if (!args.contains(key)) throw xrpla::Error(std::string("restore requires ") + key);
            auto chosen = publication();
            restore.source = chosen.source; restore.publisher_key = chosen.publisher_key;
            restore.target = args.at("--to"); restore.output = args.at("--out");
            if (!options.range.empty()) {  // --range, or the config's `range`
                auto const& text = options.range;
                if (text.ends_with("-")) restore.first = xrpla::parse_range(text.substr(0, text.size() - 1) + "-" + text.substr(0, text.size() - 1), UINT32_MAX).first;
                else { auto range = xrpla::parse_range(text, UINT32_MAX); restore.first = range.first; restore.last = range.last; }
            }
            if (args.contains("--host")) restore.clio_host = args.at("--host");
            if (args.contains("--keyspace")) restore.clio_keyspace = args.at("--keyspace");
            if (args.contains("--username")) restore.clio_username = args.at("--username");
            if (args.contains("--port")) restore.clio_port = static_cast<unsigned>(number_value("--port", args.at("--port"), 1, 65535));
            restore.clio_tls = args.contains("--tls");
            if (args.contains("--signing-key")) restore.signing_key = args.at("--signing-key");
            restore.replay = args.contains("--replay");
            if (jobs_set) restore.jobs = options.jobs;
            if (args.contains("--load-jobs")) {
                if (restore.target != "clio") throw xrpla::Error("--load-jobs applies to --to clio");
                restore.load_jobs = jobs_value(args.at("--load-jobs"), "--load-jobs");
            }
            result = xrpla::core::restore(restore);
        }
        else if (command == "serve") {
            xrpla::core::ServeOptions server{options.dir};
            if (args.contains("--http")) server.http = args.at("--http");
            if (args.contains("--ws")) server.ws = args.at("--ws");
            if (args.contains("--https")) server.https = args.at("--https");
            if (args.contains("--wss")) server.wss = args.at("--wss");
            if (args.contains("--tls-cert")) server.tls_cert = args.at("--tls-cert");
            if (args.contains("--tls-key")) server.tls_key = args.at("--tls-key");
            if (args.contains("--forward")) server.forward = args.at("--forward");
            if (args.contains("--forward-ws")) server.forward_ws = args.at("--forward-ws");
            if (args.contains("--threads")) server.threads = jobs_value(args.at("--threads"), "--threads");
            if (args.contains("--cache-mb")) server.cache_mb = number_value("--cache-mb", args.at("--cache-mb"), 1, 1ull << 30);
            if (args.contains("--warm-segments")) server.warm_segments = static_cast<unsigned>(number_value("--warm-segments", args.at("--warm-segments"), 0, UINT32_MAX));
            if (args.contains("--max-connections")) server.max_connections = static_cast<unsigned>(number_value("--max-connections", args.at("--max-connections"), 1, 1u << 20));
            if (args.contains("--max-connections-per-ip"))  // 0: no limit
                server.max_connections_per_ip = static_cast<unsigned>(number_value("--max-connections-per-ip", args.at("--max-connections-per-ip"), 0, 1u << 20));
            if (args.contains("--network-id")) server.network_id = xrpla::core::parse_network(args.at("--network-id"));
            if (args.contains("--no-loopback-trust")) server.loopback_trusted = false;
            if (args.contains("--tail")) { server.tail = args.at("--tail"); if (!args.contains("--tail-key")) throw xrpla::Error("--tail needs --tail-key"); server.tail_key = args.at("--tail-key"); }
            xrpla::core::serve(server);
            // Leave without static destructors: requests a stop left running (serve gives them
            // 10 s) and a tail's refresh thread still use them.
            std::cout.flush(); std::cerr.flush(); std::_Exit(0);
        } else if (command == "rpc") {
            if (!args.contains("--request")) throw xrpla::Error("rpc requires --request FILE (or - for stdin)");
            std::string bytes;
            if (args.at("--request") == "-") {
                char ch; while (std::cin.get(ch)) { if (bytes.size() == 64 * 1024) throw xrpla::Error("RPC request exceeds 64 KiB"); bytes += ch; }
            } else bytes = xrpla::read_file(args.at("--request"), 64 * 1024);
            if (args.contains("--network-id")) xrpla::core::configure_network(xrpla::core::parse_network(args.at("--network-id")));
            if (args.contains("--tail")) {
                if (!args.contains("--tail-key")) throw xrpla::Error("--tail needs --tail-key");
                xrpla::core::use_tail(options.dir, args.at("--tail"), args.at("--tail-key"));
            }
            auto request = xrpla::Json::parse(bytes);
            auto params = request.value("params", xrpla::Json::object());
            if (params.is_array() && params.size() == 1) params = params.at(0);
            result = xrpla::core::rpc_request(options.dir, request.at("method"), params, args.contains("--forward") ? args.at("--forward") : "");
        } else if (command == "prune") {
            if (!args.contains("--state-first")) throw xrpla::Error("prune requires --state-first N");
            result = xrpla::core::prune_store(options.dir, static_cast<std::uint32_t>(number_value("--state-first", args.at("--state-first"), 1, UINT32_MAX)));
        }
        else if (command == "keygen") {
            if (!args.contains("--out")) throw xrpla::Error("keygen requires --out");
            result = xrpla::core::create_signing_key(args.at("--out"));
        } else if (command == "export" || command == "publish") {
            for (auto key : {"--source", "--out", "--signing-key"})
                if (!args.contains(key) && !(key == std::string("--source") && args.contains("--peers"))) throw xrpla::Error(command + " requires " + key);
            unsigned delay = 200;
            if (args.contains("--delay-ms")) {
                auto const& text = args.at("--delay-ms");
                auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), delay);
                if (ec != std::errc{} || end != text.data() + text.size() || delay > 60000) throw xrpla::Error("Invalid --delay-ms");
            }
            xrpla::core::ExportOptions exporting{args.contains("--source") ? args.at("--source") : "", {0, 0}, args.at("--out"), args.at("--signing-key"), delay, args.contains("--full-state")};
            exporting.jobs = args.contains("--jobs") ? jobs_value(args.at("--jobs")) : command == "publish" ? 4 : 1;
            exporting.rpc_cache = !args.contains("--no-rpc-cache");
            if (args.contains("--grpc-source")) exporting.grpc_source = args.at("--grpc-source");
            // Nodes tried, in order, when --source does not hold a ledger or cannot be reached.
            if (args.contains("--fallback")) {
                std::stringstream list(args.at("--fallback"));
                for (std::string url; std::getline(list, url, ',');) if (!url.empty()) exporting.fallbacks.push_back(url);
            }
            // Connections to the gRPC node beside the JSON-RPC one's --jobs (without it, gRPC serves every ledger).
            if (args.contains("--grpc-jobs")) exporting.grpc_jobs = jobs_value(args.at("--grpc-jobs"), "--grpc-jobs");
            // Peers for transaction trees the source omits (a few 2013 ledgers): none unless given,
            // `default` finding them from the network's crawl. The published history holds those
            // ledgers already; only an export of them from a node needs this.
            if (args.contains("--history-peers") && args.at("--history-peers") == "default") exporting.history_peers = {"default"};
            else if (args.contains("--history-peers")) {
                std::stringstream list(args.at("--history-peers"));
                for (std::string peer; std::getline(list, peer, ',');) if (!peer.empty() && peer != "none") exporting.history_peers.push_back(peer);
            }
            if (args.contains("--history-identity")) exporting.history_identity = args.at("--history-identity");
            if (args.contains("--network-id")) exporting.network_id = xrpla::core::parse_network(args.at("--network-id"));
            if (args.contains("--compression-level")) {
                exporting.compression_level = jobs_value(args.at("--compression-level"), "--compression-level");
                if (exporting.compression_level > 19) throw xrpla::Error("Compression level must be 1..19");
            }
            if (args.contains("--interval-snapshots"))
                exporting.interval_snapshots = static_cast<unsigned>(number_value("--interval-snapshots", args.at("--interval-snapshots"), 1, UINT32_MAX));
            if (command == "export") {
                if (options.range.empty()) throw xrpla::Error("export requires --range");  // or the config's `range`
                exporting.range = xrpla::parse_range(options.range, UINT32_MAX);
                result = xrpla::core::export_archive(exporting);
            } else {
                // Whole numbers from 1 (from 0 where a check below states the flag's own rule).
                auto number = [&](char const* key, unsigned fallback, std::uint64_t low = 1) {
                    return args.contains(key) ? static_cast<unsigned>(number_value(key, args.at(key), low, UINT32_MAX)) : fallback;
                };
                xrpla::core::PublishOptions publishing{exporting};
                publishing.start = number("--start", 0); publishing.until = number("--until", 0);
                publishing.batch_ledgers = number("--batch-ledgers", 64); publishing.interval_seconds = number("--interval", 3);
                publishing.once = args.contains("--once");
                publishing.export_options.merge_chunks = args.contains("--merge-chunks");
                if (args.contains("--validator-list")) publishing.validator_list = args.at("--validator-list");
                if (args.contains("--validator-list-key")) publishing.validator_list_key = args.at("--validator-list-key");
                if (args.contains("--validations-node")) publishing.validations_node = args.at("--validations-node");
                if (args.contains("--peers")) {
                    std::stringstream list(args.at("--peers"));
                    for (std::string peer; std::getline(list, peer, ',');) if (!peer.empty()) publishing.peers.push_back(peer);
                }
                if (args.contains("--peer-identity")) publishing.peer_identity = args.at("--peer-identity");
                if (args.contains("--network-id")) publishing.network_id = xrpla::core::parse_network(args.at("--network-id"));
                publishing.peer_connections = number("--peer-connections", 10, 0);
                publishing.serve = args.contains("--peer-serve");
                publishing.serve_ledgers = number("--peer-ledgers", 256, 0);
                if (args.contains("--peer-listen")) publishing.serve_listen = args.at("--peer-listen");
                publishing.serve_max_inbound = number("--peer-max-inbound", 11);
                publishing.serve_crawl = args.contains("--peer-crawl");
                if (args.contains("--peer-reserved")) {
                    std::stringstream list(args.at("--peer-reserved"));
                    for (std::string key; std::getline(list, key, ',');) if (!key.empty()) publishing.serve_reserved.push_back(key);
                }
                publishing.relay = !args.contains("--peer-no-relay");
                for (auto const& [flag, policy] : {std::pair{"--peer-relay-validations", &publishing.relay_validations}, std::pair{"--peer-relay-proposals", &publishing.relay_proposals}}) {
                    if (!args.contains(flag)) continue;
                    *policy = args.at(flag);
                    if (*policy != "all" && *policy != "trusted" && *policy != "drop_untrusted") throw xrpla::Error(std::string(flag) + " is all, trusted or drop_untrusted");
                }
                publishing.squelch = !args.contains("--peer-no-squelch");
                publishing.squelch_sources = number("--peer-squelch-sources", 5, 0);
                publishing.peer_private = args.contains("--peer-private");
                publishing.serve_ip_limit = number("--peer-ip-limit", 2, 0);
                if (args.contains("--peer-max")) {
                    auto max_peers = number("--peer-max", 0, 0);
                    if (!max_peers) throw xrpla::Error("--peer-max must be positive");
                    auto [outbound, inbound] = xrpla::core::peer_limits(max_peers, publishing.peer_connections,
                        args.contains("--peer-max-inbound") ? std::optional<unsigned>(publishing.serve_max_inbound) : std::nullopt);
                    publishing.peer_connections = outbound; publishing.serve_max_inbound = inbound;
                }
                if (!publishing.peer_connections || !publishing.serve_ledgers) throw xrpla::Error("--peer-connections and --peer-ledgers must be positive");
                if (!publishing.squelch_sources || !publishing.serve_ip_limit) throw xrpla::Error("--peer-squelch-sources and --peer-ip-limit must be positive");
                if (publishing.peers.empty())
                    for (auto flag : {"--peer-max", "--peer-private", "--peer-no-relay", "--peer-relay-validations", "--peer-relay-proposals", "--peer-no-squelch", "--peer-squelch-sources"})
                        if (args.contains(flag)) throw xrpla::Error(std::string(flag) + " applies with --peers");
                if (!publishing.serve && (args.contains("--peer-listen") || args.contains("--peer-ledgers") || args.contains("--peer-crawl") || args.contains("--peer-max-inbound") ||
                                          args.contains("--peer-reserved") || args.contains("--peer-ip-limit")))
                    throw xrpla::Error("--peer-listen, --peer-ledgers, --peer-max-inbound, --peer-ip-limit, --peer-crawl and --peer-reserved apply with --peer-serve");
                result = xrpla::core::publish_archive(publishing);
            }
        }
        else {
            if (!args.contains("--kind") || !args.contains("--key")) throw xrpla::Error("query requires --kind and --key");
            std::uint32_t seq = 0;
            if (args.contains("--ledger")) {
                auto const& value = args.at("--ledger");
                auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), seq);
                if (ec != std::errc{} || end != value.data() + value.size() || !seq) throw xrpla::Error("--ledger requires a positive uint32 sequence");
            }
            result = xrpla::core::query_store(options.dir, args.at("--kind"), args.at("--key"), seq);
        }
#else
        throw xrpla::Error("This command requires the libxrpl-enabled core build");
#endif
    }
    std::cout << result.dump(2) << '\n';
    return 0;
}
} // namespace
// A full-history piece cache is hundreds of files, each open while served, and the cache is
// opened anew (beside the open one) whenever its selection changes: as xrpld does, the limit of
// open files is raised to what the system allows.
static void raise_open_file_limit() {
#ifndef _WIN32
    rlimit limit{};
    if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur < limit.rlim_max) {
        limit.rlim_cur = limit.rlim_max;
        setrlimit(RLIMIT_NOFILE, &limit);
    }
#endif
}
int main(int argc, char** argv) {
    raise_open_file_limit();
    try { return run(argc, argv); }
    catch (std::exception const& error) {
        std::cerr << xrpla::Json{{"error", error.what()}}.dump() << '\n';
        return 1;
    }
}
