// `xrpla setup` writes /etc/xrpla/xrpla.toml for one of the setup profiles; `xrpla run`
// carries a profile out. Profiles (docs/SETUP.md):
//   restore    stream a publication into an xrpld (NuDB/RocksDB) or Clio database, then exit
//   mirror     keep a piece cache in step with a publication and serve the RPC API from it
//   connected  follow a local or remote xrpld, seal its history into local pieces and serve
//              them with the newest ledgers as a live tail (run_connected)
//   standalone mirror a publication and follow the XRPL peer network, with no xrpld (run_standalone)
//   publisher  follow a node and publish pieces to R2/S3 (tools/live_publisher.py today)
#include "setup.hpp"
#include "config_reference.hpp"
#include "publications.hpp"
#include <xrpla/core/operations.hpp>
#include <toml++/toml.h>
#include <algorithm>
#include <optional>
#include <set>
#include <atomic>
#include <charconv>
#include <cstdint>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iostream>
#include <ctime>
#include <mutex>
#include <thread>
#include <utility>
#ifndef _WIN32
#include <csignal>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <grp.h>
#include <pwd.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>
#endif

namespace xrpla {
namespace {
std::string text(toml::table const& t, std::string_view path, std::string fallback = {}) {
    auto node = t.at_path(path);
    if (!node) return fallback;
    if (auto value = node.value<std::string>()) return *value;
    throw Error("Config " + std::string(path) + " must be a string");
}
std::optional<std::int64_t> integer(toml::table const& t, std::string_view path) {
    auto node = t.at_path(path);
    if (!node) return std::nullopt;
    if (auto value = node.value<std::int64_t>(); value && *value >= 0) return value;
    throw Error("Config " + std::string(path) + " must be a nonnegative integer");
}
// A true/false setting; `fallback` when absent. Any other value (the string "true", 1) is an
// error, not quietly the fallback.
bool flag_setting(toml::table const& t, std::string_view path, bool fallback) {
    auto node = t.at_path(path);
    if (!node) return fallback;
    if (auto value = node.value_exact<bool>()) return *value;
    throw Error("Config " + std::string(path) + " must be true or false");
}
// The pause before each request to the followed node: none by default, as the profiles follow
// the operator's own node (`xrpla publish` defaults to 200 ms, for public servers). Without
// gRPC every changed object is its own request, and 200 ms each falls behind mainnet.
std::string node_delay(toml::table const& t) { return std::to_string(integer(t, "node.delay_ms").value_or(0)); }
std::optional<std::uint32_t> ledger(toml::table const& t, std::string_view path) {
    auto node = t.at_path(path);
    if (!node) return std::nullopt;
    if (auto word = node.value<std::string>(); word && (*word == "earliest" || *word == "tip")) return std::nullopt;
    auto n = integer(t, path);
    if (!n || *n > UINT32_MAX) throw Error("Config " + std::string(path) + " must be a ledger sequence, \"earliest\" or \"tip\"");
    return static_cast<std::uint32_t>(*n);
}
// [index]: the optional piece cache indexes (both on unless set to false).
core::IndexOptions index_options(toml::table const& t) {
    core::IndexOptions options;
    for (auto [path, value] : {std::pair{"index.mpt_holders", &options.mpt_holders}, std::pair{"index.nfts_by_issuer", &options.nfts_by_issuer}}) {
        auto node = t.at_path(path);
        if (!node) continue;
        auto flag = node.value<bool>();
        if (!flag || !node.is_boolean()) throw Error(std::string("Config ") + path + " must be true or false");
        *value = *flag;
    }
    return options;
}
// [cache] snapshots_every: besides the base and the newest, the published state snapshots
// the piece cache keeps (those at a multiple of this many ledgers; 0 or unset: none).
std::uint32_t snapshots_every(toml::table const& t) {
    auto every = integer(t, "cache.snapshots_every").value_or(0);
    if (every > UINT32_MAX) throw Error("Config cache.snapshots_every must be a number of ledgers");
    return static_cast<std::uint32_t>(every);
}
std::string required(toml::table const& t, std::string_view path) {
    auto value = text(t, path);
    if (value.empty()) throw Error("Config needs " + std::string(path));
    return value;
}
// [publication]: by name from the approved list (publications.hpp), or by its source (one place,
// or an array of mirrors tried in order) and publisher_key. A source given with a name replaces
// the listed mirrors; a publisher_key given with one must be the listed key.
ChosenPublication publication_of(toml::table const& t) {
    Mirrors source;
    if (auto node = t.at_path("publication.source")) {
        if (auto one = node.value_exact<std::string>()) source.push_back(*one);
        else if (auto const* many = node.as_array())
            for (auto const& mirror : *many) source.push_back(mirror.value_exact<std::string>().value_or(""));
        if (source.empty() || std::find(source.begin(), source.end(), "") != source.end())
            throw Error("Config publication.source must be a URL or directory, or an array of them (mirrors, tried in order)");
    }
    auto name = text(t, "publication.name"), key = text(t, "publication.publisher_key");
    if (name.empty() && source.empty()) throw Error("Config needs publication.name, or publication.source and publication.publisher_key");
    if (name.empty() && key.empty()) throw Error("Config needs publication.publisher_key");
    return choose_publication(name, source, key);
}
// [network] id: a network ID, or "mainnet", "testnet" or "devnet"; mainnet unless set.
std::uint32_t network_of(toml::table const& t) {
    auto node = t.at_path("network.id");
    if (!node) return 0;
    if (auto name = node.value<std::string>(); name && node.is_string()) return core::parse_network(*name);
    if (auto id = node.value<std::int64_t>(); id && node.is_integer() && *id >= 0 && *id <= UINT32_MAX) return static_cast<std::uint32_t>(*id);
    throw Error("Config network.id must be mainnet, testnet, devnet or a network ID");
}
// An integer setting held to the bounds `xrpla serve` holds its flag to: the interim server is
// started with these as flags, and one out of bounds would fail to start every 30 s instead.
std::optional<std::int64_t> bounded(toml::table const& t, std::string_view path, std::int64_t low, std::int64_t high) {
    auto value = integer(t, path);
    if (value && (*value < low || *value > high))
        throw Error("Config " + std::string(path) + " must be from " + std::to_string(low) + " to " + std::to_string(high));
    return value;
}
core::ServeOptions serve_options(toml::table const& config, fs::path const& dir, std::string http, std::string ws) {
    core::ServeOptions serve{dir};
    serve.http = text(config, "serve.http", http); serve.ws = text(config, "serve.ws", ws);
    serve.https = text(config, "serve.https"); serve.wss = text(config, "serve.wss");
    serve.tls_cert = text(config, "serve.tls_cert"); serve.tls_key = text(config, "serve.tls_key");
    if (auto threads = bounded(config, "serve.threads", 1, 32)) serve.threads = static_cast<unsigned>(*threads);
    serve.forward = text(config, "serve.forward");
    serve.forward_ws = text(config, "serve.forward_ws");
    if (auto most = bounded(config, "serve.max_connections", 1, 1 << 20)) serve.max_connections = static_cast<unsigned>(*most);
    if (auto most = bounded(config, "serve.max_connections_per_ip", 0, 1 << 20)) serve.max_connections_per_ip = static_cast<unsigned>(*most);  // 0: no limit
    if (auto warm = bounded(config, "serve.warm_segments", 0, UINT32_MAX)) serve.warm_segments = static_cast<unsigned>(*warm);
    serve.loopback_trusted = flag_setting(config, "serve.loopback_trusted", serve.loopback_trusted);
    if (auto cache = bounded(config, "storage.cache_mb", 0, std::int64_t{1} << 30)) serve.cache_mb = static_cast<std::size_t>(*cache);  // 0: the default
    serve.network_id = network_of(config);
    return serve;
}

// Rounds of a background sync skipped because another holds the cache (a sync or index-pieces
// run by hand): logged at the first, and again every ten minutes it stays so, for a lock never
// released would otherwise stop the cache following the publication in silence.
class LockedSkips {
    std::optional<std::chrono::steady_clock::time_point> since_, logged_;
public:
    void skipped(fs::path const& cache) {
        auto now = std::chrono::steady_clock::now();
        if (!since_) since_ = now;
        if (logged_ && now - *logged_ < std::chrono::minutes(10)) return;
        logged_ = now;
        std::cerr << Json{{"event", "sync_skipped_locked"}, {"cache", cache.string()},
                          {"locked_seconds", std::chrono::duration_cast<std::chrono::seconds>(now - *since_).count()}}.dump() << std::endl;
    }
    void ran() { since_.reset(); logged_.reset(); }
};
// A sync at startup that fails while the cache already holds a selection: it is served, and the
// background sync tries again (a stale catalog pointer, a publication briefly out of reach).
bool serve_existing(fs::path const& cache, std::exception const& error) {
    if (!fs::exists(cache / "selection.json")) return false;
    std::cerr << Json{{"event", "sync_failed"}, {"error", error.what()}, {"serving", "existing selection"}}.dump() << std::endl;
    return true;
}

#ifndef _WIN32
// Runs a command (named `name` in logs) until stopped, restarting it 30 s after it exits. The
// child gets SIGTERM if this process dies; stop() sends it SIGTERM and waits for it.
class Supervised {
    std::vector<std::string> args_;
    std::atomic<bool> stopping_{false};
    std::atomic<pid_t> child_{0};
    std::thread thread_;
public:
    Supervised(std::vector<std::string> args, std::string name) : args_(std::move(args)) {
        thread_ = std::thread([this, name = std::move(name)] {
            std::vector<char*> argv;
            for (auto& arg : args_) argv.push_back(arg.data());
            argv.push_back(nullptr);
            while (!stopping_) {
                auto pid = fork();
                if (pid < 0) std::cerr << Json{{"event", name + "_failed"}, {"error", std::strerror(errno)}}.dump() << std::endl;
                if (pid == 0) {
                    prctl(PR_SET_PDEATHSIG, SIGTERM);
                    execvp(argv[0], argv.data());
                    _exit(127);
                }
                int status = -1;
                if (pid > 0) {
                    child_ = pid;
                    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
                    child_ = 0;
                }
                if (stopping_) break;
                std::cerr << Json{{"event", name + "_exited"}, {"status", status}}.dump() << std::endl;
                for (int i = 0; i < 300 && !stopping_; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }
    void stop() {
        if (stopping_.exchange(true)) return;
        if (auto pid = child_.load()) kill(pid, SIGTERM);
        thread_.join();
    }
    ~Supervised() { stop(); }
};
// [log] file: this process's stdout and stderr, and so those of the processes it starts, go
// through one pipe to a thread that is the file's only writer. It starts a new file at a line
// boundary once the file passes max_mb, renaming FILE to FILE.1 (FILE.1 to FILE.2, ...) and
// deleting the oldest beyond `keep`, so a process never writes to a file another has renamed.
class RotatingLog {
    fs::path path_;
    std::uintmax_t max_;
    unsigned keep_;
    int read_ = -1;
    std::thread thread_;
    std::atomic<bool> done_{false};
    void rotate() {
        std::error_code ignored;
        auto numbered = [&](unsigned i) { return fs::path(path_.string() + "." + std::to_string(i)); };
        if (!keep_) { fs::remove(path_, ignored); return; }
        fs::remove(numbered(keep_), ignored);
        for (unsigned i = keep_; i > 1; --i) fs::rename(numbered(i - 1), numbered(i), ignored);
        fs::rename(path_, numbered(1), ignored);
    }
    void copy() {
        std::ofstream out(path_, std::ios::app | std::ios::binary);
        std::error_code ignored;
        auto size = fs::file_size(path_, ignored);
        if (ignored) size = 0;
        std::vector<char> buffer(1 << 16);
        for (;;) {
            auto n = ::read(read_, buffer.data(), buffer.size());
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) break;
            out.write(buffer.data(), n); out.flush();
            size += static_cast<std::uintmax_t>(n);
            if (size >= max_ && buffer[static_cast<std::size_t>(n) - 1] == '\n') {
                out.close(); rotate();
                out.open(path_, std::ios::app | std::ios::binary); size = 0;
            }
        }
        done_ = true;
    }
public:
    RotatingLog(fs::path path, std::uintmax_t max_bytes, unsigned keep) : path_(std::move(path)), max_(max_bytes), keep_(keep) {
        if (path_.has_parent_path()) fs::create_directories(path_.parent_path());
        { std::ofstream probe(path_, std::ios::app | std::ios::binary);
          if (!probe) throw Error("Cannot write the log file " + path_.string() + " (under the unit xrpla setup wrote, the service writes only the directories it named then: run setup again after setting [log] file)"); }
        int fds[2];
        if (::pipe(fds) != 0) throw Error(std::string("Cannot create the log pipe: ") + std::strerror(errno));
        ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);  // started processes hold only the write end
        read_ = fds[0];
        std::cout.flush(); std::cerr.flush();
        ::dup2(fds[1], STDOUT_FILENO); ::dup2(fds[1], STDERR_FILENO); ::close(fds[1]);
        thread_ = std::thread([this] { copy(); });
    }
    // Writes what is left once this process stops writing. A process it started may still hold
    // the pipe; after two seconds the rest is left behind.
    void finish() {
        std::cout.flush(); std::cerr.flush();
        ::close(STDOUT_FILENO); ::close(STDERR_FILENO);
        for (int i = 0; i < 200 && !done_; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (thread_.joinable()) { if (done_) thread_.join(); else thread_.detach(); }
    }
    ~RotatingLog() { finish(); }
};
std::unique_ptr<RotatingLog> log_file;
void use_log(toml::table const& config) {
    auto file = text(config, "log.file");
    if (file.empty()) return;
    auto max_mb = integer(config, "log.max_mb").value_or(100), keep = integer(config, "log.keep").value_or(5);
    if (max_mb < 1 || keep < 0 || keep > 1000) throw Error("[log] max_mb must be at least 1 and keep between 0 and 1000");
    log_file = std::make_unique<RotatingLog>(file, static_cast<std::uintmax_t>(max_mb) << 20, static_cast<unsigned>(keep));
}
// Leaves once serving ends: the tail and sync loops still run on detached threads, so static
// destructors must not run under them.
[[noreturn]] void leave(Supervised& child) {
    child.stop();
    std::cout.flush(); std::cerr.flush();
    if (log_file) log_file->finish();
    std::_Exit(0);
}
// Syncs `sync` whenever the publication's catalog pointer changes, and at least every `interval`.
// A sync run by hand holds the cache meanwhile: the round is skipped, and tried again shortly.
void keep_synced(core::SyncOptions sync, fs::path pointer, std::chrono::seconds interval) {
    std::thread([sync, pointer, interval] {
        std::optional<fs::file_time_type> seen;
        std::optional<std::chrono::steady_clock::time_point> retry;
        LockedSkips locked;
        auto last = std::chrono::steady_clock::now();
        bool local = fs::exists(pointer);
        if (local) seen = fs::last_write_time(pointer);
        for (;;) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            try {
                if (retry) {
                    if (std::chrono::steady_clock::now() < *retry) continue;
                    retry.reset();
                } else if (local) {
                    auto stamp = fs::last_write_time(pointer);
                    if (stamp == seen && std::chrono::steady_clock::now() - last < interval) continue;
                    seen = stamp;
                } else if (std::chrono::steady_clock::now() - last < interval) continue;
                last = std::chrono::steady_clock::now();
                auto result = core::sync_pieces(sync);
                locked.ran();
                if (result.at("fetched_bytes").get<std::uint64_t>()) std::cerr << result.dump() << std::endl;
                core::release_freed_memory();  // what the sync's downloads and indexing freed
            }
            catch (DirectoryLocked const&) { locked.skipped(sync.cache); retry = std::chrono::steady_clock::now() + std::chrono::seconds(30); }
            catch (std::exception const& error) { locked.ran(); std::cerr << Json{{"event", "sync_failed"}, {"error", error.what()}}.dump() << std::endl; }
        }
    }).detach();
}
// The node to follow, then each fallback (a URL or a list). The first is the primary: a local
// xrpld's admin port, which is not rate-limited. Later ones are asked for ledgers it no longer holds.
std::vector<std::string> node_endpoints(toml::table const& config) {
    std::vector<std::string> urls{required(config, "node.rpc")};
    auto node = config.at_path("node.fallback");
    if (!node) return urls;
    auto add = [&](std::optional<std::string> url) {
        if (!url || url->empty()) throw Error("Config node.fallback must be a URL or an array of URLs");
        urls.push_back(*url);
    };
    if (auto one = node.value<std::string>()) add(one);
    else if (auto const* many = node.as_array()) {
        if (many->empty()) throw Error("Config node.fallback must be a URL or an array of URLs");
        for (auto const& item : *many) add(item.value<std::string>());
    } else throw Error("Config node.fallback must be a URL or an array of URLs");
    return urls;
}
void add_fallback(std::vector<std::string>& args, std::vector<std::string> const& urls) {
    std::string list;
    for (std::size_t i = 1; i < urls.size(); ++i) list += (list.empty() ? "" : ",") + urls[i];
    if (!list.empty()) { args.push_back("--fallback"); args.push_back(list); }
}
// connected: tools/live_publisher.py follows the node into a spool (`xrpla publish`, a few
// seconds behind) and seals complete pieces into a local bucket signed by this machine's key;
// a piece cache is kept in step with that bucket and served with the spool as a live tail.
//   STORAGE/keys      spool.seed (the follower's spool), local.seed/.pub (the local bucket)
//   STORAGE/follower  the live publisher's work directory (spools/, current, status.json)
//   STORAGE/bucket    sealed pieces, bundles and snapshots   STORAGE/cache  the piece cache
int run_connected(toml::table const& config) {
    fs::path storage = required(config, "storage.dir");
    auto keys = storage / "keys", work = storage / "follower", bucket = storage / "bucket", cache = storage / "cache";
    fs::create_directories(keys); fs::create_directories(work);
    auto key = [&](std::string const& name) {
        auto seed = keys / (name + ".seed"), pub = keys / (name + ".pub");
        if (!fs::exists(seed)) core::create_signing_key(seed);
        if (!fs::exists(pub)) write_file(pub, core::public_key(seed) + "\n");
        return seed;
    };
    auto spool_seed = key("spool"), local_seed = key("local");
    auto exe = fs::read_symlink("/proc/self/exe"), bin = exe.parent_path();
    fs::path tools = text(config, "connected.tools");
    bool configured = !tools.empty();
    if (!configured) tools = bin.parent_path() / "share" / "xrpla" / "tools";
    if (!fs::exists(tools / "live_publisher.py")) {
        // The source tree's tools serve only a binary run where it was built (the tests): an
        // installed one never runs a script from wherever its build once was, as the service
        // account (or root) and with that tree long gone or changed.
        std::error_code ec;
        auto built = fs::weakly_canonical(XRPLA_BUILD_DIR, ec), inside = exe.lexically_relative(built);
        if (configured || ec || inside.empty() || *inside.begin() == "..")
            throw Error("Connected mode needs " + (tools / "live_publisher.py").string() + ", which is missing; reinstall xrpla or set connected.tools");
        tools = XRPLA_SOURCE_TOOLS;
    }
    auto knob = [&](char const* name, std::int64_t fallback) { return std::to_string(integer(config, std::string("connected.") + name).value_or(fallback)); };
    auto endpoints = node_endpoints(config);
    std::vector<std::string> args{"python3", (tools / "live_publisher.py").string(), "--node", endpoints.front(),
        "--workdir", work.string(), "--spool-seed", spool_seed.string(), "--xrpla", exe.string(), "--bucket", bucket.string(),
        "--signing-key", local_seed.string(),
        "--decoder", (bin / "xrpla_history_decode").string(), "--snapshot-tool", (bin / "xrpla_snapshot").string(),
        "--encoder", (bin / "xrpla_history_encode").string(), "--index-tool", (bin / "xrpla_tx_index").string(),
        "--merge-chunks", "--poll-seconds", knob("poll_seconds", 3), "--batch-ledgers", "64",
        "--publish-seconds", knob("publish_seconds", 60), "--piece-ledgers", knob("piece_ledgers", 50000),
        "--bundle-pieces", knob("bundle_pieces", 20), "--delay-ms", node_delay(config)};
    add_fallback(args, endpoints);
    if (auto grpc = text(config, "node.grpc"); !grpc.empty()) { args.push_back("--grpc-source"); args.push_back(grpc); }
    if (auto list = text(config, "validators.list"); !list.empty()) {
        for (auto const& [flag, value] : {std::pair{"--validator-list", list}, {"--validator-list-key", required(config, "validators.list_key")},
                                          {"--validations-node", required(config, "node.validations")}}) { args.push_back(flag); args.push_back(value); }
    }
    if (!fs::exists(work / "current")) {
        // The first ledger to follow; the node must still hold its parent, whose state it exports.
        std::uint32_t start = 0;
        if (auto first = ledger(config, "connected.start")) start = *first;
        else {
            auto response = core::call_sources(endpoints, "ledger", {{"ledger_index", "validated"}}, 0, 5, {});
            auto index = response.at("ledger_index");
            start = index.is_string() ? static_cast<std::uint32_t>(std::stoul(index.get<std::string>())) : index.get<std::uint32_t>();
        }
        args.push_back("--start"); args.push_back(std::to_string(start));
    }
    // The live publisher restarts its own follower; it is restarted in turn if it exits.
    Supervised publisher(args, "live_publisher");
    // The bucket's first catalog holds the baseline snapshot, written once the follower has
    // exported the node's state at its first ledger (tens of minutes on mainnet).
    core::SyncOptions sync{{bucket.string()}, keys / "local.pub", cache, std::nullopt, std::nullopt};
    sync.indexes = index_options(config);
    sync.snapshots_every = snapshots_every(config);
    for (int waited = 0;; ++waited) {
        if (fs::exists(bucket / "catalog" / "current.json")) {
            try { std::cerr << core::sync_pieces(sync).dump() << std::endl; break; }
            catch (std::exception const& error) {
                if (serve_existing(cache, error)) break;
                std::cerr << Json{{"event", "sync_failed"}, {"error", error.what()}}.dump() << std::endl;
            }
        } else if (fs::exists(cache / "selection.json")) break;  // the bucket recreated: served meanwhile
        else if (waited % 30 == 0) std::cerr << Json{{"event", "waiting_for_baseline"}, {"bucket", bucket.string()}}.dump() << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    // Synced as soon as the local publisher writes a new catalog generation (and at least every
    // sync_seconds), so sealed pieces replace the tail they came from without a gap.
    keep_synced(sync, bucket / "catalog" / "current.json", std::chrono::seconds(integer(config, "connected.sync_seconds").value_or(30)));
    auto serve = serve_options(config, cache, "127.0.0.1:5105", "127.0.0.1:6106");
    serve.tail = work; serve.tail_key = work / "spool.pub";
    core::serve(serve);  // returns on SIGTERM or SIGINT
    leave(publisher);
}

// Runs `args` to completion (a child that dies with this process); its exit status.
int run_child(std::vector<std::string> args) {
    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    auto pid = fork();
    if (pid < 0) throw Error("Cannot start " + args.front());
    if (pid == 0) { prctl(PR_SET_PDEATHSIG, SIGTERM); execvp(argv[0], argv.data()); _exit(127); }
    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128;
}
// The first ledger of a node's newest contiguous range of complete ledgers.
std::uint32_t node_first_ledger(std::string const& url) {
    auto info = core::call_sources({url}, "server_info", Json::object(), 0, 5, {});
    auto ranges = info.at("info").at("complete_ledgers").get<std::string>();
    auto last = ranges.substr(ranges.rfind(',') == std::string::npos ? 0 : ranges.rfind(',') + 1);
    if (last.empty() || last == "empty") throw Error("The node has no complete ledgers yet");
    return static_cast<std::uint32_t>(std::stoul(last.substr(0, last.find('-'))));
}
// The earliest ledger any of the nodes still holds. One that cannot be asked is skipped.
std::uint32_t earliest_complete(std::vector<std::string> const& urls) {
    std::optional<std::uint32_t> earliest;
    std::string errors;
    for (auto const& url : urls) {
        try {
            auto first = node_first_ledger(url);
            earliest = earliest ? std::min(*earliest, first) : first;
        } catch (std::exception const& error) { errors += url + ": " + error.what() + "; "; }
    }
    if (!earliest) throw Error("No node reported complete ledgers (" + errors + ")");
    return *earliest;
}
std::string peer_list(toml::table const& config) {
    std::string list;
    if (auto peers = config.at_path("peers.endpoints").as_array())
        for (auto const& peer : *peers) if (auto text = peer.value<std::string>()) list += (list.empty() ? "" : ",") + *text;
    return list.empty() ? "default" : list;
}

// History from a publication (a piece cache kept in step with it), newer ledgers from a
// follower extending a spool that starts at the publication's newest snapshot
// (`restore --to spool`) and restarts from each newer one (`rotate-spool`). The follower reads
// the XRPL peer network (standalone) or a node (connected with a [publication]); each ledger is
// checked by the verifier, and accepted only once a quorum of the validator list signed it
// (or a later ledger whose parent chain reaches it), unless a trusted node says so.
//   STORAGE/cache  the piece cache   STORAGE/spool  the follower's spool
//   STORAGE/keys   spool.seed, node.key (the node identity on the peer network)
int run_mirrored(toml::table const& config, std::string const& profile) {
    bool from_peers = profile == "standalone";
    fs::path storage = required(config, "storage.dir");
    auto keys = storage / "keys", cache = storage / "cache", spool = storage / "spool";
    fs::create_directories(keys);
    auto seed = keys / "spool.seed";
    if (!fs::exists(seed)) core::create_signing_key(seed);
    auto knob = [&](char const* name, std::int64_t fallback) { return integer(config, profile + "." + name).value_or(fallback); };
    auto publication = publication_of(config);
    core::SyncOptions sync{publication.source, publication.publisher_key, cache, ledger(config, "history.first"), std::nullopt};
    sync.indexes = index_options(config);
    sync.snapshots_every = snapshots_every(config);
    for (;;) {
        try { std::cerr << core::sync_pieces(sync).dump() << std::endl; break; }
        catch (std::exception const& error) {
            if (serve_existing(cache, error)) break;
            std::cerr << Json{{"event", "sync_failed"}, {"error", error.what()}}.dump() << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::seconds(30));
    }
    auto exe = fs::read_symlink("/proc/self/exe");
    auto serve = from_peers ? serve_options(config, cache, "127.0.0.1:5005", "127.0.0.1:6006") : serve_options(config, cache, "127.0.0.1:5105", "127.0.0.1:6106");
    // The piece cache answers at once: while the spool is built or extended (hours, on a new
    // install), a server without the live tail serves it, then gives way to the full one.
    std::vector<std::string> interim_args{exe.string(), "serve", "--dir", cache.string(), "--threads", std::to_string(serve.threads),
                                          "--network-id", std::to_string(serve.network_id)};
    for (auto const& [flag, value] : {std::pair{"--http", serve.http}, {"--ws", serve.ws}, {"--https", serve.https}, {"--wss", serve.wss},
                                      {"--tls-cert", serve.tls_cert.string()}, {"--tls-key", serve.tls_key.string()},
                                      {"--forward", serve.forward}, {"--forward-ws", serve.forward_ws}})
        if (!value.empty()) { interim_args.push_back(flag); interim_args.push_back(value); }
    // The configured limits hold for it too: it is as public as the server it stands in for.
    for (auto const& [flag, value] : {std::pair{"--max-connections", serve.max_connections}, {"--max-connections-per-ip", serve.max_connections_per_ip},
                                      {"--warm-segments", serve.warm_segments}}) {
        interim_args.push_back(flag); interim_args.push_back(std::to_string(value));
    }
    if (serve.cache_mb) { interim_args.push_back("--cache-mb"); interim_args.push_back(std::to_string(serve.cache_mb)); }
    if (!serve.loopback_trusted) interim_args.push_back("--no-loopback-trust");
    auto interim = std::make_unique<Supervised>(interim_args, "interim_server");
    // The piece cache's last ledger: the spool starts no later than the ledger after it, so the
    // tail continues the pieces without a gap (a gap would leave the tail unserved).
    auto cached_last = [&] { return Json::parse(read_file(cache / "selection.json", 64 << 20)).at("last").get<std::uint32_t>(); };
    // A rotation stopped between its two renames left the new spool, complete, as spool.next:
    // it is put in place, rather than the spool rebuilt from the publication (hours) and the
    // ledgers the retired one held past the publication fetched again. So too before each
    // rotation check, and before the follower restarts after a rotation failed.
    auto recover = [&] {
        try {
            auto recovered = core::recover_spool(spool, seed);
            if (recovered.at("recovered").get<bool>()) std::cerr << Json{{"event", "spool_recovered"}, {"result", recovered}}.dump() << std::endl;
        } catch (std::exception const& error) { std::cerr << Json{{"event", "spool_recovery_failed"}, {"error", error.what()}}.dump() << std::endl; }
    };
    // The spool, made ready for the follower: here, and after a rotation that left none.
    auto rebuild = [&] {
        if (fs::exists(spool / "manifest.json")) return;
        // An interrupted restore left an unusable spool; it is rebuilt from the publication.
        if (fs::exists(spool)) fs::remove_all(spool);
        core::RestoreOptions restore;
        restore.source = sync.source; restore.publisher_key = sync.publisher_key; restore.target = "spool";
        restore.output = spool; restore.signing_key = seed; restore.last = cached_last();
        std::cerr << Json{{"event", "spool_restored"}, {"result", core::restore_spool(restore)}}.dump() << std::endl;
    };
    auto extend_from_publication = [&] {
        // What the publication holds past the spool's tip (a new install's spool build, or time
        // stopped) comes from it: peers keep only recent ledgers, so the follower is left the
        // last stretch only.
        core::RestoreOptions extend;
        extend.source = sync.source; extend.publisher_key = sync.publisher_key; extend.output = spool; extend.signing_key = seed;
        try { std::cerr << Json{{"event", "spool_extended"}, {"result", core::extend_spool(extend)}}.dump() << std::endl; }
        catch (std::exception const& error) { std::cerr << Json{{"event", "spool_extend_failed"}, {"error", error.what()}}.dump() << std::endl; }
    };
    recover();
    rebuild();
    extend_from_publication();
    std::vector<std::string> common{exe.string(), "publish", "--out", spool.string(), "--signing-key", seed.string(), "--merge-chunks",
        "--interval", std::to_string(knob("poll_seconds", 3)), "--batch-ledgers", "64"};
    auto from_network = common;
    for (auto const& arg : std::vector<std::string>{"--peers", peer_list(config), "--peer-identity", (keys / "node.key").string(),
                                                    "--network-id", std::to_string(network_of(config))})
        from_network.push_back(arg);
    // Serving the peer network (off by default; docs/PEER-SERVING.md).
    if (auto outbound = integer(config, "peers.outbound")) { from_network.push_back("--peer-connections"); from_network.push_back(std::to_string(*outbound)); }
    // As an xrpld on the peer network: its peer limit, privacy, and relaying (docs/PEER-SERVING.md).
    auto flag = [&](char const* name, std::string const& value) { from_network.push_back(name); if (!value.empty()) from_network.push_back(value); };
    if (auto max_peers = integer(config, "peers.max_peers")) flag("--peer-max", std::to_string(*max_peers));
    if (flag_setting(config, "peers.private", false)) flag("--peer-private", "");
    if (!flag_setting(config, "peers.relay", true)) flag("--peer-no-relay", "");
    if (auto policy = text(config, "peers.relay_validations"); !policy.empty()) flag("--peer-relay-validations", policy);
    if (auto policy = text(config, "peers.relay_proposals"); !policy.empty()) flag("--peer-relay-proposals", policy);
    if (!flag_setting(config, "peers.squelch", true)) flag("--peer-no-squelch", "");
    if (auto sources = integer(config, "peers.squelch_sources")) flag("--peer-squelch-sources", std::to_string(*sources));
    if (from_peers && flag_setting(config, "peers.serve", false)) {
        if (auto limit = integer(config, "peers.ip_limit")) flag("--peer-ip-limit", std::to_string(*limit));
        from_network.push_back("--peer-serve");
        from_network.push_back("--peer-ledgers"); from_network.push_back(std::to_string(integer(config, "peers.ledgers").value_or(256)));
        if (auto listen = text(config, "peers.listen"); !listen.empty()) { from_network.push_back("--peer-listen"); from_network.push_back(listen); }
        if (auto inbound = integer(config, "peers.max_inbound")) { from_network.push_back("--peer-max-inbound"); from_network.push_back(std::to_string(*inbound)); }
        if (flag_setting(config, "peers.crawl", false)) from_network.push_back("--peer-crawl");
        std::string reserved;
        if (auto keys = config.at_path("peers.reserved").as_array())
            for (auto const& key : *keys) if (auto text = key.value<std::string>()) reserved += (reserved.empty() ? "" : ",") + *text;
        if (!reserved.empty()) { from_network.push_back("--peer-reserved"); from_network.push_back(reserved); }
    }
    if (auto list = text(config, "validators.list"); !list.empty())
        for (auto const& arg : std::vector<std::string>{"--validator-list", list, "--validator-list-key", required(config, "validators.list_key")})
            from_network.push_back(arg);
    auto args = from_network;
    std::atomic<bool> stopping{false};  // set once the service stops: a restart under way gives up
    // A node keeps recent ledgers only. Ledgers it no longer holds come from the fallback
    // nodes while the follower runs. Ledgers none of them hold (a publication up to a piece
    // behind every node's history) come from peers.
    auto endpoints = node_endpoints(config);
    auto fill_gap = [&] {
        if (from_peers) return;
        for (;;) {
            if (stopping) return;
            try {
                auto tip = Json::parse(read_file(spool / "manifest.json", 16 << 20)).at("tip").at("seq").get<std::uint32_t>();
                auto first = earliest_complete(endpoints);
                if (tip + 1 >= first) break;
                std::cerr << Json{{"event", "filling_gap_from_peers"}, {"from", tip + 1}, {"until", first}}.dump() << std::endl;
                auto fill = from_network;
                fill.push_back("--until"); fill.push_back(std::to_string(first));
                if (run_child(fill) == 0) break;
            } catch (std::exception const& error) { std::cerr << Json{{"event", "gap_fill_failed"}, {"error", error.what()}}.dump() << std::endl; }
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }
    };
    if (!from_peers) {
        args = common;
        for (auto const& arg : std::vector<std::string>{"--source", endpoints.front(), "--delay-ms", node_delay(config)}) args.push_back(arg);
        add_fallback(args, endpoints);
        if (auto grpc = text(config, "node.grpc"); !grpc.empty()) { args.push_back("--grpc-source"); args.push_back(grpc); }
        if (auto list = text(config, "validators.list"); !list.empty())
            for (auto const& arg : std::vector<std::string>{"--validator-list", list, "--validator-list-key", required(config, "validators.list_key"),
                                                            "--validations-node", required(config, "node.validations")})
                args.push_back(arg);
        fill_gap();
    }
    auto name = from_peers ? "peer_follower" : "node_follower";
    std::mutex follower_mutex;
    auto follower = std::make_unique<Supervised>(args, name);
    // The follower, restarted after a rotation paused it, on a spool: a rotation that failed
    // between its renames and could not undo them leaves spool.next to put in place first, and
    // if that fails too the spool is rebuilt as at start-up. With no spool the follower would
    // make an empty one, which a later recovery may remove while it writes its first commit: it
    // stays stopped instead, and this is tried again (false). follower_mutex is taken only to
    // start it: a rebuild takes hours and a gap fill lasts until peers answer, and a service
    // stop (which sets `stopping`, then takes follower_mutex) waits for neither.
    auto restart = [&] {
        try {
            if (stopping) return false;
            recover();
            if (!fs::exists(spool / "manifest.json")) {
                std::cerr << Json{{"event", "spool_rebuilding"}, {"reason", "no spool after a failed rotation; the follower waits"}}.dump() << std::endl;
                rebuild();
                extend_from_publication();
                fill_gap();
            }
            std::lock_guard lock(follower_mutex);
            if (stopping) return false;
            follower = std::make_unique<Supervised>(args, name);
            return true;
        } catch (std::exception const& error) {
            std::cerr << Json{{"event", "follower_restart_failed"}, {"error", error.what()}}.dump() << std::endl;
            return false;
        }
    };
    keep_synced(sync, fs::path(sync.source.front()) / "catalog" / "current.json", std::chrono::seconds(knob("sync_seconds", 300)));
    // The spool restarts from each newer snapshot the publication gains (once the cache holds
    // it), so it stays within about one publication interval of ledgers. The new spool starts
    // no later than the ledger after the cache's last: the tail serves every ledger before it
    // from the pieces, not from the retired spool.
    std::thread([&, check = std::chrono::seconds(knob("rotate_seconds", 3600))] {
        bool stopped = false;  // the follower, left stopped by a rotation that left no spool
        for (;;) {
            std::this_thread::sleep_for(stopped ? std::chrono::seconds(30) : check);
            try {
                if (stopped) {
                    stopped = !restart();
                    continue;
                }
                // A rotation run by hand and stopped between its renames, say.
                recover();
                auto cached = cached_last();
                auto due = core::rotate_spool(spool, seed, sync.source, sync.publisher_key, false, {}, cached + 1);
                if (due.at("due").get<bool>()) {
                    // The follower keeps going while the new spool is built and caught up; it
                    // stops only for the last few ledgers and the swap.
                    std::unique_lock lock(follower_mutex, std::defer_lock);
                    auto pause = [&] { lock.lock(); follower->stop(); };
                    try { std::cerr << Json{{"event", "spool_rotated"}, {"result", core::rotate_spool(spool, seed, sync.source, sync.publisher_key, true, pause, cached + 1)}}.dump() << std::endl; }
                    catch (std::exception const& error) { std::cerr << Json{{"event", "spool_rotation_failed"}, {"error", error.what()}}.dump() << std::endl; }
                    // Restarted only if it was paused (a failure before the pause leaves it running),
                    // follower_mutex given up first (restart takes it only to start the follower).
                    if (lock.owns_lock()) { lock.unlock(); stopped = !restart(); }
                    if (stopped) continue;  // no spool: no retired one to clear yet
                }
                // A retired spool is kept until the cache holds the ledgers before the spool's
                // first (a spool rotated by hand may start past it), and a day after it was
                // retired: until then the tail serves those from the retired spool's chunks, and
                // a tail moves onto the cache's newer pieces only once its rebuild over them
                // succeeds (a day leaves it many tries).
                auto first = Json::parse(read_file(spool / "manifest.json", 16 << 20)).at("chunks").at(0).at("first").get<std::uint32_t>();
                bool covered = cached_last() + 1 >= first;
                for (auto const& entry : fs::directory_iterator(storage)) {
                    auto file = entry.path().filename().string();
                    if (!file.starts_with("spool.retired-")) continue;
                    auto retired = std::stoll(file.substr(std::string("spool.retired-").size()));
                    if (covered && std::time(nullptr) - retired > 24 * 3600) fs::remove_all(entry.path());
                }
            } catch (std::exception const& error) { std::cerr << Json{{"event", "spool_rotation_check_failed"}, {"error", error.what()}}.dump() << std::endl; }
        }
    }).detach();
    serve.tail = spool; serve.tail_key = spool / "publisher.pub";
    interim->stop(); interim.reset();  // its listeners are freed for the full server
    core::serve(serve);
    stopping = true;
    std::lock_guard lock(follower_mutex);
    leave(*follower);
}
#endif
}

int run_config(fs::path const& path) {
    auto config = toml::parse_file(path.string());
#ifndef _WIN32
    use_log(config);
#endif
    // Unknown names are usually typing mistakes; they are named, and otherwise ignored.
    for (auto const& setting : unknown_settings(config))
        std::cerr << Json{{"event", "config_unknown_setting"}, {"setting", setting}, {"reference", "/usr/share/xrpla/xrpla.example.toml"}}.dump() << std::endl;
    auto profile = required(config, "profile");
    if (profile == "restore") {
        core::RestoreOptions options;
        auto publication = publication_of(config);
        options.source = publication.source; options.publisher_key = publication.publisher_key;
        options.target = required(config, "restore.target"); options.output = required(config, "restore.out");
        options.first = ledger(config, "history.first"); options.last = ledger(config, "history.last");
        options.clio_host = text(config, "restore.clio_host"); options.clio_keyspace = text(config, "restore.clio_keyspace", "xrpla");
        options.clio_username = text(config, "restore.clio_username");
        if (auto port = integer(config, "restore.clio_port")) options.clio_port = static_cast<unsigned>(*port);
        options.clio_tls = flag_setting(config, "restore.clio_tls", false);
        std::cout << core::restore(options).dump(2) << '\n';
        return 0;
    }
    // The serving settings are checked now, not once the first sync (hours) is done.
    if (profile == "mirror" || profile == "connected" || profile == "standalone") serve_options(config, {}, {}, {});
    if (profile == "mirror") {
        auto publication = publication_of(config);
        core::SyncOptions sync{publication.source, publication.publisher_key, required(config, "storage.dir"), ledger(config, "history.first"), ledger(config, "history.last")};
        sync.indexes = index_options(config);
        sync.snapshots_every = snapshots_every(config);
        auto interval = std::chrono::seconds(integer(config, "mirror.sync_seconds").value_or(300));
        try { std::cerr << core::sync_pieces(sync).dump() << '\n'; }
        catch (std::exception const& error) { if (!serve_existing(sync.cache, error)) throw; }
        // Later generations are fetched and indexed in the background; the server switches to a
        // new selection when it appears.
        std::thread([sync, interval] {
            LockedSkips locked;
            for (;;) {
                std::this_thread::sleep_for(interval);
                try { std::cerr << core::sync_pieces(sync).dump() << '\n'; locked.ran(); core::release_freed_memory(); }
                catch (DirectoryLocked const&) { locked.skipped(sync.cache); }  // a sync run by hand holds the cache: this round is skipped
                catch (std::exception const& error) { locked.ran(); std::cerr << "{\"sync_failed\":" << Json(error.what()).dump() << "}\n"; }
            }
        }).detach();
        core::serve(serve_options(config, sync.cache, "127.0.0.1:5005", "127.0.0.1:6006"));
        return 0;
    }
#ifndef _WIN32
    // Connected with a publication, and standalone, take history from the publication.
    if (profile == "standalone" || (profile == "connected" && (config.at_path("publication.source") || config.at_path("publication.name"))))
        return run_mirrored(config, profile);
    if (profile == "connected") return run_connected(config);
#endif
    if (profile == "connected" || profile == "standalone")
        throw Error("The " + profile + " profile is not available in this build; use mirror to serve published history");
    if (profile == "publisher")
        throw Error("Run the publisher with tools/live_publisher.py (config/xrpla-publisher.service) in this release");
    throw Error("Unknown profile: " + profile);
}

namespace {
// `path` normal, without a trailing slash (but /).
std::string normal(fs::path const& path) {
    auto text = path.lexically_normal().string();
    while (text.size() > 1 && text.back() == '/') text.pop_back();
    return text;
}
// A path answer as the configuration records it: absolute (the service runs from /, and the unit
// names the same path) and normal, a leading ~ being HOME.
std::string absolute_path(std::string answer) {
    if (auto* home = std::getenv("HOME"); home && *home && (answer == "~" || answer.starts_with("~/"))) answer = home + answer.substr(1);
    return normal(fs::absolute(answer));
}
// Whether a publisher key answer is the key itself (64 hex characters) rather than its file.
bool hex_key(std::string const& answer) {
    return answer.size() == 64 && answer.find_first_not_of("0123456789abcdefABCDEF") == std::string::npos;
}
// Whether systemd can name `path` in a unit as it is. ExecStart= and ReadWritePaths= split at a
// space and take quotes, backslashes, $ and % (a specifier) as their own, so a path is taken
// only of the characters they leave alone (unit_rule).
constexpr char const* unit_rule = "a unit takes a path as it is only when made of ASCII letters, digits and . _ / @ + : -";
bool unit_safe(std::string const& path) {
    return !path.empty() && path.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/@+:-") == std::string::npos;
}
struct Prompt {
    std::istream& in; std::ostream& out;
    std::string ask(std::string const& question, std::string const& fallback = {}) {
        out << question << (fallback.empty() ? "" : " [" + fallback + "]") << ": " << std::flush;
        std::string line;
        if (!std::getline(in, line)) throw Error("Setup input ended");
        while (!line.empty() && (line.back() == '\r' || line.back() == ' ')) line.pop_back();
        return line.empty() ? fallback : line;
    }
    std::string choose(std::string const& question, std::vector<std::pair<std::string, std::string>> const& options, std::string const& fallback) {
        out << question << '\n';
        for (std::size_t i = 0; i < options.size(); ++i) out << "  " << i + 1 << ") " << options[i].first << " - " << options[i].second << '\n';
        for (;;) {
            auto answer = ask("Choose", fallback);
            for (std::size_t i = 0; i < options.size(); ++i) if (answer == options[i].first || answer == std::to_string(i + 1)) return options[i].first;
            out << "Please answer with a number or a name.\n";
        }
    }
    // A whole number from `low` to `high`, or one of `words` (returned as given), asked again
    // until the answer is one: a typo ("8GB", "-5") must not end the setup.
    std::string number(std::string const& question, std::string const& fallback, std::uint64_t low, std::uint64_t high,
                       std::vector<std::string> const& words = {}) {
        for (;;) {
            auto answer = ask(question, fallback);
            if (std::find(words.begin(), words.end(), answer) != words.end()) return answer;
            std::uint64_t value = 0;
            auto [end, ec] = std::from_chars(answer.data(), answer.data() + answer.size(), value);
            if (ec == std::errc{} && end == answer.data() + answer.size() && !answer.empty() && value >= low && value <= high) return answer;
            out << "Please answer with a whole number from " << low << " to " << high;
            for (auto const& word : words) out << ", or " << word;
            out << ".\n";
        }
    }
    std::int64_t integer(std::string const& question, std::string const& fallback, std::uint64_t low, std::uint64_t high) {
        return static_cast<std::int64_t>(std::stoull(number(question, fallback, low, high)));
    }
    bool yes(std::string const& question, bool fallback) {
        auto answer = ask(question + " (y/n)", fallback ? "y" : "n");
        return answer == "y" || answer == "Y" || answer == "yes";
    }
    // A directory (absolute_path), asked again until a systemd unit can name it.
    std::string directory(std::string const& question, std::string const& fallback) {
        for (;;) {
            auto path = absolute_path(ask(question, fallback));
            if (unit_safe(path)) return path;
            out << "A systemd unit cannot name " << path << " (" << unit_rule << "); please choose another.\n";
        }
    }
    // A publisher key: 64 hex characters as given, or its file as an absolute_path.
    std::string publisher_key() {
        auto answer = ask("Publisher key (file path, or 64 hex characters)");
        return answer.empty() || hex_key(answer) ? answer : absolute_path(answer);
    }
};
// Whether `path` is `dir` or under it (both normal).
bool within(std::string const& path, std::string const& dir) { return path == dir || path.starts_with(dir + "/"); }
// The account the service runs as: it parses public API and peer traffic, so not root.
constexpr char const* service_user = "xrpla";
// What the service writes: its data directory and its log file's directory (unless that is in
// the data directory). The unit lets it write these alone; the rest of the file system is
// read-only to it.
std::vector<fs::path> written_paths(toml::table const& config) {
    std::vector<fs::path> paths;
    if (auto dir = text(config, "storage.dir"); !dir.empty()) paths.push_back(normal(fs::absolute(dir)));
    if (auto file = text(config, "log.file"); !file.empty()) {
        auto dir = normal(fs::absolute(file).parent_path());
        if (paths.empty() || !within(dir, paths.front().string())) paths.push_back(dir);
    }
    return paths;
}
// The program the unit runs: the one setup runs as (installed under /usr/local/bin by default,
// not /usr/bin).
fs::path service_program() {
    std::error_code ec;
    auto program = fs::read_symlink("/proc/self/exe", ec).string();
    // The kernel's name for a program replaced or removed since it started (an upgrade during
    // setup): the unit names the path, which holds the replacement.
    if (constexpr std::string_view deleted = " (deleted)"; program.ends_with(deleted)) program.resize(program.size() - deleted.size());
    return ec || program.empty() ? fs::path("/usr/bin/xrpla") : fs::path(program);
}
// Whether `program` looks installed: under /usr (/usr/local included) and in no home directory
// (the invoking user's, or sudo's caller's). A build tree's binary is rebuilt, moved or removed
// under the service, and one in a home directory is behind ProtectHome.
bool installed_program(fs::path const& program) {
    auto path = program.lexically_normal().string();
    if (!path.starts_with("/usr/")) return false;
    std::vector<std::string> homes;
    if (auto* home = std::getenv("HOME"); home && *home) homes.emplace_back(home);
    if (auto* user = std::getenv("SUDO_USER"); user && *user)
        if (auto* account = getpwnam(user); account && account->pw_dir && *account->pw_dir) homes.emplace_back(account->pw_dir);
    for (auto home : homes) {
        while (home.size() > 1 && home.back() == '/') home.pop_back();
        if (home != "/" && (path == home || path.starts_with(home + "/"))) return false;
    }
    return true;
}
std::string unit(fs::path const& config, std::vector<fs::path> const& writes) {
    auto program = service_program();
    std::string writable;
    for (auto const& path : writes) writable += "ReadWritePaths=" + path.string() + "\n";
    // A dedicated account, sandboxed as the shipped units are; it may bind a port below 1024
    // (an HTTPS listener on 443) and nothing else root could.
    return "[Unit]\nDescription=XRPLA (" + config.string() + ")\nAfter=network-online.target\nWants=network-online.target\n\n"
           "[Service]\nType=simple\nUser=" + std::string(service_user) + "\nGroup=" + service_user + "\n"
           "ExecStart=" + program.string() + " run --config " + config.string() + "\nRestart=always\nRestartSec=30\n"
           "KillSignal=SIGTERM\nTimeoutStopSec=300\nLimitNOFILE=65536\n"
           "AmbientCapabilities=CAP_NET_BIND_SERVICE\nCapabilityBoundingSet=CAP_NET_BIND_SERVICE\nNoNewPrivileges=yes\n"
           "ProtectSystem=strict\nProtectHome=read-only\nPrivateTmp=yes\nRestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX\n" + writable +
           "\n[Install]\nWantedBy=multi-user.target\n";
}
// What a data directory of XRPLA's holds at its top (the profiles' layouts, a file of one being
// written, and the spool a rotation builds or retires beside the spool), so that one holding
// anything else is taken for another program's.
bool xrpla_layout(std::string const& name) {
    static std::set<std::string> const names{"cache", "spool", "spool.next", "follower", "keys", "bucket", "store", "restore", "pieces", "bundles", "snapshots",
                                             "fragments", "query", "catalog", "selection.json", ".verified", ".staging", ".xrpla-lock", ".xrpla.lock",
                                             "xrpla.log"};
    if (name.ends_with(".part")) return xrpla_layout(name.substr(0, name.size() - 5));
    return names.contains(name) || name.starts_with("spool.retired-") || name.starts_with("xrpla.log.");
}
// A directory never given to the account: one of the system's trees or in one, a directory in
// root's PATH or in one (a program the account put there would run as root), or a shared parent
// of other programs' directories.
bool system_directory(std::string const& path) {
    static std::set<std::string> const parents{"/", "/home", "/media", "/mnt", "/opt", "/root", "/srv", "/tmp", "/var", "/var/lib", "/var/log",
                                               "/var/cache", "/var/tmp", "/var/spool"};
    std::vector<std::string> trees{"/usr", "/etc", "/boot", "/dev", "/proc", "/sys", "/run", "/bin", "/sbin"};
    if (auto top = path.substr(0, path.find('/', 1)); top.starts_with("/lib")) trees.push_back(top);  // /lib, /lib64, /libx32, ...
    if (auto* search = std::getenv("PATH"))
        for (std::string_view rest = search; !rest.empty();) {
            auto entry = rest.substr(0, rest.find(':'));
            rest.remove_prefix(std::min(rest.size(), entry.size() + 1));
            if (entry.starts_with('/')) trees.push_back(normal(fs::path(entry)));
        }
    return parents.contains(path) || std::any_of(trees.begin(), trees.end(), [&](std::string const& tree) { return within(path, tree); });
}
// Closes a file descriptor when it goes.
struct Close { int fd; ~Close() { if (fd >= 0) ::close(fd); } };
// Whether the group `gid` may write a directory above one given to the account. A system group
// (below the login range, 1000) is held by system services rather than users (syslog's, which
// writes /var/log on Ubuntu), so it is trusted as root is, unless it names members (people are
// added to groups such as adm or sudo), is a login account's own (uid 1000 or above: an account
// whose group it is goes unlisted among its members, as with openSUSE's users) or is the
// account's own: the service writes as it.
bool trusted_group(gid_t gid) {
    if (gid >= 1000) return false;
    auto* group = ::getgrgid(gid);
    if (!group || (group->gr_mem && *group->gr_mem)) return false;
    if (auto* own = ::getgrnam(service_user); own && own->gr_gid == gid) return false;
    bool login = false;
    ::setpwent();
    while (auto* account = ::getpwent())
        if (account->pw_gid == gid && (account->pw_uid >= 1000 || (account->pw_name && std::strcmp(account->pw_name, service_user) == 0))) { login = true; break; }
    ::endpwent();
    return !login;
}
// Whether an account other than root and the system services may write the directory `fd`
// (`st`, its stat): any account may, or its group and that is no trusted_group. Under an access
// ACL the group bits are its mask, what its named users and groups and the owning group may do
// at most, so each entry that may write is judged: a named user unless root, a named group and
// the owning group as trusted_group judges them. An ACL not read whole is taken as shared.
bool shared_directory(int fd, struct stat const& st) {
    if (st.st_mode & S_IWOTH) return true;
    if (!(st.st_mode & S_IWGRP)) return false;
    auto size = ::fgetxattr(fd, "system.posix_acl_access", nullptr, 0);
    if (size < 0 && (errno == ENODATA || errno == ENOTSUP)) return !trusted_group(st.st_gid);
    // The kernel's form, little-endian: a version (2), then 8-byte entries of a tag, a permission
    // and an id.
    std::vector<unsigned char> acl(size > 0 ? static_cast<std::size_t>(size) : 0);
    if (size < 4 || (size - 4) % 8 != 0 || ::fgetxattr(fd, "system.posix_acl_access", acl.data(), acl.size()) != size) return true;
    auto field = [&](std::size_t at, int bytes) {
        std::uint32_t value = 0;
        for (int i = bytes; i-- > 0;) value = value << 8 | acl[at + i];
        return value;
    };
    if (field(0, 4) != 2) return true;
    constexpr std::uint32_t user = 0x02, owning_group = 0x04, group = 0x08, writes = 2;
    for (std::size_t at = 4; at < acl.size(); at += 8) {
        auto tag = field(at, 2), id = field(at + 4, 4);
        if (!(field(at + 2, 2) & writes)) continue;
        if ((tag == user && id != 0) || (tag == owning_group && !trusted_group(st.st_gid)) || (tag == group && !trusted_group(static_cast<gid_t>(id))))
            return true;
    }
    return false;
}
// Removes the POSIX ACLs of the directory `fd` (at `at`): its access ACL, through which other
// accounts may write it whatever its mode says, and its default ACL, which what is made in it
// inherits, the umask aside (one `setfacl -d` left on a parent, say). Without its access ACL its
// group bits are the owning group's again, no longer the mask. Each ACL is read first and removed
// only when there, so a directory without (a read-only mount's) is no error. Whether it had one.
bool remove_acls(int fd, std::string const& at) {
    bool had = false;
    for (auto name : {"system.posix_acl_access", "system.posix_acl_default"}) {
        if (::fgetxattr(fd, name, nullptr, 0) < 0) {
            int error = errno;
            if (error == ENODATA || error == ENOTSUP) continue;
            throw Error("Could not read the ACL of " + at + ": " + std::strerror(error));
        }
        if (::fremovexattr(fd, name) == 0) had = true;
        else if (int error = errno; error != ENODATA)  // ENODATA: gone meanwhile
            throw Error("Could not remove the ACL of " + at + ": " + std::strerror(error));
    }
    return had;
}
// Refuses `path` when `at`, open as `fd` (`st`, its stat; `path` itself or a directory above
// it), is where owners and modes do not decide who may write: on a network or cluster file system
// (NFS, SMB, 9p, Ceph, AFS, Lustre, OCFS2, GFS2) or a FUSE one, where a server, other machines or
// a daemon do whatever setup reads here, or with an ACL of
// a kind setup does not read, which may let any account write whatever the mode says: an NFSv4
// ACL (an NFS mount's, or an OpenZFS dataset's with acltype=nfsv4) or a CIFS mount's Windows one.
// A sticky directory above `path` is not asked for its ACL: another account writing it cannot
// move root's entries.
void plain_permissions(int fd, struct stat const& st, fs::path const& path, fs::path const& at) {
    auto refuse = [&](std::string const& why) {
        throw Error((at == path ? path.string() + " " : path.string() + " is in " + at.string() + ", which ") + why + "; not giving it to " + service_user +
                    ". Choose a directory on a local file system");
    };
    // linux/magic.h's NFS_SUPER_MAGIC, SMB_SUPER_MAGIC, CIFS_SUPER_MAGIC and SMB2_SUPER_MAGIC
    // (the last two missing from older versions of it), V9FS_MAGIC, CEPH_SUPER_MAGIC,
    // AFS_SUPER_MAGIC and AFS_FS_MAGIC (OpenAFS's and the kernel's), and FUSE_SUPER_MAGIC (sshfs,
    // GlusterFS, mergerfs, virtiofs, ...); Lustre's LL_SUPER_MAGIC and the cluster file systems
    // OCFS2_SUPER_MAGIC and GFS2_MAGIC, which other machines mount and write too.
    constexpr std::uint32_t network[]{0x6969,     0x517B,     0xFF534D42, 0xFE534D42, 0x01021997, 0x00C36400,
                                      0x5346414F, 0x6B414653, 0x65735546, 0x0BD00BD0, 0x7461636F, 0x01161970};
    struct statfs mounted {};
    if (::fstatfs(fd, &mounted) != 0) throw Error("Cannot read the file system of " + at.string());
    if (std::find(std::begin(network), std::end(network), static_cast<std::uint32_t>(mounted.f_type)) != std::end(network))
        refuse("is on a network, cluster or FUSE file system (NFS, SMB, 9p, Ceph, AFS, Lustre, OCFS2, GFS2, sshfs, ...), whose server, other machines or "
               "daemon decide who may write it, not the owners and modes setup reads");
    if (at != path && (st.st_mode & S_ISVTX)) return;
    for (auto name : {"system.nfs4_acl", "system.nfs4_acl_xdr", "system.cifs_acl"})
        if (::fgetxattr(fd, name, nullptr, 0) >= 0)
            refuse("has an NFSv4 or Windows ACL (setup does not read those, so cannot tell who may write it)");
}
// Opens the directory `path` (absolute and normal) from /, a component at a time: each step
// O_NOFOLLOW, so no link leads elsewhere, and each directory above it root's and written by root
// alone, or by a trusted_group as well and through its ACL by no other account (or sticky, as /tmp is), as another account could move what
// one it owns or writes holds while setup works; it and they on a local file system, with no ACL
// setup does not read (plain_permissions). With `make`, the missing ones are made (0755, whatever
// root's umask, and without the ACLs they inherit); without, -1 at the first one missing.
int open_directory(fs::path const& path, bool make) {
    int fd = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) throw Error("Cannot open /");
    fs::path at = "/";
    for (auto const& part : path.relative_path()) {
        Close parent{fd};
        struct stat st {};
        if (::fstat(fd, &st) != 0) throw Error("Cannot read " + at.string());
        if (st.st_uid != 0)
            throw Error(path.string() + " is in " + at.string() + ", which is not root's; not giving it to " + service_user + ". Choose a directory whose parents are root's");
        plain_permissions(fd, st, path, at);
        if (shared_directory(fd, st) && !(st.st_mode & S_ISVTX))
            throw Error(path.string() + " is in " + at.string() + ", which another account may write (for all, for a group not a system service's, or through an ACL); not giving it to " +
                        service_user + ". Choose a directory whose parents only root and system services write (chmod go-w " + at.string() + ")");
        auto name = part.string();
        bool made = make && ::mkdirat(fd, name.c_str(), 0755) == 0;
        if (make && !made && errno != EEXIST) throw Error("Cannot create " + (at / name).string() + ": " + std::strerror(errno));
        fd = ::openat(fd, name.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        at /= name;
        if (fd < 0 && !make && errno == ENOENT) return -1;
        if (fd < 0) throw Error(at.string() + " is a symbolic link or not a directory; not giving " + path.string() + " to " + service_user + ". Name the directory itself");
        if (made) {
            // A directory made here takes its parent's default ACL as its access ACL and its own
            // default, through which another account could write what the service makes in it.
            Close fresh{fd};
            remove_acls(fd, at.string());
            if (::fchmod(fd, 0755) != 0) throw Error("Cannot set the mode of " + at.string());
            fresh.fd = -1;
        }
    }
    Close opened{fd};
    struct stat st {};
    if (::fstat(fd, &st) != 0) throw Error("Cannot read " + path.string());
    plain_permissions(fd, st, path, path);
    return std::exchange(opened.fd, -1);
}
// The entries of the directory `fd`, by name (no "." or "..").
std::vector<std::string> entries(int fd) {
    std::vector<std::string> names;
    int copy = ::dup(fd);
    if (copy < 0) throw Error("Cannot read a directory");
    DIR* dir = ::fdopendir(copy);
    if (!dir) { ::close(copy); throw Error("Cannot read a directory"); }
    while (auto* entry = ::readdir(dir)) {
        std::string name = entry->d_name;
        if (name != "." && name != "..") names.push_back(name);
    }
    ::closedir(dir);
    return names;
}
// `text` quoted for a shell: in single quotes, each single quote in it as '\''.
std::string shell_quoted(std::string const& text) {
    std::string quoted = "'";
    for (char c : text) quoted += c == '\'' ? std::string("'\\''") : std::string(1, c);
    return quoted + "'";
}
// `text` with each byte that is not printable ASCII (a control character, a terminal's escape
// sequence, anything beyond ASCII) and each backslash as \xHH, so that a name in a directory, of
// whoever made it's choosing, prints as what it is.
std::string printable(std::string const& text) {
    constexpr char hex[] = "0123456789abcdef";
    std::string shown;
    for (unsigned char c : text) {
        if (c >= 0x20 && c < 0x7f && c != '\\') shown += static_cast<char>(c);
        else shown += std::string("\\x") + hex[c >> 4] + hex[c & 15];
    }
    return shown;
}
// Whether the file `fd` (at `at`) has a POSIX ACL: an access ACL, through which other accounts may
// reach it whatever its mode says, or a default one, which what is made in it inherits.
bool has_acls(int fd, fs::path const& at) {
    for (auto name : {"system.posix_acl_access", "system.posix_acl_default"}) {
        if (::fgetxattr(fd, name, nullptr, 0) >= 0) return true;
        if (int error = errno; error != ENODATA && error != ENOTSUP)
            throw Error("Could not read the ACL of " + printable(at.string()) + ": " + std::strerror(error));
    }
    return false;
}
// The id of the mount the entry `name` of the directory `fd` is on (`fd`'s own for ""), which
// tells a bind mount from the file system it is of when the device is the same: statx's (Linux
// 5.8), else /proc's for a descriptor of it opened O_PATH, which opens nothing. None when neither
// can be read.
std::optional<std::uint64_t> mount_id(int fd, std::string const& name) {
#ifdef STATX_MNT_ID
    struct statx sx {};
    if (::statx(fd, name.c_str(), AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT | (name.empty() ? AT_EMPTY_PATH : 0), STATX_MNT_ID, &sx) == 0 && (sx.stx_mask & STATX_MNT_ID))
        return sx.stx_mnt_id;
#endif
    Close entry{name.empty() ? ::fcntl(fd, F_DUPFD_CLOEXEC, 0) : ::openat(fd, name.c_str(), O_PATH | O_NOFOLLOW | O_CLOEXEC)};
    if (entry.fd < 0) return std::nullopt;
    std::ifstream info("/proc/self/fdinfo/" + std::to_string(entry.fd));
    for (std::string line; std::getline(info, line);) {
        if (!line.starts_with("mnt_id:")) continue;
        auto digits = line.find_first_not_of(" \t", 7);
        std::uint64_t id = 0;
        if (digits == std::string::npos) return std::nullopt;
        if (std::from_chars(line.data() + digits, line.data() + line.size(), id).ec == std::errc()) return id;
        return std::nullopt;
    }
    return std::nullopt;
}
// Whether the directory `fd` (`st` its status) is where a file system or bind mount is mounted:
// its parent is on another device or mount. Not when that cannot be read.
bool mount_root(int fd, struct stat const& st) {
    struct stat up {};
    if (::fstatat(fd, "..", &up, AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT) != 0) return false;
    if (up.st_dev != st.st_dev) return true;
    auto here = mount_id(fd, ""), parent = mount_id(fd, "..");
    return here && parent && *here != *parent;
}
// What foreign_entries finds in a directory to be given to the account: of each kind, how many
// and the first few (printable paths, some with a note).
struct Foreign {
    enum Kind { mounted, device, other_user, hard_linked, root_owned, acl, writable, kinds };
    static constexpr std::size_t shown = 3;
    std::size_t count[kinds]{};
    std::vector<std::string> paths[kinds];
    void add(Kind kind, std::string path) {
        if (count[kind]++ < shown) paths[kind].push_back(std::move(path));
    }
    bool any() const { return std::any_of(std::begin(count), std::end(count), [](std::size_t n) { return n != 0; }); }
    // Whether what was found is all what a run of XRPLA's as root leaves (root's ordinary entries,
    // and ACLs and group or other write inherited), which find -xdev reaches whole and setup
    // can name the commands for; not anything on another mount, a device node, another user's or a
    // file linked elsewhere, which those commands would hand over or change.
    bool fixable() const { return !count[mounted] && !count[device] && !count[other_user] && !count[hard_linked]; }
};
// A walk of the directory `top` (open, its device `dev` and mount, read when first needed) for
// what the account `uid` (none yet: nothing is its) is not given by being given the directory.
struct Walk {
    int top;
    dev_t dev;
    std::optional<std::uint64_t> mount;
    std::optional<uid_t> uid;
    Foreign found;
};
// Adds to `walk` each of the entries `names` of the directory `fd` (at `at`), and of what they
// hold, that the account does not own, a device node, or a directory or regular file with a POSIX
// ACL or group or other write; each on another mount than the top's (its device or, for a bind
// mount of the same device, its mount id differs) as on one. It only reads: each step relative to
// the open directory, O_NOFOLLOW and AT_SYMLINK_NOFOLLOW, so no link leads elsewhere, a regular
// file opened for reading alone (O_NONBLOCK and O_NOCTTY, should it have become another kind)
// to read its ACLs, each with O_NOATIME where the file system takes it, so reading a directory
// leaves its access time as it was, and an entry gone meanwhile passed over. An entry on another
// device (a file system mounted there, a btrfs subvolume) is judged by its own owner and mode and not entered,
// as find -xdev enters none; a bind mount of the top's device is entered, as find -xdev enters
// it too. A directory is held open per level, so the depth is held to well past XRPLA's layouts'.
constexpr int walk_depth = 32;
void foreign_entries(Walk& walk, int fd, fs::path const& at, std::vector<std::string> const& names, int depth = 0) {
    for (auto const& name : names) {
        auto path = at / name;
        struct stat st {};
        if (::fstatat(fd, name.c_str(), &st, AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT) != 0) {
            if (errno == ENOENT) continue;
            throw Error("Cannot read " + printable(path.string()));
        }
        bool directory = S_ISDIR(st.st_mode), file = S_ISREG(st.st_mode), here = st.st_dev == walk.dev;
        Close opened{-1};
        if ((directory || file) && here) {
            int flags = O_RDONLY | O_NOFOLLOW | O_CLOEXEC | (directory ? O_DIRECTORY : O_NONBLOCK | O_NOCTTY);
            opened.fd = ::openat(fd, name.c_str(), flags | O_NOATIME);
            if (opened.fd < 0 && (errno == EPERM || errno == EINVAL)) opened.fd = ::openat(fd, name.c_str(), flags);
            if (opened.fd < 0) {
                if (errno == ENOENT) continue;
                throw Error("Cannot open " + printable(path.string()));
            }
            struct stat now {};
            if (::fstat(opened.fd, &now) != 0 || now.st_dev != st.st_dev || now.st_ino != st.st_ino)
                throw Error(printable(path.string()) + " changed while setup read it; run setup again");
        }
        bool owned = walk.uid && st.st_uid == *walk.uid, device = S_ISCHR(st.st_mode) || S_ISBLK(st.st_mode);
        bool acl = opened.fd >= 0 && has_acls(opened.fd, path);
        bool writable = (directory || file) && (st.st_mode & (S_IWGRP | S_IWOTH));
        if (!owned || device || acl || writable) {
            auto shown = printable(path.string());
            if (here) {
                if (!walk.mount) walk.mount = mount_id(walk.top, "");
                auto mount = opened.fd >= 0 ? mount_id(opened.fd, "") : mount_id(fd, name);
                if (!walk.mount || !mount) throw Error("Cannot tell which mount " + shown + " is on");
                here = *mount == *walk.mount;
            }
            if (!here) walk.found.add(Foreign::mounted, shown);
            else {
                if (device) walk.found.add(Foreign::device, shown);
                else if (!owned && st.st_uid != 0) walk.found.add(Foreign::other_user, shown + " (uid " + std::to_string(st.st_uid) + ")");
                else if (!owned && !directory && st.st_nlink > 1) walk.found.add(Foreign::hard_linked, shown + " (" + std::to_string(st.st_nlink) + " links)");
                else if (!owned) walk.found.add(Foreign::root_owned, shown);
                if (acl) walk.found.add(Foreign::acl, shown);
                if (writable) walk.found.add(Foreign::writable, shown);
            }
        }
        if (!directory || st.st_dev != walk.dev) continue;
        if (depth + 1 == walk_depth)
            throw Error(printable(path.string()) + " is nested deeper than " + std::to_string(walk_depth) + " directories, which no layout of XRPLA's is; not giving it to " +
                        service_user);
        foreign_entries(walk, opened.fd, path, entries(opened.fd), depth + 1);
    }
}
// `text` as a find -path pattern matching it alone: each of its wildcards (* ? [ ]) and
// backslashes escaped.
std::string find_pattern(std::string const& text) {
    std::string pattern;
    for (char c : text) {
        if (c == '*' || c == '?' || c == '[' || c == ']' || c == '\\') pattern += '\\';
        pattern += c;
    }
    return pattern;
}
// Whether the kernel lets an account hard link only a file it owns or may read and write
// (fs.protected_hardlinks): not when it cannot be read.
bool protected_hardlinks() {
    std::ifstream setting("/proc/sys/fs/protected_hardlinks");
    std::string value;
    return std::getline(setting, value) && value == "1";
}
// Refuses the directory `path` for what `found` holds: naming it, and when it is all what a run as
// root leaves (Foreign::fixable), the commands that give it to the account and take away the ACLs
// and group and other write, preceded by the useradd setup runs when there is no `account` yet
// (`top_given`: the directory itself is the account's already); a file system's `lost_found` at
// its top is left out, as setup leaves it. Each runs find -xdev, which crosses into no other file
// system, and -execdir, which acts on each entry from its directory, so no directory moved
// meanwhile leads elsewhere. Only chown runs as root, and only on root's entries (-uid 0): another
// user's file put in the tree since it was read stays theirs, and the next setup refuses it; -h
// changes a link itself, a file with other hard links is left out, and with fs.protected_hardlinks
// no one who may write in the tree can link a file of root's there meanwhile (without it, no command
// is named). setfacl and chmod, which follow a link an entry is replaced with meanwhile, run as
// the account after it, so one leading out of the tree reaches only what the account may change;
// from /, as the account's find must return to the directory it started in. They change no file
// already open: whatever writes in the tree is stopped first.
[[noreturn]] void refuse_foreign(fs::path const& path, Foreign const& found, bool account, bool top_given, bool lost_found) {
    constexpr std::pair<Foreign::Kind, char const*> kinds[]{{Foreign::mounted, "on another file system or bind mount, mounted inside it"},
                                                            {Foreign::device, "a device node"},
                                                            {Foreign::other_user, "another user's"},
                                                            {Foreign::hard_linked, "a file with other hard links, perhaps outside it"},
                                                            {Foreign::root_owned, "root's"},
                                                            {Foreign::acl, "with a POSIX ACL"},
                                                            {Foreign::writable, "writable by its group or by all"}};
    std::string what;
    for (auto [kind, description] : kinds) {
        if (!found.count[kind]) continue;
        what += (what.empty() ? "" : "; ") + std::string(description) + ": ";
        for (std::size_t i = 0; i < found.paths[kind].size(); ++i) what += (i ? ", " : "") + found.paths[kind][i];
        if (found.count[kind] > found.paths[kind].size()) what += " and " + std::to_string(found.count[kind] - found.paths[kind].size()) + " more";
    }
    auto message = path.string() + " holds what is not " + service_user + "'s alone (" + what + "), and setup gives the account the directory, not what is in it; not giving it to " +
                   service_user + ". ";
    if (!found.fixable())
        throw Error(message + "Setup names no command for this, as one would hand over or change what is not XRPLA's: move that out of " + path.string() +
                    " (or unmount it), then run setup again");
    if (found.count[Foreign::root_owned] && !protected_hardlinks())
        throw Error(message + "Setup names no chown for root's entries while fs.protected_hardlinks is off: as it runs, an account that may write in " + path.string() +
                    " could put there a hard link to a file of root's elsewhere (/etc/shadow, say), which chown would give to " + service_user +
                    ". Turn it on (sysctl -w fs.protected_hardlinks=1, and in /etc/sysctl.d to keep it) or move root's entries out of " + path.string() +
                    ", then run setup again");
    // The account's commands leave out the directory itself, which setup puts right when it gives
    // it, and need to enter it: root's own chown gives it to the account first, or a plain chown -h
    // when nothing in it is root's (its parents are root's alone, so it is the one setup read).
    std::string fix;
    auto each = [&](std::string const& as, std::string const& options, std::string const& test, std::string const& command) {
        auto skip = lost_found ? "-path " + shell_quoted(find_pattern((path / "lost+found").string())) + " -prune -o " : std::string();
        fix += (fix.empty() ? "" : " && ") + as + "find " + shell_quoted(path.string()) + " " + options + " " + skip + test + " -execdir " + command + " {} +";
    };
    auto as_account = "runuser -u " + std::string(service_user) + " -- ";
    if (found.count[Foreign::root_owned]) each("", "-xdev", "-uid 0 \\( -type d -o -links 1 \\)", "chown -h " + std::string(service_user) + ":");
    else if (!top_given && (found.count[Foreign::acl] || found.count[Foreign::writable])) fix = "chown -h " + std::string(service_user) + ": " + shell_quoted(path.string());
    if (found.count[Foreign::acl]) each(as_account, "-mindepth 1 -xdev", "\\( -type d -o -type f \\)", "setfacl -b");
    if (found.count[Foreign::writable]) each(as_account, "-mindepth 1 -xdev", "\\( -type d -o -type f \\)", "chmod go-w");
    if (!account && found.count[Foreign::root_owned])
        fix = "useradd --system --user-group --no-create-home --shell /usr/sbin/nologin " + std::string(service_user) + " && " + fix;
    // From /: the account's find could not return to root's working directory (/root, say).
    if (found.count[Foreign::acl] || found.count[Foreign::writable]) fix = "cd / && " + fix;
    throw Error(message + "If it is XRPLA's (left by a run as root, say), stop whatever writes in " + path.string() +
                " first (a file opened before the commands keeps the access it was opened with), then run: " + fix + ", then run setup again");
}
// Opens a directory the service writes, once it is found XRPLA's to give (see give): -1 when it is
// missing and not `make`. `uid` is the account's (none before it is made).
template<class Fits> int given_directory(fs::path const& path, Fits const& fits, std::optional<uid_t> uid, bool make) {
    auto refuse = [&](std::string const& why) { throw Error(path.string() + " " + why + "; not giving it to " + service_user); };
    // A unit with DynamicUser=yes keeps its StateDirectory= under /var/lib/private, behind a link
    // from /var/lib (the shipped xrpla-serve and xrpla-tail units: /var/lib/xrpla). That state is
    // systemd's to own and the other unit's to use.
    std::error_code ec;
    if (auto canonical = normal(fs::weakly_canonical(path, ec)); !ec && within(canonical, "/var/lib/private"))
        throw Error(path.string() + " is " + canonical + ", the state directory of a unit with DynamicUser=yes (xrpla-serve.service or xrpla-tail.service?); "
                    "not giving it to " + service_user + ". Choose another storage directory, or stop and remove that unit and its state first");
    // Checked by name: open_directory follows no link, so the directory it opens is this one.
    if (within(path.string(), "/tmp") || within(path.string(), "/var/tmp"))
        refuse("is in /tmp or /var/tmp, where the service sees only an empty directory of its own (PrivateTmp=yes)");
    if (system_directory(path.string())) refuse("is a system directory, in one, or in root's PATH");
    Close dir{open_directory(path, make)};
    if (dir.fd < 0) return -1;
    struct stat st {};
    if (::fstat(dir.fd, &st) != 0) throw Error("Cannot read " + path.string());
    if (st.st_uid != 0 && (!uid || st.st_uid != *uid)) refuse("belongs to another user");
    auto top = entries(dir.fd);
    bool lost_found = false;
    if (auto at = std::find(top.begin(), top.end(), "lost+found"); at != top.end() && mount_root(dir.fd, st)) {
        struct stat lost {};
        if (::fstatat(dir.fd, "lost+found", &lost, AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT) == 0 && S_ISDIR(lost.st_mode) && lost.st_uid == 0 &&
            lost.st_dev == st.st_dev) {
            top.erase(at);
            lost_found = true;
        }
    }
    for (auto const& name : top)
        if (!fits(name))
            throw Error(path.string() + " holds " + printable(name) + ", which is not XRPLA's; not giving it to " + service_user +
                        ". Choose a new or empty directory, or one XRPLA alone uses");
    // What it holds is left as it is, so the account must own all of it already, and it alone
    // write it, for the service to write there: a new or empty directory, or one the service has
    // written.
    Walk walk{dir.fd, st.st_dev, std::nullopt, uid, {}};
    foreign_entries(walk, dir.fd, path, top);
    if (walk.found.any()) refuse_foreign(path, walk.found, uid.has_value(), uid && st.st_uid == *uid, lost_found);
    return std::exchange(dir.fd, -1);
}
// Gives a directory the service writes to the account: the directory itself, never what is in it.
// Only one that is XRPLA's to give: a system directory (system_directory) or one in /tmp, one
// under a directory not root's or another account may write, one another user owns, one holding
// anything at its top that `fits` does not take for XRPLA's, or one holding anything the account
// does not own already or others may write (foreign_entries) is refused. The lost+found of a
// file system mounted at the directory (root's, a directory on its device) is neither checked nor
// given; one elsewhere is any entry. The directory loses its POSIX ACLs (remove_acls) and group
// and other write (one left at 0777 by a run as root, say), keeping its setgid, from which what is made in
// it takes its group: its owner alone writes it. Nothing already as it should be is changed, so a
// re-run with nothing left to change passes on a read-only mount.
template<class Fits> void give(fs::path const& path, Fits const& fits, uid_t uid, gid_t gid, std::ostream& out) {
    Close dir{given_directory(path, fits, uid, true)};
    struct stat st {};
    if (::fstat(dir.fd, &st) != 0) throw Error("Cannot read " + path.string());
    if (remove_acls(dir.fd, path.string())) out << "Removed the ACLs of " << path.string() << ", through which other accounts could write it.\n";
    if (auto mode = static_cast<mode_t>(st.st_mode & 07777 & ~(S_IWGRP | S_IWOTH)); mode != (st.st_mode & 07777)) {
        if (::fchmod(dir.fd, mode) != 0) throw Error("Could not make " + path.string() + " writable by its owner alone: " + std::strerror(errno));
        out << "Made " << path.string() << " writable by its owner alone.\n";
    }
    if ((st.st_uid != uid || st.st_gid != gid) && ::fchown(dir.fd, uid, gid) != 0) throw Error("Could not give " + path.string() + " to " + service_user);
    out << path.string() << " belongs to " << service_user << ".\n";
}
// The service's account (a system user and group, made when missing) owning the directories it
// writes, and already what is in them. Every directory is checked before the account is made or
// any is given, so a refusal leaves each as it was.
void service_account(toml::table const& config, std::ostream& out) {
    auto paths = written_paths(config);
    auto storage = text(config, "storage.dir");
    if (!storage.empty()) storage = normal(fs::absolute(storage));
    fs::path log = text(config, "log.file");
    auto log_dir = log.empty() ? std::string() : normal(fs::absolute(log).parent_path());
    auto log_name = log.filename().string();
    if (!log_dir.empty() && !storage.empty() && log_dir != storage && within(storage, log_dir))
        throw Error("The storage directory " + storage + " is in " + log_dir + ", the [log] file's directory; not giving them to " + service_user +
                    ". Set [log] file to one in the storage directory or in a directory of its own, such as /var/log/xrpla/xrpla.log");
    if (!log_dir.empty() && (storage.empty() || !within(log_dir, storage)) && system_directory(log_dir))
        throw Error("The [log] file " + log.string() + " is in " + log_dir + ", a directory not XRPLA's alone; not giving it to " + service_user +
                    ". Set [log] file to one in a directory of its own (such as /var/log/xrpla/xrpla.log) or in the storage directory, and run setup again");
    // The log file's directory holds it and its older files (FILE.1, ...), XRPLA's too, and the
    // storage directory the directory it is in when it is further down (STORAGE/log/xrpla.log).
    auto fits = [&](fs::path const& path) {
        return [&, path](std::string const& name) {
            if (xrpla_layout(name)) return true;
            if (log_dir.empty()) return false;
            if (path.string() == log_dir) return name == log_name || name.starts_with(log_name + ".");
            return within(log_dir, path.string()) && name == fs::path(log_dir).lexically_relative(path).begin()->string();
        };
    };
    // systemd's range for DynamicUser=yes: an account named so only while such a unit runs (the
    // shipped xrpla-serve and xrpla-tail units, as xrpla), its uid perhaps another's next time.
    auto lent = [&](passwd const* account) {
        if (account && account->pw_uid >= 61184 && account->pw_uid <= 65519)
            throw Error(std::string("The ") + service_user + " account is uid " + std::to_string(account->pw_uid) + ", one systemd lends a running unit with DynamicUser=yes "
                        "(xrpla-serve.service or xrpla-tail.service?). Stop that unit and run setup again, which creates a static " + service_user + " account");
    };
    // The account's ids, copied at once: the next lookup (trusted_group's) overwrites getpwnam's
    // answer.
    auto lookup = [&]() -> std::optional<std::pair<uid_t, gid_t>> {
        auto* account = getpwnam(service_user);
        lent(account);
        if (!account) return std::nullopt;
        return std::pair{account->pw_uid, account->pw_gid};
    };
    auto account = lookup();
    // Without the account yet, only a new or empty directory passes: nothing in one is its.
    std::optional<uid_t> owner;
    if (account) owner = account->first;
    for (auto const& path : paths) {
        Close checked{given_directory(path, fits(path), owner, false)};
    }
    if (!account) {
        pid_t pid = fork();
        if (pid == 0) {
            execlp("useradd", "useradd", "--system", "--user-group", "--no-create-home", "--shell", "/usr/sbin/nologin", service_user, static_cast<char*>(nullptr));
            _exit(127);
        }
        int status = 0;
        if (pid < 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0 || !(account = lookup()))
            throw Error(std::string("Could not create the ") + service_user + " account (useradd)");
        out << "Created the system account " << service_user << ".\n";
    }
    auto [uid, gid] = *account;
    for (auto const& path : paths) give(path, fits(path), uid, gid, out);
}
}

namespace {
void write_config(fs::path const& config_path, toml::table& config, std::ostream& out) {
    fs::create_directories(config_path.parent_path().empty() ? fs::path(".") : config_path.parent_path());
    // A publisher key given as 64 hex characters is kept in a file next to the configuration,
    // written with it: after the service's directories are given, so not when one is refused.
    // It is named absolute, as the service runs from /.
    if (auto key = text(config, "publication.publisher_key"); hex_key(key)) {
        auto file = fs::path(normal(fs::absolute(config_path).parent_path())) / "publisher.pub";
        write_file(file, key + "\n");
        config["publication"].as_table()->insert_or_assign("publisher_key", file.string());
    }
    // The configuration reference with this machine's choices filled in, so the file documents
    // every setting; kept only if it reads back as exactly those choices.
    std::string text;
    try {
        text = "# Written by `xrpla setup`; every setting is described below (docs/SETUP.md).\n\n" + documented_config(config);
        if (toml::parse(text) != config) text.clear();
    } catch (std::exception const&) { text.clear(); }
    if (text.empty()) { std::ostringstream plain; plain << "# Written by `xrpla setup`; see docs/SETUP.md.\n" << config << '\n'; text = plain.str(); }
    write_file(config_path, text);
    out << "\nWrote " << config_path.string() << '\n';
}
void install_unit(fs::path const& config_path, toml::table const& config, fs::path const& unit_dir, bool system_unit, std::ostream& out) {
    auto path = unit_dir / "xrpla.service";
    auto writes = written_paths(config);
    try {
        fs::create_directories(unit_dir);
        write_file(path, unit(fs::absolute(config_path), writes));
    } catch (std::exception const&) {
        out << "Could not write " << path.string() << " (run setup as root to install the service). Run `xrpla run --config " << config_path.string() << "` meanwhile.\n";
        return;
    }
    out << "Wrote " << path.string() << " (it runs as the " << service_user << " account).\n";
    if (auto program = service_program(); !installed_program(program))
        out << "Warning: its ExecStart is " << program.string() << ", the program setup ran as, which is not an installed one (under /usr, outside a home directory). "
               "Install xrpla (cmake --install) and run setup again, or edit ExecStart= in " << path.string() << ".\n";
    if (!system_unit) {
        std::string given;
        for (auto const& write : writes) given += (given.empty() ? "" : " and ") + write.string();
        out << "Before starting it, create the " << service_user << " account and give it " << (given.empty() ? std::string("the data directory") : given) << ".\n";
    }
    if (auto key = text(config, "serve.tls_key"); !key.empty()) out << "The " << service_user << " account must be able to read " << key << ".\n";
    out << "Start it with: systemctl daemon-reload && systemctl enable --now xrpla\n";
}
// The configuration and the unit that runs it. When setup installs the unit itself (as root,
// into /etc/systemd/system), the account is made and given what the service writes first, so a
// directory refused leaves nothing written; a unit written elsewhere is installed by whoever
// moves it.
void install_service(fs::path const& config_path, toml::table& config, fs::path const& unit_dir, std::ostream& out) {
    std::error_code same;
    bool system_unit = fs::equivalent(unit_dir, "/etc/systemd/system", same) && ::geteuid() == 0;
    if (system_unit) service_account(config, out);
    write_config(config_path, config, out);
    install_unit(config_path, config, unit_dir, system_unit, out);
}
// The [log] table of the configuration setup replaces, kept: the rest is asked afresh, but a log
// file set by hand would be lost, and with it its directory from the unit's writable paths.
void keep_log(fs::path const& config_path, toml::table& config, std::ostream& out) {
    std::error_code ec;
    if (!fs::is_regular_file(config_path, ec)) return;
    try {
        auto existing = toml::parse_file(config_path.string());
        auto* log = existing["log"].as_table();
        if (!log) return;
        toml::table kept;
        kept.insert("log", *log);
        if (auto file = text(kept, "log.file"); !file.empty()) {
            // The service runs from /, and setup from wherever it was started: a relative file is
            // not taken for either.
            if (!fs::path(file).is_absolute()) {
                out << "Not keeping [log] from " << config_path.string() << ": its file " << file << " is not an absolute path.\n";
                return;
            }
            if (!unit_safe(normal(file))) {
                out << "Not keeping [log] from " << config_path.string() << ": a systemd unit cannot name " << normal(file) << " (" << unit_rule << ").\n";
                return;
            }
            kept["log"].as_table()->insert_or_assign("file", normal(file));
        }
        config.insert("log", *kept["log"].as_table());
        out << "Keeping [log] from " << config_path.string() << ".\n";
    } catch (std::exception const& error) {
        out << "Not keeping [log] from " << config_path.string() << " (" << error.what() << ").\n";
    }
}
// The approved list as setup offers it. One that cannot be read is named, and setup goes on
// without it (a publication is then asked for by its URL and key). One named by
// XRPLA_PUBLICATIONS is offered, with a warning: the service reads the installed list.
PublicationList offered_publications(Prompt& p) {
    PublicationList list;
    try { list = installed_publications(); }
    catch (std::exception const& error) {
        p.out << "Not offering the publication list: " << error.what() << ". A publication is asked for by its URL and key.\n";
        return {};
    }
    if (auto* named = std::getenv("XRPLA_PUBLICATIONS"); named && *named)
        p.out << "Warning: XRPLA_PUBLICATIONS names " << list.file.string() << ", which the service will not read: it reads the publication list "
                 "installed with xrpla. A publication chosen here by name must be in that list too (or choose other).\n";
    for (auto const& [entry, why] : list.invalid) p.out << "Not offering " << entry << " from " << list.file.string() << ": " << why << '\n';
    return list;
}
// The network, asked when the list has publications of one other than mainnet; written to
// [network] id unless it is mainnet.
std::string ask_network(Prompt& p, PublicationList const& list, toml::table& config) {
    std::vector<std::pair<std::string, std::string>> networks{{"mainnet", "the XRP Ledger"}};
    for (auto [network, about] : {std::pair{"testnet", "the XRP Ledger's test network"}, std::pair{"devnet", "the XRP Ledger's development network"}})
        if (std::any_of(list.entries.begin(), list.entries.end(), [&](Publication const& e) { return e.network == network; })) networks.emplace_back(network, about);
    auto network = networks.size() == 1 ? std::string("mainnet") : p.choose("Which network?", networks, "mainnet");
    if (network != "mainnet") { toml::table id; id.insert("id", network); config.insert("network", id); }
    return network;
}
// [publication]: one of the list's publications for `network`, written by name so that a later
// package's list (its mirrors updated) applies; or another, by its URL and publisher key, as
// asked when the list offers none. With `optional` (connected), none at all: false then.
bool ask_publication(Prompt& p, PublicationList const& list, std::string const& network, toml::table& config, bool optional) {
    std::vector<std::pair<std::string, std::string>> options;
    for (auto const& e : list.entries) {
        if (e.network != network) continue;
        auto about = e.description.empty() ? e.mirrors.front() : e.description;
        if (e.first_ledger) about += " (from ledger " + std::to_string(*e.first_ledger) + ")";
        options.emplace_back(e.name, about);
    }
    std::string chosen = "other";
    if (!options.empty()) {
        auto fallback = optional ? std::string("none") : options.front().first;
        options.emplace_back("other", "another publication: enter its URL and publisher key");
        if (optional) options.emplace_back("none", "no publication: history from now on");
        chosen = p.choose("Which publication?", options, fallback);
    }
    if (chosen == "none") return false;
    toml::table publication;
    if (chosen != "other") publication.insert("name", chosen);
    else {
        auto source = p.ask(optional ? "Publication for earlier history (HTTPS base URL or bucket directory; empty: history from now on)"
                                     : "Publication (HTTPS base URL or bucket directory)");
        if (source.empty() && optional) return false;
        publication.insert("source", source);
        publication.insert("publisher_key", p.publisher_key());
    }
    config.insert("publication", publication);
    return true;
}
int setup_connected(fs::path const& config_path, fs::path const& unit_dir, toml::table& config, Prompt& p) {
    toml::table node, storage, serve, connected;
    // The network first: its validator list is the one offered.
    auto list = offered_publications(p);
    auto network = ask_network(p, list, config);
    auto [list_url, list_key] = core::network_validator_list(core::parse_network(network));
    node.insert("rpc", p.ask("Node JSON-RPC URL (a local xrpld's admin port is not rate-limited)", "http://127.0.0.1:5005"));
    if (auto extra = p.ask("Fallback nodes for ledgers the primary no longer holds (comma-separated http(s):// or ws(s)://, empty: none)"); !extra.empty()) {
        toml::array list;
        std::stringstream stream(extra);
        for (std::string url; std::getline(stream, url, ',');) {
            auto begin = url.find_first_not_of(" \t");
            if (begin == std::string::npos) continue;
            auto end = url.find_last_not_of(" \t");
            list.push_back(url.substr(begin, end - begin + 1));
        }
        if (list.empty()) throw Error("Fallback nodes were empty");
        node.insert("fallback", list);
    }
    if (auto grpc = p.ask("Node gRPC URL for ledger diffs (empty: JSON-RPC only)"); !grpc.empty()) node.insert("grpc", grpc);
    if (p.yes("Accept only ledgers a quorum of validators signed, rather than the node's word (recommended for a remote node)?", false)) {
        toml::table validators;
        validators.insert("list", p.ask("Validator list", list_url));
        validators.insert("list_key", p.ask("Validator list publisher key", list_key));
        node.insert("validations", p.ask("Node WebSocket URL (its validations stream)", "ws://127.0.0.1:6006"));
        config.insert("validators", validators);
    }
    // With a publication, history comes from it and the node supplies the ledgers after it.
    if (!ask_publication(p, list, network, config, true)) {
        auto start = p.number("First ledger to keep (a sequence the node still holds, or tip)", "tip", 1, UINT32_MAX, {"tip"});
        if (start == "tip") connected.insert("start", "tip"); else connected.insert("start", static_cast<std::int64_t>(std::stoull(start)));
    }
    storage.insert("dir", p.directory("Storage directory", "/var/lib/xrpla"));
    storage.insert("cache_mb", p.integer("Decode cache (MB; 0: a quarter of memory, 3-64 GB)", "0", 0, 1ull << 30));
    // xrpld itself usually listens on 5005 and 6006.
    serve.insert("http", p.ask("HTTP JSON-RPC listener", "127.0.0.1:5105"));
    serve.insert("ws", p.ask("WebSocket listener", "127.0.0.1:6106"));
    config.insert("node", node); config.insert("connected", connected); config.insert("storage", storage); config.insert("serve", serve);
    install_service(config_path, config, unit_dir, p.out);
    return 0;
}
}

int setup(fs::path const& config_path, fs::path const& unit_dir, std::istream& in, std::ostream& out) {
    Prompt p{in, out};
    out << "XRPLA setup: writes " << config_path.string() << "\n\n";
    auto profile = p.choose("How will this machine use XRPLA?", {
        {"mirror", "serve the XRPL RPC/WebSocket API from published history, kept current from the publication"},
        {"restore", "fill an xrpld (NuDB/RocksDB) or Clio database from published history once, then exit"},
        {"connected", "follow your own or a remote xrpld a few seconds behind, keep its history compressed and serve it"},
        {"standalone", "no xrpld: history from a publication, the newest ledgers from the XRPL peer network"},
        {"publisher", "follow a node and publish history to R2/S3 (tools/live_publisher.py today)"}}, "mirror");
    if (profile == "mirror" || profile == "connected" || profile == "standalone") {
        if (auto path = fs::absolute(config_path).string(); !unit_safe(path))
            throw Error("A systemd unit cannot name the configuration " + path + " in ExecStart= (" + unit_rule + "); choose another --config");
        if (auto program = service_program().string(); !unit_safe(program))
            throw Error("A systemd unit cannot name this program, " + program + ", in ExecStart= (" + unit_rule + "); install xrpla where it can "
                        "(cmake --install BUILD --prefix /usr) and run setup from there");
    }
    toml::table config;
    config.insert("profile", profile);
    keep_log(config_path, config, out);
    if (profile == "connected") return setup_connected(config_path, unit_dir, config, p);
    auto list = offered_publications(p);
    ask_publication(p, list, ask_network(p, list, config), config, false);
    toml::table history;
    auto first = p.number("First ledger (a sequence, or earliest)", "earliest", 1, UINT32_MAX, {"earliest"});
    if (first == "earliest") history.insert("first", "earliest"); else history.insert("first", static_cast<std::int64_t>(std::stoull(first)));
    history.insert("last", "tip");
    config.insert("history", history);
    if (profile == "restore") {
        toml::table restore;
        auto target = p.choose("Restore into", {{"nudb", "xrpld node database (NuDB, the xrpld default)"}, {"rocksdb", "xrpld node database (RocksDB)"},
                                               {"clio", "Clio (Cassandra/ScyllaDB)"}, {"store", "an xrpla store (xrpla serve)"}}, "nudb");
        restore.insert("target", target);
        restore.insert("out", p.ask("Output directory", "/var/lib/xrpla/restore"));
        if (target == "clio") {
            auto host = p.ask("Cassandra/ScyllaDB host (empty: only write the import plan)");
            if (!host.empty()) {
                restore.insert("clio_host", host);
                restore.insert("clio_port", p.integer("Port", "9042", 1, 65535));
                restore.insert("clio_keyspace", p.ask("Keyspace", "xrpla"));
            }
        }
        config.insert("restore", restore);
    } else if (profile == "mirror" || profile == "standalone") {
        toml::table storage, serve, mirror;
        storage.insert("dir", p.directory("Storage directory", "/var/lib/xrpla"));
        storage.insert("cache_mb", p.integer("Decode cache (MB; 0: a quarter of memory, 3-64 GB)", "0", 0, 1ull << 30));
        serve.insert("http", p.ask("HTTP JSON-RPC listener", "127.0.0.1:5005"));
        serve.insert("ws", p.ask("WebSocket listener", "127.0.0.1:6006"));
        config.insert("storage", storage); config.insert("serve", serve);
        if (profile == "mirror") {
            mirror.insert("sync_seconds", p.integer("Check the publication every (seconds)", "300", 1, 86400 * 7));
            config.insert("mirror", mirror);
        }
    }
    if (profile == "mirror" || profile == "standalone") {
        install_service(config_path, config, unit_dir, out);
        return 0;
    }
    write_config(config_path, config, out);
    if (profile == "restore") {
        if (p.yes("Start the restore now?", true)) return run_config(config_path);
        out << "Run it later with: xrpla run --config " << config_path.string() << '\n';
    } else {
        out << "This profile runs in a later release; the configuration is kept for it.\n";
    }
    return 0;
}
}
