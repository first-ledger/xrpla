#include <xrpla/archive.hpp>
#include <curl/curl.h>
#include <sodium.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stop_token>
#include <thread>
#include <cstdlib>

namespace xrpla {
namespace {
using Curl = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
// The SHA-256 of a download, taken as its bytes are written, so a finished transfer is not
// read back whole to be checked (a first sync of full history is 1.4 TB).
class Digest {
    crypto_hash_sha256_state state_{};
    std::uint64_t bytes_ = 0;
public:
    Digest() { crypto_hash_sha256_init(&state_); }
    void add(char const* data, std::size_t n) {
        crypto_hash_sha256_update(&state_, reinterpret_cast<unsigned char const*>(data), n);
        bytes_ += n;
    }
    std::uint64_t bytes() const { return bytes_; }
    // Begins again over the first `length` bytes of `file` (what a resumed transfer has).
    void restart(fs::path const& file, std::uint64_t length) {
        *this = Digest();
        if (!length) return;
        std::ifstream in(file, std::ios::binary);
        std::array<char, 128 * 1024> buffer{};
        while (bytes_ < length) {
            auto n = static_cast<std::streamsize>(std::min<std::uint64_t>(buffer.size(), length - bytes_));
            in.read(buffer.data(), n);
            if (in.gcount() != n) throw Error("Cannot read download staging file");
            add(buffer.data(), static_cast<std::size_t>(n));
        }
    }
    std::string hex() {
        std::array<unsigned char, crypto_hash_sha256_BYTES> digest{};
        crypto_hash_sha256_final(&state_, digest.data());
        std::string out(digest.size() * 2 + 1, '\0');
        sodium_bin2hex(out.data(), out.size(), digest.data(), digest.size());
        out.pop_back();
        return out;
    }
};
struct Sink {
    std::string bytes;
    std::ofstream* file = nullptr;
    std::uint64_t remaining;
    Digest* digest = nullptr;  // with `file`: what is written is hashed
};
std::size_t receive(char* data, std::size_t size, std::size_t count, void* userdata) noexcept {
    auto& sink = *static_cast<Sink*>(userdata);
    auto n = size * count;
    if (n > sink.remaining) return 0;
    try {
        if (sink.file) {
            sink.file->write(data, static_cast<std::streamsize>(n));
            if (!*sink.file) return 0;
            if (sink.digest) sink.digest->add(data, n);
        } else {
            sink.bytes.append(data, n);
        }
        sink.remaining -= n;
        return n;
    } catch (...) { return 0; }
}
// One HTTP client per thread, reset for each request: its live connections, DNS cache and TLS
// sessions carry over, so a restore's thousands of requests to one origin skip the handshakes.
CURL* request(std::string const& url, Sink& sink, long connect_seconds = 30) {
    thread_local Curl client(curl_easy_init(), curl_easy_cleanup);
    if (!client) throw Error("Cannot allocate HTTP client");
    auto* curl = client.get();
    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    if (auto bundle = std::getenv("SSL_CERT_FILE"); bundle && *bundle) curl_easy_setopt(curl, CURLOPT_CAINFO, bundle);
    else if (fs::exists("/etc/ssl/certs/ca-certificates.crt")) curl_easy_setopt(curl, CURLOPT_CAINFO, "/etc/ssl/certs/ca-certificates.crt");
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    // No redirects: a publisher URL must point directly to its object origin.
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connect_seconds);
    // A stalled transfer ends here; a slow one that keeps moving is let finish.
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 60L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &sink);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "xrpla/" XRPLA_VERSION);
    return curl;
}
// A failure worth trying again: the network, a truncated body, or the origin busy or failing.
struct Transient : Error { using Error::Error; };
bool transient(CURLcode code, long status) {
    switch (code) {
    case CURLE_HTTP_RETURNED_ERROR: return status >= 500 || status == 408 || status == 429;
    case CURLE_COULDNT_RESOLVE_HOST: case CURLE_COULDNT_CONNECT: case CURLE_OPERATION_TIMEDOUT:
    case CURLE_PARTIAL_FILE: case CURLE_RECV_ERROR: case CURLE_SEND_ERROR: case CURLE_GOT_NOTHING:
    case CURLE_SSL_CONNECT_ERROR: case CURLE_HTTP2: case CURLE_HTTP2_STREAM: return true;
    default: return false;
    }
}
void check(CURLcode code, long status) {
    if (code != CURLE_OK) {
        auto what = std::string("Transfer failed: ") + curl_easy_strerror(code) + (status ? " (HTTP " + std::to_string(status) + ")" : "");
        if (transient(code, status)) throw Transient(what);
        throw Error(what);
    }
    if (status != 200 && status != 206) throw Error("Unexpected HTTP status " + std::to_string(status));
}
// Transient failures are tried again after 1, 2, 4 ... 60 s (with jitter), 10 tries in all
// (about 4 minutes): one blip must not end a restore that has run for hours. A wait ends early
// once `stop` is requested.
constexpr unsigned transfer_tries = 10;
void pause_after(unsigned failures, std::stop_token stop = {}) {
    thread_local std::mt19937 random{std::random_device{}()};
    auto seconds = std::min(60.0, std::ldexp(1.0, static_cast<int>(failures) - 1)) * std::uniform_real_distribution<double>(0.75, 1.25)(random);
    auto pause = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration<double>(seconds));
    if (!stop.stop_possible()) { std::this_thread::sleep_for(pause); return; }
    std::mutex mutex;
    std::unique_lock lock(mutex);
    std::condition_variable_any wake;
    wake.wait_for(lock, stop, pause, [] { return false; });
}
// A transfer given a stop token (Source::read) is abandoned once a stop is requested: libcurl
// calls this at least once a second, connecting or waiting for bytes too.
int stopped(void* stop, curl_off_t, curl_off_t, curl_off_t, curl_off_t) noexcept {
    return static_cast<std::stop_token const*>(stop)->stop_requested() ? 1 : 0;
}
// A mirror with others after it is given a few tries (about 3 s), and 5 s to connect, before
// the next is asked.
constexpr unsigned mirror_tries = 3;
constexpr long mirror_connect_seconds = 5;
template <class Transfer> auto with_retries(Transfer transfer, std::stop_token stop = {}, unsigned tries = transfer_tries) {
    for (unsigned failures = 1;; ++failures) {
        try { return transfer(); }
        catch (Transient const&) {
            if (failures >= tries || stop.stop_requested()) throw;
            pause_after(failures, stop);
            if (stop.stop_requested()) throw;
        }
    }
}
void no_symlinks(fs::path const& path) {
    fs::path current;
    for (auto const& component : fs::absolute(path)) {
        current /= component;
        if (fs::is_symlink(fs::symlink_status(current)))
            throw Error("Archive paths must not contain symlinks: " + current.string());
    }
}
// Objects a cache already holds were hashed when they arrived: `<cache>/.verified` lists each
// one's name, SHA-256, size and modification time as checked, and a file still matching its
// line is not hashed again (a sync runs every few minutes over up to 1.4 TB). One replaced
// or touched since is. Damage inside a file shows where it is read (every section carries a
// checksum) and in `xrpla verify`.
// A file is recorded only once its modification time is a few seconds old. Timestamps advance
// in ticks (milliseconds on Linux, up to 2 s on some filesystems), so a rewrite in the same
// tick as the write it replaced would keep the recorded time, and would not be noticed. A file
// written more recently is hashed again on the next check instead. A download, hashed as it
// arrived, is recorded at once: its time is set that far back first (fresh), and any later
// write stamps the current time, which still shows.
// Lines are appended, a changed object's after its old one (the later line wins), and objects
// a sync no longer selects are removed from the cache: the file is rewritten (compacted) when
// first read and once the lines appended since number as many as the objects it lists (and a thousand),
// keeping only the newest line of each object still present. Counting lines against the
// objects listed never fired: each object a sync adds is a new name, and the list grew by one
// line and one object alike, so removed objects stayed listed (and in memory) until a restart.
// Each compaction checks every object listed, so this costs one check an append, and what a
// removed object leaves is at most the live list again. A line dropped only means a hash
// on the next check. Downloads into a root run under a DirectoryLock over it (sync_pieces,
// fetch), so no other process appends while it is rewritten.
class Verified {
public:
    static constexpr std::chrono::seconds settle{5};
    static void fresh(fs::path const& file) { fs::last_write_time(file, fs::file_time_type::clock::now() - settle); }
    // The file's own stamp and the root's canonical path are taken before the lock: only the
    // caller's transfer writes the file.
    bool known(fs::path const& root, Object const& object, fs::path const& file) {
        auto current = stamp(object, file);
        auto key = root_key(root);
        std::unique_lock lock(mutex_);
        auto const& stamps = load(lock, key, root).stamps;
        auto found = stamps.find(object.object);
        return found != stamps.end() && found->second == current;
    }
    void add(fs::path const& root, Object const& object, fs::path const& file) {
        if (!settled(file)) return;
        auto line = stamp(object, file);
        auto key = root_key(root);
        std::unique_lock lock(mutex_);
        auto& list = load(lock, key, root);
        if (list.stamps[object.object] == line) return;
        list.stamps[object.object] = line;
        no_symlinks(root / ".verified");
        {
            std::ofstream out(root / ".verified", std::ios::app | std::ios::binary);
            out << object.object << ' ' << line << '\n';
        }
        ++list.lines;
        if (++list.appended >= std::max<std::size_t>(list.stamps.size(), 1024)) compact(root, list);
    }
private:
    struct List {
        std::map<std::string, std::string> stamps;  // object key: its stamp
        std::size_t lines = 0;  // in the file
        std::size_t appended = 0;  // of them, added since it was last compacted
        bool ready = false;  // read (and compacted) from the file
        bool loading = false;  // being read by one caller, with the lock released
        std::uint64_t failures = 0;  // reads that failed
        std::exception_ptr failure;  // the latest of them
    };
    // A root however it is named.
    static fs::path root_key(fs::path const& root) {
        std::error_code error;
        auto canonical = fs::weakly_canonical(root, error);
        return error ? root : canonical;
    }
    static bool settled(fs::path const& file) {
        std::error_code error;
        auto written = fs::last_write_time(file, error);
        return !error && fs::file_time_type::clock::now() - written >= settle;
    }
    static std::string stamp(Object const& object, fs::path const& file) {
        if (fs::is_symlink(fs::symlink_status(file)) || !fs::is_regular_file(file)) return {};
        return object.sha256 + ' ' + std::to_string(fs::file_size(file)) + ' ' +
               std::to_string(fs::last_write_time(file).time_since_epoch().count());
    }
    // A root's list, read from its file the first time the root is used (however it is named:
    // keyed by its canonical path, `key`). That read checks every object the file names (compact),
    // so it runs with `lock` released: other roots go on, and callers for this one wait until it
    // is done. No one appends to the file meanwhile: only add() does, once the list is ready. A
    // read that fails throws and leaves the list unread: the callers waiting for it get its
    // failure, and the next to use the root reads it again, so a failed read never stands for an
    // empty list (which a later compaction would write over the file, dropping its valid lines),
    // and callers queued behind a failing read do not each read again in turn.
    List& load(std::unique_lock<std::mutex>& lock, fs::path const& key, fs::path const& root) {
        auto& list = roots_[key];  // a map's elements stay in place as others are added
        while (!list.ready) {
            if (list.loading) {
                auto failures = list.failures;
                loaded_.wait(lock, [&] { return list.ready || !list.loading; });
                if (!list.ready && list.failures != failures) std::rethrow_exception(list.failure);
                continue;
            }
            list.loading = true;
            lock.unlock();
            List read;
            std::exception_ptr failure;
            try {
                if (fs::is_regular_file(root / ".verified")) {
                    {
                        std::ifstream in(root / ".verified", std::ios::binary);
                        for (std::string line; std::getline(in, line); ++read.lines)
                            if (auto space = line.find(' '); space != std::string::npos) read.stamps[line.substr(0, space)] = line.substr(space + 1);
                    }
                    compact(root, read);
                }
            } catch (...) { failure = std::current_exception(); }
            lock.lock();
            if (failure) { list.failure = failure; ++list.failures; }
            else { list = std::move(read); list.ready = true; }
            list.loading = false;
            loaded_.notify_all();
            if (failure) std::rethrow_exception(failure);
        }
        return list;
    }
    // Drops the lines of objects no longer in the cache (or never safe to name), and rewrites
    // the file with one line for each object left when that changes it: written beside it,
    // synced and renamed over it (durable_rename), so a crash leaves the old list or the new.
    static void compact(fs::path const& root, List& list) {
        std::erase_if(list.stamps, [&](auto const& item) {
            try { validate_key(item.first); } catch (Error const&) { return true; }
            auto status = fs::symlink_status(root / item.first);
            return !fs::is_regular_file(status);
        });
        list.appended = 0;
        if (list.lines == list.stamps.size()) return;
        std::string text;
        for (auto const& [object, line] : list.stamps) text += object + ' ' + line + '\n';
        auto partial = root / ".verified.part";
        no_symlinks(partial);
        no_symlinks(root / ".verified");
        write_file(partial, text);
        durable_rename(partial, root / ".verified");
        list.lines = list.stamps.size();
    }
    std::mutex mutex_;
    std::condition_variable loaded_;  // a root's list made ready
    std::map<fs::path, List> roots_;
};
Verified verified;
// The mirrors that failed lately, by base, for the whole process: a mirror rests 30 s after its
// first failure in a row, then twice as long after each next one, up to an hour, and is asked
// only after the others meanwhile. So a mirror that is down costs a sync or restore its few
// seconds once, not once for each Source a sync makes (and each sync), however long it stays
// down. An answer ends its rest.
class MirrorHealth {
    using Clock = std::chrono::steady_clock;
    struct Record { unsigned failures = 0; Clock::time_point until; };
    std::mutex mutex_;
    std::map<std::string, Record> records_;
public:
    void failed(std::string const& base) {
        std::lock_guard lock(mutex_);
        auto& record = records_[base];
        record.failures = std::min(record.failures + 1, 8u);
        record.until = Clock::now() + std::min<Clock::duration>(std::chrono::seconds(30) * (1u << (record.failures - 1)), std::chrono::hours(1));
    }
    void answered(std::string const& base) {
        std::lock_guard lock(mutex_);
        records_.erase(base);
    }
    // When the mirror's rest ends, while it rests.
    std::optional<Clock::time_point> resting(std::string const& base) {
        std::lock_guard lock(mutex_);
        auto record = records_.find(base);
        if (record == records_.end() || record->second.until <= Clock::now()) return std::nullopt;
        return record->second.until;
    }
};
MirrorHealth health;
} // namespace

Source::Source(std::string base) : Source(Mirrors{std::move(base)}) {}
Source::Source(Mirror mirror, Patience patience) : mirrors_{std::move(mirror)}, patience_(patience) {}
Source::Source(Mirrors const& bases) : patience_{transfer_tries, 30} {
    initialize();
    if (bases.empty()) throw Error("Archive URL or local directory is required");
    for (auto base : bases) {
        if (base.empty()) throw Error("Archive URL or local directory is required");
        bool remote = base.find("://") != std::string::npos;
        if (remote) {
            std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> url(curl_url(), curl_url_cleanup);
            if (!url || curl_url_set(url.get(), CURLUPART_URL, base.c_str(), 0) != CURLUE_OK)
                throw Error("Invalid archive URL");
            auto part = [&](CURLUPart p) {
                char* raw = nullptr;
                auto rc = curl_url_get(url.get(), p, &raw, 0);
                std::unique_ptr<char, decltype(&curl_free)> text(raw, curl_free);
                return rc == CURLUE_OK ? std::string(raw) : std::string{};
            };
            auto scheme = part(CURLUPART_SCHEME), host = part(CURLUPART_HOST);
            if (scheme != "https" && !(scheme == "http" && (host == "127.0.0.1" || host == "[::1]" || host == "localhost")))
                throw Error("Archive requires HTTPS (HTTP allowed only on loopback for testing)");
            if (!part(CURLUPART_USER).empty() || !part(CURLUPART_PASSWORD).empty() ||
                !part(CURLUPART_QUERY).empty() || !part(CURLUPART_FRAGMENT).empty())
                throw Error("Archive URL cannot contain credentials, query, or fragment");
            while (base.back() == '/') base.pop_back();
        } else if (!fs::is_directory(base)) {
            throw Error("Local archive directory does not exist: " + base);
        }
        mirrors_.push_back({std::move(base), remote});
    }
}
Source::Patience Source::before_last() const {
    return {std::min(patience_.tries, mirror_tries), std::min(patience_.connect_seconds, mirror_connect_seconds)};
}
std::vector<std::size_t> Source::by_health() const {
    std::vector<std::pair<std::chrono::steady_clock::time_point, std::size_t>> order;  // resting until (or none), place
    for (std::size_t i = 0; i < mirrors_.size(); ++i)
        order.emplace_back(mirrors_.size() > 1 ? health.resting(mirrors_[i].base).value_or(std::chrono::steady_clock::time_point{}) : std::chrono::steady_clock::time_point{}, i);
    std::stable_sort(order.begin(), order.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    std::vector<std::size_t> out;
    for (auto const& [until, i] : order) out.push_back(i);
    return out;
}
// Each mirror in turn (`order`, or by_health) until one answers. Before the last one left, a
// transient failure is tried again only a few times and a connection given 5 s: the next mirror
// is asked sooner than a blip would be waited out. The last is given the full retries, as a
// single mirror is. A stop requested is no mirror's failure, and is not tried elsewhere. With
// several mirrors, each failure and answer is kept in their health record (a single mirror has
// no other to put first).
void Source::failover(std::string const& key, std::stop_token const& stop, std::function<void(Mirror const&, Patience const&)> const& attempt,
                      std::vector<std::size_t> order) const {
    if (order.empty()) order = by_health();
    bool recorded = mirrors_.size() > 1;
    for (std::size_t i = 0;; ++i) {
        auto const& mirror = mirrors_[order[i]];
        bool last = i + 1 == order.size();
        try {
            attempt(mirror, last ? patience_ : before_last());
            if (recorded) health.answered(mirror.base);
            return;
        } catch (std::exception const& error) {
            if (stop.stop_requested()) throw;
            if (recorded) health.failed(mirror.base);
            if (last) throw;
            std::cerr << Json{{"event", "mirror_failed"}, {"mirror", mirror.base}, {"object", key}, {"error", error.what()},
                              {"next", mirrors_[order[i + 1]].base}}.dump() << std::endl;
        }
    }
}
void Source::each_mirror(std::string const& what, std::function<void(Source const&)> const& attempt,
                         std::function<std::uint64_t(Source const&)> const& rank) const {
    std::vector<std::size_t> order;
    if (rank && mirrors_.size() > 1) {
        // Ranked first, highest first (in their order when equal); then those that gave no rank, then the resting.
        std::vector<std::pair<std::uint64_t, std::size_t>> ranked;
        std::vector<std::size_t> unranked, resting;
        for (auto i : by_health()) {
            if (health.resting(mirrors_[i].base)) { resting.push_back(i); continue; }
            try { ranked.emplace_back(rank(Source(mirrors_[i], before_last())), i); }
            catch (std::exception const& error) {
                // Asked last, and rested as any failure is.
                health.failed(mirrors_[i].base);
                std::cerr << Json{{"event", "mirror_failed"}, {"mirror", mirrors_[i].base}, {"object", what}, {"error", error.what()}}.dump() << std::endl;
                unranked.push_back(i);
            }
        }
        std::stable_sort(ranked.begin(), ranked.end(), [](auto const& a, auto const& b) { return a.first > b.first; });
        for (auto const& [claimed, i] : ranked) order.push_back(i);
        order.insert(order.end(), unranked.begin(), unranked.end());
        order.insert(order.end(), resting.begin(), resting.end());
    }
    failover(what, {}, [&](Mirror const& mirror, Patience const& patience) { attempt(Source(mirror, patience)); }, order);
}
std::string Source::read(std::string const& key, std::uint64_t limit, std::stop_token stop, Check const& accept) const {
    validate_key(key);
    std::string bytes;
    failover(key, stop, [&](Mirror const& mirror, Patience const& patience) {
        if (!mirror.remote) {
            auto path = fs::path(mirror.base) / key;
            no_symlinks(path);
            bytes = read_file(path, limit);
        } else {
            // No overall time limit: snapshot parts (~40 MB) are read whole, many at once, and a
            // slow or shared link may take minutes over one (the low-speed limit ends stalls).
            bytes = with_retries([&] {
                if (stop.stop_requested()) throw Error("Read of " + key + " stopped");
                Sink sink{{}, nullptr, limit};
                auto* curl = request(mirror.base + "/" + key, sink, patience.connect_seconds);
                if (stop.stop_possible()) {
                    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, stopped);
                    curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &stop);
                    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
                }
                auto result = curl_easy_perform(curl);
                long status = 0;
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
                check(result, status);
                if (status != 200) throw Error("Metadata request requires HTTP 200");
                return std::move(sink.bytes);
            }, stop, patience.tries);
        }
        if (accept) accept(bytes);
    });
    return bytes;
}
std::string fetch_url(std::string const& url, std::uint64_t limit) {
    initialize();
    // Parsed as curl will read it (as Source does): plain HTTP only to this machine's own
    // addresses, named exactly (localhost, 127.0.0.0/8 or ::1), not to a host beginning so.
    std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> parsed(curl_url(), curl_url_cleanup);
    if (!parsed || curl_url_set(parsed.get(), CURLUPART_URL, url.c_str(), 0) != CURLUE_OK) throw Error("Invalid URL");
    auto part = [&](CURLUPart p) {
        char* raw = nullptr;
        auto rc = curl_url_get(parsed.get(), p, &raw, 0);
        std::unique_ptr<char, decltype(&curl_free)> text(raw, curl_free);
        return rc == CURLUE_OK ? std::string(raw) : std::string{};
    };
    auto loopback = [](std::string const& host) {
        if (host == "localhost" || host == "[::1]") return true;
        // Four decimal octets, the first 127.
        unsigned octets = 0, value = 0, digits = 0, first = 0;
        for (std::size_t i = 0; i <= host.size(); ++i) {
            if (i == host.size() || host[i] == '.') {
                if (digits == 0 || value > 255) return false;
                if (octets++ == 0) first = value;
                value = digits = 0;
            } else if (host[i] >= '0' && host[i] <= '9' && digits < 3) {
                value = value * 10 + unsigned(host[i] - '0'); ++digits;
            } else {
                return false;
            }
        }
        return octets == 4 && first == 127;
    };
    auto scheme = part(CURLUPART_SCHEME);
    if (scheme != "https" && !(scheme == "http" && loopback(part(CURLUPART_HOST))))
        throw Error("URLs require HTTPS (HTTP allowed only on loopback for testing)");
    return with_retries([&] {
        Sink sink{{}, nullptr, limit};
        auto* curl = request(url, sink);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 120L);
        auto result = curl_easy_perform(curl);
        long status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
        check(result, status);
        return std::move(sink.bytes);
    });
}
std::string Source::read_range(std::string const& key, std::uint64_t offset, std::uint64_t length, Check const& accept) const {
    validate_key(key);
    std::string bytes;
    failover(key, {}, [&](Mirror const& mirror, Patience const& patience) {
        if (!mirror.remote) {
            auto path = fs::path(mirror.base) / key;
            no_symlinks(path);
            std::ifstream in(path, std::ios::binary);
            if (!in) throw Error("Cannot open " + path.string());
            bytes.assign(length, '\0');
            in.seekg(static_cast<std::streamoff>(offset));
            in.read(bytes.data(), static_cast<std::streamsize>(length));
            if (static_cast<std::uint64_t>(in.gcount()) != length) throw Error("Short read from " + path.string());
        } else if (!length) {
            bytes.clear();
        } else {
            bytes = with_retries([&] {
                Sink sink{{}, nullptr, length};
                sink.bytes.reserve(length);
                auto* curl = request(mirror.base + "/" + key, sink, patience.connect_seconds);
                auto range = std::to_string(offset) + "-" + std::to_string(offset + length - 1);
                curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());
                auto result = curl_easy_perform(curl);
                long status = 0;
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
                check(result, status);
                if (status != 206 && !(status == 200 && offset == 0 && sink.bytes.size() == length)) throw Error("Range request requires HTTP 206");
                if (sink.bytes.size() != length) throw Transient("Short range response for " + key);
                return std::move(sink.bytes);
            }, {}, patience.tries);
        }
        if (accept) accept(bytes);
    });
    return bytes;
}
void Source::download(Object const& object, fs::path const& root) const {
    validate_key(object.object);
    auto destination = root / object.object;
    auto staging = root / ".staging" / (object.sha256 + ".part");
    no_symlinks(destination);
    no_symlinks(staging);
    if (verified.known(root, object, destination)) return;
    if (matches(destination, object)) { verified.add(root, object, destination); return; }
    // Never overwrite an immutable final object, even when corrupt.
    if (fs::exists(destination)) throw Error("Cached object is corrupt; remove it and retry: " + destination.string());
    fs::create_directories(destination.parent_path());
    fs::create_directories(staging.parent_path());
    // From each mirror in turn until one serves the object whole: a staging file another left
    // is resumed as any is (the hash decides), and one that differs is fetched again.
    failover(object.object, {}, [&](Mirror const& mirror, Patience const& patience) {
        // Fills `staging` from `offset` on, hashing what it writes into `digest`; true when any
        // of the bytes it ends with came from an earlier transfer.
        auto transfer = [&](std::uint64_t offset, Digest& digest) {
            bool resumed = offset > 0;
            if (offset == object.size) return resumed;
            if (mirror.remote) {
                // A transfer failing on the way resumes after the bytes that arrived.
                for (unsigned failures = 1;;) {
                    if (digest.bytes() != offset) digest.restart(staging, offset);
                    std::ofstream out(staging, std::ios::binary | (offset ? std::ios::app : std::ios::trunc));
                    if (!out) throw Error("Cannot open download staging file");
                    Sink sink{{}, &out, object.size - offset, &digest};
                    auto* curl = request(mirror.base + "/" + object.object, sink, patience.connect_seconds);
                    if (offset) curl_easy_setopt(curl, CURLOPT_RESUME_FROM_LARGE, static_cast<curl_off_t>(offset));
                    auto result = curl_easy_perform(curl);
                    long response = 0;
                    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response);
                    out.close();
                    if (!out) throw Error("Writing download failed");
                    if (offset && (result == CURLE_RANGE_ERROR || response == 200 || response == 416)) {
                        offset = 0;  // the origin will not resume: start over
                        resumed = false;
                        continue;
                    }
                    try { check(result, response); return resumed; }
                    catch (Transient const&) {
                        if (failures >= patience.tries) throw;
                        pause_after(failures++);
                        offset = fs::file_size(staging);
                        resumed = resumed || offset > 0;
                        if (offset == object.size) return resumed;  // all arrived: the hash decides
                    }
                }
            }
            auto source = fs::path(mirror.base) / object.object;
            no_symlinks(source);
            if (!fs::is_regular_file(source) || fs::file_size(source) != object.size)
                throw Error("Local source size mismatch: " + object.object);
            std::ifstream in(source, std::ios::binary);
            std::ofstream out(staging, std::ios::binary | (offset ? std::ios::app : std::ios::trunc));
            if (!in || !out) throw Error("Cannot open local transfer");
            in.seekg(static_cast<std::streamoff>(offset));
            std::array<char, 128 * 1024> buffer{};
            auto remaining = object.size - offset;
            while (remaining) {
                auto n = static_cast<std::streamsize>(std::min<std::uint64_t>(buffer.size(), remaining));
                in.read(buffer.data(), n);
                if (in.gcount() != n) throw Error("Local source truncated during transfer");
                out.write(buffer.data(), n);
                digest.add(buffer.data(), static_cast<std::size_t>(n));
                remaining -= static_cast<std::uint64_t>(n);
            }
            out.close();
            if (!out) throw Error("Local transfer write failed");
            return resumed;
        };
        // A staging file left by an interrupted sync is resumed after its bytes (read once, to
        // hash them). A crash can leave one whose tail never reached the disk (zeros where bytes
        // were), which no resume repairs: an object that then differs is fetched once more from
        // its first byte before it is refused.
        for (bool again = false;; again = true) {
            std::uint64_t offset = !again && fs::exists(staging) ? fs::file_size(staging) : 0;
            if (offset > object.size) offset = 0;
            Digest digest;
            digest.restart(staging, offset);
            bool resumed = transfer(offset, digest);
            if (digest.bytes() == object.size && fs::file_size(staging) == object.size && digest.hex() == object.sha256) break;
            fs::remove(staging);
            if (again || !resumed) throw Error("Object SHA-256 or size mismatch: " + object.object);
        }
    });
    Verified::fresh(staging);
    durable_rename(staging, destination);
    verified.add(root, object, destination);
}
} // namespace xrpla
