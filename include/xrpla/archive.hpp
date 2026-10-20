#pragma once

#include <nlohmann/json.hpp>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>
#include <optional>

namespace xrpla {
namespace fs = std::filesystem;
using Json = nlohmann::json;

struct Error : std::runtime_error { using std::runtime_error::runtime_error; };
struct Object {
    std::string object;
    std::uint64_t size;
    std::string sha256;
};
struct Chunk {
    Object file;
    std::uint32_t first, last;
    std::string last_ledger_hash;
    std::optional<Object> time_index;
    std::uint32_t first_close_time = 0, last_close_time = 0;
};
struct Snapshot {
    std::uint32_t seq;
    std::string prefix, account_hash;
    Object index;
    std::uint64_t parts, total_size;
};
struct Range { std::uint32_t first, last; };
struct Manifest {
    std::uint32_t tip;
    std::vector<Object> dicts;
    std::vector<Chunk> chunks;
    std::vector<Snapshot> snapshots;
    std::uint32_t version = 1;
    std::string tip_hash;
};
struct Plan {
    Range range;
    Snapshot snapshot;
    std::vector<Object> objects;
};

void initialize();
std::string sha256(std::string_view data);
std::string file_sha256(fs::path const& file);
std::string read_file(fs::path const& file, std::uint64_t limit);
void write_file(fs::path const& file, std::string_view bytes);
void sync_file(fs::path const& file);
void durable_rename(fs::path const& source, fs::path const& destination);
void validate_key(std::string const& key);
void validate_hash(std::string const& hash);
Object parse_object(Json const& value);
Manifest authenticate_manifest(std::string const& bytes, std::string const& signature,
                               std::string const& public_key);
// A pinned publisher key as --publisher-key and [publication] publisher_key give it, as its 32
// bytes: 64 hexadecimal characters are the key itself, always, whatever files the working
// directory holds (a key file so named is given with a path, such as ./NAME); anything else
// names a file holding 32 raw bytes or 64 hexadecimal characters.
std::string pinned_publisher_key(fs::path const& key);
Range parse_range(std::string const& input, std::uint32_t tip);
std::vector<std::pair<std::uint32_t, std::uint32_t>> decode_times(Chunk const&, std::string const&);
Plan make_plan(Manifest const& manifest, Range range);
std::vector<Object> snapshot_parts(Snapshot const& snapshot, std::string const& index);
bool matches(fs::path const& path, Object const& expected);
// Thrown when another process (or thread) holds the directory: a background job skips its round.
struct DirectoryLocked : Error { using Error::Error; };
class DirectoryLock {
    fs::path lock_;
    int fd_ = -1;
public:
    explicit DirectoryLock(fs::path const& root);
    ~DirectoryLock();
    DirectoryLock(DirectoryLock const&) = delete;
    DirectoryLock& operator=(DirectoryLock const&) = delete;
};
void safe_cache_path(fs::path const&);
struct Selection { Manifest manifest; Plan plan; };
// Caller must hold DirectoryLock for the entire subsequent use of the cache.
Selection load_selection(fs::path const& dir, fs::path const& publisher_key);

// A publication's mirrors: HTTPS base URLs (or local directories) serving the same objects,
// tried in order (docs/reference/publications.md).
using Mirrors = std::vector<std::string>;
// One easy handle per transfer; initialize() runs before workers start. With several mirrors, a
// read or download that fails on one (out of reach, an HTTP error, or bytes its check refuses)
// is tried on the next, and the switch logged. A mirror that failed rests (30 s after its first
// failure in a row, doubling up to an hour, for the whole process): until then the others are
// asked first. Nothing is trusted to a mirror: what it serves is checked against what the
// publisher signed.
class Source {
    struct Mirror { std::string base; bool remote; };
    // What a mirror is given: tries for transient failures, and the time to connect.
    struct Patience { unsigned tries; long connect_seconds; };
    std::vector<Mirror> mirrors_;
    Patience patience_;  // the last mirror's left (a single mirror's)
    Source(Mirror mirror, Patience patience);
    // What a mirror with others after it is given: fewer tries, and less time to connect.
    Patience before_last() const;
    // The mirrors' indexes, those not resting first (in their order), then the resting ones by
    // when their rest ends.
    std::vector<std::size_t> by_health() const;
    void failover(std::string const& key, std::stop_token const& stop, std::function<void(Mirror const&, Patience const&)> const& attempt,
                  std::vector<std::size_t> order = {}) const;
public:
    explicit Source(std::string base);
    explicit Source(Mirrors const& bases);
    // Throws when the bytes read are not the ones asked for: the next mirror is tried.
    using Check = std::function<void(std::string const& bytes)>;
    // A stop requested on `stop` ends a remote read within about a second, its transfer and any
    // wait to try it again, with an Error (a day range's search, archive.cpp).
    std::string read(std::string const& key, std::uint64_t limit, std::stop_token stop = {}, Check const& accept = {}) const;
    // Exactly `length` bytes of an object starting at `offset` (an HTTP range request remotely).
    std::string read_range(std::string const& key, std::uint64_t offset, std::uint64_t length, Check const& accept = {}) const;
    void download(Object const& object, fs::path const& destination) const;
    // Runs `attempt` with each mirror alone in turn until one returns, failing over as a read
    // does: for what must come whole from one mirror (a catalog, its signature and what it
    // selects). `what` names it in the log. With `rank`, each mirror not resting is first asked
    // for a rank (what it claims to hold, such as its catalog's generation: an order, not
    // trusted), and they are tried highest first; one that gives none, and the resting ones,
    // after them.
    void each_mirror(std::string const& what, std::function<void(Source const&)> const& attempt,
                     std::function<std::uint64_t(Source const&)> const& rank = {}) const;
};
// One HTTPS GET (no redirects), at most `limit` bytes.
std::string fetch_url(std::string const& url, std::uint64_t limit);
struct SignedManifest { std::string bytes, signature; Manifest manifest; };
// Publishers store signatures by manifest hash before atomically replacing the
// manifest. Legacy archives retain the detached-pair retry path.
SignedManifest read_manifest(Source const&, std::string const& public_key);

struct FetchOptions {
    std::string archive, range;
    fs::path dir, publisher_key;
    unsigned jobs = 8;
};
Json fetch(FetchOptions const& options);
Json verify(fs::path const& dir, fs::path const& publisher_key);
Json status(fs::path const& dir);
} // namespace xrpla
