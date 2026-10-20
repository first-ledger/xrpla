#pragma once
#include <xrpla/archive.hpp>
#include <xrpla/core/verify.hpp>
#include <functional>
#include <memory>
#include <optional>

namespace xrpla::core {
// Thrown by PieceFeed when the catalogs offered are older than the generation asked for (and
// no mirror offered one that is not): `offered` is the newest of them.
struct OlderCatalog : Error {
    std::uint64_t offered;
    OlderCatalog(std::string const& what, std::uint64_t generation) : Error(what), offered(generation) {}
};
// Ledgers in order from a signed piece publication, read straight from its bucket: a local
// directory or an HTTPS base such as an R2 public URL, or several mirrors of it. Nothing is
// staged on disk. The catalog is checked against the pinned publisher key, each container
// directory against the catalog, each segment against its directory row, each decoded page
// against its canonical digest and each range's last ledger against the catalog; callers replay
// the records through a LedgerVerifier, which checks every transaction and state root. The
// catalog and what it selects come from the first mirror that serves them signed and as asked;
// each object after that from any mirror, checked as it arrives (Source).
class PieceFeed {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    // Starts from the newest published snapshot below `first` (default: the earliest one) and
    // ends at `last` (default: the catalog tip). With `allow_baseline`, a publication that so far
    // has only its baseline snapshot selects that snapshot and no ledgers (last() is its ledger).
    // A catalog older than `oldest_generation` is refused, and the next mirror asked.
    // `publisher_key` is a file holding the key, or the key itself as 64 hexadecimal characters.
    PieceFeed(Mirrors const& location, fs::path const& publisher_key,
        std::optional<std::uint32_t> first = std::nullopt, std::optional<std::uint32_t> last = std::nullopt, bool allow_baseline = false,
        std::uint64_t oldest_generation = 0);
    ~PieceFeed();
    Json const& catalog() const;
    // The verified selection: the snapshot's catalog entry and index, and the ranges after it.
    Json const& snapshot_entry() const;
    Json const& snapshot_index() const;
    std::vector<Json> const& ranges() const;
    // The ledger whose state the feed starts from, and the last ledger it returns.
    Header const& snapshot() const;
    std::uint32_t last() const;
    // The snapshot's objects in key order.
    // Visits the snapshot's objects in key order; each may be moved from.
    void snapshot_objects(std::function<void(ObjectRecord&&)> const& visit) const;
    // Records of snapshot().sequence + 1 through last(), in order; false after the last.
    bool next(LedgerRecord& out);
    // Continue with `sequence` instead (not below the next record): whole segments before it
    // are not fetched.
    void skip_to(std::uint32_t sequence);
    // Bytes read from the publication so far.
    std::uint64_t bytes_read() const;
};
}
