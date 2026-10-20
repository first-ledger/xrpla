// Streams verified ledgers from a piece publication (piece_feed.hpp).
#include "piece_feed.hpp"
#include "piece_format.hpp"
#include <sodium.h>
#include <zstd.h>
#include <algorithm>
#include <atomic>
#include <deque>
#include <future>
#include <map>
#include <set>
#include <thread>

namespace xrpla::core {
using namespace pieces;
namespace {
std::string sha_hex(V bytes) { return hx::hex(hx::sha(bytes)); }
std::string lower(std::string text) { for (char& c : text) if (c >= 'A' && c <= 'F') c += 'a' - 'A'; return text; }
}
// Signed JSON is parsed only after its signature or digest checks out, rejecting duplicate keys
// (sync_pieces parses the interval snapshots' indexes with it too).
Json strict_json(std::string const& bytes) {
    std::vector<std::set<std::string>> keys;
    return Json::parse(bytes, [&keys](int, Json::parse_event_t event, Json& value) {
        if (event == Json::parse_event_t::object_start) keys.emplace_back();
        else if (event == Json::parse_event_t::object_end) keys.pop_back();
        else if (event == Json::parse_event_t::key && !keys.back().insert(value.get<std::string>()).second) throw Error("Duplicate JSON key in publication metadata");
        return true;
    });
}
namespace {
Bytes decompress(V frame, U limit) {
    auto size = ZSTD_getFrameContentSize(frame.data(), frame.size());
    if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN || size > limit) throw Error("Snapshot part size");
    Bytes raw(size, '\0');
    auto n = ZSTD_decompress(raw.data(), raw.size(), frame.data(), frame.size());
    if (ZSTD_isError(n) || n != size) throw Error("Snapshot part decode failed");
    return raw;
}
// A segment fetched, checked against its container's directory and prepared for decoding
// (every dictionary entry rendered, which the page decoders then share): its archive reads
// its bytes, so neither moves.
struct Prepared {
    Bytes bytes;
    std::unique_ptr<hx::Archive> archive;
};
struct Pending { std::size_t range, segment; std::future<std::unique_ptr<Prepared>> prepared; };
}

struct PieceFeed::Impl {
    Source source;
    Bytes key;
    Json catalog, index, entry;
    Header base;
    std::vector<Json> ranges;
    std::map<U, std::string> range_ends;  // each range's last ledger: its signed hash (lowercase)
    std::vector<Container> containers;  // parsed lazily, one per range
    U last = 0, next = 0;
    std::atomic<std::uint64_t> read{0};
    // Position: the segment being decoded, its archive, the ledgers of its current page.
    std::size_t range = 0, segment = 0;
    std::unique_ptr<Prepared> current;
    hx::Archive* archive = nullptr;  // current's
    // Segments fetched and prepared ahead of decoding, each on a thread of its own: preparing
    // one on the reading thread, when the one before had run out, held a restore's ledgers
    // back for about a quarter of its time.
    std::deque<Pending> ahead;
    static constexpr std::size_t window = 2;
    // A segment's pages decode (and their ledgers parse) on several threads, a few pages ahead
    // of the one being read: one thread decoding every page held a restore to about 400
    // ledgers a second. Declared after the archive they read, so they are waited for first.
    U scheduled = 0;  // the next page to decode
    std::deque<std::future<std::vector<LedgerRecord>>> decoding;
    std::vector<LedgerRecord> records;
    std::size_t record = 0;
    std::size_t const depth = std::clamp<std::size_t>(std::thread::hardware_concurrency() / 2, 2, 8);
    void schedule_pages() {
        while (archive && decoding.size() < depth && scheduled < archive->pages()) {
            decoding.push_back(std::async(std::launch::async, [a = archive, index = scheduled] {
                auto rows = a->decodePage(index);
                std::vector<LedgerRecord> out;
                out.reserve(rows.size());
                for (auto const& [seq, raw] : rows) {
                    out.push_back(parse_ledger_record(raw));
                    if (out.back().sequence != seq) throw Error("Ledger record sequence differs");
                }
                return out;
            }));
            ++scheduled;
        }
    }
    // Waits out pages still decoding (before the archive they read changes).
    void settle() {
        for (auto& f : decoding) f.wait();
        decoding.clear(); records.clear(); record = 0;
    }

    explicit Impl(Mirrors const& location) : source(location) {}
    // An object the catalog names, from `from` (one mirror, or the publication's mirrors).
    std::string fetch(Source const& from, std::string const& object, std::string const& sha, U limit) {
        auto bytes = from.read(object, limit, {}, [&](std::string const& bytes) {
            if (sha_hex(bytes) != sha) throw Error(object + " differs from the catalog");
        });
        read += bytes.size();
        return bytes;
    }
    std::string fetch(std::string const& object, std::string const& sha, U limit) { return fetch(source, object, sha, limit); }
    Container const& container(std::size_t r) {
        while (containers.size() <= r) {
            auto const& info = ranges[containers.size()];
            auto object = info.at("object").get<std::string>();
            // Its head (which gives the directory's length) and directory, from one mirror.
            std::optional<Container> c;
            source.each_mirror(object, [&](Source const& mirror) {
                auto head = mirror.read_range(object, 0, Container::head);
                auto directory = mirror.read_range(object, 0, Container::directory_bytes(head));
                read += head.size() + directory.size();
                c = Container::parse_directory(directory, info.at("directory_sha256").get<std::string>(), info.at("size").get<U>());
            });
            if (c->first != info.at("first").get<U>() || c->last != info.at("last").get<U>()) throw Error("Container range differs from catalog");
            containers.push_back(std::move(*c));
        }
        return containers[r];
    }
    // Keeps up to `window` segments downloading and being prepared, from the current position on.
    void schedule() {
        auto r = ahead.empty() ? range : ahead.back().range;
        auto s = ahead.empty() ? segment : ahead.back().segment + 1;
        while (ahead.size() < window && r < ranges.size()) {
            auto const& c = container(r);
            if (s >= c.segments.size()) { ++r; s = 0; continue; }
            auto const& segment_row = c.segments[s];
            if (segment_row.first > last) break;
            auto object = ranges[r].at("object").get<std::string>();
            ahead.push_back({r, s, std::async(std::launch::async, [this, object, segment_row] {
                auto prepared = std::make_unique<Prepared>();
                prepared->bytes = source.read_range(object, segment_row.offset, segment_row.length, [&](std::string const& bytes) {
                    if (hx::sha(bytes) != segment_row.digest) throw Error("Segment differs from its container directory");
                });
                read += prepared->bytes.size();
                prepared->archive = std::make_unique<hx::Archive>(V(prepared->bytes), true);
                if (prepared->archive->first != segment_row.first || prepared->archive->count != segment_row.count)
                    throw Error("Segment range differs from its container directory");
                prepared->archive->prepareAll();
                return prepared;
            })});
            ++s;
        }
    }
    // Opens the next segment holding `next`; false when none is left.
    bool open_segment() {
        settle(); archive = nullptr; current.reset(); scheduled = 0;
        for (;;) {
            if (range >= ranges.size()) return false;
            auto const& c = container(range);
            if (segment >= c.segments.size()) { ++range; segment = 0; ahead.clear(); continue; }
            auto const& s = c.segments[segment];
            if (s.first > last) return false;
            if (s.first + s.count <= next) { ++segment; ahead.clear(); continue; }  // skipped: never fetched
            break;
        }
        schedule();
        if (ahead.empty() || ahead.front().range != range || ahead.front().segment != segment) { ahead.clear(); schedule(); }
        current = ahead.front().prepared.get();
        ahead.pop_front();
        archive = current->archive.get();
        auto const& s = container(range).segments[segment];
        if (next > s.first) scheduled = archive->pageOf(next);  // resuming inside the segment
        ++segment;
        schedule();
        return true;
    }
    // The catalog `mirror` serves, checked against the pinned key, and what it selects: the
    // snapshot to start from and the ranges after it, through `last_wanted` (default: the tip).
    void select(Source const& mirror, std::optional<std::uint32_t> first, std::optional<std::uint32_t> last_wanted, bool allow_baseline,
                std::uint64_t oldest_generation, std::optional<std::uint64_t>& older) {
        ranges.clear(); range_ends.clear();
        auto pointer = strict_json(mirror.read("catalog/current.json", 1 << 16));
        auto object = pointer.at("object").get<std::string>();
        auto raw = mirror.read(object, 64 << 20);
        if (sha_hex(raw) != pointer.at("sha256").get<std::string>()) throw Error("Catalog pointer differs from its generation");
        auto signature = mirror.read(object + ".sig", 64);
        if (signature.size() != crypto_sign_BYTES || crypto_sign_verify_detached(reinterpret_cast<unsigned char const*>(signature.data()),
                reinterpret_cast<unsigned char const*>(raw.data()), raw.size(), reinterpret_cast<unsigned char const*>(key.data())) != 0)
            throw Error("Catalog signature differs from the pinned publisher key");
        catalog = strict_json(raw);
        if (catalog.at("format") != "xrpla-catalog-v1" || catalog.at("generation") != pointer.at("generation")) throw Error("Unsupported or inconsistent catalog");
        // An older generation, signed as it once was: a stale copy of the pointer, or a mirror
        // behind the others.
        if (auto generation = catalog.at("generation").get<std::uint64_t>(); generation < oldest_generation) {
            older = std::max(older.value_or(0), generation);
            throw OlderCatalog("Catalog generation " + std::to_string(generation) + " is older than generation " + std::to_string(oldest_generation), generation);
        }
        // The ranges, then the tail's fragments (the piece still filling, published in parts; a
        // catalog without a tail lists only ranges): one chain.
        std::vector<Json> all(catalog.at("ranges").begin(), catalog.at("ranges").end());
        if (catalog.contains("tail")) for (auto const& fragment : catalog.at("tail").at("ranges")) all.push_back(fragment);
        bool baseline = all.empty() && allow_baseline && !first && !last_wanted;
        if (all.empty() && !baseline) throw Error("The publication has no ledgers yet");
        for (std::size_t i = 1; i < all.size(); ++i)
            if (all[i].at("first").get<U>() != all[i - 1].at("last").get<U>() + 1 || all[i].at("parent_hash") != all[i - 1].at("last_hash"))
                throw Error("Catalog ranges are not a chain");
        std::optional<Json> selected;
        if (baseline) {
            // A new publication with only its baseline snapshot: no ledgers after it yet.
            last = catalog.at("first").get<U>() - 1;
            for (auto const& s : catalog.at("snapshots")) if (s.at("seq").get<U>() == last) selected = s;
        } else {
            U from = first.value_or(all.front().at("first").get<U>()), tip = all.back().at("last").get<U>();
            last = last_wanted.value_or(static_cast<std::uint32_t>(tip));
            if (from < all.front().at("first").get<U>() || last > tip || from > last) throw Error("Requested ledgers are not published");
            // The newest snapshot below `from` that starts a published range.
            for (auto const& s : catalog.at("snapshots")) {
                U seq = s.at("seq");
                bool starts = std::any_of(all.begin(), all.end(), [&](Json const& r) { return r.at("first").get<U>() == seq + 1; });
                if (seq < from && starts && (!selected || selected->at("seq").get<U>() < seq)) selected = s;
            }
        }
        if (!selected) throw Error("No published snapshot precedes the requested ledgers");
        U seq = selected->at("seq");
        entry = *selected;
        for (auto const& r : all) if (r.at("last").get<U>() > seq && r.at("first").get<U>() <= last) ranges.push_back(r);
        for (auto const& r : ranges) range_ends[r.at("last").get<U>()] = lower(r.at("last_hash").get<std::string>());
        index = strict_json(fetch(mirror, selected->at("index").at("object").get<std::string>(), selected->at("index").at("sha256").get<std::string>(), 16 << 20));
        if (index.at("seq").get<U>() != seq || index.at("format_version") != 2 || index.at("record_format") != "keyed-stobject-v2")
            throw Error("Unsupported snapshot index");
        base = decode_header(from_hex(index.at("ledger_header").get<std::string>()));
        if (base.sequence != seq || lower(base.hash) != lower(selected->at("ledger_hash").get<std::string>()) || lower(base.account_hash) != lower(selected->at("account_hash").get<std::string>()))
            throw Error("Snapshot header differs from the catalog");
        if (!ranges.empty() && lower(ranges.front().at("parent_hash").get<std::string>()) != lower(base.hash)) throw Error("Snapshot does not start the published ranges");
        next = seq + 1;
    }
};

PieceFeed::PieceFeed(Mirrors const& location, fs::path const& publisher_key, std::optional<std::uint32_t> first, std::optional<std::uint32_t> last, bool allow_baseline,
                     std::uint64_t oldest_generation)
    : impl_(std::make_unique<Impl>(location)) {
    auto& m = *impl_;
    m.key = pinned_publisher_key(publisher_key);
    // The mirrors claiming the newest generation are tried first (a claim orders them, and is
    // checked as any catalog is), so one lagging behind the others is not followed while they
    // are newer.
    auto claimed = [](Source const& mirror) { return strict_json(mirror.read("catalog/current.json", 1 << 16)).at("generation").get<std::uint64_t>(); };
    std::optional<std::uint64_t> older;  // the newest generation a mirror offered below oldest_generation
    std::optional<std::string> failure;  // the last failure that was not an older catalog
    try {
        m.source.each_mirror("catalog/current.json", [&](Source const& mirror) {
            try { m.select(mirror, first, last, allow_baseline, oldest_generation, older); }
            catch (OlderCatalog const&) { throw; }
            catch (std::exception const& error) { failure = error.what(); throw; }
        }, claimed);
    } catch (...) {
        if (!older) throw;
        auto detail = "generation " + std::to_string(*older) + " is older than generation " + std::to_string(oldest_generation);
        // Older is nothing newer only when that is all any mirror offered; otherwise a mirror
        // that may be newer failed, and that failure is reported.
        if (!failure) throw OlderCatalog("Catalog " + detail, *older);
        throw Error(*failure + " (another mirror's catalog " + detail + ")");
    }
}
PieceFeed::~PieceFeed() = default;
Json const& PieceFeed::catalog() const { return impl_->catalog; }
Json const& PieceFeed::snapshot_entry() const { return impl_->entry; }
Json const& PieceFeed::snapshot_index() const { return impl_->index; }
std::vector<Json> const& PieceFeed::ranges() const { return impl_->ranges; }
Header const& PieceFeed::snapshot() const { return impl_->base; }
std::uint32_t PieceFeed::last() const { return static_cast<std::uint32_t>(impl_->last); }
std::uint64_t PieceFeed::bytes_read() const { return impl_->read; }

void PieceFeed::snapshot_objects(std::function<void(ObjectRecord&&)> const& visit) const {
    auto& m = *impl_;
    std::vector<Json> parts;
    for (auto const& part : m.index.at("parts")) if (part.at("records").get<U>()) parts.push_back(part);
    // Parts download, decompress and parse into records, several at once, while the previous
    // one is visited (parts are about 10 MB compressed, 70,000 objects; one thread doing this
    // for every part held a state load back).
    std::deque<std::future<std::vector<ObjectRecord>>> ahead;
    std::size_t queued = 0, depth = std::clamp<std::size_t>(std::thread::hardware_concurrency() / 2, 4, 16);
    auto fill = [&] {
        while (ahead.size() < depth && queued < parts.size()) {
            auto const& part = parts[queued++];
            ahead.push_back(std::async(std::launch::async, [&m, part] {
                auto raw = decompress(m.fetch(part.at("object").get<std::string>(), part.at("sha256").get<std::string>(), part.at("size").get<U>()), 1ULL << 32);
                V view(raw);
                std::vector<ObjectRecord> records;
                records.reserve(part.at("records").get<U>());
                for (U at = 0; at < view.size();) {
                    if (view.size() - at < 36) throw Error("Truncated snapshot record");
                    auto key = view.substr(at, 32); U length = le(view, at + 32, 4);
                    if (length > view.size() - at - 36) throw Error("Truncated snapshot record");
                    if (!records.empty() && key <= V(records.back().key)) throw Error("Snapshot keys out of order");
                    records.push_back({Bytes(key), Bytes(view.substr(at + 36, length))});
                    at += 36 + length;
                }
                if (records.size() != part.at("records").get<U>()) throw Error("Snapshot part record count differs");
                return records;
            }));
        }
    };
    Bytes previous;
    for (std::size_t p = 0; p < parts.size(); ++p) {
        fill();
        auto records = ahead.front().get(); ahead.pop_front();
        // Within a part the task checked the order; across parts, here.
        if (!records.empty() && !previous.empty() && records.front().key <= previous) throw Error("Snapshot keys out of order");
        if (!records.empty()) previous = records.back().key;
        for (auto& record : records) visit(std::move(record));
    }
}

bool PieceFeed::next(LedgerRecord& out) {
    auto& m = *impl_;
    if (m.next > m.last) return false;
    for (;;) {
        if (m.record < m.records.size()) {
            auto& record = m.records[m.record++];
            auto seq = record.sequence;
            if (seq < m.next) continue;
            if (seq != m.next) throw Error("Ledger missing from publication");
            out = std::move(record);
            // A range's last ledger must hash to the catalog's signed value.
            if (auto end = m.range_ends.find(seq); end != m.range_ends.end() && lower(decode_header(out.header).hash) != end->second)
                throw Error("Ledger hash differs from the catalog");
            ++m.next;
            return true;
        }
        if (m.archive && (!m.decoding.empty() || m.scheduled < m.archive->pages())) {
            m.schedule_pages();
            m.records = m.decoding.front().get();
            m.decoding.pop_front();
            m.record = 0;
            m.schedule_pages();
            continue;
        }
        if (!m.open_segment()) throw Error("Publication ends before ledger " + std::to_string(m.next));
    }
}

void PieceFeed::skip_to(std::uint32_t sequence) {
    auto& m = *impl_;
    if (sequence < m.next) throw Error("Cannot skip backwards");
    m.next = sequence;
    // Drop a current segment (and fetches) that end before the new position.
    if (m.archive && m.archive->first + m.archive->count <= m.next) { m.settle(); m.archive = nullptr; m.current.reset(); m.scheduled = 0; m.ahead.clear(); }
    else if (m.archive && (m.records.empty() || m.records.back().sequence < m.next)) { m.settle(); m.scheduled = m.archive->pageOf(m.next); }
}
}
