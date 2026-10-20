#include <xrpla/core/verify.hpp>
#include "store.hpp"
#include <fstream>
#include <map>
#include <set>
#include <chrono>
#include <optional>

namespace xrpla::core {
// A journal is private to this verification run. Hashes are retained in memory;
// admission rechecks each file before constructing the capability accepted by
// Store. No writable historical database is opened until replay succeeds.
class AdmissionJournal {
    struct File { fs::path path; std::string hash; std::uint64_t count; };
    fs::path root_;
    std::vector<File> snapshots_, chunks_;
    std::vector<ObjectRecord> snapshot_pending_;
    std::vector<LedgerRecord> pending_;
    std::uint64_t snapshot_bytes_ = 0;
    std::uint64_t bytes_ = 0;
    std::optional<Header> baseline_;
    void flush_snapshot() {
        if (snapshot_pending_.empty()) return;
        auto path = root_ / ("snapshot-" + std::to_string(snapshots_.size()));
        write_snapshot_part(path, snapshot_pending_);
        snapshots_.push_back({path, file_sha256(path), snapshot_pending_.size()});
        snapshot_pending_.clear(); snapshot_bytes_ = 0;
    }
    void flush() {
        if (pending_.empty()) return;
        auto path = root_ / ("chunk-" + std::to_string(chunks_.size()));
        write_chunk(path, pending_);
        chunks_.push_back({path, file_sha256(path), pending_.size()});
        pending_.clear(); bytes_ = 0;
    }
    static void check(File const& file) { safe_cache_path(file.path); if (file_sha256(file.path) != file.hash) throw Error("Admission journal changed after verification"); }
public:
    explicit AdmissionJournal(fs::path const& dir) : root_(dir / (".admission-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {
        safe_cache_path(root_); if (!fs::create_directory(root_)) throw Error("Admission journal already exists");
    }
    ~AdmissionJournal() { std::error_code ec; fs::remove_all(root_, ec); }
    void snapshot_object(ObjectRecord const& record) {
        snapshot_pending_.push_back(record); snapshot_bytes_ += record.object.size() + 36;
        if (snapshot_bytes_ >= snapshot_part_bytes) flush_snapshot();
    }
    void baseline(LedgerVerifier const& verifier, bool captured) {
        baseline_ = verifier.header();
        if (!captured) verifier.state_objects([&](ObjectRecord const& record) { snapshot_object(record); });
        flush_snapshot();
    }
    void append(VerifiedLedger const& ledger) {
        auto const& record = ledger.record();
        bytes_ += record.header.size() + 16;
        for (auto const& tx : record.transactions) bytes_ += tx.transaction.size() + tx.metadata.size() + 8;
        for (auto const& object : record.patches) bytes_ += object.object.size() + 36;
        pending_.push_back(record);
        if (pending_.size() >= 256 || bytes_ >= 64 * 1024 * 1024) flush();
    }
    std::uint64_t admit(Store& store, Range range) {
        flush();
        if (baseline_) store.initialize(*baseline_, range, [&](auto const& visit) {
            for (auto const& part : snapshots_) { check(part); read_snapshot_part(part.path, {}, part.count, [&](ObjectRecord&& obj) { visit(obj); }); }
        });
        std::uint64_t added = 0;
        for (auto const& chunk : chunks_) {
            check(chunk);
            read_chunk(chunk.path, {}, [&](LedgerRecord&& record) {
                auto header = decode_header(record.header);
                auto tx = index_transactions(record);
                if (store.append(VerifiedLedger(std::move(header), std::move(record), std::move(tx)))) ++added;
            });
        }
        return added;
    }
};
namespace {
Json replay(VerifyOptions const& options, Selection const& selected,
    std::function<void(LedgerVerifier const&)> const& baseline = {},
    std::function<void(VerifiedLedger const&)> const& ledger_ready = {},
    std::optional<Header> const& checkpoint = {},
    std::function<void(ObjectRecord const&)> const& snapshot_object = {}) {
    auto const& [manifest, plan] = selected;
    if (manifest.version != 2) throw Error("Full verification requires archive v2 keyed snapshots and object patches; v1 supports integrity checks only");
    std::string anchor;
    if (!options.trust_ledger_hash.empty()) {
        anchor = to_hex(from_hex(options.trust_ledger_hash));
        validate_hash(anchor);
    }
    auto root = options.dir / "archive";
    std::map<std::string, Bytes> dictionaries;
    dictionaries.emplace(std::string(64, '0'), Bytes{});
    for (auto const& d : manifest.dicts) dictionaries.emplace(d.sha256, read_file(root / d.object, 16 * 1024 * 1024));
    auto dictionary = [&](std::string const& hash) -> Bytes const& {
        auto it = dictionaries.find(hash);
        if (it == dictionaries.end()) throw Error("Archive references an unlisted dictionary");
        return it->second;
    };
    auto index = Json::parse(read_file(root / plan.snapshot.index.object, 16 * 1024 * 1024));
    if (index.at("format_version") != 2 || index.at("record_format") != "keyed-stobject-v2")
        throw Error("Unsupported snapshot record format");
    auto const& dict = dictionary(index.at("dictionary_sha256").get<std::string>());
    LedgerVerifier verifier(options.dir / ".verification-nodes");
    std::uint64_t object_count = 0;
    for (auto const& part : index.at("parts")) {
        auto records = part.at("records");
        if (!records.is_number_unsigned()) throw Error("Invalid snapshot record count");
        auto count = records.get<std::uint64_t>();
        if (count > UINT64_MAX - object_count) throw Error("Snapshot object count overflow");
        object_count += count;
        if (!checkpoint) read_snapshot_part(root / parse_object(part).object, dict, count,
            [&](ObjectRecord&& record) {
                verifier.add_snapshot_object(record);
                if (snapshot_object) snapshot_object(record);
            });
    }
    auto initial = decode_header(from_hex(index.at("ledger_header").get<std::string>()));
    if (checkpoint) verifier.resume(checkpoint->raw);
    else initial = verifier.finish_snapshot(initial.raw, plan.snapshot.seq, plan.snapshot.account_hash);
    std::uint32_t anchor_sequence = anchor == initial.hash ? initial.sequence : 0;
    if (checkpoint && checkpoint->hash == anchor) anchor_sequence = checkpoint->sequence;
    bool checkpoint_seen = !checkpoint;
    std::set<std::string> paths;
    for (auto const& object : plan.objects) paths.insert(object.object);
    std::uint64_t verified = 0;
    for (auto const& chunk : manifest.chunks) {
        if (!paths.contains(chunk.file.object)) continue;
        // Wholly before a checkpoint: verified when it was stored, nothing in it is applied again,
        // so it is not read either (the chunk holding the checkpoint is, to match its header).
        if (checkpoint && chunk.last < checkpoint->sequence) continue;
        auto path = root / chunk.file.object;
        std::vector<std::pair<std::uint32_t, std::uint32_t>> times;
        if (chunk.time_index) times = decode_times(chunk, read_file(root / chunk.time_index->object, 65536 * 8));
        std::ifstream stream(path, std::ios::binary);
        stream.seekg(18);
        Bytes hash(32, '\0'); stream.read(hash.data(), 32);
        if (!stream) throw Error("Truncated chunk header");
        auto info = read_chunk(path, dictionary(to_hex(hash)), [&](LedgerRecord&& ledger) {
            if (!times.empty() && decode_header(ledger.header).close_time != times.at(ledger.sequence - chunk.first).second) throw Error("Ledger close time disagrees with signed time index");
            if (ledger.sequence <= initial.sequence) return;
            if (checkpoint && ledger.sequence <= checkpoint->sequence) {
                if (ledger.sequence == checkpoint->sequence) {
                    if (ledger.header != checkpoint->raw) throw Error("Archive disagrees with the stored checkpoint");
                    checkpoint_seen = true;
                }
                return;
            }
            if (ledger.sequence == plan.range.first && baseline) baseline(verifier);
            auto const sequence = ledger.sequence;
            // The verified ledger takes the record over (nothing reads it here afterwards): no copy.
            auto verified_ledger = [&] {
                try { return verifier.apply(std::move(ledger)); }
                catch (std::exception const& e) { throw Error("Ledger " + std::to_string(sequence) + ": " + e.what()); }
            }();
            ++verified;
            if (verifier.header().hash == anchor) anchor_sequence = sequence;
            if (sequence >= plan.range.first && sequence <= plan.range.last && ledger_ready)
                ledger_ready(verified_ledger);
            if (sequence == plan.range.last) verifier.persist();
        });
        if (info.version != 2 || info.first != chunk.first || info.last != chunk.last)
            throw Error("Chunk header disagrees with manifest");
        if ((!checkpoint || chunk.last >= checkpoint->sequence) &&
            (verifier.header().sequence != chunk.last || verifier.header().hash != chunk.last_ledger_hash))
            throw Error("Chunk final ledger hash disagrees with manifest");
    }
    auto const& final = verifier.header();
    if (!checkpoint_seen) throw Error("Archive does not include the stored checkpoint");
    if (final.sequence < plan.range.last) throw Error("Verified chain does not cover requested range");
    if (final.sequence == manifest.tip && final.hash != manifest.tip_hash) throw Error("Manifest tip hash mismatch");
    if (!anchor.empty() && !anchor_sequence) throw Error("Trusted ledger hash is not in the verified chain");
    if (!anchor.empty() && anchor_sequence < plan.range.last)
        throw Error("Trusted ledger hash must be at or after the requested range end to authenticate its ancestry");
    return {{"verification", "canonical-ledger-roots-and-parent-chain"}, {"ledger_hashes_verified", true},
            {"archive_version", 2}, {"snapshot_base", initial.sequence}, {"snapshot_objects", object_count},
            {"range", {{"first", plan.range.first}, {"last", plan.range.last}}},
            {"verified_through", final.sequence}, {"last_ledger_hash", final.hash}, {"ledgers_replayed", verified},
            {"resumed_from", checkpoint ? checkpoint->sequence : 0},
            {"trust_anchor", anchor.empty() ? "publisher-signed-manifest" : anchor}};
}
} // namespace
Json verify_full(VerifyOptions const& options) {
    DirectoryLock lock(options.dir);
    auto selection = load_selection(options.dir, options.publisher_key);
    return replay(options, selection);
}
Json build_store(VerifyOptions const& options) {
    DirectoryLock lock(options.dir);
    auto selection = load_selection(options.dir, options.publisher_key);
    std::optional<Header> checkpoint;
    if (fs::exists(options.dir / "store" / "CURRENT")) {
        Store existing(options.dir / "store", false);
        auto state = existing.initialized() ? existing.status() : Json{{"complete_ledgers", nullptr}};
        if (!state.at("complete_ledgers").is_null()) {
            auto const& range = state.at("complete_ledgers");
            if (range.at("first") != selection.plan.range.first) throw Error("Store has a different baseline; extend with the same range start");
            auto last = range.at("last").get<std::uint32_t>();
            if (last > selection.plan.range.last) throw Error("Build range would shrink existing history");
            // A store made by an older version has no persisted verifier nodes.
            auto files = existing.checkpoint_files();
            bool same_archive = !files.is_null();
            if (same_archive) for (auto const& [name, hash] : files.items()) {
                bool found = false;
                for (auto const& object : selection.plan.objects) if (object.object == name && object.sha256 == hash.get<std::string>()) { found = true; break; }
                if (!found) { same_archive = false; break; }
            }
            if (same_archive && fs::exists(options.dir / ".verification-nodes" / "CURRENT")) {
                auto candidate = decode_header(existing.header(last));
                LedgerVerifier probe(options.dir / ".verification-nodes");
                try { probe.resume(candidate.raw); checkpoint = candidate; }
                catch (Error const& error) {
                    if (std::string(error.what()) != "Verification checkpoint nodes are missing") throw;
                }
            }
        }
    }
    AdmissionJournal journal(options.dir);
    // Capture the same bytes consumed by the verifier, not a later reread of
    // mutable archive files. Seal only after its snapshot root is verified;
    // admission still waits until the entire replay and trust anchor succeed.
    bool const direct_snapshot = !checkpoint &&
        static_cast<std::uint64_t>(selection.plan.snapshot.seq) + 1 == selection.plan.range.first;
    auto result = replay(options, selection,
        [&](LedgerVerifier const& verifier) { journal.baseline(verifier, direct_snapshot); },
        [&](VerifiedLedger const& ledger) { journal.append(ledger); }, checkpoint,
        [&](ObjectRecord const& record) { if (direct_snapshot) journal.snapshot_object(record); });
    Store store(options.dir / "store", true);
    auto added = journal.admit(store, selection.plan.range);
    Json files = Json::object();
    for (auto const& object : selection.plan.objects) files[object.object] = object.sha256;
    store.finish_admission(files);
    result["store"] = store.status();
    result["ledgers_added"] = added;
    result["baseline_source"] = checkpoint ? "checkpoint" : direct_snapshot ? "verified-snapshot-stream" : "reconstructed-prelude-state";
    return result;
}
} // namespace xrpla::core
