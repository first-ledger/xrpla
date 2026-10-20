// Write a v2 state snapshot at a ledger boundary from a verified v2 archive.
// The piece publisher uses it for the snapshot at the start of each bundle
// (docs/internal/SIZE-AND-FOOTPRINT.md). State is rebuilt by the canonical verifier,
// which checks every ledger's state and transaction roots on the way, unless
// the archive's follower already wrote the snapshot as it passed the boundary.
#include <xrpla/core/verify.hpp>
#include <algorithm>
#include <chrono>
#include <deque>
#include <future>
#include <iostream>
#include <map>
#include <thread>

using namespace xrpla;
using namespace xrpla::core;
namespace {
int run(int argc, char** argv) {
    initialize();
    std::map<std::string, std::string> args;
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 == argc || !args.emplace(argv[i], argv[i + 1]).second) throw Error("Expected unique --option value pairs");
    }
    for (auto const& [key, _] : args)
        if (key != "--archive" && key != "--publisher-key" && key != "--at" && key != "--out" && key != "--scratch" && key != "--compression-level" && key != "--jobs" && key != "--compact-every")
            throw Error("Unknown option " + key);
    auto root = fs::canonical(args.at("--archive"));
    auto out = fs::absolute(args.at("--out")).lexically_normal();
    auto scratch = fs::absolute(args.at("--scratch")).lexically_normal();
    // One pass can write several snapshots: --at takes ascending, comma-separated ledgers.
    std::vector<std::uint32_t> targets;
    for (std::size_t start = 0; start <= args.at("--at").size();) {
        auto end = args.at("--at").find(',', start);
        targets.push_back(static_cast<std::uint32_t>(std::stoul(args.at("--at").substr(start, end - start))));
        if (end == std::string::npos) break;
        start = end + 1;
    }
    if (!std::is_sorted(targets.begin(), targets.end()) || std::adjacent_find(targets.begin(), targets.end()) != targets.end())
        throw Error("--at ledgers must be ascending and distinct");
    auto at = targets.back();
    int level = args.contains("--compression-level") ? std::stoi(args.at("--compression-level")) : 19;
    if (level < 1 || level > 19) throw Error("Compression level must be 1-19");
    // Parts compress in parallel; a single zstd 19 stream would otherwise set the pace.
    unsigned jobs = args.contains("--jobs") ? static_cast<unsigned>(std::stoul(args.at("--jobs"))) : std::clamp(std::thread::hardware_concurrency(), 1u, 8u);
    if (jobs < 1 || jobs > 64) throw Error("--jobs must be 1-64");
    // Replays compact the scratch node store every N ledgers (default 16384) to bound its size.
    std::uint64_t compact_every = args.contains("--compact-every") ? std::stoull(args.at("--compact-every")) : 16384;
    if (!compact_every) throw Error("--compact-every must be positive");
    using Clock = std::chrono::steady_clock;
    auto seconds = [](Clock::time_point since) { return std::chrono::duration<double>(Clock::now() - since).count(); };
    // Phase timings go to stderr: every stdout line is a snapshot descriptor for the catalog.
    auto timing = [](Json const& value) { std::cerr << value.dump() << std::endl; };
    if (fs::exists(scratch) && !fs::is_empty(scratch)) throw Error("Scratch directory must be empty");
    auto signed_manifest = read_manifest(Source(root.string()), read_file(args.at("--publisher-key"), 1024));
    auto const& manifest = signed_manifest.manifest;
    if (manifest.version != 2 || !manifest.dicts.empty() || manifest.chunks.empty()) throw Error("Requires dictionary-free v2 archive");
    if (at > manifest.tip) throw Error("Snapshot ledger is past the archive tip");
    auto plan = make_plan(manifest, {manifest.chunks.front().first, std::max(at, manifest.chunks.front().first)});
    if (targets.front() < plan.snapshot.seq) throw Error("Snapshot ledger precedes the archive's baseline");
    auto check = [&](Object const& object) {
        validate_key(object.object);
        if (!matches(root / object.object, object)) throw Error("Archive object digest differs: " + object.object);
    };

    // Snapshots the follower wrote as it passed each boundary (xrpla publish
    // --interval-snapshots) are copied from the archive, checked against their digests,
    // instead of replayed. Descriptors go to stdout in ledger order once all are written.
    std::map<std::uint32_t, Json> written;
    auto print = [&] { for (auto const& [seq, result] : written) std::cout << result.dump() << std::endl; };
    std::vector<std::uint32_t> replay;
    auto made = root / "interval-snapshots";
    auto not_copied = [&](std::uint32_t seq, std::string const& reason) {
        timing({{"phase", "not_copied"}, {"seq", seq}, {"reason", reason}});
        replay.push_back(seq);
    };
    for (auto seq : targets) {
        auto descriptor_path = made / (std::to_string(seq) + ".json");
        if (!fs::exists(descriptor_path)) { replay.push_back(seq); continue; }
        // First, which ledger the descriptor claims, against this archive's at that sequence:
        // one unreadable, or of another ledger (a fork's, from a source off the quorum's chain),
        // is not copied, the snapshot is replayed from this archive instead. Only one claiming
        // this archive's very ledger has its objects checked, and any of those that differs
        // from what it claims is an error (a damaged archive), never a reason to replay.
        Json result;
        try {
            result = Json::parse(read_file(descriptor_path, 1 << 20));
            (void)result.at("seq").get<std::uint32_t>(); (void)result.at("ledger_hash").get<std::string>();
            (void)result.at("account_hash").get<std::string>();
        } catch (std::exception const& error) {
            not_copied(seq, std::string("Interval snapshot descriptor is unreadable: ") + error.what());
            continue;
        }
        // The archive's own ledger at `seq`: the header its chunk holds.
        auto chunk = std::find_if(manifest.chunks.begin(), manifest.chunks.end(), [&](auto const& c) { return c.first <= seq && seq <= c.last; });
        if (chunk == manifest.chunks.end()) { replay.push_back(seq); continue; }
        check(chunk->file);
        Bytes header;
        read_chunk(root / chunk->file.object, {}, [&](LedgerRecord&& record) { if (record.sequence == seq) header = record.header; });
        if (header.empty()) { not_copied(seq, "Archive chunk does not hold the ledger"); continue; }
        auto decoded = decode_header(header);
        if (result.at("seq").get<std::uint32_t>() != seq || result.at("ledger_hash").get<std::string>() != decoded.hash) {
            not_copied(seq, "Interval snapshot is not this archive's ledger");
            continue;
        }
        // Each object checked against its descriptor before it is read or copied; copied through a
        // synced temporary file, so a crash leaves no short object under its final name.
        auto checked = [&](Json const& object) {
            auto expected = parse_object(object);
            validate_key(expected.object);
            if (!matches(made / expected.object, expected)) throw Error("Interval snapshot object differs: " + expected.object);
            return expected;
        };
        auto copy = [&](Json const& object) {
            auto expected = checked(object);
            fs::create_directories((out / expected.object).parent_path());
            auto temp = out / expected.object; temp += ".part";
            fs::copy_file(made / expected.object, temp, fs::copy_options::overwrite_existing);
            sync_file(temp);
            durable_rename(temp, out / expected.object);
        };
        // It claims this archive's ledger: everything it names must be exactly that.
        auto index_object = checked(result.at("index")).object;
        auto made_index = Json::parse(read_file(made / index_object, 16 * 1024 * 1024));
        if (made_index.at("ledger_header").get<std::string>() != to_hex(header) || result.at("account_hash").get<std::string>() != decoded.account_hash ||
            made_index.at("account_hash").get<std::string>() != decoded.account_hash || made_index.at("seq").get<std::uint32_t>() != seq)
            throw Error("Interval snapshot of this archive's ledger " + std::to_string(seq) + " differs from it");
        copy(result.at("index"));
        for (auto const& part : made_index.at("parts")) copy(part);
        written[seq] = std::move(result);
        timing({{"phase", "copied"}, {"seq", seq}});
    }
    if (replay.empty()) { print(); return 0; }
    targets = replay; at = targets.back();
    LedgerVerifier verifier(scratch);
    auto started = Clock::now();
    check(plan.snapshot.index);
    auto index = Json::parse(read_file(root / plan.snapshot.index.object, 16 * 1024 * 1024));
    if (index.at("format_version") != 2 || index.at("record_format") != "keyed-stobject-v2") throw Error("Unsupported snapshot index");
    for (auto const& part : index.at("parts")) {
        check(parse_object(part));
        std::vector<ObjectRecord> objects;
        read_snapshot_part(root / part.at("object").get<std::string>(), {}, part.at("records"),
                           [&](ObjectRecord&& object) { objects.push_back(std::move(object)); });
        verifier.add_snapshot_objects(objects);
    }
    verifier.finish_snapshot(from_hex(index.at("ledger_header")), plan.snapshot.seq, plan.snapshot.account_hash);
    timing({{"phase", "load"}, {"seconds", seconds(started)}});
    double replay_seconds = 0; std::uint64_t replayed = 0;
    auto write = [&](std::uint32_t seq) {
        if (verifier.header().sequence != seq) throw Error("Snapshot ledger differs from the replayed ledger");
        Json spent;
        written[seq] = write_state_snapshot(verifier, out, level, jobs, &spent);
        spent["phase"] = "write"; spent["seq"] = seq;
        timing(spent);
    };
    std::size_t next = 0;
    while (next < targets.size() && targets[next] == plan.snapshot.seq) write(targets[next++]);
    for (auto const& chunk : manifest.chunks) {
        if (next == targets.size()) break;
        if (chunk.last <= plan.snapshot.seq) continue;
        check(chunk.file);
        read_chunk(root / chunk.file.object, {}, [&](LedgerRecord&& record) {
            if (next == targets.size() || record.sequence <= plan.snapshot.seq || record.sequence > at) return;
            auto applied = Clock::now();
            verifier.advance(record);
            replay_seconds += seconds(applied); ++replayed;
            // Replaying a whole interval would otherwise keep every superseded state node in scratch.
            if (replayed % compact_every == 0) {
                auto compacting = Clock::now();
                auto kept = verifier.compact_nodes();
                timing({{"phase", "compact"}, {"ledger", record.sequence}, {"nodes", kept}, {"seconds", seconds(compacting)}});
            }
            if (record.sequence == targets[next]) write(targets[next++]);
        });
    }
    if (replayed) timing({{"phase", "replay"}, {"ledgers", replayed}, {"seconds", replay_seconds}});
    if (next != targets.size()) throw Error("Archive does not reach every snapshot ledger");
    print();
    return 0;
}
}
int main(int argc, char** argv) {
    try { return run(argc, argv); }
    catch (std::exception const& e) { std::cerr << Json{{"error", e.what()}}.dump() << '\n'; return 1; }
}
