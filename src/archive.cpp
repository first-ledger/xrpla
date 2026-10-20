#include <xrpla/archive.hpp>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>
#include <charconv>

namespace xrpla {
void safe_cache_path(fs::path const& path) {
    fs::path current;
    for (auto const& component : fs::absolute(path)) {
        current /= component;
        if (fs::is_symlink(fs::symlink_status(current))) throw Error("Symlink in cache: " + current.string());
    }
}
namespace {
constexpr std::uint64_t metadata_limit = 16 * 1024 * 1024;
void download_all(Source const& source, std::vector<Object> const& objects, fs::path const& dir, unsigned jobs) {
    std::atomic<std::size_t> next{0};
    std::atomic<bool> failed{false};
    std::exception_ptr failure;
    std::mutex mutex;
    // Different object keys can share a hash. Serialize those transfers by hash
    // to prevent workers sharing a .part path; manifests normally never do this.
    std::mutex transfer_locks[64];
    std::vector<std::jthread> workers;
    for (unsigned i = 0; i < jobs; ++i) workers.emplace_back([&] {
        while (!failed.load()) {
            auto index = next.fetch_add(1);
            if (index >= objects.size()) break;
            try {
                auto const& object = objects[index];
                auto stripe = std::hash<std::string>{}(object.sha256) % 64;
                std::lock_guard lock(transfer_locks[stripe]);
                source.download(object, dir);
            } catch (...) {
                std::lock_guard lock(mutex);
                if (!failure) failure = std::current_exception();
                failed.store(true);
            }
        }
    });
    workers.clear(); // jthread joins before failure is examined
    if (failure) std::rethrow_exception(failure);
}
std::string encode_signature(std::string const& sig) {
    constexpr char chars[] = "0123456789abcdef";
    std::string out;
    for (unsigned char c : sig) { out += chars[c >> 4]; out += chars[c & 15]; }
    return out;
}
std::string decode_signature(std::string const& hex) {
    if (hex.size() != 128 || hex.find_first_not_of("0123456789abcdef") != std::string::npos)
        throw Error("Invalid receipt signature");
    std::string sig;
    for (std::size_t i = 0; i < hex.size(); i += 2)
        sig += static_cast<char>(std::stoul(hex.substr(i, 2), nullptr, 16));
    return sig;
}
Json result(Plan const& plan, std::size_t count) {
    return {{"verification", "publisher-signature-and-object-sha256"}, {"ledger_hashes_verified", false},
            {"range", {{"first", plan.range.first}, {"last", plan.range.last}}},
            {"snapshot_base", plan.snapshot.seq}, {"objects", count}};
}
Json read_receipt(fs::path const& dir) {
    auto pointer_path = dir / "archive" / "selection.json";
    safe_cache_path(pointer_path);
    return Json::parse(read_file(pointer_path, metadata_limit * 2));
}
// The ledgers of the last `count` days: from the earliest ledger in the archive whose close
// time is at or after the cutoff, through the tip. Endpoints do not bound every close time in
// older mainnet history (decode_times), so that ledger is the first qualifying one in ledger
// order, and the range covers every qualifying ledger, including intervening ones whose time
// falls below the cutoff. Chunks are therefore searched oldest first, up to the first holding
// a qualifying ledger. A chunk whose signed first close time is at or after the cutoff ends
// the search at its first ledger without its time index being read (decode_times holds the
// index's first entry to that time, and the chunk is in the range, so its index is fetched
// and checked with it). Other chunks' indexes are read by `jobs` workers, each keeping its HTTP
// connection (a follower archive holds hundreds of thousands of chunks, each one request),
// taking the next chunk in order; the search reads their results in chunk order. No worker
// takes a chunk after the first one found to hold a qualifying ledger (or whose read failed),
// and once the search is decided, reads still under way are stopped rather than waited for.
Range days(Source const& source, Manifest const& manifest, std::string const& input, unsigned jobs) {
    std::uint32_t count = 0;
    auto text = std::string_view(input).substr(0, input.size() - 1);
    auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), count);
    if (ec != std::errc{} || end != text.data() + text.size() || !count) throw Error("Day range must be a positive integer followed by d");
    if (manifest.chunks.empty() || manifest.chunks.back().last != manifest.tip || !manifest.chunks.back().time_index) throw Error("Day ranges require signed v2 close-time indexes");
    for (auto const& chunk : manifest.chunks) if (!chunk.time_index) throw Error("Day ranges require time indexes for all chunks");
    auto tip_time = manifest.chunks.back().last_close_time;
    auto seconds = static_cast<std::uint64_t>(count) * 86400;
    auto cutoff = seconds >= tip_time ? 0 : tip_time - seconds;
    auto const& chunks = manifest.chunks;
    // The first chunk whose signed first close time qualifies: its first ledger, when no chunk
    // before it holds one.
    auto signed_start = static_cast<std::size_t>(std::find_if(chunks.begin(), chunks.end(),
        [&](Chunk const& chunk) { return chunk.first_close_time >= cutoff; }) - chunks.begin());
    struct Result {
        bool done = false;
        std::optional<std::uint32_t> found;  // the chunk's first qualifying ledger
        std::exception_ptr failure;
    };
    std::vector<Result> results(signed_start);
    std::mutex mutex;
    std::condition_variable ready;
    std::atomic<std::size_t> next{0};
    std::atomic<std::size_t> decided{signed_start};  // the search ends there: no chunk after it is taken
    auto decide = [&](std::size_t index) {
        for (auto at = decided.load(); index < at && !decided.compare_exchange_weak(at, index);) {}
    };
    std::vector<std::jthread> workers;
    workers.reserve(std::min<std::size_t>(std::max(1u, jobs), signed_start));
    // Destroyed first: every worker is told to stop before any is joined.
    struct StopAll { std::vector<std::jthread>& workers; ~StopAll() { for (auto& w : workers) w.request_stop(); } } stop_all{workers};
    for (unsigned i = 0; i < std::min<std::size_t>(std::max(1u, jobs), signed_start); ++i) workers.emplace_back([&](std::stop_token stop) {
        while (!stop.stop_requested()) {
            auto index = next.fetch_add(1);
            if (index >= decided.load()) break;
            Result result{true};
            try {
                auto const& chunk = chunks[index];
                auto times = decode_times(chunk, source.read(chunk.time_index->object, 65536 * 8, stop));
                auto found = std::find_if(times.begin(), times.end(), [&](auto const& item) { return item.second >= cutoff; });
                if (found != times.end()) result.found = found->first;
            } catch (...) { result.failure = std::current_exception(); }
            if (result.found || result.failure) decide(index);
            std::lock_guard lock(mutex);
            results[index] = std::move(result);
            ready.notify_all();
        }
    });
    for (std::size_t index = 0; index < signed_start; ++index) {
        std::unique_lock lock(mutex);
        ready.wait(lock, [&] { return results[index].done; });
        if (results[index].failure) std::rethrow_exception(results[index].failure);
        if (results[index].found) return {*results[index].found, manifest.tip};
    }
    if (signed_start < chunks.size()) return {chunks[signed_start].first, manifest.tip};
    // Unreachable for a valid manifest: the tip closed after the cutoff.
    return {manifest.tip, manifest.tip};
}
} // namespace

SignedManifest read_manifest(Source const& source, std::string const& key) {
    std::string bytes, signature;
    Manifest manifest;
    // The public manifest and signature are two independent objects. A mixed
    // generation must never be accepted; retry the pair up to three times. The manifest is
    // not parsed until its signature checks out (authenticate_manifest), so which form it was
    // published with is not read from it: its signature is looked for under its hash first
    // (signature_format sha256-path-v1), then beside it (the legacy manifest.json.sig, which a
    // publisher replaces after the manifest). Neither verifying means the manifest read was
    // replaced meanwhile, or long enough ago for its named signature to be pruned: the
    // manifest is read again. Either signature verifies the bytes it is checked against.
    for (unsigned attempt = 0; ; ++attempt) {
        bytes = source.read("manifest.json", metadata_limit);
        try { signature = source.read("signatures/" + sha256(bytes) + ".sig", 64); }
        catch (Error const&) {
            auto named = std::current_exception();
            try { signature = source.read("manifest.json.sig", 64); }
            catch (Error const&) { if (attempt == 2) std::rethrow_exception(named); continue; }
        }
        try { manifest = authenticate_manifest(bytes, signature, key); break; }
        catch (Error const&) { if (attempt == 2) throw; }
    }
    return {bytes, signature, manifest};
}
Json fetch(FetchOptions const& options) {
    if (options.jobs < 1 || options.jobs > 32) throw Error("--jobs must be between 1 and 32");
    Source source(options.archive);
    auto key = pinned_publisher_key(options.publisher_key);
    auto [bytes, signature, manifest] = read_manifest(source, key);
    auto plan = make_plan(manifest, options.range.ends_with('d') ? days(source, manifest, options.range, options.jobs) : parse_range(options.range, manifest.tip));
    DirectoryLock lock(options.dir);
    auto root = options.dir / "archive";
    safe_cache_path(root);
    source.download(plan.snapshot.index, root);
    auto parts = snapshot_parts(plan.snapshot, read_file(root / plan.snapshot.index.object, metadata_limit));
    plan.objects.insert(plan.objects.end(), parts.begin(), parts.end());
    download_all(source, plan.objects, root, options.jobs);
    Json receipt{{"manifest", bytes}, {"signature_hex", encode_signature(signature)},
                 {"range", std::to_string(plan.range.first) + "-" + std::to_string(plan.range.last)}};
    auto receipt_path = root / "selection.json";
    auto temp = root / "selection.json.part";
    safe_cache_path(receipt_path);
    safe_cache_path(temp);
    write_file(temp, receipt.dump(2) + "\n");
    durable_rename(temp, receipt_path);
    return result(plan, plan.objects.size());
}
Selection load_selection(fs::path const& dir, fs::path const& publisher_key) {
    auto receipt = read_receipt(dir);
    auto manifest = authenticate_manifest(receipt.at("manifest").get<std::string>(),
        decode_signature(receipt.at("signature_hex").get<std::string>()), pinned_publisher_key(publisher_key));
    auto plan = make_plan(manifest, parse_range(receipt.at("range").get<std::string>(), manifest.tip));
    auto root = dir / "archive";
    auto index_path = root / plan.snapshot.index.object;
    safe_cache_path(index_path);
    auto parts = snapshot_parts(plan.snapshot, read_file(index_path, metadata_limit));
    plan.objects.insert(plan.objects.end(), parts.begin(), parts.end());
    for (auto const& object : plan.objects) {
        auto path = root / object.object;
        safe_cache_path(path);
        if (!matches(path, object)) throw Error("Missing or corrupt object: " + object.object);
    }
    return {std::move(manifest), std::move(plan)};
}
Json verify(fs::path const& dir, fs::path const& publisher_key) {
    DirectoryLock lock(dir);
    auto selection = load_selection(dir, publisher_key);
    return result(selection.plan, selection.plan.objects.size());
}
Json status(fs::path const& dir) {
    auto receipt = read_receipt(dir);
    return {{"requested_range", receipt.at("range")}, {"verification", "not-run"},
            {"store_available", false}, {"hint", "Run verify with an independently pinned publisher key"}};
}
} // namespace xrpla
