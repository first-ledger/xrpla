// `xrpla restore`: a piece publication into an xrpld node database, an xrpla store or Clio.
#include "store.hpp"
#include "piece_feed.hpp"
#include "progress.hpp"
#include "replay.hpp"
#include <xrpla/core/operations.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <deque>
#include <exception>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string_view>
#include <thread>
#include <utility>

namespace xrpla::core {
// import_clio.cpp: a Clio plan from a store, replaying its ledgers to verify them only when
// `verify` (a store this restore built was verified as it was written).
Json import_clio(ClioOptions const& options, bool verify);
// piece_feed.cpp: signed JSON, parsed once its digest has matched, refusing duplicate keys.
Json strict_json(std::string const& bytes);
// piece_index.cpp: once a sync has switched selection.json to `selection`, removes the index
// files only the selection it replaced named (index_pieces keeps them until then).
Json remove_unselected_indexes(fs::path const& cache, Json const& selection, IndexOptions const& options);
// A ledger the restore's replay verified, as the store takes it (VerifiedLedger's friend), with
// its transactions' index entries (index_transactions).
struct StoreRestore {
    static VerifiedLedger verified(LedgerRecord const& record, std::vector<TransactionIndex> transactions) {
        return {decode_header(record.header), record, std::move(transactions)};
    }
};
namespace {
// The directories a piece catalog publishes into (tools/piece_publisher.py): a sync fetches only
// objects under them, and removes from them what it no longer selects. A signed catalog naming
// any other key (selection.json, query/..., .verified) is refused.
constexpr std::array<std::string_view, 4> published_directories{"pieces", "bundles", "fragments", "snapshots"};
// Builds (or extends) an xrpla RocksDB store from a publication. Ledgers are verified by the
// restore's replay (replay.hpp: the in-memory pipeline the xrpld restores use; libxrpl's
// verifier, a ledger at a time against its node store, held the store restore to about 200
// ledgers/s) and each is appended once its root has been checked. The replay keeps nothing on
// disk, so an interrupted restore replays from the snapshot through the ledgers the store holds,
// then appends again. A bulk store's writes skip RocksDB's log, so a crash keeps what was
// flushed (every column at once, consistent): besides the flushes its memtables fill, the store
// is flushed every few minutes, bounding what an interrupted restore must replay and write again.
Json restore_store(RestoreOptions const& options, fs::path const& root, bool compact = true) {
    PieceFeed feed(options.source, options.publisher_key, options.first, options.last);
    safe_cache_path(root);
    fs::create_directories(root);
    DirectoryLock lock(root);
    auto const& base = feed.snapshot();
    Store store(root / "store", true, true);  // bulk: flushed at the end
    auto started = std::chrono::steady_clock::now();
    StateReplay replay([](std::uint8_t, Bytes const&, Bytes const&) {});
    auto held = base.sequence;
    if (store.initialized()) {
        auto status = store.status();
        if (!status.at("complete_ledgers").is_null()) held = status.at("complete_ledgers").at("last").get<std::uint32_t>();
        if (held > base.sequence) std::cerr << "Resuming after ledger " << held << '\n';
    }
    {
        // The snapshot is verified as the store takes it (a store that has it already is not written).
        std::vector<ObjectRecord> batch;
        auto add = [&](ObjectRecord&& item) { batch.push_back(std::move(item)); if (batch.size() == 4096) replay.add_snapshot_objects(std::exchange(batch, {})); };
        auto finish = [&] { replay.add_snapshot_objects(std::move(batch)); replay.finish_snapshot(base.raw, base.sequence, base.account_hash); };
        if (store.initialized()) { feed.snapshot_objects(add); finish(); }
        else store.initialize(base, Range{base.sequence + 1, feed.last()}, [&](auto const& visit) {
            feed.snapshot_objects([&](ObjectRecord&& item) { visit(item); add(std::move(item)); });
            finish();
        });
    }
    // A bulk store's writes are neither logged nor synced: a crash keeps what its last flush
    // wrote (every column at once), and the baseline counts as written (Store::initialized) only
    // once that flush holds baseline_header, its last write.
    store.sync_each_write(false);
    // Where the time goes: reading the publication, waiting for the replay, the store's writes
    // and flushes.
    double reading = 0, verifying = 0, storing = 0, flushing = 0;
    constexpr std::chrono::minutes flush_every{5};
    auto flushed = std::chrono::steady_clock::now();
    auto since = [](auto& from) { auto now = std::chrono::steady_clock::now(); auto s = std::chrono::duration<double>(now - from).count(); from = now; return s; };
    auto at = std::chrono::steady_clock::now();
    std::uint64_t applied = 0;
    // Each ledger the replay is verifying, with its transactions' index entries (hashes, the
    // accounts each touches) for the store, derived on the shared task pool while the replay
    // runs: on this thread, which every ledger passes in order, they re-parsed every
    // transaction one at a time. Used only once the replay has verified the ledger.
    struct Verifying {
        std::shared_ptr<LedgerRecord const> record;
        std::future<std::vector<TransactionIndex>> transactions;  // for a ledger the store lacks
    };
    std::deque<Verifying> verifying_ledgers;
    // Appends the ledgers at the front that the replay has verified (all of them, waiting, at the end).
    auto drain = [&](bool all) {
        while (!verifying_ledgers.empty()) {
            auto& front = verifying_ledgers.front();
            auto const& record = *front.record;
            if (all) replay.wait(record.sequence);
            else if (!replay.verified(record.sequence)) break;
            verifying += since(at);
            if (record.sequence > held) {
                store.append(StoreRestore::verified(record, front.transactions.get()));
                ++applied;
                storing += since(at);
                if (record.sequence % 256 == 0 || record.sequence == feed.last()) {
                    if (std::chrono::steady_clock::now() - flushed >= flush_every) {
                        store.flush();
                        flushed = std::chrono::steady_clock::now();
                        flushing += since(at);
                    }
                    auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
                    std::cerr << "Restored ledger " << record.sequence << " / " << feed.last() << " (" << static_cast<std::uint64_t>(applied / std::max(seconds, 1e-3))
                              << " ledgers/s; seconds read " << static_cast<int>(reading) << ", verify " << static_cast<int>(verifying)
                              << ", store " << static_cast<int>(storing) << ", flush " << static_cast<int>(flushing) << ")\n";
                }
            }
            verifying_ledgers.pop_front();
        }
    };
    std::uint32_t seq = base.sequence + 1;
    for (LedgerRecord record; feed.next(record); ++seq) {
        reading += since(at);
        if (record.sequence != seq) throw Error("Ledger source is out of order at " + std::to_string(seq));
        auto shared = std::make_shared<LedgerRecord const>(std::move(record));
        replay.submit(shared);
        std::future<std::vector<TransactionIndex>> transactions;
        if (shared->sequence > held) transactions = TaskPool::shared().submit([shared] { return index_transactions(*shared); });
        verifying_ledgers.push_back({std::move(shared), std::move(transactions)});
        verifying += since(at);
        drain(false);
    }
    drain(true);
    store.flush();
    // A bulk load leaves many overlapping files that every read must check (on 300,000 ledgers
    // of 2014, book_offers 90 ms and account_currencies up to 2 s); one compaction at the end
    // costs a fraction of the restore and makes reads several times faster.
    double compact_seconds = 0;
    if (compact) {
        auto begun = std::chrono::steady_clock::now();
        std::cerr << "Compacting the store" << std::endl;
        store.compact();
        compact_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begun).count();
    }
    auto result = store.status();
    result["ledgers_applied"] = applied; result["bytes_read"] = feed.bytes_read();
    result["seconds"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    result["timings"] = {{"read", reading}, {"verify", verifying}, {"store", storing}, {"flush", flushing}, {"compact", compact_seconds}, {"replay", replay.timings()}};
    return result;
}
}
Json sync_pieces(SyncOptions const& options) {
    auto started = std::chrono::steady_clock::now();
    safe_cache_path(options.cache);
    fs::create_directories(options.cache);
    // One sync (or index-pieces) at a time: two would share staging files and indexes being
    // built, and one could switch the selection back under the other.
    DirectoryLock lock(options.cache);
    // The pointer is not signed, so a stale copy of it (a cache in front of the origin, or a
    // mirror replaying an old one) can name an older generation, signed as it once was: taken,
    // it would switch the cache back and delete what newer generations added. Generations only
    // move forward: an older one is nothing newer (the next mirror is asked), and the cache keeps
    // its selection (and its servers serving it) until the publication offers a newer one; a
    // pointer cached in front of the origin goes stale for minutes at a time. One that came back
    // different is refused.
    auto target = options.cache / "selection.json";
    std::optional<Json> held;
    if (fs::exists(target)) held = Json::parse(read_file(target, 64 << 20)).at("catalog");
    std::uint64_t have = held ? held->at("generation").get<std::uint64_t>() : 0;
    // A new publication may so far hold only its baseline snapshot (a live tail serves from it).
    std::optional<PieceFeed> offered;
    try { offered.emplace(options.source, options.publisher_key, options.first, options.last, true, have); }
    catch (OlderCatalog const& older) {
        Json refused{{"event", "catalog_refused_older"}, {"offered", older.offered}, {"held", have}, {"cache", options.cache.string()},
                     {"note", "a stale or replayed catalog; remove " + target.string() + " to follow another publication"}};
        std::cerr << refused.dump() << std::endl;
        return {{"generation", have}, {"refused_older", older.offered}, {"fetched_bytes", 0}, {"removed", Json::array()},
                {"seconds", std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count()}};
    }
    auto& feed = *offered;
    if (held && feed.catalog().at("generation").get<std::uint64_t>() == have && *held != feed.catalog())
        throw Error("Catalog generation " + std::to_string(have) + " differs from the one this cache holds; it is refused");
    auto const& entry = feed.snapshot_entry();
    std::vector<Object> objects;
    // Each object as the archive's manifests check theirs (parse_object): a safe relative key,
    // a lowercase SHA-256 (which names its staging file) and a size; and under a directory the
    // catalog publishes into, never one of the cache's own files.
    auto published = [](Json const& item) {
        auto object = parse_object(item);
        auto slash = object.object.find('/');
        if (slash == std::string::npos || std::find(published_directories.begin(), published_directories.end(),
                                                    std::string_view(object.object).substr(0, slash)) == published_directories.end())
            throw Error("Catalog object outside the published directories: " + object.object);
        return object;
    };
    auto add = [&](Json const& item) { objects.push_back(published(item)); };
    add(entry.at("index"));
    for (auto const& part : feed.snapshot_index().at("parts")) add(part);
    for (auto const& range : feed.ranges()) add(range);
    Source source(options.source);
    // Interval snapshots the selected ranges pass (a range starts right after each): ledgers
    // from one on read their state from it and the few ranges after it (PieceHistory), not from
    // the base snapshot and every range since, which for a full history is 2013's first ledger
    // and over a hundred bundles. The newest is always kept (`recent`); with snapshots_every N,
    // so is every one whose sequence is a multiple of N, which older ledgers read from (on
    // mainnet, every 10,000,000 keeps ten more, about 21 GB of parts and state indexes).
    // Each index is checked as PieceFeed checks the base snapshot's: against the signed catalog,
    // and as the state of the ledger the range after it continues from.
    std::map<std::uint32_t, Json> intervals;
    std::optional<Json> recent;
    for (auto const& s : feed.catalog().at("snapshots")) {
        auto seq = s.at("seq").get<std::uint32_t>();
        if (seq <= entry.at("seq").get<std::uint32_t>() || seq >= feed.last()) continue;
        bool starts = std::any_of(feed.ranges().begin(), feed.ranges().end(), [&](Json const& r) { return r.at("first").get<std::uint32_t>() == seq + 1; });
        if (!starts) continue;
        if (!recent || recent->at("seq").get<std::uint32_t>() < seq) recent = s;
        if (options.snapshots_every && seq % options.snapshots_every == 0) intervals.emplace(seq, s);
    }
    if (recent) intervals.emplace(recent->at("seq").get<std::uint32_t>(), *recent);
    auto lower = [](std::string text) { std::transform(text.begin(), text.end(), text.begin(), ::tolower); return text; };
    for (auto const& [seq, s] : intervals) {
        auto index = published(s.at("index"));
        source.download(index, options.cache);
        auto parsed = strict_json(read_file(options.cache / index.object, 16 << 20));
        if (parsed.at("seq") != s.at("seq") || parsed.at("format_version") != 2 || parsed.at("record_format") != "keyed-stobject-v2")
            throw Error("Unsupported snapshot index for ledger " + std::to_string(seq));
        auto header = decode_header(from_hex(parsed.at("ledger_header").get<std::string>()));
        if (header.sequence != seq || lower(header.hash) != lower(s.at("ledger_hash").get<std::string>()) ||
            lower(header.account_hash) != lower(s.at("account_hash").get<std::string>()))
            throw Error("Snapshot " + std::to_string(seq) + " differs from the catalog");
        auto after = std::find_if(feed.ranges().begin(), feed.ranges().end(), [&](Json const& r) { return r.at("first").get<std::uint32_t>() == seq + 1; });
        if (after == feed.ranges().end() || lower(after->at("parent_hash").get<std::string>()) != lower(header.hash))
            throw Error("Snapshot " + std::to_string(seq) + " does not start the range after it");
        add(s.at("index"));
        for (auto const& part : parsed.at("parts")) add(part);
    }
    // Several objects at a time: a snapshot is hundreds of parts.
    std::atomic<std::size_t> next{0}, finished{0};
    std::atomic<std::uint64_t> fetched{0};
    std::exception_ptr failure; std::mutex failed;
    // What this sync has to fetch, for its progress: objects not yet in the cache whole.
    std::uint64_t to_fetch = 0; std::size_t missing = 0;
    for (auto const& object : objects) {
        auto path = options.cache / object.object;
        if (!fs::exists(path) || fs::file_size(path) != object.size) { to_fetch += object.size; ++missing; }
    }
    // Each thread's object under way, whose staging file (Source::download) shows its bytes so far:
    // a bundle is tens of GB, too long to report only when it is whole.
    unsigned const threads_used = std::max(1u, options.jobs);
    std::vector<std::atomic<std::size_t>> current(threads_used);
    for (auto& slot : current) slot = objects.size();
    // Objects can share a hash (snapshot parts whose keys did not change between two kept
    // snapshots), and a transfer stages in a file named by its hash: those transfers
    // take turns, as download_all's do.
    std::mutex transfer_locks[64];
    auto work = [&](unsigned slot) {
        for (std::size_t i; (i = next++) < objects.size();) {
            try {
                auto path = options.cache / objects[i].object;
                std::lock_guard transfer(transfer_locks[std::hash<std::string>{}(objects[i].sha256) % 64]);
                bool present = fs::exists(path) && fs::file_size(path) == objects[i].size;
                if (!present) current[slot] = i;
                source.download(objects[i], options.cache);
                current[slot] = objects.size();
                if (!present) { fetched += objects[i].size; ++finished; }
            } catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); next = objects.size(); }
        }
    };
    {
        auto began = std::chrono::steady_clock::now();
        Progress progress(std::chrono::seconds(60), [&] {
            std::uint64_t done = fetched;
            for (auto const& slot : current) {
                std::size_t i = slot;
                if (i >= objects.size()) continue;
                std::error_code ec;
                auto staged = fs::file_size(options.cache / ".staging" / (objects[i].sha256 + ".part"), ec);
                if (!ec) done += std::min<std::uint64_t>(staged, objects[i].size);
            }
            double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count();
            double rate = done / std::max(seconds, 1.0);
            Json line{{"event", "sync_progress"}, {"phase", "download"}, {"objects", finished.load()}, {"objects_total", missing},
                      {"bytes", done}, {"bytes_total", to_fetch}, {"mb_per_second", std::round(rate / 1e5) / 10}, {"cache", options.cache.string()}};
            if (rate > 0 && to_fetch > done) line["eta_seconds"] = static_cast<std::uint64_t>((to_fetch - done) / rate);
            std::cerr << line.dump() << std::endl;
        });
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < threads_used; ++i) threads.emplace_back(work, i);
        for (auto& t : threads) t.join();
    }
    if (failure) std::rethrow_exception(failure);
    Json selection{{"catalog", feed.catalog()}, {"snapshot", entry}, {"ranges", feed.ranges()},
                   {"first", entry.at("seq").get<std::uint32_t>() + 1}, {"last", feed.last()}};
    // `snapshots` lists every interval snapshot kept, ascending; `recent`, the newest, is still
    // written for a build that knows only it (it then reads every ledger before it from the base).
    if (recent) {
        selection["recent"] = *recent;
        selection["snapshots"] = Json::array();
        for (auto const& [seq, s] : intervals) selection["snapshots"].push_back(s);
    }
    // Index before switching, so a serving process never sees ranges without their indexes.
    auto indexed = index_pieces(options.cache, std::max(1u, std::thread::hardware_concurrency()), selection, options.indexes);
    // Rewritten only when it changes: servers reopen the cache (and rebuild a live tail over
    // it) when the file does. A damaged index rebuilt in place has them reopen it too: they
    // still map the replaced file.
    auto text = selection.dump();
    if (!fs::exists(target) || !indexed.at("rebuilt").empty() || read_file(target, 64 << 20) != text) { write_file(target.string() + ".part", text); durable_rename(target.string() + ".part", target); }
    // The indexes of what the previous selection alone named, kept until the switch. They go now,
    // with its pieces below, rather than at the next sync: a server opens every file a selection
    // names when it opens the selection (PieceHistory), and keeps those it holds after they are
    // removed, so only one opening from the old pointer at this moment misses one; it is opened
    // again, from the new pointer, by its next try (open_piece_history). Keeping the old files
    // one sync longer would mean keeping its pieces and snapshot parts too (gigabytes, when the
    // base snapshot moves) to spare that one retry.
    remove_unselected_indexes(options.cache, selection, options.indexes);
    // Published objects no longer selected (pieces rolled into a bundle, tail fragments a piece
    // replaced, older snapshots).
    std::set<fs::path> live;
    for (auto const& object : objects) live.insert(fs::weakly_canonical(options.cache / object.object));
    Json removed = Json::array();
    for (auto top : published_directories) {
        if (!fs::exists(options.cache / top)) continue;
        for (auto const& file : fs::recursive_directory_iterator(options.cache / top))
            if (file.is_regular_file() && !live.contains(fs::weakly_canonical(file.path()))) { removed.push_back(fs::relative(file.path(), options.cache).generic_string()); fs::remove(file.path()); }
    }
    std::uint64_t cached = 0;
    for (auto const& object : objects) cached += object.size;
    Json kept = Json::array();
    for (auto const& [seq, s] : intervals) kept.push_back(seq);
    return {{"generation", feed.catalog().at("generation")}, {"snapshot", entry.at("seq")}, {"interval_snapshots", kept}, {"first", selection.at("first")}, {"last", selection.at("last")},
            {"ranges", feed.ranges().size()}, {"fetched_bytes", fetched.load()}, {"cached_bytes", cached}, {"removed", removed},
            {"indexes", indexed.at("ranges")}, {"seconds", std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count()}};
}
Json restore(RestoreOptions const& options) {
    if (options.source.empty() || options.publisher_key.empty() || options.output.empty()) throw Error("restore requires --publication NAME (or --source and --publisher-key) and --out");
    if (options.target == "nudb" || options.target == "rocksdb") return restore_rippled(options);
    if (options.target == "store") return restore_store(options, options.output);
    if (options.target == "spool") return restore_spool(options);
    if (options.target == "clio") {
        // Clio's tables derive from the store's indexes, so a store is built first (resumable),
        // then written as Clio's import plan and loaded when a host is given. Every ledger the
        // store holds was verified by the restore's replay before it was appended (in this run
        // or the one it resumes), so the plan does not replay them again; a complete plan for
        // the same ledgers is not written again either (import_clio). With a host, the plan is
        // loaded as it is written (the store is verified already).
        auto staging = options.output / "store-staging";
        Json result{{"store", restore_store(options, staging, false)}};  // read once, in order: not compacted
        ClioOptions clio{ImportOptions{staging, options.output / "plan", ""}, options.clio_host, options.clio_keyspace, options.clio_username, options.clio_port, options.clio_tls,
                         options.jobs, options.load_jobs};
        result["clio"] = import_clio(clio, false);
        return result;
    }
    throw Error("--to must be nudb, rocksdb, store, clio or spool");
}
}
