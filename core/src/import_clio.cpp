#include "replay.hpp"
#include "store.hpp"
#include "wire.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/protocol/AccountID.h>
#include <xrpl/protocol/nft.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <set>
#include <thread>
#ifndef _WIN32
#include <csignal>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
std::uint32_t number(std::string_view bytes) { if (bytes.size() != 4) throw Error("Invalid index integer"); std::uint32_t n = 0; for (unsigned char c : bytes) n = (n << 8) | c; return n; }
Json blob(Bytes const& bytes) { return {{"blob", to_hex(bytes)}}; }
template<class T> Bytes raw(T const& value) { return {reinterpret_cast<char const*>(value.data()), value.size()}; }
std::vector<std::pair<std::string, std::string>> const schemas{
    {"objects", "key blob, sequence bigint, object blob, PRIMARY KEY(key,sequence)) WITH CLUSTERING ORDER BY(sequence DESC)"},
    {"transactions", "hash blob PRIMARY KEY, ledger_sequence bigint, date bigint, transaction blob, metadata blob)"},
    {"ledger_transactions", "ledger_sequence bigint, hash blob, PRIMARY KEY(ledger_sequence,hash))"},
    {"successor", "key blob, seq bigint, next blob, PRIMARY KEY(key,seq))"},
    {"diff", "seq bigint, key blob, PRIMARY KEY(seq,key))"},
    {"account_tx", "account blob, seq_idx tuple<bigint,bigint>, hash blob, PRIMARY KEY(account,seq_idx)) WITH CLUSTERING ORDER BY(seq_idx DESC)"},
    {"ledgers", "sequence bigint PRIMARY KEY, header blob)"},
    {"ledger_hashes", "hash blob PRIMARY KEY, sequence bigint)"},
    {"ledger_range", "is_latest boolean PRIMARY KEY, sequence bigint)"},
    {"nf_tokens", "token_id blob, sequence bigint, owner blob, is_burned boolean, PRIMARY KEY(token_id,sequence)) WITH CLUSTERING ORDER BY(sequence DESC)"},
    {"issuer_nf_tokens_v2", "issuer blob, taxon bigint, token_id blob, PRIMARY KEY(issuer,taxon,token_id)) WITH CLUSTERING ORDER BY(taxon ASC,token_id ASC)"},
    {"nf_token_uris", "token_id blob, sequence bigint, uri blob, PRIMARY KEY(token_id,sequence)) WITH CLUSTERING ORDER BY(sequence DESC)"},
    {"nf_token_transactions", "token_id blob, seq_idx tuple<bigint,bigint>, hash blob, PRIMARY KEY(token_id,seq_idx)) WITH CLUSTERING ORDER BY(seq_idx DESC)"},
    {"mp_token_holders", "mpt_id blob, holder blob, PRIMARY KEY(mpt_id,holder)) WITH CLUSTERING ORDER BY(holder ASC)"},
    {"migrator_status", "migrator_name text PRIMARY KEY, status text)"},
    {"nodes_chat", "node_id UUID, message TEXT, PRIMARY KEY(node_id)) WITH default_time_to_live=2"}
};
// Plan lines a job prepares: whole rows, one JSON line each, as the plan holds them. Given a
// file (`spill`), lines past `cap` bytes go there instead of memory: one key's versions can be
// a whole history's worth (the ledger hashes object changes with every ledger). take() moves
// the lines, the file's first, to the plan; a file not taken is removed.
struct Rows {
    struct Spill {
        fs::path path;
        std::ofstream out;
        ~Spill() { out.close(); std::error_code ignored; fs::remove(path, ignored); }
    };
    std::string text;
    std::uint64_t count = 0;
    std::unique_ptr<Spill> spill;
    std::size_t cap = 0;
    Rows() = default;
    Rows(fs::path path, std::size_t bytes) : spill(std::make_unique<Spill>()), cap(bytes) { spill->path = std::move(path); }
    void add(std::string const& table, Json values) {
        text += Json{{"table", table}, {"values", std::move(values)}}.dump(); text += '\n'; ++count;
        if (!spill || text.size() < cap) return;
        if (!spill->out.is_open()) { spill->out.open(spill->path, std::ios::binary | std::ios::trunc); if (!spill->out) throw Error("Cannot create Clio plan spill file"); }
        spill->out << text; if (!spill->out) throw Error("Writing Clio plan spill file failed");
        text.clear();
    }
    void take(std::ostream& plan) {
        if (spill && spill->out.is_open()) {
            spill->out.close(); if (!spill->out) throw Error("Writing Clio plan spill file failed");
            std::ifstream in(spill->path, std::ios::binary);
            if (!in || !(plan << in.rdbuf())) throw Error("Copying Clio plan spill file failed");
        }
        plan << text;
        spill.reset();
    }
};
// Runs make(0) to make(count - 1) on `jobs` threads, at most `window` past the one consume
// (on this thread) has, and hands each result to consume in index order. Whatever the jobs,
// consume sees the same results in the same order, so the plan is the same file for any
// --jobs. A failure on any thread stops the others and is rethrown here.
template<class T>
void in_order(std::size_t count, unsigned jobs, std::size_t window, std::function<T(std::size_t)> const& make, std::function<void(T&)> const& consume) {
    if (jobs <= 1) { for (std::size_t i = 0; i < count; ++i) { auto result = make(i); consume(result); } return; }
    std::mutex mutex;
    std::condition_variable made, room;
    std::map<std::size_t, T> done;
    std::size_t next = 0, consumed = 0;
    std::exception_ptr failure;
    auto fail = [&](std::exception_ptr error) { { std::lock_guard lock(mutex); if (!failure) failure = error; } made.notify_all(); room.notify_all(); };
    auto work = [&] {
        for (;;) {
            std::size_t i;
            {
                std::unique_lock lock(mutex);
                room.wait(lock, [&] { return failure || next >= count || next < consumed + window; });
                if (failure || next >= count) return;
                i = next++;
            }
            try {
                auto result = make(i);
                bool wanted;
                { std::lock_guard lock(mutex); done.emplace(i, std::move(result)); wanted = i == consumed; }
                if (wanted) made.notify_one();  // only this thread waits on it, and only for the next result
            } catch (...) { fail(std::current_exception()); return; }
        }
    };
    std::vector<std::thread> threads;
    for (unsigned j = 0; j < std::min<std::size_t>(jobs, count); ++j) threads.emplace_back(work);
    try {
        for (std::size_t i = 0; i < count; ++i) {
            T result;
            {
                std::unique_lock lock(mutex);
                made.wait(lock, [&] { return failure || done.contains(i); });
                if (failure) break;
                auto item = done.find(i); result = std::move(item->second); done.erase(item);
                consumed = i + 1;
            }
            room.notify_one();  // one more index may be taken
            consume(result);
        }
    } catch (...) { fail(std::current_exception()); }
    room.notify_all();  // threads still waiting for room find nothing left to take
    for (auto& thread : threads) thread.join();
    if (failure) std::rethrow_exception(failure);
}
// Store keys split into 2^bits ranges (12 to 20 bits) by their first bits, for the jobs to take
// in key order: keys are hashes, so the ranges are about equal (a column's keys of another
// shape still fall in one of them: the first starts at the column's start, the last ends at
// its end, and each starts where the one before ends).
constexpr unsigned key_bits = 12;
constexpr std::size_t key_ranges = std::size_t(1) << key_bits;
std::pair<Bytes, Bytes> key_range(std::size_t i, unsigned bits = key_bits) {
    auto start = [bits](std::size_t n) { auto v = n << (24 - bits); return Bytes{static_cast<char>(v >> 16), static_cast<char>(v >> 8), static_cast<char>(v)}; };
    return {i == 0 ? Bytes{} : start(i), i + 1 == (std::size_t(1) << bits) ? Bytes{} : start(i + 1)};
}
// Ranges for a column of `bytes` in table files: about 8 MB of them each (some 40 MB of plan
// lines), so the rows a range's job holds stay small however long the history.
unsigned column_bits(std::uint64_t bytes) {
    unsigned bits = key_bits;
    while (bits < 20 && (std::uint64_t(1) << bits) * (std::uint64_t(8) << 20) < bytes) ++bits;
    return bits;
}
// Rows a range's job keeps in memory before it spills the rest to a file. The tests set
// smaller ranges and spills (XRPLA_CLIO_RANGE_BITS, XRPLA_CLIO_SPILL_BYTES) to see the plan
// come out the same.
std::uint64_t setting(char const* name, std::uint64_t otherwise) {
    auto const* text = std::getenv(name);
    if (!text) return otherwise;
    std::uint64_t value = 0;
    auto [end, ec] = std::from_chars(text, text + std::strlen(text), value);
    if (ec != std::errc{} || *end || !value) throw Error(std::string(name) + " must be a positive integer");
    return value;
}
// Jobs for the plan: one a core by default. RocksDB reads (each transaction is a point
// lookup by hash) and the rows' JSON were one thread's work, at about 32 mainnet ledgers/s.
unsigned plan_jobs(unsigned asked) { return asked ? asked : std::clamp(std::thread::hardware_concurrency(), 1u, 32u); }
// An eighth of the memory available, within 512 MB and 32 GB, for the store's block cache:
// the plan reads every ledger's transactions and changed objects, and kept uncompressed
// their index and data blocks are not read and decompressed again for the next ledger. The
// rest is left to the loader's processes and a Cassandra or ScyllaDB on the same machine.
std::size_t plan_cache_bytes() {
    std::ifstream meminfo("/proc/meminfo");
    for (std::string name; meminfo >> name;) {
        std::uint64_t kilobytes = 0;
        meminfo >> kilobytes;
        if (name == "MemAvailable:") return static_cast<std::size_t>(std::clamp<std::uint64_t>(kilobytes * 1024 / 8, std::uint64_t(512) << 20, std::uint64_t(32) << 30));
        meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return 0;
}
// tools/clio_load.py, run beside the plan (`follow`: as it is written) or after it. A loader
// still running when this goes away (the plan's writing failed) is stopped; it releases its
// keyspace lease, and what it loaded is resumed, as after any interrupted load.
class Loader {
#ifndef _WIN32
    pid_t child_ = -1;
    void reap(int status) {
        child_ = -1;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) throw Error("Clio loader failed; its diagnostic is shown above. The plan is safe to resume.");
    }
#endif
public:
    Loader(fs::path const& plan, ClioOptions const& options, fs::path const& report, bool follow) {
#ifdef _WIN32
        (void)plan; (void)options; (void)report; (void)follow;
        throw Error("Clio loading requires the supported Linux runtime");
#else
        auto executable = fs::read_symlink("/proc/self/exe");
        auto script = executable.parent_path().parent_path() / "share" / "xrpla" / "clio_load.py";
        // The source tree's loader serves only a binary run where it was built (the tests), never an
        // installed one (as setup's run_connected treats live_publisher.py).
        if (!fs::is_regular_file(script)) {
            std::error_code ec;
            auto inside = executable.lexically_relative(fs::weakly_canonical(XRPLA_BUILD_DIR, ec));
            if (!ec && !inside.empty() && *inside.begin() != "..") script = fs::path(XRPLA_SOURCE_TOOLS) / "clio_load.py";
        }
        if (!fs::is_regular_file(script)) throw Error("Clio loader is missing from this installation");
        std::vector<std::string> args{"python3", script.string(), "--plan", plan.string(), "--host", options.host,
            "--port", std::to_string(options.port), "--keyspace", options.keyspace, "--result", report.string()};
        if (options.tls) args.push_back("--tls");
        if (!options.username.empty()) { args.push_back("--username"); args.push_back(options.username); }
        if (options.load_jobs) { args.push_back("--workers"); args.push_back(std::to_string(options.load_jobs)); }
        if (follow) args.push_back("--follow");
        std::vector<char*> argv; for (auto& arg : args) argv.push_back(arg.data()); argv.push_back(nullptr);
        if (posix_spawnp(&child_, "python3", nullptr, nullptr, argv.data(), environ) != 0) { child_ = -1; throw Error("Cannot launch Python 3 Clio loader"); }
#endif
    }
    Loader(Loader const&) = delete;
    Loader& operator=(Loader const&) = delete;
    // While the plan is written: a loader that has stopped could only have failed.
    void check() {
#ifndef _WIN32
        int status = 0;
        auto done = ::waitpid(child_, &status, WNOHANG);
        if (done < 0 && errno != EINTR) throw Error("Cannot wait for Clio loader");
        if (done == child_) { reap(status); throw Error("Clio loader stopped before the plan was complete"); }
#endif
    }
    void wait() {
#ifndef _WIN32
        int status = 0;
        while (::waitpid(child_, &status, 0) < 0) { if (errno != EINTR) throw Error("Cannot wait for Clio loader"); }
        reap(status);
#endif
    }
    ~Loader() {
#ifndef _WIN32
        if (child_ <= 0) return;
        ::kill(child_, SIGTERM);
        int status = 0;
        while (::waitpid(child_, &status, 0) < 0 && errno == EINTR) {}
#endif
    }
};
// An order book directory: a DirectoryNode (0x0064) with an ExchangeRate (UInt64 field 6),
// read by type code, as history holds objects this libxrpl cannot decode.
Bytes book_base(ObjectRecord const& item) {
    if (item.object.empty()) return {};
    auto type = wire::top_level(item.object, 1, 1);
    if (!type || *type != std::string_view("\x00\x64", 2) || !wire::top_level(item.object, 3, 6)) return {};
    return item.key.substr(0, 24) + Bytes(8, '\0');
}
// The live objects of one key range at the range's first ledger: their keys and order books
// (and, for the replay, the objects).
struct StateSlice { std::vector<Bytes> keys, books; std::vector<ObjectRecord> objects; };
// A ledger's rows that do not depend on the ledgers before it, prepared by a job: the
// ledger's header and transaction rows, each changed key's diff row, and each changed
// object's order book (a deleted one's from its last version, read here). The successor
// rows need the live keys at the ledger, kept in ledger order, and are written in order.
struct LedgerRows {
    std::shared_ptr<LedgerRecord const> record;
    Rows head;
    std::vector<std::string> diffs;
    std::vector<Bytes> books;
};
}
// `verify`: false only for a store whose every ledger the restore's own replay has verified
// (restore.cpp's restore --to clio), which replaying again would only repeat.
Json import_clio(ClioOptions const& options, bool verify) {
    auto const& input = options.input;
    DirectoryLock lock(input.dir);
    Store store(input.dir / "store", false);
    auto bounds = store.status().at("state_ledgers");
    Range range{bounds.at("first"), bounds.at("last")};
    if (!input.range.empty()) range = parse_range(input.range, range.last);
    if (range.first < bounds.at("first").get<std::uint32_t>()) throw Error("Cannot import pruned state");
    safe_cache_path(input.output); fs::create_directories(input.output);
    DirectoryLock out_lock(input.output);
    auto path = input.output / "clio-plan.jsonl"; auto temporary = input.output / "clio-plan.jsonl.part";
    // A complete plan's identity and row count, written once it is in place.
    auto summary = input.output / "clio-plan.json";
    auto report = input.output / "clio-load-result.json";
    Json identity{{"format", "xrpla-clio-2.7.1-v1"}, {"first", range.first}, {"last", range.last}, {"first_hash", decode_header(store.header(range.first)).hash}, {"last_hash", decode_header(store.header(range.last)).hash}};
    auto load = [&](Json result) {
        if (!options.host.empty()) {
            Loader(path, options, report, false).wait();
            result["load"] = Json::parse(read_file(report, 1024 * 1024)); result["applied"] = true;
        }
        return result;
    };
    if (fs::exists(path)) {
        std::ifstream old(path); std::string line; std::getline(old, line); auto prior = Json::parse(line);
        if (prior.value("first_hash", "") != decode_header(store.header(range.first)).hash) throw Error("Clio plan destination contains different history");
        // The same ledgers' plan, complete (it is renamed into place only once every ledger is
        // verified): written again it would be the same rows, hours later for a long history.
        // A plan written without the independent replay (`restore --to clio`, whose own replay
        // verified the store) is not reused by an import that asks for it.
        if (prior == identity && fs::is_regular_file(summary)) {
            auto done = Json::parse(read_file(summary, 1 << 16));
            if (done.value("identity", Json()) == identity && (!verify || done.value("verified", false)))
                return load({{"plan", path.string()}, {"rows", done.at("rows")}, {"clio_version", "2.7.1"}, {"first", range.first}, {"last", range.last},
                             {"applied", false}, {"reused", true}});
        }
    }
    auto const jobs = plan_jobs(options.jobs);
    Store::reserve_block_cache(plan_cache_bytes());
    std::ofstream out(temporary, std::ios::binary | std::ios::trunc); if (!out) throw Error("Cannot create Clio plan");
    std::uint64_t rows = 0;
    auto write = [&](std::string const& text, std::uint64_t count) { out << text; if (!out) throw Error("Writing Clio plan failed"); rows += count; };
    auto row = [&](std::string const& table, Json values) { write(Json{{"table", table}, {"values", std::move(values)}}.dump() + '\n', 1); };
    out << identity.dump() << '\n';
    for (auto const& [table, fields] : schemas) out << Json{{"schema", "CREATE TABLE IF NOT EXISTS $KEYSPACE." + table + " (" + fields}}.dump() << '\n';
    // Loaded as it is written, when the store's ledgers are all verified already (the
    // restore's): the loader follows the plan file and publishes the range only once the plan
    // is complete and every row acknowledged. A plan whose writing verifies the ledgers is
    // loaded only once it is complete, as nothing unverified may reach the keyspace.
    std::optional<Loader> streaming;
    if (!options.host.empty() && !verify) { out.flush(); if (!out) throw Error("Writing Clio plan failed"); streaming.emplace(path, options, report, true); }
    auto written = [&] { if (streaming) { out.flush(); if (!out) throw Error("Writing Clio plan failed"); streaming->check(); } };
    // Verify the historical store independently while preparing the plan (unless the restore
    // that built it just did), with the restores'
    // replay (replay.hpp: libxrpl's verifier, a ledger at a time, held this to about 200
    // ledgers/s); the plan is renamed into place only once every ledger is verified. The live
    // keys at each ledger, kept here in order, answer each changed key's neighbours: the
    // store's version keyspace holds every key the range ever had, so finding the nearest live
    // one there scans the dead ones (busy order books leave thousands), again for every change
    // near them.
    //
    // The store's reads and the rows' JSON are done by `jobs` threads, a key range or a ledger
    // each; this thread keeps the live keys, writes the rows that depend on them, and writes
    // everything in the order one thread would have, so the plan is the same file for any jobs.
    std::optional<StateReplay> replay;
    if (verify) replay.emplace([](std::uint8_t, Bytes const&, Bytes const&) {});
    using Key = std::array<unsigned char, 32>;  // unsigned: keys order as bytes
    auto key_of = [](Bytes const& bytes) { Key key; std::copy_n(bytes.data(), 32, key.data()); return key; };
    std::set<Key> live;
    auto successor_of = [&](Bytes const& key) { auto it = live.upper_bound(key_of(key)); return it == live.end() ? Bytes(32, '\xff') : Bytes(reinterpret_cast<char const*>(it->data()), 32); };
    auto predecessor_of = [&](Bytes const& key) { auto it = live.lower_bound(key_of(key)); return it == live.begin() ? Bytes(32, '\0') : Bytes(reinterpret_cast<char const*>(std::prev(it)->data()), 32); };
    std::vector<ObjectRecord> snapshot;
    Bytes previous(32, '\0');
    Bytes previous_book;
    in_order<StateSlice>(key_ranges, jobs, std::size_t(jobs) * 4, [&](std::size_t i) {
        StateSlice slice;
        auto [from, to] = key_range(i);
        store.visit_live(range.first, from, to, [&](Bytes const& key, Bytes const& value) {
            ObjectRecord object{key, value};
            slice.keys.push_back(key); slice.books.push_back(book_base(object));
            if (replay) slice.objects.push_back(std::move(object));
            return true;
        });
        return slice;
    }, [&](StateSlice& slice) {
        for (std::size_t k = 0; k < slice.keys.size(); ++k) {
            auto const& key = slice.keys[k]; auto const& book = slice.books[k];
            live.insert(key_of(key));
            if (!book.empty() && book != previous_book) { row("successor", {{"key", blob(book)}, {"seq", range.first}, {"next", blob(key)}}); previous_book = book; }
            row("successor", {{"key", blob(previous)}, {"seq", range.first}, {"next", blob(key)}}); previous = key;
            if (!replay) continue;
            snapshot.push_back(std::move(slice.objects[k]));
            if (snapshot.size() == 4096) replay->add_snapshot_objects(std::exchange(snapshot, {}));
        }
        written();
    });
    row("successor", {{"key", blob(previous)}, {"seq", range.first}, {"next", blob(Bytes(32, '\xff'))}});
    if (replay) {
        replay->add_snapshot_objects(std::move(snapshot));
        auto initial = decode_header(store.header(range.first)); replay->finish_snapshot(initial.raw, initial.sequence, initial.account_hash);
    }
    in_order<LedgerRows>(std::size_t(range.last - range.first) + 1, jobs, std::size_t(jobs) * 8, [&](std::size_t i) {
        auto const seq = static_cast<std::uint32_t>(range.first + i);
        LedgerRows ledger;
        ledger.record = std::make_shared<LedgerRecord const>(store.ledger_record(seq));
        auto const& record = *ledger.record;
        auto header = decode_header(record.header);
        // Clio stores addRaw(header, includeHash=true), not the 118-byte wire header.
        ledger.head.add("ledgers", {{"sequence", seq}, {"header", blob(record.header + from_hex(header.hash))}});
        ledger.head.add("ledger_hashes", {{"hash", blob(from_hex(header.hash))}, {"sequence", seq}});
        auto indexes = index_transactions(record);
        for (std::size_t t = 0; t < indexes.size(); ++t) {
            auto const& tx = record.transactions[t]; auto const& index = indexes[t];
            ledger.head.add("transactions", {{"hash", blob(index.hash)}, {"ledger_sequence", seq}, {"date", header.close_time}, {"transaction", blob(tx.transaction)}, {"metadata", blob(tx.metadata)}});
            ledger.head.add("ledger_transactions", {{"ledger_sequence", seq}, {"hash", blob(index.hash)}});
            for (auto const& id : index.accounts) ledger.head.add("account_tx", {{"account", blob(id)}, {"seq_idx", Json::array({seq, index.position})}, {"hash", blob(index.hash)}});
        }
        for (auto const& patch : record.patches) {
            ledger.diffs.push_back(Json{{"table", "diff"}, {"values", {{"seq", seq}, {"key", blob(patch.key)}}}}.dump() + '\n');
            // A deleted object's book is read from its last version.
            if (seq != range.first) ledger.books.push_back(book_base(patch.object.empty() ? ObjectRecord{patch.key, store.object(patch.key, seq - 1)} : patch));
        }
        return ledger;
    }, [&](LedgerRows& ledger) {
        auto const& record = *ledger.record;
        auto const seq = record.sequence;
        // The first ledger's state is the snapshot's; only its transaction tree is checked.
        if (replay) { if (seq == range.first) replay->apply_transactions(record); else replay->submit(ledger.record); }
        std::vector<bool> existed;  // whether each changed key was live at the previous ledger
        for (auto const& patch : record.patches) {
            auto key = key_of(patch.key);
            existed.push_back(live.contains(key));
            if (seq == range.first) continue;
            if (patch.object.empty()) live.erase(key); else live.insert(key);
        }
        write(ledger.head.text, ledger.head.count);
        std::set<Bytes> changed_books;
        for (std::size_t p = 0; p < record.patches.size(); ++p) {
            auto const& patch = record.patches[p];
            write(ledger.diffs[p], 1);
            if (seq == range.first) continue;
            // Clio 2.7.1 SuccessorExt filters MODIFIED objects: changing an
            // existing object's contents does not change its key's neighbours.
            if (existed[p] && !patch.object.empty()) continue;
            if (auto const& book = ledger.books[p]; !book.empty()) changed_books.insert(book);
            auto successor = successor_of(patch.key);
            row("successor", {{"key", blob(predecessor_of(patch.key))}, {"seq", seq}, {"next", blob(patch.object.empty() ? successor : patch.key)}});
            if (!patch.object.empty()) row("successor", {{"key", blob(patch.key)}, {"seq", seq}, {"next", blob(successor)}});
        }
        for (auto const& book : changed_books) row("successor", {{"key", blob(book)}, {"seq", seq}, {"next", blob(successor_of(book))}});
        written();
        if (seq % 100 == 0 || seq == range.last) std::cerr << "Prepared Clio ledger " << seq << " / " << range.last << '\n';
    });
    // Include one baseline version per token and per object live at the first ledger, and every
    // later version; then the NFT transactions and the MPT holders. Each column in key ranges, a
    // job each, as many as its size asks (column_bits); the jobs keep at most two ranges done
    // ahead of the one written, and a range's rows past spill_bytes wait in a file beside the plan.
    std::vector<std::string> const columns{"objects", "nft", "nft_tx", "mpt_holders"};
    std::vector<std::pair<std::size_t, unsigned>> parts;  // each column's first range, its bits
    std::size_t ranges = 0;
    auto const forced_bits = setting("XRPLA_CLIO_RANGE_BITS", 0);
    if (forced_bits > 20) throw Error("XRPLA_CLIO_RANGE_BITS must be at most 20");
    auto const spill_bytes = setting("XRPLA_CLIO_SPILL_BYTES", std::uint64_t(256) << 20);
    for (auto const& column : columns) {
        auto bits = forced_bits ? static_cast<unsigned>(forced_bits) : column_bits(store.column_bytes(column));
        parts.emplace_back(ranges, bits); ranges += std::size_t(1) << bits;
    }
    in_order<Rows>(ranges, jobs, std::size_t(jobs) + 2, [&](std::size_t i) {
        Rows part(temporary.string() + "." + std::to_string(i), static_cast<std::size_t>(spill_bytes));
        std::size_t c = columns.size() - 1;
        while (parts[c].first > i) --c;
        auto const& column = columns[c];
        auto [from, to] = key_range(i - parts[c].first, parts[c].second);
        if (column == "objects" || column == "nft") {
            Bytes prior; bool baseline = false;
            store.visit(column, from, to, [&](Bytes const& key, Bytes const& value) {
                if (key.size() != 36) throw Error("Invalid versioned key");
                auto id = key.substr(0, 32); if (id != prior) { prior = id; baseline = false; }
                auto seq = ~number(std::string_view(key).substr(32));
                if (seq > range.last || (seq <= range.first && baseline)) return;
                if (seq <= range.first) {
                    baseline = true; seq = range.first;
                    // An object deleted by then (its newest version there is the deletion) is not
                    // in the first ledger's state, so Clio's own initial load writes no row for it,
                    // and a read at any ledger of the range finds no version before a later one,
                    // as it would find the deletion. The successors come from the live keys and
                    // the diffs from the ledgers, not from these rows. (NFT versions are never
                    // empty: a burned token keeps its row, is_burned set, as in Clio's tables.)
                    if (value.empty()) return;
                }
                if (column == "objects") part.add("objects", {{"key", blob(id)}, {"sequence", seq}, {"object", blob(value)}});
                else {
                    auto nft = Json::parse(value); auto owner = r::parseBase58<r::AccountID>(nft.at("owner").get<std::string>());
                    if (!owner) throw Error("Invalid NFT owner index");
                    auto token = r::uint256::fromVoid(id.data());
                    part.add("nf_tokens", {{"token_id", blob(id)}, {"sequence", seq}, {"owner", blob(raw(*owner))}, {"is_burned", nft.at("is_burned")}});
                    part.add("nf_token_uris", {{"token_id", blob(id)}, {"sequence", seq}, {"uri", blob(from_hex(nft.at("uri")))}});
                    part.add("issuer_nf_tokens_v2", {{"issuer", blob(raw(r::nft::getIssuer(token)))}, {"taxon", r::nft::toUInt32(r::nft::getTaxon(token))}, {"token_id", blob(id)}});
                }
            });
        } else if (column == "nft_tx") {
            store.visit(column, from, to, [&](Bytes const& key, Bytes const& value) {
                if (key.size() != 40) throw Error("Invalid NFT transaction index");
                auto seq = ~number(std::string_view(key).substr(32, 4)); if (seq < range.first || seq > range.last) return;
                part.add("nf_token_transactions", {{"token_id", blob(key.substr(0, 32))}, {"seq_idx", Json::array({seq, ~number(std::string_view(key).substr(36))})}, {"hash", blob(value)}});
            });
        } else {
            // Every holder the store has seen: Clio's holder table only grows, and its
            // mpt_holders reads each candidate's MPToken at the ledger asked about (a holder that
            // left by the range's last ledger still held the token earlier in it).
            store.visit(column, from, to, [&](Bytes const& key, Bytes const&) {
                if (key.size() != 44) throw Error("Invalid MPT holder index");
                part.add("mp_token_holders", {{"mpt_id", blob(key.substr(0, 24))}, {"holder", blob(key.substr(24))}});
            });
        }
        return part;
    }, [&](Rows& part) { part.take(out); if (!out) throw Error("Writing Clio plan failed"); rows += part.count; written(); });
    if (replay) replay->wait(range.last);  // every ledger verified before the plan is complete
    out.close(); if (!out) throw Error("Closing Clio plan failed");
    // The old summary goes first: it must never describe the plan replacing the one it named.
    fs::remove(summary);
    durable_rename(temporary, path);
    write_file(summary.string() + ".part", Json{{"identity", identity}, {"rows", rows}, {"verified", verify}}.dump());
    durable_rename(summary.string() + ".part", summary);
    Json result{{"plan", path.string()}, {"rows", rows}, {"clio_version", "2.7.1"}, {"first", range.first}, {"last", range.last}, {"applied", false}, {"jobs", jobs}};
    if (!streaming) return load(result);
    // The loader has seen the plan renamed into place: it loads what is left and publishes.
    streaming->wait();
    result["load"] = Json::parse(read_file(report, 1024 * 1024)); result["applied"] = true; result["streamed"] = true;
    return result;
}
Json import_clio(ClioOptions const& options) { return import_clio(options, true); }
}
