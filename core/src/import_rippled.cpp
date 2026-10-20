#include "store.hpp"
#include "piece_feed.hpp"
#include "nudb_builder.hpp"
#include "replay.hpp"
#include "sqlite_index.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/config/BasicConfig.h>
#include <xrpl/nodestore/DummyScheduler.h>
#include <xrpl/nodestore/Manager.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/HashPrefix.h>
#include <xrpl/rdb/DBInit.h>
#include <xrpl/protocol/AccountID.h>
#include <sqlite3.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <fstream>
#include <iterator>
#include <limits>
#include <iostream>
#include <set>
#include <thread>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
class Sql {
    sqlite3* db_ = nullptr;
public:
    explicit Sql(fs::path const& path) {
        if (sqlite3_open_v2(path.string().c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr) != SQLITE_OK) {
            auto message = db_ ? sqlite3_errmsg(db_) : "allocation failed"; std::string copy(message); if (db_) sqlite3_close(db_); throw Error("SQLite: " + copy);
        }
        sqlite3_busy_timeout(db_, 1000);
        // Restores insert into several hash-keyed B-trees: a large page cache keeps their upper
        // levels in memory (the default 2 MB held inserts to about 100k rows/s).
        exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-1048576; PRAGMA temp_store=MEMORY;");
    }
    ~Sql() { sqlite3_close(db_); }
    std::int64_t last_rowid() const { return sqlite3_last_insert_rowid(db_); }
    void exec(std::string const& sql) { if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK) throw Error("SQLite: " + std::string(sqlite3_errmsg(db_))); }
    // The first column of a query's first row ("" when none).
    std::string text(std::string const& query) {
        sqlite3_stmt* statement = nullptr;
        if (sqlite3_prepare_v2(db_, query.c_str(), -1, &statement, nullptr) != SQLITE_OK) throw Error("SQLite prepare: " + std::string(sqlite3_errmsg(db_)));
        std::string out;
        if (sqlite3_step(statement) == SQLITE_ROW && sqlite3_column_text(statement, 0)) out = reinterpret_cast<char const*>(sqlite3_column_text(statement, 0));
        sqlite3_finalize(statement);
        return out;
    }
    // Every row of a query, its columns as text ("" for NULL).
    std::vector<std::vector<std::string>> rows(std::string const& query) {
        sqlite3_stmt* statement = nullptr;
        if (sqlite3_prepare_v2(db_, query.c_str(), -1, &statement, nullptr) != SQLITE_OK) throw Error("SQLite prepare: " + std::string(sqlite3_errmsg(db_)));
        std::vector<std::vector<std::string>> out;
        while (sqlite3_step(statement) == SQLITE_ROW) {
            auto& row = out.emplace_back();
            for (int c = 0; c < sqlite3_column_count(statement); ++c) {
                auto const* value = sqlite3_column_text(statement, c);
                row.push_back(value ? reinterpret_cast<char const*>(value) : "");
            }
        }
        sqlite3_finalize(statement);
        return out;
    }
    class Statement {
        sqlite3_stmt* statement_ = nullptr;
        sqlite3* db_;
    public:
        Statement(Sql& sql, char const* query) : db_(sql.db_) {
            if (sqlite3_prepare_v2(db_, query, -1, &statement_, nullptr) != SQLITE_OK) throw Error("SQLite prepare: " + std::string(sqlite3_errmsg(db_)));
        }
        ~Statement() { sqlite3_finalize(statement_); }
        void text(int index, std::string const& value) { if (sqlite3_bind_text(statement_, index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT) != SQLITE_OK) throw Error("SQLite text binding failed"); }
        void blob(int index, Bytes const& value) { if (sqlite3_bind_blob(statement_, index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT) != SQLITE_OK) throw Error("SQLite blob binding failed"); }
        // Bound without a copy: `value` must outlive run() (the row writer's rows do).
        void text_kept(int index, std::string const& value) { if (sqlite3_bind_text(statement_, index, value.data(), static_cast<int>(value.size()), SQLITE_STATIC) != SQLITE_OK) throw Error("SQLite text binding failed"); }
        void blob_kept(int index, Bytes const& value) { if (sqlite3_bind_blob(statement_, index, value.data(), static_cast<int>(value.size()), SQLITE_STATIC) != SQLITE_OK) throw Error("SQLite blob binding failed"); }
        void number(int index, std::int64_t value) { if (sqlite3_bind_int64(statement_, index, value) != SQLITE_OK) throw Error("SQLite integer binding failed"); }
        void null(int index) { if (sqlite3_bind_null(statement_, index) != SQLITE_OK) throw Error("SQLite NULL binding failed"); }
        void run() {
            if (sqlite3_step(statement_) != SQLITE_DONE) throw Error("SQLite statement: " + std::string(sqlite3_errmsg(db_)));
            sqlite3_reset(statement_); sqlite3_clear_bindings(statement_);
        }
    };
};
// Crash tests (restore_test.py): XRPLA_TEST_EXIT_IN_FINISH=<stage> ends the process inside a
// restore's finish phase, after its last window committed: "indexes" once transaction.db's
// index pages are written and its header counts them but before SQLite records them (NuDB's key
// file is being built meanwhile), "receipt" once the key file and indexes are complete but
// before the receipt says so.
void fault_in_finish(std::string_view stage) {
    if (auto const* at = std::getenv("XRPLA_TEST_EXIT_IN_FINISH"); at && stage == at) std::_Exit(75);
}
// A restore into a new transaction.db loads bare tables and builds every index at the end.
// xrpld's schema indexes Transactions by TransID (its primary key) and AccountTransactions by
// TransID and by account; rows arriving in ledger order land all over those B-trees, and each
// row then rewrites whole pages (on the published million, the insert thread became the
// restore's limit, writing many times the data it kept). The two indexes led by LedgerSeq only
// grow at their ends, but keeping them still cost the insert thread a quarter of its time
// (200,000 recent ledgers: 992 ledgers/s without them, 824 with), and since the build reads
// the file once for all five (sqlite_index.hpp), they are built with the rest. The table's
// primary key is declared last, by rewriting the schema to the one xrpld's statements create:
// the unique index built on TransID becomes the index SQLite keeps for that primary key.
namespace bulk_sql {
// Whether transaction.db is still being loaded without its indexes.
bool loading(Sql& transactions) {
    return transactions.text("SELECT count(*) FROM sqlite_master WHERE name='sqlite_autoindex_Transactions_1'") == "0";
}
// XRPLA_SQL_BULK=0 creates xrpld's schema from the start instead (the tests compare the two).
void create(Sql& transactions) {
    auto const* chosen = std::getenv("XRPLA_SQL_BULK");
    if (chosen && std::string_view(chosen) == "0") { for (auto statement : r::kTxDbInit) transactions.exec(statement); return; }
    for (std::string statement : r::kTxDbInit) {
        if (statement.find("CREATE INDEX") != std::string::npos) continue;
        if (auto at = statement.find(" PRIMARY KEY"); at != std::string::npos) statement.erase(at, 12);
        transactions.exec(statement);
    }
}
// The indexes are built outside SQLite (sqlite_index.hpp): the tables are read once for all
// of them, on several threads, and every index is written from its sorted entries at once,
// where CREATE INDEX reads the table for each and inserts on one thread (200,000 recent
// ledgers: 47 s for all five, beside NuDB's key file, where SQLite took 240 s for three and
// the restore 487 s against 258). Their definitions are those of a database xrpld's
// statements create; XRPLA_SQL_INDEX=sqlite builds them with CREATE INDEX instead (the tests
// compare the two). Sorting spills to files beside the database.
void finish(Sql& transactions, fs::path const& directory, unsigned threads) {
    transactions.exec("DROP TABLE IF EXISTS xrpla_pseudo");  // RowWriter's, while loading
    // The schema as xrpld's statements record it, from a database they create.
    Sql reference(":memory:");
    for (auto statement : r::kTxDbInit) reference.exec(statement);
    auto table = reference.text("SELECT sql FROM sqlite_master WHERE type='table' AND name='Transactions'");
    if (table.find("PRIMARY KEY") == std::string::npos) throw Error("Unexpected Transactions schema");
    // Each index the load lacks, with its columns (in order, ascending, BINARY collation).
    std::vector<sqlite_index::Spec> specs;
    std::vector<std::string> sqls;
    bool outside = true;
    for (auto const& index : reference.rows("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' ORDER BY name")) {
        if (transactions.text("SELECT count(*) FROM sqlite_master WHERE type='index' AND name='" + index[0] + "'") != "0") continue;
        sqlite_index::Spec spec{index[0], index[1], {}, false};
        for (auto const& listed : reference.rows("SELECT name, \"unique\", partial FROM pragma_index_list('" + index[1] + "')"))
            if (listed[0] == index[0]) { spec.unique = listed[1] == "1"; outside = outside && listed[2] == "0"; }
        for (auto const& column : reference.rows("SELECT name, desc, coll FROM pragma_index_xinfo('" + index[0] + "') WHERE key=1 ORDER BY seqno")) {
            outside = outside && !column[0].empty() && column[1] == "0" && column[2] == "BINARY";
            spec.columns.push_back(column[0]);
        }
        specs.push_back(std::move(spec));
        sqls.push_back(index[2]);  // "" for the primary key's, which sqlite_master records as NULL
    }
    auto const* chosen = std::getenv("XRPLA_SQL_INDEX");
    std::optional<sqlite_index::Built> built;
    if (outside && !(chosen && std::string_view(chosen) == "sqlite")) {
        try { built = sqlite_index::build(directory / r::kTxDbName, specs, directory, threads); }
        catch (Error const& error) {  // the file is as it was: SQLite builds them
            std::cerr << "Building transaction.db's indexes with SQLite: " << error.what() << '\n';
        }
    }
    if (built) fault_in_finish("indexes");
    if (!built) {
        transactions.exec("PRAGMA temp_store=FILE");
        try { transactions.exec("PRAGMA temp_store_directory='" + directory.string() + "'"); } catch (Error const&) {}  // else the system's
        transactions.exec("PRAGMA threads=" + std::to_string(std::min(threads, 8u)));
        // One transaction, as the other path's: a stop part way leaves the loading schema
        // (the journal rolls the rest back), never the TransID index without the primary key
        // it becomes, or the table declaring a key its index is not yet named for. The DROP
        // takes away one an earlier release (which committed each statement) left behind.
        transactions.exec("BEGIN IMMEDIATE");
        transactions.exec("DROP INDEX IF EXISTS xrpla_transaction_id");
        for (std::string_view statement : r::kTxDbInit)  // each IF NOT EXISTS
            if (statement.find("CREATE INDEX") != std::string_view::npos) transactions.exec(std::string(statement));
        transactions.exec("CREATE UNIQUE INDEX xrpla_transaction_id ON Transactions(TransID)");
        transactions.exec("PRAGMA writable_schema=ON");
        {
            Sql::Statement update(transactions, "UPDATE sqlite_master SET sql=? WHERE type='table' AND name='Transactions'");
            update.text(1, table); update.run();
        }
        transactions.exec("UPDATE sqlite_master SET name='sqlite_autoindex_Transactions_1', sql=NULL WHERE type='index' AND name='xrpla_transaction_id'");
        auto version = std::stoll(transactions.text("PRAGMA schema_version"));
        transactions.exec("PRAGMA schema_version=" + std::to_string(version + 1));
        transactions.exec("PRAGMA writable_schema=OFF");
        fault_in_finish("sqlite-indexes");
        transactions.exec("COMMIT");
    } else {
        auto path = directory / r::kTxDbName;
        // Recorded in one transaction, with the schema cookie moved so every connection
        // reads the new schema.
        transactions.exec("BEGIN IMMEDIATE");
        transactions.exec("PRAGMA writable_schema=ON");
        {
            Sql::Statement add(transactions, "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql) VALUES('index', ?, ?, ?, ?)");
            for (std::size_t i = 0; i < specs.size(); ++i) {
                add.text(1, specs[i].name); add.text(2, specs[i].table); add.number(3, built->roots.at(specs[i].name));
                if (sqls[i].empty()) add.null(4); else add.text(4, sqls[i]);
                add.run();
            }
            Sql::Statement update(transactions, "UPDATE sqlite_master SET sql=? WHERE type='table' AND name='Transactions'");
            update.text(1, table); update.run();
        }
        auto version = std::stoll(transactions.text("PRAGMA schema_version"));
        transactions.exec("PRAGMA schema_version=" + std::to_string(version + 1));
        transactions.exec("PRAGMA writable_schema=OFF");
        transactions.exec("COMMIT");
        sqlite_index::built(path);
        std::cerr << "transaction.db indexes built:";
        for (auto const& [part, seconds] : built->seconds) std::cerr << ' ' << part << ' ' << static_cast<int>(seconds + 0.5) << " s;";
        if (!built->fallback.empty()) std::cerr << " (read through SQLite: " << built->fallback << ')';
        std::cerr << '\n';
    }
    transactions.exec("PRAGMA journal_mode=WAL");  // as xrpld keeps it
}
// While loading, the rollback journal rather than the write-ahead log: rows and index builds
// only add pages, which the journal never copies, while the log writes every page twice. And
// a small page cache: appends only come back to the tables' right edges, so full pages spill
// to the file while a window is open, and a Writeback has the kernel write them meanwhile.
// With the 1 GB cache a window's COMMIT wrote and synced about 400 MB at once (200,000 recent
// ledgers: 39 s of the insert thread's time).
void journal_while_loading(Sql& transactions) {
    transactions.exec("PRAGMA journal_mode=DELETE");
    transactions.exec("PRAGMA cache_size=-65536");
}
}
// Has the kernel start writing a file's dirty pages every quarter second (sync_file_range),
// from a thread of its own, so the next sync of the file finds them mostly written.
class Writeback {
    int fd_;
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stop_ = false;
    std::thread thread_;
public:
    explicit Writeback(fs::path const& path) : fd_(::open(path.c_str(), O_RDONLY | O_CLOEXEC)), thread_([this] {
        std::unique_lock lock(mutex_);
        while (!wake_.wait_for(lock, std::chrono::milliseconds(250), [&] { return stop_; })) {
#ifdef SYNC_FILE_RANGE_WRITE
            if (fd_ >= 0) (void)::sync_file_range(fd_, 0, 0, SYNC_FILE_RANGE_WRITE);
#endif
        }
    }) {}
    ~Writeback() {
        { std::lock_guard lock(mutex_); stop_ = true; }
        wake_.notify_all();
        thread_.join();
        if (fd_ >= 0) ::close(fd_);
    }
};
std::string upper(std::string value) { for (char& c : value) if (c >= 'a' && c <= 'f') c -= 'a' - 'A'; return value; }

// What the xrpld writer consumes: a verified baseline, then ledger records in order.
// The verifier's node store during a native restore: the node database being written. New
// nodes are queued and inserted by writer threads (each compressing its share before NuDB
// inserts it) while verification goes on; reads find queued nodes first, then the database.
// So the verifier keeps no scratch copy of the tree: every node is written once.
class NativeNodes final : public r::node_store::Database {
    std::shared_ptr<r::node_store::Backend> backend_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<r::NodeObject>> pending_;  // queued or being written
    r::node_store::Batch batch_;
    // store() is called from several threads (a snapshot's subtrees build in parallel): one
    // thread at a time launches a batch or waits for the one being written.
    std::mutex launch_mutex_;
    std::vector<std::future<void>> writing_;
    std::vector<std::string> writing_keys_;
    double waited_ = 0;
    std::uint64_t stored_ = 0, stored_bytes_ = 0, repeated_ = 0;
    std::size_t writers_ = 6;
    static std::string key(r::uint256 const& hash) { return std::string(reinterpret_cast<char const*>(hash.data()), hash.size()); }
    // Waits for the batch being written (launch_mutex_ held); its nodes are then readable.
    void finish_writes_held() {
        auto started = std::chrono::steady_clock::now();
        for (auto& w : writing_) w.get();
        writing_.clear();
        waited_ += std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
        std::lock_guard lock(mutex_);
        for (auto const& k : writing_keys_) pending_.erase(k);
        writing_keys_.clear();
    }
    void finish_writes() { std::lock_guard held(launch_mutex_); finish_writes_held(); }
    void launch() {
        std::lock_guard held(launch_mutex_);
        finish_writes_held();
        r::node_store::Batch batch;
        {
            std::lock_guard lock(mutex_);
            batch.swap(batch_);
        }
        if (batch.empty()) return;
        for (auto const& object : batch) writing_keys_.push_back(key(object->getHash()));
        auto share = (batch.size() + writers_ - 1) / writers_;
        for (std::size_t start = 0; start < batch.size(); start += share) {
            r::node_store::Batch part(batch.begin() + start, batch.begin() + std::min(start + share, batch.size()));
            writing_.push_back(std::async(std::launch::async, [this, part = std::move(part)] { backend_->storeBatch(part); }));
        }
    }
public:
    NativeNodes(std::shared_ptr<r::node_store::Backend> backend, r::node_store::Scheduler& scheduler, beast::Journal journal)
        : Database(scheduler, 0, [] { r::Section s; s.set("earliest_seq", "1"); return s; }(), journal), backend_(std::move(backend)) {
        if (auto const* writers = std::getenv("XRPLA_NODE_WRITERS")) writers_ = std::clamp<std::size_t>(std::stoul(writers), 1, 64);
    }
    Json stats() const { return {{"nodes", stored_}, {"bytes", stored_bytes_}, {"repeated", repeated_}, {"writers", writers_}, {"waited_seconds", waited_}}; }
    ~NativeNodes() override { try { finish_writes(); } catch (...) {} stop(); }
    // Seconds verification waited for the writers.
    double waited() const { return waited_; }
    std::shared_ptr<r::NodeObject> fetchNodeObject(r::uint256 const& hash, std::uint32_t, r::node_store::FetchReport& report, bool) override {
        {
            std::lock_guard lock(mutex_);
            if (auto it = pending_.find(key(hash)); it != pending_.end()) { report.wasFound = true; return it->second; }
        }
        std::shared_ptr<r::NodeObject> object;
        auto status = backend_->fetch(hash, &object);
        if (status == r::node_store::Status::Ok && object) { report.wasFound = true; return object; }
        if (status != r::node_store::Status::NotFound && status != r::node_store::Status::Ok) throw Error("Node database read failed");
        return {};
    }
    void store(r::NodeObjectType type, r::Blob&& data, r::uint256 const& hash, std::uint32_t) override {
        auto object = r::NodeObject::createObject(type, std::move(data), hash);
        bool full;
        {
            std::lock_guard lock(mutex_);
            if (!pending_.emplace(key(hash), object).second) { ++repeated_; return; }  // queued already
            ++stored_; stored_bytes_ += object->getData().size();
            batch_.push_back(std::move(object));
            full = batch_.size() >= 16384;
        }
        if (full) launch();
    }
    // Everything queued is in the database (not yet durable: NuDB commits on close).
    void sync() override { launch(); finish_writes(); }
    void forEach(std::function<void(std::shared_ptr<r::NodeObject>)>) override { throw Error("Native node iteration is unsupported"); }
    std::string getName() const override { return "xrpla native restore nodes"; }
    void importDatabase(r::node_store::Database&) override { throw Error("Native node import is unsupported"); }
    std::int32_t getWriteLoad() const override { return 0; }
    bool isSameDB(std::uint32_t, std::uint32_t) override { return true; }
    void sweep() override {}
};

// Runs tasks one at a time, in order, on its own thread: a ledger's SQL rows are written while
// the verifier goes on to the next ledger. After a task fails, the rest are skipped and the
// failure is rethrown to the next caller.
class SerialWorker {
    std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<std::pair<std::function<void()>, std::function<void()>>> tasks_;  // (task, if skipped)
    bool busy_ = false, stopping_ = false;
    std::exception_ptr error_;
    std::thread thread_;
    void run() {
        for (;;) {
            std::pair<std::function<void()>, std::function<void()>> task;
            bool skip;
            {
                std::unique_lock lock(mutex_);
                changed_.wait(lock, [&] { return !tasks_.empty() || stopping_; });
                if (stopping_) {
                    for (auto& [_, skipped] : tasks_) if (skipped) skipped();
                    return;
                }
                task = std::move(tasks_.front()); tasks_.pop_front(); busy_ = true;
                skip = bool(error_);
            }
            std::exception_ptr failed;
            if (skip) { if (task.second) task.second(); }
            else try { task.first(); } catch (...) { failed = std::current_exception(); }
            { std::lock_guard lock(mutex_); busy_ = false; if (failed && !error_) error_ = failed; }
            changed_.notify_all();
        }
    }
public:
    SerialWorker() : thread_([this] { run(); }) {}
    // Unfinished tasks are dropped (an error is on its way out).
    ~SerialWorker() {
        { std::lock_guard lock(mutex_); stopping_ = true; }
        changed_.notify_all();
        thread_.join();
    }
    // `skipped` runs instead of `task` when an earlier task failed or the worker stops first.
    void post(std::function<void()> task, std::function<void()> skipped = {}) {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return tasks_.size() < 64 || error_; });
        if (error_) std::rethrow_exception(error_);
        tasks_.emplace_back(std::move(task), std::move(skipped));
        lock.unlock();
        changed_.notify_all();
    }
    // Waits until every task posted has run.
    void wait() {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return (tasks_.empty() && !busy_) || error_; });
        if (error_) std::rethrow_exception(error_);
    }
};

// Intervals restored at once by default: a third of the cores, and no more than the memory
// holds (each job keeps its interval's state tree, about 4 GB for mainnet's, and buffers).
unsigned default_jobs() {
    auto jobs = std::clamp<unsigned>(std::thread::hardware_concurrency() / 3, 1u, 16u);
    std::ifstream meminfo("/proc/meminfo");
    for (std::string name; meminfo >> name;) {
        std::uint64_t kilobytes = 0;
        meminfo >> kilobytes;
        if (name == "MemAvailable:") return std::clamp<unsigned>(static_cast<unsigned>(kilobytes / (5ull << 20)), 1u, jobs);
        meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return jobs;
}
// A restore's verifier: the in-memory replay (StateReplay), or libxrpl's LedgerVerifier with
// its scratch node store (XRPLA_RESTORE_VERIFIER=libxrpl, kept to compare against). Both check
// every object, transaction, parent and root; the replay keeps no state on disk, so it resumes
// by replaying from the snapshot with writing off.
class RestoreVerifier {
    std::unique_ptr<StateReplay> replay_;
    std::unique_ptr<LedgerVerifier> libxrpl_;
public:
    static bool libxrpl_selected() {
        auto const* chosen = std::getenv("XRPLA_RESTORE_VERIFIER");
        return chosen && std::string_view(chosen) == "libxrpl";
    }
    // `make` builds the libxrpl verifier when selected; otherwise nodes go to `sink`.
    RestoreVerifier(LedgerVerifier::NodeSink sink, std::function<std::unique_ptr<LedgerVerifier>()> const& make) {
        if (libxrpl_selected()) {
            libxrpl_ = make();
            // A restore is a batch job: keep about a million touched objects' tree in memory (~2 GB).
            libxrpl_->retain(4096, 1u << 20);
        } else replay_ = std::make_unique<StateReplay>(std::move(sink));
    }
    bool replays() const { return bool(replay_); }
    void add_snapshot_objects(std::vector<ObjectRecord>&& items) {
        if (replay_) replay_->add_snapshot_objects(std::move(items)); else libxrpl_->add_snapshot_objects(std::move(items));
    }
    void finish_snapshot(Header const& h) {
        if (replay_) replay_->finish_snapshot(h.raw, h.sequence, h.account_hash); else libxrpl_->finish_snapshot(h.raw, h.sequence, h.account_hash);
    }
    // libxrpl's verifier advances: its VerifiedLedger (a copy of the record) is not wanted here.
    void apply(LedgerRecord const& ledger) { if (replay_) replay_->apply(ledger); else libxrpl_->advance(ledger); }
    // The replay verifies in a pipeline (wait for a ledger before relying on it); libxrpl's
    // verifier applies each ledger as it is submitted.
    void submit(std::shared_ptr<LedgerRecord const> ledger) { if (replay_) replay_->submit(std::move(ledger)); else libxrpl_->advance(*ledger); }
    void wait(std::uint32_t sequence) { if (replay_) replay_->wait(sequence); }
    bool verified(std::uint32_t sequence) const { return !replay_ || replay_->verified(sequence); }
    // A baseline's own ledger: only its transactions are new.
    void apply_transactions(LedgerRecord const& ledger) { if (replay_) replay_->apply_transactions(ledger); else libxrpl_->persist_transactions(ledger); }
    void persist() { if (libxrpl_) libxrpl_->persist(); }
    void writing(bool on) { if (replay_) replay_->writing(on); }
    void resume(Bytes const& header) { libxrpl_->resume(header); }  // libxrpl's only
    Header header() const { return replay_ ? replay_->header() : libxrpl_->header(); }
    Json timings() const { return replay_ ? replay_->timings() : libxrpl_->timings(); }
};

// Reads ledger records ahead on a thread of its own (reading and parsing pieces was a fifth of
// a restore's ledger time on the verifying thread).
class Prefetch {
    std::function<bool(LedgerRecord&)> next_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<LedgerRecord> ready_;
    bool ended_ = false, stopping_ = false;
    std::exception_ptr error_;
    std::thread thread_;
    void run() {
        try {
            for (;;) {
                LedgerRecord record;
                bool more = next_(record);
                std::unique_lock lock(mutex_);
                if (!more) { ended_ = true; changed_.notify_all(); return; }
                changed_.wait(lock, [&] { return ready_.size() < 64 || stopping_; });
                if (stopping_) return;
                ready_.push_back(std::move(record));
                changed_.notify_all();
            }
        } catch (...) {
            std::lock_guard lock(mutex_); error_ = std::current_exception(); ended_ = true; changed_.notify_all();
        }
    }
public:
    explicit Prefetch(std::function<bool(LedgerRecord&)> next) : next_(std::move(next)), thread_([this] { run(); }) {}
    ~Prefetch() {
        { std::lock_guard lock(mutex_); stopping_ = true; }
        changed_.notify_all();
        thread_.join();
    }
    bool next(LedgerRecord& out) {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return !ready_.empty() || ended_; });
        if (!ready_.empty()) { out = std::move(ready_.front()); ready_.pop_front(); changed_.notify_all(); return true; }
        if (error_) std::rethrow_exception(error_);
        return false;
    }
};

// A ledger's rows in xrpld's ledger.db and transaction.db.
struct LedgerRows {
    std::uint32_t sequence;
    std::string hash, parent_hash, account_hash, transaction_hash;
    std::int64_t drops, close_time, parent_close_time, resolution, flags;
    struct Tx { std::string id, type, account; std::int64_t sequence; Bytes tx, meta; std::uint32_t position; std::vector<std::string> accounts; };
    std::vector<Tx> transactions;
};
LedgerRows ledger_rows(LedgerRecord const& record) {
    auto header = decode_header(record.header);
    auto h = r::deserializeHeader({record.header.data(), record.header.size()});
    LedgerRows row{record.sequence, upper(header.hash), upper(header.parent_hash), upper(header.account_hash), upper(header.transaction_hash),
                   h.drops.drops(), h.closeTime.time_since_epoch().count(), h.parentCloseTime.time_since_epoch().count(),
                   h.closeTimeResolution.count(), h.closeFlags, {}};
    auto indexes = index_transactions(record);
    for (std::size_t i = 0; i < record.transactions.size(); ++i) {
        auto const& index = indexes[i];
        LedgerRows::Tx tx{upper(to_hex(index.hash)), index.type, r::toBase58(r::AccountID::fromVoid(index.account.data())), index.sequence,
                          record.transactions[i].transaction, record.transactions[i].metadata, index.position, {}};
        for (auto const& id : index.accounts) tx.accounts.push_back(r::toBase58(r::AccountID::fromVoid(id.data())));
        row.transactions.push_back(std::move(tx));
    }
    return row;
}
// Inserts ledgers' rows (in a transaction the caller opened).
// Crash tests (restore_test.py): the process ends where the Nth window's rows are committed and
// its receipt does not record them yet.
void fault_after_rows() {
    static std::atomic<int> commits{0};
    if (auto const* at = std::getenv("XRPLA_TEST_EXIT_AFTER_ROWS"); at && ++commits >= std::atoi(at)) std::_Exit(75);
}
// Pseudo-transactions (fee votes, amendments, UNL changes) are the only ones whose IDs can
// repeat: an identical vote in two flag ledgers before they named their ledger (mainnet's
// SetFee 1C15FEA3... is in ledgers 3,715,073 and 3,721,729). xrpld replaces the Transactions
// row of an ID it saves again and deletes its AccountTransactions rows first, so it holds one
// ledger's rows, those of the ledger it saved last: an xrpld that acquired its history
// backwards, as mainnet's full-history servers did, holds the earliest ledger's and answers tx
// with 3,715,073. These tables hold the earliest ledger's too (its Transactions row and its
// AccountTransactions rows), as every XRPLA backend answers, whatever order the ledgers are
// written in (intervals commit side by side; a resume writes a window again). Loading bare
// tables, where no unique key replaces anything, the writer records in xrpla_pseudo where each
// pseudo-transaction's rows are (finish drops the table).
struct RowWriter {
    Sql& transactions;
    bool loading;  // bare tables (bulk_sql): a ledger's rows are only ever inserted once
    Sql::Statement ledger_insert, tx_insert, account_insert, clear_accounts;
    static bool start(Sql& transactions) {
        bool bare = bulk_sql::loading(transactions);
        if (bare) transactions.exec("CREATE TABLE IF NOT EXISTS xrpla_pseudo(TransID CHARACTER(64) PRIMARY KEY, TxRowid INTEGER, AccountsFirst INTEGER, AccountsLast INTEGER)");
        return bare;
    }
    static bool pseudo(std::string const& type) { return type == "SetFee" || type == "EnableAmendment" || type == "UNLModify"; }
    RowWriter(Sql& ledgers, Sql& transactions)
        : transactions(transactions), loading(start(transactions)), ledger_insert(ledgers, "INSERT OR REPLACE INTO Ledgers VALUES(?,?,?,?,?,?,?,?,?,?)"),
          tx_insert(transactions, "INSERT OR REPLACE INTO Transactions VALUES(?,?,?,?,?,?,?,?)"),
          account_insert(transactions, "INSERT INTO AccountTransactions VALUES(?,?,?,?)"),
          clear_accounts(transactions, "DELETE FROM AccountTransactions WHERE LedgerSeq=?") {}
    // A window's rows commit before the receipt that records it, so a crash between the two
    // leaves rows a resume inserts again, and while loading no unique key replaces them (the
    // final unique index would then fail). They go first: past the rowids the receipt recorded
    // (rows are only ever appended while loading), or, from a receipt without them, by ledger
    // (reading the tables, which have no index yet).
    static Json rowids(Sql& transactions) {
        auto number = [&](char const* table) { auto text = transactions.text(std::string("SELECT max(rowid) FROM ") + table); return text.empty() ? 0 : std::stoll(text); };
        return {{"transactions", number("Transactions")}, {"accounts", number("AccountTransactions")}};
    }
    void forget(Sql& transactions, Json const& receipt, std::uint64_t after, std::uint64_t through) const {
        if (!loading) return;
        if (receipt.contains("sql_rowids")) {
            auto const& rowid = receipt.at("sql_rowids");
            transactions.exec("DELETE FROM Transactions WHERE rowid > " + std::to_string(rowid.at("transactions").get<std::int64_t>()));
            transactions.exec("DELETE FROM AccountTransactions WHERE rowid > " + std::to_string(rowid.at("accounts").get<std::int64_t>()));
        } else if (after < through) {
            auto range = " WHERE LedgerSeq > " + std::to_string(after) + " AND LedgerSeq <= " + std::to_string(through);
            transactions.exec("DELETE FROM Transactions" + range);
            transactions.exec("DELETE FROM AccountTransactions" + range);
        }
        transactions.exec("DELETE FROM xrpla_pseudo WHERE NOT EXISTS (SELECT 1 FROM Transactions WHERE rowid = xrpla_pseudo.TxRowid)");
    }
    // Before a pseudo-transaction's rows: whether this ledger's copy is written. A copy from a
    // later ledger is not; one from a later ledger already written goes (its rows), as does this
    // ledger's own from before (a window written again).
    bool keep(std::string const& id, std::uint64_t seq) {
        if (!loading) {
            auto held = transactions.text("SELECT LedgerSeq FROM Transactions WHERE TransID = '" + id + "'");
            if (!held.empty() && std::stoull(held) < seq) return false;
            transactions.exec("DELETE FROM AccountTransactions WHERE TransID = '" + id + "'");
            return true;
        }
        auto held = transactions.rows("SELECT p.TxRowid, p.AccountsFirst, p.AccountsLast, t.LedgerSeq FROM xrpla_pseudo p JOIN Transactions t ON t.rowid = p.TxRowid WHERE p.TransID = '" + id + "'");
        if (held.empty()) return true;
        auto const& at = held.front();
        if (std::stoull(at[3]) < seq) return false;
        transactions.exec("DELETE FROM Transactions WHERE rowid = " + at[0]);
        if (at[1] != "0") transactions.exec("DELETE FROM AccountTransactions WHERE rowid BETWEEN " + at[1] + " AND " + at[2]);
        return true;
    }
    // The rows' values are bound without copies: `ledger` outlives each run().
    void insert(LedgerRows const& ledger) {
        static std::string const validated = "V";
        if (!loading) { clear_accounts.number(1, ledger.sequence); clear_accounts.run(); }
        for (auto const& tx : ledger.transactions) {
            bool repeatable = pseudo(tx.type);
            if (repeatable && !keep(tx.id, ledger.sequence)) continue;
            tx_insert.text_kept(1, tx.id); tx_insert.text_kept(2, tx.type); tx_insert.text_kept(3, tx.account); tx_insert.number(4, tx.sequence);
            tx_insert.number(5, ledger.sequence); tx_insert.text_kept(6, validated); tx_insert.blob_kept(7, tx.tx); tx_insert.blob_kept(8, tx.meta); tx_insert.run();
            auto tx_rowid = transactions.last_rowid(); std::int64_t first = 0, last = 0;
            for (auto const& account : tx.accounts) {
                account_insert.text_kept(1, tx.id); account_insert.text_kept(2, account); account_insert.number(3, ledger.sequence); account_insert.number(4, tx.position); account_insert.run();
                last = transactions.last_rowid(); if (!first) first = last;
            }
            if (repeatable && loading)
                transactions.exec("INSERT OR REPLACE INTO xrpla_pseudo VALUES('" + tx.id + "', " + std::to_string(tx_rowid) + ", " + std::to_string(first) + ", " + std::to_string(last) + ")");
        }
        ledger_insert.text_kept(1, ledger.hash); ledger_insert.number(2, ledger.sequence); ledger_insert.text_kept(3, ledger.parent_hash);
        ledger_insert.number(4, ledger.drops); ledger_insert.number(5, ledger.close_time); ledger_insert.number(6, ledger.parent_close_time);
        ledger_insert.number(7, ledger.resolution); ledger_insert.number(8, ledger.flags); ledger_insert.text_kept(9, ledger.account_hash);
        ledger_insert.text_kept(10, ledger.transaction_hash); ledger_insert.run();
    }
};

struct RippledSource {
    Json identity;       // what ties an output directory to this history
    Header baseline;     // the state the verifier starts from
    std::function<void(std::function<void(ObjectRecord&&)> const&)> objects;
    // The baseline already holds the first record's state changes (a store's first ledger);
    // only that record's transactions are new. Otherwise records start after the baseline.
    bool baseline_is_first = false;
    std::uint32_t first = 0, last = 0;  // records written; earlier ones are only verified
    std::function<bool(LedgerRecord&)> next;
    std::function<void(std::uint32_t)> skip_to;  // resume after an interruption
    std::function<Bytes(std::uint32_t)> header;  // a written ledger's header (resume checks)
};
// A finished NuDB restore's receipt names its finish's output ("nudb") and where the data file
// then ended, spilled buckets included. xrpld appends the nodes it writes to that file, and a
// restore going on in the directory cuts the file back to the receipt's end, so one longer than
// that is refused before anything is written. A restore that goes on first rewrites the receipt
// without the finish: its own writes, and an interrupted run's, then leave the file longer.
void refuse_written_since_finished(fs::path const& output, Json const& receipt) {
    if (!receipt.contains("nudb") || !fs::is_directory(output / "node-db")) return;
    if (!receipt.contains("nudb_data_size"))
        throw Error("This directory's receipt names a finished NuDB restore but not where its data file then ended, so whether xrpld "
                    "has written to it since cannot be told; restoring into it again could cut off xrpld's nodes. Use a fresh output directory");
    auto end = receipt.at("nudb_data_size").get<std::uint64_t>();
    for (auto const& entry : fs::directory_iterator(output / "node-db"))
        if (entry.path().extension() == ".dat" && fs::file_size(entry.path()) > end)
            throw Error("This directory holds a finished restore that xrpld has since written to (its NuDB data file is longer than the "
                        "restore left it); restoring into it again would cut off xrpld's nodes. Use a fresh output directory");
}

Json write_rippled(RippledSource& source, fs::path const& output, std::string const& backend_name) {
    safe_cache_path(output);
    if (fs::exists(output) && !fs::is_empty(output) && !fs::exists(output / "xrpla-import.json"))
        throw Error("Import destination must be empty or an xrpla import directory");
    if (backend_name != "nudb" && backend_name != "rocksdb") throw Error("--backend must be nudb or rocksdb");
    bool const nudb = backend_name == "nudb";
    DirectoryLock target_lock(output);
    auto identity = source.identity;
    identity["format"] = nudb ? "rippled-3.3.0-nudb" : "rippled-3.3.0-rocksdb";
    auto receipt = output / "xrpla-import.json";
    std::optional<Header> resumed;
    NudbBuilder::Checkpoint checkpoint;  // where a NuDB build continues from
    Json previous = Json::object();      // the receipt found
    // Every ledger committed (the receipt names the publication's last) but the finish phase,
    // NuDB's key file and transaction.db's indexes, was interrupted: only that phase runs again,
    // from the nodes and rows the receipt names. `finish_header` is that last ledger's header.
    bool finish_only = false;
    Bytes finish_header;
    if (fs::exists(receipt)) {
        auto old = Json::parse(read_file(receipt, 1024 * 1024));
        previous = old;
        if (old.contains("intervals")) throw Error("This directory was restored by intervals; restore it the same way (--jobs other than 1)");
        if (old.value("format", "") != identity.at("format").get<std::string>())
            throw Error("Native import target version differs (xrpld release or node store backend); use a fresh output directory");
        for (auto const& [key, value] : identity.items()) if (old.at(key) != value) throw Error("Import destination belongs to different history");
        if (old.contains("last")) {
            auto last = old.at("last").get<std::uint32_t>();
            if (last > source.last || old.at("last_hash") != decode_header(source.header(last)).hash) throw Error("Import would shrink or replace published history");
            // Finished through the publication's last ledger: nothing to add (else the restore
            // would start over, emptying a database xrpld can open).
            if (old.value("finished", false) && last == source.last && fs::exists(output / "rippled.cfg")) { old.erase("last_header"); return old; }
            // Committed through the last ledger, unfinished: the finish phase alone, which needs
            // no verifier state (no ledger is applied again). The receipt's header must be the
            // ledger it names, and a NuDB build its checkpoint (else the build starts over, as
            // from a receipt of an older release).
            if (last == source.last && old.contains("last_header") && (!nudb || old.contains("nudb_data_size"))) {
                auto header = from_hex(old.at("last_header").get<std::string>());
                if (old.at("last_hash") == decode_header(header).hash) { finish_only = true; finish_header = std::move(header); }
            }
            // Continue after the last committed window when its verified state was kept.
            bool kept = !RestoreVerifier::libxrpl_selected() || (source.skip_to && (!nudb || fs::exists(output / ".verification-nodes" / "CURRENT")));
            // A NuDB restore going on with libxrpl's verifier needs the state it persisted: one
            // restored with the replay verifier (which keeps none), or whose state was deleted,
            // would otherwise start over and discard what it holds. Refused before anything is
            // written, as an interval is (restore_rippled_intervals).
            if (old.contains("last_header") && last < source.last && !kept && source.skip_to)
                throw Error("This directory is restored through ledger " + std::to_string(last) + " but libxrpl's verifier state for it (" +
                            (output / ".verification-nodes").string() + ") is missing: it was restored with the replay verifier, "
                            "or the state was deleted. Resume without XRPLA_RESTORE_VERIFIER=libxrpl (the replay starts again from the "
                            "restore's snapshot), or use a fresh output directory");
            if (old.contains("last_header") && last < source.last && kept) resumed = decode_header(from_hex(old.at("last_header").get<std::string>()));
            // The nodes of the ledgers it names end at the checkpoint, which the build resumes
            // from: without one it would cut them all off.
            if (resumed && nudb && !old.contains("nudb_data_size"))
                throw Error("This directory's receipt names restored ledgers but not where its NuDB build stopped; use a fresh output directory");
            if ((resumed || finish_only) && old.contains("nudb_data_size"))
                checkpoint = {old.at("nudb_data_size").get<std::uint64_t>(), old.at("nudb_entries").get<std::uint64_t>()};
            // The receipt's progress is this run's until a window or the finish records more.
            if (finish_only)
                for (auto const* key : {"last", "last_hash", "last_header", "sql_rowids", "nudb_data_size", "nudb_entries", "transactions", "ledgers_written"})
                    if (old.contains(key)) identity[key] = old.at(key);
        }
    }
    // Before any file is written, the receipt says what this run starts from: a restore that
    // goes on names what the one before committed, no longer its finish; one that starts over
    // names no ledgers. A stop before the first window commits then leaves no receipt naming
    // nodes the files no longer hold, or a finish whose data file has grown since.
    if (nudb) refuse_written_since_finished(output, previous);
    auto begun = resumed || finish_only ? previous : identity;
    begun.erase("finished"); begun.erase("nudb");
    if (!fs::exists(receipt) || begun != previous) { write_file(receipt.string() + ".part", begun.dump()); durable_rename(receipt.string() + ".part", receipt); }
    // libxrpl's own NodeStore backends write exactly what xrpld reads for [node_db] type=NuDB or RocksDB.
    r::node_store::DummyScheduler scheduler;
    beast::Journal journal{beast::Journal::getNullSink()};
    r::Section parameters;
    parameters.set("type", nudb ? "NuDB" : "RocksDB"); parameters.set("path", (output / "node-db").string()); parameters.set("earliest_seq", "1");
    // A large write buffer: NuDB commits it to disk in the background once it fills.
    auto backend = r::node_store::Manager::instance().makeBackend(parameters, 512 * 1024 * 1024, scheduler, journal);
    // NuDB is built directly (NudbBuilder) in the empty store its backend creates; RocksDB is
    // written through its backend (NativeNodes).
    std::unique_ptr<NudbBuilder> builder;
    if (nudb) {
        bool exists = fs::exists(output / "node-db") && std::any_of(fs::directory_iterator(output / "node-db"), fs::directory_iterator{},
                                                                    [](auto const& e) { return e.path().extension() == ".dat"; });
        if (!exists) { backend->open(); backend->close(); }
        builder = std::make_unique<NudbBuilder>(output / "node-db");
        builder->resume(checkpoint);
    } else backend->open();
    // Where a restore spends its time (reported per window): reading records, verifying,
    // writing node objects, writing SQL rows, and committing windows.
    struct Phases { double read = 0, verify = 0, nodes = 0, sql = 0, commit = 0; } phases;
    auto timed = [](double& slot, auto&& work) {
        auto t = std::chrono::steady_clock::now(); work();
        slot += std::chrono::duration<double>(std::chrono::steady_clock::now() - t).count();
    };
    // RocksDB: the verifier reads and writes the node database itself (NativeNodes); `nodes` is
    // the time it waited for the writers. NuDB: the verifier keeps its own scratch store and
    // hands every node it writes to the builder.
    std::shared_ptr<r::node_store::Backend> native(std::move(backend));
    auto nodes = nudb ? nullptr : std::make_shared<NativeNodes>(native, scheduler, journal);
    auto* database = nodes.get();
    auto durable_nodes = [&] {
        if (builder) {
            auto made = builder->checkpoint();
            identity["nudb_data_size"] = made.data_size; identity["nudb_entries"] = made.entries;
            return;
        }
        // RocksDB's writes are durable once synced; closing also covers NuDB-style buffering.
        database->sync(); phases.nodes = database->waited(); native->close(); native->open(false);
    };
    auto write_node = [&](std::uint8_t type, Bytes const& key, Bytes const& data) {
        auto object = r::NodeObject::createObject(static_cast<r::NodeObjectType>(type), r::Blob(data.begin(), data.end()), r::uint256::fromVoid(key.data()));
        if (builder) builder->add(object);
        else database->store(static_cast<r::NodeObjectType>(type), r::Blob(data.begin(), data.end()), r::uint256::fromVoid(key.data()), 0);
    };
    sqlite_index::undo_interrupted(output / r::kTxDbName);  // before rows are written past its pages
    bool new_ledgers = !fs::exists(output / r::kLgrDbName), new_txs = !fs::exists(output / r::kTxDbName);
    Sql ledgers(output / r::kLgrDbName), transactions(output / r::kTxDbName);
    if (new_ledgers) for (auto sql : r::kLgrDbInit) ledgers.exec(sql);
    if (new_txs) bulk_sql::create(transactions);
    RowWriter row_writer(ledgers, transactions);
    if (row_writer.loading) bulk_sql::journal_while_loading(transactions);
    std::optional<Writeback> writeback;
    if (row_writer.loading) writeback.emplace(output / r::kTxDbName);
    RestoreVerifier verifier(write_node, [&] {
        return nudb ? std::make_unique<LedgerVerifier>(output / ".verification-nodes", write_node) : std::make_unique<LedgerVerifier>(nodes);
    });
    std::uint64_t seq = source.baseline_is_first ? source.first : source.baseline.sequence + 1;
    auto started = std::chrono::steady_clock::now();
    Json snapshot_phases = Json::object();  // seconds reading the snapshot, adding it to the verifier, sealing it
    auto load_snapshot = [&] {
        // Batches let the verifier check and hash objects on several threads.
        std::vector<ObjectRecord> batch;
        using clock = std::chrono::steady_clock;
        clock::duration adding{};
        auto add = [&] { auto t = clock::now(); verifier.add_snapshot_objects(std::exchange(batch, {})); adding += clock::now() - t; };
        batch.reserve(4096);
        source.objects([&](ObjectRecord&& item) { batch.push_back(std::move(item)); if (batch.size() == 4096) { add(); batch.reserve(4096); } });
        add();
        auto finishing = clock::now();
        verifier.finish_snapshot(source.baseline);
        auto seconds = [](clock::duration d) { return std::chrono::duration<double>(d).count(); };
        snapshot_phases = {{"read", seconds(finishing - started - adding)}, {"add", seconds(adding)}, {"finish", seconds(clock::now() - finishing)}};
    };
    LedgerRecord record;  // moved to the SQL thread once verified
    if (resumed && verifier.replays()) {
        // The ledgers already written are replayed from the snapshot, writing nothing.
        std::cerr << "Replaying to ledger " << resumed->sequence << " to resume\n";
        verifier.writing(false);
        load_snapshot();
        for (; seq <= resumed->sequence; ++seq) {
            if (!source.next(record) || record.sequence != seq) throw Error("Ledger source is out of order at " + std::to_string(seq));
            if (source.baseline_is_first && seq == source.first) verifier.apply_transactions(record);
            else verifier.submit(std::make_shared<LedgerRecord const>(std::move(record)));
        }
        verifier.wait(resumed->sequence);
        if (verifier.header().hash != resumed->hash) throw Error("Replayed ledger " + std::to_string(resumed->sequence) + " differs from the restore's");
        verifier.writing(true);
        std::cerr << "Resuming after ledger " << resumed->sequence << '\n';
    } else if (resumed) {
        verifier.resume(resumed->raw);
        seq = resumed->sequence + 1;
        source.skip_to(static_cast<std::uint32_t>(seq));
        std::cerr << "Resuming after ledger " << resumed->sequence << '\n';
    } else if (finish_only) {
        // Nothing is read, verified or written before the finish phase: no ledger loop, no
        // prefetch, no verifier wait. The nodes are those of the receipt's checkpoint (resume()
        // cut what the interrupted key-file build spilled past it); transaction.db's rows are
        // all committed, and an index build it interrupted was undone above.
        std::cerr << "Every ledger through " << source.last << " is restored; finishing\n";
        seq = source.last + 1;
    } else load_snapshot();
    // From an unfinished last window's receipt (finish_only) nothing follows the rows it names:
    // with after == through and no receipt given, this drops nothing.
    row_writer.forget(transactions, resumed ? previous : Json::object(), seq - 1, source.last);
    if (database) database->sync();
    auto snapshot_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    auto snapshot_nodes = database ? database->stats() : builder->stats();
    phases.nodes = database ? database->waited() : 0;
    auto snapshot_waited = phases.nodes;
    auto ledgers_started = std::chrono::steady_clock::now();
    // This run's: each window's receipt records them so far, and a run that only finishes carries
    // them on from the last window's, so its finished receipt describes the run that wrote them.
    std::uint64_t tx_count = finish_only ? identity.value("transactions", std::uint64_t{0}) : 0;
    std::uint64_t written = finish_only ? identity.value("ledgers_written", std::uint64_t{0}) : 0;
    // Ledgers commit in windows: their nodes and SQL rows become durable, then the resume
    // receipt that names them. A restart drops or rewrites whatever the receipt does not name
    // and repeats idempotent writes for an incomplete window. With the replay into NuDB a
    // window commits on the SQL thread once its last ledger is verified, while the next ones
    // are verified meanwhile, NuDB's checkpoint and SQL's commit at once (they sync different
    // files); otherwise the loop commits it, nodes first (libxrpl's verifier persists its
    // state then, RocksDB syncs).
    constexpr std::uint64_t window = 2048;
    bool const background = builder && verifier.replays();
    bool in_window = false;  // on the SQL thread, or here once it is idle
    auto rows_ns = std::make_shared<std::atomic<std::int64_t>>(0);  // preparing rows, summed over threads
    double sql_seconds = 0, commit_seconds = 0;  // the SQL thread's
    // Where the SQL thread's commits go: waiting for verification (when another window ended
    // first), NuDB's checkpoint (what it took beyond SQL's COMMIT, which runs meanwhile), SQL's
    // COMMIT, the receipt.
    std::array<double, 4> commit_parts{};
    // A window is due to commit once its last ledger's rows are in, and commits once the
    // verifier has reached that ledger. The SQL thread goes on inserting the next ledgers' rows
    // meanwhile (they join the open transaction; the receipt names the window's last rowids, so
    // a resume drops them), and waits only if another window comes due first.
    struct Due { std::uint32_t seq; bool writing; Header header; Json rowids; std::uint64_t transactions, ledgers; };
    std::optional<Due> due;  // the SQL thread's
    auto commit_due = [&](bool wait) {
        if (!due || (!wait && !verifier.verified(due->seq))) return;
        using clock = std::chrono::steady_clock;
        auto part = [&, t = clock::now()](std::size_t i) mutable {
            auto now = clock::now(); commit_parts[i] += std::chrono::duration<double>(now - t).count(); t = now;
        };
        auto began = clock::now();
        verifier.wait(due->seq);  // every ledger of the window verified
        part(0);
        auto nodes = std::async(std::launch::async, [&] { return builder->checkpoint(); });
        if (in_window) { transactions.exec("COMMIT"); ledgers.exec("COMMIT"); in_window = false; fault_after_rows(); }
        part(2);
        auto made = nodes.get();
        part(1);  // what the nodes took beyond the rows
        if (due->writing) {
            identity["nudb_data_size"] = made.data_size; identity["nudb_entries"] = made.entries;
            identity["last"] = due->seq; identity["last_hash"] = due->header.hash; identity["last_header"] = to_hex(due->header.raw);
            if (row_writer.loading) identity["sql_rowids"] = due->rowids;
            identity["transactions"] = due->transactions; identity["ledgers_written"] = due->ledgers;
            write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
        }
        part(3);
        commit_seconds += std::chrono::duration<double>(clock::now() - began).count();
        due.reset();
    };
    // A ledger's rows are prepared on the shared pool, several ledgers at once, and inserted in
    // order on the SQL thread while the verifier goes on. It is declared after the statements it
    // uses, so it stops first.
    SerialWorker sql;
    Bytes last_header = finish_header;  // the last ledger's
    std::optional<Prefetch> ahead;      // none when only finishing: it would start reading
    if (!finish_only) ahead.emplace(source.next);
    for (; seq <= source.last; ++seq) {
        bool more = false;
        timed(phases.read, [&] { more = ahead->next(record); });
        if (!more) throw Error("Ledger source ended before ledger " + std::to_string(seq));
        if (record.sequence != seq) throw Error("Ledger source is out of order at " + std::to_string(seq));
        auto ledger = std::make_shared<LedgerRecord const>(std::move(record));
        auto nodes_before = phases.nodes;
        timed(phases.verify, [&] { if (source.baseline_is_first && seq == source.first) verifier.apply_transactions(*ledger); else verifier.submit(ledger); });
        if (database) phases.nodes = database->waited();
        phases.verify -= phases.nodes - nodes_before;  // waits for the writers happen inside apply
        bool writing = seq >= source.first;
        auto header = decode_header(ledger->header);
        last_header = ledger->header;
        auto sql_started = std::chrono::steady_clock::now();
        auto nodes_before_sql = phases.nodes;
        if (writing) {
            auto h = r::deserializeHeader({ledger->header.data(), ledger->header.size()});
            r::Serializer native; native.add32(static_cast<std::uint32_t>(r::HashPrefix::LedgerMaster)); r::addRaw(h, native);
            write_node(static_cast<std::uint8_t>(r::NodeObjectType::Ledger), from_hex(header.hash), Bytes(reinterpret_cast<char const*>(native.data()), native.size()));
            tx_count += ledger->transactions.size(); ++written;
            auto prepared = TaskPool::shared().submit([ledger, rows_ns] {
                auto began = std::chrono::steady_clock::now();
                auto rows = ledger_rows(*ledger);
                *rows_ns += (std::chrono::steady_clock::now() - began).count();
                return rows;
            }).share();
            sql.post([&, prepared] {
                commit_due(false);
                auto const& rows = prepared.get();
                auto inserting = std::chrono::steady_clock::now();
                if (!in_window) { ledgers.exec("BEGIN IMMEDIATE"); transactions.exec("BEGIN IMMEDIATE"); in_window = true; }
                row_writer.insert(rows);
                sql_seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - inserting).count();
            });
        }
        if (database) phases.nodes = database->waited();
        phases.sql += std::chrono::duration<double>(std::chrono::steady_clock::now() - sql_started).count() - (phases.nodes - nodes_before_sql);
        if ((writing && (seq - source.first + 1) % window == 0) || seq == source.last) {
            if (background) {
                // The counts through this ledger, taken here: the loop goes on counting meanwhile.
                sql.post([&, seq = static_cast<std::uint32_t>(seq), writing, header, txs = tx_count, ledgers_written = written] {
                    commit_due(true);  // the window before, if the verifier is not there yet
                    due = Due{seq, writing, header, row_writer.loading ? RowWriter::rowids(transactions) : Json(), txs, ledgers_written};
                    commit_due(false);
                });
            } else {
                timed(phases.commit, [&] {
                    sql.wait();
                    verifier.persist();
                    durable_nodes();
                    if (in_window) { transactions.exec("COMMIT"); ledgers.exec("COMMIT"); in_window = false; fault_after_rows(); }
                });
                if (writing) {
                    identity["last"] = seq; identity["last_hash"] = header.hash; identity["last_header"] = to_hex(header.raw);
                    if (row_writer.loading) identity["sql_rowids"] = RowWriter::rowids(transactions);
                    identity["transactions"] = tx_count; identity["ledgers_written"] = written;
                    write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
                }
            }
            auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - ledgers_started).count();
            std::cerr << "Imported rippled ledger " << seq << " / " << source.last << " (" << static_cast<std::uint64_t>(written / std::max(seconds, 1e-3)) << " ledgers/s; seconds read "
                      << static_cast<int>(phases.read) << ", verify " << static_cast<int>(phases.verify) << ", nodes " << static_cast<int>(phases.nodes)
                      << ", sql " << static_cast<int>(phases.sql) << ", commit " << static_cast<int>(phases.commit) << ")\n";
        }
    }
    if (!finish_only) verifier.wait(static_cast<std::uint32_t>(source.last));  // the replay has no ledger to wait for else
    if (background) sql.post([&] { commit_due(true); });
    sql.wait();
    phases.commit += commit_seconds;
    double const rows_seconds = *rows_ns / 1e9;
    // transaction.db's indexes are built while NuDB's key file is.
    auto indexing = std::async(std::launch::async, [&] {
        auto began = std::chrono::steady_clock::now();
        if (row_writer.loading) bulk_sql::finish(transactions, output, std::clamp(std::thread::hardware_concurrency() / 2, 2u, 16u));
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count();
    });
    auto indexed = [&] { if (indexing.valid()) identity["sql_index_seconds"] = indexing.get(); };
    if (builder) {
        // The key file makes the database complete; the receipt then records the data file as
        // finished (its spilled buckets included), for a later extension to continue from.
        identity["nudb"] = builder->finish();
        indexed();
        auto made = builder->checkpoint();
        if (builder->data_file_size() != made.data_size) throw Error("Built NuDB data file does not end where its receipt says");
        identity["nudb_data_size"] = made.data_size; identity["nudb_entries"] = made.entries;
        fault_in_finish("receipt");
        write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
        // xrpld's backend must open it and find the newest ledger and its state root.
        builder.reset();
        native->open(false);
        std::shared_ptr<r::NodeObject> found;
        auto tip = decode_header(last_header);
        for (auto const& hash : {tip.hash, tip.account_hash})
            if (native->fetch(r::uint256::fromVoid(from_hex(hash).data()), &found) != r::node_store::Status::Ok || !found)
                throw Error("Built NuDB database is missing node " + hash);
        identity["nudb"]["opened"] = true;
    } else {
        database->sync();
        phases.nodes = database->waited();
        indexed();
        fault_in_finish("receipt");
        write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
    }
    native->close();
    auto absolute = fs::absolute(output).generic_string();
    std::string config = "[server]\nport_rpc_admin_local\n[port_rpc_admin_local]\nport = 5007\nip = 127.0.0.1\nadmin = 127.0.0.1\nprotocol = http\n[database_path]\n" + absolute + "\n[node_db]\ntype = " + std::string(nudb ? "NuDB" : "RocksDB") + "\npath = " + absolute + "/node-db\n[ledger_history]\nfull\n[fetch_depth]\nfull\n[debug_logfile]\n" + absolute + "/rippled.log\n";
    write_file(output / "rippled.cfg", config);
    auto kept_header = identity.value("last_header", Json());  // the receipt keeps it, for an extension to resume from
    identity.erase("last_header");
    identity["transactions"] = tx_count; identity["ledgers_written"] = written;
    identity["seconds"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    identity["snapshot_seconds"] = snapshot_seconds;
    identity["snapshot_phase_seconds"] = snapshot_phases;
    phases.sql += sql_seconds;  // on the SQL thread, alongside the verifier
    identity["phase_seconds"] = {{"read", phases.read}, {"verify", phases.verify}, {"nodes", phases.nodes}, {"sql", phases.sql}, {"rows", rows_seconds}, {"commit", phases.commit},
                                 {"commit_parts", {{"verified", commit_parts[0]}, {"nodes_durable", commit_parts[1]}, {"sql_commit", commit_parts[2]}, {"receipt", commit_parts[3]}}}};
    identity["verify_seconds"] = verifier.timings();
    identity["snapshot_nodes"] = snapshot_nodes;
    identity["nodes"] = database ? database->stats() : Json(nullptr);
    identity["phase_seconds"]["nodes"] = phases.nodes - snapshot_waited;  // the ledger phase's share
    identity["phase_seconds"]["verify"] = phases.verify;
    identity["ledger_seconds"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - ledgers_started).count();
    identity["config"] = (output / "rippled.cfg").string();
    // The finished receipt holds the whole result, which a rerun with nothing to add answers. It is
    // written last: after the database opened and named its tip, and rippled.cfg is there, so a
    // stop or a failure before then is finished on the next run, never reported done.
    auto finished = identity;
    finished["finished"] = true;
    if (!kept_header.is_null()) finished["last_header"] = kept_header;
    write_file(receipt.string() + ".part", finished.dump()); durable_rename(receipt.string() + ".part", receipt);
    return identity;
}
}
Json import_rippled(ImportOptions const& options) {
    DirectoryLock source_lock(options.dir);
    Store store(options.dir / "store", false);
    auto covered = store.status().at("state_ledgers");
    Range range{covered.at("first"), covered.at("last")};
    if (!options.range.empty()) range = parse_range(options.range, range.last);
    if (range.first < covered.at("first").get<std::uint32_t>()) throw Error("Cannot import pruned state");
    RippledSource source;
    source.identity = {{"first", range.first}, {"first_hash", decode_header(store.header(range.first)).hash}};
    source.baseline = decode_header(store.header(range.first));
    source.baseline_is_first = true;
    source.first = range.first; source.last = range.last;
    source.objects = [&](auto const& visit) {
        std::string marker;
        do {
            auto page = store.object_page(range.first, marker, 2048, true);
            for (auto const& item : page.at("state")) visit({from_hex(item.at("index")), from_hex(item.at("data"))});
            marker = page.value("marker", "");
        } while (!marker.empty());
    };
    std::uint32_t cursor = range.first;
    source.next = [&](LedgerRecord& out) { if (cursor > range.last) return false; out = store.ledger_record(cursor++); return true; };
    source.skip_to = [&](std::uint32_t sequence) { cursor = sequence; };
    source.header = [&](std::uint32_t sequence) { return store.header(sequence); };
    return write_rippled(source, options.output, options.backend);
}
// Streams a piece publication straight into an xrpld node database: no intermediate archive
// or store. Ledgers before `first` (from the snapshot on) are verified but not written.
namespace {
// One ledger's SQL rows, kept until its window commits.
// A restore of several publication intervals at once (NuDB): each interval is verified from its
// own snapshot by its own job, into one NudbBuilder and one pair of SQL databases. Only the first
// interval writes its snapshot's nodes: every node of a later interval's starting state was
// written by that snapshot or by a ledger before it. A job commits a window of 256 ledgers by
// checkpointing the builder (nodes first), committing its buffered rows and recording its
// progress in the receipt, so an interrupted restore resumes each interval where it stopped.
Json restore_rippled_intervals(RestoreOptions const& options, std::vector<std::uint32_t> const& starts, std::uint32_t first_written,
                               std::uint32_t last, Json identity, unsigned jobs) {
    auto const& output = options.output;
    safe_cache_path(output);
    if (fs::exists(output) && !fs::is_empty(output) && !fs::exists(output / "xrpla-import.json"))
        throw Error("Import destination must be empty or an xrpla import directory");
    fs::create_directories(output);
    DirectoryLock target_lock(output);
    identity["format"] = "rippled-3.3.0-nudb";
    auto receipt = output / "xrpla-import.json";
    Json progress = Json::object();
    Json rowids = Json::object();  // the receipt's sql_rowids, when it has them
    NudbBuilder::Checkpoint checkpoint;
    std::uint64_t ledgers_before = 0, transactions_before = 0;  // what a run that only finishes reports
    if (fs::exists(receipt)) {
        auto old = Json::parse(read_file(receipt, 16 * 1024 * 1024));
        for (auto const& [key, value] : identity.items())
            if (!old.contains(key) || old.at(key) != value) throw Error("Import destination belongs to different history");
        // An extension adds intervals after the ones already restored.
        auto before = old.value("intervals", Json::array());
        if (before.empty() || before.size() > starts.size() || !std::equal(before.begin(), before.end(), starts.begin()))
            throw Error("Import destination was restored in other intervals, or not by intervals");
        if (old.value("finished", false) && old.value("last", 0u) >= last && fs::exists(output / "rippled.cfg")) return old;  // nothing to add
        progress = old.value("progress", Json::object());
        // Every window's progress is recorded with the builder's checkpoint, which the build
        // resumes from: without one it would cut every committed window's nodes off.
        if (!progress.empty() && !old.contains("nudb_data_size"))
            throw Error("This directory's receipt names restored intervals but not where its NuDB build stopped; use a fresh output directory");
        refuse_written_since_finished(output, old);
        if (old.contains("sql_rowids")) rowids = {{"sql_rowids", old.at("sql_rowids")}};
        if (old.contains("nudb_data_size")) checkpoint = {old.at("nudb_data_size").get<std::uint64_t>(), old.at("nudb_entries").get<std::uint64_t>()};
        // Carried into the receipt written below for this run's intervals: what the runs before
        // committed, until a window records more (a run stopped before its first commit resumes
        // from it). Not their finish ("nudb", "finished"): the data file grows past where it ended.
        for (auto const* key : {"last", "last_hash", "sql_rowids", "nudb_data_size", "nudb_entries", "ledgers_written", "transactions"})
            if (old.contains(key)) identity[key] = old.at(key);
        // Every interval committed through its end: this run only finishes, and its finished
        // receipt describes the run that wrote the windows, as their receipts counted it.
        // An interval that goes on with libxrpl's verifier goes on from the state it persists
        // beside the restore: that store is there before the interval's first commit and stays
        // until the restore finishes, when only the last interval's is kept (the one an extension
        // continues). No stop takes it away, only a restore begun with the replay verifier (which
        // keeps none) or a deletion: refused before anything is written (the verifier would open
        // an empty store and fail on its first lookup).
        bool finish_only = true;
        for (std::size_t k = 0; k < starts.size(); ++k) {
            auto key = std::to_string(starts[k]);
            std::uint32_t end = k + 1 < starts.size() ? starts[k + 1] : last;
            if (!progress.contains(key)) { finish_only = false; continue; }
            auto through = progress.at(key).at("last").get<std::uint32_t>();
            if (through >= end) continue;
            finish_only = false;
            auto scratch = output / (".verification-nodes-" + key);
            if (RestoreVerifier::libxrpl_selected() && !fs::exists(scratch / "CURRENT"))
                throw Error("Interval " + key + " is restored through ledger " + std::to_string(through) +
                            " but libxrpl's verifier state for it (" + scratch.string() + ") is missing: it was restored with the replay verifier, "
                            "or the state was deleted. Resume without XRPLA_RESTORE_VERIFIER=libxrpl (the replay starts again from the "
                            "interval's snapshot), or use a fresh output directory");
        }
        if (finish_only) {
            ledgers_before = old.value("ledgers_written", std::uint64_t{0});
            transactions_before = old.value("transactions", std::uint64_t{0});
        }
    }
    // Before any file is written (the builder's resume cuts the data file back to the checkpoint).
    identity["intervals"] = starts;
    identity["progress"] = progress;
    write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
    r::node_store::DummyScheduler scheduler;
    beast::Journal journal{beast::Journal::getNullSink()};
    r::Section parameters;
    parameters.set("type", "NuDB"); parameters.set("path", (output / "node-db").string()); parameters.set("earliest_seq", "1");
    std::shared_ptr<r::node_store::Backend> native = r::node_store::Manager::instance().makeBackend(parameters, 64 << 20, scheduler, journal);
    bool exists = fs::exists(output / "node-db") && std::any_of(fs::directory_iterator(output / "node-db"), fs::directory_iterator{},
                                                                [](auto const& e) { return e.path().extension() == ".dat"; });
    if (!exists) { native->open(); native->close(); }
    NudbBuilder builder(output / "node-db");
    builder.resume(checkpoint);
    sqlite_index::undo_interrupted(output / r::kTxDbName);  // before rows are written past its pages
    bool new_ledgers = !fs::exists(output / r::kLgrDbName), new_txs = !fs::exists(output / r::kTxDbName);
    Sql ledgers(output / r::kLgrDbName), transactions(output / r::kTxDbName);
    if (new_ledgers) for (auto sql : r::kLgrDbInit) ledgers.exec(sql);
    if (new_txs) bulk_sql::create(transactions);
    RowWriter row_writer(ledgers, transactions);
    if (row_writer.loading) bulk_sql::journal_while_loading(transactions);
    std::optional<Writeback> writeback;
    if (row_writer.loading) writeback.emplace(output / r::kTxDbName);
    for (std::size_t k = 0; k < starts.size(); ++k) {  // each interval from where its progress stops
        std::uint32_t start = starts[k], end = k + 1 < starts.size() ? starts[k + 1] : last;
        auto key = std::to_string(start);
        row_writer.forget(transactions, rowids, progress.contains(key) ? progress.at(key).at("last").get<std::uint32_t>() : start, end);
    }
    std::mutex commit_mutex;  // SQL and the receipt
    auto recorded = checkpoint;  // the furthest builder checkpoint in the receipt (under commit_mutex)
    Json timings = Json::object();  // per interval: snapshot and ledger seconds (under commit_mutex)
    std::atomic<std::uint64_t> written{ledgers_before}, tx_count{transactions_before};
    // This run's, in the windows it committed (under commit_mutex): each window's receipt records
    // them so far, for a run that then only finishes.
    std::uint64_t committed_ledgers = 0, committed_transactions = 0;
    auto started = std::chrono::steady_clock::now();
    // Every job's windows commit on one thread, each once its job has verified all its ledgers:
    // its nodes and SQL rows made durable (NuDB's checkpoint while the rows are inserted and
    // committed), then the receipt. The jobs go on meanwhile. A window's commit resolves the
    // future the job waits on before it ends.
    SerialWorker committer;
    auto commit = [&](std::uint32_t start, std::vector<std::shared_future<LedgerRows>> rows, Bytes last_header,
                      std::function<void(std::uint32_t)> verified) {
        auto done = std::make_shared<std::promise<void>>();
        auto finished = done->get_future().share();
        committer.post([&, start, rows = std::move(rows), last_header = std::move(last_header), verified = std::move(verified), done] {
            try {
                auto header = decode_header(last_header);
                verified(header.sequence);
                auto nodes = std::async(std::launch::async, [&] { return builder.checkpoint(); });
                ledgers.exec("BEGIN IMMEDIATE"); transactions.exec("BEGIN IMMEDIATE");
                std::uint64_t txs = 0;
                for (auto const& ledger : rows) { row_writer.insert(ledger.get()); txs += ledger.get().transactions.size(); }
                transactions.exec("COMMIT"); ledgers.exec("COMMIT");
                fault_after_rows();
                auto made = nodes.get();
                std::lock_guard lock(commit_mutex);
                progress[std::to_string(start)] = {{"last", header.sequence}, {"last_hash", header.hash}, {"last_header", to_hex(last_header)}};
                identity["progress"] = progress;
                if (row_writer.loading) identity["sql_rowids"] = RowWriter::rowids(transactions);
                // A later checkpoint may already be recorded: a resume truncates the files to the
                // recorded one, which must cover every committed row's nodes.
                if (made.data_size > recorded.data_size) recorded = made;
                identity["nudb_data_size"] = recorded.data_size; identity["nudb_entries"] = recorded.entries;
                committed_ledgers += rows.size(); committed_transactions += txs;
                identity["ledgers_written"] = committed_ledgers; identity["transactions"] = committed_transactions;
                write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
                done->set_value();
            } catch (...) { done->set_exception(std::current_exception()); throw; }
        }, [done] { done->set_exception(std::make_exception_ptr(Error("An earlier window failed to commit"))); });
        return finished;
    };
    auto interval = [&](std::size_t k) {
        std::uint32_t start = starts[k], end = k + 1 < starts.size() ? starts[k + 1] : last;
        auto scratch = output / (".verification-nodes-" + std::to_string(start));
        std::atomic<bool> loading_snapshot{true};
        // The first interval's snapshot is the base every later state builds on.
        bool writes_snapshot = k == 0;
        auto write_node = [&](std::uint8_t type, Bytes const& key, Bytes const& data) {
            if (loading_snapshot && !writes_snapshot) return;
            builder.add(r::NodeObject::createObject(static_cast<r::NodeObjectType>(type), r::Blob(data.begin(), data.end()), r::uint256::fromVoid(key.data())));
        };
        std::optional<Json> done;
        {
            std::lock_guard lock(commit_mutex);
            if (progress.contains(std::to_string(start))) done = progress.at(std::to_string(start));
        }
        if (done && done->at("last").get<std::uint32_t>() >= end) return;
        auto began = std::chrono::steady_clock::now();
        RestoreVerifier verifier(write_node, [&] { return std::make_unique<LedgerVerifier>(scratch, write_node); });
        PieceFeed feed(options.source, options.publisher_key, start + 1, end);
        std::uint32_t seq = start + 1;
        auto load_snapshot = [&] {
            if (feed.snapshot().sequence != start) throw Error("Interval " + std::to_string(start) + " has no snapshot of its own");
            std::vector<ObjectRecord> batch;
            batch.reserve(4096);
            feed.snapshot_objects([&](ObjectRecord&& item) {
                batch.push_back(std::move(item));
                if (batch.size() == 4096) { verifier.add_snapshot_objects(std::exchange(batch, {})); batch.reserve(4096); }
            });
            verifier.add_snapshot_objects(std::move(batch));
            verifier.finish_snapshot(feed.snapshot());
        };
        LedgerRecord record;
        if (done && verifier.replays()) {
            // The ledgers already written are replayed from the snapshot, writing nothing.
            auto header = decode_header(from_hex(done->at("last_header").get<std::string>()));
            verifier.writing(false);
            load_snapshot();
            for (; seq <= header.sequence; ++seq) {
                if (!feed.next(record) || record.sequence != seq) throw Error("Ledger source is out of order at " + std::to_string(seq));
                verifier.submit(std::make_shared<LedgerRecord const>(std::move(record)));
            }
            verifier.wait(header.sequence);
            if (verifier.header().hash != header.hash) throw Error("Replayed ledger " + std::to_string(header.sequence) + " differs from the restore's");
            verifier.writing(true);
        } else if (done) {
            auto header = decode_header(from_hex(done->at("last_header").get<std::string>()));
            verifier.resume(header.raw);
            seq = header.sequence + 1;
            feed.skip_to(seq);
        } else load_snapshot();
        loading_snapshot = false;
        auto ledgers_began = std::chrono::steady_clock::now();
        auto first_seq = seq;
        std::vector<std::shared_future<LedgerRows>> rows;
        // This job's last window commit: waited for before the verifier its commits wait on goes.
        std::shared_future<void> committed;
        struct Settle { std::shared_future<void>& last; ~Settle() { if (last.valid()) last.wait(); } } settle{committed};
        auto verified = [&verifier](std::uint32_t sequence) { verifier.wait(sequence); };
        Prefetch ahead([&](LedgerRecord& out) { return feed.next(out); });
        for (; seq <= end; ++seq) {
            if (!ahead.next(record) || record.sequence != seq) throw Error("Ledger source is out of order at " + std::to_string(seq));
            auto ledger = std::make_shared<LedgerRecord const>(std::move(record));
            verifier.submit(ledger);
            if (seq < first_written) continue;
            auto header = decode_header(ledger->header);
            auto h = r::deserializeHeader({ledger->header.data(), ledger->header.size()});
            r::Serializer native_header; native_header.add32(static_cast<std::uint32_t>(r::HashPrefix::LedgerMaster)); r::addRaw(h, native_header);
            builder.add(r::NodeObject::createObject(r::NodeObjectType::Ledger, r::Blob(native_header.begin(), native_header.end()), r::uint256::fromVoid(from_hex(header.hash).data())));
            tx_count += ledger->transactions.size();
            rows.push_back(TaskPool::shared().submit([ledger] { return ledger_rows(*ledger); }).share());
            ++written;
            if (rows.size() >= 2048 || seq == end) {
                verifier.persist();  // libxrpl's verifier: its state, before the window commits
                committed = commit(start, std::exchange(rows, {}), ledger->header, verified);
                auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
                std::cerr << "Interval " << start << ": ledger " << seq << " / " << end << " (" << static_cast<std::uint64_t>(written / std::max(seconds, 1e-3)) << " ledgers/s in all)\n";
            }
        }
        verifier.wait(end);
        if (committed.valid()) committed.get();
        auto now = std::chrono::steady_clock::now();
        auto ledger_seconds = std::chrono::duration<double>(now - ledgers_began).count();
        std::lock_guard lock(commit_mutex);
        timings[std::to_string(start)] = {{"snapshot_seconds", std::chrono::duration<double>(ledgers_began - began).count()},
            {"ledgers", end - first_seq + 1}, {"ledger_seconds", ledger_seconds}, {"ledgers_per_second", (end - first_seq + 1) / std::max(ledger_seconds, 1e-3)},
            {"started", std::chrono::duration<double>(began - started).count()}};
    };
    // Jobs take intervals in order; the first failure stops the others at their next ledger.
    std::atomic<std::size_t> next{0};
    std::exception_ptr failure;
    std::mutex failed;
    std::vector<std::thread> workers;
    for (unsigned j = 0; j < std::min<std::size_t>(jobs, starts.size()); ++j)
        workers.emplace_back([&] {
            for (std::size_t k; (k = next++) < starts.size();) {
                try { interval(k); }
                catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); next = starts.size(); }
            }
        });
    for (auto& worker : workers) worker.join();
    if (failure) std::rethrow_exception(failure);
    committer.wait();
    auto ledger_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    auto indexing = std::async(std::launch::async, [&] {
        auto began = std::chrono::steady_clock::now();
        if (row_writer.loading) bulk_sql::finish(transactions, output, std::clamp(std::thread::hardware_concurrency() / 2, 2u, 16u));
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count();
    });
    identity["nudb"] = builder.finish();
    identity["sql_index_seconds"] = indexing.get();
    auto made = builder.checkpoint();
    // The receipt's end is the file's: a store cut short reads as missing nodes in xrpld.
    if (builder.data_file_size() != made.data_size) throw Error("Built NuDB data file does not end where its receipt says");
    auto tip = decode_header(from_hex(progress.at(std::to_string(starts.back())).at("last_header").get<std::string>()));
    identity["nudb_data_size"] = made.data_size; identity["nudb_entries"] = made.entries;
    identity["last"] = tip.sequence; identity["last_hash"] = tip.hash;
    identity["ledgers_written"] = written.load(); identity["transactions"] = tx_count.load();
    fault_in_finish("receipt");
    write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
    // xrpld's backend must open it and find the newest ledger and its state root.
    native->open(false);
    std::shared_ptr<r::NodeObject> found;
    for (auto const& hash : {tip.hash, tip.account_hash})
        if (native->fetch(r::uint256::fromVoid(from_hex(hash).data()), &found) != r::node_store::Status::Ok || !found)
            throw Error("Built NuDB database is missing node " + hash);
    native->close();
    // Only the last interval's verifier state can be extended later.
    for (std::size_t k = 0; k + 1 < starts.size(); ++k) fs::remove_all(output / (".verification-nodes-" + std::to_string(starts[k])));
    auto absolute = fs::absolute(output).generic_string();
    write_file(output / "rippled.cfg", "[server]\nport_rpc_admin_local\n[port_rpc_admin_local]\nport = 5007\nip = 127.0.0.1\nadmin = 127.0.0.1\nprotocol = http\n[database_path]\n" + absolute +
               "\n[node_db]\ntype = NuDB\npath = " + absolute + "/node-db\n[ledger_history]\nfull\n[fetch_depth]\nfull\n[debug_logfile]\n" + absolute + "/rippled.log\n");
    // Finished only now: the database opened and named its tip, and rippled.cfg is there.
    identity["finished"] = true;
    write_file(receipt.string() + ".part", identity.dump()); durable_rename(receipt.string() + ".part", receipt);
    Json result = identity;
    result.erase("progress");
    result["ledgers_written"] = written.load(); result["transactions"] = tx_count.load(); result["jobs"] = jobs;
    result["ledger_seconds"] = ledger_seconds;
    result["interval_seconds"] = timings;
    result["seconds"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    result["config"] = (output / "rippled.cfg").string();
    return result;
}
}
Json compare_node_databases(fs::path const& a, fs::path const& b, std::string const& backend) {
    r::node_store::DummyScheduler scheduler;
    beast::Journal journal{beast::Journal::getNullSink()};
    auto open = [&](fs::path const& path) {
        r::Section parameters;
        parameters.set("type", backend == "rocksdb" ? "RocksDB" : "NuDB"); parameters.set("path", path.string()); parameters.set("earliest_seq", "1");
        auto store = r::node_store::Manager::instance().makeBackend(parameters, 64 << 20, scheduler, journal);
        store->open(false);
        return store;
    };
    auto first = open(a), second = open(b);
    // Every node of each is in the other, with the same type and data. The first's nodes are
    // fetched from the second and compared; the second's are then only looked up among the
    // first's hashes, kept sorted in memory (fetching both ways, and a set of every hash seen,
    // took hours for a million ledgers). Records a store holds twice, such as repeats a direct
    // build dropped from its key file, are counted once.
    using Key = std::array<unsigned char, 16>;  // a hash's first half tells nodes apart
    auto key_of = [](r::NodeObject const& object) { Key key; std::memcpy(key.data(), object.getHash().data(), key.size()); return key; };
    auto hex = [](r::NodeObject const& object) { return to_hex(Bytes(reinterpret_cast<char const*>(object.getHash().data()), 32)); };
    auto distinct = [](std::vector<Key>& keys) { std::sort(keys.begin(), keys.end()); keys.erase(std::unique(keys.begin(), keys.end()), keys.end()); };
    Json result{{"a", Json::object()}, {"b", Json::object()}};
    std::vector<Key> in_first, missing_first, differing;
    first->forEach([&](std::shared_ptr<r::NodeObject> object) {
        in_first.push_back(key_of(*object));
        std::shared_ptr<r::NodeObject> other;
        if (second->fetch(object->getHash(), &other) != r::node_store::Status::Ok || !other) {
            missing_first.push_back(in_first.back());
            if (missing_first.size() <= 3) result["a"]["missing"].push_back(hex(*object));
        } else if (other->getData() != object->getData() || other->getType() != object->getType()) differing.push_back(in_first.back());
    });
    distinct(in_first); distinct(missing_first); distinct(differing);
    std::vector<Key> in_second;
    std::size_t reported = 0;
    second->forEach([&](std::shared_ptr<r::NodeObject> object) {
        in_second.push_back(key_of(*object));
        if (!std::binary_search(in_first.begin(), in_first.end(), in_second.back()) && reported++ < 3) result["b"]["missing"].push_back(hex(*object));
    });
    distinct(in_second);
    std::vector<Key> missing_second;
    std::set_difference(in_second.begin(), in_second.end(), in_first.begin(), in_first.end(), std::back_inserter(missing_second));
    result["a"]["nodes"] = in_first.size(); result["a"]["missing_count"] = missing_first.size(); result["a"]["different"] = differing.size();
    result["b"]["nodes"] = in_second.size(); result["b"]["missing_count"] = missing_second.size(); result["b"]["different"] = differing.size();
    result["same"] = missing_first.empty() && missing_second.empty() && differing.empty();
    first->close(); second->close();
    return result;
}
Json restore_rippled(RestoreOptions const& options) {
    PieceFeed feed(options.source, options.publisher_key, options.first, options.last);
    auto const& base = feed.snapshot();
    // A directory restored by intervals continues by them; one restored in order, in order.
    Json before;
    if (auto receipt = options.output / "xrpla-import.json"; fs::exists(receipt)) before = Json::parse(read_file(receipt, 16 * 1024 * 1024));
    bool by_intervals = before.is_object() && before.contains("intervals");
    if (options.target == "nudb" && (by_intervals || (options.jobs != 1 && !before.is_object()))) {
        // Interval starts: the base, then every published snapshot that starts a range, up to the
        // end. Each interval after the first loads its snapshot's whole state first (about 10 s
        // for mainnet's, some 3,000 ledgers' work), so unless --jobs names a count, intervals
        // shorter than kMinimum are merged into the one before them.
        constexpr std::uint32_t kMinimum = 8192;
        std::uint32_t minimum = options.jobs ? 1 : kMinimum;
        std::vector<std::uint32_t> starts;
        if (by_intervals) for (auto const& start : before.at("intervals")) starts.push_back(start.get<std::uint32_t>());
        else starts.push_back(base.sequence);
        std::vector<std::uint32_t> snapshots;
        for (auto const& snapshot : feed.catalog().at("snapshots")) {
            auto seq = snapshot.at("seq").get<std::uint32_t>();
            bool starts_range = std::any_of(feed.ranges().begin(), feed.ranges().end(), [&](Json const& range) { return range.at("first").get<std::uint32_t>() == seq + 1; });
            if (seq > starts.back() && seq < feed.last() && starts_range) snapshots.push_back(seq);
        }
        std::sort(snapshots.begin(), snapshots.end());
        auto restored = starts.size();
        for (auto seq : snapshots) if (seq - starts.back() >= minimum) starts.push_back(seq);
        if (starts.size() > restored && feed.last() - starts.back() < minimum) starts.pop_back();  // a short last interval joins the one before
        if (starts.size() > 1 || by_intervals) {
            auto jobs = options.jobs ? options.jobs : default_jobs();
            Json identity{{"source", "xrpla-pieces"}, {"baseline", base.sequence}, {"baseline_hash", base.hash},
                          {"first", std::max<std::uint32_t>(options.first.value_or(base.sequence + 1), base.sequence + 1)}};
            auto result = restore_rippled_intervals(options, starts, identity.at("first"), feed.last(), identity, jobs);
            result["bytes_read"] = feed.bytes_read();
            return result;
        }
    }
    RippledSource source;
    source.identity = {{"source", "xrpla-pieces"}, {"baseline", base.sequence}, {"baseline_hash", base.hash}};
    source.baseline = base;
    source.first = std::max<std::uint32_t>(options.first.value_or(base.sequence + 1), base.sequence + 1);
    source.last = feed.last();
    source.identity["first"] = source.first;
    source.objects = [&](auto const& visit) { feed.snapshot_objects(visit); };
    source.next = [&](LedgerRecord& out) { return feed.next(out); };
    source.skip_to = [&](std::uint32_t sequence) { feed.skip_to(sequence); };
    // Resume checks compare the receipt's last hash with the publication's.
    std::optional<std::pair<std::uint32_t, Bytes>> cached;
    source.header = [&](std::uint32_t sequence) -> Bytes {
        if (cached && cached->first == sequence) return cached->second;
        PieceFeed probe(options.source, options.publisher_key, sequence, sequence);
        probe.skip_to(sequence);
        LedgerRecord record;
        if (!probe.next(record)) throw Error("Ledger missing from publication");
        cached = std::pair{sequence, record.header};
        return record.header;
    };
    auto result = write_rippled(source, options.output, options.target);
    result["bytes_read"] = feed.bytes_read();
    return result;
}
}
