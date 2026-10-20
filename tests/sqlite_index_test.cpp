// Indexes built outside SQLite (core/src/sqlite_index.cpp) against SQLite's own CREATE INDEX.
#include "../core/src/sqlite_index.hpp"
#include <gtest/gtest.h>
#include <sqlite3.h>
#include <chrono>
#include <cstdlib>
#include <random>

using namespace xrpla;
using namespace xrpla::core;
namespace {
struct Db {
    sqlite3* db = nullptr;
    explicit Db(fs::path const& path) { if (sqlite3_open(path.string().c_str(), &db) != SQLITE_OK) throw std::runtime_error("Cannot open " + path.string()); }
    ~Db() { sqlite3_close(db); }
    void exec(std::string const& sql) {
        char* error = nullptr;
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
            std::string message = error ? error : "?";
            sqlite3_free(error);
            throw std::runtime_error(message + ": " + sql);
        }
    }
    // Each row's columns, joined.
    std::vector<std::string> rows(std::string const& sql) {
        sqlite3_stmt* statement = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db));
        std::vector<std::string> out;
        while (sqlite3_step(statement) == SQLITE_ROW) {
            std::string row;
            for (int c = 0; c < sqlite3_column_count(statement); ++c)
                row += (sqlite3_column_type(statement, c) == SQLITE_NULL ? std::string("NULL") : reinterpret_cast<char const*>(sqlite3_column_text(statement, c))) + "|";
            out.push_back(row);
        }
        sqlite3_finalize(statement);
        return out;
    }
};
// Text keys of varied length with repeats and NULLs, and integers of every stored width
// (0 and 1 included, which records store as serial types 8 and 9).
void fill(Db& db, int rows, unsigned seed, int page_size) {
    db.exec("PRAGMA page_size=" + std::to_string(page_size));
    db.exec("CREATE TABLE T (id TEXT, account TEXT, seq BIGINT UNSIGNED, pos INTEGER, body BLOB)");
    db.exec("BEGIN");
    std::mt19937_64 random(seed);
    sqlite3_stmt* insert = nullptr;
    sqlite3_prepare_v2(db.db, "INSERT INTO T VALUES(?,?,?,?,?)", -1, &insert, nullptr);
    std::int64_t const widths[] = {0, 1, -1, 127, -129, 40000, -8388609, 3000000000LL, -140737488355329LL, 1LL << 62};
    for (int i = 0; i < rows; ++i) {
        std::string id(64, '0');
        for (auto& c : id) c = "0123456789ABCDEF"[random() % 16];
        std::string account = "r" + std::string(24 + random() % 10, 'a');
        for (std::size_t c = 1; c < account.size(); ++c) account[c] = "abcXYZ123"[random() % 9];
        if (i % 7 == 3) account = "rRepeated";
        sqlite3_bind_text(insert, 1, id.data(), static_cast<int>(id.size()), SQLITE_TRANSIENT);
        if (i % 11 == 5) sqlite3_bind_null(insert, 2);
        else sqlite3_bind_text(insert, 2, account.data(), static_cast<int>(account.size()), SQLITE_TRANSIENT);
        sqlite3_bind_int64(insert, 3, widths[random() % 10]);
        sqlite3_bind_int64(insert, 4, static_cast<std::int64_t>(random() % 5));
        // Rows as long as a transaction's: on 4 KB pages many spill into overflow pages after
        // their keys, which stay on the page (on smaller pages the keys could spill too).
        std::string body(random() % (page_size >= 4096 ? 6000 : page_size >= 1024 ? 600 : 100), 'b');
        sqlite3_bind_blob(insert, 5, body.data(), static_cast<int>(body.size()), SQLITE_TRANSIENT);
        sqlite3_step(insert);
        sqlite3_reset(insert);
    }
    sqlite3_finalize(insert);
    db.exec("COMMIT");
}
std::vector<sqlite_index::Spec> const specs{{"byid", "T", {"id"}, false}, {"byaccount", "T", {"account", "seq", "pos", "id"}, false}};
std::vector<std::string> const sqls{"CREATE INDEX byid ON T(id)", "CREATE INDEX byaccount ON T(account, seq, pos, id)"};
// Records built indexes in sqlite_master, as a restore does.
void record(Db& db, sqlite_index::Built const& built) {
    db.exec("BEGIN IMMEDIATE");
    db.exec("PRAGMA writable_schema=ON");
    for (std::size_t i = 0; i < specs.size(); ++i)
        db.exec("INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql) VALUES('index', '" + specs[i].name + "', 'T', " +
                std::to_string(built.roots.at(specs[i].name)) + ", '" + sqls[i] + "')");
    auto version = std::stoll(db.rows("PRAGMA schema_version").at(0));
    db.exec("PRAGMA schema_version=" + std::to_string(version + 1));
    db.exec("PRAGMA writable_schema=OFF");
    db.exec("COMMIT");
}
fs::path scratch_dir(char const* name) {
    auto dir = fs::temp_directory_path() / (std::string(name) + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(dir);
    return dir;
}
// Builds the same table's indexes both ways and compares everything a reader sees.
void compare(fs::path const& dir, int rows, int page_size) {
    auto name = std::to_string(page_size) + "-" + std::to_string(rows) + ".db";
    auto a = dir / ("a-" + name), b = dir / ("b-" + name);
    Db sqlite(a), outside(b);
    fill(sqlite, rows, static_cast<unsigned>(rows), page_size);
    fill(outside, rows, static_cast<unsigned>(rows), page_size);
    for (auto const& sql : sqls) sqlite.exec(sql);
    auto built = sqlite_index::build(b, specs, dir, 3);
    EXPECT_EQ(built.fallback, "") << rows << " rows, page " << page_size;  // read straight from the file
    record(outside, built);
    sqlite_index::built(b);
    ASSERT_EQ(outside.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"}) << rows << " rows, page " << page_size;
    // In index order (the index answers ORDER BY), and through lookups down its interior pages.
    for (auto const* query : {"SELECT id, rowid FROM T INDEXED BY byid ORDER BY id, rowid",
                              "SELECT account, seq, pos, id, rowid FROM T INDEXED BY byaccount ORDER BY account, seq, pos, id, rowid",
                              "SELECT count(*) FROM T INDEXED BY byaccount WHERE account = 'rRepeated' AND seq >= 0"})
        EXPECT_EQ(outside.rows(query), sqlite.rows(query)) << query << " (" << rows << " rows, page " << page_size << ")";
    for (auto const& id : sqlite.rows("SELECT id FROM T WHERE rowid % 13 = 1")) {
        auto key = id.substr(0, id.size() - 1);
        auto query = "SELECT rowid FROM T INDEXED BY byid WHERE id = '" + key + "'";
        EXPECT_EQ(outside.rows(query), sqlite.rows(query)) << key;
    }
}
}
TEST(SqliteIndex, MatchesCreateIndex) {
    auto dir = scratch_dir("xrpla-sqlite-index-");
    // Small pages make deep trees of few rows: every size up to 150 lands leaves and interior
    // pages on each boundary (1,024 bytes: the smallest whose cells hold these entries whole).
    for (int rows = 0; rows <= 150; ++rows) compare(dir, rows, 1024);
    // 150,000 rows: each index takes several blocks of pages, interleaved with the other's
    // (both are written at once), and names children in earlier blocks.
    for (int rows : {600, 5000, 150000}) compare(dir, rows, 1024);
    for (int rows : {0, 1, 3000, 40000}) compare(dir, rows, 4096);
    fs::remove_all(dir);
}
TEST(SqliteIndex, UniqueIndexRefusesRepeats) {
    auto dir = scratch_dir("xrpla-sqlite-unique-");
    {
        Db db(dir / "t.db");
        fill(db, 50, 7, 4096);
        db.exec("INSERT INTO T SELECT * FROM T WHERE rowid = 20");
    }
    try {
        sqlite_index::build(dir / "t.db", {{"byid", "T", {"id"}, true}}, dir, 2);
        ADD_FAILURE() << "A repeated key was indexed as unique";
    } catch (Error const& error) { EXPECT_NE(std::string(error.what()).find("UNIQUE"), std::string::npos); }
    fs::remove_all(dir);
}
TEST(SqliteIndex, AnInterruptedBuildIsUndone) {
    auto dir = scratch_dir("xrpla-sqlite-undo-");
    auto path = dir / "t.db";
    Db db(path);
    fill(db, 3000, 3, 4096);
    auto pages = std::stoll(db.rows("PRAGMA page_count").at(0));
    (void)sqlite_index::build(path, specs, dir, 2);  // stopped before its indexes were recorded
    EXPECT_GT(std::stoll(db.rows("PRAGMA page_count").at(0)), pages);
    auto built = sqlite_index::build(path, specs, dir, 2);  // undoes the first, then builds
    record(db, built);
    sqlite_index::built(path);
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    EXPECT_EQ(db.rows("SELECT count(*) FROM T INDEXED BY byid WHERE id >= '8'"), db.rows("SELECT count(*) FROM T NOT INDEXED WHERE id >= '8'"));
    fs::remove_all(dir);
}
// A marker left by an interrupted build, then rows SQLite wrote past the pages it had: those
// pages are live, so the next build keeps them (the build's own pages stay unused). A writer
// that reopens the file undoes the build first, which leaves the file whole.
TEST(SqliteIndex, AStaleMarkerKeepsPagesWrittenSince) {
    auto dir = scratch_dir("xrpla-sqlite-stale-");
    auto path = dir / "t.db";
    Db db(path);
    fill(db, 3000, 3, 4096);
    (void)sqlite_index::build(path, specs, dir, 2);  // stopped before its indexes were recorded
    ASSERT_TRUE(fs::exists(path.string() + "-xrpla-index"));
    db.exec("INSERT INTO T SELECT * FROM T");  // written past the build's pages
    auto rows = db.rows("SELECT count(*) FROM T");
    auto built = sqlite_index::build(path, specs, dir, 2);
    record(db, built);
    sqlite_index::built(path);
    EXPECT_EQ(db.rows("SELECT count(*) FROM T NOT INDEXED"), rows);
    EXPECT_EQ(db.rows("SELECT count(*) FROM T INDEXED BY byid WHERE id >= '8'"), db.rows("SELECT count(*) FROM T NOT INDEXED WHERE id >= '8'"));
    for (auto const& problem : db.rows("PRAGMA integrity_check")) EXPECT_NE(problem.find("never used"), std::string::npos) << problem;
    fs::remove_all(dir);
}
TEST(SqliteIndex, AWriterUndoesAnInterruptedBuildFirst) {
    auto dir = scratch_dir("xrpla-sqlite-reopen-");
    auto path = dir / "t.db";
    {
        Db db(path);
        fill(db, 3000, 3, 4096);
        (void)sqlite_index::build(path, specs, dir, 2);  // stopped before its indexes were recorded
    }
    sqlite_index::undo_interrupted(path);
    EXPECT_FALSE(fs::exists(path.string() + "-xrpla-index"));
    Db db(path);
    db.exec("INSERT INTO T SELECT * FROM T");
    auto built = sqlite_index::build(path, specs, dir, 2);
    record(db, built);
    sqlite_index::built(path);
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    EXPECT_EQ(db.rows("SELECT count(*) FROM T"), std::vector<std::string>{"6000|"});
    fs::remove_all(dir);
}
// An undo stopped after it cut the file back and before it wrote the header leaves a header that
// still counts the build's pages, at the counter the build left: the next undo takes it back.
TEST(SqliteIndex, AnUndoStoppedPartWayIsUndoneAgain) {
    auto dir = scratch_dir("xrpla-sqlite-undo-again-");
    auto path = dir / "t.db";
    std::uintmax_t size = 0;
    {
        Db db(path);
        fill(db, 3000, 3, 4096);
        size = fs::file_size(path);
        (void)sqlite_index::build(path, specs, dir, 2);  // stopped before its indexes were recorded
    }
    EXPECT_FALSE(fs::exists(path.string() + "-xrpla-index.part"));
    ASSERT_GT(fs::file_size(path), size);
    fs::resize_file(path, size);  // the undo's cut, without its header
    sqlite_index::undo_interrupted(path);
    EXPECT_FALSE(fs::exists(path.string() + "-xrpla-index"));
    EXPECT_EQ(fs::file_size(path), size);
    Db db(path);
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    EXPECT_EQ(std::stoull(db.rows("PRAGMA page_count").at(0)) * 4096, size);
    auto built = sqlite_index::build(path, specs, dir, 2);
    record(db, built);
    sqlite_index::built(path);
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    fs::remove_all(dir);
}
// A write-ahead log beside a marker holds commits the file's header does not show: the undo
// cannot tell what wrote since, and refuses, the file and the marker left as they are.
TEST(SqliteIndex, AWriteAheadLogBesideAMarkerIsRefused) {
    auto dir = scratch_dir("xrpla-sqlite-wal-");
    auto path = dir / "t.db";
    Db db(path);
    fill(db, 3000, 3, 4096);
    (void)sqlite_index::build(path, specs, dir, 2);  // stopped before its indexes were recorded
    db.exec("PRAGMA journal_mode=WAL");
    db.exec("INSERT INTO T SELECT * FROM T WHERE rowid <= 10");  // in the log while the connection is open
    ASSERT_GT(fs::file_size(path.string() + "-wal"), 0u);
    auto size = fs::file_size(path);
    EXPECT_THROW(sqlite_index::undo_interrupted(path), Error);
    EXPECT_TRUE(fs::exists(path.string() + "-xrpla-index"));
    EXPECT_EQ(fs::file_size(path), size);
    fs::remove_all(dir);
}
TEST(SqliteIndex, AnEntryTooLargeForACellLeavesTheFileAsItWas) {
    auto dir = scratch_dir("xrpla-sqlite-large-");
    auto path = dir / "t.db";
    Db db(path);
    fill(db, 200, 5, 512);  // 512-byte pages: a cell holds at most 102 bytes whole
    auto before = read_file(path, 64u << 20);
    EXPECT_THROW(sqlite_index::build(path, specs, dir, 2), Error);
    EXPECT_EQ(read_file(path, 64u << 20), before);
    EXPECT_FALSE(fs::exists(path.string() + "-xrpla-index"));
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    fs::remove_all(dir);
}
// A build that fails once its header counts its pages takes the header back with the pages: a
// header counting more pages than the file holds would leave it corrupt to SQLite, with no
// marker left for an undo to repair it.
TEST(SqliteIndex, ABuildFailingAfterItsHeaderTakesTheHeaderBack) {
    auto dir = scratch_dir("xrpla-sqlite-header-");
    auto path = dir / "t.db";
    Db db(path);
    fill(db, 3000, 3, 4096);
    auto size = fs::file_size(path);
    auto pages = db.rows("PRAGMA page_count");
    ::setenv("XRPLA_TEST_FAIL_IN_BUILD", "header", 1);
    EXPECT_THROW(sqlite_index::build(path, specs, dir, 2), Error);
    ::unsetenv("XRPLA_TEST_FAIL_IN_BUILD");
    EXPECT_EQ(fs::file_size(path), size);
    EXPECT_FALSE(fs::exists(path.string() + "-xrpla-index"));
    EXPECT_EQ(db.rows("PRAGMA page_count"), pages);
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    auto built = sqlite_index::build(path, specs, dir, 2);
    record(db, built);
    sqlite_index::built(path);
    EXPECT_EQ(db.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"});
    EXPECT_EQ(db.rows("SELECT count(*) FROM T INDEXED BY byid WHERE id >= '8'"), db.rows("SELECT count(*) FROM T NOT INDEXED WHERE id >= '8'"));
    fs::remove_all(dir);
}
// A build that fails once another has written the file (its header's counter past the build's):
// pages past the build's start may be live, so the file is left as it is and only the marker goes.
TEST(SqliteIndex, ABuildFailingAfterAnotherWroteLeavesTheFile) {
    auto dir = scratch_dir("xrpla-sqlite-written-since-");
    auto path = dir / "t.db";
    Db db(path);
    fill(db, 3000, 3, 4096);
    auto size = fs::file_size(path);
    ::setenv("XRPLA_TEST_FAIL_IN_BUILD", "written-since", 1);
    EXPECT_THROW(sqlite_index::build(path, specs, dir, 2), Error);
    ::unsetenv("XRPLA_TEST_FAIL_IN_BUILD");
    EXPECT_GT(fs::file_size(path), size);
    EXPECT_FALSE(fs::exists(path.string() + "-xrpla-index"));
    EXPECT_EQ(std::stoull(db.rows("PRAGMA page_count").at(0)) * 4096, fs::file_size(path));
    for (auto const& problem : db.rows("PRAGMA integrity_check")) EXPECT_NE(problem.find("never used"), std::string::npos) << problem;
    EXPECT_EQ(db.rows("SELECT count(*) FROM T NOT INDEXED"), std::vector<std::string>{"3000|"});
    fs::remove_all(dir);
}
// Tables the file is not read straight from are read through SQLite, with the same result: a
// key column in overflow pages (after a long blob), and a rowid alias (INTEGER PRIMARY KEY).
TEST(SqliteIndex, UnusualTablesAreReadThroughSQLite) {
    auto dir = scratch_dir("xrpla-sqlite-unusual-");
    for (auto const* schema : {"CREATE TABLE T (body BLOB, id TEXT, account TEXT, seq INTEGER, pos INTEGER)",
                               "CREATE TABLE T (seq INTEGER PRIMARY KEY, body BLOB, id TEXT, account TEXT, pos INTEGER)"}) {
        auto a = dir / "a.db", b = dir / "b.db";
        fs::remove(a); fs::remove(b);
        Db sqlite(a), outside(b);
        for (auto* db : {&sqlite, &outside}) {
            db->exec("PRAGMA page_size=1024");
            db->exec(schema);
            db->exec("BEGIN");
            for (int i = 0; i < 400; ++i)
                db->exec("INSERT INTO T (body, id, account, seq, pos) VALUES (randomblob(" + std::to_string(500 + i * 7 % 2500) + "), '" +
                         std::to_string(1000000 + i * 7919 % 1000) + "', 'r" + std::to_string(i % 37) + "', " + std::to_string(i * 3) + ", " + std::to_string(i % 4) + ")");
            db->exec("COMMIT");
        }
        for (auto const& sql : sqls) sqlite.exec(sql);
        auto built = sqlite_index::build(b, specs, dir, 3);
        EXPECT_NE(built.fallback, "") << schema;
        record(outside, built);
        sqlite_index::built(b);
        EXPECT_EQ(outside.rows("PRAGMA integrity_check"), std::vector<std::string>{"ok|"}) << schema;
        for (auto const* query : {"SELECT id, rowid FROM T INDEXED BY byid ORDER BY id, rowid",
                                  "SELECT account, seq, pos, id, rowid FROM T INDEXED BY byaccount ORDER BY account, seq, pos, id, rowid"})
            EXPECT_EQ(outside.rows(query), sqlite.rows(query)) << schema << ": " << query;
    }
    fs::remove_all(dir);
}
