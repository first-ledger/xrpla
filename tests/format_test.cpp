#include <xrpla/core/format.hpp>
#include <gtest/gtest.h>
#include <zstd.h>
#include <chrono>

using namespace xrpla;
using namespace xrpla::core;
namespace {
class Format : public ::testing::Test {
protected:
    fs::path dir;
    void SetUp() override { initialize(); dir = fs::temp_directory_path() / ("xrpla-format-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())); fs::create_directories(dir); }
    void TearDown() override { fs::remove_all(dir); }
};
TEST_F(Format, V1AndV2RoundTripWithDictionary) {
    auto path = dir / "chunk";
    std::vector<LedgerRecord> ledgers{{7, "header", {{"tx", "metadata"}}, {{Bytes(32, 'a'), "postimage"}, {Bytes(32, 'b'), ""}}}, {8, "next", {}, {}}};
    Bytes dictionary(4096, 'x');
    write_chunk(path, ledgers, dictionary);
    unsigned calls = 0;
    auto info = read_chunk(path, dictionary, [&](LedgerRecord&& l) {
        EXPECT_EQ(l.sequence, 7 + calls);
        if (!calls) { ASSERT_EQ(l.patches.size(), 2); EXPECT_TRUE(l.patches[1].object.empty()); EXPECT_EQ(l.transactions[0].metadata, "metadata"); }
        ++calls;
    });
    EXPECT_EQ(calls, 2); EXPECT_EQ(info.version, 2);
    EXPECT_THROW(read_chunk(path, "wrong", [](auto&&) {}), Error);
    ledgers[0].patches.clear();
    write_chunk(path, ledgers, {}, 1);
    EXPECT_EQ(read_chunk(path, {}, [](auto&&) {}).version, 1);
}
TEST_F(Format, HighCompressionPreservesRecords) {
    auto chunk = dir / "dense";
    write_chunk(chunk, {{7, "header", {{"transaction", "metadata"}}, {{Bytes(32, 'a'), Bytes(10000, 'x')}}}}, {}, 2, 19);
    unsigned records = 0;
    read_chunk(chunk, {}, [&](LedgerRecord&& ledger) {
        ++records; EXPECT_EQ(ledger.patches.at(0).object, Bytes(10000, 'x'));
        EXPECT_EQ(ledger.transactions.at(0).metadata, "metadata");
    });
    EXPECT_EQ(records, 1);
    auto snapshot = dir / "snapshot";
    write_snapshot_part(snapshot, {{Bytes(32, 'a'), Bytes(10000, 'x')}}, {}, 19);
    read_snapshot_part(snapshot, {}, 1, [&](ObjectRecord&& item) { EXPECT_EQ(item.object, Bytes(10000, 'x')); });
    EXPECT_THROW(write_snapshot_part(snapshot, {}, {}, 20), Error);
}
TEST_F(Format, ChecksumFailsBeforeAnyVisitor) {
    auto path = dir / "chunk";
    write_chunk(path, {{1, "header", {}, {}}});
    auto b = read_file(path, 10000); b[52] ^= 1; write_file(path, b);
    unsigned visited = 0;
    EXPECT_THROW(read_chunk(path, {}, [&](auto&&) { ++visited; }), Error);
    EXPECT_EQ(visited, 0);
}
TEST_F(Format, BadOffsetAndLimitsAreRejected) {
    auto path = dir / "chunk";
    write_chunk(path, {{1, "header", {}, {{Bytes(32, 'a'), "value"}}}});
    Limits limits; limits.patches_per_ledger = 0;
    EXPECT_THROW(read_chunk(path, {}, [](auto&&) {}, limits), Error);
    auto b = read_file(path, 10000); b[b.size() - 40] ^= 1; write_file(path, b);
    EXPECT_THROW(read_chunk(path, {}, [](auto&&) {}), Error);
}
TEST_F(Format, SnapshotRejectsExtraBytesConcatenationAndTruncation) {
    auto path = dir / "snapshot";
    write_snapshot_part(path, {{Bytes(32, 'a'), "object"}});
    EXPECT_NO_THROW(read_snapshot_part(path, {}, 1, [](auto&&) {}));
    EXPECT_THROW(read_snapshot_part(path, {}, 0, [](auto&&) {}), Error);
    EXPECT_THROW(read_snapshot_part(path, {}, 2, [](auto&&) {}), Error);
    auto b = read_file(path, 10000); write_file(path, b + b);
    EXPECT_THROW(read_snapshot_part(path, {}, 1, [](auto&&) {}), Error);
    // A frame with one unexpected decoded byte used to slip through finish().
    Bytes compressed(ZSTD_compressBound(1), '\0');
    compressed.resize(ZSTD_compress(compressed.data(), compressed.size(), "x", 1, 3));
    write_file(path, compressed);
    EXPECT_THROW(read_snapshot_part(path, {}, 0, [](auto&&) {}), Error);
}
}
