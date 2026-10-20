// The bound on a piece cache's key listings (KeyWalk): spent, it fails the request with xrpld's
// tooBusy, as the RPC layer's own failure, never with a partial answer or an internal error.
#include "../core/src/piece_history.hpp"
#include "../core/src/rpc.hpp"
#include <gtest/gtest.h>

using namespace xrpla::core;

TEST(KeyWalk, SpendsUpToItsBudgetThenAnswersTooBusy) {
    KeyWalk walk;
    EXPECT_EQ(walk.left, KeyWalk::budget);
    // Far past real listings, which walk tens of millions of records (a busy book read years back).
    EXPECT_GE(KeyWalk::budget, std::uint64_t(500'000'000));
    walk.spend(KeyWalk::budget - 1);
    walk.spend();  // the last one the budget holds
    EXPECT_EQ(walk.left, 0u);
    try {
        walk.spend();
        FAIL() << "a spent walk went on";
    } catch (rpc::Failure const& failure) {
        EXPECT_EQ(failure.value.value("error", ""), "tooBusy");
    }
    EXPECT_EQ(walk.left, 0u);  // a refused step charges nothing
}

TEST(KeyWalk, ARefusedStepIsNotAStdException) {
    // Fallbacks on the way (a prefetch, a listing's caller) catch std::exception; the refusal must
    // reach rpc_request, which answers it as given.
    KeyWalk walk;
    walk.left = 3;
    bool as_std = false, as_failure = false;
    try { walk.spend(4); }
    catch (std::exception const&) { as_std = true; }
    catch (rpc::Failure const&) { as_failure = true; }
    EXPECT_FALSE(as_std);
    EXPECT_TRUE(as_failure);
    EXPECT_EQ(walk.left, 3u);
}

TEST(KeyWalk, ADeadlineLeavesRoomForTheLongestRealListing) {
    // A busy book read from the first snapshot walks over 33 million records in 18 s, cold.
    EXPECT_GE(KeyWalk::time, std::chrono::seconds(60));
    KeyWalk walk;
    EXPECT_GT(walk.deadline, std::chrono::steady_clock::now() + std::chrono::seconds(50));
    // Steps past many clock checks go on while the deadline is ahead.
    for (int i = 0; i < 4; ++i) walk.spend(KeyWalk::check_every);
    for (std::uint64_t i = 0; i < 3 * KeyWalk::check_every; ++i) walk.spend();
    EXPECT_EQ(walk.left, KeyWalk::budget - 7 * KeyWalk::check_every);
}

TEST(KeyWalk, PastItsDeadlineAnswersTooBusyAtTheNextClockCheck) {
    KeyWalk walk;
    walk.deadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    // The clock is read once per check_every records: the steps before the check go on.
    walk.spend(KeyWalk::check_every - 1);
    auto left = walk.left;
    try {
        walk.spend();
        FAIL() << "a walk past its deadline went on";
    } catch (rpc::Failure const& failure) {
        EXPECT_EQ(failure.value.value("error", ""), "tooBusy");
    }
    EXPECT_EQ(walk.left, left);  // a refused step charges nothing
    // One step as large as a check is checked at once.
    KeyWalk large;
    large.deadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    EXPECT_THROW(large.spend(KeyWalk::check_every), rpc::Failure);
    EXPECT_EQ(large.left, KeyWalk::budget);
}
// The cache keys' hash reads all 32 bytes, whichever byte type holds them: keys sharing a long
// prefix (a book's quality directories, an owner's NFT pages) still spread.
TEST(SeededKeyHash, EveryByteCountsForEitherByteType) {
    std::array<char, 32> signed_key{};
    std::array<unsigned char, 32> unsigned_key{};
    for (std::size_t i = 0; i < 32; ++i) { signed_key[i] = static_cast<char>(i * 7); unsigned_key[i] = static_cast<unsigned char>(i * 7); }
    SeededKeyHash hash;
    EXPECT_EQ(hash(signed_key), hash(unsigned_key));
    std::set<std::size_t> seen{hash(unsigned_key)};
    for (std::size_t i = 0; i < 32; ++i) {
        auto changed = unsigned_key;
        changed[i] ^= 1;
        seen.insert(hash(changed));
    }
    EXPECT_EQ(seen.size(), 33U);
}
