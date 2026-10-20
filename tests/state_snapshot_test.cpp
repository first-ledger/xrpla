// A state snapshot written as the follower passes an interval's boundary, and taken again by a
// batch retried after failing past it: written whole, and reused only when its parts hold the
// verified state itself, not merely what the index beside them says they hold.
#include "core_fixture.hpp"
#include <gtest/gtest.h>
#include <chrono>

using namespace xrpla;
using namespace xrpla::core;

TEST(StateSnapshot, ReusedOnlyWhenThePartsHoldTheVerifiedState) {
    initialize();
    auto dir = fs::temp_directory_path() / ("xrpla-reused-snapshot-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Chain chain(4);
    fs::create_directories(dir / "nodes");
    {
        LedgerVerifier verifier(dir / "nodes");
        chain.seed(verifier);
        for (auto const& ledger : chain.ledgers) verifier.advance(ledger);
        auto out = dir / "out";
        EXPECT_FALSE(existing_state_snapshot(verifier, out).has_value());
        auto written = write_state_snapshot(verifier, out, 1, 2);
        auto prefix = out / written.at("prefix").get<std::string>();
        // The index goes in by a rename: no temporary file is left beside it.
        EXPECT_FALSE(fs::exists(prefix / "index.json.part"));
        auto existing = existing_state_snapshot(verifier, out);
        ASSERT_TRUE(existing.has_value());
        EXPECT_EQ(*existing, written);

        // A part rewritten with one object changed, and the index listing its new digest: the
        // digests all agree, so only the comparison with the verified state refuses it.
        auto index_path = out / written.at("index").at("object").get<std::string>();
        auto original_index = read_file(index_path, 16 * 1024 * 1024);
        auto index = Json::parse(original_index);
        auto& part = index.at("parts").at(0);
        auto part_path = out / part.at("object").get<std::string>();
        auto original_part = read_file(part_path, 1 << 30);
        std::vector<ObjectRecord> objects;
        read_snapshot_part(part_path, {}, part.at("records").get<std::uint64_t>(), [&](ObjectRecord&& object) { objects.push_back(std::move(object)); });
        ASSERT_FALSE(objects.empty());
        objects.back().object.back() ^= 1;
        write_snapshot_part(part_path, objects, {}, 1);
        part["sha256"] = file_sha256(part_path);
        part["size"] = fs::file_size(part_path);
        write_file(index_path, index.dump());
        EXPECT_TRUE(existing_state_snapshot(verifier.header(), out).has_value());
        EXPECT_FALSE(existing_state_snapshot(verifier, out).has_value());

        // So is one missing an object, or holding one more than the state.
        objects.back().object.back() ^= 1;  // as written again
        auto shorter = objects; shorter.pop_back();
        auto longer = objects; longer.push_back({Bytes(32, '\xff'), objects.back().object});
        for (auto const& listed : {shorter, longer}) {
            write_snapshot_part(part_path, listed, {}, 1);
            part["sha256"] = file_sha256(part_path);
            part["size"] = fs::file_size(part_path);
            part["records"] = listed.size();
            write_file(index_path, index.dump());
            EXPECT_TRUE(existing_state_snapshot(verifier.header(), out).has_value());
            EXPECT_FALSE(existing_state_snapshot(verifier, out).has_value());
        }

        // Put back as written, it is taken again.
        write_file(part_path, original_part);
        write_file(index_path, original_index);
        EXPECT_TRUE(existing_state_snapshot(verifier, out).has_value());
    }
    fs::remove_all(dir);
}
