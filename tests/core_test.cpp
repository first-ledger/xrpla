#include "core_fixture.hpp"
#include "../core/src/wire.hpp"
#include "../core/src/nudb_builder.hpp"
#include "../core/src/replay.hpp"
#include "../core/src/rpc.hpp"
#include <xrpl/basics/Slice.h>
#include <xrpl/protocol/digest.h>
#include <xrpl/config/BasicConfig.h>
#include <xrpl/nodestore/DummyScheduler.h>
#include <xrpl/nodestore/Manager.h>
#include <xrpl/nodestore/NodeObject.h>
#include <random>
#include <xrpl/protocol/STTx.h>
#include <xrpl/protocol/TxFormats.h>
#include <xrpl/protocol/TxMeta.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <sodium.h>
#include <cstdlib>
#include <atomic>
#include <future>
#include <thread>
#include <optional>
#include <set>
#include <thread>

using namespace xrpla;
using namespace xrpla::core;
TEST(Canonical, RecentMainnetTransactionRoots) {
    auto source = Json::parse(read_file(fs::path(XRPLA_TEST_FIXTURES) / "mainnet-modern-transactions.json", 16 * 1024 * 1024));
    std::map<std::uint32_t, Header> headers;
    std::optional<Header> previous;
    for (auto const& raw : source.at("headers")) {
        auto h = decode_header(from_hex(raw.get<std::string>()));
        if (previous) {
            EXPECT_EQ(h.sequence, previous->sequence + 1);
            EXPECT_EQ(h.parent_hash, previous->hash);
        }
        headers.emplace(h.sequence, h); previous = h;
    }
    ASSERT_TRUE(previous);
    EXPECT_EQ(previous->sequence, source.at("reference").at("sequence"));
    EXPECT_EQ(previous->hash, to_hex(from_hex(source.at("reference").at("hash").get<std::string>())));
    std::map<std::string, unsigned> counts;
    for (auto const& response : source.at("ledgers")) {
        auto const sequence = response.at("ledger_index").get<std::uint32_t>();
        auto const& header = headers.at(sequence);
        EXPECT_EQ(header.raw, from_hex(response.at("ledger").at("ledger_data").get<std::string>()));
        EXPECT_EQ(header.hash, to_hex(from_hex(response.at("ledger_hash").get<std::string>())));
        LedgerRecord ledger{sequence, header.raw, {}, {}};
        for (auto const& tx : response.at("ledger").at("transactions")) {
            TransactionRecord record{from_hex(tx.at("tx_blob").get<std::string>()), from_hex(tx.at("meta").get<std::string>())};
            ++counts[transaction_json(record).at("tx").at("TransactionType").get<std::string>()];
            ledger.transactions.push_back(std::move(record));
        }
        EXPECT_EQ(transaction_root(ledger.transactions), header.transaction_hash);
        std::set<std::uint32_t> positions;
        for (auto const& index : index_transactions(ledger)) {
            EXPECT_LT(index.position, ledger.transactions.size());
            EXPECT_TRUE(positions.insert(index.position).second);
        }
        ASSERT_FALSE(ledger.transactions.empty());
        ledger.transactions.front().metadata.back() ^= 1;
        EXPECT_NE(transaction_root(ledger.transactions), header.transaction_hash);
    }
    EXPECT_EQ(Json(counts), source.at("transaction_types"));
    for (auto type : {"NFTokenMint", "NFTokenAcceptOffer", "NFTokenCreateOffer", "NFTokenCancelOffer", "AMMDeposit", "TicketCreate"})
        EXPECT_GT(counts[type], 0);
}
namespace {
std::vector<TransactionRecord> fixture_transactions() {
    auto source = Json::parse(read_file(fs::path(XRPLA_TEST_FIXTURES) / "mainnet-modern-transactions.json", 16 * 1024 * 1024));
    std::vector<TransactionRecord> out;
    for (auto const& response : source.at("ledgers"))
        for (auto const& tx : response.at("ledger").at("transactions"))
            out.push_back({from_hex(tx.at("tx_blob").get<std::string>()), from_hex(tx.at("meta").get<std::string>())});
    return out;
}
}
// The type-code reader gives exactly what libxrpl's protocol tables give.
TEST(Wire, SummariesMatchLibxrpl) {
    namespace r = ::xrpl;
    auto records = fixture_transactions();
    ASSERT_GT(records.size(), 100u);
    for (auto const& record : records) {
        auto summary = wire::summarize(record);
        r::SerialIter tx_input(record.transaction.data(), record.transaction.size());
        r::STTx tx(tx_input);
        r::SerialIter meta_input(record.metadata.data(), record.metadata.size());
        r::TxMeta meta(tx.getTransactionID(), 1, r::STObject(meta_input, r::sfMetadata));
        EXPECT_EQ(summary.hash, Bytes(reinterpret_cast<char const*>(tx.getTransactionID().data()), 32));
        EXPECT_EQ(summary.index, meta.getIndex());
        EXPECT_EQ(summary.type, static_cast<std::uint16_t>(tx.getTxnType()));
        EXPECT_EQ(summary.sequence, tx.getFieldU32(r::sfSequence));
        auto account = tx.getAccountID(r::sfAccount);
        EXPECT_EQ(summary.account, Bytes(reinterpret_cast<char const*>(account.data()), 20));
        std::vector<Bytes> accounts;
        for (auto const& a : meta.getAffectedAccounts()) accounts.push_back(Bytes(reinterpret_cast<char const*>(a.data()), 20));
        EXPECT_EQ(summary.accounts, accounts) << to_hex(summary.hash);
        std::vector<Bytes> keys;
        for (auto const& wrapper : transaction_json(record).at("meta").at("AffectedNodes"))
            for (auto const& node : wrapper) keys.push_back(from_hex(node.at("LedgerIndex").get<std::string>()));
        EXPECT_EQ(summary.keys, keys);
    }
}
// Pseudo-transactions carry the zero account as an empty Account field (`81 00`): a summary
// names it as xrpld does, 20 zero bytes (its rows' FromAcct is rrrrrrrrrrrrrrrrrrrrrhoLvTp).
// Readers took 20 bytes from the empty field, and restores wrote garbage there.
TEST(Wire, PseudoTransactionsNameTheZeroAccount) {
    namespace r = ::xrpl;
    // Mainnet UNLModify 9FB2DF60..., ledger 107,274,240, as xrpld stored it.
    TransactionRecord record{
        from_hex("1200662400000000260664E0006840000000000000007300701321ED7FB8CC142B70AC837A9A0F57B38EB17DD569E388F33558D90B60170F1ACA5F1E810000101100"),
        from_hex("201C00000026F8E511004E2506646C00551E8C3470F2A274E61CAED77EE2A1FD8C3F1B0C69DDA4FA01AA18DA10AAE223CE562E8A59AA9D3B5B186B0B9E0F62E6C02587CA74A4D778938E957B6357D364B244E6E1E72200000000701521ED7FB8CC142B70AC837A9A0F57B38EB17DD569E388F33558D90B60170F1ACA5F1EF011E013201A06646D007121ED7FB8CC142B70AC837A9A0F57B38EB17DD569E388F33558D90B60170F1ACA5F1EE1F1E1E1F1031000")};
    auto fields = wire::parse(record.transaction);
    auto account = std::find_if(fields.begin(), fields.end(), [](auto const& f) { return f.type == 8 && f.code == 1; });
    ASSERT_NE(account, fields.end());
    EXPECT_TRUE(account->value.empty());
    auto summary = wire::summarize(record);
    EXPECT_EQ(to_hex(summary.hash), "9fb2df60b095cbcd761398bd95fc1744cb5d3cb521a6c884179df1b89e9157b9");
    r::SerialIter input(record.transaction.data(), record.transaction.size());
    auto expected = r::STTx(input).getAccountID(r::sfAccount);
    EXPECT_EQ(summary.account, Bytes(reinterpret_cast<char const*>(expected.data()), 20));
    EXPECT_EQ(r::toBase58(expected), "rrrrrrrrrrrrrrrrrrrrrhoLvTp");
}
// A field code this build does not know (a newer amendment's) is indexed like any other.
TEST(Wire, UnknownFieldsDoNotStopIndexing) {
    auto record = fixture_transactions().front();
    auto original = wire::summarize(record);
    // UInt32 field 200 (header 0x20 0xC8), appended to the transaction and to its metadata.
    Bytes field("\x20\xC8\x00\x00\x00\x07", 6);
    record.transaction += field;
    record.metadata += field;
    EXPECT_THROW((void)transaction_json(record), std::exception);  // unknown to libxrpl
    auto summary = wire::summarize(record);
    EXPECT_EQ(summary.index, original.index);
    EXPECT_EQ(summary.accounts, original.accounts);
    EXPECT_EQ(summary.keys, original.keys);
    LedgerRecord ledger{1, {}, {record}, {}};
    EXPECT_NO_THROW((void)index_transactions(ledger));
    // Malformed binary is still rejected.
    record.metadata.pop_back();
    EXPECT_THROW((void)wire::summarize(record), std::exception);
}
// Checking binary without keeping its fields accepts and rejects what parse() does, and finds
// the same top-level fields: real transactions and metadata, cut short or with a byte changed.
TEST(Wire, CheckingWithoutATreeMatchesParsing) {
    std::mt19937 random(7);
    std::size_t rejected = 0, compared = 0;
    for (auto const& record : fixture_transactions()) {
        for (auto const* blob : {&record.transaction, &record.metadata}) {
            std::vector<Bytes> variants{*blob};
            for (int i = 0; i < 8; ++i) {
                variants.push_back(blob->substr(0, random() % blob->size()));
                auto changed = *blob;
                changed[random() % changed.size()] ^= static_cast<char>(1 + random() % 255);
                variants.push_back(changed);
            }
            for (auto const& data : variants) {
                std::optional<std::vector<wire::Field>> tree;
                try { tree = wire::parse(data); } catch (std::exception const&) {}
                bool valid = true;
                try { wire::validate(data); } catch (std::exception const&) { valid = false; }
                ASSERT_EQ(valid, tree.has_value()) << to_hex(data);
                if (!tree) { ++rejected; continue; }
                wire::Field top{14, 0, {}, *tree};
                for (auto const& field : *tree) {
                    if (field.type == 14 || field.type == 15) continue;  // objects and arrays are not values
                    auto found = wire::top_level(data, field.type, field.code);
                    ASSERT_TRUE(found);
                    EXPECT_EQ(*found, top.find(field.type, field.code)->value);
                    ++compared;
                }
            }
        }
    }
    EXPECT_GT(rejected, 100u);
    EXPECT_GT(compared, 1000u);
}
// xrpld's own NuDB backend opens a database the builder wrote and reads back every node
// (leaves, inner nodes, repeats) exactly as from one it wrote itself, across an interrupted
// run resumed from its checkpoint.
// Builds a store twice interrupted and resumed, with repeats, and checks xrpld's backend reads
// every node from it as from a store it wrote itself.
void builder_round_trip(NudbBuilder::Tuning tuning, double load_factor = 0.5) {
    namespace r = ::xrpl;
    // In the working directory, not the temporary one: that is often a tmpfs, which refuses the
    // builder's direct (O_DIRECT) writes, and the restore's file systems take them.
    auto dir = fs::current_path() / ("xrpla-nudb-builder-" + std::to_string(std::random_device{}()));
    fs::create_directories(dir);
    r::node_store::DummyScheduler scheduler;
    beast::Journal journal{beast::Journal::getNullSink()};
    r::Section parameters;
    parameters.set("type", "NuDB"); parameters.set("path", (dir / "node-db").string()); parameters.set("earliest_seq", "1");
    auto backend = r::node_store::Manager::instance().makeBackend(parameters, 64 << 20, scheduler, journal);
    backend->open(); backend->close();
    r::Section reference_parameters = parameters;
    reference_parameters.set("path", (dir / "reference").string());
    auto reference = r::node_store::Manager::instance().makeBackend(reference_parameters, 64 << 20, scheduler, journal);
    reference->open();
    std::mt19937_64 random(7);
    auto make = [&](bool inner) {
        r::Blob data;
        if (inner) {  // an inner node, which xrpld's codec compresses specially
            data = {'M', 'I', 'N', '\0'};
            for (int branch = 0; branch < 16; ++branch)
                for (int b = 0; b < 32; ++b) data.push_back(branch % 3 ? static_cast<unsigned char>(random()) : 0);
        } else {
            data.resize(40 + random() % 400);
            for (auto& c : data) c = static_cast<unsigned char>(random() % 7);  // compressible
        }
        auto hash = r::sha512Half(r::Slice(data.data(), data.size()));
        return r::NodeObject::createObject(inner ? r::NodeObjectType::AccountNode : r::NodeObjectType::TransactionNode, std::move(data), hash);
    };
    std::vector<std::shared_ptr<r::NodeObject>> kept, discarded;
    for (int i = 0; i < 150000; ++i) kept.push_back(make(i % 5 == 0));  // enough to spill buckets
    NudbBuilder::Checkpoint checkpoint;
    {
        NudbBuilder builder(dir / "node-db", 4096, load_factor, tuning);
        builder.resume({});
        for (std::size_t i = 0; i < kept.size() / 2; ++i) builder.add(kept[i]);
        checkpoint = builder.checkpoint();
        for (int i = 0; i < 5000; ++i) { discarded.push_back(make(false)); builder.add(discarded.back()); }
        (void)builder.checkpoint();  // written, then abandoned: the run "stops" here
    }
    NudbBuilder builder(dir / "node-db", 4096, load_factor, tuning);
    builder.resume(checkpoint);
    for (std::size_t i = kept.size() / 2; i < kept.size(); ++i) builder.add(kept[i]);
    for (std::size_t i = 0; i < 3000; ++i) builder.add(kept[i * 7]);  // repeats, as ledgers produce
    auto added = builder.checkpoint();  // as a restore's last window commits
    auto result = builder.finish();
    // A full load factor overfills buckets: their spill records follow the data.
    if (load_factor > 0.9) EXPECT_GT(result.at("data_bytes").get<std::uint64_t>(), added.data_size);
    EXPECT_EQ(result.at("keys").get<std::uint64_t>(), kept.size());
    EXPECT_EQ(result.at("repeated").get<std::uint64_t>(), 3000u);
    // As a restore does: the finished store's checkpoint for its receipt, which must keep the
    // spill records finish() wrote past the data the builder was given.
    auto finished = builder.checkpoint();
    EXPECT_EQ(finished.data_size, result.at("data_bytes").get<std::uint64_t>());
    EXPECT_EQ(fs::file_size(dir / "node-db" / "nudb.dat"), finished.data_size);
    for (auto const& object : kept) reference->store(object);  // xrpld's own insert path
    reference->close(); reference->open(false);
    backend->open(false);
    for (auto const& object : kept) {
        std::shared_ptr<r::NodeObject> found, expected;
        ASSERT_EQ(backend->fetch(object->getHash(), &found), r::node_store::Status::Ok);
        ASSERT_EQ(reference->fetch(object->getHash(), &expected), r::node_store::Status::Ok);
        ASSERT_TRUE(found && expected);
        EXPECT_EQ(found->getType(), expected->getType());  // xrpld's codec drops inner nodes' type
        EXPECT_EQ(found->getData(), object->getData());
    }
    reference->close();
    for (auto const& object : discarded) {
        std::shared_ptr<r::NodeObject> found;
        EXPECT_NE(backend->fetch(object->getHash(), &found), r::node_store::Status::Ok);
    }
    backend->close();
    fs::remove_all(dir);
}
TEST(NudbBuilder, XrpldReadsEveryNode) { builder_round_trip({}); }
// The key file by regions: the log distributed to region files, three open at a time (so in
// several passes), each region sorted and filled on its own and spilling as it goes.
TEST(NudbBuilder, XrpldReadsEveryNodeBuiltByRegions) { builder_round_trip({20000, 4, 3}); }
// Buckets filled to spilling, then the finished store checkpointed for the restore's receipt:
// the spill records must stay in the data file (a direct-write flush once cut them off).
TEST(NudbBuilder, SpilledBucketsSurviveTheFinalCheckpoint) { builder_round_trip({}, 0.99); }
TEST(Canonical, LargeSnapshotReleasesPersistedLeaves) {
    namespace r = ::xrpl;
    auto dir = fs::temp_directory_path() / ("xrpla-bounded-snapshot-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto live_items = [] {
        int count = 0;
        for (auto const& [name, value] : r::CountedObjects::getInstance().getCounts(0))
            if (name.ends_with("SHAMapItem")) count += value;
        return count;
    };
    r::STLedgerEntry entry(r::keylet::account(r::AccountID{1}));
    entry.setAccountID(r::sfAccount, r::AccountID{1});
    entry.setFieldU32(r::sfSequence, 1);
    entry.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{1000000}));
    auto object = fixture::serialized(entry);
    auto record = [&](unsigned n) {
        Bytes key(32, '\0'); key.back() = '\x01'; key[0] = static_cast<char>(n >> 8); key[1] = static_cast<char>(n);
        return ObjectRecord{key, object};
    };
    r::LedgerHeader header{}; header.seq = 65535;
    {
        fixture::Family family;
        r::SHAMap expected(r::SHAMapType::STATE, family); expected.setUnbacked();
        for (unsigned n = 0; n < 65536; ++n) {
            auto item = record(n);
            expected.addItem(r::SHAMapNodeType::TnAccountState,
                r::makeShamapitem(r::uint256::fromVoid(item.key.data()), {item.object.data(), item.object.size()}));
        }
        header.accountHash = expected.getHash().asUInt256();
    }
    int const before = live_items();
    {
        LedgerVerifier verifier(dir);
        for (unsigned n = 0; n < 65536; ++n) {
            verifier.add_snapshot_object(record(n));
            if ((n + 1) % 4096 == 0) EXPECT_LT(live_items() - before, 8192);
        }
        auto decoded = decode_header(fixture::serialized(header));
        EXPECT_EQ(verifier.finish_snapshot(decoded.raw, decoded.sequence, decoded.account_hash).hash, decoded.hash);
        unsigned visited = 0;
        verifier.state_objects([&](ObjectRecord const& item) {
            EXPECT_EQ(item.key, record(visited).key); ++visited;
            if (visited % 4096 == 0) EXPECT_LT(live_items() - before, 8192);
        });
        EXPECT_EQ(visited, 65536);
    }
    EXPECT_EQ(live_items(), before);
    fs::remove_all(dir);
}
TEST(Canonical, DiskBackedStateMatchesMemoryAcrossFlushes) {
    auto dir = fs::temp_directory_path() / ("xrpla-nodes-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Chain chain(130);
    {
        LedgerVerifier disk(dir), memory;
        chain.seed(disk); chain.seed(memory);
        for (auto const& ledger : chain.ledgers) {
            EXPECT_EQ(disk.apply(ledger).header().hash, memory.apply(ledger).header().hash);
        }
        std::map<Bytes, Bytes> expected, actual;
        memory.state_objects([&](auto const& item) { expected[item.key] = item.object; });
        disk.state_objects([&](auto const& item) { actual[item.key] = item.object; });
        EXPECT_EQ(expected, actual);
    }
    fs::remove_all(dir);
}
TEST(Canonical, VerifierCompactsItsNodesDuringLongRuns) {
    auto base = fs::temp_directory_path() / ("xrpla-autocompact-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto stored = [](fs::path const& dir) {
        rocksdb::DB* raw = nullptr; rocksdb::Options options;
        if (!rocksdb::DB::OpenForReadOnly(options, dir.string(), &raw).ok()) return std::uint64_t(0);
        std::unique_ptr<rocksdb::DB> db(raw); std::unique_ptr<rocksdb::Iterator> it(db->NewIterator({}));
        std::uint64_t n = 0; for (it->SeekToFirst(); it->Valid(); it->Next()) ++n;
        return n;
    };
    fs::create_directories(base);
    fixture::Chain chain(260);
    LedgerVerifier memory; chain.seed(memory);
    std::map<Bytes, Bytes> expected;
    for (auto const& ledger : chain.ledgers) memory.apply(ledger);
    memory.state_objects([&](auto const& item) { expected[item.key] = item.object; });
    std::uint64_t counts[2] = {};
    for (int automatic = 0; automatic < 2; ++automatic) {
        auto dir = base / std::to_string(automatic);
        {
            LedgerVerifier disk(dir); chain.seed(disk);
            // The default floor (4M nodes) never triggers at this size; a floor of one node does.
            if (automatic) disk.compaction_floor(1);
            LedgerVerifier check; chain.seed(check);
            // Persisting every ledger leaves each ledger's superseded nodes behind.
            for (auto const& ledger : chain.ledgers) { EXPECT_EQ(disk.apply(ledger).header().hash, check.apply(ledger).header().hash); disk.persist(); }
        }
        counts[automatic] = stored(dir);
        std::map<Bytes, Bytes> actual;
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers.back().header);
        disk.state_objects([&](auto const& item) { actual[item.key] = item.object; });
        EXPECT_EQ(actual, expected);
    }
    EXPECT_LT(counts[1], counts[0]);
    fs::remove_all(base);
}
TEST(Canonical, NodeCompactionKeepsOnlyCurrentStateAndResumes) {
    auto dir = fs::temp_directory_path() / ("xrpla-compact-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto stored = [&] {
        rocksdb::DB* raw = nullptr; rocksdb::Options options;
        if (!rocksdb::DB::OpenForReadOnly(options, dir.string(), &raw).ok()) return std::uint64_t(0);
        std::unique_ptr<rocksdb::DB> db(raw); std::unique_ptr<rocksdb::Iterator> it(db->NewIterator({}));
        std::uint64_t n = 0; for (it->SeekToFirst(); it->Valid(); it->Next()) ++n;
        return n;
    };
    fixture::Chain chain(130);
    auto half = chain.ledgers.size() / 2;
    LedgerVerifier memory; chain.seed(memory);
    {
        // Persisting every ledger leaves each ledger's superseded nodes behind.
        LedgerVerifier disk(dir); chain.seed(disk);
        for (std::size_t i = 0; i < half; ++i) {
            EXPECT_EQ(disk.apply(chain.ledgers[i]).header().hash, memory.apply(chain.ledgers[i]).header().hash);
            disk.persist();
        }
    }
    auto before = stored();
    std::uint64_t kept = 0;
    {
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers[half - 1].header);
        kept = disk.compact_nodes();
        EXPECT_GT(kept, 0u);
        // Verification continues against the compacted store.
        for (std::size_t i = half; i < chain.ledgers.size(); ++i)
            EXPECT_EQ(disk.apply(chain.ledgers[i]).header().hash, memory.apply(chain.ledgers[i]).header().hash);
        disk.persist();
        kept = disk.compact_nodes();
    }
    // Only the current tree remains, and a new verifier resumes from it.
    EXPECT_EQ(stored(), kept);
    EXPECT_LT(kept, before);
    std::map<Bytes, Bytes> expected, actual;
    memory.state_objects([&](auto const& item) { expected[item.key] = item.object; });
    {
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers.back().header);
        disk.state_objects([&](auto const& item) { actual[item.key] = item.object; });
    }
    EXPECT_EQ(actual, expected);
    fs::remove_all(dir);
}
TEST(Canonical, NodeCompactionKeepsTheNewestLedgersTrees) {
    // A peer server reads the trees of the newest N ledgers: compaction keeps every node
    // reachable from their roots (after a restart too, once keep_root names the earlier ones).
    auto dir = fs::temp_directory_path() / ("xrpla-window-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain(130);  // every ledger has its own state root
    std::size_t const window = 8, total = chain.ledgers.size();
    LedgerVerifier memory; chain.seed(memory);
    std::vector<std::map<Bytes, Bytes>> states(total);
    for (std::size_t i = 0; i < total; ++i) {
        memory.apply(chain.ledgers[i]);
        if (i + window >= total || i + 20 == total) memory.state_objects([&](auto const& item) { states[i][item.key] = item.object; });
    }
    auto tree = [&](std::size_t i) {
        std::map<Bytes, Bytes> found;
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers[i].header);
        disk.state_objects([&](auto const& item) { found[item.key] = item.object; });
        return found;
    };
    std::uint64_t window_nodes = 0, current_nodes = 0;
    {
        LedgerVerifier disk(dir); chain.seed(disk);
        disk.keep_ledgers(window);
        for (auto const& ledger : chain.ledgers) { disk.apply(ledger); disk.persist(); }
        window_nodes = disk.compact_nodes();
    }
    for (std::size_t i = total - window; i < total; ++i) EXPECT_EQ(tree(i), states[i]) << "ledger " << i;
    // Twenty ledgers back is outside the window: some of its nodes are gone.
    EXPECT_ANY_THROW(tree(total - 20));
    {
        // A restarted verifier names the window's earlier roots again before compacting.
        LedgerVerifier disk(dir);
        disk.keep_ledgers(window);
        for (std::size_t i = total - window; i + 1 < total; ++i) disk.keep_ledger(decode_header(chain.ledgers[i].header));
        disk.resume(chain.ledgers.back().header);
        EXPECT_EQ(disk.compact_nodes(), window_nodes);
    }
    for (std::size_t i = total - window; i < total; ++i) EXPECT_EQ(tree(i), states[i]) << "ledger " << i;
    {
        // Without a window only the newest tree is kept.
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers.back().header);
        current_nodes = disk.compact_nodes();
    }
    EXPECT_LT(current_nodes, window_nodes);
    EXPECT_EQ(tree(total - 1), states[total - 1]);
    EXPECT_ANY_THROW(tree(total - 2));
    fs::remove_all(dir);
}

TEST(Canonical, PrefetchAndBackgroundWritesLeaveVerificationUnchanged) {
    // A follower's verifier reads coming ledgers' tree paths on other threads and writes its
    // node batches in the background: every hash, the state and the stored tree come out as
    // they do without either. Small batches and frequent reloads keep both busy.
    auto dir = fs::temp_directory_path() / ("xrpla-prefetch-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain(130);
    LedgerVerifier memory; chain.seed(memory);
    {
        LedgerVerifier disk(dir); chain.seed(disk);
        disk.write_batches(16);
        disk.retain(3, 32);
        std::atomic<bool> done{false};
        std::thread reader([&] {
            while (!done)
                for (auto const& ledger : chain.ledgers) {
                    std::vector<Bytes> keys;
                    for (auto const& patch : ledger.patches) keys.push_back(patch.key);
                    disk.prefetch(std::move(keys));
                }
        });
        for (std::size_t i = 0; i < chain.ledgers.size(); ++i) {
            EXPECT_EQ(disk.apply(chain.ledgers[i]).header().hash, memory.apply(chain.ledgers[i]).header().hash);
            if (i % 10 == 9) disk.persist();
        }
        done = true; reader.join();
        disk.persist();
        EXPECT_GT(disk.timings().at("prefetched_keys").get<std::uint64_t>(), 0u);
    }
    std::map<Bytes, Bytes> expected, actual;
    memory.state_objects([&](auto const& item) { expected[item.key] = item.object; });
    {
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers.back().header);
        disk.state_objects([&](auto const& item) { actual[item.key] = item.object; });
    }
    EXPECT_EQ(actual, expected);
    fs::remove_all(dir);
}

TEST(Canonical, BackgroundCompactionKeepsWhatLedgersWriteMeanwhile) {
    // A follower compacts on a thread of its own while ledgers keep applying: the copy swapped
    // in holds the trees of ledgers applied during the walk too (their writes reach both stores).
    auto dir = fs::temp_directory_path() / ("xrpla-background-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain(130);
    std::size_t const window = 8, total = chain.ledgers.size(), half = 60;
    LedgerVerifier memory; chain.seed(memory);
    std::vector<std::map<Bytes, Bytes>> states(total);
    for (std::size_t i = 0; i < total; ++i) {
        memory.apply(chain.ledgers[i]);
        if (i + window >= total) memory.state_objects([&](auto const& item) { states[i][item.key] = item.object; });
    }
    auto tree = [&](std::size_t i) {
        std::map<Bytes, Bytes> found;
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers[i].header);
        disk.state_objects([&](auto const& item) { found[item.key] = item.object; });
        return found;
    };
    std::uint64_t background_nodes = 0;
    {
        LedgerVerifier disk(dir); chain.seed(disk);
        disk.keep_ledgers(window);
        disk.compact_in_background(true);
        disk.write_batches(16);  // node batches written in the background meanwhile too
        for (std::size_t i = 0; i < half; ++i) { disk.apply(chain.ledgers[i]); disk.persist(); }
        std::promise<void> gate;
        ASSERT_TRUE(disk.start_compaction(gate.get_future().share()));
        EXPECT_TRUE(disk.compacting());
        EXPECT_FALSE(disk.start_compaction());   // one at a time
        EXPECT_FALSE(disk.finish_compaction());  // not ready: the walk waits at the gate
        // Every remaining ledger applies before the walk copies a single node.
        for (std::size_t i = half; i < total; ++i) { disk.apply(chain.ledgers[i]); disk.persist(); }
        gate.set_value();
        // A persist swaps the copy in once the walk is done; finish_compaction still reports
        // it, once (the follower moves its compaction marker then).
        for (auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30); disk.compacting() && std::chrono::steady_clock::now() < until;) {
            disk.persist();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_FALSE(disk.compacting());
        auto done = disk.finish_compaction();
        ASSERT_TRUE(done);
        EXPECT_FALSE(disk.finish_compaction());
        EXPECT_EQ(done->ledger, chain.ledgers[half - 1].sequence);
        background_nodes = done->nodes;
        EXPECT_EQ(disk.header().hash, decode_header(chain.ledgers.back().header).hash);
    }
    for (std::size_t i = total - window; i < total; ++i) EXPECT_EQ(tree(i), states[i]) << "ledger " << i;
    {
        // What a synchronous compaction keeps now, the background copy held already.
        LedgerVerifier disk(dir);
        disk.keep_ledgers(window);
        for (std::size_t i = total - window; i + 1 < total; ++i) disk.keep_ledger(decode_header(chain.ledgers[i].header));
        disk.resume(chain.ledgers.back().header);
        EXPECT_LE(disk.compact_nodes(), background_nodes);
    }
    {
        // A verifier closed mid-compaction drops the unfinished copy; the store is as it was.
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers.back().header);
        std::promise<void> never;
        ASSERT_TRUE(disk.start_compaction(never.get_future().share()));
    }
    EXPECT_FALSE(fs::exists(fs::path(dir.string() + ".compact")));
    EXPECT_EQ(tree(total - 1), states[total - 1]);
    fs::remove_all(dir);
}

TEST(Canonical, RewindReturnsToTheCommittedLedgerWhileCompacting) {
    // A follower's batch failing part-way: the verifier goes back to the committed ledger (its
    // state read again from the store) and the same ledgers apply again, while a background
    // compaction under way carries on and at the end still holds everything.
    auto dir = fs::temp_directory_path() / ("xrpla-rewind-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain(60);
    std::size_t const total = chain.ledgers.size(), committed = 30;
    LedgerVerifier memory; chain.seed(memory);
    std::map<Bytes, Bytes> at_commit, at_end, found;
    for (std::size_t i = 0; i < total; ++i) {
        memory.apply(chain.ledgers[i]);
        if (i + 1 == committed) memory.state_objects([&](auto const& item) { at_commit[item.key] = item.object; });
    }
    memory.state_objects([&](auto const& item) { at_end[item.key] = item.object; });
    auto state = [&](LedgerVerifier& v) { found.clear(); v.state_objects([&](auto const& item) { found[item.key] = item.object; }); return found; };
    {
        LedgerVerifier disk(dir); chain.seed(disk);
        disk.keep_ledgers(4);
        disk.compact_in_background(true);
        disk.write_batches(16);  // node batches written in the background meanwhile too
        for (std::size_t i = 0; i < committed; ++i) disk.apply(chain.ledgers[i]);
        disk.persist();  // the commit point
        std::promise<void> gate;
        ASSERT_TRUE(disk.start_compaction(gate.get_future().share()));
        for (std::size_t i = committed; i < committed + 10; ++i) disk.apply(chain.ledgers[i]);  // then the batch fails
        disk.rewind(chain.ledgers[committed - 1].header);
        EXPECT_EQ(disk.header().hash, decode_header(chain.ledgers[committed - 1].header).hash);
        EXPECT_EQ(state(disk), at_commit);
        EXPECT_TRUE(disk.compacting());
        for (std::size_t i = committed; i < total; ++i) disk.apply(chain.ledgers[i]);  // the retry
        disk.persist();
        gate.set_value();
        ASSERT_TRUE(disk.finish_compaction(true));
        EXPECT_EQ(state(disk), at_end);
    }
    {
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers.back().header);
        EXPECT_EQ(state(disk), at_end);
    }
    fs::remove_all(dir);
}
TEST(Canonical, AutomaticCompactionKeepsTheCommittedTree) {
    // A one-off export's batch can cross the compaction threshold before it commits: the tree of
    // the ledger it would resume at after failing (its commit point) survives that compaction.
    auto base = fs::temp_directory_path() / ("xrpla-committed-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain(130);
    std::size_t const committed = 30;
    LedgerVerifier memory; chain.seed(memory);
    std::map<Bytes, Bytes> at_commit;
    for (std::size_t i = 0; i < committed; ++i) memory.apply(chain.ledgers[i]);
    memory.state_objects([&](auto const& item) { at_commit[item.key] = item.object; });
    auto tree = [&](fs::path const& dir) {
        std::map<Bytes, Bytes> found;
        LedgerVerifier disk(dir);
        disk.resume(chain.ledgers[committed - 1].header);
        disk.state_objects([&](auto const& item) { found[item.key] = item.object; });
        return found;
    };
    for (bool marked : {true, false}) {
        auto dir = base / (marked ? "marked" : "unmarked");
        fs::create_directories(dir);
        {
            LedgerVerifier disk(dir); chain.seed(disk);
            for (std::size_t i = 0; i < committed; ++i) disk.apply(chain.ledgers[i]);
            disk.persist();
            if (marked) disk.commit_point();
            disk.compaction_floor(1);  // compacts as the rest of the batch applies
            for (std::size_t i = committed; i < chain.ledgers.size(); ++i) { disk.advance(chain.ledgers[i]); disk.persist(); }
        }
        if (marked) EXPECT_EQ(tree(dir), at_commit);
        else EXPECT_ANY_THROW(tree(dir));  // without it the tree is gone: compaction did run
    }
    fs::remove_all(base);
}
// A flag ledger's close applies the negative UNL's voted changes outside any transaction, so
// a follower deriving changes from metadata must name it (mainnet 107223040 and three more
// flag ledgers failed their state root without it).
TEST(Canonical, ClosingKeysNameTheNegativeUnlOnFlagLedgers) {
    namespace r = ::xrpl;
    auto has = [](std::vector<Bytes> const& keys, r::Keylet const& keylet) {
        return std::find(keys.begin(), keys.end(), Bytes(reinterpret_cast<char const*>(keylet.key.data()), 32)) != keys.end();
    };
    for (std::uint32_t seq : {107223040u, 107224320u, 65536u}) {
        auto keys = closing_keys(seq);
        EXPECT_TRUE(has(keys, r::keylet::negativeUNL())) << seq;
        EXPECT_TRUE(has(keys, r::keylet::skip()));
        EXPECT_TRUE(has(keys, r::keylet::skip(seq - 1)));
    }
    for (std::uint32_t seq : {107223041u, 107224319u, 65537u}) EXPECT_FALSE(has(closing_keys(seq), r::keylet::negativeUNL())) << seq;
}
// A baseline's transactions are checked against its own header only: the record must be the
// verified ledger's, or another ledger's transactions would be stored as the baseline's.
TEST(Canonical, BaselineTransactionsMustBeTheVerifiedLedgers) {
    fixture::Chain chain;
    LedgerRecord baseline{decode_header(chain.snapshot_header).sequence, chain.snapshot_header, {}, {}};
    {
        LedgerVerifier verifier;
        EXPECT_THROW(verifier.persist_transactions(baseline), Error);  // nothing verified yet
        chain.seed(verifier);
        EXPECT_NO_THROW(verifier.persist_transactions(baseline));
        EXPECT_THROW(verifier.persist_transactions(chain.ledgers[1]), Error);  // its (empty) tree matches its own header
        verifier.advance(chain.ledgers[0]);
        EXPECT_THROW(verifier.persist_transactions(baseline), Error);
        EXPECT_NO_THROW(verifier.persist_transactions(chain.ledgers[0]));
    }
    {
        StateReplay replay([](std::uint8_t, Bytes const&, Bytes const&) {});
        replay.add_snapshot_objects(std::vector<ObjectRecord>(chain.snapshot));
        auto h = decode_header(chain.snapshot_header);
        replay.finish_snapshot(chain.snapshot_header, h.sequence, h.account_hash);
        EXPECT_NO_THROW(replay.apply_transactions(baseline));
        EXPECT_THROW(replay.apply_transactions(chain.ledgers[1]), Error);
        replay.apply(chain.ledgers[0]);
        EXPECT_THROW(replay.apply_transactions(baseline), Error);
        EXPECT_NO_THROW(replay.apply_transactions(chain.ledgers[0]));
    }
}
// Handing a record over moves it into the verified ledger; advance verifies the same way and
// returns nothing. Either way the verifier stands at the ledger after.
TEST(Canonical, ApplyOverloadsVerifyAlike) {
    fixture::Chain chain;
    LedgerVerifier copied, moved, advanced;
    chain.seed(copied); chain.seed(moved); chain.seed(advanced);
    for (auto const& ledger : chain.ledgers) {
        auto a = copied.apply(ledger);
        auto b = moved.apply(LedgerRecord(ledger));
        advanced.advance(ledger);
        EXPECT_EQ(a.header().hash, b.header().hash);
        EXPECT_EQ(a.record().header, b.record().header);
        EXPECT_EQ(a.record().transactions.size(), b.record().transactions.size());
        EXPECT_EQ(a.transactions().size(), b.transactions().size());
        EXPECT_EQ(advanced.header().hash, a.header().hash);
    }
    auto corrupt = chain.ledgers[0];
    LedgerVerifier fresh; chain.seed(fresh);
    corrupt.transactions[0].metadata.back() ^= 1;
    EXPECT_THROW(fresh.advance(corrupt), std::exception);
    EXPECT_THROW(fresh.apply(std::move(corrupt)), std::exception);
    EXPECT_EQ(fresh.header().sequence, 65535);
}
TEST(Canonical, PatchesRecoverOmittedDirectoryFieldsAndDeletions) {
    fixture::Chain chain;
    LedgerVerifier verifier; chain.seed(verifier);
    auto first = verifier.apply(chain.ledgers[0]);
    EXPECT_EQ(first.transactions().size(), 2);
    EXPECT_EQ(first.transactions()[0].accounts.size(), 1);
    EXPECT_EQ(first.transactions()[0].accounts[0], fixture::raw(chain.account));
    bool directory_seen = false;
    verifier.state_objects([&](ObjectRecord const& obj) {
        if (obj.key == chain.directory_key) { directory_seen = true; EXPECT_EQ(object_json(obj).at("Indexes").size(), 2); }
    });
    EXPECT_TRUE(directory_seen);
    verifier.apply(chain.ledgers[1]); verifier.apply(chain.ledgers[2]);
    std::size_t count = 0;
    verifier.state_objects([&](ObjectRecord const& obj) { EXPECT_NE(obj.key, chain.directory_key); ++count; });
    EXPECT_EQ(count, 2);
}
TEST(Canonical, BadPatchCannotAdvanceVerifiedState) {
    fixture::Chain chain;
    LedgerVerifier verifier; chain.seed(verifier);
    auto corrupt = chain.ledgers[0]; corrupt.patches.clear();
    EXPECT_THROW(verifier.apply(corrupt), Error);
    EXPECT_EQ(verifier.header().sequence, 65535);
    EXPECT_NO_THROW(verifier.apply(chain.ledgers[0]));
}
TEST(Canonical, RejectsChangedMetadataAndBrokenChain) {
    fixture::Chain chain;
    LedgerVerifier verifier; chain.seed(verifier);
    auto corrupt = chain.ledgers[0];
    corrupt.transactions[0].metadata.back() ^= 1;
    EXPECT_THROW(verifier.apply(corrupt), std::exception);
    corrupt = chain.ledgers[0]; corrupt.header[12] ^= 1;
    EXPECT_THROW(verifier.apply(corrupt), Error);
    EXPECT_EQ(verifier.header().sequence, 65535);
    corrupt = chain.ledgers[0];
    std::swap(corrupt.transactions[0], corrupt.transactions[1]);
    EXPECT_THROW(verifier.apply(corrupt), Error);
}
TEST(Canonical, RejectsDuplicateKeysMissingDeletesAndNoncanonicalBytes) {
    fixture::Chain chain;
    LedgerVerifier verifier; chain.seed(verifier);
    auto corrupt = chain.ledgers[0]; corrupt.patches.insert(corrupt.patches.begin(), corrupt.patches.front());
    EXPECT_THROW(verifier.apply(corrupt), Error);
    corrupt = chain.ledgers[0]; corrupt.patches = {{Bytes(32, '\x7f'), {}}};
    EXPECT_THROW(verifier.apply(corrupt), Error);
    corrupt = chain.ledgers[0]; corrupt.header += 'x';
    EXPECT_THROW(verifier.apply(corrupt), Error);
    corrupt = chain.ledgers[0]; corrupt.patches[0].object += 'x';
    EXPECT_THROW(verifier.apply(corrupt), std::exception);
}
TEST(Canonical, DiskBackedSnapshotRejectsACorruptObject) {
    // Batches are checked on their own threads while earlier ones are placed: a bad object
    // fails the load, at a later batch or when the snapshot is sealed.
    auto dir = fs::temp_directory_path() / ("xrpla-corrupt-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Chain chain;
    auto h = decode_header(chain.snapshot_header);
    {
        LedgerVerifier verifier(dir);
        auto items = chain.snapshot;
        items.back().object += 'x';
        EXPECT_THROW({ verifier.add_snapshot_objects(items); verifier.finish_snapshot(chain.snapshot_header, h.sequence, h.account_hash); }, std::exception);
    }
    {
        LedgerVerifier verifier(dir / "good");
        verifier.add_snapshot_objects(chain.snapshot);
        EXPECT_EQ(verifier.finish_snapshot(chain.snapshot_header, h.sequence, h.account_hash).hash, h.hash);
    }
    fs::remove_all(dir);
}
TEST(Canonical, SnapshotRequiresOrderedKeysAndCorrectRoot) {
    fixture::Chain chain;
    LedgerVerifier verifier;
    verifier.add_snapshot_object(chain.snapshot[1]);
    EXPECT_THROW(verifier.add_snapshot_object(chain.snapshot[0]), Error);
    auto h = decode_header(chain.snapshot_header);
    EXPECT_THROW(verifier.finish_snapshot(chain.snapshot_header, h.sequence, h.account_hash), Error);
}
TEST(Canonical, ExistingStateSnapshotIsTakenOnlyWhenEveryPartVerifies) {
    // A follower's boundary batch retried finds the interval snapshot it wrote before failing:
    // it is taken as written only for exactly this ledger, with every part its index lists.
    initialize();
    auto dir = fs::temp_directory_path() / ("xrpla-existing-snapshot-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Chain chain(4);
    fs::create_directories(dir / "nodes");
    {
        LedgerVerifier verifier(dir / "nodes");
        chain.seed(verifier);
        for (auto const& ledger : chain.ledgers) verifier.advance(ledger);
        auto out = dir / "out";
        auto const header = verifier.header();
        EXPECT_FALSE(existing_state_snapshot(header, out).has_value());
        auto written = write_state_snapshot(verifier, out, 1, 2);
        auto existing = existing_state_snapshot(header, out);
        ASSERT_TRUE(existing.has_value());
        EXPECT_EQ(*existing, written);
        // Another ledger finds none: the parent, and one at this sequence and hash prefix whose
        // header differs (the directory is named by 16 characters of the hash only).
        EXPECT_FALSE(existing_state_snapshot(decode_header(chain.ledgers[chain.ledgers.size() - 2].header), out).has_value());
        auto other = header; other.raw.back() ^= 1;
        EXPECT_FALSE(existing_state_snapshot(other, out).has_value());
        // A part that differs from its digest, or is missing, is never taken.
        auto part = out / written.at("prefix").get<std::string>() / "part-0.xrps";
        auto original = read_file(part, 1 << 30);
        auto changed = original; changed.back() ^= 1;
        write_file(part, changed);
        EXPECT_FALSE(existing_state_snapshot(header, out).has_value());
        fs::remove(part);
        EXPECT_FALSE(existing_state_snapshot(header, out).has_value());
        write_file(part, original);
        EXPECT_TRUE(existing_state_snapshot(header, out).has_value());
        // Nor is an index that cannot be read.
        write_file(out / written.at("index").at("object").get<std::string>(), "{");
        EXPECT_FALSE(existing_state_snapshot(header, out).has_value());
    }
    fs::remove_all(dir);
}
TEST(CoreFixture, Export) {
    auto output = std::getenv("XRPLA_CORE_FIXTURE_DIR");
    if (!output) GTEST_SKIP() << "Set XRPLA_CORE_FIXTURE_DIR to export canonical synthetic v2 data";
    initialize();
    fs::path dir(output);
    fs::create_directories(dir / "chunks");
    std::string prefix = "snapshots/65535.fixture-v2";
    fs::create_directories(dir / prefix);
    auto count_text = std::getenv("XRPLA_CORE_FIXTURE_COUNT");
    fixture::Chain chain(count_text ? std::stoul(count_text) : 65536);
    auto base = decode_header(chain.snapshot_header);
    auto last = decode_header(chain.ledgers.back().header);
    auto fault = std::getenv("XRPLA_CORE_FIXTURE_FAULT");
    if (fault && std::string(fault) == "patch") chain.ledgers.front().patches.clear();
    write_chunk(dir / "chunk.tmp", chain.ledgers);
    if (fault && std::string(fault) == "offset") {
        auto bytes = read_file(dir / "chunk.tmp", 64 * 1024 * 1024);
        bytes[bytes.size() - 40] ^= 1; write_file(dir / "chunk.tmp", bytes);
    }
    auto chunk_hash = file_sha256(dir / "chunk.tmp");
    auto chunk_key = "chunks/65536-" + std::to_string(last.sequence) + "." + chunk_hash.substr(0, 16) + ".xrpa";
    fs::rename(dir / "chunk.tmp", dir / chunk_key);
    auto descriptor = [&](std::string const& path) { return Json{{"object", path}, {"size", fs::file_size(dir / path)}, {"sha256", file_sha256(dir / path)}}; };
    auto part_key = prefix + "/part-00000.xrps";
    write_snapshot_part(dir / part_key, chain.snapshot);
    auto part = descriptor(part_key); part["records"] = chain.snapshot.size();
    Json index{{"format_version", 2}, {"record_format", "keyed-stobject-v2"}, {"seq", 65535},
        {"account_hash", base.account_hash}, {"ledger_header", to_hex(chain.snapshot_header)},
        {"dictionary_sha256", std::string(64, '0')}, {"parts", Json::array({part})}};
    write_file(dir / prefix / "index.json", index.dump());
    auto chunk = descriptor(chunk_key);
    chunk["first"] = 65536; chunk["last"] = last.sequence; chunk["amendments"] = Json::array(); chunk["ledger_hash_of_last"] = last.hash;
    if (count_text) {
        Bytes times;
        for (auto const& ledger : chain.ledgers) for (auto n : {ledger.sequence, decode_header(ledger.header).close_time})
            for (int shift = 24; shift >= 0; shift -= 8) times += static_cast<char>(n >> shift);
        fs::create_directories(dir / "times"); auto key = "times/65536-" + std::to_string(last.sequence) + "." + sha256(times).substr(0, 16) + ".xrpt"; write_file(dir / key, times);
        chunk["time_index"] = descriptor(key); chunk["first_close_time"] = decode_header(chain.ledgers.front().header).close_time;
        chunk["last_close_time"] = last.close_time;
    }
    Json manifest{{"format_version", 2}, {"chunk_width", 65536}, {"published_at", "2026-09-15T00:00:00Z"},
        {"tip", {{"seq", last.sequence}, {"ledger_hash", last.hash}}}, {"dicts", Json::array()}, {"chunks", Json::array({chunk})},
        {"snapshots", Json::array({{{"seq", 65535}, {"prefix", prefix}, {"account_hash", base.account_hash},
          {"index", descriptor(prefix + "/index.json")}, {"parts", 1}, {"total_size", part.at("size")}}})}};
    auto bytes = manifest.dump();
    std::array<unsigned char, crypto_sign_SEEDBYTES> seed{};
    std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
    std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
    crypto_sign_seed_keypair(pk.data(), sk.data(), seed.data());
    Bytes signature(crypto_sign_BYTES, '\0');
    crypto_sign_detached(reinterpret_cast<unsigned char*>(signature.data()), nullptr,
        reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size(), sk.data());
    write_file(dir / "manifest.json", bytes); write_file(dir / "manifest.json.sig", signature);
    write_file(dir / "publisher.pub", fixture::raw(pk));
    Json expected{{"address", ::xrpl::toBase58(chain.account)}, {"account", to_hex(fixture::raw(chain.account))}, {"account_key", to_hex(chain.account_key)},
        {"directory_key", to_hex(chain.directory_key)}, {"second_key", to_hex(chain.second_key)}, {"anchor", last.hash}, {"snapshot_hash", base.hash},
        {"tx0", to_hex(chain.tx_hashes[0])}, {"tx1", to_hex(chain.tx_hashes[1])}};
    write_file(dir / "expected.json", expected.dump());
}
namespace {
// Ledgers 100-104 with the close times Clio's ledger_index date searches are checked against.
class DatedLedgers final : public History {
    std::map<std::uint32_t, Bytes> headers_;
public:
    DatedLedgers() {
        // 2023-12-31T23:59:00Z, 2024-01-01T00:00:00Z, 2024-01-01T00:59:51Z, 2024-01-05T00:00:00Z, 2024-01-06T00:00:00Z
        std::uint32_t seq = 100;
        for (std::uint32_t close : {757382340u, 757382400u, 757385991u, 757728000u, 757814400u}) {
            ::xrpl::LedgerHeader header{};
            header.seq = seq;
            header.drops = ::xrpl::XRPAmount{1000000000};
            header.closeTime = ::xrpl::NetClock::time_point{::xrpl::NetClock::duration{close}};
            header.closeTimeResolution = ::xrpl::NetClock::duration{10};
            headers_.emplace(seq++, fixture::serialized(header));
        }
    }
    Json status() const override { return {{"complete_ledgers", {{"first", 100}, {"last", 104}}}}; }
    Bytes header(std::uint32_t sequence) const override { return headers_.at(sequence); }
    std::uint32_t ledger_sequence(Bytes const&) const override { return 0; }
    Bytes object(Bytes const&, std::uint32_t) const override { return {}; }
    Json object_page(std::uint32_t, std::string const&, unsigned, bool) const override { return nullptr; }
    Json history_page(std::string const&, Bytes const&, std::uint32_t, std::uint32_t, unsigned, bool, Json const&) const override { return nullptr; }
    Json transaction(Bytes const&, bool) const override { return nullptr; }
    std::vector<Bytes> ledger_transactions(std::uint32_t) const override { return {}; }
    LedgerRecord ledger_record(std::uint32_t) const override { return {}; }
    Json nft(Bytes const&, std::uint32_t) const override { return nullptr; }
};
// ledger_index's answer for `date`: the ledger it names, or its error ("lgrNotFound:ledgerNotInRange").
std::string dated(std::string const& date) {
    DatedLedgers ledgers;
    try {
        return std::to_string(rpc::ledger_index(ledgers, Json{{"date", date}}, Json{{"first", 100}, {"last", 104}}).at("ledger_index").get<std::uint32_t>());
    } catch (rpc::Failure const& failure) {
        auto error = failure.value.at("error").get<std::string>();
        if (failure.value.contains("error_message") && failure.value.at("error_message") == "ledgerNotInRange") error += ":ledgerNotInRange";
        return error;
    }
}
}
// Clio 2.8's reading of ledger_index's date (as s1.ripple.com answered it): whitespace before any
// number, not before a literal; a second of 61; a year of fewer digits; and the nanosecond count
// of a date past 2262 or before 1970 wrapping as a signed 64-bit number.
TEST(Rpc, LedgerIndexReadsDatesAsClio) {
    EXPECT_EQ(dated("2024-01-05T00:00:00Z"), "103");
    EXPECT_EQ(dated("2024-01-05T00:00:00Zanything"), "103");
    EXPECT_EQ(dated("2024-01-04T23:59:59Z"), "102");
    EXPECT_EQ(dated("2024-01-01T00:59:50Z"), "101");
    EXPECT_EQ(dated("2024-01-01T00:00:00Z"), "101");
    EXPECT_EQ(dated("2023-12-31T23:59:59Z"), "100");
    EXPECT_EQ(dated("2023-12-31T23:58:59Z"), "lgrNotFound:ledgerNotInRange");
    EXPECT_EQ(dated("1999-12-31T23:59:59Z"), "lgrNotFound:ledgerNotInRange");
    EXPECT_EQ(dated("2030-01-01T00:00:00Z"), "104");
    // Whitespace, any amount, before a number does not count toward its digits.
    EXPECT_EQ(dated("  2024-01-05T00:00:00Z"), "103");
    EXPECT_EQ(dated("2024- 1-05T00:00:00Z"), "103");
    EXPECT_EQ(dated("2024-01- 5T00:00:00Z"), "103");
    EXPECT_EQ(dated("2024-01-05T 1:00:00Z"), "103");
    EXPECT_EQ(dated("2024-01-05T01: 2:00Z"), "103");
    EXPECT_EQ(dated("2024-01-05T00:00:  0Z"), "103");
    EXPECT_EQ(dated("2024-01-05T00:00:\t1Z"), "103");
    EXPECT_EQ(dated("2024-01-01T00:59: 51Z"), "102");
    // A second of 61 is the next minute's first; 62 is refused.
    EXPECT_EQ(dated("2024-01-01T00:58:61Z"), "101");
    EXPECT_EQ(dated("2024-01-01T00:59:61Z"), "102");
    EXPECT_EQ(dated("2024-01-01T00:00:62Z"), "invalidParams");
    // Literals exactly: upper-case, and no whitespace before them.
    EXPECT_EQ(dated("2024-01-05t00:00:00Z"), "invalidParams");
    EXPECT_EQ(dated("2024-01-05T00:00:00z"), "invalidParams");
    EXPECT_EQ(dated("2024-01-05 T00:00:00Z"), "invalidParams");
    EXPECT_EQ(dated("2024-01-05T00:00:00 Z"), "invalidParams");
    EXPECT_EQ(dated("2024-01-05T00:00:00"), "invalidParams");
    EXPECT_EQ(dated("20240-01-05T00:00:00Z"), "invalidParams");
    // Past 2262-04-11T23:47:16Z the count wraps negative (before 2000: refused), and further on
    // comes round to a later date that is looked up.
    EXPECT_EQ(dated("2262-04-11T23:47:16Z"), "104");
    EXPECT_EQ(dated("2262-04-11T23:47:17Z"), "lgrNotFound:ledgerNotInRange");
    EXPECT_EQ(dated("2262-04-12T00:00:00Z"), "lgrNotFound:ledgerNotInRange");
    EXPECT_EQ(dated("2300-01-01T00:00:00Z"), "lgrNotFound:ledgerNotInRange");
    EXPECT_EQ(dated("9999-12-31T23:59:59Z"), "lgrNotFound:ledgerNotInRange");
    EXPECT_EQ(dated("2608-07-21T23:34:33Z"), "100");  // 2023-12-31T23:59:59Z
    EXPECT_EQ(dated("2608-07-21T23:34:34Z"), "101");  // 2024-01-01T00:00:00Z
    EXPECT_EQ(dated("2608-07-22T00:34:33Z"), "102");  // 2024-01-01T00:59:59Z
    // Before 1970 a negative count, which wraps too: year 999 to 2168, the newest ledger.
    EXPECT_EQ(dated("999-01-05T00:00:00Z"), "104");
    EXPECT_EQ(dated("0024-01-05T00:00:00Z"), "lgrNotFound:ledgerNotInRange");
}

namespace {
// One ledger's state as objects by key, for the owner-directory methods.
class OwnedObjects final : public History {
    std::map<Bytes, Bytes> objects_;
public:
    void put(::xrpl::STLedgerEntry const& object) { objects_[fixture::raw(object.key())] = fixture::serialized(object); }
    Json status() const override { return {{"complete_ledgers", {{"first", 100}, {"last", 100}}}}; }
    Bytes header(std::uint32_t) const override { return {}; }
    std::uint32_t ledger_sequence(Bytes const&) const override { return 0; }
    Bytes object(Bytes const& key, std::uint32_t) const override { auto it = objects_.find(key); return it == objects_.end() ? Bytes{} : it->second; }
    Json object_page(std::uint32_t, std::string const&, unsigned, bool) const override { return nullptr; }
    Json history_page(std::string const&, Bytes const&, std::uint32_t, std::uint32_t, unsigned, bool, Json const&) const override { return nullptr; }
    Json transaction(Bytes const&, bool) const override { return nullptr; }
    std::vector<Bytes> ledger_transactions(std::uint32_t) const override { return {}; }
    LedgerRecord ledger_record(std::uint32_t) const override { return {}; }
    Json nft(Bytes const&, std::uint32_t) const override { return nullptr; }
    std::vector<Bytes> keys_between(std::uint32_t, Bytes const& after, Bytes const& end, unsigned limit) const override {
        std::vector<Bytes> keys;
        for (auto it = objects_.upper_bound(after); it != objects_.end() && it->first < end && keys.size() < limit; ++it) keys.push_back(it->first);
        return keys;
    }
    bool lists_keys() const override { return true; }
};
// account_objects over `objects` with these parameters: its account_objects' types (or the error).
Json owned_objects(OwnedObjects const& objects, Json params, Json* marker = nullptr) {
    Header header{};
    header.sequence = 100;
    header.hash = std::string(64, '0');
    try {
        auto result = rpc::owner_methods(rpc::Context{objects, params, header, 2}, "account_objects");
        if (marker) *marker = result.contains("marker") ? result.at("marker") : Json();
        Json types = Json::array();
        for (auto const& object : result.at("account_objects")) types.push_back(object.at("LedgerEntryType"));
        return types;
    } catch (rpc::Failure const& failure) {
        return failure.value.at("error").get<std::string>() + ": " + failure.value.value("error_message", "");
    }
}
}
// xrpld 3.4.1's `sponsored` filter: an object counts as sponsored when its reserve is (a trust
// line's on either side; for other types only those reserve sponsorship supports, so a ticket
// naming a sponsor is not), an NFT page by its own sfSponsor; objects filtered out still count
// toward the limit. deletion_blockers_only includes Sponsorship objects.
TEST(Rpc, AccountObjectsFilterBySponsorship) {
    namespace r = ::xrpl;
    r::AccountID owner{1}, sponsor{2};
    OwnedObjects objects;
    r::STLedgerEntry root(r::keylet::account(owner));
    root.setAccountID(r::sfAccount, owner);
    root.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{100000000}));
    objects.put(root);
    r::STLedgerEntry page(r::keylet::nftokenPageMax(owner));
    page.setAccountID(r::sfSponsor, sponsor);
    objects.put(page);
    std::vector<r::uint256> entries;
    for (unsigned n = 0; n < 11; ++n) {
        r::STLedgerEntry ticket(r::Keylet{r::ltTICKET, r::uint256{100 + n}});
        ticket.setAccountID(r::sfAccount, owner);
        if (n == 10) ticket.setAccountID(r::sfSponsor, sponsor);
        objects.put(ticket); entries.push_back(ticket.key());
    }
    r::STLedgerEntry check(r::Keylet{r::ltCHECK, r::uint256{201}});
    check.setAccountID(r::sfSponsor, sponsor);
    r::STLedgerEntry line(r::Keylet{r::ltRIPPLE_STATE, r::uint256{202}});
    line.setAccountID(r::sfLowSponsor, sponsor);
    r::STLedgerEntry offer(r::Keylet{r::ltOFFER, r::uint256{203}});
    r::STLedgerEntry sponsorship(r::Keylet{r::ltSPONSORSHIP, r::uint256{204}});
    for (auto const* object : {&check, &line, &offer, &sponsorship}) { objects.put(*object); entries.push_back(object->key()); }
    r::STLedgerEntry directory(r::keylet::ownerDir(owner));
    directory.setAccountID(r::sfOwner, owner);
    directory.setFieldH256(r::sfRootIndex, directory.key());
    directory.setFieldV256(r::sfIndexes, r::STVector256(r::sfIndexes, entries));
    objects.put(directory);
    Json account{{"account", r::toBase58(owner)}};
    auto with = [&](Json more) { auto params = account; params.update(more); return params; };
    EXPECT_EQ(owned_objects(objects, account).size(), 16u);
    EXPECT_EQ(owned_objects(objects, with({{"sponsored", true}})), Json::array({"NFTokenPage", "Check", "RippleState"}));
    EXPECT_EQ(owned_objects(objects, with({{"sponsored", false}})).size(), 13u);
    // The NFT page and nine tickets fill a limit of 10: the next call starts at the tenth ticket.
    Json marker;
    EXPECT_EQ(owned_objects(objects, with({{"sponsored", true}, {"limit", 10}}), &marker), Json::array({"NFTokenPage"}));
    EXPECT_EQ(marker, Json(r::to_string(directory.key()) + "," + r::to_string(entries[9])));
    EXPECT_EQ(owned_objects(objects, with({{"sponsored", true}, {"limit", 10}, {"marker", marker}}), &marker), Json::array({"Check", "RippleState"}));
    EXPECT_TRUE(marker.is_null());
    for (auto const& value : {Json("true"), Json(1), Json(nullptr)})
        EXPECT_EQ(owned_objects(objects, with({{"sponsored", value}})), Json("invalidParams: Invalid field 'sponsored', not boolean."));
    EXPECT_EQ(owned_objects(objects, with({{"deletion_blockers_only", true}})), Json::array({"NFTokenPage", "Check", "RippleState", "Sponsorship"}));
    EXPECT_EQ(owned_objects(objects, with({{"deletion_blockers_only", true}, {"type", "sponsorship"}})), Json::array({"Sponsorship"}));
}
