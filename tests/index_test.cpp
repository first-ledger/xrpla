#include "core_fixture.hpp"
#include "../core/src/store.hpp"
#include "../core/src/piece_history.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/protocol/nft.h>
#include <xrpl/protocol/Book.h>
#include <xrpl/protocol/Indexes.h>
#include <gtest/gtest.h>
#include <fstream>
#include <functional>
#include <sqlite3.h>
#include <set>
#include <tuple>

using namespace xrpla;
using namespace xrpla::core;
namespace r = ::xrpl;
namespace {
struct IndexedHistory {
    fixture::Family family;
    r::SHAMap state{r::SHAMapType::STATE, family};
    r::LedgerHeader head{};
    LedgerVerifier verifier;
    fs::path dir = [] { auto path = fs::temp_directory_path() / ("xrpla-index-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())); fs::create_directories(path); return path; }();
    Store store{dir / "store", true};
    Header baseline{};
    std::vector<ObjectRecord> snapshot;      // the baseline's objects, sorted by key
    std::vector<VerifiedLedger> appended;    // what the store has taken, in order
    explicit IndexedHistory(std::vector<ObjectRecord> objects = {}) : snapshot(std::move(objects)) {
        state.setUnbacked(); head.seq = 100; head.drops = r::XRPAmount{1000000000};
        head.closeTime = r::NetClock::time_point{r::NetClock::duration{1000}};
        std::sort(snapshot.begin(), snapshot.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
        for (auto const& item : snapshot)
            state.addItem(r::SHAMapNodeType::TnAccountState, r::makeShamapitem(r::uint256::fromVoid(item.key.data()), {item.object.data(), item.object.size()}));
        if (!snapshot.empty()) verifier.add_snapshot_objects(snapshot);
        head.accountHash = state.getHash().asUInt256();
        auto bytes = fixture::serialized(head); baseline = verifier.finish_snapshot(bytes, 100, decode_header(bytes).account_hash);
        store.initialize(verifier, {101, 200});
    }
    ~IndexedHistory() { /* Store must close before its directory can be removed. */ }
    void append(std::vector<ObjectRecord> patches, std::vector<TransactionRecord> transactions = {}) {
        std::sort(patches.begin(), patches.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
        for (auto const& item : patches) {
            auto key = r::uint256::fromVoid(item.key.data());
            if (item.object.empty()) ASSERT_TRUE(state.delItem(key));
            else {
                auto node = r::makeShamapitem(key, {item.object.data(), item.object.size()});
                if (state.peekItem(key)) ASSERT_TRUE(state.updateGiveItem(r::SHAMapNodeType::TnAccountState, node));
                else ASSERT_TRUE(state.addItem(r::SHAMapNodeType::TnAccountState, node));
            }
        }
        r::SHAMap txs(r::SHAMapType::TRANSACTION, family); txs.setUnbacked();
        for (auto const& tx : transactions) {
            r::STTx decoded(r::SerialIter(tx.transaction.data(), tx.transaction.size()));
            r::Serializer leaf; leaf.addVL(tx.transaction.data(), static_cast<int>(tx.transaction.size())); leaf.addVL(tx.metadata.data(), static_cast<int>(tx.metadata.size()));
            txs.addItem(r::SHAMapNodeType::TnTransactionMd, r::makeShamapitem(decoded.getTransactionID(), leaf.slice()));
        }
        head.parentHash = r::calculateLedgerHash(head); ++head.seq; head.parentCloseTime = head.closeTime;
        head.closeTime += r::NetClock::duration{10}; head.accountHash = state.getHash().asUInt256(); head.txHash = txs.getHash().asUInt256();
        auto verified = verifier.apply({head.seq, fixture::serialized(head), transactions, patches});
        ASSERT_TRUE(store.append(verified));
        appended.push_back(std::move(verified));
    }
};
ObjectRecord record(r::SLE const& object) { return {fixture::raw(object.key()), fixture::serialized(object)}; }
r::STObject token(r::uint256 const& id, std::string uri) {
    r::STObject value(r::sfNFToken); value.setFieldH256(r::sfNFTokenID, id); value.setFieldVL(r::sfURI, r::Blob(uri.begin(), uri.end())); return value;
}
r::SLE page(r::AccountID const& owner, std::vector<r::STObject> const& tokens) {
    r::SLE value(r::keylet::nftokenPageMax(owner)); r::STArray list(r::sfNFTokens);
    for (auto const& item : tokens) list.pushBack(item); value.setFieldArray(r::sfNFTokens, list); return value;
}
// An AffectedNodes entry for an NFTokenPage; a modified page also lists its previous tokens.
r::STObject nft_node(r::SField const& kind, r::SLE const& page, r::SLE const* previous = nullptr) {
    r::STObject node(kind);
    node.setFieldH256(r::sfLedgerIndex, page.key()); node.setFieldU16(r::sfLedgerEntryType, r::ltNFTOKEN_PAGE);
    auto const& name = kind == r::sfCreatedNode ? r::sfNewFields : r::sfFinalFields;
    r::STObject fields(name); fields.setFieldArray(r::sfNFTokens, page.getFieldArray(r::sfNFTokens));
    node.setFieldObject(name, fields);
    if (previous) { r::STObject prior(r::sfPreviousFields); prior.setFieldArray(r::sfNFTokens, previous->getFieldArray(r::sfNFTokens)); node.setFieldObject(r::sfPreviousFields, prior); }
    return node;
}
// A transaction of `type` with `fields` set, and its metadata: `result` (0: tesSUCCESS; 140:
// tecNO_ENTRY) and the nodes it affected.
TransactionRecord nft_tx(r::AccountID const& owner, std::uint32_t seq, std::vector<r::STObject> const& affected, r::TxType type = r::ttACCOUNT_SET,
                         std::function<void(r::STObject&)> const& fields = {}, std::uint8_t result = 0) {
    r::STTx tx(type, [&](r::STObject& obj) {
        obj.setAccountID(r::sfAccount, owner); obj.setFieldU32(r::sfSequence, seq);
        obj.setFieldAmount(r::sfFee, r::STAmount(r::XRPAmount{10})); obj.setFieldVL(r::sfSigningPubKey, r::Blob{});
        if (fields) fields(obj);
    });
    r::STObject meta(r::sfMetadata); meta.setFieldU8(r::sfTransactionResult, result); meta.setFieldU32(r::sfTransactionIndex, 0);
    r::STArray nodes(r::sfAffectedNodes); for (auto const& node : affected) nodes.pushBack(node); meta.setFieldArray(r::sfAffectedNodes, nodes);
    return {fixture::serialized(tx), fixture::serialized(meta)};
}
TransactionRecord mint_tx(r::AccountID const& owner, std::uint32_t seq, std::vector<r::STObject> const& affected) {
    return nft_tx(owner, seq, affected, r::ttNFTOKEN_MINT, [](r::STObject& obj) { obj.setFieldU32(r::sfNFTokenTaxon, 0); });
}
// An NFTokenBurn as xrpld writes it (`result` 0: tesSUCCESS; 140: tecNO_ENTRY, no page touched).
TransactionRecord burn_tx(r::AccountID const& owner, std::uint32_t seq, r::uint256 const& id, std::uint8_t result, std::vector<r::STObject> const& affected) {
    return nft_tx(owner, seq, affected, r::ttNFTOKEN_BURN, [&](r::STObject& obj) { obj.setFieldH256(r::sfNFTokenID, id); }, result);
}
Bytes tx_id(TransactionRecord const& record) {
    r::STTx tx(r::SerialIter(record.transaction.data(), record.transaction.size()));
    return fixture::raw(tx.getTransactionID());
}
Bytes be32(std::uint32_t n) { Bytes b; for (int i = 3; i >= 0; --i) b += static_cast<char>((n >> (8 * i)) & 255); return b; }
// Rewrites a closed store's rows as an older build left them: `edit` gets each column's handle by name.
void edit_store(fs::path const& path, std::function<void(rocksdb::DB&, std::map<std::string, rocksdb::ColumnFamilyHandle*> const&)> const& edit) {
    std::vector<std::string> names;
    ASSERT_TRUE(rocksdb::DB::ListColumnFamilies({}, path.string(), &names).ok());
    std::vector<rocksdb::ColumnFamilyDescriptor> columns;
    for (auto const& name : names) columns.emplace_back(name, rocksdb::ColumnFamilyOptions());
    std::vector<rocksdb::ColumnFamilyHandle*> handles; rocksdb::DB* db = nullptr;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::DBOptions(), path.string(), columns, &handles, &db).ok());
    std::map<std::string, rocksdb::ColumnFamilyHandle*> by_name;
    for (std::size_t i = 0; i < names.size(); ++i) by_name[names[i]] = handles[i];
    edit(*db, by_name);
    for (auto handle : handles) db->DestroyColumnFamilyHandle(handle);
    delete db;
}
}

TEST(Indexes, NftMintUriTransferBurnAndMptHistory) {
    fs::path directory;
    {
        IndexedHistory h; directory = h.dir;
        r::AccountID a{1}, b{2}; r::uint256 one{3}, two{4}; r::MPTID issuance{5};
        auto root = [&](r::AccountID owner) { r::SLE value(r::keylet::account(owner)); value.setAccountID(r::sfAccount, owner); value.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{10000000})); return value; };
        r::SLE mpt(r::keylet::mptoken(issuance, a)); mpt.setAccountID(r::sfAccount, a); mpt.setFieldH192(r::sfMPTokenIssuanceID, issuance);
        auto p = page(a, {token(one, "first"), token(two, "stable")});
        // A mint (Clio names the lowest ID its pages gained: `one`).
        h.append({record(root(a)), record(root(b)), record(p), record(mpt)}, {mint_tx(a, 1, {nft_node(r::sfCreatedNode, p)})});
        auto before = h.store.nft(fixture::raw(two), 101);
        // NFT state follows transaction metadata (as Clio records it): each page change comes with its transaction.
        auto previous = p;
        p = page(a, {token(one, "updated"), token(two, "stable")});
        auto uri = [](r::STObject& obj) { obj.setFieldH256(r::sfNFTokenID, r::uint256{3}); obj.setFieldVL(r::sfURI, r::Blob{'u'}); };
        h.append({record(p), {fixture::raw(mpt.key()), {}}}, {nft_tx(a, 2, {nft_node(r::sfModifiedNode, p, &previous)}, r::ttNFTOKEN_MODIFY, uri)});
        EXPECT_EQ(h.store.nft(fixture::raw(two), 102), before);
        EXPECT_EQ(h.store.nft(fixture::raw(one), 102).at("uri"), to_hex("updated"));
        // A URI change alone leaves ledger_index where the owner and burned flag last changed (Clio).
        EXPECT_EQ(h.store.nft(fixture::raw(one), 102).at("ledger_index"), 101);
        auto moved = page(b, {token(one, "updated"), token(two, "stable")});
        h.append({{fixture::raw(p.key()), {}}, record(moved), record(mpt)}, {nft_tx(a, 3, {nft_node(r::sfDeletedNode, p), nft_node(r::sfCreatedNode, moved)})});
        EXPECT_EQ(h.store.nft(fixture::raw(one), 103).at("owner"), r::toBase58(b));
        auto after = page(b, {token(two, "stable")});
        h.append({record(after), {fixture::raw(mpt.key()), {}}}, {burn_tx(b, 1, one, 0, {nft_node(r::sfDeletedNode, page(b, {token(one, "updated")}))})});
        EXPECT_TRUE(h.store.nft(fixture::raw(one), 104).at("is_burned").get<bool>());
        EXPECT_EQ(h.store.nft(fixture::raw(two), 104).at("ledger_index"), 103);
        // Mint, URI change and burn: Clio lists no other type (this page move is an AccountSet).
        EXPECT_EQ(h.store.history_page("nft_tx", fixture::raw(one), 101, 104, 10, true, nullptr).at("transactions").size(), 3);
        EXPECT_FALSE(h.store.object(fixture::raw(mpt.key()), 101).empty());
        EXPECT_TRUE(h.store.object(fixture::raw(mpt.key()), 102).empty());
        EXPECT_FALSE(h.store.object(fixture::raw(mpt.key()), 103).empty());
        EXPECT_TRUE(h.store.object(fixture::raw(mpt.key()), 104).empty());
        auto objects = rpc_request(h.dir, "account_objects", {{"account", r::toBase58(b)}, {"limit", 1}});
        ASSERT_EQ(objects.at("status"), "success") << objects.dump();
        EXPECT_EQ(objects.at("account_objects").size(), 1);
        EXPECT_EQ(objects.at("account_objects")[0].at("LedgerEntryType"), "NFTokenPage");
        // As in rippled, a public limit is clamped to at least 10 and a complete listing has no marker.
        EXPECT_FALSE(objects.contains("marker")) << objects.dump();
        h.store.prune(103);
        EXPECT_THROW(h.store.nft(fixture::raw(one), 102), Error);
        EXPECT_TRUE(h.store.nft(fixture::raw(one), 104).at("is_burned").get<bool>());
    }
    fs::remove_all(directory);
}
// Mainnet token 000100009DFF301D...: minted, sold, burned in ledger 94,789,063, then burned again
// several times with tecNO_ENTRY. The burn left its page short: xrpld wrote the page without the
// token, merged it into the owner's next page and deleted it, so the token is in the deleted
// page's PreviousFields alone. Its state is burned from the burn's ledger on; the failed burns
// change no page, and nft_history leaves them out (as Clio 2.8), as it leaves out a token only a
// failed transaction names, which nfts_by_issuer never offers. A store indexed by an older build
// (the burn missed, failed transactions listed) is brought to this by a writable open.
TEST(Indexes, ABurnThatMergesItsPageAwayBurnsTheToken) {
    fs::path directory;
    r::AccountID a{1}, b{2}; r::uint256 x{3}, z{4}, w{5}, q{9};
    Bytes failed_hash, ghost_hash;
    auto history = [](Store const& store, r::uint256 const& id) { return store.history_page("nft_tx", fixture::raw(id), 101, 105, 10, true, nullptr).at("transactions").size(); };
    auto offered = [](Store const& store, r::uint256 const& id) {
        auto ids = *store.nfts_by_issuer(Bytes(20, '\0'), std::nullopt, {}, 10);
        return std::find(ids.begin(), ids.end(), fixture::raw(id)) != ids.end();
    };
    {
        IndexedHistory h; directory = h.dir;
        auto root = [&](r::AccountID owner, std::uint64_t drops) { r::SLE value(r::keylet::account(owner)); value.setAccountID(r::sfAccount, owner); value.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{drops})); return value; };
        // b's tokens on two pages: its last page and one below it.
        auto low_key = r::keylet::nftokenPageMax(b).key; low_key.data()[31] = 0;
        auto low = [&](std::vector<r::STObject> const& tokens) {
            r::SLE value(r::Keylet(r::ltNFTOKEN_PAGE, low_key)); r::STArray list(r::sfNFTokens);
            for (auto const& item : tokens) list.pushBack(item); value.setFieldArray(r::sfNFTokens, list); return value;
        };
        auto pa = page(a, {token(x, "x")}), high = page(b, {token(w, "w")}), lower = low({token(z, "z")});
        h.append({record(root(a, 10000000)), record(root(b, 10000000)), record(pa), record(high), record(lower)},
                 {mint_tx(a, 1, {nft_node(r::sfCreatedNode, pa), nft_node(r::sfCreatedNode, high), nft_node(r::sfCreatedNode, lower)})});
        // 102: sold to b, onto its lower page, through a's sell offer.
        auto bought = low({token(x, "x"), token(z, "z")});
        r::uint256 offer{77};
        r::STObject sold(r::sfDeletedNode); sold.setFieldH256(r::sfLedgerIndex, offer); sold.setFieldU16(r::sfLedgerEntryType, r::ltNFTOKEN_OFFER);
        r::STObject offer_fields(r::sfFinalFields); offer_fields.setFieldH256(r::sfNFTokenID, x); offer_fields.setAccountID(r::sfOwner, a);
        sold.setFieldObject(r::sfFinalFields, offer_fields);
        h.append({{fixture::raw(pa.key()), {}}, record(bought)}, {nft_tx(b, 1, {sold, nft_node(r::sfDeletedNode, pa), nft_node(r::sfModifiedNode, bought, &lower)},
                                                                         r::ttNFTOKEN_ACCEPT_OFFER, [&](r::STObject& obj) { obj.setFieldH256(r::sfNFTokenSellOffer, offer); })});
        EXPECT_EQ(h.store.nft(fixture::raw(x), 102).at("owner"), r::toBase58(b));
        // 103: burned; the lower page, written as {z}, merges into the last page and is deleted.
        auto merged = page(b, {token(w, "w"), token(z, "z")});
        auto burn = burn_tx(b, 2, x, 0, {nft_node(r::sfDeletedNode, lower, &bought), nft_node(r::sfModifiedNode, merged, &high)});
        h.append({{fixture::raw(low_key), {}}, record(merged)}, {burn});
        // 104: burned again, failing; 105: a burn of a token that never was, failing.
        auto failed = burn_tx(b, 3, x, 140, {}), ghost = burn_tx(b, 4, q, 140, {});
        failed_hash = tx_id(failed); ghost_hash = tx_id(ghost);
        h.append({record(root(b, 9999990))}, {failed});
        h.append({record(root(b, 9999980))}, {ghost});
        // nft_changes and nft_ids are what the piece index (index-pieces) and the live tail record too.
        auto changes = nft_changes(burn);
        ASSERT_TRUE(changes.contains(fixture::raw(x)));
        EXPECT_TRUE(changes.at(fixture::raw(x)).burned);
        EXPECT_EQ(changes.at(fixture::raw(x)).owner, r::toBase58(b));
        EXPECT_FALSE(changes.at(fixture::raw(z)).burned);
        EXPECT_TRUE(nft_changes(failed).empty());
        EXPECT_TRUE(nft_ids(failed).empty());
        EXPECT_EQ(nft_ids(burn), std::set<Bytes>{fixture::raw(x)});
        for (std::uint32_t seq : {103u, 104u, 105u}) {
            auto state = h.store.nft(fixture::raw(x), seq);
            EXPECT_TRUE(state.at("is_burned").get<bool>()) << seq << ' ' << state.dump();
            EXPECT_EQ(state.at("ledger_index"), 103) << seq;
        }
        EXPECT_FALSE(h.store.nft(fixture::raw(z), 105).at("is_burned").get<bool>());
        EXPECT_TRUE(h.store.has_nft_state());
        // The mint, the sale and the burn.
        EXPECT_EQ(history(h.store, x), 3u);
        EXPECT_EQ(history(h.store, q), 0u);
        EXPECT_TRUE(offered(h.store, x));
        EXPECT_FALSE(offered(h.store, q));
    }
    // The store as an older build left it: no burn entry, both failed burns listed, the token only
    // a failed burn names offered by nfts_by_issuer, and no mark of the upgrade.
    edit_store(directory / "store", [&](rocksdb::DB& db, auto const& cf) {
        EXPECT_TRUE(db.Delete({}, cf.at("nft"), fixture::raw(x) + be32(~103u)).ok());
        EXPECT_TRUE(db.Put({}, cf.at("nft_tx"), fixture::raw(x) + be32(~104u) + be32(~0u), failed_hash).ok());
        EXPECT_TRUE(db.Put({}, cf.at("nft_tx"), fixture::raw(q) + be32(~105u) + be32(~0u), ghost_hash).ok());
        EXPECT_TRUE(db.Put({}, cf.at("nft_issuers"), Bytes(20, '\0') + nft_issuer_entry(fixture::raw(q)).second, "").ok());
        EXPECT_TRUE(db.Delete({}, cf.at("default"), "nft_clio_indexed").ok());
    });
    {
        Store stale(directory / "store", false);
        EXPECT_FALSE(stale.has_nft_state());
        EXPECT_FALSE(stale.nft(fixture::raw(x), 105).at("is_burned").get<bool>());  // what the live node answered
        EXPECT_EQ(history(stale, x), 4u);
    }
    { Store upgraded(directory / "store", true); }
    {
        Store store(directory / "store", false);
        EXPECT_TRUE(store.has_nft_state());
        auto state = store.nft(fixture::raw(x), 105);
        EXPECT_TRUE(state.at("is_burned").get<bool>()) << state.dump();
        EXPECT_EQ(state.at("ledger_index"), 103);
        EXPECT_EQ(store.nft(fixture::raw(x), 102).at("owner"), r::toBase58(b));
        EXPECT_FALSE(store.nft(fixture::raw(x), 102).at("is_burned").get<bool>());
        EXPECT_EQ(history(store, x), 3u);
        EXPECT_EQ(history(store, q), 0u);
        EXPECT_TRUE(offered(store, x));
        EXPECT_FALSE(offered(store, q));
    }
    fs::remove_all(directory);
}
// A pseudo-transaction repeated in two flag ledgers (mainnet's SetFee 1C15FEA3... in 3,715,073 and
// 3,721,729) has one ID: tx answers the earlier ledger, as xrpld, and each ledger reads its own
// copy. A store that kept the later ledger's under the ID is put right by a writable open.
TEST(Indexes, AReusedTransactionIdAnswersItsEarliestLedger) {
    fs::path directory;
    Bytes id;
    TransactionRecord fee, again;
    {
        IndexedHistory h; directory = h.dir;
        r::STTx vote(r::ttFEE, [](r::STObject& obj) {
            obj.setAccountID(r::sfAccount, r::AccountID{}); obj.setFieldU64(r::sfBaseFee, 10); obj.setFieldU32(r::sfReferenceFeeUnits, 10);
            obj.setFieldU32(r::sfReserveBase, 50000000); obj.setFieldU32(r::sfReserveIncrement, 12500000);
        });
        r::STObject meta(r::sfMetadata); meta.setFieldU8(r::sfTransactionResult, 0); meta.setFieldU32(r::sfTransactionIndex, 0);
        meta.setFieldArray(r::sfAffectedNodes, r::STArray(r::sfAffectedNodes));
        fee = {fixture::serialized(vote), fixture::serialized(meta)};
        meta.setFieldAmount(r::sfDeliveredAmount, r::STAmount(r::XRPAmount{1}));  // any difference
        again = {fee.transaction, fixture::serialized(meta)};
        id = fixture::raw(vote.getTransactionID());
        // The ledgers after flag ledgers 256 and 512, as on mainnet.
        while (h.head.seq < 256) h.append({});
        h.append({}, {fee});
        while (h.head.seq < 512) h.append({});
        h.append({}, {again});
        EXPECT_EQ(h.store.transaction(id).at("ledger_index"), 257);
        EXPECT_EQ(h.store.ledger_record(257).transactions.at(0).metadata, fee.metadata);
        EXPECT_EQ(h.store.ledger_record(513).transactions.at(0).metadata, again.metadata);
    }
    edit_store(directory / "store", [&](rocksdb::DB& db, auto const& cf) {
        std::string earlier, later;
        ASSERT_TRUE(db.Get({}, cf.at("tx"), id, &earlier).ok());
        ASSERT_TRUE(db.Get({}, cf.at("tx"), id + be32(513), &later).ok());
        EXPECT_TRUE(db.Put({}, cf.at("tx"), id, later).ok());
        EXPECT_TRUE(db.Put({}, cf.at("tx"), id + be32(257), earlier).ok());
        EXPECT_TRUE(db.Delete({}, cf.at("tx"), id + be32(513)).ok());
        EXPECT_TRUE(db.Delete({}, cf.at("default"), "tx_first_kept").ok());
    });
    { EXPECT_EQ(Store(directory / "store", false).transaction(id).at("ledger_index"), 513); }
    { Store upgraded(directory / "store", true); }
    {
        Store store(directory / "store", false);
        EXPECT_EQ(store.transaction(id).at("ledger_index"), 257);
        EXPECT_EQ(store.ledger_record(257).transactions.at(0).metadata, fee.metadata);
        EXPECT_EQ(store.ledger_record(513).transactions.at(0).metadata, again.metadata);
    }
    fs::remove_all(directory);
}
// The Clio plan's successor rows for created and deleted objects come from the verified state
// tree; they are the neighbours the store's version keyspace gives at each ledger, past the keys
// of objects deleted earlier (which the keyspace still holds).
TEST(Indexes, ClioSuccessorsOfCreatedAndDeletedObjectsAreTheLedgersNeighbours) {
    fs::path directory;
    {
        IndexedHistory h; directory = h.dir;
        auto root = [](unsigned i, std::uint64_t drops) {
            r::AccountID account{i};
            r::SLE value(r::keylet::account(account));
            value.setAccountID(r::sfAccount, account);
            value.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{drops}));
            return record(value);
        };
        auto gone = [&](unsigned i) { return ObjectRecord{root(i, 1).key, {}}; };
        std::vector<ObjectRecord> first;
        for (unsigned i = 1; i <= 40; ++i) first.push_back(root(i, 1000000 + i));
        h.append(first);
        // Ledgers that delete runs of objects, create others among the dead keys, modify some,
        // and recreate a deleted one.
        std::vector<ObjectRecord> second, third, fourth, fifth;
        for (unsigned i = 1; i <= 30; i += 1) if (i % 3) second.push_back(gone(i));
        for (unsigned i = 41; i <= 50; ++i) second.push_back(root(i, 2000000 + i));
        for (unsigned i = 31; i <= 40; ++i) third.push_back(gone(i));
        third.push_back(root(3, 3000000));
        for (unsigned i = 51; i <= 55; ++i) fourth.push_back(root(i, 4000000 + i));
        for (unsigned i = 41; i <= 45; ++i) fourth.push_back(gone(i));
        fourth.push_back(root(6, 4000006));  // modified: no successor rows
        fifth.push_back(root(1, 5000000));
        for (unsigned i = 51; i <= 55; ++i) fifth.push_back(gone(i));
        for (auto* ledger : {&second, &third, &fourth, &fifth}) h.append(*ledger);
        auto plan = import_clio({{h.dir, h.dir / "clio", "101-105"}});
        // The rows the store's version keyspace gives, as the plan was first built.
        std::multiset<std::tuple<Bytes, std::uint32_t, Bytes>> expected, planned;
        for (std::uint32_t seq = 102; seq <= 105; ++seq) {
            for (auto const& patch : h.store.ledger_record(seq).patches) {
                auto old_object = h.store.object(patch.key, seq - 1);
                if (!old_object.empty() && !patch.object.empty()) continue;
                auto next = h.store.object_page(seq, to_hex(patch.key), 1, true);
                auto successor = next.at("state").empty() ? Bytes(32, '\xff') : from_hex(next.at("state").front().at("index"));
                expected.insert({h.store.predecessor(patch.key, seq), seq, patch.object.empty() ? successor : patch.key});
                if (!patch.object.empty()) expected.insert({patch.key, seq, successor});
            }
        }
        std::ifstream input(plan.at("plan").get<std::string>());
        for (std::string line; std::getline(input, line);) {
            auto row = Json::parse(line);
            if (row.value("table", "") != "successor" || row.at("values").at("seq") == 101) continue;
            auto const& values = row.at("values");
            planned.insert({from_hex(values.at("key").at("blob").get<std::string>()), values.at("seq").get<std::uint32_t>(),
                            from_hex(values.at("next").at("blob").get<std::string>())});
        }
        // 20 deleted and 10 created, then 10 deleted; 5 created and 5 deleted; 1 recreated and 5 deleted.
        EXPECT_EQ(expected.size(), 20u + 2 * 10 + 10 + 2 * 5 + 5 + 2 * 1 + 5);
        EXPECT_EQ(planned, expected);
    }
    fs::remove_all(directory);
}
// The order book index (book_dirs) lists a book's price levels as they are at each ledger:
// directories created, deleted, recreated and changed across ledgers, in two books, against a
// walk of the store's state.
TEST(Indexes, BookDirectoriesAreTheBooksPriceLevelsAtEachLedger) {
    fs::path directory;
    {
        IndexedHistory h; directory = h.dir;
        r::Book usd{r::xrpIssue(), r::Issue{r::Currency{1}, r::AccountID{7}}, std::nullopt};
        r::Book eur{r::xrpIssue(), r::Issue{r::Currency{2}, r::AccountID{7}}, std::nullopt};
        auto level = [](r::Book const& book, std::uint64_t rate, std::uint64_t indexes) {
            r::SLE dir(r::keylet::quality(r::keylet::book(book), rate));
            dir.setFieldU64(r::sfExchangeRate, rate);
            dir.setFieldH256(r::sfRootIndex, dir.key());
            std::vector<r::uint256> entries; for (std::uint64_t i = 0; i < indexes; ++i) entries.push_back(r::uint256{i + 1});
            dir.setFieldV256(r::sfIndexes, r::STVector256{entries});
            return record(dir);
        };
        auto gone = [&](r::Book const& book, std::uint64_t rate) { return ObjectRecord{level(book, rate, 1).key, {}}; };
        // An account root, which the index leaves out.
        r::AccountID a{1};
        r::SLE root(r::keylet::account(a)); root.setAccountID(r::sfAccount, a); root.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{10000000}));
        std::vector<std::vector<ObjectRecord>> ledgers = {
            {level(usd, 100, 1), level(usd, 200, 1), level(usd, 300, 1), level(eur, 150, 1), record(root)},
            {gone(usd, 200), level(usd, 400, 1), level(usd, 100, 2)},  // one deleted, one added, one changed
            {gone(usd, 100), level(usd, 200, 1), gone(eur, 150)},      // one recreated; the other book emptied
            {level(usd, 300, 3), {fixture::raw(root.key()), {}}},
            {gone(usd, 300), gone(usd, 400), level(usd, 50, 1)},
        };
        for (auto const& patches : ledgers) h.append(patches);
        for (auto const& book : {usd, eur}) {
            auto base = r::getBookBase(book);
            auto end = r::getQualityNext(base);
            for (std::uint32_t seq = 101; seq <= 105; ++seq) {
                SCOPED_TRACE(seq);
                // The state walk: every object key in (base, end) that exists at the ledger.
                std::vector<Bytes> walked;
                std::string marker = to_hex(fixture::raw(base));
                for (;;) {
                    auto page = h.store.object_page(seq, marker, 1, true);
                    if (page.at("state").empty()) break;
                    auto key = from_hex(page.at("state").front().at("index").get<std::string>());
                    if (key >= fixture::raw(end)) break;
                    walked.push_back(key); marker = to_hex(key);
                }
                auto indexed = h.store.book_directories(seq, fixture::raw(base), fixture::raw(end), 100);
                ASSERT_TRUE(indexed.has_value());
                EXPECT_EQ(*indexed, walked);
                // A limit stops it early, in order.
                auto first = h.store.book_directories(seq, fixture::raw(base), fixture::raw(end), 1);
                EXPECT_EQ(*first, std::vector<Bytes>(walked.begin(), walked.begin() + std::min<std::size_t>(1, walked.size())));
            }
        }
        // Levels at ledger 105: 50 and 200 in the USD book, none in the EUR one.
        auto usd_base = r::getBookBase(usd);
        EXPECT_EQ(h.store.book_directories(105, fixture::raw(usd_base), fixture::raw(r::getQualityNext(usd_base)), 100)->size(), 2u);
    }
    fs::remove_all(directory);
}
// A build interrupted after the store took its ledgers but before it recorded the archive's files
// (finish_admission) initializes and appends them again: the baseline's directories deleted
// since stay deleted, and one created again later is a price level again.
TEST(Indexes, BookDirectoriesDeletedSinceTheBaselineStayDeletedWhenAdmissionResumes) {
    fs::path directory;
    {
        r::Book usd{r::xrpIssue(), r::Issue{r::Currency{1}, r::AccountID{7}}, std::nullopt};
        auto level = [&](std::uint64_t rate, std::uint64_t indexes) {
            r::SLE dir(r::keylet::quality(r::keylet::book(usd), rate));
            dir.setFieldU64(r::sfExchangeRate, rate);
            dir.setFieldH256(r::sfRootIndex, dir.key());
            std::vector<r::uint256> entries; for (std::uint64_t i = 0; i < indexes; ++i) entries.push_back(r::uint256{i + 1});
            dir.setFieldV256(r::sfIndexes, r::STVector256{entries});
            return record(dir);
        };
        auto gone = [&](std::uint64_t rate) { return ObjectRecord{level(rate, 1).key, {}}; };
        IndexedHistory h({level(100, 1), level(200, 1)}); directory = h.dir;
        h.append({gone(100)});
        h.append({level(300, 1)});
        h.store.initialize(h.baseline, {101, 200}, [&](auto const& visit) { for (auto const& item : h.snapshot) visit(item); });
        for (auto const& ledger : h.appended) EXPECT_FALSE(h.store.append(ledger));
        h.append({level(100, 2)});
        h.append({gone(200)});
        auto base = r::getBookBase(usd);
        auto end = r::getQualityNext(base);
        std::vector<std::vector<Bytes>> expected = {
            {level(200, 1).key}, {level(200, 1).key, level(300, 1).key},
            {level(100, 1).key, level(200, 1).key, level(300, 1).key}, {level(100, 1).key, level(300, 1).key}};
        for (std::uint32_t seq = 101; seq <= 104; ++seq) {
            SCOPED_TRACE(seq);
            auto indexed = h.store.book_directories(seq, fixture::raw(base), fixture::raw(end), 100);
            ASSERT_TRUE(indexed.has_value());
            auto want = expected[seq - 101]; std::sort(want.begin(), want.end());
            EXPECT_EQ(*indexed, want);
        }
    }
    fs::remove_all(directory);
}
// A book index marked "1" (by a build from before the resumed-build fix, which may have left it
// without price levels) is not used: book_offers reads the object versions instead. One built
// now is marked "2" and used.
TEST(Indexes, BookDirectoriesMarkedByAnOlderBuildAreNotUsed) {
    r::Book usd{r::xrpIssue(), r::Issue{r::Currency{1}, r::AccountID{7}}, std::nullopt};
    auto base = r::getBookBase(usd);
    auto end = r::getQualityNext(base);
    r::SLE dir(r::keylet::quality(r::keylet::book(usd), 100));
    dir.setFieldU64(r::sfExchangeRate, 100);
    dir.setFieldH256(r::sfRootIndex, dir.key());
    dir.setFieldV256(r::sfIndexes, r::STVector256{std::vector<r::uint256>{r::uint256{1}}});
    fs::path directory;
    {
        IndexedHistory h({record(dir)}); directory = h.dir;
        auto indexed = h.store.book_directories(101, fixture::raw(base), fixture::raw(end), 100);
        ASSERT_TRUE(indexed.has_value());
        EXPECT_EQ(*indexed, std::vector<Bytes>{fixture::raw(dir.key())});
    }
    edit_store(directory / "store", [&](rocksdb::DB& db, auto const& cf) {
        EXPECT_TRUE(db.Put({}, cf.at("default"), "book_dirs_indexed", "1").ok());
    });
    { Store stale(directory / "store", false); EXPECT_FALSE(stale.book_directories(101, fixture::raw(base), fixture::raw(end), 100).has_value()); }
    { Store upgraded(directory / "store", true); EXPECT_FALSE(upgraded.book_directories(101, fixture::raw(base), fixture::raw(end), 100).has_value()); }
    edit_store(directory / "store", [&](rocksdb::DB& db, auto const& cf) {
        std::string flag;
        EXPECT_TRUE(db.Get({}, cf.at("default"), "book_dirs_indexed", &flag).IsNotFound());  // removed on the writable open
    });
    fs::remove_all(directory);
}
// A fee vote identical in two flag ledgers has one ID (mainnet's SetFee in ledgers 3,715,073
// and 3,721,729): xrpld keeps one ledger's rows for it, its Transactions row and its
// AccountTransactions rows, and mainnet's full-history servers keep the earlier ledger's. So do
// the import's tables, loaded in bulk or row at a time.
TEST(Indexes, AFeeVoteInTwoLedgersHasTheEarlierLedgersRowsAsXrpld) {
    for (char const* bulk : {"1", "0"}) {
        SCOPED_TRACE(bulk);
        ::setenv("XRPLA_SQL_BULK", bulk, 1);
        fs::path directory;
        {
            IndexedHistory h; directory = h.dir;
            r::STTx vote(r::ttFEE, [](r::STObject& obj) {
                obj.setAccountID(r::sfAccount, r::AccountID{}); obj.setFieldU64(r::sfBaseFee, 10); obj.setFieldU32(r::sfReferenceFeeUnits, 10);
                obj.setFieldU32(r::sfReserveBase, 50000000); obj.setFieldU32(r::sfReserveIncrement, 12500000);
            });
            r::AccountID a{1};
            // Metadata naming an account, so each copy has AccountTransactions rows.
            r::STObject meta(r::sfMetadata); meta.setFieldU8(r::sfTransactionResult, 0); meta.setFieldU32(r::sfTransactionIndex, 0);
            r::STObject touched(r::sfModifiedNode); touched.setFieldH256(r::sfLedgerIndex, r::keylet::account(a).key); touched.setFieldU16(r::sfLedgerEntryType, r::ltACCOUNT_ROOT);
            r::STObject final_fields(r::sfFinalFields); final_fields.setAccountID(r::sfAccount, a); touched.setFieldObject(r::sfFinalFields, final_fields);
            r::STArray nodes(r::sfAffectedNodes); nodes.pushBack(touched); meta.setFieldArray(r::sfAffectedNodes, nodes);
            TransactionRecord fee{fixture::serialized(vote), fixture::serialized(meta)};
            meta.setFieldAmount(r::sfDeliveredAmount, r::STAmount(r::XRPAmount{1}));  // any difference
            TransactionRecord again{fee.transaction, fixture::serialized(meta)};
            auto root = [&](std::uint64_t drops) { r::SLE value(r::keylet::account(a)); value.setAccountID(r::sfAccount, a); value.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{drops})); return record(value); };
            h.append({root(10000000)}, {fee});
            h.append({root(9000000)}, {nft_tx(a, 1, {})});
            h.append({root(8000000)}, {again});
            // The store reads each ledger back as it was, and the ID as its earlier ledger (xrpld).
            EXPECT_EQ(h.store.ledger_record(101).transactions.at(0).metadata, fee.metadata);
            EXPECT_EQ(h.store.ledger_record(103).transactions.at(0).metadata, again.metadata);
            EXPECT_EQ(h.store.transaction(fixture::raw(vote.getTransactionID())).at("ledger_index"), 101);
            import_rippled({h.dir, h.dir / "rippled", "101-103"});
            sqlite3* db = nullptr;
            ASSERT_EQ(sqlite3_open((h.dir / "rippled" / "transaction.db").string().c_str(), &db), SQLITE_OK);
            auto rows = [&](std::string const& sql) {
                std::vector<std::string> out; sqlite3_stmt* s = nullptr;
                EXPECT_EQ(sqlite3_prepare_v2(db, sql.c_str(), -1, &s, nullptr), SQLITE_OK) << sqlite3_errmsg(db);
                while (sqlite3_step(s) == SQLITE_ROW) out.push_back(reinterpret_cast<char const*>(sqlite3_column_text(s, 0)));
                sqlite3_finalize(s); return out;
            };
            auto id = r::to_string(vote.getTransactionID());
            EXPECT_EQ(rows("SELECT LedgerSeq FROM Transactions WHERE TransID = '" + id + "'"), std::vector<std::string>{"101"});
            EXPECT_EQ(rows("SELECT DISTINCT LedgerSeq FROM AccountTransactions WHERE TransID = '" + id + "'"), std::vector<std::string>{"101"});
            EXPECT_EQ(rows("SELECT count(*) FROM Transactions"), std::vector<std::string>{"2"});
            EXPECT_EQ(rows("SELECT count(*) FROM sqlite_master WHERE name = 'xrpla_pseudo'"), std::vector<std::string>{"0"});
            EXPECT_EQ(rows("SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_autoindex_Transactions_1'"), std::vector<std::string>{"1"});
            sqlite3_close(db);
        }
        fs::remove_all(directory);
    }
    ::unsetenv("XRPLA_SQL_BULK");
}
TEST(Indexes, ClioDoesNotVersionSuccessorsForModifiedObjects) {
    fs::path directory;
    {
        IndexedHistory h; directory = h.dir;
        r::AccountID account{1};
        r::SLE root(r::keylet::account(account));
        root.setAccountID(r::sfAccount, account);
        root.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{10000000}));
        h.append({record(root)});
        root.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{9000000}));
        h.append({record(root)});
        auto plan = import_clio({{h.dir, h.dir / "clio", "101-102"}});
        std::ifstream input(plan.at("plan").get<std::string>());
        std::string line; unsigned baseline = 0, changes = 0, object_changes = 0;
        while (std::getline(input, line)) {
            auto row = Json::parse(line);
            if (row.value("table", "") == "successor") {
                if (row.at("values").at("seq") == 101) ++baseline; else ++changes;
            }
            if (row.value("table", "") == "objects" && row.at("values").at("sequence") == 102) ++object_changes;
        }
        EXPECT_EQ(baseline, 2); EXPECT_EQ(changes, 0); EXPECT_EQ(object_changes, 1);
    }
    fs::remove_all(directory);
}
