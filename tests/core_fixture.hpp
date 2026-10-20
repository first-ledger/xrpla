#pragma once
#include <xrpl/protocol/Indexes.h>
#include <xrpla/core/verify.hpp>
#include <xrpl/basics/chrono.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <xrpl/protocol/STTx.h>
#include <xrpl/shamap/Family.h>
#include <xrpl/shamap/SHAMap.h>
#include <algorithm>
#include <map>

namespace fixture {
using namespace xrpla;
using namespace xrpla::core;
namespace r = ::xrpl;
template<class T> Bytes raw(T const& v) { return {reinterpret_cast<char const*>(v.data()), v.size()}; }
inline Bytes serialized(r::STObject const& o) { return raw(o.getSerializer()); }
inline Bytes serialized(r::LedgerHeader const& h) { r::Serializer s; r::addRaw(h, s); return raw(s); }
class Family : public r::Family {
    beast::Journal j_{beast::Journal::getNullSink()};
public:
    r::node_store::Database& db() override { throw Error("Fixture attempted backing-store access"); }
    r::node_store::Database const& db() const override { throw Error("Fixture attempted backing-store access"); }
    beast::Journal const& journal() override { return j_; }
    std::shared_ptr<r::FullBelowCache> getFullBelowCache() override { return {}; }
    std::shared_ptr<r::TreeNodeCache> getTreeNodeCache() override { return {}; }
    void sweep() override {}
    void reset() override {}
    void missingNodeAcquireBySeq(std::uint32_t, r::uint256 const&) override { throw Error("Fixture missing node"); }
    void missingNodeAcquireByHash(r::uint256 const&, std::uint32_t) override { throw Error("Fixture missing node"); }
};
struct Chain {
    std::vector<ObjectRecord> snapshot;
    Bytes snapshot_header;
    std::vector<LedgerRecord> ledgers;
    r::AccountID account{1}, second{2};
    Bytes account_key, directory_key, second_key;
    std::vector<Bytes> tx_hashes;
    explicit Chain(std::size_t count = 3) {
        Family family;
        r::SHAMap state(r::SHAMapType::STATE, family);
        state.setUnbacked();
        r::STLedgerEntry a(r::keylet::account(account));
        a.setAccountID(r::sfAccount, account);
        a.setFieldU32(r::sfSequence, 1);
        a.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{1000000}));
        r::STLedgerEntry directory(r::keylet::ownerDir(account));
        directory.setAccountID(r::sfOwner, account);
        directory.setFieldH256(r::sfRootIndex, directory.key());
        directory.setFieldV256(r::sfIndexes, r::STVector256(r::sfIndexes, {r::uint256{10}}));
        r::STLedgerEntry b(r::keylet::account(second));
        b.setAccountID(r::sfAccount, second);
        b.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{1000}));
        b.setFieldU32(r::sfSequence, 1);
        account_key = raw(a.key()); directory_key = raw(directory.key()); second_key = raw(b.key());
        snapshot = {{account_key, serialized(a)}, {directory_key, serialized(directory)}};
        std::sort(snapshot.begin(), snapshot.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
        for (auto const& item : snapshot)
            state.addItem(r::SHAMapNodeType::TnAccountState, r::makeShamapitem(r::uint256::fromVoid(item.key.data()), {item.object.data(), item.object.size()}));
        r::LedgerHeader header{};
        header.seq = 65535;
        header.drops = r::XRPAmount{1000000000};
        header.closeTime = r::NetClock::time_point{r::NetClock::duration{1000}};
        header.closeTimeResolution = r::NetClock::duration{10};
        header.accountHash = state.getHash().asUInt256();
        snapshot_header = serialized(header);
        for (std::size_t n = 0; n < count; ++n) {
            LedgerRecord ledger{};
            ledger.sequence = static_cast<std::uint32_t>(65536 + n);
            if (n == 0) {
                a.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{900000}));
                a.setFieldU32(r::sfSequence, 3);
                directory.setFieldV256(r::sfIndexes, r::STVector256(r::sfIndexes, {r::uint256{10}, r::uint256{20}}));
                ledger.patches = {{account_key, serialized(a)}, {directory_key, serialized(directory)}};
                for (std::uint32_t p = 0; p < 2; ++p) {
                    r::STTx tx(r::ttACCOUNT_SET, [&](r::STObject& o) {
                        o.setAccountID(r::sfAccount, account);
                        o.setFieldU32(r::sfSequence, p + 1);
                        o.setFieldAmount(r::sfFee, r::STAmount(r::XRPAmount{10}));
                        o.setFieldVL(r::sfSigningPubKey, r::Blob{});
                    });
                    r::STObject meta(r::sfMetadata);
                    meta.setFieldU8(r::sfTransactionResult, 0);
                    meta.setFieldU32(r::sfTransactionIndex, p);
                    r::STObject final(r::sfFinalFields);
                    final.setAccountID(r::sfOwner, account);
                    final.setFieldH256(r::sfRootIndex, directory.key());
                    // sfIndexes is intentionally absent: v2 supplies it in the patch.
                    r::STObject node(r::sfModifiedNode);
                    node.setFieldH256(r::sfLedgerIndex, directory.key());
                    node.setFieldU16(r::sfLedgerEntryType, r::ltDIR_NODE);
                    node.setFieldObject(r::sfFinalFields, final);
                    r::STArray nodes(r::sfAffectedNodes); nodes.pushBack(node);
                    meta.setFieldArray(r::sfAffectedNodes, nodes);
                    ledger.transactions.push_back({serialized(tx), serialized(meta)});
                    tx_hashes.push_back(raw(tx.getTransactionID()));
                }
                if (tx_hashes[0] > tx_hashes[1]) std::swap(ledger.transactions[0], ledger.transactions[1]);
            } else if (n == 1) ledger.patches = {{second_key, serialized(b)}};
            else if (n == 2) ledger.patches = {{directory_key, {}}};
            std::sort(ledger.patches.begin(), ledger.patches.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
            for (auto const& patch : ledger.patches) {
                auto key = r::uint256::fromVoid(patch.key.data());
                if (patch.object.empty()) state.delItem(key);
                else {
                    auto item = r::makeShamapitem(key, {patch.object.data(), patch.object.size()});
                    if (state.peekItem(key)) state.updateGiveItem(r::SHAMapNodeType::TnAccountState, item);
                    else state.addItem(r::SHAMapNodeType::TnAccountState, item);
                }
            }
            r::SHAMap transactions(r::SHAMapType::TRANSACTION, family);
            transactions.setUnbacked();
            for (auto const& record : ledger.transactions) {
                r::STTx tx(r::SerialIter(record.transaction.data(), record.transaction.size()));
                r::Serializer leaf;
                leaf.addVL(record.transaction.data(), static_cast<int>(record.transaction.size()));
                leaf.addVL(record.metadata.data(), static_cast<int>(record.metadata.size()));
                transactions.addItem(r::SHAMapNodeType::TnTransactionMd, r::makeShamapitem(tx.getTransactionID(), leaf.slice()));
            }
            header.parentHash = r::calculateLedgerHash(header);
            header.seq = ledger.sequence;
            header.parentCloseTime = header.closeTime;
            header.closeTime += r::NetClock::duration{10};
            header.accountHash = state.getHash().asUInt256();
            header.txHash = transactions.getHash().asUInt256();
            ledger.header = serialized(header);
            ledgers.push_back(std::move(ledger));
        }
    }
    void seed(LedgerVerifier& verifier) const {
        for (auto const& item : snapshot) verifier.add_snapshot_object(item);
        auto header = decode_header(snapshot_header);
        verifier.finish_snapshot(snapshot_header, header.sequence, header.account_hash);
    }
};
// A chain whose state changes in every ledger: `accounts` accounts, and in ledger n account
// n % accounts sends an AccountSet (its sequence and balance change), so each ledger has its
// own state root and a one-transaction tree.
struct Evolving {
    std::vector<ObjectRecord> snapshot;
    Bytes snapshot_header;
    std::vector<LedgerRecord> ledgers;
    explicit Evolving(std::size_t count, unsigned accounts = 32) {
        Family family;
        r::SHAMap state(r::SHAMapType::STATE, family);
        state.setUnbacked();
        std::vector<r::STLedgerEntry> roots;
        for (unsigned n = 0; n < accounts; ++n) {
            r::AccountID id{n + 1};
            r::STLedgerEntry a(r::keylet::account(id));
            a.setAccountID(r::sfAccount, id);
            a.setFieldU32(r::sfSequence, 1);
            a.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{100000000}));
            snapshot.push_back({raw(a.key()), serialized(a)});
            roots.push_back(std::move(a));
        }
        std::sort(snapshot.begin(), snapshot.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
        for (auto const& item : snapshot)
            state.addItem(r::SHAMapNodeType::TnAccountState, r::makeShamapitem(r::uint256::fromVoid(item.key.data()), {item.object.data(), item.object.size()}));
        r::LedgerHeader header{};
        header.seq = 65535;
        header.drops = r::XRPAmount{1000000000};
        header.closeTime = r::NetClock::time_point{r::NetClock::duration{1000}};
        header.closeTimeResolution = r::NetClock::duration{10};
        header.accountHash = state.getHash().asUInt256();
        snapshot_header = serialized(header);
        for (std::size_t n = 0; n < count; ++n) {
            auto& a = roots[n % accounts];
            auto sequence = a.getFieldU32(r::sfSequence);
            a.setFieldU32(r::sfSequence, sequence + 1);
            a.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{100000000 - 10 * static_cast<std::int64_t>(n / accounts + 1)}));
            LedgerRecord ledger{};
            ledger.sequence = static_cast<std::uint32_t>(65536 + n);
            ledger.patches = {{raw(a.key()), serialized(a)}};
            r::STTx tx(r::ttACCOUNT_SET, [&](r::STObject& o) {
                o.setAccountID(r::sfAccount, a.getAccountID(r::sfAccount));
                o.setFieldU32(r::sfSequence, sequence);
                o.setFieldAmount(r::sfFee, r::STAmount(r::XRPAmount{10}));
                o.setFieldVL(r::sfSigningPubKey, r::Blob{});
            });
            r::STObject meta(r::sfMetadata);
            meta.setFieldU8(r::sfTransactionResult, 0);
            meta.setFieldU32(r::sfTransactionIndex, 0);
            r::STObject final(r::sfFinalFields);
            final.setAccountID(r::sfAccount, a.getAccountID(r::sfAccount));
            final.setFieldAmount(r::sfBalance, a.getFieldAmount(r::sfBalance));
            final.setFieldU32(r::sfSequence, sequence + 1);
            r::STObject node(r::sfModifiedNode);
            node.setFieldH256(r::sfLedgerIndex, a.key());
            node.setFieldU16(r::sfLedgerEntryType, r::ltACCOUNT_ROOT);
            node.setFieldObject(r::sfFinalFields, final);
            r::STArray nodes(r::sfAffectedNodes); nodes.pushBack(node);
            meta.setFieldArray(r::sfAffectedNodes, nodes);
            ledger.transactions = {{serialized(tx), serialized(meta)}};
            state.updateGiveItem(r::SHAMapNodeType::TnAccountState,
                r::makeShamapitem(a.key(), {ledger.patches[0].object.data(), ledger.patches[0].object.size()}));
            r::SHAMap transactions(r::SHAMapType::TRANSACTION, family);
            transactions.setUnbacked();
            r::Serializer leaf;
            leaf.addVL(ledger.transactions[0].transaction.data(), static_cast<int>(ledger.transactions[0].transaction.size()));
            leaf.addVL(ledger.transactions[0].metadata.data(), static_cast<int>(ledger.transactions[0].metadata.size()));
            transactions.addItem(r::SHAMapNodeType::TnTransactionMd, r::makeShamapitem(tx.getTransactionID(), leaf.slice()));
            header.parentHash = r::calculateLedgerHash(header);
            header.seq = ledger.sequence;
            header.parentCloseTime = header.closeTime;
            header.closeTime += r::NetClock::duration{10};
            header.accountHash = state.getHash().asUInt256();
            header.txHash = transactions.getHash().asUInt256();
            ledger.header = serialized(header);
            ledgers.push_back(std::move(ledger));
        }
    }
    void seed(LedgerVerifier& verifier) const {
        for (auto const& item : snapshot) verifier.add_snapshot_object(item);
        auto header = decode_header(snapshot_header);
        verifier.finish_snapshot(snapshot_header, header.sequence, header.account_hash);
    }
};
} // namespace fixture
