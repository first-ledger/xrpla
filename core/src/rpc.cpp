#include "rpc.hpp"
#include "piece_history.hpp"
#include "live_history.hpp"
#include "wire.hpp"
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/HashPrefix.h>
#include <xrpl/protocol/digest.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/basics/StringUtilities.h>
#include <xrpl/protocol/nft.h>
#include <xrpl/protocol/NFTokenID.h>
#include <xrpl/protocol/NFTokenOfferID.h>
#include <xrpl/protocol/STTx.h>
#include <xrpl/protocol/TER.h>
#include <xrpl/protocol/TxMeta.h>
#include <xrpl/ledger/helpers/AccountRootHelpers.h>
#include <algorithm>
#include <cctype>
#include <atomic>
#include <charconv>
#include <cmath>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <set>

namespace xrpla::core::rpc {
[[noreturn]] void fail(r::ErrorCodeI code, std::string const& message) {
    auto value = message.empty() ? r::rpc::makeError(code) : r::rpc::makeError(code, message);
    throw Failure{Json::parse(value.toStyledString())};
}
std::string text(Json const& p, std::string const& field) {
    if (!p.contains(field)) fail(r::RpcInvalidParams, "Missing field '" + field + "'.");
    if (!p.at(field).is_string()) fail(r::RpcInvalidParams, "Invalid field '" + field + "'.");
    return p.at(field).get<std::string>();
}
bool boolean(Json const& p, std::string const& field, bool fallback) {
    if (!p.contains(field)) return fallback;
    if (!p.at(field).is_boolean()) fail(r::RpcInvalidParams, "Invalid field '" + field + "'.");
    return p.at(field).get<bool>();
}
bool loose_boolean(Json const& p, std::string const& field) {
    if (!p.contains(field)) return false;
    auto const& value = p.at(field);
    if (value.is_boolean()) return value.get<bool>();
    if (value.is_number_integer()) return value.get<std::int64_t>() != 0;
    if (value.is_number()) return value.get<double>() != 0.0;
    if (value.is_string()) return !value.get_ref<std::string const&>().empty();
    if (value.is_array() || value.is_object()) return !value.empty();
    return false;
}
std::uint32_t integer(Json const& p, std::string const& field, std::uint32_t fallback, std::uint32_t maximum) {
    if (!p.contains(field)) return fallback;
    auto const& value = p.at(field);
    if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<std::int64_t>() < 0) || value.get<std::uint64_t>() > maximum) fail(r::RpcInvalidParams, "Invalid field '" + field + "'.");
    return value.get<std::uint32_t>();
}
Bytes hash(std::string const& value) {
    try { auto result = from_hex(value); if (result.size() == 32) return result; } catch (...) {}
    fail(r::RpcInvalidParams, "Malformed hash.");
}
std::string upper(std::string value) { for (char& c : value) if (c >= 'a' && c <= 'f') c -= 'a' - 'A'; return value; }
r::AccountID account(std::string const& text) {
    auto result = r::parseBase58<r::AccountID>(text);
    if (!result) fail(r::RpcActMalformed);
    return *result;
}
Json json(r::STBase const& object) { return to_json(object.getJson(r::JsonOptions::Values::None)); }
Json header_json(Header const& header, bool binary, unsigned version) {
    if (binary) return {{"ledger_data", upper(to_hex(header.raw))}, {"closed", true}};
    auto h = r::deserializeHeader({header.raw.data(), header.raw.size()});
    Json result{{"account_hash", upper(header.account_hash)}, {"close_flags", h.closeFlags},
        {"close_time", h.closeTime.time_since_epoch().count()}, {"close_time_human", r::to_string(h.closeTime)},
        {"close_time_iso", r::toStringIso(h.closeTime)}, {"close_time_resolution", h.closeTimeResolution.count()},
        {"ledger_hash", upper(header.hash)}, {"parent_close_time", h.parentCloseTime.time_since_epoch().count()},
        {"parent_hash", upper(header.parent_hash)}, {"total_coins", r::to_string(h.drops)},
        {"transaction_hash", upper(header.transaction_hash)}, {"closed", true}};
    result["ledger_index"] = version == 1 ? Json(std::to_string(h.seq)) : Json(h.seq);
    return result;
}
Json Context::base() const { return {{"ledger_hash", upper(header.hash)}, {"ledger_index", header.sequence}, {"validated", true}}; }
std::shared_ptr<r::SLE const> Context::read(r::Keylet const& key) const {
    Bytes bytes;
    bool fresh = false;  // read from the store now, not again
    auto id = raw(key.key);
    if (auto it = preloaded.find(id); it != preloaded.end()) bytes = it->second;
    else if (auto known = memo.find(id); known != memo.end()) bytes = known->second;
    else {
        bytes = store.object(id, header.sequence);
        fresh = true;
        if (memo.size() < 100000) memo.emplace(std::move(id), bytes);
    }
    if (bytes.empty()) return {};
    r::SerialIter it(bytes.data(), bytes.size());
    auto object = std::make_shared<r::SLE>(it, key.key);
    if (!key.check(*object)) return nullptr;
    // Walkers read a directory page's entries next, one by one (unless they were preloaded, or
    // the page was read before: its entries were asked for then). A page of another directory
    // than the preloaded window's is outside that window.
    if (fresh && object->getType() == r::ltDIR_NODE && object->isFieldPresent(r::sfIndexes) &&
        (preloaded.empty() || !object->isFieldPresent(r::sfRootIndex) || object->getFieldH256(r::sfRootIndex) != preloaded_root)) {
        auto const& entries = object->getFieldV256(r::sfIndexes);
        if (entries.size() > 1) {
            std::vector<Bytes> keys;
            for (auto const& entry : entries) keys.push_back(raw(entry));
            store.prefetch(keys, header.sequence);
        }
    }
    return object;
}
bool Context::amendment(r::uint256 const& id) const {
    if (!amendments) {
        // Kept only once read: a read that throws leaves the next call to read again, rather
        // than an empty set that would say no amendment is enabled.
        std::set<r::uint256> enabled;
        if (auto object = read(r::keylet::amendments()))
            for (auto const& item : object->getFieldV256(r::sfAmendments)) enabled.insert(item);
        amendments = std::move(enabled);
    }
    return amendments->contains(id);
}
void Context::leave_window(r::Keylet const& root) const {
    if (!preloaded.empty() && preloaded_root != root.key) preloaded.clear();
}
void Context::preload_directory(r::Keylet const& root, std::uint64_t page, std::size_t want) const {
    leave_window(root);
    if (want <= kDirectoryWindow * 16) return;
    auto root_bytes = store.object(raw(root.key), header.sequence);
    if (root_bytes.empty()) return;
    r::SerialIter it(root_bytes.data(), root_bytes.size());
    r::SLE first(it, root.key);
    if (first.getType() != r::ltDIR_NODE || !first.isFieldPresent(r::sfIndexPrevious)) return;
    auto last = first.getFieldU64(r::sfIndexPrevious);  // the root links back to the last page
    if (last < page) return;
    last = std::min(last, page + kPreloadPages - 1);
    // Pages are numbered consecutively but for pages emptied since: those read as nothing.
    std::vector<Bytes> page_keys;
    for (auto n = page; n <= last; ++n) page_keys.push_back(raw(r::keylet::page(root, n).key));
    auto pages = store.objects(page_keys, header.sequence);
    std::vector<Bytes> entries;
    for (std::size_t i = 0; i < pages.size(); ++i) {
        if (pages[i].empty()) continue;
        r::SerialIter p(pages[i].data(), pages[i].size());
        r::SLE dir(p, r::uint256::fromVoid(page_keys[i].data()));
        if (dir.getType() != r::ltDIR_NODE) continue;
        for (auto const& entry : dir.getFieldV256(r::sfIndexes)) entries.push_back(raw(entry));
        preloaded.emplace(page_keys[i], std::move(pages[i]));
    }
    auto objects = store.objects(entries, header.sequence);
    for (std::size_t i = 0; i < entries.size(); ++i) preloaded.emplace(std::move(entries[i]), std::move(objects[i]));
    preloaded_through = last;
    preloaded_root = root.key;
}
void Context::plan_ahead(r::Keylet const& root, std::uint64_t next, std::uint64_t walked, std::size_t want) const {
    leave_window(root);
    if (!preloaded.empty()) {
        // Past the preloaded window: the next one replaces it.
        if (next > preloaded_through) { preloaded.clear(); preload_directory(root, next, std::numeric_limits<std::size_t>::max()); }
        return;
    }
    // A walk still going after a window (one whose limit counts only some entries, as
    // noripple_check's problems) reads the rest of the directory in bulk.
    if (walked == kDirectoryWindow) {
        preload_directory(root, next, std::numeric_limits<std::size_t>::max());
        if (!preloaded.empty()) return;
    }
    // Owner directory pages are usually numbered consecutively: the window planned from page P
    // covers P .. P + kDirectoryWindow - 1, so halfway through it the next one starts half a
    // window past the page read next.
    if (want > kDirectoryWindow * 16 && walked % kDirectoryWindow == kDirectoryWindow / 2)
        plan_directory(root, next + kDirectoryWindow / 2, want);
}
void Context::plan_directory(r::Keylet const& root, std::uint64_t page, std::size_t want) const {
    leave_window(root);
    if (!store.prefetches() || want < 2 || !preloaded.empty()) return;
    auto pages = std::min<std::size_t>(kDirectoryWindow, want / 16 + 2);
    // Owner and book directory pages are usually numbered consecutively.
    std::vector<Bytes> keys;
    for (std::size_t i = 0; i < pages; ++i) keys.push_back(raw(r::keylet::page(root, page + i).key));
    store.prefetch(keys, header.sequence);
    std::vector<Bytes> entries;
    std::set<std::uint64_t> seen;
    for (std::size_t n = 0; n < pages && entries.size() < want && seen.insert(page).second; ++n) {
        auto key = r::keylet::page(root, page).key;
        auto bytes = store.object(raw(key), header.sequence);
        // The walk reads this page next: it finds it here rather than reading it again.
        if (memo.size() < 100000) memo.emplace(raw(key), bytes);
        if (bytes.empty()) break;
        r::SerialIter it(bytes.data(), bytes.size());
        r::SLE dir(it, key);
        if (dir.getType() != r::ltDIR_NODE) break;
        for (auto const& entry : dir.getFieldV256(r::sfIndexes)) entries.push_back(raw(entry));
        page = dir.getFieldU64(r::sfIndexNext);
        if (!page) break;
    }
    store.prefetch(entries, header.sequence);
}
LedgerFees ledger_fees(Context const& ctx) {
    LedgerFees fees;
    auto sle = ctx.read(r::keylet::feeSettings());
    if (!sle) return fees;
    if (sle->isFieldPresent(r::sfBaseFee)) fees.base = sle->getFieldU64(r::sfBaseFee);
    if (sle->isFieldPresent(r::sfReserveBase)) fees.reserve = sle->getFieldU32(r::sfReserveBase);
    if (sle->isFieldPresent(r::sfReserveIncrement)) fees.increment = sle->getFieldU32(r::sfReserveIncrement);
    if (sle->isFieldPresent(r::sfBaseFeeDrops)) fees.base = static_cast<std::uint64_t>(sle->getFieldAmount(r::sfBaseFeeDrops).xrp().drops());
    if (sle->isFieldPresent(r::sfReserveBaseDrops)) fees.reserve = static_cast<std::uint64_t>(sle->getFieldAmount(r::sfReserveBaseDrops).xrp().drops());
    if (sle->isFieldPresent(r::sfReserveIncrementDrops)) fees.increment = static_cast<std::uint64_t>(sle->getFieldAmount(r::sfReserveIncrementDrops).xrp().drops());
    return fees;
}
r::XRPAmount account_reserve(LedgerFees const& fees, r::SLE const& root) {
    auto field = [&](r::SF_UINT32 const& name) -> std::int64_t { return root.isFieldPresent(name) ? root.getFieldU32(name) : 0; };
    // OwnerCounts::count (OwnerCounts.h) and accountCountImpl (AccountRootHelpers.cpp), each held
    // to a uint32; then Fees::accountReserve.
    auto owners = std::clamp<std::int64_t>(field(r::sfOwnerCount) - field(r::sfSponsoredOwnerCount) + field(r::sfSponsoringOwnerCount), 0, UINT32_MAX);
    auto accounts = std::min<std::int64_t>((root.isFieldPresent(r::sfSponsor) ? 0 : 1) + field(r::sfSponsoringAccountCount), UINT32_MAX);
    return r::XRPAmount{static_cast<std::int64_t>(fees.reserve) * accounts + static_cast<std::int64_t>(fees.increment) * owners};
}
namespace {
std::atomic<std::uint32_t> network_id{0};  // configure_network
// XLS-37 compact transaction ID; none where a field does not fit, as xrpld's encodeCTID (the
// network a transaction names in NetworkID over the server's, as its Transaction::getJson and
// transaction stream do). `tx_method`: the stricter bounds xrpld's tx handler checks itself
// (ledger below 0x0FFFFFFF, network below 0xFFFF), with the server's network.
std::string ctid(std::uint32_t sequence, std::uint32_t index, bool tx_method = false, Json const& tx = nullptr) {
    std::uint32_t network = network_id;
    if (!tx_method && tx.is_object() && tx.contains("NetworkID") && tx.at("NetworkID").is_number_unsigned()) network = tx.at("NetworkID").get<std::uint32_t>();
    if (tx_method ? (sequence >= 0x0fffffff || index > 0xffff || network >= 0xffff) : (sequence > 0x0fffffff || index > 0xffff || network > 0xffff)) return {};
    std::uint64_t value = ((0xc0000000ULL | sequence) << 32) | (static_cast<std::uint64_t>(index) << 16) | network;
    Bytes bytes(8, '\0'); for (unsigned n = 0; n < 8; ++n) bytes[n] = static_cast<char>(value >> (56 - n * 8));
    return upper(to_hex(bytes));
}
// A transaction read once from the History: its binary form (tx_blob and meta in hex,
// ledger_index, hash) and its bytes. Every History decodes a transaction as transaction_json of
// those bytes, so the decoded form comes from them; listings used to read each transaction two or
// three times and decode it twice.
struct Stored { Json binary; TransactionRecord record; };
std::optional<Stored> stored(History const& store, Bytes const& id) {
    auto binary = store.transaction(id, true);
    if (binary.is_null()) return std::nullopt;
    TransactionRecord record{from_hex(binary.at("tx_blob").get<std::string>()), from_hex(binary.at("meta").get<std::string>())};
    return Stored{std::move(binary), std::move(record)};
}
// A transaction of a ledger record already read (a ledger listing or the stream's events), as
// stored() gives it, without looking it up again by hash.
Stored stored(TransactionRecord record, std::uint32_t sequence, Bytes const& id) {
    Json binary{{"tx_blob", to_hex(record.transaction)}, {"meta", to_hex(record.metadata)}, {"ledger_index", sequence}, {"hash", to_hex(id)}};
    return Stored{std::move(binary), std::move(record)};
}
Bytes transaction_id(TransactionRecord const& record) {
    return raw(r::sha512Half(r::HashPrefix::TransactionId, r::makeSlice(record.transaction)));
}
// As xrpld's APIs show transactions: not at all when libxrpl cannot decode one (a few of 2013),
// though the ledger (and the History) holds it.
bool listed(std::optional<Stored> const& s) { return s && api_decodable(s->record); }
Json decoded(Stored const& s) {
    auto result = transaction_json(s.record);
    result["ledger_index"] = s.binary.at("ledger_index"); result["hash"] = s.binary.at("hash");
    return result;
}
// The decoded form of a transaction xrpld's APIs list, or nothing for one they do not: decoding
// it is the api_decodable check, so a listing decodes each transaction once instead of twice.
std::optional<Json> decoded_if_listed(Stored const& s) {
    try { return decoded(s); } catch (std::exception const&) { return std::nullopt; }
}
// Whether xrpld 3.4.1 can read a transaction and its metadata at all: the STTx and metadata
// objects built from the bytes, as Ledger::deserializeTxPlusMeta (every walk over a ledger's
// transactions) and convertBlobsToTxResult (account_tx) build them. Not the canonical-form check
// of api_decodable: a transaction xrpld reads but writes back otherwise is not one this stops at.
bool xrpld_reads(TransactionRecord const& record) {
    try {
        r::SerialIter tx(record.transaction.data(), record.transaction.size());
        r::STTx parsed(tx);
        r::SerialIter meta(record.metadata.data(), record.metadata.size());
        r::STObject object(meta, r::sfMetadata);
        return true;
    } catch (std::exception const&) { return false; }
}
// Whether serialized data holds an XRP amount with the 0x20 bit among its value bits: some 2013
// transactions do (an amount too large to be valid; 625,681's OfferCreate), and libxrpl reads that
// bit as an MPT amount's since MPTs came, though the rippled versions of the time read the amount.
bool pre_mpt_amount(std::vector<wire::Field> const& fields) {
    for (auto const& field : fields) {
        if (field.type == 6 && field.value.size() == 8 && !(static_cast<unsigned char>(field.value[0]) & 0x80) && (static_cast<unsigned char>(field.value[0]) & 0x20)) return true;
        if (pre_mpt_amount(field.fields)) return true;
    }
    return false;
}
bool pre_mpt_amount(TransactionRecord const& record) {
    try {
        return pre_mpt_amount(wire::parse({record.transaction.data(), record.transaction.size()})) ||
               pre_mpt_amount(wire::parse({record.metadata.data(), record.metadata.size()}));
    } catch (std::exception const&) { return false; }
}
// The ledgers xrpld 3.4.1 does not have: those its SQL tables (Ledgers, Transactions,
// AccountTransactions) never got. saveValidatedLedger writes a ledger's rows only after building
// its AcceptedLedger, which reads every transaction of the ledger (deserializeTxPlusMeta, then
// AcceptedLedgerTx); when one cannot be read the save fails (failedSave) and nothing is written.
// So xrpld answers lgrNotFound for the ledger (LedgerHistory loads ledgers by their Ledgers row)
// and txnNotFound for each of its transactions, and lists none in account_tx. Full-history nodes
// keep the rows their earlier versions wrote, so what decides is whether the rippled that first
// saved the ledger could read it: 2013's pre-MPT XRP amounts (625,681 and 625,753) were read
// before libxrpl knew MPTs, and those ledgers are held (their transactions are then unreadable
// rows: txnNotFound, an internal error in account_tx); a transaction unreadable for any other
// reason (6,409,247's Payment, whose Memos array holds an element that is not an object:
// "Non-object in array") was refused by every version since that check, and its ledger is not
// held. Rippled has refused such transactions at consensus since 2015, so only earlier ledgers
// are read for this (each once a process).
constexpr std::uint32_t kUnreadableBefore = 20'000'000;
bool refused_ledger(History const& store, std::uint32_t sequence) {
    if (sequence >= kUnreadableBefore) return false;
    static std::mutex mutex;
    static std::vector<bool> known, refused;
    {
        std::lock_guard lock(mutex);
        if (sequence < known.size() && known[sequence]) return refused[sequence];
    }
    bool verdict = false;
    try { verdict = !ledger_held_by_xrpld(store.ledger_transaction_records(sequence)); } catch (std::exception const&) { return false; }  // a ledger without a record (a baseline): held
    std::lock_guard lock(mutex);
    if (known.size() <= sequence) { known.resize(kUnreadableBefore); refused.resize(kUnreadableBefore); }
    known[sequence] = true; refused[sequence] = verdict;
    return verdict;
}
// Whether `method` answers as xrpld does about the ledgers it holds (refused_ledger). Clio's own
// methods, and those this node answers as Clio (feature at any ledger, nft_info), answer from
// Clio's tables, which hold every ledger; the stream's internal requests publish every ledger.
bool held_as_xrpld(std::string const& method) {
    static std::set<std::string> const clio{"feature", "account_mptokens", "account_mptoken_issuances", "mpt_holders", "nfts_by_issuer",
                                            "nft_info", "nft_history", "ledger_index"};
    return !method.starts_with("_") && !clio.contains(method);
}
Json expanded(Stored const& s, Json result, Header const& header, unsigned version, bool nft_fields = true) {
    // The synthetic fields xrpld 3.4.0's insertAllSyntheticInJson adds, in ledger listings as in
    // tx and account_tx: delivered_amount, nftoken_id(s), offer_id, mpt_issuance_id. Clio's
    // nft_history (toExpandedJson with NFTokenjson::DISABLE) adds all but the NFToken ones.
    auto& tx = result.at("tx"); auto& meta = result.at("meta");
    auto type = tx.value("TransactionType", "");
    if (meta.value("TransactionResult", "") == "tesSUCCESS" && (type == "Payment" || type == "CheckCash" || type == "AccountDelete")) {
        if (meta.contains("DeliveredAmount")) meta["delivered_amount"] = meta.at("DeliveredAmount");
        else if (tx.contains("Amount") && (header.sequence >= 4594095 || header.close_time > 446000000)) meta["delivered_amount"] = tx.at("Amount");
        else meta["delivered_amount"] = "unavailable";
    }
    if (type == "Payment" && tx.contains("Amount")) { tx["DeliverMax"] = tx.at("Amount"); if (version > 1) tx.erase("Amount"); }
    if (type == "MPTokenIssuanceCreate" && meta.value("TransactionResult", "") == "tesSUCCESS") {
        for (auto const& wrapper : meta.at("AffectedNodes")) if (wrapper.contains("CreatedNode")) {
            auto const& node = wrapper.at("CreatedNode");
            if (node.value("LedgerEntryType", "") == "MPTokenIssuance") {
                auto const& fields = node.at("NewFields");
                meta["mpt_issuance_id"] = upper(to_hex(raw(r::makeMptID(fields.at("Sequence").get<std::uint32_t>(), account(fields.at("Issuer").get<std::string>())))));
            }
        }
    }
    if (nft_fields && type.starts_with("NFToken")) {
        auto const& tx_bytes = s.record.transaction; auto const& meta_bytes = s.record.metadata;
        auto native_tx = std::make_shared<r::STTx const>(r::SerialIter(tx_bytes.data(), tx_bytes.size()));
        r::SerialIter iterator(meta_bytes.data(), meta_bytes.size()); r::STObject native_meta(iterator, r::sfMetadata);
        r::TxMeta metadata(native_tx->getTransactionID(), header.sequence, native_meta);
        // As xrpld 3.4.0's insertAllSyntheticInJson: nftoken_id(s) and offer_id into the metadata.
        json::Value extra{json::ValueType::Object};
        r::insertNFTokenID(extra, native_tx, metadata); r::insertNFTokenOfferID(extra, native_tx, metadata);
        auto value = to_json(extra); if (value.is_object()) meta.update(value);
    }
    return result;
}
Json expanded(Stored const& s, Header const& header, unsigned version) { return expanded(s, decoded(s), header, version); }
// xrpld's accountFunds(..., IgnoreFreeze, IgnoreAuth): what an offer's owner holds of what it sells.
r::STAmount offer_funds(Context const& ctx, r::AccountID const& owner, r::STAmount const& gets) {
    if (gets.native()) {
        auto root = ctx.read(r::keylet::account(owner));
        if (!root) return r::STAmount{r::XRPAmount{0}};
        if (r::isPseudoAccount(root)) return root->getFieldAmount(r::sfBalance);  // xrpLiquid: no reserve
        r::STAmount reserve{account_reserve(ledger_fees(ctx), *root)};
        auto balance = root->getFieldAmount(r::sfBalance);
        return balance < reserve ? r::STAmount{r::XRPAmount{0}} : balance - reserve;
    }
    auto issue = gets.get<r::Issue>();
    r::STAmount balance{issue};
    if (auto line = ctx.read(r::keylet::trustLine(owner, issue.account, issue.currency))) {
        balance = line->getFieldAmount(r::sfBalance);
        if (owner > issue.account) balance.negate();
        balance.setIssue(issue);  // unclamped, as xrpld's accountFunds: a negative balance is shown
    }
    return balance;
}
// Whether `method` names its ledger as xrpld's ledgerFromRequest reads one (the deprecated
// `ledger` field among them). account_tx and nft_history read their ledgers themselves,
// ledger_closed takes none, and Clio's own methods read theirs as Clio does.
bool ledger_lookup(std::string const& method) {
    static std::set<std::string> const own{"account_tx", "nft_history", "ledger_closed", "nft_info", "nfts_by_issuer", "mpt_holders",
                                           "account_mptokens", "tx", "server_info", "ping", "random", "version", "ledger_index"};
    return !own.contains(method);
}
// The ledger a request names. With `lookup` (the methods xrpld answers through its
// ledgerFromRequest), also as xrpld does there: at most one of `ledger`, `ledger_hash` and
// `ledger_index`, and the deprecated `ledger` read as a hash (64 characters) or an index.
// account_tx and nft_history read their ledgers themselves and take no `ledger`.
// The account field checks xrpld 3.4.1's account handlers make before they look up the ledger
// (so a missing or non-string account is refused even when the ledger is not held):
// account_info and account_currencies take `account` or else `ident`; account_nfts also
// refuses a malformed address before the lookup. gateway_balances and account_tx check theirs
// after it, as xrpld does.
void check_account_field(std::string const& method, Json const& p) {
    static std::set<std::string> const by_account{"account_channels", "account_lines", "account_offers", "account_objects", "account_nfts"};
    auto string_field = [&](char const* field) {
        if (!p.at(field).is_string()) fail(r::RpcInvalidParams, std::string("Invalid field '") + field + "'.");
    };
    if (method == "account_info" || method == "account_currencies") {
        if (p.contains("account")) string_field("account");
        else if (p.contains("ident")) string_field("ident");
        else fail(r::RpcInvalidParams, "Missing field 'account'.");
    } else if (by_account.contains(method)) {
        if (!p.contains("account")) fail(r::RpcInvalidParams, "Missing field 'account'.");
        string_field("account");
        if (method == "account_nfts" && !r::parseBase58<r::AccountID>(p.at("account").get<std::string>())) fail(r::RpcActMalformed);
    }
}
// The ledger a request names, as xrpld 3.4.1's ledgerFromRequest reads it (RPCLedgerHelpers.cpp):
// `ledger` (deprecated: a 64-character string is a hash, anything else an index), `ledger_hash`
// or `ledger_index`, at most one of them; an index a string or an integer, read as its text
// ("validated", "closed", "current" or empty, or a number, a leading '+' allowed).
Header select(History const& store, Json const& params, Json const& range, bool lookup) {
    std::uint32_t seq = range.at("last");
    auto by_hash = [&](Json const& value, std::string const& field) {
        auto bad = [&] { fail(r::RpcInvalidParams, "Invalid field '" + field + "', not hex string."); };
        if (!value.is_string()) bad();
        auto text = value.get<std::string>();
        if (text.size() != 64 || !std::all_of(text.begin(), text.end(), [](unsigned char c) { return std::isxdigit(c); })) bad();
        seq = store.ledger_sequence(from_hex(text));
    };
    auto by_index = [&](Json const& value, std::string const& field) {
        auto bad = [&] { fail(r::RpcInvalidParams, "Invalid field '" + field + "', not string or number."); };
        if (!value.is_string() && !value.is_number_integer()) bad();
        auto text = value.is_string() ? value.get<std::string>() : value.dump();
        if (text.empty() || text == "current") fail(r::RpcLgrNotFound, "Current ledger is unavailable.");
        if (text == "validated" || text == "closed") return;
        auto digits = std::string_view(text).substr(text.front() == '+' ? 1 : 0);
        auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), seq);
        if (digits.empty() || ec != std::errc{} || end != digits.data() + digits.size()) bad();
    };
    bool legacy = lookup && params.contains("ledger");
    if (lookup && legacy + params.contains("ledger_hash") + params.contains("ledger_index") > 1)
        fail(r::RpcInvalidParams, legacy ? "Exactly one of 'ledger', 'ledger_hash', or 'ledger_index' can be specified."
                                         : "Exactly one of 'ledger_hash' or 'ledger_index' can be specified.");
    if (legacy) {
        auto const& value = params.at("ledger");
        if (!value.is_string() && !value.is_number_integer()) fail(r::RpcInvalidParams, "Invalid field 'ledger', not string or number.");
        if (value.is_string() && value.get<std::string>().size() == 64) by_hash(value, "ledger");
        else by_index(value, "ledger");
    } else if (params.contains("ledger_hash")) by_hash(params.at("ledger_hash"), "ledger_hash");
    else if (params.contains("ledger_index")) by_index(params.at("ledger_index"), "ledger_index");
    if (seq < range.at("first").get<std::uint32_t>() || seq > range.at("last").get<std::uint32_t>()) fail(r::RpcLgrNotFound, "ledgerNotFound");
    auto bytes = store.header(seq); if (bytes.empty()) fail(r::RpcLgrNotFound, "ledgerNotFound");
    return decode_header(bytes);
}
// The tx answer for a transaction xrpld lists (`listed`, its decoded form) in ledger `header`.
Json tx_response(Stored const& s, Json listed, Header const& header, bool binary, unsigned version, bool ledger_view) {
    auto h = r::deserializeHeader({header.raw.data(), header.raw.size()});
    auto decoded = expanded(s, std::move(listed), header, version);
    auto hash = upper(s.binary.at("hash").get<std::string>());
    Json result;
    if (binary) result = {{version == 1 ? "tx" : "tx_blob", upper(s.binary.at("tx_blob"))}, {version == 1 ? "meta" : "meta_blob", upper(s.binary.at("meta"))}, {"hash", hash}};
    else if (version == 1) { result = std::move(decoded.at("tx")); result["meta"] = decoded.at("meta"); }
    else {
        auto tx = std::move(decoded.at("tx")); tx.erase("hash");
        if (!ledger_view) { tx["ledger_index"] = header.sequence; tx["date"] = header.close_time; }
        result = {{"tx_json", std::move(tx)}, {"meta", decoded.at("meta")}, {"hash", hash},
            {"ledger_hash", upper(header.hash)}, {"close_time_iso", r::toStringIso(h.closeTime)}};
    }
    result["ledger_index"] = header.sequence; result["validated"] = true;
    // rippled reports the compact transaction ID (XLS-37, network 0) with every tx response.
    if (!ledger_view) if (auto compact = ctid(header.sequence, decoded.at("meta").at("TransactionIndex"), true); !compact.empty()) result["ctid"] = compact;
    if (version == 1) { result["date"] = h.closeTime.time_since_epoch().count(); result["inLedger"] = header.sequence; }
    else { result["ledger_hash"] = upper(header.hash); result["close_time_iso"] = r::toStringIso(h.closeTime); }
    return result;
}
// A value as xrpld's json::Value::asUInt reads it: nothing where that throws (a negative number,
// a string not a number, an object).
std::optional<std::uint32_t> as_uint(Json const& value) {
    if (value.is_null()) return 0;
    if (value.is_boolean()) return value.get<bool>() ? 1 : 0;
    if (value.is_number_unsigned()) { auto n = value.get<std::uint64_t>(); if (n <= UINT32_MAX) return static_cast<std::uint32_t>(n); return std::nullopt; }
    if (value.is_number_integer()) return std::nullopt;  // negative
    if (value.is_number_float()) { auto d = value.get<double>(); if (d >= 0 && d <= UINT32_MAX) return static_cast<std::uint32_t>(d); return std::nullopt; }
    if (value.is_string()) {
        auto const& text = value.get_ref<std::string const&>();
        std::uint32_t n = 0;
        auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), n);
        if (!text.empty() && ec == std::errc{} && end == text.data() + text.size()) return n;
    }
    return std::nullopt;
}
// xrpld's LedgerMaster::txnIdFromIndex: the ID of the transaction of ledger `sequence` whose
// metadata names `index`, its transactions read in hash order; one that cannot be read throws
// (an internal error, as from xrpld), and a ledger xrpld does not hold has none.
std::optional<Bytes> transaction_at(History const& store, Json const& range, std::uint32_t sequence, std::uint32_t index) {
    if (sequence < range.at("first").get<std::uint32_t>() || sequence > range.at("last").get<std::uint32_t>()) return std::nullopt;
    if (store.header(sequence).empty() || refused_ledger(store, sequence)) return std::nullopt;
    auto records = store.ledger_transaction_records(sequence);
    std::vector<std::pair<Bytes, std::size_t>> ids;
    for (std::size_t i = 0; i < records.size(); ++i) ids.emplace_back(transaction_id(records[i]), i);
    std::sort(ids.begin(), ids.end());
    for (auto const& [id, i] : ids) {
        auto const& record = records[i];
        r::SerialIter tx(record.transaction.data(), record.transaction.size());
        r::STTx parsed(tx);
        r::SerialIter meta(record.metadata.data(), record.metadata.size());
        r::STObject object(meta, r::sfMetadata);
        if (object.isFieldPresent(r::sfTransactionIndex) && object.getFieldU32(r::sfTransactionIndex) == index) return raw(parsed.getTransactionID());
    }
    return std::nullopt;
}
// xrpld 3.4.1's tx (Tx.cpp doTxJson): by `transaction` (a hash; anything else is notImpl) or by
// `ctid` (XLS-37, on this server's network), not both; `min_ledger` and `max_ledger` (at most 1000
// apart) make a transaction not found say whether every ledger between was searched: xrpld's
// count of the distinct ledgers its Transactions table has rows of there.
Json tx_method(History const& store, Json const& p, Json const& range, unsigned version) {
    if (p.contains("transaction") && p.contains("ctid")) fail(r::RpcInvalidParams);
    std::optional<Bytes> id;
    std::optional<std::pair<std::uint32_t, std::uint32_t>> compact;
    if (p.contains("transaction")) {
        r::uint256 parsed;
        if (!p.at("transaction").is_string() || !parsed.parseHex(p.at("transaction").get<std::string>())) fail(r::RpcNotImpl);
        id = raw(parsed);
    } else if (p.contains("ctid")) {
        auto const& value = p.at("ctid");
        auto text = value.is_string() ? value.get<std::string>() : std::string{};
        std::uint64_t number = 0;
        bool hex = text.size() == 16 && std::all_of(text.begin(), text.end(), [](unsigned char c) { return std::isxdigit(c); });
        if (hex) std::from_chars(text.data(), text.data() + text.size(), number, 16);
        if (!hex || (number & 0xF000'0000'0000'0000ULL) != 0xC000'0000'0000'0000ULL) fail(r::RpcInvalidParams);
        auto sequence = static_cast<std::uint32_t>((number >> 32) & 0x0FFF'FFFF);
        auto index = static_cast<std::uint32_t>((number >> 16) & 0xFFFF);
        auto network = static_cast<std::uint32_t>(number & 0xFFFF);
        if (network != network_id.load())
            fail(r::RpcWrongNetwork, "Wrong network. You should submit this request to a node running on NetworkID: " + std::to_string(network));
        compact = {sequence, index};
    } else fail(r::RpcInvalidParams);
    bool binary = loose_boolean(p, "binary");
    std::optional<std::pair<std::uint32_t, std::uint32_t>> span;
    if (p.contains("min_ledger") && p.contains("max_ledger")) {
        auto low = as_uint(p.at("min_ledger")), high = as_uint(p.at("max_ledger"));
        if (!low || !high) fail(r::RpcInvalidLgrRange);
        span = {*low, *high};
        if (span->second < span->first) fail(r::RpcInvalidLgrRange);
        if (span->second - span->first > 1000) fail(r::RpcExcessiveLgrRange);
    }
    if (compact) {
        id = transaction_at(store, range, compact->first, compact->second);
        if (!id) fail(r::RpcTxnNotFound);
        // xrpld then searches the ledger and index as if they were a range (Tx.cpp): a ledger
        // above its index, so nothing there counts as searched.
        if (span) span = {compact->first, compact->second};
    }
    auto s = stored(store, *id);
    if (!s || refused_ledger(store, s->binary.at("ledger_index").get<std::uint32_t>())) {
        if (!span) fail(r::RpcTxnNotFound);
        // Every ledger of the span must have rows there: be held, have transactions (a non-zero
        // transaction tree), and not be one xrpld refuses. That is a fact of each ledger, not of
        // the hash, so the transaction index's not holding the hash says nothing of it; but the
        // checks go cheapest first. The span is checked against the held range before any
        // header is read, then every header (a point read), and only then are ledgers before
        // kUnreadableBefore decoded for refused_ledger (each once a process), so a span with a
        // ledger without transactions is answered without decoding any.
        auto const held_first = range.at("first").get<std::uint32_t>(), held_last = range.at("last").get<std::uint32_t>();
        bool all = span->first <= span->second && span->first >= held_first && span->second <= held_last;
        for (auto seq = span->first; all; ++seq) {
            auto bytes = store.header(seq);
            all = !bytes.empty() && decode_header(bytes).transaction_hash.find_first_not_of('0') != std::string::npos;
            if (seq == span->second) break;
        }
        for (auto seq = span->first; all && seq < kUnreadableBefore; ++seq) {
            all = !refused_ledger(store, seq);
            if (seq == span->second) break;
        }
        fail_into(Json{{"searched_all", all}}, r::RpcTxnNotFound);
    }
    // A row xrpld holds but cannot read (a few of 2013): not found, nothing said of the search.
    auto listed = decoded_if_listed(*s);
    if (!listed) fail(r::RpcTxnNotFound);
    auto header = decode_header(store.header(s->binary.at("ledger_index")));
    return tx_response(*s, std::move(*listed), header, binary, version, false);
}
// An account's history page of the rows xrpld's tables hold: none of a ledger it does not hold
// (refused_ledger). Rows past those are read so the page still has `limit` rows, and the marker
// names the next held row.
Json held_history_page(History const& store, std::string const& column, Bytes const& key, std::uint32_t first, std::uint32_t last,
                       unsigned limit, bool forward, Json const& marker) {
    Json rows = Json::array(), next = nullptr;
    for (Json from = marker; next.is_null();) {
        auto want = static_cast<unsigned>(std::min<std::size_t>(limit + 1 - rows.size(), 1000));
        auto page = store.history_page(column, key, first, last, want, forward, from);
        for (auto& row : page.at("transactions")) {
            if (refused_ledger(store, row.at("ledger_index").get<std::uint32_t>())) continue;
            if (rows.size() == limit) { next = {{"ledger", row.at("ledger_index")}, {"seq", row.at("tx_index")}}; break; }
            rows.push_back(std::move(row));
        }
        if (!page.contains("marker")) break;
        from = page.at("marker");
    }
    Json result{{"transactions", std::move(rows)}, {"limit", limit}};
    if (!next.is_null()) result["marker"] = std::move(next);
    return result;
}
// account_tx's `delegate` (xrpld 3.4.1): transactions signed for another account under a
// delegation. As the "actor" the queried account owns the transaction and another signed it
// (`Delegate`); as the "authorizer" it signed on another's behalf; `counter_party` names the
// other side.
struct DelegateFilter { bool actor; std::optional<r::AccountID> counter_party; };
std::optional<DelegateFilter> delegate_filter(Json const& p) {
    if (!p.contains("delegate")) return std::nullopt;
    auto const& node = p.at("delegate");
    if (!node.is_object()) fail(r::RpcInvalidParams, "Invalid field 'delegate'.");
    auto const& kind = node.value("delegate_filter", Json());
    if (!kind.is_string() || (kind != "actor" && kind != "authorizer")) fail(r::RpcInvalidParams, "Invalid field 'delegate_filter'.");
    DelegateFilter filter{kind == "actor", std::nullopt};
    if (node.contains("counter_party")) {
        if (!node.at("counter_party").is_string()) fail(r::RpcInvalidParams, "Invalid field 'counter_party'.");
        filter.counter_party = r::parseBase58<r::AccountID>(node.at("counter_party").get<std::string>());
        if (!filter.counter_party) fail(r::RpcActMalformed);
    }
    return filter;
}
bool passes(DelegateFilter const& filter, Bytes const& blob, r::AccountID const& queried) {
    std::optional<r::STTx> parsed;
    try { r::SerialIter it(blob.data(), blob.size()); parsed.emplace(it); }
    catch (std::exception const&) { return false; }  // one libxrpl cannot read (2013) was signed by no delegate
    auto const& tx = *parsed;
    if (!tx.isFieldPresent(r::sfDelegate)) return false;
    auto owner = tx.getAccountID(r::sfAccount), signer = tx.getAccountID(r::sfDelegate);
    if (filter.actor) return owner == queried && signer != queried && (!filter.counter_party || signer == *filter.counter_party);
    return signer == queried && owner != queried && (!filter.counter_party || owner == *filter.counter_party);
}
// A delegate-filtered page as xrpld 3.4.1 reads one (Node.cpp's accountTxPage): the account's next
// `limit` + 1 transactions from the marker's own row on are read, filtered after reading, the
// marker's row skipped. The marker returned is the last transaction listed when another passing
// one follows, else the last one read when the read was full, so the next page goes on from there.
Json delegate_page(History const& store, Bytes const& account, DelegateFilter const& filter, std::uint32_t first, std::uint32_t last,
                   unsigned limit, bool forward, Json const& marker) {
    auto queried = r::AccountID::fromVoid(account.data());
    auto reading = static_cast<std::size_t>(limit) + 1;
    std::vector<Json> rows;
    for (Json from = marker; rows.size() < reading;) {
        auto page = held_history_page(store, "account_tx", account, first, last, static_cast<unsigned>(std::min<std::size_t>(reading - rows.size(), 1000)), forward, from);
        for (auto& row : page.at("transactions")) rows.push_back(std::move(row));
        if (!page.contains("marker")) break;
        from = page.at("marker");
    }
    std::optional<std::pair<Json, Json>> looking;  // the marker's row, until it is read
    if (marker.is_object()) looking = std::pair{marker.at("ledger"), marker.at("seq")};
    Json listed = Json::array(), next = nullptr, last_listed = nullptr, last_read = nullptr;
    unsigned room = limit;
    for (auto const& row : rows) {
        last_read = {{"ledger", row.at("ledger_index")}, {"seq", row.at("tx_index")}};
        if (looking) {
            if (row.at("ledger_index") == looking->first && row.at("tx_index") == looking->second) looking.reset();
            continue;
        }
        auto s = stored(store, hash(row.at("hash").get<std::string>()));
        if (!s || !passes(filter, s->record.transaction, queried)) continue;
        if (!room) { next = last_listed; break; }
        listed.push_back(row);
        --room;
        last_listed = {{"ledger", row.at("ledger_index")}, {"seq", row.at("tx_index")}};
    }
    if (next.is_null() && !looking && rows.size() == reading) next = last_read;
    Json result{{"transactions", std::move(listed)}, {"limit", limit}};
    if (!next.is_null()) { next["delegate"] = true; result["marker"] = std::move(next); }
    return result;
}
// The transactions of a history page as account_tx and nft_history list them. As xrpld's
// account_tx (`as_xrpld`): binary rows are its table's bytes, listed whether or not it can read
// them, and a row it cannot read fails the whole page (its convertBlobsToTxResult throws: an
// internal error). Clio's nft_history (NFTHistory.cpp) leaves out what libxrpl cannot decode,
// adds `date` to a binary row, and gives no ctid, inLedger or NFToken synthetic fields.
Json history_transactions(History const& store, Json const& rows, bool binary, unsigned version, bool as_xrpld) {
    Json transactions = Json::array();
    std::map<std::uint32_t, Header> headers;  // decoded once per ledger: a page often lists several of one
    for (auto const& entry : rows) {
        auto s = stored(store, hash(entry.at("hash").get<std::string>()));
        if (!s) continue;
        std::optional<Json> listed;
        if (binary) { if (!as_xrpld && !api_decodable(s->record)) continue; }
        else if (!(listed = decoded_if_listed(*s))) {
            if (as_xrpld && !xrpld_reads(s->record)) throw Error("account_tx: a transaction xrpld cannot read: " + entry.at("hash").get<std::string>());
            continue;
        }
        Json item{{"validated", true}};
        if (binary) {
            item["tx_blob"] = upper(s->binary.at("tx_blob")); item[version == 1 ? "meta" : "meta_blob"] = upper(s->binary.at("meta")); item["ledger_index"] = s->binary.at("ledger_index");
            if (!as_xrpld) {
                auto seq = s->binary.at("ledger_index").get<std::uint32_t>();
                auto known = headers.find(seq);
                if (known == headers.end()) known = headers.emplace(seq, decode_header(store.header(seq))).first;
                item["date"] = known->second.close_time;
            }
        }
        else {
            auto seq = s->binary.at("ledger_index").get<std::uint32_t>();
            auto known = headers.find(seq);
            if (known == headers.end()) known = headers.emplace(seq, decode_header(store.header(seq))).first;
            auto const& tx_header = known->second;
            auto tx = expanded(*s, std::move(*listed), tx_header, version, as_xrpld);
            auto value = std::move(tx.at("tx"));
            value["ledger_index"] = tx.at("ledger_index"); value["date"] = tx_header.close_time;
            if (as_xrpld) {
                auto compact_id = ctid(tx_header.sequence, tx.at("meta").at("TransactionIndex"), false, value); if (!compact_id.empty()) value["ctid"] = compact_id;
                if (version == 1) value["inLedger"] = tx_header.sequence;
            }
            if (version == 2) {
                item["ledger_index"] = tx_header.sequence;
                item["hash"] = value.at("hash"); value.erase("hash");
                item["ledger_hash"] = upper(tx_header.hash);
                auto native = r::deserializeHeader({tx_header.raw.data(), tx_header.raw.size()});
                item["close_time_iso"] = r::toStringIso(native.closeTime);
            }
            item[version == 1 ? "tx" : "tx_json"] = std::move(value); item["meta"] = std::move(tx.at("meta"));
        }
        transactions.push_back(std::move(item));
    }
    return transactions;
}
// Whether a marker field is one xrpld's account_tx takes: json::Value::isConvertibleTo(UInt).
bool uint_convertible(Json const& value) {
    if (value.is_null() || value.is_boolean()) return true;
    if (value.is_number_unsigned()) return value.get<std::uint64_t>() <= UINT32_MAX;
    if (value.is_number_integer()) return false;  // negative
    if (value.is_number_float()) { auto d = value.get<double>(); return d >= 0 && d <= UINT32_MAX && std::floor(d) == d; }
    return false;
}
// xrpld 3.4.1's account_tx (AccountTx.cpp, with Node.cpp's accountTxPage), for a public caller:
// - in API v2, `binary` and `forward` must be booleans (v1 reads any value as asBool); the limit is
//   readLimitField's (10-400, default 200), of which a JSON page reads at most 200 rows
//   (SQLiteDatabase's page length; a binary one 500), while `limit` answers as asked;
// - a range: a bound below 0 is the default; in v2 one outside the held ledgers (but 0 and -1) is
//   lgrIdxMalformed; an inverted range lgrIdxsInvalid in v1, invalidLgrRange in v2. A range and
//   a ledger together are refused in v2; in v1 the range is read. A named ledger is checked as
//   parseLedgerArgs checks it and looked up only after the marker and the delegate filter;
// - a marker of ledger 0 is none; otherwise the page is read from the marker's own row, past the
//   range's bound on its side (xrpld's SQL reads from the marker's ledger), and a marker naming
//   no row reads nothing (accountTxPage skips every row while it looks for it);
// - rows are those xrpld's tables hold: none of a ledger it does not hold (refused_ledger).
Json account_tx(History const& store, Json const& p, Json const& range, unsigned version) {
    std::uint32_t held_first = range.at("first"), held_last = range.at("last");
    if (version > 1)
        for (auto field : {"binary", "forward"})
            if (p.contains(field) && !p.at(field).is_boolean()) fail(r::RpcInvalidParams, std::string("Invalid field '") + field + "'.");
    auto limit = limit_field(p, 10, 200, 400);
    bool binary = loose_boolean(p, "binary"), forward = loose_boolean(p, "forward");
    auto key_text = text(p, "account");
    auto id = raw(account(key_text));
    bool ranged = p.contains("ledger_index_min") || p.contains("ledger_index_max");
    bool one_ledger = p.contains("ledger_hash") || p.contains("ledger_index");
    if (version > 1 && ranged && one_ledger) fail(r::RpcInvalidParams, "invalidParams");
    std::uint32_t low = 0, high = UINT32_MAX;
    // A named ledger, looked up later: a sequence, a hash, or the current (open) ledger.
    std::optional<std::uint32_t> named_seq;
    std::optional<Bytes> named_hash;
    bool named_current = false;
    if (ranged) {
        // xrpld 3.4.1's parseLedgerArgs: `asInt() >= 0 ? asUInt() : fallback`, with json::Value's
        // conversions: null is 0, a boolean 0 or 1, a number truncated, a string read as an
        // integer ('+' allowed). Whatever they throw on (an integer past INT_MAX, a negative
        // fraction, text that is no integer, an array) is xrpld's internal error.
        auto bound = [&](char const* name, std::uint32_t fallback) -> std::uint32_t {
            if (!p.contains(name)) return fallback;
            auto const& value = p.at(name);
            auto internal = [] { fail(r::RpcInternal); };
            if (value.is_null()) return 0;
            if (value.is_boolean()) return value.get<bool>() ? 1 : 0;
            if (value.is_number_integer()) {
                // json_reader stores up to INT_MAX as an Int (asInt reads it), above as a UInt
                // (asInt throws).
                if (value.is_number_unsigned() && value.get<std::uint64_t>() > INT32_MAX) internal();
                auto n = value.get<std::int64_t>();
                if (n < INT32_MIN) internal();
                return n < 0 ? fallback : static_cast<std::uint32_t>(n);
            }
            if (value.is_number_float()) {
                auto real = value.get<double>();
                if (real < INT32_MIN || real > INT32_MAX) internal();
                if (static_cast<std::int32_t>(real) < 0) return fallback;
                if (real < 0) internal();  // -0.5: asInt gives 0, asUInt refuses it
                return static_cast<std::uint32_t>(real);
            }
            if (value.is_string()) {
                auto text = value.get<std::string>();
                std::string_view digits(text);
                if (!digits.empty() && digits.front() == '+') digits.remove_prefix(1);
                std::int32_t n = 0;
                auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), n);
                if (ec != std::errc{} || end != digits.data() + digits.size()) internal();
                if (n < 0) return fallback;
                return static_cast<std::uint32_t>(n);
            }
            internal();
        };
        low = bound("ledger_index_min", 0); high = bound("ledger_index_max", UINT32_MAX);
    } else if (p.contains("ledger_hash")) {
        auto const& value = p.at("ledger_hash");
        if (!value.is_string()) fail(r::RpcInvalidParams, "ledgerHashNotString");
        r::uint256 parsed;
        if (!parsed.parseHex(value.get<std::string>())) fail(r::RpcInvalidParams, "ledgerHashMalformed");
        named_hash = raw(parsed);
    } else if (p.contains("ledger_index")) {
        auto const& value = p.at("ledger_index");
        if (value.is_boolean()) named_seq = value.get<bool>() ? 1 : 0;  // isNumeric there: asUInt
        else if (value.is_number()) {
            auto seq = as_uint(value);
            if (!seq) fail(r::RpcInternal);  // xrpld's asUInt throws on a negative index
            named_seq = *seq;
        } else if (value.is_array() || value.is_object()) fail(r::RpcInternal);  // its asString throws
        else {
            auto name = value.is_string() ? value.get<std::string>() : value.is_boolean() ? std::string(value.get<bool>() ? "true" : "false") : std::string{};
            if (name == "current" || name.empty()) named_current = true;
            else if (name == "closed" || name == "validated") named_seq = held_last;
            else fail(r::RpcInvalidParams, "ledger_index string malformed");
        }
    }
    std::optional<std::pair<std::uint32_t, std::uint32_t>> marker;
    if (p.contains("marker")) {
        auto const& token = p.at("marker");
        if (!token.is_object() || !token.contains("ledger") || !token.contains("seq") || !uint_convertible(token.at("ledger")) || !uint_convertible(token.at("seq")))
            fail(r::RpcInvalidParams, "invalid marker. Provide ledger index via ledger field, and transaction sequence number via seq field");
        marker = std::pair{*as_uint(token.at("ledger")), *as_uint(token.at("seq"))};
    }
    auto delegate = delegate_filter(p);
    if (marker) {
        // A delegate-filtered page's marker continues after the last row it read, a plain
        // page's at the next one: one must not be used for the other, as xrpld refuses.
        auto const& from = p.at("marker").value("delegate", Json());
        if ((from.is_boolean() && from.get<bool>()) != delegate.has_value())
            fail(r::RpcInvalidParams, "Do not mix delegate and non-delegate pagination markers in account_tx; "
                                      "repeat the same `delegate` object when using a delegate marker.");
    }
    // getLedgerRange.
    std::uint32_t first = held_first, last = held_last;
    if (ranged) {
        if (version > 1 && ((high > held_last && high != UINT32_MAX) || (low < held_first && low != 0))) fail(r::RpcLgrIdxMalformed);
        first = std::max(low, held_first); last = std::min(high, held_last);
        if (last < first) fail(version == 1 ? r::RpcLgrIdxsInvalid : r::RpcInvalidLgrRange);
    } else if (named_current) fail(r::RpcLgrNotValidated);  // the open ledger is not validated
    else if (named_seq || named_hash) {
        auto seq = named_hash ? store.ledger_sequence(*named_hash) : *named_seq;
        if (seq < held_first || seq > held_last || store.header(seq).empty() || refused_ledger(store, seq)) fail(r::RpcLgrNotFound, "ledgerNotFound");
        first = last = seq;
    }
    // The rows read: from the marker's row on, whatever the range's bound on its side.
    auto read_first = first, read_last = last;
    Json from = nullptr;
    bool unread = false;  // a marker no row can match: nothing is read
    if (marker && marker->first) {
        from = {{"ledger", marker->first}, {"seq", marker->second}};
        if (marker->first < held_first || marker->first > held_last) unread = true;
        else if (forward) { read_first = marker->first; read_last = std::max(last, marker->first); }
        else { read_last = marker->first; read_first = std::min(first, marker->first); }
    }
    auto reading = std::min<unsigned>(limit, binary ? 500 : 200);
    Json page{{"transactions", Json::array()}};
    if (!unread) {
        if (delegate) page = delegate_page(store, id, *delegate, read_first, read_last, reading, forward, from);
        else {
            page = held_history_page(store, "account_tx", id, read_first, read_last, reading, forward, from);
            auto const& rows = page.at("transactions");
            // The marker's own row must be there, or xrpld's walk skips every row looking for it.
            if (!from.is_null() && (rows.empty() || rows.front().at("ledger_index") != from.at("ledger") || rows.front().at("tx_index") != from.at("seq")))
                page = {{"transactions", Json::array()}};
        }
    }
    Json result{{"account", key_text}, {"ledger_index_min", first}, {"ledger_index_max", last}, {"limit", limit},
                {"transactions", history_transactions(store, page.at("transactions"), binary, version, true)}, {"validated", true}};
    if (page.contains("marker")) result["marker"] = page.at("marker");
    return result;
}
Json handle(History const& store, std::string const& method, Json const& p) {
    // The held ledgers, a baseline held whole served as the first (on mainnet, 32,570), from what
    // the backend keeps in memory where it can: only server_info shows the rest of status().
    auto served = store.served_ledgers();
    if (!served) fail(r::RpcNotReady);
    Json range{{"first", served->first}, {"last", served->second}};
    auto version = integer(p, "api_version", 1, 2);
    if (!version) fail(r::RpcInvalidParams, "Invalid API version.");
    if (method == "ledger_range") return {{"ledger_index_min", range.at("first")}, {"ledger_index_max", range.at("last")}};
    // Not a rippled method: hit rates and sizes of a piece cache's decode caches.
    if (method == "xrpla_cache_stats") {
        if (auto stats = store.cache_stats(); !stats.is_null()) return stats;  // a piece cache's, or its tail's base's
        fail(r::RpcNotSupported, "Only piece caches keep decode caches.");
    }
    if (method == "server_info") {
        auto h = decode_header(store.header(range.at("last")));
        auto info = store.status();
        auto state = info.at("state_ledgers");
        // As the served range: from a baseline held whole, and no further than the ledger this
        // answer names (a live tail may have advanced since served_ledgers()).
        if (state.at("first").get<std::uint32_t>() == served->first + 1 && store.baseline_ledger() == served->first) state["first"] = served->first;
        if (state.at("last").get<std::uint32_t>() > h.sequence) state["last"] = h.sequence;
        return {{"info", {{"build_version", "xrpla-" XRPLA_VERSION}, {"complete_ledgers", std::to_string(range.at("first").get<std::uint32_t>()) + "-" + std::to_string(h.sequence)},
            {"server_state", "full"}, {"network_id", network_id.load()}, {"validated_ledger", {{"hash", upper(h.hash)}, {"seq", h.sequence}}},
            {"xrpla", {{"state_ledgers", state}, {"verification", info.at("verification")}}}}}};
    }
    if (method == "tx") return tx_method(store, p, range, version);
    if (method == "ping") return Json::object();
    if (method == "random") return random_value();
    if (method == "version") {
        if (version == 1) return {{"version", {{"first", "1.0.0"}, {"good", "1.0.0"}, {"last", "1.0.0"}}}};
        return {{"version", {{"first", 1}, {"last", 2}}}};
    }
    if (method == "ledger_index") return ledger_index(store, p, range);
    if (method == "ledger_entry") check_ledger_entry(p);
    if (method == "noripple_check") check_noripple(p, version);
    check_more(method, p);
    check_account_field(method, p);
    // xrpld 3.4.1's LedgerHandler::check: its seven flags booleans if given, before the ledger
    // is looked up, refused with plain invalidParams.
    if (method == "ledger")
        for (auto flag : {"full", "transactions", "accounts", "expand", "binary", "owner_funds", "queue"})
            if (p.contains(flag) && !p.at(flag).is_boolean()) fail(r::RpcInvalidParams);
    auto own_ledger = method == "account_tx" || method == "nft_history" || method == "ledger_closed";
    auto h = own_ledger ? decode_header(store.header(range.at("last"))) : select(store, p, range, ledger_lookup(method));
    if (!own_ledger && held_as_xrpld(method) && refused_ledger(store, h.sequence)) fail(r::RpcLgrNotFound, "ledgerNotFound");
    Context context{store, p, h, version};
    auto result = context.base();
    if (method == "ledger_closed") { result.erase("validated"); return result; }
    if (method == "_ledger_event") {
        result.erase("validated"); result["ledger_time"] = h.close_time;
        result["txn_count"] = store.ledger_transactions(h.sequence).size();
        // As xrpld publishes it: the ledgers it held when this one was validated.
        result["validated_ledgers"] = std::to_string(range.at("first").get<std::uint32_t>()) + "-" + std::to_string(h.sequence);
        result["network_id"] = network_id.load();
        auto fees = ledger_fees(context);
        result["fee_base"] = fees.base; result["reserve_base"] = fees.reserve; result["reserve_inc"] = fees.increment;
        return result;
    }
    if (method == "_transaction_events") {
        // The transaction stream's messages for a validated ledger, in TransactionIndex order as
        // rippled publishes them, each with the accounts it affected (for account streams).
        auto ledger = store.ledger_record(h.sequence);
        auto indexes = index_transactions(ledger);  // by record position
        std::vector<std::size_t> order(indexes.size());
        for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
        std::sort(order.begin(), order.end(), [&](auto a, auto b) { return indexes[a].position < indexes[b].position; });
        auto native = r::deserializeHeader({h.raw.data(), h.raw.size()});
        Json events = Json::array();
        for (auto i : order) {
            auto const& index = indexes[i];
            auto s = std::optional<Stored>(stored(std::move(ledger.transactions[i]), h.sequence, index.hash));
            auto listed = decoded_if_listed(*s);
            if (!listed) continue;  // one xrpld cannot decode
            auto decoded = expanded(*s, std::move(*listed), h, version);
            auto tx = decoded.at("tx"), meta = decoded.at("meta");
            auto code = r::transCode(meta.at("TransactionResult").get<std::string>());
            if (!code) throw Error("Unknown transaction result " + meta.at("TransactionResult").dump());
            std::string token, human;
            r::transResultInfo(*code, token, human);
            tx["date"] = h.close_time;
            if (tx.value("TransactionType", "") == "OfferCreate") {
                // rippled's accountFunds(..., fhIGNORE_FREEZE) for an offer not funded by its issuer.
                auto const& blob = s->record.transaction;
                r::SerialIter it(blob.data(), blob.size());
                r::STTx offer(it);
                auto owner = offer.getAccountID(r::sfAccount);
                auto gets = offer.getFieldAmount(r::sfTakerGets);
                if (gets.native() || gets.getIssuer() != owner) tx["owner_funds"] = offer_funds(context, owner, gets).getText();
            }
            Json event{{"type", "transaction"}, {"status", "closed"}, {"validated", true}, {"ledger_index", h.sequence},
                {"ledger_hash", upper(h.hash)}, {"close_time_iso", r::toStringIso(native.closeTime)}, {"engine_result", token},
                {"engine_result_code", TERtoInt(*code)}, {"engine_result_message", human}, {"meta", meta}};
            if (auto compact = ctid(h.sequence, meta.at("TransactionIndex"), false, tx); !compact.empty()) event["ctid"] = compact;
            if (version == 1) event["transaction"] = tx;
            else { event["hash"] = tx.at("hash"); tx.erase("hash"); event["tx_json"] = tx; }
            Json affected = Json::array();
            for (auto const& id : index.accounts) affected.push_back(r::toBase58(r::AccountID::fromVoid(id.data())));
            // The books whose offers it changed, as xrpld's OrderBookDB::processTxn finds them: a
            // created offer's new fields, a modified one's previous fields, a deleted one's final
            // fields, keyed by (TakerGets, TakerPays). As in xrpld's pubValidatedTransaction, only
            // for a transaction that succeeded: a tec one's removed offers are not a book's news.
            std::set<std::string> books;
            if (*code == r::tesSUCCESS) for (auto const& wrapper : meta.at("AffectedNodes"))
                for (auto const& [kind, node] : wrapper.items()) {
                    if (node.value("LedgerEntryType", "") != "Offer") continue;
                    auto fields = kind == "ModifiedNode" ? "PreviousFields" : kind == "CreatedNode" ? "NewFields" : "FinalFields";
                    if (!node.contains(fields)) continue;
                    auto const& data = node.at(fields);
                    if (!data.contains("TakerGets") || !data.contains("TakerPays")) continue;
                    auto const& current = node.contains("FinalFields") ? node.at("FinalFields") : node.value("NewFields", Json::object());
                    try { books.insert(book_key(data.at("TakerGets"), data.at("TakerPays"), current.value("DomainID", ""))); }
                    catch (std::exception const&) {}
                }
            events.push_back({{"event", event}, {"accounts", affected}, {"books", books}});
        }
        return {{"events", events}};
    }
    if (method == "ledger") {
        // As xrpld for a client that is not admin: every state node (full, accounts) is refused,
        // and the queue exists only for the open ledger. Without a ledger named xrpld ignores them.
        bool named = p.contains("ledger") || p.contains("ledger_hash") || p.contains("ledger_index");
        if (named && (boolean(p, "full") || boolean(p, "accounts"))) fail(r::RpcNoPermission);
        if (named && boolean(p, "queue")) fail(r::RpcInvalidParams);
        auto ledger = header_json(h, boolean(p, "binary"), version);
        // owner_funds: an expanded OfferCreate not funded by its own issuer carries what its
        // account holds of TakerGets after the ledger (LedgerToJson's accountFunds, freeze and
        // authorization ignored), as the transaction stream's do.
        auto with_owner_funds = [&, owner_funds = boolean(p, "owner_funds")](Json& entry, TransactionRecord const& record) {
            if (!owner_funds) return;
            r::SerialIter it(record.transaction.data(), record.transaction.size());
            r::STTx tx(it);
            if (tx.getTxnType() != r::ttOFFER_CREATE) return;
            auto owner = tx.getAccountID(r::sfAccount);
            auto gets = tx.getFieldAmount(r::sfTakerGets);
            if (gets.native() || gets.getIssuer() != owner) entry["owner_funds"] = offer_funds(context, owner, gets).getText();
        };
        if (boolean(p, "transactions")) {
            Json transactions = Json::array();
            // rippled lists a ledger's transactions in transaction-tree (hash) order. They come
            // from the ledger's record, read once, not looked up one at a time by hash.
            auto records = store.ledger_transaction_records(h.sequence);
            std::vector<std::pair<Bytes, std::size_t>> ids;
            for (std::size_t i = 0; i < records.size(); ++i) ids.emplace_back(transaction_id(records[i]), i);
            std::sort(ids.begin(), ids.end());
            bool expand = boolean(p, "expand"), binary = boolean(p, "binary");
            for (auto const& [id, i] : ids) {
                auto s = stored(std::move(records[i]), h.sequence, id);
                // As xrpld's fillJsonTx: its walk over the ledger's transactions (hash order) reads
                // each, and the first it cannot read (a few of 2013) ends the listing, the
                // exception caught. One it reads but this build cannot render is left out.
                if (!xrpld_reads(s.record)) break;
                if (!expand || binary) {
                    // xrpld lists every transaction it reads, as its STTx and metadata write it
                    // back: for the few whose stored bytes are not that canonical form, the hash
                    // and blobs of the form it writes.
                    Bytes listed_id = id, tx_blob = from_hex(s.binary.at("tx_blob")), meta_blob = from_hex(s.binary.at("meta"));
                    if (!api_decodable(s.record)) {
                        r::SerialIter tx_bytes(s.record.transaction.data(), s.record.transaction.size());
                        r::STTx tx(tx_bytes);
                        r::SerialIter meta_bytes(s.record.metadata.data(), s.record.metadata.size());
                        r::STObject meta(meta_bytes, r::sfMetadata);
                        listed_id = raw(tx.getTransactionID());
                        auto written = [](r::STObject const& object) { auto serializer = object.getSerializer(); return Bytes(reinterpret_cast<char const*>(serializer.data()), serializer.size()); };
                        tx_blob = written(tx); meta_blob = written(meta);
                    }
                    if (!expand) { transactions.push_back(upper(to_hex(listed_id))); continue; }
                    Json entry{{"tx_blob", upper(to_hex(tx_blob))}, {version == 1 ? "meta" : "meta_blob", upper(to_hex(meta_blob))}};
                    if (version > 1) entry["hash"] = upper(to_hex(listed_id));
                    with_owner_funds(entry, s.record);
                    transactions.push_back(entry);
                    continue;
                }
                auto listed = decoded_if_listed(s);
                if (!listed) continue;
                Json entry;
                if (version == 1) {
                    auto tx = expanded(s, std::move(*listed), h, version); entry = std::move(tx.at("tx")); entry["metaData"] = std::move(tx.at("meta"));
                } else entry = tx_response(s, std::move(*listed), h, false, version, true);
                with_owner_funds(entry, s.record);
                transactions.push_back(std::move(entry));
            }
            ledger["transactions"] = transactions;
        }
        if (boolean(p, "diff")) {
            auto binary = boolean(p, "binary"); ledger["diff"] = Json::array();
            for (auto const& patch : store.ledger_record(h.sequence).patches)
                ledger["diff"].push_back({{"object_id", upper(to_hex(patch.key))}, {"object", patch.object.empty() || binary ? Json(upper(to_hex(patch.object))) : object_json(patch)}});
        }
        result["ledger"] = ledger; return result;
    }
    if (method == "ledger_data") {
        // rippled's ledger_data: pages of `limit` visited objects (2048 binary, 256 JSON at most);
        // the marker is the next unreturned key minus one, and `type` filters within the page.
        // Clio's out_of_order walks the same pages (this node reads any ledger's keys in order,
        // where Clio walks its newest ledger's and then each later ledger's deletions): its
        // marker is the last key returned, and none is left when the walk is done. An integer
        // marker D, as Clio pages its deletions, returns the objects ledger D deleted that exist
        // at the ledger asked about, then D - 1 while D is later.
        bool out_of_order = boolean(p, "out_of_order");
        if (p.contains("binary") && !p.at("binary").is_boolean()) fail(r::RpcInvalidParams, "Invalid field 'binary', not boolean.");
        bool binary = boolean(p, "binary");
        auto type_filter = entry_type(p);
        auto add = [&](Json& items, Bytes const& key, Bytes const& bytes) {
            // A serialized entry starts with its LedgerEntryType field (header 0x11, then two bytes).
            if (type_filter && (bytes.size() < 3 || ((static_cast<unsigned char>(bytes[1]) << 8) | static_cast<unsigned char>(bytes[2])) != *type_filter)) return;
            if (binary) items.push_back({{"data", upper(to_hex(bytes))}, {"index", upper(to_hex(key))}});
            else { auto entry = object_json({key, bytes}); entry["index"] = upper(to_hex(key)); items.push_back(entry); }
        };
        if (out_of_order && p.contains("marker") && p.at("marker").is_number_integer()) {
            auto deleted_in = integer(p, "marker", 0);
            if (deleted_in < range.at("first").get<std::uint32_t>() || deleted_in > range.at("last").get<std::uint32_t>()) fail(r::RpcLgrNotFound, "ledgerNotFound");
            std::vector<Bytes> keys;
            for (auto const& patch : store.ledger_record(deleted_in).patches) if (patch.object.empty()) keys.push_back(patch.key);
            store.prefetch(keys, h.sequence);
            Json items = Json::array();
            for (auto const& key : keys) if (auto bytes = store.object(key, h.sequence); !bytes.empty()) add(items, key, bytes);
            result["state"] = std::move(items);
            result["cache_full"] = true;
            if (deleted_in > h.sequence) result["marker"] = deleted_in - 1;
            return result;
        }
        r::uint256 after;
        if (p.contains("marker") && !(p.at("marker").is_string() && after.parseHex(p.at("marker").get<std::string>())))
            fail(r::RpcInvalidParams, out_of_order ? "Invalid field 'marker', not a key or a ledger index." : "Invalid field 'marker', not valid.");
        long long limit = -1, most = binary ? 2048 : 256;
        if (p.contains("limit")) {
            if (!p.at("limit").is_number_integer()) fail(r::RpcInvalidParams, "Invalid field 'limit', not integer.");
            limit = p.at("limit").get<long long>();
        }
        if (limit < 0 || limit > most) limit = most;
        auto page = store.object_page(h.sequence, p.contains("marker") ? to_hex(raw(after)) : "", static_cast<unsigned>(limit + 1), true);
        Json items = Json::array();
        auto const& state = page.at("state");
        for (std::size_t i = 0; i < state.size(); ++i) {
            auto index = from_hex(state[i].at("index").get<std::string>());
            auto key = r::uint256::fromVoid(index.data());
            if (static_cast<long long>(i) == limit) {
                if (out_of_order && i) key = r::uint256::fromVoid(from_hex(state[i - 1].at("index").get<std::string>()).data());
                else --key;
                result["marker"] = r::to_string(key);
                break;
            }
            add(items, index, from_hex(state[i].at("data").get<std::string>()));
        }
        if (!p.contains("marker")) result["ledger"] = header_json(h, binary, version);
        result["state"] = std::move(items);
        if (out_of_order) result["cache_full"] = true;
        return result;
    }
    if (method == "ledger_entry") return ledger_entry(context);
    if (method == "account_tx") return account_tx(store, p, range, version);
    if (method == "nft_history") {
        // Clio 2.8's nft_history (NFTHistory.cpp): the token's transactions in the range (or the
        // one ledger) asked for. A bound outside the held ledgers is refused, not narrowed (-1 is
        // none); the
        // limit is clamped to 1-100 (default 50) and answered only when asked. Clio's marker
        // follows a full page, whether or not more remain: going forward the last entry's
        // position plus one, read from inclusively; going back the last entry's, read before.
        auto key_text = text(p, "nft_id");
        auto id = hash(key_text);
        std::uint32_t held_first = range.at("first"), held_last = range.at("last");
        auto given = [&](char const* name) {
            if (!p.contains(name)) return false;
            auto const& value = p.at(name);
            if (!value.is_number_integer() || value.get<std::int64_t>() < INT32_MIN || value.get<std::int64_t>() > INT32_MAX) fail(r::RpcInvalidParams);
            return value.get<std::int64_t>() != -1;
        };
        auto bound = [&](char const* name, char const* error, std::uint32_t fallback) -> std::uint32_t {
            if (!given(name)) return fallback;
            auto seq = p.at(name).get<std::int64_t>();
            if (seq < held_first || seq > held_last) fail(r::RpcLgrIdxMalformed, error);
            return static_cast<std::uint32_t>(seq);
        };
        for (auto field : {"binary", "forward"})
            if (p.contains(field) && !p.at(field).is_boolean()) fail(r::RpcInvalidParams, std::string("Invalid field '") + field + "'.");
        std::optional<std::uint32_t> limit;
        if (p.contains("limit")) {
            auto const& value = p.at("limit");
            if (!value.is_number_unsigned() || value.get<std::uint64_t>() > UINT32_MAX || value.get<std::uint64_t>() < 1) fail(r::RpcInvalidParams);
            limit = static_cast<std::uint32_t>(std::min<std::uint64_t>(value.get<std::uint64_t>(), 100));
        }
        std::optional<std::pair<std::uint32_t, std::uint32_t>> marker;
        if (p.contains("marker")) {
            auto const& value = p.at("marker");
            if (!value.is_object()) fail(r::RpcInvalidParams, "invalidMarker");
            for (auto field : {"ledger", "seq"}) {
                if (!value.contains(field)) fail(r::RpcInvalidParams, std::string("Required field '") + field + "' missing");
                if (!value.at(field).is_number_unsigned() || value.at(field).get<std::uint64_t>() > UINT32_MAX) fail(r::RpcInvalidParams);
            }
            marker = std::pair{value.at("ledger").get<std::uint32_t>(), value.at("seq").get<std::uint32_t>()};
        }
        auto first = bound("ledger_index_min", "ledgerSeqMinOutOfRange", held_first), last = bound("ledger_index_max", "ledgerSeqMaxOutOfRange", held_last);
        if (first > last) fail(r::RpcLgrIdxsInvalid);
        if (p.contains("ledger_hash") || p.contains("ledger_index")) {
            if (given("ledger_index_min") || given("ledger_index_max")) fail(r::RpcInvalidParams, "containsLedgerSpecifierAndRange");
            first = last = select(store, p, range, false).sequence;
        }
        bool forward = p.contains("forward") && p.at("forward").get<bool>();
        unsigned count = limit.value_or(50);
        // The page's span and inclusive cursor in this node's terms. A marker outside the range
        // (one Clio does not give) reads what the held ledgers allow from it.
        std::uint32_t low = first, high = last;
        Json cursor = nullptr;
        if (marker && forward) {
            low = std::max(marker->first, held_first);
            if (low == marker->first) cursor = {{"ledger", marker->first}, {"seq", marker->second}};
        } else if (marker) {
            auto [ledger, seq] = *marker;
            bool none_before = ledger == 0 && seq == 0;
            if (seq == 0) { if (ledger) --ledger; seq = UINT32_MAX; } else --seq;
            high = none_before ? 0 : std::min(ledger, last);
            if (!none_before && high == ledger) cursor = {{"ledger", ledger}, {"seq", seq}};
        }
        Json rows = Json::array();
        if (low <= high && high >= held_first) rows = store.history_page("nft_tx", id, low, high, count, forward, cursor).at("transactions");
        result = {{"nft_id", key_text}, {"ledger_index_min", first}, {"ledger_index_max", last},
                  {"transactions", history_transactions(store, rows, boolean(p, "binary"), version, false)}, {"validated", true}};
        if (limit) result["limit"] = *limit;
        if (rows.size() == count) {
            auto const& end = rows.back();
            std::uint32_t seq = end.at("tx_index");
            result["marker"] = {{"ledger", end.at("ledger_index")}, {"seq", forward ? seq + 1 : seq}};
        }
        return result;
    }
    if (method == "nft_info") {
        if (!store.has_nft_state()) fail(r::RpcNotSupported, "nft_info needs NFT state: run `xrpla index-pieces` on this piece cache.");
        auto id = hash(text(p, "nft_id")); auto value = store.nft(id, h.sequence); if (value.is_null()) fail(r::RpcObjectNotFound, "NFT not found");
        auto token = r::uint256::fromVoid(id.data());
        value.update({{"nft_id", upper(to_hex(id))}, {"flags", r::nft::getFlags(token)}, {"transfer_fee", r::nft::getTransferFee(token)},
            {"issuer", r::toBase58(r::nft::getIssuer(token))}, {"nft_taxon", r::nft::toUInt32(r::nft::getTaxon(token))}, {"nft_serial", r::nft::getSequence(token)}, {"validated", true}});
        return value;
    }
    if (clio_methods().contains(method)) return clio(context, method);
    if (is_owner_method(method)) return owner_methods(context, method);
    if (method.starts_with("account_") || method == "gateway_balances") return accounts(context, method);
    if (method == "book_offers") return books(context);
    if (more_methods().contains(method)) return more(context, method);
    fail(r::RpcUnknownCommand);
}
}
bool read_by_xrpld(TransactionRecord const& record) { return xrpld_reads(record); }
bool ledger_held_by_xrpld(std::vector<TransactionRecord> const& transactions) {
    return std::all_of(transactions.begin(), transactions.end(), [](auto const& record) { return xrpld_reads(record) || pre_mpt_amount(record); });
}
}
namespace xrpla::core {
namespace {
std::mutex live_mutex;
std::shared_ptr<History const> live;
// A store is opened once and kept between requests, as a piece cache is (open_piece_history):
// opening RocksDB for each request cost about 8 ms, most of a request's time. Another process
// may be appending to the store, and a read-only open sees only what was written before it,
// so the store is opened again once two seconds old; its blocks stay in the process's shared
// block cache (store.cpp) across the reopen. The reopen runs outside the lock, and other
// requests use the open store meanwhile; a reopen that fails keeps it, and is tried again.
std::shared_ptr<Store const> open_store(fs::path const& dir) {
    struct Entry { std::chrono::steady_clock::time_point opened; std::shared_ptr<Store const> store; bool opening = false; };
    static std::mutex mutex;
    static std::map<std::string, Entry> open;  // by the directory as given: no lookup per request
    auto now = std::chrono::steady_clock::now();
    std::unique_lock lock(mutex);
    auto& entry = open[dir.string()];
    if (entry.store && (entry.opening || now - entry.opened <= std::chrono::seconds(2))) return entry.store;
    entry.opening = true;
    auto current = entry.store;
    lock.unlock();
    std::shared_ptr<Store const> fresh;
    try { fresh = std::make_shared<Store const>(dir / "store", false); }
    catch (...) {
        lock.lock(); entry.opening = false;
        if (current) return current;
        throw;
    }
    lock.lock();
    entry = {now, fresh, false};
    return fresh;
}
// Logs an exception a request raised (rpc_request), which a client can trigger at will: at most
// one line a second across all kinds, which counts the ones left unlogged since the last.
void log_request_exception(std::string const& method, std::string_view what, std::string_view cause) {
    static std::atomic<std::int64_t> next{0};  // steady_clock milliseconds before which none is logged
    static std::atomic<std::uint64_t> skipped{0};
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    auto due = next.load();
    if (now < due || !next.compare_exchange_strong(due, now + 1000)) { ++skipped; return; }
    auto unlogged = skipped.exchange(0);
    std::cerr << "rpc " << method << ": " << what << cause;
    if (unlogged) std::cerr << " (" << unlogged << " more since the last logged)";
    std::cerr << std::endl;
}
// Whether `dir` is a piece cache, found once per thread: a directory does not change kind while
// it is served, and the check cost each request a filesystem lookup.
bool serves_pieces(fs::path const& dir) {
    thread_local std::string known;  // the last directory found to be one
    if (!known.empty() && known == dir.native()) return true;
    if (!is_piece_cache(dir)) return false;
    known = dir.native();
    return true;
}
}
void configure_network(std::uint32_t id) { rpc::network_id = id; }
void use_live_history(std::shared_ptr<History const> history) { std::lock_guard lock(live_mutex); live = std::move(history); }
std::shared_ptr<History const> live_history() { std::lock_guard lock(live_mutex); return live; }
bool forwarded_request(std::string const& method, Json const& params) {
    // As Clio forwards them: transactions, fees, paths and what only a node knows now. Not
    // "batch": xrpld reads that method name as a JSON-RPC batch, every element a request of its
    // own (any method) run with the role it gives this server.
    static std::set<std::string> const forwarded{"submit", "submit_multisigned", "fee", "path_find", "ripple_path_find", "ledger_current", "server_state",
        "server_definitions", "manifest", "channel_authorize", "channel_verify", "simulate"};
    // The public methods this node answers from a ledger: asked of the open ("current") ledger,
    // which only a node has, they go upstream. Nothing else does, whatever its ledger: an admin
    // command or an unknown name sent upstream would run there with this server's address
    // (often an admin one, the upstream's localhost port).
    static std::set<std::string> const ledger_readers{"account_info", "account_lines", "account_offers", "account_channels", "account_currencies",
        "account_nfts", "account_objects", "gateway_balances", "noripple_check", "book_offers", "book_changes", "deposit_authorized",
        "nft_buy_offers", "nft_sell_offers", "amm_info", "transaction_entry", "ledger", "ledger_data", "ledger_entry", "get_aggregate_price",
        "vault_info", "account_mptokens", "account_mptoken_issuances", "mpt_holders", "nfts_by_issuer", "nft_info"};
    if (forwarded.contains(method)) return true;
    if (!params.is_object() || !ledger_readers.contains(method)) return false;
    if (params.value("ledger_index", Json()) == "current") return true;
    // ledgerFromIndex (RPCLedgerHelpers.cpp) reads an empty index as "current" too, in the legacy
    // `ledger` and in `ledger_index` alike, when it is the only ledger named.
    if (!rpc::ledger_lookup(method)) return false;
    if (params.value("ledger_index", Json()) == "" && !params.contains("ledger_hash") && !params.contains("ledger")) return true;
    auto const& legacy = params.value("ledger", Json());
    return (legacy == "current" || legacy == "") && !params.contains("ledger_hash") && !params.contains("ledger_index");
}
// A forwarded request as a public client may make it, whatever role the upstream gives this
// server (xrpld grants admin by address, and its localhost ports are admin ones): what xrpld 3.4.1
// allows only an admin is answered as it answers anyone else. No signing for the client
// (submit without tx_blob, channel_authorize: a public xrpld has signing_support off), no whole
// state of a ledger (full, accounts), and limits above the public ranges lowered to their
// maximum (readLimitField, Tuning.h; ledger_data's page lengths). A limit at or below the
// maximum goes as sent: the upstream refuses 0 and negative ones itself, and raises small ones
// to the minimum for a public client. What only the client's own role or envelope uses goes
// no further: admin_user and admin_password (xrpld reads its role from them), the keys and
// secrets signing would take (they would only reach another operator's logs; simulate refuses
// them here, as xrpld does), and the JSON-RPC envelope (jsonrpc, ripplerpc, id; this server
// shapes its own answer from them, and the upstream's ripplerpc 2.0 answer is not the one it
// reads).
Json public_request(std::string const& method, Json params) {
    if ((method == "submit" && !params.contains("tx_blob")) || method == "channel_authorize")
        rpc::fail(::xrpl::RpcNotSupported, "Signing is not supported by this server.");
    if (method == "simulate") {
        // doSimulate's first checks, in its order: binary a boolean if given, then none of the
        // four keys, the first given named. Its NeedsCurrentLedger condition (noNetwork, noCurrent)
        // comes before them in xrpld, and only the upstream knows it: the keys never leave here.
        if (params.contains("binary") && !params.at("binary").is_boolean()) rpc::fail(::xrpl::RpcInvalidParams, "Invalid field 'binary'.");
        for (std::string name : {"secret", "seed", "seed_hex", "passphrase"})
            if (params.contains(name)) rpc::fail(::xrpl::RpcInvalidParams, "Invalid field '" + name + "'.");
    }
    for (auto name : {"admin_user", "admin_password", "key_type", "jsonrpc", "ripplerpc", "id", "secret", "seed", "seed_hex", "passphrase"}) params.erase(name);
    if (method == "ledger") {
        // LedgerHandler::check's order: its seven flags booleans if given, then no whole state.
        for (auto flag : {"full", "transactions", "accounts", "expand", "binary", "owner_funds", "queue"})
            if (params.contains(flag) && !params.at(flag).is_boolean()) rpc::fail(::xrpl::RpcInvalidParams);
        for (auto flag : {"full", "accounts"})
            if (params.value(flag, false)) rpc::fail(::xrpl::RpcNoPermission);
    }
    static std::map<std::string, std::int64_t> const most{
        {"account_lines", 400}, {"account_channels", 400}, {"account_objects", 400}, {"account_offers", 400}, {"book_offers", 100},
        {"noripple_check", 400}, {"account_nfts", 400}, {"nft_buy_offers", 500}, {"nft_sell_offers", 500}};
    if (auto it = params.find("limit"); it != params.end() && it->is_number_integer()) {
        if (auto range = most.find(method); range != most.end())
            *it = std::min(it->get<std::int64_t>(), range->second);
        else if (method == "ledger_data")
            *it = std::min<std::int64_t>(it->get<std::int64_t>(), params.value("binary", false) == true ? 2048 : 256);
    }
    return params;
}
Json rpc::masked(Json request) {
    if (request.is_object())
        for (auto name : {"passphrase", "secret", "seed", "seed_hex"}) if (request.contains(name)) request[name] = "<masked>";
    return request;
}
Json rpc_request(fs::path const& dir, std::string const& method, Json const& params, std::string const& forward, std::string const& client,
                 std::string const& resolve) {
    Json result;
    try {
        if (!params.is_object()) rpc::fail(::xrpl::RpcInvalidParams);
        // A path search streams its updates, which only a WebSocket carries; xrpld gives this
        // answer elsewhere. A WebSocket session relays it to the upstream's WebSocket (server.cpp).
        if (method == "path_find") rpc::fail(::xrpl::RpcNoEvents);
        if (forwarded_request(method, params)) {
            if (forward.empty()) rpc::fail(::xrpl::RpcNotSupported, "An upstream server is required for this request.");
            // Not retried here: a submission must not be repeated behind the client's back (libcurl
            // itself resends a request whose kept connection died before any answer, as any HTTP
            // client does), and the client sees the upstream's own answer (or notReady when it
            // cannot be reached).
            RpcClient upstream{forward, {}, 0};
            upstream.attempts = 1; upstream.forwarded_for = client; upstream.resolve = resolve;
            auto request = public_request(method, params);
            try { result = upstream.call(method, request); }
            catch (std::exception const& error) {
                log_request_exception(method, "upstream: ", error.what());  // each client request may meet it
                rpc::fail(::xrpl::RpcNotReady, "The upstream server did not answer.");
            }
        } else if (auto live = live_history()) {
            result = rpc::handle(*live, method, params);
        } else if (serves_pieces(dir)) {
            result = rpc::handle(*open_piece_history(dir), method, params);
        } else {
            result = rpc::handle(*open_store(dir), method, params);
        }
        if (!result.contains("error")) result["status"] = "success";
    } catch (rpc::Failure const& failure) { result = failure.value; }
      catch (Json::exception const& error) {
        // Mostly a parameter of the wrong type, which handlers read as it comes and leave to
        // throw: invalidParams, as xrpld answers. A handler reading stored data that is not as
        // expected (a missing metadata field, a malformed row) throws the same exceptions, and
        // their messages do not tell the two apart, so each goes to the operator's log with its
        // method, at most one line a second (a client's bad parameters are no fault of the
        // server, and must not flood it); the client's answer is unchanged.
        log_request_exception(method, "invalidParams from a JSON exception: ", error.what());
        result = Json::parse(::xrpl::rpc::makeError(::xrpl::RpcInvalidParams).toStyledString());
      }
      catch (std::exception const& error) {
        auto message = std::string(error.what());
        auto code = message.find("pruned") != std::string::npos ? ::xrpl::RpcLgrNotFound : ::xrpl::RpcInternal;
        // Clients get rippled's bare internal error; the operator's log gets the cause, rate
        // limited as above (a client can ask for an unreadable transaction again and again).
        if (code == ::xrpl::RpcInternal) log_request_exception(method, "", message);
        result = Json::parse(::xrpl::rpc::makeError(code).toStyledString());
      }
    if (result.contains("error")) {
        // Like rippled, an error echoes the request.
        result["status"] = "error";
        auto request = params.is_object() ? params : Json::object(); request["command"] = method; result["request"] = rpc::masked(std::move(request));
    }
    return result;
}
}
