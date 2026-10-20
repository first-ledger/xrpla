// Methods Clio 2.7 answers beyond xrpld's read surface, and xrpld methods Clio answers itself:
// feature (at any ledger, as Clio), get_aggregate_price and vault_info (as xrpld 3.3.0),
// account_mptokens and account_mptoken_issuances (Clio 2.8's), mpt_holders and nfts_by_issuer
// (Clio's, from the optional indexes), and random.
#include "rpc.hpp"
#include <xrpl/basics/Number.h>
#include <xrpl/protocol/Feature.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/protocol/Protocol.h>
#include <xrpl/protocol/STAmount.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STCurrency.h>
#include <xrpl/protocol/STInteger.h>
#include <xrpl/protocol/UintTypes.h>
#include <xrpl/protocol/digest.h>
#include <xrpl/protocol/nft.h>
#include <sodium.h>
#include <algorithm>
#include <charconv>
#include <functional>
#include <map>
#include <numeric>
#include <set>

namespace xrpla::core::rpc {
namespace {
// Every amendment libxrpl knows, as Clio lists them: enabled when the ledger's Amendments
// object holds it, supported when this build's libxrpl implements it. `majority` (xrpld's) is
// the close time at which an amendment not yet enabled gained a majority.
Json feature(Context const& ctx) {
    auto const& p = ctx.params;
    if (p.contains("vetoed")) fail(r::RpcNoPermission, "The admin portion of feature API is not available through this archive API.");
    std::optional<std::string> wanted;
    if (p.contains("feature")) {
        if (!p.at("feature").is_string()) fail(r::RpcInvalidParams);
        wanted = p.at("feature").get<std::string>();
    }
    std::set<r::uint256> enabled;
    std::map<r::uint256, std::uint32_t> majorities;
    if (auto object = ctx.read(r::keylet::amendments())) {
        if (object->isFieldPresent(r::sfAmendments)) for (auto const& id : object->getFieldV256(r::sfAmendments)) enabled.insert(id);
        if (object->isFieldPresent(r::sfMajorities))
            for (auto const& majority : object->getFieldArray(r::sfMajorities)) majorities[majority.getFieldH256(r::sfAmendment)] = majority.getFieldU32(r::sfCloseTime);
    }
    Json features = Json::object();
    for (auto const& [name, support] : r::allAmendments()) {
        auto id = r::getRegisteredFeature(name).value_or(r::sha512Half(r::Slice(name.data(), name.size())));
        auto key = r::to_string(id);
        if (wanted && *wanted != name && upper(*wanted) != key) continue;
        Json entry{{"name", name}, {"enabled", enabled.contains(id)}, {"supported", support != r::AmendmentSupport::Unsupported}};
        if (auto m = majorities.find(id); m != majorities.end()) entry["majority"] = m->second;
        features[key] = entry;
    }
    if (features.empty()) fail(r::RpcBadFeature);
    auto result = ctx.base();
    if (wanted) result.update(features);
    else result["features"] = features;
    return result;
}

// xrpld's get_aggregate_price: each oracle's price for the pair, from its newest version that
// has one (the oracle object, then up to three earlier versions through its transactions'
// metadata, as far back as this node's history reaches), and their statistics.
void price_versions(Context const& ctx, std::shared_ptr<r::SLE const> const& sle, std::function<bool(r::STObject const&)> const& take) {
    constexpr int most = 3;
    bool is_new = false;
    int history = 0;
    // `oracle` holds a PriceDataSeries (the object, then a node's new or final fields); `chain`
    // its PreviousTxnID and PreviousTxnLgrSeq (the object, then the node).
    r::STObject const* oracle = sle.get();
    r::STObject const* chain = oracle;
    r::STObject const* previous_chain = nullptr;
    std::shared_ptr<r::STObject> meta;
    for (;;) {
        if (previous_chain == chain) return;  // the metadata held no oracle node
        if (!oracle || take(*oracle) || is_new) return;
        if (++history > most) return;
        auto previous_tx = chain->getFieldH256(r::sfPreviousTxnID);
        auto previous_ledger = chain->getFieldU32(r::sfPreviousTxnLgrSeq);
        auto tx = ctx.store.transaction(raw(previous_tx), true);
        if (tx.is_null() || tx.at("ledger_index") != previous_ledger) return;
        auto bytes = from_hex(tx.at("meta").get<std::string>());
        r::SerialIter it(bytes.data(), bytes.size());
        auto next = std::make_shared<r::STObject>(it, r::sfMetadata);
        previous_chain = chain;
        for (auto const& node : next->getFieldArray(r::sfAffectedNodes)) {
            if (node.getFieldU16(r::sfLedgerEntryType) != r::ltORACLE) continue;
            chain = &node;
            is_new = node.isFieldPresent(r::sfNewFields);
            // The transaction that created the object: no earlier data.
            if (is_new && history == 1) return;
            oracle = &static_cast<r::STObject const&>(node.peekAtField(is_new ? r::sfNewFields : r::sfFinalFields));
            break;
        }
        meta = std::move(next);
    }
}
std::tuple<r::STAmount, r::Number, std::uint16_t> price_stats(std::vector<r::STAmount>::const_iterator begin, std::vector<r::STAmount>::const_iterator end) {
    r::STAmount mean{r::noIssue(), 0, 0};
    r::Number deviation{0};
    auto size = static_cast<std::uint16_t>(std::distance(begin, end));
    mean = std::accumulate(begin, end, mean, [](r::STAmount const& sum, r::STAmount const& price) { return sum + price; });
    mean = r::divide(mean, r::STAmount{r::noIssue(), size, 0}, r::noIssue());
    if (size > 1) {
        deviation = std::accumulate(begin, end, deviation, [&](r::Number const& sum, r::STAmount const& price) {
            return sum + r::Number(price - mean) * r::Number(price - mean);
        });
        deviation = r::root2(deviation / (size - 1));
    }
    return {mean, deviation, size};
}
Json aggregate_price(Context const& ctx) {
    auto const& p = ctx.params;
    if (!p.contains("oracles")) fail(r::RpcInvalidParams, "Missing field 'oracles'.");
    auto const& oracles = p.at("oracles");
    if (!oracles.is_array() || oracles.empty() || oracles.size() > 200) fail(r::RpcOracleMalformed);
    if (!p.contains("base_asset")) fail(r::RpcInvalidParams, "Missing field 'base_asset'.");
    if (!p.contains("quote_asset")) fail(r::RpcInvalidParams, "Missing field 'quote_asset'.");
    // A nonnegative integer, or one written as a decimal string.
    auto whole = [](Json const& v) -> std::optional<std::uint32_t> {
        if (v.is_number_unsigned() && v.get<std::uint64_t>() <= UINT32_MAX) return static_cast<std::uint32_t>(v.get<std::uint64_t>());
        if (v.is_number_integer() && v.get<std::int64_t>() >= 0 && v.get<std::int64_t>() <= UINT32_MAX) return static_cast<std::uint32_t>(v.get<std::int64_t>());
        if (v.is_string()) {
            auto s = v.get<std::string>(); std::uint32_t n = 0;
            auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
            if (!s.empty() && ec == std::errc{} && end == s.data() + s.size()) return n;
        }
        return std::nullopt;
    };
    std::uint32_t trim = 0, threshold = 0;
    if (p.contains("trim")) {
        auto value = whole(p.at("trim"));
        if (!value || !*value || *value > r::kMaxTrim) fail(r::RpcInvalidParams);
        trim = *value;
    }
    if (p.contains("time_threshold")) {
        auto value = whole(p.at("time_threshold"));
        if (!value) fail(r::RpcInvalidParams);
        threshold = *value;
    }
    auto currency = [&](char const* field) {
        auto const& v = p.at(field);
        r::Currency c;
        if (!v.is_string() || v.get<std::string>().empty() || !r::toCurrency(c, v.get<std::string>()) || c == r::badCurrency()) fail(r::RpcInvalidParams);
        return v.get<std::string>();
    };
    auto base = currency("base_asset"), quote = currency("quote_asset");
    std::vector<std::pair<std::uint32_t, r::STAmount>> prices;  // (LastUpdateTime, price)
    std::set<std::pair<r::AccountID, std::uint32_t>> seen;  // xrpld 3.4.0 counts each oracle once
    for (auto const& oracle : oracles) {
        if (!oracle.is_object() || !oracle.contains("oracle_document_id") || !oracle.contains("account")) fail_into(ctx.base(), r::RpcOracleMalformed);
        auto document = whole(oracle.at("oracle_document_id"));
        auto owner = oracle.at("account").is_string() ? r::parseBase58<r::AccountID>(oracle.at("account").get<std::string>()) : std::nullopt;
        if (!owner || owner->isZero() || !document) fail_into(ctx.base(), r::RpcInvalidParams);
        if (!seen.emplace(*owner, *document).second) continue;
        price_versions(ctx, ctx.read(r::keylet::oracle(*owner, *document)), [&](r::STObject const& node) {
            for (auto const& entry : node.getFieldArray(r::sfPriceDataSeries)) {
                if (entry.getFieldCurrency(r::sfBaseAsset).getText() != base || entry.getFieldCurrency(r::sfQuoteAsset).getText() != quote ||
                    !entry.isFieldPresent(r::sfAssetPrice)) continue;
                auto scale = entry.isFieldPresent(r::sfScale) ? -static_cast<int>(entry.getFieldU8(r::sfScale)) : 0;
                prices.emplace_back(node.getFieldU32(r::sfLastUpdateTime), r::STAmount{r::noIssue(), entry.getFieldU64(r::sfAssetPrice), scale});
                return true;
            }
            return false;
        });
    }
    if (prices.empty()) fail_into(ctx.base(), r::RpcObjectNotFound);
    // Prices older than time_threshold before the newest are left out.
    auto latest = std::max_element(prices.begin(), prices.end(), [](auto const& a, auto const& b) { return a.first < b.first; })->first;
    if (threshold) {
        auto oldest = std::min_element(prices.begin(), prices.end(), [](auto const& a, auto const& b) { return a.first < b.first; })->first;
        auto bound = latest > threshold ? latest - threshold : oldest;
        if (bound > oldest) std::erase_if(prices, [&](auto const& price) { return price.first < bound; });
    }
    std::vector<r::STAmount> sorted;
    for (auto const& [time, price] : prices) sorted.push_back(price);
    std::sort(sorted.begin(), sorted.end());
    auto result = ctx.base();
    result["time"] = latest;
    auto [mean, deviation, size] = price_stats(sorted.begin(), sorted.end());
    result["entire_set"] = {{"mean", mean.getText()}, {"size", size}, {"standard_deviation", r::to_string(deviation)}};
    auto middle = size / 2;
    auto median = size % 2 ? sorted[middle] : r::divide(sorted[middle - 1] + sorted[middle], r::STAmount{r::noIssue(), 2, 0}, r::noIssue());
    result["median"] = median.getText();
    if (trim) {
        auto cut = static_cast<std::ptrdiff_t>(sorted.size() * trim / 100);
        auto [trimmed_mean, trimmed_deviation, trimmed_size] = price_stats(sorted.begin() + cut, sorted.end() - cut);
        result["trimmed_set"] = {{"mean", trimmed_mean.getText()}, {"size", trimmed_size}, {"standard_deviation", r::to_string(trimmed_deviation)}};
    }
    return result;
}

// UInt64 amounts as base-10 strings, as xrpld's STUInt64::getJson writes them.
Json uint64_json(r::SField const& field, std::uint64_t value) { return to_json(r::STUInt64{field, value}.getJson(r::JsonOptions::Values::None)); }
// The fields Clio 2.8 adds to an MPToken beside its account, issuance and amount, in
// account_mptokens and mpt_holders alike: the locked amount and the confidential balances.
void mptoken_extras(Json& token, r::SLE const& sle) {
    auto hex_of = [&](r::SField const& field, char const* name) {
        if (sle.isFieldPresent(field)) token[name] = r::strHex(sle.getFieldVL(field));
    };
    if (sle.isFieldPresent(r::sfLockedAmount)) token["locked_amount"] = uint64_json(r::sfLockedAmount, sle.getFieldU64(r::sfLockedAmount));
    hex_of(r::sfConfidentialBalanceInbox, "confidential_balance_inbox");
    hex_of(r::sfConfidentialBalanceSpending, "confidential_balance_spending");
    if (sle.isFieldPresent(r::sfConfidentialBalanceVersion)) token["confidential_balance_version"] = sle.getFieldU32(r::sfConfidentialBalanceVersion);
    hex_of(r::sfIssuerEncryptedBalance, "issuer_encrypted_balance");
    hex_of(r::sfAuditorEncryptedBalance, "auditor_encrypted_balance");
    hex_of(r::sfHolderEncryptionKey, "holder_encryption_key");
}
// Clio's account_mptokens and account_mptoken_issuances, as Clio 2.8 reports them (the
// protocol definitions of libxrpl 3.3: immutable flags, confidential balances): the account's
// MPTokens, or the issuances it created, from its owner directory (limit 10-400, default 200,
// counting every entry visited; the marker names the last one visited and its page).
Json account_mpts(Context const& ctx, bool issuances) {
    auto const& p = ctx.params;
    auto address = text(p, "account");
    auto owner = account(address);
    // Clio's Min(1): zero is refused with its bare invalidParams (xrpld's own limit error differs).
    if (p.contains("limit") && p.at("limit").is_number_integer() && p.at("limit").get<std::int64_t>() == 0) fail(r::RpcInvalidParams);
    auto limit = limit_field(p, 10, 200, 400);
    std::string marker;
    if (p.contains("marker")) marker = text(p, "marker");
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    Json list = Json::array();
    auto amount = uint64_json;
    auto flags_of = [](Json& item, std::uint32_t flags, std::initializer_list<std::pair<std::uint32_t, char const*>> names) {
        for (auto const& [mask, name] : names) if (flags & mask) item[name] = true;
    };
    auto hex_of = [](Json& item, r::SLE const& sle, r::SField const& field, char const* name) {
        if (sle.isFieldPresent(field)) item[name] = r::strHex(sle.getFieldVL(field));
    };
    auto next = owned(ctx, owner, marker, limit, [&](r::SLE const& sle) {
        auto flags = sle.getFieldU32(r::sfFlags);
        if (!issuances && sle.getType() == r::ltMPTOKEN) {
            Json token{{"mpt_id", r::to_string(sle.key())}, {"account", r::toBase58(sle.getAccountID(r::sfAccount))},
                       {"mpt_issuance_id", r::to_string(sle.getFieldH192(r::sfMPTokenIssuanceID))},
                       {"mpt_amount", amount(r::sfMPTAmount, sle.getFieldU64(r::sfMPTAmount))}};
            flags_of(token, flags, {{r::lsfMPTLocked, "mpt_locked"}, {r::lsfMPTAuthorized, "mpt_authorized"}});
            mptoken_extras(token, sle);
            list.push_back(token);
        } else if (issuances && sle.getType() == r::ltMPTOKEN_ISSUANCE) {
            Json issuance{{"mpt_issuance_id", r::to_string(sle.key())}, {"issuer", r::toBase58(owner)}, {"sequence", sle.getFieldU32(r::sfSequence)}};
            if (sle.isFieldPresent(r::sfTransferFee)) issuance["transfer_fee"] = sle.getFieldU16(r::sfTransferFee);
            if (sle.isFieldPresent(r::sfAssetScale)) issuance["asset_scale"] = sle.getFieldU8(r::sfAssetScale);
            for (auto const* field : {&r::sfMaximumAmount, &r::sfOutstandingAmount, &r::sfLockedAmount, &r::sfConfidentialOutstandingAmount})
                if (sle.isFieldPresent(*field)) {
                    auto name = *field == r::sfMaximumAmount ? "maximum_amount" : *field == r::sfOutstandingAmount ? "outstanding_amount"
                              : *field == r::sfLockedAmount ? "locked_amount" : "confidential_outstanding_amount";
                    issuance[name] = amount(*field, sle.getFieldU64(*field));
                }
            hex_of(issuance, sle, r::sfMPTokenMetadata, "mptoken_metadata");
            if (sle.isFieldPresent(r::sfDomainID)) issuance["domain_id"] = r::to_string(sle.getFieldH256(r::sfDomainID));
            flags_of(issuance, flags, {{r::lsfMPTLocked, "mpt_locked"}, {r::lsfMPTCanLock, "mpt_can_lock"}, {r::lsfMPTRequireAuth, "mpt_require_auth"},
                                       {r::lsfMPTCanEscrow, "mpt_can_escrow"}, {r::lsfMPTCanTrade, "mpt_can_trade"}, {r::lsfMPTCanTransfer, "mpt_can_transfer"},
                                       {r::lsfMPTCanClawback, "mpt_can_clawback"}, {r::lsfMPTCanHoldConfidentialBalance, "mpt_can_hold_confidential_balance"}});
            if (sle.isFieldPresent(r::sfImmutableFlags))
                flags_of(issuance, sle.getFieldU32(r::sfImmutableFlags),
                         {{r::lsifMPTCanLock, "mpt_immutable_can_lock"}, {r::lsifMPTRequireAuth, "mpt_immutable_require_auth"},
                          {r::lsifMPTCanEscrow, "mpt_immutable_can_escrow"}, {r::lsifMPTCanTrade, "mpt_immutable_can_trade"},
                          {r::lsifMPTCanTransfer, "mpt_immutable_can_transfer"}, {r::lsifMPTCanClawback, "mpt_immutable_can_clawback"},
                          {r::lsifMPTCanHoldConfidentialBalance, "mpt_immutable_can_hold_confidential_balance"},
                          {r::lsifMPTMetadata, "mpt_immutable_metadata"}, {r::lsifMPTTransferFee, "mpt_immutable_transfer_fee"}});
            hex_of(issuance, sle, r::sfIssuerEncryptionKey, "issuer_encryption_key");
            hex_of(issuance, sle, r::sfAuditorEncryptionKey, "auditor_encryption_key");
            list.push_back(issuance);
        }
    });
    auto result = ctx.base();
    result["account"] = address;
    result["limit"] = limit;
    result[issuances ? "mpt_issuances" : "mptokens"] = list;
    if (!next.empty()) result["marker"] = next;
    return result;
}

// Clio's limit for mpt_holders and nfts_by_issuer: 1-100, default 50; zero is refused.
std::uint32_t clio_limit(Json const& p) {
    if (!p.contains("limit")) return 50;
    auto const& v = p.at("limit");
    if (!v.is_number_integer() || (!v.is_number_unsigned() && v.get<std::int64_t>() < 0) || v.get<std::uint64_t>() > UINT32_MAX)
        fail(r::RpcInvalidParams, "Invalid parameters.");
    auto n = v.get<std::uint64_t>();
    if (!n) fail(r::RpcInvalidParams, "Invalid parameters.");
    return static_cast<std::uint32_t>(std::min<std::uint64_t>(n, 100));
}
[[noreturn]] void not_indexed(std::string const& method) {
    fail(r::RpcNotSupported, method + " needs its index, which this cache does not keep: set " + method +
         " = true under [index] in xrpla.toml (or run `xrpla index-pieces` without --indexes).");
}

// Clio's mpt_holders: the issuance's holders any held ledger has seen, in AccountID order after
// the marker, each read at the ledger asked about (those without an MPToken there are left
// out, so a page can be short, or empty with a marker); the marker is the last candidate when
// a page took `limit` of them.
Json mpt_holders(Context const& ctx) {
    auto const& p = ctx.params;
    if (!p.contains("mpt_issuance_id")) fail(r::RpcInvalidParams, "Required field 'mpt_issuance_id' missing");
    r::uint192 issuance;
    if (!p.at("mpt_issuance_id").is_string() || !issuance.parseHex(p.at("mpt_issuance_id").get<std::string>())) fail(r::RpcInvalidParams, "Malformed mpt_issuance_id.");
    auto limit = clio_limit(p);
    Bytes after;
    if (p.contains("marker")) {
        r::uint160 holder;
        if (!p.at("marker").is_string() || !holder.parseHex(p.at("marker").get<std::string>())) fail(r::RpcInvalidParams, "Malformed marker.");
        after = raw(holder);
    }
    if (!ctx.read(r::keylet::mptokenIssuance(issuance))) fail(r::RpcObjectNotFound, "objectNotFound");
    auto candidates = ctx.store.mpt_holders(raw(issuance), after, limit);
    if (!candidates) not_indexed("mpt_holders");
    std::vector<Bytes> keys;
    for (auto const& holder : *candidates) keys.push_back(raw(r::keylet::mptoken(issuance, r::AccountID::fromVoid(holder.data())).key));
    ctx.store.prefetch(keys, ctx.header.sequence);
    Json tokens = Json::array();
    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto bytes = ctx.store.object(keys[i], ctx.header.sequence);
        if (bytes.empty()) continue;
        r::SerialIter it(bytes.data(), bytes.size());
        r::SLE token(it, r::uint256::fromVoid(keys[i].data()));
        auto amount = r::STUInt64{r::sfMPTAmount, token.isFieldPresent(r::sfMPTAmount) ? token.getFieldU64(r::sfMPTAmount) : 0};
        Json holder{{"account", r::toBase58(token.getAccountID(r::sfAccount))}, {"flags", token.getFlags()},
                    {"mpt_amount", to_json(amount.getJson(r::JsonOptions::Values::None))}, {"mptoken_index", r::to_string(token.key())}};
        mptoken_extras(holder, token);  // Clio 2.8's MPTHolders.cpp: the same fields as account_mptokens
        tokens.push_back(std::move(holder));
    }
    Json result{{"mpt_issuance_id", r::to_string(issuance)}, {"limit", limit}, {"ledger_index", ctx.header.sequence}, {"mptokens", tokens}, {"validated", true}};
    if (candidates->size() == limit) result["marker"] = upper(to_hex(candidates->back()));
    return result;
}

// Clio's nfts_by_issuer: the issuer's tokens any held ledger has seen (only `nft_taxon`'s when
// given), in (taxon, token ID) order after the marker, each as nft_info reports it at the
// ledger asked about (tokens not minted yet are left out); the marker is the last candidate
// when a page took `limit` of them.
Json nfts_by_issuer(Context const& ctx) {
    auto const& p = ctx.params;
    if (!p.contains("issuer")) fail(r::RpcInvalidParams, "Required field 'issuer' missing");
    if (!p.at("issuer").is_string()) fail(r::RpcInvalidParams, "issuerNotString");
    auto issuer = r::parseBase58<r::AccountID>(p.at("issuer").get<std::string>());
    if (!issuer) fail(r::RpcActMalformed, "Account malformed.");
    std::optional<std::uint32_t> taxon;
    if (p.contains("nft_taxon")) {
        auto const& v = p.at("nft_taxon");
        if (!v.is_number_integer() || (!v.is_number_unsigned() && v.get<std::int64_t>() < 0) || v.get<std::uint64_t>() > UINT32_MAX)
            fail(r::RpcInvalidParams, "Invalid parameters.");
        taxon = static_cast<std::uint32_t>(v.get<std::uint64_t>());
    }
    auto limit = clio_limit(p);
    Bytes after;
    if (p.contains("marker")) {
        r::uint256 cursor;
        if (!p.at("marker").is_string() || !cursor.parseHex(p.at("marker").get<std::string>())) fail(r::RpcInvalidParams, "Malformed marker.");
        after = raw(cursor);
    }
    if (!ctx.read(r::keylet::account(*issuer))) fail(r::RpcActNotFound);  // Clio 2.8 NFTsByIssuer.cpp: the code's own message
    if (!ctx.store.has_nft_state()) fail(r::RpcNotSupported, "nfts_by_issuer needs NFT state: run `xrpla index-pieces` on this piece cache.");
    auto candidates = ctx.store.nfts_by_issuer(raw(*issuer), taxon, after, limit);
    if (!candidates) not_indexed("nfts_by_issuer");
    Json nfts = Json::array();
    auto states = ctx.store.nfts(*candidates, ctx.header.sequence);
    for (std::size_t i = 0; i < candidates->size(); ++i) {
        auto const& state = states[i];
        if (state.is_null()) continue;
        auto token = r::uint256::fromVoid((*candidates)[i].data());
        nfts.push_back({{"nft_id", upper(to_hex((*candidates)[i]))}, {"ledger_index", state.at("ledger_index")}, {"owner", state.at("owner")},
                        {"is_burned", state.at("is_burned")}, {"uri", upper(state.at("uri").get<std::string>())}, {"flags", r::nft::getFlags(token)},
                        {"transfer_fee", r::nft::getTransferFee(token)}, {"issuer", r::toBase58(r::nft::getIssuer(token))},
                        {"nft_taxon", r::nft::toUInt32(r::nft::getTaxon(token))}, {"nft_serial", r::nft::getSequence(token)}});
    }
    Json result{{"issuer", r::toBase58(*issuer)}, {"limit", limit}, {"ledger_index", ctx.header.sequence}, {"nfts", nfts}, {"validated", true}};
    if (taxon) result["nft_taxon"] = *taxon;
    if (candidates->size() == limit) result["marker"] = upper(to_hex(candidates->back()));
    return result;
}

// xrpld 3.4.0's vault_info: the vault named by vault_id, or by owner and seq, with its share
// issuance. Parse errors keep their own codes and messages; a zero key or a missing vault or
// issuance is entryNotFound (3.3.0 renamed them all malformedRequest).
Json vault_info(Context const& ctx) {
    auto const& p = ctx.params;
    auto result = ctx.base();
    bool has_id = p.contains("vault_id"), has_owner = p.contains("owner"), has_seq = p.contains("seq");
    r::uint256 index;
    if (has_id && !has_owner && !has_seq) {
        if (!p.at("vault_id").is_string() || !index.parseHex(p.at("vault_id").get<std::string>()))
            fail_into(result, r::RpcInvalidParams, "Invalid field 'vault_id', not hex string.");
    } else if (!has_id && has_owner && has_seq) {
        auto owner = p.at("owner").is_string() ? r::parseBase58<r::AccountID>(p.at("owner").get<std::string>()) : std::nullopt;
        if (!owner) fail_into(result, r::RpcActMalformed, "Invalid field 'owner', not AccountID.");
        // xrpld takes a 32-bit Int or UInt above zero.
        auto const& seq = p.at("seq");
        bool fits = seq.is_number_integer() && (seq.is_number_unsigned() ? seq.get<std::uint64_t>() <= UINT32_MAX
                                                                         : seq.get<std::int64_t>() >= INT32_MIN);
        if (!fits || (seq.is_number_unsigned() ? seq.get<std::uint64_t>() == 0 : seq.get<std::int64_t>() <= 0))
            fail_into(result, r::RpcInvalidParams, "Invalid field 'seq', not a positive 32-bit integer.");
        index = r::keylet::vault(*owner, r::SeqProxy::rawSequence(static_cast<std::uint32_t>(seq.get<std::uint64_t>()))).key;
    } else fail_into(result, r::RpcInvalidParams, "Must specify either 'vault_id' or both 'owner' and 'seq'.");
    if (index.isZero()) fail_into(result, r::RpcEntryNotFound);
    auto vault = ctx.read(r::keylet::vault(index));
    std::shared_ptr<r::SLE const> shares;
    if (vault) shares = ctx.read(r::keylet::mptokenIssuance((*vault)[r::sfShareMPTID]));
    if (!vault || !shares) fail_into(result, r::RpcEntryNotFound);
    result["vault"] = json(*vault);
    result["vault"]["shares"] = json(*shares);
    return result;
}
}

Json random_value() {
    r::uint256 value;
    randombytes_buf(value.data(), value.size());
    return {{"random", r::to_string(value)}};
}
std::set<std::string> const& clio_methods() {
    static std::set<std::string> const names{"feature", "get_aggregate_price", "account_mptokens", "account_mptoken_issuances", "vault_info",
        "mpt_holders", "nfts_by_issuer"};
    return names;
}
Json clio(Context const& ctx, std::string const& method) {
    if (method == "feature") return feature(ctx);
    if (method == "get_aggregate_price") return aggregate_price(ctx);
    if (method == "account_mptokens" || method == "account_mptoken_issuances") return account_mpts(ctx, method == "account_mptoken_issuances");
    if (method == "vault_info") return vault_info(ctx);
    if (method == "mpt_holders") return mpt_holders(ctx);
    if (method == "nfts_by_issuer") return nfts_by_issuer(ctx);
    fail(r::RpcUnknownCommand);
}
}
