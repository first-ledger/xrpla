// More of the rippled/xrpld read surface, answered from any History backend.
// Each handler follows the corresponding rippled 3.x handler; tools/xrpld_parity.py
// compares the answers with a live xrpld for the same validated ledgers.
#include "rpc.hpp"
#include <xrpl/basics/chrono.h>
#include <xrpl/protocol/AMMCore.h>
#include <xrpl/protocol/Book.h>
#include <xrpl/protocol/Feature.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/protocol/MPTIssue.h>
#include <xrpl/protocol/Protocol.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STTx.h>
#include <xrpl/protocol/TxFlags.h>
#include <xrpl/protocol/TxFormats.h>
#include <algorithm>
#include <cctype>
#include <charconv>
#include <initializer_list>
#include <string_view>
#include <map>
#include <set>

namespace xrpla::core::rpc {
namespace {

bool enabled(Context const& ctx, r::uint256 const& amendment) { return ctx.amendment(amendment); }
// xrpld 3.4.1's isFrozen for an IOU (RippleStateHelpers.cpp): its issuer's global freeze, or for an
// account other than the issuer the issuer's freeze of their line. Deep freeze is isDeepFrozen's.
bool iou_frozen(Context const& ctx, r::AccountID const& account, r::Issue const& issue) {
    if (r::isXRP(issue.currency)) return false;
    if (auto root = ctx.read(r::keylet::account(issue.account)); root && root->isFlag(r::lsfGlobalFreeze)) return true;
    if (issue.account == account) return false;
    auto line = ctx.read(r::keylet::trustLine(account, issue.account, issue.currency));
    return line && line->isFlag(issue.account > account ? r::lsfHighFreeze : r::lsfLowFreeze);
}
// xrpld 3.4.1's assetOfHolding (MPTokenHelpers.cpp): the asset of a vault share issuance's
// sfReferenceHolding, the vault's MPToken or its trust line with the asset's issuer.
r::Asset holding_asset(r::SLE const& issuance, r::SLE const& holding) {
    if (holding.getType() == r::ltMPTOKEN) return r::MPTIssue{holding.getFieldH192(r::sfMPTokenIssuanceID)};
    auto vault = issuance.getAccountID(r::sfIssuer);
    auto low = holding.getFieldAmount(r::sfLowLimit), high = holding.getFieldAmount(r::sfHighLimit);
    return r::Issue{low.get<r::Issue>().currency, low.getIssuer() != vault ? low.getIssuer() : high.getIssuer()};
}
bool mpt_locked(Context const& ctx, r::Keylet const& key) { auto sle = ctx.read(key); return sle && sle->isFlag(r::lsfMPTLocked); }
bool any_frozen(Context const&, std::initializer_list<r::AccountID>, r::Asset const&, std::uint8_t depth);
// xrpld 3.4.1's isVaultPseudoAccountFrozen (View.cpp): with SingleAssetVault, vault shares are frozen
// for an account while the vault's asset (its sfReferenceHolding's, or that of the issuer's vault)
// is frozen for the vault or the account; frozen past kMaxAssetCheckDepth levels of shares.
bool vault_frozen(Context const& ctx, r::AccountID const& account, r::MPTID const& id, std::uint8_t depth) {
    if (!enabled(ctx, r::featureSingleAssetVault)) return false;
    if (depth >= r::kMaxAssetCheckDepth) return true;
    auto issuance = ctx.read(r::keylet::mptokenIssuance(id));
    if (!issuance) return false;
    auto issuer = issuance->getAccountID(r::sfIssuer);
    if (issuance->isFieldPresent(r::sfReferenceHolding)) {
        auto holding = ctx.read(r::keylet::unchecked(issuance->getFieldH256(r::sfReferenceHolding)));
        return holding && any_frozen(ctx, {issuer, account}, holding_asset(*issuance, *holding), depth + 1);
    }
    auto root = ctx.read(r::keylet::account(issuer));
    if (!root || !root->isFieldPresent(r::sfVaultID)) return false;
    auto vault = ctx.read(r::keylet::vault(root->getFieldH256(r::sfVaultID)));
    return vault && any_frozen(ctx, {issuer, account}, (*vault)[r::sfAsset], depth + 1);
}
// xrpld 3.4.1's isAnyFrozen (TokenHelpers.cpp, MPTokenHelpers.cpp): an IOU frozen for any of the
// accounts; an MPT whose issuance is locked, or locked for any of them, or vault shares frozen for any.
bool any_frozen(Context const& ctx, std::initializer_list<r::AccountID> accounts, r::Asset const& asset, std::uint8_t depth) {
    return asset.visit(
        [&](r::Issue const& issue) { return std::ranges::any_of(accounts, [&](auto const& account) { return iou_frozen(ctx, account, issue); }); },
        [&](r::MPTIssue const& issue) {
            auto const& id = issue.getMptID();
            return mpt_locked(ctx, r::keylet::mptokenIssuance(id)) ||
                   std::ranges::any_of(accounts, [&](auto const& account) { return mpt_locked(ctx, r::keylet::mptoken(id, account)); }) ||
                   std::ranges::any_of(accounts, [&](auto const& account) { return vault_frozen(ctx, account, id, depth); });
        });
}
// xrpld 3.4.1's canTransfer(view, asset, from, to) succeeds (MPTokenHelpers.cpp, RippleStateHelpers.cpp,
// not waived): an IOU unless both sides' lines (or, lacking one, the issuer's DefaultRipple) block
// rippling; an MPT to or from its issuer, or one with lsfMPTCanTransfer whose vault asset, past
// fixCleanup3_2_0, can be transferred too. A missing issuer or issuance cannot.
bool transferable(Context const& ctx, r::Asset const& asset, r::AccountID const& from, r::AccountID const& to, std::uint8_t depth) {
    return asset.visit(
        [&](r::Issue const& issue) {
            if (r::isXRP(issue) || issue.account == from || issue.account == to) return true;
            auto root = ctx.read(r::keylet::account(issue.account));
            if (!root) return false;
            auto no_ripple = [&](r::AccountID const& account) {
                auto line = ctx.read(r::keylet::trustLine(account, issue.account, issue.currency));
                return line ? line->isFlag(issue.account > account ? r::lsfHighNoRipple : r::lsfLowNoRipple) : !root->isFlag(r::lsfDefaultRipple);
            };
            return !no_ripple(from) || !no_ripple(to);
        },
        [&](r::MPTIssue const& issue) {
            auto issuance = ctx.read(r::keylet::mptokenIssuance(issue.getMptID()));
            if (!issuance) return false;
            auto issuer = issuance->getAccountID(r::sfIssuer);
            if (from == issuer || to == issuer) return true;
            if (!issuance->isFlag(r::lsfMPTCanTransfer)) return false;
            if (!enabled(ctx, r::fixCleanup3_2_0) || !issuance->isFieldPresent(r::sfReferenceHolding)) return true;
            if (depth >= r::kMaxAssetCheckDepth) return false;
            auto holding = ctx.read(r::keylet::unchecked(issuance->getFieldH256(r::sfReferenceHolding)));
            return holding && transferable(ctx, holding_asset(*issuance, *holding), from, to, depth + 1);
        });
}
// An issue as the books stream keys it (book_key): a currency code and, but for XRP, an address.
// amm_info reads its assets with parse_asset.
r::Issue parse_issue(Json const& value) {
    if (!value.is_object() || !value.contains("currency") || !value.at("currency").is_string()) fail(r::RpcIssueMalformed);
    r::Currency currency;
    if (!r::toCurrency(currency, value.at("currency").get<std::string>())) fail(r::RpcIssueMalformed);
    r::AccountID issuer;
    if (value.contains("issuer")) {
        if (!value.at("issuer").is_string()) fail(r::RpcIssueMalformed);
        auto parsed = r::parseBase58<r::AccountID>(value.at("issuer").get<std::string>());
        if (!parsed) fail(r::RpcIssueMalformed);
        issuer = *parsed;
    }
    if (r::isXRP(currency) != (issuer == r::AccountID{})) fail(r::RpcIssueMalformed);
    return {currency, issuer};
}
// An amm_info asset as xrpld 3.4.1's getAsset (AMMInfo.cpp) reads it with assetFromJson (Asset.cpp,
// Issue.cpp, MPTIssue.cpp): an object with a currency or an mpt_issuance_id, not both. An MPT has
// no issuer and a hex id; a currency is a string code other than badCurrency (hex "XRP") and
// noCurrency (hex 1, or an unparsable code); XRP has no issuer or a null one, any other currency an
// address that is neither ACCOUNT_ZERO nor ACCOUNT_ONE. Otherwise nullopt (issueMalformed).
std::optional<r::Asset> parse_asset(Json const& value) {
    if (!value.is_object() || (!value.contains("currency") && !value.contains("mpt_issuance_id"))) return std::nullopt;
    if (!value.contains("currency")) {
        r::MPTID id;
        auto const& text = value.at("mpt_issuance_id");
        if (value.contains("issuer") || !text.is_string() || !id.parseHex(text.get<std::string>())) return std::nullopt;
        return r::Asset{r::MPTIssue{id}};
    }
    if (value.contains("mpt_issuance_id") || !value.at("currency").is_string()) return std::nullopt;
    auto currency = r::toCurrency(value.at("currency").get<std::string>());
    if (currency == r::badCurrency() || currency == r::noCurrency()) return std::nullopt;
    auto issuer = value.value("issuer", Json());
    if (r::isXRP(currency)) return issuer.is_null() ? std::optional<r::Asset>{r::xrpIssue()} : std::nullopt;
    auto parsed = issuer.is_string() ? r::parseBase58<r::AccountID>(issuer.get<std::string>()) : std::nullopt;
    if (!parsed || *parsed == r::noAccount() || *parsed == r::xrpAccount()) return std::nullopt;
    return r::Asset{r::Issue{currency, *parsed}};
}
// Seconds since the XRPL epoch as "%Y-%m-%dT%H:%M:%S+0000", rippled's to_iso8601.
std::string iso8601(std::uint64_t seconds) {
    auto text = r::toStringIso(r::NetClock::time_point{r::NetClock::duration{seconds}});
    if (!text.empty() && text.back() == 'Z') text.pop_back();
    return text + "+0000";
}
std::uint64_t base_fee(Context const& ctx) { return ledger_fees(ctx).base; }

// deposit_authorized's accounts as DepositAuthorized.cpp reads them, before the ledger: each
// present, a string and an address, the source's checks first.
r::AccountID deposit_account(Json const& p, char const* field) {
    if (!p.contains(field)) fail(r::RpcInvalidParams, std::string("Missing field '") + field + "'.");
    if (!p.at(field).is_string()) fail(r::RpcInvalidParams, std::string("Invalid field '") + field + "', not a string.");
    return account(p.at(field).get<std::string>());
}
// xrpld 3.4.1's deposit_authorized (DepositAuthorized.cpp), credentials included: each must exist,
// be accepted, not be expired as of the ledger's parent close time, belong to the source, and
// name an (issuer, type) pair no other one does; the destination's preauthorization of that set
// of pairs authorizes the deposit as its preauthorization of the source does.
Json deposit_authorized(Context const& ctx) {
    auto const& p = ctx.params;
    auto source = deposit_account(p, "source_account");
    auto destination = deposit_account(p, "destination_account");
    if (!ctx.read(r::keylet::account(source))) fail_into(ctx.base(), r::RpcSrcActNotFound);
    auto target = ctx.read(r::keylet::account(destination));
    if (!target) fail_into(ctx.base(), r::RpcDstActNotFound);
    bool required = target->isFlag(r::lsfDepositAuth) && source != destination;
    bool with_credentials = p.contains("credentials");
    std::set<std::pair<r::AccountID, r::Slice>> sorted;
    std::vector<r::Blob> types;  // what the slices in `sorted` point into
    if (with_credentials) {
        auto const& credentials = p.at("credentials");
        if (!credentials.is_array() || credentials.empty())
            fail(r::RpcInvalidParams, "Invalid field 'credentials', not is non-empty array of CredentialID(hash256).");
        if (credentials.size() > r::kMaxCredentialsArraySize) fail(r::RpcInvalidParams, "Invalid field 'credentials', not array too long.");
        types.reserve(credentials.size());
        auto parent_close = r::deserializeHeader({ctx.header.raw.data(), ctx.header.raw.size()}).parentCloseTime.time_since_epoch().count();
        for (auto const& item : credentials) {
            r::uint256 id;
            if (!item.is_string() || !id.parseHex(item.get<std::string>()))
                fail(r::RpcInvalidParams, "Invalid field 'credentials', not an array of CredentialID(hash256).");
            auto credential = ctx.read(r::keylet::credential(id));
            if (!credential) fail_into(ctx.base(), r::RpcBadCredentials, "credentials don't exist");
            if (!credential->isFlag(r::lsfAccepted)) fail_into(ctx.base(), r::RpcBadCredentials, "credentials aren't accepted");
            auto expiration = credential->isFieldPresent(r::sfExpiration) ? credential->getFieldU32(r::sfExpiration) : UINT32_MAX;
            if (parent_close > expiration) fail_into(ctx.base(), r::RpcBadCredentials, "credentials are expired");
            if (credential->getAccountID(r::sfSubject) != source) fail_into(ctx.base(), r::RpcBadCredentials, "credentials doesn't belong to the root account");
            types.push_back(credential->getFieldVL(r::sfCredentialType));
            if (!sorted.emplace(credential->getAccountID(r::sfIssuer), r::makeSlice(types.back())).second)
                fail_into(ctx.base(), r::RpcBadCredentials, "duplicates in credentials");
        }
    }
    bool authorized = true;
    if (required)
        authorized = bool(ctx.read(r::keylet::depositPreauth(destination, source))) ||
                     (with_credentials && ctx.read(r::keylet::depositPreauth(destination, sorted)));
    auto result = ctx.base();
    result["source_account"] = p.at("source_account"); result["destination_account"] = p.at("destination_account");
    if (with_credentials) result["credentials"] = p.at("credentials");
    result["deposit_authorized"] = authorized;
    return result;
}

// The NFT an nft_buy_offers or nft_sell_offers request names, as NFTBuyOffers.cpp reads it before
// the ledger: present, then `parseHex(asString())` (asString throws on an array or object).
r::uint256 offers_nft(Json const& p) {
    if (!p.contains("nft_id")) fail(r::RpcInvalidParams, "Missing field 'nft_id'.");
    auto const& value = p.at("nft_id");
    if (value.is_array() || value.is_object()) fail(r::RpcInternal);
    r::uint256 id;
    if (!value.is_string() || !id.parseHex(value.get<std::string>())) fail(r::RpcInvalidParams, "Invalid field 'nft_id'.");
    return id;
}
Json nft_offers(Context const& ctx, bool buys) {
    auto const& p = ctx.params;
    auto id = offers_nft(p);
    auto count_limit = limit_field(p, 50, 250, 500);
    auto directory = buys ? r::keylet::nftBuys(id) : r::keylet::nftSells(id);
    if (!ctx.read(directory)) fail(r::RpcObjectNotFound);
    auto offer_json = [](r::SLE const& offer) {
        Json o{{"nft_offer_index", r::to_string(offer.key())}, {"flags", offer.getFieldU32(r::sfFlags)}, {"owner", r::toBase58(offer.getAccountID(r::sfOwner))}};
        if (offer.isFieldPresent(r::sfDestination)) o["destination"] = r::toBase58(offer.getAccountID(r::sfDestination));
        if (offer.isFieldPresent(r::sfExpiration)) o["expiration"] = offer.getFieldU32(r::sfExpiration);
        o["amount"] = json(offer.getFieldAmount(r::sfAmount));
        return o;
    };
    Json offers = Json::array();
    r::uint256 after; std::uint64_t hint = 0; unsigned reserve = count_limit;
    std::vector<std::shared_ptr<r::SLE const>> found;
    if (p.contains("marker")) {
        if (!p.at("marker").is_string()) fail(r::RpcInvalidParams, "Invalid field 'marker', not string.");
        if (!after.parseHex(p.at("marker").get<std::string>())) fail(r::RpcInvalidParams);
        auto sle = ctx.read(r::keylet::nftokenOffer(after));
        if (!sle || id != sle->getFieldH256(r::sfNFTokenID)) fail(r::RpcInvalidParams);
        // As 3.4.1 (NFTOffersHelpers.h): an offer on the other side is refused before its node
        // hint, a page of the other directory, has the walk read all of this one.
        if (sle->isFlag(r::lsfSellNFToken) == buys) fail(r::RpcInvalidParams);
        hint = sle->getFieldU64(r::sfNFTokenOfferNode);
        offers.push_back(offer_json(*sle));
    } else ++reserve;
    if (!for_each_after(ctx, directory, after, hint, reserve, [&](std::shared_ptr<r::SLE const> const& sle) {
            if (sle && sle->getType() == r::ltNFTOKEN_OFFER) { found.push_back(sle); return true; }
            return false;
        })) fail(r::RpcInvalidParams);
    Json result = Json::object();
    if (found.size() == reserve) { result["limit"] = count_limit; result["marker"] = r::to_string(found.back()->key()); found.pop_back(); }
    for (auto const& offer : found) offers.push_back(offer_json(*offer));
    result["nft_id"] = r::to_string(id); result["offers"] = offers;
    return result;
}

Json amm_info(Context const& ctx) {
    auto const& p = ctx.params;
    // As xrpld 3.4.1's doAMMInfo: every error on top of the ledger's fields (injectError into
    // lookupLedger's result), the pool's assets Issues or MPTs.
    if (p.contains("asset") != p.contains("asset2") || p.contains("asset") == p.contains("amm_account")) fail_into(ctx.base(), r::RpcInvalidParams);
    std::optional<r::Asset> first, second;
    if (p.contains("asset")) {
        if (!(first = parse_asset(p.at("asset")))) fail_into(ctx.base(), r::RpcIssueMalformed);
        if (!(second = parse_asset(p.at("asset2")))) fail_into(ctx.base(), r::RpcIssueMalformed);
    }
    std::optional<r::uint256> id;
    if (p.contains("amm_account")) {
        if (!p.at("amm_account").is_string()) fail_into(ctx.base(), r::RpcActMalformed);
        auto parsed = r::parseBase58<r::AccountID>(p.at("amm_account").get<std::string>());
        if (!parsed) fail_into(ctx.base(), r::RpcActMalformed);
        auto root = ctx.read(r::keylet::account(*parsed));
        if (!root) fail_into(ctx.base(), r::RpcActMalformed);
        id = root->isFieldPresent(r::sfAMMID) ? root->getFieldH256(r::sfAMMID) : r::uint256{};
        if (id->isZero()) fail_into(ctx.base(), r::RpcActNotFound);
    }
    std::optional<r::AccountID> holder;
    if (p.contains("account")) {
        auto parsed = p.at("account").is_string() ? r::parseBase58<r::AccountID>(p.at("account").get<std::string>()) : std::nullopt;
        if (!parsed || !ctx.read(r::keylet::account(*parsed))) fail_into(ctx.base(), r::RpcActMalformed);
        holder = *parsed;
    }
    auto amm = ctx.read(id ? r::keylet::amm(*id) : r::keylet::amm(*first, *second));
    if (!amm) fail_into(ctx.base(), r::RpcActNotFound);
    if (!first) { first = r::Asset{(*amm)[r::sfAsset]}; second = r::Asset{(*amm)[r::sfAsset2]}; }
    auto pool = amm->getAccountID(r::sfAccount);
    // ammPoolHolds: accountHolds ignoring freeze and authorization; an MPT's MPToken amount.
    auto holds = [&](r::Asset const& asset) -> r::STAmount {
        return asset.visit(
            [&](r::Issue const& issue) -> r::STAmount {
                if (r::isXRP(issue)) { if (auto root = ctx.read(r::keylet::account(pool))) return root->getFieldAmount(r::sfBalance); }
                else if (auto line = ctx.read(r::keylet::trustLine(pool, issue.account, issue.currency))) {
                    auto amount = line->getFieldAmount(r::sfBalance);
                    if (pool > issue.account) amount.negate();
                    amount.setIssue(issue);
                    return amount;
                }
                return r::STAmount{issue};
            },
            [&](r::MPTIssue const& issue) -> r::STAmount {
                if (auto token = ctx.read(r::keylet::mptoken(issue.getMptID(), pool))) return r::STAmount{issue, token->getFieldU64(r::sfMPTAmount)};
                return r::STAmount{issue};
            });
    };
    auto a1 = holds(*first), a2 = holds(*second);
    r::STAmount lp = amm->getFieldAmount(r::sfLPTokenBalance);
    if (holder) {
        auto lpt = lp.get<r::Issue>();
        r::STAmount balance{lpt};
        if (auto line = ctx.read(r::keylet::trustLine(*holder, lpt.account, lpt.currency)); line && !iou_frozen(ctx, *holder, lpt)) {
            balance = line->getFieldAmount(r::sfBalance);
            if (*holder > lpt.account) balance.negate();
            balance.setIssue(lpt);
        }
        lp = balance;
    }
    Json value{{"amount", json(a1)}, {"amount2", json(a2)}, {"lp_token", json(lp)}, {"trading_fee", amm->getFieldU16(r::sfTradingFee)},
        {"account", r::toBase58(pool)}};
    if (amm->isFieldPresent(r::sfVoteSlots)) {
        Json votes = Json::array();
        for (auto const& vote : amm->getFieldArray(r::sfVoteSlots))
            votes.push_back({{"account", r::toBase58(vote.getAccountID(r::sfAccount))}, {"trading_fee", vote.getFieldU16(r::sfTradingFee)},
                             {"vote_weight", vote.getFieldU32(r::sfVoteWeight)}});
        if (!votes.empty()) value["vote_slots"] = votes;
    }
    if (amm->isFieldPresent(r::sfAuctionSlot)) {
        auto const& slot = static_cast<r::STObject const&>(amm->peekAtField(r::sfAuctionSlot));
        if (slot.isFieldPresent(r::sfAccount)) {
            auto parent = r::deserializeHeader({ctx.header.raw.data(), ctx.header.raw.size()}).parentCloseTime.time_since_epoch().count();
            auto time_slot = r::ammAuctionTimeSlot(parent, slot);
            Json auction{{"time_interval", time_slot ? unsigned(*time_slot) : unsigned(r::kAuctionSlotTimeIntervals)},
                {"price", json(slot.getFieldAmount(r::sfPrice))}, {"discounted_fee", slot.getFieldU16(r::sfDiscountedFee)},
                {"account", r::toBase58(slot.getAccountID(r::sfAccount))}, {"expiration", iso8601(slot.getFieldU32(r::sfExpiration))}};
            if (slot.isFieldPresent(r::sfAuthAccounts)) {
                Json auth = Json::array();
                for (auto const& entry : slot.getFieldArray(r::sfAuthAccounts)) auth.push_back({{"account", r::toBase58(entry.getAccountID(r::sfAccount))}});
                auction["auth_accounts"] = auth;
            }
            value["auction_slot"] = auction;
        }
    }
    if (!r::isXRP(*first)) value["asset_frozen"] = asset_frozen(ctx, pool, *first);
    if (!r::isXRP(*second)) value["asset2_frozen"] = asset_frozen(ctx, pool, *second);
    auto result = ctx.base();
    result["amm"] = value;
    return result;
}

// rippled's transaction_entry: errors are plain strings on top of the ledger fields.
Json transaction_entry(Context const& ctx) {
    auto const& p = ctx.params;
    auto result = ctx.base();
    auto error = [&](char const* name) { fail_named(result, name); };
    if (!p.contains("tx_hash")) error("fieldNotFoundTransaction");
    // xrpld refuses the open ledger, whose lookup result has no ledger_hash; this one serves no open
    // ledger (one named "current" or "" is forwarded or refused before this handler), but names a
    // default (none given) where xrpld takes the open one.
    if (!p.contains("ledger_hash") && !p.contains("ledger_index") && !p.contains("ledger")) error("notYetImplemented");
    // xrpld reads the hash with asString, which throws for an array or object: a bare internal error.
    if (p.at("tx_hash").is_array() || p.at("tx_hash").is_object()) fail(r::RpcInternal);
    r::uint256 id;
    if (!p.at("tx_hash").is_string() || !id.parseHex(p.at("tx_hash").get<std::string>())) error("malformedRequest");
    auto tx = ctx.store.transaction(raw(id), true);
    if (tx.is_null() || tx.at("ledger_index") != ctx.header.sequence) error("transactionNotFound");
    // As xrpld, which cannot decode it (a few of 2013).
    if (!api_decodable({from_hex(tx.at("tx_blob").get<std::string>()), from_hex(tx.at("meta").get<std::string>())})) error("transactionNotFound");
    auto blob = from_hex(tx.at("tx_blob").get<std::string>()), meta = from_hex(tx.at("meta").get<std::string>());
    r::STTx native{r::SerialIter(blob.data(), blob.size())};
    r::SerialIter iterator(meta.data(), meta.size());
    r::STObject metadata(iterator, r::sfMetadata);
    auto tx_json = json(native);
    if (ctx.version > 1) {
        tx_json.erase("hash");
        result["hash"] = r::to_string(native.getTransactionID());
        auto h = r::deserializeHeader({ctx.header.raw.data(), ctx.header.raw.size()});
        result["close_time_iso"] = r::toStringIso(h.closeTime);
    }
    if (native.getTxnType() == r::ttPAYMENT && tx_json.contains("Amount")) {
        tx_json["DeliverMax"] = tx_json.at("Amount");
        if (ctx.version > 1) tx_json.erase("Amount");
    }
    result["tx_json"] = tx_json;
    result[ctx.version > 1 ? "meta" : "metadata"] = json(metadata);
    return result;
}

// xrpld 3.4.0's computeBookChanges (rpc/BookChanges.h): per asset pair (with the last crossing's
// domain), the volumes and rates of the ledger's offer crossings, in transaction-tree order.
Json book_changes(Context const& ctx) {
    using Tally = std::tuple<r::STAmount, r::STAmount, r::STAmount, r::STAmount, r::STAmount, r::STAmount, std::optional<r::uint256>>;
    std::map<std::string, Tally> tally;
    // A volume past what its asset can represent drops that crossing: IOU addition throws, and
    // MPT addition would wrap past kMaxMpTokenAmount.
    auto checked_add = [](r::STAmount& acc, r::STAmount const& delta) {
        return acc.asset().visit(
            [&](r::Issue const&) {
                try { acc += delta; } catch (std::overflow_error const&) { return false; }
                return true;
            },
            [&](r::MPTIssue const&) {
                auto room = static_cast<std::int64_t>(r::kMaxMpTokenAmount) - acc.mpt().value();
                if (delta.mpt().value() > room) return false;
                acc += delta;
                return true;
            });
    };
    for (auto const& record : ctx.store.ledger_transaction_records(ctx.header.sequence)) {
        if (!api_decodable(record)) continue;  // one xrpld cannot decode (a few of 2013): not shown by its APIs
        r::STTx tx{r::SerialIter(record.transaction.data(), record.transaction.size())};
        r::SerialIter iterator(record.metadata.data(), record.metadata.size());
        r::STObject meta(iterator, r::sfMetadata);
        if (!meta.isFieldPresent(r::sfAffectedNodes)) continue;
        std::optional<std::uint32_t> cancel;
        auto type = tx.getFieldU16(r::sfTransactionType);
        if ((type == r::ttOFFER_CANCEL || type == r::ttOFFER_CREATE) && tx.isFieldPresent(r::sfOfferSequence)) cancel = tx.getFieldU32(r::sfOfferSequence);
        for (auto const& node : meta.getFieldArray(r::sfAffectedNodes)) {
            auto const& kind = node.getFName();
            if (node.getFieldU16(r::sfLedgerEntryType) != r::ltOFFER || kind == r::sfCreatedNode) continue;
            if (!node.isFieldPresent(r::sfFinalFields) || !node.isFieldPresent(r::sfPreviousFields)) continue;
            auto const& final_fields = static_cast<r::STObject const&>(node.peekAtField(r::sfFinalFields));
            auto const& previous = static_cast<r::STObject const&>(node.peekAtField(r::sfPreviousFields));
            if (!final_fields.isFieldPresent(r::sfTakerGets) || !final_fields.isFieldPresent(r::sfTakerPays) ||
                !previous.isFieldPresent(r::sfTakerGets) || !previous.isFieldPresent(r::sfTakerPays)) continue;
            if (kind == r::sfDeletedNode && cancel && final_fields.getFieldU32(r::sfSequence) == *cancel) continue;
            auto gets = final_fields.getFieldAmount(r::sfTakerGets) - previous.getFieldAmount(r::sfTakerGets);
            auto pays = final_fields.getFieldAmount(r::sfTakerPays) - previous.getFieldAmount(r::sfTakerPays);
            auto g = r::to_string(gets.asset()), q = r::to_string(pays.asset());
            bool noswap = r::isXRP(gets) || (!r::isXRP(pays) && g < q);
            auto first = noswap ? gets : pays, second = noswap ? pays : gets;
            if (second.signum() == 0) continue;
            r::STAmount rate;
            try { rate = r::divide(first, second, r::noIssue()); } catch (std::overflow_error const&) { continue; }
            if (first.signum() < 0) first = -first;
            if (second.signum() < 0) second = -second;
            std::optional<r::uint256> domain;
            if (final_fields.isFieldPresent(r::sfDomainID)) domain = final_fields.getFieldH256(r::sfDomainID);
            auto key = noswap ? g + "|" + q : q + "|" + g;
            auto it = tally.find(key);
            if (it == tally.end()) { tally.emplace(key, Tally{first, second, rate, rate, rate, rate, domain}); continue; }
            auto& [a, b, high, low, open, close, last_domain] = it->second;
            // Both volumes or neither, as an unrepresentable rate is skipped above.
            auto volume_a = a, volume_b = b;
            if (!checked_add(volume_a, first) || !checked_add(volume_b, second)) continue;
            a = volume_a; b = volume_b;
            if (high < rate) high = rate;
            if (low > rate) low = rate;
            close = rate; last_domain = domain;
        }
    }
    auto volume = [](r::STAmount const& v) {
        return v.asset().visit([&](r::Issue const& issue) { return r::isXRP(issue) ? r::to_string(v.xrp()) : r::to_string(v.iou()); },
                               [&](r::MPTIssue const&) { return r::to_string(v.mpt()); });
    };
    Json changes = Json::array();
    for (auto const& [key, entry] : tally) {
        auto const& [a, b, high, low, open, close, domain] = entry;
        Json change = Json::object();
        a.asset().visit([&](r::Issue const&) { change["currency_a"] = r::isXRP(a) ? "XRP_drops" : r::to_string(a.asset()); },
                        [&](r::MPTIssue const&) { change["mpt_issuance_id_a"] = r::to_string(a.asset()); });
        b.asset().visit([&](r::Issue const&) { change["currency_b"] = r::isXRP(b) ? "XRP_drops" : r::to_string(b.asset()); },
                        [&](r::MPTIssue const&) { change["mpt_issuance_id_b"] = r::to_string(b.asset()); });
        change["volume_a"] = volume(a); change["volume_b"] = volume(b);
        change["high"] = r::to_string(high.iou()); change["low"] = r::to_string(low.iou());
        change["open"] = r::to_string(open.iou()); change["close"] = r::to_string(close.iou());
        if (domain) change["domain"] = r::to_string(*domain);
        changes.push_back(change);
    }
    return {{"type", "bookChanges"}, {"validated", true}, {"ledger_index", ctx.header.sequence}, {"ledger_hash", upper(ctx.header.hash)},
            {"ledger_time", ctx.header.close_time}, {"changes", changes}};
}

Json noripple_check(Context const& ctx) {
    auto const& p = ctx.params;
    check_noripple(p, ctx.version);
    bool gateway = p.at("role") == "gateway";
    auto count_limit = limit_field(p, 10, 300, 400);
    bool with_transactions = loose_boolean(p, "transactions");
    // xrpld parses the account only once the ledger is found: malformed, it is refused on top of
    // the ledger's fields (and the empty transactions list it has added by then).
    auto owner = r::parseBase58<r::AccountID>(p.at("account").get<std::string>());
    if (!owner) {
        auto so_far = ctx.base();
        if (with_transactions) so_far["transactions"] = Json::array();
        fail_into(so_far, r::RpcActMalformed);
    }
    auto root = ctx.read(r::keylet::account(*owner));
    if (!root) fail(r::RpcActNotFound);
    std::uint32_t sequence = root->getFieldU32(r::sfSequence);
    auto fee = base_fee(ctx);
    auto result = ctx.base();
    Json problems = Json::array(), transactions = Json::array();
    auto fill = [&](Json tx) { tx["Sequence"] = sequence++; tx["Account"] = r::toBase58(*owner); tx["Fee"] = fee; transactions.push_back(tx); };
    bool default_ripple = root->isFlag(r::lsfDefaultRipple);
    if (default_ripple && !gateway)
        problems.push_back("You appear to have set your default ripple flag even though you are not a gateway. This is not recommended unless you are experimenting");
    else if (gateway && !default_ripple) {
        problems.push_back("You should immediately set your default ripple flag");
        if (with_transactions) fill({{"TransactionType", "AccountSet"}, {"SetFlag", 8}});
    }
    for_each_after(ctx, r::keylet::ownerDir(*owner), r::uint256{}, 0, count_limit, [&](std::shared_ptr<r::SLE const> const& item) {
        if (!item || item->getType() != r::ltRIPPLE_STATE) return false;
        bool low = *owner == item->getFieldAmount(r::sfLowLimit).getIssuer();
        bool no_ripple = item->isFlag(low ? r::lsfLowNoRipple : r::lsfHighNoRipple);
        std::string problem;
        if (no_ripple && gateway) problem = "You should clear the no ripple flag on your ";
        else if (!gateway && !no_ripple) problem = "You should probably set the no ripple flag on your ";
        else return false;
        auto peer_limit = item->getFieldAmount(low ? r::sfHighLimit : r::sfLowLimit);
        problems.push_back(problem + r::to_string(peer_limit.get<r::Issue>().currency) + " line to " + r::toBase58(peer_limit.getIssuer()));
        auto amount = item->getFieldAmount(low ? r::sfLowLimit : r::sfHighLimit);
        amount.setIssue(r::Issue{amount.get<r::Issue>().currency, peer_limit.getIssuer()});
        if (with_transactions) fill({{"TransactionType", "TrustSet"}, {"LimitAmount", json(amount)}, {"Flags", no_ripple ? r::tfClearNoRipple : r::tfSetNoRipple}});
        return true;
    });
    result["problems"] = problems;
    if (with_transactions) result["transactions"] = transactions;
    return result;
}

// "2024-01-02T03:04:05Z" as seconds since the Unix epoch, read as Clio 2.8 reads it (checked
// against s1.ripple.com): std::get_time's "%Y-%m-%dT%H:%M:%SZ", then timegm. So 1 to 4 digits a
// year and 1 or 2 the other fields, each after any whitespace (which does not count toward its
// digits), a month of 1-12, a day of 1-31 (Feb 30 is Mar 2, timegm carrying it on), an hour of
// 0-23, a minute of 0-59 and a second of 0-61 (61 is the next minute's first), and the literals
// '-', 'T', ':' and 'Z' exactly where they stand, upper-case and with no whitespace before them;
// whatever follows the 'Z' is not read. A year before 1970 gives a negative count.
std::optional<long long> parse_date(std::string const& text) {
    std::size_t at = 0;
    auto number = [&](std::size_t widest, int low, int high) -> std::optional<int> {
        while (at < text.size() && std::isspace(static_cast<unsigned char>(text[at]))) ++at;
        int value = 0; std::size_t digits = 0;
        while (at < text.size() && digits < widest && text[at] >= '0' && text[at] <= '9') { value = value * 10 + (text[at++] - '0'); ++digits; }
        if (!digits || value < low || value > high) return std::nullopt;
        return value;
    };
    auto literal = [&](char c) { return at < text.size() && text[at++] == c; };
    auto y = number(4, 0, 9999);
    if (!y || !literal('-')) return std::nullopt;
    auto mo = number(2, 1, 12);
    if (!mo || !literal('-')) return std::nullopt;
    auto d = number(2, 1, 31);
    if (!d || !literal('T')) return std::nullopt;
    auto h = number(2, 0, 23);
    if (!h || !literal(':')) return std::nullopt;
    auto mi = number(2, 0, 59);
    if (!mi || !literal(':')) return std::nullopt;
    auto sec = number(2, 0, 61);
    if (!sec || !literal('Z')) return std::nullopt;
    // Days since 1970-01-01 of the month's first day, plus the day: a day past the month's end
    // lands in the next month, as timegm normalises it.
    int year = *y - (*mo <= 2);
    long era = (year >= 0 ? year : year - 399) / 400, yoe = year - era * 400, doy = (153 * (*mo + (*mo > 2 ? -3 : 9)) + 2) / 5 + *d - 1;
    long days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    return days * 86400LL + *h * 3600 + *mi * 60 + *sec;
}
}

r::Issue issue_of(Json const& value) { return parse_issue(value); }
bool asset_frozen(Context const& ctx, r::AccountID const& account, r::Asset const& asset) { return any_frozen(ctx, {account}, asset, 0); }
bool lp_tokens_transferable(Context const& ctx, r::AccountID const& holder, r::AccountID const& issuer) {
    return lp_tokens_transferable(ctx, holder, ctx.read(r::keylet::account(issuer)).get());
}
bool lp_tokens_transferable(Context const& ctx, r::AccountID const& holder, r::SLE const* root) {
    if (!root || !root->isFieldPresent(r::sfAMMID)) return true;
    auto amm = ctx.read(r::keylet::amm(root->getFieldH256(r::sfAMMID)));
    // Only the pool's MPTs are checked: an IOU pool asset is canTransferLPToken's no-op.
    auto check = [&](r::Asset const& asset) { return !asset.holds<r::MPTIssue>() || transferable(ctx, asset, holder, holder, 0); };
    return amm && check((*amm)[r::sfAsset]) && check((*amm)[r::sfAsset2]);
}
void check_noripple(Json const& p, unsigned version) {
    // xrpld 3.4.1's noripple_check checks these before it looks up the ledger, in this order: the
    // account and role present, the account a string, the role "gateway" or "user" (any value read
    // as a string), the limit, and in API v2 `transactions` a boolean.
    if (!p.contains("account")) fail(r::RpcInvalidParams, "Missing field 'account'.");
    if (!p.contains("role")) fail(r::RpcInvalidParams, "Missing field 'role'.");
    if (!p.at("account").is_string()) fail(r::RpcInvalidParams, "Invalid field 'account'.");
    auto const& role = p.at("role");
    if (role != "gateway" && role != "user") fail(r::RpcInvalidParams, "Invalid field 'role'.");
    (void)limit_field(p, 10, 300, 400);
    if (version > 1 && p.contains("transactions") && !p.at("transactions").is_boolean()) fail(r::RpcInvalidParams, "Invalid field 'transactions'.");
}
void check_more(std::string const& method, Json const& p) {
    if (method == "deposit_authorized") { deposit_account(p, "source_account"); deposit_account(p, "destination_account"); }
    if (method == "nft_buy_offers" || method == "nft_sell_offers") { offers_nft(p); (void)limit_field(p, 50, 250, 500); }
}
std::string book_key(Json const& in, Json const& out, std::string const& domain) {
    auto issue = [](Json const& v) -> std::string {
        if (v.is_string()) return "XRP";
        if (v.contains("mpt_issuance_id")) return "MPT:" + upper(v.at("mpt_issuance_id").get<std::string>());
        Json issue{{"currency", v.at("currency")}};
        if (v.contains("issuer")) issue["issuer"] = v.at("issuer");
        return r::to_string(parse_issue(issue));
    };
    return issue(in) + ">" + issue(out) + (domain.empty() ? "" : "#" + upper(domain));
}
std::set<std::string> const& more_methods() {
    static std::set<std::string> const names{"deposit_authorized", "nft_buy_offers", "nft_sell_offers", "amm_info",
        "transaction_entry", "book_changes", "noripple_check"};
    return names;
}
Json more(Context const& ctx, std::string const& method) {
    if (method == "deposit_authorized") return deposit_authorized(ctx);
    if (method == "nft_buy_offers" || method == "nft_sell_offers") return nft_offers(ctx, method == "nft_buy_offers");
    if (method == "amm_info") return amm_info(ctx);
    if (method == "transaction_entry") return transaction_entry(ctx);
    if (method == "book_changes") return book_changes(ctx);
    if (method == "noripple_check") return noripple_check(ctx);
    fail(r::RpcUnknownCommand);
}
// Clio's ledger_index: the last ledger closed at or before `date`, or the newest ledger.
Json ledger_index(History const& store, Json const& p, Json const& range) {
    std::uint32_t lo = range.at("first"), hi = range.at("last");
    auto answer = [&](std::uint32_t seq) {
        auto h = decode_header(store.header(seq));
        return Json{{"ledger_index", seq}, {"ledger_hash", upper(h.hash)}, {"closed", r::toStringIso(r::NetClock::time_point{r::NetClock::duration{h.close_time}})},
                    {"validated", true}};
    };
    if (!p.contains("date")) return answer(hi);
    // Clio 2.8 refuses a date it cannot read with a bare invalidParams, and one before the first
    // ledger held (the XRPL epoch, 2000-01-01, included) with ledgerNotInRange. It counts the date
    // in nanoseconds as a signed 64-bit number, which wraps: a date after 2262-04-11T23:47:16Z or
    // before 1970 is the wrapped count's (one before 2000 refused, a later one looked up as that).
    if (!p.at("date").is_string()) fail(r::RpcInvalidParams);
    auto date = parse_date(p.at("date").get<std::string>());
    if (!date) fail(r::RpcInvalidParams);
    constexpr std::int64_t kNano = 1'000'000'000, kRippleEpoch = 946684800;
    auto nanoseconds = static_cast<std::int64_t>(static_cast<std::uint64_t>(*date) * static_cast<std::uint64_t>(kNano));
    if (nanoseconds < kRippleEpoch * kNano) fail(r::RpcLgrNotFound, "ledgerNotInRange");
    auto target = static_cast<std::uint64_t>(nanoseconds / kNano - kRippleEpoch);
    if (decode_header(store.header(lo)).close_time > target) fail(r::RpcLgrNotFound, "ledgerNotInRange");  // Clio 2.8's message
    while (lo < hi) {
        auto mid = lo + (hi - lo + 1) / 2;
        if (decode_header(store.header(mid)).close_time <= target) lo = mid; else hi = mid - 1;
    }
    return answer(lo);
}
}
