#include "rpc.hpp"
#include <xrpl/protocol/Book.h>
#include <xrpl/protocol/Rate.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/protocol/Feature.h>
#include <xrpl/protocol/UintTypes.h>
#include <xrpl/ledger/helpers/AccountRootHelpers.h>
#include <map>
#include <optional>
#include <set>

namespace xrpla::core::rpc {
namespace {
// One side of a book as xrpld 3.4.1's BookOffers.cpp reads it (validateTakerJSON,
// parseTakerAssetJSON, parseTakerIssuerJSON), with its errors: taker_pays' asset and issuer errors
// are srcCurMalformed and srcIsrMalformed, taker_gets' dstAmtMalformed and dstIsrMalformed.
void check_taker(Json const& taker, std::string const& name) {
    bool currency = taker.is_object() && taker.contains("currency"), mpt = taker.is_object() && taker.contains("mpt_issuance_id");
    if (!currency && !mpt) fail(r::RpcInvalidParams, "Missing field '" + name + ".currency'.");
    if (mpt && (currency || taker.contains("issuer"))) fail(r::RpcInvalidParams, "Invalid field '" + name + "'.");
    if ((currency && !taker.at("currency").is_string()) || (mpt && !taker.at("mpt_issuance_id").is_string()))
        fail(r::RpcInvalidParams, "Invalid field '" + name + ".currency', not string.");
}
// The asset: an Issue, or nullopt for an MPT (whose books this archive does not serve).
std::optional<r::Issue> taker_asset(Json const& taker, std::string const& name) {
    auto code = name == "taker_pays" ? r::RpcSrcCurMalformed : r::RpcDstAmtMalformed;
    if (taker.contains("currency")) {
        r::Issue issue = r::xrpIssue();
        if (!r::toCurrency(issue.currency, taker.at("currency").get<std::string>())) fail(code, "Invalid field '" + name + ".currency', bad currency.");
        return issue;
    }
    r::MPTID id;
    if (!id.parseHex(taker.at("mpt_issuance_id").get<std::string>())) fail(code, "Invalid field '" + name + ".mpt_issuance_id'");
    return std::nullopt;
}
void taker_issuer(r::Issue& issue, Json const& taker, std::string const& name) {
    auto code = name == "taker_pays" ? r::RpcSrcIsrMalformed : r::RpcDstIsrMalformed;
    if (taker.contains("issuer")) {
        if (!taker.at("issuer").is_string()) fail(r::RpcInvalidParams, "Invalid field '" + name + ".issuer', not string.");
        if (!r::toIssuer(issue.account, taker.at("issuer").get<std::string>())) fail(code, "Invalid field '" + name + ".issuer', bad issuer.");
        if (issue.account == r::noAccount()) fail(code, "Invalid field '" + name + ".issuer', bad issuer account one.");
    } else issue.account = r::xrpAccount();
    if (r::isXRP(issue.currency) && !r::isXRP(issue.account)) fail(code, "Unneeded field '" + name + ".issuer' for XRP currency specification.");
    if (!r::isXRP(issue.currency) && r::isXRP(issue.account)) fail(code, "Invalid field '" + name + ".issuer', expected non-XRP issuer.");
}
// What funding a book's offers needs that is the same for the whole page, or for every offer of
// one owner or issuer: read once a request. Before, each offer (up to 100 a page) read the fee
// settings and its issuer again.
struct Funding {
    Context const& ctx;
    std::map<r::AccountID, std::shared_ptr<r::SLE const>> accounts;
    std::optional<LedgerFees> fees;
    std::shared_ptr<r::SLE const> const& account(r::AccountID const& id) {
        auto found = accounts.find(id);
        if (found == accounts.end()) found = accounts.emplace(id, ctx.read(r::keylet::account(id))).first;
        return found->second;
    }
    LedgerFees const& reserves() { if (!fees) fees = ledger_fees(ctx); return *fees; }
    // fixFrozenLPTokenTransfer: an AMM's LP tokens are frozen for a holder of a frozen pool asset.
    bool frozen_lp_tokens() const { return ctx.amendment(r::fixFrozenLPTokenTransfer); }
};
bool global_frozen(Funding& f, r::AccountID const& issuer) {
    auto const& root = f.account(issuer); return root && root->isFlag(r::lsfGlobalFreeze);
}
r::STAmount funds(Funding& f, r::AccountID const& holder, r::Issue const& issue) {
    if (r::isXRP(issue.currency)) {
        auto const& account = f.account(holder);
        if (!account) return r::STAmount{r::XRPAmount{0}};
        auto balance = account->getFieldAmount(r::sfBalance);
        // xrpLiquid: pseudo-accounts (an AMM's, a vault's) keep no reserve. (lsfAMMNode, checked
        // here before, is a trust line flag: on an account root its bit is lsfDepositAuth.)
        if (r::isPseudoAccount(account)) return balance;
        r::STAmount reserve{account_reserve(f.reserves(), *account)};
        if (balance < reserve) balance.clear(); else balance -= reserve;
        return balance;
    }
    // xrpld 3.4.1's getLineIfUsable (TokenHelpers.cpp) with ZeroIfFrozen: nothing from a line that
    // is frozen (isFrozen: the issuer's global freeze, or for a holder other than the issuer its
    // freeze of the line) or deep frozen (isDeepFrozen); with fixFrozenLPTokenTransfer, nothing
    // when the issuer's account is missing, or from an AMM's LP tokens while either pool asset is
    // frozen for the holder (isLPTokenFrozen, View.cpp: isFrozen of each, an MPT's locks included);
    // and nothing from LP tokens while a pool MPT cannot be transferred (canTransferLPToken).
    r::STAmount balance{issue};
    auto line = f.ctx.read(r::keylet::trustLine(holder, issue.account, issue.currency));
    if (!line) return balance;
    if (global_frozen(f, issue.account) || (holder != issue.account && (line->isFlag(issue.account > holder ? r::lsfHighFreeze : r::lsfLowFreeze) ||
                                                                         line->isFlag(r::lsfHighDeepFreeze) || line->isFlag(r::lsfLowDeepFreeze))))
        return balance;
    if (f.frozen_lp_tokens()) {
        auto const& issuer = f.account(issue.account);
        if (!issuer) return balance;
        if (issuer->isFieldPresent(r::sfAMMID)) {
            auto pool = f.ctx.read(r::keylet::amm(issuer->getFieldH256(r::sfAMMID)));
            if (!pool || asset_frozen(f.ctx, holder, (*pool)[r::sfAsset]) || asset_frozen(f.ctx, holder, (*pool)[r::sfAsset2])) return balance;
        }
    }
    if (!lp_tokens_transferable(f.ctx, holder, f.account(issue.account).get())) return balance;
    balance = line->getFieldAmount(r::sfBalance);
    if (holder > issue.account) balance.negate();
    balance.setIssue(issue); if (balance.signum() < 0) balance.clear();
    return balance;
}
}
Json books(Context const& ctx) {
    auto const& p = ctx.params;
    // In xrpld 3.4.1's order: both sides present, objects (or null), well formed, then their
    // assets, then their issuers, the taker, the domain, the market and the limit.
    for (auto name : {"taker_pays", "taker_gets"}) if (!p.contains(name)) fail(r::RpcInvalidParams, std::string("Missing field '") + name + "'.");
    auto const& pays_json = p.at("taker_pays");
    auto const& gets_json = p.at("taker_gets");
    for (auto name : {"taker_pays", "taker_gets"})
        if (!p.at(name).is_object() && !p.at(name).is_null()) fail(r::RpcInvalidParams, std::string("Invalid field '") + name + "', not object.");
    check_taker(pays_json, "taker_pays"); check_taker(gets_json, "taker_gets");
    auto pays_asset = taker_asset(pays_json, "taker_pays"), gets_asset = taker_asset(gets_json, "taker_gets");
    if (pays_asset) taker_issuer(*pays_asset, pays_json, "taker_pays");
    if (gets_asset) taker_issuer(*gets_asset, gets_json, "taker_gets");
    std::optional<r::AccountID> taker_id;
    if (p.contains("taker")) {
        if (!p.at("taker").is_string()) fail(r::RpcInvalidParams, "Invalid field 'taker', not string.");
        taker_id = r::parseBase58<r::AccountID>(p.at("taker").get<std::string>());
        if (!taker_id) fail(r::RpcInvalidParams, "Invalid field 'taker'.");
    }
    std::optional<r::uint256> domain;
    if (p.contains("domain")) {
        r::uint256 key;
        if (!p.at("domain").is_string() || !key.parseHex(p.at("domain").get<std::string>())) fail(r::RpcDomainMalformed, "Unable to parse domain.");
        domain = key;
    }
    if (!pays_asset || !gets_asset) {
        if (pays_json == gets_json) fail(r::RpcBadMarket);
        fail(r::RpcNotSupported, "This archive does not serve MPT order books.");
    }
    auto pays = *pays_asset, gets = *gets_asset;
    if (pays == gets) fail(r::RpcBadMarket);
    r::Book book{pays, gets, domain};
    auto base = raw(r::getBookBase(book));
    auto limit = limit_field(p, 1, 60, 100);
    auto taker = taker_id.value_or(r::AccountID{});
    Funding funding{ctx};
    bool blocked = global_frozen(funding, pays.account) || global_frozen(funding, gets.account);
    r::Rate rate = r::kParityRate;
    if (auto const& issuer = funding.account(gets.account); issuer && issuer->isFieldPresent(r::sfTransferRate)) rate = r::Rate{issuer->getFieldU32(r::sfTransferRate)};
    std::map<r::AccountID, r::STAmount> remaining;
    Json offers = Json::array();
    // rippled's getBookPage: each loop turn reads at most one directory entry, so a quality
    // directory whose pages hold no offers still uses up one unit of the limit.
    auto end = r::getQualityNext(r::uint256::fromVoid(base.data()));
    auto tip = r::uint256::fromVoid(base.data());
    // The price levels in effect, from the store's book index where it keeps one; each takes at
    // least one turn of the limit, so `limit` of them are enough.
    auto levels = ctx.store.book_directories(ctx.header.sequence, base, raw(end), limit);
    // Quality directories often hold a single offer, so the walk below would load one object
    // at a time. Load the first `limit` directories, then all their offers, concurrently first.
    if (auto dirs = levels ? *levels : ctx.store.keys_between(ctx.header.sequence, base, raw(end), limit); !dirs.empty()) {
        ctx.store.prefetch(dirs, ctx.header.sequence);
        std::vector<Bytes> entries;
        for (auto const& key : dirs)
            if (auto dir = ctx.read({r::ltDIR_NODE, r::uint256::fromVoid(key.data())}))
                for (auto const& entry : dir->getFieldV256(r::sfIndexes)) entries.push_back(raw(entry));
        ctx.store.prefetch(entries, ctx.header.sequence);
    }
    std::size_t level = 0;
    auto next_level = [&]() -> std::optional<r::uint256> {
        if (!levels) return successor(ctx, tip, end);
        if (level == levels->size()) return std::nullopt;
        return r::uint256::fromVoid((*levels)[level++].data());
    };
    std::shared_ptr<r::SLE const> page;
    r::uint256 root, entry;
    unsigned index = 0;
    auto next_entry = [&]() {
        std::set<std::uint64_t> seen;
        while (page) {
            auto const& indexes = page->getFieldV256(r::sfIndexes);
            if (index < indexes.size()) { entry = indexes[index++]; return true; }
            auto next = page->getFieldU64(r::sfIndexNext);
            if (!next) { entry.zero(); return false; }
            if (!seen.insert(next).second) throw Error("Book directory cycle");
            page = ctx.read(r::keylet::page(root, next)); index = 0;
        }
        entry.zero(); return false;
    };
    bool done = false, advance = true;
    r::STAmount rate_of_dir;
    for (unsigned turns = limit; !done && turns-- > 0;) {
        if (advance) {
            advance = false;
            auto key = next_level();
            page = key ? ctx.read({r::ltDIR_NODE, *key}) : nullptr;
            if (!page) done = true;
            else { tip = root = *key; rate_of_dir = r::amountFromQuality(r::getQuality(tip)); index = 0; next_entry(); }
        }
        if (done) break;
        if (auto offer = entry.isZero() ? nullptr : ctx.read({r::ltOFFER, entry})) {
            auto owner = offer->getAccountID(r::sfAccount);
            auto selling = offer->getFieldAmount(r::sfTakerGets), buying = offer->getFieldAmount(r::sfTakerPays);
            if (!selling.holds<r::Issue>() || !buying.holds<r::Issue>()) fail(r::RpcNotSupported, "This API version requires currency/issuer order books.");
            auto balance = selling; bool first = true;
            if (owner != gets.account) {
                if (blocked) balance.clear();
                else if (auto it = remaining.find(owner); it != remaining.end()) { balance = it->second; first = false; }
                else balance = funds(funding, owner, gets);
            }
            auto transfer = rate != r::kParityRate && taker != gets.account && owner != gets.account ? rate : r::kParityRate;
            auto available = transfer == r::kParityRate ? balance : r::divide(balance, transfer);
            auto funded = std::min(selling, available);
            auto value = json(*offer);
            if (funded < selling) {
                value["taker_gets_funded"] = json(funded);
                value["taker_pays_funded"] = json(std::min(buying, r::multiply(funded, rate_of_dir, pays)));
            }
            remaining[owner] = balance - (transfer == r::kParityRate ? funded : std::min(balance, r::multiply(funded, transfer)));
            if (first) value["owner_funds"] = balance.getText();
            value["quality"] = rate_of_dir.getText(); offers.push_back(value);
        }
        if (!next_entry()) advance = true;
    }
    auto result = ctx.base(); result["offers"] = offers; return result;
}
}
