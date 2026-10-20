#include "rpc.hpp"
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/ledger/helpers/AccountRootHelpers.h>
#include <xrpl/protocol/Feature.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/nft.h>
#include <charconv>
#include <set>

namespace xrpla::core::rpc {
std::string owned(Context const& ctx, r::AccountID const& owner, std::string const& marker,
    std::uint32_t limit, std::function<void(r::SLE const&)> const& visit, bool include_nfts) {
    auto root = r::keylet::ownerDir(owner);
    std::uint64_t page = 0;
    Bytes after;
    if (!marker.empty()) {
        auto comma = marker.find(',');
        if (comma == std::string::npos) fail(r::RpcInvalidParams, "Invalid marker.");
        after = hash(marker.substr(0, comma));
        auto hint = marker.substr(comma + 1);
        auto [end, ec] = std::from_chars(hint.data(), hint.data() + hint.size(), page);
        if (ec != std::errc{} || end != hint.data() + hint.size()) fail(r::RpcInvalidParams, "Invalid marker.");
    }
    std::set<std::uint64_t> visited;
    std::uint32_t count = 0;
    if (include_nfts && (marker.empty() || page == UINT32_MAX)) {
        if (marker.empty() || after != Bytes(32, '\0')) {
            auto key = marker.empty() ? r::keylet::nftokenPageMax(owner).key : r::uint256::fromVoid(after.data());
            if (raw(key).substr(0, 20) != raw(owner)) fail(r::RpcInvalidParams, "Invalid marker.");
            auto nft = ctx.read({r::ltNFTOKEN_PAGE, key});
            if (!nft && !marker.empty()) fail(r::RpcInvalidParams, "Invalid marker.");
            std::set<r::uint256> nft_pages;
            while (nft) {
                if (!nft_pages.insert(nft->key()).second) throw Error("NFT page cycle");
                visit(*nft); ++count;
                auto previous = nft->getFieldH256(r::sfPreviousPageMin);
                if (count == limit) return upper(to_hex(raw(previous))) + ",4294967295";
                if (previous == r::uint256{}) break;
                nft = ctx.read({r::ltNFTOKEN_PAGE, previous});
                if (!nft) throw Error("NFT page is missing");
            }
        }
        after.clear(); page = 0;
    }
    ctx.preload_directory(root, page, limit - count);
    ctx.plan_directory(root, page, std::min<std::uint32_t>(limit - count, 4096));
    for (;;) {
        if (!visited.insert(page).second) throw Error("Owner directory contains a cycle");
        auto directory = ctx.read(r::keylet::page(root, page));
        if (!directory) {
            if (!after.empty()) fail(r::RpcInvalidParams, "Invalid marker.");
            if (page != 0) throw Error("Owner directory page is missing");
            return {};
        }
        bool found = after.empty();
        for (auto const& key : directory->getFieldV256(r::sfIndexes)) {
            if (!found) { if (raw(key) == after) found = true; continue; }
            auto object = ctx.read({r::ltANY, key});
            if (!object) throw Error("Owner directory references a missing object");
            visit(*object);
            if (++count == limit) return upper(to_hex(raw(key))) + "," + std::to_string(page);
            if (count > 1000000) fail(r::RpcTooBusy);
        }
        if (!found) fail(r::RpcInvalidParams, "Invalid marker.");
        after.clear();
        page = directory->getFieldU64(r::sfIndexNext);
        if (!page) return {};
        ctx.plan_ahead(root, page, visited.size(), limit - count);
    }
}
namespace {
bool enabled(Context const& ctx, r::uint256 const& amendment) { return ctx.amendment(amendment); }
void add_amount(std::map<r::Currency, r::STAmount>& sums, r::STAmount const& amount) {
    auto currency = amount.get<r::Issue>().currency;
    auto it = sums.find(currency);
    if (it == sums.end()) sums.emplace(currency, amount);
    else try { it->second += amount; }
    catch (std::runtime_error const&) { it->second = r::STAmount(it->second.get<r::Issue>(), r::STAmount::kMaxValue, r::STAmount::kMaxOffset); }
}
// gateway_balances' `hotwallet` as xrpld reads it: a string or an array of them (null is an empty
// array), every one an address; otherwise invalidHotWallet in API v1, invalidParams in v2, on top
// of the result's fields.
std::set<r::AccountID> hot_wallets(Context const& ctx, Json const& result) {
    std::set<r::AccountID> hot;
    if (!ctx.params.contains("hotwallet")) return hot;
    auto wallets = ctx.params.at("hotwallet"); if (wallets.is_string()) wallets = Json::array({wallets});
    bool valid = wallets.is_array() || wallets.is_null();
    if (wallets.is_array())
        for (auto const& wallet : wallets) {
            auto parsed = wallet.is_string() ? r::parseBase58<r::AccountID>(wallet.get<std::string>()) : std::nullopt;
            if (parsed) hot.insert(*parsed); else valid = false;
        }
    if (!valid) fail_into(result, ctx.version < 2 ? r::RpcInvalidHotwallet : r::RpcInvalidParams);
    return hot;
}
}
Json accounts(Context const& ctx, std::string const& method) {
    auto const& p = ctx.params;
    static std::set<std::string> const methods{"account_info", "gateway_balances"};
    if (!methods.contains(method)) fail(r::RpcUnknownCommand);
    // As xrpld 3.4.0: `account`, else `ident`; missing both names `account`. gateway_balances
    // also refuses an `ident` that is not a string when `account` is given.
    if (!p.contains("account") && !p.contains("ident")) fail(r::RpcInvalidParams, "Missing field 'account'.");
    if (method == "gateway_balances" && p.contains("account") && p.contains("ident") && !p.at("ident").is_string())
        fail(r::RpcInvalidParams, "Invalid field 'ident'.");
    auto address = text(p, p.contains("account") ? "account" : "ident");
    // account_info injects actMalformed into the ledger's fields; gateway_balances answers it alone.
    if (method == "account_info" && !r::parseBase58<r::AccountID>(address)) fail_into(ctx.base(), r::RpcActMalformed);
    auto owner = account(address);
    auto root = ctx.read(r::keylet::account(owner));
    if (!root) {
        if (method == "gateway_balances") {
            // rippled answers API v1 with just the account; v2 reports it missing.
            auto missing = ctx.base(); missing["account"] = r::toBase58(owner);
            if (ctx.version > 1) fail_into(missing, r::RpcActNotFound);
            hot_wallets(ctx, missing);  // checked all the same
            return missing;
        }
        auto missing = ctx.base(); missing["account"] = r::toBase58(owner);
        fail_into(missing, r::RpcActNotFound);
    }
    auto result = ctx.base();
    if (method == "account_info") {
        // xrpld reads `queue` with asBool and refuses it for a closed ledger named in the request;
        // its default ledger is the open one, which only an upstream has.
        if (loose_boolean(p, "queue")) {
            if (p.contains("ledger_index") || p.contains("ledger_hash") || p.contains("ledger")) fail_into(result, r::RpcInvalidParams);
            fail(r::RpcNotSupported, "Queue data requires a current-ledger upstream request.");
        }
        result["account_data"] = json(*root);
        if (root->isFieldPresent(r::sfEmailHash)) {
            auto md5 = r::strHex(root->getFieldH128(r::sfEmailHash));
            for (auto& c : md5) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            result["account_data"]["urlgravatar"] = "https://www.gravatar.com/avatar/" + md5;
        }
        // As xrpld 3.4.0 (and 3.3.0): these flags always, allowTrustLineLocking once TokenEscrow is enabled.
        Json flags = Json::object();
        std::vector<std::pair<char const*, std::uint32_t>> bits{{"allowTrustLineClawback", r::lsfAllowTrustLineClawback},
            {"defaultRipple", r::lsfDefaultRipple}, {"depositAuth", r::lsfDepositAuth}, {"disableMasterKey", r::lsfDisableMaster},
            {"disallowIncomingCheck", r::lsfDisallowIncomingCheck}, {"disallowIncomingNFTokenOffer", r::lsfDisallowIncomingNFTokenOffer},
            {"disallowIncomingPayChan", r::lsfDisallowIncomingPayChan}, {"disallowIncomingTrustline", r::lsfDisallowIncomingTrustline},
            {"disallowIncomingXRP", r::lsfDisallowXRP}, {"globalFreeze", r::lsfGlobalFreeze}, {"noFreeze", r::lsfNoFreeze},
            {"passwordSpent", r::lsfPasswordSpent}, {"requireAuthorization", r::lsfRequireAuth}, {"requireDestinationTag", r::lsfRequireDestTag}};
        if (enabled(ctx, r::featureTokenEscrow)) bits.emplace_back("allowTrustLineLocking", r::lsfAllowTrustLineLocking);
        for (auto const& [name, flag] : bits) flags[name] = root->isFlag(flag);
        result["account_flags"] = flags;
        for (auto const* field : r::getPseudoAccountFields()) if (root->isFieldPresent(*field)) {
            std::string name = field->fieldName;
            if (name.ends_with("ID")) name.resize(name.size() - 2);
            result["pseudo_account"]["type"] = name;
            break;
        }
        // xrpld: in API v2 `signer_lists` must be a boolean (refused on top of the answer so far); in
        // v1 any value is read with asBool.
        if (ctx.version > 1 && p.contains("signer_lists") && !p.at("signer_lists").is_boolean()) fail_into(result, r::RpcInvalidParams);
        if (loose_boolean(p, "signer_lists")) {
            Json signers = Json::array(); if (auto list = ctx.read(r::keylet::signerList(owner))) signers.push_back(json(*list));
            if (ctx.version == 1) result["account_data"]["signer_lists"] = signers; else result["signer_lists"] = signers;
        }
        return result;
    }
    result["account"] = r::toBase58(owner);
    if (method == "gateway_balances") {
        auto hot = hot_wallets(ctx, result);
        std::map<r::Currency, r::STAmount> obligations, locked;
        Json balances = Json::object(), assets = Json::object(), frozen = Json::object();
        auto append = [](Json& group, r::AccountID const& peer, r::STAmount const& amount) {
            auto name = r::toBase58(peer); if (!group.contains(name)) group[name] = Json::array();
            group[name].push_back({{"currency", r::to_string(amount.get<r::Issue>().currency)}, {"value", amount.getText()}});
        };
        owned(ctx, owner, "", UINT32_MAX, [&](r::SLE const& object) {
            if (object.getType() == r::ltESCROW && method == "gateway_balances") {
                auto const& amount = object.getFieldAmount(r::sfAmount);
                if (amount.holds<r::Issue>()) add_amount(locked, amount);  // XRP escrows count too; MPTs do not
            }
            if (object.getType() != r::ltRIPPLE_STATE) return;
            auto low = object.getFieldAmount(r::sfLowLimit), high = object.getFieldAmount(r::sfHighLimit);
            auto balance = object.getFieldAmount(r::sfBalance); auto lower = low.getIssuer() == owner;
            if (!lower) balance.negate();
            auto peer = lower ? high.getIssuer() : low.getIssuer();
            if (!balance.signum()) return;
            if (hot.contains(peer)) append(balances, peer, -balance);
            else if (balance.signum() > 0) append(assets, peer, balance);
            else if (object.isFlag(lower ? r::lsfLowFreeze : r::lsfHighFreeze)) append(frozen, peer, -balance);
            else add_amount(obligations, -balance);
        });
            if (!balances.empty()) result["balances"] = balances;
            if (!assets.empty()) result["assets"] = assets;
            if (!frozen.empty()) result["frozen_balances"] = frozen;
            if (!obligations.empty()) { result["obligations"] = Json::object(); for (auto const& [currency, amount] : obligations) result["obligations"][r::to_string(currency)] = amount.getText(); }
            if (!locked.empty()) { result["locked"] = Json::object(); for (auto const& [currency, amount] : locked) result["locked"][r::to_string(currency)] = amount.getText(); }
        return result;
    }
    fail(r::RpcUnknownCommand);
}
}
