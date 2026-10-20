// Owner-directory methods, following the rippled 3.2 handlers line for line:
// limits count every visited directory entry, a marker is returned only when
// another entry follows, and markers are checked against the requesting account.
#include "rpc.hpp"
#include <xrpl/ledger/helpers/SponsorHelpers.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/Quality.h>
#include <xrpl/protocol/UintTypes.h>
#include <xrpl/protocol/nft.h>
#include <xrpl/protocol/nftPageMask.h>
#include <charconv>
#include <set>

namespace xrpla::core::rpc {
[[noreturn]] void fail_into(Json result, r::ErrorCodeI code, std::string const& message) {
    auto error = message.empty() ? r::rpc::makeError(code) : r::rpc::makeError(code, message);
    result.update(Json::parse(error.toStyledString()));
    throw Failure{result};
}
[[noreturn]] void fail_named(Json result, std::string const& name) {
    result["error"] = name;
    if (!result.contains("error_message")) result["error_message"] = nullptr;
    throw Failure{result};
}
std::uint32_t limit_field(Json const& p, std::uint32_t lo, std::uint32_t fallback, std::uint32_t hi) {
    if (!p.contains("limit") || p.at("limit").is_null()) return fallback;
    auto const& value = p.at("limit");
    // xrpld's JSON reader reads a number past 32 bits as a real: not an unsigned integer either.
    if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<std::int64_t>() < 0) || value.get<std::uint64_t>() > UINT32_MAX)
        fail(r::RpcInvalidParams, "Invalid field 'limit', not unsigned integer.");
    auto n = value.get<std::uint64_t>();
    if (!n) fail(r::RpcInvalidParams, "Invalid field 'limit'.");
    return static_cast<std::uint32_t>(std::max<std::uint64_t>(lo, std::min<std::uint64_t>(hi, n)));
}
bool for_each_after(Context const& ctx, r::Keylet const& root, r::uint256 const& after, std::uint64_t hint, unsigned limit,
    std::function<bool(std::shared_ptr<r::SLE const> const&)> const& visit) {
    auto current = root;
    bool found = after.isZero();
    if (!found) {
        auto hinted = r::keylet::page(root, hint);
        if (auto dir = ctx.read(hinted))
            for (auto const& key : dir->getFieldV256(r::sfIndexes)) if (key == after) { current = hinted; break; }
    }
    ctx.preload_directory(root, current.key == root.key ? 0 : hint, limit);
    ctx.plan_directory(root, current.key == root.key ? 0 : hint, limit);
    // A marker its hint page does not hold has the walk read from the root until it finds it: as
    // owned(), past 1,000,000 entries passed over (no real directory has as many) it is tooBusy.
    std::set<r::uint256> pages;
    std::uint64_t passed = 0;
    for (;;) {
        if (!pages.insert(current.key).second) throw Error("Directory contains a cycle");
        auto dir = ctx.read(current);
        if (!dir) return found;
        for (auto const& key : dir->getFieldV256(r::sfIndexes)) {
            if (!found) {
                if (key == after) found = true;
                else if (++passed > 1000000) fail(r::RpcTooBusy);
                continue;
            }
            if (visit(ctx.read({r::ltANY, key})) && limit-- <= 1) return true;
        }
        auto next = dir->getFieldU64(r::sfIndexNext);
        if (!next) return found;
        ctx.plan_ahead(root, next, pages.size(), limit);
        current = r::keylet::page(root, next);
    }
}
std::uint64_t start_hint(r::SLE const& sle, r::AccountID const& owner) {
    if (sle.getType() == r::ltRIPPLE_STATE) {
        if (sle.getFieldAmount(r::sfLowLimit).getIssuer() == owner) return sle.getFieldU64(r::sfLowNode);
        if (sle.getFieldAmount(r::sfHighLimit).getIssuer() == owner) return sle.getFieldU64(r::sfHighNode);
    }
    return sle.isFieldPresent(r::sfOwnerNode) ? sle.getFieldU64(r::sfOwnerNode) : 0;
}
std::optional<r::uint256> successor(Context const& ctx, r::uint256 const& key, r::uint256 const& end) {
    // A merge of keys alone where the backend lists them: a page of objects decodes values.
    if (ctx.store.lists_keys()) {
        auto keys = ctx.store.keys_between(ctx.header.sequence, raw(key), raw(end), 1);
        if (keys.empty()) return std::nullopt;
        return r::uint256::fromVoid(keys.front().data());
    }
    auto page = ctx.store.object_page(ctx.header.sequence, to_hex(raw(key)), 1, true);
    if (page.at("state").empty()) return std::nullopt;
    auto next = r::uint256::fromVoid(from_hex(page.at("state").front().at("index").get<std::string>()).data());
    if (next >= end) return std::nullopt;
    return next;
}

namespace {
bool related(std::shared_ptr<r::SLE const> const& sle, r::AccountID const& owner) {
    if (sle->getType() == r::ltRIPPLE_STATE)
        return sle->getFieldAmount(r::sfLowLimit).getIssuer() == owner || sle->getFieldAmount(r::sfHighLimit).getIssuer() == owner;
    if (sle->isFieldPresent(r::sfAccount))
        return sle->getAccountID(r::sfAccount) == owner || (sle->isFieldPresent(r::sfDestination) && sle->getAccountID(r::sfDestination) == owner);
    if (sle->getType() == r::ltSIGNER_LIST) return sle->key() == r::keylet::signerList(owner).key;
    if (sle->getType() == r::ltNFTOKEN_OFFER) return sle->getAccountID(r::sfOwner) == owner;
    return false;
}
// "INDEX,HINT"; `field_errors` selects rippled's invalidFieldError(marker) over rpcINVALID_PARAMS.
std::pair<r::uint256, std::uint64_t> owner_marker(Context const& ctx, r::AccountID const& owner, bool field_errors) {
    auto const& p = ctx.params;
    if (!p.contains("marker")) return {};
    if (!p.at("marker").is_string()) fail(r::RpcInvalidParams, "Invalid field 'marker', not string.");
    auto bad = [&]() { if (field_errors) fail(r::RpcInvalidParams, "Invalid field 'marker'."); fail(r::RpcInvalidParams); };
    auto text = p.at("marker").get<std::string>();
    auto comma = text.find(',');
    r::uint256 after;
    if (comma == std::string::npos || !after.parseHex(text.substr(0, comma))) bad();
    auto tail = text.substr(comma + 1);
    if (auto second = tail.find(','); second != std::string::npos) tail = tail.substr(0, second);
    // xrpld 3.4.1's toUInt64 (beast::lexicalCastChecked): decimal digits, a leading '+' allowed.
    if (!tail.empty() && tail.front() == '+') tail.erase(0, 1);
    std::uint64_t hint = 0;
    auto [end, ec] = std::from_chars(tail.data(), tail.data() + tail.size(), hint);
    if (tail.empty() || ec != std::errc{} || end != tail.data() + tail.size()) bad();
    auto sle = ctx.read({r::ltANY, after});
    if (!sle || !related(sle, owner)) fail(r::RpcInvalidParams);
    return {after, hint};
}
// The account a method names. A malformed one is actMalformed with the ledger's fields, as xrpld's
// injectError leaves them (account_lines, _offers, _objects, _currencies), or alone, as its
// rpcError does (account_channels, account_nfts).
r::AccountID owner_of(Context const& ctx, char const* field = "account", bool bare = false) {
    auto address = text(ctx.params, field);
    auto parsed = r::parseBase58<r::AccountID>(address);
    if (!parsed) { if (bare) fail(r::RpcActMalformed); fail_into(ctx.base(), r::RpcActMalformed); }
    return *parsed;
}
std::string hex_key(r::uint256 const& key) { return r::to_string(key); }

Json line_json(r::SLE const& line, r::AccountID const& owner) {
    auto low = line.getFieldAmount(r::sfLowLimit), high = line.getFieldAmount(r::sfHighLimit);
    auto balance = line.getFieldAmount(r::sfBalance);
    bool lower = low.getIssuer() == owner;
    if (!lower) balance.negate();
    auto flags = line.getFieldU32(r::sfFlags);
    Json result{{"account", r::toBase58(lower ? high.getIssuer() : low.getIssuer())},
        {"balance", balance.getText()}, {"currency", r::to_string(balance.get<r::Issue>().currency)},
        {"limit", (lower ? low : high).getText()}, {"limit_peer", (lower ? high : low).getText()},
        {"quality_in", line.getFieldU32(lower ? r::sfLowQualityIn : r::sfHighQualityIn)},
        {"quality_out", line.getFieldU32(lower ? r::sfLowQualityOut : r::sfHighQualityOut)}};
    for (auto const& [name, flag] : std::vector<std::pair<char const*, std::uint32_t>>{
        {"authorized", lower ? r::lsfLowAuth : r::lsfHighAuth}, {"peer_authorized", lower ? r::lsfHighAuth : r::lsfLowAuth},
        {"no_ripple", lower ? r::lsfLowNoRipple : r::lsfHighNoRipple}, {"no_ripple_peer", lower ? r::lsfHighNoRipple : r::lsfLowNoRipple},
        {"freeze", lower ? r::lsfLowFreeze : r::lsfHighFreeze}, {"freeze_peer", lower ? r::lsfHighFreeze : r::lsfLowFreeze},
        {"deep_freeze", lower ? r::lsfLowDeepFreeze : r::lsfHighDeepFreeze}, {"deep_freeze_peer", lower ? r::lsfHighDeepFreeze : r::lsfLowDeepFreeze}})
        if (flags & flag) result[name] = true;
    return result;
}

// Owner-directory walk with rippled's limit + 1 look-ahead; `take` sees every visited entry.
Json paged(Context const& ctx, r::AccountID const& owner, std::uint32_t limit, std::pair<r::uint256, std::uint64_t> start,
    std::function<void(std::shared_ptr<r::SLE const> const&)> const& take) {
    auto result = ctx.base();
    unsigned count = 0; std::optional<r::uint256> marker; std::uint64_t next_hint = 0;
    if (!for_each_after(ctx, r::keylet::ownerDir(owner), start.first, start.second, limit + 1, [&](std::shared_ptr<r::SLE const> const& sle) {
            if (!sle) return false;
            if (++count == limit) { marker = sle->key(); next_hint = start_hint(*sle, owner); }
            if (count <= limit) take(sle);
            return true;
        })) fail(r::RpcInvalidParams);
    if (count == limit + 1 && marker) { result["limit"] = limit; result["marker"] = hex_key(*marker) + "," + std::to_string(next_hint); }
    return result;
}

Json account_lines(Context const& ctx) {
    auto owner = owner_of(ctx);
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    std::optional<r::AccountID> peer;
    if (ctx.params.contains("peer")) {
        if (!ctx.params.at("peer").is_string()) fail(r::RpcInvalidParams, "Invalid field 'peer'.");  // xrpld 3.4.0
        auto text = ctx.params.at("peer").get<std::string>();
        if (!text.empty()) { peer = r::parseBase58<r::AccountID>(text); if (!peer) fail_into(ctx.base(), r::RpcActMalformed); }
    }
    auto limit = limit_field(ctx.params, 10, 200, 400);
    bool ignore_default = loose_boolean(ctx.params, "ignore_default");  // xrpld: asBool
    auto start = owner_marker(ctx, owner, false);
    Json lines = Json::array();
    auto result = paged(ctx, owner, limit, start, [&](std::shared_ptr<r::SLE const> const& sle) {
        if (sle->getType() != r::ltRIPPLE_STATE) return;
        if (ignore_default && !sle->isFlag(sle->getFieldAmount(r::sfLowLimit).getIssuer() == owner ? r::lsfLowReserve : r::lsfHighReserve)) return;
        if (peer) {
            // The line's other side, compared before rendering it.
            auto low = sle->getFieldAmount(r::sfLowLimit).getIssuer();
            if ((low == owner ? sle->getFieldAmount(r::sfHighLimit).getIssuer() : low) != *peer) return;
        }
        lines.push_back(line_json(*sle, owner));
    });
    result["account"] = r::toBase58(owner); result["lines"] = lines;
    return result;
}

Json account_offers(Context const& ctx) {
    auto owner = owner_of(ctx);
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    auto limit = limit_field(ctx.params, 10, 200, 400);
    auto start = owner_marker(ctx, owner, true);
    Json offers = Json::array();
    auto result = paged(ctx, owner, limit, start, [&](std::shared_ptr<r::SLE const> const& sle) {
        if (sle->getType() != r::ltOFFER) return;
        Json offer{{"taker_pays", json(sle->getFieldAmount(r::sfTakerPays))}, {"taker_gets", json(sle->getFieldAmount(r::sfTakerGets))},
            {"seq", sle->getFieldU32(r::sfSequence)}, {"flags", sle->getFieldU32(r::sfFlags)},
            {"quality", r::amountFromQuality(r::getQuality(sle->getFieldH256(r::sfBookDirectory))).getText()}};
        if (sle->isFieldPresent(r::sfExpiration)) offer["expiration"] = sle->getFieldU32(r::sfExpiration);
        offers.push_back(offer);
    });
    result["account"] = r::toBase58(owner); result["offers"] = offers;
    return result;
}

Json account_channels(Context const& ctx) {
    auto const& p = ctx.params;
    auto owner = owner_of(ctx, "account", true);
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    std::optional<r::AccountID> destination;
    if (p.contains("destination_account")) {
        // As xrpld: not a string is an invalid field; an empty one filters nothing.
        if (!p.at("destination_account").is_string()) fail(r::RpcInvalidParams, "Invalid field 'destination_account'.");
        if (auto named = p.at("destination_account").get<std::string>(); !named.empty()) {
            destination = r::parseBase58<r::AccountID>(named);
            if (!destination) fail(r::RpcActMalformed);
        }
    }
    auto limit = limit_field(p, 10, 200, 400);
    auto start = owner_marker(ctx, owner, false);
    Json channels = Json::array();
    auto result = paged(ctx, owner, limit, start, [&](std::shared_ptr<r::SLE const> const& line) {
        if (line->getType() != r::ltPAYCHAN || line->getAccountID(r::sfAccount) != owner) return;
        if (destination && *destination != line->getAccountID(r::sfDestination)) return;
        Json c{{"channel_id", hex_key(line->key())}, {"account", r::toBase58(line->getAccountID(r::sfAccount))},
            {"destination_account", r::toBase58(line->getAccountID(r::sfDestination))}, {"amount", line->getFieldAmount(r::sfAmount).getText()},
            {"balance", line->getFieldAmount(r::sfBalance).getText()}, {"settle_delay", line->getFieldU32(r::sfSettleDelay)}};
        auto key = line->getFieldVL(r::sfPublicKey);
        if (r::publicKeyType(r::makeSlice(key))) {
            r::PublicKey pk(r::makeSlice(key));
            c["public_key"] = r::toBase58(r::TokenType::AccountPublic, pk); c["public_key_hex"] = r::strHex(pk);
        }
        if (line->isFieldPresent(r::sfExpiration)) c["expiration"] = line->getFieldU32(r::sfExpiration);
        if (line->isFieldPresent(r::sfCancelAfter)) c["cancel_after"] = line->getFieldU32(r::sfCancelAfter);
        if (line->isFieldPresent(r::sfSourceTag)) c["source_tag"] = line->getFieldU32(r::sfSourceTag);
        if (line->isFieldPresent(r::sfDestinationTag)) c["destination_tag"] = line->getFieldU32(r::sfDestinationTag);
        channels.push_back(c);
    });
    result["account"] = r::toBase58(owner); result["channels"] = channels;
    return result;
}

Json account_currencies(Context const& ctx) {
    auto owner = owner_of(ctx, ctx.params.contains("account") ? "account" : "ident");
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    std::set<r::Currency> send, receive;
    for_each_after(ctx, r::keylet::ownerDir(owner), r::uint256{}, 0, UINT32_MAX, [&](std::shared_ptr<r::SLE const> const& sle) {
        if (!sle || sle->getType() != r::ltRIPPLE_STATE) return false;
        auto low = sle->getFieldAmount(r::sfLowLimit), high = sle->getFieldAmount(r::sfHighLimit);
        bool lower = low.getIssuer() == owner;
        auto balance = sle->getFieldAmount(r::sfBalance);
        if (!lower) balance.negate();
        auto limit = lower ? low : high, peer = lower ? high : low;
        if (balance < limit) receive.insert(balance.get<r::Issue>().currency);
        if (-balance < peer) send.insert(balance.get<r::Issue>().currency);
        return false;
    });
    send.erase(r::badCurrency()); receive.erase(r::badCurrency());
    auto result = ctx.base();
    result["send_currencies"] = Json::array(); result["receive_currencies"] = Json::array();
    for (auto const& c : send) result["send_currencies"].push_back(r::to_string(c));
    for (auto const& c : receive) result["receive_currencies"].push_back(r::to_string(c));
    return result;
}

Json account_nfts(Context const& ctx) {
    auto const& p = ctx.params;
    auto owner = owner_of(ctx, "account", true);
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    auto limit = limit_field(p, 20, 100, 400);
    r::uint256 marker;
    bool marker_set = p.contains("marker");
    if (marker_set) {
        if (!p.at("marker").is_string()) fail(r::RpcInvalidParams, "Invalid field 'marker', not string.");
        if (!marker.parseHex(p.at("marker").get<std::string>())) fail(r::RpcInvalidParams, "Invalid field 'marker'.");
    }
    auto first = r::keylet::nftokenPage(r::keylet::nftokenPageMin(owner), marker);
    auto last = r::keylet::nftokenPageMax(owner);
    auto cp = ctx.read({r::ltNFTOKEN_PAGE, successor(ctx, first.key, last.key.next()).value_or(last.key)});
    auto result = ctx.base();
    Json nfts = Json::array();
    unsigned count = 0;
    bool past = marker.isZero(), found = false;
    auto masked_marker = marker & r::nft::kPageMask;
    std::set<r::uint256> seen;
    while (cp) {
        if (!seen.insert(cp->key()).second) throw Error("NFT page cycle");
        for (auto const& o : cp->getFieldArray(r::sfNFTokens)) {
            auto id = o.getFieldH256(r::sfNFTokenID);
            auto masked = id & r::nft::kPageMask;
            if (!past) {
                if (masked < masked_marker) continue;
                if (masked == masked_marker && id < marker) continue;
                if (id == marker) { found = true; continue; }
            }
            if (marker_set && !found) fail(r::RpcInvalidParams, "Invalid field 'marker'.");
            past = true;
            auto token = json(o);
            token["Flags"] = r::nft::getFlags(id); token["Issuer"] = r::toBase58(r::nft::getIssuer(id));
            token["NFTokenTaxon"] = r::nft::toUInt32(r::nft::getTaxon(id)); token["nft_serial"] = r::nft::getSequence(id);
            if (auto fee = r::nft::getTransferFee(id)) token["TransferFee"] = fee;
            nfts.push_back(token);
            if (++count == limit) {
                result["account_nfts"] = nfts; result["limit"] = limit; result["marker"] = hex_key(id);
                return result;
            }
        }
        cp = cp->isFieldPresent(r::sfNextPageMin) ? ctx.read({r::ltNFTOKEN_PAGE, cp->getFieldH256(r::sfNextPageMin)}) : nullptr;
    }
    if (marker_set && !found) fail(r::RpcInvalidParams, "Invalid field 'marker'.");
    result["account_nfts"] = nfts; result["account"] = r::toBase58(owner);
    return result;
}

// The ledger entry type for a `type` filter: rippled accepts the canonical name
// (case-insensitive) or the RPC name.
std::optional<r::LedgerEntryType> entry_type_of(Json const& p) {
    if (!p.contains("type")) return std::nullopt;
    if (!p.at("type").is_string()) fail(r::RpcInvalidParams, "Invalid field 'type', not string.");
    auto filter = p.at("type").get<std::string>();
    auto lower = [](std::string s) { for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); return s; };
    static std::vector<std::tuple<std::string, std::string, r::LedgerEntryType>> const types{
#pragma push_macro("LEDGER_ENTRY")
#undef LEDGER_ENTRY
#define LEDGER_ENTRY(tag, value, name, rpcName, ...) {#name, #rpcName, r::tag},
#include <xrpl/protocol/detail/ledger_entries.macro>
#undef LEDGER_ENTRY
#pragma pop_macro("LEDGER_ENTRY")
    };
    for (auto const& [name, rpc_name, type] : types) if (lower(name) == lower(filter) || rpc_name == filter) return type;
    fail(r::RpcInvalidParams, "Invalid field 'type'.");
}

Json account_objects(Context const& ctx) {
    auto const& p = ctx.params;
    auto owner = owner_of(ctx);
    if (!ctx.read(r::keylet::account(owner))) fail(r::RpcActNotFound);
    std::optional<std::vector<r::LedgerEntryType>> filter;
    if (loose_boolean(p, "deletion_blockers_only")) {  // xrpld: asBool
        static std::vector<std::pair<std::string, r::LedgerEntryType>> const blockers{{"check", r::ltCHECK}, {"escrow", r::ltESCROW},
            {"nft_page", r::ltNFTOKEN_PAGE}, {"payment_channel", r::ltPAYCHAN}, {"state", r::ltRIPPLE_STATE},
            {"xchain_owned_claim_id", r::ltXCHAIN_OWNED_CLAIM_ID}, {"xchain_owned_create_account_claim_id", r::ltXCHAIN_OWNED_CREATE_ACCOUNT_CLAIM_ID},
            {"bridge", r::ltBRIDGE}, {"mpt_issuance", r::ltMPTOKEN_ISSUANCE}, {"mptoken", r::ltMPTOKEN},
            {"permissioned_domain", r::ltPERMISSIONED_DOMAIN}, {"vault", r::ltVAULT}, {"sponsorship", r::ltSPONSORSHIP}};
        filter.emplace();
        for (auto const& [name, type] : blockers) {
            if (p.contains("type") && (!p.at("type").is_string() || p.at("type").get<std::string>() != name)) continue;
            filter->push_back(type);
        }
    } else if (auto type = entry_type_of(p)) {
        if (*type == r::ltAMENDMENTS || *type == r::ltDIR_NODE || *type == r::ltFEE_SETTINGS || *type == r::ltLEDGER_HASHES || *type == r::ltNEGATIVE_UNL)
            fail(r::RpcInvalidParams, "Invalid field 'type'.");
        filter = std::vector<r::LedgerEntryType>{*type};
    }
    auto limit = limit_field(p, 10, 200, 400);
    r::uint256 dir_index, entry_index;
    if (p.contains("marker")) {
        if (!p.at("marker").is_string()) fail(r::RpcInvalidParams, "Invalid field 'marker', not string.");
        auto text = p.at("marker").get<std::string>();
        auto comma = text.find(',');
        if (comma == std::string::npos || !dir_index.parseHex(text.substr(0, comma)) || !entry_index.parseHex(text.substr(comma + 1)))
            fail(r::RpcInvalidParams, "Invalid field 'marker'.");
    }
    std::optional<bool> sponsored;
    if (p.contains("sponsored")) {
        if (!p.at("sponsored").is_boolean()) fail(r::RpcInvalidParams, "Invalid field 'sponsored', not boolean.");
        sponsored = p.at("sponsored").get<bool>();
    }
    // As xrpld: an entry the `sponsored` filter leaves out still counts toward the limit.
    auto sponsored_matches = [&](std::optional<r::AccountID> const& sponsor) { return !sponsored || sponsor.has_value() == *sponsored; };
    auto invalid_marker = [] { fail(r::RpcInvalidParams, "Invalid field 'marker'."); };
    auto result = ctx.base();
    Json objects = Json::array();
    auto matches = [&](r::LedgerEntryType type) { return !filter || std::find(filter->begin(), filter->end(), type) != filter->end(); };
    auto finish = [&](std::optional<std::string> marker) {
        result["account_objects"] = objects;
        if (marker) { result["limit"] = limit; result["marker"] = *marker; }
        result["account"] = r::toBase58(owner);
        return result;
    };
    if (!dir_index.isZero() && !ctx.read({r::ltDIR_NODE, dir_index})) invalid_marker();
    bool nft_pages = matches(r::ltNFTOKEN_PAGE) && dir_index.isZero();
    auto first_page = r::keylet::nftokenPageMin(owner);
    if (nft_pages && entry_index.isNonZero() && first_page.key != (entry_index & ~r::nft::kPageMask)) nft_pages = false;
    std::uint32_t left = limit;
    if (nft_pages) {
        r::Keylet first = entry_index.isZero() ? first_page : r::Keylet{r::ltNFTOKEN_PAGE, entry_index};
        auto last = r::keylet::nftokenPageMax(owner);
        auto key = successor(ctx, first.key, last.key.next()).value_or(last.key);
        auto page = ctx.read({r::ltNFTOKEN_PAGE, key});
        while (page) {
            if (sponsored_matches(page->isFieldPresent(r::sfSponsor) ? std::optional(page->getAccountID(r::sfSponsor)) : std::nullopt))
                objects.push_back(json(*page));
            auto next = page->isFieldPresent(r::sfNextPageMin) ? std::optional<r::uint256>(page->getFieldH256(r::sfNextPageMin)) : std::nullopt;
            page = next ? ctx.read({r::ltNFTOKEN_PAGE, *next}) : nullptr;
            if (--left == 0 && page) return finish("0," + hex_key(key));
            if (!next) break;
            key = *next;
        }
        entry_index = r::uint256{};
    }
    auto root = r::keylet::ownerDir(owner);
    bool start_found = false;
    if (dir_index.isZero()) { dir_index = root.key; start_found = true; ctx.plan_directory(root, 0, left); }
    auto dir = ctx.read({r::ltDIR_NODE, dir_index});
    if (!dir) return finish(std::nullopt);
    std::uint32_t added = 0;
    std::set<r::uint256> seen;
    for (;;) {
        if (!seen.insert(dir_index).second) throw Error("Owner directory contains a cycle");
        auto const& entries = dir->getFieldV256(r::sfIndexes);
        auto it = entries.begin();
        if (!start_found) {
            it = std::find(it, entries.end(), entry_index);
            if (it == entries.end()) invalid_marker();
            start_found = true;
        }
        if (added == left && left < limit && it != entries.end()) return finish(hex_key(dir_index) + "," + hex_key(*it));
        for (; it != entries.end(); ++it) {
            auto sle = ctx.read({r::ltANY, *it});
            if (!sle) throw Error("Owner directory references a missing object");
            // A trust line counts as sponsored whichever side's reserve is sponsored.
            std::optional<r::AccountID> sponsor;
            if (sle->getType() == r::ltRIPPLE_STATE) {
                sponsor = r::getLedgerEntryReserveSponsorID(sle, r::sfHighSponsor);
                if (!sponsor) sponsor = r::getLedgerEntryReserveSponsorID(sle, r::sfLowSponsor);
            } else if (r::isLedgerEntrySupportedBySponsorship(*sle)) {
                sponsor = r::getLedgerEntryReserveSponsorID(sle);
            }
            if (matches(sle->getType()) && sponsored_matches(sponsor)) objects.push_back(json(*sle));
            if (++added == left) {
                if (++it != entries.end()) return finish(hex_key(dir_index) + "," + hex_key(*it));
                break;
            }
        }
        auto next = dir->getFieldU64(r::sfIndexNext);
        if (!next) return finish(std::nullopt);
        dir_index = r::keylet::page(root, next).key;
        dir = ctx.read({r::ltDIR_NODE, dir_index});
        if (!dir) return finish(std::nullopt);
        if (added == left) {
            auto const& following = dir->getFieldV256(r::sfIndexes);
            return finish(following.empty() ? std::nullopt : std::optional<std::string>(hex_key(dir_index) + "," + hex_key(*following.begin())));
        }
    }
}
}

std::optional<r::LedgerEntryType> entry_type(Json const& p) { return entry_type_of(p); }
bool is_owner_method(std::string const& method) {
    static std::set<std::string> const names{"account_lines", "account_offers", "account_channels", "account_currencies", "account_nfts", "account_objects"};
    return names.contains(method);
}
Json owner_methods(Context const& ctx, std::string const& method) {
    if (method == "account_lines") return account_lines(ctx);
    if (method == "account_offers") return account_offers(ctx);
    if (method == "account_channels") return account_channels(ctx);
    if (method == "account_currencies") return account_currencies(ctx);
    if (method == "account_nfts") return account_nfts(ctx);
    if (method == "account_objects") return account_objects(ctx);
    fail(r::RpcUnknownCommand);
}
}
