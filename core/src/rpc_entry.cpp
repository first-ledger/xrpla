// ledger_entry as xrpld 3.3.0 answers it (src/xrpld/rpc/handlers/ledger/LedgerEntry.cpp): one
// selector per request (each ledger entry type's RPC name, plus index, account_root and
// ripple_state), parsed and refused as xrpld does, and the entry's type checked against the
// selector's. With Clio's include_deleted, an entry deleted at or before the ledger is
// answered as it was before its deletion, with deleted_ledger_index.
#include "rpc.hpp"
#include <xrpl/json/json_reader.h>
#include <xrpl/ledger/helpers/CredentialHelpers.h>
#include <xrpl/protocol/Asset.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/Issue.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/protocol/Protocol.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STXChainBridge.h>
#include <xrpl/protocol/UintTypes.h>
#include <charconv>
#include <cmath>
#include <functional>
#include <limits>
#include <map>

namespace xrpla::core::rpc {
namespace {
// Errors as xrpld's LedgerEntryHelpers make them: a name of their own with invalidParams' code.
[[noreturn]] void malformed(std::string const& error, std::string const& message) {
    throw Failure{Json{{"error", error}, {"error_code", static_cast<int>(r::RpcInvalidParams)}, {"error_message", message}}};
}
[[noreturn]] void invalid_field(std::string const& error, std::string const& field, std::string const& type) {
    malformed(error, "Invalid field '" + field + "', not " + type + ".");
}
[[noreturn]] void missing_field(std::string const& field, std::string const& error = "malformedRequest") {
    malformed(error, "Missing field '" + field + "'.");
}
bool present(Json const& p, std::string const& field) { return p.is_object() && p.contains(field) && !p.at(field).is_null(); }

std::optional<r::uint256> hash256(Json const& v) {
    r::uint256 x;
    if (!v.is_string() || !x.parseHex(v.get<std::string>())) return std::nullopt;
    return x;
}
std::optional<r::uint192> hash192(Json const& v) {
    r::uint192 x;
    if (!v.is_string() || !x.parseHex(v.get<std::string>())) return std::nullopt;
    return x;
}
std::optional<r::AccountID> account_id(Json const& v) {
    if (!v.is_string()) return std::nullopt;
    auto id = r::parseBase58<r::AccountID>(v.get<std::string>());
    if (!id || id->isZero()) return std::nullopt;
    return id;
}
// A nonnegative 32-bit integer, or one written as a decimal string.
std::optional<std::uint32_t> uint32(Json const& v) {
    if (v.is_number_unsigned()) { auto n = v.get<std::uint64_t>(); if (n <= UINT32_MAX) return static_cast<std::uint32_t>(n); return std::nullopt; }
    if (v.is_number_integer()) { auto n = v.get<std::int64_t>(); if (n >= 0 && n <= UINT32_MAX) return static_cast<std::uint32_t>(n); return std::nullopt; }
    if (v.is_string()) {
        auto s = v.get<std::string>(); std::uint32_t n = 0;
        auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
        if (!s.empty() && ec == std::errc{} && end == s.data() + s.size()) return n;
    }
    return std::nullopt;
}
// libxrpl's own JSON value, for its parsers of issues and assets.
json::Value native_json(Json const& v) {
    json::Value value;
    if (!json::Reader().parse(v.dump(), value)) throw std::runtime_error("Unreadable JSON");
    return value;
}
std::optional<r::Asset> asset(Json const& v) {
    try { return r::assetFromJson(native_json(v)); } catch (std::runtime_error const&) { return std::nullopt; }
}
std::optional<r::Issue> issue(Json const& v) {
    try { return r::issueFromJson(native_json(v)); } catch (std::runtime_error const&) { return std::nullopt; }
}

template<class T> T required(Json const& p, std::string const& field, std::string const& error, std::string const& type, std::optional<T> (*parse)(Json const&)) {
    if (!present(p, field)) missing_field(field);
    if (auto value = parse(p.at(field))) return *value;
    invalid_field(error, field, type);
}
r::AccountID required_account(Json const& p, std::string const& field, std::string const& error) { return required(p, field, error, "AccountID", account_id); }
std::uint32_t required_uint32(Json const& p, std::string const& field, std::string const& error) { return required(p, field, error, "number", uint32); }
void has_required(Json const& p, std::initializer_list<char const*> fields, std::string const& error = "malformedRequest") {
    for (auto field : fields) if (!present(p, field)) missing_field(field, error);
}
Bytes required_blob(Json const& p, std::string const& field, std::size_t most, std::string const& error) {
    if (!present(p, field)) missing_field(field);
    auto const& v = p.at(field);
    if (v.is_string()) {
        auto blob = r::strUnHex(v.get<std::string>());
        if (blob && !blob->empty() && blob->size() <= most) return Bytes(blob->begin(), blob->end());
    }
    invalid_field(error, field, "hex string");
}
r::uint256 object_id(Json const& v, std::string const& field, std::string const& type = "hex string or object") {
    if (auto id = hash256(v)) return *id;
    invalid_field("malformedRequest", field, type);
}
// Entries at a fixed key: `true` names them, or their key does.
r::uint256 fixed(r::Keylet const& keylet, Json const& v, std::string const& field) {
    if (!v.is_boolean()) return object_id(v, field, "hex string");
    if (!v.get<bool>()) invalid_field("invalidParams", field, "true");
    return keylet.key;
}
r::STXChainBridge bridge_fields(Json const& p) {
    has_required(p, {"LockingChainDoor", "LockingChainIssue", "IssuingChainDoor", "IssuingChainIssue"});
    auto locking_door = required_account(p, "LockingChainDoor", "malformedLockingChainDoor");
    auto issuing_door = required_account(p, "IssuingChainDoor", "malformedIssuingChainDoor");
    auto locking_issue = issue(p.at("LockingChainIssue"));
    if (!locking_issue) invalid_field("malformedIssue", "LockingChainIssue", "Issue");
    auto issuing_issue = issue(p.at("IssuingChainIssue"));
    if (!issuing_issue) invalid_field("malformedIssue", "IssuingChainIssue", "Issue");
    return r::STXChainBridge(locking_door, *locking_issue, issuing_door, *issuing_issue);
}
r::STArray authorize_credentials(Json const& v) {
    if (!v.is_array()) invalid_field("malformedAuthorizedCredentials", "authorized_credentials", "array");
    if (v.size() > r::kMaxCredentialsArraySize) malformed("malformedAuthorizedCredentials", "Invalid field 'authorized_credentials', array too long.");
    if (v.empty()) malformed("malformedAuthorizedCredentials", "Invalid field 'authorized_credentials', array empty.");
    r::STArray list(r::sfAuthorizeCredentials, static_cast<int>(v.size()));
    for (auto const& item : v) {
        if (!item.is_object()) invalid_field("malformedAuthorizedCredentials", "authorized_credentials", "array of objects");
        has_required(item, {"issuer", "credential_type"}, "malformedAuthorizedCredentials");
        auto issuer = required_account(item, "issuer", "malformedAuthorizedCredentials");
        auto type = required_blob(item, "credential_type", r::kMaxCredentialTypeLength, "malformedAuthorizedCredentials");
        auto credential = r::STObject::makeInnerObject(r::sfCredential);
        credential.setAccountID(r::sfIssuer, issuer);
        credential.setFieldVL(r::sfCredentialType, r::Slice(type.data(), type.size()));
        list.pushBack(std::move(credential));
    }
    return list;
}

// The key a selector names: `value` is the selector's field, `params` the whole request.
using Parser = std::function<r::uint256(Json const& value, Json const& params, std::string const& field)>;
std::map<std::string, Parser> const& parsers() {
    static std::map<std::string, Parser> const table = [] {
        std::map<std::string, Parser> t;
        auto hex_only = [](Json const& v, Json const&, std::string const& field) { return object_id(v, field, "hex string"); };
        auto account = [](Json const& v, Json const&, std::string const& field) {
            auto id = account_id(v);
            if (!id) invalid_field("malformedAddress", field, "AccountID");
            return r::keylet::account(*id).key;
        };
        t["index"] = hex_only;
        t["account"] = account;
        t["account_root"] = account;
        t["amendments"] = [](Json const& v, Json const&, std::string const& field) { return fixed(r::keylet::amendments(), v, field); };
        t["fee"] = [](Json const& v, Json const&, std::string const& field) { return fixed(r::keylet::feeSettings(), v, field); };
        t["nunl"] = [](Json const& v, Json const&, std::string const& field) { return fixed(r::keylet::negativeUNL(), v, field); };
        t["hashes"] = [](Json const& v, Json const&, std::string const& field) {
            // A ledger index names the "long" skip list holding it; otherwise the "short" one.
            if (v.is_number_integer()) {
                if ((!v.is_number_unsigned() && v.get<std::int64_t>() < 0) || v.get<std::uint64_t>() > UINT32_MAX) fail(r::RpcInvalidParams);
                return r::keylet::skip(static_cast<r::LedgerIndex>(v.get<std::uint64_t>())).key;
            }
            return fixed(r::keylet::skip(), v, field);
        };
        for (auto name : {"check", "nft_offer", "nft_page", "payment_channel", "signer_list"}) t[name] = hex_only;
        t["did"] = [](Json const& v, Json const&, std::string const& field) {
            auto id = account_id(v);
            if (!id) invalid_field("malformedAddress", field, "AccountID");
            return r::keylet::did(*id).key;
        };
        t["directory"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            std::uint64_t sub = 0;
            if (v.contains("sub_index") && !v.at("sub_index").is_null()) {
                auto const& s = v.at("sub_index");
                // As xrpld's isConvertibleTo(UInt): an integer from 0 to 2^32 - 1, or a real in that
                // range within an epsilon of a whole number, which asUInt truncates.
                bool convertible = s.is_number_float()
                    ? s.get<double>() >= 0 && s.get<double>() <= UINT32_MAX && std::fabs(std::round(s.get<double>()) - s.get<double>()) < std::numeric_limits<double>::epsilon()
                    : s.is_number_unsigned() ? s.get<std::uint64_t>() <= UINT32_MAX : s.is_number_integer() && s.get<std::int64_t>() >= 0;
                if (!convertible) invalid_field("malformedRequest", "sub_index", "number");
                sub = s.is_number_float() ? static_cast<std::uint32_t>(s.get<double>()) : s.get<std::uint64_t>();
            }
            if (v.contains("owner") == v.contains("dir_root")) malformed("malformedRequest", "Must have exactly one of `owner` and `dir_root` fields.");
            if (v.contains("dir_root")) {
                auto root = hash256(v.at("dir_root"));
                if (!root) invalid_field("malformedDirRoot", "dir_root", "hash");
                return r::keylet::page(*root, sub).key;
            }
            auto owner = account_id(v.at("owner"));
            if (!owner) invalid_field("malformedAddress", "owner", "AccountID");
            return r::keylet::page(r::keylet::ownerDir(*owner), sub).key;
        };
        t["escrow"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto owner = required_account(v, "owner", "malformedOwner");
            return r::keylet::escrow(owner, r::SeqProxy::rawSequence(required_uint32(v, "seq", "malformedSeq"))).key;
        };
        t["loan_broker"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field, "hex string");
            auto owner = required_account(v, "owner", "malformedOwner");
            return r::keylet::loanBroker(owner, r::SeqProxy::rawSequence(required_uint32(v, "seq", "malformedSeq"))).key;
        };
        t["loan"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field, "hex string");
            auto broker = required(v, "loan_broker_id", "malformedBroker", "Hash256", hash256);
            return r::keylet::loan(broker, r::SeqProxy::rawSequence(required_uint32(v, "loan_seq", "malformedSeq"))).key;
        };
        t["mptoken"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto issuance = required(v, "mpt_issuance_id", "malformedMPTIssuanceID", "Hash192", hash192);
            return r::keylet::mptoken(issuance, required_account(v, "account", "malformedAccount")).key;
        };
        // An issuance is named by its 192-bit ID, not by its key.
        t["mpt_issuance"] = [](Json const& v, Json const&, std::string const& field) {
            auto id = hash192(v);
            if (!id) invalid_field("malformedMPTokenIssuance", field, "Hash192");
            return r::keylet::mptokenIssuance(*id).key;
        };
        t["offer"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto owner = required_account(v, "account", "malformedAddress");
            return r::keylet::offer(owner, r::SeqProxy::rawSequence(required_uint32(v, "seq", "malformedRequest"))).key;
        };
        t["oracle"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto owner = required_account(v, "account", "malformedAccount");
            return r::keylet::oracle(owner, required_uint32(v, "oracle_document_id", "malformedDocumentID")).key;
        };
        t["permissioned_domain"] = [](Json const& v, Json const&, std::string const& field) {
            if (v.is_string()) return object_id(v, field);
            if (!v.is_object()) invalid_field("malformedRequest", field, "hex string or object");
            auto owner = required_account(v, "account", "malformedAddress");
            return r::keylet::permissionedDomain(owner, r::SeqProxy::rawSequence(required_uint32(v, "seq", "malformedRequest"))).key;
        };
        t["sponsorship"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto sponsor = required_account(v, "sponsor", "malformedSponsor");
            return r::keylet::sponsorship(sponsor, required_account(v, "sponsee", "malformedSponsee")).key;
        };
        t["ticket"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto owner = required_account(v, "account", "malformedAddress");
            return r::keylet::ticket(owner, r::SeqProxy::rawTicket(required_uint32(v, "ticket_seq", "malformedRequest"))).key;
        };
        t["vault"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto owner = required_account(v, "owner", "malformedOwner");
            return r::keylet::vault(owner, r::SeqProxy::rawSequence(required_uint32(v, "seq", "malformedRequest"))).key;
        };
        t["delegate"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto owner = required_account(v, "account", "malformedAddress");
            return r::keylet::delegate(owner, required_account(v, "authorize", "malformedAddress")).key;
        };
        t["credential"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto subject = required_account(v, "subject", "malformedRequest");
            auto issuer = required_account(v, "issuer", "malformedRequest");
            auto type = required_blob(v, "credential_type", r::kMaxCredentialTypeLength, "malformedRequest");
            return r::keylet::credential(subject, issuer, r::Slice(type.data(), type.size())).key;
        };
        t["deposit_preauth"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            if (v.contains("authorized") == v.contains("authorized_credentials"))
                malformed("malformedRequest", "Must have exactly one of `authorized` and `authorized_credentials`.");
            auto owner = required_account(v, "owner", "malformedOwner");
            if (v.contains("authorized")) {
                auto authorized = account_id(v.at("authorized"));
                if (!authorized) invalid_field("malformedAuthorized", "authorized", "AccountID");
                return r::keylet::depositPreauth(owner, *authorized).key;
            }
            auto sorted = r::credentials::makeSorted(authorize_credentials(v.at("authorized_credentials")));
            if (sorted.empty()) invalid_field("malformedAuthorizedCredentials", "authorized_credentials", "array");
            return r::keylet::depositPreauth(owner, sorted).key;
        };
        auto state = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            has_required(v, {"currency", "accounts"});
            auto const& accounts = v.at("accounts");
            if (!accounts.is_array() || accounts.size() != 2) invalid_field("malformedRequest", "accounts", "length-2 array of Accounts");
            auto a = account_id(accounts[0]), b = account_id(accounts[1]);
            if (!a || !b) invalid_field("malformedAddress", "accounts", "array of Accounts");
            if (*a == *b) malformed("malformedRequest", "Cannot have a trustline to self.");
            r::Currency currency;
            auto const& c = v.at("currency");
            if (!c.is_string() || c.get<std::string>().empty() || !r::toCurrency(currency, c.get<std::string>()))
                invalid_field("malformedCurrency", "currency", "Currency");
            return r::keylet::trustLine(*a, *b, currency).key;
        };
        t["state"] = state;
        t["ripple_state"] = state;
        t["amm"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            has_required(v, {"asset", "asset2"});
            auto first = required(v, "asset", "malformedRequest", "Asset", asset);
            return r::keylet::amm(first, required(v, "asset2", "malformedRequest", "Asset", asset)).key;
        };
        // The bridge is the only selector with a field beside it (bridge_account).
        t["bridge"] = [](Json const&, Json const& p, std::string const& field) {
            if (!present(p, "bridge")) missing_field("bridge");
            if (p.at("bridge").is_string()) return object_id(p, field);  // as xrpld: it checks the request, not the field
            auto bridge = bridge_fields(p.at("bridge"));
            auto door = required_account(p, "bridge_account", "malformedBridgeAccount");
            auto chain = r::STXChainBridge::srcChain(door == bridge.lockingChainDoor());
            if (door != bridge.door(chain)) malformed("malformedRequest", "");
            return r::keylet::bridge(bridge, chain).key;
        };
        t["xchain_owned_claim_id"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto bridge = bridge_fields(v);
            return r::keylet::xChainClaimID(bridge, required_uint32(v, "xchain_owned_claim_id", "malformedXChainOwnedClaimID")).key;
        };
        t["xchain_owned_create_account_claim_id"] = [](Json const& v, Json const&, std::string const& field) {
            if (!v.is_object()) return object_id(v, field);
            auto bridge = bridge_fields(v);
            return r::keylet::xChainCreateAccountClaimID(bridge, required_uint32(v, "xchain_owned_create_account_claim_id",
                                                                                 "malformedXChainOwnedCreateAccountClaimID")).key;
        };
        return t;
    }();
    return table;
}
// Each selector and the entry type it names (ltANY: any), in xrpld's order: each type's RPC
// name, then index and the aliases.
std::vector<std::pair<std::string, r::LedgerEntryType>> const& selectors() {
    static std::vector<std::pair<std::string, r::LedgerEntryType>> const list = [] {
        std::vector<std::pair<std::string, r::LedgerEntryType>> l;
#pragma push_macro("LEDGER_ENTRY")
#undef LEDGER_ENTRY
#define LEDGER_ENTRY(tag, value, name, rpcName, ...) l.emplace_back(#rpcName, r::tag);
#include <xrpl/protocol/detail/ledger_entries.macro>
#undef LEDGER_ENTRY
#pragma pop_macro("LEDGER_ENTRY")
        l.emplace_back("index", r::ltANY); l.emplace_back("account_root", r::ltACCOUNT_ROOT); l.emplace_back("ripple_state", r::ltRIPPLE_STATE);
        return l;
    }();
    return list;
}
}

void check_ledger_entry(Json const& p) {
    unsigned count = 0;
    for (auto const& [name, type] : selectors()) if (p.contains(name) && ++count > 1) fail(r::RpcInvalidParams, "Too many fields provided.");
}

Json ledger_entry(Context const& ctx) {
    auto const& p = ctx.params;
    auto result = ctx.base();
    std::optional<r::uint256> key;
    auto expected = r::ltANY;
    for (auto const& [name, type] : selectors()) {
        if (!p.contains(name)) continue;
        auto parser = parsers().find(name);
        if (parser == parsers().end()) fail(r::RpcNotSupported, "The " + name + " selector is not supported by this archive API.");
        try { key = parser->second(p.at(name), p, name); }
        catch (Json::exception const&) { fail(r::RpcInvalidParams); }
        expected = type;
        break;
    }
    if (!key) {
        if (ctx.version < 2) fail_named(result, "unknownOption");
        fail(r::RpcInvalidParams, "No ledger_entry params provided.");
    }
    bool deleted = boolean(p, "include_deleted"), binary = loose_boolean(p, "binary");  // xrpld: asBool
    auto id = raw(*key);
    result["index"] = upper(to_hex(id));
    if (key->isZero()) fail_into(result, r::RpcEntryNotFound);
    auto value = ctx.store.object(id, ctx.header.sequence);
    if (value.empty() && deleted) {
        if (auto found = ctx.store.deleted_object(id, ctx.header.sequence)) {
            result["deleted_ledger_index"] = found->first;
            value = std::move(found->second);
        }
    }
    if (value.empty()) fail_into(result, r::RpcEntryNotFound);
    if (expected != r::ltANY) {
        r::SerialIter it(value.data(), value.size());
        if (r::SLE(it, *key).getType() != expected) fail_into(result, r::RpcUnexpectedLedgerType);
    }
    if (binary) result["node_binary"] = upper(to_hex(value));
    else { result["node"] = object_json({id, value}); result["node"]["index"] = upper(to_hex(id)); }
    return result;
}
}
