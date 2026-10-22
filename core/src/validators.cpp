// Validator lists and validation quorums (validators.hpp).
#include "validators.hpp"
#include "ws_client.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/basics/StringUtilities.h>
#include <xrpl/basics/base64.h>
#include <xrpl/json/json_writer.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <xrpl/protocol/STValidation.h>
#include <xrpl/protocol/SField.h>
#include <xrpl/protocol/STObject.h>
#include <xrpl/protocol/tokens.h>
#include <xrpl/server/Manifest.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <iterator>
#include <vector>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
Bytes raw(r::PublicKey const& key) { return Bytes(reinterpret_cast<char const*>(key.data()), key.size()); }
Bytes hex_bytes(std::string const& text, char const* what) {
    auto decoded = r::strUnHex(text);
    if (!decoded) throw Error(std::string("Invalid hex in ") + what);
    return Bytes(decoded->begin(), decoded->end());
}
r::PublicKey public_key(Bytes const& bytes, char const* what) {
    if (!r::publicKeyType(r::makeSlice(bytes))) throw Error(std::string("Invalid public key in ") + what);
    return r::PublicKey(r::makeSlice(bytes));
}
constexpr std::size_t max_manifest_base64 = 480;  // xrpld's kMaxManifestBase64 (Manifest.h): 358 bytes in base64
// A list publisher's manifest, read as xrpld's applyList reads one (ValidatorList.cpp:1023-1040):
// no longer than a manifest can be in base64, so that one followed by more (base64Decode stops
// at the first character not base64) is refused. Its signatures are not checked here.
r::Manifest publisher_manifest(std::string const& base64, char const* what) {
    if (base64.size() > max_manifest_base64) throw Error(std::string("Manifest for ") + what + " is longer than a manifest can be");
    auto manifest = r::deserializeManifest(r::base64Decode(base64), beast::Journal{beast::Journal::getNullSink()});
    if (!manifest) throw Error(std::string("Invalid manifest for ") + what);
    return std::move(*manifest);
}
// Of these masters, those not on the negative UNL: the validations xrpld counts (negativeUNLFilter).
std::size_t outside(std::set<Bytes> const& masters, std::set<Bytes> const& negative_unl) {
    return static_cast<std::size_t>(std::count_if(masters.begin(), masters.end(), [&](Bytes const& master) { return !negative_unl.contains(master); }));
}
// Whether a number in a validator list is one xrpld takes (ValidatorSite.cpp:403-406,
// ValidatorList.cpp:1278-1282): a JSON integer its reader holds as a signed 32-bit one
// (json::Value::isInt: written without a fraction or an exponent, from -2^31 to 2^31 - 1).
bool list_int(Json const& value) {
    if (value.is_number_unsigned()) return value.get<std::uint64_t>() <= static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max());
    return value.is_number_integer() && value.get<std::int64_t>() >= std::numeric_limits<std::int32_t>::min() &&
           value.get<std::int64_t>() <= std::numeric_limits<std::int32_t>::max();
}
// Reads a JSON document as xrpld's reader does (json_reader.cpp), noting what it refuses that
// nlohmann takes: a value nested more than 25 deep (kNestLimit), an integer outside -2^31 to
// 2^32 - 1, including one too large for nlohmann to hold as an integer, and a key twice in an
// object.
struct XrpldReader : Json::json_sax_t {
    std::vector<std::set<std::string>> open;  // the arrays and objects being read: an object's keys so far
    bool refused = false;
    bool one_value() {
        if (open.size() > 25) refused = true;
        return true;
    }
    bool null() override { return one_value(); }
    bool boolean(bool) override { return one_value(); }
    bool number_integer(Json::number_integer_t number) override {
        if (number < std::numeric_limits<std::int32_t>::min() || number > std::numeric_limits<std::uint32_t>::max()) refused = true;
        return one_value();
    }
    bool number_unsigned(Json::number_unsigned_t number) override {
        if (number > std::numeric_limits<std::uint32_t>::max()) refused = true;
        return one_value();
    }
    bool number_float(Json::number_float_t, Json::string_t const& text) override {
        if (text.find_first_of(".eE") == std::string::npos) refused = true;  // written as an integer, beyond 64 bits
        return one_value();
    }
    bool string(Json::string_t&) override { return one_value(); }
    bool binary(Json::binary_t&) override { return one_value(); }
    bool start_object(std::size_t) override {
        one_value();
        open.emplace_back();
        return true;
    }
    bool key(Json::string_t& name) override {
        if (!open.back().insert(name).second) refused = true;
        return true;
    }
    bool end_object() override { open.pop_back(); return true; }
    bool start_array(std::size_t) override {
        one_value();
        open.emplace_back();
        return true;
    }
    bool end_array() override { open.pop_back(); return true; }
    bool parse_error(std::size_t, std::string const&, Json::exception const&) override { return false; }
};
constexpr std::size_t max_blobs = 5;  // xrpld's kMaxSupportedBlobs (ValidatorList.h)
constexpr std::uint32_t flag_interval = 256;  // xrpld's kFlagLedgerInterval (Protocol.h)
// The pending lists xrpld's applyLists keeps for a publisher (ValidatorList.cpp:913-927): by
// sequence, one of each (the first given), those after the list in force (`in_force`, its
// sequence), each unless the next takes effect no later.
std::vector<ValidatorList> pending_after(std::vector<ValidatorList> lists, std::uint32_t in_force) {
    std::stable_sort(lists.begin(), lists.end(), [](ValidatorList const& a, ValidatorList const& b) { return a.sequence < b.sequence; });
    lists.erase(std::unique(lists.begin(), lists.end(), [](ValidatorList const& a, ValidatorList const& b) { return a.sequence == b.sequence; }), lists.end());
    std::vector<ValidatorList> kept;
    for (std::size_t at = 0; at < lists.size(); ++at)
        if (lists[at].sequence > in_force && (at + 1 == lists.size() || lists[at + 1].effective > lists[at].effective)) kept.push_back(std::move(lists[at]));
    return kept;
}
}

Json parse_as_xrpld(std::string const& text) {
    XrpldReader reader;
    if (!Json::sax_parse(text, &reader) || reader.refused) throw Error("Not a JSON document xrpld reads");
    auto document = Json::parse(text);
    if (!document.is_object() && !document.is_array() && !document.is_null()) throw Error("A JSON document xrpld reads is an object or an array");
    return document;
}
std::size_t ValidatorList::quorum(std::set<Bytes> const& negative_unl) const {
    auto size = signing_keys.size(), effective = size;
    for (auto const& [master, signing] : signing_keys) if (negative_unl.contains(master)) --effective;
    return static_cast<std::size_t>(std::max(std::ceil(effective * 0.8f), std::ceil(size * 0.6f)));
}
NegativeUnl negative_unl(Bytes const& serialized) {
    NegativeUnl unl;
    if (serialized.empty()) return unl;
    r::SerialIter it(serialized.data(), serialized.size());
    r::STLedgerEntry sle(it, r::keylet::negativeUNL().key);
    if (sle.getType() != r::ltNEGATIVE_UNL) throw Error("Not a NegativeUNL object");
    auto key = [](r::Blob const& bytes) { return Bytes(bytes.begin(), bytes.end()); };
    // As Ledger::negativeUNL: an entry that is not a public key is passed over.
    if (sle.isFieldPresent(r::sfDisabledValidators))
        for (auto const& disabled : sle.getFieldArray(r::sfDisabledValidators))
            if (disabled.isFieldPresent(r::sfPublicKey) && r::publicKeyType(r::makeSlice(disabled.getFieldVL(r::sfPublicKey))))
                unl.disabled.insert(key(disabled.getFieldVL(r::sfPublicKey)));
    // As Ledger::updateNegativeUNL on the next flag ledger.
    unl.next = unl.disabled;
    if (sle.isFieldPresent(r::sfValidatorToReEnable)) unl.next.erase(key(sle.getFieldVL(r::sfValidatorToReEnable)));
    if (sle.isFieldPresent(r::sfValidatorToDisable) && r::publicKeyType(r::makeSlice(sle.getFieldVL(r::sfValidatorToDisable))))
        unl.next.insert(key(sle.getFieldVL(r::sfValidatorToDisable)));
    return unl;
}
std::uint64_t xrpl_seconds_now() {
    auto unix = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return static_cast<std::uint64_t>(unix) - 946684800;  // 2000-01-01T00:00:00Z
}

ValidatorList verify_validator_list(Json const& published, std::string const& publisher_key, std::uint64_t now) {
    std::optional<PublisherManifest> publisher;
    return verify_validator_list(published, publisher_key, now, publisher);
}
ValidatorList verify_validator_list(Json const& published, std::string const& publisher_key, std::uint64_t now, std::optional<PublisherManifest>& publisher) {
    auto pinned = hex_bytes(publisher_key, "the pinned list key");
    if (hex_bytes(published.at("public_key").get<std::string>(), "the list") != pinned) throw Error("Validator list is from a different publisher");
    // As xrpld's verify (ValidatorList.cpp:1250-1266) and ManifestCache::applyManifest
    // (Manifest.cpp:400-473): a blob's publisher manifest (its own, else the list's) applies when
    // newer than the newest seen and its signatures verify, and the blob is checked against the
    // key the newest names. One not newer changes nothing, its signatures not checked (Stale), nor
    // does a newer one naming the key in use already (BadEphemeralKey): the blob is checked
    // against the key in use. A revoked key stands (its manifest has the highest sequence). Once
    // the key is revoked no list is taken (xrpld would check one against the master key itself
    // then, ManifestCache::getSigningKey).
    auto signer_for = [&](std::string const& base64, char const* what) {
        auto manifest = publisher_manifest(base64, what);
        if (raw(manifest.masterKey) != pinned) throw Error(std::string("Manifest for ") + what + " names a different master key");
        if (publisher && manifest.sequence <= publisher->sequence) return publisher->signing_key;
        if (!manifest.verify()) throw Error(std::string("Invalid manifest for ") + what);
        auto signing = manifest.revoked() ? std::nullopt : std::optional(raw(*manifest.signingKey));
        if (!publisher || !signing || signing != publisher->signing_key) publisher = PublisherManifest{manifest.sequence, signing};
        return publisher->signing_key;
    };
    ValidatorList revoked;
    revoked.sequence = revoked_sequence;
    if (publisher && !publisher->signing_key) return revoked;
    // Version 1 carries one blob; version 2 carries several, each effective from a time. As
    // xrpld's parseJsonResponse and parseBlobs (ValidatorSite.cpp:403-413, ValidatorList.cpp:
    // 396-450), the whole list fails unless its manifest is a string and its version an integer;
    // one of version 1 carries a blob and its signature, strings, and no blobs_v2; one of version
    // 2 carries 1 to 5 blobs in blobs_v2, each with its signature and, if any, a manifest of its
    // own, strings, and no blob or signature beside them.
    auto has_string = [](Json const& object, char const* name) { return object.contains(name) && object.at(name).is_string(); };
    if (!published.is_object() || !has_string(published, "manifest") || !published.contains("version") || !list_int(published.at("version")))
        throw Error("Validator list lacks its manifest or version");
    struct Blob { std::string blob, signature, manifest; char const* what; };
    std::vector<Blob> blobs;
    auto version = published.at("version").get<std::int64_t>();
    if (version == 1) {
        if (!has_string(published, "blob") || !has_string(published, "signature") || published.contains("blobs_v2"))
            throw Error("Validator list of version 1 lacks its blob or signature, or carries blobs_v2");
        blobs.push_back({published.at("blob").get<std::string>(), published.at("signature").get<std::string>(),
                         published.at("manifest").get<std::string>(), "the list publisher"});
    } else if (version == 2) {
        if (!published.contains("blobs_v2") || !published.at("blobs_v2").is_array() || published.at("blobs_v2").empty() ||
            published.at("blobs_v2").size() > max_blobs || published.contains("blob") || published.contains("signature"))
            throw Error("Validator list of version 2 lacks 1 to 5 blobs in blobs_v2, or carries a blob or signature beside them");
        for (auto const& entry : published.at("blobs_v2")) {
            if (!entry.is_object() || !has_string(entry, "blob") || !has_string(entry, "signature") || (entry.contains("manifest") && !entry.at("manifest").is_string()))
                throw Error("Validator list blob lacks its blob or signature");
            // A blob may carry a publisher manifest of its own.
            bool own = entry.contains("manifest");
            blobs.push_back({entry.at("blob").get<std::string>(), entry.at("signature").get<std::string>(),
                             (own ? entry : published).at("manifest").get<std::string>(), own ? "a list blob" : "the list publisher"});
        }
    } else {
        throw Error("Unsupported validator list version");
    }
    // As xrpld's applyLists, each blob is judged on its own: one whose manifest or signature does
    // not verify, or whose body is not a list as xrpld's reader and verify read one (Untrusted,
    // Invalid), is passed over, and the list fails only when every blob is. One whose sequence,
    // expiration or effective time is negative ends the list there: xrpld's verify throws then
    // (json::Value::asUInt, json_value.cpp:638-641), and applyLists takes no blob after it
    // (ValidatorSite.cpp:612-616), those before it taken.
    std::vector<ValidatorList> lists;
    std::optional<std::string> refused;  // why the first blob passed over was
    bool taken = false;
    for (auto const& entry : blobs) {
        try {
            auto signer = signer_for(entry.manifest, entry.what);
            if (!signer) return revoked;
            auto blob = r::base64Decode(entry.blob);
            auto signature = hex_bytes(entry.signature, "the list signature");
            if (!r::verify(public_key(*signer, "the list publisher manifest"), r::makeSlice(blob), r::makeSlice(signature)))
                throw Error("Validator list signature differs from the publisher's key");
            auto body = parse_as_xrpld(blob);
            if (!body.is_object() || !body.contains("sequence") || !body.contains("expiration") || !body.contains("validators"))
                throw Error("Validator list blob lacks its sequence, expiration or validators");
            if (!list_int(body.at("sequence")) || !list_int(body.at("expiration")) || (body.contains("effective") && !list_int(body.at("effective"))))
                throw Error("Validator list blob's sequence, expiration or effective time is not an integer of -2^31 to 2^31 - 1");
            if (!body.at("validators").is_array()) throw Error("Validator list blob's validators are not an array");
            auto number = [&](char const* name) { return body.contains(name) ? body.at(name).get<std::int64_t>() : std::int64_t{0}; };
            if (number("sequence") < 0 || number("expiration") < 0 || number("effective") < 0) {
                if (!refused) refused = "Validator list blob's sequence, expiration or effective time is negative";
                break;
            }
            auto sequence = static_cast<std::uint32_t>(number("sequence"));
            auto expiration = static_cast<std::uint64_t>(number("expiration"));
            auto effective = static_cast<std::uint64_t>(number("effective"));
            if (expiration <= effective) throw Error("Validator list blob expires before it takes effect");
            taken = true;
            // Of the sequence xrpld gives a publisher with no list yet (SameSequence).
            if (sequence == 0) continue;
            ValidatorList list;
            list.sequence = sequence;
            list.effective = effective;
            list.expiration = expiration;
            // As xrpld's applyList (ValidatorList.cpp:1140-1160, 990-1008): a listed key that is
            // not a public key is passed over; a validator signs with its master key until a
            // manifest that verifies names another (ManifestCache::getSigningKey), and a manifest
            // of a validator not listed, or not newer than one already applied, changes nothing.
            std::vector<std::string> manifests;
            for (auto const& v : body.at("validators")) {
                if (!v.is_object() || !v.contains("validation_public_key") || !v.at("validation_public_key").is_string()) continue;
                auto key = r::strUnHex(v.at("validation_public_key").get<std::string>());
                if (!key || !r::publicKeyType(r::makeSlice(*key))) continue;
                Bytes master(key->begin(), key->end());
                list.signing_keys[master] = master;
                if (v.contains("manifest") && v.at("manifest").is_string()) manifests.push_back(v.at("manifest").get<std::string>());
            }
            std::set<Bytes> listed;
            for (auto const& [master, signing] : list.signing_keys) listed.insert(master);
            for (auto const& text : manifests) {
                auto manifest = r::deserializeManifest(r::base64Decode(text), beast::Journal{beast::Journal::getNullSink()});
                if (!manifest || !manifest->verify()) continue;
                auto master = raw(manifest->masterKey);
                auto known = list.manifest_sequences.find(master);
                if (!listed.contains(master) || (known != list.manifest_sequences.end() && manifest->sequence <= known->second)) continue;
                list.manifest_sequences[master] = manifest->sequence;
                if (manifest->revoked() || !manifest->signingKey) list.signing_keys.erase(master);  // a revoked validator cannot validate
                else list.signing_keys[master] = raw(*manifest->signingKey);
            }
            lists.push_back(std::move(list));
        } catch (std::exception const& error) {
            if (!refused) refused = error.what();
        }
    }
    if (!taken && refused) throw Error(*refused);
    // Each blob whose time has come with a later sequence replaces the one before (an earlier one
    // is stale), expired or not: xrpld's verify judges the sequence first (Stale, SameSequence),
    // then the expiration, and puts an expired one in force (Expired). Those in force later wait.
    ValidatorList chosen;
    std::vector<ValidatorList> pending;
    for (auto& list : lists)
        if (list.effective > now) pending.push_back(std::move(list));
        else if (list.sequence > chosen.sequence) chosen = std::move(list);
    chosen.pending = pending_after(std::move(pending), chosen.sequence);
    return chosen;
}
ValidatorList validating_list(ValidatorList list, std::uint64_t now) {
    if (list.sequence == revoked_sequence) throw Error("List publisher key is revoked");
    if (!list.sequence || (list.expiration && list.expiration <= now)) throw Error("No validator list is in force (expired or not yet effective)");
    if (list.signing_keys.empty()) throw Error("Validator list names no validators");
    return list;
}

ValidationQuorum::ValidationQuorum(ValidatorList list, std::function<std::uint64_t()> now) : list_(std::move(list)), now_(std::move(now)) {
    for (auto const& [master, signing] : list_.signing_keys) master_of_[signing] = master;
}
std::optional<Bytes> ValidationQuorum::master_for(Bytes const& signing_key) const {
    // As xrpld's ManifestCache::getMasterKey: a key no manifest names as a signing key stands
    // for itself, so a listed validator's master key is trusted as well.
    if (auto master = master_of_.find(signing_key); master != master_of_.end()) return master->second;
    if (list_.signing_keys.contains(signing_key)) return signing_key;
    return std::nullopt;
}
std::optional<Bytes> manifest_from_stream(Json const& message) {
    if (message.value("type", "") != "manifestReceived" || !message.contains("master_key") || !message.contains("seq")) return std::nullopt;
    auto master = r::parseBase58<r::PublicKey>(r::TokenType::NodePublic, message.at("master_key").get<std::string>());
    if (!master) return std::nullopt;
    r::STObject object(r::sfGeneric);
    object[r::sfPublicKey] = *master;
    object[r::sfSequence] = message.at("seq").get<std::uint32_t>();
    if (message.contains("signing_key")) {
        auto signing = r::parseBase58<r::PublicKey>(r::TokenType::NodePublic, message.at("signing_key").get<std::string>());
        if (!signing) return std::nullopt;
        object[r::sfSigningPubKey] = *signing;
    }
    if (message.contains("domain")) {
        auto domain = message.at("domain").get<std::string>();
        object[r::sfDomain] = r::makeSlice(domain);
    }
    if (message.contains("signature")) { auto s = hex_bytes(message.at("signature").get<std::string>(), "manifest signature"); object[r::sfSignature] = r::makeSlice(s); }
    if (message.contains("master_signature")) { auto s = hex_bytes(message.at("master_signature").get<std::string>(), "manifest master signature"); object[r::sfMasterSignature] = r::makeSlice(s); }
    r::Serializer out;
    object.add(out);
    return Bytes(reinterpret_cast<char const*>(out.data()), out.size());
}
Json manifest_request(Bytes const& master, int id) {
    return {{"id", id}, {"command", "manifest"}, {"public_key", r::toBase58(r::TokenType::NodePublic, public_key(master, "a listed validator"))}};
}
std::optional<Bytes> manifest_from_answer(Json const& message) {
    // As xrpld's doManifest answers: the manifest it holds for the key, base64.
    if (message.value("type", "") != "response" || !message.contains("result") || !message.at("result").is_object()) return std::nullopt;
    auto const& result = message.at("result");
    if (!result.contains("manifest") || !result.at("manifest").is_string()) return std::nullopt;
    auto decoded = r::base64Decode(result.at("manifest").get<std::string>());
    if (decoded.empty()) return std::nullopt;
    return Bytes(decoded.begin(), decoded.end());
}
bool ValidationQuorum::manifest(Bytes const& serialized) {
    auto parsed = r::deserializeManifest(r::makeSlice(serialized), beast::Journal{beast::Journal::getNullSink()});
    if (!parsed) return false;
    // A listed validator's newer manifest only (any, for one signing with its master key), found
    // before its signatures are checked: peers relay manifests of every validator they know, and
    // any peer can send junk ones, each a signature check that would otherwise run for nothing
    // (under the caller's lock).
    auto master = raw(parsed->masterKey);
    auto known = list_.manifest_sequences.find(master);
    if (known == list_.manifest_sequences.end() ? !list_.signing_keys.contains(master) : parsed->sequence <= known->second) return false;
    if (!parsed->verify()) return false;
    if (auto old = list_.signing_keys.find(master); old != list_.signing_keys.end()) master_of_.erase(old->second);
    list_.manifest_sequences[master] = parsed->sequence;
    if (parsed->revoked() || !parsed->signingKey) {
        list_.signing_keys.erase(master);
        for (auto& [key, signers] : ledgers_) signers.masters.erase(master);
        return true;
    }
    list_.signing_keys[master] = raw(*parsed->signingKey);
    master_of_[raw(*parsed->signingKey)] = master;
    return true;
}
namespace {
// A ledger's key in ValidationQuorum's maps: its hash, then its sequence as 4 big-endian bytes.
Bytes ledger_key(Bytes key, std::uint32_t sequence) {
    for (int shift = 24; shift >= 0; shift -= 8) key.push_back(static_cast<char>(sequence >> shift));
    return key;
}
std::uint32_t sequence_of(Bytes const& key) {
    std::uint32_t sequence = 0;
    for (auto at = key.size() - 4; at < key.size(); ++at) sequence = sequence << 8 | static_cast<unsigned char>(key[at]);
    return sequence;
}
}
std::optional<ValidationQuorum::Counted> ValidationQuorum::add(Bytes const& serialized, std::chrono::steady_clock::time_point now) {
    try {
        r::SerialIter it(serialized.data(), serialized.size());
        r::STValidation validation(it, [](r::PublicKey const& key) { return r::calcNodeID(key); }, {true, true});
        if (!validation.isValid()) return std::nullopt;
        auto signer = raw(validation.getSignerPublic());
        auto master = master_for(signer);
        if (!master) return std::nullopt;
        auto hash = validation.getLedgerHash();
        auto sequence = validation.getFieldU32(r::sfLedgerSequence);
        auto key = ledger_key(Bytes(reinterpret_cast<char const*>(hash.data()), hash.size()), sequence);
        // xrpld's checks of the validator's sequence (see validators.hpp), a partial validation's
        // too: its newest validation is forgotten 10 minutes after it was seen. The same ledger
        // again passes only after a full validation of it (counted already).
        bool full = validation.isFull();
        auto& newest = newest_[*master];
        bool current = !newest.key.empty() && now <= newest.when + std::chrono::minutes(10);
        if (current && (sequence < newest.ledger_index || (sequence == newest.ledger_index && (key != newest.key || !newest.full)))) return std::nullopt;
        if (!current || sequence > newest.ledger_index) newest = {sequence, key, now, full};
        // Only full validations count for a ledger (xrpld's checkAccept counts those alone).
        if (!full) return std::nullopt;
        // Trimmed before this ledger's entry is taken: forget_old may erase it.
        if (++counted_ % trim_every == 0) forget_old();
        auto& signers = ledgers_[key];
        signers.ledger_index = sequence;
        signers.masters.insert(*master);
        return judged(key, signers);
    } catch (std::exception const&) {
        return std::nullopt;  // malformed or badly signed
    }
}
Json watch_validators(ValidatorsOptions const& options) {
    auto text = options.list.starts_with("https://") || options.list.starts_with("http://") ? fetch_url(options.list, 16 << 20) : read_file(options.list, 16 << 20);
    auto now = xrpl_seconds_now();
    auto list = validating_list(verify_validator_list(parse_as_xrpld(text), options.list_key, now), now);
    Json result{{"list", options.list}, {"sequence", list.sequence}, {"expiration_unix", list.expiration + 946684800},
                {"validators", list.signing_keys.size()}, {"quorum", list.quorum()}};
    if (options.node.empty()) return result;
    ValidationQuorum quorum(std::move(list));
    WsClient client(options.node);
    client.send({{"id", 1}, {"command", "subscribe"}, {"streams", {"validations", "manifests"}}});
    std::set<Bytes> reported;
    Json ledgers = Json::array();
    std::uint64_t seen = 0, counted = 0;
    auto started = std::chrono::steady_clock::now();
    while (ledgers.size() < options.ledgers) {
        auto message = client.receive();
        if (auto manifest = manifest_from_stream(message)) { (void)quorum.manifest(*manifest); continue; }
        if (message.value("type", "") != "validationReceived" || !message.contains("data")) continue;
        ++seen;
        auto result = quorum.add(from_hex(message.at("data").get<std::string>()));
        if (!result) continue;
        ++counted;
        // No ledger's state is read here, so the negative UNL is not known: a ledger is reported
        // once it reaches the quorum of the whole list.
        if (result->count < quorum.list().quorum() || !reported.insert(result->ledger_hash).second) continue;
        Json ledger{{"ledger_index", result->ledger_index}, {"ledger_hash", to_hex(result->ledger_hash)}, {"trusted_validations", result->count},
                    {"seconds", std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count()}};
        std::cerr << ledger.dump() << '\n';
        ledgers.push_back(ledger);
    }
    result["node"] = options.node; result["validations_seen"] = seen; result["validations_counted"] = counted; result["ledgers"] = ledgers;
    return result;
}
std::size_t ValidationQuorum::count(Bytes const& ledger_hash) const {
    std::size_t most = 0;  // of the sequences validations named for the hash (one, unless a validator is faulty)
    for (auto it = ledgers_.lower_bound(ledger_hash); it != ledgers_.end() && it->first.size() == ledger_hash.size() + 4 &&
                                                      std::equal(ledger_hash.begin(), ledger_hash.end(), it->first.begin()); ++it)
        most = std::max(most, judged(it->first, it->second).count);
    return most;
}
void ValidationQuorum::negative_unl(std::uint32_t ledger, NegativeUnl unl) {
    // Every ledger of a period carries the same list and the same pending changes (set only in
    // flag ledgers): the one given last for the period stands for it.
    auto period = ledger / flag_interval * flag_interval;
    for (auto same = negative_unls_.lower_bound(period); same != negative_unls_.end() && same->first - period < flag_interval;) same = negative_unls_.erase(same);
    negative_unls_.emplace(ledger, std::move(unl));
    auto newest = negative_unls_.rbegin()->first / flag_interval * flag_interval;
    while (negative_unls_.begin()->first / flag_interval * flag_interval + flag_interval < newest) negative_unls_.erase(negative_unls_.begin());
    // A ledger's own list is no longer needed once a committed ledger at or after it is given.
    for (auto it = parents_.begin(); it != parents_.end();)
        it = sequence_of(it->first) <= negative_unls_.rbegin()->first ? parents_.erase(it) : std::next(it);
    // And the validations of ledgers no longer needed, as a list fetched again forgets them: not
    // only hourly (a list site out of reach for long fetches none).
    forget_old();
}
void ValidationQuorum::negative_unl(Bytes const& ledger_hash, std::uint32_t ledger, NegativeUnl parent) {
    if (!negative_unls_.empty() && ledger <= negative_unls_.rbegin()->first) return;
    parents_.insert_or_assign(ledger_key(ledger_hash, ledger), std::move(parent));
    // One per newest ledger learned while a follower catches up: the oldest go first.
    while (parents_.size() > 256)
        parents_.erase(std::min_element(parents_.begin(), parents_.end(), [](auto const& a, auto const& b) { return sequence_of(a.first) < sequence_of(b.first); }));
}
bool ValidationQuorum::validates(Bytes const& ledger_hash, std::uint32_t ledger, NegativeUnl const& parent) const {
    auto signers = ledgers_.find(ledger_key(ledger_hash, ledger));
    if (!validating() || signers == ledgers_.end()) return false;
    std::vector<std::set<Bytes>> lists{parent.disabled};
    if (ledger % flag_interval == 0) lists.push_back(parent.next);
    return std::all_of(lists.begin(), lists.end(), [&](std::set<Bytes> const& list) { return outside(signers->second.masters, list) >= list_.quorum(list); });
}
std::optional<std::set<Bytes>> ValidationQuorum::disabled(std::uint32_t ledger) const {
    auto period = ledger / flag_interval * flag_interval;  // its flag ledger, which set the list
    // A ledger of the same period carries the same list; one of the period before, the list
    // its pending changes give.
    if (auto same = negative_unls_.lower_bound(period); same != negative_unls_.end() && same->first - period < flag_interval)
        return same->second.disabled;
    if (period >= flag_interval)
        if (auto before = negative_unls_.lower_bound(period - flag_interval); before != negative_unls_.end() && before->first < period)
            return before->second.next;
    return std::nullopt;
}
std::optional<std::vector<std::set<Bytes>>> ValidationQuorum::lists_for(Bytes const& key, std::uint32_t ledger) const {
    // The parent's list; a flag ledger's own as well (its validations judged once the next
    // round started from it), which its parent's pending changes give.
    if (auto parent = parents_.find(key); parent != parents_.end()) {
        std::vector<std::set<Bytes>> lists{parent->second.disabled};
        if (ledger % flag_interval == 0) lists.push_back(parent->second.next);
        return lists;
    }
    std::vector<std::uint32_t> judging{ledger ? ledger - 1 : 0};
    if (ledger % flag_interval == 0) judging.push_back(ledger);
    std::vector<std::set<Bytes>> lists;
    for (auto at : judging) {
        auto list = disabled(at);
        if (!list) return std::nullopt;
        lists.push_back(std::move(*list));
    }
    return lists;
}
ValidationQuorum::Counted ValidationQuorum::judged(Bytes const& key, Signers const& signers) const {
    Counted counted{Bytes(key.begin(), key.end() - 4), signers.ledger_index, signers.masters.size(), false, Verdict::short_of_quorum};
    if (!validating()) return counted;
    if (auto lists = lists_for(key, signers.ledger_index)) {
        counted.count = outside(signers.masters, lists->front());
        counted.validated = std::all_of(lists->begin(), lists->end(), [&](std::set<Bytes> const& list) { return outside(signers.masters, list) >= list_.quorum(list); });
        if (counted.validated) counted.verdict = Verdict::validated;
    } else if (counted.count >= list_.quorum(all_masters())) {
        counted.verdict = Verdict::provisional;  // as many as the lowest quorum any negative UNL leaves
    }
    return counted;
}
std::set<Bytes> ValidationQuorum::all_masters() const {
    std::set<Bytes> masters;
    for (auto const& [master, signing] : list_.signing_keys) masters.insert(master);
    return masters;
}
std::vector<ValidationQuorum::Counted> ValidationQuorum::ledgers() const {
    std::vector<Counted> out;
    out.reserve(ledgers_.size());
    for (auto const& [key, signers] : ledgers_) out.push_back(judged(key, signers));
    return out;
}
void ValidationQuorum::adopt(ValidationQuorum const& previous) {
    negative_unls_ = previous.negative_unls_;
    parents_ = previous.parents_;
    // A validator this list names (signing with its master key, or with a manifest's key) whose
    // newer manifest the previous quorum had.
    for (auto const& [master, sequence] : previous.list_.manifest_sequences) {
        auto known = list_.manifest_sequences.find(master);
        if (known == list_.manifest_sequences.end() ? !list_.signing_keys.contains(master) : sequence <= known->second) continue;
        list_.manifest_sequences[master] = sequence;
        if (auto old = list_.signing_keys.find(master); old != list_.signing_keys.end()) { master_of_.erase(old->second); list_.signing_keys.erase(old); }
        if (auto signing = previous.list_.signing_keys.find(master); signing != previous.list_.signing_keys.end()) {
            list_.signing_keys[master] = signing->second;
            master_of_[signing->second] = master;
        }
    }
    for (auto const& [master, newest] : previous.newest_)
        if (list_.signing_keys.contains(master)) newest_[master] = newest;
    if (previous.kept_) kept_ = previous.kept_;
    for (auto const& [key, signers] : previous.ledgers_) {
        auto& carried = ledgers_[key];
        carried.ledger_index = signers.ledger_index;
        for (auto const& master : signers.masters)
            if (list_.signing_keys.contains(master)) carried.masters.insert(master);
    }
    closed_ = std::max(closed_, previous.closed_);
    forget_old();
}
void ValidationQuorum::replace(ValidatorList list) {
    ValidationQuorum next(std::move(list), now_);
    next.adopt(*this);
    *this = std::move(next);
}
ValidationQuorum::Disposition ValidationQuorum::apply(ValidatorList fetched) {
    auto pending = std::move(list_.pending);
    pending.insert(pending.end(), std::make_move_iterator(fetched.pending.begin()), std::make_move_iterator(fetched.pending.end()));
    fetched.pending.clear();
    auto disposition = fetched.sequence > list_.sequence ? Disposition::accepted
                     : fetched.sequence == list_.sequence ? Disposition::same_sequence : Disposition::stale;
    if (disposition == Disposition::accepted) replace(std::move(fetched));
    list_.pending = pending_after(std::move(pending), list_.sequence);
    return disposition;
}
bool ValidationQuorum::rotate() {
    if (list_.pending.empty()) return false;
    auto later = std::find_if(list_.pending.begin(), list_.pending.end(), [at = now()](ValidatorList const& list) { return list.effective > at; });
    if (later == list_.pending.begin()) return false;
    std::vector<ValidatorList> pending(std::make_move_iterator(later), std::make_move_iterator(list_.pending.end()));
    auto due = std::move(*std::prev(later));  // the last whose time has come; those before it are passed over
    replace(std::move(due));
    list_.pending = std::move(pending);
    return true;
}
void ValidationQuorum::keep(Bytes const& ledger_hash, std::uint32_t ledger) {
    if (!negative_unls_.empty() && ledger <= negative_unls_.rbegin()->first) return;  // committed already
    kept_ = ledger_key(ledger_hash, ledger);
}
void ValidationQuorum::forget_old() {
    // The newest 256 ledgers' validations, and those of every ledger after the newest committed
    // one (any, while none is given) within the newest `pending` ledgers: the ledgers a follower
    // has yet to commit stay judged as negative UNLs and manifests are given, however long it
    // takes to catch up and however often the list is fetched. The ledger kept, however old.
    constexpr std::uint32_t pending = 16384;  // some 18 hours of mainnet's ledgers
    auto committed = negative_unls_.empty() ? std::optional<std::uint32_t>() : negative_unls_.rbegin()->first;
    if (kept_ && committed && sequence_of(*kept_) <= *committed) kept_.reset();
    // The newest ledger validators as many as the lowest quorum any negative UNL leaves signed
    // (the provisional rule), else the one most signed, or the committed one if later: not the
    // newest any validator signed, which one faulty validator may put far ahead.
    auto lowest = list_.quorum(all_masters());
    std::optional<std::uint32_t> reached;
    std::uint32_t most_signed = 0;
    std::size_t most = 0;
    for (auto const& [key, signers] : ledgers_) {
        auto count = signers.masters.size();
        if (count >= lowest) reached = std::max(reached.value_or(0), signers.ledger_index);
        if (count > most || (count == most && signers.ledger_index > most_signed)) { most = count; most_signed = signers.ledger_index; }
    }
    auto newest = std::max(reached.value_or(most_signed), committed.value_or(0));
    for (auto it = ledgers_.begin(); it != ledgers_.end();) {
        auto at = it->second.ledger_index;
        bool kept = kept_ && it->first == *kept_;
        bool ahead = at > newest && at - newest <= pending;  // still being signed, as the network moves on
        bool recent = at <= newest && newest - at < 256;
        bool uncommitted = at <= newest && (!committed || at > *committed) && newest - at < pending;
        it = kept || ahead || recent || uncommitted ? std::next(it) : ledgers_.erase(it);
    }
}
ValidationQuorum::Verdict ValidationQuorum::verdict(Bytes const& ledger_hash) const {
    auto best = Verdict::short_of_quorum;
    for (auto it = ledgers_.lower_bound(ledger_hash); it != ledgers_.end() && it->first.size() == ledger_hash.size() + 4 &&
                                                      std::equal(ledger_hash.begin(), ledger_hash.end(), it->first.begin()); ++it) {
        auto verdict = judged(it->first, it->second).verdict;
        if (verdict == Verdict::validated) return verdict;
        if (verdict == Verdict::provisional) best = verdict;
    }
    return best;
}
}
