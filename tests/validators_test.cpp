// Validator list verification and validation quorums over mainnet data captured with
// tools/capture_validations.py (tests/fixtures/mainnet-validations.json).
#include "../core/src/validators.hpp"
#include "../core/src/ws_client.hpp"
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <gtest/gtest.h>
#include <xrpl/basics/base64.h>
#include <xrpl/json/json_writer.h>
#include <xrpl/protocol/HashPrefix.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <xrpl/protocol/STValidation.h>
#include <xrpl/protocol/digest.h>
#include <xrpl/protocol/SecretKey.h>
#include <xrpl/protocol/Seed.h>
#include <xrpl/protocol/Sign.h>
#include <xrpl/protocol/STObject.h>
#include <xrpl/protocol/tokens.h>
#include <xrpl/basics/strHex.h>
#include <chrono>
#include <fstream>
#include <limits>
#include <sstream>
#include <thread>

using namespace xrpla;
using namespace xrpla::core;

namespace {
constexpr char ripple_list_key[] = "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734";
Json const& fixture() {
    static Json data = [] {
        std::ifstream in(std::string(XRPLA_TEST_FIXTURES) + "/mainnet-validations.json");
        std::stringstream text; text << in.rdbuf();
        return Json::parse(text.str());
    }();
    return data;
}
Bytes fixture_bytes(::xrpl::STObject const& object) {
    auto serialized = object.getSerializer();
    return Bytes(reinterpret_cast<char const*>(serialized.data()), serialized.size());
}
std::uint64_t captured() { return fixture().at("captured_at").get<std::uint64_t>() - 946684800; }
// The fixture's list judged as at its capture (it expires later).
std::uint64_t at_capture() { return captured(); }
std::vector<Bytes> validations_of(std::string const& ledger_hash) {
    std::vector<Bytes> out;
    for (auto const& m : fixture().at("messages"))
        if (m.at("type") == "validationReceived" && m.at("ledger_hash") == ledger_hash) out.push_back(from_hex(m.at("data").get<std::string>()));
    return out;
}
}

TEST(Validators, MainnetListVerifiesAndMatchesTheNodesQuorum) {
    auto list = verify_validator_list(fixture().at("validator_list"), ripple_list_key, captured());
    EXPECT_GT(list.signing_keys.size(), 20u);
    EXPECT_EQ(list.quorum(), fixture().at("node_quorum").get<std::size_t>());
    EXPECT_GT(list.expiration, captured());
}

TEST(Validators, CapturedLedgersReachQuorumFromTrustedValidatorsOnly) {
    ValidationQuorum quorum(verify_validator_list(fixture().at("validator_list"), ripple_list_key, captured()), at_capture);
    // In sequence order, as validators sign them (an earlier sequence after a later one does not
    // count, as in xrpld).
    std::map<std::uint32_t, std::pair<std::string, Json>> ordered;
    for (auto const& [hash, ledger] : fixture().at("ledgers").items()) ordered[ledger.at("ledger_index").get<std::uint32_t>()] = {hash, ledger};
    for (auto const& [index, entry] : ordered) {
        auto const& [hash, ledger] = entry;
        // Judged under its parent's negative UNL (none at capture time).
        quorum.negative_unl(ledger.at("ledger_index").get<std::uint32_t>() - 1, {});
        // The fixture's ledger header hashes to the validated hash.
        auto header = from_hex(ledger.at("header").get<std::string>());
        auto digest = ::xrpl::sha512Half(::xrpl::HashPrefix::LedgerMaster, ::xrpl::makeSlice(header));
        EXPECT_EQ(to_hex(std::string_view(reinterpret_cast<char const*>(digest.data()), digest.size())), from_hex(hash).empty() ? "" : to_hex(from_hex(hash)));
        auto all = validations_of(hash);
        std::size_t trusted = 0;
        for (auto const& v : all) {
            auto counted = quorum.add(v);
            if (!counted) continue;
            ++trusted;
            EXPECT_EQ(to_hex(counted->ledger_hash), to_hex(from_hex(hash)));
            EXPECT_EQ(counted->ledger_index, ledger.at("ledger_index").get<std::uint32_t>());
        }
        EXPECT_LT(trusted, all.size()) << "validators outside the list must not count";
        EXPECT_TRUE(quorum.validated(from_hex(hash))) << hash;
        EXPECT_LE(quorum.count(from_hex(hash)), quorum.list().signing_keys.size());
        // Replaying a validation does not count its validator twice.
        auto before = quorum.count(from_hex(hash));
        for (auto const& v : all) quorum.add(v);
        EXPECT_EQ(quorum.count(from_hex(hash)), before);
    }
}

TEST(Validators, TamperedValidationsAndListsAreRejected) {
    auto const& published = fixture().at("validator_list");
    auto list = verify_validator_list(published, ripple_list_key, captured());
    // A different publisher key, a changed list and an expired list.
    auto other = std::string(ripple_list_key); other[10] = other[10] == '0' ? '1' : '0';
    EXPECT_THROW(verify_validator_list(published, other, captured()), Error);
    auto changed = published;
    auto blob = ::xrpl::base64Decode(changed.at("blob").get<std::string>());
    blob[blob.find("\"sequence\"") + 12] ^= 1;
    changed["blob"] = ::xrpl::base64Encode(blob);
    EXPECT_THROW(verify_validator_list(changed, ripple_list_key, captured()), Error);
    // Expired: still the list in force (as xrpld's, Expired), for nothing to be validated.
    auto expired = verify_validator_list(published, ripple_list_key, list.expiration + 1);
    EXPECT_EQ(expired.sequence, list.sequence);
    EXPECT_THROW(validating_list(expired, list.expiration + 1), Error);
    EXPECT_TRUE(ValidationQuorum(expired, [&] { return list.expiration + 1; }).expired());
    // A validation with one flipped byte no longer verifies.
    ValidationQuorum quorum(list);
    auto hash = fixture().at("ledgers").begin().key();
    for (auto v : validations_of(hash)) {
        if (!quorum.add(v)) continue;
        v[v.size() / 2] ^= 1;
        ValidationQuorum fresh(list);
        EXPECT_FALSE(fresh.add(v));
        break;
    }
}

// A validator's key rotation reaches followers as a manifests-stream message (fields, not bytes):
// rebuilt and verified, it moves the validator to its new signing key; a tampered or stale one
// changes nothing.
TEST(Validators, StreamedManifestRotatesASigningKey) {
    namespace r = ::xrpl;
    auto [master, master_secret] = r::randomKeyPair(r::KeyType::Ed25519);
    auto [old_key, old_secret] = r::randomKeyPair(r::KeyType::Secp256k1);
    auto [new_key, new_secret] = r::randomKeyPair(r::KeyType::Secp256k1);
    auto raw = [](r::PublicKey const& k) { return Bytes(reinterpret_cast<char const*>(k.data()), k.size()); };
    ValidatorList list;
    list.signing_keys[raw(master)] = raw(old_key);
    list.manifest_sequences[raw(master)] = 1;
    ValidationQuorum quorum(list);
    auto message = [&](std::uint32_t sequence) {
        r::STObject manifest(r::sfGeneric);
        manifest[r::sfPublicKey] = master;
        manifest[r::sfSequence] = sequence;
        manifest[r::sfSigningPubKey] = new_key;
        r::sign(manifest, r::HashPrefix::Manifest, r::KeyType::Secp256k1, new_secret);
        r::sign(manifest, r::HashPrefix::Manifest, r::KeyType::Ed25519, master_secret, r::sfMasterSignature);
        return Json{{"type", "manifestReceived"}, {"master_key", r::toBase58(r::TokenType::NodePublic, master)},
                    {"signing_key", r::toBase58(r::TokenType::NodePublic, new_key)}, {"seq", sequence},
                    {"signature", r::strHex(manifest.getFieldVL(r::sfSignature))},
                    {"master_signature", r::strHex(manifest.getFieldVL(r::sfMasterSignature))}};
    };
    auto tampered = message(2);
    tampered["seq"] = 3;  // signatures no longer match
    EXPECT_FALSE(quorum.manifest(*manifest_from_stream(tampered)));
    EXPECT_EQ(quorum.list().signing_keys.at(raw(master)), raw(old_key));
    EXPECT_TRUE(quorum.manifest(*manifest_from_stream(message(2))));
    EXPECT_EQ(quorum.list().signing_keys.at(raw(master)), raw(new_key));
    EXPECT_FALSE(quorum.manifest(*manifest_from_stream(message(2))));  // not newer
    EXPECT_FALSE(manifest_from_stream(Json{{"type", "validationReceived"}}));
    // The list fetched again still names the old key (its site lags the rotation): the quorum
    // built from it keeps the newer key the stream gave.
    ValidationQuorum refetched(list);
    refetched.adopt(quorum);
    EXPECT_EQ(refetched.list().signing_keys.at(raw(master)), raw(new_key));
    EXPECT_TRUE(refetched.trusted(raw(new_key)));
    EXPECT_FALSE(refetched.trusted(raw(old_key)));
}

// The list is re-fetched hourly while a ledger may be short of its quorum: the validations
// counted for it carry over to the new list's quorum, so it still reaches it.
TEST(Validators, ARefetchedListKeepsTheValidationsCounted) {
    auto list = verify_validator_list(fixture().at("validator_list"), ripple_list_key, captured());
    auto hash = fixture().at("ledgers").begin().key();
    auto all = validations_of(hash);
    ValidationQuorum before(list, at_capture);
    before.negative_unl(fixture().at("ledgers").begin().value().at("ledger_index").get<std::uint32_t>() - 1, {});
    std::size_t added = 0;
    for (auto const& v : all) {
        if (before.count(from_hex(hash)) + 1 >= list.quorum()) break;
        if (before.add(v)) ++added;
    }
    ASSERT_GT(added, 0u);
    EXPECT_FALSE(before.validated(from_hex(hash)));
    ValidationQuorum after(list, at_capture);
    after.adopt(before);
    EXPECT_EQ(after.count(from_hex(hash)), before.count(from_hex(hash)));
    for (auto const& v : all) after.add(v);
    EXPECT_TRUE(after.validated(from_hex(hash)));
}

// xrpld's quorum (ValidatorList::calculateQuorum): 80% of the validators not on the negative UNL,
// and never under 60% of the list; a negative UNL entry the list does not name changes nothing.
TEST(Validators, TheNegativeUnlLowersTheQuorumAsXrpldDoes) {
    ValidatorList list;
    for (int n = 0; n < 35; ++n) list.signing_keys["master" + std::to_string(n)] = "signing" + std::to_string(n);
    auto masters = [](int from, int to) { std::set<Bytes> out; for (int n = from; n < to; ++n) out.insert("master" + std::to_string(n)); return out; };
    EXPECT_EQ(list.quorum(), 28u);
    EXPECT_EQ(list.quorum({"elsewhere"}), 28u);
    EXPECT_EQ(list.quorum(masters(0, 3)), 26u);   // ceil(0.8 * 32)
    EXPECT_EQ(list.quorum(masters(0, 10)), 21u);  // ceil(0.6 * 35) over ceil(0.8 * 25) = 20
    ValidatorList five;
    for (int n = 0; n < 5; ++n) five.signing_keys["master" + std::to_string(n)] = "signing" + std::to_string(n);
    EXPECT_EQ(five.quorum(), 4u);
    EXPECT_EQ(five.quorum(masters(0, 1)), 4u);  // ceil(0.8 * 4)
}

// A ledger's NegativeUNL object read as Ledger::negativeUNL reads it, and as the next flag
// ledger's Ledger::updateNegativeUNL leaves it.
TEST(Validators, TheNegativeUnlIsReadFromItsLedgerObject) {
    namespace r = ::xrpl;
    auto key = [](r::PublicKey const& k) { return Bytes(reinterpret_cast<char const*>(k.data()), k.size()); };
    auto blob = [](Bytes const& b) { return r::Blob(b.begin(), b.end()); };
    auto a = key(r::randomKeyPair(r::KeyType::Ed25519).first), b = key(r::randomKeyPair(r::KeyType::Ed25519).first);
    auto c = key(r::randomKeyPair(r::KeyType::Ed25519).first);
    r::STLedgerEntry object(r::keylet::negativeUNL());
    r::STArray disabled(r::sfDisabledValidators);
    for (auto const& k : {a, b, Bytes("not a key")}) {
        auto entry = r::STObject::makeInnerObject(r::sfDisabledValidator);
        entry.setFieldVL(r::sfPublicKey, blob(k));
        entry.setFieldU32(r::sfFirstLedgerSequence, 256);
        disabled.push_back(std::move(entry));
    }
    object.setFieldArray(r::sfDisabledValidators, disabled);
    object.setFieldVL(r::sfValidatorToReEnable, blob(a));
    object.setFieldVL(r::sfValidatorToDisable, blob(c));
    auto unl = negative_unl(fixture_bytes(object));
    EXPECT_EQ(unl.disabled, (std::set<Bytes>{a, b}));
    EXPECT_EQ(unl.next, (std::set<Bytes>{b, c}));
    EXPECT_TRUE(negative_unl({}).disabled.empty());
}

// Validations from validators on the negative UNL count for nothing, as xrpld's
// negativeUNLFilter, while the quorum is lowered by those listed: a ledger short of 80% of the
// whole list is validated once the validators that did not sign it are on its parent's negative
// UNL, and not once those that did are.
TEST(Validators, TheNegativeUnlFiltersValidationsAndLowersTheQuorum) {
    auto list = verify_validator_list(fixture().at("validator_list"), ripple_list_key, captured());
    auto hash = from_hex(fixture().at("ledgers").begin().key());
    auto parent = fixture().at("ledgers").begin().value().at("ledger_index").get<std::uint32_t>() - 1;
    auto all = validations_of(fixture().at("ledgers").begin().key());
    std::map<Bytes, Bytes> master_of;
    for (auto const& [master, signing] : list.signing_keys) master_of[signing] = master;
    ValidationQuorum quorum(list, at_capture);
    std::set<Bytes> signed_it;
    for (auto const& v : all) {
        if (quorum.count(hash) + 1 >= list.quorum()) break;
        ::xrpl::SerialIter it(v.data(), v.size());
        ::xrpl::STValidation validation(it, [](::xrpl::PublicKey const& key) { return ::xrpl::calcNodeID(key); }, {true, true});
        auto signer = validation.getSignerPublic();
        auto master = master_of.find(Bytes(reinterpret_cast<char const*>(signer.data()), signer.size()));
        if (quorum.add(v) && master != master_of.end()) signed_it.insert(master->second);
    }
    ASSERT_EQ(quorum.count(hash), list.quorum() - 1);
    ASSERT_EQ(signed_it.size(), list.quorum() - 1);
    EXPECT_FALSE(quorum.validated(hash));
    std::set<Bytes> silent;
    for (auto const& [master, signing] : list.signing_keys) if (!signed_it.contains(master)) silent.insert(master);
    ASSERT_GE(silent.size(), 2u);
    quorum.negative_unl(parent, {silent, silent});
    EXPECT_EQ(quorum.count(hash), list.quorum() - 1);
    EXPECT_TRUE(quorum.validated(hash));
    // Validators that signed, on the negative UNL: their validations no longer count.
    std::set<Bytes> signers(signed_it.begin(), std::next(signed_it.begin(), 2));
    quorum.negative_unl(parent, {signers, signers});
    EXPECT_EQ(quorum.count(hash), list.quorum() - 3);
    EXPECT_FALSE(quorum.validated(hash));
    // The negative UNL carries over to a quorum built from the list fetched again.
    quorum.negative_unl(parent, {silent, silent});
    ValidationQuorum after(list, at_capture);
    after.adopt(quorum);
    EXPECT_TRUE(after.validated(hash));
}

namespace {
Bytes key_bytes(::xrpl::PublicKey const& key) { return Bytes(reinterpret_cast<char const*>(key.data()), key.size()); }
std::string hex_of(::xrpl::PublicKey const& key) { return ::xrpl::strHex(key_bytes(key)); }
// Ledger `seq`'s hash, made from it (and from `fork`, for another ledger of that sequence).
::xrpl::uint256 ledger_id(std::uint32_t seq, std::uint32_t fork) { return fork ? ::xrpl::sha512Half(seq, fork) : ::xrpl::sha512Half(seq); }
Bytes ledger_hash(std::uint32_t seq, std::uint32_t fork = 0) {
    auto h = ledger_id(seq, fork);
    return Bytes(reinterpret_cast<char const*>(h.data()), h.size());
}
// A full validation of ledger `seq`, signed with `key` (a partial one, unless `full`).
Bytes signed_validation(::xrpl::PublicKey const& key, ::xrpl::SecretKey const& secret, std::uint32_t seq, std::uint32_t fork = 0, bool full = true) {
    namespace r = ::xrpl;
    auto signed_at = r::NetClock::time_point{r::NetClock::duration{static_cast<std::uint32_t>(xrpl_seconds_now())}};
    r::STValidation built(signed_at, key, secret, r::calcNodeID(key), [&](r::STValidation& v) {
        v.setFieldH256(r::sfLedgerHash, ledger_id(seq, fork));
        v.setFieldU32(r::sfLedgerSequence, seq);
        if (full) v.setFlag(r::kVfFullValidation);
    });
    r::Serializer s;
    built.add(s);
    return Bytes(reinterpret_cast<char const*>(s.data()), s.size());
}
// A validator's manifest: its master key names `signing` (a revocation without one).
Bytes manifest_of(std::pair<::xrpl::PublicKey, ::xrpl::SecretKey> const& master, std::optional<std::pair<::xrpl::PublicKey, ::xrpl::SecretKey>> const& signing,
                  std::uint32_t sequence) {
    namespace r = ::xrpl;
    r::STObject manifest(r::sfGeneric);
    manifest[r::sfPublicKey] = master.first;
    manifest[r::sfSequence] = signing ? sequence : std::numeric_limits<std::uint32_t>::max();
    if (signing) {
        manifest[r::sfSigningPubKey] = signing->first;
        r::sign(manifest, r::HashPrefix::Manifest, *r::publicKeyType(signing->first), signing->second);
    }
    r::sign(manifest, r::HashPrefix::Manifest, *r::publicKeyType(master.first), master.second, r::sfMasterSignature);
    return fixture_bytes(manifest);
}
// Ten validators made up for a test (master keys made from "master0" to "master9"), whose
// validations of a ledger are signed here: ledger n's hash is made from n.
struct TestValidators {
    ValidatorList list;
    std::vector<std::pair<::xrpl::PublicKey, ::xrpl::SecretKey>> keys;
    TestValidators() {
        for (int n = 0; n < 10; ++n) {
            auto [key, secret] = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
            list.signing_keys[master(n)] = key_bytes(key);
            list.manifest_sequences[master(n)] = 1;
            keys.emplace_back(key, secret);
        }
    }
    static std::pair<::xrpl::PublicKey, ::xrpl::SecretKey> master_keys(int n) {
        return ::xrpl::generateKeyPair(::xrpl::KeyType::Ed25519, ::xrpl::generateSeed("master" + std::to_string(n)));
    }
    static Bytes master(int n) { return key_bytes(master_keys(n).first); }
    static std::set<Bytes> masters(std::initializer_list<int> numbers) {
        std::set<Bytes> out;
        for (auto n : numbers) out.insert(master(n));
        return out;
    }
    static Bytes hash(std::uint32_t seq, std::uint32_t fork = 0) { return ledger_hash(seq, fork); }
    // Validators `from` to `to` (inclusive) validate ledger `seq` (of fork `fork`).
    void validate(ValidationQuorum& quorum, std::uint32_t seq, int from, int to, std::uint32_t fork = 0) const {
        for (auto n = from; n <= to; ++n) ASSERT_TRUE(quorum.add(signed_validation(keys[n].first, keys[n].second, seq, fork))) << n;
    }
    // Validator `n`'s manifest revoking its key.
    Bytes revocation(int n) const { return manifest_of(master_keys(n), std::nullopt, 0); }
};
using Verdict = ValidationQuorum::Verdict;
}

// A validator voted to be disabled while seven of ten validate: xrpld's quorum is 8 of 10 under
// the negative UNL ledger 1000 carries (master9), 7 under the one flag ledger 1024 takes on
// (master8 and master9). xrpld validates no ledger whose parent precedes 1024 and every one
// after; flag ledger 1024 itself, whose validations it may judge under either list, needs both.
// A ledger past the next period (from 1280) is judged only once a ledger near it is verified.
TEST(Validators, ALedgerIsJudgedUnderItsParentsNegativeUnl) {
    TestValidators v;
    ASSERT_EQ(v.list.quorum(v.masters({9})), 8u);
    ASSERT_EQ(v.list.quorum(v.masters({8, 9})), 7u);
    ValidationQuorum quorum(v.list);
    quorum.negative_unl(1000, {v.masters({9}), v.masters({8, 9})});
    for (std::uint32_t seq : {1010u, 1023u, 1024u, 1025u, 1279u, 1280u, 1300u}) v.validate(quorum, seq, 0, 6);
    EXPECT_EQ(quorum.verdict(v.hash(1010)), Verdict::short_of_quorum);
    EXPECT_EQ(quorum.verdict(v.hash(1023)), Verdict::short_of_quorum);
    EXPECT_EQ(quorum.verdict(v.hash(1024)), Verdict::short_of_quorum);
    EXPECT_EQ(quorum.verdict(v.hash(1025)), Verdict::validated);
    EXPECT_EQ(quorum.verdict(v.hash(1279)), Verdict::validated);
    EXPECT_EQ(quorum.verdict(v.hash(1280)), Verdict::provisional);
    EXPECT_EQ(quorum.verdict(v.hash(1300)), Verdict::provisional);
    EXPECT_EQ(quorum.disabled(1023), v.masters({9}));
    EXPECT_EQ(quorum.disabled(1279), v.masters({8, 9}));
    EXPECT_FALSE(quorum.disabled(1280));
    // Ledger 1279 verified, nothing more voted: the next period carries the same list.
    quorum.negative_unl(1279, {v.masters({8, 9}), v.masters({8, 9})});
    EXPECT_EQ(quorum.verdict(v.hash(1280)), Verdict::validated);
    EXPECT_EQ(quorum.verdict(v.hash(1300)), Verdict::validated);
    // Ledger 1025 and those before it, committed and more than 256 before the newest, are no
    // longer counted (the committed ledger given forgets them).
    std::size_t validated = 0;
    for (auto const& counted : quorum.ledgers()) validated += counted.validated;
    EXPECT_EQ(validated, 3u);
    EXPECT_EQ(quorum.count(v.hash(1025)), 0u);
    // A validator to be re-enabled instead: the quorum rises only for ledgers after the flag ledger.
    ValidationQuorum reenabling(v.list);
    reenabling.negative_unl(1000, {v.masters({8, 9}), v.masters({9})});
    for (std::uint32_t seq : {1023u, 1024u, 1025u}) v.validate(reenabling, seq, 0, 6);
    EXPECT_EQ(reenabling.verdict(v.hash(1023)), Verdict::validated);
    EXPECT_EQ(reenabling.verdict(v.hash(1024)), Verdict::short_of_quorum);
    EXPECT_EQ(reenabling.verdict(v.hash(1025)), Verdict::short_of_quorum);
}

// Before any ledger's negative UNL is given (a follower starting), no ledger is validated, even
// with the whole list's quorum: a validator on the negative UNL may be among its signers. Once
// the list is given the ledgers counted are judged again.
TEST(Validators, NoLedgerIsValidatedBeforeItsNegativeUnlIsKnown) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    v.validate(quorum, 1010, 0, 6);
    v.validate(quorum, 1010, 9, 9);  // master9 is on the negative UNL
    v.validate(quorum, 1011, 0, 4);
    EXPECT_EQ(quorum.count(v.hash(1010)), 8u);
    EXPECT_EQ(quorum.verdict(v.hash(1010)), Verdict::provisional);
    EXPECT_FALSE(quorum.validated(v.hash(1010)));
    EXPECT_EQ(quorum.verdict(v.hash(1011)), Verdict::short_of_quorum);  // under 60% of the list
    quorum.negative_unl(1000, {v.masters({9}), v.masters({9})});
    EXPECT_EQ(quorum.count(v.hash(1010)), 7u);
    EXPECT_EQ(quorum.verdict(v.hash(1010)), Verdict::short_of_quorum);
    for (auto const& counted : quorum.ledgers()) EXPECT_EQ(counted.verdict, Verdict::short_of_quorum);
    // Without master9 counted but with master7's validation (now eight): validated.
    v.validate(quorum, 1010, 7, 7);
    EXPECT_TRUE(quorum.validated(v.hash(1010)));
}

// A follower more than a flag period behind (committed ledger 1000): the network's ledger 2000
// is provisional, not validated, and so a batch may end there only after the commit check, given
// the negative UNL of its parent from verified state, judges it exactly: validated under the
// list voted since (master8 and master9 disabled), refused under the one ledger 1000 had. That
// list judges ledger 2000 alone, and the commit check keeps none: ledger 2001 (whose parent may
// be another ledger 2000) and another ledger 2000 stay as they were.
TEST(Validators, LedgersBeyondTheNextPeriodAreJudgedWithTheirParentsList) {
    TestValidators v;
    auto disabled = NegativeUnl{v.masters({8, 9}), v.masters({8, 9})};
    for (bool voted : {true, false}) {
        SCOPED_TRACE(voted ? "disabled since" : "nothing voted");
        ValidationQuorum quorum(v.list);
        quorum.negative_unl(1000, {{}, {}});
        v.validate(quorum, 1500, 0, 7);
        v.validate(quorum, 2000, 0, 6);
        v.validate(quorum, 2000, 7, 9, 1);  // a fork's
        v.validate(quorum, 2001, 0, 6);
        EXPECT_EQ(quorum.verdict(v.hash(1500)), Verdict::provisional);  // a whole-list quorum, still unknown
        EXPECT_EQ(quorum.verdict(v.hash(2000)), Verdict::provisional);
        EXPECT_FALSE(quorum.disabled(1999));
        // The commit check, judging the ledger alone.
        EXPECT_EQ(quorum.validates(v.hash(2000), 2000, voted ? disabled : NegativeUnl{}), voted);
        EXPECT_FALSE(quorum.validates(v.hash(2000, 1), 2000, disabled));
        EXPECT_EQ(quorum.verdict(v.hash(2000)), Verdict::provisional);
        // Read from peers for the ledger.
        quorum.negative_unl(v.hash(2000), 2000, voted ? disabled : NegativeUnl{});
        EXPECT_EQ(quorum.verdict(v.hash(2000)), voted ? Verdict::validated : Verdict::short_of_quorum);
        EXPECT_EQ(quorum.verdict(v.hash(2001)), Verdict::provisional);
        EXPECT_EQ(quorum.verdict(v.hash(2000, 1)), Verdict::short_of_quorum);
        EXPECT_EQ(quorum.verdict(v.hash(1500)), Verdict::provisional);  // still unknown
        EXPECT_FALSE(quorum.disabled(1999));
        // The committed ledger's list is unchanged by one given further on.
        EXPECT_EQ(quorum.disabled(1000), std::set<Bytes>{});
        // Its parent committed: the ledgers after it are judged with that list.
        quorum.negative_unl(1999, voted ? disabled : NegativeUnl{});
        EXPECT_EQ(quorum.verdict(v.hash(2000)), voted ? Verdict::validated : Verdict::short_of_quorum);
        EXPECT_EQ(quorum.verdict(v.hash(2001)), voted ? Verdict::validated : Verdict::short_of_quorum);
        EXPECT_EQ(quorum.verdict(v.hash(2000, 1)), Verdict::short_of_quorum);
    }
}

// A flag ledger judged by the commit check: under its parent's list and under its own, which
// its parent's pending changes give (a validator to disable: master9).
TEST(Validators, AFlagLedgerIsJudgedUnderItsParentsPendingChangesToo) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    v.validate(quorum, 2048, 0, 6);
    EXPECT_TRUE(quorum.validates(v.hash(2048), 2048, {v.masters({8, 9}), v.masters({8, 9})}));
    EXPECT_FALSE(quorum.validates(v.hash(2048), 2048, {v.masters({8, 9}), v.masters({8})}));  // master9 re-enabled
    EXPECT_FALSE(quorum.validates(v.hash(2047), 2047, {v.masters({8, 9}), v.masters({8, 9})}));  // not counted
}

// A committed ledger's list is kept while the ledgers after it need it: lists given for single
// ledgers, however many, and later ledgers of its period leave it in place; a committed ledger
// of the next period keeps it too, and only one two periods on retires it.
TEST(Validators, ACommittedLedgersListIsKeptWhileLedgersAfterItNeedIt) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    quorum.negative_unl(1000, {v.masters({9}), v.masters({9})});
    v.validate(quorum, 1001, 0, 7);
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    for (std::uint32_t seq = 3000; seq < 3300; ++seq) quorum.negative_unl(v.hash(seq), seq, {});
    for (std::uint32_t seq = 1001; seq < 1020; ++seq) quorum.negative_unl(seq, {v.masters({9}), v.masters({9})});
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    EXPECT_EQ(quorum.disabled(1100), v.masters({9}));
    quorum.negative_unl(1100, {v.masters({9}), v.masters({9})});
    EXPECT_EQ(quorum.disabled(1001), v.masters({9}));  // the period before the newest
    quorum.negative_unl(1300, {v.masters({9}), v.masters({9})});
    EXPECT_FALSE(quorum.disabled(1001));
    // Committed and more than 256 before the newest: its validations are forgotten, so it is no
    // longer among the ledgers judged again, not refused (the follower keeps its validated verdict).
    EXPECT_EQ(quorum.count(v.hash(1001)), 0u);
    EXPECT_TRUE(quorum.ledgers().empty());
}

// A validator's key revoked (a newer manifest naming none): as xrpld's updateTrusted and
// trustChanged, the validator is no longer trusted, its validations no longer count, and the
// quorum is that of the validators left. A quorum built from the list fetched again (still
// naming the old key) keeps the revocation.
TEST(Validators, ARevokedValidatorNoLongerCounts) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 0, 7);
    ASSERT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    EXPECT_TRUE(quorum.manifest(v.revocation(0)));
    EXPECT_FALSE(quorum.manifest(v.revocation(0)));  // not newer
    EXPECT_FALSE(quorum.trusted(key_bytes(v.keys[0].first)));
    EXPECT_FALSE(quorum.trusted(v.master(0)));
    EXPECT_EQ(quorum.list().quorum(), 8u);  // ceil(0.8 * 9)
    EXPECT_EQ(quorum.count(v.hash(1001)), 7u);
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::short_of_quorum);
    EXPECT_FALSE(quorum.validates(v.hash(1001), 1001, {}));
    EXPECT_FALSE(quorum.add(signed_validation(v.keys[0].first, v.keys[0].second, 1002)));
    v.validate(quorum, 1001, 8, 8);
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    ValidationQuorum refetched(v.list);
    refetched.adopt(quorum);
    EXPECT_FALSE(refetched.trusted(key_bytes(v.keys[0].first)));
    EXPECT_EQ(refetched.list().signing_keys.size(), 9u);
    EXPECT_EQ(refetched.count(v.hash(1001)), 8u);
}

// Once the list expires nothing is validated, as xrpld's quorum is then out of reach, nor
// provisionally validated; the commit check refuses every ledger. A list without an expiration
// (one made up here) never expires.
TEST(Validators, NothingIsValidatedOnceTheListExpires) {
    TestValidators v;
    v.list.expiration = 5000;
    std::uint64_t now = 4000;
    ValidationQuorum quorum(v.list, [&] { return now; }), unknown(v.list, [&] { return now; });
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 0, 7);
    v.validate(unknown, 2000, 0, 7);
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    EXPECT_EQ(unknown.verdict(v.hash(2000)), Verdict::provisional);
    EXPECT_FALSE(quorum.expired());
    now = 5000;
    EXPECT_TRUE(quorum.expired());
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::short_of_quorum);
    EXPECT_EQ(unknown.verdict(v.hash(2000)), Verdict::short_of_quorum);
    EXPECT_FALSE(quorum.validates(v.hash(1001), 1001, {}));
    for (auto const& counted : quorum.ledgers()) EXPECT_EQ(counted.verdict, Verdict::short_of_quorum);
    // A list fetched again, in force: what was counted is validated again.
    v.list.expiration = 9000;
    ValidationQuorum refetched(v.list, [&] { return now; });
    refetched.adopt(quorum);
    EXPECT_EQ(refetched.verdict(v.hash(1001)), Verdict::validated);
    v.list.expiration = 0;
    EXPECT_FALSE(ValidationQuorum(v.list, [] { return std::uint64_t(-1); }).expired());
}

// xrpld's checks of a validator's sequences (Validations::add, SeqEnforcer): a second ledger at
// the sequence it validated last, or an earlier sequence, does not count; the same validation
// again does; 10 minutes on, its newest validation is forgotten.
TEST(Validators, AValidatorCountsForOneLedgerPerSequence) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    auto const& [key, secret] = v.keys[0];
    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(quorum.add(signed_validation(key, secret, 1010), start));
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1010, 1), start));  // Conflicting
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1009), start));  // BadSeq
    EXPECT_TRUE(quorum.add(signed_validation(key, secret, 1010), start));  // the same ledger again
    EXPECT_EQ(quorum.count(v.hash(1010)), 1u);
    EXPECT_EQ(quorum.count(v.hash(1010, 1)), 0u);
    EXPECT_TRUE(quorum.add(signed_validation(key, secret, 1011), start + std::chrono::minutes(9)));
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1010, 1), start + std::chrono::minutes(18)));  // 9 minutes after 1011
    EXPECT_TRUE(quorum.add(signed_validation(key, secret, 1009), start + std::chrono::minutes(20)));
    // Other validators are judged apart.
    EXPECT_TRUE(quorum.add(signed_validation(v.keys[1].first, v.keys[1].second, 1010, 1), start));
    // Carried to a quorum built from the list fetched again.
    ValidationQuorum refetched(v.list);
    refetched.adopt(quorum);
    EXPECT_FALSE(refetched.add(signed_validation(key, secret, 1008), start + std::chrono::minutes(21)));
}

// A partial validation never counts, but takes the validator's sequence as a full one does
// (xrpld's Validations::add runs for both): an earlier sequence, or a full validation of that
// same ledger after it, no longer counts; a later sequence does.
TEST(Validators, APartialValidationTakesTheValidatorsSequence) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    quorum.negative_unl(1000, {{}, {}});
    auto const& [key, secret] = v.keys[0];
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1010, 0, false), start));
    EXPECT_EQ(quorum.count(v.hash(1010)), 0u);
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1009), start));  // BadSeq
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1010), start));  // refused: not after the partial one
    EXPECT_EQ(quorum.count(v.hash(1010)), 0u);
    EXPECT_TRUE(quorum.add(signed_validation(key, secret, 1011), start));
    EXPECT_EQ(quorum.count(v.hash(1011)), 1u);
    EXPECT_FALSE(quorum.add(signed_validation(key, secret, 1011, 0, false), start));  // the same ledger, partial: not counted again
    EXPECT_EQ(quorum.count(v.hash(1011)), 1u);
}

// A follower catching up commits ledgers the quorum validated long before, while the list is
// fetched again every hour: a quorum built from the list fetched again keeps the validations of
// the ledgers after the newest committed one, not only of the newest 256, so such a ledger is
// still validated (the commit check accepts it) and still judged again: a validator revoked,
// or the list expired, withdraws its verdict. Those of older ledgers, and of ledgers more than
// 16384 before the newest, are not kept.
TEST(Validators, ARefetchedListKeepsTheValidationsOfLedgersNotCommitted) {
    TestValidators v;
    v.list.expiration = 5000;
    std::uint64_t now = 4000;
    ValidationQuorum quorum(v.list, [&] { return now; });
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 990, 8, 9);
    v.validate(quorum, 1001, 0, 7);
    v.validate(quorum, 1300, 0, 7);  // the network 300 ledgers on
    ASSERT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    ValidationQuorum refetched(v.list, [&] { return now; });
    refetched.adopt(quorum);
    EXPECT_EQ(refetched.count(v.hash(1001)), 8u);
    EXPECT_EQ(refetched.verdict(v.hash(1001)), Verdict::validated);
    EXPECT_TRUE(refetched.validates(v.hash(1001), 1001, {}));
    EXPECT_EQ(refetched.count(v.hash(990)), 0u);  // committed, and not among the newest 256
    // Judged again: withdrawn by a revocation, and once the list expires.
    ValidationQuorum revoked(v.list, [&] { return now; });
    revoked.adopt(refetched);
    EXPECT_TRUE(revoked.manifest(v.revocation(1)));
    EXPECT_EQ(revoked.verdict(v.hash(1001)), Verdict::short_of_quorum);
    EXPECT_FALSE(revoked.validates(v.hash(1001), 1001, {}));
    now = 5000;
    EXPECT_EQ(refetched.verdict(v.hash(1001)), Verdict::short_of_quorum);
    EXPECT_FALSE(refetched.validates(v.hash(1001), 1001, {}));
    // More than 16384 ledgers before the newest: no longer kept.
    now = 4000;
    v.validate(refetched, 1001 + 16384, 0, 7);
    ValidationQuorum later(v.list, [&] { return now; });
    later.adopt(refetched);
    EXPECT_EQ(later.count(v.hash(1001)), 0u);
    EXPECT_EQ(later.count(v.hash(1001 + 16384)), 8u);
}

// A batch from a node catching up may take longer than the 16384 ledgers a list fetched again
// keeps: the ledger it ends at (kept) keeps its validations through every fetch and every
// committed ledger given before it, so its commit check still finds them, and is forgotten
// once it is committed. A ledger not kept is forgotten as before.
TEST(Validators, TheLedgerABatchEndsAtKeepsItsValidations) {
    TestValidators v;
    for (bool kept : {false, true}) {
        SCOPED_TRACE(kept ? "kept" : "not kept");
        ValidationQuorum quorum(v.list);
        quorum.negative_unl(1000, {{}, {}});
        v.validate(quorum, 2000, 0, 7);  // provisional: past the next period
        if (kept) quorum.keep(v.hash(2000), 2000);
        v.validate(quorum, 2000 + 20000, 0, 7);  // the network long after
        ValidationQuorum refetched(v.list);
        refetched.adopt(quorum);
        EXPECT_EQ(refetched.count(v.hash(2000)), kept ? 8u : 0u);
        EXPECT_EQ(refetched.validates(v.hash(2000), 2000, {}), kept);
        if (!kept) continue;
        refetched.negative_unl(1500, {{}, {}});  // a batch before it committed
        ValidationQuorum again(v.list);
        again.adopt(refetched);
        EXPECT_EQ(again.count(v.hash(2000)), 8u);
        EXPECT_TRUE(again.validates(v.hash(2000), 2000, {}));
        again.negative_unl(2000, {{}, {}});  // committed
        EXPECT_EQ(again.count(v.hash(2000)), 0u);
        again.keep(v.hash(1999), 1999);  // committed already: not kept
        v.validate(again, 2000 + 20001, 0, 7);
        ValidationQuorum last(v.list);
        last.adopt(again);
        EXPECT_EQ(last.count(v.hash(2000 + 20001)), 8u);
    }
}

// One trusted validator signing a far later sequence (a faulty one) does not make the network's
// ledgers look old: the newest ledger is the newest the lowest quorum signed, so a list fetched
// again, or a committed ledger given, keeps the ledgers counted near it, and forgets those signed
// more than 16384 ledgers after it.
TEST(Validators, AFarLaterSequenceOneValidatorSignedForgetsNoLedgerBeforeIt) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 0, 7);
    v.validate(quorum, 1002, 0, 3);  // signed in part so far
    v.validate(quorum, 1001 + 16384, 8, 8);
    v.validate(quorum, 1001 + 16385, 9, 9);
    v.validate(quorum, 1001 + 1000000, 8, 8);
    ValidationQuorum refetched(v.list);
    refetched.adopt(quorum);
    EXPECT_EQ(refetched.verdict(v.hash(1001)), Verdict::validated);
    EXPECT_EQ(refetched.count(v.hash(1002)), 4u);
    EXPECT_EQ(refetched.count(v.hash(1001 + 16384)), 1u);  // within 16384 after the newest
    EXPECT_EQ(refetched.count(v.hash(1001 + 16385)), 0u);
    EXPECT_EQ(refetched.count(v.hash(1001 + 1000000)), 0u);
    // A committed ledger given forgets them too.
    quorum.negative_unl(1001, {{}, {}});
    EXPECT_EQ(quorum.verdict(v.hash(1001)), Verdict::validated);
    EXPECT_EQ(quorum.count(v.hash(1002)), 4u);
    EXPECT_EQ(quorum.count(v.hash(1001 + 1000000)), 0u);
    EXPECT_EQ(quorum.ledgers().size(), 3u);
}

// The validations counted are forgotten as committed ledgers are given, not only as the list is
// fetched again (which a list site out of reach may hold off for long): those of ledgers
// committed and more than 256 before the newest go.
TEST(Validators, ACommittedLedgerGivenForgetsTheValidationsNoLongerNeeded) {
    TestValidators v;
    ValidationQuorum quorum(v.list);
    quorum.negative_unl(1000, {{}, {}});
    for (std::uint32_t seq = 1010; seq <= 1600; seq += 10) v.validate(quorum, seq, 0, 5);
    EXPECT_EQ(quorum.ledgers().size(), 60u);
    quorum.negative_unl(1500, {{}, {}});
    EXPECT_EQ(quorum.ledgers().size(), 26u);  // 1350 to 1600
    EXPECT_EQ(quorum.count(v.hash(1340)), 0u);
    EXPECT_EQ(quorum.count(v.hash(1350)), 6u);
    EXPECT_EQ(quorum.count(v.hash(1600)), 6u);
}

namespace {
// A validator list as list sites publish it, signed here by a made-up publisher: each validator
// a master key and, optionally, a manifest (base64).
struct TestPublisher {
    std::pair<::xrpl::PublicKey, ::xrpl::SecretKey> master = ::xrpl::randomKeyPair(::xrpl::KeyType::Ed25519);
    std::pair<::xrpl::PublicKey, ::xrpl::SecretKey> signing = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
    std::uint32_t manifest_sequence = 1;  // of the manifest naming `signing`
    std::string key() const { return hex_of(master.first); }
    Json blob(Json const& validators, std::uint64_t effective, std::uint64_t expiration, std::uint32_t sequence = 1) const {
        auto body = Json{{"sequence", sequence}, {"expiration", expiration}, {"validators", validators}};
        if (effective) body["effective"] = effective;
        return signed_blob(body.dump());
    }
    // Any body, signed.
    Json signed_blob(std::string const& text) const {
        namespace r = ::xrpl;
        auto signature = r::sign(signing.first, signing.second, r::makeSlice(text));
        return {{"blob", r::base64Encode(text)}, {"signature", r::strHex(Bytes(reinterpret_cast<char const*>(signature.data()), signature.size()))}};
    }
    Json published(std::vector<Json> const& blobs) const {
        Json out{{"public_key", key()}, {"manifest", ::xrpl::base64Encode(manifest_of(master, signing, manifest_sequence))}};
        if (blobs.size() == 1) { out["version"] = 1; out["blob"] = blobs[0].at("blob"); out["signature"] = blobs[0].at("signature"); }
        else { out["version"] = 2; out["blobs_v2"] = blobs; }
        return out;
    }
};
}

// As xrpld's applyList: a listed validator without a manifest, or with one that does not verify,
// signs with its master key; a validation signed with a validator's master key is trusted even
// once a manifest names another key (ManifestCache::getMasterKey); a manifest of a validator not
// listed changes nothing. A version 2 list's later blob is kept, pending.
TEST(Validators, ListedValidatorsWithoutManifestsSignWithTheirMasterKeys) {
    namespace r = ::xrpl;
    TestPublisher publisher;
    // Validations are signed with secp256k1 keys only, so a validator signing with its master key has one.
    auto a = r::randomKeyPair(r::KeyType::Secp256k1), a_signing = r::randomKeyPair(r::KeyType::Secp256k1);
    auto b = r::randomKeyPair(r::KeyType::Secp256k1), c = r::randomKeyPair(r::KeyType::Secp256k1), d = r::randomKeyPair(r::KeyType::Secp256k1);
    auto other = r::randomKeyPair(r::KeyType::Ed25519), other_signing = r::randomKeyPair(r::KeyType::Secp256k1);
    auto broken = manifest_of(c, other_signing, 2);
    broken[broken.size() - 1] ^= 1;  // its master signature no longer verifies
    Json validators = Json::array({
        {{"validation_public_key", hex_of(a.first)}, {"manifest", r::base64Encode(manifest_of(a, a_signing, 3))}},
        {{"validation_public_key", hex_of(b.first)}},
        {{"validation_public_key", hex_of(c.first)}, {"manifest", r::base64Encode(broken)}},
        {{"validation_public_key", hex_of(d.first)}, {"manifest", r::base64Encode(manifest_of(other, other_signing, 1))}},
        {{"validation_public_key", "not a key"}}});
    auto now = std::uint64_t{100000};
    auto list = verify_validator_list(publisher.published({publisher.blob(validators, 0, now + 1000), publisher.blob(validators, now + 500, now + 5000, 2)}),
                                      publisher.key(), now);
    EXPECT_EQ(list.expiration, now + 1000);
    EXPECT_EQ(list.next_effective(), now + 500);
    ASSERT_EQ(list.signing_keys.size(), 4u);
    EXPECT_EQ(list.signing_keys.at(key_bytes(a.first)), key_bytes(a_signing.first));
    EXPECT_EQ(list.signing_keys.at(key_bytes(b.first)), key_bytes(b.first));
    EXPECT_EQ(list.signing_keys.at(key_bytes(c.first)), key_bytes(c.first));
    EXPECT_EQ(list.signing_keys.at(key_bytes(d.first)), key_bytes(d.first));
    EXPECT_EQ(list.manifest_sequences.size(), 1u);
    ValidationQuorum quorum(list, [now] { return now; });
    EXPECT_TRUE(quorum.trusted(key_bytes(a.first)));
    EXPECT_TRUE(quorum.trusted(key_bytes(a_signing.first)));
    EXPECT_FALSE(quorum.trusted(key_bytes(other_signing.first)));
    quorum.negative_unl(1000, {{}, {}});
    for (auto const* signer : {&a, &b, &c})
        EXPECT_TRUE(quorum.add(signed_validation(signer->first, signer->second, 1001)));
    EXPECT_TRUE(quorum.add(signed_validation(a_signing.first, a_signing.second, 1001)));  // a again, with its signing key
    EXPECT_EQ(quorum.count(ledger_hash(1001)), 3u);
    EXPECT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::short_of_quorum);  // 4 of 4
    EXPECT_TRUE(quorum.add(signed_validation(d.first, d.second, 1001)));
    EXPECT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::validated);
    // b's first manifest rotates it to a signing key of its own.
    auto b_signing = r::randomKeyPair(r::KeyType::Secp256k1);
    EXPECT_TRUE(quorum.manifest(manifest_of(b, b_signing, 1)));
    EXPECT_TRUE(quorum.trusted(key_bytes(b_signing.first)));
    EXPECT_FALSE(quorum.manifest(manifest_of(other, other_signing, 2)));  // not listed
}

namespace {
// Five validators signing with their master keys, and a list naming those from `from` to `to`.
struct ListedKeys {
    std::vector<std::pair<::xrpl::PublicKey, ::xrpl::SecretKey>> keys;
    ListedKeys() { for (int n = 0; n < 5; ++n) keys.push_back(::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1)); }
    Json listing(int from, int to) const {
        Json validators = Json::array();
        for (auto n = from; n <= to; ++n) validators.push_back({{"validation_public_key", hex_of(keys[n].first)}});
        return validators;
    }
    Bytes key(int n) const { return key_bytes(keys[n].first); }
    void validate(ValidationQuorum& quorum, std::uint32_t seq, int from, int to) const {
        for (auto n = from; n <= to; ++n) ASSERT_TRUE(quorum.add(signed_validation(keys[n].first, keys[n].second, seq))) << n;
    }
};
}

// As xrpld's applyList judges a publisher's list against the one in force: one fetched again
// with an earlier sequence (a list site serving an older list, or one replayed) is refused,
// stale, however much later it expires; one with the same sequence changes nothing; a later one
// replaces it, the validations counted carrying over. Of the blobs in force, the one with the
// highest sequence is.
TEST(Validators, AnOlderListFetchedAgainLeavesTheNewerOneInForce) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto newer = verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 2)}), publisher.key(), now);
    auto older = verify_validator_list(publisher.published({publisher.blob(v.listing(1, 4), 0, now + 9000, 1)}), publisher.key(), now);
    ValidationQuorum quorum(newer, [now] { return now; });
    EXPECT_EQ(quorum.apply(older), ValidationQuorum::Disposition::stale);
    EXPECT_EQ(quorum.list().sequence, 2u);
    EXPECT_EQ(quorum.list().expiration, now + 5000);
    EXPECT_TRUE(quorum.trusted(v.key(0)));
    EXPECT_FALSE(quorum.trusted(v.key(4)));
    EXPECT_EQ(quorum.apply(newer), ValidationQuorum::Disposition::same_sequence);
    EXPECT_EQ(quorum.list().sequence, 2u);
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 1, 3);
    auto later = verify_validator_list(publisher.published({publisher.blob(v.listing(1, 4), 0, now + 9000, 1), publisher.blob(v.listing(1, 4), 0, now + 9000, 3)}),
                                       publisher.key(), now);
    EXPECT_EQ(later.sequence, 3u);
    EXPECT_EQ(quorum.apply(later), ValidationQuorum::Disposition::accepted);
    EXPECT_EQ(quorum.list().sequence, 3u);
    EXPECT_FALSE(quorum.trusted(v.key(0)));
    EXPECT_TRUE(quorum.trusted(v.key(4)));
    EXPECT_EQ(quorum.count(ledger_hash(1001)), 3u);
    EXPECT_EQ(quorum.apply(older), ValidationQuorum::Disposition::stale);
    EXPECT_EQ(quorum.list().sequence, 3u);
}

// A version 2 list's later blobs are kept, verified, and put in force at their effective time
// from the list fetched before, as xrpld's updateTrusted does at the first ledger closed from
// then: the list site need not answer then. A blob with a sequence no later than the one in
// force, and one a later sequence takes the place of
// no later are not kept; of several due at once, the last is put in force. An older list fetched
// meanwhile keeps the pending ones.
TEST(Validators, APendingListTakesEffectWithoutTheListFetchedAgain) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto published = publisher.published({
        publisher.blob(v.listing(0, 3), 0, now + 1000, 5),
        publisher.blob(v.listing(0, 4), now + 700, now + 9000, 4),
        publisher.blob(v.listing(1, 4), now + 500, now + 5000, 6),
        publisher.blob(v.listing(0, 4), now + 2000, now + 9000, 7),
        publisher.blob(v.listing(0, 4), now + 1500, now + 9000, 8)});  // xrpld takes at most 5
    auto list = verify_validator_list(published, publisher.key(), now);
    EXPECT_EQ(list.sequence, 5u);
    ASSERT_EQ(list.pending.size(), 2u);
    EXPECT_EQ(list.pending[0].sequence, 6u);
    EXPECT_EQ(list.pending[1].sequence, 8u);
    EXPECT_EQ(list.next_effective(), now + 500);
    auto clock = now;
    ValidationQuorum quorum(list, [&] { return clock; });
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 1, 3);
    EXPECT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::short_of_quorum);  // 3 of 4
    EXPECT_FALSE(quorum.rotate());
    clock = now + 500;
    EXPECT_TRUE(quorum.rotate());
    EXPECT_EQ(quorum.list().sequence, 6u);
    EXPECT_EQ(quorum.list().expiration, now + 5000);
    EXPECT_FALSE(quorum.trusted(v.key(0)));
    EXPECT_TRUE(quorum.trusted(v.key(4)));
    EXPECT_EQ(quorum.count(ledger_hash(1001)), 3u);
    v.validate(quorum, 1001, 4, 4);
    EXPECT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::validated);
    EXPECT_EQ(quorum.list().next_effective(), now + 1500);
    EXPECT_FALSE(quorum.rotate());
    // The list site serving the older list meanwhile: refused, the pending one kept.
    EXPECT_EQ(quorum.apply(verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 1000, 5)}), publisher.key(), clock)),
              ValidationQuorum::Disposition::stale);
    EXPECT_EQ(quorum.list().sequence, 6u);
    EXPECT_EQ(quorum.list().next_effective(), now + 1500);
    // Both due at once: the last.
    ValidationQuorum late(list, [&] { return clock; });
    clock = now + 1600;
    EXPECT_TRUE(late.rotate());
    EXPECT_EQ(late.list().sequence, 8u);
    EXPECT_TRUE(late.list().pending.empty());
    EXPECT_TRUE(late.trusted(v.key(0)));
    // One put in force after it expired validates nothing, as xrpld clears it.
    ValidationQuorum expired(list, [&] { return clock; });
    clock = now + 9000;
    EXPECT_TRUE(expired.rotate());
    EXPECT_TRUE(expired.expired());
}

// As xrpld's verify and applyList: a list with a later sequence is put in force even when it
// names no validators (or revokes all it names) or has expired, and nothing is validated then;
// a later list still replaces it, and an older one is still refused. Of the blobs whose time has
// come, the one with the highest sequence is in force, expired or not.
TEST(Validators, ALaterListNamingNoValidatorsOrExpiredValidatesNothing) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto fetch = [&](std::vector<Json> const& blobs) { return verify_validator_list(publisher.published(blobs), publisher.key(), now); };
    ValidationQuorum quorum(fetch({publisher.blob(v.listing(0, 3), 0, now + 5000, 1)}), [&] { return now; });
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 0, 3);
    ASSERT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::validated);
    auto empty = fetch({publisher.blob(Json::array(), 0, now + 5000, 2)});
    EXPECT_EQ(empty.sequence, 2u);
    EXPECT_TRUE(empty.signing_keys.empty());
    EXPECT_THROW(validating_list(empty, now), Error);
    EXPECT_EQ(quorum.apply(empty), ValidationQuorum::Disposition::accepted);
    EXPECT_FALSE(quorum.validating());
    EXPECT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::short_of_quorum);
    EXPECT_FALSE(quorum.validates(ledger_hash(1001), 1001, {}));
    // Every validator it names revoked: none left.
    Json revoked = Json::array();
    revoked.push_back({{"validation_public_key", hex_of(v.keys[0].first)}, {"manifest", ::xrpl::base64Encode(manifest_of(v.keys[0], std::nullopt, 0))}});
    EXPECT_TRUE(fetch({publisher.blob(revoked, 0, now + 5000, 2)}).signing_keys.empty());
    // Expired, and preferred to an older blob in force.
    auto expired = fetch({publisher.blob(v.listing(0, 3), 0, now + 5000, 1), publisher.blob(v.listing(0, 3), now - 2000, now - 1000, 3)});
    EXPECT_EQ(expired.sequence, 3u);
    EXPECT_THROW(validating_list(expired, now), Error);
    EXPECT_EQ(quorum.apply(expired), ValidationQuorum::Disposition::accepted);
    EXPECT_TRUE(quorum.expired());
    v.validate(quorum, 1002, 0, 3);
    EXPECT_EQ(quorum.verdict(ledger_hash(1002)), Verdict::short_of_quorum);
    EXPECT_EQ(quorum.apply(fetch({publisher.blob(v.listing(0, 3), 0, now + 5000, 1)})), ValidationQuorum::Disposition::stale);
    EXPECT_TRUE(quorum.expired());
    // A later list in force: the validations counted meanwhile validate.
    EXPECT_EQ(quorum.apply(fetch({publisher.blob(v.listing(0, 3), 0, now + 5000, 4)})), ValidationQuorum::Disposition::accepted);
    EXPECT_TRUE(quorum.validating());
    EXPECT_EQ(quorum.verdict(ledger_hash(1002)), Verdict::validated);
    // The first list fetched naming none: nothing validated until a later one.
    ValidationQuorum first(empty, [&] { return now; });
    EXPECT_FALSE(first.validating());
    EXPECT_EQ(first.apply(fetch({publisher.blob(v.listing(0, 3), 0, now + 5000, 3)})), ValidationQuorum::Disposition::accepted);
    EXPECT_TRUE(first.validating());
}

// A list with none in force yet (sequence 0), or with one naming no validators, keeps its pending
// lists: they take effect at their time, as xrpld's would. Applied to a quorum with a list in
// force, one with none in force changes nothing but its pending lists are kept.
TEST(Validators, PendingListsAreKeptWhileNoneInForceValidates) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto clock = now;
    auto none = verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), now + 500, now + 5000, 2),
                                                           publisher.blob(v.listing(1, 4), now + 1000, now + 5000, 3)}), publisher.key(), now);
    EXPECT_EQ(none.sequence, 0u);
    EXPECT_TRUE(none.signing_keys.empty());
    ASSERT_EQ(none.pending.size(), 2u);
    EXPECT_THROW(validating_list(none, now), Error);
    ValidationQuorum quorum(none, [&] { return clock; });
    EXPECT_FALSE(quorum.validating());
    EXPECT_FALSE(quorum.rotate());
    clock = now + 500;
    EXPECT_TRUE(quorum.rotate());
    EXPECT_EQ(quorum.list().sequence, 2u);
    EXPECT_TRUE(quorum.validating());
    EXPECT_EQ(quorum.list().next_effective(), now + 1000);
    // In force, a list naming none.
    clock = now;
    auto empty = verify_validator_list(publisher.published({publisher.blob(Json::array(), 0, now + 5000, 1),
                                                            publisher.blob(v.listing(0, 3), now + 500, now + 5000, 2)}), publisher.key(), now);
    EXPECT_EQ(empty.sequence, 1u);
    EXPECT_EQ(empty.next_effective(), now + 500);
    ValidationQuorum emptied(empty, [&] { return clock; });
    EXPECT_FALSE(emptied.validating());
    clock = now + 500;
    EXPECT_TRUE(emptied.rotate());
    EXPECT_TRUE(emptied.validating());
    // A quorum with a list in force: the pending lists of one with none in force are kept.
    clock = now;
    ValidationQuorum in_force(verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 1)}), publisher.key(), now),
                              [&] { return clock; });
    EXPECT_EQ(in_force.apply(none), ValidationQuorum::Disposition::stale);
    EXPECT_EQ(in_force.list().sequence, 1u);
    EXPECT_EQ(in_force.list().next_effective(), now + 500);
    clock = now + 1000;
    EXPECT_TRUE(in_force.rotate());
    EXPECT_EQ(in_force.list().sequence, 3u);
}

// As xrpld's publisherManifests_: the publisher's newest manifest seen is kept from one fetch to
// the next. A newer one rotates the key lists are checked against; an older one a list site
// serves then changes nothing, a list signed with the old key is refused and one signed with the
// newer key taken. A revocation puts a list naming no validators in force that no list replaces,
// whatever manifest is served after. Without the manifest seen (a restart), the older one is
// taken, as for a publisher first heard from.
TEST(Validators, ThePublishersNewestManifestIsKeptFromOneFetchToTheNext) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    std::optional<PublisherManifest> seen;
    auto fetch = [&](Json const& published) { return verify_validator_list(published, publisher.key(), now, seen); };
    ValidationQuorum quorum(fetch(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 1)})), [&] { return now; });
    ASSERT_TRUE(seen);
    EXPECT_EQ(seen->sequence, 1u);
    auto old = publisher;
    publisher.signing = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
    publisher.manifest_sequence = 2;
    auto rotated = fetch(publisher.published({publisher.blob(v.listing(0, 4), 0, now + 5000, 2)}));
    EXPECT_EQ(rotated.sequence, 2u);
    EXPECT_EQ(seen->sequence, 2u);
    EXPECT_EQ(seen->signing_key.value_or(""), key_bytes(publisher.signing.first));
    EXPECT_EQ(quorum.apply(rotated), ValidationQuorum::Disposition::accepted);
    // The older manifest served again.
    EXPECT_THROW(fetch(old.published({old.blob(v.listing(0, 3), 0, now + 9000, 3)})), Error);
    EXPECT_EQ(seen->sequence, 2u);
    EXPECT_EQ(fetch(old.published({publisher.blob(v.listing(0, 4), 0, now + 9000, 3)})).sequence, 3u);
    EXPECT_EQ(verify_validator_list(old.published({old.blob(v.listing(0, 3), 0, now + 9000, 3)}), publisher.key(), now).sequence, 3u);
    // A blob's own manifest counts as the list's does: a newer one rotates the key again.
    auto third = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
    auto next = publisher;
    next.signing = third;
    auto own = next.blob(v.listing(0, 4), 0, now + 9000, 4);
    own["manifest"] = ::xrpl::base64Encode(manifest_of(publisher.master, third, 3));
    EXPECT_EQ(fetch(publisher.published({publisher.blob(v.listing(0, 4), 0, now + 9000, 3), own})).sequence, 4u);
    EXPECT_EQ(seen->sequence, 3u);
    EXPECT_EQ(seen->signing_key.value_or(""), key_bytes(third.first));
    // Revoked.
    auto revocation = publisher.published({publisher.blob(v.listing(0, 4), 0, now + 9000, 4)});
    revocation["manifest"] = ::xrpl::base64Encode(manifest_of(publisher.master, std::nullopt, 0));
    auto revoked = fetch(revocation);
    EXPECT_EQ(revoked.sequence, revoked_sequence);
    EXPECT_TRUE(revoked.signing_keys.empty());
    EXPECT_TRUE(revoked.pending.empty());
    EXPECT_FALSE(seen->signing_key);
    EXPECT_THROW(validating_list(revoked, now), Error);
    EXPECT_EQ(quorum.apply(revoked), ValidationQuorum::Disposition::accepted);
    EXPECT_FALSE(quorum.validating());
    publisher.manifest_sequence = 4;
    auto after = fetch(publisher.published({publisher.blob(v.listing(0, 4), 0, now + 9000, 5)}));
    EXPECT_EQ(after.sequence, revoked_sequence);
    EXPECT_EQ(quorum.apply(after), ValidationQuorum::Disposition::same_sequence);
    EXPECT_FALSE(quorum.validating());
}

// A validated ledger's close time given: the list expires, and a pending one takes effect, once
// it reaches their times, should the host's clock be behind.
TEST(Validators, AValidatedLedgersCloseTimeJudgesTheListsTimes) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto list = verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 1000, 1),
                                                           publisher.blob(v.listing(1, 4), now + 500, now + 5000, 2)}), publisher.key(), now);
    ValidationQuorum quorum(list, [&] { return now; }), expiring(list, [&] { return now; });
    quorum.closed(now + 600);
    EXPECT_TRUE(quorum.rotate());
    EXPECT_EQ(quorum.list().sequence, 2u);
    expiring.closed(now + 400);
    expiring.closed(now + 100);  // the newest given stands
    EXPECT_FALSE(expiring.rotate());
    list.pending.clear();
    ValidationQuorum ended(list, [&] { return now; });
    ended.closed(now + 1000);
    EXPECT_TRUE(ended.expired());
    ValidationQuorum refetched(list, [&] { return now; });
    refetched.adopt(ended);
    EXPECT_TRUE(refetched.expired());
}

// A validator a list put in force later adds counts from its next validation on: one seen
// before, while not listed, is not counted then, as xrpld's trustChanged counts none of a
// validator signing with a manifest's key (it held them under that key, not the master key).
TEST(Validators, ValidationsSeenBeforeTheirValidatorsAreListedAreNotCounted) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto list = verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 1),
                                                           publisher.blob(v.listing(0, 4), now + 500, now + 5000, 2)}), publisher.key(), now);
    auto clock = now;
    ValidationQuorum quorum(list, [&] { return clock; });
    quorum.negative_unl(1000, {{}, {}});
    v.validate(quorum, 1001, 1, 3);
    EXPECT_FALSE(quorum.add(signed_validation(v.keys[4].first, v.keys[4].second, 1001), std::chrono::steady_clock::now() - std::chrono::minutes(1)));
    clock = now + 500;
    EXPECT_TRUE(quorum.rotate());
    EXPECT_TRUE(quorum.trusted(v.key(4)));
    EXPECT_EQ(quorum.count(ledger_hash(1001)), 3u);
    EXPECT_EQ(quorum.verdict(ledger_hash(1001)), Verdict::short_of_quorum);  // 4 of 5 needed
    // Its next validation counts.
    v.validate(quorum, 1002, 1, 4);
    EXPECT_EQ(quorum.verdict(ledger_hash(1002)), Verdict::validated);
}

// As xrpld's verify: a list blob's sequence, expiration and effective time are JSON integers of 0
// to 2^31 - 1 (json::Value::isInt, and not negative), and its validators an array; another blob
// is not taken, however well signed, where a cast would read -1 as the revoked sequence or a
// negative expiration as one far off (a negative one also ends the list: see below).
TEST(Validators, AListBlobsNumbersAreTheIntegersXrpldReads) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto validators = v.listing(0, 3).dump();
    auto good = publisher.blob(v.listing(0, 3), 0, now + 5000, 2);
    for (std::string body : {R"({"sequence": -1, "expiration": 105000, "validators": )" + validators + "}",
                             R"({"sequence": 2147483648, "expiration": 105000, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": -1, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": 4294967295, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": 105000.0, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": 1.05e5, "validators": )" + validators + "}",
                             R"({"sequence": "3", "expiration": 105000, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": 105000, "effective": -100, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": 105000, "effective": null, "validators": )" + validators + "}",
                             R"({"sequence": 3, "expiration": 105000, "effective": 105000, "validators": )" + validators + "}",
                             std::string(R"({"sequence": 3, "expiration": 105000, "validators": {}})"),
                             R"({"expiration": 105000, "validators": )" + validators + "}",
                             std::string(R"([3, 105000])")}) {
        SCOPED_TRACE(body);
        auto bad = publisher.signed_blob(body);
        EXPECT_THROW(verify_validator_list(publisher.published({bad}), publisher.key(), now), Error);
        auto list = verify_validator_list(publisher.published({good, bad}), publisher.key(), now);
        EXPECT_EQ(list.sequence, 2u);
        EXPECT_EQ(list.signing_keys.size(), 4u);
    }
    auto largest = verify_validator_list(publisher.published({publisher.signed_blob(R"({"sequence": 2147483647, "expiration": 2147483647, "validators": )" + validators + "}")}),
                                         publisher.key(), now);
    EXPECT_EQ(largest.sequence, 2147483647u);
    EXPECT_EQ(largest.expiration, 2147483647u);
}

// As xrpld's applyLists, each blob of a list is judged on its own: one signed with another key,
// or whose manifest does not verify or names another publisher (Invalid, Untrusted), is passed
// over and the others taken. Only when every blob is does the fetch fail.
TEST(Validators, ABadBlobIsPassedOverAndTheOthersTaken) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto stranger = publisher;
    stranger.signing = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
    auto other_key = stranger.blob(v.listing(0, 4), 0, now + 5000, 5);  // not the key the manifest names
    TestPublisher other;
    auto other_publisher = other.blob(v.listing(0, 4), 0, now + 5000, 6);
    other_publisher["manifest"] = ::xrpl::base64Encode(manifest_of(other.master, other.signing, 1));
    auto broken = publisher.blob(v.listing(0, 4), 0, now + 5000, 7);
    auto manifest = manifest_of(publisher.master, publisher.signing, 2);
    manifest[manifest.size() - 1] ^= 1;  // its master signature no longer verifies
    broken["manifest"] = ::xrpl::base64Encode(manifest);
    auto good = publisher.blob(v.listing(0, 3), 0, now + 5000, 2);
    auto pending = publisher.blob(v.listing(1, 4), now + 500, now + 5000, 3);
    auto list = verify_validator_list(publisher.published({other_key, good, other_publisher, broken, pending}), publisher.key(), now);
    EXPECT_EQ(list.sequence, 2u);
    EXPECT_EQ(list.signing_keys.size(), 4u);
    ASSERT_EQ(list.pending.size(), 1u);
    EXPECT_EQ(list.pending[0].sequence, 3u);
    EXPECT_THROW(verify_validator_list(publisher.published({other_key, other_publisher, broken}), publisher.key(), now), Error);
    EXPECT_THROW(verify_validator_list(publisher.published({other_key}), publisher.key(), now), Error);
}

// As xrpld's verify, which throws on a negative sequence, expiration or effective time
// (json::Value::asUInt): its applyLists ends there, the blobs before it taken and none after it
// read. A list whose first blob taken would be one after it fails.
TEST(Validators, ANegativeNumberEndsTheListAtItsBlob) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto validators = v.listing(0, 4).dump();
    auto negative = publisher.signed_blob(R"({"sequence": 4, "expiration": -1, "validators": )" + validators + "}");
    auto good = publisher.blob(v.listing(0, 3), 0, now + 5000, 2);
    auto later = publisher.blob(v.listing(0, 4), 0, now + 5000, 3);
    auto pending = publisher.blob(v.listing(1, 4), now + 500, now + 5000, 5);
    EXPECT_THROW(verify_validator_list(publisher.published({negative, good, later}), publisher.key(), now), Error);
    auto stranger = publisher;
    stranger.signing = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
    auto passed_over = stranger.blob(v.listing(0, 4), 0, now + 5000, 6);  // not the key the manifest names
    EXPECT_THROW(verify_validator_list(publisher.published({passed_over, negative, good}), publisher.key(), now), Error);
    auto list = verify_validator_list(publisher.published({good, negative, later, pending}), publisher.key(), now);
    EXPECT_EQ(list.sequence, 2u);
    EXPECT_EQ(list.signing_keys.size(), 4u);
    EXPECT_TRUE(list.pending.empty());
    for (std::string body : {R"({"sequence": -3, "expiration": 105000, "validators": )" + validators + "}",
                             R"({"sequence": 4, "expiration": 105000, "effective": -2147483648, "validators": )" + validators + "}"}) {
        SCOPED_TRACE(body);
        auto ends = publisher.signed_blob(body);
        EXPECT_EQ(verify_validator_list(publisher.published({pending, good, ends, later}), publisher.key(), now).sequence, 2u);
        EXPECT_EQ(verify_validator_list(publisher.published({pending, good, ends, later}), publisher.key(), now).pending.size(), 1u);
    }
}

// As xrpld's reader (json_reader.cpp), which fails a whole document nlohmann reads: a blob body
// with an integer anywhere outside -2^31 to 2^32 - 1, nested more than 25 deep (kNestLimit), or
// naming a key twice in an object is not taken (Invalid); one at those bounds is.
TEST(Validators, ABlobBodyIsReadAsXrpldsReaderReadsIt) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto validators = v.listing(0, 4).dump();
    auto nested = [](int depth) { return std::string(depth, '[') + "0" + std::string(depth, ']'); };
    auto good = publisher.blob(v.listing(0, 3), 0, now + 5000, 2);
    auto body = [&](std::string const& extra) { return R"({"sequence": 3, "expiration": 105000, "validators": )" + validators + ", " + extra + "}"; };
    for (std::string refused : {body(R"("x": 4294967296)"), body(R"("x": -2147483649)"), body(R"("x": 18446744073709551616)"),
                                body(R"("x": {"y": [1, 2, 99999999999999999999]})"), body(R"("x": )" + nested(25)),
                                body(R"("sequence": 4)"), body(R"("x": [{"y": 1, "y": 2}])")}) {
        SCOPED_TRACE(refused);
        EXPECT_THROW(parse_as_xrpld(refused), Error);
        auto bad = publisher.signed_blob(refused);
        EXPECT_THROW(verify_validator_list(publisher.published({bad}), publisher.key(), now), Error);
        EXPECT_EQ(verify_validator_list(publisher.published({good, bad}), publisher.key(), now).sequence, 2u);
    }
    auto bounds = publisher.signed_blob(body(R"("x": [4294967295, -2147483648, 1.5e300], "y": )" + nested(24) + R"(, "z": [{"y": 1}, {"y": 2}])"));
    EXPECT_EQ(verify_validator_list(publisher.published({good, bounds}), publisher.key(), now).sequence, 3u);
    EXPECT_THROW(parse_as_xrpld("5"), Error);
    EXPECT_TRUE(parse_as_xrpld("[]").is_array());
}

// As xrpld's parseJsonResponse and parseBlobs: a list without its manifest or an integer
// version, a version 1 list carrying blobs_v2, a version 2 list carrying a blob or signature
// beside blobs_v2, or more than 5 blobs (kMaxSupportedBlobs), or blobs_v2 not an array, is
// refused whole, however good its blobs.
TEST(Validators, AListXrpldRefusesWholeIsRefused) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto one = publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 2)});
    std::vector<Json> five;
    for (std::uint32_t sequence = 1; sequence <= 5; ++sequence) five.push_back(publisher.blob(v.listing(0, 3), 0, now + 5000, sequence));
    auto several = publisher.published(five);
    ASSERT_EQ(verify_validator_list(one, publisher.key(), now).sequence, 2u);
    ASSERT_EQ(verify_validator_list(several, publisher.key(), now).sequence, 5u);
    std::vector<Json> refused;
    auto changed = [&](Json list, auto change) { change(list); refused.push_back(list); };
    changed(one, [](Json& list) { list.erase("version"); });
    changed(one, [](Json& list) { list["version"] = 1.0; });
    changed(one, [](Json& list) { list["version"] = "1"; });
    changed(one, [](Json& list) { list["version"] = 3; });
    changed(one, [](Json& list) { list.erase("manifest"); });
    changed(one, [](Json& list) { list["blobs_v2"] = Json::array(); });
    changed(one, [](Json& list) { list["signature"] = 1; });
    changed(several, [](Json& list) { list["blob"] = list["blobs_v2"][0]["blob"]; });
    changed(several, [](Json& list) { list["signature"] = list["blobs_v2"][0]["signature"]; });
    changed(several, [&](Json& list) { list["blobs_v2"].push_back(publisher.blob(v.listing(0, 3), 0, now + 5000, 6)); });
    changed(several, [](Json& list) {
        Json first = list["blobs_v2"][0];
        list["blobs_v2"] = first;  // an object, not an array
    });
    changed(several, [](Json& list) { list["blobs_v2"] = Json::array(); });
    changed(several, [](Json& list) { list["blobs_v2"][1]["manifest"] = 1; });
    changed(several, [](Json& list) { list["blobs_v2"][1].erase("signature"); });
    // Each blob with a manifest of its own: the list's is still needed.
    changed(several, [](Json& list) {
        for (auto& blob : list["blobs_v2"]) blob["manifest"] = list["manifest"];
        list.erase("manifest");
    });
    for (auto const& list : refused) {
        SCOPED_TRACE(list.dump());
        EXPECT_THROW(verify_validator_list(list, publisher.key(), now), Error);
    }
}

// As xrpld's applyList, which refuses a publisher manifest longer in base64 than a manifest can
// be (kMaxManifestBase64, 480 characters) before decoding it: a genuine manifest followed by
// more, which base64Decode would stop at, fails its blob; one within that length is read.
TEST(Validators, APublisherManifestLongerThanOneCanBeIsRefused) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    auto manifest = ::xrpl::base64Encode(manifest_of(publisher.master, publisher.signing, publisher.manifest_sequence));
    ASSERT_LT(manifest.size(), 480u);
    auto padded = [&](std::size_t size) { return manifest + std::string(size - manifest.size(), '*'); };
    auto list = publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 2)});
    list["manifest"] = padded(480);
    EXPECT_EQ(verify_validator_list(list, publisher.key(), now).sequence, 2u);
    list["manifest"] = padded(481);
    EXPECT_THROW(verify_validator_list(list, publisher.key(), now), Error);
    // A blob's own: that blob is passed over, the others taken.
    auto own = publisher.blob(v.listing(0, 4), 0, now + 5000, 3);
    own["manifest"] = padded(481);
    auto both = verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 2), own}), publisher.key(), now);
    EXPECT_EQ(both.sequence, 2u);
    own["manifest"] = padded(480);
    EXPECT_EQ(verify_validator_list(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 2), own}), publisher.key(), now).sequence, 3u);
}

// As xrpld's ManifestCache::applyManifest: a publisher manifest not newer than the newest seen
// changes nothing, its signatures not checked (Stale), and a newer one naming the key in use
// changes nothing either (BadEphemeralKey): its sequence is not taken, so one between the two
// still rotates the key.
TEST(Validators, APublisherManifestOlderOrNamingTheKeyInUseChangesNothing) {
    TestPublisher publisher;
    ListedKeys v;
    auto now = std::uint64_t{100000};
    std::optional<PublisherManifest> seen;
    auto fetch = [&](Json const& published) { return verify_validator_list(published, publisher.key(), now, seen); };
    ASSERT_EQ(fetch(publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 1)})).sequence, 1u);
    auto stale = publisher.published({publisher.blob(v.listing(0, 3), 0, now + 5000, 2)});
    auto broken = manifest_of(publisher.master, publisher.signing, 1);
    broken[broken.size() - 1] ^= 1;  // its master signature no longer verifies
    stale["manifest"] = ::xrpl::base64Encode(broken);
    EXPECT_EQ(fetch(stale).sequence, 2u);
    EXPECT_THROW(verify_validator_list(stale, publisher.key(), now), Error);  // the first manifest seen: checked
    auto same_key = publisher;
    same_key.manifest_sequence = 3;
    EXPECT_EQ(fetch(same_key.published({same_key.blob(v.listing(0, 3), 0, now + 5000, 3)})).sequence, 3u);
    EXPECT_EQ(seen->sequence, 1u);
    auto rotated = publisher;
    rotated.signing = ::xrpl::randomKeyPair(::xrpl::KeyType::Secp256k1);
    rotated.manifest_sequence = 2;
    EXPECT_EQ(fetch(rotated.published({rotated.blob(v.listing(0, 3), 0, now + 5000, 4)})).sequence, 4u);
    EXPECT_EQ(seen->sequence, 2u);
    EXPECT_EQ(seen->signing_key.value_or(""), key_bytes(rotated.signing.first));
}

// A validations stream that goes silent (a node out of touch, a connection gone half-open):
// a receive with a deadline ends in an error, for the watcher to connect again, instead of
// waiting for ever.
TEST(Validators, ASilentStreamEndsAReceiveAtItsDeadline) {
    namespace net = boost::asio;
    namespace websocket = boost::beast::websocket;
    using tcp = net::ip::tcp;
    net::io_context io;
    tcp::acceptor acceptor(io, {net::ip::make_address("127.0.0.1"), 0});
    auto port = acceptor.local_endpoint().port();
    std::thread server([&] {
        websocket::stream<tcp::socket> stream(acceptor.accept());
        stream.accept();
        stream.text(true);
        stream.write(net::buffer(std::string(R"({"type":"validationReceived"})")));
        boost::beast::flat_buffer buffer;
        boost::beast::error_code closed;
        stream.read(buffer, closed);  // silent until the client goes
    });
    WsClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
    EXPECT_EQ(client.receive(std::chrono::milliseconds(5000)).at("type"), "validationReceived");
    auto started = std::chrono::steady_clock::now();
    EXPECT_THROW(client.receive(std::chrono::milliseconds(300)), Error);
    auto waited = std::chrono::steady_clock::now() - started;
    EXPECT_GE(waited, std::chrono::milliseconds(250));
    EXPECT_LT(waited, std::chrono::seconds(5));
    server.join();  // the client closed its end
}

// A node that accepts the connection but never answers the WebSocket handshake: connecting ends
// in an error at its deadline instead of holding the watcher's thread for ever.
TEST(Validators, AnUnansweredHandshakeEndsAtItsDeadline) {
    namespace net = boost::asio;
    using tcp = net::ip::tcp;
    net::io_context io;
    tcp::acceptor acceptor(io, {net::ip::make_address("127.0.0.1"), 0});
    auto port = acceptor.local_endpoint().port();
    std::thread server([&] {
        auto socket = acceptor.accept();
        char byte;
        boost::system::error_code closed;
        socket.read_some(net::buffer(&byte, 1), closed);  // the request, never answered
        while (!closed) socket.read_some(net::buffer(&byte, 1), closed);  // until the client goes
    });
    auto started = std::chrono::steady_clock::now();
    EXPECT_THROW(WsClient("ws://127.0.0.1:" + std::to_string(port) + "/", std::chrono::milliseconds(300)), Error);
    auto waited = std::chrono::steady_clock::now() - started;
    EXPECT_GE(waited, std::chrono::milliseconds(250));
    EXPECT_LT(waited, std::chrono::seconds(5));
    server.join();
}
