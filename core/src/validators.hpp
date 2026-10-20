#pragma once
#include <xrpla/core/verify.hpp>
#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <vector>

namespace xrpla::core {
// A validator list in the format list sites serve (e.g. vl.ripple.com), verified against the
// list publisher's pinned master key: the publisher's manifest names the key that signed the
// list, and each listed validator's manifest names its current signing key. As xrpld's
// applyList, a listed validator without a manifest (or with one that does not verify) signs with
// its master key, and a revoked one is not listed.
struct ValidatorList {
    std::uint32_t sequence = 0;
    std::uint64_t effective = 0;              // seconds since 2000-01-01 (the XRPL epoch); 0: from the start
    std::uint64_t expiration = 0;             // seconds since 2000-01-01; 0: never
    std::map<Bytes, Bytes> signing_keys;      // validator master key -> signing key
    std::map<Bytes, std::uint32_t> manifest_sequences;  // of those listed with a manifest
    // The publisher's later lists (a version 2 list's blobs not in force yet), verified, by
    // sequence: each takes effect later than the one before (ValidationQuorum::rotate).
    std::vector<ValidatorList> pending;
    // When the first of them takes effect; 0: none.
    std::uint64_t next_effective() const { return pending.empty() ? 0 : pending.front().effective; }
    // As xrpld computes it for a list of n validators, k of them on the ledger's negative UNL
    // (master keys): max(ceil(0.8 (n - k)), ceil(0.6 n)), in single precision as xrpld does.
    std::size_t quorum(std::set<Bytes> const& negative_unl = {}) const;
};
// The negative UNL as xrpld reads it from a ledger's NegativeUNL object (serialized; empty when
// the ledger has none): the master keys sfDisabledValidators lists, and those the next flag
// ledger lists once it applies sfValidatorToDisable and sfValidatorToReEnable.
struct NegativeUnl { std::set<Bytes> disabled, next; };
NegativeUnl negative_unl(Bytes const& serialized);
// The newest manifest seen of a list publisher (its master key pinned): its sequence, and the key
// it names to sign lists, none once the key is revoked (as xrpld's publisherManifests_).
struct PublisherManifest { std::uint32_t sequence = 0; std::optional<Bytes> signing_key; };
// The sequence of the list verify_validator_list gives for a publisher whose key is revoked: it
// names no validators, and no list replaces it (ValidationQuorum::apply), as xrpld removes the
// publisher's lists, those pending too, for good (ValidatorList.cpp:1256-1262, verify).
constexpr std::uint32_t revoked_sequence = std::numeric_limits<std::uint32_t>::max();
// A JSON document parsed as xrpld's reader reads it (json_reader.cpp), for a validator list and
// its blobs: an error where that refuses one nlohmann takes, a value nested more than 25 deep
// (kNestLimit), an integer outside -2^31 to 2^32 - 1, a key twice in an object, or a document
// not an object, an array or null. Some documents xrpld's reader takes are refused here, as
// nlohmann refuses them: comments, numbers with leading zeros, content after the document,
// strings not UTF-8.
Json parse_as_xrpld(std::string const& text);
// `published` is the list as a list site serves it, parsed with parse_as_xrpld; `publisher_key`
// is the list publisher's master public key in hex; `now` is XRPL-epoch seconds.
// As xrpld's applyLists for a publisher first heard from (ValidatorList.cpp:1287-1333, 913-927):
// of the blobs whose time has come by `now`, the one with the highest sequence, expired or not
// (xrpld puts an expired one in force, Expired, and validates nothing), and as its pending lists
// those with later sequences not in force yet, each kept unless a later one takes effect no
// later. A blob with sequence 0 (xrpld's for a publisher with no list yet: SameSequence) is
// taken, but neither in force nor pending. With none in force, a list of sequence 0 naming no
// validators, with the pending ones; one in force may name none as well. Neither validates.
// A blob whose publisher manifest or signature does not verify, whose body xrpld's reader
// refuses (parse_as_xrpld), or whose body xrpld's verify does not read as a list (its sequence,
// expiration and effective time integers of -2^31 to 2^31 - 1, its validators an array), or
// that expires before it takes effect, is passed over, as xrpld's applyLists judges each blob on
// its own (Untrusted, Invalid): an error only when every blob is. A blob whose sequence,
// expiration or effective time is negative ends the list there, as xrpld's verify throws then
// (asUInt) and its applyLists reads no blob after it: those before it are kept, an error only
// when none was taken. A list xrpld's parseJsonResponse and parseBlobs refuse whole is an error:
// its manifest not a string, its version not an integer, 1 or 2, a version 1 list without its
// blob and signature (strings) or with blobs_v2, a version 2 list without 1 to 5 blobs in
// blobs_v2 (each with its blob and signature, and any manifest of its own, strings) or with a
// blob or signature beside them.
//
// `publisher` is the newest publisher manifest seen before (none for a publisher first heard
// from), updated here as xrpld's verify applies each blob's (its own, else the list's): an older
// one changes nothing, its signatures not checked, nor does a newer one naming the key in use
// (xrpld's ManifestCache refuses one naming a signing key it holds, BadEphemeralKey), the blob
// then checked against the key in use; a manifest longer in base64 than one can be (480
// characters) fails its blob. Once one revokes the key, the list of revoked_sequence is given,
// however the list site answers later.
ValidatorList verify_validator_list(Json const& published, std::string const& publisher_key, std::uint64_t now, std::optional<PublisherManifest>& publisher);
ValidatorList verify_validator_list(Json const& published, std::string const& publisher_key, std::uint64_t now);
// The list, for a tool that needs one validating at `now`: an error unless one is in force, has
// not expired and names a validator.
ValidatorList validating_list(ValidatorList list, std::uint64_t now);
std::uint64_t xrpl_seconds_now();
// A message from xrpld's manifests stream carries a manifest's fields, not its bytes: the
// serialized manifest rebuilt from them (ValidationQuorum::manifest checks its signatures).
// nullopt when the message is not a manifest.
std::optional<Bytes> manifest_from_stream(Json const& message);
// xrpld's manifest request for a validator (its master key), and the serialized manifest its
// answer carries: nullopt when the message is not one, or the node holds none.
Json manifest_request(Bytes const& master, int id);
std::optional<Bytes> manifest_from_answer(Json const& message);

// Counts full validations from trusted validators per ledger hash. A ledger is validated
// once validations from a quorum of distinct trusted validators name its hash, under the
// negative UNL xrpld judges it with:
//
// - xrpld judges validations for ledger L under the negative UNL of L's parent: each consensus
//   round starts from the previous ledger and sets that ledger's sfDisabledValidators as the
//   negative UNL (NetworkOPs.cpp:2254, beginConsensus; Ledger.cpp:639, Ledger::negativeUNL),
//   which sets the quorum (ValidatorList.cpp:1940-1960 updateTrusted, 1708 calculateQuorum)
//   and filters the validations LedgerMaster::checkAccept counts for a ledger
//   (LedgerMaster.cpp:900 and 965; ValidatorList.cpp:2010 negativeUNLFilter). Validations
//   for L that arrive once the next round has started (from L) are judged under L's list.
// - The list changes only at flag ledgers (every 256th): BuildLedger.cpp:49 applies the
//   pending sfValidatorToDisable and sfValidatorToReEnable as it builds one
//   (Ledger.cpp:692, updateNegativeUNL), and UNLModify sets new ones only in flag ledgers
//   (Change.cpp:297). So every ledger from flag ledger F up to F + 255 carries one list, and
//   ledger F + 256 the one that ledger's pending changes give (NegativeUnl::next).
//
// A ledger L is therefore validated once it reaches the quorum under L - 1's list and, when L
// is a flag ledger (whose own list may differ), under L's too (L - 1's pending changes applied).
// Those lists are known for the ledgers after a committed ledger, from its verified state, up to
// the end of the next 256-ledger period (negative_unl(ledger, unl) below: exact for the ledgers
// that descend from it, the only ones a follower commits), and for one ledger, from its own
// parent's verified state (negative_unl(hash, ledger, unl): another ledger of that sequence or
// period may descend from another parent). A ledger whose lists are not known is never
// validated: it is provisional once validators as many as the lowest quorum any negative UNL
// leaves signed it (60% of the list, ValidatorList.cpp:1795), a ledger worth verifying up to so
// that its parent's list is known, and it is judged again once its list is given.
//
// Once the list expires nothing is validated, as xrpld's quorum is then out of reach
// (ValidatorList.cpp:1881 removePublisherList, 1758 calculateQuorum); nor while it names no
// validator (or all of them are revoked), as xrpld then locks down (updateTrusted, setUNLBlocked).
//
// xrpld judges expirations and effective times against the newest ledger's close time (its
// network clock, should that be more than 30 s ahead: updateTrusted); a follower against its own
// clock, or the newest validated ledger's close time given (closed) when later. So the host's
// clock must be kept to time (NTP): one behind rotates to a pending list late, one ahead early.
class ValidationQuorum {
public:
    // `now` gives XRPL-epoch seconds, against which the list's expiration is judged.
    explicit ValidationQuorum(ValidatorList list, std::function<std::uint64_t()> now = xrpl_seconds_now);
    // A newer manifest (from the manifests stream) rotates a validator's signing key, or revokes
    // it: the validator is then no longer trusted, nor the validations it signed (xrpld's
    // updateTrusted and trustChanged).
    bool manifest(Bytes const& serialized);
    enum class Verdict { short_of_quorum, validated, provisional };
    struct Counted { Bytes ledger_hash; std::uint32_t ledger_index; std::size_t count; bool validated; Verdict verdict; };
    // A serialized validation: nullopt when its signature fails, its signer is not a trusted
    // validator's current key (or master key, which xrpld trusts as well), xrpld's checks of a
    // validator's sequence refuse it (below), or it is partial; otherwise the ledger's updated
    // count. One of a key not trusted is not kept (see adopt).
    //
    // As xrpld's Validations::add (Validations.h:631-706, SeqEnforcer): a validator's validation
    // counts only for a sequence after the newest it validated in the last 10 minutes
    // (validationSetExpires), or for that same ledger again (a full validation of it counted);
    // a second ledger at that sequence, or an earlier sequence, is refused (Conflicting, BadSeq).
    // A partial validation is never counted, but takes the validator's sequence as a full one
    // does (xrpld adds both; checkAccept counts full ones only). `now` is when it arrived. xrpld's
    // staleness window (isCurrent: signed more than 3 minutes before its network-adjusted clock)
    // is not applied: a follower keeps no network time, and a clock off by minutes would refuse
    // every validation.
    std::optional<Counted> add(Bytes const& serialized, std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
    // The validations counted for the hash that xrpld counts (those of validators not on its
    // negative UNL; while that is not known, all).
    std::size_t count(Bytes const& ledger_hash) const;
    bool validated(Bytes const& ledger_hash) const { return verdict(ledger_hash) == Verdict::validated; }
    // Of the sequences validations named for the hash (one, unless a validator is faulty), the
    // best verdict: validated, else provisional, else short of the quorum.
    Verdict verdict(Bytes const& ledger_hash) const;
    // Every ledger counted, judged under the negative UNLs known now (to judge them again
    // once another is given).
    std::vector<Counted> ledgers() const;
    // The negative UNL read from committed ledger `ledger`'s verified state (its NegativeUNL
    // object): the list of every ledger in its 256-ledger period and of the next period's. One
    // per period is kept, for the newest two periods given (the ledgers after the newest
    // committed one are judged with those). The validations of ledgers no longer needed are
    // forgotten then (adopt says which are kept).
    void negative_unl(std::uint32_t ledger, NegativeUnl unl);
    // The negative UNL read from the verified state of the parent of ledger `ledger` (the one
    // with this hash): that ledger alone is judged under it, before any list given above.
    // Kept until a committed ledger at or after it is given.
    void negative_unl(Bytes const& ledger_hash, std::uint32_t ledger, NegativeUnl parent);
    // Whether the ledger reaches its quorum under its parent's negative UNL (from verified state)
    // and, for a flag ledger, its own (the parent's pending changes applied), judged here alone:
    // no list is given or kept.
    bool validates(Bytes const& ledger_hash, std::uint32_t ledger, NegativeUnl const& parent) const;
    // The negative UNL in ledger `ledger`'s state (its sfDisabledValidators), which xrpld judges
    // the next ledger's validations under: nullopt unless a committed ledger given above is in
    // its period or the one before.
    std::optional<std::set<Bytes>> disabled(std::uint32_t ledger) const;
    // Whether the list has expired: nothing is validated then.
    bool expired() const { return list_.expiration && now() >= list_.expiration; }
    // Whether anything can be validated: the list in force has not expired and names a validator.
    bool validating() const { return !expired() && !list_.signing_keys.empty(); }
    // The close time (XRPL-epoch seconds) of a ledger a quorum validated: the list expires, and a
    // pending one takes effect, once the newest given reaches their times, should the host's
    // clock not have yet.
    void closed(std::uint64_t close_time) { closed_ = std::max(closed_, close_time); }
    // Whether a key is a listed validator's current signing key, or its master key.
    bool trusted(Bytes const& signing_key) const { return master_for(signing_key).has_value(); }
    ValidatorList const& list() const { return list_; }
    // What a quorum built from an earlier fetch of the list learned, for one that replaces it
    // (the list re-fetched): the newer manifests (key rotations, revocations) of validators both
    // lists name, since the list site may still serve older ones, the validations counted from
    // validators this list names for the newest 256 ledgers and for the ledgers after the newest
    // committed one (the newest 16384 ledgers at most), so a ledger short of its quorum when the
    // list changed still reaches it and one not committed yet is still judged again (a validator
    // revoked, a negative UNL given), each validator's newest validation (for the checks add()
    // makes), the negative UNLs given, and the ledger kept (keep). The newest ledger is the
    // newest that validators as many as the lowest quorum signed (else the one most of them
    // signed), or the newest committed one if later: one trusted validator signing a far later
    // sequence (a faulty one) does not make the network's ledgers look old, and the validations
    // of ledgers more than 16384 after it are not kept. A committed ledger given forgets the
    // validations the same rule leaves out, as a list fetched again does. The newest close time
    // given carries over too. A validator this list adds counts from its next validation on, as
    // xrpld counts one signing with a manifest's key: it holds the validations of a validator not
    // listed under their signing key (handleNewValidation), and its trustChanged trusts only those
    // held under a master key the list adds. (Those of one signing with its master key, which
    // xrpld does count then, are not counted here either: it counts from its next one too.)
    void adopt(ValidationQuorum const& previous);
    // The list fetched again, judged against the one in force as xrpld's applyList judges a
    // publisher's list (ValidatorList.cpp:1296-1303): one with a later sequence replaces it,
    // what this quorum learned carried over (adopt), even one that names no validators or has
    // expired (nothing is validated then); one with the same sequence changes nothing; one with
    // an earlier sequence (a list site serving an older list) is refused, stale, as is one with
    // none in force (sequence 0) while one is, and every list after the publisher's key is
    // revoked (revoked_sequence). The pending lists of both are kept either way, those with later
    // sequences than the one in force then, each unless a later one takes effect no later
    // (applyLists, ValidatorList.cpp:913-927).
    enum class Disposition { accepted, same_sequence, stale };
    Disposition apply(ValidatorList fetched);
    // Puts in force the last of the pending lists whose effective time has come (see above), as
    // xrpld's updateTrusted does at the first ledger closed from then (ValidatorList.cpp:
    // 1813-1872), without the list fetched again; what this quorum learned carries over (adopt).
    // One expired by then validates nothing. Whether one was put in force.
    bool rotate();
    // The ledger a batch under way ends at: its validations are kept however long the batch
    // takes, through lists fetched again (adopt) and committed ledgers given before it, so its
    // commit check still finds them; until a committed ledger at or after it is given. One at a
    // time: another replaces it.
    void keep(Bytes const& ledger_hash, std::uint32_t ledger);
private:
    ValidatorList list_;
    std::function<std::uint64_t()> now_;
    std::map<Bytes, Bytes> master_of_;               // signing key -> master key
    struct Signers { std::uint32_t ledger_index = 0; std::set<Bytes> masters; };
    // (ledger hash, its sequence as 4 big-endian bytes) -> masters that validated it: a validation
    // naming another sequence for a hash (a faulty validator's) is counted apart, never for it.
    std::map<Bytes, Signers> ledgers_;
    // Master key -> the newest validation seen from it (its ledger's key above), when, and
    // whether it was full (counted).
    struct Newest { std::uint32_t ledger_index = 0; Bytes key; std::chrono::steady_clock::time_point when; bool full = false; };
    std::map<Bytes, Newest> newest_;
    // The newest validated ledger's close time given (closed); 0: none.
    std::uint64_t closed_ = 0;
    // XRPL-epoch seconds, the list's times are judged against: the host's, or the newest close
    // time given when later.
    std::uint64_t now() const { return std::max(now_(), closed_); }
    // Committed ledger sequence -> the negative UNL read from its state (one per period).
    std::map<std::uint32_t, NegativeUnl> negative_unls_;
    // Ledger (its key above) -> the negative UNL read from its parent's state.
    std::map<Bytes, NegativeUnl> parents_;
    // The ledger (its key above) whose validations are kept (keep).
    std::optional<Bytes> kept_;
    // Forgets the validations of the ledgers adopt does not keep.
    void forget_old();
    // This quorum, judged under `list` instead, what it learned carried over (adopt).
    void replace(ValidatorList list);
    std::optional<Bytes> master_for(Bytes const& signing_key) const;
    // The lists the ledger is judged under (see above): nullopt while not known.
    std::optional<std::vector<std::set<Bytes>>> lists_for(Bytes const& key, std::uint32_t ledger) const;
    Counted judged(Bytes const& key, Signers const& signers) const;
    std::set<Bytes> all_masters() const;
};
}
