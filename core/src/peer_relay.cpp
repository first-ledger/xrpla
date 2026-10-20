// Relaying validations and proposals among peers (peer_relay.hpp).
#include "peer_relay.hpp"
#include "validators.hpp"
#include <xrpl/json/json_writer.h>  // before the protocol headers (their journal streams print JSON)
#include <xrpl/proto/xrpl.pb.h>
#include <xrpl/protocol/HashPrefix.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/STValidation.h>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/digest.h>
#include <algorithm>
#include <atomic>
#include <deque>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
using Clock = std::chrono::steady_clock;
// xrpld's isCurrent takes a validation signed from 3 minutes before now to 5 after; these
// stay a little inside that, so a peer whose clock differs slightly still takes it.
constexpr std::int64_t signed_before = 170, signed_after = 290;
// A proposal whose close time is further than this from now is not relayed.
constexpr std::int64_t proposal_window = 180;
// At most this many messages are remembered (the network sends a few thousand in five
// minutes); past it the oldest are forgotten first, so a flood costs no more memory.
constexpr std::size_t seen_limit = 65536;
// Validators followed as sources (or remembered as relayed) at most, the last `trusted_room`
// places only for trusted ones: under Policy::all anyone's keys are taken, and a key costs
// nothing to make, so untrusted ones must not keep the network's validators out.
constexpr std::size_t validators_limit = 4096, trusted_room = 1024;
bool validator_room(std::size_t followed, bool is_trusted) { return followed < validators_limit - (is_trusted ? 0 : trusted_room); }
// How long a source that sent nothing stays one.
constexpr auto source_idle = std::chrono::minutes(5);
// What xrpld charges a peer that relays a message whose signature fails or cannot be one
// (kFeeInvalidSignature), or one too short to be a validation (kFeeMalformedRequest).
constexpr double invalid_signature_fee = 2000, malformed_fee = 200;
Bytes raw(r::uint256 const& value) { return Bytes(reinterpret_cast<char const*>(value.data()), value.size()); }
}

PeerRelay::Policy PeerRelay::policy(std::string const& name) {
    if (name == "all") return Policy::all;
    if (name == "trusted") return Policy::trusted;
    if (name == "drop_untrusted") return Policy::drop_untrusted;
    throw Error("A relay policy is all, trusted or drop_untrusted, not " + name);
}
std::string PeerRelay::name(Policy policy) {
    return policy == Policy::all ? "all" : policy == Policy::trusted ? "trusted" : "drop_untrusted";
}

struct PeerRelay::Impl {
    Options options;
    std::function<bool(Bytes const&)> trusted;
    std::function<void(Visit const&)> sessions;
    std::shared_mutex sending;  // held shared while relaying, exclusively by stop()
    bool stopped = false;
    mutable std::mutex mutex;   // everything below
    // Messages seen in the last five minutes, by xrpld's suppression key: whose each is and the
    // sessions that sent it (it is never relayed back to them). `passed`: it was signed by the
    // key it names and current (only then are `validator` and `trusted` set).
    struct Seen {
        Clock::time_point at;
        Bytes validator;
        bool passed = false, trusted = false;
        std::vector<std::uint64_t> from;
    };
    std::map<Bytes, Seen> seen;
    std::deque<std::pair<Clock::time_point, Bytes>> order;
    // Squelches peers asked of this node: validator key -> session -> until when.
    std::map<Bytes, std::map<std::uint64_t, Clock::time_point>> squelched;
    // Per validator: the sessions kept as its sources (with when each last sent its messages),
    // and those asked to stop, until when.
    struct Sources { std::map<std::uint64_t, Clock::time_point> kept, stopped; };
    std::map<Bytes, Sources> sources;
    // Validators whose messages this node relayed, with when it last did (at most
    // validators_limit, forgotten after source_idle): kept whether or not the squelch option
    // fills `sources`, so peers' squelches for them are honoured either way.
    std::map<Bytes, Clock::time_point> relayed;
    std::function<void(PeerSession&, double)> charge;  // see PeerRelay::charges
    Clock::time_point pruned = Clock::now();
    std::mt19937 random{std::random_device{}()};
    struct Counts {
        std::atomic<std::uint64_t> received{0}, duplicate{0}, invalid{0}, stale{0}, untrusted{0}, relayed{0}, sent{0}, bytes{0};
        Json json() const {
            return {{"received", received.load()}, {"duplicate", duplicate.load()}, {"invalid", invalid.load()}, {"stale", stale.load()},
                    {"untrusted_dropped", untrusted.load()}, {"relayed", relayed.load()}, {"messages_sent", sent.load()}, {"bytes_sent", bytes.load()}};
        }
    };
    Counts validations, proposals;
    std::atomic<std::uint64_t> squelches_sent{0}, squelches_received{0}, squelched_skips{0};

    // What was seen over five minutes ago is forgotten; squelches past their time every minute.
    void expire(Clock::time_point now) {  // mutex held
        while (!order.empty() && now - order.front().first > std::chrono::minutes(5)) forget_oldest();
        if (now - pruned < std::chrono::minutes(1)) return;
        pruned = now;
        for (auto it = squelched.begin(); it != squelched.end();) {
            std::erase_if(it->second, [&](auto const& entry) { return entry.second <= now; });
            it = it->second.empty() ? squelched.erase(it) : std::next(it);
        }
        for (auto it = sources.begin(); it != sources.end();) {
            std::erase_if(it->second.stopped, [&](auto const& entry) { return entry.second <= now; });
            std::erase_if(it->second.kept, [&](auto const& entry) { return now - entry.second > source_idle; });
            it = it->second.kept.empty() && it->second.stopped.empty() ? sources.erase(it) : std::next(it);
        }
        std::erase_if(relayed, [&](auto const& entry) { return now - entry.second > source_idle; });
    }
    // The oldest message remembered, forgotten (mutex held).
    void forget_oldest() {
        auto it = seen.find(order.front().second);
        if (it != seen.end() && it->second.at == order.front().first) seen.erase(it);
        order.pop_front();
    }
    // A sender charged for what it sent, as xrpld charges it (outside the mutex).
    void penalize(PeerSession& from, double fee) {
        std::function<void(PeerSession&, double)> to;
        { std::lock_guard lock(mutex); to = charge; }
        if (to) to(from, fee);
    }
    // One more session sending a validator's messages. Validators whose messages this node
    // relays (or counts) keep `sources` senders; any other sender is asked to stop, if it
    // negotiated squelch messages (vprr) with this node (to one that did not, xrpld would not
    // send them either). Only messages that passed their checks count, as xrpld's reduce-relay
    // counts only those it relayed (a forged or stale one costs nothing to send in another's
    // name): a source that sends none for five minutes is one no longer.
    void source(PeerSession& from, Bytes const& validator, bool is_trusted, Clock::time_point now) {  // mutex held
        if (!options.squelch || validator.empty()) return;
        auto id = from.info().id;
        if (!sources.contains(validator) && !validator_room(sources.size(), is_trusted)) return;
        auto& s = sources[validator];
        if (auto it = s.kept.find(id); it != s.kept.end()) { it->second = now; return; }
        if (auto it = s.stopped.find(id); it != s.stopped.end()) {
            if (it->second > now) return;
            s.stopped.erase(it);
        }
        bool wanted = is_trusted || (options.relay && options.validations == Policy::all);
        if (wanted && s.kept.size() < options.sources) { s.kept[id] = now; return; }
        if (!from.info().squelch) { if (s.kept.empty() && s.stopped.empty()) sources.erase(validator); return; }
        auto seconds = std::uniform_int_distribution<unsigned>(300, 600)(random);
        s.stopped[id] = now + std::chrono::seconds(seconds);
        protocol::TMSquelch squelch;
        squelch.set_squelch(true);
        squelch.set_validatorpubkey(validator);
        squelch.set_squelchduration(seconds);
        try { from.send(protocol::mtSQUELCH, squelch); ++squelches_sent; } catch (std::exception const&) {}
    }
    // A message's first sight is remembered with its sender (true: the caller checks it); a
    // later one adds its sender, which counts as one of the validator's sources once (and only
    // if) the message passed its checks. Only messages that parse get here.
    bool sighting(PeerSession& from, Bytes const& key, Clock::time_point now) {
        std::lock_guard lock(mutex);
        expire(now);
        auto [it, added] = seen.try_emplace(key);
        auto& entry = it->second;
        if (added) {
            entry.at = now; order.emplace_back(now, key);
            while (seen.size() > seen_limit && order.front().second != key) forget_oldest();
        }
        auto id = from.info().id;
        if (std::find(entry.from.begin(), entry.from.end(), id) != entry.from.end()) return added;
        entry.from.push_back(id);
        if (entry.passed) source(from, entry.validator, entry.trusted, now);
        return added;
    }
    // A message signed by the key it names and current: its sender is one of the validator's
    // sources, then each session whose copy came while it was checked, as is each later sender
    // of a copy (xrpld counts every peer it has the message from once it passed). Those
    // sessions are found outside the mutex, as relay() finds them.
    void passed(PeerSession& from, Bytes const& key, Bytes const& validator, bool is_trusted, Clock::time_point now) {
        std::vector<std::uint64_t> others;
        {
            std::lock_guard lock(mutex);
            if (auto it = seen.find(key); it != seen.end()) {
                it->second.validator = validator; it->second.trusted = is_trusted; it->second.passed = true;
                others = it->second.from;
                std::erase(others, from.info().id);
            }
            source(from, validator, is_trusted, now);
        }
        if (others.empty() || !options.squelch) return;
        std::shared_lock hold(sending);
        if (stopped || !sessions) return;
        sessions([&](PeerSession& session) {
            if (std::find(others.begin(), others.end(), session.info().id) == others.end()) return;
            std::lock_guard lock(mutex);
            source(session, validator, is_trusted, now);
        });
    }
    // To every open session but those that sent it and those that squelched its validator.
    void relay(int type, std::string const& payload, Bytes const& key, Bytes const& validator, bool is_trusted, Counts& counts) {
        std::vector<std::uint64_t> skip;
        std::map<std::uint64_t, Clock::time_point> squelchers;
        auto now = Clock::now();
        {
            std::lock_guard lock(mutex);
            if (auto it = seen.find(key); it != seen.end()) skip = it->second.from;
            if (auto it = squelched.find(validator); it != squelched.end()) squelchers = it->second;
            if (relayed.contains(validator) || validator_room(relayed.size(), is_trusted)) relayed[validator] = now;
        }
        std::shared_lock hold(sending);
        if (stopped || !sessions) return;
        ++counts.relayed;
        sessions([&](PeerSession& session) {
            auto id = session.info().id;
            if (std::find(skip.begin(), skip.end(), id) != skip.end()) return;
            if (auto it = squelchers.find(id); it != squelchers.end() && it->second > now) { ++squelched_skips; return; }
            try { session.send_raw(type, payload); ++counts.sent; counts.bytes += payload.size() + 6; } catch (std::exception const&) {}
        });
    }

    std::optional<Bytes> validation(PeerSession& from, std::string const& payload) {
        auto now = Clock::now();
        ++validations.received;
        protocol::TMValidation message;
        if (!message.ParseFromString(payload) || message.validation().size() < 50) {
            ++validations.invalid; penalize(from, malformed_fee); return std::nullopt;
        }
        auto const& bytes = message.validation();
        // Parsed before it is remembered (what does not parse is not), its signature checked
        // only on its first sight, as xrpld does.
        std::shared_ptr<r::STValidation> parsed;
        try {
            r::SerialIter it(bytes.data(), bytes.size());
            parsed = std::make_shared<r::STValidation>(it, [](r::PublicKey const& signer) { return r::calcNodeID(signer); },
                                                       r::STValidation::DeserializeOptions{false, true});
        } catch (std::exception const&) { ++validations.invalid; penalize(from, malformed_fee); return std::nullopt; }
        auto key = raw(r::sha512Half(r::makeSlice(bytes)));
        if (!sighting(from, key, now)) { ++validations.duplicate; return std::nullopt; }
        bool valid = false;
        try { valid = parsed->isValid(); } catch (std::exception const&) {}
        if (!valid) { ++validations.invalid; penalize(from, invalid_signature_fee); return std::nullopt; }
        auto const& signer_key = parsed->getSignerPublic();
        Bytes signer(reinterpret_cast<char const*>(signer_key.data()), signer_key.size());
        bool is_trusted = trusted && trusted(signer);
        auto signed_at = static_cast<std::int64_t>(parsed->getSignTime().time_since_epoch().count());
        auto network_now = static_cast<std::int64_t>(xrpl_seconds_now());
        if (signed_at <= network_now - signed_before || signed_at >= network_now + signed_after) { ++validations.stale; return std::nullopt; }
        passed(from, key, signer, is_trusted, now);
        if (!is_trusted && options.validations == Policy::drop_untrusted) { ++validations.untrusted; return std::nullopt; }
        if (options.relay && (is_trusted || options.validations == Policy::all)) relay(protocol::mtVALIDATION, payload, key, signer, is_trusted, validations);
        if (!is_trusted) return std::nullopt;
        return Bytes(bytes);
    }

    void proposal(PeerSession& from, std::string const& payload) {
        auto now = Clock::now();
        ++proposals.received;
        protocol::TMProposeSet set;
        if (!set.ParseFromString(payload)) { ++proposals.invalid; penalize(from, malformed_fee); return; }
        auto const& signature = set.signature();
        auto const& node_key = set.nodepubkey();
        auto key_type = r::publicKeyType(r::makeSlice(node_key));
        // xrpld charges a signature that cannot be one, or a key that is not secp256k1, as a
        // failed signature; only bad hashes as a malformed request (PeerImp::onMessage).
        if (signature.size() < 64 || signature.size() > 72 || !key_type || *key_type != r::KeyType::Secp256k1) {
            ++proposals.invalid; penalize(from, invalid_signature_fee); return;
        }
        if (set.currenttxhash().size() != 32 || set.previousledger().size() != 32) { ++proposals.invalid; penalize(from, malformed_fee); return; }
        auto position = r::uint256::fromVoid(set.currenttxhash().data());
        auto previous = r::uint256::fromVoid(set.previousledger().data());
        // xrpld's proposalUniqueId.
        r::Serializer unique(512);
        unique.addBitString(position);
        unique.addBitString(previous);
        unique.add32(set.proposeseq());
        unique.add32(set.closetime());
        unique.addVL(r::makeSlice(node_key));
        unique.addVL(r::makeSlice(signature));
        auto key = raw(unique.getSHA512Half());
        if (!sighting(from, key, now)) { ++proposals.duplicate; return; }
        Bytes validator(node_key);
        bool is_trusted = trusted && trusted(validator);
        if (!is_trusted && options.proposals == Policy::drop_untrusted) { ++proposals.untrusted; return; }
        // This node takes no part in consensus: a proposal it will not relay needs no checking,
        // and, unchecked, makes no source (its validator's validations still do).
        if (!options.relay || (!is_trusted && options.proposals != Policy::all)) return;
        auto network_now = static_cast<std::int64_t>(xrpl_seconds_now());
        auto close = static_cast<std::int64_t>(set.closetime());
        if (close < network_now - proposal_window || close > network_now + proposal_window) { ++proposals.stale; return; }
        auto digest = r::sha512Half(r::HashPrefix::Proposal, std::uint32_t(set.proposeseq()), std::uint32_t(set.closetime()), previous, position);
        if (!r::verifyDigest(r::PublicKey(r::makeSlice(node_key)), digest, r::makeSlice(signature), false)) {
            ++proposals.invalid; penalize(from, invalid_signature_fee); return;
        }
        passed(from, key, validator, is_trusted, now);
        relay(protocol::mtPROPOSE_LEDGER, payload, key, validator, is_trusted, proposals);
    }

    // A peer asking this node not to relay a validator's messages to it (from 5 minutes to an
    // hour, as xrpld takes it; any other duration, or squelch false, lifts it).
    void squelch_request(PeerSession& from, std::string const& payload) {
        protocol::TMSquelch message;
        if (!message.ParseFromString(payload) || !message.has_validatorpubkey()) return;
        Bytes const& validator = message.validatorpubkey();
        if (!r::publicKeyType(r::makeSlice(validator))) return;
        ++squelches_received;
        auto id = from.info().id;
        auto seconds = message.has_squelchduration() ? message.squelchduration() : 0u;
        bool is_trusted = trusted && trusted(validator);  // before the relay's lock, as receive() asks
        std::lock_guard lock(mutex);
        if (!message.squelch() || seconds < 300 || seconds > 3600) {
            if (auto it = squelched.find(validator); it != squelched.end()) it->second.erase(id);
            return;
        }
        // Only for a validator whose messages this node relays (one it relayed or kept sources
        // for, at most 4096 of each, or a trusted one): any other key would be an entry kept for
        // an hour, and a peer could send them without end.
        if (!is_trusted && !sources.contains(validator) && !relayed.contains(validator)) return;
        squelched[validator][id] = Clock::now() + std::chrono::seconds(seconds);
    }
};

PeerRelay::PeerRelay(Options options, std::function<bool(Bytes const&)> trusted, std::function<void(Visit const&)> sessions)
    : impl_(std::make_unique<Impl>()) {
    impl_->options = options;
    impl_->trusted = std::move(trusted);
    impl_->sessions = std::move(sessions);
}
PeerRelay::~PeerRelay() = default;
std::optional<Bytes> PeerRelay::receive(PeerSession& from, int type, std::string const& payload) {
    switch (type) {
        case protocol::mtVALIDATION: return impl_->validation(from, payload);
        case protocol::mtPROPOSE_LEDGER: impl_->proposal(from, payload); return std::nullopt;
        case protocol::mtSQUELCH: impl_->squelch_request(from, payload); return std::nullopt;
        default: return std::nullopt;
    }
}
void PeerRelay::ended(std::uint64_t session) {
    auto& m = *impl_;
    std::lock_guard lock(m.mutex);
    for (auto& [validator, s] : m.sources) { s.kept.erase(session); s.stopped.erase(session); }
    for (auto& [validator, sessions] : m.squelched) sessions.erase(session);
}
void PeerRelay::charges(std::function<void(PeerSession&, double)> charge) {
    std::lock_guard lock(impl_->mutex);
    impl_->charge = std::move(charge);
}
void PeerRelay::stop() {
    std::unique_lock hold(impl_->sending);
    impl_->stopped = true;
}
PeerRelay::Options const& PeerRelay::options() const { return impl_->options; }
Json PeerRelay::status() const {
    auto& m = *impl_;
    std::size_t validators = 0;
    { std::lock_guard lock(m.mutex); validators = m.sources.size(); }
    return {{"relay", m.options.relay}, {"relay_validations", name(m.options.validations)}, {"relay_proposals", name(m.options.proposals)},
            {"squelch", m.options.squelch}, {"sources", m.options.sources}, {"validators", validators},
            {"validations", m.validations.json()}, {"proposals", m.proposals.json()},
            {"squelches_sent", m.squelches_sent.load()}, {"squelches_received", m.squelches_received.load()}, {"squelched_skips", m.squelched_skips.load()}};
}
}
