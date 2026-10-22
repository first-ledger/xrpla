#pragma once
#include "peer.hpp"
#include <functional>

namespace xrpla::core {
// Relays validators' validations and proposals among this node's peers, as a non-validating
// xrpld does (docs/PEER-SERVING.md). Each message is checked once: well formed, signed by the
// key it names, and current (a validation signed within the last three minutes, a proposal
// whose close time is within three minutes of now), as xrpld charges a peer that relays one
// that is not. It then goes, as received, to every open session except those that sent it and
// those that asked not to hear its validator (TMSquelch), and those with 16 MiB or more
// waiting to be sent (backlogged_skips in status()). A copy seen again is a duplicate,
// neither checked nor relayed again. As xrpld's reduce-relay does, once a validator's messages
// come from more than `sources` peers, the others are asked to stop sending them for five to
// ten minutes; validators whose messages are not relayed are squelched on every peer.
// Transactions are not relayed: xrpld relays one only after applying it to its open ledger,
// which this node does not keep.
class PeerRelay {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    // For validators off the trusted list (xrpld's [relay_validations] and [relay_proposals]):
    // relay their messages (all), use but not relay them (trusted), or drop them unchecked
    // (drop_untrusted). Trusted validators' messages are always relayed.
    enum class Policy { all, trusted, drop_untrusted };
    static Policy policy(std::string const& name);  // "all", "trusted" or "drop_untrusted"
    static std::string name(Policy policy);
    struct Options {
        bool relay = true;                   // false: check and use messages, relay none
        Policy validations = Policy::all;    // xrpld's defaults
        Policy proposals = Policy::trusted;
        bool squelch = true;                 // ask surplus sources to stop sending
        unsigned sources = 5;                // xrpld's vp_base_squelch_max_selected_peers
    };
    using Visit = std::function<void(PeerSession&)>;
    // `trusted`: whether a signing key is a listed validator's current one. `sessions` calls
    // its argument with every open session, inbound and outbound.
    PeerRelay(Options options, std::function<bool(Bytes const&)> trusted, std::function<void(Visit const&)> sessions);
    ~PeerRelay();
    // A validation (mtVALIDATION), proposal (mtPROPOSE_LEDGER) or squelch (mtSQUELCH) the
    // session `from` sent. Returns a new, checked validation from a trusted validator (the
    // serialized STValidation) for the caller to count.
    std::optional<Bytes> receive(PeerSession& from, int type, std::string const& payload);
    // A session ended: its squelches and its standing as a source are forgotten.
    void ended(std::uint64_t session);
    // Called with a session that sent a message failing its checks and what xrpld charges
    // for it: 2,000 for a signature that does not hold, 200 for one malformed.
    void charges(std::function<void(PeerSession&, double fee)> charge);
    // Relays nothing more; once it returns, no relay is in progress (call it before the
    // sessions it reaches go away).
    void stop();
    Options const& options() const;
    Json status() const;
};
}
