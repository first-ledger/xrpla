// Validated ledgers from the peer network (peer_ledgers.hpp).
#include "peer_ledgers.hpp"
#include <xrpla/core/format.hpp>
#include <xrpl/proto/xrpl.pb.h>
#include <xrpl/basics/IntrusivePointer.ipp>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/STTx.h>
#include <xrpla/core/operations.hpp>
#include <xrpl/shamap/SHAMapInnerNode.h>
#include <xrpl/shamap/SHAMapLeafNode.h>
#include <xrpl/shamap/SHAMapNodeID.h>
#include <xrpl/shamap/SHAMapTreeNode.h>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <optional>
#include <random>
#include <set>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <algorithm>
#include <thread>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
r::uint256 to_uint256(Bytes const& bytes) {
    if (bytes.size() != 32) throw Error("Expected a 32-byte hash");
    return r::uint256::fromVoid(bytes.data());
}
Bytes raw(r::uint256 const& value) { return Bytes(reinterpret_cast<char const*>(value.data()), value.size()); }
using Node = r::SHAMapTreeNodePtr;
using Wanted = std::vector<std::pair<r::SHAMapNodeID, r::uint256>>;  // node IDs and the hashes they must have
// The tree nodes of a reply that check: each asked-for one against its expected hash, and those
// below it (queryDepth) against the child hashes of a checked parent. Copies that are
// unreadable, or that hang from no checked node, are left out; nullopt when an asked-for node
// was sent but no copy of it checks (the peer sent wrong data).
std::optional<std::map<std::string, Node>> checked_nodes(protocol::TMLedgerData const& data, Wanted const& asked) {
    std::multimap<std::string, std::string const*> sent;  // node ID -> each copy's wire data
    for (auto const& node : data.nodes()) if (node.has_nodeid()) sent.emplace(node.nodeid(), &node.nodedata());
    std::set<std::string> roots;
    for (auto const& [id, hash] : asked) roots.insert(id.getRawString());
    std::map<std::string, Node> found;
    Wanted walk(asked.begin(), asked.end());
    while (!walk.empty()) {
        auto [id, hash] = walk.back(); walk.pop_back();
        auto key = id.getRawString();
        if (found.contains(key)) continue;
        auto [from, to] = sent.equal_range(key);
        if (from == to) continue;
        Node node;
        for (auto it = from; it != to && !node; ++it) {
            try {
                if (auto made = r::SHAMapTreeNode::makeFromWire(r::Slice(it->second->data(), it->second->size())); made && made->getHash().asUInt256() == hash)
                    node = std::move(made);
            } catch (std::exception const&) {}
        }
        if (!node) {
            if (roots.contains(key)) return std::nullopt;
            continue;
        }
        if (node->isInner() && id.getDepth() < 64) {
            auto const& inner = static_cast<r::SHAMapInnerNode const&>(*node);
            for (int branch = 0; branch < 16; ++branch)
                if (!inner.isEmptyBranch(branch)) walk.push_back({id.getChildNodeID(branch), inner.getChildHash(branch).asUInt256()});
        }
        found.emplace(std::move(key), std::move(node));
    }
    return found;
}
constexpr std::size_t batch_nodes = 128;
// xrpld charges a TMGetLedger that carries no request cookie 250 (kFeeModerateBurdenPeer)
// against a balance that decays by 1/32 a second; it warns a peer whose balance (the charges
// over 32) reaches 5,000 and drops it at 25,000. Requests to one peer keep that balance at half
// the warning level: bursts of about 320, then 10 a second.
constexpr double request_fee = 250, request_budget = 2500;
// Requests out at once to one peer, and to all; batches of one nodes() call fetched at once.
constexpr unsigned peer_in_flight = 6, total_in_flight = 32, batch_workers = 8;
// A ledger's tree, a level per round or more, is at most 65 levels deep; more rounds than this
// means peers are not supplying it.
constexpr int tree_rounds = 80;
}

struct PeerLedgers::Impl {
    Options options;
    NodeIdentity identity;
    std::function<void(Bytes const&)> on_validation, on_manifest;
    using clock = std::chrono::steady_clock;
    // A request's replies, from any of the peers it went to (a copy goes to another peer when the
    // first is slow): the first good one is used. Once one is (`done`), later ones are dropped as
    // they come rather than kept, each up to 64 MiB, until the request returns.
    struct Replies {
        std::mutex mutex;
        std::condition_variable arrived;
        std::deque<std::pair<std::uint64_t, protocol::TMLedgerData>> received;  // copy, reply
        bool done = false;
        void finish() {
            std::lock_guard lock(mutex);
            done = true;
            received.clear();
        }
    };
    // A copy of a request out to a peer. Requests carry no cookie (xrpld takes one as a request
    // it relays for another of its peers, and charges nothing for it): a reply is the copy's
    // that asked its peer for the same ledger and tree, and for a node it carries.
    struct Waiting {
        std::uint64_t copy;
        std::string ledger;
        int type;
        std::set<std::string> ids;
        std::shared_ptr<Replies> replies;
        bool answers(protocol::TMLedgerData const& data) const {
            if (data.ledgerhash() != ledger || data.type() != type) return false;
            if (ids.empty()) return true;
            for (auto const& node : data.nodes()) if (node.has_nodeid() && ids.contains(node.nodeid())) return true;
            return false;
        }
    };
    // What a peer has charged this node, as xrpld does: it keys the balance by this node's
    // address and keeps it across sessions (Resource::Logic drops an entry five minutes after
    // its last session, by when the balance has decayed to nothing), so it is kept by the
    // peer's endpoint, not the slot. Impl::mutex held.
    struct Charge {
        double charged = 0;
        clock::time_point at{};
        double balance(clock::time_point now) const {
            return charged * std::pow(31.0 / 32.0, std::chrono::duration<double>(now - at).count()) / 32;
        }
    };
    struct Slot {
        std::unique_ptr<PeerSession> session;
        std::thread reader;
        // Requests in a row it failed to answer, or answered with data that does not check:
        // only one sent after the last miss was noticed counts again (a burst out at once is
        // one miss).
        std::atomic<unsigned> misses{0};
        std::atomic<clock::rep> missed_at{0};
        // Impl::mutex held: copies out to it, and its peer's charges (Impl::charges).
        std::vector<Waiting> waiting;
        unsigned in_flight = 0;
        std::shared_ptr<Charge> charge;
        // The ledgers the peer says it holds (its status messages; 0: not said yet). Most keep
        // only recent ones: a ledger hours old is asked of peers that hold it.
        std::atomic<std::uint32_t> first{0}, last{0};
        // Ledgers it said it accepted, with their hashes (the newest 64): it holds them, and
        // `lastseq` trails the newest.
        mutable std::mutex announced_mutex;
        std::map<std::uint32_t, Bytes> announced;
        // How long it takes to answer, smoothed (0: no answer yet).
        std::atomic<double> answer_ms{0};
        bool holds(std::uint32_t sequence) const {
            if (first && first <= sequence && sequence <= last) return true;
            std::lock_guard lock(announced_mutex);
            return announced.contains(sequence);
        }
        bool lacks(std::uint32_t sequence) const { return first && (sequence < first || sequence > last) && !holds(sequence); }
        void answered(std::chrono::steady_clock::duration took) {
            auto ms = std::chrono::duration<double, std::milli>(took).count(), before = answer_ms.load();
            answer_ms = before ? 0.8 * before + 0.2 * ms : ms;
        }
        // A miss (a request sent at `sent`), counted as a 10 s answer; true at the third in a row.
        bool missed(clock::time_point sent) {
            answered(std::chrono::seconds(10));
            auto now = clock::now().time_since_epoch().count();
            // Out with the one that missed before it, or one counted at the same moment: not again.
            auto at = missed_at.load();
            do { if (sent.time_since_epoch().count() < at) return false; } while (!missed_at.compare_exchange_weak(at, now));
            return ++misses >= 3;
        }
    };
    mutable std::mutex mutex;  // slots, candidates, backoff, each slot's waiting, in-flight and charges
    std::condition_variable freed;  // a copy came back (a slot may take another)
    unsigned in_flight = 0;
    std::vector<std::shared_ptr<Slot>> slots;
    // By endpoint; one no slot holds is forgotten five minutes after its last charge.
    std::map<std::string, std::shared_ptr<Charge>> charges;
    std::vector<std::string> bootstrap;  // see Options::bootstrap
    std::deque<std::string> candidates;  // endpoints peers named (not configured, not bootstrap)
    // An endpoint that failed is tried again after a delay doubling from 5 s to 5 minutes.
    struct Backoff { std::chrono::steady_clock::time_point until; std::chrono::seconds delay{0}; };
    std::map<std::string, Backoff> backoff;
    std::chrono::steady_clock::time_point rebalanced = std::chrono::steady_clock::now();
    std::atomic<std::uint64_t> hedged{0};
    std::atomic<std::uint64_t> copies{0};
    std::atomic<std::uint64_t> requests{0}, retries{0}, unanswered{0}, rejected{0};
    std::atomic<bool> stopping{false};
    std::thread keeper;
    Json refused = Json::array();
    // Sessions opened, why sessions ended and why connections failed (mutex held).
    std::uint64_t connects = 0, short_sessions = 0;
    std::map<std::string, std::uint64_t> closed, dial_failures;

    void read(std::shared_ptr<Slot> slot) {
        while (!stopping) {
            std::optional<PeerSession::Message> message;
            try { message = slot->session->receive(std::chrono::milliseconds(500)); } catch (std::exception const&) { return; }
            if (!message) continue;
            if (message->type == protocol::mtLEDGER_DATA) {
                protocol::TMLedgerData data;
                if (!data.ParseFromString(message->payload)) continue;
                std::shared_ptr<Replies> replies;
                std::uint64_t copy = 0;
                {
                    std::lock_guard lock(mutex);
                    auto& out = slot->waiting;
                    if (auto it = std::find_if(out.begin(), out.end(), [&](Waiting const& w) { return w.answers(data); }); it != out.end()) {
                        replies = it->replies; copy = it->copy; out.erase(it);
                    }
                }
                if (!replies) continue;  // answered already by another peer, or given up on
                std::lock_guard lock(replies->mutex);
                if (replies->done) continue;
                replies->received.emplace_back(copy, std::move(data));
                replies->arrived.notify_all();
            } else if (message->type == protocol::mtVALIDATION || message->type == protocol::mtPROPOSE_LEDGER || message->type == protocol::mtSQUELCH) {
                if (options.relay) {
                    if (auto validation = options.relay->receive(*slot->session, message->type, message->payload); validation && on_validation)
                        on_validation(*validation);
                } else if (message->type == protocol::mtVALIDATION) {
                    protocol::TMValidation validation;
                    if (validation.ParseFromString(message->payload) && on_validation) on_validation(validation.validation());
                }
            } else if (message->type == protocol::mtSTATUS_CHANGE) {
                protocol::TMStatusChange status;
                if (!status.ParseFromString(message->payload)) continue;
                if (status.has_firstseq() && status.has_lastseq() && status.firstseq() <= status.lastseq()) {
                    slot->first = status.firstseq(); slot->last = status.lastseq();
                }
                if (status.newevent() == protocol::neACCEPTED_LEDGER && status.has_ledgerseq() && status.ledgerhash().size() == 32) {
                    std::lock_guard lock(slot->announced_mutex);
                    slot->announced[status.ledgerseq()] = status.ledgerhash();
                    while (slot->announced.size() > 64) slot->announced.erase(slot->announced.begin());
                }
            } else if (message->type == protocol::mtMANIFESTS) {
                protocol::TMManifests manifests;
                // A peer's manifests, at most a list's worth per message (the network's validators number in the
                // hundreds); each is checked against the validator list before its signatures are.
                if (manifests.ParseFromString(message->payload) && on_manifest)
                    for (int i = 0; i < std::min(manifests.list_size(), 512); ++i) on_manifest(manifests.list(i).stobject());
            } else if (message->type == protocol::mtENDPOINTS && !options.private_mode) {
                // Peers share the addresses of others: more candidates beyond the configured ones,
                // public addresses only and a few from each message.
                protocol::TMEndpoints endpoints;
                if (!endpoints.ParseFromString(message->payload)) continue;
                std::lock_guard lock(mutex);
                int taken = 0;
                for (auto const& endpoint : endpoints.endpoints_v2())
                    if (endpoint.hops() > 0 && taken < 64 && public_endpoint(endpoint.endpoint())) { remember(canonical_endpoint(endpoint.endpoint())); ++taken; }  // hops 0: the sender itself
            }
        }
    }
    bool configured(std::string const& endpoint) const { return std::find(options.peers.begin(), options.peers.end(), endpoint) != options.peers.end(); }
    bool hub(std::string const& endpoint) const { return std::find(bootstrap.begin(), bootstrap.end(), endpoint) != bootstrap.end(); }
    // An endpoint a peer named, to try (mutex held).
    void remember(std::string const& endpoint) {
        if (options.private_mode || configured(endpoint) || hub(endpoint)) return;
        if (candidates.size() < 200 && std::find(candidates.begin(), candidates.end(), endpoint) == candidates.end()) candidates.push_back(endpoint);
    }
    void failed(std::string const& endpoint, std::chrono::steady_clock::time_point now) {  // mutex held
        auto& b = backoff[endpoint];
        b.delay = std::min(std::max(b.delay * 2, std::chrono::seconds(5)), std::chrono::seconds(300));
        b.until = now + b.delay;
        remember(endpoint);
        if (backoff.size() > 1000) std::erase_if(backoff, [&](auto const& entry) { return entry.second.until < now; });
    }
    // A session that ended, logged and counted (mutex held). The endpoint's backoff resets only
    // after a session that lasted a minute and that the peer ended: one this node closed, or that
    // ended sooner, waits out the next delay, so a peer that drops sessions soon after the
    // handshake is not dialled again and again.
    void ended_session(Slot const& slot, std::chrono::steady_clock::time_point now) {
        auto const& info = slot.session->info();
        auto reason = slot.session->failure(); auto traffic = slot.session->traffic();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - info.connected).count();
        std::cerr << Json{{"event", "peer_closed"}, {"direction", "out"}, {"endpoint", info.endpoint}, {"server", info.server},
                          {"seconds", seconds}, {"reason", reason},
                          {"received", {{"messages", traffic.messages_in}, {"bytes", traffic.bytes_in}}},
                          {"sent", {{"messages", traffic.messages_out}, {"bytes", traffic.bytes_out}}}}.dump() << std::endl;
        count_reason(closed, reason);
        if (seconds < 60) ++short_sessions;
        if (seconds < 60 || reason.starts_with("Closed by this node")) failed(info.endpoint, now);
        else { backoff.erase(info.endpoint); remember(info.endpoint); }
    }
    // The next endpoint for a free session: configured peers, then peers the network named,
    // then the bootstrap endpoints (mutex held).
    std::string choose(std::chrono::steady_clock::time_point now) {
        auto usable = [&](std::string const& endpoint) {
            for (auto const& slot : slots) if (slot->session->info().endpoint == endpoint) return false;
            auto it = backoff.find(endpoint);
            return it == backoff.end() || it->second.until <= now;
        };
        for (auto const& endpoint : options.peers) if (usable(endpoint)) return endpoint;
        for (auto it = candidates.begin(); it != candidates.end(); ++it)
            if (usable(*it)) { auto endpoint = *it; candidates.erase(it); return endpoint; }
        for (auto const& endpoint : bootstrap) if (usable(endpoint)) return endpoint;
        return {};
    }
    // Keeps `connections` sessions open; bootstrap sessions beyond `bootstrap_sessions` give
    // way, one each `rebalance`, while peers the network named are waiting.
    void maintain() {
        std::vector<std::shared_ptr<Slot>> ended;
        std::string endpoint;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard lock(mutex);
            std::size_t open = 0, hubs = 0;
            for (auto it = slots.begin(); it != slots.end();) {
                if ((*it)->session->open()) { ++open; hubs += hub((*it)->session->info().endpoint); ++it; continue; }
                ended_session(**it, now);
                ended.push_back(*it);
                it = slots.erase(it);
            }
            if (open < options.connections) endpoint = choose(now);
            else if (hubs > options.bootstrap_sessions && now - rebalanced >= options.rebalance &&
                     std::any_of(candidates.begin(), candidates.end(), [&](auto const& e) { auto it = backoff.find(e); return it == backoff.end() || it->second.until <= now; })) {
                rebalanced = now;
                for (auto const& slot : slots)
                    if (slot->session->open() && hub(slot->session->info().endpoint)) {
                        std::cerr << Json{{"event", "peer_rebalanced"}, {"endpoint", slot->session->info().endpoint}}.dump() << std::endl;
                        slot->session->close("a bootstrap hub gives way to a peer the network named");
                        break;
                    }
            }
        }
        for (auto& slot : ended) {
            if (slot->reader.joinable()) slot->reader.join();
            if (options.relay) options.relay->ended(slot->session->info().id);
        }
        if (!endpoint.empty()) connect(endpoint, now);
    }
    void connect(std::string const& endpoint, std::chrono::steady_clock::time_point now) {
        try {
            auto slot = std::make_shared<Slot>();
            PeerLocal local{identity, options.network_id, options.crawl && !options.private_mode, options.ledger_replay,
                            options.responder ? options.responder : serving_nothing(), options.closed};
            local.squelch = options.relay && options.relay->options().squelch;
            std::set<int> wanted{protocol::mtLEDGER_DATA, protocol::mtVALIDATION, protocol::mtMANIFESTS, protocol::mtENDPOINTS, protocol::mtSTATUS_CHANGE};
            if (options.relay) { wanted.insert(protocol::mtPROPOSE_LEDGER); wanted.insert(protocol::mtSQUELCH); }
            slot->session = std::make_unique<PeerSession>(endpoint, local, wanted);
            if (options.connected) options.connected(*slot->session);
            slot->reader = std::thread([this, slot] { read(slot); });
            std::cerr << Json{{"event", "peer_connected"}, {"endpoint", endpoint}, {"server", slot->session->info().server}}.dump() << std::endl;
            std::lock_guard lock(mutex);
            std::erase_if(charges, [&](auto const& entry) { return entry.second.use_count() == 1 && now - entry.second->at > std::chrono::minutes(5); });
            auto& charge = charges[endpoint];
            if (!charge) charge = std::make_shared<Charge>();
            slot->charge = charge;
            slots.push_back(slot);
            ++connects;
        } catch (PeerRedirect const& redirect) {
            std::lock_guard lock(mutex);
            // Suggested by whoever answered, so held to the rules gossiped endpoints are.
            for (auto const& peer : redirect.peers) if (public_endpoint(peer)) remember(canonical_endpoint(peer));
            failed(endpoint, now);  // full: perhaps later
            count_reason(dial_failures, "Peer is full");
        } catch (std::exception const& error) {
            std::lock_guard lock(mutex);
            failed(endpoint, now);
            count_reason(dial_failures, without_endpoint(error.what(), endpoint));
            if (refused.size() < 20) refused.push_back({{"endpoint", endpoint}, {"reason", error.what()}});
        }
    }
    // An open session to ask, scored as xrpld scores peers (PeerImp::getScore): random up to
    // 9999, 10000 more for one that holds the ledger, less 30 a millisecond of its answer time
    // (at most 9999, so even a slow peer is asked now and then and the random part still
    // spreads requests), or 8000 while it has not answered yet (so a new peer is tried). Peers
    // in `tried` are taken only when no other can be. One already has `peer_in_flight` copies
    // out, or whose next request would take what it has charged this node past
    // `request_budget`, waits, as all do once `total_in_flight` are out. The slot returned has
    // the copy counted and charged; with `wait` false (a copy to keep a slow one company), only
    // an untried peer is taken, and null when none may take it now.
    std::shared_ptr<Slot> pick(std::uint32_t sequence, std::vector<std::shared_ptr<Slot>> const& tried, bool wait) {
        thread_local std::mt19937 random{std::random_device{}()};
        std::unique_lock lock(mutex);
        std::optional<clock::time_point> give_up;  // 60 s from when no session was open
        while (!stopping) {
            auto now = clock::now();
            std::shared_ptr<Slot> best;
            double best_score = 0;
            bool best_untried = false, open = false;
            for (auto const& slot : slots) {
                if (!slot->session->open()) continue;
                open = true;
                if (slot->in_flight >= peer_in_flight || slot->charge->balance(now) + request_fee / 32 > request_budget) continue;
                bool untried = std::find(tried.begin(), tried.end(), slot) == tried.end();
                if (!wait && !untried) continue;
                auto ms = slot->answer_ms.load();
                double score = std::uniform_int_distribution<int>(0, 9999)(random) + (sequence && slot->holds(sequence) ? 10000 : 0) -
                               (ms ? std::min(30 * ms, 9999.0) : 8000);
                if (!best || (untried && !best_untried) || (untried == best_untried && score > best_score)) {
                    best = slot; best_score = score; best_untried = untried;
                }
            }
            if (best && in_flight < total_in_flight) {
                ++best->in_flight; ++in_flight;
                auto& charge = *best->charge;
                charge.charged = charge.balance(now) * 32 + request_fee;
                charge.at = now;
                return best;
            }
            if (!wait) return nullptr;
            // Only time with no session open counts towards giving up: busy peers answer in time.
            if (open) give_up.reset();
            else if (!give_up) give_up = now + std::chrono::seconds(60);
            else if (now >= *give_up) break;
            freed.wait_for(lock, std::chrono::milliseconds(100));
        }
        throw Error("No peer is connected");
    }
    // A copy is done with: no longer matched to replies, nor counted as out.
    void settle(Slot& slot, std::uint64_t copy) {
        {
            std::lock_guard lock(mutex);
            std::erase_if(slot.waiting, [&](Waiting const& w) { return w.copy == copy; });
            --slot.in_flight; --in_flight;
        }
        freed.notify_all();
    }
    // One TMGetLedger answered without error, with data `accept` takes (data a peer should
    // not have sent counts as its miss, and the request goes to another). Peers send nothing at
    // all when they lack the ledger or are busy, so a request not answered within three times
    // the peer's usual time (at least 500 ms, at most 3 s) also goes to another peer, and the
    // first good answer is used, as xrpld asks more peers when one is slow. At most two copies
    // are out at once, six in all; a peer that has not answered in 10 s has missed (counted as a
    // 10 s answer), and three misses in a row drop it.
    protocol::TMLedgerData request(protocol::TMGetLedger const& message, std::uint32_t sequence,
                                   std::function<bool(protocol::TMLedgerData const&)> const& accept) {
        struct Sent { std::uint64_t copy; std::shared_ptr<Slot> slot; clock::time_point at; };
        auto replies = std::make_shared<Replies>();
        std::set<std::string> ids(message.nodeids().begin(), message.nodeids().end());
        std::vector<Sent> out;  // copies not yet answered
        std::vector<std::shared_ptr<Slot>> tried;
        std::string last;
        unsigned sends = 0;
        auto hedge_after = clock::time_point{};  // when no peer could take a copy, the next try
        auto send = [&](bool wait) {
            std::shared_ptr<Slot> slot;
            try { slot = pick(sequence, tried, wait); } catch (std::exception const&) { if (wait) throw; }  // a copy out is still waited for
            if (!slot) return false;
            auto copy = ++copies;
            { std::lock_guard lock(mutex); slot->waiting.push_back({copy, message.ledgerhash(), message.itype(), ids, replies}); }
            ++requests; ++sends; tried.push_back(slot);
            try { slot->session->send(protocol::mtGET_LEDGER, message); out.push_back({copy, slot, clock::now()}); }
            catch (std::exception const& error) { last = error.what(); settle(*slot, copy); }
            return true;
        };
        auto patience = [](Slot const& slot) {
            auto ms = slot.answer_ms.load();
            return std::chrono::milliseconds(std::clamp<long long>(static_cast<long long>(3 * (ms ? ms : 300.0)), 500, 3000));
        };
        // One asked for a ledger it says it does not hold is not at fault.
        auto strike = [&](Sent const& sent, char const* why) {
            if (!(sequence && sent.slot->lacks(sequence)) && sent.slot->missed(sent.at)) sent.slot->session->close(why);
        };
        auto give_up = [&] { replies->finish(); for (auto const& sent : out) settle(*sent.slot, sent.copy); };
        while (!stopping) {
            if (out.empty()) {
                if (sends >= 6) break;
                if (sends) ++retries;
                send(true);
                continue;
            }
            // Until an answer, the oldest copy's 10 s, or (while another copy may go) the moment
            // the newest one has been out longer than its peer's patience.
            auto due = std::max(out.back().at + patience(*out.back().slot), hedge_after);
            bool may_hedge = out.size() < 2 && sends < 6;
            auto wake = out.front().at + std::chrono::seconds(10);
            if (may_hedge) wake = std::min(wake, due);
            std::optional<std::pair<std::uint64_t, protocol::TMLedgerData>> reply;
            {
                std::unique_lock lock(replies->mutex);
                replies->arrived.wait_until(lock, wake, [&] { return !replies->received.empty(); });
                if (!replies->received.empty()) { reply = std::move(replies->received.front()); replies->received.pop_front(); }
            }
            auto now = clock::now();
            if (reply) {
                auto it = std::find_if(out.begin(), out.end(), [&](Sent const& s) { return s.copy == reply->first; });
                if (it == out.end()) continue;
                auto sent = *it; out.erase(it);
                settle(*sent.slot, sent.copy);
                auto const& endpoint = sent.slot->session->info().endpoint;
                if (reply->second.has_error()) {
                    sent.slot->answered(now - sent.at);
                    last = "peer " + endpoint + " answered error " + std::to_string(reply->second.error());
                    continue;
                }
                bool good = false;
                try { good = accept(reply->second); } catch (std::exception const&) {}
                if (good) { sent.slot->answered(now - sent.at); sent.slot->misses = 0; give_up(); return std::move(reply->second); }
                ++rejected;
                last = "peer " + endpoint + " sent ledger data that does not check";
                strike(sent, "sent ledger data that does not check 3 times in a row");
                continue;
            }
            // Copies out 10 s have missed, as have those whose session ended; a slow one gets company.
            for (auto it = out.begin(); it != out.end();) {
                bool ended = !it->slot->session->open();
                if (!ended && now - it->at < std::chrono::seconds(10)) { ++it; continue; }
                settle(*it->slot, it->copy);
                last = "peer " + it->slot->session->info().endpoint + (ended ? " closed" : " did not answer");
                ++unanswered;
                if (!ended) strike(*it, "did not answer 3 requests in a row");
                it = out.erase(it);
            }
            if (!out.empty() && may_hedge && now >= due) {
                if (send(false)) ++hedged;
                else hedge_after = now + std::chrono::milliseconds(100);
            }
        }
        give_up();
        throw Error("Peers could not supply ledger data: " + last);
    }
    // One batch of node IDs: the nodes of the first reply that brings some that check.
    std::map<std::string, Node> batch(Bytes const& ledger, protocol::TMLedgerInfoType type, Wanted const& wanted, unsigned depth, std::uint32_t sequence) {
        protocol::TMGetLedger message;
        message.set_itype(type);
        message.set_ledgerhash(ledger);
        message.set_querydepth(depth);
        for (auto const& [id, hash] : wanted) message.add_nodeids(id.getRawString());
        std::map<std::string, Node> found;
        request(message, sequence, [&](protocol::TMLedgerData const& data) {
            auto checked = checked_nodes(data, wanted);
            if (!checked || checked->empty()) return false;
            found = std::move(*checked);
            return true;
        });
        return found;
    }
    // Nodes of one of the ledger's trees by node ID, each checked against its expected hash, and
    // the nodes below them replies carry (queryDepth), checked against their parents. Batches go
    // out `batch_workers` at a time; asked-for nodes no reply carried (a reply stops at 8,192
    // nodes) are asked for again while each round brings some.
    std::map<std::string, Node> nodes(Bytes const& ledger, protocol::TMLedgerInfoType type, Wanted const& wanted, unsigned depth, std::uint32_t sequence) {
        std::map<std::string, Node> found;
        Wanted remaining = wanted;
        for (int round = 0; !remaining.empty() && round < tree_rounds; ++round) {
            std::vector<Wanted> batches;
            for (std::size_t start = 0; start < remaining.size(); start += batch_nodes)
                batches.emplace_back(remaining.begin() + static_cast<std::ptrdiff_t>(start),
                                     remaining.begin() + static_cast<std::ptrdiff_t>(std::min(start + batch_nodes, remaining.size())));
            std::vector<std::map<std::string, Node>> got(batches.size());
            std::atomic<std::size_t> next{0};
            std::mutex failed;
            std::exception_ptr error;
            auto work = [&] {
                for (std::size_t i; (i = next++) < batches.size();) {
                    try { got[i] = batch(ledger, type, batches[i], depth, sequence); }
                    catch (...) {
                        std::lock_guard lock(failed);
                        if (!error) error = std::current_exception();
                        next = batches.size();
                    }
                }
            };
            std::vector<std::future<void>> workers;
            for (std::size_t n = 1; n < std::min<std::size_t>(batches.size(), batch_workers); ++n) workers.push_back(std::async(std::launch::async, work));
            work();
            for (auto& worker : workers) worker.get();
            if (error) std::rethrow_exception(error);
            for (auto& part : got) found.merge(part);
            Wanted missing;
            for (auto const& entry : remaining) if (!found.contains(entry.first.getRawString())) missing.push_back(entry);
            if (missing.size() == remaining.size()) break;  // no progress: what peers did not supply stays missing
            remaining = std::move(missing);
        }
        return found;
    }
    // The objects at `keys` (PeerLedgers::objects), starting from nodes already fetched.
    std::vector<Bytes> objects(Header const& header, std::vector<Bytes> const& keys, std::map<std::string, Node> have);
};

PeerLedgers::PeerLedgers(Options options, std::function<void(Bytes const&)> on_validation, std::function<void(Bytes const&)> on_manifest)
    : impl_(std::make_unique<Impl>()) {
    auto& m = *impl_;
    m.options = std::move(options);
    m.identity = m.options.identity.empty() ? NodeIdentity::ephemeral() : NodeIdentity::load_or_create(m.options.identity);
    m.on_validation = std::move(on_validation); m.on_manifest = std::move(on_manifest);
    m.bootstrap = !m.options.bootstrap.empty() ? m.options.bootstrap : m.options.peers.empty() ? default_peers(m.options.network_id) : std::vector<std::string>{};
    if (m.bootstrap.empty() && m.options.peers.empty())
        throw Error("Network " + std::to_string(m.options.network_id) + " has no public hubs known by name: list peers to connect to ([peers] endpoints, --peers)");
    m.keeper = std::thread([&m] {
        while (!m.stopping) {
            m.maintain();
            std::size_t open = 0;
            { std::lock_guard lock(m.mutex); open = m.slots.size(); }
            std::this_thread::sleep_for(std::chrono::milliseconds(open < m.options.connections ? 200 : 2000));
        }
    });
}
PeerLedgers::~PeerLedgers() {
    auto& m = *impl_;
    m.stopping = true;
    if (m.keeper.joinable()) m.keeper.join();
    std::vector<std::shared_ptr<Impl::Slot>> slots;
    { std::lock_guard lock(m.mutex); slots = m.slots; }
    for (auto& slot : slots) if (slot->reader.joinable()) slot->reader.join();
}
Header PeerLedgers::header(Bytes const& hash, std::uint32_t sequence) {
    protocol::TMGetLedger message;
    message.set_itype(protocol::liBASE);
    message.set_ledgerhash(hash);
    // A reply whose header is not this ledger's is its peer's miss: another peer is asked.
    std::optional<Header> found;
    try {
        impl_->request(message, sequence, [&](protocol::TMLedgerData const& data) {
            if (data.nodes_size() == 0) return false;
            auto header = decode_header(data.nodes(0).nodedata());
            if (from_hex(header.hash) != hash) return false;
            found = std::move(header);
            return true;
        });
    } catch (std::exception const& error) { throw Error("Peers sent no header for ledger " + to_hex(hash) + ": " + error.what()); }
    return *found;
}
std::vector<TransactionRecord> PeerLedgers::transactions(Header const& header) {
    auto ledger = from_hex(header.hash);
    auto root = to_uint256(from_hex(header.transaction_hash));
    std::vector<TransactionRecord> result;
    if (root.isZero()) return result;
    // The transaction tree is small: fetch it three levels at a time from each missing node,
    // until a round brings nothing new (or takes more rounds than the tree has levels).
    std::map<std::string, Node> have;
    Wanted missing{{r::SHAMapNodeID(), root}};
    Wanted walk;
    for (int round = 0; !missing.empty(); ++round) {
        auto before = have.size();
        if (round < tree_rounds) have.merge(impl_->nodes(ledger, protocol::liTX_NODE, missing, 3, header.sequence));
        if (have.size() == before) throw Error("Peers did not supply the transaction tree of ledger " + header.hash);
        walk = std::move(missing); missing.clear();
        // Walk down from each requested node, checking children against their parents.
        while (!walk.empty()) {
            auto [id, hash] = walk.back(); walk.pop_back();
            auto it = have.find(id.getRawString());
            if (it == have.end()) { missing.push_back({id, hash}); continue; }
            if (it->second->getHash().asUInt256() != hash) throw Error("Peer sent a transaction tree node that differs from its hash");
            if (it->second->isInner()) {
                auto const& inner = static_cast<r::SHAMapInnerNode const&>(*it->second);
                for (int branch = 0; branch < 16; ++branch)
                    if (!inner.isEmptyBranch(branch)) walk.push_back({id.getChildNodeID(branch), inner.getChildHash(branch).asUInt256()});
            } else {
                if (it->second->getType() != r::SHAMapNodeType::TnTransactionMd) throw Error("Transaction tree leaf has no metadata");
                auto const& leaf = static_cast<r::SHAMapLeafNode const&>(*it->second);
                r::SerialIter items(leaf.peekItem()->slice());
                auto tx = items.getVL(); auto meta = items.getVL();
                result.push_back({Bytes(reinterpret_cast<char const*>(tx.data()), tx.size()), Bytes(reinterpret_cast<char const*>(meta.data()), meta.size())});
            }
        }
    }
    return result;
}
std::vector<Bytes> PeerLedgers::objects(Header const& header, std::vector<Bytes> const& keys) {
    return impl_->objects(header, keys, {});
}
PeerLedgers::Contents PeerLedgers::contents(Header const& header, std::function<std::vector<Bytes>(std::vector<TransactionRecord> const&)> const& keys_of) {
    auto ledger = from_hex(header.hash);
    auto root = to_uint256(from_hex(header.account_hash));
    auto top = std::async(std::launch::async, [&] {
        return impl_->nodes(ledger, protocol::liAS_NODE, {{r::SHAMapNodeID(), root}}, 2, header.sequence);
    });
    Contents result;
    try { result.transactions = transactions(header); } catch (...) { top.wait(); throw; }
    result.keys = keys_of(result.transactions);
    result.values = impl_->objects(header, result.keys, top.get());
    return result;
}
std::vector<Bytes> PeerLedgers::Impl::objects(Header const& header, std::vector<Bytes> const& keys, std::map<std::string, Node> have) {
    auto ledger = from_hex(header.hash);
    std::vector<Bytes> values(keys.size());
    // Each key's path through the state tree, one level per round of requests.
    struct Path { std::size_t index; r::uint256 key; r::SHAMapNodeID id; r::uint256 hash; };
    std::vector<Path> open;
    for (std::size_t i = 0; i < keys.size(); ++i) open.push_back({i, to_uint256(keys[i]), r::SHAMapNodeID(), to_uint256(from_hex(header.account_hash))});
    // `have`: nodes already fetched (the tree's top), each still checked against its parent below.
    while (!open.empty()) {
        std::vector<std::pair<r::SHAMapNodeID, r::uint256>> wanted;
        std::set<std::string> asked;
        for (auto const& path : open)
            if (!have.contains(path.id.getRawString()) && asked.insert(path.id.getRawString()).second) wanted.push_back({path.id, path.hash});
        // The first round asks for the root and the two levels below it at once (nearly all of
        // them change with every ledger); later rounds for each path's next node.
        bool top = wanted.size() == 1 && wanted.front().first.isRoot();
        if (!wanted.empty()) have.merge(nodes(ledger, protocol::liAS_NODE, wanted, top ? 2 : 0, header.sequence));
        std::vector<Path> next;
        for (auto& path : open) {
            auto it = have.find(path.id.getRawString());
            if (it == have.end()) throw Error("Peers did not supply a state tree node of ledger " + header.hash);
            if (it->second->getHash().asUInt256() != path.hash) throw Error("Peer sent a state tree node that differs from its hash");
            if (it->second->isInner()) {
                auto const& inner = static_cast<r::SHAMapInnerNode const&>(*it->second);
                auto branch = r::selectBranch(path.id, path.key);
                if (inner.isEmptyBranch(branch)) continue;  // absent
                next.push_back({path.index, path.key, path.id.getChildNodeID(branch), inner.getChildHash(branch).asUInt256()});
            } else {
                auto const& item = *static_cast<r::SHAMapLeafNode const&>(*it->second).peekItem();
                if (item.key() == path.key) values[path.index] = Bytes(reinterpret_cast<char const*>(item.slice().data()), item.slice().size());
            }
        }
        open = std::move(next);
    }
    return values;
}
std::vector<ObjectRecord> PeerLedgers::changes(Header const& header, LedgerVerifier const& parent) {
    auto ledger = from_hex(header.hash);
    // One tree's node at a position: nothing, a leaf (its object), or an inner node (child hashes).
    struct Side {
        enum Kind { Empty, Leaf, Inner } kind = Empty;
        r::uint256 hash{};
        Bytes key, object;
        std::array<r::uint256, 16> children{};
    };
    auto side_of = [](Node const& node) {
        Side side;
        side.hash = node->getHash().asUInt256();
        if (node->isInner()) {
            side.kind = Side::Inner;
            auto const& inner = static_cast<r::SHAMapInnerNode const&>(*node);
            for (int b = 0; b < 16; ++b) if (!inner.isEmptyBranch(b)) side.children[b] = inner.getChildHash(b).asUInt256();
        } else {
            side.kind = Side::Leaf;
            auto const& item = *static_cast<r::SHAMapLeafNode const&>(*node).peekItem();
            side.key = raw(item.key());
            side.object = Bytes(reinterpret_cast<char const*>(item.slice().data()), item.slice().size());
        }
        return side;
    };
    auto parse = [](Bytes const& wire) {
        auto node = r::SHAMapTreeNode::makeFromWire(r::Slice(wire.data(), wire.size()));
        if (!node) throw Error("Unreadable state tree node");
        return node;
    };
    auto local = [&](r::SHAMapNodeID const& id) {
        auto nodes = parent.state_nodes(id.getRawString(), 0);
        return nodes.empty() ? Side{} : side_of(parse(nodes.front().second));
    };
    // The parent's objects under a position.
    std::map<Bytes, Bytes> theirs, ours;
    auto local_leaves = [&](r::SHAMapNodeID const& id, Side const& at) {
        if (at.kind == Side::Leaf) { ours[at.key] = at.object; return; }
        if (at.kind == Side::Empty) return;
        for (auto const& [position, wire] : parent.state_nodes(id.getRawString(), 64)) {
            auto node = parse(wire);
            if (node->isInner()) continue;
            auto leaf = side_of(node);
            ours[leaf.key] = leaf.object;
        }
    };
    struct Pending { r::SHAMapNodeID id; r::uint256 hash; Side local; };
    auto root = to_uint256(from_hex(header.account_hash));
    auto parent_root = local(r::SHAMapNodeID());
    if (parent_root.hash == root) return {};
    std::vector<Pending> pending{{r::SHAMapNodeID(), root, parent_root}};
    while (!pending.empty()) {
        std::vector<std::pair<r::SHAMapNodeID, r::uint256>> wanted;
        for (auto const& p : pending) wanted.push_back({p.id, p.hash});
        auto fetched = impl_->nodes(ledger, protocol::liAS_NODE, wanted, 0, header.sequence);
        std::vector<Pending> next;
        for (auto const& p : pending) {
            auto it = fetched.find(p.id.getRawString());
            if (it == fetched.end()) throw Error("Peers did not supply a state tree node of ledger " + header.hash);
            auto remote = side_of(it->second);
            if (remote.kind == Side::Leaf) {
                theirs[remote.key] = remote.object;
                local_leaves(p.id, p.local);
                continue;
            }
            for (int b = 0; b < 16; ++b) {
                auto theirs_child = remote.children[b];
                auto child = p.id.getChildNodeID(b);
                // The parent's node at the child position: its own child there, or the single
                // leaf it held at this position when that leaf's key selects this branch.
                Side ours_child;
                if (p.local.kind == Side::Inner) {
                    if (p.local.children[b] == theirs_child) continue;  // the same subtree
                    if (!p.local.children[b].isZero()) ours_child = local(child);
                } else if (p.local.kind == Side::Leaf && r::selectBranch(p.id, to_uint256(p.local.key)) == b) {
                    ours_child = p.local;
                    if (ours_child.hash == theirs_child) continue;
                }
                if (theirs_child.isZero()) local_leaves(child, ours_child);
                else next.push_back({child, theirs_child, ours_child});
            }
        }
        pending = std::move(next);
    }
    std::vector<ObjectRecord> result;
    for (auto const& [key, object] : theirs) {
        auto it = ours.find(key);
        if (it == ours.end() || it->second != object) result.push_back({key, object});
    }
    for (auto const& [key, object] : ours) if (!theirs.contains(key)) result.push_back({key, {}});
    std::sort(result.begin(), result.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
    return result;
}
std::optional<Bytes> PeerLedgers::announced(std::uint32_t sequence) const {
    std::map<Bytes, unsigned> votes;
    {
        std::lock_guard lock(impl_->mutex);
        for (auto const& slot : impl_->slots) {
            if (!slot->session->open()) continue;
            std::lock_guard held(slot->announced_mutex);
            if (auto it = slot->announced.find(sequence); it != slot->announced.end()) ++votes[it->second];
        }
    }
    for (auto const& [hash, count] : votes) if (count >= 2) return hash;
    return std::nullopt;
}
Json PeerLedgers::status() const {
    auto& m = *impl_;
    std::lock_guard lock(m.mutex);
    Json peers = Json::array();
    for (auto const& slot : m.slots)
        peers.push_back({{"endpoint", slot->session->info().endpoint}, {"server", slot->session->info().server}, {"open", slot->session->open()},
                         {"bootstrap", m.hub(slot->session->info().endpoint)}, {"answer_ms", std::round(slot->answer_ms.load())}});
    return {{"identity", m.identity.base58()}, {"peers", peers}, {"candidates", m.candidates.size()}, {"requests", m.requests.load()},
            {"retries", m.retries.load()}, {"hedged", m.hedged.load()}, {"unanswered", m.unanswered.load()}, {"rejected", m.rejected.load()},
            {"connects", m.connects}, {"short_sessions", m.short_sessions},
            {"closed", m.closed}, {"dial_failures", m.dial_failures}, {"refused", m.refused}};
}
void PeerLedgers::broadcast(int type, std::string const& payload) {
    std::vector<std::shared_ptr<Impl::Slot>> slots;
    { std::lock_guard lock(impl_->mutex); slots = impl_->slots; }
    for (auto const& slot : slots) try { slot->session->send_raw(type, payload); } catch (std::exception const&) {}
}
void PeerLedgers::visit(std::function<void(PeerSession&)> const& visit) const {
    std::vector<std::shared_ptr<Impl::Slot>> slots;
    { std::lock_guard lock(impl_->mutex); slots = impl_->slots; }
    for (auto const& slot : slots) if (slot->session->open()) visit(*slot->session);
}
std::vector<PeerSession::Info> PeerLedgers::sessions() const {
    std::lock_guard lock(impl_->mutex);
    std::vector<PeerSession::Info> out;
    for (auto const& slot : impl_->slots) if (slot->session->open()) out.push_back(slot->session->info());
    return out;
}
std::vector<std::string> PeerLedgers::known() const {
    std::lock_guard lock(impl_->mutex);
    std::vector<std::string> out(impl_->candidates.begin(), impl_->candidates.end());
    for (auto const& slot : impl_->slots) out.push_back(slot->session->info().endpoint);
    return out;
}
void PeerLedgers::learn(std::vector<std::string> const& endpoints) {
    std::lock_guard lock(impl_->mutex);
    for (auto const& endpoint : endpoints) impl_->remember(endpoint);
}
NodeIdentity const& PeerLedgers::identity() const { return impl_->identity; }

Json peer_ledger(std::vector<std::string> const& peers, std::string const& hash, std::uint32_t network_id, fs::path const& identity) {
    PeerLedgers::Options options;
    options.peers = peers; options.network_id = network_id; options.connections = peers.empty() ? 10 : 2; options.private_mode = !peers.empty();
    options.identity = identity;
    options.bootstrap = default_peers(network_id);
    PeerLedgers ledgers(options, [](Bytes const&) {}, [](Bytes const&) {});
    auto header = ledgers.header(from_hex(hash));
    auto transactions = ledgers.transactions(header);
    Json listed = Json::array();
    for (auto const& tx : transactions) {
        Json entry{{"bytes", tx.transaction.size()}, {"metadata_bytes", tx.metadata.size()}};
        // TransactionType is the first field of a canonical transaction (UInt16, field 2: 0x12).
        if (tx.transaction.size() >= 3 && static_cast<unsigned char>(tx.transaction[0]) == 0x12)
            entry["type_code"] = (static_cast<unsigned char>(tx.transaction[1]) << 8) | static_cast<unsigned char>(tx.transaction[2]);
        try {
            r::SerialIter it(tx.transaction.data(), tx.transaction.size());
            r::STTx parsed(it);
            entry["type"] = parsed.getTxnType();
            entry["id"] = r::to_string(parsed.getTransactionID());
        } catch (std::exception const& error) { entry["parse_error"] = error.what(); }
        entry["hex"] = to_hex(tx.transaction);
        entry["metadata_hex"] = to_hex(tx.metadata);
        listed.push_back(entry);
    }
    return {{"sequence", header.sequence}, {"hash", header.hash}, {"transaction_hash", header.transaction_hash}, {"header", to_hex(header.raw)},
            {"transactions", transactions.size()}, {"list", listed}};
}
}
