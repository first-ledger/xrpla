// Serving ledger data on the XRPL peer network (peer_serving.hpp, docs/PEER-SERVING.md).
#include "peer_serving.hpp"
#include "peer_ledgers.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/basics/TaggedCache.ipp>
#include <xrpl/basics/chrono.h>
#include <xrpl/nodestore/Database.h>
#include <xrpl/proto/xrpl.pb.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/tokens.h>
#include <xrpl/shamap/Family.h>
#include <xrpl/shamap/SHAMap.h>
#include <xrpl/shamap/SHAMapNodeID.h>
#include <xrpl/shamap/SHAMapTreeNode.h>
#include <sodium.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
Bytes bytes(r::uint256 const& value) { return {reinterpret_cast<char const*>(value.data()), value.size()}; }
r::uint256 key256(Bytes const& data) {
    if (data.size() != 32) throw Error("Expected a 32-byte hash");
    return r::uint256::fromVoid(data.data());
}
// Seconds since 2000-01-01, as the ledger and peer protocol count time.
std::uint64_t network_seconds() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()) - 946684800;
}
std::string base64(Bytes const& data) {
    std::string out(sodium_base64_encoded_len(data.size(), sodium_base64_VARIANT_ORIGINAL), '\0');
    sodium_bin2base64(out.data(), out.size(), reinterpret_cast<unsigned char const*>(data.data()), data.size(), sodium_base64_VARIANT_ORIGINAL);
    out.resize(std::strlen(out.c_str()));
    return out;
}
std::string upper(std::string text) { for (auto& c : text) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c))); return text; }
// The data one reply carries at most. xrpld bounds its replies by node count alone, under its
// 64 MiB message limit (kMaximumMessageSize); a reply that stops early is one it sends too (past
// kSoftMaxReplyNodes), and the asking side asks again for what it still lacks. A reply this
// size is copied a few times on its way out and waits in the peer's send queue, so it is kept
// well under that queue's limits (PeerSession).
constexpr std::size_t reply_budget = 8u << 20;
// Requests waiting or running hold what peers sent until answered: at most this much from one
// peer (an xrpld asks for a few hundred nodes at a time, a few KB), and from all.
constexpr std::size_t peer_request_bytes = 4u << 20, request_bytes = 32u << 20;
// Reads the trees of a node store the follower's verifier writes, through caches of its own.
class ServingFamily final : public r::Family {
    beast::Journal journal_{beast::Journal::getNullSink()};
    std::shared_ptr<r::FullBelowCache> below_ = std::make_shared<r::FullBelowCache>("xrpla-serving", r::stopwatch(), journal_);
    std::shared_ptr<r::TreeNodeCache> tree_ = std::make_shared<r::TreeNodeCache>("xrpla-serving", 65536, std::chrono::minutes{1}, r::stopwatch(), journal_);
    std::shared_ptr<r::node_store::Database> database_;
public:
    explicit ServingFamily(std::shared_ptr<r::node_store::Database> database) : database_(std::move(database)) {}
    r::node_store::Database& db() override { return *database_; }
    r::node_store::Database const& db() const override { return *database_; }
    beast::Journal const& journal() override { return journal_; }
    std::shared_ptr<r::FullBelowCache> getFullBelowCache() override { return below_; }
    std::shared_ptr<r::TreeNodeCache> getTreeNodeCache() override { return tree_; }
    void sweep() override { below_->sweep(); tree_->sweep(); }
    void reset() override { below_->reset(); tree_->reset(); }
    // A node missing from the store belongs to a ledger this node no longer serves.
    void missingNodeAcquireBySeq(std::uint32_t, r::uint256 const&) override {}
    void missingNodeAcquireByHash(r::uint256 const&, std::uint32_t) override {}
};
// A tree read for one request; it holds its family (and so the store) while in use.
struct Tree {
    std::shared_ptr<ServingFamily> family;
    std::shared_ptr<r::SHAMap> map;
    explicit operator bool() const { return bool(map); }
};
}

struct LedgerServer::Impl {
    std::size_t window;
    mutable std::shared_mutex mutex;  // ledgers and the store
    std::map<std::uint32_t, Header> ledgers;
    std::map<Bytes, std::uint32_t> hashes;  // raw ledger hash -> sequence
    std::shared_ptr<r::node_store::Database> store;
    std::shared_ptr<ServingFamily> family;
    // libxrpl's node caches evict only when swept (xrpld sweeps them on a timer): without it,
    // every node served stays cached. Swept every 30 s by whichever thread finds it due (a
    // worker, waking for it while idle, or add()).
    std::atomic<std::chrono::steady_clock::rep> swept{std::chrono::steady_clock::now().time_since_epoch().count()};
    // Held by each request in progress, and exclusively while the store changes: once
    // node_store() returns, nothing here holds the old store (the follower may reopen it).
    std::shared_mutex serving;
    // Requests wait here for a worker; too many waiting and the newest go unanswered, as xrpld
    // leaves requests unanswered when it is loaded. One peer (address_group) has at most
    // `peer_jobs` waiting or running, and at most one fetch pack, of which `packs_limit` run
    // at once (each takes a worker up to a second), so no peer keeps the workers to itself.
    struct Job { std::function<void()> run; std::string peer; bool pack = false; std::size_t bytes = 0; };
    struct Load { unsigned jobs = 0, packs = 0; std::size_t bytes = 0; };
    static constexpr unsigned peer_jobs = 64;
    std::mutex queue_mutex;
    std::condition_variable wake;
    std::deque<Job> jobs;
    std::map<std::string, Load> load;
    std::size_t queued_bytes = 0;  // the requests of `jobs` and those running, as sent
    unsigned packs = 0, packs_limit = 1;
    bool stopping = false;
    std::vector<std::thread> workers;
    // Each peer's charges by remote address, decaying by 1/32 a second (xrpld's resource
    // balance, kept by IP): a peer dropped keeps its balance, raised by xrpld's 6,000 for the
    // drop, and its address is refused new sessions until the balance falls below 25,000. An
    // IPv6 address is charged by its /64 (address_group), so a peer dropped cannot come back
    // from the next address in it.
    struct Balance { double value = 0; std::chrono::steady_clock::time_point at{}; std::uint64_t dropped = 0; };
    std::mutex charge_mutex;
    std::map<std::string, Balance> balances;
    std::chrono::steady_clock::time_point balances_pruned{};  // past 4096 balances, at most once a second
    std::set<std::string> reserved;  // never charged
    static std::string address_of(PeerRequester const& from) { return from.address.empty() ? from.endpoint : address_group(from.address); }
    static double decayed(Balance const& balance, std::chrono::steady_clock::time_point now) {
        if (balance.at == std::chrono::steady_clock::time_point{}) return balance.value;
        return balance.value * std::pow(31.0 / 32.0, std::chrono::duration<double>(now - balance.at).count());
    }
    std::atomic<std::uint64_t> requests{0}, answered{0}, unanswered{0}, nodes_sent{0}, bytes_sent{0}, dropped{0}, busy{0};
    // The part of those from reserved peers (the operator's own nodes), told apart in status().
    std::atomic<std::uint64_t> reserved_requests{0}, reserved_answered{0}, reserved_unanswered{0}, reserved_nodes{0}, reserved_bytes{0};
    // Unanswered requests by kind (as status() names them), all peers' and reserved peers'.
    std::mutex kinds_mutex;
    std::map<std::string, std::uint64_t> unanswered_kinds, reserved_unanswered_kinds;
    // Ledgers peers other than reserved ones asked for by hash while not served, each with when
    // it was first asked for: to count how many this node served soon after (the network's
    // newest, which it has a few seconds after the network does) and how many it did not serve
    // within five minutes.
    std::mutex asked_mutex;
    std::map<Bytes, std::chrono::steady_clock::time_point> asked;
    std::deque<std::pair<std::chrono::steady_clock::time_point, Bytes>> asked_order;
    std::uint64_t asked_total = 0, not_served = 0;
    std::array<std::uint64_t, 4> served_after{};  // within 5 s, 15 s, 60 s, later

    std::optional<Header> by_hash(std::string const& hash) const {
        std::shared_lock lock(mutex);
        auto it = hashes.find(hash);
        if (it == hashes.end()) return std::nullopt;
        return ledgers.at(it->second);
    }
    std::optional<Header> by_seq(std::uint32_t seq) const {
        std::shared_lock lock(mutex);
        auto it = ledgers.find(seq);
        if (it == ledgers.end()) return std::nullopt;
        return it->second;
    }
    std::optional<Header> latest() const {
        std::shared_lock lock(mutex);
        if (ledgers.empty()) return std::nullopt;
        return ledgers.rbegin()->second;
    }
    Tree tree(r::SHAMapType type, std::string const& root_hex, std::uint32_t seq) const {
        std::shared_ptr<ServingFamily> reader;
        { std::shared_lock lock(mutex); reader = family; }
        if (!reader) return {};
        auto root = key256(from_hex(root_hex));
        if (root.isZero()) return {};
        auto map = std::make_shared<r::SHAMap>(type, root, *reader);
        map->setLedgerSeq(seq);
        try { if (!map->fetchRoot(r::SHAMapHash{root}, nullptr)) return {}; }
        catch (std::exception const&) { return {}; }
        map->clearSynching();
        map->setImmutable();
        return {std::move(reader), std::move(map)};
    }
    // True when the charge drops the peer (its session ends; what else it asked is not answered).
    bool charge(PeerRequester const& from, double fee) {
        auto now = std::chrono::steady_clock::now();
        bool drop = false, first = false;
        {
            std::lock_guard lock(charge_mutex);
            if (reserved.contains(from.public_key)) return false;
            auto& balance = balances[address_of(from)];
            balance.value = decayed(balance, now) + fee;
            balance.at = now;
            if (balance.value / 32 >= 25000) {
                drop = true;
                // Once a session: what it asked before the drop took effect is charged, not logged again.
                if (balance.dropped != from.session || !from.session) { first = true; balance.dropped = from.session; balance.value += 6000; }
            }
            if (balances.size() > 4096 && now - balances_pruned >= std::chrono::seconds(1)) {
                balances_pruned = now;
                for (auto it = balances.begin(); it != balances.end();) it = decayed(it->second, now) < 1 ? balances.erase(it) : std::next(it);
            }
        }
        if (first) {
            ++dropped;
            std::cerr << Json{{"event", "peer_dropped"}, {"endpoint", from.endpoint}, {"public_key", from.public_key}, {"reason", "charge: Resources"}}.dump() << std::endl;
        }
        if (drop && from.drop) from.drop("charge: Resources");
        return drop;
    }
    bool admits(std::string const& address) {
        std::lock_guard lock(charge_mutex);
        auto it = balances.find(address_group(address));
        return it == balances.end() || decayed(it->second, std::chrono::steady_clock::now()) / 32 < 25000;
    }
    bool is_reserved(PeerRequester const& from) {
        std::lock_guard lock(charge_mutex);
        return reserved.contains(from.public_key);
    }
    void send(PeerRequester const& from, int type, google::protobuf::MessageLite const& message, std::size_t nodes) {
        auto payload = message.SerializeAsString();
        ++answered; nodes_sent += nodes; bytes_sent += payload.size();
        if (is_reserved(from)) { ++reserved_answered; reserved_nodes += nodes; reserved_bytes += payload.size(); }
        from.reply(type, payload);
    }
    // A request left without a reply, as xrpld leaves what it lacks, counted by kind (the total
    // last, so that a status counting it counts the rest).
    void decline(PeerRequester const& from, std::string const& kind) {
        bool own = is_reserved(from);
        {
            std::lock_guard lock(kinds_mutex);
            ++unanswered_kinds[kind];
            if (own) ++reserved_unanswered_kinds[kind];
        }
        if (own) ++reserved_unanswered;
        ++unanswered;
    }
    void asked_for(Bytes const& hash) {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard lock(asked_mutex);
        expire_asked(now);
        if (asked.size() >= 4096) return;
        if (asked.emplace(hash, now).second) { ++asked_total; asked_order.emplace_back(now, hash); }
    }
    void served_now(Bytes const& hash) {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard lock(asked_mutex);
        if (auto it = asked.find(hash); it != asked.end()) {
            auto seconds = std::chrono::duration<double>(now - it->second).count();
            ++served_after[seconds < 5 ? 0 : seconds < 15 ? 1 : seconds < 60 ? 2 : 3];
            asked.erase(it);
        }
        expire_asked(now);
    }
    // Those asked for over five minutes ago and still not served (asked_mutex held).
    void expire_asked(std::chrono::steady_clock::time_point now) {
        while (!asked_order.empty() && now - asked_order.front().first > std::chrono::minutes(5)) {
            auto const& [at, hash] = asked_order.front();
            if (auto it = asked.find(hash); it != asked.end() && it->second == at) { ++not_served; asked.erase(it); }
            asked_order.pop_front();
        }
    }
    // The kind of a request for a ledger not served: newer than the newest served, older than
    // the window, or unknown (asked for by hash alone, or not the ledger served at its sequence).
    std::string unserved(std::uint32_t seq) const {
        std::shared_lock lock(mutex);
        if (seq == 0 || ledgers.empty()) return "ledger_unknown";
        if (seq > ledgers.rbegin()->first) return "ledger_newer";
        if (seq < ledgers.begin()->first) return "ledger_older";
        return "ledger_unknown";
    }
    void sweep() {
        auto now = std::chrono::steady_clock::now();
        auto last = swept.load();
        if (now - std::chrono::steady_clock::time_point(std::chrono::steady_clock::duration(last)) < std::chrono::seconds(30)) return;
        if (!swept.compare_exchange_strong(last, now.time_since_epoch().count())) return;  // another thread sweeps
        // Not while the store is replaced (node_store()): the family would keep the old one open.
        std::shared_lock working(serving, std::try_to_lock);
        if (!working) return;
        std::shared_ptr<ServingFamily> reader;
        { std::shared_lock lock(mutex); reader = family; }
        if (reader) reader->sweep();
    }
    // Each takes a request already charged its base fee (LedgerServer::request).
    void get_ledger(protocol::TMGetLedger const& request, PeerRequester const& from);
    void get_objects(protocol::TMGetObjectByHash const& request, PeerRequester const& from);
    void fetch_pack(protocol::TMGetObjectByHash const& request, PeerRequester const& from);
    void proof_path(std::string const& payload, PeerRequester const& from);
    void replay_delta(std::string const& payload, PeerRequester const& from);
};

// xrpld's PeerImp::processLedgerRequest: the header and tree roots (liBASE), or each asked-for
// tree node with its children (getNodeFat), no node started past 8,192 and none past 12,288
// (Tuning kSoftMaxReplyNodes, kHardMaxReplyNodes: an xrpld charges a reply with more as
// invalid data) or past `reply_budget`; nothing when the ledger is not served (an error-only
// reply would be charged by the asking xrpld).
void LedgerServer::Impl::get_ledger(protocol::TMGetLedger const& request, PeerRequester const& from) {
    auto type = request.itype();
    if (type == protocol::liTS_CANDIDATE) { decline(from, "tx_set"); return; }  // consensus sets: none here
    if ((request.has_ledgerhash() && request.ledgerhash().size() != 32) ||
        (!request.has_ledgerhash() && !request.has_ledgerseq() && !(request.has_ltype() && request.ltype() == protocol::ltCLOSED)) ||
        (type != protocol::liBASE && (request.nodeids_size() == 0 || request.nodeids_size() > 12288)) ||
        (request.has_querydepth() && (type == protocol::liBASE || request.querydepth() > 3))) { charge(from, 200); return; }
    auto header = request.has_ledgerhash() ? by_hash(request.ledgerhash()) : request.has_ledgerseq() ? by_seq(request.ledgerseq()) : latest();
    if (!header) {
        auto kind = unserved(request.has_ledgerseq() ? request.ledgerseq() : 0);
        if (request.has_ledgerhash() && kind != "ledger_older" && !is_reserved(from)) asked_for(request.ledgerhash());
        decline(from, kind);
        return;
    }
    if (request.has_ledgerseq() && request.ledgerseq() != header->sequence) { if (!request.has_requestcookie()) charge(from, 200); return; }
    protocol::TMLedgerData reply;
    reply.set_ledgerhash(from_hex(header->hash));
    reply.set_ledgerseq(header->sequence);
    reply.set_type(type);
    if (request.has_requestcookie()) reply.set_requestcookie(static_cast<std::uint32_t>(request.requestcookie()));
    if (type == protocol::liBASE) {
        // The 118-byte header, then the state and transaction tree roots in wire format.
        reply.add_nodes()->set_nodedata(header->raw);
        if (auto state = tree(r::SHAMapType::STATE, header->account_hash, header->sequence)) {
            r::Serializer root;
            state.map->serializeRoot(root);
            reply.add_nodes()->set_nodedata(root.data(), root.size());
            if (auto transactions = tree(r::SHAMapType::TRANSACTION, header->transaction_hash, header->sequence)) {
                r::Serializer tx_root;
                transactions.map->serializeRoot(tx_root);
                reply.add_nodes()->set_nodedata(tx_root.data(), tx_root.size());
            }
        }
    } else {
        bool state = type == protocol::liAS_NODE;
        auto map = tree(state ? r::SHAMapType::STATE : r::SHAMapType::TRANSACTION, state ? header->account_hash : header->transaction_hash, header->sequence);
        if (!map) { decline(from, "tree_nodes"); return; }
        auto depth = request.has_querydepth() ? request.querydepth() : 1u;
        std::vector<r::SHAMapNodeID> ids;
        for (auto const& raw : request.nodeids()) {
            auto id = r::deserializeSHAMapNodeID(raw);
            if (!id) { charge(from, 400); return; }
            ids.push_back(*id);
        }
        std::vector<r::SHAMapNodeData> data;
        std::size_t size = 0;
        for (auto const& id : ids) {
            if (reply.nodes_size() >= 8192 || size >= reply_budget) break;
            data.clear();
            try { map.map->getNodeFat(id, data, true, depth); } catch (std::exception const&) {}  // a node no longer stored
            for (auto const& [node_id, is_leaf, blob] : data) {
                if (reply.nodes_size() >= 12288 || size >= reply_budget) break;
                auto* node = reply.add_nodes();
                node->set_nodeid(node_id.getRawString());
                node->set_nodedata(blob.data(), blob.size());
                size += node->nodeid().size() + blob.size();
            }
        }
    }
    if (reply.nodes_size() == 0) { decline(from, "tree_nodes"); return; }
    send(from, protocol::mtLEDGER_DATA, reply, static_cast<std::size_t>(reply.nodes_size()));
}

// xrpld's processGetObjectByHash: each object found (a served ledger's header, or a tree node in
// the node-store format, with its hash prefix) until `reply_budget`, always answered; fetch
// packs apart.
void LedgerServer::Impl::get_objects(protocol::TMGetObjectByHash const& request, PeerRequester const& from) {
    if (request.type() == protocol::TMGetObjectByHash::otFETCH_PACK) { fetch_pack(request, from); return; }
    if (request.has_ledgerhash() && request.ledgerhash().size() != 32) { charge(from, 200); return; }
    if (request.objects_size() > 12288) { charge(from, 400); return; }
    protocol::TMGetObjectByHash reply;
    reply.set_query(false);
    reply.set_type(request.type());
    if (request.has_ledgerhash()) reply.set_ledgerhash(request.ledgerhash());
    std::shared_ptr<r::node_store::Database> db;
    { std::shared_lock lock(mutex); db = store; }
    std::uint64_t hits = 0, misses = 0;
    std::size_t size = 0;
    for (auto const& object : request.objects()) {
        if (size >= reply_budget) break;
        std::string data;
        if (object.has_hash() && object.hash().size() == 32) {
            if (auto header = by_hash(object.hash())) data = std::string("LWR\0", 4) + header->raw;
            else if (db) try {
                if (auto node = db->fetchNodeObject(key256(object.hash()), object.has_ledgerseq() ? object.ledgerseq() : 0))
                    data.assign(reinterpret_cast<char const*>(node->getData().data()), node->getData().size());
            } catch (std::exception const&) {}
        }
        if (data.empty()) { ++misses; continue; }
        ++hits;
        size += object.hash().size() + data.size();
        auto* found = reply.add_objects();
        found->set_hash(object.hash());
        found->set_data(std::move(data));
        if (object.has_nodeid()) found->set_index(object.nodeid());
        if (object.has_ledgerseq()) found->set_ledgerseq(object.ledgerseq());
    }
    // Billed as xrpld bills it: past 16 objects, misses at 8 and hits at 1, plus a size band.
    std::uint64_t asked = static_cast<std::uint64_t>(request.objects_size()), billable = asked > 16 ? asked - 16 : 0;
    auto billed_misses = std::min(misses, billable), billed_hits = std::min(hits, billable - billed_misses);
    charge(from, static_cast<double>(billed_misses * 8 + billed_hits + (asked > 1024 ? 1000 : asked > 64 ? 100 : 0)));
    send(from, protocol::mtGET_OBJECTS, reply, static_cast<std::size_t>(reply.objects_size()));
}

// xrpld's makeFetchPack: from the asked-for ledger back, each parent's header, the state nodes
// it has that its child lacks (16,384 at most) and its transaction nodes (512), until 512
// objects or a second; here also until `reply_budget`, checked at each node added.
void LedgerServer::Impl::fetch_pack(protocol::TMGetObjectByHash const& request, PeerRequester const& from) {
    if (!request.has_ledgerhash() || request.ledgerhash().size() != 32) { charge(from, 200); return; }
    auto have = by_hash(request.ledgerhash());
    if (!have) { charge(from, 10); decline(from, "fetch_pack"); return; }
    auto started = std::chrono::steady_clock::now();
    protocol::TMGetObjectByHash reply;
    reply.set_query(false);
    reply.set_type(protocol::TMGetObjectByHash::otFETCH_PACK);
    reply.set_ledgerhash(request.ledgerhash());
    std::size_t size = 0;
    auto add = [&](Bytes const& hash, std::string data, std::uint32_t seq) {
        size += hash.size() + data.size();
        auto* object = reply.add_objects();
        object->set_hash(hash); object->set_data(std::move(data)); object->set_ledgerseq(seq);
    };
    auto have_state = tree(r::SHAMapType::STATE, have->account_hash, have->sequence);
    try {
        while (have_state && reply.objects_size() < 512 && size < reply_budget && std::chrono::steady_clock::now() - started < std::chrono::seconds(1)) {
            auto want = by_seq(have->sequence - 1);
            if (!want || want->hash != have->parent_hash) break;
            auto want_state = tree(r::SHAMapType::STATE, want->account_hash, want->sequence);
            if (!want_state) break;
            add(from_hex(want->hash), std::string("LWR\0", 4) + want->raw, want->sequence);
            int count = 0;
            want_state.map->visitDifferences(have_state.map.get(), [&](r::SHAMapTreeNode const& node) {
                r::Serializer s;
                node.serializeWithPrefix(s);
                add(bytes(node.getHash().asUInt256()), std::string(static_cast<char const*>(s.data()), s.size()), want->sequence);
                return ++count < 16384 && size < reply_budget;
            });
            if (auto transactions = tree(r::SHAMapType::TRANSACTION, want->transaction_hash, want->sequence)) {
                int tx_count = 0;
                transactions.map->visitNodes([&](r::SHAMapTreeNode& node) {
                    r::Serializer s;
                    node.serializeWithPrefix(s);
                    add(bytes(node.getHash().asUInt256()), std::string(static_cast<char const*>(s.data()), s.size()), want->sequence);
                    return ++tx_count < 512 && size < reply_budget;
                });
            }
            have = want;
            have_state = std::move(want_state);
        }
    } catch (std::exception const&) {}  // a node no longer stored: send what was gathered
    if (reply.objects_size() == 0) { decline(from, "fetch_pack"); return; }
    send(from, protocol::mtGET_OBJECTS, reply, static_cast<std::size_t>(reply.objects_size()));
}

// Ledger replay (only when both sides negotiated it): a key's path from leaf to root, or a
// ledger's header and transactions. xrpld never sends an error reply; it charges instead. A
// ledger's transactions go in full or not at all (only all of them match its header), so one
// whose transactions are past `reply_budget` is not answered.
void LedgerServer::Impl::proof_path(std::string const& payload, PeerRequester const& from) {
    protocol::TMProofPathRequest request;
    if (!from.ledger_replay || !request.ParseFromString(payload) || request.key().size() != 32 || request.ledgerhash().size() != 32 ||
        (request.type() != protocol::lmTRANSACTION && request.type() != protocol::lmACCOUNT_STATE)) { charge(from, 200); return; }
    auto header = by_hash(request.ledgerhash());
    if (!header) { charge(from, 10); decline(from, "proof_path"); return; }
    bool state = request.type() == protocol::lmACCOUNT_STATE;
    auto map = tree(state ? r::SHAMapType::STATE : r::SHAMapType::TRANSACTION, state ? header->account_hash : header->transaction_hash, header->sequence);
    std::optional<std::vector<r::Blob>> path;
    if (map) try { path = map.map->getProofPath(key256(request.key())); } catch (std::exception const&) {}
    if (!path) { charge(from, 10); decline(from, "proof_path"); return; }
    protocol::TMProofPathResponse reply;
    reply.set_key(request.key());
    reply.set_ledgerhash(request.ledgerhash());
    reply.set_type(request.type());
    reply.set_ledgerheader(header->raw);
    for (auto const& node : *path) reply.add_path(node.data(), node.size());
    send(from, protocol::mtPROOF_PATH_RESPONSE, reply, path->size());
}
void LedgerServer::Impl::replay_delta(std::string const& payload, PeerRequester const& from) {
    protocol::TMReplayDeltaRequest request;
    if (!from.ledger_replay || !request.ParseFromString(payload) || request.ledgerhash().size() != 32) { charge(from, 200); return; }
    auto header = by_hash(request.ledgerhash());
    if (!header) { charge(from, 10); decline(from, "replay_delta"); return; }
    protocol::TMReplayDeltaResponse reply;
    reply.set_ledgerhash(request.ledgerhash());
    reply.set_ledgerheader(header->raw);
    if (!key256(from_hex(header->transaction_hash)).isZero()) {
        auto map = tree(r::SHAMapType::TRANSACTION, header->transaction_hash, header->sequence);
        if (!map) { charge(from, 10); decline(from, "replay_delta"); return; }
        std::size_t size = 0;
        try {
            map.map->visitLeaves([&](boost::intrusive_ptr<r::SHAMapItem const> const& item) {
                if (size > reply_budget) return;
                size += item->slice().size();
                if (size > reply_budget) { reply.clear_transaction(); return; }
                reply.add_transaction(item->slice().data(), item->slice().size());
            });
        } catch (std::exception const&) { charge(from, 10); decline(from, "replay_delta"); return; }
        if (size > reply_budget) { decline(from, "replay_delta"); return; }
    }
    send(from, protocol::mtREPLAY_DELTA_RESPONSE, reply, static_cast<std::size_t>(reply.transaction_size()));
}

LedgerServer::LedgerServer(std::size_t window, unsigned threads) : impl_(std::make_unique<Impl>()) {
    auto& m = *impl_;
    if (!window) throw Error("A served window needs at least one ledger");
    m.window = window;
    m.packs_limit = std::max(threads, 2u) / 2;
    for (unsigned n = 0; n < std::max(threads, 1u); ++n)
        m.workers.emplace_back([&m] {
            for (;;) {
                Impl::Job job;
                {
                    std::unique_lock lock(m.queue_mutex);
                    m.wake.wait_for(lock, std::chrono::seconds(30), [&] { return m.stopping || !m.jobs.empty(); });
                    if (m.stopping) return;
                    if (m.jobs.empty()) { lock.unlock(); m.sweep(); continue; }
                    job = std::move(m.jobs.front());
                    m.jobs.pop_front();
                }
                m.sweep();
                try { job.run(); } catch (std::exception const&) {}
                std::lock_guard lock(m.queue_mutex);
                m.queued_bytes -= job.bytes;
                auto it = m.load.find(job.peer);
                if (it == m.load.end()) continue;
                --it->second.jobs;
                it->second.bytes -= job.bytes;
                if (job.pack) { --it->second.packs; --m.packs; }
                if (!it->second.jobs) m.load.erase(it);
            }
        });
}
LedgerServer::~LedgerServer() {
    auto& m = *impl_;
    { std::lock_guard lock(m.queue_mutex); m.stopping = true; }
    m.wake.notify_all();
    for (auto& worker : m.workers) worker.join();
}
std::size_t LedgerServer::window() const { return impl_->window; }
void LedgerServer::node_store(std::shared_ptr<xrpl::node_store::Database> store) {
    auto& m = *impl_;
    { std::shared_lock lock(m.mutex); if (store == m.store) return; }
    std::unique_lock quiet(m.serving);
    std::unique_lock lock(m.mutex);
    m.store = store;
    m.family = store ? std::make_shared<ServingFamily>(std::move(store)) : nullptr;
}
void LedgerServer::add(Header const& header) {
    auto& m = *impl_;
    std::shared_ptr<r::node_store::Database> db;
    {
        std::shared_lock lock(m.mutex);
        if (!m.ledgers.empty() && header.sequence <= m.ledgers.rbegin()->first) return;
        db = m.store;
    }
    m.sweep();
    // Served only once both its trees are stored (an earlier ledger's may have been compacted
    // away, or applied before its transaction tree was kept).
    if (!db) return;
    for (auto const* hash : {&header.account_hash, &header.transaction_hash}) {
        auto root = key256(from_hex(*hash));
        if (root.isZero()) continue;
        try { if (!db->fetchNodeObject(root, header.sequence)) return; }
        catch (std::exception const&) { return; }
    }
    {
        std::unique_lock lock(m.mutex);
        // The range stays contiguous: after a gap it starts again at this ledger.
        if (!m.ledgers.empty() && header.sequence != m.ledgers.rbegin()->first + 1) { m.ledgers.clear(); m.hashes.clear(); }
        m.ledgers.emplace(header.sequence, header);
        m.hashes.emplace(from_hex(header.hash), header.sequence);
        while (m.ledgers.size() > m.window) {
            m.hashes.erase(from_hex(m.ledgers.begin()->second.hash));
            m.ledgers.erase(m.ledgers.begin());
        }
    }
    m.served_now(from_hex(header.hash));
}
std::optional<std::pair<std::uint32_t, std::uint32_t>> LedgerServer::range() const {
    std::shared_lock lock(impl_->mutex);
    if (impl_->ledgers.empty()) return std::nullopt;
    return std::pair{impl_->ledgers.begin()->first, impl_->ledgers.rbegin()->first};
}
std::optional<Header> LedgerServer::newest() const { return impl_->latest(); }
std::string LedgerServer::status_message() const {
    std::shared_lock lock(impl_->mutex);
    if (impl_->ledgers.empty()) return {};
    auto const& newest = impl_->ledgers.rbegin()->second;
    protocol::TMStatusChange status;
    status.set_newevent(protocol::neACCEPTED_LEDGER);
    status.set_ledgerseq(newest.sequence);
    status.set_ledgerhash(from_hex(newest.hash));
    status.set_ledgerhashprevious(from_hex(newest.parent_hash));
    status.set_networktime(network_seconds());
    status.set_firstseq(impl_->ledgers.begin()->first);
    status.set_lastseq(newest.sequence);
    return status.SerializeAsString();
}
void LedgerServer::reserve(std::set<std::string> keys) {
    std::lock_guard lock(impl_->charge_mutex);
    impl_->reserved = std::move(keys);
}
void LedgerServer::greet(PeerSession& session) const {
    if (auto status = status_message(); !status.empty()) try { session.send_raw(protocol::mtSTATUS_CHANGE, status); } catch (std::exception const&) {}
}
// Each request is charged its base fee as it arrives (xrpld's, on the asking session's thread),
// so one refused for a full queue costs the peer as much as one answered; what its answer
// costs beyond that is charged by the worker. A peer whose send queue is long is not answered
// (xrpld's kDropSendQueue), here and again when a worker takes the request. One too large to
// answer is charged here as the worker would charge it, so it is never held while it waits; a
// queue holding `peer_request_bytes` of the peer's requests, or `request_bytes` of all, is full.
void LedgerServer::request(int type, std::string const& payload, PeerRequester const& from) {
    auto& m = *impl_;
    ++m.requests;
    bool own = m.is_reserved(from);
    if (own) ++m.reserved_requests;
    std::function<void()> run;
    double fee = 250;
    double oversized = 0;  // what the worker would charge for a request too large to answer
    bool pack = false;
    if (type == protocol::mtGET_LEDGER) {
        auto request = std::make_shared<protocol::TMGetLedger>();
        if (!request->ParseFromString(payload)) { m.charge(from, 200); return; }
        fee = request->has_requestcookie() ? 1 : 250;  // one relayed for another of its peers: not charged
        if (request->itype() != protocol::liTS_CANDIDATE && request->itype() != protocol::liBASE && request->nodeids_size() > 12288) oversized = 200;
        run = [&m, request, from] { m.get_ledger(*request, from); };
    } else if (type == protocol::mtGET_OBJECTS) {
        auto request = std::make_shared<protocol::TMGetObjectByHash>();
        if (!request->ParseFromString(payload)) { m.charge(from, 200); return; }
        if (request->type() == protocol::TMGetObjectByHash::otTRANSACTIONS) { m.decline(from, "transactions"); return; }  // transaction relay: not offered
        pack = request->type() == protocol::TMGetObjectByHash::otFETCH_PACK;
        if (pack) {
            fee = 2000;
            // A fetch pack is made from the ledger hash alone: the objects listed are not kept.
            auto bare = std::make_shared<protocol::TMGetObjectByHash>();
            bare->set_type(request->type());
            bare->set_query(request->query());
            if (request->has_ledgerhash()) bare->set_ledgerhash(request->ledgerhash());
            request = std::move(bare);
        } else if (request->objects_size() > 12288) {
            oversized = request->has_ledgerhash() && request->ledgerhash().size() != 32 ? 200 : 400;
        }
        run = [&m, request, from] { m.get_objects(*request, from); };
    } else if (type == protocol::mtPROOF_PATH_REQ || type == protocol::mtREPLAY_DELTA_REQ) {
        // Two hashes, a key and a type: anything much longer is no request the worker would answer.
        if (payload.size() > 4096) oversized = 200;
        if (type == protocol::mtPROOF_PATH_REQ) run = [&m, payload, from] { m.proof_path(payload, from); };
        else run = [&m, payload, from] { m.replay_delta(payload, from); };
    } else return;
    if (m.charge(from, fee)) return;
    if (from.backlogged && from.backlogged()) { m.decline(from, "send_queue"); return; }
    if (oversized) { m.charge(from, oversized); return; }
    auto peer = Impl::address_of(from);
    auto bytes = pack ? 0 : payload.size();  // what the job holds
    {
        std::lock_guard lock(m.queue_mutex);
        if (m.stopping) return;
        auto& load = m.load[peer];
        bool full = m.jobs.size() >= 4096 || m.queued_bytes + bytes > request_bytes ||
                    (!own && (load.jobs >= Impl::peer_jobs || load.bytes + bytes > peer_request_bytes)) ||
                    (pack && (load.packs || m.packs >= m.packs_limit));
        if (full) {
            if (!load.jobs) m.load.erase(peer);
            ++m.busy;
            return;
        }
        ++load.jobs;
        load.bytes += bytes; m.queued_bytes += bytes;
        if (pack) { ++load.packs; ++m.packs; }
        m.jobs.push_back({[&m, run = std::move(run), from] {
            if (from.backlogged && from.backlogged()) { m.decline(from, "send_queue"); return; }
            std::shared_lock working(m.serving);
            run();
        }, peer, pack, bytes});
    }
    m.wake.notify_one();
}
bool LedgerServer::admits(std::string const& address) const { return impl_->admits(address); }
void LedgerServer::charge(PeerRequester const& from, double fee) { impl_->charge(from, fee); }
Json LedgerServer::status() const {
    auto& m = *impl_;
    Json result{{"window", m.window}, {"requests", m.requests.load()}, {"answered", m.answered.load()}, {"unanswered", m.unanswered.load()},
                {"nodes_sent", m.nodes_sent.load()}, {"bytes_sent", m.bytes_sent.load()}, {"peers_dropped", m.dropped.load()}, {"busy", m.busy.load()}};
    if (auto served = range()) result["ledgers"] = std::to_string(served->first) + "-" + std::to_string(served->second);
    bool any_reserved;
    { std::lock_guard lock(m.charge_mutex); any_reserved = !m.reserved.empty(); }
    {
        std::lock_guard asked_lock(m.asked_mutex);
        m.expire_asked(std::chrono::steady_clock::now());
        result["unserved_ledgers"] = {{"asked", m.asked_total},
                                      {"served_after", {{"0-5s", m.served_after[0]}, {"5-15s", m.served_after[1]}, {"15-60s", m.served_after[2]}, {"60s+", m.served_after[3]}}},
                                      {"not_served", m.not_served}, {"pending", m.asked.size()}};
    }
    std::lock_guard kinds(m.kinds_mutex);
    result["unanswered_kinds"] = m.unanswered_kinds;
    // Included in the totals above; what the network asked for is the difference.
    if (any_reserved)
        result["reserved"] = {{"requests", m.reserved_requests.load()}, {"answered", m.reserved_answered.load()}, {"unanswered", m.reserved_unanswered.load()},
                              {"unanswered_kinds", m.reserved_unanswered_kinds}, {"nodes_sent", m.reserved_nodes.load()}, {"bytes_sent", m.reserved_bytes.load()}};
    return result;
}

struct PeerOverlay::Impl {
    Options options;
    PeerLedgers& outbound;
    std::shared_ptr<LedgerServer> server;
    std::function<void(Bytes const&)> on_validation, on_manifest;
    std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
    struct Inbound {
        std::unique_ptr<PeerSession> session;
        std::string address;
        std::thread reader;
    };
    mutable std::mutex mutex;
    std::vector<std::shared_ptr<Inbound>> inbound;
    // Sessions `admit` let in whose handshakes are still finishing, (address, node key): counted
    // as inbound ones, so concurrent handshakes do not all pass the limits.
    std::vector<std::pair<std::string, std::string>> admitting;
    std::unique_ptr<PeerListener> listener;
    std::atomic<bool> stopping{false};
    std::thread keeper;
    std::atomic<std::uint64_t> accepted{0}, refused{0};
    // Why sessions ended and why connections failed before becoming sessions (mutex held).
    std::uint64_t short_sessions = 0;
    std::map<std::string, std::uint64_t> closed, handshake_failures;

    Impl(Options options, PeerLedgers& outbound, std::shared_ptr<LedgerServer> server,
         std::function<void(Bytes const&)> on_validation, std::function<void(Bytes const&)> on_manifest)
        : options(std::move(options)), outbound(outbound), server(std::move(server)),
          on_validation(std::move(on_validation)), on_manifest(std::move(on_manifest)) {}
    std::size_t open_inbound() const {
        std::lock_guard lock(mutex);
        return static_cast<std::size_t>(std::count_if(inbound.begin(), inbound.end(), [](auto const& peer) { return peer->session->open(); }));
    }
    // As xrpld's PeerFinder: one session per node key, two per address (an IPv6 /64 counted as
    // one, address_group), `max_inbound` in all; reserved keys are admitted beyond the limits. An
    // address whose peer was dropped for its charges waits until its balance has decayed
    // (xrpld's "Over resource limit").
    bool admit(PeerSession::Info const& info, std::string const& address) {
        if (!options.reserved.contains(info.public_key) && !server->admits(address)) { ++refused; return false; }
        for (auto const& session : outbound.sessions()) if (session.public_key == info.public_key) { ++refused; return false; }
        auto group = address_group(address);
        std::lock_guard lock(mutex);
        std::size_t open = 0, same = 0;
        for (auto const& peer : inbound) {
            if (!peer->session->open()) continue;
            if (peer->session->info().public_key == info.public_key) { ++refused; return false; }
            if (options.reserved.contains(peer->session->info().public_key)) continue;  // not counted
            ++open;
            if (address_group(peer->address) == group) ++same;
        }
        for (auto const& [from, key] : admitting) {
            if (key == info.public_key) { ++refused; return false; }
            if (options.reserved.contains(key)) continue;
            ++open;
            if (address_group(from) == group) ++same;
        }
        if (!options.reserved.contains(info.public_key) && (open >= options.max_inbound || same >= options.ip_limit)) { ++refused; return false; }
        admitting.emplace_back(address, info.public_key);
        return true;
    }
    // An admitted handshake finished (accepted) or failed: no longer counted as admitting.
    void admitted(std::string const& address, std::string const& key) { std::lock_guard lock(mutex); unadmit(address, key); }
    void unadmit(std::string const& address, std::string const& key) {  // under `mutex`
        if (auto it = std::find(admitting.begin(), admitting.end(), std::pair{address, key}); it != admitting.end()) admitting.erase(it);
    }
    // Listed first, then greeted: a peer that has this node's status is among its peers. One that
    // fails before it is listed (a thread or memory) throws with its place still counted as
    // admitting; the listener then gives the place back (`withdrawn`).
    void accept(std::unique_ptr<PeerSession> session, std::string const& address) {
        auto peer = std::make_shared<Inbound>();
        peer->session = std::move(session);
        peer->address = address;
        std::cerr << Json{{"event", "peer_accepted"}, {"endpoint", peer->session->info().endpoint}, {"server", peer->session->info().server}}.dump() << std::endl;
        peer->reader = std::thread([this, raw = peer.get()] { read(*raw); });
        try {
            std::lock_guard lock(mutex);
            inbound.push_back(peer);  // counted as inbound before it stops being counted as admitting
            ++accepted;
            unadmit(address, peer->session->info().public_key);
        } catch (...) {
            // Not listed: its reader stops once the session is closed (a joinable thread
            // destroyed with `peer` would end the process).
            peer->session->close("Not accepted");
            peer->reader.join();
            throw;
        }
        server->greet(*peer->session);
    }
    // Validations, manifests and endpoints from an inbound peer count as from outbound ones.
    void read(Inbound& peer) {
        while (!stopping) {
            std::optional<PeerSession::Message> message;
            try { message = peer.session->receive(std::chrono::milliseconds(500)); } catch (std::exception const&) { return; }
            if (!message) continue;
            if (message->type == protocol::mtVALIDATION || message->type == protocol::mtPROPOSE_LEDGER || message->type == protocol::mtSQUELCH) {
                if (options.relay) {
                    if (auto validation = options.relay->receive(*peer.session, message->type, message->payload); validation && on_validation)
                        on_validation(*validation);
                } else if (message->type == protocol::mtVALIDATION) {
                    protocol::TMValidation validation;
                    if (validation.ParseFromString(message->payload) && on_validation) on_validation(validation.validation());
                }
            } else if (message->type == protocol::mtMANIFESTS) {
                protocol::TMManifests manifests;
                // A peer's manifests, at most a list's worth per message (the network's validators number in the
                // hundreds); each is checked against the validator list before its signatures are.
                if (manifests.ParseFromString(message->payload) && on_manifest)
                    for (int i = 0; i < std::min(manifests.list_size(), 512); ++i) on_manifest(manifests.list(i).stobject());
            } else if (message->type == protocol::mtENDPOINTS) {
                protocol::TMEndpoints endpoints;
                if (!endpoints.ParseFromString(message->payload)) continue;
                std::vector<std::string> learned;
                for (auto const& endpoint : endpoints.endpoints_v2())
                    if (endpoint.hops() > 0 && learned.size() < 64 && public_endpoint(endpoint.endpoint())) learned.push_back(canonical_endpoint(endpoint.endpoint()));
                outbound.learn(learned);
            }
        }
    }
    // Closed inbound sessions are logged, counted and removed (their readers joined outside the lock).
    void reap() {
        std::vector<std::shared_ptr<Inbound>> ended;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard lock(mutex);
            for (auto it = inbound.begin(); it != inbound.end();) {
                if ((*it)->session->open()) { ++it; continue; }
                auto const& info = (*it)->session->info();
                auto reason = (*it)->session->failure(); auto traffic = (*it)->session->traffic();
                auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now - info.connected).count();
                std::cerr << Json{{"event", "peer_closed"}, {"direction", "in"}, {"endpoint", info.endpoint}, {"server", info.server},
                                  {"seconds", seconds}, {"reason", reason},
                                  {"received", {{"messages", traffic.messages_in}, {"bytes", traffic.bytes_in}}},
                                  {"sent", {{"messages", traffic.messages_out}, {"bytes", traffic.bytes_out}}}}.dump() << std::endl;
                count_reason(closed, reason);
                if (seconds < 60) ++short_sessions;
                ended.push_back(std::move(*it));
                it = inbound.erase(it);
            }
        }
        for (auto& peer : ended) {
            if (peer->reader.joinable()) peer->reader.join();
            if (options.relay) options.relay->ended(peer->session->info().id);
        }
    }
    void broadcast(int type, std::string const& payload) {
        std::vector<std::shared_ptr<Inbound>> peers;
        { std::lock_guard lock(mutex); peers = inbound; }
        for (auto const& peer : peers) try { peer->session->send_raw(type, payload); } catch (std::exception const&) {}
        outbound.broadcast(type, payload);
    }
    // This node's own endpoint (hops 0) while it has room: peers then offer it to others.
    void advertise() {
        if (!listener || options.private_mode || open_inbound() >= options.max_inbound) return;
        protocol::TMEndpoints endpoints;
        endpoints.set_version(2);
        auto* self = endpoints.add_endpoints_v2();
        self->set_endpoint("[::]:" + std::to_string(listener->port()));
        self->set_hops(0);
        broadcast(protocol::mtENDPOINTS, endpoints.SerializeAsString());
    }
    // As xrpld's NetworkOPs moves a node following the network to `full`: its newest ledger closed
    // within twice its close time resolution (20 s on mainnet). Validators and observers alike
    // say `full`; a node that falls behind by up to a minute says `tracking`, then `syncing`.
    std::string state() const {
        auto newest = server->newest();
        if (!newest) return "connected";
        auto age = network_seconds() > newest->close_time ? network_seconds() - newest->close_time : 0;
        std::uint32_t resolution = 10;
        try { resolution = static_cast<std::uint32_t>(r::deserializeHeader({newest->raw.data(), newest->raw.size()}).closeTimeResolution.count()); }
        catch (std::exception const&) {}
        return age <= 2 * resolution ? "full" : age <= 60 ? "tracking" : "syncing";
    }
};

PeerOverlay::PeerOverlay(Options options, PeerLedgers& outbound, std::shared_ptr<LedgerServer> server,
                         std::function<void(Bytes const&)> on_validation, std::function<void(Bytes const&)> on_manifest)
    : impl_(std::make_unique<Impl>(std::move(options), outbound, std::move(server), std::move(on_validation), std::move(on_manifest))) {
    auto& m = *impl_;
    m.server->reserve(m.options.reserved);
    // A peer relaying messages that fail their checks is charged with what it asks for.
    if (m.options.relay)
        m.options.relay->charges([server = m.server](PeerSession& session, double fee) { server->charge(session.requester(), fee); });
    PeerLocal local{outbound.identity(), m.options.network_id, m.options.crawl && !m.options.private_mode, true, m.server,
                    [server = m.server]() -> std::optional<std::pair<Bytes, Bytes>> {
                        auto newest = server->newest();
                        if (!newest) return std::nullopt;
                        return std::pair{from_hex(newest->hash), from_hex(newest->parent_hash)};
                    }};
    local.squelch = m.options.relay && m.options.relay->options().squelch;
    std::set<int> wanted{protocol::mtVALIDATION, protocol::mtMANIFESTS, protocol::mtENDPOINTS};
    if (m.options.relay) { wanted.insert(protocol::mtPROPOSE_LEDGER); wanted.insert(protocol::mtSQUELCH); }
    PeerListener::Handlers handlers;
    handlers.admit = [&m](PeerSession::Info const& info, std::string const& address) { return m.admit(info, address); };
    handlers.accepted = [&m](std::unique_ptr<PeerSession> session, std::string const& address) { m.accept(std::move(session), address); };
    handlers.redirect = [&m] { return m.outbound.known(); };
    handlers.withdrawn = [&m](PeerSession::Info const& info, std::string const& address) { m.admitted(address, info.public_key); };
    handlers.failed = [&m](std::string const& why, std::string const&) { std::lock_guard lock(m.mutex); count_reason(m.handshake_failures, why); };
    handlers.http = [this](std::string const& target) -> std::optional<std::pair<int, std::string>> {
        if (target == "/crawl" && impl_->options.crawl) return std::pair{200, crawl().dump()};
        if (target == "/health") { auto [code, body] = health(); return std::pair{code, body.dump()}; }
        return std::nullopt;
    };
    if (!m.options.listen.empty())
        m.listener = std::make_unique<PeerListener>(m.options.listen, local, wanted, handlers);
    m.keeper = std::thread([&m] {
        auto advertised = std::chrono::steady_clock::now();
        while (!m.stopping) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            m.reap();
            if (std::chrono::steady_clock::now() - advertised >= std::chrono::seconds(151)) { m.advertise(); advertised = std::chrono::steady_clock::now(); }
        }
    });
}
PeerOverlay::~PeerOverlay() {
    auto& m = *impl_;
    // The keeper first: it may be advertising, which reads the listener's port.
    m.stopping = true;
    if (m.keeper.joinable()) m.keeper.join();
    m.listener.reset();  // stops accepting and waits for handshakes in progress
    std::vector<std::shared_ptr<Impl::Inbound>> peers;
    { std::lock_guard lock(m.mutex); peers = std::move(m.inbound); }
    for (auto& peer : peers) { peer->session->close(); if (peer->reader.joinable()) peer->reader.join(); }
}
std::uint16_t PeerOverlay::port() const { return impl_->listener ? impl_->listener->port() : 0; }
void PeerOverlay::visit(std::function<void(PeerSession&)> const& visit) const {
    std::vector<std::shared_ptr<Impl::Inbound>> peers;
    { std::lock_guard lock(impl_->mutex); peers = impl_->inbound; }
    for (auto const& peer : peers) if (peer->session->open()) visit(*peer->session);
}
std::pair<unsigned, unsigned> peer_limits(unsigned max_peers, unsigned outbound, std::optional<unsigned> max_inbound) {
    if (!max_peers) return {outbound, max_inbound.value_or(11)};
    auto out = std::min(outbound, max_peers);
    auto room = max_peers - out;
    return {out, max_inbound ? std::min(*max_inbound, room) : room};
}
void PeerOverlay::announce() {
    auto status = impl_->server->status_message();
    if (!status.empty()) impl_->broadcast(protocol::mtSTATUS_CHANGE, status);
}
Json PeerOverlay::status() const {
    auto& m = *impl_;
    Json inbound = Json::array(), closed, failures;
    std::uint64_t short_sessions;
    {
        std::lock_guard lock(m.mutex);
        for (auto const& peer : m.inbound)
            if (peer->session->open()) inbound.push_back({{"endpoint", peer->session->info().endpoint}, {"server", peer->session->info().server}});
        closed = m.closed; failures = m.handshake_failures; short_sessions = m.short_sessions;
    }
    return {{"listening", port()}, {"inbound", inbound}, {"accepted", m.accepted.load()}, {"refused", m.refused.load()},
            {"short_sessions", short_sessions}, {"closed", closed}, {"handshake_failures", failures}, {"serving", m.server->status()}};
}
// xrpld's /crawl layout: the peers (with addresses only for those that are public) and a
// server_info subset, its `server_state` as xrpld's (state()): this node follows validated
// ledgers, taking no part in consensus, as a node that does not validate.
Json PeerOverlay::crawl() const {
    auto& m = *impl_;
    auto now = std::chrono::steady_clock::now();
    Json active = Json::array();
    auto add = [&](PeerSession::Info const& info, std::string const& address) {
        Json peer{{"type", info.inbound ? "in" : "out"}, {"version", info.server},
                  {"uptime", std::chrono::duration_cast<std::chrono::seconds>(now - info.connected).count()}};
        if (auto key = r::parseBase58<r::PublicKey>(r::TokenType::NodePublic, info.public_key))
            peer["public_key"] = base64(Bytes(reinterpret_cast<char const*>(key->data()), key->size()));
        if (info.crawl) {
            peer["ip"] = address;
            if (!info.inbound) if (auto colon = info.endpoint.rfind(':'); colon != std::string::npos) {
                peer["ip"] = info.endpoint.substr(0, colon);
                peer["port"] = info.endpoint.substr(colon + 1);
            }
        }
        active.push_back(std::move(peer));
    };
    for (auto const& info : m.outbound.sessions()) add(info, info.endpoint);
    {
        std::lock_guard lock(m.mutex);
        for (auto const& peer : m.inbound) if (peer->session->open()) add(peer->session->info(), peer->address);
    }
    Json server{{"build_version", "xrpla-" XRPLA_VERSION}, {"server_state", m.state()}, {"pubkey_node", m.outbound.identity().base58()},
                {"peers", active.size()}, {"network_id", m.options.network_id},
                {"uptime", std::chrono::duration_cast<std::chrono::seconds>(now - m.started).count()}};
    auto served = m.server->range();
    server["complete_ledgers"] = served ? std::to_string(served->first) + "-" + std::to_string(served->second) : "empty";
    if (auto newest = m.server->newest())
        server["validated_ledger"] = {{"seq", newest->sequence}, {"hash", upper(newest->hash)},
                                      {"age", network_seconds() > newest->close_time ? network_seconds() - newest->close_time : 0}};
    return {{"version", 2}, {"overlay", {{"active", active}}}, {"server", server}, {"unl", Json::object()}};
}
// xrpld's /health layout and peer thresholds (7 or fewer a warning, none critical); 200, 503
// or 500. The newest ledger's age allows for this node following validated ledgers a few
// seconds behind the network (a quorum's signatures, then the fetch): a warning from 20 s,
// critical from 60 s (xrpld's are 7 s and 20 s).
std::pair<int, Json> PeerOverlay::health() const {
    auto& m = *impl_;
    int level = 0;
    Json info = Json::object();
    if (auto newest = m.server->newest()) {
        auto age = network_seconds() > newest->close_time ? network_seconds() - newest->close_time : 0;
        info["validated_ledger"] = age;
        level = std::max(level, age >= 60 ? 2 : age >= 20 ? 1 : 0);
    } else { info["validated_ledger"] = -1; level = std::max(level, 1); }
    auto peers = m.outbound.sessions().size() + m.open_inbound();
    info["peers"] = peers;
    level = std::max(level, peers == 0 ? 2 : peers <= 7 ? 1 : 0);
    info["server_state"] = m.state();
    return {level == 0 ? 200 : level == 1 ? 503 : 500, Json{{"info", info}}};
}
}
