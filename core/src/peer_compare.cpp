// The same peer-protocol requests sent to a reference xrpld and to an XRPLA node (operations.hpp,
// peer_compare): ledger data, tree nodes at several query depths, objects by hash and, where both
// offer it, ledger replay, each answer compared with the reference's.
#include "peer.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/proto/xrpl.pb.h>
#include <xrpl/basics/IntrusivePointer.ipp>
#include <xrpl/shamap/SHAMapInnerNode.h>
#include <xrpl/shamap/SHAMapNodeID.h>
#include <xrpl/shamap/SHAMapTreeNode.h>
#include <map>
#include <random>
#include <thread>

namespace xrpla::core {
namespace r = ::xrpl;
namespace {
using clock = std::chrono::steady_clock;
// One session and the replies it has received, by request cookie (ledger data) or arrival.
struct Side {
    std::string name;
    std::unique_ptr<PeerSession> session;
    std::uint64_t cookie = 0;
    std::optional<PeerSession::Message> next(int type, std::chrono::milliseconds timeout,
                                             std::function<bool(std::string const&)> const& matches = {}) {
        auto until = clock::now() + timeout;
        while (clock::now() < until) {
            auto message = session->receive(std::chrono::duration_cast<std::chrono::milliseconds>(until - clock::now()));
            if (message && message->type == type && (!matches || matches(message->payload))) return message;
        }
        return std::nullopt;
    }
    // A TMGetLedger's answer, or nullopt when the peer sends none (as xrpld does for a ledger it
    // lacks).
    std::optional<protocol::TMLedgerData> ledger_data(protocol::TMGetLedger request, std::chrono::milliseconds timeout) {
        request.set_requestcookie(++cookie);
        session->send(protocol::mtGET_LEDGER, request);
        auto reply = next(protocol::mtLEDGER_DATA, timeout, [&](std::string const& payload) {
            protocol::TMLedgerData data;
            return data.ParseFromString(payload) && data.requestcookie() == cookie;
        });
        if (!reply) return std::nullopt;
        protocol::TMLedgerData data;
        data.ParseFromString(reply->payload);
        return data;
    }
    std::optional<protocol::TMGetObjectByHash> objects(protocol::TMGetObjectByHash const& request, std::chrono::milliseconds timeout) {
        session->send(protocol::mtGET_OBJECTS, request);
        auto reply = next(protocol::mtGET_OBJECTS, timeout, [](std::string const& payload) {
            protocol::TMGetObjectByHash objects;
            return objects.ParseFromString(payload) && !objects.query();
        });
        if (!reply) return std::nullopt;
        protocol::TMGetObjectByHash objects;
        objects.ParseFromString(reply->payload);
        return objects;
    }
};
// A ledger-data answer as compared: its fields and its nodes by node ID (order apart).
Json ledger_data_json(std::optional<protocol::TMLedgerData> const& data) {
    if (!data) return {{"answered", false}};
    Json nodes = Json::object();
    Json order = Json::array();
    for (auto const& node : data->nodes()) {
        auto id = to_hex(node.has_nodeid() ? node.nodeid() : std::string());
        nodes[id] = to_hex(node.nodedata());
        order.push_back(id);
    }
    return {{"answered", true}, {"error", data->has_error() ? Json(static_cast<int>(data->error())) : Json(nullptr)},
            {"type", static_cast<int>(data->type())}, {"ledger_hash", to_hex(data->ledgerhash())}, {"ledger_seq", data->ledgerseq()},
            {"nodes", nodes}, {"order", order}};
}
Json objects_json(std::optional<protocol::TMGetObjectByHash> const& reply) {
    if (!reply) return {{"answered", false}};
    Json objects = Json::object();
    for (auto const& object : reply->objects()) objects[to_hex(object.hash())] = to_hex(object.data());
    return {{"answered", true}, {"type", static_cast<int>(reply->type())}, {"objects", objects}};
}
// Where two answers differ, by path (at most `limit`).
void differences(Json const& a, Json const& b, std::string const& path, Json& out, std::size_t limit = 8) {
    if (out.size() >= limit) return;
    if (a.is_object() && b.is_object()) {
        std::set<std::string> keys;
        for (auto const& [k, v] : a.items()) keys.insert(k);
        for (auto const& [k, v] : b.items()) keys.insert(k);
        for (auto const& k : keys) {
            if (!a.contains(k)) out.push_back(path + "/" + k + ": only xrpla");
            else if (!b.contains(k)) out.push_back(path + "/" + k + ": only xrpld");
            else differences(a.at(k), b.at(k), path + "/" + k, out, limit);
        }
    } else if (a != b) out.push_back(path + ": xrpld " + a.dump().substr(0, 120) + " xrpla " + b.dump().substr(0, 120));
}
r::uint256 node_hash(std::string const& wire) {
    auto node = r::SHAMapTreeNode::makeFromWire(r::Slice(wire.data(), wire.size()));
    if (!node) throw Error("Unreadable tree node");
    return node->getHash().asUInt256();
}
}

Json peer_compare(PeerCompareOptions const& options) {
    PeerLocal local{options.identity.empty() ? NodeIdentity::ephemeral() : NodeIdentity::load_or_create(options.identity), options.network_id};
    local.ledger_replay = true;  // offered: compared where both sides take it up
    std::set<int> wanted{protocol::mtLEDGER_DATA, protocol::mtGET_OBJECTS, protocol::mtSTATUS_CHANGE,
                         protocol::mtREPLAY_DELTA_RESPONSE, protocol::mtPROOF_PATH_RESPONSE};
    Side reference{"xrpld", std::make_unique<PeerSession>(options.reference, local, wanted)};
    Side subject{"xrpla", std::make_unique<PeerSession>(options.subject, local, wanted)};
    Json result{{"reference", {{"endpoint", options.reference}, {"server", reference.session->info().server}}},
                {"subject", {{"endpoint", options.subject}, {"server", subject.session->info().server}}}};
    // The ledgers compared: the newest the subject holds (its status message), going back.
    std::uint32_t newest = 0;
    if (auto message = subject.next(protocol::mtSTATUS_CHANGE, std::chrono::seconds(30), [](std::string const& payload) {
            protocol::TMStatusChange status;
            return status.ParseFromString(payload) && status.has_lastseq();
        })) {
        protocol::TMStatusChange status;
        status.ParseFromString(message->payload);
        newest = status.lastseq();
    }
    if (!newest) throw Error("The subject did not say which ledgers it holds");
    auto timeout = std::chrono::milliseconds(options.timeout_ms);
    std::uint64_t same = 0, different = 0, reference_silent = 0;
    Json cases = Json::array();
    auto compare = [&](std::string const& what, Json const& expected, Json const& actual) {
        if (!expected.value("answered", false) && actual.value("answered", false)) { ++reference_silent; cases.push_back({{"case", what}, {"result", "reference did not answer"}}); return; }
        Json found = Json::array();
        Json e = expected, a = actual;
        e.erase("order"); a.erase("order");  // node order is compared apart
        differences(e, a, "", found);
        if (found.empty()) ++same; else ++different;
        Json entry{{"case", what}, {"result", found.empty() ? "same" : "different"}};
        if (!found.empty()) entry["differences"] = found;
        if (expected.contains("order") && actual.contains("order") && expected.at("order") != actual.at("order") && found.empty()) entry["node_order"] = "differs";
        cases.push_back(entry);
    };
    std::mt19937 random(options.seed);
    for (unsigned i = 0; i < options.ledgers; ++i) {
        std::uint32_t seq = newest - 4 - i * std::max(1u, options.spacing);
        // The ledger's hash, from the subject, then everything asked of both by hash.
        protocol::TMGetLedger base; base.set_itype(protocol::liBASE); base.set_ledgerseq(seq);
        auto subject_base = subject.ledger_data(base, timeout);
        if (!subject_base || subject_base->nodes_size() == 0) { cases.push_back({{"case", "ledger " + std::to_string(seq)}, {"result", "subject did not answer"}}); continue; }
        auto header = decode_header(subject_base->nodes(0).nodedata());
        auto hash = from_hex(header.hash);
        auto both = [&](std::string const& what, protocol::TMGetLedger request) {
            compare(what, ledger_data_json(reference.ledger_data(request, timeout)), ledger_data_json(subject.ledger_data(request, timeout)));
        };
        auto label = [&](std::string const& what) { return std::to_string(seq) + " " + what; };
        protocol::TMGetLedger by_hash; by_hash.set_itype(protocol::liBASE); by_hash.set_ledgerhash(hash);
        both(label("header by hash"), by_hash);
        both(label("header by sequence"), base);
        // Tree nodes at several depths: the transaction tree's root, and the state tree's root
        // and some of its children.
        auto tree = [&](protocol::TMLedgerInfoType type, std::string const& id, unsigned depth) {
            protocol::TMGetLedger request; request.set_itype(type); request.set_ledgerhash(hash); request.add_nodeids(id); request.set_querydepth(depth);
            return request;
        };
        auto root = r::SHAMapNodeID().getRawString();
        if (header.transaction_hash != std::string(64, '0'))
            for (unsigned depth : {0u, 1u, 3u}) both(label("transaction tree root, depth " + std::to_string(depth)), tree(protocol::liTX_NODE, root, depth));
        for (unsigned depth : {0u, 1u, 2u}) both(label("state tree root, depth " + std::to_string(depth)), tree(protocol::liAS_NODE, root, depth));
        auto top = reference.ledger_data(tree(protocol::liAS_NODE, root, 1), timeout);
        std::vector<std::pair<std::string, std::string>> children;  // node ID, wire
        if (top) for (auto const& node : top->nodes()) if (node.nodeid() != root) children.push_back({node.nodeid(), node.nodedata()});
        std::shuffle(children.begin(), children.end(), random);
        for (std::size_t c = 0; c < std::min<std::size_t>(children.size(), 3); ++c) {
            both(label("state node " + to_hex(children[c].first).substr(0, 8) + ", depth 0"), tree(protocol::liAS_NODE, children[c].first, 0));
            both(label("state node " + to_hex(children[c].first).substr(0, 8) + ", depth 1"), tree(protocol::liAS_NODE, children[c].first, 1));
        }
        // Several nodes in one request, one of them unknown.
        {
            auto request = tree(protocol::liAS_NODE, root, 0);
            for (std::size_t c = 0; c < std::min<std::size_t>(children.size(), 4); ++c) request.add_nodeids(children[c].first);
            both(label("state nodes, several in one request"), request);
        }
        // The same nodes by hash (node-store format), with one hash nobody has.
        protocol::TMGetObjectByHash query;
        query.set_type(protocol::TMGetObjectByHash::otSTATE_NODE); query.set_query(true); query.set_ledgerhash(hash);
        for (std::size_t c = 0; c < std::min<std::size_t>(children.size(), 4); ++c) {
            auto* object = query.add_objects();
            auto h = node_hash(children[c].second);
            object->set_hash(std::string(reinterpret_cast<char const*>(h.data()), h.size()));
            object->set_ledgerseq(seq);
        }
        query.add_objects()->set_hash(std::string(32, '\x7f'));
        compare(label("state nodes by hash"), objects_json(reference.objects(query, timeout)), objects_json(subject.objects(query, timeout)));
    }
    // A ledger neither holds: xrpld sends no answer at all, and so must XRPLA.
    {
        protocol::TMGetLedger unknown; unknown.set_itype(protocol::liBASE); unknown.set_ledgerhash(std::string(32, '\x5a'));
        auto expected = ledger_data_json(reference.ledger_data(unknown, std::chrono::seconds(3)));
        auto actual = ledger_data_json(subject.ledger_data(unknown, std::chrono::seconds(3)));
        Json found = Json::array();
        differences(expected, actual, "", found);
        (found.empty() ? same : different) += 1;
        Json entry{{"case", "unknown ledger"}, {"result", found.empty() ? "same" : "different"}};
        if (!found.empty()) entry["differences"] = found;
        cases.push_back(entry);
    }
    result["ledger_replay"] = {{"xrpld", reference.session->info().ledger_replay}, {"xrpla", subject.session->info().ledger_replay}};
    result["newest"] = newest;
    result["same"] = same; result["different"] = different; result["reference_silent"] = reference_silent;
    result["cases"] = cases;
    return result;
}
}
