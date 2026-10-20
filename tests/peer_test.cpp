// Serving the peer network (docs/PEER-SERVING.md): an XRPLA peer fetches whole ledgers from an
// XRPLA listener (PeerLedgers checks every node against the hash its parent commits to), and
// what is not served is answered as xrpld answers it.
#include "core_fixture.hpp"
#include "../core/src/peer.hpp"
#include "../core/src/peer_ledgers.hpp"
#include "../core/src/peer_relay.hpp"
#include "../core/src/peer_serving.hpp"
#include "../core/src/validators.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/basics/chrono.h>
#include <xrpl/json/json_writer.h>
#include <xrpl/proto/xrpl.pb.h>
#include <xrpl/protocol/HashPrefix.h>
#include <xrpl/protocol/STValidation.h>
#include <xrpl/protocol/SecretKey.h>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/digest.h>
#include <xrpl/shamap/SHAMapNodeID.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>

using namespace xrpla;
using namespace xrpla::core;
namespace {
namespace net = boost::asio;
namespace http = boost::beast::http;
// A verifier holding a 40-ledger chain whose newest `window` ledgers are served.
struct Served {
    fs::path dir = fs::temp_directory_path() / ("xrpla-serving-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain{40};
    std::size_t window = 16;
    std::unique_ptr<LedgerVerifier> verifier;
    std::shared_ptr<LedgerServer> server;
    Served() {
        verifier = std::make_unique<LedgerVerifier>(dir);
        verifier->keep_ledgers(window);
        chain.seed(*verifier);
        for (auto const& ledger : chain.ledgers) verifier->apply(ledger);
        verifier->persist();
        verifier->compact_nodes();  // what is served survives compaction
        server = std::make_shared<LedgerServer>(window);
        server->node_store(verifier->node_store());
        for (auto const& ledger : chain.ledgers) server->add(decode_header(ledger.header));
    }
    ~Served() { server.reset(); verifier.reset(); fs::remove_all(dir); }
    Header header(std::size_t i) const { return decode_header(chain.ledgers[i].header); }
};
// A listener serving `served`, keeping what it accepts (greeted with this node's status).
struct Listening {
    std::mutex mutex;
    std::vector<std::unique_ptr<PeerSession>> sessions;
    bool admit = true;
    std::unique_ptr<PeerListener> listener;
    Listening(Served& served, NodeIdentity identity = NodeIdentity::ephemeral()) {
        PeerLocal local{std::move(identity), 0, false, true, served.server, {}};
        PeerListener::Handlers handlers;
        handlers.admit = [this](PeerSession::Info const&, std::string const&) { return admit; };
        handlers.accepted = [this, &served](std::unique_ptr<PeerSession> session, std::string const&) {
            served.server->greet(*session);
            std::lock_guard lock(mutex);
            sessions.push_back(std::move(session));
        };
        handlers.redirect = [] { return std::vector<std::string>{"192.0.2.1:51235", "192.0.2.2:51235"}; };
        listener = std::make_unique<PeerListener>("127.0.0.1:0", local, std::set<int>{}, handlers);
    }
    std::string endpoint() const { return "127.0.0.1:" + std::to_string(listener->port()); }
};
std::optional<PeerSession::Message> await(PeerSession& session, int type, std::chrono::milliseconds limit) {
    auto until = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < until)
        if (auto message = session.receive(std::chrono::milliseconds(100)); message && message->type == type) return message;
    return std::nullopt;
}
std::pair<int, Json> https_get(std::uint16_t port, std::string const& target) {
    net::io_context io;
    net::ssl::context tls(net::ssl::context::tls_client);
    tls.set_verify_mode(net::ssl::verify_none);
    net::ssl::stream<net::ip::tcp::socket> stream(io, tls);
    stream.next_layer().connect({net::ip::make_address("127.0.0.1"), port});
    stream.handshake(net::ssl::stream_base::client);
    http::request<http::empty_body> request{http::verb::get, target, 11};
    request.set(http::field::host, "localhost");
    http::write(stream, request);
    boost::beast::flat_buffer buffer;
    http::response<http::string_body> response;
    http::read(stream, buffer, response);
    return {response.result_int(), response.body().empty() ? Json() : Json::parse(response.body())};
}
}

namespace {
// Ledgers that add, change and delete accounts (no transactions), so the state tree changes
// shape: branches appear, and inner nodes fold back into single leaves.
struct Mutations {
    std::vector<ObjectRecord> snapshot;
    Bytes snapshot_header;
    std::vector<LedgerRecord> ledgers;
    Mutations() {
        namespace r = ::xrpl;
        fixture::Family family;
        r::SHAMap state(r::SHAMapType::STATE, family);
        state.setUnbacked();
        auto account = [](unsigned n, std::int64_t drops) {
            r::AccountID id{n};
            r::STLedgerEntry a(r::keylet::account(id));
            a.setAccountID(r::sfAccount, id);
            a.setFieldU32(r::sfSequence, 1);
            a.setFieldAmount(r::sfBalance, r::STAmount(r::XRPAmount{drops}));
            return ObjectRecord{fixture::raw(a.key()), fixture::serialized(a)};
        };
        std::map<Bytes, Bytes> start;
        for (unsigned n = 1; n <= 40; ++n) { auto o = account(n, 1000000); start[o.key] = o.object; }
        for (auto const& [key, object] : start) {
            snapshot.push_back({key, object});
            state.addItem(r::SHAMapNodeType::TnAccountState, r::makeShamapitem(r::uint256::fromVoid(key.data()), {object.data(), object.size()}));
        }
        r::LedgerHeader header{};
        header.seq = 65535;
        header.drops = r::XRPAmount{1000000000};
        header.closeTime = r::NetClock::time_point{r::NetClock::duration{1000}};
        header.closeTimeResolution = r::NetClock::duration{10};
        header.accountHash = state.getHash().asUInt256();
        snapshot_header = fixture::serialized(header);
        // Each ledger's (account, drops) to set; 0 drops deletes the account.
        std::vector<std::vector<std::pair<unsigned, std::int64_t>>> steps = {
            {{3, 999}, {41, 5}, {42, 6}, {43, 7}, {7, 0}, {9, 0}},
            {{1, 0}, {2, 0}, {4, 0}, {5, 0}, {6, 0}, {8, 0}, {10, 0}, {11, 0}, {12, 0}, {13, 0}, {14, 0}, {15, 0}, {16, 0}, {17, 0}, {18, 0}},
            {{100, 1}, {101, 2}, {102, 3}, {103, 4}, {104, 5}},
            {{3, 0}, {41, 0}, {100, 9}, {19, 0}, {20, 0}, {21, 0}, {22, 0}, {23, 0}, {24, 0}, {25, 0}, {26, 0}, {27, 0}, {28, 0}, {29, 0}, {30, 0}},
            {{31, 7}},
        };
        for (std::size_t i = 0; i < steps.size(); ++i) {
            LedgerRecord ledger{};
            ledger.sequence = static_cast<std::uint32_t>(65536 + i);
            for (auto const& [n, drops] : steps[i]) {
                auto object = account(n, drops ? drops : 1);
                auto key = r::uint256::fromVoid(object.key.data());
                if (!drops) { ledger.patches.push_back({object.key, {}}); state.delItem(key); continue; }
                ledger.patches.push_back(object);
                auto item = r::makeShamapitem(key, {object.object.data(), object.object.size()});
                if (state.peekItem(key)) state.updateGiveItem(r::SHAMapNodeType::TnAccountState, item);
                else state.addItem(r::SHAMapNodeType::TnAccountState, item);
            }
            std::sort(ledger.patches.begin(), ledger.patches.end(), [](auto const& a, auto const& b) { return a.key < b.key; });
            header.parentHash = r::calculateLedgerHash(header);
            header.seq = ledger.sequence;
            header.parentCloseTime = header.closeTime;
            header.closeTime += r::NetClock::duration{10};
            header.accountHash = state.getHash().asUInt256();
            header.txHash = r::uint256{};
            ledger.header = fixture::serialized(header);
            ledgers.push_back(std::move(ledger));
        }
    }
    void seed(LedgerVerifier& verifier) const {
        for (auto const& item : snapshot) verifier.add_snapshot_object(item);
        auto header = decode_header(snapshot_header);
        verifier.finish_snapshot(snapshot_header, header.sequence, header.account_hash);
    }
};
}

// The safety net for a change no metadata names: comparing a ledger's state tree with the
// verified parent's finds exactly the ledger's changes, deletions included.
TEST(PeerServing, StateChangesFromTheTreeAreTheLedgersChanges) {
    Mutations chain;
    auto dir = fs::temp_directory_path() / ("xrpla-tree-diff-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    {
        LedgerVerifier served(dir);
        served.keep_ledgers(8);
        chain.seed(served);
        for (auto const& ledger : chain.ledgers) served.apply(ledger);
        served.persist();
        auto server = std::make_shared<LedgerServer>(8);
        server->node_store(served.node_store());
        for (auto const& ledger : chain.ledgers) server->add(decode_header(ledger.header));
        ASSERT_EQ(server->range()->second - server->range()->first + 1, chain.ledgers.size());
        std::vector<std::unique_ptr<PeerSession>> sessions;
        std::mutex mutex;
        PeerListener::Handlers handlers;
        handlers.admit = [](PeerSession::Info const&, std::string const&) { return true; };
        handlers.accepted = [&](std::unique_ptr<PeerSession> session, std::string const&) { std::lock_guard lock(mutex); sessions.push_back(std::move(session)); };
        PeerListener listener("127.0.0.1:0", PeerLocal{NodeIdentity::ephemeral(), 0, false, true, server, {}}, {}, handlers);
        PeerLedgers::Options options;
        options.peers = {"127.0.0.1:" + std::to_string(listener.port())};
        options.connections = 1;
        PeerLedgers peers(options, {}, {});
        LedgerVerifier local;  // the follower's verified parent
        chain.seed(local);
        for (auto const& ledger : chain.ledgers) {
            auto changes = peers.changes(decode_header(ledger.header), local);
            ASSERT_EQ(changes.size(), ledger.patches.size()) << ledger.sequence;
            for (std::size_t n = 0; n < changes.size(); ++n) {
                EXPECT_EQ(changes[n].key, ledger.patches[n].key) << ledger.sequence;
                EXPECT_EQ(changes[n].object, ledger.patches[n].object) << ledger.sequence;
            }
            local.apply(ledger);
        }
        server->node_store(nullptr);
    }
    fs::remove_all(dir);
}

TEST(PeerServing, APeerFetchesWholeLedgersInTheWindow) {
    Served served;
    Listening listening(served);
    PeerLedgers::Options options;
    options.peers = {listening.endpoint()};
    options.connections = 1;
    PeerLedgers peers(options, {}, {});
    auto range = served.server->range();
    ASSERT_TRUE(range);
    EXPECT_EQ(range->first, served.header(40 - 16).sequence);
    EXPECT_EQ(range->second, served.header(39).sequence);
    for (std::size_t i = 40 - 16; i < 40; i += 5) {
        auto expected = served.header(i);
        auto header = peers.header(from_hex(expected.hash));
        EXPECT_EQ(header.raw, expected.raw);
        auto const& ledger = served.chain.ledgers[i];
        auto transactions = peers.transactions(header);
        ASSERT_EQ(transactions.size(), ledger.transactions.size());
        EXPECT_EQ(transactions[0].transaction, ledger.transactions[0].transaction);
        EXPECT_EQ(transactions[0].metadata, ledger.transactions[0].metadata);
        // The object this ledger changed, as it holds it, and one it lacks.
        Bytes absent(32, '\x7f');
        auto values = peers.objects(header, {ledger.patches[0].key, absent});
        EXPECT_EQ(values[0], ledger.patches[0].object);
        EXPECT_TRUE(values[1].empty());
    }
    EXPECT_GT(served.server->status().at("answered").get<std::uint64_t>(), 0u);
}

TEST(PeerServing, AnswersAsXrpldForWhatItLacks) {
    Served served;
    Listening listening(served);
    PeerSession client(listening.endpoint(), NodeIdentity::ephemeral(), 0,
                       {protocol::mtLEDGER_DATA, protocol::mtGET_OBJECTS, protocol::mtSTATUS_CHANGE, protocol::mtREPLAY_DELTA_RESPONSE});
    // Greeted with the ledger served newest and the range, as xrpld's peers need to prefer it.
    auto greeting = await(client, protocol::mtSTATUS_CHANGE, std::chrono::seconds(5));
    ASSERT_TRUE(greeting);
    protocol::TMStatusChange status;
    ASSERT_TRUE(status.ParseFromString(greeting->payload));
    EXPECT_EQ(status.newevent(), protocol::neACCEPTED_LEDGER);
    EXPECT_EQ(status.ledgerseq(), served.header(39).sequence);
    EXPECT_EQ(status.ledgerhash(), from_hex(served.header(39).hash));
    EXPECT_EQ(status.ledgerhashprevious(), from_hex(served.header(39).parent_hash));
    EXPECT_EQ(status.firstseq(), served.header(24).sequence);
    EXPECT_EQ(status.lastseq(), served.header(39).sequence);
    // A ledger before the window: no reply at all. An object it lacks: an empty reply.
    protocol::TMGetLedger old;
    old.set_itype(protocol::liBASE); old.set_ledgerhash(from_hex(served.header(3).hash)); old.set_ledgerseq(served.header(3).sequence); old.set_requestcookie(7);
    client.send(protocol::mtGET_LEDGER, old);
    // Nor for a ledger newer than the newest served, a consensus transaction set, or
    // transactions by hash (transaction relay).
    protocol::TMGetLedger ahead, candidate;
    ahead.set_itype(protocol::liBASE); ahead.set_ledgerhash(std::string(32, 'n')); ahead.set_ledgerseq(served.header(39).sequence + 1); ahead.set_requestcookie(9);
    client.send(protocol::mtGET_LEDGER, ahead);
    candidate.set_itype(protocol::liTS_CANDIDATE); candidate.set_ledgerhash(std::string(32, 's'));
    client.send(protocol::mtGET_LEDGER, candidate);
    protocol::TMGetObjectByHash relay;
    relay.set_type(protocol::TMGetObjectByHash::otTRANSACTIONS); relay.set_query(true);
    relay.add_objects()->set_hash(std::string(32, 't'));
    client.send(protocol::mtGET_OBJECTS, relay);
    protocol::TMGetObjectByHash query;
    query.set_type(protocol::TMGetObjectByHash::otSTATE_NODE); query.set_query(true);
    query.add_objects()->set_hash(std::string(32, 'x'));
    client.send(protocol::mtGET_OBJECTS, query);
    // The newest ledger's base: its header, then its state and transaction tree roots.
    protocol::TMGetLedger base;
    base.set_itype(protocol::liBASE); base.set_ledgerhash(from_hex(served.header(39).hash)); base.set_requestcookie(8);
    client.send(protocol::mtGET_LEDGER, base);
    // Replay delta without having negotiated ledger replay: charged, not answered.
    protocol::TMReplayDeltaRequest delta;
    delta.set_ledgerhash(from_hex(served.header(39).hash));
    client.send(protocol::mtREPLAY_DELTA_REQ, delta);
    std::vector<protocol::TMLedgerData> ledgers;
    std::vector<protocol::TMGetObjectByHash> objects;
    int deltas = 0;
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < until) {
        auto message = client.receive(std::chrono::milliseconds(100));
        if (!message) continue;
        if (message->type == protocol::mtLEDGER_DATA) { ledgers.emplace_back(); ASSERT_TRUE(ledgers.back().ParseFromString(message->payload)); }
        if (message->type == protocol::mtGET_OBJECTS) { objects.emplace_back(); ASSERT_TRUE(objects.back().ParseFromString(message->payload)); }
        if (message->type == protocol::mtREPLAY_DELTA_RESPONSE) ++deltas;
    }
    ASSERT_EQ(ledgers.size(), 1u);
    EXPECT_EQ(ledgers[0].requestcookie(), 8u);
    EXPECT_FALSE(ledgers[0].has_error());
    ASSERT_EQ(ledgers[0].nodes_size(), 3);
    EXPECT_EQ(ledgers[0].nodes(0).nodedata(), served.header(39).raw);
    ASSERT_EQ(objects.size(), 1u);
    EXPECT_FALSE(objects[0].query());
    EXPECT_EQ(objects[0].objects_size(), 0);
    EXPECT_EQ(deltas, 0);
    EXPECT_TRUE(client.open());
    // What went unanswered, by kind.
    EXPECT_EQ(served.server->status().at("unanswered_kinds"), (Json{{"ledger_newer", 1}, {"ledger_older", 1}, {"transactions", 1}, {"tx_set", 1}}));
}

TEST(PeerServing, ServesObjectsByHashAndLedgerReplay) {
    Served served;
    Listening listening(served);
    PeerLocal local{NodeIdentity::ephemeral(), 0, false, true, serving_nothing(), {}};
    PeerSession client(listening.endpoint(), local, {protocol::mtGET_OBJECTS, protocol::mtREPLAY_DELTA_RESPONSE, protocol::mtPROOF_PATH_RESPONSE});
    ASSERT_TRUE(client.info().ledger_replay);
    auto newest = served.header(39);
    // A header by its hash (the node-store form: "LWR\0" and the 118 bytes) and the state root.
    protocol::TMGetObjectByHash query;
    query.set_type(protocol::TMGetObjectByHash::otLEDGER); query.set_query(true);
    query.add_objects()->set_hash(from_hex(newest.hash));
    auto* root = query.add_objects();
    root->set_hash(from_hex(newest.account_hash)); root->set_ledgerseq(newest.sequence);
    client.send(protocol::mtGET_OBJECTS, query);
    auto reply = await(client, protocol::mtGET_OBJECTS, std::chrono::seconds(5));
    ASSERT_TRUE(reply);
    protocol::TMGetObjectByHash objects;
    ASSERT_TRUE(objects.ParseFromString(reply->payload));
    ASSERT_EQ(objects.objects_size(), 2);
    EXPECT_EQ(objects.objects(0).data(), std::string("LWR\0", 4) + newest.raw);
    EXPECT_EQ(objects.objects(1).data().substr(0, 4), std::string("MIN\0", 4));
    // A replay delta: the header and each transaction (tx and metadata, as tree leaves hold them).
    protocol::TMReplayDeltaRequest delta;
    delta.set_ledgerhash(from_hex(newest.hash));
    client.send(protocol::mtREPLAY_DELTA_REQ, delta);
    auto answer = await(client, protocol::mtREPLAY_DELTA_RESPONSE, std::chrono::seconds(5));
    ASSERT_TRUE(answer);
    protocol::TMReplayDeltaResponse response;
    ASSERT_TRUE(response.ParseFromString(answer->payload));
    EXPECT_EQ(response.ledgerheader(), newest.raw);
    ASSERT_EQ(response.transaction_size(), 1);
    // A proof path to the skip list... this chain has none: to the changed account instead.
    protocol::TMProofPathRequest proof;
    proof.set_key(served.chain.ledgers[39].patches[0].key);
    proof.set_ledgerhash(from_hex(newest.hash));
    proof.set_type(protocol::lmACCOUNT_STATE);
    client.send(protocol::mtPROOF_PATH_REQ, proof);
    auto path = await(client, protocol::mtPROOF_PATH_RESPONSE, std::chrono::seconds(5));
    ASSERT_TRUE(path);
    protocol::TMProofPathResponse proven;
    ASSERT_TRUE(proven.ParseFromString(path->payload));
    std::vector<::xrpl::Blob> nodes;
    for (auto const& node : proven.path()) nodes.emplace_back(node.begin(), node.end());
    EXPECT_TRUE(::xrpl::SHAMap::verifyProofPath(::xrpl::uint256::fromVoid(from_hex(newest.account_hash).data()),
                                                ::xrpl::uint256::fromVoid(proof.key().data()), nodes));
}

TEST(PeerServing, ReleasesTheStoreForTheFollowerToReopen) {
    // After a failed batch the follower drops its verifier and opens the store again: the
    // server must hold no reference to it by then (RocksDB allows one opener per process).
    Served served;
    Listening listening(served);
    PeerLedgers::Options options;
    options.peers = {listening.endpoint()};
    options.connections = 1;
    PeerLedgers peers(options, {}, {});
    EXPECT_EQ(peers.header(from_hex(served.header(39).hash)).raw, served.header(39).raw);
    served.server->node_store(nullptr);
    served.verifier.reset();
    LedgerVerifier reopened(served.dir);
    reopened.resume(served.chain.ledgers.back().header);
    served.server->node_store(reopened.node_store());
    auto header = peers.header(from_hex(served.header(39).hash));
    EXPECT_EQ(peers.transactions(header).size(), 1u);
    served.server->node_store(nullptr);
}

TEST(PeerServing, ReservedPeersPassFullSlots) {
    Served served;
    PeerLedgers::Options options;
    options.peers = {"127.0.0.1:1"};
    options.connections = 1;
    PeerLedgers peers(options, {}, {});
    auto reserved = NodeIdentity::ephemeral();
    PeerOverlay overlay({"127.0.0.1:0", 1, false, 0, {reserved.base58()}}, peers, served.server, {}, {});
    auto endpoint = "127.0.0.1:" + std::to_string(overlay.port());
    PeerSession first(endpoint, NodeIdentity::ephemeral(), 0, {protocol::mtSTATUS_CHANGE, protocol::mtLEDGER_DATA});
    ASSERT_TRUE(await(first, protocol::mtSTATUS_CHANGE, std::chrono::seconds(5)));  // listed by now
    EXPECT_THROW(PeerSession(endpoint, NodeIdentity::ephemeral(), 0, {}), PeerRedirect);  // one slot, taken
    PeerSession kept(endpoint, reserved, 0, {protocol::mtSTATUS_CHANGE, protocol::mtLEDGER_DATA});
    ASSERT_TRUE(await(kept, protocol::mtSTATUS_CHANGE, std::chrono::seconds(5)));
    EXPECT_EQ(overlay.status().at("inbound").size(), 2u);
    // What reserved peers ask for is also counted apart: both peers ask for the newest ledger's
    // base, the reserved one also for a ledger before the window (left unanswered).
    protocol::TMGetLedger base, old;
    base.set_itype(protocol::liBASE); base.set_ledgerhash(from_hex(served.header(39).hash)); base.set_requestcookie(1);
    old.set_itype(protocol::liBASE); old.set_ledgerhash(from_hex(served.header(3).hash)); old.set_requestcookie(2);
    kept.send(protocol::mtGET_LEDGER, old);
    kept.send(protocol::mtGET_LEDGER, base);
    first.send(protocol::mtGET_LEDGER, base);
    ASSERT_TRUE(await(kept, protocol::mtLEDGER_DATA, std::chrono::seconds(5)));
    ASSERT_TRUE(await(first, protocol::mtLEDGER_DATA, std::chrono::seconds(5)));
    auto status = served.server->status();
    for (auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5); status.at("unanswered") != 1u && std::chrono::steady_clock::now() < until;
         status = served.server->status())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(status.at("requests"), 3u);
    EXPECT_EQ(status.at("answered"), 2u);
    EXPECT_EQ(status.at("unanswered"), 1u);
    auto const& own = status.at("reserved");
    EXPECT_EQ(own.at("requests"), 2u);
    EXPECT_EQ(own.at("answered"), 1u);
    EXPECT_EQ(own.at("unanswered"), 1u);
    EXPECT_EQ(own.at("unanswered_kinds"), (Json{{"ledger_unknown", 1}}));  // asked for by hash alone
    EXPECT_EQ(status.at("unanswered_kinds"), own.at("unanswered_kinds"));
    EXPECT_EQ(own.at("nodes_sent").get<std::uint64_t>() * 2, status.at("nodes_sent").get<std::uint64_t>());  // the same reply to each
    EXPECT_EQ(own.at("bytes_sent").get<std::uint64_t>() * 2, status.at("bytes_sent").get<std::uint64_t>());
    EXPECT_FALSE(LedgerServer(4).status().contains("reserved"));  // none configured
}

// Ledgers asked for by hash before they are served (the network's newest, which this node has a
// few seconds later) are told apart from ledgers it never serves.
TEST(PeerServing, CountsLedgersServedAfterBeingAskedFor) {
    Served served;
    auto server = std::make_shared<LedgerServer>(served.window);
    server->node_store(served.verifier->node_store());
    for (std::size_t i = 40 - 16; i < 39; ++i) server->add(served.header(i));  // all but the newest
    server->reserve({"nReserved"});
    auto ask = [&](std::string const& key, std::string const& hash, std::uint32_t seq) {
        protocol::TMGetLedger request;
        request.set_itype(protocol::liBASE); request.set_ledgerhash(hash); request.set_requestcookie(1);
        if (seq) request.set_ledgerseq(seq);
        server->request(protocol::mtGET_LEDGER, request.SerializeAsString(), PeerRequester{[](int, std::string const&) {}, [](std::string const&) {}, key, "192.0.2.9:51235"});
    };
    ask("nPeer", from_hex(served.header(39).hash), served.header(39).sequence);  // the next ledger, not served yet
    ask("nPeer", std::string(32, 'u'), 0);                                          // one never served
    ask("nPeer", from_hex(served.header(3).hash), served.header(3).sequence);       // before the window: left out
    ask("nReserved", std::string(32, 'r'), 0);                                      // a reserved peer's: left out
    auto status = server->status();
    for (auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5); status.at("unanswered") != 4u && std::chrono::steady_clock::now() < until;
         status = server->status())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(status.at("unserved_ledgers").at("asked"), 2u);
    EXPECT_EQ(status.at("unserved_ledgers").at("pending"), 2u);
    server->add(served.header(39));
    EXPECT_EQ(server->status().at("unserved_ledgers"),
              (Json{{"asked", 2}, {"served_after", {{"0-5s", 1}, {"5-15s", 0}, {"15-60s", 0}, {"60s+", 0}}}, {"not_served", 0}, {"pending", 1}}));
    server->node_store(nullptr);
}

// A reply starts no node past xrpld's 8,192 and holds none past its 12,288 (an xrpld charges a
// larger one as invalid data), however many nodes are asked for.
TEST(PeerServing, RepliesStayWithinXrpldsNodeLimits) {
    Served served;
    Listening listening(served);
    PeerSession client(listening.endpoint(), NodeIdentity::ephemeral(), 0, {protocol::mtLEDGER_DATA});
    protocol::TMGetLedger request;
    request.set_itype(protocol::liAS_NODE);
    request.set_ledgerhash(from_hex(served.header(39).hash));
    request.set_querydepth(3);
    for (int i = 0; i < 12288; ++i) request.add_nodeids(::xrpl::SHAMapNodeID().getRawString());
    client.send(protocol::mtGET_LEDGER, request);
    auto reply = await(client, protocol::mtLEDGER_DATA, std::chrono::seconds(10));
    ASSERT_TRUE(reply);
    protocol::TMLedgerData data;
    ASSERT_TRUE(data.ParseFromString(reply->payload));
    EXPECT_GE(data.nodes_size(), 8192);
    EXPECT_LE(data.nodes_size(), 12288);
}

// Charges are kept by address, as xrpld keeps them: a peer dropped for them leaves its balance
// behind, so another session from its address is refused until the balance decays, and is
// dropped at its first request if it gets in.
TEST(PeerServing, ChargesStayWithTheAddressAfterADrop) {
    LedgerServer server(4);
    std::atomic<int> drops{0};
    auto peer = [&](std::string key, std::string address, std::uint64_t session) {
        PeerRequester from{[](int, std::string const&) {}, [&](std::string const&) { ++drops; }, std::move(key), address + ":51235"};
        from.address = std::move(address); from.session = session;
        return from;
    };
    EXPECT_TRUE(server.admits("192.0.2.7"));
    server.charge(peer("nFirst", "192.0.2.7", 1), 25000.0 * 32);
    EXPECT_EQ(drops.load(), 1);
    EXPECT_FALSE(server.admits("192.0.2.7"));
    EXPECT_TRUE(server.admits("192.0.2.8"));
    server.charge(peer("nSecond", "192.0.2.7", 2), 1);  // another node key, the same address
    EXPECT_EQ(drops.load(), 2);
    server.charge(peer("nThird", "192.0.2.8", 3), 1);
    EXPECT_EQ(drops.load(), 2);
    EXPECT_EQ(server.status().at("peers_dropped"), 2u);
    // An IPv6 peer's balance is its /64's: the next address in it is refused too.
    server.charge(peer("nFourth", "2600:1f18:1:2::7", 4), 25000.0 * 32);
    EXPECT_EQ(drops.load(), 3);
    EXPECT_FALSE(server.admits("2600:1f18:1:2::8"));
    EXPECT_TRUE(server.admits("2600:1f18:1:3::7"));
}

TEST(PeerServing, RefusesAsXrpldRefuses) {
    Served served;
    auto identity = NodeIdentity::ephemeral();
    Listening listening(served, identity);
    // Its own node key: a self connection.
    EXPECT_THROW(PeerSession(listening.endpoint(), identity, 0, {}), Error);
    // Another network.
    EXPECT_THROW(PeerSession(listening.endpoint(), NodeIdentity::ephemeral(), 1, {}), Error);
    // Full: redirected to peers it knows.
    listening.admit = false;
    try {
        PeerSession refused(listening.endpoint(), NodeIdentity::ephemeral(), 0, {});
        FAIL() << "a full listener accepted a peer";
    } catch (PeerRedirect const& redirect) {
        EXPECT_EQ(redirect.peers, (std::vector<std::string>{"192.0.2.1:51235", "192.0.2.2:51235"}));
    }
}

TEST(PeerServing, OverlayAnswersCrawlAndHealthAndAcceptsPeers) {
    Served served;
    PeerLedgers::Options options;
    options.peers = {"127.0.0.1:1"};  // nothing there: this node has no outbound peers
    options.connections = 1;
    PeerLedgers peers(options, {}, {});
    PeerOverlay overlay({"127.0.0.1:0", 2, true, 0, {}}, peers, served.server, {}, {});
    auto [crawl_status, crawl] = https_get(overlay.port(), "/crawl");
    EXPECT_EQ(crawl_status, 200);
    EXPECT_EQ(crawl.at("version"), 2);
    EXPECT_EQ(crawl.at("server").at("build_version"), "xrpla-" XRPLA_VERSION);
    EXPECT_EQ(crawl.at("server").at("complete_ledgers"), std::to_string(served.header(24).sequence) + "-" + std::to_string(served.header(39).sequence));
    EXPECT_EQ(crawl.at("server").at("pubkey_node"), peers.identity().base58());
    auto [health_status, health] = https_get(overlay.port(), "/health");
    EXPECT_EQ(health_status, 500);  // no peers, and the fixture's newest ledger is from 2000
    EXPECT_EQ(health.at("info").at("peers"), 0);
    EXPECT_EQ(https_get(overlay.port(), "/other").first, 404);
    // An inbound peer is greeted with this node's status and listed.
    PeerSession client("127.0.0.1:" + std::to_string(overlay.port()), NodeIdentity::ephemeral(), 0, {protocol::mtSTATUS_CHANGE});
    EXPECT_TRUE(await(client, protocol::mtSTATUS_CHANGE, std::chrono::seconds(5)));
    EXPECT_EQ(overlay.status().at("inbound").size(), 1u);
    overlay.announce();
    EXPECT_TRUE(await(client, protocol::mtSTATUS_CHANGE, std::chrono::seconds(5)));
    EXPECT_EQ(https_get(overlay.port(), "/crawl").second.at("overlay").at("active").size(), 1u);
}

namespace {
namespace xr = ::xrpl;
using namespace std::chrono_literals;
struct Validator {
    xr::PublicKey key;
    xr::SecretKey secret;
    Bytes raw() const { return Bytes(reinterpret_cast<char const*>(key.data()), key.size()); }
};
Validator validator() { auto [key, secret] = xr::randomKeyPair(xr::KeyType::Secp256k1); return {key, secret}; }
// A validation of ledger `seq`, signed `age` seconds ago.
protocol::TMValidation validation(Validator const& v, std::uint32_t seq, std::int64_t age = 0) {
    auto signed_at = xr::NetClock::time_point{xr::NetClock::duration{static_cast<std::uint32_t>(xrpl_seconds_now() - age)}};
    xr::STValidation built(signed_at, v.key, v.secret, xr::calcNodeID(v.key), [&](xr::STValidation& val) {
        val.setFieldH256(xr::sfLedgerHash, xr::sha512Half(seq));
        val.setFieldU32(xr::sfLedgerSequence, seq);
    });
    xr::Serializer s;
    built.add(s);
    protocol::TMValidation message;
    message.set_validation(s.data(), s.size());
    return message;
}
// A proposal for the round after ledger `seq`, its close time `age` seconds ago.
protocol::TMProposeSet proposal(Validator const& v, std::uint32_t seq, std::int64_t age = 0) {
    auto position = xr::sha512Half(seq, std::uint32_t(1)), previous = xr::sha512Half(seq);
    auto close = static_cast<std::uint32_t>(xrpl_seconds_now() - age);
    auto signature = xr::signDigest(v.key, v.secret, xr::sha512Half(xr::HashPrefix::Proposal, std::uint32_t(0), close, previous, position));
    protocol::TMProposeSet set;
    set.set_proposeseq(0);
    set.set_currenttxhash(position.data(), position.size());
    set.set_previousledger(previous.data(), previous.size());
    set.set_closetime(close);
    set.set_nodepubkey(v.key.data(), v.key.size());
    set.set_signature(signature.data(), signature.size());
    return set;
}
// A serving node relaying among `count` inbound peers (the clients).
struct Relaying {
    Served served;
    std::mutex mutex;
    std::set<Bytes> trusted;
    std::atomic<PeerOverlay*> target{nullptr};
    std::shared_ptr<PeerRelay> relay;
    std::unique_ptr<PeerLedgers> peers;
    std::unique_ptr<PeerOverlay> overlay;
    std::vector<std::unique_ptr<PeerSession>> clients;
    explicit Relaying(unsigned count, PeerRelay::Options options = {}) {
        relay = std::make_shared<PeerRelay>(options, [this](Bytes const& key) { std::lock_guard lock(mutex); return trusted.contains(key); },
                                            [this](PeerRelay::Visit const& visit) { if (auto in = target.load()) in->visit(visit); });
        PeerLedgers::Options outbound;
        outbound.peers = {"127.0.0.1:1"};  // nothing there: only inbound peers
        outbound.connections = 1;
        outbound.relay = relay;
        peers = std::make_unique<PeerLedgers>(outbound, nullptr, nullptr);
        overlay = std::make_unique<PeerOverlay>(PeerOverlay::Options{"127.0.0.1:0", 8, false, 0, {}, 8, false, relay}, *peers, served.server, nullptr, nullptr);
        target = overlay.get();
        for (unsigned i = 0; i < count; ++i) {
            PeerLocal local{NodeIdentity::ephemeral(), 0};
            local.squelch = true;
            clients.push_back(std::make_unique<PeerSession>("127.0.0.1:" + std::to_string(overlay->port()), local,
                std::set<int>{protocol::mtVALIDATION, protocol::mtPROPOSE_LEDGER, protocol::mtSQUELCH, protocol::mtSTATUS_CHANGE}));
            // Greeted once the overlay lists it.
            if (!await(*clients.back(), protocol::mtSTATUS_CHANGE, 5s)) throw Error("A client was not greeted");
        }
    }
    ~Relaying() { relay->stop(); }
    void trust(Validator const& v) { std::lock_guard lock(mutex); trusted.insert(v.raw()); }
};
}

TEST(PeerRelay, RelaysCheckedValidationsOnceToPeersThatLackThem) {
    Relaying node(3);
    EXPECT_TRUE(node.clients[0]->info().squelch);  // both sides offered squelch (vprr)
    auto v = validator();
    node.trust(v);
    auto message = validation(v, 100);
    node.clients[0]->send(protocol::mtVALIDATION, message);
    for (int i : {1, 2}) {
        auto got = await(*node.clients[i], protocol::mtVALIDATION, 5s);
        ASSERT_TRUE(got) << i;
        EXPECT_EQ(got->payload, message.SerializeAsString());  // as received
    }
    EXPECT_FALSE(await(*node.clients[0], protocol::mtVALIDATION, 500ms));  // not back to its sender
    // A copy from another peer is a duplicate, relayed to no one.
    node.clients[1]->send(protocol::mtVALIDATION, message);
    EXPECT_FALSE(await(*node.clients[2], protocol::mtVALIDATION, 500ms));
    auto counts = node.relay->status().at("validations");
    EXPECT_EQ(counts.at("received"), 2u);
    EXPECT_EQ(counts.at("duplicate"), 1u);
    EXPECT_EQ(counts.at("relayed"), 1u);
    EXPECT_EQ(counts.at("messages_sent"), 2u);
}

TEST(PeerRelay, DropsForgedAndStaleValidations) {
    Relaying node(2);
    auto v = validator();
    node.trust(v);
    // Signed, then its ledger hash changed: the signature no longer holds.
    auto forged = validation(v, 101);
    auto bytes = forged.validation();
    auto hash = xr::sha512Half(std::uint32_t(101));
    auto at = bytes.find(std::string(reinterpret_cast<char const*>(hash.data()), hash.size()));
    ASSERT_NE(at, std::string::npos);
    bytes[at] = static_cast<char>(bytes[at] ^ 1);
    forged.set_validation(bytes);
    node.clients[0]->send(protocol::mtVALIDATION, forged);
    node.clients[0]->send(protocol::mtVALIDATION, validation(v, 102, 600));  // signed ten minutes ago
    EXPECT_FALSE(await(*node.clients[1], protocol::mtVALIDATION, 1s));
    auto counts = node.relay->status().at("validations");
    EXPECT_EQ(counts.at("invalid"), 1u);
    EXPECT_EQ(counts.at("stale"), 1u);
    EXPECT_EQ(counts.at("relayed"), 0u);
}

TEST(PeerRelay, RelaysTrustedProposalsAsXrpldDoesByDefault) {
    Relaying node(2);
    auto trusted = validator(), unlisted = validator();
    node.trust(trusted);
    auto message = proposal(trusted, 200);
    node.clients[0]->send(protocol::mtPROPOSE_LEDGER, message);
    auto got = await(*node.clients[1], protocol::mtPROPOSE_LEDGER, 5s);
    ASSERT_TRUE(got);
    EXPECT_EQ(got->payload, message.SerializeAsString());
    // Unlisted validators' proposals are not relayed (relay_proposals = trusted); nor stale or
    // forged ones.
    node.clients[0]->send(protocol::mtPROPOSE_LEDGER, proposal(unlisted, 201));
    node.clients[0]->send(protocol::mtPROPOSE_LEDGER, proposal(trusted, 202, 600));
    auto forged = proposal(trusted, 203);
    forged.set_closetime(forged.closetime() + 1);
    node.clients[0]->send(protocol::mtPROPOSE_LEDGER, forged);
    EXPECT_FALSE(await(*node.clients[1], protocol::mtPROPOSE_LEDGER, 1s));
    auto counts = node.relay->status().at("proposals");
    EXPECT_EQ(counts.at("relayed"), 1u);
    EXPECT_EQ(counts.at("stale"), 1u);
    EXPECT_EQ(counts.at("invalid"), 1u);
}

TEST(PeerRelay, HonoursSquelchesPeersSend) {
    Relaying node(3);
    auto v = validator();
    node.trust(v);
    protocol::TMSquelch squelch;
    squelch.set_squelch(true);
    squelch.set_validatorpubkey(v.raw());
    squelch.set_squelchduration(600);
    node.clients[2]->send(protocol::mtSQUELCH, squelch);
    std::this_thread::sleep_for(300ms);
    node.clients[0]->send(protocol::mtVALIDATION, validation(v, 300));
    EXPECT_TRUE(await(*node.clients[1], protocol::mtVALIDATION, 5s));
    EXPECT_FALSE(await(*node.clients[2], protocol::mtVALIDATION, 500ms));
    // Lifted: it hears the validator again.
    squelch.set_squelch(false);
    node.clients[2]->send(protocol::mtSQUELCH, squelch);
    std::this_thread::sleep_for(300ms);
    node.clients[0]->send(protocol::mtVALIDATION, validation(v, 301));
    EXPECT_TRUE(await(*node.clients[2], protocol::mtVALIDATION, 5s));
    EXPECT_EQ(node.relay->status().at("squelches_received"), 2u);
}

// With squelch messages off here, a peer's squelch for a validator this node relays (one off the
// trusted list, under policy all) still holds.
TEST(PeerRelay, HonoursSquelchesWithItsOwnSquelchOff) {
    PeerRelay::Options options;
    options.squelch = false;
    Relaying node(3, options);
    auto v = validator();  // not trusted
    node.clients[0]->send(protocol::mtVALIDATION, validation(v, 600));
    ASSERT_TRUE(await(*node.clients[2], protocol::mtVALIDATION, 5s));
    protocol::TMSquelch squelch;
    squelch.set_squelch(true);
    squelch.set_validatorpubkey(v.raw());
    squelch.set_squelchduration(600);
    node.clients[2]->send(protocol::mtSQUELCH, squelch);
    std::this_thread::sleep_for(300ms);
    node.clients[0]->send(protocol::mtVALIDATION, validation(v, 601));
    EXPECT_TRUE(await(*node.clients[1], protocol::mtVALIDATION, 5s));
    EXPECT_FALSE(await(*node.clients[2], protocol::mtVALIDATION, 500ms));
    EXPECT_EQ(node.relay->status().at("squelches_sent"), 0u);
}

TEST(PeerRelay, SquelchesSourcesBeyondTheLimit) {
    PeerRelay::Options options;
    options.sources = 1;
    options.validations = PeerRelay::Policy::trusted;
    Relaying node(3, options);
    auto v = validator(), unlisted = validator();
    node.trust(v);
    auto message = validation(v, 400);
    node.clients[0]->send(protocol::mtVALIDATION, message);
    ASSERT_TRUE(await(*node.clients[2], protocol::mtVALIDATION, 5s));
    // A second source of the same validator is asked to stop, for five to ten minutes.
    node.clients[1]->send(protocol::mtVALIDATION, message);
    auto got = await(*node.clients[1], protocol::mtSQUELCH, 5s);
    ASSERT_TRUE(got);
    protocol::TMSquelch squelch;
    ASSERT_TRUE(squelch.ParseFromString(got->payload));
    EXPECT_TRUE(squelch.squelch());
    EXPECT_EQ(squelch.validatorpubkey(), v.raw());
    EXPECT_GE(squelch.squelchduration(), 300u);
    EXPECT_LE(squelch.squelchduration(), 600u);
    // An unlisted validator's messages are not relayed here: its only source is asked to stop.
    node.clients[2]->send(protocol::mtVALIDATION, validation(unlisted, 401));
    got = await(*node.clients[2], protocol::mtSQUELCH, 5s);
    ASSERT_TRUE(got);
    ASSERT_TRUE(squelch.ParseFromString(got->payload));
    EXPECT_EQ(squelch.validatorpubkey(), unlisted.raw());
    EXPECT_FALSE(await(*node.clients[0], protocol::mtVALIDATION, 500ms));
    EXPECT_EQ(node.relay->status().at("squelches_sent"), 2u);
}

// A peer that did not negotiate squelch messages (vprr) is sent none.
TEST(PeerRelay, SquelchesOnlyPeersThatNegotiatedThem) {
    PeerRelay::Options options;
    options.validations = PeerRelay::Policy::trusted;
    Relaying node(1, options);
    PeerSession plain("127.0.0.1:" + std::to_string(node.overlay->port()), NodeIdentity::ephemeral(), 0,
                      {protocol::mtVALIDATION, protocol::mtSQUELCH, protocol::mtSTATUS_CHANGE});
    ASSERT_TRUE(await(plain, protocol::mtSTATUS_CHANGE, 5s));
    EXPECT_FALSE(plain.info().squelch);
    auto unlisted = validator();
    plain.send(protocol::mtVALIDATION, validation(unlisted, 500));
    EXPECT_FALSE(await(plain, protocol::mtSQUELCH, 1s));
    node.clients[0]->send(protocol::mtVALIDATION, validation(unlisted, 501));  // one that did is asked to stop
    EXPECT_TRUE(await(*node.clients[0], protocol::mtSQUELCH, 5s));
    EXPECT_EQ(node.relay->status().at("squelches_sent"), 1u);
}

// A sender becomes one of a validator's sources only with a message that passed its checks:
// forged proposals in a trusted validator's name (and copies of them), or stale ones, keep no
// place from the peers that send its real ones.
TEST(PeerRelay, OnlyCheckedProposalsMakeSources) {
    PeerRelay::Options options;
    options.sources = 1;
    Relaying node(3, options);
    auto v = validator();
    node.trust(v);
    auto forged = proposal(v, 700);
    forged.set_closetime(forged.closetime() + 1);
    node.clients[0]->send(protocol::mtPROPOSE_LEDGER, forged);
    node.clients[0]->send(protocol::mtPROPOSE_LEDGER, proposal(v, 701, 600));
    std::this_thread::sleep_for(300ms);
    node.clients[2]->send(protocol::mtPROPOSE_LEDGER, forged);  // a copy
    std::this_thread::sleep_for(300ms);
    // The first checked proposal's sender is the source; the next to send it is asked to stop.
    auto message = proposal(v, 702);
    node.clients[1]->send(protocol::mtPROPOSE_LEDGER, message);
    ASSERT_TRUE(await(*node.clients[0], protocol::mtPROPOSE_LEDGER, 5s));
    EXPECT_FALSE(await(*node.clients[1], protocol::mtSQUELCH, 500ms));
    node.clients[2]->send(protocol::mtPROPOSE_LEDGER, message);
    auto got = await(*node.clients[2], protocol::mtSQUELCH, 5s);
    ASSERT_TRUE(got);
    protocol::TMSquelch squelch;
    ASSERT_TRUE(squelch.ParseFromString(got->payload));
    EXPECT_EQ(squelch.validatorpubkey(), v.raw());
    EXPECT_FALSE(await(*node.clients[0], protocol::mtSQUELCH, 500ms));
    auto counts = node.relay->status().at("proposals");
    EXPECT_EQ(counts.at("invalid"), 1u);
    EXPECT_EQ(counts.at("stale"), 1u);
    EXPECT_EQ(counts.at("duplicate"), 2u);
    EXPECT_EQ(counts.at("relayed"), 1u);
    EXPECT_EQ(node.relay->status().at("squelches_sent"), 1u);
}

// Peers whose copy of a message came while the first was checked count as sources once it
// passed, as later senders do: past `sources`, they are asked to stop.
TEST(PeerRelay, CopiesDuringTheCheckMakeSources) {
    PeerRelay::Options options;
    options.sources = 1;
    Relaying node(3, options);
    auto v = validator();
    node.trust(v);
    auto message = validation(v, 800);
    {
        // The first copy's check waits (asking whether its validator is trusted) for the others.
        std::unique_lock checking(node.mutex);
        node.clients[0]->send(protocol::mtVALIDATION, message);
        std::this_thread::sleep_for(300ms);
        node.clients[1]->send(protocol::mtVALIDATION, message);
        node.clients[2]->send(protocol::mtVALIDATION, message);
        std::this_thread::sleep_for(300ms);
    }
    for (int i : {1, 2}) {
        auto got = await(*node.clients[i], protocol::mtSQUELCH, 5s);
        ASSERT_TRUE(got) << i;
        protocol::TMSquelch squelch;
        ASSERT_TRUE(squelch.ParseFromString(got->payload));
        EXPECT_EQ(squelch.validatorpubkey(), v.raw());
    }
    EXPECT_FALSE(await(*node.clients[0], protocol::mtSQUELCH, 500ms));
    auto counts = node.relay->status().at("validations");
    EXPECT_EQ(counts.at("duplicate"), 2u);
    EXPECT_EQ(counts.at("messages_sent"), 0u);  // every peer sent it
    EXPECT_EQ(node.relay->status().at("squelches_sent"), 2u);
}

TEST(PeerRelay, PeerLimitsFollowXrpldsPeersMax) {
    using Limits = std::pair<unsigned, unsigned>;
    EXPECT_EQ(peer_limits(0, 5, std::nullopt), (Limits{5, 11}));   // no cap: today's defaults
    EXPECT_EQ(peer_limits(0, 5, 20u), (Limits{5, 20}));
    EXPECT_EQ(peer_limits(21, 10, std::nullopt), (Limits{10, 11}));  // xrpld's defaults
    EXPECT_EQ(peer_limits(21, 5, std::nullopt), (Limits{5, 16}));
    EXPECT_EQ(peer_limits(21, 5, 11u), (Limits{5, 11}));
    EXPECT_EQ(peer_limits(50, 5, 60u), (Limits{5, 45}));
    EXPECT_EQ(peer_limits(3, 5, std::nullopt), (Limits{3, 0}));
}

namespace {
// A listener answering through `responder` (serving_nothing: silent, as xrpld is for a ledger it
// lacks), keeping its sessions so the test can send them status messages.
struct Answering {
    std::mutex mutex;
    std::vector<std::unique_ptr<PeerSession>> sessions;
    std::unique_ptr<PeerListener> listener;
    explicit Answering(std::shared_ptr<PeerResponder> responder) {
        PeerListener::Handlers handlers;
        handlers.admit = [](PeerSession::Info const&, std::string const&) { return true; };
        handlers.accepted = [this](std::unique_ptr<PeerSession> session, std::string const&) { std::lock_guard lock(mutex); sessions.push_back(std::move(session)); };
        listener = std::make_unique<PeerListener>("127.0.0.1:0", PeerLocal{NodeIdentity::ephemeral(), 0, false, false, std::move(responder), {}}, std::set<int>{}, handlers);
    }
    std::string endpoint() const { return "127.0.0.1:" + std::to_string(listener->port()); }
    void announce(std::uint32_t seq, Bytes const& hash) {
        protocol::TMStatusChange status;
        status.set_newevent(protocol::neACCEPTED_LEDGER); status.set_ledgerseq(seq); status.set_ledgerhash(hash);
        std::lock_guard lock(mutex);
        for (auto& session : sessions) session->send(protocol::mtSTATUS_CHANGE, status);
    }
};
// Answers as `inner` does, `delay` later.
struct Delayed : PeerResponder {
    std::shared_ptr<PeerResponder> inner;
    std::chrono::milliseconds delay;
    Delayed(std::shared_ptr<PeerResponder> responder, std::chrono::milliseconds wait) : inner(std::move(responder)), delay(wait) {}
    void request(int type, std::string const& payload, PeerRequester const& from) override {
        std::thread([inner = inner, delay = delay, type, payload, from] { std::this_thread::sleep_for(delay); inner->request(type, payload, from); }).detach();
    }
};
bool both_open(PeerLedgers const& peers) {
    for (auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10); std::chrono::steady_clock::now() < until;
         std::this_thread::sleep_for(std::chrono::milliseconds(20)))
        if (peers.sessions().size() == 2) return true;
    return false;
}
}

// Peers send nothing at all for a ledger they lack (or when busy): a request is asked of another
// peer within its patience, never waiting out the 10 s a silent peer used to cost.
TEST(PeerLedgersFetch, ASilentPeerIsNotWaitedFor) {
    Served served;
    Answering silent(serving_nothing()), serving(served.server);
    PeerLedgers::Options options;
    options.peers = {silent.endpoint(), serving.endpoint()};
    options.connections = 2;
    PeerLedgers peers(options, {}, {});
    ASSERT_TRUE(both_open(peers));
    // The silent peer says it holds every ledger (the other says nothing), so it is mostly asked first.
    for (std::size_t i = 40 - 16; i < 40; ++i) silent.announce(served.header(i).sequence, from_hex(served.header(i).hash));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    auto started = std::chrono::steady_clock::now();
    for (std::size_t i = 40 - 16; i < 40; ++i)
        EXPECT_EQ(peers.header(from_hex(served.header(i).hash), served.header(i).sequence).raw, served.header(i).raw);
    auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    EXPECT_LT(seconds, 16 * 1.5) << "each header within its first peer's patience";
    auto status = peers.status();
    // Most requests went to the silent peer first (it holds them; the scores keep some chance for
    // the other), each then to the other: never more than two copies of one request.
    auto hedged = status.at("hedged").get<std::uint64_t>();
    EXPECT_GE(hedged, 6u);
    EXPECT_EQ(status.at("requests").get<std::uint64_t>(), 16u + hedged);
}

// The quicker of two peers ends up answering most requests (xrpld's PeerSet scores peers by latency).
TEST(PeerLedgersFetch, TheQuickerPeerIsPreferred) {
    Served fast_served, slow_served;
    Answering fast(fast_served.server), slow(std::make_shared<Delayed>(slow_served.server, std::chrono::milliseconds(250)));
    PeerLedgers::Options options;
    options.peers = {fast.endpoint(), slow.endpoint()};
    options.connections = 2;
    PeerLedgers peers(options, {}, {});
    ASSERT_TRUE(both_open(peers));
    for (int round = 0; round < 3; ++round)
        for (std::size_t i = 40 - 16; i < 40; ++i) EXPECT_EQ(peers.header(from_hex(fast_served.header(i).hash)).raw, fast_served.header(i).raw);
    auto quick = fast_served.server->status().at("answered").get<std::uint64_t>(), late = slow_served.server->status().at("answered").get<std::uint64_t>();
    EXPECT_GT(quick, 3 * late) << quick << " answered by the quicker peer, " << late << " by the slower";
    for (auto const& peer : peers.status().at("peers"))
        if (peer.at("endpoint") == slow.endpoint() && late) EXPECT_GE(peer.at("answer_ms").get<double>(), 200);
}

namespace {
// Answers as `inner` does, each tree-node reply carrying first, for every child of the root, an
// empty and an unreadable node, and before each node a corrupt copy of it.
struct Tampering : PeerResponder {
    std::shared_ptr<PeerResponder> inner;
    explicit Tampering(std::shared_ptr<PeerResponder> responder) : inner(std::move(responder)) {}
    void request(int type, std::string const& payload, PeerRequester const& from) override {
        auto wrapped = from;
        wrapped.reply = [reply = from.reply](int type, std::string const& payload) {
            protocol::TMLedgerData data;
            if (type != protocol::mtLEDGER_DATA || !data.ParseFromString(payload) || data.type() == protocol::liBASE) { reply(type, payload); return; }
            auto tampered = data;
            tampered.clear_nodes();
            for (int branch = 0; branch < 16; ++branch) {
                auto id = ::xrpl::SHAMapNodeID().getChildNodeID(branch).getRawString();
                auto* empty = tampered.add_nodes(); empty->set_nodeid(id); empty->set_nodedata("");
                auto* junk = tampered.add_nodes(); junk->set_nodeid(id); junk->set_nodedata(std::string("\x01\x02\x03", 3));
            }
            for (auto const& node : data.nodes()) {
                auto* corrupt = tampered.add_nodes();
                *corrupt = node;
                auto bytes = node.nodedata();
                if (!bytes.empty()) bytes[0] = static_cast<char>(bytes[0] ^ 1);
                corrupt->set_nodedata(bytes);
                *tampered.add_nodes() = node;
            }
            reply(type, tampered.SerializeAsString());
        };
        inner->request(type, payload, wrapped);
    }
};
// Answers every header request at once with the header of another ledger, saying it holds them all.
struct Lying : PeerResponder {
    Bytes header;
    std::atomic<int> asked{0};
    explicit Lying(Bytes other) : header(std::move(other)) {}
    void request(int type, std::string const& payload, PeerRequester const& from) override {
        protocol::TMGetLedger request;
        if (type != protocol::mtGET_LEDGER || !request.ParseFromString(payload) || request.itype() != protocol::liBASE) return;
        ++asked;
        protocol::TMLedgerData reply;
        reply.set_ledgerhash(request.ledgerhash());
        reply.set_ledgerseq(1);
        reply.set_type(protocol::liBASE);
        reply.add_nodes()->set_nodedata(header);
        from.reply(protocol::mtLEDGER_DATA, reply.SerializeAsString());
    }
};
}

// What a reply carries beyond the nodes that check (empty, unreadable or corrupt copies) is left
// out: the fetch still finds every node, and nothing it holds is unchecked.
TEST(PeerLedgersFetch, NodesThatDoNotCheckAreLeftOut) {
    Served served;
    Answering tampering(std::make_shared<Tampering>(served.server));
    PeerLedgers::Options options;
    options.peers = {tampering.endpoint()};
    options.connections = 1;
    PeerLedgers peers(options, {}, {});
    for (std::size_t i = 40 - 16; i < 40; i += 5) {
        auto expected = served.header(i);
        auto header = peers.header(from_hex(expected.hash));
        auto const& ledger = served.chain.ledgers[i];
        auto transactions = peers.transactions(header);
        ASSERT_EQ(transactions.size(), ledger.transactions.size());
        EXPECT_EQ(transactions[0].transaction, ledger.transactions[0].transaction);
        auto values = peers.objects(header, {ledger.patches[0].key});
        EXPECT_EQ(values[0], ledger.patches[0].object);
    }
    EXPECT_EQ(peers.status().at("rejected"), 0u);
}

// A peer whose answers do not check is asked again no sooner than one that does not answer: the
// request goes to another peer, and after three such answers its session ends.
TEST(PeerLedgersFetch, APeerSendingWrongDataIsNotAskedAgain) {
    Served served;
    auto liar = std::make_shared<Lying>(served.header(3).raw);
    Answering lying(liar), serving(served.server);
    PeerLedgers::Options options;
    options.peers = {lying.endpoint(), serving.endpoint()};
    options.connections = 2;
    PeerLedgers peers(options, {}, {});
    ASSERT_TRUE(both_open(peers));
    for (std::size_t i = 40 - 16; i < 40; ++i) lying.announce(served.header(i).sequence, from_hex(served.header(i).hash));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    for (std::size_t i = 40 - 16; i < 40; ++i)
        EXPECT_EQ(peers.header(from_hex(served.header(i).hash), served.header(i).sequence).raw, served.header(i).raw);
    EXPECT_GE(liar->asked.load(), 1);
    EXPECT_LE(liar->asked.load(), 3);
    EXPECT_EQ(peers.status().at("rejected").get<int>(), liar->asked.load());
}

// The hash peers announce for a ledger they accepted is where fetching may start, once two agree.
TEST(PeerLedgersFetch, AnnouncedLedgersNeedTwoPeersAgreeing) {
    Served served;
    Answering a(served.server), b(served.server);
    PeerLedgers::Options options;
    options.peers = {a.endpoint(), b.endpoint()};
    options.connections = 2;
    PeerLedgers peers(options, {}, {});
    ASSERT_TRUE(both_open(peers));
    auto settle = [] { std::this_thread::sleep_for(std::chrono::milliseconds(300)); };
    auto hash = from_hex(served.header(39).hash), other = Bytes(32, '\x11');
    a.announce(500, hash); settle();
    EXPECT_FALSE(peers.announced(500));  // one peer is not enough
    b.announce(500, other); settle();
    EXPECT_FALSE(peers.announced(500));  // two peers, two hashes
    b.announce(500, hash); settle();
    EXPECT_EQ(peers.announced(500), hash);
    EXPECT_FALSE(peers.announced(501));
}

TEST(PeerRelay, BootstrapHubsGiveWayToPeersTheNetworkNames) {
    Served served;
    Listening hub1(served), hub2(served), learned1(served), learned2(served);
    PeerLedgers::Options options;
    options.bootstrap = {hub1.endpoint(), hub2.endpoint()};
    options.connections = 2;
    options.rebalance = 1s;
    PeerLedgers peers(options, nullptr, nullptr);
    auto connected = [&](std::string const& endpoint) {
        for (auto const& s : peers.sessions()) if (s.endpoint == endpoint) return true;
        return false;
    };
    auto until = std::chrono::steady_clock::now() + 10s;
    while (peers.sessions().size() < 2 && std::chrono::steady_clock::now() < until) std::this_thread::sleep_for(50ms);
    ASSERT_TRUE(connected(hub1.endpoint()) && connected(hub2.endpoint()));  // no one else known yet
    peers.learn({learned1.endpoint(), learned2.endpoint()});
    // One hub gives way; the other stays (bootstrap_sessions = 1).
    until = std::chrono::steady_clock::now() + 15s;
    auto hubs = [&] { return connected(hub1.endpoint()) + connected(hub2.endpoint()); };
    while ((hubs() != 1 || peers.sessions().size() != 2) && std::chrono::steady_clock::now() < until) std::this_thread::sleep_for(50ms);
    EXPECT_EQ(hubs(), 1);
    EXPECT_EQ(peers.sessions().size(), 2u);
    EXPECT_TRUE(connected(learned1.endpoint()) || connected(learned2.endpoint()));
}

TEST(PeerRelay, PrivateNodesUseOnlyTheirConfiguredPeers) {
    Served served;
    Listening configured(served), other(served);
    PeerLedgers::Options options;
    options.peers = {configured.endpoint()};
    options.connections = 2;
    options.private_mode = true;
    PeerLedgers peers(options, nullptr, nullptr);
    peers.learn({other.endpoint()});
    std::this_thread::sleep_for(2s);
    auto sessions = peers.sessions();
    ASSERT_EQ(sessions.size(), 1u);
    EXPECT_EQ(sessions[0].endpoint, configured.endpoint());
    EXPECT_TRUE(peers.known().size() <= 1u);
}

TEST(PeerSessions, GossipedEndpointsArePublicAddressesOnly) {
    for (auto const* endpoint : {"8.8.8.8:51235", "34.1.2.3:2459", "[2600:1f18::1]:51235", "203.0.114.9"})
        EXPECT_TRUE(public_endpoint(endpoint)) << endpoint;
    for (auto const* endpoint : {"127.0.0.1:51235", "10.1.2.3:51235", "172.16.0.1:51235", "192.168.1.1:51235", "169.254.1.1:51235",
                                 "100.64.0.1:51235", "0.0.0.0:51235", "224.0.0.1:51235", "8.8.8.8:0", "8.8.8.8:70000", "[::1]:51235",
                                 "[fe80::1]:51235", "[fd00::1]:51235", "[::ffff:10.0.0.1]:51235", "r.ripple.com:51235", "", "8.8.8.8:x",
                                 "8.8.8.8:", "[]:51235", "[8.8.8.8", "8.8.8.8:1:2"})
        EXPECT_FALSE(public_endpoint(endpoint)) << endpoint;
    // Special-purpose IPv4 ranges, and IPv4 addresses carried in IPv6.
    for (auto const* endpoint : {"192.0.0.9:51235", "192.0.2.1:51235", "198.18.0.1:51235", "198.19.255.1:51235", "198.51.100.7:51235",
                                 "203.0.113.5:51235", "255.255.255.255:51235", "240.0.0.1:51235", "[64:ff9b::808:808]:51235",
                                 "[64:ff9b:1::a00:1]:51235", "[::808:808]:51235", "[::a00:1]:51235", "[2002:a00:1::1]:51235",
                                 "[2002:c0a8:101::1]:51235", "[2001:db8::1]:51235"})
        EXPECT_FALSE(public_endpoint(endpoint)) << endpoint;
    for (auto const* endpoint : {"192.0.1.1:51235", "198.20.0.1:51235", "198.51.101.1:51235", "203.0.112.1:51235", "[2002:808:808::1]:51235",
                                 "[2001:db9::1]:51235", "[2001:4860::8888]:51235", "[2a00:1450::1]:51235"})
        EXPECT_TRUE(public_endpoint(endpoint)) << endpoint;
    // IPv6 outside global unicast (2000::/3): discard, SIIT's IPv4-translated form, reserved;
    // Teredo; and a zone or a NUL, which would name this machine's interface or cut the name short.
    for (auto const* endpoint : {"[100::1]:51235", "[::ffff:0:808:808]:51235", "[4000::1]:51235", "[e000::1]:51235", "[2001:0:4136:e378::1]:51235",
                                 "[2600:1f18::1%eth0]:51235", "[fe80::1%25eth0]:51235"})
        EXPECT_FALSE(public_endpoint(endpoint)) << endpoint;
    EXPECT_FALSE(public_endpoint(std::string("8.8.8.8\0.example", 16) + ":51235"));
    EXPECT_FALSE(public_endpoint(std::string("8.8.8.8:51235\0", 14)));
}
TEST(PeerSessions, RememberedEndpointsHaveOneSpelling) {
    EXPECT_EQ(canonical_endpoint("[2600:1f18:0:0::1]:01235"), "[2600:1f18::1]:1235");
    EXPECT_EQ(canonical_endpoint("[2600:1F18::1]:51235"), "[2600:1f18::1]:51235");
    EXPECT_EQ(canonical_endpoint("[::ffff:8.8.8.8]:51235"), "8.8.8.8:51235");
    EXPECT_EQ(canonical_endpoint("8.8.8.8"), "8.8.8.8:51235");
    EXPECT_EQ(canonical_endpoint("r.ripple.com:51235"), "r.ripple.com:51235");
}
TEST(PeerSessions, AddressLimitsCountAnIPv6SlashSixtyFourAsOne) {
    EXPECT_EQ(address_group("2600:1f18:1:2:aaaa::1"), address_group("2600:1f18:1:2:bbbb::2"));
    EXPECT_NE(address_group("2600:1f18:1:2::1"), address_group("2600:1f18:1:3::1"));
    EXPECT_EQ(address_group("2600:1f18:1:2:aaaa::1"), "2600:1f18:1:2::/64");
    EXPECT_EQ(address_group("8.8.8.8"), "8.8.8.8");
    EXPECT_NE(address_group("8.8.8.8"), address_group("8.8.8.9"));
    EXPECT_EQ(address_group("::ffff:8.8.8.8"), "8.8.8.8");
    EXPECT_EQ(address_group("not an address"), "not an address");
}
TEST(PeerSessions, LongEndpointsAreParsedWithoutRecursion) {
    // Peers choose these strings: megabytes of one character must be refused, not overflow a stack.
    std::string huge(4 << 20, 'a');
    EXPECT_FALSE(public_endpoint(huge));
    EXPECT_FALSE(public_endpoint(huge + ":51235"));
}
TEST(PeerSessions, FailureReasonsAreCountedWithoutTheirEndpoints) {
    EXPECT_EQ(without_endpoint("Peer 10.0.0.1:51235: Connection refused", "10.0.0.1:51235"), "Peer: Connection refused");
    EXPECT_EQ(without_endpoint("Peer TLS handshake timed out: [::1]:5", "[::1]:5"), "Peer TLS handshake timed out");
    std::map<std::string, std::uint64_t> counts;
    for (int i = 0; i < 40; ++i) count_reason(counts, "reason " + std::to_string(i));
    count_reason(counts, "reason 0");
    EXPECT_EQ(counts.size(), 33u);
    EXPECT_EQ(counts["reason 0"], 2u);
    EXPECT_EQ(counts["other"], 8u);
}

TEST(PeerSessions, APeerThatDropsSessionsSoonIsNotRedialledAtOnce) {
    // A listener that closes every session it accepts.
    std::mutex mutex;
    std::vector<std::unique_ptr<PeerSession>> dropped;
    std::atomic<int> accepted{0};
    PeerListener::Handlers handlers;
    handlers.admit = [](PeerSession::Info const&, std::string const&) { return true; };
    handlers.accepted = [&](std::unique_ptr<PeerSession> session, std::string const&) {
        ++accepted;
        session->close("testing");
        std::lock_guard lock(mutex);
        dropped.push_back(std::move(session));
    };
    PeerListener dropping("127.0.0.1:0", PeerLocal{NodeIdentity::ephemeral(), 0}, {}, handlers);
    PeerLedgers::Options options;
    options.peers = {"127.0.0.1:" + std::to_string(dropping.port())};
    options.connections = 1;
    PeerLedgers peers(options, nullptr, nullptr);
    std::this_thread::sleep_for(3s);
    // The first retry waits 5 s (without the backoff it would come within 200 ms).
    EXPECT_EQ(accepted.load(), 1);
    auto status = peers.status();
    EXPECT_EQ(status.at("connects"), 1);
    EXPECT_EQ(status.at("short_sessions"), 1);
    ASSERT_EQ(status.at("closed").size(), 1u);
    EXPECT_TRUE(status.at("closed").begin().key().starts_with("Peer connection closed")) << status.at("closed").dump();
    // Why the listener's side ended: this node closed it.
    std::lock_guard lock(mutex);
    EXPECT_EQ(dropped.at(0)->failure(), "Closed by this node: testing");
}

TEST(PeerSessions, InboundFailuresAndClosesAreCounted) {
    Served served;
    PeerLedgers::Options options;
    options.peers = {"127.0.0.1:1"};  // nothing there: this node has no outbound peers
    options.connections = 1;
    PeerLedgers peers(options, {}, {});
    PeerOverlay overlay({"127.0.0.1:0", 2, true, 0, {}}, peers, served.server, {}, {});
    auto endpoint = "127.0.0.1:" + std::to_string(overlay.port());
    auto wait_for = [&](auto done) {
        auto until = std::chrono::steady_clock::now() + 5s;
        while (!done(overlay.status()) && std::chrono::steady_clock::now() < until) std::this_thread::sleep_for(50ms);
        return overlay.status();
    };
    // A connection that ends before TLS, and a peer of another network.
    { net::io_context io; net::ip::tcp::socket socket(io); socket.connect({net::ip::make_address("127.0.0.1"), overlay.port()}); }
    EXPECT_THROW(PeerSession(endpoint, NodeIdentity::ephemeral(), 1, {}), Error);
    auto status = wait_for([](Json const& s) { return s.at("handshake_failures").size() >= 2; });
    std::vector<std::string> failures;
    for (auto const& [reason, count] : status.at("handshake_failures").items()) failures.push_back(reason);
    ASSERT_EQ(failures.size(), 2u) << status.at("handshake_failures").dump();
    EXPECT_TRUE(failures[0].starts_with("Refused: ")) << failures[0];
    EXPECT_TRUE(failures[1].starts_with("TLS handshake: ")) << failures[1];
    // A peer that leaves soon after its handshake.
    {
        PeerSession client(endpoint, NodeIdentity::ephemeral(), 0, {protocol::mtSTATUS_CHANGE});
        EXPECT_TRUE(await(client, protocol::mtSTATUS_CHANGE, 5s));
    }
    status = wait_for([](Json const& s) { return s.at("short_sessions") == 1; });
    EXPECT_EQ(status.at("short_sessions"), 1);
    EXPECT_EQ(status.at("closed").size(), 1u);
    EXPECT_TRUE(status.at("inbound").empty());
}

TEST(PeerSessions, NetworksKnownByNameBringTheirHubsAndValidatorLists) {
    EXPECT_EQ(parse_network("mainnet"), 0u);
    EXPECT_EQ(parse_network("main"), 0u);
    EXPECT_EQ(parse_network("testnet"), 1u);
    EXPECT_EQ(parse_network("devnet"), 2u);
    EXPECT_EQ(parse_network("21337"), 21337u);
    EXPECT_THROW(parse_network("moonnet"), Error);
    EXPECT_THROW(parse_network(""), Error);
    EXPECT_EQ(default_peers(), (std::vector<std::string>{"r.ripple.com:51235", "sahyadri.isrdc.in:51235", "hubs.xrpkuwait.com:51235",
                                                      "hub.xrpl-commons.org:51235", "s1.ripple.com:51235", "s2.ripple.com:51235"}));
    EXPECT_EQ(default_peers(2), (std::vector<std::string>{"s.devnet.rippletest.net:51235"}));
    EXPECT_EQ(network_defaults(1)->list, "https://vl.altnet.rippletest.net");
    EXPECT_EQ(network_defaults(2)->list_key, "EDBB54B0D9AEE071BB37784AF5A9E7CC49AC7A0EFCE868C54532BCB966B9CFC13B");
    EXPECT_TRUE(default_peers(21337).empty());
    // A network with no hubs known by name needs its peers listed.
    PeerLedgers::Options options;
    options.network_id = 21337;
    EXPECT_THROW(PeerLedgers(options, nullptr, nullptr), Error);
}

