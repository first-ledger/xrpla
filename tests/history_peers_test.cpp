// Ledgers whose transactions a node's API omits (xrpld sends none of a ledger's transactions
// when it cannot decode one, as with some 2013 transactions on current xrpld): an export from
// gRPC reads those ledgers' transaction trees raw from history peers, and the archive is the
// same as from a source that sent everything.
#include "core_fixture.hpp"
#include "../core/src/peer.hpp"
#include "../core/src/peer_serving.hpp"
#include "../core/src/piece_history.hpp"
#include "../core/src/replay.hpp"
#include "../core/src/rpc.hpp"
#include "../core/src/wire.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/proto/org/xrpl/rpc/v1/xrp_ledger.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <set>
#include <thread>

using namespace xrpla;
using namespace xrpla::core;
namespace {
namespace api = org::xrpl::rpc::v1;
namespace net = boost::asio;
namespace http = boost::beast::http;
std::string text(Bytes const& bytes) { return std::string(bytes.begin(), bytes.end()); }

// A 40-ledger chain of `accounts` accounts, its newest ledgers served to peers by an XRPLA listener.
struct Chain {
    fs::path dir = fs::temp_directory_path() / ("xrpla-history-peers-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fixture::Evolving chain;
    std::unique_ptr<LedgerVerifier> verifier;
    std::shared_ptr<LedgerServer> server;
    std::unique_ptr<PeerListener> listener;
    std::vector<std::unique_ptr<PeerSession>> sessions;
    std::mutex mutex;
    explicit Chain(unsigned accounts = 32) : chain(40, accounts) {
        fs::create_directories(dir);
        verifier = std::make_unique<LedgerVerifier>(dir / "served");
        verifier->keep_ledgers(40);
        chain.seed(*verifier);
        for (auto const& ledger : chain.ledgers) verifier->apply(ledger);
        verifier->persist();
        server = std::make_shared<LedgerServer>(40);
        server->node_store(verifier->node_store());
        for (auto const& ledger : chain.ledgers) server->add(decode_header(ledger.header));
        PeerLocal local{NodeIdentity::ephemeral(), 0, false, true, server, {}};
        PeerListener::Handlers handlers;
        handlers.admit = [](PeerSession::Info const&, std::string const&) { return true; };
        handlers.accepted = [this](std::unique_ptr<PeerSession> session, std::string const&) {
            server->greet(*session);
            std::lock_guard lock(mutex);
            sessions.push_back(std::move(session));
        };
        handlers.redirect = [] { return std::vector<std::string>{}; };
        listener = std::make_unique<PeerListener>("127.0.0.1:0", local, std::set<int>{}, handlers);
    }
    ~Chain() {
        listener.reset();
        { std::lock_guard lock(mutex); sessions.clear(); }
        server.reset(); verifier.reset();
        fs::remove_all(dir);
    }
    std::string peer() const { return "127.0.0.1:" + std::to_string(listener->port()); }
    std::uint32_t first() const { return chain.ledgers.front().sequence; }
    std::uint32_t last() const { return chain.ledgers.back().sequence; }
};

// xrpld's gRPC API over the chain, except that it sends no transactions for `omit`, for
// `alter` a list that is not the ledger's (as when xrpld stops at a transaction it cannot decode),
// lacks the ledgers in `missing` (a hole in its history: it then cannot diff the next one), and
// for `stale` gives one changed object a value that is not the ledger's (as a Clio node did for
// a directory at mainnet ledger 89,429,363).
class Xrpld final : public api::XRPLedgerAPIService::Service {
    Chain const& c_;
    std::set<std::uint32_t> omit_, alter_, missing_, stale_;
    // The header as xrpld's gRPC sends it: the 118 bytes, then the ledger's hash.
    static std::string wire(Bytes const& raw) { return text(raw) + text(from_hex(decode_header(raw).hash)); }
public:
    Xrpld(Chain const& c, std::set<std::uint32_t> omit, std::set<std::uint32_t> alter, std::set<std::uint32_t> missing, std::set<std::uint32_t> stale)
        : c_(c), omit_(std::move(omit)), alter_(std::move(alter)), missing_(std::move(missing)), stale_(std::move(stale)) {}
    std::atomic<unsigned> ledgers{0};  // ledgers sent with their objects
    std::atomic<std::size_t> page{0};  // objects a GetLedgerData answer holds at most (0: all)
    std::atomic<unsigned> spills{0};   // the most files of state pages read ahead the export held open at once
    grpc::Status GetLedger(grpc::ServerContext*, api::GetLedgerRequest const* request, api::GetLedgerResponse* response) override {
        auto seq = request->ledger().sequence();
        if (request->ledger().has_shortcut()) seq = c_.last();
        response->set_validated(true);
        response->set_is_unlimited(true);
        if (seq == c_.first() - 1) {
            if (request->get_objects()) return {grpc::StatusCode::NOT_FOUND, "parent ledger not validated"};
            response->set_ledger_header(wire(c_.chain.snapshot_header));
            return grpc::Status::OK;
        }
        if (seq < c_.first() || seq > c_.last() || missing_.contains(seq)) return {grpc::StatusCode::NOT_FOUND, "ledgerNotFound"};
        if (request->get_objects() && missing_.contains(seq - 1)) return {grpc::StatusCode::NOT_FOUND, "parent ledger not validated"};
        auto const& ledger = c_.chain.ledgers[seq - c_.first()];
        response->set_ledger_header(wire(ledger.header));
        if (request->transactions() && !omit_.contains(seq))
            for (auto const& tx : ledger.transactions) {
                auto* listed = response->mutable_transactions_list()->add_transactions();
                listed->set_transaction_blob(text(tx.transaction));
                // Another ledger's metadata: a list whose tree root is not the header's.
                listed->set_metadata_blob(text(alter_.contains(seq) ? c_.chain.ledgers[seq - c_.first() - 1].transactions.front().metadata : tx.metadata));
            }
        if (request->get_objects()) {
            ++ledgers;
            response->set_objects_included(true);
            response->set_skiplist_included(true);
            bool spoiled = false;
            for (auto const& patch : ledger.patches) {
                auto* object = response->mutable_ledger_objects()->add_objects();
                object->set_key(text(patch.key));
                auto data = text(patch.object);
                if (stale_.contains(seq) && !spoiled && !data.empty()) { data.back() ^= 1; spoiled = true; }
                object->set_data(data);
            }
        }
        return grpc::Status::OK;
    }
    grpc::Status GetLedgerData(grpc::ServerContext*, api::GetLedgerDataRequest const* request, api::GetLedgerDataResponse* response) override {
        if (request->ledger().sequence() != c_.first() - 1) return {grpc::StatusCode::NOT_FOUND, "ledgerNotFound"};
        for (auto files = spill_files(), seen = spills.load(); files > seen && !spills.compare_exchange_weak(seen, files);) {}
        for (auto const& item : c_.chain.snapshot) {
            auto key = text(item.key);
            if (!request->marker().empty() && key <= request->marker()) continue;
            if (!request->end_marker().empty() && key >= request->end_marker()) continue;
            // A full page, with more to come: the next starts after its last key.
            if (page && static_cast<std::size_t>(response->ledger_objects().objects_size()) == page) {
                response->set_marker(response->ledger_objects().objects().rbegin()->key());
                break;
            }
            auto* object = response->mutable_ledger_objects()->add_objects();
            object->set_key(key);
            object->set_data(text(item.object));
        }
        response->set_is_unlimited(true);
        return grpc::Status::OK;
    }
    // The files of state pages read ahead this process holds open (the export unlinks each as it
    // creates it, so only its descriptors show it).
    // Files without a name: an O_TMPFILE one shows as "#<inode> (deleted)", one unlinked once
    // created (where the file system lacks O_TMPFILE) by its name and " (deleted)".
    static unsigned spill_files() {
        unsigned files = 0;
        std::error_code ec;
        for (fs::directory_iterator it("/proc/self/fd", ec), end; !ec && it != end; it.increment(ec)) {
            std::error_code unread;
            auto name = fs::read_symlink(it->path(), unread).filename().string();
            if (!unread && (name.starts_with(".state-ahead-") || (name.starts_with('#') && name.ends_with(" (deleted)")))) ++files;
        }
        return files;
    }
};
struct Server {
    Xrpld service;
    std::unique_ptr<grpc::Server> server;
    int port = 0;
    Server(Chain const& c, std::set<std::uint32_t> omit, std::set<std::uint32_t> alter = {}, std::set<std::uint32_t> missing = {},
           std::set<std::uint32_t> stale = {})
        : service(c, std::move(omit), std::move(alter), std::move(missing), std::move(stale)) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
    }
    ~Server() { server->Shutdown(); }
    std::string source() const { return "grpc://127.0.0.1:" + std::to_string(port); }
};
// A Clio node's JSON-RPC API over the chain, as an export reads it: `ledger` with the ledger's
// transactions and its object diff.
class Clio {
    Chain const& c_;
    net::io_context io_;
    net::ip::tcp::acceptor acceptor_{io_, {net::ip::make_address("127.0.0.1"), 0}};
    std::mutex mutex_;
    std::vector<std::shared_ptr<net::ip::tcp::socket>> sockets_;
    std::vector<std::thread> connections_;
    std::atomic<bool> stopping_{false};
    std::thread accepting_;
    Json answer(Json const& request) {
        auto const& params = request.at("params").at(0);
        if (request.at("method") != "ledger" || !params.at("ledger_index").is_number_unsigned()) return {{"error", "notSupported"}};
        auto seq = params.at("ledger_index").get<std::uint32_t>();
        if (seq < c_.first() || seq > c_.last()) return {{"error", "lgrNotFound"}};
        auto const& ledger = c_.chain.ledgers[seq - c_.first()];
        Json transactions = Json::array(), diff = Json::array();
        for (auto const& tx : ledger.transactions) transactions.push_back({{"tx_blob", to_hex(tx.transaction)}, {"meta", to_hex(tx.metadata)}});
        for (auto const& patch : ledger.patches) diff.push_back({{"object_id", to_hex(patch.key)}, {"object", to_hex(patch.object)}});
        ++ledgers;
        return {{"ledger", {{"ledger_data", to_hex(ledger.header)}, {"transactions", transactions}, {"diff", diff}}},
                {"ledger_hash", decode_header(ledger.header).hash}, {"ledger_index", seq}, {"validated", true}, {"status", "success"}};
    }
    void serve(std::shared_ptr<net::ip::tcp::socket> socket) {
        boost::beast::flat_buffer buffer;
        for (;;) {
            http::request<http::string_body> request;
            boost::system::error_code ec;
            http::read(*socket, buffer, request, ec);
            if (ec) return;
            http::response<http::string_body> response{http::status::ok, request.version()};
            response.set(http::field::content_type, "application/json");
            response.keep_alive(request.keep_alive());
            response.body() = Json{{"result", answer(Json::parse(request.body()))}}.dump();
            response.prepare_payload();
            http::write(*socket, response, ec);
            if (ec || !request.keep_alive()) return;
        }
    }
public:
    std::atomic<unsigned> ledgers{0};
    explicit Clio(Chain const& c) : c_(c) {
        accepting_ = std::thread([this] {
            for (;;) {
                auto socket = std::make_shared<net::ip::tcp::socket>(io_);
                boost::system::error_code ec;
                acceptor_.accept(*socket, ec);
                if (ec || stopping_) return;
                std::lock_guard lock(mutex_);
                sockets_.push_back(socket);
                connections_.emplace_back([this, socket] { serve(socket); });
            }
        });
    }
    ~Clio() {
        stopping_ = true;
        {   // wakes the accepting thread
            net::ip::tcp::socket wake(io_); boost::system::error_code ec;
            wake.connect({net::ip::make_address("127.0.0.1"), acceptor_.local_endpoint().port()}, ec);
        }
        accepting_.join();
        std::lock_guard lock(mutex_);
        for (auto& socket : sockets_) { boost::system::error_code ec; socket->shutdown(net::ip::tcp::socket::shutdown_both, ec); }
        for (auto& connection : connections_) connection.join();
    }
    std::string source() const { return "http://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) + "/"; }
};
ExportOptions options(Chain const& c, Server const& s, fs::path const& out) {
    ExportOptions o;
    o.source = s.source();
    o.range = {c.first(), c.last()};
    o.output = out;
    o.signing_key = c.dir / "signing.seed";
    if (!fs::exists(o.signing_key)) create_signing_key(o.signing_key);
    o.delay_ms = 0;
    o.jobs = 2;
    o.rpc_cache = false;
    return o;
}
// Every file of an archive except its signatures and timings: identical archives have identical files.
std::map<std::string, std::string> contents(fs::path const& archive) {
    std::map<std::string, std::string> files;
    for (auto const& entry : fs::recursive_directory_iterator(archive)) {
        if (!entry.is_regular_file()) continue;
        auto name = fs::relative(entry.path(), archive).generic_string();
        if (name.starts_with(".") || name.find("/.") != std::string::npos) continue;
        std::ifstream in(entry.path(), std::ios::binary);
        files[name] = std::string(std::istreambuf_iterator<char>(in), {});
    }
    return files;
}
}

TEST(HistoryPeers, OmittedTransactionsComeFromPeersAndTheArchiveIsTheSame) {
    Chain c;
    // Two ledgers without transactions, one whose list is not the ledger's.
    Server complete(c, {}), omitting(c, {c.first() + 5, c.first() + 30}, {c.first() + 12});
    auto expected = c.dir / "expected", actual = c.dir / "actual";
    export_archive(options(c, complete, expected));
    auto o = options(c, omitting, actual);
    o.history_peers = {c.peer()};
    export_archive(o);
    auto a = contents(expected), b = contents(actual);
    ASSERT_FALSE(a.empty());
    // Timings and the signed manifest's times differ; every ledger chunk is byte-identical.
    std::size_t chunks = 0;
    for (auto const& [name, bytes] : a) {
        if (!name.starts_with("chunks/")) continue;
        ++chunks;
        ASSERT_TRUE(b.contains(name)) << name;
        EXPECT_EQ(bytes, b.at(name)) << name;
    }
    EXPECT_GT(chunks, 0u);
}

TEST(HistoryPeers, ALedgerTheNodeLacksComesFromPeersAndTheArchiveIsTheSame) {
    Chain c;
    // The node lacks two ledgers; each and the one after it (undiffable without its parent) come from peers.
    Server complete(c, {}), holed(c, {}, {}, {c.first() + 9, c.first() + 27});
    auto expected = c.dir / "expected", actual = c.dir / "actual";
    export_archive(options(c, complete, expected));
    auto o = options(c, holed, actual);
    o.history_peers = {c.peer()};
    export_archive(o);
    auto a = contents(expected), b = contents(actual);
    std::size_t chunks = 0;
    for (auto const& [name, bytes] : a) {
        if (!name.starts_with("chunks/")) continue;
        ++chunks;
        ASSERT_TRUE(b.contains(name)) << name;
        EXPECT_EQ(bytes, b.at(name)) << name;
    }
    EXPECT_GT(chunks, 0u);
    // Without history peers the export stops there.
    EXPECT_ANY_THROW(export_archive(options(c, holed, c.dir / "stops")));
}

TEST(HistoryPeers, AnObjectTheSourceGetsWrongIsFoundByComparingStateTreesAndTheArchiveIsTheSame) {
    Chain c;
    // The ledger's state then does not match its header; its state tree, from peers, gives every change.
    Server complete(c, {}), spoiling(c, {}, {}, {}, {c.first() + 7, c.first() + 33});
    auto expected = c.dir / "expected", actual = c.dir / "actual";
    export_archive(options(c, complete, expected));
    auto o = options(c, spoiling, actual);
    o.history_peers = {c.peer()};
    export_archive(o);
    auto a = contents(expected), b = contents(actual);
    std::size_t chunks = 0;
    for (auto const& [name, bytes] : a) {
        if (!name.starts_with("chunks/")) continue;
        ++chunks;
        ASSERT_TRUE(b.contains(name)) << name;
        EXPECT_EQ(bytes, b.at(name)) << name;
    }
    EXPECT_GT(chunks, 0u);
    // Without history peers the export stops at the first, as it may not publish a wrong state.
    try {
        export_archive(options(c, spoiling, c.dir / "stops"));
        FAIL() << "the export went past a ledger whose state does not match";
    } catch (std::exception const& error) {
        std::string what = error.what();
        EXPECT_NE(what.find("ledger " + std::to_string(c.first() + 7)), std::string::npos) << what;
        EXPECT_NE(what.find("root mismatch"), std::string::npos) << what;
    }
}

TEST(SourceSplit, LedgersFromAJsonRpcAndAGrpcNodeAtOnceMakeTheSameArchive) {
    Chain c;
    Server grpc(c, {});
    Clio clio(c);
    auto expected = c.dir / "expected", actual = c.dir / "actual";
    export_archive(options(c, grpc, expected));
    grpc.service.ledgers = 0;
    // Two connections to the JSON-RPC node and two to the gRPC node.
    auto o = options(c, grpc, actual);
    o.source = clio.source(); o.grpc_source = grpc.source(); o.grpc_jobs = 2; o.jobs = 2;
    export_archive(o);
    auto a = contents(expected), b = contents(actual);
    std::size_t chunks = 0;
    for (auto const& [name, bytes] : a) {
        if (!name.starts_with("chunks/")) continue;
        ++chunks;
        ASSERT_TRUE(b.contains(name)) << name;
        EXPECT_EQ(bytes, b.at(name)) << name;
    }
    EXPECT_GT(chunks, 0u);
    // Both nodes sent ledgers, and together each ledger once.
    EXPECT_GT(clio.ledgers.load(), 0u);
    EXPECT_GT(grpc.service.ledgers.load(), 0u);
    EXPECT_EQ(clio.ledgers.load() + grpc.service.ledgers.load(), c.last() - c.first() + 1);
    // Without --grpc-source there is nothing to split with.
    auto lone = options(c, grpc, c.dir / "lone");
    lone.source = clio.source(); lone.grpc_jobs = 2;
    EXPECT_ANY_THROW(export_archive(lone));
}

// A base state read on several connections, each range read ahead of its turn: past the memory
// budget (XRPLA_TEST_STATE_AHEAD_BYTES; with none, every page of a range not yet taken goes to
// its file, with a few pages', memory and files alternate) a range's pages wait in a file of its
// own, and the archive is the one a single connection writes with all in memory.
TEST(StateReadAhead, PagesPastTheBudgetWaitInFilesAndTheArchiveIsTheSame) {
    Chain c(2000);
    Server grpc(c, {});
    grpc.service.page = 16;  // some 16 pages a range on 8 connections
    auto archive = [&](unsigned jobs, char const* budget, std::string const& name) {
        if (budget) ::setenv("XRPLA_TEST_STATE_AHEAD_BYTES", budget, 1);
        auto out = c.dir / name;
        auto o = options(c, grpc, out);
        o.jobs = jobs;
        grpc.service.spills = 0;
        export_archive(o);
        ::unsetenv("XRPLA_TEST_STATE_AHEAD_BYTES");
        return contents(out);
    };
    auto expected = archive(1, nullptr, "expected");
    EXPECT_EQ(grpc.service.spills.load(), 0u);
    for (unsigned jobs : {1u, 4u, 8u})
        for (char const* budget : {static_cast<char const*>(nullptr), "0", "8000"}) {
            SCOPED_TRACE(std::to_string(jobs) + " connections, budget " + (budget ? budget : "default"));
            auto actual = archive(jobs, budget, std::to_string(jobs) + "-" + (budget ? budget : "default"));
            // Without other ranges (one connection) or within the default budget nothing waits in
            // a file; with none, the ranges after the first do.
            if (jobs == 1 || !budget) EXPECT_EQ(grpc.service.spills.load(), 0u);
            else if (std::string_view(budget) == "0") EXPECT_GT(grpc.service.spills.load(), 0u);
            std::size_t compared = 0;
            for (auto const& [name, bytes] : expected) {
                if (!name.starts_with("chunks/") && !name.starts_with("snapshots/")) continue;
                ++compared;
                ASSERT_TRUE(actual.contains(name)) << name;
                EXPECT_EQ(bytes, actual.at(name)) << name;
            }
            EXPECT_GT(compared, 0u);
        }
}

// Once the files would take more than their room (XRPLA_TEST_STATE_SPILL_BYTES; by default half
// the space free on their file system when the download starts), as once one cannot be created
// or written (a full disk), no page goes to a file again: every reader waits for room in the
// budget, the pages already in files are read back in turn, and the archive is the same.
TEST(StateReadAhead, PastTheFilesRoomReadersWaitForTheBudgetAndTheArchiveIsTheSame) {
    Chain c(2000);
    Server grpc(c, {});
    grpc.service.page = 16;
    auto archive = [&](unsigned jobs, char const* room, std::string const& name) {
        if (room) {
            ::setenv("XRPLA_TEST_STATE_AHEAD_BYTES", "0", 1);
            ::setenv("XRPLA_TEST_STATE_SPILL_BYTES", room, 1);
        }
        auto out = c.dir / name;
        auto o = options(c, grpc, out);
        o.jobs = jobs;
        grpc.service.spills = 0;
        export_archive(o);
        ::unsetenv("XRPLA_TEST_STATE_AHEAD_BYTES");
        ::unsetenv("XRPLA_TEST_STATE_SPILL_BYTES");
        return contents(out);
    };
    auto expected = archive(1, nullptr, "expected");
    for (unsigned jobs : {4u, 8u})
        for (char const* room : {"1", "20000"}) {
            SCOPED_TRACE(std::to_string(jobs) + " connections, room " + room);
            auto actual = archive(jobs, room, std::to_string(jobs) + "-" + room);
            // No page fits in a byte: no file is ever created. Some fit in 20,000 bytes.
            if (std::string_view(room) == "1") EXPECT_EQ(grpc.service.spills.load(), 0u);
            else EXPECT_GT(grpc.service.spills.load(), 0u);
            std::size_t compared = 0;
            for (auto const& [name, bytes] : expected) {
                if (!name.starts_with("chunks/") && !name.starts_with("snapshots/")) continue;
                ++compared;
                ASSERT_TRUE(actual.contains(name)) << name;
                EXPECT_EQ(bytes, actual.at(name)) << name;
            }
            EXPECT_GT(compared, 0u);
        }
    // A setting that is not a number of bytes is refused, by its name.
    ::setenv("XRPLA_TEST_STATE_AHEAD_BYTES", "lots", 1);
    auto o = options(c, grpc, c.dir / "refused");
    o.jobs = 4;
    try {
        export_archive(o);
        ADD_FAILURE() << "the export took a budget that is not a number";
    } catch (std::exception const& error) {
        EXPECT_NE(std::string(error.what()).find("XRPLA_TEST_STATE_AHEAD_BYTES"), std::string::npos) << error.what();
    }
    ::unsetenv("XRPLA_TEST_STATE_AHEAD_BYTES");
}

TEST(HistoryPeers, WithoutHistoryPeersTheExportStopsAtTheLedgerAndSaysWhy) {
    Chain c;
    Server omitting(c, {c.first() + 5});
    try {
        export_archive(options(c, omitting, c.dir / "archive"));
        FAIL() << "the export went past a ledger without its transactions";
    } catch (std::exception const& error) {
        std::string what = error.what();
        EXPECT_NE(what.find("ledger " + std::to_string(c.first() + 5)), std::string::npos) << what;
        EXPECT_NE(what.find("--history-peers"), std::string::npos) << what;
    }
}

// The transactions of mainnet ledgers 625,681 and 625,753 (May 2013) as full-history peers
// hold them: current xrpld cannot decode the first (an OfferCreate with a field since removed)
// nor the second's metadata, so its APIs send neither. Every step from verification to the
// restore row builders and the query indexes reads them raw.
namespace {
struct Old {
    std::uint32_t sequence;
    Header header;
    std::vector<TransactionRecord> transactions;
};
Old old_ledger(char const* name) {
    std::ifstream in(fs::path(XRPLA_TEST_FIXTURES) / "undecodable" / name);
    auto d = Json::parse(in);
    Old l{d.at("sequence").get<std::uint32_t>(), decode_header(from_hex(d.at("header").get<std::string>())), {}};
    for (auto const& t : d.at("list")) l.transactions.push_back({from_hex(t.at("hex").get<std::string>()), from_hex(t.at("metadata_hex").get<std::string>())});
    return l;
}
}
TEST(UndecodableHistory, TransactionsOfferedByPeersAreTheLedgersAndEveryIndexReadsThem) {
    for (auto name : {"625681.json", "625753.json", "6409247.json"}) {
        auto l = old_ledger(name);
        SCOPED_TRACE(l.sequence);
        EXPECT_EQ(l.header.sequence, l.sequence);
        ASSERT_FALSE(l.transactions.empty());
        // The bytes are the ledger's: they hash to its transaction root.
        EXPECT_EQ(transaction_root(l.transactions), l.header.transaction_hash);
        LedgerRecord record{l.sequence, l.header.raw, l.transactions, {}};
        for (auto const& tx : l.transactions) {
            EXPECT_NO_THROW(wire::parse(tx.transaction));       // read by type code
            EXPECT_NO_THROW(wire::parse(tx.metadata));
            EXPECT_NO_THROW(checks::transaction(tx));           // the verifier's check
            EXPECT_NO_THROW(nft_ids(tx));                       // piece and live query indexes
            EXPECT_NO_THROW(nft_changes(tx));
        }
        // The xrpld (Transactions, AccountTransactions) and Clio (transactions, account_tx) rows.
        auto indexes = index_transactions(record);
        ASSERT_EQ(indexes.size(), l.transactions.size());
        for (auto const& index : indexes) {
            EXPECT_EQ(index.hash.size(), 32u);
            EXPECT_FALSE(index.accounts.empty());
            EXPECT_EQ(index.account.size(), 20u);
        }
    }
    // Why the node's APIs omit it: libxrpl cannot decode the OfferCreate.
    EXPECT_ANY_THROW(transaction_json(old_ledger("625681.json").transactions.front()));
    // And why xrpld no longer loads ledger 6,409,247 at all: the Payment whose Memos holds an
    // element that is not an object.
    auto memo = old_ledger("6409247.json").transactions.at(1);
    EXPECT_ANY_THROW(transaction_json(memo));
    auto fields = wire::parse(memo.transaction);
    auto memos = std::find_if(fields.begin(), fields.end(), [](wire::Field const& f) { return f.type == 15 && f.code == 9; });
    ASSERT_NE(memos, fields.end());
    ASSERT_EQ(memos->fields.size(), 1u);
    EXPECT_EQ(memos->fields[0].type, 7);   // MemoType, a blob field, naming an empty object
    EXPECT_EQ(memos->fields[0].code, 12);
}
// Which of these ledgers xrpld 3.4.1 holds. Each has a transaction libxrpl 3.4.1 cannot read
// (625,681's OfferCreate, 625,753's metadata, 6,409,247's Payment), which fails xrpld's save of a
// ledger (AcceptedLedger reads every transaction). The 2013 two fail only on XRP amounts with the
// 0x20 bit, which the rippled that first saved them read (libxrpl reads that bit as MPT's since):
// full-history xrpld serves those ledgers, their transactions unreadable rows. 6,409,247's Memos
// element that is not an object was refused by every version since: xrpld has no such ledger.
TEST(UndecodableHistory, LedgersXrpldHolds) {
    for (auto name : {"625681.json", "625753.json", "6409247.json"}) {
        auto l = old_ledger(name);
        SCOPED_TRACE(l.sequence);
        EXPECT_EQ(rpc::ledger_held_by_xrpld(l.transactions), l.sequence != 6409247);
        // None of them is read whole: the ledger listing stops at the first unreadable one.
        EXPECT_FALSE(std::all_of(l.transactions.begin(), l.transactions.end(), rpc::read_by_xrpld));
    }
    auto refused = old_ledger("6409247.json").transactions;
    EXPECT_TRUE(rpc::read_by_xrpld(refused.at(0)));
    EXPECT_FALSE(rpc::read_by_xrpld(refused.at(1)));  // "Non-object in array"
    EXPECT_TRUE(rpc::read_by_xrpld(refused.at(2)));
    EXPECT_FALSE(rpc::read_by_xrpld(old_ledger("625681.json").transactions.at(0)));  // "Unknown field"
    EXPECT_FALSE(rpc::read_by_xrpld(old_ledger("625753.json").transactions.at(0)));  // its metadata
}
