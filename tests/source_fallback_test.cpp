// A ledger the first source does not hold is read from the next, over HTTP or WebSocket.
// entryNotFound is the answer from the source that holds the ledger, and is not asked again.
#include <xrpla/core/operations.hpp>
#include <gtest/gtest.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <sys/socket.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>
#include <vector>

using namespace xrpla;
using namespace xrpla::core;
namespace {
namespace net = boost::asio;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = net::ip::tcp;

class HttpNode {
    net::io_context io_;
    tcp::acceptor acceptor_{io_, {net::ip::make_address("127.0.0.1"), 0}};
    std::atomic<bool> stopping_{false};
    std::mutex mutex_;
    std::vector<std::shared_ptr<tcp::socket>> sockets_;
    std::thread thread_;
    Json answer_;
    std::optional<Json> first_;
public:
    std::atomic<unsigned> hits{0};
    // `first`, when set, answers the first request only (a node busy for a moment).
    explicit HttpNode(Json answer, std::optional<Json> first = {}) : answer_(std::move(answer)), first_(std::move(first)) {
        thread_ = std::thread([this] {
            while (!stopping_) {
                auto socket = std::make_shared<tcp::socket>(io_);
                boost::system::error_code ec;
                acceptor_.accept(*socket, ec);
                if (ec || stopping_) return;
                {
                    std::lock_guard lock(mutex_);
                    sockets_.push_back(socket);
                }
                boost::beast::flat_buffer buffer;
                for (;;) {
                    http::request<http::string_body> request;
                    http::read(*socket, buffer, request, ec);
                    if (ec) break;
                    auto n = ++hits;
                    http::response<http::string_body> response{http::status::ok, request.version()};
                    response.set(http::field::content_type, "application/json");
                    response.keep_alive(request.keep_alive());
                    response.body() = Json{{"result", n == 1 && first_ ? *first_ : answer_}}.dump();
                    response.prepare_payload();
                    http::write(*socket, response, ec);
                    if (ec || !request.keep_alive()) break;
                }
            }
        });
    }
    ~HttpNode() {
        stopping_ = true;
        boost::system::error_code ec;
        tcp::socket wake(io_);
        wake.connect({net::ip::make_address("127.0.0.1"), acceptor_.local_endpoint().port()}, ec);
        std::vector<std::shared_ptr<tcp::socket>> sockets;
        {
            std::lock_guard lock(mutex_);
            sockets.swap(sockets_);
        }
        // libcurl keeps the connection. The read is on this socket, so shutting the descriptor
        // down is what lets the thread leave it. Closing the asio socket here deadlocks with that.
        for (auto& socket : sockets) {
            auto fd = socket->native_handle();
            if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
        }
        thread_.join();
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) + "/"; }
};

// Accepts every connection and answers every message, so a caller that keeps one connection per
// URL can be told from one that opens another whenever the URL changes. A second connection is
// accepted: a regression then fails the count, rather than waiting out the handshake.
class WsNode {
    net::io_context io_;
    tcp::acceptor acceptor_{io_, {net::ip::make_address("127.0.0.1"), 0}};
    std::atomic<bool> stopping_{false};
    std::mutex mutex_;
    std::vector<std::shared_ptr<websocket::stream<tcp::socket>>> streams_;
    std::vector<std::thread> sessions_;
    std::thread accepting_;
    Json result_;
    bool hang_up_;
    void serve(std::shared_ptr<websocket::stream<tcp::socket>> stream) {
        boost::system::error_code ec;
        stream->accept(ec);
        if (ec || stopping_) return;
        ++connections;
        boost::beast::flat_buffer buffer;
        while (!stopping_) {
            buffer.clear();
            stream->read(buffer, ec);
            if (ec) return;
            ++hits;
            auto request = Json::parse(boost::beast::buffers_to_string(buffer.data()));
            auto body = Json{{"id", request.at("id")}, {"status", "success"}, {"type", "response"}, {"result", result_}}.dump();
            stream->text(true);
            stream->write(net::buffer(body), ec);
            if (ec) return;
            // As a server that drops idle connections: the caller's kept connection is closed.
            if (hang_up_) { stream->close(websocket::close_code::going_away, ec); return; }
        }
    }
public:
    std::atomic<unsigned> hits{0};
    std::atomic<unsigned> connections{0};
    // `hang_up`: the connection is closed after each answer.
    explicit WsNode(Json result, bool hang_up = false) : result_(std::move(result)), hang_up_(hang_up) {
        accepting_ = std::thread([this] {
            for (;;) {
                tcp::socket socket(io_);
                boost::system::error_code ec;
                acceptor_.accept(socket, ec);
                if (ec || stopping_) return;
                auto stream = std::make_shared<websocket::stream<tcp::socket>>(std::move(socket));
                std::lock_guard lock(mutex_);
                streams_.push_back(stream);
                sessions_.emplace_back([this, stream] { serve(stream); });
            }
        });
    }
    ~WsNode() {
        stopping_ = true;
        boost::system::error_code ec;
        tcp::socket wake(io_);
        wake.connect({net::ip::make_address("127.0.0.1"), acceptor_.local_endpoint().port()}, ec);
        accepting_.join();
        std::vector<std::shared_ptr<websocket::stream<tcp::socket>>> streams;
        std::vector<std::thread> sessions;
        {
            std::lock_guard lock(mutex_);
            streams.swap(streams_);
            sessions.swap(sessions_);
        }
        // The asio socket is not safe to close while another thread is in read. Shutting the
        // native descriptor down makes that read return.
        for (auto& stream : streams) {
            auto fd = boost::beast::get_lowest_layer(*stream).native_handle();
            if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
        }
        for (auto& session : sessions) session.join();
    }
    std::string url() const { return "ws://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) + "/"; }
};

// Accepts a connection and closes it before any HTTP answer: reached, then not.
class ClosingNode {
    net::io_context io_;
    tcp::acceptor acceptor_{io_, {net::ip::make_address("127.0.0.1"), 0}};
    std::atomic<bool> stopping_{false};
    std::thread thread_;
public:
    std::atomic<unsigned> accepts{0};
    ClosingNode() {
        thread_ = std::thread([this] {
            while (!stopping_) {
                tcp::socket socket(io_);
                boost::system::error_code ec;
                acceptor_.accept(socket, ec);
                if (ec || stopping_) return;
                ++accepts;
                socket.close(ec);
            }
        });
    }
    ~ClosingNode() {
        stopping_ = true;
        boost::system::error_code ec;
        tcp::socket wake(io_);
        wake.connect({net::ip::make_address("127.0.0.1"), acceptor_.local_endpoint().port()}, ec);
        thread_.join();
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) + "/"; }
};

struct CaptureStderr {
    std::stringstream buffer;
    std::streambuf* previous = std::cerr.rdbuf(buffer.rdbuf());
    ~CaptureStderr() { std::cerr.rdbuf(previous); }
    std::string take() {
        auto text = buffer.str();
        buffer.str("");
        buffer.clear();
        return text;
    }
};
}

TEST(Sources, AMissingLedgerIsReadFromTheNextSource) {
    forget_unreachable_sources();
    HttpNode shallow(Json{{"error", "lgrNotFound"}, {"error_message", "ledgerNotFound"}});
    HttpNode deep(Json{{"kept", true}, {"ledger_index", 9}});
    auto result = call_sources({shallow.url(), deep.url()}, "ledger", {{"ledger_index", 9}}, 0, 1, {});
    EXPECT_TRUE(result.at("kept").get<bool>());
    EXPECT_EQ(result.at("ledger_index").get<int>(), 9);
    EXPECT_GE(shallow.hits.load(), 1u);
    EXPECT_GE(deep.hits.load(), 1u);
}

TEST(Sources, AnAbsentObjectIsNotAskedOfTheNextSource) {
    forget_unreachable_sources();
    HttpNode primary(Json{{"error", "entryNotFound"}});
    HttpNode other(Json{{"kept", true}});
    auto result = call_sources({primary.url(), other.url()}, "ledger_entry", {{"ledger_index", 9}, {"index", "00"}}, 0, 1, {});
    EXPECT_EQ(result.at("error").get<std::string>(), "entryNotFound");
    EXPECT_EQ(other.hits.load(), 0u);
}

TEST(Sources, AWebSocketFallbackServesTheLedger) {
    forget_unreachable_sources();
    HttpNode shallow(Json{{"error", "lgrNotFound"}});
    WsNode deep(Json{{"kept", true}, {"ledger_index", 4}});
    auto result = call_sources({shallow.url(), deep.url()}, "ledger", {{"ledger_index", 4}}, 0, 1, {});
    EXPECT_TRUE(result.at("kept").get<bool>());
    EXPECT_EQ(result.at("ledger_index").get<int>(), 4);
    EXPECT_EQ(deep.hits.load(), 1u);
}

TEST(Sources, EachWebSocketSourceKeepsItsConnection) {
    forget_unreachable_sources();
    WsNode missing(Json{{"error", "lgrNotFound"}});
    WsNode kept(Json{{"kept", true}, {"ledger_index", 7}});
    auto params = Json{{"ledger_index", 7}};
    // A, then B, then A again: one slot for every URL would reconnect on the way back.
    auto first = call_sources({missing.url(), kept.url()}, "ledger", params, 0, 1, {});
    EXPECT_TRUE(first.at("kept").get<bool>());
    auto second = call_sources({missing.url()}, "ledger", params, 0, 1, {});
    EXPECT_EQ(second.at("error").get<std::string>(), "lgrNotFound");
    auto third = call_sources({kept.url()}, "ledger", params, 0, 1, {});
    EXPECT_TRUE(third.at("kept").get<bool>());
    EXPECT_EQ(missing.connections.load(), 1u);
    EXPECT_EQ(kept.connections.load(), 1u);
    // lgrNotFound is an answer. The primary is asked again on the next ledger, on that connection.
    auto again = call_sources({missing.url(), kept.url()}, "ledger", params, 0, 1, {});
    EXPECT_TRUE(again.at("kept").get<bool>());
    EXPECT_EQ(missing.connections.load(), 1u);
    EXPECT_EQ(kept.connections.load(), 1u);
    EXPECT_GE(missing.hits.load(), 2u);
    EXPECT_GE(kept.hits.load(), 2u);
}

TEST(Sources, APrimaryThatAcceptsAndClosesIsSkippedForThirtySeconds) {
    forget_unreachable_sources();
    ClosingNode closed;
    HttpNode kept(Json{{"kept", true}, {"ledger_index", 2}});
    auto start = std::chrono::steady_clock::now();
    auto result = call_sources({closed.url(), kept.url()}, "ledger", {{"ledger_index", 2}}, 0, 5, {});
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(3));
    EXPECT_TRUE(result.at("kept").get<bool>());
    EXPECT_EQ(closed.accepts.load(), 1u);
    auto again = call_sources({closed.url(), kept.url()}, "ledger", {{"ledger_index", 3}}, 0, 5, {});
    EXPECT_TRUE(again.at("kept").get<bool>());
    EXPECT_EQ(closed.accepts.load(), 1u);
}

TEST(Sources, ARefusedPrimaryFailsOverWithoutTheBackoff) {
    forget_unreachable_sources();
    // Bound and not listening, so the port stays refused for the call and nothing else takes it.
    net::io_context io;
    tcp::socket reserved(io);
    reserved.open(tcp::v4());
    reserved.bind({net::ip::make_address("127.0.0.1"), 0});
    auto url = "http://127.0.0.1:" + std::to_string(reserved.local_endpoint().port()) + "/";
    HttpNode kept(Json{{"kept", true}, {"ledger_index", 3}});
    auto start = std::chrono::steady_clock::now();
    auto result = call_sources({url, kept.url()}, "ledger", {{"ledger_index", 3}}, 0, 5, {});
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(3));
    EXPECT_TRUE(result.at("kept").get<bool>());
    EXPECT_GE(kept.hits.load(), 1u);
}

TEST(Sources, AFallbackIsLoggedOnceWhenServingSwitches) {
    forget_unreachable_sources();
    HttpNode primary(Json{{"error", "lgrNotFound"}});
    HttpNode fallback(Json{{"kept", true}, {"ledger_index", 1}, {"validated", true}});
    CaptureStderr captured;
    call_sources({primary.url(), fallback.url()}, "ledger", {{"ledger_index", 1}}, 0, 1, {});
    auto first = captured.take();
    call_sources({primary.url(), fallback.url()}, "ledger", {{"ledger_index", 2}}, 0, 1, {});
    auto second = captured.take();
    EXPECT_NE(first.find("ledger_from_fallback"), std::string::npos) << first;
    EXPECT_EQ(second.find("ledger_from_fallback"), std::string::npos) << second;
}

TEST(Sources, AWebSocketPrimaryThatDroppedItsConnectionIsDialedAgain) {
    forget_unreachable_sources();
    WsNode primary(Json{{"kept", true}, {"ledger_index", 5}}, true);
    HttpNode fallback(Json{{"fallback", true}});
    // Each answer is followed by the server closing the connection this thread keeps. The next
    // request finds it closed: the primary is dialed again, not passed over for 30 seconds.
    for (int n = 0; n < 3; ++n) {
        auto result = call_sources({primary.url(), fallback.url()}, "ledger", {{"ledger_index", 5}}, 0, 5, {});
        EXPECT_TRUE(result.value("kept", false)) << result.dump();
    }
    EXPECT_EQ(primary.hits.load(), 3u);
    EXPECT_EQ(primary.connections.load(), 3u);
    EXPECT_EQ(fallback.hits.load(), 0u);
}

TEST(Sources, ABusyPrimaryIsAskedOnceMoreBeforeTheFallback) {
    forget_unreachable_sources();
    HttpNode primary(Json{{"kept", true}, {"ledger_index", 6}}, Json{{"error", "tooBusy"}});
    HttpNode fallback(Json{{"fallback", true}});
    auto result = call_sources({primary.url(), fallback.url()}, "ledger", {{"ledger_index", 6}}, 0, 5, {});
    EXPECT_TRUE(result.value("kept", false)) << result.dump();
    EXPECT_EQ(primary.hits.load(), 2u);
    EXPECT_EQ(fallback.hits.load(), 0u);
}

TEST(Sources, APrimaryStillBusyLeavesTheLedgerToTheFallback) {
    forget_unreachable_sources();
    HttpNode primary(Json{{"error", "tooBusy"}});
    HttpNode fallback(Json{{"fallback", true}});
    auto result = call_sources({primary.url(), fallback.url()}, "ledger", {{"ledger_index", 6}}, 0, 5, {});
    EXPECT_TRUE(result.value("fallback", false)) << result.dump();
    EXPECT_EQ(primary.hits.load(), 2u);
}

TEST(Sources, APrimaryStillSyncingLeavesTheTipToTheFallback) {
    forget_unreachable_sources();
    HttpNode primary(Json{{"error", "noNetwork"}, {"error_message", "InsufficientNetworkMode"}});
    HttpNode fallback(Json{{"fallback", true}});
    auto result = call_sources({primary.url(), fallback.url()}, "ledger", {{"ledger_index", "validated"}}, 0, 5, {});
    EXPECT_TRUE(result.value("fallback", false)) << result.dump();
    EXPECT_EQ(primary.hits.load(), 1u);
}
