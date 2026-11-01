// A ledger the first source does not hold is read from the next, over HTTP or WebSocket.
// entryNotFound is the answer from the source that holds the ledger, and is not asked again.
#include <xrpla/core/operations.hpp>
#include <gtest/gtest.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <atomic>
#include <thread>

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
    std::thread thread_;
    Json answer_;
public:
    std::atomic<unsigned> hits{0};
    explicit HttpNode(Json answer) : answer_(std::move(answer)) {
        thread_ = std::thread([this] {
            while (!stopping_) {
                tcp::socket socket(io_);
                boost::system::error_code ec;
                acceptor_.accept(socket, ec);
                if (ec || stopping_) return;
                boost::beast::flat_buffer buffer;
                for (;;) {
                    http::request<http::string_body> request;
                    http::read(socket, buffer, request, ec);
                    if (ec) break;
                    ++hits;
                    http::response<http::string_body> response{http::status::ok, request.version()};
                    response.set(http::field::content_type, "application/json");
                    response.keep_alive(request.keep_alive());
                    response.body() = Json{{"result", answer_}}.dump();
                    response.prepare_payload();
                    http::write(socket, response, ec);
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
        thread_.join();
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) + "/"; }
};

class WsNode {
    net::io_context io_;
    tcp::acceptor acceptor_{io_, {net::ip::make_address("127.0.0.1"), 0}};
    std::atomic<bool> stopping_{false};
    std::thread thread_;
    Json result_;
public:
    std::atomic<unsigned> hits{0};
    explicit WsNode(Json result) : result_(std::move(result)) {
        thread_ = std::thread([this] {
            tcp::socket socket(io_);
            boost::system::error_code ec;
            acceptor_.accept(socket, ec);
            if (ec || stopping_) return;
            websocket::stream<tcp::socket> stream(std::move(socket));
            stream.accept(ec);
            if (ec) return;
            boost::beast::flat_buffer buffer;
            stream.read(buffer, ec);
            if (ec) return;
            ++hits;
            auto request = Json::parse(boost::beast::buffers_to_string(buffer.data()));
            auto body = Json{{"id", request.at("id")}, {"status", "success"}, {"type", "response"}, {"result", result_}}.dump();
            stream.text(true);
            stream.write(net::buffer(body), ec);
        });
    }
    ~WsNode() {
        stopping_ = true;
        boost::system::error_code ec;
        tcp::socket wake(io_);
        wake.connect({net::ip::make_address("127.0.0.1"), acceptor_.local_endpoint().port()}, ec);
        thread_.join();
    }
    std::string url() const { return "ws://127.0.0.1:" + std::to_string(acceptor_.local_endpoint().port()) + "/"; }
};
}

TEST(Sources, AMissingLedgerIsReadFromTheNextSource) {
    HttpNode shallow(Json{{"error", "lgrNotFound"}, {"error_message", "ledgerNotFound"}});
    HttpNode deep(Json{{"kept", true}, {"ledger_index", 9}});
    auto result = call_sources({shallow.url(), deep.url()}, "ledger", {{"ledger_index", 9}}, 0, 1, {});
    EXPECT_TRUE(result.at("kept").get<bool>());
    EXPECT_EQ(result.at("ledger_index").get<int>(), 9);
    EXPECT_GE(shallow.hits.load(), 1u);
    EXPECT_GE(deep.hits.load(), 1u);
}

TEST(Sources, AnAbsentObjectIsNotAskedOfTheNextSource) {
    HttpNode primary(Json{{"error", "entryNotFound"}});
    HttpNode other(Json{{"kept", true}});
    auto result = call_sources({primary.url(), other.url()}, "ledger_entry", {{"ledger_index", 9}, {"index", "00"}}, 0, 1, {});
    EXPECT_EQ(result.at("error").get<std::string>(), "entryNotFound");
    EXPECT_EQ(other.hits.load(), 0u);
}

TEST(Sources, AWebSocketFallbackServesTheLedger) {
    HttpNode shallow(Json{{"error", "lgrNotFound"}});
    WsNode deep(Json{{"kept", true}, {"ledger_index", 4}});
    auto result = call_sources({shallow.url(), deep.url()}, "ledger", {{"ledger_index", 4}}, 0, 1, {});
    EXPECT_TRUE(result.at("kept").get<bool>());
    EXPECT_EQ(result.at("ledger_index").get<int>(), 4);
    EXPECT_EQ(deep.hits.load(), 1u);
}
