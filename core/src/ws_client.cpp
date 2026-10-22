// A blocking WebSocket client over Boost.Beast (ws_client.hpp).
#include "ws_client.hpp"
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <openssl/ssl.h>
#include <algorithm>
#include <optional>
#include <regex>
#include <variant>

namespace xrpla::core {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace ws = beast::websocket;
using tcp = net::ip::tcp;

struct WsClient::Impl {
    net::io_context io;
    net::ssl::context tls{net::ssl::context::tls_client};
    std::variant<std::monostate, ws::stream<tcp::socket>, ws::stream<beast::ssl_stream<tcp::socket>>> stream;
    std::chrono::milliseconds timeout;
    template <class F> void with_stream(F&& f) {
        std::visit([&](auto& s) { if constexpr (!std::is_same_v<std::decay_t<decltype(s)>, std::monostate>) f(s); }, stream);
    }
    // Runs the operation `start` begins asynchronously for at most `limit`; one still pending
    // then is ended by closing the socket, and the connection is left for the caller to make
    // again. A peer that accepts the connection but never answers (a handshake, a read, a write
    // into a full window) thus never holds a thread for ever.
    template <class Start> void bounded(std::chrono::milliseconds limit, std::string const& what, Start&& start) {
        std::optional<beast::error_code> done;
        start([&](beast::error_code const& error, auto const&...) { done = error; });
        io.restart();
        io.run_for(limit);
        if (!done) {
            with_stream([](auto& s) { beast::error_code ignored; beast::get_lowest_layer(s).close(ignored); });
            io.restart();
            io.run();  // the operation, aborted
            throw Error(what + " for " + std::to_string(limit.count()) + " ms");
        }
        if (*done) throw beast::system_error(*done);
    }
    // Connects to `url` (to `address` in place of its host's addresses, when given): the TCP, TLS
    // and WebSocket handshakes all within `limit`, one deadline for the whole.
    void open(std::string const& url, std::string const& address, std::chrono::milliseconds limit) {
        static std::regex const pattern(R"(^(wss?)://([^/:]+|\[[^\]]+\])(?::(\d+))?(/.*)?$)");
        std::smatch m;
        if (!std::regex_match(url, m, pattern)) throw Error("WebSocket URL must be ws://HOST[:PORT][/PATH] or wss://...");
        bool secure = m[1] == "wss";
        std::string host = m[2], port = m[3].matched ? std::string(m[3]) : (secure ? "443" : "80"), path = m[4].matched ? std::string(m[4]) : "/";
        timeout = limit;
        auto until = std::chrono::steady_clock::now() + limit;
        auto left = [&] { return std::max(std::chrono::milliseconds(1), std::chrono::duration_cast<std::chrono::milliseconds>(until - std::chrono::steady_clock::now())); };
        tcp::resolver resolver(io);
        auto endpoints = address.empty() ? resolver.resolve(host, port) : resolver.resolve(address, port, tcp::resolver::numeric_host | tcp::resolver::numeric_service);
        auto connect = [&](auto& s) {
            bounded(left(), "No connection to " + url, [&](auto handler) { net::async_connect(beast::get_lowest_layer(s), endpoints, handler); });
        };
        auto handshake = [&](auto& s) {
            bounded(left(), "No WebSocket handshake from " + url, [&](auto handler) { s.async_handshake(host, path, handler); });
        };
        if (secure) {
            tls.set_default_verify_paths();
            tls.set_verify_mode(net::ssl::verify_peer);
            // The certificate names the host without the brackets an IPv6 address has in a URL.
            auto name = host.starts_with('[') && host.ends_with(']') ? host.substr(1, host.size() - 2) : host;
            auto& s = stream.emplace<ws::stream<beast::ssl_stream<tcp::socket>>>(io, tls);
            if (!SSL_set_tlsext_host_name(s.next_layer().native_handle(), name.c_str())) throw Error("Cannot set TLS server name");
            s.next_layer().set_verify_callback(net::ssl::host_name_verification(name));
            connect(s);
            bounded(left(), "No TLS handshake from " + url, [&](auto handler) { s.next_layer().async_handshake(net::ssl::stream_base::client, handler); });
            handshake(s);
        } else {
            auto& s = stream.emplace<ws::stream<tcp::socket>>(io);
            connect(s);
            handshake(s);
        }
    }
};

namespace {
// A message as JSON. A parse error quotes the bytes it met, which need not be text (a binary
// frame): it is given as one saying only that the message is not JSON, so it can be logged
// (in JSON) as any other error.
Json parsed(beast::flat_buffer const& buffer) {
    try { return Json::parse(beast::buffers_to_string(buffer.data())); }
    catch (Json::parse_error const&) { throw Error("A WebSocket message that is not JSON"); }
}
}
WsClient::WsClient(std::string const& url, std::chrono::milliseconds timeout) : impl_(std::make_unique<Impl>()) { impl_->open(url, {}, timeout); }
WsClient::WsClient(std::string const& url, std::string const& address, std::chrono::milliseconds timeout) : impl_(std::make_unique<Impl>()) {
    impl_->open(url, address, timeout);
}
WsClient::~WsClient() = default;
void WsClient::read_message_max(std::size_t bytes) {
    impl_->with_stream([&](auto& s) { s.read_message_max(bytes); });
}
void WsClient::send(Json const& message) {
    auto text = message.dump();
    impl_->with_stream([&](auto& s) {
        s.text(true);
        impl_->bounded(impl_->timeout, "WebSocket message not sent", [&](auto handler) { s.async_write(net::buffer(text), handler); });
    });
}
Json WsClient::receive() {
    beast::flat_buffer buffer;
    impl_->with_stream([&](auto& s) { s.read(buffer); });
    return parsed(buffer);
}
Json WsClient::receive(std::chrono::milliseconds idle) {
    // Control frames, pings among them, are answered while the read waits.
    beast::flat_buffer buffer;
    impl_->with_stream([&](auto& s) {
        impl_->bounded(idle, "No WebSocket message", [&](auto handler) { s.async_read(buffer, handler); });
    });
    return parsed(buffer);
}
}
