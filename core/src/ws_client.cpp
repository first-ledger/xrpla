// A blocking WebSocket client over Boost.Beast (ws_client.hpp).
#include <xrpla/core/operations.hpp>
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
#include <thread>
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
        // A ledger with its transactions is larger than the 16 MB a stream accepts by default.
        auto widen = [](auto& s) { s.read_message_max(512ull * 1024 * 1024); };
        if (secure) {
            tls.set_default_verify_paths();
            tls.set_verify_mode(net::ssl::verify_peer);
            // The certificate names the host without the brackets an IPv6 address has in a URL.
            auto name = host.starts_with('[') && host.ends_with(']') ? host.substr(1, host.size() - 2) : host;
            auto& s = stream.emplace<ws::stream<beast::ssl_stream<tcp::socket>>>(io, tls);
            widen(s);
            if (!SSL_set_tlsext_host_name(s.next_layer().native_handle(), name.c_str())) throw Error("Cannot set TLS server name");
            s.next_layer().set_verify_callback(net::ssl::host_name_verification(name));
            connect(s);
            bounded(left(), "No TLS handshake from " + url, [&](auto handler) { s.next_layer().async_handshake(net::ssl::stream_base::client, handler); });
            handshake(s);
        } else {
            auto& s = stream.emplace<ws::stream<tcp::socket>>(io);
            widen(s);
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
namespace {
// This thread's connection to one WebSocket source. A prefetch fetches ledgers on several
// threads; each keeps its own, as RpcClient keeps a curl handle per thread.
struct WsSlot {
    std::string url;
    std::unique_ptr<WsClient> client;
    std::uint64_t next_id = 1;
};
thread_local WsSlot ws_slot;

bool same_id(Json const& message, std::uint64_t id) {
    if (!message.contains("id")) return false;
    auto const& got = message.at("id");
    if (got.is_number()) return got.get<std::uint64_t>() == id;
    if (got.is_string()) return got.get<std::string>() == std::to_string(id);
    return false;
}
// The result object RpcClient::call returns. A WebSocket success wraps it in "result"; an error
// (lgrNotFound, rate_limited) is often the message itself.
Json unwrap_rpc(Json const& message) {
    if (message.contains("result") && message.at("result").is_object()) {
        auto result = message.at("result");
        if (!result.contains("error") && message.contains("error") && message.at("error").is_string()) result["error"] = message.at("error");
        if (!result.contains("error_message") && message.contains("error_message")) result["error_message"] = message.at("error_message");
        else if (!result.contains("error_message") && message.contains("message") && message.at("message").is_string()) result["error_message"] = message.at("message");
        return result;
    }
    if (message.contains("error")) {
        Json result{{"error", message.value("error", "ws")}, {"status", message.value("status", "error")}};
        if (message.contains("error_message")) result["error_message"] = message.at("error_message");
        else if (message.contains("message")) result["error_message"] = message.at("message");
        return result;
    }
    return message;
}
bool retryable_result(Json const& result) {
    auto error = result.value("error", "");
    return error == "tooBusy" || error == "slowDown" || error == "rate_limited";
}
}
Json ws_rpc_call(RpcClient const& client, std::string const& method, Json const& params) {
    if (!params.is_object()) throw Error("WebSocket RPC parameters must be an object");
    auto body = Json{{"method", method}, {"params", Json::array({params})}}.dump();
    auto cached = client.cache / (sha256(client.url + body) + ".json");
    if (!client.cache.empty()) {
        safe_cache_path(cached);
        if (fs::exists(cached)) return Json::parse(read_file(cached, 512ULL * 1024 * 1024));
        fs::create_directories(client.cache);
    }
    unsigned tries = std::max(1u, client.attempts);
    for (unsigned attempt = 0; attempt < tries; ++attempt) {
        if (client.delay_ms || attempt) std::this_thread::sleep_for(std::chrono::milliseconds{client.delay_ms + (attempt ? 500U << attempt : 0)});
        try {
            if (!ws_slot.client || ws_slot.url != client.url) {
                ws_slot.client.reset();
                ws_slot.url = client.url;
                ws_slot.client = std::make_unique<WsClient>(client.url, std::chrono::seconds(30));
            }
            auto id = ws_slot.next_id++;
            Json message = params;
            message["command"] = method;
            message["id"] = id;
            ws_slot.client->send(message);
            Json result;
            bool matched = false;
            for (int skipped = 0; skipped < 8 && !matched; ++skipped) {
                auto incoming = ws_slot.client->receive(std::chrono::seconds(60));
                if (incoming.contains("id") && !same_id(incoming, id)) continue;
                result = unwrap_rpc(incoming);
                matched = true;
            }
            if (!matched) throw Error("WebSocket response did not match the request");
            if (retryable_result(result)) {
                if (attempt + 1 < tries) continue;
            }
            // The same ledger answers as HTTP: cached once validated, and an entryNotFound.
            if (!client.cache.empty() && (params.contains("ledger_hash") ||
                (params.contains("ledger_index") && params.at("ledger_index").is_number_unsigned())) &&
                (!result.contains("error") || result["error"] == "entryNotFound") &&
                (result.contains("validated") ? result["validated"] == true : method != "ledger")) {
                auto temp = cached; temp += ".part";
                write_file(temp, result.dump()); durable_rename(temp, cached);
            }
            return result;
        } catch (std::exception const&) {
            ws_slot.client.reset();
            if (attempt + 1 >= tries) throw;
        }
    }
    throw Error("WebSocket RPC retries exhausted");
}
}
