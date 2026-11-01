#pragma once
#include <xrpla/core/verify.hpp>
#include <chrono>
#include <memory>

namespace xrpla::core {
// A blocking JSON WebSocket client for xrpld's API (ws:// or wss:// with certificate and host
// name checks), used to follow streams such as validations.
class WsClient {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    // Connecting (TCP, TLS and WebSocket handshakes, together; not the host's name lookup) and
    // each send take at most `timeout`, or end in an Error: a node that accepts the connection
    // but never answers is left.
    explicit WsClient(std::string const& url, std::chrono::milliseconds timeout = std::chrono::seconds(30));
    // The same, connecting to `address` (an IP address) in place of the addresses the URL's host
    // has: no name is looked up, and the host still names the server for TLS and the handshake.
    WsClient(std::string const& url, std::string const& address, std::chrono::milliseconds timeout);
    ~WsClient();
    // The largest message received (Beast's default: 16 MB); a larger one ends the read in an
    // error.
    void read_message_max(std::size_t bytes);
    void send(Json const& message);
    // The next message; one that is not JSON is an Error.
    Json receive();
    // The next message, or an Error once `idle` passes without one: a connection gone half-open
    // (the server gone without a FIN or RST reaching here) never ends a read by itself. The
    // connection is closed then, and the caller connects again.
    Json receive(std::chrono::milliseconds idle);
};
struct RpcClient;
// One JSON-RPC request on `url` (ws:// or wss://), returned as RpcClient::call returns it (the
// result object; an error is that object's "error"). The connection is this thread's, kept and
// used again for the same url. `client` supplies the pause, the retries and the cache.
Json ws_rpc_call(RpcClient const& client, std::string const& method, Json const& params);
}
