#include <xrpla/core/operations.hpp>
#include "piece_history.hpp"
#include "live_history.hpp"
#include "rpc.hpp"
#include "peer.hpp"
#include "ws_client.hpp"
#include <xrpl/protocol/AccountID.h>
#include <xrpl/protocol/ErrorCodes.h>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <curl/curl.h>
#include <openssl/ssl.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <limits>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <regex>
#include <cctype>
#include <cstring>
#include <set>
#include <unordered_map>
#include <utility>
#include <variant>

namespace xrpla::core {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;
namespace {
Json error(::xrpl::ErrorCodeI code) {
    auto result = Json::parse(::xrpl::rpc::makeError(code).toStyledString()); result["status"] = "error"; return result;
}
struct Failure { Json value; };
// A request xrpld's json::Reader would not parse, with its message (getFormattedErrorMessages:
// "* Line L, Column C\n  what\n").
struct Unparsable : Error { using Error::Error; };
// A string's contents (between its quotes) as the Reader decodes them (decodeString), or nullopt
// where it would refuse them (nlohmann's parser then says why).
std::optional<std::string> reader_string(std::string_view body) {
    std::string out;
    auto hex = [&](std::size_t& i, unsigned& value) {
        if (body.size() - i < 4) return false;
        value = 0;
        for (int n = 0; n < 4; ++n) {
            auto c = body[i++];
            value *= 16;
            if (c >= '0' && c <= '9') value += static_cast<unsigned>(c - '0');
            else if (c >= 'a' && c <= 'f') value += static_cast<unsigned>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') value += static_cast<unsigned>(c - 'A' + 10);
            else return false;
        }
        return true;
    };
    for (std::size_t i = 0; i < body.size();) {
        auto c = body[i++];
        if (c != '\\') { out += c; continue; }
        if (i == body.size()) return std::nullopt;
        switch (auto escape = body[i++]) {
            case '"': case '/': case '\\': out += escape; break;
            case 'b': out += '\b'; break;
            case 'f': out += '\f'; break;
            case 'n': out += '\n'; break;
            case 'r': out += '\r'; break;
            case 't': out += '\t'; break;
            case 'u': {
                unsigned point = 0, low = 0;
                if (!hex(i, point)) return std::nullopt;
                if (point >= 0xD800 && point <= 0xDBFF) {  // a surrogate pair, its second half taken as it comes
                    if (body.size() - i < 6 || body[i] != '\\' || body[i + 1] != 'u') return std::nullopt;
                    i += 2;
                    if (!hex(i, low)) return std::nullopt;
                    point = 0x10000 + ((point & 0x3FF) << 10) + (low & 0x3FF);
                }
                // codePointToUTF8
                if (point <= 0x7F) out += static_cast<char>(point);
                else if (point <= 0x7FF) { out += static_cast<char>(0xC0 | (point >> 6)); out += static_cast<char>(0x80 | (point & 0x3F)); }
                else if (point <= 0xFFFF) {
                    out += static_cast<char>(0xE0 | (point >> 12)); out += static_cast<char>(0x80 | ((point >> 6) & 0x3F)); out += static_cast<char>(0x80 | (point & 0x3F));
                } else {
                    out += static_cast<char>(0xF0 | (point >> 18)); out += static_cast<char>(0x80 | ((point >> 12) & 0x3F));
                    out += static_cast<char>(0x80 | ((point >> 6) & 0x3F)); out += static_cast<char>(0x80 | (point & 0x3F));
                }
                break;
            }
            default: return std::nullopt;
        }
    }
    return out;
}
// What xrpld 3.4.1's json::Reader refuses that nlohmann's parser takes, found before that parser
// runs: a value nested deeper than 25 levels (kNestLimit, which also keeps copies and prints of a
// value, both recursive, off the end of the stack), an integer outside its 32-bit range, a key
// an object already has (nlohmann keeps the last; the Reader compares names as C strings, so up
// to a NUL), and a document that is not an object or array. The first one is reported, where the
// Reader finds it.
void check_reader_limits(std::string_view text) {
    auto fail_at = [&](std::size_t at, std::string const& what) {
        std::size_t line = 1, start = 0;
        for (std::size_t i = 0; i < at; ++i)
            if (text[i] == '\n' || (text[i] == '\r' && (i + 1 >= text.size() || text[i + 1] != '\n'))) { ++line; start = i + 1; }
        throw Unparsable("* Line " + std::to_string(line) + ", Column " + std::to_string(at - start + 1) + "\n  " + what + "\n");
    };
    auto space = [&](std::size_t i) { while (i < text.size() && (text[i] == ' ' || text[i] == '\t' || text[i] == '\r' || text[i] == '\n')) ++i; return i; };
    auto first = space(0);
    if (first < text.size() && text[first] != '{' && text[first] != '[') {
        auto rest = text.substr(first);
        bool value = rest[0] == '"' || rest[0] == '-' || std::isdigit(static_cast<unsigned char>(rest[0])) ||
                     rest.starts_with("true") || rest.starts_with("false") || rest.starts_with("null");
        if (value) fail_at(0, "A valid JSON document must be either an array or an object value.");
        fail_at(first, "Syntax error: value, object or array expected.");
    }
    std::vector<char> open;  // the containers around the next value, innermost last
    std::vector<std::set<std::string>> names;  // for each of them, the keys an object has had
    bool key = false;         // the next string is an object's key, not a value
    for (std::size_t i = first; i < text.size(); ++i) {
        auto c = text[i];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == ':') continue;
        if (c == ',') { key = !open.empty() && open.back() == '{'; continue; }
        if (c == '}' || c == ']') { if (!open.empty()) { open.pop_back(); names.pop_back(); } key = false; continue; }
        if (c == '"') {
            if (!key && open.size() > 25) fail_at(i, "Syntax error: maximum nesting depth exceeded");
            auto start = i;
            for (++i; i < text.size() && text[i] != '"'; ++i) if (text[i] == '\\') ++i;
            // A key's name is checked once its ':' is read, as the Reader checks it (a key with
            // no ':' after it is a syntax error, which nlohmann's parser reports).
            if (key && !open.empty() && i < text.size() && space(i + 1) < text.size() && text[space(i + 1)] == ':')
                if (auto name = reader_string(text.substr(start + 1, i - start - 1))) {
                    if (!names.back().insert(name->substr(0, name->find('\0'))).second) fail_at(start, "Key '" + *name + "' appears twice.");
                }
            key = false;
            continue;
        }
        if (open.size() > 25) fail_at(i, "Syntax error: maximum nesting depth exceeded");
        if (c == '{' || c == '[') { open.push_back(c); names.emplace_back(); key = c == '{'; continue; }
        if (c == '-' || std::isdigit(static_cast<unsigned char>(c))) {
            auto end = i + 1;
            bool integer = true;
            while (end < text.size() && (std::isdigit(static_cast<unsigned char>(text[end])) || (text[end] && std::strchr(".eE+-", text[end])))) {
                if (!std::isdigit(static_cast<unsigned char>(text[end]))) integer = false;
                ++end;
            }
            auto token = text.substr(i, end - i);
            if (integer && token != "-") {
                auto digits = token.substr(c == '-' ? 1 : 0);
                auto too_large = digits.size() > 10 || (digits.size() == 10 && digits > (c == '-' ? "2147483648" : "4294967295"));
                if (too_large) fail_at(i, "'" + std::string(token) + "' exceeds the allowable range.");
            }
            i = end - 1;
        }
        // true, false, null and anything else: nlohmann's parser judges the rest.
    }
}
// A client's request: refused (Unparsable, or nlohmann's own parse error) where xrpld's Reader
// refuses it.
Json parse_request(std::string const& text) {
    check_reader_limits(text);
    return Json::parse(text, [](int depth, Json::parse_event_t, Json&) {
        if (depth > 26) throw Unparsable("* Line 1, Column 1\n  Syntax error: maximum nesting depth exceeded\n");
        return true;
    });
}
// Requests answered from a few reads (a ledger's header, one transaction, one object, the
// server's own state). They run on a lane of their own (Server), so they never wait behind an
// order book, a history page or a ledger's transactions; everything else, and any method not
// named here, is heavy.
bool quick(std::string const& method, Json const& params) {
    static std::set<std::string> const point{"server_info", "ping", "version", "random", "ledger_range", "ledger_closed", "tx", "transaction_entry",
        "ledger_entry", "account_info", "nft_info", "_ledger_event"};
    if (point.contains(method)) return true;
    if (method != "ledger" || !params.is_object()) return false;
    // A ledger's header is a point read; its transactions, state or changes are not.
    for (auto name : {"transactions", "expand", "diff", "full", "accounts", "queue", "owner_funds"})
        if (auto it = params.find(name); it != params.end() && !it->is_null() && *it != false) return false;
    return true;
}
// Whether a request's api_version is one xrpld 3.4.1 serves (rpc::getAPIVersionNumber): absent,
// or an integer from 1 to 2. Anything else (0, 3, -1, "2", 2.0, true) is invalid_API_version,
// which xrpld answers before looking at the method.
bool api_version_valid(Json const& request) {
    if (!request.is_object() || !request.contains("api_version")) return true;
    auto const& version = request.at("api_version");
    if (version.is_number_unsigned()) return version.get<std::uint64_t>() >= 1 && version.get<std::uint64_t>() <= 2;
    if (version.is_number_integer()) return version.get<std::int64_t>() >= 1 && version.get<std::int64_t>() <= 2;
    return false;
}
// The server's own requests (a stream's ledger and transaction events): refused to clients but
// those on this machine (the stream parity tool asks them), as xrpld would answer unknownCmd;
// refused to all when this machine's clients are not trusted ([serve] loopback_trusted false:
// behind a proxy on this machine, every client arrives from it).
bool internal(std::string const& method, std::string const& client, bool loopback_trusted) {
    if (!method.starts_with('_')) return false;
    if (!loopback_trusted) return true;
    beast::error_code ec;
    auto address = net::ip::make_address(client, ec);
    return ec || !address.is_loopback();
}
// An IPv4 client reaching an IPv6 listener is counted (and forwarded) as the IPv4 address it is.
net::ip::address plain(net::ip::address const& address) {
    if (address.is_v6() && address.to_v6().is_v4_mapped()) return net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
    return address;
}
// What an upstream ([serve] forward, forward_ws) makes of this server, asked with get_counts,
// which xrpld 3.4.1 answers only an admin. Anyone else is refused: over HTTP with 403 Forbidden
// (requestRole gives an admin method to anyone else FORBID), over WebSocket with `forbidden`,
// and by a handler's own role check with noPermission. An upstream without get_counts (Clio,
// answering unknownCmd) is not an xrpld granting this server admin: Clio runs its own methods
// and forwards to its xrpld as itself; it passes, but the role it gives this server is not
// confirmed (logged as forward_unverifiable), nor is an HTTP 403 whose body is not xrpld's.
// Anything else (no connection, TLS, a timeout, HTTP 429 or 5xx, tooBusy) tells nothing of the
// role.
enum class UpstreamRole { unknown, public_client, admin };
struct Probe {
    UpstreamRole role = UpstreamRole::unknown;
    std::string why;           // what was answered (unknown), where (admin), what was left out (public_client)
    std::string unverifiable;  // public_client: why the role could not be confirmed
    std::vector<net::ip::address> addresses;  // public_client: the addresses that answered as one
};
Probe judged(Json const& answer) {
    if (!answer.contains("error")) return {UpstreamRole::admin, {}};
    auto const& code = answer.at("error");
    if (code == "forbidden" || code == "noPermission") return {UpstreamRole::public_client, {}};
    if (code == "unknownCmd") return {UpstreamRole::public_client, {}, "get_counts answered unknownCmd (not an xrpld: Clio, say), so the role it gives this server is not known"};
    return {UpstreamRole::unknown, "get_counts answered " + code.dump()};
}
// An upstream's URL, taken apart: an http(s) one as libcurl reads it (so its host is the one
// RpcClient::resolve's entry must name), a ws(s) one as WsClient and PathProxy read it.
struct Upstream {
    std::string scheme, host, name, port, path;  // host as written (an IPv6 address in brackets), name as looked up
    std::uint16_t number = 0;                    // the port, the scheme's default when not given
    bool secure = false;
};
std::optional<Upstream> upstream_url(std::string const& url) {
    Upstream out;
    if (url.starts_with("http://") || url.starts_with("https://")) {
        std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> parsed(curl_url(), curl_url_cleanup);
        if (!parsed || curl_url_set(parsed.get(), CURLUPART_URL, url.c_str(), 0) != CURLUE_OK) return std::nullopt;
        auto part = [&](CURLUPart which, unsigned flags) -> std::optional<std::string> {
            char* text = nullptr;
            if (curl_url_get(parsed.get(), which, &text, flags) != CURLUE_OK || !text) return std::nullopt;
            std::string value(text);
            curl_free(text);
            return value;
        };
        auto scheme = part(CURLUPART_SCHEME, 0), host = part(CURLUPART_HOST, 0), port = part(CURLUPART_PORT, CURLU_DEFAULT_PORT);
        if (!scheme || !host || host->empty() || !port) return std::nullopt;
        out.scheme = *scheme; out.host = *host; out.port = *port; out.path = "/";
    } else {
        static std::regex const pattern(R"(^(wss?)://([^/:]+|\[[^\]]+\])(?::(\d+))?(/.*)?$)");
        std::smatch m;
        if (!std::regex_match(url, m, pattern)) return std::nullopt;
        out.scheme = m[1]; out.host = m[2];
        out.port = m[3].matched ? m[3].str() : (out.scheme == "wss" ? "443" : "80");
        out.path = m[4].matched ? m[4].str() : "/";
    }
    out.secure = out.scheme == "https" || out.scheme == "wss";
    out.name = out.host.starts_with('[') && out.host.ends_with(']') ? out.host.substr(1, out.host.size() - 2) : out.host;
    unsigned number = 0;
    auto [end, failure] = std::from_chars(out.port.data(), out.port.data() + out.port.size(), number);
    if (failure != std::errc() || end != out.port.data() + out.port.size() || !number || number > 65535) return std::nullopt;
    out.number = static_cast<std::uint16_t>(number);
    return out;
}
// RpcClient::resolve's entry sending requests for `upstream` to `addresses` only; empty for a
// host that is an address itself, which is never looked up.
std::string pinned(Upstream const& upstream, std::vector<net::ip::address> const& addresses) {
    beast::error_code ec;
    (void)net::ip::make_address(upstream.name, ec);
    if (!ec || addresses.empty()) return {};
    auto entry = upstream.host + ":" + upstream.port + ":";
    for (std::size_t i = 0; i < addresses.size(); ++i)
        entry += (i ? "," : "") + (addresses[i].is_v6() ? "[" + addresses[i].to_string() + "]" : addresses[i].to_string());
    return entry;
}
// The time left before `until`, at most `most` (and at least 1 ms, so a step late already fails).
std::chrono::milliseconds before(std::chrono::steady_clock::time_point until, std::chrono::milliseconds most) {
    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(until - std::chrono::steady_clock::now());
    return std::clamp(left, std::chrono::milliseconds(1), most);
}
// The addresses `name` has, looked up within `until`: getaddrinfo cannot be stopped, so it runs
// on a thread of its own, left to finish alone when it is late.
std::vector<net::ip::address> addresses_of(std::string const& name, std::string const& port, std::chrono::steady_clock::time_point until) {
    beast::error_code ec;
    if (auto address = net::ip::make_address(name, ec); !ec) return {plain(address)};
    auto found = std::make_shared<std::promise<std::vector<net::ip::address>>>();
    auto answer = found->get_future();
    std::thread([found, name, port] {
        try {
            net::io_context io;
            tcp::resolver resolver(io);
            std::vector<net::ip::address> out;
            for (auto const& entry : resolver.resolve(name, port))
                if (auto address = plain(entry.endpoint().address()); std::find(out.begin(), out.end(), address) == out.end()) out.push_back(address);
            found->set_value(std::move(out));
        } catch (...) { found->set_exception(std::current_exception()); }
    }).detach();
    if (answer.wait_until(until) != std::future_status::ready) throw Error("No address for " + name + " in time");
    return answer.get();
}
std::size_t received_text(char* data, std::size_t size, std::size_t count, void* to) noexcept {
    auto& text = *static_cast<std::string*>(to);
    if (size * count > (1u << 20) - std::min<std::size_t>(text.size(), 1u << 20)) return 0;  // a probe's answer is small
    try { text.append(data, size * count); return size * count; } catch (...) { return 0; }
}
// An error's message as printable ASCII, any other byte as '?': a parse error quotes the bytes
// an upstream sent, which need not be UTF-8, and the log line (JSON) it goes into must be.
std::string printable(std::string text) {
    for (auto& c : text) if (static_cast<unsigned char>(c) < 0x20 || static_cast<unsigned char>(c) > 0x7e) c = '?';
    return text;
}
// One of an upstream's addresses, asked by `until`.
using AddressProbe = Probe (*)(std::string const& url, Upstream const&, net::ip::address const&, std::chrono::steady_clock::time_point until);
// Over HTTP, as RpcClient sends a request (its certificate store, HTTP/1.1, no redirects, no
// proxy, and a connection only to the address asked), once.
Probe probe_http(std::string const& url, Upstream const& upstream, net::ip::address const& address, std::chrono::steady_clock::time_point until) {
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), curl_easy_cleanup);
    if (!curl) return {UpstreamRole::unknown, "Cannot allocate an HTTP client"};
    auto body = Json{{"method", "get_counts"}, {"params", Json::array({Json::object()})}}.dump();
    std::string response;
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(curl_slist_append(nullptr, "Content-Type: application/json"), curl_slist_free_all);
    auto entry = pinned(upstream, {address});
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> resolve(entry.empty() ? nullptr : curl_slist_append(nullptr, entry.c_str()), curl_slist_free_all);
    if (!entry.empty() && !resolve) return {UpstreamRole::unknown, "Cannot allocate an HTTP client"};
    std::vector<std::string> allowed{address.to_string()};  // the probe reaches the address it asks, whatever libcurl's name cache holds
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    if (resolve) curl_easy_setopt(curl.get(), CURLOPT_RESOLVE, resolve.get());
    curl_easy_setopt(curl.get(), CURLOPT_PROXY, "");
    curl_easy_setopt(curl.get(), CURLOPT_NOPROXY, "*");
    curl_easy_setopt(curl.get(), CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    pin_connections(curl.get(), allowed);
    if (auto bundle = std::getenv("SSL_CERT_FILE"); bundle && *bundle) curl_easy_setopt(curl.get(), CURLOPT_CAINFO, bundle);
    else if (fs::exists("/etc/ssl/certs/ca-certificates.crt")) curl_easy_setopt(curl.get(), CURLOPT_CAINFO, "/etc/ssl/certs/ca-certificates.crt");
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.data());
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size()));
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, received_text);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(before(until, std::chrono::seconds(15)).count()));
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, static_cast<long>(before(until, std::chrono::seconds(30)).count()));
    curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_USERAGENT, "xrpla/" XRPLA_VERSION);
    if (auto code = curl_easy_perform(curl.get()); code != CURLE_OK) return {UpstreamRole::unknown, curl_easy_strerror(code)};
    long status = 0; curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
    if (status == 403) {
        // xrpld's own refusal (httpReply: "Forbidden" and CRLF). A proxy in front of xrpld may
        // refuse get_counts itself while it reaches an admin port: its 403 is not xrpld's.
        auto text = response;
        while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) text.pop_back();
        if (text == "Forbidden") return {UpstreamRole::public_client, {}};
        return {UpstreamRole::public_client, {}, "get_counts answered HTTP 403 without xrpld's \"Forbidden\" (a proxy's, say), so the role it gives this server is not known"};
    }
    if (status != 200) return {UpstreamRole::unknown, "get_counts answered HTTP " + std::to_string(status)};
    auto document = Json::parse(response, nullptr, false);
    if (!document.is_object() || !document.contains("result") || !document.at("result").is_object()) return {UpstreamRole::unknown, "get_counts answered no JSON-RPC result"};
    return judged(document.at("result"));
}
// Over WebSocket: an error is unwrapped beside the response, a result is under `result`.
Probe probe_ws(std::string const& url, Upstream const&, net::ip::address const& address, std::chrono::steady_clock::time_point until) {
    try {
        WsClient client(url, address.to_string(), before(until, std::chrono::seconds(15)));
        // get_counts answers in kilobytes; nothing larger is read from an upstream being judged.
        client.read_message_max(1 << 20);
        client.send({{"id", 1}, {"command", "get_counts"}});
        auto answer = client.receive(before(until, std::chrono::seconds(15)));
        if (!answer.is_object() || answer.value("type", "") != "response") return {UpstreamRole::unknown, "get_counts answered no response"};
        return judged(answer);
    } catch (std::exception const& error) { return {UpstreamRole::unknown, printable(error.what())}; }
}
// An upstream asked at each address its host has (the first 8), all at once and within 60 s: one
// that never answers (an IPv6 route that drops everything, say) takes no time from the others.
// xrpld gives a connection its role by the addresses it joins (its admin ports are listed by
// address), and a host with several (localhost: 127.0.0.1 and ::1; a name in round robin) may
// have a public port at one and an admin one at another: asking one of them, as a connection
// would, vouches for none of the others. So the upstream is an admin if any address answers so;
// otherwise a public client at the addresses that refuse get_counts, the only ones it is sent
// requests at (UpstreamCheck); unknown if none does.
Probe examined(std::string const& url, AddressProbe at) {
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    auto upstream = upstream_url(url);
    if (!upstream) return {UpstreamRole::unknown, "not an upstream URL"};
    std::vector<net::ip::address> addresses;
    try { addresses = addresses_of(upstream->name, upstream->port, until); }
    catch (std::exception const& error) { return {UpstreamRole::unknown, printable(error.what())}; }
    if (addresses.empty()) return {UpstreamRole::unknown, "No address for " + upstream->name};
    if (addresses.size() > 8) addresses.resize(8);
    // Each probe ends by its deadline (its connection and answer are bounded by it), so waiting
    // for them all takes no longer. The futures are declared after what they use, so destroying
    // them (waiting for their threads) comes first.
    std::vector<std::future<Probe>> asked;
    for (auto const& address : addresses)
        asked.push_back(std::async(std::launch::async, [&url, &upstream, at, address, until] {
            Probe found;
            try { found = at(url, *upstream, address, until); }
            catch (std::exception const& error) { try { found.why = printable(error.what()); } catch (...) {} }
            catch (...) {}
            return found;
        }));
    Probe out;
    std::optional<net::ip::address> admin;
    for (std::size_t i = 0; i < addresses.size(); ++i) {
        auto found = asked[i].get();
        if (found.role == UpstreamRole::admin) { if (!admin) admin = addresses[i]; continue; }
        if (found.role == UpstreamRole::unknown) { out.why += (out.why.empty() ? "" : "; ") + addresses[i].to_string() + ": " + found.why; continue; }
        out.role = UpstreamRole::public_client;
        out.addresses.push_back(addresses[i]);
        if (out.unverifiable.empty()) out.unverifiable = found.unverifiable;
    }
    if (admin) return {UpstreamRole::admin, "at " + admin->to_string()};
    return out;
}
// Whether an upstream may be sent clients' requests, and at which addresses: only once a probe
// (examined) found it treats this server as a public client (an admin one would run every
// client's request as an admin), at the addresses that answered so, which requests are then
// pinned to (RpcClient::resolve; PathProxy connects to those only). Each probe looks the host up
// again. Probes run one at a time, on the probes' lane (Server::probe_later): never on the network
// thread nor on a request's, and no request waits for one. Until one passes (the upstream could
// not be asked: started with this server, say) forwarding answers notReady, and the upstream is
// asked again as requests come, at most every 10 s. One that passed is asked again every 5
// minutes as requests come, and 10 s after the last probe once a forwarded request could not
// reach it (restarting, say). A probe that finds an admin stops forwarding (notReady, logged as
// forward_refused each time) until a later one finds a public client; one that tells nothing (no
// answer, a busy one) keeps the finding before, and its addresses.
//
// The risk accepted: an upstream reconfigured (or restarted) to grant this server admin is sent
// requests until the next probe finds it so, up to 5 minutes later, or about 10 s after a
// forwarded request could not reach it. Those requests run with the role it gives, sanitised
// either way (public_request: no signing, no credentials, public limits). Used from any thread.
struct UpstreamCheck {
    std::string name, url;  // the option and its value
    std::optional<Upstream> upstream;
    AddressProbe at;
    std::mutex mutex;
    UpstreamRole role = UpstreamRole::unknown;
    std::vector<net::ip::address> addresses;  // public_client: where requests go, sorted (the resolver's order may change)
    std::string logged;                       // the event last logged
    std::optional<std::chrono::steady_clock::time_point> asked;  // when the last probe ended
    // asking: a probe is left for later or running; soon: since it began, a forwarded request
    // could not reach the upstream.
    bool asking = false, soon = false;
    UpstreamCheck(std::string option, std::string target, AddressProbe probe)
        : name(std::move(option)), url(std::move(target)), upstream(upstream_url(url)), at(probe) {}
    // A probe's finding (mutex held), and the line to log (once the mutex is released) when it
    // changes what is forwarded. One that tells nothing keeps the finding before.
    std::optional<Json> found(Probe const& probe) {
        asked = std::chrono::steady_clock::now();
        bool kept = probe.role == UpstreamRole::unknown && role != UpstreamRole::unknown;
        if (!kept) {
            auto sorted = probe.addresses;
            std::sort(sorted.begin(), sorted.end());
            if (sorted != addresses) logged.clear();
            role = probe.role; addresses = std::move(sorted);
        }
        std::string event = probe.role == UpstreamRole::admin ? "forward_refused" : probe.role == UpstreamRole::unknown ? "forward_unchecked" :
                            probe.unverifiable.empty() ? "forward_checked" : "forward_unverifiable";
        if (event != "forward_refused" && logged == event) return std::nullopt;
        logged = event;
        Json line{{"event", event}, {"option", name}, {"forward", url}};
        if (probe.role == UpstreamRole::admin) line["reason"] = "the upstream answers this server as an admin (get_counts) " + probe.why + ": nothing is forwarded until it does not";
        else if (kept && role == UpstreamRole::public_client) line["reason"] = "the upstream could not be asked again: requests go on to the addresses found before";
        else if (probe.role == UpstreamRole::unknown) line["reason"] = "the upstream could not be asked: requests are answered notReady until it is found a public client";
        else if (!probe.unverifiable.empty()) line["reason"] = probe.unverifiable;
        if (role == UpstreamRole::public_client) {
            line["addresses"] = Json::array();
            for (auto const& address : addresses) line["addresses"].push_back(address.to_string());
        }
        if (!probe.why.empty() && probe.role != UpstreamRole::admin) line["error"] = probe.why;
        return line;
    }
    // An error may quote what the upstream sent, which need not be UTF-8: replaced, not thrown.
    static void log(std::optional<Json> const& line) {
        if (line) std::cerr << line->dump(-1, ' ', false, Json::error_handler_t::replace) << std::endl;
    }
    // The probe at start (serve).
    void settle(Probe const& probe) {
        std::optional<Json> line;
        { std::lock_guard lock(mutex); line = found(probe); }
        log(line);
    }
    // When the next probe is due (mutex held).
    std::chrono::steady_clock::time_point due() const {
        auto every = role != UpstreamRole::public_client || soon ? std::chrono::seconds(10) : std::chrono::seconds(300);
        return asked ? *asked + every : std::chrono::steady_clock::time_point{};
    }
    // Leaves a probe to later(probe) when one is due and none is left or running (mutex held).
    template<class Later> void schedule(Later& later) {
        if (asking || std::chrono::steady_clock::now() < due()) return;
        asking = true;
        try { later([this] { ask(); }); } catch (...) { asking = false; throw; }
    }
    // The probe schedule() left: asks the upstream and records what it found, clearing `asking`
    // whatever the probe throws.
    void ask() {
        { std::lock_guard lock(mutex); soon = false; }
        Probe probe;  // unknown, unless examined answers
        try { probe = examined(url, at); }
        catch (std::exception const& error) { try { probe.why = printable(error.what()); } catch (...) {} }
        catch (...) {}
        std::optional<Json> line;
        {
            std::lock_guard lock(mutex);
            asking = false;
            line = found(probe);
        }
        log(line);
    }
    // The addresses a request may go upstream at now, or nullopt while none may; a probe due is
    // left to later(probe) first, and the request does not wait for it.
    template<class Later> std::optional<std::vector<net::ip::address>> ready(Later later) {
        std::lock_guard lock(mutex);
        schedule(later);
        return role == UpstreamRole::public_client ? std::optional(addresses) : std::nullopt;
    }
    // A forwarded request that could not reach the upstream: it is asked again 10 s after the
    // last probe (now, if that was longer ago).
    template<class Later> void failed(Later later) {
        std::lock_guard lock(mutex);
        soon = true;
        schedule(later);
    }
};
// The answer to a request held back from an upstream not checked (UpstreamCheck).
Json unchecked_upstream() {
    auto answer = error(::xrpl::RpcNotReady);
    answer["error_message"] = "The upstream server is not available.";
    return answer;
}
struct StreamHub;
struct WsSession;
struct Server {
    ServeOptions options;
    // Whether the upstreams may be sent requests: declared before the lanes, whose threads ask.
    UpstreamCheck forward_check{"forward", options.forward, probe_http}, forward_ws_check{"forward_ws", options.forward_ws, probe_ws};
    // Open connections, in all and from each address ([serve] max_connections_per_ip; an IPv6
    // address counted by its /64, address_group, as one host or site is usually given a whole
    // /64), and the ones refused since the last refusal logged: network thread only. Declared
    // before the io_context and the lanes, so they outlive them: destroying those destroys the
    // handlers still pending, and the sessions they hold release their connection here as they go.
    unsigned connections = 0;
    std::unordered_map<std::string, unsigned> by_group;
    unsigned refused = 0;
    std::optional<std::chrono::steady_clock::time_point> refusal_logged;
    std::atomic<unsigned> running{0};  // requests a worker is answering now
    // The WebSocket sessions open, and the bytes queued for them to write (replies and stream
    // messages; each session counts its own, WsSession::queued). Past max_ws_queued, the session
    // holding the most is disconnected (shed): a thousand clients each within their own limit
    // could otherwise hold tens of GB. Declared before the io_context, as the counts above.
    // A stream message is one string shared by every session it goes to, so it counts once
    // however many queue it (`ws_streamed`: how many entries hold each, by address; the string
    // lives while any does): counted per session, a thousand transaction subscribers each a
    // ledger behind passed the limit with a few MB in memory and healthy sessions were shed.
    std::set<WsSession*> ws_sessions;
    std::atomic<std::size_t> ws_queued{0};
    std::unordered_map<std::string const*, unsigned> ws_streamed;
    static constexpr std::size_t max_ws_queued = 2ULL << 30;
    void shed();
    net::io_context io;
    // Requests run on lanes, each with its own threads and its own limit: a slow kind can make
    // only its own lane busy. Quick requests (quick() above) have half as many threads as heavy
    // ones (at least 2); in one queue, a server_info waited seconds behind book_offers.
    // Forwarded requests wait on the upstream, so a slow or stalled upstream makes only them
    // busy. The stream hub's work has one thread of its own and no limit: it is one ledger at a
    // time, and under load a stream that shared the queue fell behind or stopped. The upstreams'
    // probes have a thread each (each check runs one at a time): behind the forwarded requests
    // queued, a probe would wait for up to 128 of them.
    struct Lane {
        net::thread_pool threads;
        unsigned limit, queued = 0;  // queued or running, at most `limit` (more answer tooBusy); network thread only
        Lane(unsigned count, unsigned most) : threads(count), limit(most) {}
    };
    static constexpr unsigned forward_threads = 8;
    static unsigned quick_threads(unsigned threads) { return std::max(2u, threads / 2); }
    // Requests queued or running at once on a lane: 16 per thread.
    Lane fast, heavy, forwarded, streams, probes;
    std::shared_ptr<StreamHub> hub;  // stream messages, rendered once for every subscriber
    // The TLS listeners' certificate; a session keeps the one it started with.
    std::shared_ptr<net::ssl::context> tls;
    fs::file_time_type tls_cert_time, tls_key_time;
    // The client context for wss:// upstreams (path_find), made once: loading the system's CA
    // store for each session's first path search held the network thread.
    std::shared_ptr<net::ssl::context> upstream_tls;
    explicit Server(ServeOptions value)
        : options(std::move(value)), fast(quick_threads(options.threads), quick_threads(options.threads) * 16), heavy(options.threads, options.threads * 16),
          forwarded(forward_threads, forward_threads * 16), streams(1, std::numeric_limits<unsigned>::max()), probes(2, std::numeric_limits<unsigned>::max()) {}
    Lane& lane(std::string const& method, Json const& params) {
        if (!options.forward.empty() && forwarded_request(method, params)) return forwarded;
        return quick(method, params) ? fast : heavy;
    }
    std::shared_ptr<net::ssl::context> const& client_tls() {
        if (!upstream_tls) {
            upstream_tls = std::make_shared<net::ssl::context>(net::ssl::context::tls_client);
            upstream_tls->set_default_verify_paths(); upstream_tls->set_verify_mode(net::ssl::verify_peer);
        }
        return upstream_tls;
    }
    // Runs an upstream's probe (UpstreamCheck) on the probes' lane, counted as a request running:
    // a stop waits for it as for them, its 10 s at most, not for the probe's minute. One that
    // throws (memory) is logged rather than end the process; the probe has cleared its check's
    // `asking` whatever it threw (UpstreamCheck::ask).
    void probe_later(std::function<void()> probe) {
        net::post(probes.threads, [this, probe = std::move(probe)] {
            ++running;
            try { probe(); }
            catch (std::exception const& e) { std::cerr << "serve: upstream probe failed: " << e.what() << std::endl; }
            catch (...) { std::cerr << "serve: upstream probe failed" << std::endl; }
            --running;
        });
    }
    // Runs `work` on a lane's threads; `done` gets its result on the network thread. A lane at
    // its limit answers fallback(tooBusy) at once; work that throws (a reply that cannot be
    // rendered, memory) answers fallback(internal) rather than end the process, and the lane's
    // count and the caller's session are released on the network thread either way.
    template<class Work, class Fallback, class Done> void run(Lane& lane, Work work, Fallback fallback, Done done) {
        if (lane.queued >= lane.limit) { done(fallback(::xrpl::RpcTooBusy)); return; }
        ++lane.queued;
        net::post(lane.threads, [this, &lane, work = std::move(work), fallback = std::move(fallback), done = std::move(done)]() mutable {
            std::optional<decltype(work())> result;
            ++running;
            try { result.emplace(work()); }
            catch (std::exception const& e) { std::cerr << "serve: request failed: " << e.what() << std::endl; }
            catch (...) { std::cerr << "serve: request failed" << std::endl; }
            --running;
            net::post(io, [&lane, result = std::move(result), fallback = std::move(fallback), done = std::move(done)]() mutable {
                --lane.queued;
                done(result ? std::move(*result) : fallback(::xrpl::RpcInternal));
            });
        });
    }
    // A request, with its reply rendered on the worker thread too (`render` turns the result into
    // the text sent): one network thread serves every connection, and serializing large results
    // there capped a server's throughput with most of its cores idle. `client` is the address it
    // came from, passed on when the request is forwarded (send_upstream). path_find over HTTP is
    // answered here (noEvents), never upstream.
    template<class Render, class Done> void dispatch_text(std::string method, Json params, Render render, Done done, std::string client = {}) {
        auto& to = lane(method, params);
        bool forward = &to == &forwarded && method != "path_find";
        run(to, [this, forward, method = std::move(method), params = std::move(params), render, client = std::move(client)] {
            return render(forward ? send_upstream(method, params, client) : rpc_request(options.dir, method, params, options.forward, client));
        },
            [render](::xrpl::ErrorCodeI code) { return render(error(code)); }, std::move(done));
    }
    // A request for the upstream, on a forwarding lane's thread. It goes only once the upstream is
    // checked (UpstreamCheck), and only to the addresses checked. One that cannot reach the
    // upstream has it checked again soon (what the upstream answers, notReady included, does not).
    Json send_upstream(std::string const& method, Json const& params, std::string const& client) {
        auto later = [this](std::function<void()> probe) { probe_later(std::move(probe)); };
        auto addresses = forward_check.ready(later);
        if (!addresses || addresses->empty() || !forward_check.upstream) return unchecked_upstream();
        auto result = rpc_request(options.dir, method, params, options.forward, client, pinned(*forward_check.upstream, *addresses));
        if (result.value("error", Json()) == "notReady" && !RpcClient::last_reached()) forward_check.failed(later);
        return result;
    }
    template<class Done> void dispatch(std::string method, Json params, Done done, std::string client = {}) {
        dispatch_text(std::move(method), std::move(params), [](Json result) { return result; }, std::move(done), std::move(client));
    }
    // The stream hub's work, on its own lane; `done` gets T{} when it fails.
    template<class Work, class Done> void stream(Work work, Done done) {
        using Result = decltype(work());
        run(streams, std::move(work), [](::xrpl::ErrorCodeI) { return Result{}; }, std::move(done));
    }
    // Why a connection from `address` is refused, or nullptr: max_connections in all, and
    // max_connections_per_ip from one address (address_group) other than this machine's own (a
    // local proxy or benchmark is all its clients), which are limited too when not trusted
    // (loopback_trusted).
    char const* refusal(net::ip::address const& address) const {
        if (connections >= options.max_connections) return "max_connections";
        if (!options.max_connections_per_ip || (address.is_loopback() && options.loopback_trusted)) return nullptr;
        auto it = by_group.find(address_group(address.to_string()));
        return it != by_group.end() && it->second >= options.max_connections_per_ip ? "max_connections_per_ip" : nullptr;
    }
    // Refusals are logged at most every 10 s, with how many there were since the last line.
    void refuse(char const* reason, std::string const& address) {
        ++refused;
        auto now = std::chrono::steady_clock::now();
        if (refusal_logged && now - *refusal_logged < std::chrono::seconds(10)) return;
        std::cerr << Json{{"event", "connections_refused"}, {"reason", reason}, {"address", address}, {"refused", refused}}.dump() << std::endl;
        refused = 0; refusal_logged = now;
    }
    void admit(std::string const& address) { ++connections; ++by_group[address_group(address)]; }
    void release(std::string const& address) {
        --connections;
        if (auto it = by_group.find(address_group(address)); it != by_group.end() && !--it->second) by_group.erase(it);
    }
};
// A ledger's stream messages, rendered once (StreamHub) and shared by every session: the
// ledgerClosed message, and in each API version subscribers use, each transaction's message
// with the transactions each account and book concerns (positions in `transactions`), so a
// session finds its own without testing every transaction against every subscription.
struct Rendered {
    std::uint32_t sequence = 0;
    std::shared_ptr<std::string const> closed;
    struct Version {
        bool rendered = false;
        std::vector<std::shared_ptr<std::string const>> transactions;
        std::unordered_map<::xrpl::AccountID, std::vector<std::uint32_t>> accounts;
        std::unordered_map<std::string, std::vector<std::uint32_t>> books;
    };
    std::array<Version, 2> versions;  // by API version; the hub adds one a subscriber comes to need
};
struct WsSession;
struct PathProxy;
void stream_join(std::shared_ptr<WsSession> const&);
struct WsSession : std::enable_shared_from_this<WsSession> {
    using Plain = websocket::stream<beast::tcp_stream>;
    using Secure = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;
    Server& server;
    std::shared_ptr<net::ssl::context> tls;  // a TLS session's certificate, kept while it is open
    std::variant<Plain, Secure> ws;
    std::string client;                 // the address the session came from
    std::shared_ptr<PathProxy> paths;   // its path searches, relayed upstream
    beast::flat_buffer input;
    // Messages to write, each marked when it is a stream message (rather than a reply).
    std::deque<std::pair<std::shared_ptr<std::string const>, bool>> outgoing;
    std::size_t streamed = 0;  // bytes of stream messages in `outgoing`
    std::size_t replied = 0;   // bytes of replies in `outgoing`
    unsigned replies = 0;      // replies in `outgoing`
    net::steady_timer deadline;  // the write in progress's (write_timeout)
    // Requests are read while earlier ones run, up to max_in_flight at once counting the replies
    // not yet written, and while the replies not yet written are at most max_replied (then
    // reading waits and TCP holds the client back), and answered as each is ready, matched by
    // its id as xrpld's are: one request at a time put a client's server_info behind its own
    // book_offers.
    // Subscribe and unsubscribe change what the session receives, so they wait for each other
    // and apply in the order sent.
    static constexpr unsigned max_in_flight = 8;
    static constexpr std::size_t max_replied = 8 << 20;
    unsigned in_flight = 0;
    bool reading = false, changing = false;
    std::deque<Json> changes;
    // Streams: ledger, transactions and accounts (rippled's subscribe), in the API version of the
    // subscribe request. A ledger's ledgerClosed message comes before its transactions.
    bool ledger_stream = false, transactions_stream = false, subscribed = false, closed = false;
    // The accounts followed are kept sorted, 20 bytes each: as base58 strings in a set, the
    // 10000 a session may follow took about 1 MB, a GB over a thousand sessions.
    std::vector<::xrpl::AccountID> accounts;
    std::set<std::string> books;
    static constexpr std::size_t max_accounts = 10000, max_books = 1000, max_snapshots = 64;  // change() says why
    unsigned api_version = 1;
    std::uint32_t seen = 0;  // the last ledger whose messages were sent
    // Ledgers the hub rendered that the session has not had yet, in order. One without its
    // transactions in the session's API version waits for the hub to render them, and the ones
    // after it wait too; all wait while a first subscription's answer, or one with book
    // snapshots, is outstanding (`holding`), so its messages follow the answer and none is
    // skipped (or sent before its snapshot) while the snapshots are read.
    std::deque<std::shared_ptr<Rendered>> backlog;
    bool joined = false, holding = false;
    WsSession(Server& s, beast::tcp_stream stream, std::string address, std::shared_ptr<net::ssl::context>)
        : server(s), ws(std::in_place_type<Plain>, std::move(stream)), client(std::move(address)), deadline(s.io) { server.ws_sessions.insert(this); }
    WsSession(Server& s, beast::ssl_stream<beast::tcp_stream> stream, std::string address, std::shared_ptr<net::ssl::context> context)
        : server(s), tls(std::move(context)), ws(std::in_place_type<Secure>, std::move(stream)), client(std::move(address)), deadline(s.io) {
        server.ws_sessions.insert(this);
    }
    ~WsSession();
    template<class F> void with(F&& f) { std::visit([&](auto& s) { f(s); }, ws); }
    void path_find(Json const& request);
    void start(http::request<http::string_body> request) {
        with([&](auto& s) {
            s.read_message_max(64 * 1024);
            // A subscriber may only listen: it is pinged when quiet (and answers, as clients do)
            // rather than dropped after five minutes without sending anything.
            auto timeouts = websocket::stream_base::timeout::suggested(beast::role_type::server);
            timeouts.keep_alive_pings = true;
            s.set_option(timeouts);
            s.async_accept(request, [self = shared_from_this()](beast::error_code ec) { if (!ec) self->read(); });
        });
    }
    // A client that cannot keep up is disconnected, and the operator's log says why (once: a
    // session already closed, its read or write having failed, is only made sure of). What waits
    // behind the message being written is never sent, so its memory goes now.
    void drop(char const* reason) {
        if (!closed) std::cerr << Json{{"event", "subscriber_dropped"}, {"reason", reason}, {"address", client}, {"queued", queued()}}.dump() << std::endl;
        closed = true; backlog.clear();
        while (outgoing.size() > 1) { forget(*outgoing.back().first, outgoing.back().second); outgoing.pop_back(); }
        beast::error_code ec; with([&](auto& s) { beast::get_lowest_layer(s).socket().close(ec); });
    }
    std::size_t queued() const { return streamed + replied; }
    // A message's bytes leave the session's count and the server's (Server::ws_queued) as it
    // leaves `outgoing`, written or dropped; what is left when the session ends leaves with it.
    // A stream message leaves the server's as the last session holding it lets it go.
    void count(std::string const& text, bool stream) {
        if (stream) streamed += text.size(); else { replied += text.size(); ++replies; }
        if (!stream || server.ws_streamed[&text]++ == 0) server.ws_queued += text.size();
    }
    void forget(std::string const& text, bool stream) {
        if (stream) streamed -= text.size(); else { replied -= text.size(); --replies; }
        if (stream) {
            auto it = server.ws_streamed.find(&text);
            if (--it->second) return;
            server.ws_streamed.erase(it);
        }
        server.ws_queued -= text.size();
    }
    void send(Json value) { send(std::make_shared<std::string const>(value.dump())); }
    // A subscriber more than 16 MB of stream messages behind (past the message being written) is
    // disconnected. A count of messages dropped a healthy one on a single large ledger. Replies
    // are not held to it: pipelined account_tx or ledger_data pages of a few MB each passed it,
    // and read() takes no request while max_replied of them wait: a session holds at most that
    // and the replies of the requests already running (max_in_flight), whatever they weigh. A
    // client that drains them too slowly is disconnected by write_timeout, and every session's
    // together by max_ws_queued. (xrpld's BaseWSPeer counts messages, ws_queue_limit's 100.)
    static constexpr std::size_t max_queued = 16 << 20;
    void send(std::shared_ptr<std::string const> text, bool stream = false) {
        if (closed) return;
        if (!outgoing.empty()) {
            auto writing = outgoing.front().first->size();
            if (stream && streamed - (outgoing.front().second ? writing : 0) + text->size() > max_queued) { drop("max_queued"); return; }
        }
        bool counted = stream && server.ws_streamed.contains(text.get());  // queued already, by some session
        if (!counted && server.ws_queued + text->size() > Server::max_ws_queued) {
            server.shed();
            if (closed) return;  // this session held the most
        }
        count(*text, stream);
        outgoing.emplace_back(std::move(text), stream);
        if (outgoing.size() == 1) write();
    }
    // A write pending past write_timeout disconnects the session: a client that answers the
    // keep-alive pings (the idle timeout counts only what it sends) but does not read would keep
    // its queue for good. xrpld's BaseWSPeer has no such deadline, only its ping's.
    static constexpr std::chrono::seconds write_timeout{60};
    void write() {
        deadline.expires_after(write_timeout);
        deadline.async_wait([self = shared_from_this()](beast::error_code ec) {
            // One that fired as its write ended, with the next write's deadline set since, is not due.
            if (!ec && self->deadline.expiry() <= std::chrono::steady_clock::now()) self->drop("write_timeout");
        });
        with([&](auto& s) {
            s.text(true);
            s.async_write(net::buffer(*outgoing.front().first), [self = shared_from_this()](beast::error_code ec, std::size_t) {
                self->deadline.cancel();
                if (ec) { self->closed = true; return; }
                auto [text, stream] = std::move(self->outgoing.front());
                self->outgoing.pop_front();
                self->forget(*text, stream);
                if (!self->outgoing.empty()) self->write();
                if (!stream) self->read();
            });
        });
    }
    // Ledgers waiting for the session past this many (a minute of mainnet ledgers: one version
    // of a ledger the hub keeps failing to render for this session holds every later one for
    // it, up to the 30 tries StreamHub gives it) disconnect it.
    static constexpr std::size_t max_backlog = 64;
    void queue(std::shared_ptr<Rendered> const& ledger) {
        if (closed || ledger->sequence <= seen || (!backlog.empty() && ledger->sequence <= backlog.back()->sequence)) return;
        if (backlog.size() >= max_backlog) { drop("max_backlog"); return; }
        backlog.push_back(ledger);
    }
    void pump();
    // A ledger's messages this subscription wants: its ledgerClosed message first, then its
    // transactions in order; as rippled does, a transaction matching several subscriptions
    // (the stream, an account, a book) is sent for each, once for however many accounts (or
    // books) of the session it concerns.
    void deliver(Rendered const& ledger) {
        if (ledger_stream && ledger.closed) send(ledger.closed, true);
        if (!wants_transactions()) return;
        auto const& version = ledger.versions[api_version - 1];
        // Books are a set, accounts a sorted vector.
        auto contains = [](auto const& wanted, auto const& key) {
            if constexpr (requires { wanted.contains(key); }) return wanted.contains(key);
            else return std::binary_search(wanted.begin(), wanted.end(), key);
        };
        auto matching = [&contains](auto const& wanted, auto const& index) {
            std::vector<std::uint32_t> out;
            if (wanted.size() < index.size()) {
                for (auto const& key : wanted) if (auto it = index.find(key); it != index.end()) out.insert(out.end(), it->second.begin(), it->second.end());
            } else {
                for (auto const& [key, positions] : index) if (contains(wanted, key)) out.insert(out.end(), positions.begin(), positions.end());
            }
            std::sort(out.begin(), out.end()); out.erase(std::unique(out.begin(), out.end()), out.end());
            return out;
        };
        auto by_account = matching(accounts, version.accounts), by_book = matching(books, version.books);
        auto a = by_account.begin(), b = by_book.begin();
        auto count = static_cast<std::uint32_t>(version.transactions.size());
        for (std::uint32_t i = 0; i < count; ++i) {
            if (!transactions_stream) i = std::min(a != by_account.end() ? *a : count, b != by_book.end() ? *b : count);
            if (i >= count) break;
            if (transactions_stream) send(version.transactions[i], true);
            if (a != by_account.end() && *a == i) { send(version.transactions[i], true); ++a; }
            if (b != by_book.end() && *b == i) { send(version.transactions[i], true); ++b; }
        }
    }
    bool wants_transactions() const { return transactions_stream || !accounts.empty() || !books.empty(); }
    // As xrpld's ServerHandler::processSession: an error unwrapped beside the request (its
    // secrets masked), a result under `result`; id, jsonrpc, ripplerpc and api_version echoed.
    static std::shared_ptr<std::string const> response_text(Json result, Json const& request) {
        Json response{{"type", "response"}, {"status", result.value("status", "error")}};
        if (result.contains("error")) { response.update(result); response["request"] = rpc::masked(request); }
        else { result.erase("status"); response["result"] = std::move(result); }  // moved: a copy held a large answer twice
        for (auto name : {"id", "jsonrpc", "ripplerpc", "api_version"})
            if (request.is_object() && request.contains(name)) response[name] = request.at(name);
        return std::make_shared<std::string const>(response.dump());
    }
    void reply(Json result, Json const& request) {
        send(response_text(std::move(result), request));
    }
    // Books subscribed with `snapshot` answer with their offers in `ledger` (0: the newest), as
    // xrpld's subscribe does: `offers`, or `bids` (the book) and `asks` (its reverse) with `both`.
    // The ledger is the one the session's messages follow, so no offer change falls between.
    void with_snapshots(Json const& request, std::uint32_t ledger, Json result, std::function<void(Json)> done) {
        auto queries = std::make_shared<std::vector<std::pair<std::string, Json>>>();
        for (auto const& book : request.value("books", Json::array())) {
            if (!book.is_object() || !book.value("snapshot", false)) continue;
            Json params{{"taker_pays", book.at("taker_pays")}, {"taker_gets", book.at("taker_gets")}, {"limit", 60}, {"api_version", api_version}};
            if (ledger) params["ledger_index"] = ledger;
            for (auto name : {"taker", "domain"}) if (book.contains(name)) params[name] = book.at(name);
            bool both = book.value("both", false);
            queries->push_back({both ? "bids" : "offers", params});
            if (both) { auto reversed = params; std::swap(reversed["taker_pays"], reversed["taker_gets"]); queries->push_back({"asks", reversed}); }
        }
        auto answer = std::make_shared<Json>(std::move(result));
        auto step = std::make_shared<std::function<void(std::size_t)>>();
        *step = [self = shared_from_this(), queries, answer, step, done = std::move(done)](std::size_t i) {
            if (i == queries->size()) { done(std::move(*answer)); *step = nullptr; return; }
            self->server.dispatch("book_offers", (*queries)[i].second, [answer, queries, step, i](Json offers) {
                if (!offers.contains("error") && offers.contains("offers")) (*answer)[(*queries)[i].first] = offers.at("offers");
                (*step)(i + 1);
            });
        };
        (*step)(0);
    }
    // rippled's subscribe/unsubscribe parameters for the streams served here; false when the
    // request names none.
    bool change(Json const& request, bool add) {
        std::set<std::string> streams;
        std::set<::xrpl::AccountID> ids;
        if (request.contains("streams")) {
            if (!request.at("streams").is_array()) throw Failure{error(::xrpl::RpcInvalidParams)};
            for (auto const& stream : request.at("streams")) {
                if (!stream.is_string() || (stream != "ledger" && stream != "transactions")) throw Failure{error(::xrpl::RpcStreamMalformed)};
                streams.insert(stream.get<std::string>());
            }
        }
        if (request.contains("accounts")) {
            if (!request.at("accounts").is_array() || request.at("accounts").empty()) throw Failure{error(::xrpl::RpcActMalformed)};
            for (auto const& id : request.at("accounts")) {
                auto parsed = id.is_string() ? ::xrpl::parseBase58<::xrpl::AccountID>(id.get<std::string>()) : std::nullopt;
                if (!parsed) throw Failure{error(::xrpl::RpcActMalformed)};
                ids.insert(*parsed);
            }
        }
        // Books: taker_pays is the issue going in, taker_gets the one coming out; `both` adds
        // the reverse book.
        std::set<std::string> keys;
        if (request.contains("books")) {
            if (!request.at("books").is_array() || request.at("books").empty()) throw Failure{error(::xrpl::RpcInvalidParams)};
            for (auto const& book : request.at("books")) {
                if (!book.is_object() || !book.contains("taker_pays") || !book.contains("taker_gets")) throw Failure{error(::xrpl::RpcInvalidParams)};
                auto domain = book.value("domain", std::string());
                try {
                    keys.insert(rpc::book_key(book.at("taker_pays"), book.at("taker_gets"), domain));
                    if (book.value("both", false)) keys.insert(rpc::book_key(book.at("taker_gets"), book.at("taker_pays"), domain));
                } catch (std::exception const&) { throw Failure{error(::xrpl::RpcBadMarket)}; }
            }
        }
        for (auto name : {"accounts_proposed", "url"})
            if (request.contains(name)) throw Failure{error(::xrpl::RpcNotSupported)};
        if (streams.empty() && ids.empty() && keys.empty()) return false;
        // Each account and book a session follows is kept and matched against every ledger's
        // transactions, and each snapshot book is a book_offers read: a client gets that much.
        // (xrpld allows 100000 subscriptions a connection, InfoSub.h's
        // kMaxSubscriptionsPerConnection; the 10000 accounts here are what one client needs.)
        if (add) {
            auto const asked = request.value("books", Json::array());
            std::size_t snapshots = std::count_if(asked.begin(), asked.end(), [](Json const& book) { return book.value("snapshot", false); });
            std::size_t more_accounts = std::count_if(ids.begin(), ids.end(), [&](auto const& id) { return !std::binary_search(accounts.begin(), accounts.end(), id); });
            std::size_t more_books = std::count_if(keys.begin(), keys.end(), [&](auto const& key) { return !books.contains(key); });
            if (accounts.size() + more_accounts > max_accounts || books.size() + more_books > max_books || snapshots > max_snapshots) {
                auto refused = error(::xrpl::RpcInvalidParams);
                refused["error_message"] = "A session follows at most " + std::to_string(max_accounts) + " accounts and " + std::to_string(max_books) +
                                           " books, with at most " + std::to_string(max_snapshots) + " snapshots a request.";
                throw Failure{refused};
            }
        }
        for (auto const& key : keys) { if (add) books.insert(key); else books.erase(key); }
        if (streams.contains("ledger")) ledger_stream = add;
        if (streams.contains("transactions")) transactions_stream = add;
        // Both sorted, so the change is one merge.
        std::vector<::xrpl::AccountID> followed;
        followed.reserve(add ? accounts.size() + ids.size() : accounts.size());
        if (add) std::set_union(accounts.begin(), accounts.end(), ids.begin(), ids.end(), std::back_inserter(followed));
        else std::set_difference(accounts.begin(), accounts.end(), ids.begin(), ids.end(), std::back_inserter(followed));
        followed.shrink_to_fit();
        accounts = std::move(followed);
        subscribed = ledger_stream || transactions_stream || !accounts.empty() || !books.empty();
        return true;
    }
    // The next subscribe or unsubscribe, once the one before it is answered.
    void next_change() {
        if (changing || changes.empty()) return;
        changing = true;
        auto request = std::move(changes.front()); changes.pop_front();
        auto self = shared_from_this();
        auto finish = [self, request](Json result) { self->reply(std::move(result), request); self->changing = false; self->settle(); self->next_change(); };
        bool add = false, was = subscribed, with_ledger = false;
        std::optional<Json> refused;
        try {
            add = request.at(request.contains("command") ? "command" : "method") == "subscribe";
            if (!change(request, add)) throw Failure{error(::xrpl::RpcInvalidParams)};
            if (add && request.contains("api_version") && request.at("api_version").is_number_unsigned())
                api_version = std::clamp<unsigned>(request.at("api_version").get<unsigned>(), 1, 2);
            auto const& streams = request.value("streams", Json::array());
            with_ledger = std::find(streams.begin(), streams.end(), "ledger") != streams.end();
        } catch (Failure const& failure) { refused = failure.value; }
          catch (std::exception const&) { refused = error(::xrpl::RpcInvalidParams); }
        if (refused) { finish(std::move(*refused)); return; }
        if (!add) { finish({{"status", "success"}}); return; }
        // Messages wait while snapshots are read (`holding`), then follow the answer. Holding ends
        // before the answer is sent: sending it starts the next change, which may hold again.
        auto answered = [self, finish](Json answer) { self->holding = false; finish(std::move(answer)); self->pump(); };
        if (was) {
            // Already subscribed: the snapshots are read in the last ledger whose messages were
            // sent, and a new book's messages for later ledgers wait for them.
            auto const& books = request.value("books", Json::array());
            if (std::none_of(books.begin(), books.end(), [](Json const& book) { return book.is_object() && book.value("snapshot", false); })) { finish({{"status", "success"}}); return; }
            holding = true;
            with_snapshots(request, seen, {{"status", "success"}}, answered);
            return;
        }
        server.dispatch("_ledger_event", Json::object(), [self, request, with_ledger, answered](Json result) {
            // Messages start after the ledger current at the first subscription. The session joins
            // the stream now, holding its messages until the answer (with snapshots read in that
            // ledger) is sent.
            if (result.contains("error")) {
                // No ledger to start after: the subscription is not made (the session had none).
                self->ledger_stream = self->transactions_stream = self->subscribed = false;
                self->accounts.clear(); self->books.clear();
                answered(std::move(result));
                return;
            }
            self->seen = result.at("ledger_index");
            if (!with_ledger) result = {{"status", "success"}};
            self->holding = true;
            stream_join(self);
            self->with_snapshots(request, self->seen, result, answered);
        });
    }
    void read() {
        if (reading || closed || in_flight + replies >= max_in_flight || replied > max_replied) return;
        reading = true;
        with([&](auto& s) { s.async_read(input, [self = shared_from_this()](beast::error_code ec, std::size_t) { self->reading = false; self->received(ec); }); });
    }
    // A request answered: the next may be read.
    void settle() { --in_flight; read(); }
    void received(beast::error_code ec) {
        if (ec) { closed = true; return; }
        Json request;
        try {
            auto text = beast::buffers_to_string(input.data()); input.consume(input.size());
            try { request = parse_request(text); } catch (std::exception const&) { request = nullptr; }
            if (!request.is_object()) {
                // As xrpld's onWSMessage: anything it cannot parse, or that is no object.
                send(std::make_shared<std::string const>(Json{{"type", "error"}, {"error", "jsonInvalid"}, {"value", text}}.dump(-1, ' ', false, Json::error_handler_t::replace)));
                read();
                return;
            }
            // As xrpld: the method is `command` or `method` (both: the same), and an invalid
            // api_version is refused before either is looked at.
            auto named = [&](char const* field) { return request.contains(field) ? &request.at(field) : nullptr; };
            auto const* command = named("command"); auto const* alias = named("method");
            if (!api_version_valid(request) || (!command && !alias) || (command && !command->is_string()) || (alias && !alias->is_string()) ||
                (command && alias && *command != *alias)) {
                Json refused{{"type", "response"}, {"status", "error"}, {"error", api_version_valid(request) ? "missingCommand" : "invalid_API_version"},
                             {"request", request}};
                for (auto name : {"id", "jsonrpc", "ripplerpc", "api_version"}) if (request.contains(name)) refused[name] = request.at(name);
                send(std::make_shared<std::string const>(refused.dump()));
                read();
                return;
            }
            // The request itself is left as sent: errors echo it unchanged, as xrpld's do.
            auto method = (command ? *command : *alias).get<std::string>();
            if (method == "subscribe" || method == "unsubscribe") {
                ++in_flight; changes.push_back(request); next_change();
            } else if (method == "path_find") {
                path_find(request);
            } else if (internal(method, client, server.options.loopback_trusted)) {
                reply(error(::xrpl::RpcUnknownCommand), request);
            } else {
                auto params = request; params.erase("command"); params.erase("id");
                ++in_flight;
                server.dispatch_text(method, params, [request](Json result) { return response_text(std::move(result), request); },
                                     [self = shared_from_this()](std::shared_ptr<std::string const> text) { self->send(std::move(text)); self->settle(); }, client);
            }
        } catch (std::exception const&) {
            input.consume(input.size()); reply(error(::xrpl::RpcInvalidParams), request.is_object() ? request : Json::object());
        }
        read();
    }
};
// path_find for one WebSocket session, relayed to the upstream's WebSocket ([serve] forward_ws):
// xrpld streams a path search's updates over WebSocket only, so a one-shot HTTP forward cannot
// carry it. Opened on the session's first path_find, to the addresses the upstream was checked
// at (UpstreamCheck), and closed with the session; it runs on the network thread, as the
// session does. When the relay ends (the upstream not reached, or closing it), the requests not
// yet sent are answered notReady, the upstream is checked again soon, and the session's next
// path_find opens a new connection.
struct PathProxy : std::enable_shared_from_this<PathProxy> {
    using Plain = websocket::stream<beast::tcp_stream>;
    using Secure = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;
    // Requests waiting to be sent upstream at once, and their bytes; more are answered tooBusy.
    static constexpr std::size_t max_outgoing = 64, max_pending = 1 << 20;
    Server& server;
    std::weak_ptr<WsSession> session;
    std::string client;
    std::vector<tcp::endpoint> endpoints;  // the upstream's checked addresses
    std::shared_ptr<net::ssl::context> tls;  // the server's client context, kept while the stream uses it
    std::variant<std::monostate, Plain, Secure> stream;
    beast::flat_buffer input;
    std::deque<std::string> outgoing;
    std::size_t pending = 0;  // bytes in `outgoing`
    bool open = false, writing = false, closed = false;
    PathProxy(Server& s, std::weak_ptr<WsSession> owner, std::string address)
        : server(s), session(std::move(owner)), client(std::move(address)) {}
    template<class F> void with(F&& f) {
        std::visit([&](auto& s) { if constexpr (!std::is_same_v<std::decay_t<decltype(s)>, std::monostate>) f(s); }, stream);
    }
    // Whether `more` bytes of requests can wait to be sent too.
    bool full(std::size_t more) const { return outgoing.size() >= max_outgoing || pending + more > max_pending; }
    template<class S> void handshake(S& s, std::string const& host, std::string const& path) {
        s.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
        // A search's update is a few alternatives, kilobytes; one past 4 MB ends the relay rather
        // than be read whole (Beast's default is 16 MB), and the session's stream limit applies
        // to what is relayed.
        s.read_message_max(4 << 20);
        // xrpld meters a client by its address; with this machine in its secure_gateway it reads this one.
        s.set_option(websocket::stream_base::decorator([address = client](websocket::request_type& r) { if (!address.empty()) r.set("X-Forwarded-For", address); }));
        s.async_handshake(host, path, [self = shared_from_this()](beast::error_code ec) { if (ec || self->closed) return self->fail(); self->opened(); });
    }
    // Connects to `addresses`, never looking the upstream's host up again: another address it
    // has may not be one the upstream was checked at.
    void start(std::vector<net::ip::address> const& addresses) {
        auto const& upstream = server.forward_ws_check.upstream;
        if (!upstream || addresses.empty()) { fail(); return; }
        for (auto const& address : addresses) endpoints.emplace_back(address, upstream->number);
        auto self = shared_from_this();
        auto host = upstream->host, path = upstream->path;
        if (upstream->secure) {
            tls = server.client_tls();
            auto& s = stream.template emplace<Secure>(server.io, *tls);
            SSL_set_tlsext_host_name(s.next_layer().native_handle(), upstream->name.c_str());
            s.next_layer().set_verify_callback(net::ssl::host_name_verification(upstream->name));
            beast::get_lowest_layer(s).expires_after(std::chrono::seconds(15));
            beast::get_lowest_layer(s).async_connect(endpoints, [self, host, path](beast::error_code ec, tcp::endpoint) {
                if (ec || self->closed) return self->fail();
                std::get<Secure>(self->stream).next_layer().async_handshake(net::ssl::stream_base::client, [self, host, path](beast::error_code ec) {
                    if (ec || self->closed) return self->fail();
                    auto& s = std::get<Secure>(self->stream);
                    beast::get_lowest_layer(s).expires_never();
                    self->handshake(s, host, path);
                });
            });
        } else {
            auto& s = stream.template emplace<Plain>(server.io);
            beast::get_lowest_layer(s).expires_after(std::chrono::seconds(15));
            beast::get_lowest_layer(s).async_connect(endpoints, [self, host, path](beast::error_code ec, tcp::endpoint) {
                if (ec || self->closed) return self->fail();
                auto& s = std::get<Plain>(self->stream);
                beast::get_lowest_layer(s).expires_never();
                self->handshake(s, host, path);
            });
        }
    }
    void opened() { open = true; read(); if (!outgoing.empty() && !writing) write(); }
    void send(std::string text) {
        if (closed) return;
        pending += text.size();
        outgoing.push_back(std::move(text));
        if (open && !writing) write();
    }
    void write() {
        writing = true;
        with([&](auto& s) {
            s.text(true);
            s.async_write(net::buffer(outgoing.front()), [self = shared_from_this()](beast::error_code ec, std::size_t) {
                self->writing = false;
                if (ec == net::error::operation_aborted) return;  // the stream closed: the read pending tells why
                if (ec) return self->fail();
                self->pending -= self->outgoing.front().size();
                self->outgoing.pop_front();
                if (!self->outgoing.empty()) self->write();
            });
        });
    }
    void read() {
        with([&](auto& s) {
            s.async_read(input, [self = shared_from_this()](beast::error_code ec, std::size_t) {
                if (ec) return self->fail();
                auto text = beast::buffers_to_string(self->input.data()); self->input.consume(self->input.size());
                auto owner = self->session.lock();
                if (!owner || owner->closed) return self->close();
                owner->send(std::make_shared<std::string const>(std::move(text)), true);  // updates stream, as a subscription does
                self->read();
            });
        });
    }
    void close() {
        closed = true;
        beast::error_code ignored;
        with([&](auto& s) { beast::get_lowest_layer(s).socket().close(ignored); });
    }
    // Ends the relay, which failed: the upstream is checked again soon (UpstreamCheck::failed).
    void fail();
};
// Follows the newest ledger while anyone is subscribed (one poll a second), renders each new
// ledger's stream messages once (the ledgerClosed message, and the transactions' messages in
// each API version a subscriber uses) and hands the same text to every session, which picks
// what its subscription wants. Before, every session polled and rendered every ledger itself.
// Its work runs on the server's stream lane, one job at a time: a ledger (its event, its
// transactions and their rendering, in one job), or a version a session newly needs for a
// ledger already rendered (it joined, or changed api_version, after the ledger was rendered).
struct StreamHub : std::enable_shared_from_this<StreamHub> {
    Server& server;
    net::steady_timer timer;
    std::vector<std::weak_ptr<WsSession>> sessions;
    std::deque<std::shared_ptr<Rendered>> recent;  // for sessions joining a ledger late
    std::deque<std::pair<std::shared_ptr<Rendered>, unsigned>> wanted;  // versions sessions wait for
    std::uint32_t seen = 0, newest = 0;  // the last ledger rendered, and the newest closed one known
    bool running = false, working = false, wanted_failed = false;
    // Ledgers are published as xrpld publishes validated ones (LedgerMaster::
    // findNewLedgersToPublish): in order, each whole (its ledgerClosed message and its
    // transactions) or not at all. One that fails is not passed over but tried again at the next
    // tick, however long that lasts, while the newest closed ledger is at most max_gap past the
    // last published. Once it is further, only the newest is published, the ledgers between
    // dropped at once: subscribers see a gap in ledger_index, as xrpld's do (kMaxLedgerGap).
    static constexpr std::uint32_t max_gap = 100;
    std::optional<std::chrono::steady_clock::time_point> waiting_logged;
    // A version a session waits for (wanted) has no such rule in xrpld, which renders each
    // ledger's transactions once for every subscriber. The first one waiting that fails
    // max_failures times in a row (about as many seconds) is given up, published with no
    // transaction messages, so a session is not held behind it for good.
    static constexpr unsigned max_failures = 30;
    unsigned wanted_failures = 0;
    // A ledger's rendering: all its messages, or what failed.
    struct Attempt {
        std::shared_ptr<Rendered> ledger;  // none if any of its messages failed
        std::string error;
    };
    // A version's rendering for a ledger already rendered, or what failed.
    struct VersionAttempt {
        std::optional<Rendered::Version> rendered;
        std::string error;
    };
    explicit StreamHub(Server& s) : server(s), timer(s.io) {}
    std::vector<std::shared_ptr<WsSession>> live() {
        std::vector<std::shared_ptr<WsSession>> out;
        std::erase_if(sessions, [&](auto const& weak) {
            auto session = weak.lock();
            if (!session || session->closed) return true;
            if (session->subscribed) out.push_back(std::move(session));
            return false;
        });
        return out;
    }
    void join(std::shared_ptr<WsSession> const& session) {
        // Starting (no one else subscribed): from the session's ledger (0: the newest at the first poll).
        // Nothing failed before is counted again.
        if (!running) {
            running = true; recent.clear(); wanted.clear(); seen = session->seen; newest = 0;
            wanted_failed = false; wanted_failures = 0; waiting_logged.reset();
            arm();
        }
        else if (!seen) seen = session->seen;  // started, before its first poll
        if (!session->joined) { session->joined = true; sessions.push_back(session); }
        for (auto const& ledger : recent) session->queue(ledger);
        session->pump();
    }
    void want(std::shared_ptr<Rendered> const& ledger, unsigned version) {
        for (auto const& [l, v] : wanted) if (l == ledger && v == version) return;
        wanted.emplace_back(ledger, version);
        work();
    }
    void arm() {
        timer.expires_after(std::chrono::seconds{1});
        timer.async_wait([hub = shared_from_this()](beast::error_code ec) { if (!ec) hub->tick(); });
    }
    // Each second: the newest closed ledger, between jobs. Work that failed is tried again then.
    void tick() {
        if (!working && live().empty()) { running = false; return; }
        wanted_failed = false;
        if (!working) {
            working = true;
            auto& s = server;
            server.stream([&s] {
                auto latest = rpc_request(s.options.dir, "ledger_closed", Json::object(), s.options.forward, {});
                return latest.contains("error") ? std::uint32_t{0} : latest.at("ledger_index").get<std::uint32_t>();
            }, [hub = shared_from_this()](std::uint32_t latest) {
                hub->working = false;
                if (latest) { if (!hub->seen) hub->seen = latest; hub->newest = std::max(hub->newest, latest); }
                hub->work();
            });
        }
        arm();
    }
    // A version's transaction messages for a ledger, rendered (on the stream lane); `error`, if
    // given, gets the error of one that failed.
    static std::optional<Rendered::Version> render_version(Server const& s, std::uint32_t sequence, unsigned version, std::string* error = nullptr) {
        auto value = rpc_request(s.options.dir, "_transaction_events", {{"ledger_index", sequence}, {"api_version", version}}, s.options.forward, {});
        if (value.contains("error")) { if (error) *error = value.value("error", ""); return std::nullopt; }
        Rendered::Version out;
        out.rendered = true;
        for (auto const& item : value.at("events")) {
            auto position = static_cast<std::uint32_t>(out.transactions.size());
            out.transactions.push_back(std::make_shared<std::string const>(item.at("event").dump()));
            auto note = [position](std::vector<std::uint32_t>& positions) { if (positions.empty() || positions.back() != position) positions.push_back(position); };
            for (auto const& a : item.at("accounts"))
                if (auto id = ::xrpl::parseBase58<::xrpl::AccountID>(a.get<std::string>())) note(out.accounts[*id]);
            for (auto const& b : item.value("books", Json::array())) note(out.books[b.get<std::string>()]);
        }
        return out;
    }
    // A ledger's messages (on the stream lane): all of them, the transactions' in each version
    // asked for, or none.
    static Attempt render(Server const& s, std::uint32_t sequence, std::array<bool, 2> versions) {
        Attempt out;
        auto value = rpc_request(s.options.dir, "_ledger_event", {{"ledger_index", sequence}}, s.options.forward, {});
        if (value.contains("error")) { out.error = value.value("error", ""); return out; }
        value.erase("status"); value["type"] = "ledgerClosed";
        auto ledger = std::make_shared<Rendered>();
        ledger->sequence = sequence;
        ledger->closed = std::make_shared<std::string const>(value.dump());
        for (unsigned version : {1u, 2u}) {
            if (!versions[version - 1]) continue;
            auto rendered = render_version(s, sequence, version, &out.error);
            if (!rendered) return out;
            ledger->versions[version - 1] = std::move(*rendered);
        }
        out.ledger = std::move(ledger);
        return out;
    }
    // The next job, unless one is running: versions sessions wait for first, then the next ledger.
    void work() {
        if (working) return;
        auto subscribers = live();
        if (subscribers.empty()) return;
        auto& s = server;
        if (!wanted.empty() && !wanted_failed) {
            auto ledger = wanted.front().first;
            auto version = wanted.front().second;
            working = true;
            server.stream([&s, sequence = ledger->sequence, version] {
                VersionAttempt out;
                out.rendered = render_version(s, sequence, version, &out.error);
                return out;
            }, [hub = shared_from_this(), ledger, version](VersionAttempt attempt) {
                hub->working = false;
                if (!attempt.rendered) {
                    // Tried again at the next tick; until then the next ledgers are rendered. One
                    // that fails max_failures times in a row is published with no transaction
                    // messages, for every session that needs it.
                    if (++hub->wanted_failures < max_failures) { hub->wanted_failed = true; hub->work(); return; }
                    std::cerr << Json{{"event", "stream_transactions_dropped"}, {"ledger_index", ledger->sequence}, {"api_version", version},
                                      {"attempts", max_failures}, {"error", attempt.error}}.dump() << std::endl;
                    attempt.rendered = Rendered::Version{.rendered = true};
                }
                hub->wanted_failures = 0;
                std::erase_if(hub->wanted, [&](auto const& w) { return w.first == ledger && w.second == version; });
                ledger->versions[version - 1] = std::move(*attempt.rendered);
                for (auto const& session : hub->live()) session->pump();
                hub->work();
            });
            return;
        }
        if (!seen || newest <= seen) return;
        if (newest - seen > max_gap) {
            // Too far behind: only the newest is published, as xrpld does past kMaxLedgerGap.
            std::cerr << Json{{"event", "stream_ledgers_skipped"}, {"from", seen + 1}, {"to", newest - 1}}.dump() << std::endl;
            seen = newest - 1;
        }
        std::array<bool, 2> versions{};
        for (auto const& session : subscribers) if (session->wants_transactions()) versions[session->api_version - 1] = true;
        working = true;
        auto sequence = seen + 1;
        server.stream([&s, sequence, versions] { return render(s, sequence, versions); }, [hub = shared_from_this(), sequence](Attempt attempt) {
            hub->working = false;
            if (!attempt.ledger) {
                // Not published, nor passed over: tried again at the next tick.
                auto now = std::chrono::steady_clock::now();
                if (!hub->waiting_logged || now - *hub->waiting_logged >= std::chrono::minutes(1)) {
                    hub->waiting_logged = now;
                    std::cerr << Json{{"event", "stream_ledger_waiting"}, {"ledger_index", sequence}, {"error", attempt.error}}.dump() << std::endl;
                }
                return;
            }
            auto ledger = std::move(attempt.ledger);
            if (hub->waiting_logged) {
                std::cerr << Json{{"event", "stream_ledger_published"}, {"ledger_index", ledger->sequence}}.dump() << std::endl;
                hub->waiting_logged.reset();
            }
            hub->seen = ledger->sequence;
            hub->recent.push_back(ledger);
            if (hub->recent.size() > 8) hub->recent.pop_front();
            for (auto const& session : hub->live()) { session->queue(ledger); session->pump(); }
            hub->work();
        });
    }
};
void WsSession::pump() {
    if (closed || !subscribed) { backlog.clear(); return; }
    while (!holding && !backlog.empty()) {
        auto ledger = backlog.front();
        if (ledger->sequence <= seen) { backlog.pop_front(); continue; }
        if (wants_transactions() && !ledger->versions[api_version - 1].rendered) { server.hub->want(ledger, api_version); return; }
        backlog.pop_front();
        seen = ledger->sequence;
        deliver(*ledger);
    }
}
void PathProxy::fail() {
    if (closed) return;
    server.forward_ws_check.failed([this](std::function<void()> probe) { server.probe_later(std::move(probe)); });
    auto owner = session.lock();
    if (owner) {
        for (auto const& text : outgoing) {
            Json request = Json::parse(text, nullptr, false);
            auto answer = error(::xrpl::RpcNotReady); answer["error_message"] = "The upstream server did not answer.";
            owner->send(WsSession::response_text(std::move(answer), request.is_object() ? request : Json::object()));
        }
        if (owner->paths.get() == this) owner->paths.reset();
    }
    outgoing.clear(); pending = 0;
    close();
}
WsSession::~WsSession() {
    for (auto const& [text, stream] : outgoing) forget(*text, stream);
    server.ws_sessions.erase(this);
    server.release(client); if (paths) paths->close();
}
// The session holding the most bytes queued, of those still open, is disconnected; one closed
// already releases the rest of its queue when its write ends.
void Server::shed() {
    WsSession* largest = nullptr;
    for (auto* session : ws_sessions) if (!session->closed && (!largest || session->queued() > largest->queued())) largest = session;
    if (largest && largest->queued()) largest->drop("max_queued_total");
}
void WsSession::path_find(Json const& request) {
    if (server.options.forward_ws.empty()) {
        auto answer = error(::xrpl::RpcNotSupported);
        answer["error_message"] = "path_find needs [serve] forward_ws, the upstream server's WebSocket.";
        reply(std::move(answer), request); return;
    }
    // The upstream's WebSocket is checked as the HTTP upstream is (UpstreamCheck), its probe on
    // the probes' lane: the network thread never waits for it. A search already open may still
    // be closed or asked for its status when the check stops passing: neither starts one, and the
    // relay's connection is one that was checked (xrpld gives a connection's requests the role of
    // the addresses it joins, which do not change while it is open).
    auto addresses = server.forward_ws_check.ready([this](std::function<void()> probe) { server.probe_later(std::move(probe)); });
    auto subcommand = request.value("subcommand", Json());
    bool ending = paths && (subcommand == "close" || subcommand == "status");
    if ((!addresses || addresses->empty()) && !ending) { reply(unchecked_upstream(), request); return; }
    // What would give the request a role of its own (xrpld reads admin_user and admin_password
    // from each request) or carries a key is not sent on. Queued before a new connection
    // starts, so a connection that fails at once answers it.
    auto relayed = request;
    for (auto name : {"admin_user", "admin_password", "secret", "seed", "seed_hex", "passphrase"}) relayed.erase(name);
    auto text = relayed.dump();
    if (paths && paths->full(text.size())) { reply(error(::xrpl::RpcTooBusy), request); return; }
    bool fresh = !paths;
    if (fresh) paths = std::make_shared<PathProxy>(server, weak_from_this(), client);
    auto proxy = paths;
    proxy->send(std::move(text));
    if (fresh) proxy->start(*addresses);
}
void stream_join(std::shared_ptr<WsSession> const& session) {
    if (!session->server.hub) session->server.hub = std::make_shared<StreamHub>(session->server);
    session->server.hub->join(session);
}
struct HttpSession : std::enable_shared_from_this<HttpSession> {
    using Plain = beast::tcp_stream;
    using Secure = beast::ssl_stream<beast::tcp_stream>;
    Server& server;
    std::shared_ptr<net::ssl::context> tls;  // set on a TLS listener's sessions
    std::string client;  // the address the request came from
    std::variant<Plain, Secure> stream;
    beast::flat_buffer input;
    std::optional<http::request_parser<http::string_body>> parser;  // a new one for each request
    http::response<http::string_body> response;
    bool transferred = false;
    // Kept open for the client's next request (HTTP/1.1's default, as xrpld does), up to this
    // many requests and 15 s idle, so an HTTPS client does not pay a TLS handshake each time.
    static constexpr unsigned max_requests_per_connection = 1000;
    unsigned served = 0;
    bool keep_alive = false;
    static std::variant<Plain, Secure> open(tcp::socket socket, std::shared_ptr<net::ssl::context> const& tls) {
        if (tls) return std::variant<Plain, Secure>(std::in_place_type<Secure>, std::move(socket), *tls);
        return std::variant<Plain, Secure>(std::in_place_type<Plain>, std::move(socket));
    }
    HttpSession(Server& s, tcp::socket socket, std::string address, std::shared_ptr<net::ssl::context> context)
        : server(s), tls(std::move(context)), client(std::move(address)), stream(open(std::move(socket), tls)) {
        server.admit(client);
    }
    ~HttpSession() { if (!transferred) server.release(client); }
    template<class F> void with(F&& f) { std::visit([&](auto& s) { f(s); }, stream); }
    // As xrpld's ServerHandler::processRequest: the result, with the request's id, jsonrpc and
    // ripplerpc beside it. An error echoes the request (`method`'s params, its secrets masked, with
    // the method as `command`) where rpc_request has not; one asked with ripplerpc "2.0" or
    // later (compared as strings, as xrpld does) is under `error` instead, with code and message
    // beside error_code and in place of error_message, and no echo.
    static std::string body_text(Json result, Json const& params = Json::object(), std::string const& method = {}) {
        bool failed = result.contains("error");
        if (failed && !method.empty() && !result.contains("request")) {
            auto request = params; request["command"] = method; result["request"] = rpc::masked(std::move(request));
        }
        auto ripplerpc = params.contains("ripplerpc") && params.at("ripplerpc").is_string() ? params.at("ripplerpc").get<std::string>() : std::string("1.0");
        Json body = Json::object();
        if (ripplerpc >= "2.0" && failed) {
            result.erase("request");
            result["status"] = "error";
            // As xrpld's operator[], a field missing is read as null (and error_code then kept as null).
            Json code = result["error_code"], message = result["error_message"];
            result["code"] = std::move(code); result["message"] = std::move(message);
            result.erase("error_message");
            body["error"] = std::move(result);
        } else {
            body["result"] = std::move(result);
        }
        for (auto name : {"jsonrpc", "ripplerpc", "id"}) if (params.contains(name)) body[name] = params.at(name);
        return body.dump();
    }
    void respond(Json result) { respond_text(body_text(std::move(result))); }
    // A request xrpld refuses before running it: 400 and a line of text (its httpReply).
    void refuse(std::string const& why) { respond_text(why + "\r\n", http::status::bad_request); }
    void respond_text(std::string body, http::status status = http::status::ok) {
        response.version(11); response.result(status); response.set(http::field::content_type, "application/json");
        response.set(http::field::server, "xrpla"); response.keep_alive(keep_alive); response.body() = std::move(body); response.prepare_payload();
        with([&](auto& s) {
            beast::get_lowest_layer(s).expires_after(std::chrono::seconds{30});
            http::async_write(s, response, [self = shared_from_this()](beast::error_code ec, std::size_t) {
                if (ec || !self->keep_alive) { self->finish(); return; }
                self->response = {};
                self->read(std::chrono::seconds{15});
            });
        });
    }
    // After the response: TLS says goodbye first (close_notify), then the connection closes.
    void finish() {
        if (auto* s = std::get_if<Secure>(&stream)) { s->async_shutdown([self = shared_from_this()](beast::error_code) {}); return; }
        beast::error_code ec; std::get<Plain>(stream).socket().shutdown(tcp::socket::shutdown_send, ec);
    }
    void start() {
        with([&](auto& s) { beast::get_lowest_layer(s).expires_after(std::chrono::seconds{30}); });
        if (auto* s = std::get_if<Secure>(&stream)) {
            s->async_handshake(net::ssl::stream_base::server, [self = shared_from_this()](beast::error_code ec) { if (!ec) self->read(); });
            return;
        }
        read();
    }
    void read(std::chrono::seconds wait = std::chrono::seconds{30}) {
        parser.emplace(); parser->body_limit(64 * 1024);
        with([&](auto& s) {
            beast::get_lowest_layer(s).expires_after(wait);
            http::async_read(s, input, *parser, [self = shared_from_this()](beast::error_code ec, std::size_t) { self->received(ec); });
        });
    }
    void received(beast::error_code ec) {
        {
            auto self = shared_from_this();
            if (ec) return;  // closed, idle too long, or not HTTP
            auto request = self->parser->release();
            self->keep_alive = request.keep_alive() && ++self->served < max_requests_per_connection;
            if (websocket::is_upgrade(request)) {
                self->transferred = true;
                self->with([&](auto& s) {
                    beast::get_lowest_layer(s).expires_never();
                    std::make_shared<WsSession>(self->server, std::move(s), self->client, self->tls)->start(std::move(request));
                });
                return;
            }
            if (request.method() != http::verb::post) { self->respond(error(::xrpl::RpcInvalidParams)); return; }
            Json document;
            try { document = parse_request(request.body()); }
            catch (Unparsable const& failure) { self->refuse(std::string("Unable to parse request: ") + failure.what()); return; }
            catch (std::exception const& failure) {  // nlohmann's words where the Reader's would be its own
                self->refuse(std::string("Unable to parse request: * ") + failure.what() + "\n"); return;
            }
            // xrpld's checks, in its order, each a 400.
            // As xrpld's `!jsonOrig || !jsonOrig.isObject()`: an empty object is false too, and
            // the Reader, having parsed it, has no message to add.
            if (!document.is_object() || document.empty()) { self->refuse("Unable to parse request: "); return; }
            auto const& listed = document.value("params", Json());
            if (listed.is_array() && !listed.empty() && listed[0].is_object() && !api_version_valid(listed[0])) { self->refuse("invalid_API_version"); return; }
            if (!document.contains("method") || document.at("method").is_null()) { self->refuse("Null method"); return; }
            if (!document.at("method").is_string()) { self->refuse("method is not string"); return; }
            auto method = document.at("method").get<std::string>();
            if (method.empty()) { self->refuse("method is empty"); return; }
            Json params = Json::object();
            // As xrpld's `if (!params)`: null, and an empty array, object or string, are no params.
            bool none = listed.is_null() || (listed.is_array() && listed.empty()) || (listed.is_object() && listed.empty()) ||
                        (listed.is_string() && listed.get_ref<std::string const&>().empty());
            if (!none) {
                if (!listed.is_array() || listed.size() != 1 || !(listed[0].is_object() || listed[0].is_null())) { self->refuse("params unparsable"); return; }
                if (listed[0].is_object()) params = listed[0];
            }
            if (params.contains("ripplerpc") && !params.at("ripplerpc").is_string()) { self->refuse("ripplerpc is not a string"); return; }
            if (internal(method, self->client, self->server.options.loopback_trusted)) {
                self->respond_text(body_text(error(::xrpl::RpcUnknownCommand), params, method)); return;
            }
            self->server.dispatch_text(method, params, [params, method](Json result) { return body_text(std::move(result), params, method); },
                                       [self](std::string body) { self->respond_text(std::move(body)); }, self->client);
        }
    }
};
struct Listener : std::enable_shared_from_this<Listener> {
    Server& server;
    tcp::acceptor acceptor;
    net::steady_timer pause;  // after a failed accept
    bool secure;  // TLS: HTTPS and WSS
    bool failing = false;
    Listener(Server& s, std::string const& endpoint, bool tls) : server(s), acceptor(s.io), pause(s.io), secure(tls) {
        auto colon = endpoint.rfind(':'); if (colon == std::string::npos) throw Error("Listen address must be IP:PORT");
        auto ip = endpoint.substr(0, colon), port = endpoint.substr(colon + 1);
        if (ip.starts_with('[') && ip.ends_with(']')) ip = ip.substr(1, ip.size() - 2);
        unsigned number = 0; auto [end, ec] = std::from_chars(port.data(), port.data() + port.size(), number);
        if (ec != std::errc{} || end != port.data() + port.size() || !number || number > 65535) throw Error("Invalid listen port");
        tcp::endpoint target(net::ip::make_address(ip), static_cast<unsigned short>(number));
        acceptor.open(target.protocol()); acceptor.set_option(net::socket_base::reuse_address(true)); acceptor.bind(target); acceptor.listen(128);
    }
    void start() {
        acceptor.async_accept([self = shared_from_this()](beast::error_code ec, tcp::socket socket) {
            if (!self->acceptor.is_open()) return;
            if (ec) {
                // Out of file descriptors, say, which the next accept meets at once: wait a moment
                // rather than spin the network thread, and say so once until an accept succeeds.
                if (!self->failing) std::cerr << Json{{"event", "accept_failed"}, {"error", ec.message()}}.dump() << std::endl;
                self->failing = true;
                self->pause.expires_after(std::chrono::milliseconds{100});
                self->pause.async_wait([self](beast::error_code ec) { if (!ec) self->start(); });
                return;
            }
            self->failing = false;
            // Without TCP_NODELAY, a response's last partial segment waits for the client's
            // delayed ACK (about 40 ms) whenever the response spans more than one segment.
            beast::error_code ignored; socket.set_option(tcp::no_delay(true), ignored);
            beast::error_code unknown; auto peer = socket.remote_endpoint(unknown);
            if (!unknown) {
                auto address = plain(peer.address());
                if (auto reason = self->server.refusal(address)) self->server.refuse(reason, address.to_string());
                else std::make_shared<HttpSession>(self->server, std::move(socket), address.to_string(), self->secure ? self->server.tls : nullptr)->start();
            }
            self->start();
        });
    }
};
// A TLS server context from a PEM certificate chain and private key: TLS 1.2 and 1.3 only.
std::shared_ptr<net::ssl::context> tls_context(fs::path const& cert, fs::path const& key) {
    auto context = std::make_shared<net::ssl::context>(net::ssl::context::tls_server);
    context->set_options(net::ssl::context::default_workarounds | net::ssl::context::no_sslv2 | net::ssl::context::no_sslv3 |
                         net::ssl::context::no_tlsv1 | net::ssl::context::no_tlsv1_1 | net::ssl::context::single_dh_use);
    // An idle connection's read and write buffers (about 34 KB) are freed until it next has data.
    SSL_CTX_set_mode(context->native_handle(), SSL_MODE_RELEASE_BUFFERS);
    try {
        context->use_certificate_chain_file(cert.string());
        context->use_private_key_file(key.string(), net::ssl::context::pem);
    } catch (std::exception const& e) { throw Error("TLS certificate " + cert.string() + " or key " + key.string() + ": " + e.what()); }
    if (SSL_CTX_check_private_key(context->native_handle()) != 1) throw Error("TLS key " + key.string() + " does not match certificate " + cert.string());
    return context;
}
// Every minute, a certificate or key file that changed (a renewal) is loaded for new sessions;
// one that fails to load leaves the current one in use.
void watch_tls(Server& server, std::shared_ptr<net::steady_timer> timer) {
    timer->expires_after(std::chrono::minutes{1});
    timer->async_wait([&server, timer](beast::error_code ec) {
        if (ec) return;
        std::error_code a, b;
        auto cert = fs::last_write_time(server.options.tls_cert, a), key = fs::last_write_time(server.options.tls_key, b);
        if (!a && !b && (cert != server.tls_cert_time || key != server.tls_key_time)) {
            try {
                server.tls = tls_context(server.options.tls_cert, server.options.tls_key);
                server.tls_cert_time = cert; server.tls_key_time = key;
                std::cerr << Json{{"event", "tls_reloaded"}, {"cert", server.options.tls_cert.string()}}.dump() << std::endl;
            } catch (std::exception const& error) {
                std::cerr << Json{{"event", "tls_reload_failed"}, {"error", error.what()}}.dump() << std::endl;
            }
        }
        watch_tls(server, timer);
    });
}
}
void serve(ServeOptions const& options) {
    if (!options.threads || options.threads > 32) throw Error("Server threads must be between 1 and 32");
    configure_network(options.network_id);
    if (options.http.empty() && options.ws.empty() && options.https.empty() && options.wss.empty()) throw Error("At least one listener is required");
    bool secure = !options.https.empty() || !options.wss.empty();
    if (secure && (options.tls_cert.empty() || options.tls_key.empty())) throw Error("HTTPS and WSS listeners need a TLS certificate and key");
    std::set<std::string> plain_endpoints, secure_endpoints;
    for (auto const& e : {options.http, options.ws}) if (!e.empty()) plain_endpoints.insert(e);
    for (auto const& e : {options.https, options.wss}) if (!e.empty()) secure_endpoints.insert(e);
    for (auto const& e : secure_endpoints) if (plain_endpoints.contains(e)) throw Error("Listener " + e + " cannot be both plain and TLS");
    // A piece cache is opened (and its query index checked) before listening, like a store.
    if (is_piece_cache(options.dir)) {
        auto cache_mb = options.cache_mb;
        if (!cache_mb) {
            std::size_t memory_kb = 0;
            std::ifstream meminfo("/proc/meminfo");
            for (std::string name; meminfo >> name;) {
                if (name == "MemTotal:") { meminfo >> memory_kb; break; }
                meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            }
            // A quarter of memory (3-64 GB): a busy span of history decodes the same segments and
            // pages again under less (at an eighth, 16 GB, mainnet's full mix ran a third as fast).
            cache_mb = std::clamp<std::size_t>(memory_kb / 1024 / 4, 3072, 65536);
        }
        configure_piece_caches(cache_mb, options.warm_segments);
        std::cerr << Json{{"event", "piece_cache"}, {"cache_mb", cache_mb}}.dump() << std::endl;
        if (!options.tail.empty()) follow_tail(options.dir, options.tail, options.tail_key);
        else (void)open_piece_history(options.dir);
    }
    else if (!options.tail.empty()) throw Error("A tail needs a piece cache in --dir");
    else (void)store_status(options.dir);
    // The upstreams must not grant this server admin (xrpld does by address, and its localhost
    // ports are admin ones): every client's forwarded request would run with it. Each is asked
    // now, at every address it has (UpstreamCheck): one that grants it is a configuration error;
    // one not reached is asked again when requests come, and is sent none until it passes.
    if (!options.forward.empty() && ((!options.forward.starts_with("http://") && !options.forward.starts_with("https://")) || !upstream_url(options.forward)))
        throw Error("[serve] forward must be an http:// or https:// URL with a host: " + options.forward);
    if (!options.forward_ws.empty() && ((!options.forward_ws.starts_with("ws://") && !options.forward_ws.starts_with("wss://")) || !upstream_url(options.forward_ws)))
        throw Error("[serve] forward_ws must be a ws:// or wss:// URL with a host: " + options.forward_ws);
    auto ask = [](std::string const& name, std::string const& url, AddressProbe probe) {
        if (url.empty()) return Probe{};
        auto found = examined(url, probe);
        if (found.role == UpstreamRole::admin)
            throw Error("[serve] " + name + " " + url + " answers this server as an admin (get_counts) " + found.why + ": use a public (non-admin) port instead");
        return found;
    };
    auto forward_probe = ask("forward", options.forward, probe_http);
    auto forward_ws_probe = ask("forward_ws", options.forward_ws, probe_ws);
    // Kept past serve() when requests are still running at a stop (below): their threads use it.
    auto& server = *new Server(options);
    if (!options.forward.empty()) server.forward_check.settle(forward_probe);
    if (!options.forward_ws.empty()) server.forward_ws_check.settle(forward_ws_probe);
    auto tls_timer = std::make_shared<net::steady_timer>(server.io);
    if (secure) {
        server.tls = tls_context(options.tls_cert, options.tls_key);
        server.tls_cert_time = fs::last_write_time(options.tls_cert); server.tls_key_time = fs::last_write_time(options.tls_key);
        watch_tls(server, tls_timer);
    }
    std::vector<std::shared_ptr<Listener>> listeners;
    for (auto const& [endpoints, tls] : {std::pair{&plain_endpoints, false}, {&secure_endpoints, true}})
        for (auto const& endpoint : *endpoints) {
            auto listener = std::make_shared<Listener>(server, endpoint, tls); listener->start(); listeners.push_back(listener);
        }
    auto signals = std::make_unique<net::signal_set>(server.io, SIGINT, SIGTERM);
    signals->async_wait([&](beast::error_code, int) { tls_timer->cancel(); server.io.stop(); });
    std::cerr << Json{{"http", options.http}, {"ws", options.ws}, {"https", options.https}, {"wss", options.wss}, {"event", "listening"}}.dump() << '\n';
    server.io.run();
    // Stopping: requests still queued are dropped (their connections are closing). Those already
    // running get 10 s; a history query can take far longer (a busy order book, a request that
    // will time out), and a stop that waited for them held a restart for minutes. Past that the
    // server is left to them and serve() returns: the process is ending, and its callers leave
    // with std::_Exit, without the static destructors those requests still use.
    auto lanes = {&server.fast, &server.heavy, &server.forwarded, &server.streams, &server.probes};
    for (auto* lane : lanes) lane->threads.stop();
    for (auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10); server.running && std::chrono::steady_clock::now() < until;)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (server.running) {
        std::cerr << Json{{"event", "stopped_with_requests_running"}, {"requests", server.running.load()}}.dump() << std::endl;
        return;
    }
    for (auto* lane : lanes) lane->threads.join();
    listeners.clear(); signals.reset(); tls_timer.reset();  // what uses the server's io goes first
    delete &server;
}
}
