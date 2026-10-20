#include <xrpla/core/operations.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <curl/curl.h>
#include <sys/socket.h>
#include <chrono>
#include <memory>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <utility>

namespace xrpla::core {
namespace {
namespace net = boost::asio;
std::size_t receive(char* data, std::size_t size, std::size_t count, void* p) noexcept {
    auto& bytes = *static_cast<Bytes*>(p);
    auto n = size * count;
    if (n > 512ULL * 1024 * 1024 - bytes.size()) return 0;
    try { bytes.append(data, n); return n; } catch (...) { return 0; }
}
// An IPv4 address written as IPv6 (::ffff:a.b.c.d) is the IPv4 address it is.
net::ip::address unmapped(net::ip::address const& address) {
    if (address.is_v6() && address.to_v6().is_v4_mapped()) return net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
    return address;
}
// The addresses a CURLOPT_RESOLVE entry ("HOST:PORT:ADDRESS[,ADDRESS]...", an IPv6 address in
// brackets) names, as pin_connections takes them. One that is not an address is left out: a
// request pinned to none goes nowhere.
std::vector<std::string> pinned_addresses(std::string const& entry) {
    std::vector<std::string> out;
    auto port = entry.find(':');
    auto list = port == std::string::npos ? port : entry.find(':', port + 1);
    if (list == std::string::npos) return out;
    for (auto start = list + 1; start <= entry.size();) {
        auto end = std::min(entry.find(',', start), entry.size());
        auto text = entry.substr(start, end - start);
        if (text.size() > 2 && text.front() == '[' && text.back() == ']') text = text.substr(1, text.size() - 2);
        boost::system::error_code ec;
        auto address = net::ip::make_address(text, ec);
        if (!ec) out.push_back(unmapped(address).to_string());
        start = end + 1;
    }
    return out;
}
// Whether libcurl's connection to `target` goes to one of `allowed` (pin_connections).
bool allowed_target(std::vector<std::string> const& allowed, curlsocktype purpose, curl_sockaddr const* target) noexcept {
    try {
        net::ip::tcp::endpoint endpoint;
        if (purpose != CURLSOCKTYPE_IPCXN || (target->family != AF_INET && target->family != AF_INET6) || target->addrlen > endpoint.capacity()) return false;
        std::memcpy(endpoint.data(), &target->addr, target->addrlen);
        endpoint.resize(target->addrlen);
        return endpoint.data()->sa_family == target->family && std::find(allowed.begin(), allowed.end(), unmapped(endpoint.address()).to_string()) != allowed.end();
    } catch (...) { return false; }
}
// libcurl's socket for a connection (CURLOPT_OPENSOCKETFUNCTION), refused unless it goes to one
// of `allowed` (pin_connections).
curl_socket_t pinned_socket(void* allowed, curlsocktype purpose, curl_sockaddr* target) noexcept {
    if (!allowed_target(*static_cast<std::vector<std::string> const*>(allowed), purpose, target)) return CURL_SOCKET_BAD;
    return ::socket(target->family, target->socktype, target->protocol);
}
thread_local bool reached = true;  // RpcClient::last_reached
}
void pin_connections(void* curl, std::vector<std::string> const& allowed) {
    curl_easy_setopt(static_cast<CURL*>(curl), CURLOPT_OPENSOCKETFUNCTION, pinned_socket);
    curl_easy_setopt(static_cast<CURL*>(curl), CURLOPT_OPENSOCKETDATA, const_cast<std::vector<std::string>*>(&allowed));
}
bool RpcClient::last_reached() { return reached; }
Json RpcClient::call(std::string const& method, Json params) const {
    reached = true;
    if (!url.starts_with("https://") && !url.starts_with("http://")) throw Error("RPC source must be HTTP or HTTPS");
    auto body = Json{{"method", method}, {"params", Json::array({params})}}.dump();
    auto cached = cache / (sha256(url + body) + ".json");
    if (!cache.empty()) {
        safe_cache_path(cached);
        if (fs::exists(cached)) return Json::parse(read_file(cached, 512ULL * 1024 * 1024));
        fs::create_directories(cache);
    }
    unsigned tries = std::max(1u, attempts);
    for (unsigned attempt = 0; attempt < tries; ++attempt) {
        if (delay_ms || attempt) std::this_thread::sleep_for(std::chrono::milliseconds{delay_ms + (attempt ? 500U << attempt : 0)});
        Bytes response;
        // Reset options, retaining the per-worker connection cache. Its connections were opened
        // under the pin they were made with: when the pin changes (the upstream checked again at
        // other addresses), a new handle, with none, is used, so no request goes on a connection
        // to an address no longer checked.
        thread_local std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), curl_easy_cleanup);
        thread_local std::string pinned_with;
        if (!curl || resolve != pinned_with) { curl.reset(curl_easy_init()); pinned_with = resolve; }
        if (!curl) throw Error("Cannot allocate RPC client");
        curl_easy_reset(curl.get());
        auto raw_headers = curl_slist_append(nullptr, "Content-Type: application/json");
        if (!forwarded_for.empty()) raw_headers = curl_slist_append(raw_headers, ("X-Forwarded-For: " + forwarded_for).c_str());
        std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw_headers, curl_slist_free_all);
        // An entry replaces the one the handle's name cache has for the same host and port.
        std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> pinned(resolve.empty() ? nullptr : curl_slist_append(nullptr, resolve.c_str()), curl_slist_free_all);
        if (!resolve.empty() && !pinned) throw Error("Cannot allocate RPC client");
        auto allowed = pinned_addresses(resolve);
        curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
        if (pinned) { curl_easy_setopt(curl.get(), CURLOPT_RESOLVE, pinned.get()); pin_connections(curl.get(), allowed); }
        // HTTP/1.1, as xrpld speaks it: never HTTP/2, which libcurl offers over TLS.
        curl_easy_setopt(curl.get(), CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        // Never through a proxy the environment names (http_proxy, HTTPS_PROXY, ALL_PROXY): the
        // pin does not apply to a proxy's connection, and a forwarded request's role upstream is
        // the one of the address it arrives from.
        curl_easy_setopt(curl.get(), CURLOPT_PROXY, "");
        curl_easy_setopt(curl.get(), CURLOPT_NOPROXY, "*");
        if (auto bundle = std::getenv("SSL_CERT_FILE"); bundle && *bundle) curl_easy_setopt(curl.get(), CURLOPT_CAINFO, bundle);
        else if (fs::exists("/etc/ssl/certs/ca-certificates.crt")) curl_easy_setopt(curl.get(), CURLOPT_CAINFO, "/etc/ssl/certs/ca-certificates.crt");
        curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.data());
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size()));
        curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, receive);
        curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 0L);
        curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT, 15L);
        curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, 60L);
        curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl.get(), CURLOPT_USERAGENT, "xrpla/" XRPLA_VERSION);
        auto code = curl_easy_perform(curl.get());
        long status = 0; curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
        // An answer refused for its size (receive) was still an answer.
        reached = code == CURLE_OK || code == CURLE_WRITE_ERROR;
        if (code != CURLE_OK || status == 429 || status >= 500) {
            if (attempt + 1 < tries) continue;
            throw Error("RPC " + method + " failed: HTTP " + std::to_string(status) + " " + curl_easy_strerror(code));
        }
        if (status != 200) throw Error("RPC " + method + " returned HTTP " + std::to_string(status));
        auto document = Json::parse(response);
        if (!document.contains("result") || !document["result"].is_object()) throw Error("Invalid RPC response envelope");
        auto result = document.at("result");
        if (result.value("error", "") == "tooBusy" || result.value("error", "") == "slowDown") {
            if (attempt + 1 < tries) continue;
        }
        // Cache only immutable ledger-pinned extraction calls, never forwarding
        // requests or transient failures. entryNotFound is a deletion postimage.
        // A reply the source has not validated (a ledger it has only closed) may
        // yet differ: kept only once validated.
        if (!cache.empty() && (params.contains("ledger_hash") ||
            (params.contains("ledger_index") && params.at("ledger_index").is_number_unsigned())) &&
            (!result.contains("error") || result["error"] == "entryNotFound") &&
            (result.contains("validated") ? result["validated"] == true : method != "ledger")) {
            auto temp = cached; temp += ".part";
            write_file(temp, result.dump()); durable_rename(temp, cached);
        }
        return result;
    }
    throw Error("RPC retries exhausted");
}
std::vector<Json> RpcClient::call_many(std::string const& method, std::vector<Json> const& params, unsigned jobs) const {
    if (!jobs || jobs > 32) throw Error("RPC jobs must be between 1 and 32");
    std::vector<Json> results(params.size());
    std::atomic<std::size_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex mutex;
    std::exception_ptr failure;
    std::vector<std::jthread> workers;
    for (unsigned n = 0; n < jobs && n < params.size(); ++n) workers.emplace_back([&] {
        while (!failed.load()) {
            auto index = next.fetch_add(1);
            if (index >= params.size()) break;
            try { results[index] = call(method, params[index]); }
            catch (...) { std::lock_guard guard(mutex); if (!failure) failure = std::current_exception(); failed = true; }
        }
    });
    workers.clear();
    if (failure) std::rethrow_exception(failure);
    return results;
}
}
