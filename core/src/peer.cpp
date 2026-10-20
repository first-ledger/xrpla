// A session on the XRPL peer network (peer.hpp).
#include "peer.hpp"
#include "validators.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/proto/xrpl.pb.h>
#include <xrpl/protocol/LedgerHeader.h>
#include <xrpl/protocol/PublicKey.h>
#include <xrpl/protocol/SecretKey.h>
#include <xrpl/protocol/tokens.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/v6_only.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <sodium.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <condition_variable>
#include <initializer_list>
#include <iostream>
#include <map>
#include <deque>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <unistd.h>

namespace xrpla::core {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace r = ::xrpl;
using tcp = net::ip::tcp;

namespace {
std::string base64(Bytes const& data) {
    std::string out(sodium_base64_encoded_len(data.size(), sodium_base64_VARIANT_ORIGINAL), '\0');
    sodium_bin2base64(out.data(), out.size(), reinterpret_cast<unsigned char const*>(data.data()), data.size(), sodium_base64_VARIANT_ORIGINAL);
    out.resize(std::strlen(out.c_str()));
    return out;
}
Bytes unbase64(std::string const& text) {
    Bytes out(text.size(), '\0');
    std::size_t length = 0;
    if (sodium_base642bin(reinterpret_cast<unsigned char*>(out.data()), out.size(), text.data(), text.size(), " \r\n", &length, nullptr,
                          sodium_base64_VARIANT_ORIGINAL) != 0) throw Error("Invalid base64 in peer handshake");
    out.resize(length);
    return out;
}
// The session value both sides sign: SHA-512 half of the XOR of each Finished message's SHA-512.
r::uint256 shared_value(SSL* ssl) {
    auto digest = [&](bool peer) {
        unsigned char message[1024];
        auto length = peer ? SSL_get_peer_finished(ssl, message, sizeof message) : SSL_get_finished(ssl, message, sizeof message);
        if (length < 12) throw Error("Peer TLS session has no Finished message");
        std::array<unsigned char, 64> hash{};
        SHA512(message, length, hash.data());
        return hash;
    };
    auto mine = digest(false), theirs = digest(true);
    bool zero = true;
    for (std::size_t i = 0; i < mine.size(); ++i) { mine[i] ^= theirs[i]; zero = zero && !mine[i]; }
    if (zero) throw Error("Peer TLS session value is zero");
    std::array<unsigned char, 64> half{};
    SHA512(mine.data(), mine.size(), half.data());
    return r::uint256::fromVoid(half.data());
}
std::uint32_t be32(unsigned char const* p) { return std::uint32_t(p[0]) << 24 | std::uint32_t(p[1]) << 16 | std::uint32_t(p[2]) << 8 | p[3]; }
std::uint64_t network_time() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()) - 946684800;
}
// HOST[:PORT], HOST a name, an IPv4 address or a bracketed IPv6 one. Parsed by hand: peers send
// these (TMEndpoints, up to a message's 64 MB), and std::regex recurses once per character. A
// NUL (the resolver would read the name only up to it) or a '%' (an IPv6 zone, naming one of
// this machine's interfaces) is refused.
std::pair<std::string, std::string> split_endpoint(std::string const& endpoint) {
    auto bad = [&] { throw Error("Peer endpoint must be HOST[:PORT]: " + endpoint.substr(0, 128)); };
    if (endpoint.find_first_of(std::string_view("%\0", 2)) != std::string::npos) bad();
    std::string_view rest(endpoint), host;
    if (rest.starts_with('[')) {
        auto close = rest.find(']');
        if (close == std::string_view::npos || close == 1) bad();
        host = rest.substr(1, close - 1); rest.remove_prefix(close + 1);
    } else {
        auto colon = rest.find(':');
        host = rest.substr(0, colon); rest.remove_prefix(colon == std::string_view::npos ? rest.size() : colon);
        if (host.empty() || host.find_first_of("[]") != std::string_view::npos) bad();
    }
    std::string port = "51235";
    if (!rest.empty()) {
        if (rest.front() != ':' || rest.size() == 1 || rest.substr(1).find_first_not_of("0123456789") != std::string_view::npos) bad();
        port = std::string(rest.substr(1));
    }
    return {std::string(host), port};
}
}

NodeIdentity NodeIdentity::ephemeral() {
    auto [pk, sk] = r::randomKeyPair(r::KeyType::Secp256k1);
    return {Bytes(reinterpret_cast<char const*>(pk.data()), pk.size()), Bytes(reinterpret_cast<char const*>(sk.data()), sk.size())};
}
NodeIdentity NodeIdentity::load_or_create(fs::path const& file) {
    if (!fs::exists(file)) {
        auto created = ephemeral();
        if (!file.parent_path().empty()) fs::create_directories(file.parent_path());
        write_file(file, created.secret_key);
        fs::permissions(file, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace);
        return created;
    }
    auto secret = read_file(file, 64);
    if (secret.size() != 32) throw Error("Node identity file must hold a 32-byte secp256k1 secret key");
    r::SecretKey sk(r::Slice(secret.data(), secret.size()));
    auto pk = r::derivePublicKey(r::KeyType::Secp256k1, sk);
    return {Bytes(reinterpret_cast<char const*>(pk.data()), pk.size()), secret};
}
std::string NodeIdentity::base58() const {
    return r::toBase58(r::TokenType::NodePublic, r::PublicKey(r::Slice(public_key.data(), public_key.size())));
}
std::optional<NetworkDefaults> network_defaults(std::uint32_t network_id) {
    static std::vector<NetworkDefaults> const known{
        // xrpld's own bootstrap hubs (OverlayImpl::start: Ripple, ISRDC, XRPKuwait, XRPL Commons),
        // then Ripple's full-history servers, which also hold ledgers most peers have deleted.
        {0, "mainnet", {"r.ripple.com:51235", "sahyadri.isrdc.in:51235", "hubs.xrpkuwait.com:51235", "hub.xrpl-commons.org:51235",
                        "s1.ripple.com:51235", "s2.ripple.com:51235"},
         "https://vl.ripple.com", "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734"},
        {1, "testnet", {"s.altnet.rippletest.net:51235", "r.altnet.rippletest.net:51235"},
         "https://vl.altnet.rippletest.net", "ED264807102805220DA0F312E71FC2C69E1552C9C5790F6C25E3729DEB573D5860"},
        {2, "devnet", {"s.devnet.rippletest.net:51235"},
         "https://vl.devnet.rippletest.net", "EDBB54B0D9AEE071BB37784AF5A9E7CC49AC7A0EFCE868C54532BCB966B9CFC13B"}};
    for (auto const& network : known) if (network.id == network_id) return network;
    return std::nullopt;
}
std::pair<std::string, std::string> network_validator_list(std::uint32_t network_id) {
    auto network = network_defaults(network_id);
    return network ? std::pair{network->list, network->list_key} : std::pair<std::string, std::string>{};
}
std::uint32_t parse_network(std::string const& text) {
    if (text == "main") return 0;
    for (std::uint32_t id = 0; id < 3; ++id) if (network_defaults(id)->name == text) return id;
    std::uint32_t id = 0;
    auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), id);
    if (text.empty() || ec != std::errc{} || end != text.data() + text.size())
        throw Error("Unknown network '" + text + "': mainnet, testnet, devnet or a network ID");
    return id;
}
std::vector<std::string> default_peers(std::uint32_t network_id) {
    auto network = network_defaults(network_id);
    return network ? network->hubs : std::vector<std::string>{};
}
namespace {
std::shared_ptr<net::ssl::context> client_context();
// A GET over TLS to a peer port (xrpld's /crawl), with 15 s limits on connecting and on each read and write.
std::string peer_https_get(std::string const& endpoint, std::string const& target) {
    auto [host, port] = split_endpoint(endpoint);
    net::io_context io;
    auto context = client_context();
    net::ssl::stream<tcp::socket> stream(io, *context);
    tcp::resolver resolver(io);
    beast::error_code failed = net::error::timed_out;
    net::async_connect(stream.next_layer(), resolver.resolve(host, port), [&](beast::error_code ec, tcp::endpoint const&) { failed = ec; });
    io.run_for(std::chrono::seconds(15));
    if (failed) throw Error("Cannot connect to " + endpoint + ": " + failed.message());
    timeval limit{15, 0};
    auto fd = stream.next_layer().native_handle();
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &limit, sizeof limit);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &limit, sizeof limit);
    stream.handshake(net::ssl::stream_base::client);
    http::request<http::empty_body> request{http::verb::get, target, 11};
    request.set(http::field::host, host);
    request.set(http::field::user_agent, "xrpla");
    http::write(stream, request);
    beast::flat_buffer buffer;
    http::response_parser<http::string_body> parser;
    parser.body_limit(16 << 20);
    http::read(stream, buffer, parser);
    if (parser.get().result() != http::status::ok) throw Error(endpoint + target + " answered " + std::to_string(parser.get().result_int()));
    return parser.get().body();
}
// Whether complete_ledgers ("32570-107317299", or ranges joined by commas) holds `sequence`.
bool covers(std::string const& ranges, std::uint32_t sequence) {
    std::stringstream list(ranges);
    for (std::string range; std::getline(list, range, ',');) {
        auto dash = range.find('-');
        try {
            auto first = std::stoul(range.substr(0, dash)), last = dash == std::string::npos ? first : std::stoul(range.substr(dash + 1));
            if (first <= sequence && sequence <= last) return true;
        } catch (std::exception const&) {}
    }
    return false;
}
}
std::vector<std::string> peers_holding(std::uint32_t sequence, std::uint32_t network_id) {
    std::set<std::string> found;
    for (auto const& hub : default_peers(network_id)) {
        try {
            auto crawl = Json::parse(peer_https_get(hub, "/crawl"));
            for (auto const& peer : crawl.at("overlay").at("active")) {
                auto ip = peer.value("ip", std::string());
                std::uint64_t port = 0;
                if (peer.contains("port") && peer.at("port").is_number_unsigned()) port = peer.at("port").get<std::uint64_t>();
                else if (peer.contains("port") && peer.at("port").is_string()) try { port = std::stoul(peer.at("port").get<std::string>()); } catch (std::exception const&) {}
                if (ip.empty() || !port || port > 65535 || !covers(peer.value("complete_ledgers", std::string()), sequence)) continue;
                if (ip.starts_with("::ffff:")) ip = ip.substr(7);
                found.insert((ip.find(':') == std::string::npos ? ip : "[" + ip + "]") + ":" + std::to_string(port));
            }
        } catch (std::exception const&) {}  // another hub may answer
    }
    std::vector<std::string> peers(found.begin(), found.end());
    std::shuffle(peers.begin(), peers.end(), std::mt19937{std::random_device{}()});
    return peers;
}
namespace {
// An endpoint that is an IP address and a port from 1 to 65535, the address unwrapped when it
// is IPv4 mapped into IPv6; nullopt for anything else (a name, say).
std::optional<tcp::endpoint> literal_endpoint(std::string const& endpoint) {
    if (endpoint.size() > 64) return std::nullopt;  // "[ipv6]:port" is at most 47 characters
    try {
        auto [host, port] = split_endpoint(endpoint);
        if (port.size() > 5 || std::stoul(port) == 0 || std::stoul(port) > 65535) return std::nullopt;
        beast::error_code ec;
        auto address = net::ip::make_address(host, ec);
        if (ec) return std::nullopt;
        if (address.is_v6() && address.to_v6().is_v4_mapped()) address = net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
        return tcp::endpoint(address, static_cast<std::uint16_t>(std::stoul(port)));
    } catch (std::exception const&) { return std::nullopt; }
}
}
std::string canonical_endpoint(std::string const& endpoint) {
    auto literal = literal_endpoint(endpoint);
    if (!literal) return endpoint;
    auto address = literal->address().to_string();
    return (literal->address().is_v6() ? "[" + address + "]" : address) + ":" + std::to_string(literal->port());
}
bool public_endpoint(std::string const& endpoint) {
    auto literal = literal_endpoint(endpoint);
    if (!literal) return false;
    try {
        auto address = literal->address();
        if (address.is_unspecified() || address.is_loopback() || address.is_multicast()) return false;
        // Not this network, private, shared (100.64/10), link-local, IETF protocol assignments
        // (192.0.0/24), documentation (192.0.2/24, 198.51.100/24, 203.0.113/24), benchmarking
        // (198.18/15), multicast, reserved and broadcast (224/3).
        auto public_v4 = [](std::uint8_t const* b) {
            return !(b[0] == 0 || b[0] == 10 || b[0] == 127 || (b[0] == 100 && (b[1] & 0xC0) == 64) || (b[0] == 169 && b[1] == 254) ||
                     (b[0] == 172 && (b[1] & 0xF0) == 16) || (b[0] == 192 && b[1] == 168) || (b[0] == 192 && b[1] == 0 && (b[2] == 0 || b[2] == 2)) ||
                     (b[0] == 198 && (b[1] & 0xFE) == 18) || (b[0] == 198 && b[1] == 51 && b[2] == 100) || (b[0] == 203 && b[1] == 0 && b[2] == 113) ||
                     b[0] >= 224);
        };
        if (address.is_v4()) return public_v4(address.to_v4().to_bytes().data());
        auto v6 = address.to_v6();
        auto b = v6.to_bytes();
        auto starts = [&](std::initializer_list<std::uint8_t> prefix) { return std::equal(prefix.begin(), prefix.end(), b.begin()); };
        // Global unicast (2000::/3) only: the rest is link-local, unique local (fc00::/7),
        // discard (100::/64) or reserved, or carries an IPv4 address to some translator (NAT64's
        // 64:ff9b::/96 and 64:ff9b:1::/48, SIIT's ::ffff:0:a.b.c.d, the deprecated IPv4-compatible
        // ::a.b.c.d; a peer names the IPv4 address itself).
        if ((b[0] & 0xE0) != 0x20) return false;
        // 6to4 (2002::/16) is dialled only when the IPv4 address it embeds is public. Teredo
        // (2001::/32) tunnels through a relay to an address it obscures, and 2001:db8::/32 is
        // for documentation.
        if (starts({0x20, 0x02})) return public_v4(b.data() + 2);
        return !starts({0x20, 0x01, 0x00, 0x00}) && !starts({0x20, 0x01, 0x0d, 0xb8});
    } catch (std::exception const&) { return false; }
}
std::string address_group(std::string const& address) {
    beast::error_code ec;
    auto ip = net::ip::make_address(address, ec);
    if (ec) return address;
    if (ip.is_v6() && ip.to_v6().is_v4_mapped()) return net::ip::make_address_v4(net::ip::v4_mapped, ip.to_v6()).to_string();
    if (ip.is_v4()) return ip.to_string();
    auto bytes = ip.to_v6().to_bytes();
    std::fill(bytes.begin() + 8, bytes.end(), 0);
    return net::ip::address_v6(bytes).to_string() + "/64";
}
std::string without_endpoint(std::string text, std::string const& endpoint) {
    if (endpoint.empty()) return text;
    for (auto at = text.find(endpoint); at != std::string::npos; at = text.find(endpoint)) {
        auto from = at > 0 && text[at - 1] == ' ' ? at - 1 : at;
        text.erase(from, at + endpoint.size() - from);
    }
    while (!text.empty() && (text.back() == ':' || text.back() == ' ')) text.pop_back();
    return text;
}
void count_reason(std::map<std::string, std::uint64_t>& counts, std::string reason) {
    if (reason.size() > 160) reason.resize(160);
    if (!counts.contains(reason) && counts.size() >= 32) reason = "other";
    ++counts[reason];
}

std::shared_ptr<PeerResponder> serving_nothing() {
    struct Nothing final : PeerResponder {
        void request(int type, std::string const& payload, PeerRequester const& from) override {
            // Object queries are answered with the objects found (none); fetch packs and
            // transaction lists, like ledger requests, get no reply.
            if (type != protocol::mtGET_OBJECTS) return;
            protocol::TMGetObjectByHash query;
            if (!query.ParseFromString(payload) || !query.query()) return;
            if (query.type() == protocol::TMGetObjectByHash::otFETCH_PACK || query.type() == protocol::TMGetObjectByHash::otTRANSACTIONS) return;
            protocol::TMGetObjectByHash none;
            none.set_type(query.type());
            none.set_query(false);
            if (query.has_ledgerhash()) none.set_ledgerhash(query.ledgerhash());
            from.reply(protocol::mtGET_OBJECTS, none.SerializeAsString());
        }
    };
    static auto const nothing = std::make_shared<Nothing>();
    return nothing;
}

namespace {
bool iequals(std::string_view a, std::string_view b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) { return std::tolower(static_cast<unsigned char>(x)) == std::tolower(static_cast<unsigned char>(y)); });
}
// Whether a comma-separated header value lists `token` (case-insensitive, spaces ignored).
bool lists(std::string_view value, std::string_view token) {
    std::size_t start = 0;
    while (start <= value.size()) {
        auto end = value.find(',', start);
        auto item = value.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start);
        while (!item.empty() && std::isspace(static_cast<unsigned char>(item.front()))) item.remove_prefix(1);
        while (!item.empty() && std::isspace(static_cast<unsigned char>(item.back()))) item.remove_suffix(1);
        if (iequals(item, token)) return true;
        if (end == std::string_view::npos) break;
        start = end + 1;
    }
    return false;
}
// X-Protocol-Ctl: "feature=v[,v]*[;feature=...]"; whether `feature` lists `value`.
// Parsed by hand, item by item (a peer's header is up to 16 KB; std::regex recurses per character).
bool feature(std::string const& header, std::string const& name, std::string const& value) {
    auto trim = [](std::string_view v) {
        while (!v.empty() && std::isspace(static_cast<unsigned char>(v.front()))) v.remove_prefix(1);
        while (!v.empty() && std::isspace(static_cast<unsigned char>(v.back()))) v.remove_suffix(1);
        return v;
    };
    std::string_view rest(header);
    while (!rest.empty()) {
        auto end = rest.find(';');
        auto item = trim(rest.substr(0, end));
        rest.remove_prefix(end == std::string_view::npos ? rest.size() : end + 1);
        auto equals = item.find('=');
        if (equals != std::string_view::npos && trim(item.substr(0, equals)) == name) return lists(trim(item.substr(equals + 1)), value);
    }
    return false;
}
// The X-Protocol-Ctl this side sends: the features it enables (in an answer, those both enable).
std::string features(bool ledger_replay, bool squelch) {
    std::string out;
    if (ledger_replay) out += "ledgerreplay=1;";
    if (squelch) out += "vprr=1;";
    return out;
}
static std::atomic<std::uint64_t> session_ids{0};
std::string upper_hex(Bytes const& data) {
    static char const digits[] = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : data) { out += digits[c >> 4]; out += digits[c & 15]; }
    return out;
}
template<class Message> std::string field(Message const& message, char const* name) {
    auto it = message.find(name);
    return it == message.end() ? std::string() : std::string(it->value());
}
std::shared_ptr<net::ssl::context> client_context() {
    auto context = std::make_shared<net::ssl::context>(net::ssl::context::tls_client);
    // xrpld signs the TLS 1.2 Finished messages; its peers present self-signed certificates.
    context->set_verify_mode(net::ssl::verify_none);
    SSL_CTX_set_min_proto_version(context->native_handle(), TLS1_2_VERSION);
    SSL_CTX_set_max_proto_version(context->native_handle(), TLS1_2_VERSION);
    return context;
}
// As xrpld's: TLS 1.2 with its cipher list and a self-signed RSA-2048 certificate made at start
// (peers authenticate each other by the node keys that sign the session, not certificates).
std::shared_ptr<net::ssl::context> server_context() {
    auto context = std::make_shared<net::ssl::context>(net::ssl::context::tls_server);
    auto* ctx = context->native_handle();
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_cipher_list(ctx, "TLSv1.2:!CBC:!DSS:!PSK:!eNULL:!aNULL");
    context->set_verify_mode(net::ssl::verify_none);
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_RSA_gen(2048), EVP_PKEY_free);
    std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), X509_free);
    if (!key || !cert) throw Error("Cannot create the peer certificate");
    X509_set_version(cert.get(), 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), static_cast<long>(std::random_device{}() & 0x7fffffff));
    X509_gmtime_adj(X509_getm_notBefore(cert.get()), -86400);
    X509_gmtime_adj(X509_getm_notAfter(cert.get()), 10L * 365 * 86400);
    X509_set_pubkey(cert.get(), key.get());
    auto* name = X509_get_subject_name(cert.get());
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<unsigned char const*>("xrpla"), -1, -1, 0);
    X509_set_issuer_name(cert.get(), name);
    if (!X509_sign(cert.get(), key.get(), EVP_sha256()) || SSL_CTX_use_certificate(ctx, cert.get()) != 1 || SSL_CTX_use_PrivateKey(ctx, key.get()) != 1)
        throw Error("Cannot create the peer certificate");
    return context;
}
// The handshake headers both sides send (xrpld's buildHandshake).
template<class Fields> void sign_session(Fields& fields, PeerLocal const& local, r::uint256 const& value) {
    r::PublicKey pk(r::Slice(local.identity.public_key.data(), local.identity.public_key.size()));
    r::SecretKey sk(r::Slice(local.identity.secret_key.data(), local.identity.secret_key.size()));
    auto signature = r::signDigest(pk, sk, value);
    fields.set("Network-ID", std::to_string(local.network_id));
    fields.set("Network-Time", std::to_string(network_time()));
    fields.set("Public-Key", local.identity.base58());
    fields.set("Session-Signature", base64(Bytes(reinterpret_cast<char const*>(signature.data()), signature.size())));
    fields.set("Instance-Cookie", std::to_string(std::random_device{}() | std::uint64_t(std::random_device{}()) << 32));
    if (local.closed)
        if (auto closed = local.closed(); closed && closed->first.size() == 32) {
            fields.set("Closed-Ledger", upper_hex(closed->first));
            if (closed->second.size() == 32) fields.set("Previous-Ledger", upper_hex(closed->second));
        }
}
// The checks xrpld's verifyHandshake makes on the other side's headers; its node key.
template<class Message> std::string verify_session(Message const& message, PeerLocal const& local, r::uint256 const& value, std::string const& who) {
    if (auto id = field(message, "Network-ID"); !id.empty() && id != std::to_string(local.network_id)) throw Error(who + " is on network " + id);
    if (auto time = field(message, "Network-Time"); !time.empty()) {
        std::uint64_t theirs = 0;
        auto [end, ec] = std::from_chars(time.data(), time.data() + time.size(), theirs);
        if (ec != std::errc{} || end != time.data() + time.size()) throw Error(who + " sent an invalid Network-Time");
        auto ours = network_time();
        if ((theirs > ours ? theirs - ours : ours - theirs) > 20) throw Error(who + "'s clock is too far off");
    }
    auto key = r::parseBase58<r::PublicKey>(r::TokenType::NodePublic, field(message, "Public-Key"));
    if (!key || r::publicKeyType(*key) != r::KeyType::Secp256k1) throw Error(who + " sent no valid node public key");
    if (Bytes(reinterpret_cast<char const*>(key->data()), key->size()) == local.identity.public_key) throw Error("Self connection");
    auto signature = unbase64(field(message, "Session-Signature"));
    if (!r::verifyDigest(*key, value, r::Slice(signature.data(), signature.size()), false))
        throw Error(who + " did not sign this TLS session");
    return field(message, "Public-Key");
}
}

struct PeerSession::Impl : std::enable_shared_from_this<Impl> {
    net::io_context io;
    std::shared_ptr<net::ssl::context> tls;
    std::optional<net::ssl::stream<tcp::socket>> stream;
    std::thread thread;
    Info info;
    std::set<int> wanted;
    std::shared_ptr<PeerResponder> responder;
    // Owned by the I/O thread.
    std::string received;
    std::size_t consumed = 0;
    std::array<char, 65536> chunk{};
    std::deque<std::string> outbox;
    std::size_t outbox_bytes = 0;
    std::optional<net::steady_timer> timer;
    std::optional<std::uint32_t> ping;  // the ping awaiting its pong
    unsigned long_queue = 0;            // timer ticks since a message was queued behind fewer than 128
    std::atomic<std::uint64_t> messages_in{0}, bytes_in{0}, messages_out{0}, bytes_out{0};
    // The outbox's size, for responders on other threads.
    std::atomic<std::size_t> queued_messages{0}, queued_bytes{0};
    // Shared with callers.
    mutable std::mutex mutex;
    std::condition_variable arrived;
    std::deque<Message> inbox;
    std::string failure;
    bool closed = false;

    Impl() { info.id = ++session_ids; }
    // Runs the queued asynchronous handshake steps, failing after `seconds`.
    void run_for(int seconds, char const* step) {
        io.restart();
        io.run_for(std::chrono::seconds(seconds));
        if (!io.stopped()) {
            beast::error_code ignored; stream->lowest_layer().close(ignored);
            io.restart(); io.run();
            throw Error(std::string("Peer ") + step + " timed out: " + info.endpoint);
        }
    }
    // After a handshake: `leftover` holds bytes read past it (the first messages).
    void start(std::string leftover) {
        received = std::move(leftover);
        io.restart();
        timer.emplace(io);
        net::post(io, [this] { if (parse()) { read(); tick(); } });
        thread = std::thread([this] {
            try { io.run(); }
            catch (std::exception const& error) { fail(error.what()); }
            fail("Peer connection ended");
        });
    }
    // xrpld's peer timer (PeerImp::onTimer, every 60 s): a peer whose send queue stayed long
    // (Tuning kTargetSendQueue, kSendqIntervals) or that left the last ping unanswered is gone
    // (so a dead session frees its slot); otherwise it is pinged again.
    void tick() {
        timer->expires_after(std::chrono::seconds(60));
        timer->async_wait([this](beast::error_code ec) {
            if (ec) return;
            { std::lock_guard lock(mutex); if (closed) return; }
            if (long_queue++ >= 4) { fail("Large send queue"); return; }
            if (ping) { fail("Ping Timeout"); return; }
            thread_local std::mt19937 random{std::random_device{}()};
            ping = std::uniform_int_distribution<std::uint32_t>()(random);
            protocol::TMPing message;
            message.set_type(protocol::TMPing::ptPING);
            message.set_seq(*ping);
            queue(frame(protocol::mtPING, message.SerializeAsString()));
            tick();
        });
    }
    void fail(std::string const& why) {
        {
            std::lock_guard lock(mutex);
            if (closed) return;
            closed = true; failure = why;
        }
        arrived.notify_all();
        beast::error_code ignored; if (stream) stream->lowest_layer().close(ignored);
        if (timer) timer->cancel();
    }
    PeerRequester requester() {
        PeerRequester from;
        auto weak = weak_from_this();
        from.reply = [weak](int type, std::string const& payload) {
            auto self = weak.lock();
            if (!self) return;
            { std::lock_guard lock(self->mutex); if (self->closed) return; }
            net::post(self->io, [weak, bytes = frame(type, payload)]() mutable { if (auto s = weak.lock()) s->queue(std::move(bytes)); });
        };
        from.drop = [weak](std::string const& why) {
            if (auto self = weak.lock()) net::post(self->io, [weak, why] { if (auto s = weak.lock()) s->fail(why); });
        };
        from.backlogged = [weak] {
            auto self = weak.lock();
            return self && (self->queued_messages >= 192 || self->queued_bytes >= (16u << 20));
        };
        from.public_key = info.public_key; from.endpoint = info.endpoint; from.ledger_replay = info.ledger_replay;
        from.address = info.address; from.session = info.id;
        return from;
    }
    void deliver(int type, std::string payload) {
        if (type == protocol::mtPING) {
            protocol::TMPing message;
            if (!message.ParseFromString(payload)) return;
            if (message.type() == protocol::TMPing::ptPING) {
                message.set_type(protocol::TMPing::ptPONG);
                queue(frame(protocol::mtPING, message.SerializeAsString()));
            } else if (message.type() == protocol::TMPing::ptPONG && message.has_seq() && ping && message.seq() == *ping) {
                ping.reset();  // only the pong of this node's ping, as xrpld takes it
            }
            return;
        }
        if (type == protocol::mtGET_OBJECTS) {
            // A query is a request; a reply answers one of ours.
            protocol::TMGetObjectByHash message;
            if (!message.ParseFromString(payload)) return;
            if (!message.query()) { if (wanted.contains(type)) enqueue(type, std::move(payload)); return; }
            responder->request(type, payload, requester());
            return;
        }
        if (type == protocol::mtGET_LEDGER || type == protocol::mtPROOF_PATH_REQ || type == protocol::mtREPLAY_DELTA_REQ) {
            responder->request(type, payload, requester());
            return;
        }
        if (!wanted.contains(type)) return;
        enqueue(type, std::move(payload));
    }
    // Queues a wanted message for receive(); a session whose messages are not being read ends.
    void enqueue(int type, std::string payload) {
        bool full;
        {
            std::lock_guard lock(mutex);
            full = inbox.size() >= 100000;
            if (!full) inbox.push_back({type, std::move(payload)});
        }
        if (full) { fail("Peer messages are not being read"); return; }
        arrived.notify_all();
    }
    // Delivers the complete frames received so far; false after a malformed one.
    bool parse() {
        while (received.size() - consumed >= 6) {
            auto const* p = reinterpret_cast<unsigned char const*>(received.data() + consumed);
            // No compression is offered, so the top six bits of the length are always clear.
            if (p[0] & 0xfc) { fail("Peer sent a compressed or malformed message"); return false; }
            std::size_t size = be32(p) & 0x03ffffff;
            if (received.size() - consumed < 6 + size) break;
            int type = p[4] << 8 | p[5];
            ++messages_in; bytes_in += 6 + size;
            deliver(type, received.substr(consumed + 6, size));
            consumed += 6 + size;
        }
        if (consumed > (1 << 20) || consumed == received.size()) { received.erase(0, consumed); consumed = 0; }
        return true;
    }
    void read() {
        stream->async_read_some(net::buffer(chunk), [this](beast::error_code ec, std::size_t n) {
            if (ec) { fail("Peer connection closed: " + ec.message()); return; }
            received.append(chunk.data(), n);
            if (parse()) read();
        });
    }
    static std::string frame(int type, std::string const& payload) {
        if (payload.size() >= (1u << 26)) throw Error("Peer message too large");
        std::string out(6, '\0');
        auto size = static_cast<std::uint32_t>(payload.size());
        for (int i = 0; i < 4; ++i) out[i] = static_cast<char>(size >> (24 - 8 * i));
        out[4] = static_cast<char>(type >> 8); out[5] = static_cast<char>(type);
        return out + payload;
    }
    // A peer whose queue stays long is dropped by the timer (tick()); responders stop adding
    // replies at 192 messages or 16 MiB (PeerRequester::backlogged), so what still comes is
    // relayed messages. 64 MiB waiting is a peer not reading at all: dropped at once.
    void queue(std::string bytes) {
        if (outbox.size() < 128) long_queue = 0;  // as xrpld's send(): a queue that empties now and then is read
        outbox_bytes += bytes.size();
        outbox.push_back(std::move(bytes));
        queued_messages = outbox.size(); queued_bytes = outbox_bytes;
        if (outbox_bytes > (64u << 20)) { fail("Peer is not reading"); return; }
        if (outbox.size() == 1) write();
    }
    void write() {
        net::async_write(*stream, net::buffer(outbox.front()), [this](beast::error_code ec, std::size_t) {
            if (ec) { fail("Peer write failed: " + ec.message()); return; }
            ++messages_out; bytes_out += outbox.front().size();
            outbox_bytes -= outbox.front().size();
            outbox.pop_front();
            queued_messages = outbox.size(); queued_bytes = outbox_bytes;
            if (!outbox.empty()) write();
        });
    }
};

PeerSession::PeerSession() = default;
PeerSession::PeerSession(std::string const& endpoint, NodeIdentity const& identity, std::uint32_t network_id, std::set<int> wanted)
    : PeerSession(endpoint, PeerLocal{identity, network_id}, std::move(wanted)) {}
PeerSession::PeerSession(std::string const& endpoint, PeerLocal const& local, std::set<int> wanted)
    : impl_(std::make_shared<Impl>()) {
    auto& m = *impl_;
    m.info.endpoint = endpoint;
    m.wanted = std::move(wanted);
    m.responder = local.responder ? local.responder : serving_nothing();
    m.tls = client_context();
    m.stream.emplace(m.io, *m.tls);
    auto [host, port] = split_endpoint(endpoint);
    tcp::resolver resolver(m.io);
    beast::error_code failed;
    resolver.async_resolve(host, port, [&](beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) { failed = ec; return; }
        net::async_connect(m.stream->lowest_layer(), results, [&](beast::error_code e, tcp::endpoint const&) {
            if (e) { failed = e; return; }
            m.stream->async_handshake(net::ssl::stream_base::client, [&](beast::error_code h) { failed = h; });
        });
    });
    m.run_for(15, "connection");
    if (failed) throw Error("Peer " + endpoint + ": " + failed.message());
    if (auto remote = m.stream->lowest_layer().remote_endpoint(failed); !failed) {
        auto address = remote.address();
        if (address.is_v6() && address.to_v6().is_v4_mapped()) address = net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
        m.info.address = address.to_string();
    }
    failed = {};
    auto value = shared_value(m.stream->native_handle());
    http::request<http::empty_body> request{http::verb::get, "/", 11};
    request.set(http::field::user_agent, "xrpla-" XRPLA_VERSION);
    request.set(http::field::upgrade, "XRPL/2.2");
    request.set(http::field::connection, "Upgrade");
    request.set("Connect-As", "Peer");
    request.set("Crawl", local.crawl ? "public" : "private");
    if (auto offered = features(local.ledger_replay, local.squelch); !offered.empty()) request.set("X-Protocol-Ctl", offered);
    sign_session(request, local, value);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    http::async_write(*m.stream, request, [&](beast::error_code ec, std::size_t) {
        if (ec) { failed = ec; return; }
        http::async_read(*m.stream, buffer, response, [&](beast::error_code e, std::size_t) { failed = e; });
    });
    m.run_for(15, "handshake");
    if (failed) throw Error("Peer " + endpoint + " handshake: " + failed.message());
    if (response.result() == http::status::service_unavailable) {
        std::vector<std::string> suggested;
        try { for (auto const& ip : Json::parse(response.body()).value("peer-ips", Json::array())) suggested.push_back(ip.get<std::string>()); }
        catch (std::exception const&) {}
        throw PeerRedirect("Peer " + endpoint + " is full", std::move(suggested));
    }
    if (response.result() != http::status::switching_protocols)
        throw Error("Peer " + endpoint + " refused the handshake: " + std::to_string(response.result_int()) + " " + response.body().substr(0, 200));
    if (auto chosen = field(response, "Upgrade"); !chosen.empty() && chosen != "XRPL/2.2")
        throw Error("Peer " + endpoint + " chose protocol " + chosen);
    m.info.public_key = verify_session(response, local, value, "Peer " + endpoint);
    m.info.network_id = field(response, "Network-ID");
    m.info.server = field(response, "Server");
    m.info.closed_ledger = field(response, "Closed-Ledger");
    m.info.crawl = iequals(field(response, "Crawl"), "public");
    m.info.ledger_replay = local.ledger_replay && feature(field(response, "X-Protocol-Ctl"), "ledgerreplay", "1");
    m.info.squelch = local.squelch && feature(field(response, "X-Protocol-Ctl"), "vprr", "1");
    m.info.connected = std::chrono::steady_clock::now();
    // Bytes read past the response are the first messages.
    m.start(beast::buffers_to_string(buffer.data()));
}
PeerSession::~PeerSession() {
    if (!impl_) return;
    auto& m = *impl_;
    net::post(m.io, [&m] {
        beast::error_code ignored; if (m.stream) m.stream->lowest_layer().close(ignored);
        if (m.timer) m.timer->cancel();
    });
    if (m.thread.joinable()) m.thread.join();
}
PeerSession::Info const& PeerSession::info() const { return impl_->info; }
PeerRequester PeerSession::requester() const { return impl_->requester(); }
void PeerSession::close(std::string const& why) {
    net::post(impl_->io, [m = impl_.get(), why] { m->fail(why.empty() ? "Peer closed by this node" : "Closed by this node: " + why); });
}
bool PeerSession::open() const { std::lock_guard lock(impl_->mutex); return !impl_->closed; }
std::string PeerSession::failure() const { std::lock_guard lock(impl_->mutex); return impl_->failure; }
PeerSession::Traffic PeerSession::traffic() const {
    auto& m = *impl_;
    return {m.messages_in.load(), m.bytes_in.load(), m.messages_out.load(), m.bytes_out.load()};
}
void PeerSession::send(int type, google::protobuf::MessageLite const& message) { send_raw(type, message.SerializeAsString()); }
void PeerSession::send_raw(int type, std::string const& payload) {
    if (!open()) throw Error("Peer " + impl_->info.endpoint + " is closed");
    auto bytes = Impl::frame(type, payload);
    net::post(impl_->io, [m = impl_.get(), bytes = std::move(bytes)]() mutable { m->queue(std::move(bytes)); });
}
std::optional<PeerSession::Message> PeerSession::receive(std::chrono::milliseconds timeout) {
    auto& m = *impl_;
    std::unique_lock lock(m.mutex);
    m.arrived.wait_for(lock, timeout, [&] { return !m.inbox.empty() || m.closed; });
    if (!m.inbox.empty()) { auto message = std::move(m.inbox.front()); m.inbox.pop_front(); return message; }
    if (m.closed) throw Error(m.failure);
    return std::nullopt;
}

struct PeerListener::Impl {
    net::io_context io;
    tcp::acceptor acceptor{io};
    std::thread thread;
    PeerLocal local;
    std::set<int> wanted;
    Handlers handlers;
    std::shared_ptr<net::ssl::context> tls = server_context();
    std::mutex mutex;
    std::condition_variable idle;
    unsigned pending = 0;
    std::map<std::string, unsigned> pending_from;  // handshakes running, by address_group
    bool stopping = false;

    void accept() {
        acceptor.async_accept([this](beast::error_code ec, tcp::socket socket) {
            if (ec == net::error::operation_aborted) return;
            if (!ec) {
                std::lock_guard lock(mutex);
                beast::error_code ignored;
                auto remote = socket.remote_endpoint(ignored);
                auto address = remote.address();
                if (address.is_v6() && address.to_v6().is_v4_mapped()) address = net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
                // A few handshakes from one address: else one client holding idle connections
                // (each step may take 15 s) could fill every slot and keep other peers out.
                auto group = address_group(address.to_string());
                bool room = address.is_loopback() || pending_from[group] < 2;
                if (!stopping && pending < 32 && room && !ignored) {
                    auto protocol = remote.protocol();
                    auto fd = socket.release(ignored);
                    if (!ignored) {
                        ++pending; ++pending_from[group];
                        try {
                            std::thread([this, fd, protocol, group, name = address.to_string(), port = remote.port()] {
                                try { handshake(fd, protocol, name, port); }
                                catch (std::exception const& error) {  // a step that timed out
                                    auto endpoint = (name.find(':') != std::string::npos ? "[" + name + "]" : name) + ":" + std::to_string(port);
                                    if (handlers.failed) handlers.failed(without_endpoint(error.what(), endpoint), name);
                                }
                                std::lock_guard done(mutex);
                                if (--pending_from[group] == 0) pending_from.erase(group);
                                if (--pending == 0) idle.notify_all();
                            }).detach();
                        } catch (std::exception const&) {
                            // No thread to be had (the process's limit, memory): the connection is
                            // closed, by a socket that takes it back, and its place given back.
                            tcp::socket orphan(io);
                            if (orphan.assign(protocol, fd, ignored); ignored) ::close(fd);
                            --pending_from[group];
                            if (--pending == 0) idle.notify_all();
                        }
                    }
                }
                if (auto it = pending_from.find(group); it != pending_from.end() && it->second == 0) pending_from.erase(it);
            }
            accept();
        });
    }
    // An accepted connection, on its own thread: TLS, then an upgrade to a peer session or a
    // plain HTTP answer.
    void handshake(tcp::socket::native_handle_type fd, tcp protocol, std::string const& address, std::uint16_t port) {
        std::unique_ptr<PeerSession> session(new PeerSession());
        session->impl_ = std::make_shared<PeerSession::Impl>();
        auto& m = *session->impl_;
        m.info.endpoint = (address.find(':') != std::string::npos ? "[" + address + "]" : address) + ":" + std::to_string(port);
        m.info.address = address;
        m.info.inbound = true;
        m.wanted = wanted;
        m.responder = local.responder ? local.responder : serving_nothing();
        m.tls = tls;
        m.stream.emplace(m.io, *m.tls);
        auto give_up = [&](std::string const& why) { if (handlers.failed) handlers.failed(why, address); };
        beast::error_code failed;
        m.stream->next_layer().assign(protocol, fd, failed);
        if (failed) return give_up("Socket: " + failed.message());
        m.stream->async_handshake(net::ssl::stream_base::server, [&](beast::error_code h) { failed = h; });
        m.run_for(15, "TLS handshake");
        if (failed) return give_up("TLS handshake: " + failed.message());
        beast::flat_buffer buffer;
        http::request_parser<http::string_body> parser;
        parser.header_limit(16 * 1024); parser.body_limit(16 * 1024);
        http::async_read(*m.stream, buffer, parser, [&](beast::error_code e, std::size_t) { failed = e; });
        m.run_for(15, "request");
        if (failed) return give_up("Request: " + failed.message());
        auto request = parser.release();
        auto answer = [&](http::response<http::string_body> response) {
            response.set(http::field::server, "xrpla-" XRPLA_VERSION);
            response.set(http::field::connection, "close");
            response.prepare_payload();
            http::async_write(*m.stream, response, [&](beast::error_code, std::size_t) {});
            m.run_for(15, "response");
        };
        auto upgrade = request.method() == http::verb::get && request.version() >= 11 && lists(field(request, "Connection"), "upgrade") && request.count(http::field::upgrade);
        if (!upgrade) {
            // Plain HTTP on the peer port (/crawl, /health).
            std::optional<std::pair<int, std::string>> body;
            if (request.method() == http::verb::get && handlers.http) body = handlers.http(std::string(request.target()));
            http::response<http::string_body> response{body ? static_cast<http::status>(body->first) : http::status::not_found, request.version()};
            response.set(http::field::content_type, "application/json");
            response.body() = body ? body->second : Json{{"error", "not found"}}.dump();
            answer(std::move(response));
            return;
        }
        auto redirect = [&] {
            http::response<http::string_body> response{http::status::service_unavailable, request.version()};
            response.set("Remote-Address", address);
            response.set(http::field::content_type, "application/json");
            Json ips = Json::array();
            if (handlers.redirect) for (auto const& peer : handlers.redirect()) if (ips.size() < 10) ips.push_back(peer);
            response.body() = Json{{"peer-ips", ips}}.dump();
            answer(std::move(response));
        };
        auto refuse = [&](std::string const& why) {
            give_up("Refused: " + why);
            http::response<http::string_body> response{http::status::bad_request, request.version()};
            response.reason("Bad Request (" + why + ")");
            response.set("Remote-Address", address);
            answer(std::move(response));
        };
        if (!lists(field(request, "Upgrade"), "XRPL/2.2")) return refuse("Unable to agree on a protocol version");
        if (!lists(field(request, "Connect-As"), "peer")) return redirect();
        r::uint256 value;
        try { value = shared_value(m.stream->native_handle()); }
        catch (std::exception const&) { return refuse("Incorrect security cookie"); }
        try { m.info.public_key = verify_session(request, local, value, "Peer"); }
        catch (std::exception const& error) { return refuse(error.what()); }
        m.info.server = field(request, "User-Agent");
        m.info.network_id = field(request, "Network-ID");
        m.info.closed_ledger = field(request, "Closed-Ledger");
        m.info.crawl = iequals(field(request, "Crawl"), "public");
        m.info.ledger_replay = local.ledger_replay && feature(field(request, "X-Protocol-Ctl"), "ledgerreplay", "1");
        m.info.squelch = local.squelch && feature(field(request, "X-Protocol-Ctl"), "vprr", "1");
        m.info.connected = std::chrono::steady_clock::now();
        if (handlers.admit && !handlers.admit(m.info, address)) return redirect();
        // Let in, and counted by `admit` until `accepted`: a failure from here on (a step's timeout
        // throws) gives the place back. The info is a copy: the session may be gone by then.
        struct Admitted {
            Handlers const& handlers; PeerSession::Info info; std::string const& address; bool armed = true;
            ~Admitted() { if (armed && handlers.withdrawn) handlers.withdrawn(info, address); }
        } admitted{handlers, m.info, address};
        http::response<http::empty_body> response{http::status::switching_protocols, request.version()};
        response.set(http::field::connection, "Upgrade");
        response.set(http::field::upgrade, "XRPL/2.2");
        response.set("Connect-As", "Peer");
        response.set(http::field::server, "xrpla-" XRPLA_VERSION);
        response.set("Crawl", local.crawl ? "public" : "private");
        response.set("X-Protocol-Ctl", features(m.info.ledger_replay, m.info.squelch));
        sign_session(response, local, value);
        http::async_write(*m.stream, response, [&](beast::error_code e, std::size_t) { failed = e; });
        m.run_for(15, "response");
        if (failed) return give_up("Response: " + failed.message());
        m.start(beast::buffers_to_string(buffer.data()));
        // Disarmed only once `accepted` returns: one that throws before it has taken the place
        // gives it back here (`withdrawn` after `accepted` took it finds nothing to give back).
        if (handlers.accepted) handlers.accepted(std::move(session), address);
        admitted.armed = false;
    }
};
PeerListener::PeerListener(std::string const& endpoint, PeerLocal local, std::set<int> wanted, Handlers handlers) : impl_(std::make_unique<Impl>()) {
    auto& m = *impl_;
    m.local = std::move(local); m.wanted = std::move(wanted); m.handlers = std::move(handlers);
    auto [host, port] = split_endpoint(endpoint);
    tcp::endpoint where(net::ip::make_address(host), static_cast<std::uint16_t>(std::stoul(port)));
    m.acceptor.open(where.protocol());
    m.acceptor.set_option(net::socket_base::reuse_address(true));
    if (where.address().is_v6()) m.acceptor.set_option(net::ip::v6_only(false));
    m.acceptor.bind(where);
    m.acceptor.listen();
    m.accept();
    m.thread = std::thread([&m] { m.io.run(); });
}
PeerListener::~PeerListener() {
    auto& m = *impl_;
    { std::lock_guard lock(m.mutex); m.stopping = true; }
    net::post(m.io, [&m] { beast::error_code ignored; m.acceptor.close(ignored); });
    if (m.thread.joinable()) m.thread.join();
    std::unique_lock lock(m.mutex);
    m.idle.wait(lock, [&] { return m.pending == 0; });
}
std::uint16_t PeerListener::port() const { return impl_->acceptor.local_endpoint().port(); }
}

namespace xrpla::core {
Json watch_peers(PeersOptions const& options) {
    auto network = network_defaults(options.network_id);
    if (!network && (options.list.empty() || options.list_key.empty()))
        throw Error("Network " + std::to_string(options.network_id) + " has no validator list known by name: give --list and --list-key");
    auto list = options.list.empty() ? network->list : options.list;
    auto list_key = options.list_key.empty() ? network->list_key : options.list_key;
    auto text = list.starts_with("https://") || list.starts_with("http://") ? fetch_url(list, 16 << 20) : read_file(list, 16 << 20);
    auto now = xrpl_seconds_now();
    ValidationQuorum quorum(validating_list(verify_validator_list(parse_as_xrpld(text), list_key, now), now));
    auto identity = options.identity.empty() ? NodeIdentity::ephemeral() : NodeIdentity::load_or_create(options.identity);
    // Full servers answer with other peers to try.
    std::deque<std::string> candidates;
    for (auto const& peer : options.peers.empty() ? default_peers(options.network_id) : options.peers) candidates.push_back(peer);
    if (candidates.empty()) throw Error("Network " + std::to_string(options.network_id) + " has no public hubs known by name: give --peers");
    std::set<std::string> tried;
    std::vector<std::unique_ptr<PeerSession>> sessions;
    Json connected = Json::array(), refused = Json::array();
    auto started = std::chrono::steady_clock::now();
    while (sessions.size() < options.connections && !candidates.empty() && tried.size() < 50) {
        auto endpoint = candidates.front(); candidates.pop_front();
        if (!tried.insert(endpoint).second) continue;
        try {
            sessions.push_back(std::make_unique<PeerSession>(endpoint, identity, options.network_id,
                std::set<int>{protocol::mtVALIDATION, protocol::mtMANIFESTS, protocol::mtLEDGER_DATA}));
            auto const& info = sessions.back()->info();
            connected.push_back({{"endpoint", info.endpoint}, {"public_key", info.public_key}, {"server", info.server}, {"network_id", info.network_id}});
            std::cerr << connected.back().dump() << std::endl;
        } catch (PeerRedirect const& redirect) {
            for (auto const& peer : redirect.peers) candidates.push_back(peer);
            refused.push_back({{"endpoint", endpoint}, {"reason", "full"}, {"suggested", redirect.peers.size()}});
        } catch (std::exception const& error) {
            refused.push_back({{"endpoint", endpoint}, {"reason", error.what()}});
        }
    }
    if (sessions.empty()) throw Error("No peer accepted a connection: " + refused.dump());
    std::mutex mutex;
    std::condition_variable finished;
    std::uint64_t seen = 0, counted = 0;
    std::map<Bytes, Json> pending;  // quorum-validated ledger hash -> report awaiting its header
    Json ledgers = Json::array();
    std::atomic<bool> done{false};
    std::vector<std::thread> readers;
    for (auto& session : sessions) readers.emplace_back([&, s = session.get()] {
        while (!done) {
            std::optional<PeerSession::Message> message;
            try { message = s->receive(std::chrono::milliseconds(500)); } catch (std::exception const&) { return; }
            if (!message) continue;
            // What a peer sends is its own: one that cannot be read (or a request that cannot be
            // sent back) is reported, and the session read on until it ends.
            try {
                std::lock_guard lock(mutex);
                if (message->type == protocol::mtMANIFESTS) {
                    protocol::TMManifests manifests;
                    if (manifests.ParseFromString(message->payload)) for (auto const& m : manifests.list()) quorum.manifest(m.stobject());
                } else if (message->type == protocol::mtVALIDATION) {
                    protocol::TMValidation validation;
                    if (!validation.ParseFromString(message->payload)) continue;
                    ++seen;
                    auto result = quorum.add(validation.validation());
                    if (!result) continue;
                    ++counted;
                    // No ledger's state is read here, so the negative UNL is not known: a ledger is
                    // reported once it reaches the quorum of the whole list.
                    if (result->count < quorum.list().quorum() || pending.contains(result->ledger_hash) || done) continue;
                    bool reported = std::any_of(ledgers.begin(), ledgers.end(), [&](Json const& l) { return l.at("ledger_hash") == to_hex(result->ledger_hash); });
                    if (reported) continue;
                    // The ledger's header, from the peer that completed the quorum.
                    protocol::TMGetLedger request;
                    request.set_itype(protocol::liBASE);
                    request.set_ledgerhash(result->ledger_hash);
                    s->send(protocol::mtGET_LEDGER, request);
                    pending[result->ledger_hash] = {{"ledger_index", result->ledger_index}, {"ledger_hash", to_hex(result->ledger_hash)},
                        {"trusted_validations", result->count}, {"validated_seconds", std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count()}};
                } else if (message->type == protocol::mtLEDGER_DATA) {
                    protocol::TMLedgerData data;
                    if (!data.ParseFromString(message->payload) || data.type() != protocol::liBASE) continue;
                    auto it = pending.find(data.ledgerhash());
                    if (it == pending.end() || data.nodes_size() == 0) continue;
                    auto const& bytes = data.nodes(0).nodedata();
                    auto header = ::xrpl::deserializeHeader(::xrpl::Slice(bytes.data(), bytes.size()));
                    auto hash = ::xrpl::calculateLedgerHash(header);
                    auto entry = std::move(it->second);
                    pending.erase(it);
                    entry["header_from"] = s->info().endpoint;
                    entry["header_verified"] = Bytes(reinterpret_cast<char const*>(hash.data()), hash.size()) == data.ledgerhash() && header.seq == entry.at("ledger_index");
                    entry["header_seconds"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
                    std::cerr << entry.dump() << std::endl;
                    ledgers.push_back(entry);
                    if (ledgers.size() >= options.ledgers) { done = true; finished.notify_all(); }
                }
            } catch (std::exception const& error) {
                std::cerr << Json{{"endpoint", s->info().endpoint}, {"error", error.what()}}.dump() << std::endl;
            }
        }
    });
    {
        std::unique_lock lock(mutex);
        finished.wait_for(lock, std::chrono::seconds(options.seconds), [&] { return done.load(); });
    }
    done = true;
    for (auto& reader : readers) reader.join();
    return {{"identity", identity.base58()}, {"validators", quorum.list().signing_keys.size()}, {"quorum", quorum.list().quorum()},
            {"peers", connected}, {"refused", refused}, {"validations_seen", seen}, {"validations_counted", counted}, {"ledgers", ledgers}};
}
}
