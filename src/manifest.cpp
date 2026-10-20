#include <xrpla/archive.hpp>
#include <sodium.h>
#include <curl/curl.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <fstream>
#include <limits>
#include <set>

namespace xrpla {
namespace {
std::uint64_t number(Json const& value, char const* field, std::uint64_t max) {
    auto const& n = value.at(field);
    if (!n.is_number_unsigned() || n.get<std::uint64_t>() > max)
        throw Error(std::string("Invalid unsigned integer: ") + field);
    return n.get<std::uint64_t>();
}
std::uint32_t sequence(Json const& j, char const* field) {
    auto n = number(j, field, UINT32_MAX);
    if (!n) throw Error(std::string("Ledger sequence must be positive: ") + field);
    return static_cast<std::uint32_t>(n);
}
std::uint32_t decimal(std::string_view text) {
    std::uint32_t out = 0;
    auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), out);
    if (ec != std::errc{} || end != text.data() + text.size() || !out)
        throw Error("Range requires positive uint32 ledger sequences");
    return out;
}
std::string hex(unsigned char const* bytes, std::size_t size) {
    std::string out(size * 2 + 1, '\0');
    sodium_bin2hex(out.data(), out.size(), bytes, size);
    out.pop_back();
    return out;
}
} // namespace

void initialize() {
    static bool const ready = [] {
        if (sodium_init() < 0 || curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK)
            throw Error("Crypto or HTTP initialization failed");
        return true;
    }();
    (void)ready;
}
std::string sha256(std::string_view bytes) {
    std::array<unsigned char, crypto_hash_sha256_BYTES> digest{};
    crypto_hash_sha256(digest.data(), reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size());
    return hex(digest.data(), digest.size());
}
std::string file_sha256(fs::path const& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw Error("Cannot read " + path.string());
    crypto_hash_sha256_state state{};
    crypto_hash_sha256_init(&state);
    std::array<char, 128 * 1024> buffer{};
    while (in.read(buffer.data(), buffer.size()) || in.gcount())
        crypto_hash_sha256_update(&state, reinterpret_cast<unsigned char const*>(buffer.data()),
                                  static_cast<unsigned long long>(in.gcount()));
    if (!in.eof()) throw Error("Read failed: " + path.string());
    std::array<unsigned char, crypto_hash_sha256_BYTES> digest{};
    crypto_hash_sha256_final(&state, digest.data());
    return hex(digest.data(), digest.size());
}
std::string read_file(fs::path const& path, std::uint64_t limit) {
    if (!fs::is_regular_file(path) || fs::file_size(path) > limit)
        throw Error("File missing, not regular, or exceeds size limit: " + path.string());
    std::ifstream in(path, std::ios::binary);
    if (!in) throw Error("Cannot read " + path.string());
    std::string bytes(static_cast<std::size_t>(fs::file_size(path)), '\0');
    in.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!in || in.peek() != std::char_traits<char>::eof())
        throw Error("File changed or read failed: " + path.string());
    return bytes;
}
void write_file(fs::path const& path, std::string_view bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw Error("Cannot write " + path.string());
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
    if (!out) throw Error("Write failed: " + path.string());
}
void validate_hash(std::string const& hash) {
    if (hash.size() != 64 || hash.find_first_not_of("0123456789abcdef") != std::string::npos)
        throw Error("Expected lowercase 32-byte hex hash");
}
// A published object's key, safe as a relative path under a cache: no component may start with
// '.' (which also refuses "." and ".."), so none can name a cache's own files (.verified,
// .staging/, .xrpla-lock); no published object's does (chunks/, dict/, times/, snapshots/,
// signatures/, catalog/, pieces/, bundles/, fragments/).
void validate_key(std::string const& key) {
    if (key.empty() || key.size() > 512 || key.front() == '/' || key.back() == '/' ||
        key.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-._/") != std::string::npos)
        throw Error("Unsafe archive object key: " + key);
    std::size_t start = 0;
    while (start < key.size()) {
        auto end = key.find('/', start);
        if (end == std::string::npos) end = key.size();
        auto part = key.substr(start, end - start);
        auto stem = part.substr(0, part.find('.'));
        if (part.empty() || part.front() == '.' || part.back() == '.' ||
            stem == "con" || stem == "prn" || stem == "aux" || stem == "nul" ||
            (stem.size() == 4 && (stem.starts_with("com") || stem.starts_with("lpt")) &&
             stem.back() >= '0' && stem.back() <= '9'))
            throw Error("Unsafe archive path component: " + key);
        start = end + 1;
    }
}
Object parse_object(Json const& value) {
    Object out{value.at("object").get<std::string>(),
               number(value, "size", static_cast<std::uint64_t>(INT64_MAX)),
               value.at("sha256").get<std::string>()};
    validate_key(out.object);
    validate_hash(out.sha256);
    if (out.size == 0) throw Error("Archive object cannot be empty");
    return out;
}

std::string pinned_publisher_key(fs::path const& key) {
    initialize();
    auto hex = [](std::string const& text) {
        std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> decoded{};
        std::size_t size = 0;
        if (text.size() != 2 * decoded.size() || text.find_first_not_of("0123456789abcdefABCDEF") != std::string::npos ||
            sodium_hex2bin(decoded.data(), decoded.size(), text.data(), text.size(), nullptr, &size, nullptr) != 0 || size != decoded.size())
            return std::optional<std::string>();
        return std::optional<std::string>(std::string(reinterpret_cast<char const*>(decoded.data()), decoded.size()));
    };
    if (auto given = hex(key.string())) return *given;
    auto bytes = read_file(key, 128);
    // 32 raw bytes may end in any byte; only the hex form carries line endings.
    if (bytes.size() == crypto_sign_PUBLICKEYBYTES) return bytes;
    while (!bytes.empty() && (bytes.back() == '\n' || bytes.back() == '\r' || bytes.back() == ' ')) bytes.pop_back();
    if (auto read = hex(bytes)) return *read;
    throw Error("Publisher key " + key.string() + " must hold 32 raw bytes or 64 hexadecimal characters");
}
Manifest authenticate_manifest(std::string const& bytes, std::string const& signature,
                               std::string const& public_key) {
    initialize();
    std::string key = public_key;
    if (key.size() != crypto_sign_PUBLICKEYBYTES) {
        while (!key.empty() && (key.back() == '\n' || key.back() == '\r')) key.pop_back();
        std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> decoded{};
        std::size_t decoded_size = 0;
        if (key.size() != 64 || sodium_hex2bin(decoded.data(), decoded.size(), key.data(), key.size(),
                                              nullptr, &decoded_size, nullptr) != 0 || decoded_size != decoded.size())
            throw Error("Publisher key must be 32 raw bytes or 64 hexadecimal characters");
        key.assign(reinterpret_cast<char const*>(decoded.data()), decoded.size());
    }
    if (signature.size() != crypto_sign_BYTES ||
        crypto_sign_verify_detached(reinterpret_cast<unsigned char const*>(signature.data()),
            reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size(),
            reinterpret_cast<unsigned char const*>(key.data())) != 0)
        throw Error("Manifest signature verification failed");

    // Only authenticated bytes reach the parser. Reject duplicate keys as well.
    std::vector<std::set<std::string>> keys;
    auto doc = Json::parse(bytes, [&keys](int, Json::parse_event_t event, Json& value) {
        if (event == Json::parse_event_t::object_start) keys.emplace_back();
        if (event == Json::parse_event_t::key && !keys.back().insert(value.get<std::string>()).second)
            throw Error("Duplicate JSON key in manifest");
        if (event == Json::parse_event_t::object_end) keys.pop_back();
        return true;
    });
    auto version = number(doc, "format_version", UINT32_MAX);
    if ((version != 1 && version != 2) || number(doc, "chunk_width", UINT32_MAX) != 65536)
        throw Error("Unsupported archive version or chunk width");
    Manifest manifest{};
    manifest.version = static_cast<std::uint32_t>(version);
    manifest.tip = sequence(doc.at("tip"), "seq");
    manifest.tip_hash = doc.at("tip").at("ledger_hash").get<std::string>();
    validate_hash(manifest.tip_hash);
    if (!doc.at("published_at").is_string()) throw Error("published_at must be a string");
    std::set<std::string> paths;
    auto unique = [&paths](Object const& object) {
        if (!paths.insert(object.object).second) throw Error("Duplicate archive object: " + object.object);
    };
    for (auto const* field : {"dicts", "chunks", "snapshots"})
        if (!doc.at(field).is_array()) throw Error(std::string(field) + " must be an array");
    for (auto const& d : doc.at("dicts")) {
        auto object = parse_object(d);
        if (object.object != "dict/" + object.sha256 + ".zdict") throw Error("Invalid dictionary name");
        unique(object);
        manifest.dicts.push_back(object);
    }
    for (auto const& c : doc.at("chunks")) {
        Chunk chunk{parse_object(c), sequence(c, "first"), sequence(c, "last"), c.at("ledger_hash_of_last").get<std::string>()};
        if (c.contains("time_index")) {
            if (version != 2) throw Error("Time indexes require archive v2");
            chunk.time_index = parse_object(c.at("time_index"));
            chunk.first_close_time = static_cast<std::uint32_t>(number(c, "first_close_time", UINT32_MAX));
            chunk.last_close_time = static_cast<std::uint32_t>(number(c, "last_close_time", UINT32_MAX));
            if (chunk.time_index->object != "times/" + std::to_string(chunk.first) + "-" + std::to_string(chunk.last) + "." + chunk.time_index->sha256.substr(0, 16) + ".xrpt" ||
                chunk.time_index->size != (static_cast<std::uint64_t>(chunk.last) - chunk.first + 1) * 8)
                throw Error("Invalid chunk time index");
            unique(*chunk.time_index);
        }
        if (chunk.first > chunk.last || chunk.last > manifest.tip ||
            static_cast<std::uint64_t>(chunk.last) - chunk.first + 1 > 65536 ||
            (version == 1 && ((chunk.first != 1 && chunk.first % 65536 != 0) ||
            static_cast<std::uint64_t>(chunk.last) + 1 !=
              (static_cast<std::uint64_t>(chunk.first) / 65536 + 1) * 65536)))
            throw Error("Invalid or unaligned chunk range");
        if (chunk.file.object != "chunks/" + std::to_string(chunk.first) + "-" +
            std::to_string(chunk.last) + "." + chunk.file.sha256.substr(0, 16) + ".xrpa")
            throw Error("Invalid content-addressed chunk name");
        validate_hash(c.at("ledger_hash_of_last").get<std::string>());
        if (!c.at("amendments").is_array()) throw Error("amendments must be an array");
        for (auto const& a : c.at("amendments")) validate_hash(a.get<std::string>());
        unique(chunk.file);
        manifest.chunks.push_back(chunk);
    }
    std::sort(manifest.chunks.begin(), manifest.chunks.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    for (std::size_t i = 1; i < manifest.chunks.size(); ++i) {
        if (manifest.chunks[i].first <= manifest.chunks[i - 1].last) throw Error("Overlapping chunks");
    }
    std::set<std::uint32_t> snapshots;
    for (auto const& s : doc.at("snapshots")) {
        Snapshot snap{sequence(s, "seq"), s.at("prefix").get<std::string>(),
            s.at("account_hash").get<std::string>(), parse_object(s.at("index")),
            number(s, "parts", UINT32_MAX), number(s, "total_size", INT64_MAX)};
        validate_key(snap.prefix);
        validate_hash(snap.account_hash);
        if (!snap.prefix.starts_with("snapshots/" + std::to_string(snap.seq) + ".") ||
            snap.index.object != snap.prefix + "/index.json" || snap.index.size > 16 * 1024 * 1024 ||
            snap.seq > manifest.tip || !snap.parts || !snap.total_size || !snapshots.insert(snap.seq).second)
            throw Error("Invalid snapshot descriptor");
        unique(snap.index);
        manifest.snapshots.push_back(snap);
    }
    return manifest;
}
Range parse_range(std::string const& input, std::uint32_t tip) {
    Range range{};
    if (input.starts_with("latest-")) {
        auto count = decimal(std::string_view(input).substr(7));
        if (count > tip) throw Error("Requested ledger count exceeds tip");
        range = {tip - count + 1, tip};
    } else {
        auto dash = input.find('-');
        if (dash == std::string::npos) throw Error("Use --range A-B or latest-N; day ranges require the core header index");
        range = {decimal(std::string_view(input).substr(0, dash)), decimal(std::string_view(input).substr(dash + 1))};
    }
    if (!range.first || range.first > range.last || range.last > tip) throw Error("Range is reversed or exceeds archive tip");
    return range;
}
Plan make_plan(Manifest const& manifest, Range range) {
    if (!range.first || range.first > range.last || range.last > manifest.tip) throw Error("Invalid range");
    Snapshot const* nearest = nullptr;
    // Snapshot A contains state but not A's transactions; replay must include A.
    for (auto const& s : manifest.snapshots)
        if (s.seq < range.first && (!nearest || s.seq > nearest->seq)) nearest = &s;
    if (!nearest) throw Error("No snapshot strictly before range start; choose a later range");
    Plan plan{range, *nearest, manifest.dicts};
    plan.objects.push_back(nearest->index);
    std::uint64_t next = static_cast<std::uint64_t>(nearest->seq) + 1;
    for (auto const& chunk : manifest.chunks) {
        if (chunk.last < next) continue;
        if (next > range.last) break;
        if (chunk.first > next) throw Error("Archive gap at ledger " + std::to_string(next));
        plan.objects.push_back(chunk.file);
        if (chunk.time_index) plan.objects.push_back(*chunk.time_index);
        next = static_cast<std::uint64_t>(chunk.last) + 1;
    }
    if (next <= range.last) throw Error("Archive does not cover through requested ledger");
    return plan;
}
std::vector<Object> snapshot_parts(Snapshot const& snapshot, std::string const& bytes) {
    if (bytes.size() != snapshot.index.size || sha256(bytes) != snapshot.index.sha256)
        throw Error("Snapshot index integrity check failed");
    auto index = Json::parse(bytes);
    if (sequence(index, "seq") != snapshot.seq || index.at("account_hash") != snapshot.account_hash ||
        !index.at("parts").is_array() || index.at("parts").size() != snapshot.parts)
        throw Error("Snapshot index disagrees with signed manifest");
    std::set<std::string> keys;
    std::vector<Object> parts;
    std::uint64_t size = 0;
    for (auto const& p : index.at("parts")) {
        auto part = parse_object(p);
        if (!part.object.starts_with(snapshot.prefix + "/part-") || !part.object.ends_with(".xrps") ||
            part.object.substr(snapshot.prefix.size() + 1).find('/') != std::string::npos ||
            !keys.insert(part.object).second || part.size > snapshot.total_size - size)
            throw Error("Invalid snapshot part");
        size += part.size;
        parts.push_back(part);
    }
    if (size != snapshot.total_size) throw Error("Snapshot total size mismatch");
    return parts;
}
bool matches(fs::path const& path, Object const& expected) {
    return !fs::is_symlink(fs::symlink_status(path)) && fs::is_regular_file(path) &&
           fs::file_size(path) == expected.size && file_sha256(path) == expected.sha256;
}
std::vector<std::pair<std::uint32_t, std::uint32_t>> decode_times(Chunk const& chunk, std::string const& bytes) {
    if (!chunk.time_index || bytes.size() != chunk.time_index->size || sha256(bytes) != chunk.time_index->sha256) throw Error("Time index integrity check failed");
    auto number = [&](std::size_t offset) { std::uint32_t value = 0; for (unsigned i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(bytes.at(offset + i)); return value; };
    std::vector<std::pair<std::uint32_t, std::uint32_t>> result;
    for (std::size_t offset = 0; offset < bytes.size(); offset += 8) {
        auto seq = number(offset), close = number(offset + 4);
        // Early canonical mainnet headers can have decreasing close times.
        // Sequence remains contiguous; pipeline verification binds each time
        // to its canonical header rather than imposing a modern time invariant.
        if (seq != static_cast<std::uint64_t>(chunk.first) + result.size()) throw Error("Invalid time index order");
        result.emplace_back(seq, close);
    }
    if (result.empty() || result.front().second != chunk.first_close_time || result.back().second != chunk.last_close_time || result.back().first != chunk.last) throw Error("Time index bounds disagree with manifest");
    return result;
}
} // namespace xrpla
