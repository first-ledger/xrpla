#include <xrpla/core/format.hpp>
#include <sodium.h>
#include <zstd.h>
#include <algorithm>
#include <fstream>
#include <memory>

namespace xrpla::core {
namespace {
constexpr std::uint64_t header_size = 51, trailer_size = 40;
std::uint64_t le(std::string_view bytes) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < bytes.size(); ++i)
        value |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[i])) << (i * 8);
    return value;
}
Bytes integer(std::uint64_t value, unsigned count) {
    Bytes bytes(count, '\0');
    for (unsigned i = 0; i < count; ++i) bytes[i] = static_cast<char>((value >> (8 * i)) & 255);
    return bytes;
}
Bytes exact(std::istream& input, std::size_t length) {
    Bytes bytes(length, '\0');
    input.read(bytes.data(), static_cast<std::streamsize>(length));
    if (!input) throw Error("Truncated archive container");
    return bytes;
}
Bytes unhex(std::string const& text) {
    validate_hash(text);
    Bytes out(32, '\0');
    if (sodium_hex2bin(reinterpret_cast<unsigned char*>(out.data()), out.size(), text.data(),
                      text.size(), nullptr, nullptr, nullptr) != 0) throw Error("Invalid digest");
    return out;
}
std::string ashex(std::string_view bytes) {
    std::string text(bytes.size() * 2 + 1, '\0');
    sodium_bin2hex(text.data(), text.size(), reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size());
    text.pop_back(); return text;
}
void zcheck(std::size_t result) {
    if (ZSTD_isError(result)) throw Error(std::string("Invalid zstd stream: ") + ZSTD_getErrorName(result));
}
class Decoder {
    std::istream& input_;
    std::uint64_t remaining_, maximum_, position_ = 0;
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context_{ZSTD_createDCtx(), ZSTD_freeDCtx};
    std::array<char, 128 * 1024> buffer_{};
    ZSTD_inBuffer compressed_{buffer_.data(), 0, 0};
    bool ended_ = false;
public:
    Decoder(std::istream& input, std::uint64_t size, Bytes const& dictionary, std::uint64_t maximum)
        : input_(input), remaining_(size), maximum_(maximum) {
        if (!context_) throw Error("Cannot allocate zstd decoder");
        zcheck(ZSTD_DCtx_setParameter(context_.get(), ZSTD_d_windowLogMax, 27));
        zcheck(ZSTD_DCtx_loadDictionary(context_.get(), dictionary.data(), dictionary.size()));
    }
    Bytes read(std::size_t count) {
        if (count > maximum_ - position_) throw Error("Decompressed chunk exceeds limit");
        Bytes bytes(count, '\0');
        ZSTD_outBuffer output{bytes.data(), bytes.size(), 0};
        while (output.pos < output.size) {
            if (ended_) throw Error("Truncated decompressed chunk");
            if (compressed_.pos == compressed_.size && remaining_) {
                auto n = static_cast<std::size_t>(std::min<std::uint64_t>(buffer_.size(), remaining_));
                input_.read(buffer_.data(), static_cast<std::streamsize>(n));
                if (!input_) throw Error("Truncated compressed frame");
                remaining_ -= n;
                compressed_ = {buffer_.data(), n, 0};
            }
            auto before = compressed_.pos, produced = output.pos;
            auto result = ZSTD_decompressStream(context_.get(), &output, &compressed_);
            zcheck(result);
            if (result == 0) ended_ = true;
            if (before == compressed_.pos && produced == output.pos && !ended_)
                throw Error("Truncated or stalled zstd frame");
        }
        position_ += count;
        return bytes;
    }
    std::uint64_t number(unsigned count) { return le(read(count)); }
    Bytes record(std::uint32_t maximum) {
        auto size = number(4);
        if (!size || size > maximum) throw Error("Invalid binary record length");
        return read(static_cast<std::size_t>(size));
    }
    std::uint64_t position() const { return position_; }
    void finish() {
        // Drain a frame footer, while rejecting any extra decompressed byte.
        while (!ended_) {
            if (compressed_.pos == compressed_.size && remaining_) {
                auto n = static_cast<std::size_t>(std::min<std::uint64_t>(buffer_.size(), remaining_));
                input_.read(buffer_.data(), static_cast<std::streamsize>(n));
                if (!input_) throw Error("Truncated compressed frame");
                remaining_ -= n;
                compressed_ = {buffer_.data(), n, 0};
            }
            char byte;
            ZSTD_outBuffer output{&byte, 1, 0};
            auto before = compressed_.pos;
            auto result = ZSTD_decompressStream(context_.get(), &output, &compressed_);
            zcheck(result);
            if (output.pos) throw Error("Extra decompressed data after records");
            ended_ = result == 0;
            if (before == compressed_.pos && !ended_) throw Error("Truncated zstd footer");
        }
        if (!ended_ || remaining_ || compressed_.pos != compressed_.size)
            throw Error("Trailing data or concatenated zstd frames");
    }
};
} // namespace

ChunkInfo read_chunk(fs::path const& path, Bytes const& dictionary,
    std::function<void(LedgerRecord&&)> const& visitor, Limits limits) {
    initialize();
    auto size = fs::file_size(path);
    if (size <= header_size + trailer_size) throw Error("Chunk is too short");
    std::ifstream file(path, std::ios::binary);
    if (!file) throw Error("Cannot open chunk: " + path.string());
    crypto_hash_sha256_state hash{};
    crypto_hash_sha256_init(&hash);
    for (std::uint64_t left = size - trailer_size; left;) {
        auto bytes = exact(file, static_cast<std::size_t>(std::min<std::uint64_t>(left, 128 * 1024)));
        crypto_hash_sha256_update(&hash, reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size());
        left -= bytes.size();
    }
    auto table = le(exact(file, 8));
    auto expected = exact(file, 32);
    std::array<unsigned char, 32> digest{};
    crypto_hash_sha256_final(&hash, digest.data());
    if (sodium_memcmp(digest.data(), expected.data(), digest.size()) != 0)
        throw Error("Chunk trailer SHA-256 mismatch");
    file.seekg(0);
    auto header = exact(file, header_size);
    auto version = le(std::string_view(header).substr(4, 2));
    if (header.substr(0, 4) != "XRPA" || (version != 1 && version != 2) || header.back() != 0)
        throw Error("Unsupported chunk magic, version, or flags");
    ChunkInfo info{static_cast<std::uint16_t>(version), static_cast<std::uint32_t>(le(std::string_view(header).substr(6, 4))),
        static_cast<std::uint32_t>(le(std::string_view(header).substr(10, 4))),
        static_cast<std::uint32_t>(le(std::string_view(header).substr(14, 4))),
        ashex(std::string_view(header).substr(18, 32))};
    if (!info.first || info.first > info.last || info.count > 65536 ||
        info.count != static_cast<std::uint64_t>(info.last) - info.first + 1)
        throw Error("Invalid chunk sequence range/count");
    auto dict_hash = dictionary.empty() ? std::string(64, '0') : sha256(dictionary);
    if (info.dictionary_hash != dict_hash) throw Error("Chunk dictionary hash mismatch");
    Decoder body(file, size - header_size - trailer_size, dictionary, limits.decompressed_bytes);
    std::vector<std::uint64_t> offsets;
    for (std::uint32_t i = 0; i < info.count; ++i) {
        offsets.push_back(body.position());
        LedgerRecord ledger{};
        ledger.sequence = static_cast<std::uint32_t>(body.number(4));
        if (ledger.sequence != static_cast<std::uint64_t>(info.first) + i)
            throw Error("Non-consecutive chunk sequence");
        ledger.header = body.record(4096);
        auto count = body.number(4);
        if (count > limits.transactions_per_ledger) throw Error("Transaction count exceeds limit");
        auto ledger_limit = [&] {
            if (body.position() - offsets.back() > limits.ledger_bytes) throw Error("Ledger exceeds decoded size limit");
        };
        for (std::uint64_t t = 0; t < count; ++t) {
            ledger.transactions.push_back({body.record(limits.record_bytes), body.record(limits.record_bytes)});
            ledger_limit();
        }
        if (version == 2) {
            auto patches = body.number(4);
            if (patches > limits.patches_per_ledger) throw Error("Patch count exceeds limit");
            for (std::uint64_t p = 0; p < patches; ++p) {
                auto key = body.read(32);
                auto length = body.number(4);
                if (length > limits.record_bytes) throw Error("Patch record exceeds limit");
                ledger.patches.push_back({std::move(key), body.read(static_cast<std::size_t>(length))});
                ledger_limit();
            }
        }
        ledger_limit();
        visitor(std::move(ledger));
    }
    if (table != body.position()) throw Error("Chunk offset table position mismatch");
    for (std::uint32_t i = 0; i < info.count; ++i)
        if (body.number(4) != static_cast<std::uint64_t>(info.first) + i || body.number(8) != offsets[i])
            throw Error("Invalid per-ledger offset table");
    body.finish();
    return info;
}

void write_chunk(fs::path const& path, std::vector<LedgerRecord> const& ledgers, Bytes const& dictionary,
                 std::uint16_t version, int compression_level) {
    if (compression_level < 1 || compression_level > 19) throw Error("Compression level must be 1..19");
    if (version != 1 && version != 2) throw Error("Unsupported writer version");
    if (ledgers.empty() || ledgers.size() > 65536) throw Error("Invalid chunk ledger count");
    Bytes body;
    std::vector<std::uint64_t> offsets;
    auto record = [&](Bytes const& bytes) {
        if (bytes.empty() || bytes.size() > UINT32_MAX) throw Error("Invalid record length");
        body += integer(bytes.size(), 4); body += bytes;
    };
    for (std::size_t i = 0; i < ledgers.size(); ++i) {
        auto const& l = ledgers[i];
        if (!l.sequence || l.sequence != static_cast<std::uint64_t>(ledgers.front().sequence) + i)
            throw Error("Non-consecutive writer input");
        offsets.push_back(body.size());
        body += integer(l.sequence, 4); record(l.header);
        body += integer(l.transactions.size(), 4);
        for (auto const& tx : l.transactions) { record(tx.transaction); record(tx.metadata); }
        if (version == 2) {
            body += integer(l.patches.size(), 4);
            for (auto const& patch : l.patches) {
                if (patch.key.size() != 32 || patch.object.size() > UINT32_MAX) throw Error("Invalid patch record");
                body += patch.key + integer(patch.object.size(), 4) + patch.object;
            }
        } else if (!l.patches.empty()) throw Error("Patches require archive version 2");
    }
    auto table = body.size();
    for (std::size_t i = 0; i < ledgers.size(); ++i)
        body += integer(ledgers[i].sequence, 4) + integer(offsets[i], 8);
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(ZSTD_createCCtx(), ZSTD_freeCCtx);
    if (!ctx) throw Error("Cannot allocate zstd encoder");
    Bytes compressed(ZSTD_compressBound(body.size()), '\0');
    auto n = ZSTD_compress_usingDict(ctx.get(), compressed.data(), compressed.size(), body.data(), body.size(),
                                   dictionary.data(), dictionary.size(), compression_level);
    zcheck(n); compressed.resize(n);
    Bytes bytes = "XRPA" + integer(version, 2) + integer(ledgers.front().sequence, 4) +
        integer(ledgers.back().sequence, 4) + integer(ledgers.size(), 4) +
        (dictionary.empty() ? Bytes(32, '\0') : unhex(sha256(dictionary))) + Bytes(1, '\0') + compressed;
    auto digest = unhex(sha256(bytes));
    bytes += integer(table, 8) + digest;
    write_file(path, bytes);
}

void read_snapshot_part(fs::path const& path, Bytes const& dictionary, std::uint64_t count,
    std::function<void(ObjectRecord&&)> const& visitor, Limits limits) {
    if (count > limits.decompressed_bytes / 36) throw Error("Snapshot count exceeds limit");
    std::ifstream file(path, std::ios::binary);
    if (!file) throw Error("Cannot open snapshot part");
    Decoder body(file, fs::file_size(path), dictionary, limits.decompressed_bytes);
    for (std::uint64_t n = 0; n < count; ++n) {
        auto key = body.read(32);
        visitor({std::move(key), body.record(limits.record_bytes)});
    }
    body.finish();
}

void write_snapshot_part(fs::path const& path, std::vector<ObjectRecord> const& objects, Bytes const& dictionary, int compression_level) {
    if (compression_level < 1 || compression_level > 19) throw Error("Compression level must be 1..19");
    Bytes body;
    for (auto const& item : objects) {
        if (item.key.size() != 32 || item.object.empty() || item.object.size() > UINT32_MAX)
            throw Error("Invalid snapshot record");
        body += item.key + integer(item.object.size(), 4) + item.object;
    }
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(ZSTD_createCCtx(), ZSTD_freeCCtx);
    if (!ctx) throw Error("Cannot allocate zstd encoder");
    Bytes compressed(ZSTD_compressBound(body.size()), '\0');
    auto n = ZSTD_compress_usingDict(ctx.get(), compressed.data(), compressed.size(), body.data(), body.size(),
                                   dictionary.data(), dictionary.size(), compression_level);
    zcheck(n); compressed.resize(n);
    write_file(path, compressed);
}
} // namespace xrpla::core
