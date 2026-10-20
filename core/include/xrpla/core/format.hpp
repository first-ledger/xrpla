#pragma once
#include <xrpla/archive.hpp>
#include <array>
#include <functional>

namespace xrpla::core {
using Bytes = std::string;
struct TransactionRecord { Bytes transaction, metadata; };
// A full canonical postimage; an empty object denotes deletion.
struct ObjectRecord { Bytes key, object; };
struct LedgerRecord {
    std::uint32_t sequence;
    Bytes header;
    std::vector<TransactionRecord> transactions;
    std::vector<ObjectRecord> patches;
};
struct ChunkInfo {
    std::uint16_t version;
    std::uint32_t first, last, count;
    std::string dictionary_hash;
};
struct Limits {
    std::uint64_t decompressed_bytes = 128ULL * 1024 * 1024 * 1024;
    std::uint32_t record_bytes = 16 * 1024 * 1024;
    std::uint64_t ledger_bytes = 512ULL * 1024 * 1024;
    std::uint32_t transactions_per_ledger = 1000000;
    std::uint32_t patches_per_ledger = 1000000;
};

// Validates the compressed checksum before visiting any ledger. The callback
// must not publish a batch until this function also validates the offset table.
ChunkInfo read_chunk(fs::path const&, Bytes const& dictionary,
    std::function<void(LedgerRecord&&)> const& visitor, Limits limits = {});

// Container-only writer. Callers supplying real ledgers must first obtain them
// from the canonical verifier. Used by fixture generation and format tests.
void write_chunk(fs::path const&, std::vector<LedgerRecord> const&,
                 Bytes const& dictionary = {}, std::uint16_t version = 2, int compression_level = 3);

// Snapshot parts contain sorted keyed postimages. Limits apply to each part.
// Writers start a new part once one holds this much (keys, lengths and objects): about 40 MB
// compressed, so a mainnet snapshot is ~55 objects, each one ranged GET of a download and
// still decoded in parallel and refetched alone.
constexpr std::uint64_t snapshot_part_bytes = 64ULL * 1024 * 1024;
void read_snapshot_part(fs::path const&, Bytes const& dictionary,
    std::uint64_t count, std::function<void(ObjectRecord&&)> const&, Limits = {});
void write_snapshot_part(fs::path const&, std::vector<ObjectRecord> const&,
                         Bytes const& dictionary = {}, int compression_level = 3);
} // namespace xrpla::core
