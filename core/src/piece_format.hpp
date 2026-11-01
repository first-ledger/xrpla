#pragma once
// Readers for published pieces and bundles (XRPP containers, their XTXI
// transaction index) and for the local XQIX query index derived from them.
// docs/PUBLICATION.md describes the formats.
#include "history_reader.hpp"
#include <xrpla/core/format.hpp>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <optional>

namespace xrpla::core::pieces {
namespace hx = ::xrpla::history;
using U = std::uint64_t;
using V = std::string_view;

// A read-only memory map; containers reach tens of GB and are never copied.
class Mapped {
    int fd_ = -1;
    void* data_ = nullptr;
    std::size_t size_ = 0;
public:
    explicit Mapped(fs::path const& path) {
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0) throw Error("Cannot open " + path.string());
        size_ = fs::file_size(path);
        if (size_) {
            data_ = ::mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd_, 0);
            if (data_ == MAP_FAILED) { data_ = nullptr; ::close(fd_); throw Error("Cannot map " + path.string()); }
        }
    }
    ~Mapped() { if (data_) ::munmap(data_, size_); if (fd_ >= 0) ::close(fd_); }
    Mapped(Mapped const&) = delete;
    Mapped& operator=(Mapped const&) = delete;
    V view() const { return {static_cast<char const*>(data_), size_}; }
    // Unmaps the pages of `part` (a view of this file) from the process; they stay in the page cache
    // and are faulted back in if read again. Keeps a one-pass scan's resident set small.
    void release(V part) const {
        auto page = static_cast<std::uintptr_t>(::sysconf(_SC_PAGESIZE));
        auto begin = reinterpret_cast<std::uintptr_t>(part.data()), end = begin + part.size();
        begin = (begin + page - 1) / page * page; end = end / page * page;
        if (end > begin) ::madvise(reinterpret_cast<void*>(begin), end - begin, MADV_DONTNEED);
    }
};

inline U le(V data, U at, unsigned width) {
    if (at > data.size() || width > data.size() - at) throw Error("Truncated piece data");
    U n = 0;
    for (unsigned i = 0; i < width; ++i) n |= U(static_cast<unsigned char>(data[at + i])) << (8 * i);
    return n;
}
inline void put(Bytes& out, U n, unsigned width) { for (unsigned i = 0; i < width; ++i) { out += char(n & 255); n >>= 8; } }
inline void put_be32(char* out, std::uint32_t n) { for (int i = 3; i >= 0; --i) { out[i] = char(n & 255); n >>= 8; } }
inline std::uint32_t be32(char const* in) { std::uint32_t n = 0; for (int i = 0; i < 4; ++i) n = (n << 8) | static_cast<unsigned char>(in[i]); return n; }
inline U varint(V data, U& at) {
    U n = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        if (at >= data.size()) throw Error("Truncated varint");
        U b = static_cast<unsigned char>(data[at++]);
        n |= (b & 127) << shift;
        if (!(b & 128)) return n;
    }
    throw Error("Oversized varint");
}
// A decoded page row (the canonical ledger record): sequence, header, transactions with
// metadata, then object patches.
inline LedgerRecord parse_ledger_record(V raw) {
    hx::Reader r{raw};
    LedgerRecord record;
    record.sequence = static_cast<std::uint32_t>(r.little(4));
    record.header = Bytes(r.take(r.little(4)));
    for (U n = r.little(4), i = 0; i < n; ++i) { Bytes tx(r.take(r.little(4))); Bytes meta(r.take(r.little(4))); record.transactions.push_back({std::move(tx), std::move(meta)}); }
    for (U n = r.little(4), i = 0; i < n; ++i) { Bytes key(r.take(32)); Bytes value(r.take(r.little(4))); record.patches.push_back({std::move(key), std::move(value)}); }
    r.end();
    return record;
}

// XRPP directory: header, one row per XRPH segment, index descriptor.
struct Segment { U first, count, offset, length; Bytes digest; };
struct Container {
    U first = 0, last = 0, transactions = 0, directory_size = 0;
    Bytes parent_hash, last_hash;
    std::vector<Segment> segments;
    U index_offset = 0, index_length = 0;
    static constexpr U head = 92, row = 56, index = 48;
    // `directory_sha256` (hex) comes from the signed catalog.
    static Container parse(V data, std::string const& directory_sha256) { return parse_directory(data, directory_sha256, data.size()); }
    // Bytes of the directory needed: `head` bytes tell how many rows follow.
    static U directory_bytes(V header) {
        if (header.size() < head || header.substr(0, 4) != "XRPP") throw Error("Invalid container header");
        U n = le(header, 16, 4);
        if (!n || n > 65536) throw Error("Invalid container header");
        return head + row * n + index;
    }
    // Parses a directory read on its own (a ranged read); `file_size` comes from the signed catalog.
    static Container parse_directory(V data, std::string const& directory_sha256, U file_size) {
        Container c;
        if (data.size() < head + index || data.substr(0, 4) != "XRPP" || le(data, 4, 2) != 1) throw Error("Invalid container header");
        c.first = le(data, 8, 4); c.last = le(data, 12, 4);
        U n = le(data, 16, 4);
        if (!n || n > 65536 || c.last < c.first) throw Error("Invalid container header");
        c.parent_hash = Bytes(data.substr(20, 32)); c.last_hash = Bytes(data.substr(52, 32)); c.transactions = le(data, 84, 8);
        c.directory_size = head + row * n + index;
        if (data.size() < c.directory_size) throw Error("Truncated container directory");
        if (hx::hex(hx::sha(data.substr(0, c.directory_size))) != directory_sha256) throw Error("Container directory differs from catalog");
        U expected = c.first, offset = c.directory_size;
        for (U i = 0; i < n; ++i) {
            U at = head + row * i;
            Segment s{le(data, at, 4), le(data, at + 4, 4), le(data, at + 8, 8), le(data, at + 16, 8), Bytes(data.substr(at + 24, 32))};
            if (s.first != expected || !s.count || s.offset != offset) throw Error("Invalid segment table");
            expected += s.count; offset += s.length; c.segments.push_back(std::move(s));
        }
        U at = head + row * n;
        c.index_offset = le(data, at, 8); c.index_length = le(data, at + 8, 8);
        if (expected != c.last + 1 || c.index_offset != offset || offset + c.index_length + 32 != file_size) throw Error("Invalid segment table");
        return c;
    }
    std::size_t segment_of(U ledger) const {
        auto it = std::upper_bound(segments.begin(), segments.end(), ledger, [](U n, Segment const& s) { return n < s.first; });
        if (it == segments.begin() || ledger >= (it - 1)->first + (it - 1)->count) throw Error("Ledger outside container");
        return static_cast<std::size_t>(it - segments.begin() - 1);
    }
};

// XTXI fingerprint index (tools/tx_fingerprint_index.py): hash -> candidate (ledger, record position).
class TxIndex {
    V bitmap_, samples_, fingerprints_, locations_, counts_;
    unsigned q_ = 0, fbits_ = 0, lbits_ = 0;
    U first_ = 0, n_ = 0, ledgers_ = 0;
    // Where the varint counts of every `stride`-th ledger start, and the entries before it. A
    // start per ledger would be 8 bytes each, about 0.8 GB for all of history outside the cache
    // budget; these are a 64th of that, and a lookup decodes at most stride - 1 counts.
    struct Checkpoint { U at, total; };
    static constexpr U stride = 64;
    std::vector<Checkpoint> checkpoints_;
    // Walks ledger counts from the checkpoint at or before ledger offset `i`, to `i`.
    Checkpoint seek(U i) const {
        if (i >= ledgers_) throw Error("Ledger outside transaction index");
        auto c = checkpoints_[i / stride];
        for (U k = i - i % stride; k < i; ++k) c.total += varint(counts_, c.at);
        return c;
    }
    static U bits(V buf, U at, unsigned width) {
        if (!width) return 0;
        U first = at / 8, last = (at + width - 1) / 8;
        if (last >= buf.size() || width + at % 8 > 64) throw Error("Truncated bit field");
        U v = 0;
        for (U i = first; i <= last; ++i) v |= U(static_cast<unsigned char>(buf[i])) << (8 * (i - first));
        return (v >> (at % 8)) & (width == 64 ? ~U(0) : ((U(1) << width) - 1));
    }
public:
    static constexpr U sample = 256;
    TxIndex() = default;
    explicit TxIndex(V data) {
        constexpr U head = 30;
        if (data.size() < head + 40 + 32 || data.substr(0, 4) != "XTXI" || le(data, 4, 2) != 1) throw Error("Invalid transaction index");
        q_ = unsigned(le(data, 6, 1)); fbits_ = unsigned(le(data, 7, 1)); lbits_ = unsigned(le(data, 8, 1));
        first_ = le(data, 10, 4); n_ = le(data, 14, 8); U ledgers = le(data, 22, 8);
        if (q_ < 1 || q_ > 41 || fbits_ < 1 || fbits_ > 32 || lbits_ < 1 || lbits_ > 41) throw Error("Invalid transaction index");
        U at = head + 40, lengths[5];
        for (U i = 0; i < 5; ++i) lengths[i] = le(data, head + 8 * i, 8);
        V parts[5];
        for (U i = 0; i < 5; ++i) { if (at + lengths[i] > data.size() - 32) throw Error("Index section lengths differ"); parts[i] = data.substr(at, lengths[i]); at += lengths[i]; }
        if (at != data.size() - 32) throw Error("Index section lengths differ");
        bitmap_ = parts[0]; samples_ = parts[1]; fingerprints_ = parts[2]; locations_ = parts[3];
        if (bitmap_.size() != (n_ + (U(1) << q_) + 7) / 8 || samples_.size() != 8 * (((U(1) << q_) + sample - 1) / sample) ||
            fingerprints_.size() != (n_ * fbits_ + 7) / 8 || locations_.size() != (n_ * lbits_ + 7) / 8) throw Error("Index section sizes differ");
        counts_ = parts[4];
        // Each count takes at least a byte, so a ledger total beyond the section's bytes is invalid
        // and cannot size the reservation.
        if (ledgers > counts_.size()) throw Error("Ledger counts differ from entries");
        checkpoints_.reserve((ledgers + stride - 1) / stride);
        U total = 0, pos = 0;
        for (U i = 0; i < ledgers; ++i) {
            if (i % stride == 0) checkpoints_.push_back({pos, total});
            total += varint(counts_, pos);
        }
        if (pos != counts_.size() || total != n_) throw Error("Ledger counts differ from entries");
        ledgers_ = ledgers;
    }
    U first() const { return first_; }
    U ledgers() const { return ledgers_; }
    U count(U ledger) const { auto c = seek(ledger - first_); return varint(counts_, c.at); }
    std::vector<std::pair<U, U>> candidates(V hash) const {
        if (hash.size() != 32) throw Error("Hash length");
        std::vector<std::pair<U, U>> out;
        if (!n_) return out;
        unsigned __int128 top = 0;
        for (unsigned i = 0; i < 16; ++i) top = (top << 8) | static_cast<unsigned char>(hash[i]);
        U value = static_cast<U>(top >> (128 - q_ - fbits_));
        U bucket = value >> fbits_, fingerprint = value & ((U(1) << fbits_) - 1);
        U base = bucket - bucket % sample;
        U pos = le(samples_, 8 * (base / sample), 8), entry = pos - base;
        for (U b = base; b < bucket; ++b) { while (bits(bitmap_, pos, 1)) { ++pos; ++entry; } ++pos; }
        while (bits(bitmap_, pos, 1)) {
            if (bits(fingerprints_, entry * fbits_, fbits_) == fingerprint) {
                U ordinal = bits(locations_, entry * lbits_, lbits_);
                if (ordinal >= n_) throw Error("Location out of range");
                // The last checkpoint at or below the entry precedes its ledger (counts only add),
                // and the next one, above it, follows: the ledger is within this stride.
                auto it = std::upper_bound(checkpoints_.begin(), checkpoints_.end(), ordinal, [](U n, Checkpoint const& c) { return n < c.total; }) - 1;
                auto c = *it;
                U ledger = static_cast<U>(it - checkpoints_.begin()) * stride;
                for (;; ++ledger) {
                    if (ledger >= ledgers_) throw Error("Location out of range");
                    U n = varint(counts_, c.at);
                    if (ordinal < c.total + n) break;
                    c.total += n;
                }
                out.emplace_back(first_ + ledger, ordinal - c.total);
            }
            ++pos; ++entry;
        }
        return out;
    }
};

// XQIX local query index, one per container (built by `xrpla index-pieces`):
//   head: "XQIX", u16 version, u16 sections, u32 first, u32 ledgers, container SHA-256,
//         then (u64 offset, u64 length) per section; SHA-256 trailer over everything before it.
//   0 headers        118 bytes per ledger
//   1 ledger hashes  (u64 hash prefix, u32 ledger offset), sorted
//   2 tx order       (ledgers + 1) u64 offsets, then per ledger: 0 (record order is
//                    TransactionIndex order) or 1 + record positions in TransactionIndex order
//   3 accounts, 4 NFTs, 5 objects, 6 NFT states: keyed tables (below)
// A keyed table is u64 keys, u64 blocks, u32 key width, u32 keys per block, a directory
// of (first key, u64 record offset) per block, then records: key, varint entries,
// varint payload bytes, payload. Posting payloads hold (ledger offset delta, TransactionIndex
// or its delta within a ledger, record position); object payloads hold
// (ledger offset delta << 1 | deleted). NFT state payloads hold, for each ledger where a
// transaction moved the token between pages (a mint, transfer, URI change or burn), (ledger
// offset delta, record position of the ledger's last such transaction, 16-byte fingerprint of
// the state it left: piece_history.hpp's nft_fingerprint). Entries ascend. Version 2 added NFT
// states. Version 3 holds what Clio 2.8 holds: NFT postings for the transactions its
// nft_history lists (successful NFToken transactions, piece_index.cpp's nft_ids), the burns
// version 2 missed (a token in a deleted page's PreviousFields alone: nft_changes), and
// fingerprints whose owner-and-burned half dates nft_info's ledger_index (a URI change alone
// does not move it). A version 2 index is still read for everything else, but its NFT states
// never: PieceHistory reports NFT state as not indexed until index-pieces (which every sync
// runs) has rebuilt it.
constexpr unsigned query_version = 3;
constexpr unsigned query_sections = 7;
constexpr U query_head = 48 + 16 * query_sections;
constexpr unsigned block_keys = 64;

struct Record { V key; U entries; V payload; U next; };
class Table {
    V directory_, records_;
    U keys_ = 0, blocks_ = 0;
    unsigned width_ = 0;
public:
    Table() = default;
    Table(V data, unsigned width) {
        if (data.size() < 24) throw Error("Truncated query table");
        keys_ = le(data, 0, 8); blocks_ = le(data, 8, 8); width_ = unsigned(le(data, 16, 4));
        if (width_ != width || le(data, 20, 4) != block_keys || blocks_ != (keys_ + block_keys - 1) / block_keys) throw Error("Invalid query table");
        U dir = blocks_ * (width_ + 8);
        if (dir > data.size() - 24) throw Error("Truncated query table");
        directory_ = data.substr(24, dir); records_ = data.substr(24 + dir);
    }
    U keys() const { return keys_; }
    // Where a record this table returned starts (for at()).
    U offset_of(Record const& r) const { return static_cast<U>(r.key.data() - records_.data()); }
    std::optional<Record> at(U offset) const {
        if (offset >= records_.size()) return std::nullopt;
        if (width_ > records_.size() - offset) throw Error("Truncated query record");
        Record r{records_.substr(offset, width_), 0, {}, offset + width_};
        r.entries = varint(records_, r.next);
        U length = varint(records_, r.next);
        if (length > records_.size() - r.next) throw Error("Truncated query record");
        r.payload = records_.substr(r.next, length); r.next += length;
        return r;
    }
    // Offset of the first record whose key is >= key (> key when strict); the end offset when none.
    U seek(V key, bool strict) const {
        if (!blocks_) return records_.size();
        U lo = 0, hi = blocks_;  // first block whose first key > key
        while (lo < hi) { U mid = (lo + hi) / 2; if (directory_.substr(mid * (width_ + 8), width_) <= key) lo = mid + 1; else hi = mid; }
        U offset = lo ? le(directory_, (lo - 1) * (width_ + 8) + width_, 8) : 0;
        while (auto r = at(offset)) {
            if (strict ? r->key > key : r->key >= key) return offset;
            offset = r->next;
        }
        return records_.size();
    }
    std::optional<Record> find(V key) const {
        if (key.size() != width_) return std::nullopt;
        auto r = at(seek(key, false));
        if (r && r->key == key) return r;
        return std::nullopt;
    }
};
struct Posting { U ledger, index, position; };  // ledger offset, TransactionIndex, record position
// Where a PostingReader stands: the payload offset of the next posting, how many it has read,
// and the ledger offset and TransactionIndex of the last one read (both 0 before the first),
// which the next posting's deltas build on. A seek index (XQSK, below) records it.
struct PostingState { U at = 0, read = 0, ledger = 0, index = 0; };
// Streaming readers: posting and version lists can hold millions of entries for busy keys.
class PostingReader {
    Record const& r_;
    PostingState s_;
public:
    // From the list's start, or from a state an earlier read of the same record stood at.
    explicit PostingReader(Record const& r, PostingState const& from = {}) : r_(r), s_(from) {
        if (s_.read > r_.entries || s_.at > r_.payload.size() || (!s_.read && (s_.at || s_.ledger || s_.index))) throw Error("Posting reader state outside its list");
    }
    PostingState const& state() const { return s_; }
    bool next(Posting& p) {
        if (s_.read == r_.entries) { if (s_.at != r_.payload.size()) throw Error("Posting payload length"); return false; }
        U delta = varint(r_.payload, s_.at), value = varint(r_.payload, s_.at);
        s_.ledger += delta; s_.index = delta || !s_.read ? value : s_.index + value;
        p = {s_.ledger, s_.index, varint(r_.payload, s_.at)}; ++s_.read;
        return true;
    }
};
inline std::vector<Posting> postings(Record const& r) {
    std::vector<Posting> out; out.reserve(r.entries);
    U at = 0, ledger = 0, index = 0;
    for (U i = 0; i < r.entries; ++i) {
        U delta = varint(r.payload, at), value = varint(r.payload, at);
        ledger += delta; index = delta || !i ? value : index + value;
        out.push_back({ledger, index, varint(r.payload, at)});
    }
    if (at != r.payload.size()) throw Error("Posting payload length");
    return out;
}
struct NftEvent { U ledger, position; V fingerprint; };
inline std::vector<NftEvent> nft_events(Record const& r) {
    std::vector<NftEvent> out; out.reserve(r.entries);
    U at = 0, ledger = 0;
    for (U i = 0; i < r.entries; ++i) {
        ledger += varint(r.payload, at); U position = varint(r.payload, at);
        if (r.payload.size() - at < 16) throw Error("NFT state payload length");
        out.push_back({ledger, position, r.payload.substr(at, 16)}); at += 16;
    }
    if (at != r.payload.size()) throw Error("NFT state payload length");
    return out;
}
struct Version { U ledger; bool deleted; };
// The newest version at or before ledger offset `until`, reading only as far as needed.
inline std::optional<Version> version_at(Record const& r, U until) {
    std::optional<Version> found;
    U at = 0, ledger = 0;
    for (U i = 0; i < r.entries; ++i) {
        U v = varint(r.payload, at); ledger += v >> 1;
        if (ledger > until) return found;
        found = Version{ledger, bool(v & 1)};
    }
    if (at != r.payload.size()) throw Error("Version payload length");
    return found;
}
inline std::vector<Version> versions(Record const& r) {
    std::vector<Version> out; out.reserve(r.entries);
    U at = 0, ledger = 0;
    for (U i = 0; i < r.entries; ++i) { U v = varint(r.payload, at); ledger += v >> 1; out.push_back({ledger, bool(v & 1)}); }
    if (at != r.payload.size()) throw Error("Version payload length");
    return out;
}

class QueryIndex {
    std::unique_ptr<Mapped> map_;
    V headers_, hashes_, order_;
public:
    U first = 0, count = 0, version = 0;
    Table accounts, nfts, objects, nft_states;
    QueryIndex(fs::path const& path, Bytes const& container_sha256) : map_(std::make_unique<Mapped>(path)) {
        auto data = map_->view();
        if (data.size() < query_head + 32 || data.substr(0, 4) != "XQIX" || le(data, 6, 2) != query_sections) throw Error("Invalid query index " + path.string());
        version = le(data, 4, 2);
        if (version != query_version && version != 2) throw Error("Invalid query index " + path.string());
        first = le(data, 8, 4); count = le(data, 12, 4);
        if (data.substr(16, 32) != container_sha256) throw Error("Query index belongs to another container; rebuild it with index-pieces");
        V parts[query_sections];
        for (unsigned i = 0; i < query_sections; ++i) {
            U offset = le(data, 48 + 16 * i, 8), length = le(data, 56 + 16 * i, 8);
            if (offset > data.size() - 32 || length > data.size() - 32 - offset) throw Error("Invalid query index sections");
            parts[i] = data.substr(offset, length);
        }
        headers_ = parts[0]; hashes_ = parts[1]; order_ = parts[2];
        if (headers_.size() != count * 118 || hashes_.size() != count * 12 || order_.size() < (count + 1) * 8) throw Error("Invalid query index sections");
        accounts = Table(parts[3], 20); nfts = Table(parts[4], 32); objects = Table(parts[5], 32); nft_states = Table(parts[6], 32);
    }
    // The file's SHA-256 trailer, which names this build of it (XQSK records it).
    V checksum() const { auto data = map_->view(); return data.substr(data.size() - 32); }
    V header(U seq) const { return headers_.substr((seq - first) * 118, 118); }
    std::optional<U> ledger_of(V hash) const {
        if (hash.size() != 32) return std::nullopt;
        U prefix = 0; for (unsigned i = 0; i < 8; ++i) prefix = (prefix << 8) | static_cast<unsigned char>(hash[i]);
        U lo = 0, hi = count;
        while (lo < hi) { U mid = (lo + hi) / 2; if (le(hashes_, mid * 12, 8) < prefix) lo = mid + 1; else hi = mid; }
        for (; lo < count && le(hashes_, lo * 12, 8) == prefix; ++lo) {
            U offset = le(hashes_, lo * 12 + 8, 4);
            if (offset < count && hx::half(Bytes("LWR\0", 4) + Bytes(header(first + offset))) == hash) return first + offset;
        }
        return std::nullopt;
    }
    // Record positions of one ledger's transactions in TransactionIndex order.
    std::vector<U> order(U seq, U transactions) const {
        U i = seq - first, base = (count + 1) * 8, at = base + le(order_, i * 8, 8), end = base + le(order_, i * 8 + 8, 8);
        if (end > order_.size() || at >= end) throw Error("Invalid transaction order");
        std::vector<U> out;
        U mode = varint(order_, at);
        if (mode == 0) { for (U n = 0; n < transactions; ++n) out.push_back(n); }
        else { while (at < end) out.push_back(varint(order_, at)); }
        if (at != end || out.size() != transactions) throw Error("Transaction order differs");
        return out;
    }
};
inline fs::path query_path(fs::path const& cache, std::string const& container_sha256) {
    return cache / "query" / (container_sha256.substr(0, 32) + ".xqix");
}

// XQSK local seek index, one per XQIX and derived from it alone (built by `xrpla index-pieces`
// and every sync, also for a query index an older build left): for each account and NFT
// posting list longer than two strides, the PostingReader state before every `stride`-th
// posting, so a history page starts reading near its first wanted posting instead of at the
// list's start. A checkpoint is only where to start: the postings read from it are the ones a
// read from the start would reach there, and a range without the file reads from the start.
//   head: "XQSK", u16 version, u16 reserved, u32 stride, u32 first (the XQIX's), the XQIX's
//         SHA-256 trailer, then per table (accounts, NFTs) u64 lists and u64 checkpoints;
//   per table: one row per list, ascending by record offset (u64 offset of the record in the
//         table's records, u64 ordinal of its first checkpoint), then the checkpoints: for
//         posting j * stride (j = 1, 2, ... while below the list's entries) the state before it,
//         u32 payload offset, u32 ledger offset and u32 TransactionIndex of posting j * stride - 1;
//   SHA-256 trailer over everything before it.
// A list whose payload or values do not fit 32 bits has no row (and is read from its start).
constexpr unsigned seek_version = 1;
constexpr U seek_default_stride = 128;
constexpr U seek_head = 4 + 2 + 2 + 4 + 4 + 32 + 2 * 16;
constexpr U seek_row = 16, seek_point = 12;
// A list's checkpoints: state(j) for j in [0, size()] (0 is the list's start).
class SeekPoints {
    V rows_;
    U stride_ = 0;
public:
    SeekPoints() = default;
    SeekPoints(V rows, U stride) : rows_(rows), stride_(stride) {}
    U size() const { return rows_.size() / seek_point; }
    U stride() const { return stride_; }
    PostingState state(U j) const {
        if (!j) return {};
        U at = (j - 1) * seek_point;
        return {le(rows_, at, 4), j * stride_, le(rows_, at + 4, 4), le(rows_, at + 8, 4)};
    }
};
class SeekIndex {
    std::unique_ptr<Mapped> map_;
    V lists_[2], points_[2];
    U counts_[2] = {};
public:
    enum Kind : unsigned { accounts = 0, nfts = 1 };
    U stride = 0;
    SeekIndex(fs::path const& path, QueryIndex const& query) : map_(std::make_unique<Mapped>(path)) {
        auto data = map_->view();
        if (data.size() < seek_head + 32 || data.substr(0, 4) != "XQSK" || le(data, 4, 2) != seek_version) throw Error("Invalid seek index " + path.string());
        stride = le(data, 8, 4);
        if (!stride || le(data, 12, 4) != query.first) throw Error("Invalid seek index " + path.string());
        if (data.substr(16, 32) != query.checksum()) throw Error("Seek index belongs to another query index; rebuild it with index-pieces");
        U at = seek_head, end = data.size() - 32;
        for (unsigned t = 0; t < 2; ++t) {
            U lists = le(data, 48 + 16 * t, 8), points = le(data, 56 + 16 * t, 8);
            if (lists > (end - at) / seek_row) throw Error("Invalid seek index sections");
            lists_[t] = data.substr(at, lists * seek_row); at += lists * seek_row;
            if (points > (end - at) / seek_point) throw Error("Invalid seek index sections");
            points_[t] = data.substr(at, points * seek_point); at += points * seek_point;
            counts_[t] = points;
        }
        if (at != end) throw Error("Invalid seek index sections");
    }
    // The checkpoints of `record`, found in its table at `offset` (Table::offset_of); none
    // when the list is short. They must be as many as its entries need; a reader started at
    // one checks the payload bounds, and one that reads on to the next checkpoint must arrive
    // at its state (PieceHistory::history_page).
    SeekPoints points(Kind kind, U offset, Record const& record) const {
        V lists = lists_[kind];
        U rows = lists.size() / seek_row, lo = 0, hi = rows;
        while (lo < hi) { U mid = (lo + hi) / 2; if (le(lists, mid * seek_row, 8) < offset) lo = mid + 1; else hi = mid; }
        if (lo == rows || le(lists, lo * seek_row, 8) != offset) return {};
        U begin = le(lists, lo * seek_row + 8, 8), end = lo + 1 < rows ? le(lists, (lo + 1) * seek_row + 8, 8) : counts_[kind];
        if (begin > end || end > counts_[kind] || !record.entries || end - begin != (record.entries - 1) / stride) throw Error("Seek index differs from its query index");
        return SeekPoints(points_[kind].substr(begin * seek_point, (end - begin) * seek_point), stride);
    }
};
inline fs::path seek_path(fs::path const& cache, std::string const& container_sha256) {
    return cache / "query" / (container_sha256.substr(0, 32) + ".xqsk");
}

// XQSS local snapshot state: the base snapshot's sorted records re-cut into ~32 KiB
// blocks, each compressed with one zstd dictionary trained on the snapshot, so a
// point read decompresses one small block instead of a 16 MiB published part.
//   head: "XQSS", u16 version, u16 reserved, u32 ledger, u64 objects, u64 blocks,
//         snapshot index SHA-256, u64 dictionary length; then the dictionary, then
//   per block (first key, u64 offset, u32 compressed bytes, u32 raw bytes, u32 records),
//   then the compressed blocks; SHA-256 trailer over everything before it.
constexpr U state_head = 4 + 2 + 2 + 4 + 8 + 8 + 32 + 8;
constexpr U state_row = 32 + 8 + 4 + 4 + 4;
struct StateBlock { Bytes raw; std::vector<std::pair<V, V>> items; };
class StateIndex {
    std::unique_ptr<Mapped> map_;
    V directory_, blocks_;
    std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)> dictionary_{nullptr, ZSTD_freeDDict};
public:
    U ledger = 0, objects = 0, count = 0;
    StateIndex(fs::path const& path, Bytes const& snapshot_index_sha256) : map_(std::make_unique<Mapped>(path)) {
        auto data = map_->view();
        if (data.size() < state_head + 32 || data.substr(0, 4) != "XQSS" || le(data, 4, 2) != 1) throw Error("Invalid snapshot state index " + path.string());
        ledger = le(data, 8, 4); objects = le(data, 12, 8); count = le(data, 20, 8);
        if (data.substr(28, 32) != snapshot_index_sha256) throw Error("Snapshot state index belongs to another snapshot; rebuild it with index-pieces");
        U dictionary = le(data, 60, 8);
        if (dictionary > data.size() - state_head - 32 || count > (data.size() - state_head - 32 - dictionary) / state_row) throw Error("Invalid snapshot state index");
        dictionary_.reset(ZSTD_createDDict(data.data() + state_head, dictionary));
        if (!dictionary_) throw Error("Invalid snapshot state dictionary");
        directory_ = data.substr(state_head + dictionary, count * state_row);
        blocks_ = data.substr(state_head + dictionary + count * state_row);
        blocks_.remove_suffix(32);
    }
    V first_key(U i) const { return directory_.substr(i * state_row, 32); }
    // The block that would hold `key`: the last block whose first key is <= key (block 0 if none).
    U block_for(V key) const {
        U lo = 0, hi = count;
        while (lo < hi) { U mid = (lo + hi) / 2; if (first_key(mid) <= key) lo = mid + 1; else hi = mid; }
        return lo ? lo - 1 : 0;
    }
    std::shared_ptr<StateBlock> load(U i) const {
        U at = i * state_row + 32, offset = le(directory_, at, 8), compressed = le(directory_, at + 8, 4), raw = le(directory_, at + 12, 4), records = le(directory_, at + 16, 4);
        if (offset > blocks_.size() || compressed > blocks_.size() - offset) throw Error("Snapshot state block bounds");
        auto block = std::make_shared<StateBlock>();
        block->raw.resize(raw);
        // One decompression context per thread: making one costs about as much as a block's decode.
        thread_local std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx(ZSTD_createDCtx(), ZSTD_freeDCtx);
        auto n = ZSTD_decompress_usingDDict(ctx.get(), block->raw.data(), raw, blocks_.data() + offset, compressed, dictionary_.get());
        if (ZSTD_isError(n) || n != raw) throw Error("Snapshot state block decode failed");
        V view(block->raw);
        block->items.reserve(std::min<U>(records, raw / 36));
        for (U p = 0; p < view.size();) {
            auto key = view.substr(p, 32); p += 32;
            U length = le(view, p, 4); p += 4;
            if (length > view.size() - p) throw Error("Truncated snapshot state record");
            block->items.emplace_back(key, view.substr(p, length)); p += length;
        }
        if (block->items.size() != records || block->items.empty() || block->items.front().first != first_key(i)) throw Error("Snapshot state block differs from its directory");
        return block;
    }
};
inline fs::path state_path(fs::path const& cache, U ledger) { return cache / "query" / ("snapshot-" + std::to_string(ledger) + ".xqss"); }

// XQNF local snapshot NFT map: the owner of every NFToken in the base snapshot, so
// nft_info can find a token that no cached transaction has touched since.
//   head: "XQNF", u16 version, u16 reserved, u32 ledger, u64 tokens, snapshot index SHA-256;
//   then (NFTokenID, owner AccountID) rows ascending by ID; SHA-256 trailer over everything before it.
constexpr U nft_head = 4 + 2 + 2 + 4 + 8 + 32;
constexpr U nft_row = 32 + 20;
class NftIndex {
    std::unique_ptr<Mapped> map_;
    V rows_;
public:
    U ledger = 0, count = 0;
    NftIndex(fs::path const& path, Bytes const& snapshot_index_sha256) : map_(std::make_unique<Mapped>(path)) {
        auto data = map_->view();
        if (data.size() < nft_head + 32 || data.substr(0, 4) != "XQNF" || le(data, 4, 2) != 1) throw Error("Invalid snapshot NFT index " + path.string());
        ledger = le(data, 8, 4); count = le(data, 12, 8);
        if (data.substr(20, 32) != snapshot_index_sha256) throw Error("Snapshot NFT index belongs to another snapshot; rebuild it with index-pieces");
        if (count != (data.size() - nft_head - 32) / nft_row || (data.size() - nft_head - 32) % nft_row) throw Error("Invalid snapshot NFT index");
        rows_ = data.substr(nft_head, count * nft_row);
    }
    std::optional<V> owner(V id) const {
        U lo = 0, hi = count;
        while (lo < hi) { U mid = (lo + hi) / 2; if (rows_.substr(mid * nft_row, 32) < id) lo = mid + 1; else hi = mid; }
        if (lo < count && rows_.substr(lo * nft_row, 32) == id) return rows_.substr(lo * nft_row + 32, 20);
        return std::nullopt;
    }
};
inline fs::path nft_path(fs::path const& cache, U ledger) { return cache / "query" / ("snapshot-" + std::to_string(ledger) + ".xqnf"); }

// XQOX optional local indexes, one per container and one for the base snapshot, built by
// `xrpla index-pieces` (and every sync) unless the configuration turns them off:
//   head: "XQOX", u16 version, u16 kind, u32 ledger (the container's first, or the snapshot's),
//         source SHA-256 (the container's, or the snapshot index's); then one keyed table
//         (as in XQIX); SHA-256 trailer over everything before it.
//   kind 1, MPT holders: key MPTokenIssuanceID (24 bytes), payload the AccountIDs (20 bytes
//         each, ascending) of every MPToken of it the snapshot holds or the container changes.
//   kind 2, NFTs by issuer: key issuer AccountID (20 bytes), payload 16-byte entries ascending:
//         the taxon (unscrambled, big-endian), then the token ID without its issuer (its first
//         4 and last 8 bytes), for every token the snapshot holds or the container's NFT
//         postings list (piece_index.cpp's nft_ids: tokens minted, or named by another
//         successful NFToken transaction, never one a failed transaction alone names).
//         Entries sort as (taxon, token ID), Clio's order. Version 2: version 1 took every
//         token a transaction named, failed ones included, and is rebuilt.
// Both are candidates only: every held ledger's, whatever the ledger asked about, which a
// reader checks at that ledger, as Clio's holder and issuer tables are.
enum class Optional : unsigned { mpt_holders = 1, nfts_by_issuer = 2 };
constexpr unsigned optional_version(Optional kind) { return kind == Optional::nfts_by_issuer ? 2 : 1; }
constexpr U optional_head = 4 + 2 + 2 + 4 + 32;
constexpr unsigned optional_width(Optional kind) { return kind == Optional::mpt_holders ? 24 : 20; }
constexpr std::size_t optional_entry(Optional kind) { return kind == Optional::mpt_holders ? 20 : 16; }
class OptionalIndex {
    std::unique_ptr<Mapped> map_;
public:
    Optional kind;
    U ledger = 0;
    Table table;
    OptionalIndex(fs::path const& path, Optional kind_, Bytes const& source_sha256) : map_(std::make_unique<Mapped>(path)), kind(kind_) {
        auto data = map_->view();
        if (data.size() < optional_head + 24 + 32 || data.substr(0, 4) != "XQOX" || le(data, 4, 2) != optional_version(kind) || le(data, 6, 2) != static_cast<unsigned>(kind))
            throw Error("Invalid optional index " + path.string());
        ledger = le(data, 8, 4);
        if (data.substr(12, 32) != source_sha256) throw Error("Optional index belongs to other data; rebuild it with index-pieces");
        table = Table(data.substr(optional_head, data.size() - optional_head - 32), optional_width(kind));
    }
    // The entries under one key: a sorted run of fixed-width entries.
    V entries(V key) const {
        auto record = table.find(key);
        if (!record) return {};
        if (record->payload.size() != record->entries * optional_entry(kind)) throw Error("Optional index entries differ from their count");
        return record->payload;
    }
};
// An NFTs-by-issuer entry for a token ID, and the token ID back from an entry and its issuer.
inline Bytes issuer_entry(V id, std::uint32_t taxon) {
    Bytes entry(4, '\0'); put_be32(entry.data(), taxon);
    entry.append(id.substr(0, 4)); entry.append(id.substr(24, 8));
    return entry;
}
inline Bytes token_of(V entry, V issuer) { return Bytes(entry.substr(4, 4)) + Bytes(issuer) + Bytes(entry.substr(8, 8)); }
inline fs::path optional_path(fs::path const& cache, std::string const& stem, Optional kind) {
    return cache / "query" / (stem + (kind == Optional::mpt_holders ? ".xqmh" : ".xqni"));
}
inline std::string snapshot_stem(U ledger) { return "snapshot-" + std::to_string(ledger); }
inline std::string container_stem(std::string const& container_sha256) { return container_sha256.substr(0, 32); }
}
