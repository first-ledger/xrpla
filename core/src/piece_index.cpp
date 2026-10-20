// Builds the XQIX query index for each range in a piece cache (piece_format.hpp).
// Every segment is decoded in full and checked against its digests; postings are
// derived with the same rules as the RocksDB store's account_tx and nft_tx
// indexes, so both backends answer history queries identically.
#include "piece_history.hpp"
#include "piece_format.hpp"
#include "wire.hpp"
#include <xrpl/basics/strHex.h>
#include <xrpl/protocol/AccountID.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <xrpl/protocol/SField.h>
#include <xrpl/protocol/STArray.h>
#include <xrpl/protocol/STObject.h>
#include <xrpl/protocol/STTx.h>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/nft.h>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <zdict.h>
#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <fstream>
#include <map>
#include <mutex>
#include <queue>
#include <thread>

namespace xrpla::core {
namespace r = ::xrpl;
using namespace pieces;

namespace {
template<class T> Bytes raw_bytes(T const& value) { return Bytes(reinterpret_cast<char const*>(value.data()), value.size()); }
}

bool pseudo_transaction(std::string_view transaction) {
    try {
        auto type = wire::top_level(transaction, 1, 2);
        return type && type->size() == 2 && type->at(0) == 0 && static_cast<unsigned char>(type->at(1)) >= 100 && static_cast<unsigned char>(type->at(1)) <= 102;
    } catch (std::exception const&) { return false; }
}
// Clio 2.8's getNFTDataFromTx (src/etl/NFTHelpers.cpp) lists a transaction in a token's
// nft_history only when it succeeded and is one of the NFToken transactions, each naming its
// token(s) its own way: a mint the token its NFTokenPage changes add (the lowest ID on a page
// after it that was on none before), a burn, an offer's creation and a modification the
// transaction's NFTokenID, an accepted offer the NFTokenID of the buy offer it names (else the
// sell offer), and a cancellation every NFTokenOffer node's. Anything else (a failed burn, an
// AccountDelete that removes offers, a payment) is listed for no token. A transaction this
// build's libxrpl cannot decode (some of 2013) predates NFTokens: none. The ledger's tree
// hashes prove such data, so indexing must not fail on it.
std::set<Bytes> nft_ids(TransactionRecord const& record) {
    // Only the NFToken transactions name tokens: the type is read from the bytes first, as
    // decoding every transaction and its metadata would be most of an index build's time.
    try {
        if (auto type = wire::top_level(record.transaction, 1, 2); type && type->size() == 2) {
            auto code = static_cast<std::uint16_t>(static_cast<unsigned char>(type->at(0)) << 8 | static_cast<unsigned char>(type->at(1)));
            static std::set<std::uint16_t> const nftoken{r::ttNFTOKEN_MINT, r::ttNFTOKEN_BURN, r::ttNFTOKEN_CREATE_OFFER, r::ttNFTOKEN_CANCEL_OFFER,
                                                         r::ttNFTOKEN_ACCEPT_OFFER, r::ttNFTOKEN_MODIFY};
            if (!nftoken.contains(code)) return {};
        }
    } catch (std::exception const&) {}  // bytes the reader cannot walk are decoded below
    std::optional<r::STTx> tx;
    std::optional<r::STObject> meta;
    try {
        r::SerialIter tx_iter(record.transaction.data(), record.transaction.size());
        tx.emplace(tx_iter);
        r::SerialIter meta_iter(record.metadata.data(), record.metadata.size());
        meta.emplace(meta_iter, r::sfMetadata);
    } catch (std::exception const&) { return {}; }
    if (!meta->isFieldPresent(r::sfTransactionResult) || meta->getFieldU8(r::sfTransactionResult) != 0) return {};
    std::vector<r::STObject const*> nodes;
    if (meta->isFieldPresent(r::sfAffectedNodes)) for (auto const& node : meta->getFieldArray(r::sfAffectedNodes)) nodes.push_back(&node);
    auto inner = [](r::STObject const& node, r::SField const& f) -> r::STObject const* {
        return node.isFieldPresent(f) ? &static_cast<r::STObject const&>(node.peekAtField(f)) : nullptr;
    };
    auto type_of = [](r::STObject const& node) { return node.isFieldPresent(r::sfLedgerEntryType) ? node.getFieldU16(r::sfLedgerEntryType) : 0; };
    std::set<Bytes> out;
    auto token_in = [&](r::STObject const* fields) {
        if (fields && fields->isFieldPresent(r::sfNFTokenID)) out.insert(raw_bytes(fields->getFieldH256(r::sfNFTokenID)));
    };
    switch (tx->getTxnType()) {
    case r::ttNFTOKEN_MINT: {
        std::set<Bytes> before, after;
        auto tokens = [](r::STObject const* fields, std::set<Bytes>& ids) {
            if (fields && fields->isFieldPresent(r::sfNFTokens))
                for (auto const& token : fields->getFieldArray(r::sfNFTokens)) ids.insert(raw_bytes(token.getFieldH256(r::sfNFTokenID)));
        };
        for (auto const* node : nodes) {
            if (type_of(*node) != r::ltNFTOKEN_PAGE) continue;
            if (node->getFName() == r::sfCreatedNode) tokens(inner(*node, r::sfNewFields), after);
            else if (auto previous = inner(*node, r::sfPreviousFields); previous && previous->isFieldPresent(r::sfNFTokens)) {
                tokens(previous, before);
                tokens(inner(*node, r::sfFinalFields), after);
            }
        }
        for (auto const& id : after) if (!before.contains(id)) { out.insert(id); break; }
        break;
    }
    case r::ttNFTOKEN_BURN:
    case r::ttNFTOKEN_CREATE_OFFER:
    case r::ttNFTOKEN_MODIFY:
        token_in(&*tx);
        break;
    case r::ttNFTOKEN_ACCEPT_OFFER: {
        auto const& field = tx->isFieldPresent(r::sfNFTokenBuyOffer) ? r::sfNFTokenBuyOffer : r::sfNFTokenSellOffer;
        if (!tx->isFieldPresent(field)) break;
        auto offer = tx->getFieldH256(field);
        for (auto const* node : nodes)
            if (node->isFieldPresent(r::sfLedgerIndex) && node->getFieldH256(r::sfLedgerIndex) == offer) { token_in(inner(*node, r::sfFinalFields)); break; }
        break;
    }
    case r::ttNFTOKEN_CANCEL_OFFER:
        for (auto const* node : nodes) if (type_of(*node) == r::ltNFTOKEN_OFFER) token_in(inner(*node, r::sfFinalFields));
        break;
    default:
        break;
    }
    return out;
}

namespace {
std::string uri_of(r::STObject const& token) { return token.isFieldPresent(r::sfURI) ? r::strHex(token.getFieldVL(r::sfURI)) : ""; }
}
// A token on an NFTokenPage after the transaction wins over one that left a page, which is a burn when it landed on none. A deleted page's
// tokens before the transaction are its PreviousFields' when it has them (as Clio's
// getNFTokenBurnData reads it), with its FinalFields': a burn that leaves its page short
// writes the page without the token, then merges it into the next page and deletes it, so the
// token is in the deleted page's PreviousFields alone. Read from FinalFields only, such a burn
// moved no token, and the token's state stayed the one before it.
std::map<Bytes, NftState> nft_changes(TransactionRecord const& record) {
    std::optional<r::STObject> decoded;
    try {
        r::SerialIter meta_iter(record.metadata.data(), record.metadata.size());
        decoded.emplace(meta_iter, r::sfMetadata);
    } catch (std::exception const&) { return {}; }  // metadata this libxrpl cannot decode (some of 2013): no page changes
    auto const& meta = *decoded;
    std::map<Bytes, NftState> before, after;
    auto add = [&](r::STObject const* fields, r::uint256 const& page, std::map<Bytes, NftState>& out) {
        if (!fields || !fields->isFieldPresent(r::sfNFTokens)) return;
        auto owner = r::toBase58(r::AccountID::fromVoid(page.data()));
        for (auto const& token : fields->getFieldArray(r::sfNFTokens))
            out[raw_bytes(token.getFieldH256(r::sfNFTokenID))] = NftState{owner, uri_of(token), false};
    };
    if (meta.isFieldPresent(r::sfAffectedNodes)) for (auto const& node : meta.getFieldArray(r::sfAffectedNodes)) {
        if (!node.isFieldPresent(r::sfLedgerEntryType) || node.getFieldU16(r::sfLedgerEntryType) != r::ltNFTOKEN_PAGE) continue;
        auto inner = [&](r::SField const& f) -> r::STObject const* {
            return node.isFieldPresent(f) ? &static_cast<r::STObject const&>(node.peekAtField(f)) : nullptr;
        };
        auto page = node.getFieldH256(r::sfLedgerIndex);
        auto const& kind = node.getFName();
        if (kind == r::sfCreatedNode) add(inner(r::sfNewFields), page, after);
        else if (kind == r::sfDeletedNode) { add(inner(r::sfFinalFields), page, before); add(inner(r::sfPreviousFields), page, before); }
        else if (auto previous = inner(r::sfPreviousFields); previous && previous->isFieldPresent(r::sfNFTokens)) {
            add(previous, page, before);
            add(inner(r::sfFinalFields), page, after);
        }
    }
    for (auto& [id, state] : before) if (!after.contains(id)) { state.burned = true; after.emplace(id, std::move(state)); }
    return after;
}
std::optional<NftState> nft_change(TransactionRecord const& record, Bytes const& id) {
    auto changes = nft_changes(record);
    if (auto it = changes.find(id); it != changes.end()) return it->second;
    return std::nullopt;
}
Bytes nft_fingerprint(NftState const& state) {
    auto burned = state.burned ? '1' : '0';
    return hx::sha(state.owner + '\0' + burned).substr(0, 8) + hx::sha(state.owner + '\0' + state.uri + '\0' + burned).substr(0, 8);
}
std::vector<std::pair<Bytes, std::string>> page_tokens(std::string_view page) {
    r::SerialIter iter(page.data(), page.size());
    r::STObject object(iter, r::sfLedgerEntry);
    std::vector<std::pair<Bytes, std::string>> out;
    if (object.getFieldU16(r::sfLedgerEntryType) != r::ltNFTOKEN_PAGE) throw Error("Object is not an NFTokenPage");
    if (object.isFieldPresent(r::sfNFTokens))
        for (auto const& token : object.getFieldArray(r::sfNFTokens)) out.emplace_back(raw_bytes(token.getFieldH256(r::sfNFTokenID)), uri_of(token));
    return out;
}
std::optional<std::pair<Bytes, Bytes>> mptoken_holder(std::string_view object) {
    // LedgerEntryType is always the first field: 0x11, then the type big-endian.
    if (object.size() < 3 || object[0] != 0x11 || object[1] != char(r::ltMPTOKEN >> 8) || object[2] != char(r::ltMPTOKEN & 255)) return std::nullopt;
    r::SerialIter iter(object.data(), object.size());
    r::STObject token(iter, r::sfLedgerEntry);
    return std::pair{raw_bytes(token.getFieldH192(r::sfMPTokenIssuanceID)), raw_bytes(token.getAccountID(r::sfAccount))};
}
std::pair<Bytes, Bytes> nft_issuer_entry(Bytes const& id) {
    if (id.size() != 32) throw Error("Invalid NFToken ID");
    auto token = r::uint256::fromVoid(id.data());
    return {id.substr(4, 20), issuer_entry(id, r::nft::toUInt32(r::nft::getTaxon(token)))};
}

namespace {
// Run entries compare as bytes: key, big-endian ledger offset, then TransactionIndex
// and record position (postings) or the deleted flag (object versions).
constexpr std::size_t account_entry = 20 + 12, nft_entry = 32 + 12, object_entry = 32 + 5, nft_state_entry = 32 + 8 + 16;
template<std::size_t N> using Entry = std::array<char, N>;

// Sorted runs are written as one zstd frame each: sorted keys and small offsets
// compress several-fold, which keeps a 1M-ledger build's temporary space small.
template<std::size_t N> void write_run(fs::path const& path, std::vector<Entry<N>>& entries) {
    static_assert(sizeof(Entry<N>) == N);
    std::sort(entries.begin(), entries.end(), [](auto const& a, auto const& b) { return std::memcmp(a.data(), b.data(), N) < 0; });
    auto size = entries.size() * N;
    Bytes compressed(ZSTD_compressBound(size), '\0');
    auto n = ZSTD_compress(compressed.data(), compressed.size(), entries.data(), size, 1);
    if (ZSTD_isError(n)) throw Error("Cannot compress index run");
    // Closed and checked: a small run is still in the stream's buffer after write(), and a
    // failure flushing it (a full disk) would otherwise leave a short file read as a whole run.
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(compressed.data(), static_cast<std::streamsize>(n));
    out.close();
    if (!out) throw Error("Cannot write index run " + path.string());
    std::vector<Entry<N>>().swap(entries);
}

template<std::size_t N> class RunReader {
    std::ifstream in_;
    std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> stream_{ZSTD_createDStream(), ZSTD_freeDStream};
    Bytes input_, buffer_;
    ZSTD_inBuffer in_buffer_{nullptr, 0, 0};
    std::size_t at_ = 0, have_ = 0;
    bool ended_ = false, framed_ = false;  // framed_: the last decompress call ended the frame
    void fill() {
        // Keep any partial entry, then decompress until the buffer holds whole entries or the frame ends.
        std::size_t keep = have_ - std::min(at_, have_);
        std::memmove(buffer_.data(), buffer_.data() + at_, keep);
        have_ = keep; at_ = 0;
        while (have_ < buffer_.size() && !ended_) {
            if (in_buffer_.pos == in_buffer_.size) {
                in_.read(input_.data(), static_cast<std::streamsize>(input_.size()));
                in_buffer_ = {input_.data(), static_cast<std::size_t>(in_.gcount()), 0};
                if (!in_buffer_.size) {
                    // The file ends: whole only where its frame did (a cut one can still hold whole entries).
                    if (!framed_) throw Error("Truncated index run");
                    ended_ = true; break;
                }
            }
            ZSTD_outBuffer out{buffer_.data(), buffer_.size(), have_};
            auto rc = ZSTD_decompressStream(stream_.get(), &out, &in_buffer_);
            if (ZSTD_isError(rc)) throw Error("Corrupt index run");
            have_ = out.pos;
            framed_ = rc == 0;
            if (rc == 0 && in_buffer_.pos == in_buffer_.size && in_.peek() == EOF) ended_ = true;
        }
        if (ended_ && have_ % N) throw Error("Truncated index run");
    }
public:
    explicit RunReader(fs::path const& path) : in_(path, std::ios::binary), input_(1 << 16, '\0'), buffer_(N * 4096, '\0') {
        if (!in_ || !stream_) throw Error("Cannot open index run");
        ZSTD_initDStream(stream_.get());
        fill();
    }
    bool valid() const { return at_ < have_; }
    char const* current() const { return buffer_.data() + at_; }
    void next() { at_ += N; if (at_ >= have_) fill(); }
};

struct TableOut { Bytes directory; fs::path records; U keys = 0, entries = 0, record_bytes = 0; };

// K-way merge of sorted runs into one keyed table; `encode` turns one key's entries into its payload.
template<std::size_t N, class Encode>
TableOut merge_runs(std::vector<fs::path> const& runs, unsigned width, fs::path const& records, Encode encode) {
    std::vector<std::unique_ptr<RunReader<N>>> readers;
    // Every run is one frame (an empty one too), so an empty file is a run that was not written.
    for (auto const& path : runs) {
        if (!fs::file_size(path)) throw Error("Truncated index run " + path.string());
        readers.push_back(std::make_unique<RunReader<N>>(path));
    }
    auto after = [&](std::size_t a, std::size_t b) {
        int c = std::memcmp(readers[a]->current(), readers[b]->current(), N);
        return c ? c > 0 : a > b;
    };
    std::priority_queue<std::size_t, std::vector<std::size_t>, decltype(after)> heap(after);
    for (std::size_t i = 0; i < readers.size(); ++i) if (readers[i]->valid()) heap.push(i);
    TableOut table{{}, records};
    std::ofstream out(records, std::ios::binary | std::ios::trunc);
    Bytes key, group, record;
    auto flush = [&] {
        if (key.empty()) return;
        if (table.keys % block_keys == 0) { table.directory += key; put(table.directory, table.record_bytes, 8); }
        auto payload = encode(V(group));
        record = key; hx::vu(record, group.size() / N); hx::vu(record, payload.size()); record += payload;
        out.write(record.data(), static_cast<std::streamsize>(record.size()));
        table.record_bytes += record.size(); table.entries += group.size() / N; ++table.keys; group.clear();
    };
    while (!heap.empty()) {
        auto i = heap.top(); heap.pop();
        auto entry = readers[i]->current();
        if (key.empty() || std::memcmp(key.data(), entry, width)) { flush(); key.assign(entry, width); }
        group.append(entry, N);
        readers[i]->next();
        if (readers[i]->valid()) heap.push(i);
    }
    flush();
    if (!out.flush()) throw Error("Cannot write query table");
    return table;
}

Bytes encode_postings(V group, unsigned width) {
    Bytes out; U previous = 0, index = 0; std::size_t n = width + 12;
    for (std::size_t at = 0; at < group.size(); at += n) {
        U ledger = be32(group.data() + at + width), value = be32(group.data() + at + width + 4), position = be32(group.data() + at + width + 8);
        U delta = ledger - previous;
        if (at && !delta && value <= index) throw Error("Duplicate posting");
        hx::vu(out, delta); hx::vu(out, delta || !at ? value : value - index); hx::vu(out, position);
        previous = ledger; index = value;
    }
    return out;
}
Bytes encode_versions(V group) {
    Bytes out; U previous = 0;
    for (std::size_t at = 0; at < group.size(); at += object_entry) {
        U ledger = be32(group.data() + at + 32);
        if (at && ledger <= previous) throw Error("Duplicate object version");
        hx::vu(out, ((ledger - previous) << 1) | U(group[at + 36] != 0));
        previous = ledger;
    }
    return out;
}

Bytes encode_nft_states(V group) {
    Bytes out; U previous = 0;
    for (std::size_t at = 0; at < group.size(); at += nft_state_entry) {
        U ledger = be32(group.data() + at + 32);
        if (at && ledger <= previous) throw Error("Duplicate NFT state");
        hx::vu(out, ledger - previous); hx::vu(out, be32(group.data() + at + 36)); out.append(group.data() + at + 40, 16);
        previous = ledger;
    }
    return out;
}

struct LedgerRow { Bytes header, order; };

// Rows of the optional indexes (piece_format.hpp's XQOX): the key, then one entry.
constexpr std::size_t holder_row = 24 + 20, issuer_row = 20 + 16;
template<std::size_t N> using Row = std::array<char, N>;
Row<holder_row> make_holder_row(Bytes const& issuance, Bytes const& holder) {
    Row<holder_row> row; std::memcpy(row.data(), issuance.data(), 24); std::memcpy(row.data() + 24, holder.data(), 20); return row;
}
Row<issuer_row> make_issuer_row(Bytes const& id) {
    auto [issuer, entry] = nft_issuer_entry(id);
    Row<issuer_row> row; std::memcpy(row.data(), issuer.data(), 20); std::memcpy(row.data() + 20, entry.data(), 16); return row;
}
char const* optional_name(Optional kind) { return kind == Optional::mpt_holders ? "mpt_holders" : "nfts_by_issuer"; }

// Writes an XQOX file from its rows (in any order; repeats are dropped).
template<std::size_t N> Json write_optional(fs::path const& target, Optional kind, U ledger, Bytes const& source_sha256, std::vector<Row<N>>& rows) {
    auto started = hx::Clock::now();
    std::size_t width = optional_width(kind), entry = optional_entry(kind);
    if (width + entry != N) throw Error("Optional index row width");
    std::sort(rows.begin(), rows.end(), [](auto const& a, auto const& b) { return std::memcmp(a.data(), b.data(), N) < 0; });
    rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
    Bytes directory, records;
    U keys = 0;
    for (std::size_t i = 0; i < rows.size();) {
        auto j = i;
        while (j < rows.size() && !std::memcmp(rows[j].data(), rows[i].data(), width)) ++j;
        if (keys % block_keys == 0) { directory.append(rows[i].data(), width); put(directory, records.size(), 8); }
        records.append(rows[i].data(), width); hx::vu(records, j - i); hx::vu(records, (j - i) * entry);
        for (auto k = i; k < j; ++k) records.append(rows[k].data() + width, entry);
        ++keys; i = j;
    }
    Bytes head("XQOX", 4);
    put(head, optional_version(kind), 2); put(head, static_cast<unsigned>(kind), 2); put(head, ledger, 4); head += source_sha256;
    put(head, keys, 8); put(head, (keys + block_keys - 1) / block_keys, 8); put(head, width, 4); put(head, block_keys, 4);
    auto partial = fs::path(target.string() + ".part");
    {
        std::ofstream out(partial, std::ios::binary | std::ios::trunc);
        crypto_hash_sha256_state state; crypto_hash_sha256_init(&state);
        for (auto const* part : {&head, &directory, &records}) {
            out.write(part->data(), static_cast<std::streamsize>(part->size()));
            crypto_hash_sha256_update(&state, reinterpret_cast<unsigned char const*>(part->data()), part->size());
        }
        unsigned char digest[32]; crypto_hash_sha256_final(&state, digest);
        out.write(reinterpret_cast<char const*>(digest), 32);
        if (!out.flush()) throw Error("Cannot write optional index " + target.string());
    }
    durable_rename(partial, target);
    OptionalIndex check(target, kind, source_sha256);
    Json result{{"index", optional_name(kind)}, {"status", "built"}, {"keys", keys}, {"entries", rows.size()},
                {"bytes", fs::file_size(target)}, {"seconds", hx::elapsed(started)}};
    std::vector<Row<N>>().swap(rows);
    return result;
}
template<std::size_t N> std::vector<Row<N>> joined(std::vector<std::vector<Row<N>>>& parts) {
    std::vector<Row<N>> rows;
    for (auto& part : parts) { rows.insert(rows.end(), part.begin(), part.end()); std::vector<Row<N>>().swap(part); }
    return rows;
}

// The optional indexes a range's decode can fill as it goes; empty paths are not wanted.
struct OptionalTargets { fs::path holders, issuers; };

Json build_index(fs::path const& container_path, Json const& range, fs::path const& target, unsigned jobs, OptionalTargets const& optional = {}) {
    auto started = hx::Clock::now();
    Mapped file(container_path);
    auto data = file.view();
    auto container = Container::parse(data, range.at("directory_sha256").get<std::string>());
    U first = container.first, count = container.last - container.first + 1;
    auto work = target.parent_path() / ("tmp-" + target.stem().string());
    fs::remove_all(work); fs::create_directories(work);
    std::vector<LedgerRow> rows(count);
    std::vector<std::vector<Row<holder_row>>> holder_rows(container.segments.size());
    std::vector<std::vector<Row<issuer_row>>> issuer_rows(container.segments.size());
    bool want_holders = !optional.holders.empty(), want_issuers = !optional.issuers.empty();
    std::atomic<std::size_t> next{0};
    std::atomic<U> transactions{0};
    std::exception_ptr failure; std::mutex failed;
    auto worker = [&] {
        try {
            for (std::size_t s; (s = next++) < container.segments.size();) {
                auto const& segment = container.segments[s];
                auto bytes = data.substr(segment.offset, segment.length);
                if (hx::sha(bytes) != segment.digest) throw Error("Segment checksum differs");
                hx::Archive archive(bytes, true);  // its bytes checked just now: not hashed again
                if (archive.first != segment.first || archive.count != segment.count) throw Error("Segment range differs from its container directory");
                std::vector<Entry<account_entry>> accounts; std::vector<Entry<nft_entry>> nfts; std::vector<Entry<object_entry>> objects;
                std::vector<Entry<nft_state_entry>> nft_states;
                for (U page = 0; page < archive.pages(); ++page) {
                    for (auto const& [seq, raw] : archive.decodePage(page)) {
                        auto record = parse_ledger_record(raw);
                        if (seq < first || seq > container.last || record.sequence != seq) throw Error("Segment ledger outside container");
                        auto offset = static_cast<std::uint32_t>(seq - first);
                        auto& row = rows[offset];
                        row.header = record.header;
                        auto index = index_transactions(record);
                        std::vector<std::pair<std::uint32_t, std::uint32_t>> order;
                        // Per token, the state left by the ledger's last transaction (in TransactionIndex order) that moved it.
                        std::map<Bytes, std::tuple<std::uint32_t, std::uint32_t, Bytes>> moved;
                        for (std::uint32_t pos = 0; pos < index.size(); ++pos) {
                            auto const& tx = index[pos];
                            order.emplace_back(tx.position, pos);
                            for (auto const& account : tx.accounts) {
                                Entry<account_entry> e; std::memcpy(e.data(), account.data(), 20);
                                put_be32(e.data() + 20, offset); put_be32(e.data() + 24, tx.position); put_be32(e.data() + 28, pos); accounts.push_back(e);
                            }
                            auto ids = nft_ids(record.transactions[pos]);
                            if (want_issuers) for (auto const& id : ids) issuer_rows[s].push_back(make_issuer_row(id));
                            auto changes = ids.empty() ? std::map<Bytes, NftState>() : nft_changes(record.transactions[pos]);
                            for (auto const& id : ids) {
                                Entry<nft_entry> e; std::memcpy(e.data(), id.data(), 32);
                                put_be32(e.data() + 32, offset); put_be32(e.data() + 36, tx.position); put_be32(e.data() + 40, pos); nfts.push_back(e);
                                if (auto state = changes.find(id); state != changes.end()) {
                                    auto [slot, fresh] = moved.try_emplace(id, tx.position, pos, nft_fingerprint(state->second));
                                    if (!fresh && std::get<0>(slot->second) < tx.position) slot->second = {tx.position, pos, nft_fingerprint(state->second)};
                                }
                            }
                        }
                        std::sort(order.begin(), order.end());
                        bool identity = true;
                        for (std::size_t i = 0; i < order.size(); ++i) {
                            if (i && order[i].first == order[i - 1].first) throw Error("Duplicate TransactionIndex");
                            identity = identity && order[i].second == i;
                        }
                        row.order.clear(); hx::vu(row.order, identity ? 0 : 1);
                        if (!identity) for (auto const& item : order) hx::vu(row.order, item.second);
                        for (auto const& patch : record.patches) {
                            Entry<object_entry> e; std::memcpy(e.data(), patch.key.data(), 32);
                            put_be32(e.data() + 32, offset); e[36] = patch.object.empty() ? 1 : 0; objects.push_back(e);
                            if (want_holders && !patch.object.empty())
                                if (auto holder = mptoken_holder(patch.object)) holder_rows[s].push_back(make_holder_row(holder->first, holder->second));
                        }
                        for (auto const& [id, change] : moved) {
                            Entry<nft_state_entry> e; std::memcpy(e.data(), id.data(), 32); put_be32(e.data() + 32, offset);
                            put_be32(e.data() + 36, std::get<1>(change)); std::memcpy(e.data() + 40, std::get<2>(change).data(), 16); nft_states.push_back(e);
                        }
                        transactions += record.transactions.size();
                    }
                    archive.releasePage(page);
                }
                file.release(bytes);
                auto name = std::to_string(s);
                write_run(work / ("accounts-" + name), accounts);
                write_run(work / ("nfts-" + name), nfts);
                write_run(work / ("objects-" + name), objects);
                write_run(work / ("nft-states-" + name), nft_states);
            }
        } catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); next = container.segments.size(); }
    };
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < std::max(1u, jobs); ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();
    if (failure) std::rethrow_exception(failure);
    if (transactions != container.transactions) throw Error("Container transaction count differs");
    auto decoded = hx::elapsed(started);

    std::vector<fs::path> account_runs, nft_runs, object_runs, state_runs;
    for (std::size_t s = 0; s < container.segments.size(); ++s) {
        auto name = std::to_string(s);
        account_runs.push_back(work / ("accounts-" + name)); nft_runs.push_back(work / ("nfts-" + name)); object_runs.push_back(work / ("objects-" + name));
        state_runs.push_back(work / ("nft-states-" + name));
    }
    std::array<TableOut, 4> tables{
        merge_runs<account_entry>(account_runs, 20, work / "accounts.records", [](V g) { return encode_postings(g, 20); }),
        merge_runs<nft_entry>(nft_runs, 32, work / "nfts.records", [](V g) { return encode_postings(g, 32); }),
        merge_runs<object_entry>(object_runs, 32, work / "objects.records", encode_versions),
        merge_runs<nft_state_entry>(state_runs, 32, work / "nft-states.records", encode_nft_states)};
    for (auto const* runs : {&account_runs, &nft_runs, &object_runs, &state_runs}) for (auto const& p : *runs) fs::remove(p);

    Bytes headers, hashes, order;
    std::vector<std::pair<U, U>> by_hash;
    headers.reserve(count * 118);
    for (U i = 0; i < count; ++i) {
        auto const& h = rows[i].header;
        if (h.size() != 118) throw Error("Ledger header missing from container");
        headers += h;
        auto hash = hx::half(Bytes("LWR\0", 4) + h);
        U prefix = 0; for (unsigned b = 0; b < 8; ++b) prefix = (prefix << 8) | static_cast<unsigned char>(hash[b]);
        by_hash.emplace_back(prefix, i);
    }
    std::sort(by_hash.begin(), by_hash.end());
    for (auto const& [prefix, i] : by_hash) { put(hashes, prefix, 8); put(hashes, i, 4); }
    U at = 0;
    for (U i = 0; i < count; ++i) { put(order, at, 8); at += rows[i].order.size(); }
    put(order, at, 8);
    for (auto const& row : rows) order += row.order;
    std::vector<LedgerRow>().swap(rows);

    // Assemble: head, in-memory sections, then each table's header, directory and records.
    Bytes table_heads[4];
    for (unsigned i = 0; i < 4; ++i) {
        auto& t = tables[i];
        put(table_heads[i], t.keys, 8); put(table_heads[i], (t.keys + block_keys - 1) / block_keys, 8);
        put(table_heads[i], i == 0 ? 20 : 32, 4); put(table_heads[i], block_keys, 4);
        table_heads[i] += t.directory;
    }
    U lengths[query_sections] = {headers.size(), hashes.size(), order.size(),
        table_heads[0].size() + tables[0].record_bytes, table_heads[1].size() + tables[1].record_bytes, table_heads[2].size() + tables[2].record_bytes,
        table_heads[3].size() + tables[3].record_bytes};
    Bytes head("XQIX", 4);
    put(head, query_version, 2); put(head, query_sections, 2); put(head, first, 4); put(head, count, 4);
    head += from_hex(range.at("sha256").get<std::string>());
    U offset = query_head;
    for (auto length : lengths) { put(head, offset, 8); put(head, length, 8); offset += length; }
    auto partial = fs::path(target.string() + ".part");
    std::ofstream out(partial, std::ios::binary | std::ios::trunc);
    crypto_hash_sha256_state state; crypto_hash_sha256_init(&state);
    auto emit = [&](V bytes) {
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        crypto_hash_sha256_update(&state, reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size());
    };
    emit(head); emit(headers); emit(hashes); emit(order);
    Bytes buffer(8 << 20, '\0');
    for (unsigned i = 0; i < 4; ++i) {
        emit(table_heads[i]);
        std::ifstream in(tables[i].records, std::ios::binary);
        while (in) {
            in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            emit(V(buffer.data(), static_cast<std::size_t>(in.gcount())));
        }
    }
    unsigned char digest[32]; crypto_hash_sha256_final(&state, digest);
    out.write(reinterpret_cast<char const*>(digest), 32);
    if (!out.flush()) throw Error("Cannot write query index");
    out.close();
    durable_rename(partial, target);
    fs::remove_all(work);
    auto source = from_hex(range.at("sha256").get<std::string>());
    QueryIndex check(target, source);
    Json built = Json::array();
    if (want_holders) { auto list = joined(holder_rows); built.push_back(write_optional(optional.holders, Optional::mpt_holders, first, source, list)); }
    if (want_issuers) { auto list = joined(issuer_rows); built.push_back(write_optional(optional.issuers, Optional::nfts_by_issuer, first, source, list)); }
    return {{"object", range.at("object")}, {"status", "built"}, {"optional", built}, {"ledgers", count}, {"transactions", transactions.load()},
            {"bytes", fs::file_size(target)}, {"decode_seconds", decoded}, {"seconds", hx::elapsed(started)},
            {"sections", {{"headers", lengths[0]}, {"ledger_hashes", lengths[1]}, {"tx_order", lengths[2]},
                          {"accounts", lengths[3]}, {"nfts", lengths[4]}, {"objects", lengths[5]}, {"nft_states", lengths[6]}}},
            {"account_keys", tables[0].keys}, {"account_postings", tables[0].entries}, {"nft_keys", tables[1].keys},
            {"nft_postings", tables[1].entries}, {"object_keys", tables[2].keys}, {"object_versions", tables[2].entries},
            {"nft_state_keys", tables[3].keys}, {"nft_state_changes", tables[3].entries}};
}
}

namespace {
constexpr std::size_t state_block = 32 << 10;
Bytes decompress_part(fs::path const& path, Json const& part) {
    auto compressed = read_file(path, 1ULL << 32);
    if (hx::hex(hx::sha(compressed)) != part.at("sha256").get<std::string>()) throw Error("Snapshot part differs from its index");
    auto size = ZSTD_getFrameContentSize(compressed.data(), compressed.size());
    if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN || size > (1ULL << 32)) throw Error("Snapshot part size");
    Bytes raw(size, '\0');
    auto n = ZSTD_decompress(raw.data(), raw.size(), compressed.data(), compressed.size());
    if (ZSTD_isError(n) || n != size) throw Error("Snapshot part decode failed");
    return raw;
}
// Record-aligned blocks of about state_block bytes: (offset, length, records) within one part.
std::vector<std::array<U, 3>> cut_blocks(V raw, U expected_records) {
    std::vector<std::array<U, 3>> blocks;
    U at = 0, start = 0, records = 0, total = 0;
    while (at < raw.size()) {
        if (raw.size() - at < 36) throw Error("Truncated snapshot record");
        U length = le(raw, at + 32, 4);
        if (length > raw.size() - at - 36) throw Error("Truncated snapshot record");
        at += 36 + length; ++records; ++total;
        if (at - start >= state_block) { blocks.push_back({start, at - start, records}); start = at; records = 0; }
    }
    if (records) blocks.push_back({start, at - start, records});
    if (total != expected_records) throw Error("Snapshot part record count differs");
    return blocks;
}

Json build_state(fs::path const& cache, Json const& snapshot, fs::path const& target, unsigned jobs) {
    auto started = hx::Clock::now();
    auto index_bytes = read_file(cache / snapshot.at("index").at("object").get<std::string>(), 16 << 20);
    if (hx::hex(hx::sha(index_bytes)) != snapshot.at("index").at("sha256").get<std::string>()) throw Error("Snapshot index differs from catalog");
    auto index = Json::parse(index_bytes);
    std::vector<Json> parts;
    for (auto const& part : index.at("parts")) if (part.at("records").get<U>()) parts.push_back(part);
    auto path_of = [&](Json const& part) { return cache / part.at("object").get<std::string>(); };
    // Train one dictionary on blocks sampled evenly across the snapshot.
    Bytes samples; std::vector<std::size_t> sizes;
    std::size_t sampled_parts = std::min<std::size_t>(parts.size(), 48);
    for (std::size_t k = 0; k < sampled_parts; ++k) {
        auto const& part = parts[k * parts.size() / sampled_parts];
        auto raw = decompress_part(path_of(part), part);
        auto blocks = cut_blocks(raw, part.at("records"));
        for (std::size_t b = 0; b < blocks.size() && samples.size() < (96u << 20); b += 3) {
            samples.append(raw, blocks[b][0], blocks[b][1]); sizes.push_back(blocks[b][1]);
        }
    }
    Bytes dictionary(112640, '\0');
    auto dictionary_size = sizes.empty() ? 0 : ZDICT_trainFromBuffer(dictionary.data(), dictionary.size(), samples.data(), sizes.data(), static_cast<unsigned>(sizes.size()));
    if (sizes.empty() || ZDICT_isError(dictionary_size)) {
        // Too little data to train on (tiny snapshots): an empty-content dictionary still frames blocks.
        dictionary = Bytes(8, '\0'); dictionary_size = dictionary.size();
    }
    dictionary.resize(dictionary_size);
    Bytes().swap(samples);
    std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> cdict(ZSTD_createCDict(dictionary.data(), dictionary.size(), 12), ZSTD_freeCDict);
    if (!cdict) throw Error("Cannot build snapshot dictionary");
    // Compress each part's blocks in parallel into its own run file; rows keep block order.
    auto work = target.parent_path() / ("tmp-" + target.stem().string());
    fs::remove_all(work); fs::create_directories(work);
    struct Row { Bytes first; U compressed, raw, records; };
    std::vector<std::vector<Row>> rows(parts.size());
    std::vector<Bytes> last_keys(parts.size()), first_keys(parts.size());
    std::atomic<std::size_t> next{0}; std::exception_ptr failure; std::mutex failed;
    auto worker = [&] {
        try {
            std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(ZSTD_createCCtx(), ZSTD_freeCCtx);
            for (std::size_t p; (p = next++) < parts.size();) {
                auto raw = decompress_part(path_of(parts[p]), parts[p]);
                std::ofstream out(work / std::to_string(p), std::ios::binary | std::ios::trunc);
                Bytes compressed; V view(raw); Bytes previous;
                for (auto const& [offset, length, records] : cut_blocks(raw, parts[p].at("records"))) {
                    compressed.resize(ZSTD_compressBound(length));
                    auto n = ZSTD_compress_usingCDict(ctx.get(), compressed.data(), compressed.size(), raw.data() + offset, length, cdict.get());
                    if (ZSTD_isError(n)) throw Error("Snapshot block compression failed");
                    out.write(compressed.data(), static_cast<std::streamsize>(n));
                    rows[p].push_back({Bytes(view.substr(offset, 32)), n, length, records});
                }
                // Keys must ascend within and across parts.
                for (U at = 0; at < raw.size();) {
                    auto key = view.substr(at, 32);
                    if (!previous.empty() && key <= V(previous)) throw Error("Snapshot keys out of order");
                    previous = Bytes(key); at += 36 + le(view, at + 32, 4);
                }
                first_keys[p] = rows[p].front().first; last_keys[p] = previous;
                if (!out.flush()) throw Error("Cannot write snapshot state run");
            }
        } catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); next = parts.size(); }
    };
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < std::max(1u, jobs); ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();
    if (failure) std::rethrow_exception(failure);
    for (std::size_t p = 1; p < parts.size(); ++p) if (first_keys[p] <= last_keys[p - 1]) throw Error("Snapshot parts out of order");

    U objects = 0, blocks = 0, offset = 0, raw_bytes = 0;
    Bytes directory;
    for (auto const& part_rows : rows) for (auto const& row : part_rows) {
        directory += row.first; put(directory, offset, 8); put(directory, row.compressed, 4); put(directory, row.raw, 4); put(directory, row.records, 4);
        offset += row.compressed; objects += row.records; raw_bytes += row.raw; ++blocks;
    }
    Bytes head("XQSS", 4);
    put(head, 1, 2); put(head, 0, 2); put(head, snapshot.at("seq").get<U>(), 4); put(head, objects, 8); put(head, blocks, 8);
    head += hx::sha(index_bytes); put(head, dictionary.size(), 8);
    auto partial = fs::path(target.string() + ".part");
    std::ofstream out(partial, std::ios::binary | std::ios::trunc);
    crypto_hash_sha256_state state; crypto_hash_sha256_init(&state);
    auto emit = [&](V bytes) {
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        crypto_hash_sha256_update(&state, reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size());
    };
    emit(head); emit(dictionary); emit(directory);
    Bytes buffer(8 << 20, '\0');
    for (std::size_t p = 0; p < parts.size(); ++p) {
        std::ifstream in(work / std::to_string(p), std::ios::binary);
        while (in) { in.read(buffer.data(), static_cast<std::streamsize>(buffer.size())); emit(V(buffer.data(), static_cast<std::size_t>(in.gcount()))); }
    }
    unsigned char digest[32]; crypto_hash_sha256_final(&state, digest);
    out.write(reinterpret_cast<char const*>(digest), 32);
    if (!out.flush()) throw Error("Cannot write snapshot state index");
    out.close();
    durable_rename(partial, target);
    fs::remove_all(work);
    StateIndex check(target, hx::sha(index_bytes));
    if (blocks) (void)check.load(blocks - 1);
    return {{"snapshot", snapshot.at("seq")}, {"status", "built"}, {"objects", objects}, {"blocks", blocks}, {"raw_bytes", raw_bytes},
            {"bytes", fs::file_size(target)}, {"dictionary_bytes", dictionary.size()}, {"seconds", hx::elapsed(started)}};
}
// One pass over the snapshot's parts for the maps it lacks (empty paths are not wanted):
// the owner of every NFToken (XQNF, for nft_info), read from the NFTokenPages whose keys
// begin with the owner's AccountID, and the optional MPT holder and NFTs-by-issuer indexes.
struct SnapshotTargets { fs::path owners, holders, issuers; };
Json build_snapshot_maps(fs::path const& cache, Json const& snapshot, SnapshotTargets const& targets, unsigned jobs) {
    auto started = hx::Clock::now();
    auto index_bytes = read_file(cache / snapshot.at("index").at("object").get<std::string>(), 16 << 20);
    if (hx::hex(hx::sha(index_bytes)) != snapshot.at("index").at("sha256").get<std::string>()) throw Error("Snapshot index differs from catalog");
    auto index = Json::parse(index_bytes);
    std::vector<Json> parts;
    for (auto const& part : index.at("parts")) if (part.at("records").get<U>()) parts.push_back(part);
    using OwnerRow = std::array<char, nft_row>;
    static_assert(sizeof(OwnerRow) == nft_row);
    bool owners = !targets.owners.empty(), holders = !targets.holders.empty(), issuers = !targets.issuers.empty();
    std::vector<std::vector<OwnerRow>> found(parts.size());
    std::vector<std::vector<Row<holder_row>>> holder_rows(parts.size());
    std::vector<std::vector<Row<issuer_row>>> issuer_rows(parts.size());
    std::atomic<std::size_t> next{0}; std::exception_ptr failure; std::mutex failed;
    auto worker = [&] {
        try {
            for (std::size_t p; (p = next++) < parts.size();) {
                auto raw = decompress_part(cache / parts[p].at("object").get<std::string>(), parts[p]);
                V view(raw);
                for (U at = 0; at < view.size();) {
                    if (view.size() - at < 36) throw Error("Truncated snapshot record");
                    auto key = view.substr(at, 32); U length = le(view, at + 32, 4);
                    if (length > view.size() - at - 36) throw Error("Truncated snapshot record");
                    auto value = view.substr(at + 36, length); at += 36 + length;
                    // LedgerEntryType is always the first field: 0x11, then the type big-endian.
                    if (value.size() >= 3 && value[0] == 0x11 && value[1] == char(r::ltNFTOKEN_PAGE >> 8) && value[2] == char(r::ltNFTOKEN_PAGE & 255)) {
                        if (!owners && !issuers) continue;
                        for (auto const& [id, uri] : page_tokens(value)) {
                            if (owners) { OwnerRow row; std::memcpy(row.data(), id.data(), 32); std::memcpy(row.data() + 32, key.data(), 20); found[p].push_back(row); }
                            if (issuers) issuer_rows[p].push_back(make_issuer_row(id));
                        }
                    } else if (holders) {
                        if (auto holder = mptoken_holder(value)) holder_rows[p].push_back(make_holder_row(holder->first, holder->second));
                    }
                }
            }
        } catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); next = parts.size(); }
    };
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < std::max(1u, jobs); ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();
    if (failure) std::rethrow_exception(failure);
    auto source = hx::sha(index_bytes);
    auto seq = snapshot.at("seq").get<U>();
    Json results = Json::array();
    if (owners) {
        std::vector<OwnerRow> rows;
        for (auto& part : found) { rows.insert(rows.end(), part.begin(), part.end()); std::vector<OwnerRow>().swap(part); }
        std::sort(rows.begin(), rows.end(), [](OwnerRow const& a, OwnerRow const& b) { return std::memcmp(a.data(), b.data(), 32) < 0; });
        for (std::size_t i = 1; i < rows.size(); ++i) if (!std::memcmp(rows[i - 1].data(), rows[i].data(), 32)) throw Error("NFToken on two snapshot pages");
        Bytes head("XQNF", 4);
        put(head, 1, 2); put(head, 0, 2); put(head, seq, 4); put(head, rows.size(), 8); head += source;
        auto partial = fs::path(targets.owners.string() + ".part");
        std::ofstream out(partial, std::ios::binary | std::ios::trunc);
        crypto_hash_sha256_state state; crypto_hash_sha256_init(&state);
        auto emit = [&](char const* data, std::size_t size) {
            out.write(data, static_cast<std::streamsize>(size));
            crypto_hash_sha256_update(&state, reinterpret_cast<unsigned char const*>(data), size);
        };
        emit(head.data(), head.size());
        if (!rows.empty()) emit(rows.front().data(), rows.size() * nft_row);
        unsigned char digest[32]; crypto_hash_sha256_final(&state, digest);
        out.write(reinterpret_cast<char const*>(digest), 32);
        if (!out.flush()) throw Error("Cannot write snapshot NFT index");
        out.close();
        durable_rename(partial, targets.owners);
        NftIndex check(targets.owners, source);
        results.push_back({{"snapshot", snapshot.at("seq")}, {"status", "built"}, {"nfts", rows.size()}, {"bytes", fs::file_size(targets.owners)},
                           {"seconds", hx::elapsed(started)}});
    }
    if (holders) { auto rows = joined(holder_rows); auto built = write_optional(targets.holders, Optional::mpt_holders, seq, source, rows); built["snapshot"] = seq; results.push_back(built); }
    if (issuers) { auto rows = joined(issuer_rows); auto built = write_optional(targets.issuers, Optional::nfts_by_issuer, seq, source, rows); built["snapshot"] = seq; results.push_back(built); }
    return results;
}
// An optional index for a range whose query index is already built: token IDs from its NFT
// posting table (every token its transactions name, which is all nfts_by_issuer needs), and
// MPToken holders from its object patches, decoded without metadata.
Json build_optional_later(fs::path const& cache, Json const& range, Optional kind, fs::path const& target, unsigned jobs) {
    auto sha = range.at("sha256").get<std::string>();
    auto source = from_hex(sha);
    if (kind == Optional::nfts_by_issuer) {
        QueryIndex query(query_path(cache, sha), source);
        std::vector<Row<issuer_row>> rows;
        for (auto record = query.nfts.at(0); record; record = query.nfts.at(record->next)) rows.push_back(make_issuer_row(Bytes(record->key)));
        return write_optional(target, kind, query.first, source, rows);
    }
    Mapped file(cache / range.at("object").get<std::string>());
    auto data = file.view();
    auto container = Container::parse(data, range.at("directory_sha256").get<std::string>());
    std::vector<std::vector<Row<holder_row>>> found(container.segments.size());
    std::atomic<std::size_t> next{0}; std::exception_ptr failure; std::mutex failed;
    auto worker = [&] {
        try {
            for (std::size_t s; (s = next++) < container.segments.size();) {
                auto const& segment = container.segments[s];
                auto bytes = data.substr(segment.offset, segment.length);
                if (hx::sha(bytes) != segment.digest) throw Error("Segment checksum differs");
                hx::Archive archive(bytes, true);  // its bytes checked just now: not hashed again
                if (archive.first != segment.first || archive.count != segment.count) throw Error("Segment range differs from its container directory");
                for (U page = 0; page < archive.pages(); ++page) {
                    for (auto const& [seq, raw] : archive.decodePage(page, true))
                        for (auto const& patch : parse_ledger_record(raw).patches)
                            if (!patch.object.empty())
                                if (auto holder = mptoken_holder(patch.object)) found[s].push_back(make_holder_row(holder->first, holder->second));
                    archive.releasePage(page);
                }
                file.release(bytes);
            }
        } catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); next = container.segments.size(); }
    };
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < std::max(1u, jobs); ++i) threads.emplace_back(worker);
    for (auto& t : threads) t.join();
    if (failure) std::rethrow_exception(failure);
    auto rows = joined(found);
    return write_optional(target, kind, container.first, source, rows);
}

// The seek index's stride: piece_format.hpp's default. XRPLA_TEST_SEEK_STRIDE sets another for
// tests, whose fixtures' lists are far shorter than two default strides; a file of another
// stride is rebuilt.
U seek_stride() {
    static U const stride = [] {
        auto const* chosen = std::getenv("XRPLA_TEST_SEEK_STRIDE");
        return chosen ? std::clamp<U>(std::stoull(chosen), 1, U(1) << 20) : seek_default_stride;
    }();
    return stride;
}
// The XQSK seek index (piece_format.hpp) of a range's query index: one pass over its account
// and NFT posting tables, reading each list longer than two strides as history_page does and
// recording the reader's state before every stride-th posting. No container is read.
Json build_seek(fs::path const& query_file, Bytes const& source, fs::path const& target) {
    auto started = hx::Clock::now();
    QueryIndex query(query_file, source);
    U stride = seek_stride();
    Bytes sections[2];
    U lists[2] = {}, points[2] = {}, postings = 0;
    Table const* tables[2] = {&query.accounts, &query.nfts};
    for (unsigned t = 0; t < 2; ++t) {
        Bytes directory, rows;
        for (auto record = tables[t]->at(0); record; record = tables[t]->at(record->next)) {
            if (record->entries <= 2 * stride || record->payload.size() > UINT32_MAX) continue;
            Bytes list;
            PostingReader reader(*record);
            bool fits = true;
            for (Posting p;;) {
                auto const& s = reader.state();
                if (s.read && s.read < record->entries && s.read % stride == 0) {
                    if (s.ledger > UINT32_MAX || s.index > UINT32_MAX) { fits = false; break; }
                    put(list, s.at, 4); put(list, s.ledger, 4); put(list, s.index, 4);
                }
                if (!reader.next(p)) break;
            }
            if (!fits) continue;
            put(directory, tables[t]->offset_of(*record), 8); put(directory, points[t], 8);
            rows += list; points[t] += list.size() / seek_point; ++lists[t]; postings += record->entries;
        }
        sections[t] = std::move(directory) + rows;
    }
    Bytes head("XQSK", 4);
    put(head, seek_version, 2); put(head, 0, 2); put(head, stride, 4); put(head, query.first, 4); head += query.checksum();
    for (unsigned t = 0; t < 2; ++t) { put(head, lists[t], 8); put(head, points[t], 8); }
    auto partial = fs::path(target.string() + ".part");
    {
        std::ofstream out(partial, std::ios::binary | std::ios::trunc);
        crypto_hash_sha256_state state; crypto_hash_sha256_init(&state);
        for (auto const* part : {&head, &sections[0], &sections[1]}) {
            out.write(part->data(), static_cast<std::streamsize>(part->size()));
            crypto_hash_sha256_update(&state, reinterpret_cast<unsigned char const*>(part->data()), part->size());
        }
        unsigned char digest[32]; crypto_hash_sha256_final(&state, digest);
        out.write(reinterpret_cast<char const*>(digest), 32);
        if (!out.flush()) throw Error("Cannot write seek index " + target.string());
    }
    durable_rename(partial, target);
    SeekIndex check(target, query);
    return {{"index", "seek"}, {"status", "built"}, {"stride", stride}, {"lists", lists[0] + lists[1]}, {"postings", postings},
            {"checkpoints", points[0] + points[1]}, {"bytes", fs::file_size(target)}, {"query_bytes", fs::file_size(query_file)},
            {"seconds", hx::elapsed(started)}};
}

// Every index file is written beside its target, synced, and renamed into place with its
// directory synced (durable_rename), so a crash leaves the old file or the whole new one. Each
// ends in the SHA-256 of everything before it, which a sync checks in full when it first finds
// the file (one renamed before its bytes reached the disk, as files once were, can hold zeros
// where its tail should be) and rebuilds it when it differs. `query/.checked` then lists the
// file's size, inode and modification time as checked, and a file still matching its line is
// not read again: a full history's indexes are hundreds of GB, and a sync runs every few
// minutes. As for the cache's `.verified` (transport.cpp), a file is recorded once its
// modification time is a few seconds old, so a rewrite within the same timestamp tick cannot
// keep the recorded time; a file built here is recorded at once, its time set that far back
// first, which any later write moves forward again.
constexpr std::chrono::seconds settle{5};
void check_trailer(fs::path const& path) {
    Mapped file(path);
    auto data = file.view();
    if (data.size() < 32) throw Error("Truncated local index " + path.string());
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) throw Error("SHA-256 unavailable");
    auto body = data.substr(0, data.size() - 32);
    constexpr std::size_t step = std::size_t(64) << 20;
    for (std::size_t at = 0; at < body.size(); at += step) {
        auto chunk = body.substr(at, step);
        if (EVP_DigestUpdate(context.get(), chunk.data(), chunk.size()) != 1) throw Error("SHA-256 failed");
        file.release(chunk);  // a one-pass scan: its pages leave the process as it goes
    }
    unsigned char digest[32]; unsigned length = 0;
    if (EVP_DigestFinal_ex(context.get(), digest, &length) != 1 || length != 32) throw Error("SHA-256 failed");
    if (data.substr(data.size() - 32) != V(reinterpret_cast<char const*>(digest), 32)) throw Error("Local index differs from its checksum: " + path.string());
}
class Checked {
    fs::path list_;
    std::map<std::string, std::string> lines_;
    static std::string stamp(fs::path const& file) {
        struct stat st{};
        if (::lstat(file.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) return {};
        return std::to_string(st.st_size) + ' ' + std::to_string(st.st_ino) + ' ' + std::to_string(st.st_mtim.tv_sec) + '.' + std::to_string(st.st_mtim.tv_nsec);
    }
public:
    explicit Checked(fs::path const& query) : list_(query / ".checked") {
        if (!fs::is_regular_file(list_)) return;
        std::ifstream in(list_, std::ios::binary);
        for (std::string line; std::getline(in, line);)
            if (auto space = line.find(' '); space != std::string::npos) lines_[line.substr(0, space)] = line.substr(space + 1);
    }
    fs::path const& path() const { return list_; }
    bool known(fs::path const& file) const {
        auto found = lines_.find(file.filename().string());
        auto now = stamp(file);
        return found != lines_.end() && !now.empty() && found->second == now;
    }
    // A file whose trailer was just checked.
    void checked(fs::path const& file) {
        std::error_code error;
        auto written = fs::last_write_time(file, error);
        if (!error && fs::file_time_type::clock::now() - written >= settle) lines_[file.filename().string()] = stamp(file);
    }
    // A file this run built, its trailer computed as it was written.
    void built(fs::path const& file) {
        fs::last_write_time(file, fs::file_time_type::clock::now() - settle);
        lines_[file.filename().string()] = stamp(file);
    }
    // Rewritten whole, naming only the files kept.
    void save(std::set<fs::path> const& live) {
        std::string text;
        for (auto const& [name, line] : lines_) if (live.contains(list_.parent_path() / name)) text += name + ' ' + line + '\n';
        auto partial = fs::path(list_.string() + ".part");
        write_file(partial, text);
        fs::rename(partial, list_);
    }
};
bool optional_wanted(IndexOptions const& options, Optional kind) { return kind == Optional::mpt_holders ? options.mpt_holders : options.nfts_by_issuer; }
// The query/ files serving a selection reads: its base snapshot's state and NFT owners, each
// interval snapshot's state, and every range's query and seek indexes, with the optional
// indexes `options` keeps for the base snapshot and each range.
std::set<fs::path> selected_indexes(fs::path const& cache, Json const& selection, IndexOptions const& options) {
    auto seq = selection.at("snapshot").at("seq").get<U>();
    std::set<fs::path> files{state_path(cache, seq), nft_path(cache, seq)};
    for (auto key : {"snapshots", "recent"}) {
        if (!selection.contains(key)) continue;
        auto const& listed = selection.at(key);
        if (listed.is_array()) for (auto const& s : listed) files.insert(state_path(cache, s.at("seq").get<U>()));
        else files.insert(state_path(cache, listed.at("seq").get<U>()));
    }
    for (auto kind : {Optional::mpt_holders, Optional::nfts_by_issuer})
        if (optional_wanted(options, kind)) files.insert(optional_path(cache, snapshot_stem(seq), kind));
    for (auto const& range : selection.at("ranges")) {
        auto sha = range.at("sha256").get<std::string>();
        files.insert(query_path(cache, sha));
        files.insert(seek_path(cache, sha));
        for (auto kind : {Optional::mpt_holders, Optional::nfts_by_issuer})
            if (optional_wanted(options, kind)) files.insert(optional_path(cache, container_stem(sha), kind));
    }
    return files;
}
// Removes every file in query/ that `keep` does not name, with what an interrupted build left
// (its .part file and tmp- directory; no build runs while the cache is locked).
Json remove_others(fs::path const& cache, std::set<fs::path> const& keep) {
    Json removed = Json::array();
    if (!fs::exists(cache / "query")) return removed;
    for (auto const& entry : fs::directory_iterator(cache / "query")) {
        auto name = entry.path().filename().string();
        if (entry.is_regular_file() && !keep.contains(entry.path())) { removed.push_back(name); fs::remove(entry.path()); }
        else if (entry.is_directory() && !entry.is_symlink() && name.starts_with("tmp-")) { removed.push_back(name); fs::remove_all(entry.path()); }
    }
    return removed;
}
}

// `xrpla index-pieces`: the cache is locked (as sync_pieces locks it, then calls the overload
// below), so a sync and an index run cannot build the same files at once. A damaged file of the
// selection rebuilt has servers reopen the cache, as sync_pieces has them: selection.json is
// written again as it was.
Json index_pieces(fs::path const& cache, unsigned jobs, IndexOptions const& options) {
    DirectoryLock lock(cache);
    auto pointer = cache / "selection.json";
    auto text = read_file(pointer, 64 << 20);
    auto result = index_pieces(cache, jobs, Json::parse(text), options);
    if (!result.at("rebuilt").empty()) { write_file(pointer.string() + ".part", text); durable_rename(pointer.string() + ".part", pointer); }
    return result;
}
Json index_pieces(fs::path const& cache, unsigned jobs, Json const& selection, IndexOptions const& options) {
    fs::create_directories(cache / "query");
    Checked checked(cache / "query");
    std::set<fs::path> checked_now;
    Json results = Json::array();
    auto const& snapshot = selection.at("snapshot");
    auto seq = snapshot.at("seq").get<U>();
    auto state = state_path(cache, seq);
    auto index_sha = from_hex(snapshot.at("index").at("sha256").get<std::string>());
    // A file is kept when it opens, belongs to the data it indexes and matches its trailer
    // (checked once: Checked); otherwise it is rebuilt. A damaged file stays until its
    // replacement is renamed over it: a server starting meanwhile finds the file it names, and
    // servers holding it mapped keep the old inode until they reopen the cache, which the
    // caller has them do once the files are in place (`rebuilt`: selection.json rewritten).
    Json rebuilt = Json::array();
    auto usable = [&](fs::path const& path, auto&& open) {
        if (!fs::exists(path)) return false;
        try {
            open();
            if (!checked.known(path)) { check_trailer(path); checked.checked(path); checked_now.insert(path); }
            return true;
        } catch (std::exception const&) { rebuilt.push_back(path.filename().string()); return false; }
    };
    if (usable(state, [&] { StateIndex existing(state, index_sha); })) results.push_back({{"snapshot", seq}, {"status", "present"}});
    else results.push_back(build_state(cache, snapshot, state, jobs));
    auto optional_usable = [&](fs::path const& path, Optional kind, Bytes const& source) {
        return usable(path, [&] { OptionalIndex existing(path, kind, source); });
    };
    // The snapshot's NFT owners and optional maps, whichever are missing, in one pass over its parts.
    auto nfts = nft_path(cache, seq);
    SnapshotTargets missing;
    if (!usable(nfts, [&] { NftIndex existing(nfts, index_sha); })) missing.owners = nfts;
    // The interval snapshots' states, which the ledgers after each read from (sync_pieces):
    // `snapshots` lists every one selected, `recent` the newest (alone, in a cache synced before
    // the list existed). A snapshot no longer selected loses its state file below.
    std::map<U, Json const*> intervals;
    for (auto key : {"snapshots", "recent"}) {
        if (!selection.contains(key)) continue;
        auto const& listed = selection.at(key);
        if (listed.is_array()) for (auto const& s : listed) intervals.emplace(s.at("seq").get<U>(), &s);
        else intervals.emplace(listed.at("seq").get<U>(), &listed);
    }
    for (auto const& [interval_seq, interval] : intervals) {
        auto interval_state = state_path(cache, interval_seq);
        auto interval_sha = from_hex(interval->at("index").at("sha256").get<std::string>());
        if (usable(interval_state, [&] { StateIndex existing(interval_state, interval_sha); })) results.push_back({{"snapshot", interval_seq}, {"status", "present"}});
        else results.push_back(build_state(cache, *interval, interval_state, jobs));
    }
    auto want = [&](Optional kind) { return optional_wanted(options, kind); };
    for (auto kind : {Optional::mpt_holders, Optional::nfts_by_issuer}) {
        if (!want(kind)) continue;
        auto path = optional_path(cache, snapshot_stem(seq), kind);
        if (!optional_usable(path, kind, index_sha)) (kind == Optional::mpt_holders ? missing.holders : missing.issuers) = path;
    }
    if (!missing.owners.empty() || !missing.holders.empty() || !missing.issuers.empty())
        for (auto& built : build_snapshot_maps(cache, snapshot, missing, jobs)) results.push_back(std::move(built));
    else results.push_back({{"snapshot", seq}, {"nfts", "present"}});
    for (auto const& range : selection.at("ranges")) {
        auto sha = range.at("sha256").get<std::string>();
        auto source = from_hex(sha);
        auto target = query_path(cache, sha);
        OptionalTargets wanted;
        for (auto kind : {Optional::mpt_holders, Optional::nfts_by_issuer}) {
            if (!want(kind)) continue;
            auto path = optional_path(cache, container_stem(sha), kind);
            if (!optional_usable(path, kind, source)) (kind == Optional::mpt_holders ? wanted.holders : wanted.issuers) = path;
        }
        // The seek index, derived from the query index alone: built after it, and for a query
        // index an older build left, without rebuilding it.
        auto seek = seek_path(cache, sha);
        bool seek_usable = false;
        auto check_seek = [&] {
            seek_usable = usable(seek, [&] {
                SeekIndex existing(seek, QueryIndex(target, source));
                if (existing.stride != seek_stride()) throw Error("Seek index stride differs");
            });
        };
        // A version 2 index (piece_format.hpp) opens, but is rebuilt for its NFT states.
        if (!usable(target, [&] { if (QueryIndex(target, source).version != query_version) throw Error("Query index predates its version"); })) {
            // Built with the query index, from the same decode.
            auto item = build_index(cache / range.at("object").get<std::string>(), range, target, jobs, wanted);
            check_seek();  // one for the replaced query index names its checksum, and is rebuilt
            if (!seek_usable) item["seek"] = build_seek(target, source, seek);
            results.push_back(item);
            continue;
        }
        Json item{{"object", range.at("object")}, {"status", "present"}};
        Json built = Json::array();
        if (!wanted.holders.empty()) built.push_back(build_optional_later(cache, range, Optional::mpt_holders, wanted.holders, jobs));
        if (!wanted.issuers.empty()) built.push_back(build_optional_later(cache, range, Optional::nfts_by_issuer, wanted.issuers, jobs));
        if (!built.empty()) item["optional"] = built;
        bool had_seek = fs::exists(seek);
        check_seek();
        if (!seek_usable) {
            item["seek"] = build_seek(target, source, seek);
            // Servers holding the cache open read this range's lists from their start until they
            // reopen it: a new seek index has them reopen it too, as a damaged one rebuilt does.
            if (!had_seek) rebuilt.push_back(seek.filename().string());
        }
        results.push_back(item);
    }
    // A re-sync can drop ranges or move the base snapshot, and an optional index can be turned
    // off: their files go too. A sync indexes a selection before switching selection.json to
    // it, so the files of the one still named there stay: servers starting before the switch
    // (or after a sync killed before it) open that one. sync_pieces removes them once it has
    // switched (remove_unselected_indexes), as it removes the pieces it no longer selects.
    auto live = selected_indexes(cache, selection, options);
    auto keep = live;
    keep.insert(checked.path());
    if (auto pointer = cache / "selection.json"; fs::exists(pointer)) {
        auto current = selected_indexes(cache, Json::parse(read_file(pointer, 64 << 20)), options);
        keep.insert(current.begin(), current.end());
    }
    auto removed = remove_others(cache, keep);
    // Every file of the selection was checked above or built since.
    for (auto const& path : live)
        if (fs::exists(path) && !checked.known(path) && !checked_now.contains(path)) checked.built(path);
    checked.save(keep);
    return {{"cache", cache.string()}, {"ranges", results}, {"removed", removed}, {"rebuilt", rebuilt},
            {"optional", {{"mpt_holders", options.mpt_holders}, {"nfts_by_issuer", options.nfts_by_issuer}}}};
}
// After sync_pieces has switched selection.json to `selection`: the index files only the
// selection it replaced still named (index_pieces kept them until the switch).
Json remove_unselected_indexes(fs::path const& cache, Json const& selection, IndexOptions const& options) {
    Checked checked(cache / "query");
    auto keep = selected_indexes(cache, selection, options);
    keep.insert(checked.path());
    auto removed = remove_others(cache, keep);
    if (fs::exists(cache / "query")) checked.save(keep);
    return removed;
}
}
