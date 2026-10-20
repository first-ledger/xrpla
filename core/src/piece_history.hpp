#pragma once
#include "history.hpp"
#include <xrpla/core/operations.hpp>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string_view>

namespace xrpla::core {
// A hash of a 32-byte key (an object key, a transaction or ledger hash, an NFT ID, a padded
// account), all of it mixed with a key drawn per process: such keys share long prefixes (a book's
// quality directories their first 24 bytes, an owner's NFT pages 20), and clients choose the keys
// they ask about, so neither a prefix nor a fixed hash may decide a bucket. PieceHistory's
// newest-version shards and LiveHistory's tail indexes are keyed by it.
struct SeededKeyHash {
    template<class Byte> std::size_t operator()(std::array<Byte, 32> const& k) const {
        static_assert(sizeof(Byte) == 1);
        std::uint64_t h = seed();
        for (std::size_t i = 0; i < k.size(); i += 8) {
            std::uint64_t word; std::memcpy(&word, k.data() + i, 8);
            h ^= word; h *= 0xBF58476D1CE4E5B9ull; h ^= h >> 31;  // splitmix64's finaliser, per word
        }
        h *= 0x94D049BB133111EBull; h ^= h >> 29;
        return static_cast<std::size_t>(h);
    }
    static std::uint64_t seed() {
        static std::uint64_t const value = std::random_device{}() * 0x9E3779B97F4A7C15ull ^ std::random_device{}();
        return value;
    }
};
// The work one key listing may do: the object table records and snapshot keys it looks at, live or
// not. A listing walks every key any range since its base snapshot ever held between its bounds
// until it has `limit` that exist, and a busy book's range holds millions of price levels long
// gone (the query index cannot skip them: PieceHistory::keys_between). Past this many the request
// fails with tooBusy (rpc::Failure) rather than answering with part of the keys, which a caller
// could not tell from all of them: book_offers, ledger_data and successors must be xrpld's. Real
// listings do walk that far: mainnet's XRP/CNY.RippleFox book at ledger 52,768,795, read from the
// first snapshot, examines over 33 million records (18 s cold). So the bound is 2^30 (about a
// billion records, minutes of walking): far past any listing history holds, and still an end to
// one that would otherwise walk without one. One walk may span several calls: LiveHistory's merge
// of the base and its tail passes its own.
// A walk also has a deadline, so one request cannot hold a thread for the minutes its record
// budget allows: 60 s from its start, past which it fails with the same tooBusy. The clock is
// read once every `check_every` records spent (a record costs well under a microsecond; reading
// the clock each time would be a large part of a walk), so a walk overruns its deadline by at
// most that many records. The longest real listing above (18 s) is well within it.
struct KeyWalk {
    static constexpr std::uint64_t budget = std::uint64_t(1) << 30;
    static constexpr std::uint64_t check_every = 65536;
    static constexpr std::chrono::seconds time{60};
    std::uint64_t left = budget;
    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + time;
    std::uint64_t until_check = check_every;  // records left before the clock is read again
    // A refused step charges nothing.
    void spend(std::uint64_t n = 1) {
        if (n > left) exhausted();
        if (n >= until_check) {
            if (std::chrono::steady_clock::now() >= deadline) exhausted();
            until_check = check_every;
        } else until_check -= n;
        left -= n;
    }
    [[noreturn]] static void exhausted();  // throws rpc::Failure{tooBusy}
};
// Serves the History read surface straight from a piece cache written by
// `piece_client.py sync`: the signed selection (catalog, base snapshot, ranges),
// the compressed containers, and the XQIX query index next to them.
class PieceHistory : public History {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    explicit PieceHistory(fs::path const& cache);
    ~PieceHistory() override;
    Json status() const override;
    std::optional<std::uint32_t> baseline_ledger() const override;
    std::optional<std::pair<std::uint32_t, std::uint32_t>> served_ledgers() const override;
    Bytes header(std::uint32_t sequence) const override;
    std::uint32_t ledger_sequence(Bytes const& hash) const override;
    Bytes object(Bytes const& key, std::uint32_t sequence) const override;
    std::optional<std::pair<std::uint32_t, Bytes>> deleted_object(Bytes const& key, std::uint32_t sequence) const override;
    Json object_page(std::uint32_t sequence, std::string const& marker, unsigned limit, bool binary = false) const override;
    // As above, its key listing charged to `walk` (a caller merging several pages: LiveHistory).
    Json object_page(std::uint32_t sequence, std::string const& marker, unsigned limit, bool binary, KeyWalk& walk) const;
    Json history_page(std::string const& column, Bytes const& key, std::uint32_t first,
        std::uint32_t last, unsigned limit, bool forward, Json const& marker) const override;
    Json transaction(Bytes const& hash, bool binary = false) const override;
    std::vector<Bytes> ledger_transactions(std::uint32_t sequence) const override;
    LedgerRecord ledger_record(std::uint32_t sequence) const override;
    std::vector<TransactionRecord> ledger_transaction_records(std::uint32_t sequence) const override;
    Json nft(Bytes const&, std::uint32_t) const override;
    std::vector<Json> nfts(std::vector<Bytes> const& ids, std::uint32_t sequence) const override;
    bool has_nft_state() const override;
    std::optional<std::vector<Bytes>> mpt_holders(Bytes const& issuance, Bytes const& after, unsigned limit) const override;
    std::optional<std::vector<Bytes>> nfts_by_issuer(Bytes const& issuer, std::optional<std::uint32_t> taxon, Bytes const& after, unsigned limit) const override;
    void prefetch(std::vector<Bytes> const&, std::uint32_t) const override;
    std::vector<Bytes> objects(std::vector<Bytes> const&, std::uint32_t) const override;
    bool prefetches() const override { return true; }
    bool lists_keys() const override { return true; }
    // Opens the newest `segments` segments of the last range (their codec dictionaries).
    void warm(unsigned segments) const;
    std::vector<Bytes> keys_between(std::uint32_t, Bytes const&, Bytes const&, unsigned) const override;
    // As above, the records looked at charged to `walk` (tooBusy once it is spent).
    std::vector<Bytes> keys_between(std::uint32_t, Bytes const&, Bytes const&, unsigned, KeyWalk& walk) const;
    std::optional<std::vector<Bytes>> book_directories(std::uint32_t, Bytes const&, Bytes const&, unsigned) const override;
    Json cache_stats() const override;
    // Gives back what this history's decode caches hold, now that a newer history of the same
    // cache directory (open_piece_history) serves in its place and shares its memory budget. It
    // still answers the requests that hold it, caching only within room the budget has left.
    void retire() const;
};
bool is_piece_cache(fs::path const&);
// Applies to piece histories opened afterwards: a memory budget split across decoded pages
// (30%), open segments (60%, about 150 MB each) and snapshot blocks (10%).
void configure_piece_caches(std::size_t megabytes, unsigned warm_segments);
// One shared instance per cache directory, reopened when the selection changes.
std::shared_ptr<PieceHistory const> open_piece_history(fs::path const&);
// index_pieces (operations.hpp) builds or checks the local indexes of every range in a piece cache.
// EnableAmendment, SetFee and UNLModify (TransactionType 100-102, read by type code): the only
// transactions whose IDs can repeat (an identical vote in two flag ledgers). Every backend
// answers such an ID with its earliest ledger, as xrpld.
bool pseudo_transaction(std::string_view transaction);
// The NFToken IDs whose nft_history lists a transaction, as Clio 2.8 lists them (piece_index.cpp):
// what the Store's nft_tx index, the query index's NFT postings and the tail's hold.
std::set<Bytes> nft_ids(TransactionRecord const&);
// A token's owner (base58) and URI (hex) as the Store's nft index records them.
struct NftState {
    std::string owner, uri;
    bool burned = false;
    bool operator==(NftState const&) const = default;
};
// Where a transaction leaves an NFToken: on a page (its owner and URI), burned (the last
// page it was on), or nullopt when the transaction changed no page holding it.
std::optional<NftState> nft_change(TransactionRecord const&, Bytes const& id);
// nft_change for every token the transaction's NFTokenPage nodes hold, from one parse.
std::map<Bytes, NftState> nft_changes(TransactionRecord const&);
// 16 bytes identifying an NftState, stored in the query index's NFT state table: 8 of its owner
// and burned flag (a change of either starts nft_info's ledger_index, as in Clio; a URI change
// alone does not), then 8 of the owner, URI and burned flag.
Bytes nft_fingerprint(NftState const&);
constexpr std::size_t nft_holder_print = 8;
// The tokens on a serialized NFTokenPage: ID and URI.
std::vector<std::pair<Bytes, std::string>> page_tokens(std::string_view page);
// An MPToken object's issuance ID (24 bytes) and holder AccountID (20); nothing for other objects.
std::optional<std::pair<Bytes, Bytes>> mptoken_holder(std::string_view object);
// A token's issuer (20 bytes) and its NFTs-by-issuer entry (piece_format.hpp, XQOX kind 2).
std::pair<Bytes, Bytes> nft_issuer_entry(Bytes const& id);
}
