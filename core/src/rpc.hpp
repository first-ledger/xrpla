#pragma once
#include "store.hpp"
#include <xrpla/core/operations.hpp>
#include <xrpl/protocol/ErrorCodes.h>
#include <xrpl/protocol/AccountID.h>
#include <xrpl/protocol/Asset.h>
#include <xrpl/protocol/Indexes.h>
#include <xrpl/protocol/STLedgerEntry.h>
#include <xrpl/protocol/Issue.h>
#include <xrpl/protocol/LedgerFormats.h>
#include <set>
#include <unordered_map>

namespace xrpla::core::rpc {
namespace r = ::xrpl;
struct Failure { Json value; };
[[noreturn]] void fail(r::ErrorCodeI, std::string const& message = {});
std::string text(Json const&, std::string const& field);
bool boolean(Json const&, std::string const& field, bool fallback = false);
// A field as xrpld's json::Value::asBool reads any value, where a handler does not check that it is
// a boolean: absent or null false, a number non-zero, a string or an array or object non-empty.
bool loose_boolean(Json const&, std::string const& field);
std::uint32_t integer(Json const&, std::string const& field, std::uint32_t fallback, std::uint32_t maximum = UINT32_MAX);
Bytes hash(std::string const&);
std::string upper(std::string value);
r::AccountID account(std::string const&);
template<class T> Bytes raw(T const& value) { return {reinterpret_cast<char const*>(value.data()), value.size()}; }
Json json(r::STBase const&);
Json header_json(Header const&, bool binary, unsigned version);
// Directory pages loaded per batch when walking a directory (32 entries each).
inline constexpr std::size_t kDirectoryWindow = 128;
inline constexpr std::uint64_t kPreloadPages = 2048;  // a preloaded window: about 65,000 entries
struct Context {
    History const& store;
    Json const& params;
    Header header;
    unsigned version;
    Json base() const;
    std::shared_ptr<r::SLE const> read(r::Keylet const&) const;
    // Whether the ledger's Amendments object lists `amendment`: the list read once a request.
    bool amendment(r::uint256 const& amendment) const;
    mutable std::optional<std::set<r::uint256>> amendments = {};
    // Before walking a directory from page `page`: loads the pages that `want` entries likely
    // span, then all of those entries, as two concurrent batches rather than one page at a time.
    void plan_directory(r::Keylet const& root, std::uint64_t page, std::size_t want) const;
    // Called by a directory walk after each page it has walked (`walked` of them so far, `next`
    // the page it reads next): halfway through each planned window of pages, plans the next
    // window. A walk over a large directory (an issuer's 100,000 trust lines) then loads its
    // entries in a few large parallel batches instead of one page's 32 entries per round.
    void plan_ahead(r::Keylet const& root, std::uint64_t next, std::uint64_t walked, std::size_t want) const;
    // For a walk over a whole large directory (want > one window): reads the pages from `page`
    // on (up to kPreloadPages of them, to the root's last) and every entry they list in two bulk
    // reads (History::objects), which read() then serves. A walk past them has plan_ahead read
    // the next pages in place of these, so a request holds one such window, not a directory of
    // a million entries. Nothing for a small directory.
    void preload_directory(r::Keylet const& root, std::uint64_t page, std::size_t want) const;
    // Objects read in bulk by preload_directory, by key, the last page they come from, and the
    // directory (its root key) they come from: a walk over another directory is outside the window.
    mutable std::unordered_map<Bytes, Bytes> preloaded = {};
    mutable std::uint64_t preloaded_through = 0;
    mutable r::uint256 preloaded_root = {};
    // Drops a preloaded window of a directory other than `root`'s.
    void leave_window(r::Keylet const& root) const;
    // Every other object this request has read, by key (empty: absent), so one read again (the
    // amendments, fees and issuer that book_offers checks for each offer) is not read twice.
    mutable std::unordered_map<Bytes, Bytes> memo = {};
};
// A ledger's fees (drops) as xrpld's Ledger::setup reads them: its FeeSettings fields over xrpld's
// configured defaults, which ledgers before the first fee vote (early 2013, no FeeSettings) use.
struct LedgerFees { std::uint64_t base = 10, reserve = 10'000'000, increment = 2'000'000; };
LedgerFees ledger_fees(Context const&);
// The XRP an account root holds back, as xrpld 3.4.1's xrpLiquid reckons it (AccountRootHelpers.cpp)
// for an account that is not a pseudo-account: the base reserve for itself (none when another
// account sponsors it, sfSponsor) and each account it sponsors (sfSponsoringAccountCount), and the
// owner reserve for its OwnerCount less those sponsored (sfSponsoredOwnerCount) plus those it
// sponsors for others (sfSponsoringOwnerCount).
r::XRPAmount account_reserve(LedgerFees const&, r::SLE const& root);
Json accounts(Context const&, std::string const&);
Json books(Context const&);
std::string owned(Context const&, r::AccountID const&, std::string const& marker, std::uint32_t limit,
    std::function<void(r::SLE const&)> const&, bool include_nfts = false);
}
namespace xrpla::core::rpc {
// rpc_more.cpp: further rippled read methods over any History.
std::set<std::string> const& more_methods();
Json more(Context const&, std::string const& method);
Json ledger_index(History const&, Json const& params, Json const& range);
r::Issue issue_of(Json const&);
// xrpld 3.4.1's isFrozen(view, account, asset) (TokenHelpers.cpp): an IOU under its issuer's global
// freeze or, for an account other than the issuer, the issuer's freeze of its line (deep freeze
// apart); an MPT locked for everyone or for the account, or vault shares whose vault's asset is
// frozen for the vault or the account (isVaultPseudoAccountFrozen).
bool asset_frozen(Context const&, r::AccountID const& account, r::Asset const&);
// xrpld 3.4.1's canTransferLPToken(view, holder, holder, issuer) (View.cpp) succeeds: true but for
// an AMM's LP tokens with a pool MPT the holder cannot transfer (canTransfer), or a missing pool.
bool lp_tokens_transferable(Context const&, r::AccountID const& holder, r::AccountID const& issuer);
// The same with the issuer's account root already read (null: no such account).
bool lp_tokens_transferable(Context const&, r::AccountID const& holder, r::SLE const* issuer_root);
// noripple_check's parameter checks, which xrpld makes before it looks up the ledger.
void check_noripple(Json const& params, unsigned version);
// deposit_authorized's and nft_buy_offers'/nft_sell_offers' parameter checks, which xrpld makes
// before it looks up the ledger (nothing for other methods).
void check_more(std::string const& method, Json const& params);
// A book as the books stream keys it: the issue going in, the issue coming out, and the
// permissioned domain if any. Issues are given as issue objects or as amounts (an XRP amount
// is a string).
std::string book_key(Json const& in, Json const& out, std::string const& domain = {});
// A request as an error echoes it: passphrase, secret, seed and seed_hex shown as "<masked>",
// as xrpld's ServerHandler does.
Json masked(Json request);
}
namespace xrpla::core::rpc {
// Shared by the rippled-compatible handlers (rpc_owner.cpp, rpc_more.cpp).
// An error that keeps the fields already in `result`, as rippled's injectError does.
[[noreturn]] void fail_into(Json result, r::ErrorCodeI code, std::string const& message = {});
// An error some xrpld handlers name with a bare string on top of the result's fields. xrpld's
// JSON-RPC answer then also carries "error_message": null (its request logging reads the field,
// which creates it), and so does this one.
[[noreturn]] void fail_named(Json result, std::string const& name);
// xrpld's readLimitField for a public caller: absent or null -> fallback, zero refused ("Invalid
// field 'limit'."), out of range -> clamped to [lo, hi].
std::uint32_t limit_field(Json const& params, std::uint32_t lo, std::uint32_t fallback, std::uint32_t hi);
// rippled's forEachItemAfter: `visit` returns true for items that count toward `limit`;
// false when `after` is set but not found in the directory.
bool for_each_after(Context const&, r::Keylet const& root, r::uint256 const& after, std::uint64_t hint, unsigned limit,
    std::function<bool(std::shared_ptr<r::SLE const> const&)> const& visit);
std::uint64_t start_hint(r::SLE const&, r::AccountID const&);
// The first state key strictly greater than `key` and below `end`, as ReadView::succ.
std::optional<r::uint256> successor(Context const&, r::uint256 const& key, r::uint256 const& end);
Json owner_methods(Context const&, std::string const& method);
bool is_owner_method(std::string const& method);
// A `type` filter as rippled reads it: the canonical entry name (any case) or its RPC name.
std::optional<r::LedgerEntryType> entry_type(Json const& params);
// rpc_entry.cpp: ledger_entry. The selector check comes before the ledger is looked up, as in xrpld.
void check_ledger_entry(Json const& params);
Json ledger_entry(Context const&);
// rpc_clio.cpp: Clio's methods beyond xrpld's read surface, and random (no ledger needed).
std::set<std::string> const& clio_methods();
Json clio(Context const&, std::string const& method);
Json random_value();
// rpc.cpp: whether xrpld 3.4.1 reads a transaction (its STTx and metadata objects built), and
// whether it holds a ledger of these transactions: not when one is unreadable for a reason the
// rippled versions that first stored the early ledgers shared (see refused_ledger).
bool read_by_xrpld(TransactionRecord const&);
bool ledger_held_by_xrpld(std::vector<TransactionRecord> const&);
}
