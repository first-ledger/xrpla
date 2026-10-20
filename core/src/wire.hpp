#pragma once
#include <xrpla/core/format.hpp>
#include <initializer_list>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace xrpla::core::wire {
// XRPL canonical binary read by type code alone, without libxrpl's field table: fields a
// newer amendment added (new field codes of known types) parse like any other. Only a new
// serialized type is unreadable. Used where ingestion must not depend on the protocol
// version this build knows; ledger integrity comes from the tree hashes, not from parsing.
struct Field {
    int type = 0, code = 0;
    std::string_view value;    // the value (variable-length types: without the length prefix)
    std::vector<Field> fields; // an object's fields, or an array's elements
    Field const* find(int type, int code) const;
};
// Throws on truncation, an unknown type or unbalanced objects and arrays.
std::vector<Field> parse(std::string_view data);
// The same checks without keeping the fields (what every ledger's objects and transactions
// get): all of `data`, and the value of its first top-level field of a type and code.
void validate(std::string_view data);
std::optional<std::string_view> top_level(std::string_view data, int type, int code);
// Whether any field, at any depth, has one of these (type code, field code) pairs.
bool has_any(std::string_view data, std::initializer_list<std::pair<int, int>> fields);

// What indexing needs from a transaction and its metadata.
struct Summary {
    Bytes hash;                // the transaction ID
    std::uint32_t index = 0;   // TransactionIndex
    std::uint16_t type = 0;    // TransactionType
    Bytes account;             // the sending account, 20 bytes (zero for pseudo-transactions)
    std::uint32_t sequence = 0;
    std::vector<Bytes> accounts;  // affected accounts, as xrpld's TxMeta::getAffectedAccounts
    std::vector<Bytes> keys;      // LedgerIndex of every affected node
};
Summary summarize(TransactionRecord const&);
// SHA-512 through one OpenSSL context per thread, its algorithm fetched once: OpenSSL's
// one-shot SHA512() looks the algorithm up and allocates a context on every call, and
// libsodium's is portable C (about 1.4x slower on a 516-byte inner node). The first half
// names every SHAMap node and transaction.
void sha512(void const* data, std::size_t size, unsigned char out[64]);
Bytes transaction_id(Bytes const& transaction);
}
