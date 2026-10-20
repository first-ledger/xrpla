// XRPL binary by type code (wire.hpp); the same rules as tools/field_codec.py.
#include "wire.hpp"
#include <openssl/evp.h>
#include <algorithm>
#include <initializer_list>
#include <optional>

namespace xrpla::core::wire {
namespace {
enum Type { UInt16 = 1, UInt32 = 2, UInt64 = 3, UInt128 = 4, Hash256 = 5, Amount = 6, Blob = 7, Account = 8, Number = 9,
            Int32 = 10, Int64 = 11, Object = 14, Array = 15, UInt8 = 16, UInt160 = 17, PathSet = 18, Vector256 = 19,
            UInt96 = 20, UInt192 = 21, UInt384 = 22, UInt512 = 23, Issue = 24, XChainBridge = 25, Currency = 26 };
int fixed_width(int type) {
    switch (type) {
    case UInt16: return 2; case UInt32: case Int32: return 4; case UInt64: case Int64: return 8; case UInt128: return 16;
    case Hash256: return 32; case Number: case UInt96: return 12; case UInt8: return 1; case UInt160: case Currency: return 20;
    case UInt192: return 24; case UInt384: return 48; case UInt512: return 64;
    default: return 0;
    }
}
struct Reader {
    std::string_view data;
    std::size_t at = 0;
    unsigned char byte() { if (at >= data.size()) throw Error("Truncated binary field"); return static_cast<unsigned char>(data[at++]); }
    std::string_view take(std::size_t n) { if (data.size() - at < n) throw Error("Truncated binary field"); auto v = data.substr(at, n); at += n; return v; }
    std::size_t length() {
        std::size_t a = byte();
        if (a <= 192) return a;
        if (a <= 240) return 193 + (a - 193) * 256 + byte();
        if (a <= 254) { std::size_t b = byte(), c = byte(); return 12481 + (a - 241) * 65536 + b * 256 + c; }
        throw Error("Invalid length prefix");
    }
    // Issue: XRP (20 zero bytes), currency and issuer, or an MPT (issuer, the "no account"
    // marker, then its sequence).
    std::string_view issue() {
        auto start = at;
        auto currency = take(20);
        if (std::all_of(currency.begin(), currency.end(), [](char c) { return c == 0; })) return data.substr(start, 20);
        auto second = take(20);
        static std::string const no_account = std::string(19, '\0') + '\x01';
        if (second == no_account) take(4);
        return data.substr(start, at - start);
    }
    // Reads fields in order, telling `sink` of each: open(type, code, depth) and close(bytes)
    // around an object's or array's contents (`bytes` all of them), field(type, code, depth,
    // value) for any other field, its value as parse() keeps it.
    template <class Sink> void walk(int closing, unsigned depth, Sink& sink) {
        if (depth > 64) throw Error("Binary nesting too deep");
        for (;;) {
            if (at == data.size()) { if (closing) throw Error("Missing object or array end"); return; }
            auto h = byte();
            int type = h >> 4, code = h & 15;
            if (!type) type = byte();
            if (!code) code = byte();
            if (!type || !code) throw Error("Invalid field ID");
            if ((type == Object || type == Array) && code == 1) {
                if (type != closing) throw Error("Unbalanced object or array");
                return;
            }
            auto start = at;
            // An array's element is an object whatever field names it: rippled once read any
            // field there as the element's name, and in 2014 a Payment whose Memos holds an
            // element named MemoType (f9 7c e1 f1) entered ledger 6,409,247. Current libxrpl
            // rejects it ("Non-object in array"), so xrpld no longer loads that ledger.
            if (type == Object || type == Array || closing == Array) {
                sink.open(type, code, depth);
                walk(closing == Array ? Object : type, depth + 1, sink);
                sink.close(data.substr(start, at - start));
                continue;
            }
            std::string_view value;
            if (auto width = fixed_width(type)) value = take(width);
            else if (type == Blob || type == Account || type == Vector256) { auto n = length(); value = take(n); }
            else if (type == Amount) {
                // Issued: 48 bytes. MPT: 33, and its first byte is exactly 0x60 or 0x20 (libxrpl
                // writes only the sign and MPT bits there). Else XRP, 8 bytes: in 2013 some XRP
                // amounts too large to be valid, with the 0x20 bit set among their value's bits,
                // entered ledgers (625,681's OfferCreate); current libxrpl misreads them as MPT.
                auto first = static_cast<unsigned char>(data.at(at));
                take((first & 0x80) ? 48 : (first == 0x60 || first == 0x20) ? 33 : 8);
            } else if (type == PathSet) {
                for (;;) {
                    auto flag = byte();
                    if (flag == 0x00) break;
                    if (flag == 0xff) continue;
                    if (flag & ~0x31) throw Error("Unknown path element");
                    if (flag & 0x01) take(20);
                    if (flag & 0x10) take(20);
                    if (flag & 0x20) take(20);
                }
            } else if (type == Issue) issue();
            else if (type == XChainBridge) {
                if (length() != 20) throw Error("Invalid bridge door"); take(20); issue();
                if (length() != 20) throw Error("Invalid bridge door"); take(20); issue();
            } else throw Error("Unknown serialized type " + std::to_string(type));
            // Types read in parts (amounts, paths, issues, bridges) keep their serialized bytes. A
            // variable-length field's value is its content, which may be empty (it had become its
            // length byte: pseudo-transactions' empty Account read as one byte).
            bool variable = type == Blob || type == Account || type == Vector256;
            if (!variable && value.empty()) value = data.substr(start, at - start);
            sink.field(type, code, depth, value);
        }
    }
};
// Builds parse()'s fields. An object or array without fields keeps its serialized bytes.
struct Tree {
    std::vector<Field> top;
    std::vector<std::vector<Field>*> filling{&top};  // the innermost open object's fields last
    void open(int type, int code, unsigned) {
        filling.back()->push_back(Field{type, code, {}, {}});
        filling.push_back(&filling.back()->back().fields);
    }
    void close(std::string_view bytes) {
        filling.pop_back();
        auto& field = filling.back()->back();
        if (field.fields.empty()) field.value = bytes;
    }
    void field(int type, int code, unsigned, std::string_view value) { filling.back()->push_back(Field{type, code, value, {}}); }
};
// The first top-level field of a type and code, when asked for; everything is read either way.
struct TopLevel {
    int type = 0, code = 0;
    std::optional<std::string_view> found;
    void open(int, int, unsigned) {}
    void close(std::string_view) {}
    void field(int t, int c, unsigned depth, std::string_view value) { if (depth == 0 && !found && t == type && c == code) found = value; }
};
Bytes bytes(std::string_view v) { return Bytes(v.data(), v.size()); }
std::uint64_t number(std::string_view v) { std::uint64_t n = 0; for (unsigned char c : v) n = n << 8 | c; return n; }
}

Field const* Field::find(int t, int c) const {
    for (auto const& f : fields) if (f.type == t && f.code == c) return &f;
    return nullptr;
}
std::vector<Field> parse(std::string_view data) { Reader r{data}; Tree tree; r.walk(0, 0, tree); return std::move(tree.top); }
std::optional<std::string_view> top_level(std::string_view data, int type, int code) {
    Reader r{data};
    TopLevel sink{type, code, std::nullopt};
    r.walk(0, 0, sink);
    return sink.found;
}
void validate(std::string_view data) { (void)top_level(data, 0, 0); }
bool has_any(std::string_view data, std::initializer_list<std::pair<int, int>> fields) {
    struct Seeking {
        std::initializer_list<std::pair<int, int>> fields;
        bool found = false;
        void check(int t, int c) { for (auto const& [type, code] : fields) found = found || (t == type && c == code); }
        void open(int t, int c, unsigned) { check(t, c); }
        void close(std::string_view) {}
        void field(int t, int c, unsigned, std::string_view) { check(t, c); }
    } sink{fields};
    Reader r{data};
    r.walk(0, 0, sink);
    return sink.found;
}

namespace {
// SHA-512 of `parts` one after another.
void sha512_of(std::initializer_list<std::string_view> parts, unsigned char out[64]) {
    static EVP_MD* const md = [] {
        auto* fetched = EVP_MD_fetch(nullptr, "SHA512", nullptr);
        if (!fetched) throw Error("SHA-512 is unavailable");
        return fetched;
    }();
    struct Context { EVP_MD_CTX* ctx = EVP_MD_CTX_new(); ~Context() { EVP_MD_CTX_free(ctx); } };
    thread_local Context context;
    unsigned length = 0;
    bool ok = context.ctx && EVP_DigestInit_ex2(context.ctx, md, nullptr) == 1;
    for (auto part : parts) ok = ok && EVP_DigestUpdate(context.ctx, part.data(), part.size()) == 1;
    if (!ok || EVP_DigestFinal_ex(context.ctx, out, &length) != 1 || length != 64) throw Error("SHA-512 failed");
}
// What summarize() takes from a transaction's top-level fields.
struct TransactionFields {
    std::optional<std::string_view> type, account, sequence;
    void open(int, int, unsigned) {}
    void close(std::string_view) {}
    void field(int t, int c, unsigned depth, std::string_view value) {
        if (depth) return;
        if (t == UInt16 && c == 2 && !type) type = value;
        else if (t == Account && c == 1 && !account) account = value;
        else if (t == UInt32 && c == 4 && !sequence) sequence = value;
    }
};
// What summarize() takes from metadata: TransactionIndex, and from AffectedNodes each node's
// LedgerIndex and the accounts its fields name (CreatedNode (3) lists NewFields (8);
// ModifiedNode (5) and DeletedNode (4) FinalFields (7)). The first of a repeated field counts,
// as Field::find finds it.
struct MetadataFields {
    std::optional<std::string_view> index;
    std::vector<Bytes> keys, accounts;
    enum class In { Top, Nodes, Node, Fields, Other } in = In::Top;
    std::vector<In> stack;                       // where each open object or array is
    bool nodes_seen = false;                     // the first AffectedNodes only
    bool key_seen = false, fields_seen = false;  // per node
    int node_code = 0;
    void open(int type, int code, unsigned) {
        stack.push_back(in);
        if (in == In::Top && type == Array && code == 8 && !nodes_seen) { nodes_seen = true; in = In::Nodes; }
        else if (in == In::Nodes) { in = In::Node; node_code = code; key_seen = fields_seen = false; }
        else if (in == In::Node && type == Object && code == (node_code == 3 ? 8 : 7) && !fields_seen) { fields_seen = true; in = In::Fields; }
        else in = In::Other;
    }
    void close(std::string_view) { in = stack.back(); stack.pop_back(); }
    void field(int t, int c, unsigned, std::string_view value) {
        static std::string_view const zero("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20);
        if (in == In::Top) { if (t == UInt32 && c == 28 && !index) index = value; return; }
        if (in == In::Node) { if (t == Hash256 && c == 6 && !key_seen) { key_seen = true; keys.push_back(bytes(value)); } return; }
        if (in != In::Fields) return;
        if (t == Account && value.size() == 20) accounts.push_back(bytes(value));
        // LowLimit, HighLimit, TakerPays, TakerGets: the issuer of an issued amount (or of an
        // MPT amount's issuance).
        else if (t == Amount && (c == 6 || c == 7 || c == 4 || c == 5)) {
            auto first = static_cast<unsigned char>(value.at(0));
            std::string_view issuer;
            if (first & 0x80) issuer = value.substr(28, 20);
            else if (first == 0x60 || first == 0x20) issuer = value.substr(13, 20);  // MPT (see walk)
            if (!issuer.empty() && issuer != zero) accounts.push_back(bytes(issuer));
        } else if (t == UInt192 && c == 1) {  // MPTokenIssuanceID
            auto issuer = value.substr(4, 20);
            if (issuer != zero) accounts.push_back(bytes(issuer));
        }
    }
};
}
void sha512(void const* data, std::size_t size, unsigned char out[64]) { sha512_of({std::string_view(static_cast<char const*>(data), size)}, out); }
Bytes transaction_id(Bytes const& transaction) {
    unsigned char digest[64];
    sha512_of({std::string_view("TXN\0", 4), transaction}, digest);
    return Bytes(reinterpret_cast<char const*>(digest), 32);
}

// Read field by field without keeping them: every restored ledger's transactions are
// summarized, and building and freeing a tree of their fields cost more than reading them.
Summary summarize(TransactionRecord const& record) {
    Summary s;
    s.hash = transaction_id(record.transaction);
    TransactionFields tx;
    Reader{record.transaction}.walk(0, 0, tx);
    if (tx.type) s.type = static_cast<std::uint16_t>(number(*tx.type));
    if (tx.account) s.account = bytes(*tx.account);
    // Pseudo-transactions (UNLModify, SetFee, EnableAmendment) leave Account empty; xrpld names
    // it the zero account. Readers take 20 bytes, so an empty one read memory past it.
    if (s.account.empty()) s.account = Bytes(20, '\0');
    if (s.account.size() != 20) throw Error("Transaction Account is not 20 bytes");
    if (tx.sequence) s.sequence = static_cast<std::uint32_t>(number(*tx.sequence));
    MetadataFields meta;
    Reader{record.metadata}.walk(0, 0, meta);
    if (!meta.index) throw Error("Metadata has no TransactionIndex");
    s.index = static_cast<std::uint32_t>(number(*meta.index));
    s.keys = std::move(meta.keys);
    std::sort(meta.accounts.begin(), meta.accounts.end());
    meta.accounts.erase(std::unique(meta.accounts.begin(), meta.accounts.end()), meta.accounts.end());
    s.accounts = std::move(meta.accounts);
    return s;
}
}
