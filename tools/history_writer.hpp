#pragma once
// XRPH v2 encoder: the compiled counterpart of tools/history_codec.py, which stays
// the reference. Uncompressed sections are byte-identical to the Python encoder's
// (checked by tests/history_writer_test.py); frames use this binary's zstd. Every
// derived hash is verified against the original bytes before a recipe is used.
#include "history_reader.hpp"
#include <functional>
#include <optional>
#include <map>
#include <set>
#include <unordered_set>

namespace xrpla::history {

// One canonical field: a leaf (value) or an object/array (children + end marker).
struct Node { S header; U type = 0, field = 0; S value; std::vector<Node> children; S end; bool container = false; };

inline std::vector<Node> parse(V data) {
    static std::map<U, U> const fixed{{1, 2}, {2, 4}, {3, 8}, {4, 16}, {5, 32}, {9, 12}, {10, 4}, {11, 8},
                                      {16, 1}, {17, 20}, {20, 12}, {21, 24}, {22, 48}, {23, 64}, {26, 20}};
    Reader r{data};
    std::function<std::vector<Node>(U, unsigned, S&)> fields = [&](U endType, unsigned depth, S& end) {
        require(depth <= 64, "Nesting bound");
        std::vector<Node> nodes;
        while (r.pos < data.size()) {
            auto start = r.pos; U b = r.byte(); U t = b >> 4, f = b & 15;
            if (!t) t = r.byte();
            if (!f) f = r.byte();
            require(t && f, "Invalid field");
            S header(data.substr(start, r.pos - start));
            if ((t == 14 || t == 15) && f == 1) { require(t == endType, "Unexpected end"); end = header; return nodes; }
            // An array's element is an object whatever field names it (core/src/wire.cpp).
            if (t == 14 || t == 15 || endType == 15) {
                Node node{header, t, f, {}, {}, {}, true};
                node.children = fields(endType == 15 ? 14 : t, depth + 1, node.end);
                nodes.push_back(std::move(node)); continue;
            }
            start = r.pos;
            if (auto it = fixed.find(t); it != fixed.end()) r.take(it->second);
            else if (t == 7 || t == 8 || t == 19) {
                U a = r.byte(), n;
                if (a <= 192) n = a;
                else if (a <= 240) n = 193 + (a - 193) * 256 + r.byte();
                else { require(a <= 254, "Invalid VL"); U x = r.byte(), y = r.byte(); n = 12481 + (a - 241) * 65536 + x * 256 + y; }
                r.take(n);
            } else if (t == 6) {
                U a = static_cast<unsigned char>(r.take(8)[0]);
                // Issued, MPT (first byte exactly 0x60 or 0x20), else XRP: 2013 held XRP amounts
                // with the 0x20 bit among their value's bits (core/src/wire.cpp).
                if (a & 128) r.take(40); else if (a == 0x60 || a == 0x20) r.take(25);
            } else if (t == 18) {
                while (true) {
                    U flag = r.byte();
                    if (flag == 0) break;
                    if (flag == 255) continue;
                    require(!(flag & ~U(0x31)), "Invalid path");
                    for (U mask : {1, 16, 32}) if (flag & mask) r.take(20);
                }
            } else throw std::runtime_error("Unsupported type");
            nodes.push_back(Node{header, t, f, S(data.substr(start, r.pos - start)), {}, {}, false});
        }
        require(endType == 0, "Missing end");
        end.clear(); return nodes;
    };
    S end; auto nodes = fields(0, 0, end); r.end(); return nodes;
}

inline void serialize(Node const& n, S& out) {
    out += n.header;
    if (!n.container) { out += n.value; return; }
    for (auto const& c : n.children) serialize(c, out);
    out += n.end;
}
inline S serialize(Node const& n) { S out; serialize(n, out); return out; }

struct Recipe { U mode = 0; std::vector<U> args; };
struct EncodedAtom { U header = 0, kind = 0; std::vector<U> args; };
using Cache = std::unordered_map<U, std::unordered_map<U, U>>;

// Interning order is part of the format: it mirrors history_codec.Pool call for call.
class Pool {
    template <class Key, class Index> static U intern(std::vector<Key>& items, Index& index, Key const& value) {
        auto [it, inserted] = index.try_emplace(value, items.size());
        if (inserted) items.push_back(value);
        return it->second;
    }
    struct ShapeHash { std::size_t operator()(std::vector<U> const& v) const { std::size_t h = v.size(); for (U x : v) h = h * 1000003 ^ x; return h; } };
public:
    bool derive, extra;
    std::vector<S> headers, values, hashes, rawAtoms;
    std::unordered_map<S, U> hmap, vmap, hashmap, amap;
    std::vector<EncodedAtom> atoms;
    std::vector<std::vector<U>> shapes; std::unordered_map<std::vector<U>, U, ShapeHash> smap;
    std::unordered_map<S, Recipe> recipes; std::unordered_set<S> tried; std::unordered_map<S, U> ledgerHashes;

    Pool(bool recipes_, bool extra_) : derive(recipes_), extra(recipes_ && extra_) {}
    U header(S const& h) { return intern(headers, hmap, h); }
    U value(S const& v) { return intern(values, vmap, v); }
    U hash(S const& h) { require(h.size() == 32, "Hash length"); return intern(hashes, hashmap, h); }

    void keyRecipe(S const& key, std::vector<Node const*> const& nodes) {
        if (!derive) return;
        std::map<std::pair<U, U>, S> v;
        for (auto n : nodes) if (!n->container) v[{n->type, n->field}] = n->value;
        auto field = [&](U t, U f) -> S const* { auto it = v.find({t, f}); return it == v.end() ? nullptr : &it->second; };
        auto account = [&](U t, U f) -> S { auto x = field(t, f); return x && x->size() == 21 && (*x)[0] == 20 ? x->substr(1) : S(); };
        auto kindField = field(1, 1); S kind = kindField ? *kindField : S();
        bool haveKind = kindField != nullptr;
        std::optional<S> got; Recipe recipe;
        if (haveKind && kind == S("\0a", 2) && !account(8, 1).empty()) {
            auto a = account(8, 1); got = half(S("\0a", 2) + a); recipe = {2, {value(a)}};
        } else if (haveKind && kind == S("\0o", 2) && !account(8, 1).empty() && field(2, 4) && field(2, 4)->size() == 4) {
            auto a = account(8, 1); auto seq = *field(2, 4); got = half(S("\0o", 2) + a + seq); recipe = {3, {value(a), value(seq)}};
        } else if (haveKind && kind == S("\0r", 2)) {
            S low = field(6, 6) ? *field(6, 6) : S(), high = field(6, 7) ? *field(6, 7) : S();
            if (low.size() == 48 && high.size() == 48 && low.substr(8, 20) == high.substr(8, 20)) {
                S a = low.substr(28), b = high.substr(28), currency = low.substr(8, 20);
                if (b < a) std::swap(a, b);
                got = half(S("\0r", 2) + a + b + currency); recipe = {4, {value(a), value(b), value(currency)}};
            }
        }
        if (got && *got == key) { recipes[key] = recipe; return; }
        if (extra && !tried.contains(key)) extraRecipe(key, field, account, haveKind, kind);
    }

    template <class Field, class Account>
    void extraRecipe(S const& key, Field const& field, Account const& account, bool haveKind, S const& kind) {
        if (!haveKind) return;
        std::optional<Recipe> recipe;
        auto sized = [&](U t, U f, std::size_t n) { auto x = field(t, f); return x && x->size() == n; };
        if (kind == S("\0T", 2) && !account(8, 1).empty() && sized(2, 41, 4)) {
            if (half(S("\0T", 2) + account(8, 1) + *field(2, 41)) == key) recipe = Recipe{5, {value(account(8, 1)), value(*field(2, 41))}};
        } else if (kind == S("\0C", 2) && !account(8, 1).empty() && sized(2, 4, 4)) {
            if (half(S("\0C", 2) + account(8, 1) + *field(2, 4)) == key) recipe = Recipe{6, {value(account(8, 1)), value(*field(2, 4))}};
        } else if (kind == S("\0d", 2)) {
            auto owner = account(8, 2); auto root = field(5, 8);
            // Metadata omits default values such as XRP's all-zero currency and issuer.
            std::vector<S> book;
            for (U i : {1, 3, 2, 4}) book.push_back(field(17, i) ? *field(17, i) : S(20, '\0'));
            auto rate = field(3, 6);
            auto bookBase = [&] { S in("\0B", 2); for (int i = 0; i < 4; ++i) in += book[i]; return half(in).substr(0, 24); };
            if (!owner.empty() && half(S("\0O", 2) + owner) == key) recipe = Recipe{7, {value(owner)}};
            else if (!field(5, 34) && rate && rate->size() == 8 && bookBase() + *rate == key) {
                Recipe r{8, {}}; for (auto const& x : book) r.args.push_back(value(x)); r.args.push_back(value(*rate)); recipe = r;
            } else if (root && root->size() == 32 && *root != key) {
                std::set<Wide> hints; for (U p = 1; p <= 64; ++p) hints.insert(p);
                auto u64 = [](S const& s) { U x = 0; for (unsigned char c : s) x = x << 8 | c; return Wide(x); };
                if (sized(3, 2, 8)) hints.insert(u64(*field(3, 2)) + 1);
                if (sized(3, 1, 8)) { Wide next = u64(*field(3, 1)); if (next > 0) hints.insert(next - 1); }
                bool found = false;
                for (Wide p : hints) {
                    if (p == 0 || p >= (Wide(1) << 64)) continue;
                    S page(8, '\0'); U x = static_cast<U>(p); for (int i = 7; i >= 0; --i) { page[i] = char(x & 255); x >>= 8; }
                    if (half(S("\0d", 2) + *root + page) == key) { recipe = Recipe{9, {hash(*root), static_cast<U>(p)}}; found = true; break; }
                }
                if (!found) tried.insert(key);
            }
        }
        if (recipe) recipes[key] = *recipe;
    }

    U atom(Node const& node) {
        S raw = serialize(node);
        if (auto it = amap.find(raw); it != amap.end()) return it->second;
        EncodedAtom a; a.header = header(node.header);
        auto const& v = node.value;
        if (node.container) { a.kind = 0; a.args = {value(raw.substr(node.header.size()))}; }
        else if (node.type == 5) { a.kind = 1; a.args = {hash(v)}; }
        else if (node.type == 8 && v.size() == 21 && v[0] == 20) { a.kind = 2; a.args = {value(v.substr(1))}; }
        else if (node.type == 6 && v.size() == 48) { a.kind = 3; auto x = value(v.substr(0, 8)); auto y = value(v.substr(8, 20)); a.args = {x, y, value(v.substr(28))}; }
        else if (node.type == 19) {
            unsigned char first = static_cast<unsigned char>(v[0]);
            std::size_t prefix = first <= 192 ? 1 : first <= 240 ? 2 : 3;
            auto parts = V(v).substr(prefix);
            if (parts.size() % 32 == 0) {
                a.kind = 4; a.args = {value(v.substr(0, prefix))};
                for (std::size_t i = 0; i < parts.size(); i += 32) a.args.push_back(hash(S(parts.substr(i, 32))));
            } else { a.kind = 0; a.args = {value(v)}; }
        } else { a.kind = 0; a.args = {value(v)}; }
        U id = atoms.size(); amap.emplace(std::move(raw), id); atoms.push_back(std::move(a)); return id;
    }

    void view(U key, std::vector<Node> const& nodes, Cache& cache, S& out) {
        std::vector<U> ids, shape;
        for (auto const& n : nodes) ids.push_back(atom(n));
        for (U i : ids) shape.push_back(atoms[i].header);
        U sid = intern(shapes, smap, shape);
        S mask((ids.size() + 7) / 8, '\0'), changes;
        auto& old = cache[key];
        for (std::size_t at = 0; at < ids.size(); ++at) {
            auto it = old.find(shape[at]);
            if (it == old.end() || it->second != ids[at]) {
                mask[at / 8] = char(static_cast<unsigned char>(mask[at / 8]) | (1 << (at % 8)));
                vu(changes, ids[at]); old[shape[at]] = ids[at];
            }
        }
        vu(out, sid); out += mask; out += changes;
    }

    void node(Node const& n, Cache& cache, std::optional<U> context, S& out) {
        if (!n.container) { out += '\0'; vu(out, atom(n)); return; }
        if (n.type == 14 && (n.field == 3 || n.field == 4 || n.field == 5)) {
            std::vector<Node const*> keys, types;
            for (auto const& c : n.children) {
                if (!c.container && c.type == 5 && c.field == 6) keys.push_back(&c);
                if (!c.container && c.type == 1 && c.field == 1) types.push_back(&c);
            }
            context = keys.size() == 1 && keys[0]->value.size() == 32 ? std::optional<U>(hash(keys[0]->value)) : std::nullopt;
            if (context && types.size() == 1)
                for (auto const& part : n.children)
                    if (part.container && part.type == 14 && (part.field == 7 || part.field == 8)) {
                        std::vector<Node const*> fields{types[0]};
                        for (auto const& c : part.children) fields.push_back(&c);
                        keyRecipe(keys[0]->value, fields);
                    }
        }
        if (n.type == 14 && (n.field == 6 || n.field == 7 || n.field == 8) && context) {
            if (n.field == 8) cache.erase(*context);
            out += '\2'; vu(out, header(n.header)); blob(out, n.end); vu(out, *context); out += char(n.field == 8);
            view(*context, n.children, cache, out); return;
        }
        out += '\1'; vu(out, header(n.header)); blob(out, n.end); vu(out, n.children.size());
        for (auto const& c : n.children) node(c, cache, context, out);
    }

    void metadata(S const& raw, std::optional<std::vector<Node>> const& nodes, Cache& cache, S& out) {
        if (!nodes) { out += '\0'; blob(out, raw); return; }
        out += '\1'; vu(out, nodes->size());
        for (auto const& n : *nodes) node(n, cache, std::nullopt, out);
    }

    S finish() {
        if (extra) for (auto const& h : hashes) if (!recipes.contains(h)) if (auto it = ledgerHashes.find(h); it != ledgerHashes.end()) recipes[h] = Recipe{10, {it->second}};
        S out("XHD2"); vu(out, headers.size());
        for (auto const& h : headers) blob(out, h);
        vu(out, values.size());
        for (auto const& v : values) blob(out, v);
        vu(out, hashes.size());
        for (auto const& h : hashes) {
            auto it = derive ? recipes.find(h) : recipes.end();
            if (it != recipes.end()) { out += char(it->second.mode); for (U a : it->second.args) vu(out, a); }
            else { out += '\0'; out += h; }
        }
        vu(out, atoms.size());
        for (auto const& a : atoms) {
            vu(out, a.header); vu(out, a.kind);
            if (a.kind == 4) { vu(out, a.args[0]); vu(out, a.args.size() - 1); for (std::size_t i = 1; i < a.args.size(); ++i) vu(out, a.args[i]); }
            else for (U x : a.args) vu(out, x);
        }
        vu(out, shapes.size());
        for (auto const& s : shapes) { vu(out, s.size()); for (U h : s) vu(out, h); }
        return out;
    }
};

// A canonical v2 ledger record split the way history_codec.encode consumes it.
struct Row { U seq; S prefix; std::vector<std::pair<S, S>> patches; };

inline S rawRecord(Row const& row) {
    S out = row.prefix; le(out, row.patches.size(), 4);
    for (auto const& [k, v] : row.patches) { out += k; le(out, v.size(), 4); out += v; }
    return out;
}

struct Encoded { S data; std::vector<S> sections; };

// history_codec.encode(rows, zstd, page_ledgers, recipes=True, level, transaction_columns=False, extra_recipes).
inline Encoded encode(std::vector<Row> const& rows, U pageLedgers = 64, int level = 19, bool extraRecipes = true, bool recipes = true, unsigned threads = 1) {
    require(!rows.empty() && rows.size() <= 4096 && pageLedgers >= 1 && pageLedgers <= 256, "Segment/page bound");
    for (std::size_t i = 0; i < rows.size(); ++i) require(rows[i].seq == rows[0].seq + i, "Noncontiguous segment");
    Pool pool(recipes, extraRecipes);
    struct Page { U first, count, txFirst, txCount; S digest, tx, control; };
    std::vector<Page> pages; U rawTotal = 0, txTotal = 0, position = 0; S allRecords, table;
    crypto_hash_sha256_state all; crypto_hash_sha256_init(&all);
    for (std::size_t start = 0; start < rows.size(); start += pageLedgers) {
        auto end = std::min<std::size_t>(rows.size(), start + pageLedgers);
        S tx, control; vu(tx, end - start); vu(control, end - start); Cache cache; U pageTxFirst = txTotal;
        std::vector<std::pair<U, S>> pageRecords;
        for (std::size_t i = start; i < end; ++i) {
            auto const& row = rows[i];
            Reader r{row.prefix}; require(r.little(4) == row.seq, "Row sequence");
            S header(r.take(r.little(4))); U n = r.little(4);
            if (pool.extra) pool.ledgerHashes.try_emplace(half(S("LWR\0", 4) + header), row.seq - rows[0].seq);
            le(tx, row.seq, 4); blob(tx, header); vu(tx, n);
            struct Meta { U execution, index; S raw; std::optional<std::vector<Node>> nodes; };
            std::vector<Meta> metas;
            for (U j = 0; j < n; ++j) {
                S t(r.take(r.little(4))); S m(r.take(r.little(4)));
                require(t.size() <= recordMax && m.size() <= recordMax, "Oversized record");
                blob(tx, t);
                if (recipes) pool.recipes[half(S("TXN\0", 4) + t)] = Recipe{1, {txTotal}};
                ++txTotal;
                std::optional<std::vector<Node>> nodes;
                try { nodes = parse(m); } catch (std::exception const&) { nodes.reset(); }
                require(m.size() >= 6 && m[0] == '\x20' && m[1] == '\x1c', "Missing canonical transaction index");
                U execution = 0; for (int k = 2; k < 6; ++k) execution = execution << 8 | static_cast<unsigned char>(m[k]);
                metas.push_back({execution, j, std::move(m), std::move(nodes)});
            }
            r.end();
            std::sort(metas.begin(), metas.end(), [](auto const& a, auto const& b) { return a.execution < b.execution || (a.execution == b.execution && a.index < b.index); });
            for (U j = 0; j < metas.size(); ++j) require(metas[j].execution == j, "Invalid transaction execution order");
            vu(control, n);
            for (auto const& m : metas) { vu(control, m.index); vu(control, m.raw.size()); pool.metadata(m.raw, m.nodes, cache, control); }
            vu(control, row.patches.size());
            for (auto const& [key, value] : row.patches) {
                U kid = pool.hash(key); vu(control, kid); vu(control, value.size());
                if (value.empty()) { cache.erase(kid); continue; }
                std::optional<std::vector<Node>> nodes;
                try { nodes = parse(value); } catch (std::exception const&) { nodes.reset(); }
                if (!nodes) { control += '\0'; blob(control, value); }
                else {
                    std::vector<Node const*> fields; for (auto const& x : *nodes) fields.push_back(&x);
                    pool.keyRecipe(key, fields); control += '\1'; pool.view(kid, *nodes, cache, control);
                }
            }
            auto original = rawRecord(row);
            crypto_hash_sha256_update(&all, reinterpret_cast<unsigned char const*>(original.data()), original.size());
            le(table, row.seq, 4); le(table, position, 8); position += original.size();
            pageRecords.emplace_back(row.seq, std::move(original));
        }
        auto original = body(pageRecords); rawTotal += original.size();
        pages.push_back({rows[start].seq, end - start, pageTxFirst, txTotal - pageTxFirst, sha(original), std::move(tx), std::move(control)});
    }
    crypto_hash_sha256_update(&all, reinterpret_cast<unsigned char const*>(table.data()), table.size());
    S allDigest(32, '\0'); crypto_hash_sha256_final(&all, reinterpret_cast<unsigned char*>(allDigest.data()));
    require(rawTotal <= segmentMax, "Segment decoded bound");
    Encoded result; result.sections.push_back(pool.finish());
    for (auto& p : pages) { result.sections.push_back(std::move(p.tx)); result.sections.push_back(std::move(p.control)); }
    // Sections are independent zstd frames, so they compress on `threads` threads; the
    // bytes do not depend on the thread count.
    std::vector<S> frames(result.sections.size());
    for (auto const& s : result.sections) require(s.size() <= sectionMax, "Section bound");
    std::atomic<std::size_t> next{0};
    std::exception_ptr failure; std::mutex failed;
    auto compress = [&] {
        try {
            for (std::size_t i; (i = next++) < frames.size();) {
                auto const& s = result.sections[i];
                S frame(ZSTD_compressBound(s.size()), '\0');
                auto n = ZSTD_compress(frame.data(), frame.size(), s.data(), s.size(), level);
                require(!ZSTD_isError(n), "Zstd compression failed"); frame.resize(n); frames[i] = std::move(frame);
            }
        } catch (...) { std::lock_guard lock(failed); if (!failure) failure = std::current_exception(); }
    };
    std::vector<std::thread> workers;
    for (unsigned t = 1; t < std::max(1u, threads); ++t) workers.emplace_back(compress);
    compress();
    for (auto& w : workers) w.join();
    if (failure) std::rethrow_exception(failure);
    S& out = result.data;
    out += "XRPH"; le(out, 2, 2); le(out, rows[0].seq, 4); le(out, rows.size(), 4); le(out, pageLedgers, 2); le(out, pages.size(), 2); out += allDigest;
    for (auto const& p : pages) { le(out, p.first, 4); le(out, p.count, 4); le(out, p.txFirst, 4); le(out, p.txCount, 4); out += p.digest; }
    for (std::size_t i = 0; i < frames.size(); ++i) { le(out, result.sections[i].size(), 4); le(out, frames[i].size(), 4); out += sha(frames[i]); }
    for (auto const& f : frames) out += f;
    out += sha(out);
    require(out.size() <= segmentMax, "Encoded segment bound");
    return result;
}

} // namespace xrpla::history
