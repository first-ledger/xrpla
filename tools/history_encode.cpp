// Encode a canonical v2 body (ledger records + offset table) as one XRPH segment.
// --sections writes each uncompressed section as vu(length) + bytes, for comparison
// with the Python reference encoder; --check decodes the result and compares bytes;
// --threads compresses sections in parallel (same bytes).
#include "history_writer.hpp"
using namespace xrpla::history;

// Split a canonical v2 body into encoder rows, checking the trailing offset table.
std::vector<Row> rowsFromBody(V body, U count) {
    Reader r{body}; std::vector<Row> rows; std::vector<std::pair<U, U>> offsets;
    for (U i = 0; i < count; ++i) {
        auto start = r.pos; U seq = r.little(4); r.take(r.little(4)); U n = r.little(4);
        for (U j = 0; j < 2 * n; ++j) r.take(r.little(4));
        Row row{seq, S(body.substr(start, r.pos - start)), {}};
        U patches = r.little(4);
        for (U j = 0; j < patches; ++j) { S key(r.take(32)); S value(r.take(r.little(4))); row.patches.emplace_back(std::move(key), std::move(value)); }
        offsets.emplace_back(seq, start); rows.push_back(std::move(row));
    }
    for (auto const& [seq, at] : offsets) { require(r.little(4) == seq, "Offset table sequence"); require(r.little(8) == at, "Offset table position"); }
    r.end(); return rows;
}

int main(int argc, char** argv) {
    try {
        require(sodium_init() >= 0, "Sodium initialization");
        S bodyPath, out, sections; U count = 0, page = 64; int level = 19; bool extra = true, check = false; unsigned threads = 1;
        for (int i = 1; i < argc; ++i) {
            S a = argv[i]; auto next = [&] { require(i + 1 < argc, "Missing argument"); return S(argv[++i]); };
            if (a == "--body") bodyPath = next(); else if (a == "--count") count = std::stoull(next());
            else if (a == "--out") out = next(); else if (a == "--sections") sections = next();
            else if (a == "--page-ledgers") page = std::stoull(next()); else if (a == "--level") level = std::stoi(next());
            else if (a == "--no-extra-recipes") extra = false; else if (a == "--check") check = true;
            else if (a == "--threads") { threads = static_cast<unsigned>(std::stoul(next())); require(threads >= 1 && threads <= 64, "--threads must be 1-64"); }
            else throw std::runtime_error("Unknown option " + a);
        }
        require(!bodyPath.empty() && !out.empty() && count, "Usage: --body FILE --count N --out SEGMENT");
        auto input = load(bodyPath); auto start = Clock::now();
        auto rows = rowsFromBody(input, count);
        auto encoded = encode(rows, page, level, extra, true, threads);
        double seconds = elapsed(start);
        save(out, encoded.data);
        if (!sections.empty()) { S dump; for (auto const& s : encoded.sections) { vu(dump, s.size()); dump += s; } save(sections, dump); }
        Json report{{"ledgers", rows.size()}, {"bytes", encoded.data.size()}, {"encode_seconds", seconds}, {"sha256", hex(sha(encoded.data))}};
        if (check) { Archive archive(encoded.data); require(archive.restore() == S(input), "Decoded segment differs"); report["checked"] = true; }
        std::cout << report.dump() << '\n'; return 0;
    } catch (std::exception const& e) { std::cerr << Json{{"error", e.what()}}.dump() << '\n'; return 1; }
}
