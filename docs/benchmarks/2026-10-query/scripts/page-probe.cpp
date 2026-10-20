// History page decode modes on real segments, timed: whole page with and without its canonical
// digest, patches only (whole page and through mid-page), and one ledger (mid-page, last and first
// of the page, two in a row, every ledger in turn and in a shuffled order). Each mode runs on a
// freshly opened segment (dictionary loaded and transaction section read untimed), then once more
// warm. It also reports each segment's dictionary load (read, parse, memory) and checks that
// one-ledger decodes match the page's in order, in reverse and shuffled. Built against
// tools/history_reader.hpp, linked with zstd, libsodium and OpenSSL.
// Usage: page-probe CONTAINER < lines of "segment-offset segment-length" (from its directory)
#include "history_reader.hpp"
#include <random>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
using namespace xrpla::history;
int main(int argc, char** argv) {
    int fd = open(argv[1], O_RDONLY); off_t size = lseek(fd, 0, SEEK_END);
    auto base = static_cast<char const*>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    std::vector<std::pair<U, U>> segs; U off, len;
    while (std::cin >> off >> len) segs.emplace_back(off, len);
    char const* names[] = {"full page, digest checked", "full page, no digest", "patches, whole page", "patches, to mid ledger",
                           "one ledger mid-page, full", "one ledger mid-page, patches", "one ledger, last of page, full", "one ledger, first of page, full",
                           "two ledgers, mid then last (the second resumes)", "every ledger of the page in turn", "every ledger of the page, shuffled"};
    constexpr int N = 11; double cold[N] = {}, warm[N] = {}; U pages = 0; std::size_t cursor = 0;
    double read = 0, parse = 0; std::size_t dictionary = 0;
    std::mt19937_64 random(7);
    for (auto [o, l] : segs) {
        V data(base + o, l);
        Archive probe(data, true); U np = probe.pages(), per = probe.pageLedgers();
        probe.prepare(); auto [r, p] = probe.dictionarySeconds(); read += r; parse += p; dictionary += probe.dictionaryBytes();
        for (U pi = 0; pi < np; pi += std::max<U>(1, np / 6)) {
            U f = probe.first + pi * per, mid = f + per / 2, last = f + per - 1;
            if (pi == np - 1) continue;  // a short last page
            std::vector<U> shuffled; for (U s = f; s <= last; ++s) shuffled.push_back(s);
            std::shuffle(shuffled.begin(), shuffled.end(), random);
            for (int s = 0; s < N; ++s) {
                Archive a(data, true); a.prepare(); a.header(f);
                auto run = [&] {
                    switch (s) {
                    case 0: a.decodePage(pi); break;
                    case 1: a.decodePage(pi, false, ~U(0), false); break;
                    case 2: a.decodePage(pi, true); break;
                    case 3: a.decodePage(pi, true, mid); break;
                    case 4: a.decodeLedger(mid); break;
                    case 5: a.decodeLedger(mid, true); break;
                    case 6: a.decodeLedger(last); break;
                    case 7: a.decodeLedger(f); break;
                    case 8: a.decodeLedger(mid); a.decodeLedger(last); break;
                    case 9: for (U s = f; s <= last; ++s) a.decodeLedger(s); break;
                    case 10: for (U s : shuffled) a.decodeLedger(s); break;
                    }
                };
                auto t = Clock::now(); run(); cold[s] += elapsed(t);
                t = Clock::now(); run(); warm[s] += elapsed(t);
            }
            // One-ledger decodes match the full page's ledgers: in turn (each resuming), in
            // reverse (each starting a cursor) and shuffled.
            Archive a(data, true);
            auto rows = a.decodePage(pi);
            for (auto& [seq, raw] : rows) if (seq == mid) require(raw == a.decodeLedger(mid), "One-ledger decode differs");
            auto prow = a.decodePage(pi, true, mid); require(Archive::patchesOf(prow.back().second) == Archive::patchesOf(a.decodeLedger(mid, true)), "One-ledger patches differ");
            for (auto& [seq, raw] : rows) require(raw == a.decodeLedger(seq), "Resumed decode differs");
            for (auto it = rows.rbegin(); it != rows.rend(); ++it) require(it->second == a.decodeLedger(it->first), "Restarted decode differs");
            for (U s : shuffled) require(rows[s - f].second == a.decodeLedger(s), "Shuffled decode differs");
            cursor = std::max(cursor, a.cursorBytes());
            ++pages;
        }
    }
    std::cout << "segments " << segs.size() << ", pages " << pages << "\n";
    std::cout << "dictionary per segment: read " << 1000 * read / segs.size() << " ms, parse " << 1000 * parse / segs.size()
              << " ms, memory " << dictionary / segs.size() / 1024 << " KiB; largest cursors: " << cursor / 1024 << " KiB\n";
    std::cout << "ms per decode, cold segment / warm repeat\n";
    for (int s = 0; s < N; ++s) std::cout << "  " << names[s] << ": " << 1000 * cold[s] / pages << " / " << 1000 * warm[s] / pages << "\n";
}
