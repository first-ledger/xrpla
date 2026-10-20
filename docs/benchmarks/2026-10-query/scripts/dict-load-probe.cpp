// Where a segment dictionary's load goes: SHA-256 of the compressed section, zstd (one-shot and with
// a reused context), and the parse of each table, mirroring tools/history_reader.hpp's format.
// Usage: dict-load-probe CONTAINER < lines of "segment-offset segment-length" (from its directory)
#include "history_reader.hpp"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
using namespace xrpla::history;
int main(int argc, char** argv) {
    int fd = open(argv[1], O_RDONLY); off_t size = lseek(fd, 0, SEEK_END);
    auto base = static_cast<char const*>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
    U off, len; double t_sha = 0, t_zstd = 0, t_zstd_ctx = 0, t_head = 0, t_val = 0, t_hash = 0, t_atom = 0, t_shape = 0; U n = 0, comp = 0, raw_total = 0, hashes = 0, atoms = 0, values = 0;
    ZSTD_DCtx* ctx = ZSTD_createDCtx();
    while (std::cin >> off >> len) {
        V data(base + off, len);
        Reader h{data}; h.take(16); U np = h.little(2); h.take(32); h.pos += np * 48;
        U start = h.pos + (2 * np + 1) * 40; U sz = h.little(4), l = h.little(4);
        V frame = data.substr(start, l);
        auto t = Clock::now(); auto d = sha(frame); t_sha += elapsed(t);
        S dict(sz, '\0');
        t = Clock::now(); ZSTD_decompress(dict.data(), dict.size(), frame.data(), frame.size()); t_zstd += elapsed(t);
        t = Clock::now(); ZSTD_decompressDCtx(ctx, dict.data(), dict.size(), frame.data(), frame.size()); t_zstd_ctx += elapsed(t);
        comp += l; raw_total += sz; ++n;
        Reader r{dict}; r.take(4);
        t = Clock::now();
        U c = r.var(); std::vector<V> headers; for (U i = 0; i < c; ++i) headers.push_back(r.bytes());
        t_head += elapsed(t); t = Clock::now();
        c = r.var(); std::vector<V> vals; vals.reserve(c); for (U i = 0; i < c; ++i) vals.push_back(r.bytes()); values += c;
        t_val += elapsed(t); t = Clock::now();
        static constexpr U ra[] = {0, 1, 1, 2, 3, 2, 2, 1, 5, 1, 1};
        c = r.var(); hashes += c; std::vector<std::uint32_t> ho; ho.reserve(c);
        for (U i = 0; i < c; ++i) { U m = r.byte(); ho.push_back(r.pos); if (!m) r.take(32); else for (U j = 0; j < ra[m] + (m == 9); ++j) r.skip(); }
        t_hash += elapsed(t); t = Clock::now();
        c = r.var(); atoms += c; std::vector<std::uint32_t> ao; ao.reserve(c);
        for (U i = 0; i < c; ++i) { r.var(); U k = r.var(); ao.push_back(r.pos); if (k == 4) { r.skip(); U cnt = r.var(); for (U j = 0; j < cnt; ++j) r.skip(); } else for (U j = 0; j < (k == 3 ? 3U : 1U); ++j) r.skip(); }
        t_atom += elapsed(t); t = Clock::now();
        c = r.var(); for (U i = 0; i < c; ++i) { U s = r.var(); for (U j = 0; j < s; ++j) r.var(); }
        t_shape += elapsed(t);
    }
    auto ms = [&](double s) { return 1000 * s / n; };
    std::cout << n << " dictionaries; compressed " << comp / n / 1024 << " KiB, raw " << raw_total / n / 1024 << " KiB; values " << values / n << ", hashes " << hashes / n << ", atoms " << atoms / n << "\n"
              << "ms each: sha " << ms(t_sha) << ", zstd " << ms(t_zstd) << " (reused context " << ms(t_zstd_ctx) << "), headers " << ms(t_head)
              << ", values " << ms(t_val) << ", hashes " << ms(t_hash) << ", atoms " << ms(t_atom) << ", shapes " << ms(t_shape) << "\n";
}
