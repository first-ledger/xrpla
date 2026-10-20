// Build the XTXI transaction index for one or more XRPP containers, as
// tx_fingerprint_index.build_compact does (byte for byte): per-ledger counts and
// each transaction's first 8 hash bytes, read from the containers' transaction
// pages without decoding metadata. Used when pieces roll up into a bundle.
//
//   xrpla_tx_index --out INDEX CONTAINER...   (containers in ledger order, contiguous)
#include "history_reader.hpp"
#include <bit>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
using namespace xrpla::history;

namespace {
constexpr U sample = 256;
struct Bits {
    S buf; U acc = 0; unsigned pending = 0;
    U size() const { return 8 * buf.size() + pending; }
    void put(U value, unsigned width) {
        require(width == 64 || value >> width == 0, "Bit field overflow");
        acc |= value << pending; pending += width;
        while (pending >= 8) { buf += char(acc & 255); acc >>= 8; pending -= 8; }
    }
    S bytes() const { return pending ? buf + char(acc) : buf; }
};
U ceil_log2(U n) { return n <= 1 ? 0 : std::bit_width(n - 1); }

S build_compact(U first, std::vector<std::uint32_t> const& counts, std::vector<U> const& prefixes, unsigned fbits = 16) {
    U n = prefixes.size(), total = 0;
    for (auto c : counts) total += c;
    require(n <= (U(1) << 40) && fbits >= 1 && fbits <= 32 && total == n, "Index bounds");
    unsigned q = static_cast<unsigned>(std::max<U>(1, ceil_log2(std::max<U>(n, 2))));
    unsigned lbits = q, width = q + fbits;
    unsigned partition_bits = std::min<unsigned>(q, std::max<int>(8, int(width + lbits) - 64));
    unsigned rest_bits = width - partition_bits;
    require(width <= 64 && rest_bits + lbits <= 64, "Index too large for 64-bit entries");
    std::vector<std::vector<U>> parts(U(1) << partition_bits);
    U rest_mask = rest_bits == 64 ? ~U(0) : (U(1) << rest_bits) - 1, location_mask = (U(1) << lbits) - 1;
    for (U ordinal = 0; ordinal < n; ++ordinal) {
        U value = prefixes[ordinal] >> (64 - width);
        parts[value >> rest_bits].push_back(((value & rest_mask) << lbits) | ordinal);
    }
    Bits bitmap, fingerprints, locations; S samples;
    U mask = (U(1) << fbits) - 1, low_bits = q - partition_bits, current = ~U(0), cursor = 0;
    std::vector<U> entries;
    for (U bucket = 0; bucket < (U(1) << q); ++bucket) {
        if (bucket % sample == 0) le(samples, bitmap.size(), 8);
        U partition = bucket >> low_bits;
        if (partition != current) {
            current = partition; entries = std::move(parts[partition]); std::vector<U>().swap(parts[partition]);
            std::sort(entries.begin(), entries.end()); cursor = 0;
        }
        U low = bucket & ((U(1) << low_bits) - 1);
        while (cursor < entries.size() && ((entries[cursor] >> lbits) >> fbits) == low) {
            U entry = entries[cursor++];
            bitmap.put(1, 1); fingerprints.put((entry >> lbits) & mask, fbits); locations.put(entry & location_mask, lbits);
        }
        bitmap.put(0, 1);
    }
    S ledger_counts;
    for (auto c : counts) vu(ledger_counts, c);
    S sections[] = {bitmap.bytes(), samples, fingerprints.bytes(), locations.bytes(), ledger_counts};
    S out("XTXI", 4);
    le(out, 1, 2); out += char(q); out += char(fbits); out += char(lbits); out += char(0);
    le(out, first, 4); le(out, n, 8); le(out, counts.size(), 8);
    for (auto const& s : sections) le(out, s.size(), 8);
    for (auto const& s : sections) out += s;
    return out + sha(out);
}

struct Mapped {
    int fd = -1; void* data = nullptr; std::size_t size = 0;
    explicit Mapped(S const& path) {
        fd = ::open(path.c_str(), O_RDONLY); require(fd >= 0, "Cannot open container");
        size = std::filesystem::file_size(path);
        data = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0); require(data != MAP_FAILED, "Cannot map container");
    }
    ~Mapped() { if (data && data != MAP_FAILED) ::munmap(data, size); if (fd >= 0) ::close(fd); }
    V view() const { return {static_cast<char const*>(data), size}; }
};
U little(V data, U at, unsigned width) { Reader r{data.substr(at, width)}; return r.little(width); }
}

int main(int argc, char** argv) {
    try {
        S out; std::vector<S> containers;
        for (int i = 1; i < argc; ++i) {
            S a = argv[i];
            if (a == "--out") { require(i + 1 < argc, "Missing --out value"); out = argv[++i]; }
            else containers.push_back(a);
        }
        require(!out.empty() && !containers.empty(), "Usage: xrpla_tx_index --out INDEX CONTAINER...");
        auto started = Clock::now();
        std::vector<std::uint32_t> counts; std::vector<U> prefixes; U first = 0, expected = 0;
        for (auto const& path : containers) {
            Mapped file(path); auto data = file.view();
            // XRPP directory (tools/xrpp.py): header, one row per segment, index descriptor.
            require(data.size() >= 92 + 48 && data.substr(0, 4) == "XRPP", "Invalid container header");
            U cfirst = little(data, 8, 4), clast = little(data, 12, 4), n = little(data, 16, 4);
            if (!expected) first = expected = cfirst;
            require(cfirst == expected && clast >= cfirst && n >= 1, "Containers must be contiguous and in order");
            for (U i = 0; i < n; ++i) {
                U at = 92 + 56 * i, sfirst = little(data, at, 4), scount = little(data, at + 4, 4), offset = little(data, at + 8, 8), length = little(data, at + 16, 8);
                auto segment = data.substr(offset, length);
                require(sha(segment) == data.substr(at + 24, 32), "Segment checksum differs");
                Archive archive(segment, true);
                require(archive.first == sfirst && archive.count == scount && sfirst == expected, "Segment range differs");
                for (U seq = sfirst; seq < sfirst + scount; ++seq) {
                    auto const& txs = archive.transactionsOf(seq);
                    counts.push_back(static_cast<std::uint32_t>(txs.size()));
                    for (auto tx : txs) {
                        auto h = half(S("TXN\0", 4) + S(tx));
                        U prefix = 0; for (int b = 0; b < 8; ++b) prefix = (prefix << 8) | static_cast<unsigned char>(h[b]);
                        prefixes.push_back(prefix);
                    }
                }
                expected = sfirst + scount;
            }
            require(expected == clast + 1, "Container segments do not reach its last ledger");
        }
        auto index = build_compact(first, counts, prefixes);
        save(out, index);
        std::cout << Json{{"first", first}, {"ledgers", counts.size()}, {"transactions", prefixes.size()}, {"bytes", index.size()},
                          {"sha256", hex(sha(index))}, {"seconds", elapsed(started)}}.dump() << '\n';
        return 0;
    } catch (std::exception const& e) { std::cerr << Json{{"error", e.what()}}.dump() << '\n'; return 1; }
}
