# Query benchmarks

Measured 2026-10-05 and 2026-10-06. Raw results, scripts and per-run notes are under [`docs/benchmarks/2026-10-query/`](../benchmarks/2026-10-query/).

This compares three ways of answering xrpld-style queries over XRPL history, using the same ledgers, the same requests and the same machine:

| Configuration | How it is built | Main use |
|---|---|---|
| **XRPLA compressed piece cache** (the main design) | `xrpla sync` downloads published bundles, pieces and snapshots as they are in R2, then builds query indexes (`xrpla index-pieces` builds any still missing). `xrpla serve --dir CACHE` answers from the compressed data. | Restoring from R2 and standalone nodes: full history in about 1.3–1.6 TiB |
| **XRPLA RocksDB store** | `xrpla restore --to store` replays every ledger into RocksDB. `xrpla serve --dir OUT` answers from it. | A faster query copy where disk allows (about 1 KB per transaction); the input the Clio restore builds from |
| **xrpld 3.4.0** | `xrpla restore --to nudb`, then `xrpld --standalone --ledger LAST --valid` | The reference implementation and the answers to match |

Clio has not been measured yet: a fair comparison needs a local Clio with ScyllaDB or Cassandra loaded with the same ledgers.

## Summary

- **Correctness:** both XRPLA configurations returned exactly xrpld's answers on every comparable request: 3,509 of 3,509, across `ledger`, `tx`, `transaction_entry`, `ledger_entry`, the `account_*` family, `gateway_balances`, `noripple_check`, `deposit_authorized`, `book_offers`, `book_changes`, `ledger_data`, NFT offers and `version`.
- **RocksDB store:** close to xrpld on almost every command.
  - Median 0.3–3 ms, p95 under 10 ms except `book_offers` (5 ms median against 1 ms) and the biggest 2014 gateways' `gateway_balances`/`account_currencies` (p95 48–67 ms against 12–13 ms).
  - Over WebSocket, with 16 requests in flight: about 680 `tx`, 4,100–4,500 `account_info` and 6,000–6,400 `ledger` requests a second.
- **Compressed piece cache, as first measured:** medians close to the store's, but slow tails.
  - State reads at past ledgers had p95s of 60–900 ms.
  - Nearly all of it was loading segments' compression dictionaries (about 78 ms each), with only 12 segments kept open.
- **After round 2 (see below), at the default memory budget** (an eighth of RAM; 15.6 GB here):
  - The piece cache matches the store warm on nearly every command. The whole request set took 11 s warm against the store's 12 s, and 23 s cold against 16 s.
  - Cold p95s fell to tens of milliseconds.
  - With a 3 GB budget the tails remained (p95s of 30–350 ms), from a cold-read floor of about 50–70 ms per segment spent parsing its dictionary.
- **After round 3 (see below):** a segment's dictionary loads in about 30 ms instead of about 58 ms, and takes about 20 MB in memory instead of about 43 MB.
  - With a 3 GB budget, the whole request set takes 18 s warm instead of 36 s. Most p95s fall from hundreds of milliseconds to tens.
  - What remains at small budgets is decoding pages, not loading segments.
- **After round 4 (see below):** a read of one ledger builds only that ledger instead of its whole page. It resumes the page's parse where the segment's last decode stopped, and serving no longer re-checks page digests.
  - With a 3 GB budget, the whole request set takes 10 s warm instead of 18 s, the same as the default budget, in 136 MB of decoded ledgers instead of 966 MB.
  - NFT commands on 2025 data at 3 GB: warm p95s fall from 28–111 ms to 0.4–6 ms, and each NFT run takes a third less time.
  - Cold reads of 2025 objects spread over many ledgers are slower at the median than whole-page decodes were (`account_nfts` 1.0 → 3.5 ms).
- **After round 5 (see below):** a 2025 segment opens in about 80 ms instead of 104, its dictionary takes 60 MB in memory instead of 80, and WebSocket responses no longer wait 40 ms on delayed ACKs: `tx` p95 0.8 ms instead of 42, and 2.5–3.8 times the requests a second.
- **NFT commands** (`nft_info`, `nft_history`, `nfts_by_issuer`, `account_nfts`, NFT offers, on 2025 data): the store answers in 0.4–3 ms median and under 10 ms max. The piece cache answers in 0.4–7.5 ms median, with a p95 up to 225 ms (`nfts_by_issuer`).
- **Disk:** for the same 300,000 ledgers of 2014 (about 4.4 M transactions), xrpld needs 41 GB and the store 4.6 GB. The piece cache holds the whole million-ledger bundle in 5.1 GB.

## Test setup

- **Machine:** Hetzner, 48 cores, 125 GB RAM, two NVMe drives in RAID 1, Ubuntu. The production backfills and the publication audit ran at the same time (load average 10–16), so absolute numbers are pessimistic and somewhat noisy.
- **Builds:** XRPLA `6e084e3` for the final comparison (earlier rounds noted below), xrpld 3.4.0 from packages.xrplf.org. Both ran on the same machine and were queried over loopback.
- **Data, 2014:** ledgers 8,000,001–8,300,000 from the published early 8–9 M bundle.
  - Busy 2014 order books (CNY, USD, EUR, JPY and BTC against XRP).
  - Gateways with tens of thousands of trust lines.
  - Almost no NFTs.
- **Data, 2025:** ledgers 91,000,001–91,050,000 from the 91,000,000 snapshot (about 15 M objects) and its first piece, NFT-heavy.
- **xrpld standalone:** a standalone xrpld treats only its tip ledger as validated.
  - Its `tx` answers say `validated: false`.
  - Its `account_tx` errors or returns nothing for older ledgers, so its `account_tx` times are not comparable.
  - These artifacts make up the 933 reported "differences"; there were no other differences.

### Tools

- **All commands, against xrpld:** [`tools/xrpld_parity.py`](../../tools/xrpld_parity.py) samples 40 random ledgers and derives requests from what they contain:
  - accounts, transactions, order books, objects and NFTs;
  - continuation pages through markers, with both API versions.

  It sends each request to both servers, compares the answers and times each server: median, p95, max and the slowest requests. `--range` is for a standalone xrpld; `--clio-methods` times Clio-only commands on XRPLA alone. Each run is about 4,400 requests.
- **Latency and throughput:** [`latency-bench.py`](../benchmarks/2026-10-query/scripts/latency-bench.py) uses 2,000 `tx` by hash, 500 `ledger` by hash and 289 `account_info` requests over HTTP and over one WebSocket, first one at a time and then with 16 in flight.
- **NFT:** [`nft-bench.py`](../benchmarks/2026-10-query/scripts/nft-bench.py) samples NFTs, issuers, owners and offered tokens from every 25th ledger and times:
  - `nft_info` at two ledgers;
  - `nft_history` (pages of 20 and 100, both directions, continued);
  - `nfts_by_issuer` (paged, with and without a taxon);
  - `account_nfts` (pages of 20 and 400);
  - NFT buy and sell offers.
- **Cold and warm:** cold is the first pass on a freshly started server; warm repeats the same requests on the same server. The operating system's file cache was warm for both, since the data had just been written or read.

## Disk footprint

Measured:

| Data | Piece cache | RocksDB store | xrpld (NuDB + SQLite) |
|---|---|---|---|
| 2014: 8.0–8.3 M (4.4 M transactions) | 5.1 GB for the whole 8–9 M bundle (14.8 M transactions): 4,458 MB bundle, 640 MB indexes (612 MB of it the main query index), 23 MB snapshot | 4.6 GB | 41 GB |
| 2025: 91.00–91.05 M (about 3.3 M transactions), with the 91 M snapshot | 4.9 GB: 658 MB piece, 1,791 MB snapshot, 2,437 MB indexes (1,936 MB the snapshot's state index, 303 MB NFT state, 94 MB NFTs by issuer, 106 MB main index) | 6.3 GB | — |

Per transaction:

| Configuration | Bytes per transaction |
|---|---|
| Published bundle | 190–230 (316 in 2014's 8–9 M) |
| Main query index | 35–45 |
| RocksDB store | about 1,050 (2014) |
| xrpld | about 9,300 (2014) |

**Full history** (about 5.25 B transactions to ledger 107 M: the 2.19 B published by 2026-10-06, plus a per-ledger sample of the rest scaled by how the sample compared with what was published):

| Configuration | Estimate | Fits 2 TB (1.82 TiB)? |
|---|---|---|
| Published in R2 | about 1.1 TiB (1.03 TiB bundles, about 0.09 TiB snapshots) | — |
| Piece cache: bundles + main index + NFT indexes + one snapshot | about 1.25–1.6 TiB | Yes, with about 0.2–0.5 TiB headroom |
| RocksDB store | about 5 TB | No |
| xrpld | tens of TB | No |

The piece cache range is wide because the share taken by indexes varies by era: 14% in 2014, 21% in 2016, and much more in the NFT era, where the NFT indexes are sized by the NFTs that exist. The NFT index sizes at full-history scale are not yet measured.

## Results: all commands, against xrpld (2014 data)

Times in milliseconds, median / p95, from `xrpld_parity.py` with about 4,400 requests per run. Sorted by the piece cache's warm p95.

| Command | n | Pieces, cold | Pieces, warm | Store, cold | Store, warm | xrpld |
|---|---|---|---|---|---|---|
| `ledger_data` | 20 | 0.78 / 903 | 0.91 / 759 | 0.71 / 3.4 | 0.54 / 2.4 | 0.57 / 1.8 |
| `account_mptokens` (Clio) | 40 | 96 / 352 | 70 / 330 | 0.85 / 4.0 | 0.44 / 2.3 | — |
| `gateway_balances` | 113 | 1.2 / 308 | 1.2 / 300 | 0.55 / 47 | 0.62 / 48 | 0.58 / 12 |
| `account_currencies` | 113 | 1.7 / 353 | 1.7 / 274 | 0.62 / 154 | 0.71 / 67 | 0.64 / 13 |
| `book_offers` | 40 | 119 / 326 | 92 / 272 | 8.2 / 27 | 5.3 / 12 | 1.1 / 1.6 |
| `noripple_check` | 226 | 0.88 / 298 | 0.86 / 236 | 0.64 / 8.0 | 0.69 / 6.6 | 0.71 / 4.1 |
| `account_lines` | 355 | 0.75 / 156 | 0.73 / 113 | 0.56 / 4.3 | 0.59 / 3.4 | 0.62 / 2.3 |
| `account_tx` | 452 | 5.3 / 113 | 4.2 / 95 | 3.6 / 8.6 | 3.0 / 8.3 | not comparable |
| `ledger` (all forms) | 210 | 2.5 / 85 | 1.8 / 69 | 1.8 / 9.1 | 1.7 / 9.9 | 1.2 / 4.3 |
| `account_info` | 226 | 0.47 / 82 | 0.43 / 65 | 0.49 / 1.0 | 0.51 / 1.1 | 0.56 / 1.2 |
| `deposit_authorized` | 113 | 0.61 / 75 | 0.51 / 61 | 0.60 / 0.94 | 0.50 / 0.95 | 0.58 / 1.1 |
| `account_channels` | 113 | 0.73 / 5.0 | 0.70 / 5.0 | 0.45 / 3.4 | 0.55 / 3.0 | 0.58 / 2.3 |
| `account_offers` | 355 | 0.73 / 3.4 | 0.67 / 3.9 | 0.51 / 2.2 | 0.59 / 2.5 | 0.60 / 1.8 |
| `account_objects` | 242 | 0.99 / 2.2 | 0.95 / 2.5 | 0.80 / 1.6 | 0.91 / 1.7 | 0.79 / 1.4 |
| `book_changes` | 42 | 0.86 / 2.1 | 0.90 / 2.3 | 0.88 / 2.5 | 1.2 / 3.0 | 0.89 / 2.3 |
| `account_nfts` | 226 | 0.34 / 0.64 | 0.33 / 0.80 | 0.38 / 0.97 | 0.42 / 0.79 | 0.48 / 1.1 |
| `tx` | 480 | 0.57 / 1.0 | 0.53 / 0.80 | 0.62 / 1.0 | 0.62 / 0.90 | 0.56 / 0.72 |
| `ledger_entry` | 913 | 0.42 / 1.0 | 0.28 / 0.69 | 0.32 / 0.71 | 0.33 / 0.60 | 0.40 / 0.78 |
| `transaction_entry` | 160 | 0.51 / 0.72 | 0.47 / 0.68 | 0.59 / 0.75 | 0.58 / 0.91 | 0.54 / 0.67 |
| `server_info`, `ledger_range`, `ping`, `version` | — | 0.2–0.6 | | 0.2–0.4 | | 0.3–0.4 |

Total time for the whole run: pieces 87 s cold, 63 s warm; store 16 s cold, 12 s warm.

**Slowest requests on the piece cache** (warm):
- `noripple_check` for gateway `rMAz5ZnK…` took 3.1 s, and for `rnuF96W4…` 2.6 s.
- `account_mptokens` for `rM3X3QSr…` took 1.2 s.
- `account_lines` for `rM3X3QSr…` took 0.85 s.
- `ledger_data` with `type: offer` at 8,300,000 took 0.66–0.76 s.

## Results: latency and throughput (2014 data)

Each cell is median / p95 in milliseconds, then requests per second over one WebSocket with 16 in flight.

| | Pieces, cold | Pieces, warm | Store, cold | Store, warm |
|---|---|---|---|---|
| `tx` by hash, HTTP | 2.2 / 83 | 1.4 / 65 | 0.86 / 1.5 | 0.57 / 0.90 |
| `tx` by hash, WebSocket | 1.3 / 58; 578/s | 1.3 / 61; 579/s | 0.61 / 42; 686/s | 0.56 / 42; 677/s |
| `ledger` by hash, HTTP | 0.36 / 1.1 | 0.26 / 0.30 | 0.48 / 0.84 | 0.25 / 0.34 |
| `ledger` by hash, WebSocket | 0.10 / 0.18; 6,628/s | 0.12 / 0.16; 6,629/s | 0.13 / 0.19; 6,391/s | 0.13 / 0.20; 6,038/s |
| `account_info`, HTTP | 2.0 / 84 | 2.1 / 90 | 0.44 / 0.64 | 0.29 / 0.37 |
| `account_info`, WebSocket | 0.95 / 75; 4,121/s | 0.88 / 87; 3,993/s | 0.13 / 0.22; 4,489/s | 0.14 / 0.26; 4,146/s |

Notes:
- `tx` over WebSocket had a p95 of about 42 ms on the store too, which HTTP didn't show. It was Nagle's algorithm meeting the client's delayed ACK (about 40 ms) on responses spanning more than one TCP segment; since round 5 the server sets TCP_NODELAY (see "Piece cache, round 5").
- Throughput with 16 in flight is similar between the configurations because most requests are fast. The slow-tail requests are a minority.

## Results: NFT commands (2025 data)

XRPLA only: xrpld has no `nft_info`, `nft_history` or `nfts_by_issuer`. Median / p95 / max in milliseconds.

| Command | n | Pieces, cold | Pieces, warm | Store, cold | Store, warm |
|---|---|---|---|---|---|
| `nft_info` | 300 | 1.1 / 59 / 455 | 0.72 / 56 / 396 | 0.38 / 0.69 / 7.6 | 0.38 / 0.60 / 8.7 |
| `nft_history` | 300 | 6.0 / 103 / 262 | 5.2 / 95 / 262 | 2.8 / 4.8 / 9.3 | 3.1 / 6.2 / 11 |
| `nfts_by_issuer` | 127 | 7.5 / 228 / 512 | 5.2 / 225 / 385 | 1.2 / 3.3 / 8.4 | 1.0 / 1.7 / 2.7 |
| `account_nfts` | 421 | 1.6 / 39 / 535 | 1.6 / 44 / 544 | 0.75 / 3.9 / 5.6 | 0.68 / 4.3 / 6.9 |
| `nft_buy_offers` | 150 | 0.57 / 14 / 248 | 0.40 / 13 / 248 | 0.42 / 0.58 / 1.5 | 0.37 / 0.48 / 0.59 |
| `nft_sell_offers` | 150 | 0.62 / 30 / 276 | 0.42 / 25 / 304 | 0.49 / 0.75 / 1.1 | 0.36 / 0.46 / 0.67 |

The sample covered 519 NFTs, 29 issuers, 208 owners and 224 offered NFTs. "Not found" answers (offers already taken, tokens minted after the ledger asked) are counted as requests; the server answered them correctly.

## Piece cache, round 2

Changes to how the piece cache reads, measured with the same data and requests. Raw results are in [`benchmarks/2026-10-query/pieces2/`](../benchmarks/2026-10-query/pieces2/).

**Where the time went.** The server's stage timers (`xrpla_cache_stats`) over one all-commands run on the first build:

| Stage | Total | Count | Per event |
|---|---|---|---|
| Loading segment dictionaries | 117 s | 1,499 segment misses | about 78 ms |
| Decoding pages | 33 s | 2,437 page misses | about 13 ms |
| Opening segments | 0.4 s | | |

Only 12 segments stayed open, because each was charged a flat 150 MB. A loaded dictionary is about 45 MB in memory: about 7 MB decompressed (14 ms to decompress), and the rest is the tables parsed from it.

**Changes:**
1. Open segments are charged their actual dictionary size, so 43 fit in 3 GB instead of 12.
2. Bulk object reads (big owner directories: `gateway_balances`, `account_currencies`) go through the shared segment and page caches. A repeat used to redo everything: 720 ms; now 68 ms.
3. Finding a page within a segment reads the segment's header without loading its dictionary.
4. The default `--cache-mb` is an eighth of memory, between 3,072 and 32,768 MB, instead of a fixed 3,072. The setup wizard, `docs/SETUP.md` and `config/xrpla.example.toml` follow it.

**Results** (median / p95 in ms; "before" is the first measurement above, at its default 3 GB):

| Command | Pieces before, warm | Pieces 3 GB, warm | Pieces default, cold | Pieces default, warm | Store, warm | xrpld |
|---|---|---|---|---|---|---|
| `ledger_data` | 0.91 / 759 | 1.25 / 349 | 0.64 / 33 | 0.57 / 4.7 | 0.54 / 2.4 | 0.57 / 1.8 |
| `account_mptokens` | 70 / 330 | 50 / 274 | 4.6 / 28 | 0.75 / 3.7 | 0.44 / 2.3 | — |
| `gateway_balances` | 1.2 / 300 | 1.3 / 42 | 1.2 / 34 | 1.3 / 36 | 0.62 / 48 | 0.58 / 12 |
| `account_currencies` | 1.7 / 274 | 1.8 / 201 | 1.4 / 150 | 1.3 / 31 | 0.71 / 67 | 0.64 / 13 |
| `book_offers` | 92 / 272 | 49 / 287 | 20 / 62 | 5.4 / 25 | 5.3 / 12 | 1.1 / 1.6 |
| `noripple_check` | 0.86 / 236 | 0.82 / 9.6 | 0.83 / 9.0 | 0.80 / 11 | 0.69 / 6.6 | 0.71 / 4.1 |
| `account_lines` | 0.73 / 113 | 0.75 / 97 | 0.74 / 25 | 0.77 / 4.1 | 0.59 / 3.4 | 0.62 / 2.3 |
| `account_tx` | 4.2 / 95 | 4.6 / 93 | 4.3 / 38 | 2.9 / 8.3 | 3.0 / 8.3 | not comparable |
| `ledger` | 1.8 / 69 | 2.3 / 36 | 2.6 / 82 | 0.95 / 6.4 | 1.7 / 9.9 | 1.2 / 4.3 |
| `account_info` | 0.43 / 65 | 0.43 / 61 | 0.43 / 7.5 | 0.42 / 0.69 | 0.51 / 1.1 | 0.56 / 1.2 |
| `deposit_authorized` | 0.51 / 61 | 0.47 / 28 | 0.51 / 8.7 | 0.42 / 0.67 | 0.50 / 0.95 | 0.58 / 1.1 |

The other commands were already within about 1 ms of the store and stayed there.

Whole-run time and segment misses (warm run's cache statistics):

| Configuration | Cold | Warm | Segment misses | Dictionary time |
|---|---|---|---|---|
| Pieces before | 87 s | 63 s | 1,499 | 117 s |
| Pieces 3 GB | 40 s | 36 s | 1,357 | 84 s |
| Pieces default (15.6 GB) | 23 s | 11 s | 83 | 4.7 s |
| Store | 16 s | 12 s | — | — |

The 3 GB numbers cover the cold and warm runs together.

**NFT commands** (2025 data; median / p95 / max in ms):

| Command | Pieces before, cold | Pieces default, cold | Pieces default, warm | Pieces 3 GB, warm | Store, warm |
|---|---|---|---|---|---|
| `nft_info` | 1.1 / 59 / 455 | 0.63 / 5.1 / 44 | 0.44 / 0.98 / 44 | 0.99 / 99 / 505 | 0.38 / 0.60 / 8.7 |
| `nft_history` | 6.0 / 103 / 262 | 4.0 / 8.3 / 87 | 3.0 / 7.3 / 86 | 6.7 / 141 / 650 | 3.1 / 6.2 / 11 |
| `nfts_by_issuer` | 7.5 / 228 / 512 | 4.7 / 13 / 75 | 3.3 / 9.4 / 72 | 5.7 / 316 / 444 | 1.0 / 1.7 / 2.7 |
| `account_nfts` | 1.6 / 39 / 535 | 1.2 / 6.7 / 31 | 0.59 / 6.0 / 26 | 1.8 / 117 / 656 | 0.68 / 4.3 / 6.9 |
| `nft_buy_offers` | 0.57 / 14 / 248 | 0.82 / 3.1 / 5.5 | 0.27 / 0.40 / 0.73 | 0.44 / 15 / 215 | 0.37 / 0.48 / 0.59 |
| `nft_sell_offers` | 0.62 / 30 / 276 | 0.99 / 6.4 / 28 | 0.27 / 0.71 / 19 | 0.50 / 37 / 334 | 0.36 / 0.46 / 0.67 |

**Latency, piece cache at the default budget** (median / p95 in ms; then requests/s with 16 in flight over one WebSocket):

| Request | HTTP, warm | WebSocket, warm |
|---|---|---|
| `tx` by hash | 0.64 / 1.3 | 0.53 / 42; 704/s |
| `ledger` by hash | 0.30 / 0.36 | 0.10 / 0.14; 7,296/s |
| `account_info` | 0.33 / 0.47 | 0.16 / 0.30; 4,475/s |

**What this does not show.** These 300,000 ledgers touch about 80 segments, which fit in 15.6 GB. A full-history cache has thousands. Recent ledgers, which most queries ask about, stay in memory. A query deep into history still pays about 50–70 ms per segment it has to load, almost all of it parsing the dictionary into tables that also take about 45 MB each. The next step is in the history decoder: parse the dictionary faster, and hold it in less memory so more segments fit.

## Piece cache, round 3

A change to the history decoder (`tools/history_reader.hpp`), measured with the same data and requests. Raw results are in [`benchmarks/2026-10-query/pieces3/`](../benchmarks/2026-10-query/pieces3/).

**Where a segment's dictionary load went.** New timers in `xrpla_cache_stats` (`dictionaries`) split it in two. On round 2's build, over one run at 3 GB:
- reading the dictionary (checksum and zstd): about 17 ms;
- parsing it into tables: about 41 ms;
- memory: about 43 MB per open segment.

A 2014 segment's dictionary holds about 470,000 atoms and 130,000 hash recipes. Their arguments came to about 2.2 million numbers, decoded one by one into 8-byte table entries. That decoding was most of the parse and most of the memory.

**Change:** loading now records where each atom's and hash recipe's arguments start in the dictionary, and skips over them. They are decoded when the entry is first rendered, which is already memoized. Validation is unchanged, except that atom headers are now checked at load. The archive format is unchanged.

**Result:**

| | Round 2 | Round 3 |
|---|---|---|
| Dictionary parse, per segment | about 41 ms | about 14 ms |
| Dictionary read, per segment | about 17 ms | about 16 ms |
| Memory per open segment | about 43 MB | about 20 MB |
| Segments open at 3 GB | 43 | all 78 (1.5 GB) |
| Segment misses at 3 GB, cold and warm runs | 1,357 | 83 |

**Results** (median / p95 in ms):

| Command | Round 2, 3 GB, cold | Round 3, 3 GB, cold | Round 2, 3 GB, warm | Round 3, 3 GB, warm | Round 3, default, warm |
|---|---|---|---|---|---|
| `ledger_data` | 0.87 / 359 | 0.80 / 46 | 1.2 / 349 | 0.92 / 42 | 0.56 / 4.0 |
| `account_mptokens` | 47 / 312 | 10 / 56 | 50 / 274 | 9.4 / 45 | 0.89 / 3.8 |
| `account_currencies` | 1.7 / 205 | 1.6 / 82 | 1.8 / 201 | 1.6 / 60 | 1.5 / 37 |
| `book_offers` | 55 / 289 | 23 / 60 | 49 / 287 | 18 / 51 | 5.4 / 35 |
| `account_lines` | 0.87 / 102 | 0.83 / 26 | 0.75 / 97 | 0.84 / 15 | 0.75 / 3.6 |
| `account_tx` | 5.2 / 91 | 5.0 / 45 | 4.6 / 93 | 4.7 / 35 | 3.1 / 8.2 |
| `ledger` | 2.8 / 81 | 1.9 / 49 | 2.3 / 36 | 1.8 / 9.4 | 1.1 / 7.8 |
| `account_info` | 0.49 / 65 | 0.52 / 6.3 | 0.43 / 61 | 0.47 / 3.6 | 0.41 / 0.62 |
| `deposit_authorized` | 0.56 / 62 | 0.58 / 8.0 | 0.47 / 28 | 0.59 / 3.8 | 0.42 / 0.62 |

The other commands were within about 1 ms either way. At the default budget, this data already fit in round 2, and round 3 is the same within noise.

Whole-run time:

| Configuration | Cold | Warm | Dictionary time (cache statistics) |
|---|---|---|---|
| Round 2, 3 GB | 40 s | 36 s | 84 s |
| Round 3, 3 GB | 21 s | 18 s | 2.3 s |
| Round 2, default | 23 s | 11 s | 4.7 s |
| Round 3, default | 28 s | 11 s | 2.6 s |
| Store | 16 s | 12 s | — |

The default budget's cold run was 5 s slower in round 3, with the same segment loads and less dictionary time. That is within the run-to-run variation on this machine, which was running the backfills.

**NFT commands at 3 GB** (2025 data; median / p95 / max in ms):

| Command | Round 2, warm | Round 3, warm |
|---|---|---|
| `nft_info` | 0.99 / 99 / 505 | 0.85 / 46 / 57 |
| `nft_history` | 6.7 / 141 / 650 | 5.1 / 96 / 255 |
| `nfts_by_issuer` | 5.7 / 316 / 444 | 5.0 / 111 / 412 |
| `account_nfts` | 1.8 / 117 / 656 | 1.5 / 28 / 123 |
| `nft_buy_offers` | 0.44 / 15 / 215 | 0.44 / 8.3 / 24 |
| `nft_sell_offers` | 0.50 / 37 / 334 | 0.44 / 14 / 35 |

At the default budget, the NFT and latency results match round 2's within noise.

**What remains at 3 GB:** decoding pages. Over the cold and warm runs at 3 GB, the server decoded pages for 25 s in total: 6,040 misses at about 4 ms each. The page cache gets 30% of the budget (900 MB). The next section tests giving it more.

## Piece cache: splitting the budget

The piece cache's budget is split between open segments (60%), decoded pages (30%) and snapshot blocks (10%). Raw results are in [`benchmarks/2026-10-query/page-share/`](../benchmarks/2026-10-query/page-share/) and [`benchmarks/2026-10-query/shared-budget/`](../benchmarks/2026-10-query/shared-budget/).

**A larger fixed share for pages is worse.** This sweep used an experiment build that read the page share from an environment variable. Segments got 90% minus that share. The all-commands benchmark ran cold then warm on each server:

| Pages / segments | 3 GB, warm | 3 GB, segment loads | 3 GB, page misses | 1.5 GB, warm | 1.5 GB, segment loads |
|---|---|---|---|---|---|
| 30 / 60 | 15 s | 83 | 6,044 | 28 s | 1,259 |
| 45 / 45 | 16 s | 186 | 5,136 | 34 s | 2,023 |
| 60 / 30 | 21 s | 998 | 4,115 | 36 s | 3,039 |
| 75 / 15 | 18 s | 2,488 | 2,803 | 35 s | 3,956 |

A segment load (about 30 ms) costs several page decodes (about 4 ms), so room taken from segments loses more than pages gain. Even 2 GB of pages leaves 2,803 misses. Most are first reads of the run's roughly 2,300 distinct pages, which no cache size avoids.

**Change: one shared budget.** The three caches now share the budget. Each keeps its share as a guarantee and may use room the others leave free. While the total is over budget, a cache over its share evicts down to it, so a cache under its share gets its room back. `xrpla_cache_stats` reports the budget's total and use, and each cache's share.

| | Fixed split | Shared budget |
|---|---|---|
| 3 GB: pages held | 966 MB | 1,631 MB |
| 3 GB: page misses | 6,044 | 4,789 |
| 3 GB: page decode time | 25 s | 22 s |
| 3 GB: segment loads | 83 | 83 |
| 3 GB: cold / warm run | 33 s / 15 s | 23 s / 15 s |
| 1.5 GB: page misses | 7,023 | 6,792 |
| 1.5 GB: cold / warm run | 29 s / 28 s | 32 s / 31 s |

Per-command medians and p95s are the same within run-to-run noise. The shared budget never takes room from segments. It gives pages whatever the other caches leave unused: about 650 MB here at 3 GB, and more with a larger budget or fewer segments. What remains at small budgets is first reads of pages; decoding only the part of a page a request needs would cut that.

## Piece cache, round 4: decoding one ledger

Raw results are in [`benchmarks/2026-10-query/pieces4/`](../benchmarks/2026-10-query/pieces4/), with two variants that were dropped in [`pieces4-alone/`](../benchmarks/2026-10-query/pieces4-alone/) and [`pieces4-promote/`](../benchmarks/2026-10-query/pieces4-promote/). Decode costs come from [`scripts/page-probe.cpp`](../benchmarks/2026-10-query/scripts/page-probe.cpp).

**Where a page decode went.** The probe decodes pages of 13 segments each from the 2014 bundle (8–9 M) and the 2025 piece (91.00–91.05 M). Each mode runs on a freshly opened segment, then repeats warm, in ms:

| Decode | 2014, cold | 2014, warm | 2025, cold | 2025, warm |
|---|---|---|---|---|
| Whole page with its canonical digest (every transaction read before) | 13–16 | 4.2–4.5 | 56–61 | 22–23 |
| Whole page without the digest | 10.2–10.6 | 2.3–2.4 | 46 | 15 |
| Patches through mid-page (an object read before) | 4.5–4.6 | 1.0–1.1 | 15–16 | 5.6–5.7 |
| One ledger, mid-page | 2.4 | 0.77–0.80 | 9.2–9.7 | 4.3 |
| One ledger, mid-page, patches only | 2.2 | 0.75–0.78 | 7.5–7.9 | 4.1–4.2 |
| Two ledgers, mid then last, the second resuming | 3.2 | 1.2 | 13 | 6.4 |
| Every ledger of the page in turn, each resuming | 10.4 | 2.3 | 45 | 14 |

Ranges span the probe's runs. Reading and decompressing a page's sections takes under 1 ms. The rest is building ledgers from the dictionary, and every server read needs only one ledger of the page: `tx`, `ledger`, `account_tx`, NFT history and object reads. A page's records refer to earlier records of the same page, so a ledger cannot be decoded without the ones before it. They can be parsed without being built, which costs a fraction as much. Resuming where the last one-ledger decode stopped, the ledgers of a page decoded one by one in ascending order cost no more than the whole page.

**Changes:**
1. The reader decodes one ledger. It parses the page's earlier ledgers, updating only the page's view cache, and builds that ledger. It keeps where it stopped (the page's history section, its view cache and the next ledger's position; at most 1.3 MB per open segment in the probe), so a later ledger of the same page continues from there. `xrpla_history_decode --ledger` exposes it. The codec test checks every ledger against a full decode at three page sizes, and the probe checks every ledger of its pages decoded in turn and in reverse.
2. The server caches decoded ledgers instead of pages. Batched loads (`account_tx`, NFT and object prefetches, bulk object reads) decode each segment's ledgers together, in ascending order and under one hold of the segment, so they resume rather than restart.
3. The server skips the canonical page digest. `index-pieces` checked every page's digest when it built the indexes, as object reads already assumed; each section's checksum is still checked on read.

**Results** (median / p95 in ms):

| Command | Round 3, 3 GB, cold | Round 4, 3 GB, cold | Round 3, 3 GB, warm | Round 4, 3 GB, warm | Round 4, default, warm |
|---|---|---|---|---|---|
| `ledger_data` | 0.80 / 46 | 0.71 / 26 | 0.92 / 42 | 0.67 / 3.6 | 0.64 / 6.3 |
| `account_mptokens` | 10 / 56 | 5.2 / 30 | 9.4 / 45 | 0.72 / 3.7 | 0.78 / 4.2 |
| `book_offers` | 23 / 60 | 17 / 40 | 18 / 51 | 5.1 / 25 | 4.5 / 25 |
| `account_currencies` | 1.6 / 82 | 1.4 / 62 | 1.6 / 60 | 1.2 / 33 | 1.3 / 33 |
| `account_lines` | 0.83 / 26 | 0.75 / 16 | 0.84 / 15 | 0.62 / 2.6 | 0.69 / 3.6 |
| `account_tx` | 5.0 / 45 | 4.4 / 12 | 4.7 / 35 | 2.6 / 7.9 | 2.8 / 8.1 |
| `ledger` | 1.9 / 49 | 2.4 / 44 | 1.8 / 9.4 | 1.1 / 7.0 | 0.92 / 5.9 |
| `account_info` | 0.52 / 6.3 | 0.51 / 3.7 | 0.47 / 3.6 | 0.35 / 0.58 | 0.40 / 0.77 |
| `deposit_authorized` | 0.58 / 8.0 | 1.2 / 4.6 | 0.59 / 3.8 | 0.36 / 0.62 | 0.42 / 0.65 |
| `tx` | 0.51 / 0.69 | 0.50 / 0.64 | 0.55 / 0.71 | 0.60 / 0.82 | 0.61 / 1.0 |

| Configuration | Cold | Warm | Decoded memory | Decode time |
|---|---|---|---|---|
| Round 3, 3 GB | 21 s | 18 s | 966 MB | 25 s |
| Round 4, 3 GB | 17 s | 10 s | 136 MB | 7.9 s |
| Round 3, default | 28 s | 11 s | 2,023 MB | 16 s |
| Round 4, default | 27 s | 11 s | 136 MB | 8.9 s |
| Store | 16 s | 12 s | — | — |

The default budget's cold run is the first in each script, and the slowest in every round.

**NFT commands** (2025 data; median / p95 / max in ms, then the whole run's time):

| Command | Round 3, 3 GB, warm | Round 4, 3 GB, warm | Round 3, default, cold | Round 4, default, cold |
|---|---|---|---|---|
| `nft_info` | 0.85 / 46 / 57 | 0.42 / 0.66 / 1.3 | 0.48 / 4.7 / 53 | 0.95 / 9.6 / 17 |
| `nft_history` | 5.1 / 96 / 255 | 3.2 / 5.9 / 11 | 2.9 / 6.2 / 81 | 5.8 / 18 / 53 |
| `nfts_by_issuer` | 5.0 / 111 / 412 | 3.1 / 5.8 / 14 | 4.4 / 12 / 79 | 6.4 / 38 / 104 |
| `account_nfts` | 1.5 / 28 / 123 | 0.60 / 4.9 / 7.9 | 0.98 / 5.1 / 25 | 3.5 / 24 / 112 |
| `nft_sell_offers` | 0.44 / 14 / 35 | 0.32 / 0.49 / 0.92 | 0.81 / 5.1 / 28 | 0.81 / 9.4 / 23 |
| Whole run | 106 s | 66 s | 108 s | 85 s |

| NFT run | Round 3 | Round 4 |
|---|---|---|
| Default, cold / warm | 108 s / 84 s | 85 s / 72 s |
| 3 GB, cold / warm | 130 s / 106 s | 87 s / 66 s |

Warm and at 3 GB, the NFT commands answer as fast as at the default budget. Cold, the commands that read many objects have higher medians and p95s than in round 3, though each run as a whole takes less time. A 2025 page costs 4–5 times as much to decode as a 2014 page. Separate requests need ledgers of the same pages in no particular order, and a ledger before the segment's last-decoded one parses its page again from the start. In round 3, one decode of the page through the newest ledger needed served later requests too.

**Tried and dropped:**
- **One ledger at a time without resuming, and the whole page for 3 or more ledgers in a batch** ([`pieces4-alone/`](../benchmarks/2026-10-query/pieces4-alone/)). Each ledger decoded alone parsed its page from the start. 3 GB: 19 s cold and 11 s warm; NFT runs 88/70 s (default) and 98/74 s (3 GB). Resuming was faster in the other runs except at the default budget warm (NFT 72 s against 70 s; 2014 requests 11 s against 10 s), and holds about a third of the decoded memory.
- **Promoting a page after repeated reads** ([`pieces4-promote/`](../benchmarks/2026-10-query/pieces4-promote/)). The third ledger of a page read alone decoded the whole page, so that requests arriving one at a time would share it. Same on 2014 data; on 2025 data every NFT run took longer (108/75 s and 112/98 s), because whole 2025 pages are large and pushed each other out of a 3 GB cache.

## Piece cache, round 5: segment opens, resume points, WebSocket

Raw results are in [`benchmarks/2026-10-query/pieces5/`](../benchmarks/2026-10-query/pieces5/); decode and dictionary costs from [`scripts/page-probe.cpp`](../benchmarks/2026-10-query/scripts/page-probe.cpp) and [`scripts/dict-load-probe.cpp`](../benchmarks/2026-10-query/scripts/dict-load-probe.cpp). Nothing here changes the archive format or what is stored on disk.

**Where a segment open went.** A segment's dictionary load, per segment, over 13 segments each (warm OS cache; the 2025 checksum takes 24 ms when the file is read from disk):

| | 2014 | 2025 |
|---|---|---|
| Dictionary, compressed / whole | 2.1 MB / 7.5 MB | 11.2 MB / 28.2 MB |
| SHA-256 of the compressed section | 2.0 ms | 10.5 ms |
| zstd decompression | 11 ms | 51–63 ms |
| Parse: values / hashes / atoms | 1.1 / 1.4 / 7.3 ms | 5.4 / 3.5 / 23 ms |

The decompression is the data's own cost: the system's libzstd 1.5.5 takes as long on the same frames. A 2025 segment, which is what most of full history will look like, costs four times a 2014 one to open.

**Changes:**
1. **Leaner dictionaries.** A value is kept as its offset in the dictionary (4 bytes) instead of a view (16), and an atom record is 12 bytes instead of 16 (a 16-bit field header index). Loaded dictionaries take 21–26% less memory: 16.5 → 13.1 MB per 2014 segment, 80 → 60 MB per 2025 segment, so more segments stay open in a budget. Each open segment is now charged its dictionary plus 8 MB (4 MB before) for transaction pages and decode cursors.
2. **Overlapped open.** The dictionary decompresses on a second thread while the parse reads what has arrived, and its checksum is computed on a third; the dictionary is used only once the checksum matched, and a failure still reports a checksum mismatch first. A 2025 open: 104 → 80 ms. 2014: about the same (24 → 22 ms).
3. **Several resume points per segment.** Each open segment keeps up to 4 cursors; a ledger resumes from the cursor on its page nearest before it, and cursors on one page share its decompressed history section. A page's ledgers decoded in shuffled order, warm: 29 → 11 ms (2014), 160 → 63 ms (2025).
4. **`book_offers` price levels from one key walk.** The piece cache answers `book_directories` with its ordered key merge, instead of `book_offers` walking one successor at a time.
5. **TCP_NODELAY** on every accepted connection.

Varint skipping eight bytes at a time was tried in the dictionary scan and made no measurable difference; it is kept, being no slower.

**Results.** Whole runs:

| Run | Round 4 | Round 5 |
|---|---|---|
| 2014 requests, 3 GB, cold / warm | 17 s / 10 s | 14 s / 10 s |
| 2014 requests, default, cold / warm | 27 s / 11 s | 26 s / 9 s |
| NFT, default, cold / warm | 85 s / 72 s | 75 s / 59 s |
| NFT, 3 GB, cold / warm | 87 s / 66 s | 83 s / 68 s |
| Dictionary load, wall time per segment (2014, 3 GB) | 34 ms | 22 ms |

WebSocket, 16 requests in flight (median / p95 in ms; requests a second):

| Request | Round 4 | Round 5 |
|---|---|---|
| `tx` | 0.57 / 42; 687/s | 0.41 / 0.80; 2,438–2,650/s |
| `ledger` by hash | 0.11 / 0.17; 6,855/s | 0.11 / 0.14; 13,940–15,016/s |
| `account_info` | 0.13 / 0.22; 4,353/s | 0.13 / 0.20; 10,639–11,415/s |

Per command (median / p95 in ms):

| Command | Round 4, 3 GB, cold | Round 5, 3 GB, cold | Round 4, default, cold | Round 5, default, cold |
|---|---|---|---|---|
| `book_offers` | 17 / 40 | 14 / 40 | 18 / 67 | 16 / 49 |
| `account_currencies` | 1.4 / 62 | 1.3 / 56 | 1.6 / 86 | 1.3 / 65 |
| `ledger` | 2.4 / 44 | 1.7 / 28 | 2.0 / 44 | 2.1 / 35 |
| `account_lines` | 0.75 / 16 | 0.60 / 14 | 0.73 / 21 | 0.67 / 18 |
| `account_nfts` (2025) | 4.1 / 24 / 80 max | 3.3 / 22 / 66 max | 3.5 / 24 / 112 max | 2.4 / 18 / 54 max |
| `nfts_by_issuer` (2025) | 6.4 / 38 / 104 max | 5.6 / 30 / 100 max | 6.4 / 38 / 104 max | 5.5 / 36 / 79 max |

Warm results are within run-to-run noise of round 4's. Every xrpld comparison matched: 3,509 answers.

## How the RocksDB store got here

All on the 2014 data, against xrpld on the same machine. Each round's raw results are in [`benchmarks/2026-10-query/store/`](../benchmarks/2026-10-query/store/).

| Change | Commit | Effect |
|---|---|---|
| Starting point: the store was opened for every request | `34394f7` | about 8–10 ms per request, any command |
| Bloom (Ribbon) filters, open-file limit | `4794a8c` | no change while the store reopened per request |
| Keep the store open between requests; one block cache per process | `4794a8c` | `tx` 9.2 → 0.6 ms, `ledger` 8.2 → 0.3 ms, `account_info` 10.5 → 0.4 ms; WebSocket 110–130 → 650–5,700 requests/s |
| Reused iterators, batch reads, per-request object cache; `book_changes` reads only transactions | `d4d22aa` | `book_changes` 20 → 0.9 ms |
| Compaction after a restore | `d4d22aa` | `book_offers` 90 → 24 ms, `account_currencies` max 2.1 s → 0.24 s, `ledger_data` p95 16 → 5.7 ms |
| Key walks step over a key's few versions | `d4d22aa` | `book_offers` 24 → 12 ms |
| Order-book index (`book_dirs`) | `6e084e3` | `book_offers` 12 → 6 ms median, p95 63 → 18 ms |

Restores also got faster over the same period:
- **Store restore:** about 160 → 3,100 ledgers/s on 2014 data, from the in-memory replay verifier, decoding only NFT/MPT-bearing transactions and RocksDB bulk loading.
- **Clio restore:** about 3 → 220 ledgers/s on its plan step.

These are recorded in the commit history.

## Known slow spots and next steps

1. **Piece cache, cold segments:** opening a segment costs about 22 ms on 2014 data and about 80 ms on 2025 data (round 5), mostly zstd decompressing its dictionary. That is the format's cost: cutting it means a format change (for example a dictionary in several frames decompressed in parallel), which republication would need.
2. **Piece cache at small budgets** (3 GB): warm, within a few milliseconds of the default budget since round 4. Cold reads of 2025 objects spread over many ledgers of a page have higher medians than with whole-page decodes (`account_nfts` 3.5 ms against 1.0). Separate requests arrive in no particular ledger order, and an earlier ledger than the segment's last-decoded one parses its page again. More than one resume point per segment would cut that.
3. **`account_mptokens`** follows Clio's definition and reads up to `limit` owner-directory entries (200 by default) to find MPTokens. It is fast once the cache holds the account's segments; it is slow cold on accounts with many trust lines.
4. **Store `book_offers`:** about 3–5× xrpld. The remaining cost is reading each offer and its owner's funds from the version history; xrpld reads current state.
5. **Store `account_currencies` and `gateway_balances`** on the largest 2014 gateways: p95 48–67 ms against xrpld's 12–13 ms.
6. **`account_tx`:** 3–4 ms median on both XRPLA configurations. It builds each listed transaction in full; xrpld could not be compared here.
7. **WebSocket `tx`:** fixed in round 5 (TCP_NODELAY): p95 0.8 ms, about 2,500 requests a second.
8. **Full-history scale:** these tests use 300,000 ledgers that fit in memory. At full history:
   - cold reads go to disk;
   - the store's bloom filters and the piece cache's segment cache matter more;
   - key walks cross far more deleted keys.

   Next, the same benchmark should run on a piece cache of several million recent ledgers.
9. **Clio:** not measured. It needs a local Clio with ScyllaDB or Cassandra loaded with the same ledgers.

## Reproduce

The scripts are in [`benchmarks/2026-10-query/scripts/`](../benchmarks/2026-10-query/scripts/):
- `xrpld-bench-setup.sh`: the xrpld restore and standalone start.
- `pieces-bench.sh`: the piece cache, store, latency and NFT runs.
- `book-bench.sh` and `compact-bench.sh`: store rounds.

`tools/xrpld_parity.py` is installed with XRPLA. The ledgers come from the published `mainnet-early/` (8–9 M bundle) and `mainnet/` (91 M snapshot and first piece) prefixes.
