# Query benchmarks: XRPLA RocksDB store, 2026-10-05

These are the raw results for the RocksDB store rounds that `docs/internal/QUERY-BENCHMARKS.md` summarizes. All runs used the production Hetzner machine: 48 cores, 125 GB RAM, two NVMe drives in RAID 1. Both backfills and the audit ran on the same machine at the same time, so absolute times carry that load.

## Data

| Name | Built with | Contents |
|---|---|---|
| Store, 2014 | `xrpla restore --to store` | Ledgers 8,000,001–8,300,000 (300,000 ledgers, about 4.4 M transactions) from the early 8–9 M bundle. The store directories had several names across the runs: `q-old`, `q-new`, `q-compact` and `q-book`. |
| Store, 2025 | `xrpla restore --to store` | Ledgers 91,000,001–91,020,000 (`r25-*`) and 91,000,001–91,050,000 (`n25`), each from the 91,000,000 snapshot (about 15 M objects). |
| xrpld | xrpld 3.4.0, standalone (`--standalone --ledger 8300000 --valid`) | The same 300,000 ledgers restored with `xrpla restore --to nudb` (41 GB), serving on 127.0.0.1. |

## Tools

- `tools/xrpld_parity.py` is the all-commands benchmark. It derives requests from 40 random ledgers (`--seed 1`) and sends each to both servers, recording matches and each side's median, p95 and max.
  - `--range` covers the standalone xrpld, which reports only its tip ledger.
  - `--clio-methods` times Clio's commands on XRPLA alone.
  - Because standalone xrpld treats only its tip as validated, the `tx` "differences" (`validated`) and all `account_tx` comparisons are artifacts. xrpld's `account_tx` times are errors and are not comparable.
- `query-bench.sh` measures HTTP latency, and `ws-bench.py` measures WebSocket latency and throughput with 16 requests in flight, using the same 2,000 `tx`, 500 `ledger` and 289 `account_info` requests (seed 7).
- `nft-bench.py` covers NFT commands on XRPLA alone (`nft_info`, `nft_history`, `nfts_by_issuer`, `account_nfts`, NFT offers).

## Files, in order

| File | Build | What changed |
|---|---|---|
| `query-bench.log`, `ws-bench.log` | `34394f7` ("old"), and `34394f7` + bloom filters + open-file limit ("new") | The baseline. The store was opened for every request, about 8–9 ms each, and filters made no difference. |
| `query-bench2.log`, `ws-bench2.log` | `4794a8c` | The store stays open between requests: 15–60× faster. |
| `parity-bench-before.jsonl` | `4794a8c` | The first all-commands run against xrpld; the store is as restored, uncompacted. |
| `parity-bench.jsonl` | + iterator reuse, batch reads, per-request object cache, `book_changes` reading transactions only | `book_changes` dropped from 20 to 0.9 ms. |
| `compact-bench.log`, `parity-new.jsonl`, `parity-compact.jsonl` | the same, with the slowest requests listed | The same store before and after `xrpla compact` (33 s): 2–9× faster on directory-heavy commands. |
| `compact-bench2.log` | `d4d22aa` (compacted, with version stepping in key walks) | `book_offers` dropped from 24 to 12 ms. |
| `book-bench.log`, `parity-book.jsonl` | `6e084e3` (order-book index, compaction at the end of a restore) | `book_offers` dropped to 6 ms; all 40 books matched xrpld. |
| `nft-run.log` | `d4d22aa` | The NFT benchmark on the 2025 store (`n25`), plus the `book_offers` rerun. |
| `nft-compare2.log` | old and new builds | 84 NFT and MPT queries returned identical answers before and after the store changes. |
