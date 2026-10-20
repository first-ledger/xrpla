# Query benchmarks: compressed piece cache against RocksDB store and xrpld, 2026-10-06

These are the raw results behind the comparison in `docs/internal/QUERY-BENCHMARKS.md`. The run was `scripts/pieces-bench.sh` on the production Hetzner machine (48 cores, 125 GB RAM, NVMe RAID 1), with load average 10–16 from the two backfills and the audit. All servers used one XRPLA build, `6e084e3` (binary sha256 prefix `ce38c21433237528`), with default `serve` settings: 4 threads and a 3072 MB piece cache.

| File | Contents |
|---|---|
| `run.log` | Timeline, sync times and disk sizes per directory and per index file type. |
| `p14-sync.json`, `p25-sync.json` | `xrpla sync` reports (index build details: postings, object versions, decode seconds). |
| `parity-pieces-cold.jsonl`, `parity-pieces-warm.jsonl` | All-commands benchmark against xrpld 3.4.0, piece cache `p14` (2014). Cold is a fresh server; warm repeats the same requests on the same server. |
| `parity-store-cold.jsonl`, `parity-store-warm.jsonl` | The same, against store `q-book` (300,000 ledgers of 2014, restored with `6e084e3`). |
| `latency-2014.jsonl` | HTTP and WebSocket latency, and WebSocket throughput with 16 in flight, for piece cache and store (`scripts/latency-bench.py`). |
| `nft-pieces-*.txt`, `nft-store-*.txt` | NFT benchmark on 91,000,001–91,050,000: piece cache `p25` against store `n25`, cold then warm. |

Every xrpld comparison matched in all four runs: 3,509 matching answers. The 933 "differences" are the standalone-xrpld artifacts described in `../store/README.md`.

In the NFT runs, the error counts are answers, not failures:
- `nft_buy_offers` / `nft_sell_offers` return "not found" for tokens with no open offers at the ledger asked.
- `nft_info` returns "not found" at the midpoint ledger for tokens minted after it.
