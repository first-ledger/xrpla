# Query benchmarks: piece cache, round 3, 2026-10-06

These results come from the piece cache after the dictionary change described in `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache, round 3"). Atoms' and hash recipes' arguments are now read from the dictionary on first use, instead of being parsed into tables when a segment loads.

The run was `scripts/pieces-bench3.sh`, on the same machine, data and requests as `../pieces2/`. The load average was 11 from the backfills and the audit. `scripts/dict-count.py` counts a segment dictionary's entries by table.

The build is the commit that adds these files; its binary's sha256 prefix is `542b0d87e795b5cc`. It runs at two budgets:
- **auto**, the default of an eighth of memory: 15,973 MB on this 125 GB machine;
- **3gb**, an explicit `--cache-mb 3072`, standing in for a small machine.

| File | Contents |
|---|---|
| `run.log` | Timeline and cache statistics after each all-commands run. |
| `parity-pieces-{auto,3gb}-{cold,warm}.jsonl` | All-commands benchmark against xrpld 3.4.0 standalone, piece cache `p14` (8,000,001–8,300,000). |
| `cache-stats-{auto,3gb}.json` | `xrpla_cache_stats` after the warm run. New in this round: `dictionaries`, with the number loaded, read and parse time, and mean bytes per open segment. |
| `nft-pieces-{auto,3gb}-{cold,warm}.txt` | NFT benchmark, piece cache `p25` (91,000,001–91,050,000). |
| `latency-pieces-auto.jsonl` | HTTP and WebSocket latency and throughput, piece cache `p14` at the auto budget. |

Every xrpld comparison matched: 3,509 matching answers. The 933 differences are the standalone-xrpld artifacts.
