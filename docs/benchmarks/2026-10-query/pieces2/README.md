# Query benchmarks: piece cache, round 2, 2026-10-06

These results come from the piece cache after the read-path changes described in `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache, round 2"). The run was `scripts/pieces-bench2.sh` on the same machine, data and requests as `../pieces/`, with load average 15–17 from the backfills and the audit.

The build is the commit that adds these files; its binary's sha256 prefix is `a78e2d893e8d49b3`. It runs at two budgets:
- **auto**, the new default of an eighth of memory: 15,973 MB on this 125 GB machine;
- **3gb**, an explicit `--cache-mb 3072`, standing in for a small machine.

| File | Contents |
|---|---|
| `run.log` | Timeline and cache statistics after each all-commands run. |
| `parity-pieces-{auto,3gb}-{cold,warm}.jsonl` | All-commands benchmark against xrpld 3.4.0 standalone, piece cache `p14` (8,000,001–8,300,000). |
| `cache-stats-{auto,3gb}.json` | `xrpla_cache_stats` after the warm run: open segments, decoded pages, snapshot blocks, and load time by stage. |
| `nft-pieces-{auto,3gb}-{cold,warm}.txt` | NFT benchmark, piece cache `p25` (91,000,001–91,050,000). |
| `latency-pieces-auto.jsonl` | HTTP and WebSocket latency and throughput, piece cache `p14` at the auto budget. |

Every xrpld comparison matched: 3,509 matching answers. The 933 differences are the standalone-xrpld artifacts.
