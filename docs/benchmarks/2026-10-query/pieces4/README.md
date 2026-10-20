# Query benchmarks: piece cache, round 4, 2026-10-06

These results come from the piece cache after one-ledger decodes, described in `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache, round 4: decoding one ledger"):
- a read of one ledger parses its page's earlier ledgers but builds only that ledger;
- the parse resumes where the segment's last one-ledger decode stopped;
- batched loads decode each segment's ledgers together, in ascending order;
- serving skips the canonical page digest, which `index-pieces` checked;
- the budget is shared by the caches.

The run was `scripts/pieces-bench4.sh`, on the same machine, data and requests as `../pieces3/`. The load average was 10–15 from the backfills and the audit. The decode costs by mode come from `scripts/page-probe.cpp`.

The build is the commit that adds these files; its binary's sha256 prefix is `c2452f015fb65899`. It runs at two budgets:
- **auto**, the default of an eighth of memory: 15,973 MB on this 125 GB machine;
- **3gb**, an explicit `--cache-mb 3072`, standing in for a small machine.

| File | Contents |
|---|---|
| `run.log` | Timeline and cache statistics after each all-commands run. |
| `parity-pieces-{auto,3gb}-{cold,warm}.jsonl` | All-commands benchmark against xrpld 3.4.0 standalone, piece cache `p14` (8,000,001–8,300,000). |
| `cache-stats-{auto,3gb}.json` | `xrpla_cache_stats` after the warm run. `pages` now counts decoded ledgers. |
| `nft-pieces-{auto,3gb}-{cold,warm}.txt` | NFT benchmark, piece cache `p25` (91,000,001–91,050,000). |
| `latency-pieces-auto.jsonl` | HTTP and WebSocket latency and throughput, piece cache `p14` at the auto budget. |

Every xrpld comparison matched: 3,509 matching answers. The 933 differences are the standalone-xrpld artifacts.
