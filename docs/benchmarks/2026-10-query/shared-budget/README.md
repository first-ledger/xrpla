# Query benchmarks: piece cache shared budget, 2026-10-06

The piece cache with one budget shared by its three caches. Each cache keeps its share as a guarantee (segments 60%, pages 30%, snapshot blocks 10%) and may use room the others leave free. It is described in `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache: splitting the budget").

The run was `scripts/shared-budget-bench.sh`, on the same machine, data and requests as `../page-share/`. It compares with that sweep's 30% runs, which are the fixed split. The load average was about 13 from the backfills and the audit.

The build is the commit that adds these files; its binary's sha256 prefix is `04f318562f7d5ba8`. It runs at budgets of 3,072 MB and 1,536 MB.

| File | Contents |
|---|---|
| `run.log` | Timeline and cache statistics for each budget. |
| `parity-{3072,1536}-shared-{cold,warm}.jsonl` | All-commands benchmark against xrpld 3.4.0 standalone, piece cache `p14` (8,000,001–8,300,000). |
| `cache-stats-{3072,1536}-shared.json` | `xrpla_cache_stats` after the warm run. New: `budget` (total and used bytes), and each cache's `share`. |

Every xrpld comparison matched: 3,509 matching answers per run. The 933 differences are the standalone-xrpld artifacts.
