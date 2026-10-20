# Query benchmarks: piece cache budget split, 2026-10-06

A sweep of the piece cache's split between decoded pages and open segments. It is described in `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache: splitting the budget").

The run was `scripts/page-share-bench.sh`, on the same machine, data and requests as `../pieces3/`. The load average was 10–14 from the backfills and the audit.

The build was an experiment, not committed. It was round 3's code, plus a `XRPLA_BENCH_PAGE_PCT` environment variable that set the page share; segments got 90% minus that share, and snapshot blocks 10%. Its binary's sha256 prefix is `0791fac3ca5e851e`. Shares of 30, 45, 60 and 75% ran at budgets of 3,072 MB and 1,536 MB.

| File | Contents |
|---|---|
| `run.log` | Timeline and cache statistics for each budget and share. |
| `parity-{3072,1536}-{30,45,60,75}-{cold,warm}.jsonl` | All-commands benchmark against xrpld 3.4.0 standalone, piece cache `p14` (8,000,001–8,300,000). |
| `cache-stats-{3072,1536}-{30,45,60,75}.json` | `xrpla_cache_stats` after the warm run. |

Every xrpld comparison matched: 3,509 matching answers per run. The 933 differences are the standalone-xrpld artifacts.
