# Internal notes: design, plans and benchmarks

These documents are background: design decisions, plans, experiments and measurement reports
written while XRPLA was built. They are **not instructions**. Some describe prototypes that were
replaced, plans that changed, or numbers from older builds. For how to use XRPLA, start at the
[documentation index](../README.md).

Only the files marked *versioned* are in the public repository; the others are development notes
kept locally (see `.gitignore`).

## Design and goals

| Document | What it records |
|---|---|
| [SIZE-AND-FOOTPRINT.md](SIZE-AND-FOOTPRINT.md) (versioned) | Size goals, budget and the experiments that decided the publication format |
| SOURCE-SPEC.md | The original specification shared by the first three repositories |
| PROMPT-REVIEW.md | The first specification review (2026-09-15) and what superseded it |
| ARCHIVE-V2.md | Why archive v2 added keyed state patches |
| FETCH-PROFILE.md | The authenticated fetch contract for v2 archives |
| RPC-DECISION.md | Early RPC implementation and compatibility decisions (libxrpl 3.3.0 era) |
| ARCHIVE-SIZE-OPTIONS.md, CHUNK-REDESIGN-OPTIONS.md | Options considered for smaller archives |
| DIRECT-ARCHIVE-QUERIES.md | The first prototype of queries over compressed history |

## Plans

| Document | What it records |
|---|---|
| [PARITY-PLAN.md](PARITY-PLAN.md) (versioned) | How XRPLA's answers are compared with xrpld and Clio |
| IMPLEMENTATION-PLAN.md | The September 2026 delivery plan and checklist |
| FULL-HISTORY-PUBLISHING-PLAN.md | The plan for full-history compression and R2 publishing |
| NODE-QUALIFICATION.md | A qualification procedure against a running xrpld |

## Benchmarks and measurements

| Document | What it records |
|---|---|
| [QUERY-BENCHMARKS.md](QUERY-BENCHMARKS.md) (versioned) | Piece cache vs RocksDB store vs xrpld, October 2026; raw data in [../benchmarks/2026-10-query/](../benchmarks/2026-10-query/) |
| [SCALING-AND-COST.md](SCALING-AND-COST.md) (versioned) | Cost and scaling of XRPLA, xrpld and Clio clusters for full-history serving |
| [../benchmarks/2026-10-audit/NOTES.md](../benchmarks/2026-10-audit/NOTES.md) (versioned) | The October 2026 audit: restores, Clio, older history, parity; the PDF report is in `report/` |
| VALIDATION.md | The validation record of the September 2026 builds (receipts in `docs/validation/`, local) |
| STORAGE-BENCHMARK.md, MILLION-LEDGER-BENCHMARK.md, NATIVE-MILLION-BENCHMARK.md | Million-ledger storage comparisons with xrpld and Clio |
| FAST-ARCHIVE-BENCHMARK.md, THROUGHPUT-10000.md, PUBLISHER-PERFORMANCE.md | Packaging and publishing throughput |
| DELTA-BENCHMARK.md, FIELD-BENCHMARK.md, HISTORY-BENCHMARK.md, HISTORY-SCALE-BENCHMARK.md | Compression experiments behind the history codec |
| FULL-HISTORY-SIZE-ESTIMATE.md | An early full-history size estimate |

## Earlier publication prototypes (Python, September 2026)

Local prototypes of journals, catalogs and routing, built before the publication pipeline
described in [../PUBLICATION.md](../PUBLICATION.md):
PUBLISHER-FOUNDATIONS.md, PUBLICATION-JOURNAL.md, PUBLICATION-WORKER.md,
PUBLICATION-PREPARE.md, PUBLICATION-QUERIES.md, PUBLICATION-CLIENT.md,
PUBLICATION-ROLLOVER.md, PUBLICATION-PROMOTION.md, PUBLICATION-SUPERVISOR.md,
PUBLICATION-PIECE-INDEXES.md, PUBLICATION-TREE.md, PUBLICATION-TREE-ROUTING.md,
TRANSACTION-INDEX-SHARDS.md, TRANSACTION-ROUTING.md.
