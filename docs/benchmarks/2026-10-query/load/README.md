# Load benchmarks: throughput per machine, 2026-10-12

Concurrent WebSocket load against XRPLA and xrpld on the same data, for `docs/internal/SCALING-AND-COST.md`.

**Setup.**
- Hetzner, Intel Xeon Gold 5412U.
- Each server is pinned to 16 logical CPUs (`taskset -c 0-15`, about 8 physical cores with SMT).
- The load generator runs on 24 others (16–39).
- The machine's remaining cores ran the backfills and audits throughout (load average about 14).

**Data and servers.**
- Ledgers 8,000,001–8,300,000 (2014).
- XRPLA 0.4.0 on libxrpl 3.4.1 with `--threads 32`, serving:
  - the piece cache `p14`, at the default memory budget;
  - the RocksDB store `q-compact`.
- xrpld 3.4.0 in standalone mode, holding the same ledgers, reached on a WebSocket admin port (admin ports skip xrpld's per-client rate limits).

**Method.** `scripts/load-bench.py`, driven by `scripts/load-bench.sh`.
- 16 processes (32 for some runs).
- Requests pipelined over many connections; 20–30 s per level.
- Each run starts after a warm-up pass.
- Requests are sampled from the ledgers themselves: 4,891 account cases, 4,386 transactions and 2,265 order books.

**Mixes.**
- `common`: `account_info` 25%, `tx` 20%, `ledger` 10%, `ledger_entry` 10%, `account_lines` 10%, `book_offers` 5%, `account_objects` 5%, `server_info` 5%.
- `full`: `common` plus 10% `account_tx`, which standalone xrpld cannot answer for these ledgers.

| File | Contents |
|---|---|
| `run.log` | The first run's timeline and corpus size. |
| `results.jsonl` | First run, as built: XRPLA (pieces, store) and xrpld (default config). XRPLA's fixed 64-request in-flight limit answers `tooBusy` at 256 in flight. |
| `results-uncapped.jsonl` | XRPLA built with that limit raised to 1,024 (an experiment, not committed), and xrpld with `[workers] = 32`. The xrpld rows labelled `xrpld-workers32` are the ones to read. |
| `results-ceilings.jsonl` | XRPLA after the two ceilings were fixed (in-flight limit 16 per thread, replies serialized on the worker threads), `--threads 32`. |
| `single-method.jsonl` | One method at a time, 64 in flight: throughput and server CPU, giving CPU time per request. Recorded from the run's output. |

Per line:
- `per_second`: requests answered per second.
- `p50_ms` / `p90_ms` / `p99_ms`: latency percentiles.
- `server_cores_busy`: the server's CPU time over the run, in cores.
- `errors`: counts by method, with samples.

The generator's own CPU column (`generator_cores_busy`) under-reports and should be ignored. That the generator was not the limit is shown by single-method runs reaching 45,000 requests a second.
