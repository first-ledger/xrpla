# October 2026 audit: benchmark notes

These are the raw results behind the benchmark report. Each `.jsonl` line from `perf.py run` is one load
level, with:
- requests per second;
- latency percentiles, overall and per method;
- the server's busy cores;
- its RSS (resident memory);
- errors.

`env` lines record the server, its PIDs, the time and the load average before each run.

## Machines and servers

- **Hetzner** (publisher-host): 48 threads, 125 GB RAM. All servers run on cores 16-39, and the load generator runs on cores 40-47 (64 in flight, 4 processes).
  - **A′**: an XRPLA piece cache of 107,000,001-107,697,250, served by `xrpla serve`.
  - **X**: xrpld 3.4.1 restored from that range (comparison 4). Its NuDB has since been removed to make room for C.
  - **C**: Clio 2.8.0 with Cassandra 4.1.10 (G1, 24 GB heap), in user space. It runs on the keyspace that `xrpla restore --to clio` loaded from R2 (comparison 5), and follows the tip through its ETL from the box's xrpld.
- **Test server** (test-host, `xrpla-test.example`): 16 threads, 61 GB RAM. It is the full-history XRPLA node, with a 32 GB decode cache.

The request sets are `corpus-107M.json` (recent and old ledgers of the range) and `corpus-107M-old.json` (ledgers more than 100,000 behind the range's tip, for cold runs). Both are in Hetzner's `~/parity-x`. The mixes:
- **point**: `account_info`, `tx`, `ledger`, `ledger_entry` and `server_info`;
- **full**: the point mix plus `account_lines`, `account_objects`, `account_tx` and `book_offers`;
- **lines**: `account_lines` alone.

## Comparison 5: C (Clio) against A′ and against R (`clio-comparison/`)

- **`hetzner-compare-c-r-seed{1,2,3}.jsonl`**: C against the Clio node at clio-host, whose database Clio's own ETL built. Each seed covers 300 ledgers. Across 906 ledgers, 14,092 answers were the same; the only differences were the version numbers, 2 per seed (the node runs an older Clio). This shows that a Clio restored from XRPLA's publication answers as one built by Clio.
- **`hetzner-compare-c-a-seed{1,2,3}.jsonl`**: Clio's own methods, C against A′. A′ still ran 2b19b97 here; see `clio-methods/` for the current build.
- **`hetzner-speed-c-a.jsonl`** and **`hetzner-cold-c-a.jsonl`**: speed and cold runs with A′ on 2b19b97.
  - The first attempt at C's cold half recorded nothing: Clio started before Cassandra's CQL port opened and exited.
  - C's cold rows come from the rerun, after the restart was fixed to wait for port 9042.
  - `hetzner-cold-evict.log` holds the restarts and evictions.
- **`hetzner-speed-c-a-803bc33.jsonl`** and **`hetzner-cold-c-a-803bc33.jsonl`**: the same runs with A′ on 803bc33, reindexed over the same range. These are the numbers to report.

## Clio's methods on the full-history node (`clio-methods/`)

`test-server-803bc33-clio-seed{1..5}.jsonl` compares the test server on 803bc33 with the Clio node at clio-host, using `--clio-only`. Every remaining difference is one of:
- the Clio node's history starting at 75,445,518 (`ledger_index_min`, and `nfts_by_issuer` `ledger_index`);
- tip timing (`ledger_index_max`);
- the error framing XRPLA keeps from xrpld (the `request` echo and `type`).

With those ignored, seed 1 matched on all 265 answers.

## account_lines investigation (`account-lines/`)

- **`test-server-lines-run.json`** and **`test-server-report.txt`**: `account_lines` alone on the full-history node, with a `perf` profile.
  - Result: 4,474 requests per second, 2.4 ms median, 13 cores.
  - Page decodes take 56% of the CPU and dictionary loads about 11%.
- **`test-server-ab-cursors.jsonl`**: a second read-only server on the node's cache (12 GB cache, cores 0-7), with 4, 16 and 64 one-ledger cursors per segment. There was no difference (about 66 requests per second): with too little cache the server thrashes on segment dictionaries.
- **`test-server-ab-dict.jsonl`**: a local cache of decoded segment dictionaries (a mapped prototype that was not committed), on the same harness. It doubled throughput there, from 66 to 125 requests per second.
- **`hetzner-{speed,cold}-a-{dict,mem32,mem32-dict,mem64}.jsonl`**: A′-only runs on Hetzner, each after a 120 s full-mix prefill (`*-prefill.jsonl`). `*-cache-stats.json` is the server's `xrpla_cache_stats` at the end of each.
  - **dict** (803bc33 plus the dictionary cache prototype, default 16 GB cache): full mix about 440-550 requests per second, against about 420 without it. The cold full mix fell to 12 requests per second, because decodes waited on page faults into the mapped files. The prototype was dropped.
  - **mem32** (803bc33, `--cache-mb 32768`): full mix about 1,410-1,474 requests per second (3.5×), with owner methods at a 105-130 ms median. Cold full mix: 2,352 requests per second, and 3,006 warm.
  - **mem32-dict**: worse than mem32 (about 750-850 requests per second), for the same reason as dict.
  - **mem64** (`--cache-mb 65536`): invalid. The kernel's OOM killer stopped A′ at 63 GB anonymous RSS while Clio and Cassandra shared the box.

## Older history against stock xrpld (`older-history/`)

Four 100,000-ledger stretches, run with `hetzner-run-old.sh`. For each stretch:
- `xrpla restore --to nudb` (543c523, `--jobs 8`) restored it into xrpld's NuDB and SQLite;
- `xrpla sync` made a local piece cache of the same range;
- xrpld 3.4.1 ran standalone on the restored tip, with `--force_ledger_present_range` over the stretch (standalone, it otherwise counts only the loaded ledger as present) and public ports (an admin connection has no minimum limits, and XRPLA answers as a public xrpld does);
- `xrpla serve` ran on the cache;
- `tools/xrpld_parity.py` compared the two: three seeds of 300 ledgers, every request also over XRPLA's WebSocket.

| Stretch | Restore | xrpld database | Peak RSS | Piece cache sync | Answers (3 seeds) | Different |
|---|---|---|---|---|---|---|
| 14,000,001-14,100,000 (2015) | 25 s | 18 GB | 4.3 GB | 3 min 16 s | 13,707 | 0 |
| 38,000,001-38,100,000 (2018) | 51 s | 44 GB | 11.6 GB | 7 min 47 s | 13,234 | 0 |
| 63,000,001-63,100,000 (2021) | 57 s | 55 GB | 11.4 GB | 11 min 28 s | 12,921 | 0 |
| 88,000,001-88,100,000 (2024) | 1 min 16 s | 48 GB | 13.8 GB | 37 min 52 s | 13,803 | 0 |

Every answer was also the same over the WebSocket; there were no timeouts and no reference errors. The files for each stretch:
- `hetzner-rNNm-compare-seed*.jsonl`: the comparisons.
- `hetzner-rNNm-restore.log`: the restore's summary.
- `hetzner-rNNm-restore.err` and `hetzner-rNNm-sync.err`: their `time -v` output.
- `hetzner-rNNm-xrpld-up.json`: xrpld's `server_info` at the start.

## Other results the report uses

- **`clio-restore/`**: the Clio restore on Hetzner.
  - `hetzner-restore-baseline.*`: the code before the audit, stopped at 5 h 22 min.
  - `hetzner-restore-fast1.*`: the fast plan build. It took 58 min; its load died in a 21-minute Cassandra garbage-collection pause, and Cassandra was then switched to G1.
  - `hetzner-restore.*`: the final load of 951,674,801 rows in 1 h 35 min.
  - The scripts are beside them.
- **`parity-campaign/test-server-campaign-c1.log`**: the parity campaign's log. Its last `finished` line is the total: 108 intervals, 120,617 the same, 0 different.
- **`invariants/test-server-ed6f07f-seed{1,3,4}.jsonl`**: the invariant checker over all history on the full-history node, 50 ledgers per seed, with listings past the page cap counted as skipped.
- **`account-lines/hetzner-decode-ab.jsonl`**: the decode A/B at 32 GB. `base` is 803bc33's reader; `notx` is object reads skipping the transaction section, which became 543c523; `notx-s8` also keeps 8 decompressed sections and was dropped.

## The report

`report/make_report.py` builds `report/XRPLA-benchmarks-2026-10.pdf` and its `report/figures/` from these files. It needs matplotlib and reportlab.

## Memory per open segment (`account-lines/hetzner-mem-breakdown-*`, `hetzner-txkept-ab.jsonl`)

`xrpla_cache_stats` now reports `open_segments`, which sums what the open segments hold, by part. After 180 s of the full mix at a 32 GB cache, 175 segments held 117 MB each:

| Part | MB per segment |
|---|---|
| Decoded transaction sections | 49.4 |
| Dictionary tables | 26.2 |
| Dictionary | 23.7 |
| Cursors | 8.6 |
| Memos | 8.4 |

The A/B capped the decoded transaction sections kept per segment (`XRPLA_EXPERIMENT_TXKEPT`; a prototype, not committed):
- **No cap:** 1,873-1,938 requests/s, with 23,426 dictionary loads.
- **Cap of 4:** 1,337-1,370 requests/s and 12 busy cores instead of 19. Dictionary loads fell to 7,435, but requests waited on sections being decoded again.
- **Cap of 1:** reached 62 GB of anonymous memory under a 32 GB budget and was killed by the kernel's OOM killer.

The cap was dropped. The baseline here ran faster than the decode A/B's (1,548) because the box's file cache was warmer; the report quotes the decode A/B.

## A cache share of their own for transaction sections (`account-lines/hetzner-txshare-ab.jsonl`)

This prototype (not committed) moved decoded transaction sections out of their segment into a cache of their own: the segment kept only weak references, and the new cache held the strong ones. That cache took `XRPLA_EXPERIMENT_TXSHARE` percent of the budget from the segments' 60%. Results at 32 GB, full mix:

| Share | Requests/s | Segments held | Per segment | Dictionary loads | Section decodes |
|---|---|---|---|---|---|
| 0 (kept with their segment) | 1,871-1,993 | 155 | 133 MB | 23,656 | - |
| 15% | 1,673-1,689 | 179 | 86 MB | 19,760 | 633,432 |
| 25% | 1,433-1,493 | 156 | 77 MB | 25,663 | 553,679 |

The evicted sections were decoded again more often than the extra open segments saved dictionary loads. Kept with their segment, a section lives as long as the reads that use it.

**Accounting:** in every setting, A′'s anonymous memory was about 47 GB under a 32 GB budget, about 1.45 times the budget, under the full mix.

## Memory accounting (`account-lines/hetzner-mem-acct-{before,after}.jsonl`)

These runs sampled `xrpla_cache_stats` every 30 s during 300 s of the full mix, with A′ at 32 GB. The new `allocator` section reports jemalloc's own figures. Under steady load:

| | Budget charged | Allocated | Resident | Anonymous RSS |
|---|---|---|---|---|
| Before | 34.3 GB | 45.3 GB | 50.5 GB | 48.8 GB |
| After | 34.4 GB | 37.9 GB | 43.2 GB | 41.9 GB |

Most of the uncharged memory was decoded pages: each was charged its record's size, while the record's buffer (built by appending) held up to twice that, and the views parsed from it were not charged at all. Pages are now trimmed before they are parsed and charged what they hold; snapshot blocks are charged the same way. Resident memory fell from 1.47 to 1.26 times the budget. The rest is request working memory, the key lookup maps and the allocator's own.

## Smaller dictionary records (`account-lines/hetzner-tables-stats.json`, `hetzner-rec8-ab.jsonl`)

After 150 s of the full mix, a segment's dictionary tables averaged 32 MB: atoms 21.4 MB (about 1.78 million at 12 bytes), hashes 7.5 MB, values 3.4 MB, and shapes and headers under 0.1 MB.

Hash and atom records are now 8 bytes each:
- a hash's recipe mode is the byte before its arguments;
- an atom's header and kind are the varints its record starts with, read again when it is rendered.

A/B at 32 GB, full mix, alternating builds, two runs each per round:

| | 6fa16ef | 8-byte records |
|---|---|---|
| Requests/s (4 runs) | 1,675-1,709 (mean 1,690) | 1,750-1,825 (mean 1,779), +5.3% |
| p99 | 313-320 ms | 297-306 ms |
| Tables per segment | 30.2-30.9 MB | 21.5-21.8 MB |
| Dictionary loads | 26,200-26,423 | 24,941-24,972 |

## Atom headers beside the records (`account-lines/hetzner-hdr-ab.jsonl`)

With 8-byte records, every field of every view decoded read its atom's header from the dictionary, a random read into about 63 MB. The headers are now also kept in an array beside the records: one byte per atom where a dictionary has at most 256 headers (about 80 in a 2025 segment), else two. The A/B ran at 32 GB on the full mix, alternating builds:

| | a130dc5 | With the header array |
|---|---|---|
| Requests/s (4 runs) | 1,735-1,789 (mean 1,761) | 1,797-1,862 (mean 1,825), +3.6% |
| p99 | 299-306 ms | 291-300 ms |
| Tables per segment | 22.5-23.5 MB | 23.1-24.2 MB |

## book_offers' key walk (`account-lines/hetzner-keys-ab.jsonl`)

The walk over a book's key range (`PieceHistory::keys_between`) copied every key it looked at into a new string. A busy book's range holds every price level it ever had, nearly all long gone, so most of those copies were dropped. The keys are now compared as views, and only the keys returned are copied. The A/B ran at 32 GB, alternating builds over two rounds; perf.py has a `books` mix (book_offers only):

| | c4dd4cf | Views |
|---|---|---|
| Books mix, requests/s | 670, 672 | 777, 782 (+16%) |
| book_offers p50 / p99 | 81 / 292 ms | 69.5 / 251 ms |
| Full mix, requests/s | 1,836, 1,883 | 1,874, 1,850 (unchanged) |

## Key prefixes in the book walk (`account-lines/hetzner-prefix-ab.jsonl`, dropped)

A books-only profile on dd6d2d3 put 93% of book_offers in `keys_between`: about 37% in key comparisons, 20% in the heap and 25% in the function itself. A prototype kept each cursor's first 8 key bytes as a big-endian number and compared those first. It was slower: books mix 735 and 736 requests/s, against 765 and 775 for dd6d2d3. The full mix was unchanged.

Keys are hashes, so `memcmp` already ends at the first byte. The comparisons' time is fetching the keys from the mapped indexes, not comparing them, and the prefix bookkeeping only added work.
