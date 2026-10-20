# Size and footprint: goals, budget and experiments

XRPLA exists to make full XRP Ledger history accessible. Every byte of the
published archive is downloaded by each user and stored on their disk.
**Footprint is a primary product requirement, not an operating-cost detail.**
This document records the goals, the current measured position and the
experiments that decide the format. Update it when a measurement changes.

## Goals, in priority order

1. **Smallest possible download and disk footprint** for complete, lossless
   history. A full-history rippled NuDB node is roughly 40 TB; the ordinary
   XRPLA v2 archive projects to several TB; the target is well below 2 TB
   including the indexes needed to query it.
2. **Queryable while compressed.** Ledger, transaction, account-history and
   historical-state queries run against the compressed archive plus compact
   indexes. Users must not have to expand the archive into a database first.
3. **Easy conversion to native stores.** The archive restores to exact
   canonical records that load into rippled and Clio (Cassandra or
   ScyllaDB). **NuDB is the default rippled restore target**, because
   almost all rippled operators use it; RocksDB remains an option. A
   restore writes NuDB directly, without an intermediate RocksDB copy.
   Full history can be published to R2 one million ledgers at a time and
   deleted from the publisher after verification; a restore rebuilds the
   native database from R2 alone.
4. **Verifiable.** Publisher signatures and canonical digests authenticate
   every range a client downloads, including partial byte-range reads.

## What must never be traded for size

Every header, transaction, signature, metadata record, object version,
deletion and ordering must round-trip byte for byte. Dropping or truncating
retained data creates a different product with weaker guarantees; it is not a
size win. A derived value is acceptable only when the decoder recomputes the
exact original bytes and the encoder verifies that before accepting it, with a
literal fallback when derivation fails.

## Where these goals conflict

- **Size vs. queryability.** The most aggressive representations (for example,
  deriving metadata by replaying transactions) need prior ledger state to
  decode a single record. That breaks cheap random-access queries.
- **Size vs. decode speed.** Larger shared dictionaries and higher compression
  levels save bytes but raise first-read latency and memory.
- **Size vs. conversion speed.** Anything that needs execution or state to
  decode makes native import slower.

If one format can't meet all goals, use two tiers from the same canonical
source: a **query tier** (random access, compact indexes) and a smaller
**transport tier** for bulk download and native conversion, where decoding
cost is paid once. Only adopt the transport tier if it saves substantially
more than the query tier.

**Decision (2026-09-25):** the published form stays the query tier. It is what
mirrors and standalone servers query directly, and it restores an interval at
hundreds of ledgers a second. Replay, measured natively on a 24-core NVMe host,
reproduced 1,000 recent ledgers exactly at 34 ledgers (3,108 transactions) a
second, sequential within an interval: a million recent ledgers would take eight
hours or more to restore from a transport tier, against about one from the query
tier, to save about half the download. A transport tier could still be added
later as an optional second download for slow links.

## Budget and current position

Per million recent mainnet ledgers (106,027,812–107,027,811, measured 2026-09-17):

| Component | Current bytes | Notes |
|---|---:|---|
| Historical chunks, v2 zstd-9 | 32.10 GB | Production format today |
| Same, experimental history codec | ~25.9 GB (projected) | 19.25% smaller on 32,768 sampled ledgers; the full million has not been encoded |
| Baseline state snapshot | 2.81 GB | One snapshot; periodic checkpoints for state queries would add more |
| Transaction hash index (SQLite) | ~5.69 GB | 49.53 bytes/transaction, 114.9M transactions |
| Account-history, state indexes | not built | Must be designed compact from the start |

The full-history projection is **0.9–1.9 TB, central 1.4 TB**, for chunks
alone. Adding the current transaction index gives about 1.7 TB. That
projection assumes early-era ledgers are 10–30% of recent bytes per ledger;
**no early or middle era has been measured**, so full-history size is the
largest open uncertainty.

## Evidence so far

| Experiment | Result versus v2 zstd-9 | Decision |
|---|---:|---|
| zstd level 19 instead of 9 (one 64-ledger chunk) | 5.5% smaller | Never applied to the codec |
| Bounded object deltas (full million) | 1.10% smaller, 3.5× slower reads | Rejected |
| Shared-field typed columns (525 ledgers) | 10.40% smaller | Superseded |
| Transaction columns | 4.6% larger for transactions | Rejected |
| History codec: object histories, derived hashes, 1k-ledger dictionaries | 17.32% smaller | Candidate |
| Same with up to 4k-ledger dictionaries (32,768 ledgers) | **19.25% smaller** | Current best |

In the current best result, transaction pages are 42% of output, the
shared dictionary (mostly 32-byte hashes) is 28%, and metadata/object history
pages are 30%. Signatures and non-derivable hashes are effectively
incompressible and set the floor.

## Open levers and experiments

Each experiment reports total retained bytes, including dictionaries,
indexes and envelopes, and verifies exact canonical round trips.

| # | Lever | Question | Status |
|---|---|---|---|
| E1 | Stronger publisher-side compression | How much do zstd 19/22 and long windows save on codec output, and at what encode/decode cost? | Done: adopt zstd 19 (+7 points) |
| E2 | Compact transaction index | Can a static fingerprint + location index replace 49.5 bytes/tx with ~6 bytes/tx while keeping exact lookups? | Done: 4.75 bytes/tx, ~9× smaller |
| E3 | More derived identities | Which literal hashes remain, by kind, and how many can be derived (directories, book directories, skip lists, other keylets)? | Done: +2.6 points; ≤4% left |
| E4 | Dictionary scope | Does a dictionary spanning a full 50,000-ledger piece save more, and what does it cost in cold reads? | Planned |
| E5 | Era sampling | What are bytes per ledger in early and middle eras? | 2013 sampled; 2014–2020 needed |
| E6 | Replay-derived metadata (transport tier) | What fraction of metadata and object changes does a libxrpl replay reproduce byte for byte, per era? | Planned; decides whether a transport tier is worth building |

Results are recorded below as they arrive, with receipts in
`docs/validation/` (local only).

## Results

### E1–E3 on the million-ledger corpus, 2026-09-22

Eight evenly spaced 4,096-ledger windows of 106,027,812–107,027,811:
**32,768 ledgers, 3,895,779 transactions, 1,102,383,569 bytes as v2
zstd-9 chunks** (33,642 bytes per ledger). Busy windows split at the
codec's 480 MiB segment budget. Every codec segment, with and without the
new recipes, decoded back to the original canonical bytes, and every
recompressed zstd frame was decompressed and compared. zstd 1.5.5, one
core per worker, lowest CPU/IO priority. The experiment's script,
`tools/size_experiments.py`, has been removed from the tree; it is in the git
history (last present at commit a70ada4).

**E1 and E3b: smaller than v2 zstd-9 (summed bytes)**

| Representation | zstd 9 | zstd 19 | zstd 19, long window | zstd 22, long window |
|---|---:|---:|---:|---:|
| Ordinary v2 chunks | — | 7.73% | 11.08% | 11.30% |
| History codec (current best) | 19.88% | 26.86% | 26.93% | 26.95% |
| **History codec + new recipes** | 22.48% | **29.45%** | 29.46% | 29.46% |

- **Use zstd 19 for publication.** It adds 7 points over level 9. Long
  windows and level 22 add ≤0.1 point to the codec, so they aren't worth it.
- Cost: publisher compression takes about 14.6 ms per ledger per core
  (≈4 core-hours per million ledgers), 14.6× level 9. Decompression is
  0.06 ms per ledger, about 35% slower than level 9 but negligible next to
  parsing and hashing.
- Per window, the best result ranges from 26.5% to 30.9% smaller.
- At zstd 19, codec output is about 46% transaction pages, 29%
  metadata/object-history pages and 25% dictionary.

**E2: transaction hash index**

| Layout | Bytes per transaction, sample | Projected, one index per million ledgers | Wrong-page reads per absent lookup |
|---|---:|---:|---:|
| SQLite `tx(hash, ledger, position)` (current) | 44.62 | ~45 | 0 |
| 16-bit fingerprint + exact ordinal | 4.75 | 5.65 | 0.00002 |
| 16-bit fingerprint + page only | 3.06 | 4.02 | 0.00002 |
| 12-bit fingerprint + exact ordinal | 4.25 | 5.15 | 0.00018 |

The fingerprint index stores the sorted hash prefix in Elias-Fano form plus
a location. The reader recomputes the full hash from the fetched transaction,
so storing all 32 bytes is unnecessary. Recommended: 16-bit fingerprint with
an exact ordinal, **about 9× smaller than SQLite**. Routing across bundles
still needs a design.

**E3: remaining literal hashes**

Of 2,708,824 literal dictionary hashes, 1,537,379 are derivable with
verified recipes: book directory roots 673,810, tickets 643,902, checks
92,279, owner directory roots 47,777, directory pages 44,786 and ledger
hashes 34,615. Book-directory keys save little, because keys in one book
share 24 bytes that zstd already matches. The largest remaining literals are
directory `Indexes` entries (461,897), out-of-segment `PreviousTxnID`
(397,329), NFToken offer keys (118,274) and NFToken IDs (66,845). Deriving
all of these would add at most about 4%.

Metadata can omit default-valued fields, such as XRP's all-zero currency and
issuer in a book directory created and deleted within one ledger. The recipe
fills those defaults and still verifies the exact hash.

### E5: first early-era samples (local fixtures)

| Ledgers | Year | Transactions | v2 zstd-9 bytes per ledger | Codec + zstd 19 |
|---|---|---:|---:|---:|
| 32,571–33,570 | 2013 | 0 | 144 | 9.4% smaller |
| 1,000,000–1,001,023 | 2013 | 1,560 | 559 | 14.3% smaller |

Recent ledgers average 33,642 bytes. The earliest ledgers are 0.4–1.7% of
that, well below the 10–30% the full-history estimate assumes for the first
50 million. These are the very first months of the chain; 2014–2020 still
needs sampling before the estimate changes.

### What this means for the footprint

Per million recent ledgers:

| Component | Before | With these results |
|---|---:|---:|
| Historical chunks | 32.10 GB (v2 zstd-9) | **22.65 GB** (codec + recipes, zstd 19) |
| Transaction hash index | ~5.69 GB (SQLite) | **~0.65 GB** (fingerprint index) |
| **Chunks + index** | **37.79 GB** | **23.30 GB (38% smaller)** |

Applying the same era scenarios as the earlier estimate, the central
full-history figure for chunks plus transaction index falls from about
1.7 TB to about **1.23 TB**. The early-era samples suggest that is still
conservative, but the middle eras are unmeasured.

### Built into the code, 2026-09-22

- **zstd 19 and hash recipes 5–10** are now part of `tools/history_codec.py`
  and the C++ reader (`tools/history_reader.hpp`). A real 512-ledger modern
  segment that uses all ten recipe modes decodes byte for byte in C++. A
  compiled encoder (`xrpla_history_encode`, 2026-09-23) now writes the same
  sections byte for byte at about 30 ms of CPU per modern ledger, most of it
  zstd 19 (docs/PUBLICATION.md).
- **Compact transaction index** (`tools/tx_fingerprint_index.py`): on 4,096
  real ledgers it finds all 331,373 transactions at their exact ledger and
  position using **4.76 bytes per transaction** (SQLite: 44.6), with no
  wasted reads over 100,000 absent-hash lookups. The C++ query server
  (`xrpla serve` over a piece cache) now looks transactions up through it.
- **Queryable while compressed.** Published pieces and bundles stay the
  only download. A query node derives a local index from them: about 6.6 GB
  per million modern ledgers, 26% on top of the synced data. Object-version
  keys are most of it. With that index it answers the rippled RPC surface
  identically to xrpld on the ledgers both hold (docs/PUBLICATION.md). A node
  that only restores to NuDB, RocksDB or Clio never builds it.
- **NuDB is the default `import rippled` target**, written through libxrpl's
  own backend. Stock xrpld 3.3.0 served the imported fixture with the
  publisher-signed tip hash.

### Replay probe (E6), first results

The replay probe (`tools/replay_probe.cpp`, since removed from the tree; it
is in the git history, last present at commit a70ada4) rebuilt each archived
ledger the way rippled's
`buildLedger` replays a closed ledger: parent state from the NodeStore,
transactions in `TransactionIndex` order through libxrpl 3.3.0's engine.
It then compared every metadata blob byte for byte, plus the resulting state
and transaction-tree hashes. Signatures are pre-marked valid, because the
verified transaction tree already authenticates them; re-checking them under
current rules rejects early high-S signatures (`RequireFullyCanonicalSig`
is now retired and always enforced).

| Ledgers | Era | Transactions | Exact metadata | Exact ledgers (state + tx tree) |
|---|---|---:|---:|---:|
| 1,000,000–1,000,004 | 2013 | 6 | 6 (100%) | 5 / 5 |
| 1,000,000–1,001,023 | 2013 | 1,560 | 1,359 (87.1%) | 846 / 1,024 (82.6%) |
| **106,027,812–106,037,811** | **2026** | **764,551** | **764,551 (100%)** | **10,000 / 10,000** |

On current history, replay reproduced all 543.8 MB of metadata and every
state and transaction-tree hash exactly, starting from the 19,241,572-object
snapshot. Replay alone took 1,536 s: **154 ms per ledger, 498 transactions/s
on one core**. That was measured under Docker Desktop on Windows, with lazy
NuDB node reads and no tuning; loading the snapshot took another 50 minutes. Natively on the
Hetzner host (2026-09-25; ledgers 107,214,591–107,215,590, 91,197
transactions) the same probe matched every metadata byte and every state and
transaction-tree hash in **29.3 s: 34 ledgers, 3,108 transactions a second**.

Early-era misses are mostly `OfferCreate`, path payments (`temBAD_PATH`)
and follow-on `terPRE_SEQ`. That is consistent with DEX and path logic
whose pre-amendment behaviour current code no longer contains. Replay with
stored corrections still works for such eras, but those corrections take
22% of that sample's metadata bytes.

### What replay means for the transport tier

Headers plus transaction pages alone are 33.1% of v2 zstd-9 at zstd 19
(E1 data, 32,768 ledgers). Where replay is exact, that is the whole payload:
about **67% smaller than v2 zstd-9, ~10.6 GB per million recent ledgers**,
versus 22.65 GB for the query tier. The rough full-history figure is
0.55–0.6 TB plus periodic state snapshots and early-era corrections.

A restore stays exact because every archived header carries the consensus
state and transaction-tree hashes; each replayed ledger must reproduce both,
and the publisher stores full metadata and object changes for any ledger
that doesn't. Requirements this places on the format:

- **Pin the replay engine.** Corrections are relative to one libxrpl version;
  the archive names it and the restore tool ships it.
- **Checkpoints enable parallel restore.** At 498 tx/s per core, full history
  (a few billion transactions) is months of single-core work. Snapshots at
  interval boundaries let intervals replay independently; their size must be
  measured (recent full state is ~2.8 GB compressed; deltas may be smaller).
- **Measure early and middle eras.** 2013 needs corrections for 13% of
  transactions; 2014–2025 is unmeasured.

### Replay-derived transport tier (E6): original plan

Metadata/object-history pages plus the metadata-only part of the dictionary
make up roughly 40–50% of the best output. If a libxrpl replay reproduced
those records exactly, with residuals stored where it doesn't, the transport
tier could be roughly **55–60% smaller than v2 zstd-9** instead of 29%.
libxrpl 3.2+ includes the transaction engine (`xrpl/tx`), so a probe needs a
small `ServiceRegistry` stub and a prior-state view. The probe must measure,
per era, the exact-match rate for metadata and object changes, the residual
size, replay speed, and the checkpoint cost. Replay makes conversion and
metadata queries CPU-bound, so it suits the transport tier, not the query
tier.
