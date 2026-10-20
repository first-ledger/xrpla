# Parity test plan

What this proves: the history XRPLA publishes answers every query exactly as the XRP Ledger's
own servers do. That holds whether it is served by XRPLA itself, over HTTP JSON-RPC and
WebSocket, or restored from the publication into an xrpld (NuDB and SQLite) or a Clio
(ScyllaDB/Cassandra). The results go into a report (`PARITY-REPORT.md` and a PDF): every
comparison made, every difference found, and why each remaining one is not about the data.

Hashes already prove the ledgers themselves. Every ledger's state tree, transaction tree and
header chain are checked against validator-signed hashes when the history is published, synced
and restored. Parity covers what hashes cannot: the indexes and derived answers each server
builds (`tx` by hash, `account_tx` order and paging, owner directories, books, synthetic fields
such as `delivered_amount`), and each server's API behaviour (API versions, binary forms,
errors, markers).

## Servers compared

| Name | What | History |
|---|---|---|
| **R** | Reference: an independent full-history xrpld (xrplcluster.com; s2.ripple.com to check the reference itself) | full |
| **A** | XRPLA serving the published piece cache (`xrpla run`, standalone profile) over HTTP, WebSocket, HTTPS and WSS | full |
| **A′** | XRPLA serving only X's range, synced from R2 beside X and C | as X |
| **X** | xrpld 3.4.1 started on a NuDB and SQLite restore from R2 | one recent bundle plus the pieces after it |
| **C** | Clio (current release) on a ScyllaDB restore from R2 (`xrpla restore --to clio`) | the same range as X |

## Comparisons

1. **A against R, all of history.** Ledgers sampled from every one of the publication's
   million-ledger bundles (about 108): 16 a bundle, half at random and half at chosen points.
   The chosen points:
   - each bundle's first and last ledger, and the piece boundaries;
   - flag ledgers;
   - each amendment's activation ledger;
   - the 2013 ledgers holding transactions xrpld cannot decode;
   - ledgers whose transactions were read from peers.

   Requests are derived from each ledger's contents, as `tools/xrpld_parity.py` does today. A
   fixed corpus of heavy cases is added: exchange accounts with long `account_tx` histories,
   the largest order books, AMMs, NFTs, MPTs, oracles, vaults, escrows, checks, payment channels,
   DIDs and credentials.
2. **A over WebSocket against A over HTTP, and over TLS.** Every request of comparison 1 sent
   over HTTP, WebSocket, HTTPS and WSS must give the same answer. This runs on A's machine, so
   at full volume.
3. **A's streams against a live xrpld.** For 24 hours, `tools/stream_parity.py` compares the
   ledger, transactions, accounts and books streams, message for message, with xrpld's.
4. **X against R, X against A′, and A′ against A, on the restored range.** The same derived
   requests, plus paging every `account_tx` and `ledger_data` marker to the end for a sample of
   accounts. X against A′ runs on X's machine at high volume; X against R confirms
   independently, and A′ against A (a sample, over the public endpoint) ties the local copy to
   the full node. X must also show it was restored and is in use:
   - it starts from the restored tip ledger, loaded from disk;
   - it reads its state from NuDB rather than fetching it from peers (`get_counts`);
   - its complete ledgers run unbroken from the range's first ledger;
   - it reaches `full` on mainnet and stays there.
   X also gives a fair speed comparison: X and A′ serve the same recent ledgers on the same
   machine, under the same load (`load-bench.py`, the common and full mixes, HTTP and
   WebSocket).
5. **C against R, and C against A′, on the restored range.** As comparison 4, with the same
   speed comparison. Clio's own methods (`nft_info`, `nft_history`, `nfts_by_issuer`,
   `mpt_holders`, `ledger_range`) are compared C against A′, as xrpld has none of them. C must also take new ledgers from an xrpld after the
   restored tip (its ETL), showing that the restored database is one Clio continues.

Beside the comparisons, A runs as an open full-history node, its long-running behaviour
measured:

6. **Peer network stability**, for weeks: lag behind the network's validated ledger, gaps and
   how they were filled, peer churn and why peers were dropped, restarts, memory and disk over
   time, and spool rotation as the publication advances.
7. **A's throughput and latency**, HTTP, WebSocket, HTTPS and WSS, with `load-bench.py` (the
   common and full mixes, cold and warm cache, the server and the load generator on separate
   cores). A is public, so each run records the outside request rate during it; numbers for
   the report come from runs with little outside load.
8. **A new node's start**: a spool from R2's tip, then the gap to the network from peers, timed
   from an empty disk (first measured: 7 min for the spool, then 2 h 10 min from peers for a
   5,000-ledger gap; with 250-ledger tip fragments the gap is a few hundred ledgers).

And on the peer protocol, A behaves on the wire as xrpld does:

9. **The same peer requests to an xrpld and to A** (`xrpla peer-compare`): headers by hash and by
   sequence, transaction and state tree nodes at query depths 0-3, several nodes per request,
   nodes by hash in node-store form, and a ledger neither holds (no answer). Answers compared
   whole, node order apart. First run (2026-10-15, r.ripple.com, xrpld 3.4.1): 65 of 65
   identical, in the same order; ledger replay not compared (r.ripple.com does not enable it).
10. **An xrpld whose only peer is A**: an isolated xrpld 3.4.1 on A's machine, with A serving on
    loopback. First run (2026-10-15): it reached `full` from validations and proposals A relayed,
    stayed connected throughout (22 minutes), and every ledger it built (11 sampled) and acquired
    on request (3 older ledgers, by hash, only A could supply) was identical to s2.ripple.com's.
    Findings: A's serving charges (modelled on xrpld's) dropped this xrpld five times while it
    fetched its first ledger's whole state (6.8 GB from A alone) until its key was listed in
    `[peers] reserved`, as xrpld's own cluster peers are; and with A as its only peer xrpld moves
    between `full`, `tracking` and `syncing`, since A serves no consensus transaction sets (it is
    not in consensus) and has each validated ledger a second or two after the network.
11. **A good citizen** (`tools/peer_health.py`, hourly on A and on the publisher): requests left
    unanswered and sent twice, sessions peers ended early, closings for load. First hour on
    each: 0.14-0.26% unanswered, 1.2-1.5% sent twice, no session ended early by a peer, none
    closed for load.

Each comparison covers:
- **methods:** every history method both servers answer (the list in `xrpld_parity.py`, extended
  to all `ledger_entry` types, `ledger` with transactions expanded and binary, `account_objects`
  by type, `book_changes`, `gateway_balances`, `noripple_check`, `deposit_authorized`,
  `get_aggregate_price` and `vault_info`);
- **API versions** 1 and 2, and binary and JSON forms where a method has both;
- **errors:** malformed parameters, unknown objects and ledgers outside the range give the same
  error codes;
- **paging:** markers followed to the end, forwards and backwards.

## Invariant checks

Comparisons need a reference, which is slow and rate-limited. `tools/xrpla_invariants.py` needs
none: it checks A's derived answers against the records the hashes cover (each ledger's
transactions with metadata, and its state through `ledger_entry`, `ledger_data` and
`account_nfts`), so it can sweep all of history at A's own pace (8 requests at a time by
default). For each sampled ledger L:
- **`account_tx`** lists each transaction of L under exactly the accounts xrpld's
  `TxMeta::getAffectedAccounts` finds in its metadata (every account field of each node's
  NewFields or FinalFields, the issuers of LowLimit, HighLimit, TakerPays and TakerGets, and
  the issuer of MPTokenIssuanceID), and nothing else in L; accounts no transaction of L
  touches list nothing there.
- **`tx`** by hash gives L, the same TransactionIndex, `tx_json` and `meta` as the ledger.
- **`nft_info`** for tokens touched in L (and a sample of their issuers' others) agrees with
  the owner's NFToken pages at L (there if and only if not burned, same URI and ID fields) and
  with what L's NFTokenPage changes leave; **`nfts_by_issuer`** entries are `nft_info`'s answers.
- **`nft_history`** at L lists every transaction Clio 2.8's ETL records for the token
  (`etl/NFTHelpers.cpp`: successful mints, burns, offers created, cancelled and accepted, and
  modifies) and none that neither names the token nor changes its page entry.
- **`mpt_holders`** entries are the issuance's MPTokens in state, holders L's metadata leaves
  are listed and deleted ones are not (with `--scan-state`, exactly `ledger_data`'s MPTokens);
  **`account_mptokens`** agrees with them.
- **`book_offers`** returns the offers, in order, of xrpld's `getBookPage` walk over the book's
  directories in state (found by successor through `ledger_data`), each the Offer in state.

Every violation is printed with the exact requests that show it. The `invariants_*` tests run
it over every ledger of the fixtures, served both as a store and as a piece cache, and require
none. Its first run found one: a token burned from an NFToken page that was then deleted (the
token only in the page's PreviousFields) stays live in `nft_info`.

A check that needs a listing longer than `--page-cap` pages (default 200), such as the
NFToken pages of an account holding hundreds of thousands of tokens, can't be decided. It is
counted as `skipped`, not as passed or violated.

## Rules

- **Answers are compared whole**, after removing only the fields that describe the server
  rather than the ledger (`status`, `warnings`, `api_version`).
- **Every other difference counts.** A difference is ignored only by a named rule, kept in the
  tool and listed in the report with its reason. For example:
  - xrplcluster.com's `book_changes` omits a `type` that stock xrpld sets;
  - xrpld repeats `ctid` inside `tx_json` for transactions in its cache.
- **Reference noise is separated from real differences.** A reference that cannot answer (a
  load-balanced node without the ledger) is retried and counted apart, and a reference answer
  that changes when asked again is recorded as "reference inconsistent". Neither counts as a
  match.
- **Every run is recorded:** machine, builds and versions (XRPLA commit, xrpld, Clio, ScyllaDB),
  publication generation, sample seeds, and per-method counts, timings and differences, as JSON
  lines under `docs/benchmarks/parity/`. Every run is repeatable from these.

## Machines

- **Test server** (test-host, Ryzen 7 7700, 61 GB, 2 × 1 TB NVMe striped): A, kept running
  as an open full-history node at `xrpla-test.example`, plain HTTP and WebSocket and
  native HTTPS and WSS. It runs comparisons 1–3 and the long-running checks 6–8. The piece cache
  with its indexes is about 1.3 TB of its 1.9 TB.
- **Publisher** (publisher-host, Xeon Gold 5412U 24 cores/48 threads, 126 GB, 2 × 3.5 TB NVMe
  mirrored): X, then C, each beside A′, for comparisons 4 and 5. It is also the mainnet
  publisher (now following peers only), and runs a soak and a Postgres replica, so the restores
  are fitted around them:
  - disk: about 1.4 TB free. X is about 1.15 TB, so it is measured before starting and the
    publisher keeps its 50 GB floor; X is removed before C (about 0.5–1 TB, to measure);
  - memory and cores: the publisher's encoder keeps a reserve, and the soak uses about 35 GB;
    X, C and the load generator are pinned to cores the publisher does not need;
  - the publisher's lag behind the network is watched throughout: a publisher following peers
    must stay within the peers' history.
- **The two machines are not interchangeable for speed** (sysbench and fio, 2026-10-15, the
  publisher under its usual load): per core the Ryzen is about twice as fast (sysbench cpu,
  1 thread: 2,276 against 1,147 events/s; 8 threads: 17,552 against 8,136), while the Xeon's
  48 threads give about a quarter more in all (23,685 against 18,847). Memory bandwidth (114
  against 126 GiB/s with 8 threads) and random 4 KiB reads (881k against 939k IOPS) are alike;
  sequential reads are 13.7 against 9.7 GB/s. Answers do not depend on the machine, so the
  parity comparisons can run across machines. Speed is only compared on one machine: X and C
  against A′ on the publisher, each server given the same cores. A full-history A does not fit
  there beside X (1.3 TB plus 1.15 TB against 1.4 TB free), and A′ holds exactly X's range, as
  a fair counterpart should.
- **Speed runs on the publisher** share it with the publisher, the soak and the replica, so:
  the servers measured and the load generator run on cores the publisher leaves alone (for
  example 16-39 of 0-47), each server measured gets the same number of them, runs alternate
  between servers (A′, X, A′, X) so drift in the background load falls on both, the load
  average and the publisher's lag are recorded with every run, and no run overlaps a piece or
  bundle being encoded. A run stops if the publisher falls more than 50 ledgers behind.
- The reference is public: comparisons against R keep to 4 requests at a time.

## Order and estimated time

On the test server (done: A synced from R2 in 8 h 8 min, 3 h download and 5 h indexing;
following the network from peers since):
1. Comparisons 1 and 2: about a day, set by the reference's rate.
2. Comparison 3: 24 hours, running beside the others.
3. Checks 6–8: from now on, reported at intervals.

On the publisher, alongside:
4. Sync A′ for X's range; restore X (about 2–4 hours); bring it up and show it syncs;
   comparison 4 (hours).
5. Remove X; restore C (to measure); bring Clio up; comparison 5 (hours).

Then the report: `PARITY-REPORT.md` and the PDF from the recorded results.
