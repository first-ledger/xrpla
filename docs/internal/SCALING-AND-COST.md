# Scaling and cost: XRPLA, xrpld and Clio for full-history API serving

This is research written on 2026-10-12, about one question: if a load-balanced cluster serves full XRP Ledger history over JSON-RPC and WebSocket, what does each option cost and how far does it scale? The options are XRPLA nodes, xrpld (rippled) full-history nodes, or a Clio cluster.

Each figure is labelled with where it comes from:
- **Measured**: we ran it ([`benchmarks/2026-10-query/load/`](../benchmarks/2026-10-query/load/)).
- **Published**: a vendor or project page, cited.
- **Estimate**: arithmetic on the two above, with its assumptions stated.

Neither xrpld nor Clio has published API throughput benchmarks. The xrpld comparison below is our own measurement; Clio's is an estimate from its architecture and published sizes.

## Summary

- **Per machine, XRPLA served 2.7–3 times xrpld's mixed load on the same hardware and data:** about 13,300 requests a second from compressed pieces and 15,200 from its RocksDB store, against about 5,000 for xrpld.
  - These XRPLA figures are after fixing the two ceilings this study found (see "Ceilings found"); before, it served about 12,200.
  - Neither server was CPU-bound on the common mix: XRPLA used about 12 of the 16 logical CPUs it was given, and xrpld about 1.5.
  - With 10% `account_tx`, XRPLA serves about 9,100 a second from pieces and uses nearly all 16.
- **Per node, storage differs by a factor of about 30.**
  - XRPLA's full history with its indexes is about 1.3 TB.
  - xrpld's is about 42 TB.
  - Clio's is an estimated 10 TB or more per database replica, kept three times.
  - Storage, not CPU, decides what each node costs.
- **Bringing a new node into a cluster:**
  - XRPLA: hours. It downloads about 1.2 TB from R2 (Cloudflare's object storage, with free egress) and builds its indexes.
  - xrpld: "several months" from peers, or a copy of about 42 TB from another node.
- **Approximate monthly hardware cost for a cluster sized for a mixed load, with one spare node:**

| Load (mixed requests a second) | XRPLA | xrpld | Clio |
|---|---|---|---|
| 10,000 | 2 nodes, about €500 | 3 nodes, about $3,600–13,500 | about €2,500–5,000 and up (estimate) |
| 50,000 | 5 nodes, about €1,300 | 11 nodes, about $13,000–50,000 | unknown, scales with database and API nodes |
| 100,000 | 9 nodes, about €2,300 | 21 nodes, about $25,000–95,000 | unknown |

  The assumptions are in "Cost model". Per machine, XRPLA costs one to two orders of magnitude less than xrpld at every load, because each XRPLA node is a commodity server with about 2 TB of NVMe instead of a 40 TB-plus storage server.
- **What XRPLA can't do itself:** submit transactions, report fees, find payment paths, or answer about the open ledger.
  - It forwards those requests (`submit`, `fee`, `ripple_path_find`, `simulate` and the others Clio forwards) to an xrpld set in `[serve] forward`, over HTTP JSON-RPC. A cluster keeps one or two such xrpld nodes, which need only recent history.
  - `path_find`, xrpld's streaming path search, works over WebSocket only. A WebSocket client's path searches are relayed to the xrpld's WebSocket, set in `[serve] forward_ws`. Over HTTP, `path_find` answers `noEvents`, as xrpld does.
  - Forwarded requests are sent once, never retried, and run on their own threads, so a slow xrpld can't hold up history queries. They carry the client's address as `X-Forwarded-For`; with the XRPLA nodes listed in the xrpld's `secure_gateway`, its rate limits apply per client.

## What was measured

| | |
|---|---|
| Machine | Hetzner, Intel Xeon Gold 5412U (24 cores, 48 threads), 125 GB RAM, NVMe RAID1 |
| Server under test | pinned to 16 logical CPUs (about 8 physical cores); load generator on 24 others |
| Shared load | the machine's backfills and audits ran throughout (load average about 14 on the other cores) |
| Data | ledgers 8,000,001–8,300,000 (2014), held by every server under test, warm |
| XRPLA | 0.4.0 on libxrpl 3.4.1, `--threads 32`; piece cache (the main design) and RocksDB store |
| xrpld | 3.4.0, standalone, the same ledgers, WebSocket admin port (no per-client rate limits) |
| Load | many WebSocket connections with pipelined requests, 16 to 1,024 in flight, 20–30 s per level |
| Mix | `account_info` 25%, `tx` 20%, `ledger` 10%, `ledger_entry` 10%, `account_lines` 10%, `book_offers` 5%, `account_objects` 5%, `server_info` 5%; requests drawn from the ledgers themselves |

**Mixed load, before the ceiling fixes** (requests a second; latency p50 / p99 in ms; cores busy):

| In flight | XRPLA, pieces | XRPLA, store | xrpld (`[workers]` 32) |
|---|---|---|---|
| 64 | 9,727; 5.4 / 29; 8.5 | 9,962; 5.3 / 28; 8.2 | 2,291; 22 / 94; 0.8 |
| 256 | 12,123; 19 / 55; 10.1 | 12,198; 19 / 55; 9.7 | 4,248; 57 / 115; 1.4 |
| 512 | 12,628; 39 / 79; 10.1 | 12,125; 40 / 83; 9.8 | 4,956; 102 / 147; 1.5 |
| 1,024 | 12,242; 82 / 131; 10.1 | 11,887; 84 / 139; 9.5 | 4,876; 210 / 272; 1.6 |

- **The XRPLA columns use an experimental build** with its in-flight limit raised from 64 to 1,024 (see "Ceilings found").
- **The build released before this study answered `tooBusy` beyond 64 requests in flight,** so it peaked at about 10,800 a second.
- **xrpld's default config peaked at 4,991 a second** and set 32 workers didn't raise it.
- **Adding `account_tx` (10%) to the mix** halves XRPLA's throughput, to about 6,000 a second. xrpld can't be compared on it: in standalone mode it doesn't serve `account_tx` for these ledgers.

**One method at a time** (64 in flight; CPU per request = cores busy ÷ requests a second):

| Method | XRPLA req/s | XRPLA CPU ms/req | xrpld req/s | xrpld CPU ms/req |
|---|---|---|---|---|
| `server_info` | 45,379 | 0.04 | 13,342 | 0.16 |
| `ledger` (header) | 37,106 | 0.05 | 18,063 | 0.14 |
| `account_info` | 31,160 | 0.11 | 13,126 | 0.17 |
| `tx` | 14,076 | 0.48 | 5,711 | 0.26 |
| `account_lines` | 7,230 | 1.4 | 3,631 | 0.38 |
| `book_offers` | 1,557 | 8.9 | 1,149 | 1.1 |
| `account_tx` | 785 | 9.9 | not comparable | |

- **XRPLA delivers more requests a second on every method.**
- **Per request, xrpld is cheaper in CPU on `tx`, `account_lines` and above all `book_offers`.** xrpld reads current state directly, while XRPLA rebuilds historical objects from compressed pages.
- **xrpld can't turn its efficiency into throughput:** it never uses more than about 2.5 cores, whatever the load.

**Mixed load, after the ceiling fixes** (same test; `--threads 32`, so up to 512 requests in flight):

| In flight | XRPLA, pieces | XRPLA, store |
|---|---|---|
| 64 | 12,314; 3.0 / 36; 12.1 | 12,108; 4.3 / 26; 9.8 |
| 256 | 13,062; 16 / 59; 12.2 | 14,735; 15 / 57; 11.9 |
| 512 | 13,337; 34 / 89; 11.9 | 15,213; 31 / 82; 11.9 |
| 1,024 | 13,027; 74 / 149; 11.8 | 15,230; 64 / 131; 12.3 |
| 1,024, with 10% `account_tx` | 9,077; 108 / 184; 14.2 | 9,691; 100 / 192; 15.7 |

No errors. With `account_tx` in the mix, throughput rose by half (from about 6,000) and the servers use 14–16 of their 16 CPUs: that mix is now limited by CPU.

### Ceilings found

Both of the first two are fixed. The figures are in the "after the ceiling fixes" table above.

- **XRPLA's in-flight limit was fixed at 64 requests** (`core/src/server.cpp`), whatever `--threads` was; past it, requests got `tooBusy`.
  - It is now 16 per worker thread. The default of 4 threads keeps 64, and `--threads 32` allows 512.
  - Raising it to 1,024 in an experiment had lifted peak throughput from about 10,800 to about 12,600 a second.
- **XRPLA's single network thread serialised every response to JSON** and wrote it to the socket. Both back ends stopped at the same rate with 6 of their 16 CPUs idle.
  - Replies are now serialised on the worker threads, and the network thread only frames and writes them.
  - Peak mixed throughput rose to about 13,300 (pieces) and 15,200 (store), and to about 9,100–9,700 with `account_tx`.
  - The common mix still stops at about 12 busy CPUs. The network thread's remaining work (WebSocket framing, writes, request parsing) is the likely next limit; several network threads would remove it.
- **XRPLA's expensive methods.**
  - `book_offers` (about 9 ms of CPU) and `account_tx` (about 10 ms) cost tens of times more than a typical request.
  - `account_lines` (about 1.4 ms) costs 4 times xrpld's.
  - They decide capacity for mixes heavy in order books or account history, and are the main optimisation targets.
- **xrpld's ceiling** of about 5,000 mixed requests a second on about 1.5 cores held from 16 to 1,024 in flight and didn't move with `[workers] = 32`. It looks like a serialisation point in xrpld's request handling or ledger access.
  - A production xrpld also applies every new ledger and takes part in the network, which leaves less for the API.
  - On public ports it rate-limits each client IP. Behind a load balancer, everything arrives from the balancer's address, so the cluster needs the balancer treated as a trusted gateway (`secure_gateway`) or the limits raised.

### How far these numbers carry

- **The test data fits in memory.** At full history, cold reads go to disk on every server. For XRPLA, that means opening segments (about 22 ms each on 2014 data and about 80 ms on 2025 data; see `QUERY-BENCHMARKS.md`). For xrpld, it means NuDB and SQLite reads from about 42 TB, where xrpl.org asks for 10,000 sustained IOPS.
  - XRPLA's cold costs fall with memory: the default cache is a quarter of RAM (3-64 GB).
  - Measuring a full-history cache is the next benchmark, once the publication is complete.
- **2014 ledgers are smaller than today's.** Requests about recent ledgers carry more data, so both servers serve fewer per second.
- **The servers had 16 logical CPUs on a shared machine.** A dedicated 16-core, 32-thread server would do more per node.
- **The mix is our assumption.** No public traffic mix for XRPL API servers exists. Order-book and account-history requests are the expensive ones on both servers.

## Storage and bootstrapping per node

| | XRPLA | xrpld (full history) | Clio |
|---|---|---|---|
| Full history on disk | about 1.3 TB, measured and projected: about 1.13 TB of compressed history plus about 15% indexes (`SIZE-AND-FOOTPRINT.md`, `QUERY-BENCHMARKS.md`) | about 39 TB as of 2026-01-25 (published, xrpl.org "Configure Full History"); about 42 TB now (estimate, at about 12 GB/day) | "up to 4 times less" than xrpld (published, Clio 1.0 announcement): 10 TB or more per replica, 30 TB or more raw at the default replication factor of 3 (estimate) |
| Growth | about 0.45 GB/day (measured: history plus index) | about 12 GB/day (published) | about 3 GB/day or more per replica (estimate) |
| Recommended disk | NVMe; 2 TB fits full history with years of growth | SSD/NVMe with 10,000 sustained IOPS, not EBS (published); over 16 TB, so no ext4 | NVMe for Cassandra/ScyllaDB (general practice) |
| New node, from nothing | download about 1.2 TB from R2 (free egress; at the measured 120 MB/s, about 3 hours) plus index building (hours) | "several months" from peers (published), or a copy of about 42 TB from a running node (about 4 days at 1 Gbit/s) | backfill through xrpld: months; or a database copy |
| Where data comes from | the signed publication (R2), verified by hash | peers, or another operator's database | an xrpld (gRPC) feeding a single writer |
| Writes while serving | none: read-only data | applies every ledger | one writer node ingests; readers serve |

The XRPLA publication itself costs about $18 a month: about 1.2 TB at Cloudflare R2's $0.015 per GB-month, with free egress, so downloads cost the publisher nothing. Every node in every cluster reads that same copy.

## Cost model

Prices are published list prices, monthly, excluding VAT, gathered on 2026-10-12. They have been volatile in 2026: Hetzner raised prices on 2026-04-01 and 2026-06-15 on RAM and SSD costs.

**Per-node hardware:**

| Role | Example | Monthly | Source |
|---|---|---|---|
| XRPLA node | Hetzner AX102 (Ryzen 9 7950X3D, 16 cores / 32 threads, 128 GB RAM, 2×1.92 TB NVMe) | €257 | published, Hetzner, 2026-06-15 prices |
| XRPLA node, cloud | AWS i4i.2xlarge (8 vCPU, 64 GB, 1.875 TB NVMe) | about $500 | published, on-demand |
| xrpld full history, dedicated | about 46 TB NVMe (OVH Scale, 6×7.68 TB) | $1,000+ (estimate; no list price for the full configuration) | partly published |
| xrpld full history, dedicated | Latitude.sh, 64 cores, 32 TB NVMe | $3,971 (too little disk for 42 TB without more drives) | published |
| xrpld full history, cloud | AWS i7ie.48xlarge (120 TB NVMe) | about $18,200 | published |
| xrpld recent history (submit, fee) | Hetzner AX102 | €257 | published |

**Clusters** for a mixed load, with one spare node (N+1). These are estimates:
- **XRPLA:** 13,000 mixed requests a second per node, measured on about half the CPU of the server above. It needs one AX102 per node and nothing else, since the publication is shared and costs about $18 a month.
- **xrpld:** 5,000 per node, measured. It needs a 42 TB-plus NVMe server per node, at $1,200–4,500 a month on dedicated hardware.
- Neither column includes the load balancer or bandwidth.

| Load | XRPLA nodes | XRPLA monthly | xrpld nodes | xrpld monthly |
|---|---|---|---|---|
| 10,000/s | 2 | about €500 | 3 | about $3,600–13,500 |
| 50,000/s | 5 | about €1,300 | 11 | about $13,000–50,000 |
| 100,000/s | 9 | about €2,300 | 21 | about $25,000–95,000 |

- **A mix heavy in `account_tx` or order books** lowers XRPLA's per-node figure: about 9,000 a second with 10% `account_tx`. That raises its node counts by about half, still a small fraction of xrpld's cost.
- **A dedicated 16-core, 32-thread node** has about twice the CPU of the 16 logical CPUs measured. For the CPU-bound mixes that should mean up to about twice the throughput (an estimate, not measured).
- **A cluster of XRPLA nodes still needs one or two xrpld nodes** for `submit`, `fee` and path finding. They hold recent history only, at about €257 each.

**Clio**, an estimate only. Neither its throughput nor its cluster sizing is published.
- **What it needs at minimum:**
  - a Cassandra or ScyllaDB cluster of three or more nodes, each with about 10 TB or more of NVMe;
  - two or more Clio API servers;
  - an xrpld to feed it.
- **What that costs before any scaling:** with 10 TB-class dedicated nodes at about €600–1,600 a month each (Hetzner AX162-class; the configurations behind each price tier aren't published), the base is about €2,500–5,000 a month.
- **How it scales:** Clio's API servers are stateless and scale out cheaply, but reads go to the database cluster, which scales by adding 10 TB-plus nodes. So Clio's cost per request probably sits between XRPLA's and xrpld's.
- **What's needed to place it properly:** this same load test against a Clio cluster loaded with the same ledgers.

## How each scales

- **XRPLA scales by copying a server.**
  - Each node is independent and read-only, and serves the whole history from local disk. There is no shared database, no single writer and no coordination between nodes.
  - Adding capacity means starting another commodity server. It syncs from R2 in hours.
  - Removing capacity means stopping one; nothing is lost.
  - Nodes match exactly: they serve the same signed publication, verified by hash.
  - For the newest ledgers, a node follows the network itself (the `standalone` profile) or serves what the publication has sealed (the `mirror` profile, minutes behind).
- **xrpld scales by adding whole full-history nodes.**
  - Each one stores and applies the entire ledger: about 42 TB of fast NVMe, growing by about 4.4 TB a year.
  - Adding a node takes months from peers, or days of copying.
- **Clio separates serving from storage.**
  - API servers scale cheaply.
  - The database is a stateful cluster that needs capacity planning, replication and repair.
  - Ingestion has a single writer fed by xrpld.

## Next steps

1. **Several network threads** (per-session strands), to remove the remaining ceiling on the common mix.
2. **Cut the CPU cost of `book_offers`, `account_tx` and `account_lines`.**
3. **Run a full-history load test** once the publication is complete, with cold-read latency on a node with the default memory budget.
4. **Run the same load test against a Clio cluster** loaded with the same ledgers, to replace the Clio estimate.
5. **Measure a production-configured xrpld** (following the network, public port behind a trusted load balancer) for a realistic per-node figure.
