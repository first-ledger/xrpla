# Publication: pieces, bundles, catalogs and R2

How XRPLA turns validated history into small, signed, downloadable chunks,
keeps publishing as the ledger grows, and lets clients pull any range back.
Size decisions behind the format are in [SIZE-AND-FOOTPRINT.md](internal/SIZE-AND-FOOTPRINT.md).
To run a publisher step by step, see [Run a publisher](guides/publish.md); terms are
explained in the [glossary](reference/glossary.md).

## Layout

History is cut into fixed, aligned **intervals** of 1,000,000 ledgers
(`[k·1,000,000 + 1, (k+1)·1,000,000]`), each split into twenty **pieces** of
50,000 ledgers. Completed intervals are published as one **bundle**; the
active interval is published piece by piece. When an interval completes, its
twenty pieces are rolled into a bundle made from the *same segment bytes*
(nothing is re-encoded), the catalog switches to the bundle, and the pieces
are deleted after a grace period (seven days by default). The first published
piece may start mid-interval, where the source's history begins. Piece and
interval sizes are configurable; tests use tiny ones.

A piece takes about two days of mainnet ledgers to fill. With
`--fragment-ledgers N` (the Hetzner publisher uses 5,000, about 5.4 hours), the
piece still filling is published in **fragments** of N ledgers. They are listed
in the catalog's `tail` (`fragment_ledgers`, `tip` and `ranges`, each range like
a piece's, with kind `fragment`), stored under `fragments/`, and continue the
chain of `ranges`. A reader that knows only `ranges` ignores the tail, and its
`tip` stays the newest piece's. When a piece completes, it is joined from its
fragments' segments, as a bundle is from its pieces: the fragments published as
the tail, plus the rest encoded then. So a piece is encoded once, and the tail
fragments it covers retire after the grace period. The publication then trails
the network by under N ledgers instead of up to a piece. `sync`, `restore` and
the standalone profile follow the tail.

With `--tip-fragment-ledgers M` as well (the Hetzner publisher uses 250, about
15 minutes), the complete M-ledger fragments after the newest N-ledger one end
the tail, so a new node restores to within M ledgers of the network and
catches up the rest from peers in minutes. They are never joined: each N-ledger
fragment is encoded whole when it completes, since small segments compress
worse, and the small fragments it covers retire after `--tip-grace-seconds`
(a day by default), keeping the catalog's retired list short.

Every interval has a **state snapshot** at its start (the ledger before its
first ledger), written as soon as the source reaches that boundary. A restore
starts at a snapshot and applies history forward, so any interval can be
restored without earlier history.

## Formats

- **XRPP container** (`tools/xrpp.py`), used for pieces and bundles:
  a directory (range, parent and last ledger hash, transaction count, one row
  per codec segment with offset/length/SHA-256, index descriptor), then the
  XRPH segments (`tools/history_codec.py`, zstd 19, hash recipes 1–10), then
  the XTXI transaction index (`tools/tx_fingerprint_index.py`), then a
  SHA-256 trailer. The catalog stores both the file and directory hashes, so
  a reader can fetch the directory with one ranged read and then single
  segments, each checked on its own.
- **Snapshots** use the existing v2 snapshot format (sorted keyed objects in
  zstd parts plus `index.json`), written by `xrpla_snapshot`.
- **Catalog generations** (`catalog/NNNNNNNN.json` + `.sig`) are signed with
  the publisher's Ed25519 key and list the layout, codec, snapshots, ranges
  (which must form a hash chain), retired objects and tip.
  `catalog/current.json` points at the newest generation and is written last.

## Operation

| Tool | Role |
|---|---|
| `piece_publisher.py` | One idempotent run: encode complete pieces (every segment decoded by the C++ reader and compared byte for byte), roll complete intervals into bundles, write interval snapshots, reclaim expired pieces, sign a new generation. Holds an exclusive lock on the bucket. `--encoder` and `--index-tool` use the compiled encoder and XTXI builder. |
| `live_publisher.py` | Follows a node with `xrpla publish` into a signed v2 spool, runs the piece publisher, optionally pushes to R2, and rotates the spool at each interval boundary: the new spool is the published boundary snapshot plus the old spool's later ledgers, so the node never re-exports state and the spool stays about one interval long. With `--push`, the local copies R2 then holds (each read back intact after its upload, and still there) that nothing local reads again are deleted: bundles and the snapshots before the next rotation, so a publisher's disk holds about one interval (`--keep-pushed` keeps them). Below `--min-free-gb` free space (default 5; the Hetzner publisher sets 50) the follower pauses until space returns. Keeps `WORKDIR/status.json` current; `--health --workdir WORKDIR` exits non-zero and lists the problems when the runner, follower, spool, publishing or pushing has stalled (30 minutes by default). |
| `s3_remote.py push` | Mirrors a generation to R2/S3: uploads new objects (multipart when large), reads each back and checks its SHA-256, uploads the signed generation, moves the pointer last, then deletes the remote objects the generation no longer uses that are gone locally (reclaimed pieces; the bundles and snapshots of a publication it replaced on the prefix). What the generation the pointer leaves uses and the new one does not stays through its grace period counted from the move, not from its local retirement (the bucket's `.remote-retired` record, written before the pointer moves, so a push that fails after the move or is run again keeps it too); so do the pieces and fragments the catalog retired, through theirs, and the copies of a merge not yet finished (`.merging`, reported by every push while it lasts). Every object read back intact is recorded in the bucket's `.pushed` file (compacted under the lock once past 16 MiB and twice what it keeps: the last record of each object the remote still holds, and the latest pointers); one already under the same name remotely that no push recorded is read back once and replaced when it differs, unless the generation the remote pointer names uses it: then the push is refused, since that generation's readers would find other bytes (replacing a publication takes a fresh prefix). A prefix holding another publisher key is refused, and so is a remote pointer no push from this bucket recorded in `.pushed` (each push records the pointer it moves to, before the move): the bucket may lack the records that keep objects readers still use (see Moving a publisher below), and `--adopt-remote` takes such a remote as it is, keeping every object the local catalog does not name through the longest grace period (recorded in `.remote-retired` before the pointer is recorded in `.pushed`, so a push that stops part way keeps them too). A `.pushed` that records objects alone (from before pushes recorded pointers) is taken at its word, but not while `.merging` is there: a merge records its copies in `.pushed` as well. An object the pointer leaves that the catalog does not list as retired keeps the longest grace period the catalog gives, never less than the default seven days. Holds the bucket's publisher lock, and exits when another process holds it. The pointer only moves forward: a push refuses a remote generation that is newer or differs, and writes the pointer with `If-Match` on the ETag it read first (`If-None-Match` for the first push), so two publishers on one prefix fail instead of overwriting each other. `XRPLA_S3_PREFIX` lets several publications share a bucket. |
| `publication_merge.py` | Joins an earlier publication onto the front of this one, for a history filled from both ends under two prefixes (one publisher per prefix). The earlier catalog must carry the same publisher key, network, layout and codec, consist of whole bundles, and end at the ledger before this publication's first, with that ledger's hash this publication's parent hash. Its bundles and snapshots are copied to this prefix inside the store and each copy is read back against its SHA-256; its snapshot indexes are written to the local bucket; the joined catalog is signed as the next generation and pushed, the pointer moving last. A snapshot both hold stays this publication's own. Stop both publishers first. A remote pointer the bucket's `.pushed` does not record is refused before anything is written, as a push refuses it. Without `--apply` it prints the plan and writes nothing; run again after a stop, it does not repeat verified copies. The earlier prefix is left as it is. A merge that will not be finished is given up with `--abandon`: its `.merging` record goes (with the snapshot indexes it wrote locally that the catalog does not name), and the pushes that follow delete its copies like any other object neither named nor held locally. |
| `piece_client.py` | `sync` a signed range up to a configured ledger (local bucket or `--remote`, eight objects at a time), `query` ledgers and transaction hashes from the compressed data, or `restore` a range into a v2 archive for `xrpla verify --full`, the NuDB importer and the other v2 tools. |
| `xrpla index-pieces`, `xrpla serve` | Build the local query index for a synced cache, then serve the rippled RPC surface from the compressed data (see Querying compressed history). |

`config/xrpla-publisher.service` runs the live publisher under systemd from an
installed package: credentials and `XRPLA_S3_PREFIX` in
`/etc/xrpla/publisher.env`, the spool and publisher seeds in `/etc/xrpla`, and
state in `/var/lib/xrpla-publisher`. The first start of a new publication needs
`--start FIRST_LEDGER` once.

Moving a publisher to another machine: stop it (and any push or merge), then copy
the whole local bucket, its dotfiles included, before starting it on the new one:
`.pushed` (the objects and pointers pushes verified), `.remote-retired` (what a
pointer move left, kept remotely through its grace period from the move) and
`.merging` (the copies of a merge not yet finished). They are kept nowhere else,
and without them a push would delete remote objects readers may still be
fetching; so a push refuses a remote pointer that `.pushed` does not record. If
they are lost, push once with `s3_remote.py push --adopt-remote`: every remote
object the local catalog does not name is then kept through the longest grace
period (seven days by default) before the pushes that follow delete it.

Live following polls the node every 600 seconds by default, so the spool
manifest holds about 150 ledgers per chunk and stays far below its 16 MiB
limit for a whole interval; the runner warns if it grows past 12 MiB.

## Measured on the million-ledger archive (2026-09-23)

Source: mainnet ledgers 106,027,812–107,027,811 as a signed v2 archive. The
published range is 106,027,812–107,000,000 (the next interval had no complete
piece yet).

| Measure | Result |
|---|---|
| Bundle | **22.85 GB** for 972,189 ledgers and 111,889,144 transactions (339 segments) |
| Same ledgers as v2 zstd-9 chunks | 31.23 GB, with no transaction index |
| Saving versus v2 | **26.9%** including the bundle's own index; 28.9% for the codec alone |
| Transaction index | 637.6 MB, **5.70 bytes per transaction** (SQLite would be ~5 GB) |
| Interval snapshot (106,027,811) | 2.57 GB at zstd 19 for 19,241,572 objects |
| Publishing | 3 h 07 min for 18 pieces + snapshot, then 1 h 29 min for 2 pieces + rollover, on 9 cores (~106 ms per ledger per core, Python encoder) |
| Client sync | 25.4 GB fetched and hash-checked in 191 s, 42 MB RSS |
| Queries | 20/20 transactions found through the index; 8 s per ledger and 18 s per transaction lookup in the Python client, each in a fresh process. The C++ query server replaced this path; see Querying compressed history. |
| Restore to a v2 archive | 51 min for all 339 segments; ends at the catalog's signed tip hash |
| Push to R2 (16 streams) | 22.85 GB bundle uploaded and read back verified in 284 s |
| Download from R2 (client `sync --remote`) | 25.4 GB (bundle + snapshot) in 491 s, every object checked against the catalog |
| R2 throughput, 256 MiB object | 1 stream: 9.8 MB/s up, 15.7 MB/s down; 16 streams: 56.5 MB/s up, 99.2 MB/s down |

On a live mainnet node the runner published pieces, rolled intervals into
bundles, pushed every generation to R2 with read-back verification, and
rotated its spool at interval boundaries.

## Compiled encoder

`xrpla_history_encode` (`tools/history_writer.hpp`) is a C++ port of
`history_codec.encode`. `piece_publisher.py --encoder PATH` (and
`live_publisher.py --encoder PATH`) uses it; with `--check` the encoder decodes
its own output with the C++ reader before a segment is accepted.

- **Same format.** `tests/history_writer_test.py` compares every uncompressed
  section with the Python reference, and both decoders must reproduce the
  canonical bytes. Identical on the synthetic fixtures, on 1,024 ledgers from
  2013, and on 8,192 modern mainnet ledgers from 106,027,812.
- **Speed** (1,024 modern ledgers, busy 12-core host). About 12 ms per ledger
  for the codec itself, about 18 ms for zstd level 19 and about 2 ms for
  `--check`, so roughly 30 ms of CPU per ledger in total. Over 12,288 ledgers
  it took 51 ms of wall time per ledger against 127 ms for the Python encoder
  on the same segments. zstd 19 is now most of the cost; level 9 halves the
  time but writes 10% more bytes, so level 19 stays the default.
- **Threads.** `--threads N` compresses a segment's sections on N threads
  with identical output: 1,024 modern ledgers took 19.5 s with 8 threads
  against 42 s with one. The piece publisher gives each encoding job an equal
  share of the host's cores, so a single pending piece still uses all of them.

`xrpla_tx_index --out INDEX CONTAINER...` is the compiled XTXI builder used at
bundle rollover. It checks each segment's SHA-256, reads only the transaction
sections, and writes the same bytes as the Python builder: the million bundle's
637.6 MB index was rebuilt identically in 153 s.

## Snapshots and the follower's state

`xrpla_snapshot` replays a spool to an interval boundary and writes the
snapshot. It streams the base snapshot's sorted objects straight into a
disk-backed SHAMap (hashing leaves on several threads and bulk-loading
RocksDB without its write-ahead log), walks the final state tree in parallel,
and compresses parts on `--jobs` threads. For the 19,921,276-object snapshot
at 107,192,320 on the live test host it took 567 s and 5.9 GB RSS, against
2,558 s before; the output was byte-identical to the published snapshot.

`xrpla publish` and `xrpla_snapshot` keep the verified state tree in a node
store that only grows as ledgers apply (about 0.7 MB per mainnet ledger).
Every 16,384 ledgers (`--compact-every`) the store is compacted by copying the
nodes reachable from the current root into a fresh store and swapping it in;
a crash during the swap is recovered on the next start. Any long verification
(`verify --full`, imports, a following publisher) also compacts on its own once
the nodes written since the last compaction reach twice the live set; before
that, a `verify --full` of the million grew its store past 200 GB. A following
publisher (or standalone node) compacts on a thread of its own while ledgers
keep applying: nodes written meanwhile go to the copy as well, and the copy is
swapped in at the end of a later batch, a pause of about a second. Stopped for
the walk, a standalone node on HDDs fell 186 ledgers behind for 12 minutes.

## Querying compressed history

`xrpla serve --dir CACHE` and `xrpla rpc --dir CACHE` answer RPC straight from a
piece cache (`xrpla sync`). Nothing is restored or imported. `xrpla sync` builds the
local query index; to build it by hand:

```
xrpla index-pieces --dir CACHE [--jobs N]
```

It writes two local, derived files next to the cache (`CACHE/query/`). Neither
is published, and both can be rebuilt at any time from the signed data:

- **XQSS**, one per snapshot the cache keeps (below). The snapshot's sorted objects are re-cut
  into ~32 KiB blocks and compressed with one zstd dictionary trained on the
  snapshot. A point read then decompresses one block instead of a 16 MiB
  published part.
- **XQIX**, one per container. It holds:
  - ledger headers;
  - a ledger-hash map;
  - each ledger's TransactionIndex order;
  - account and NFT postings, derived exactly as the RocksDB store's
    `account_tx` and `nft_tx` indexes;
  - for every object key, the ledgers where it changed;
  - for every NFToken, each ledger where a transaction moved it between
    pages (mint, transfer, URI change or burn), with a fingerprint of the
    owner, URI and burned flag it left. Version 2 added this table; caches
    indexed with version 1 are rebuilt by the next `index-pieces`. Version 3
    follows Clio 2.8: NFT postings only for the transactions Clio's
    `nft_history` lists (successful NFToken transactions, each naming its
    token as Clio's `getNFTDataFromTx` does; a failed burn is listed for no
    token), the burns version 2 missed (a burn that left its page short has
    the page merged into the next one and deleted, and the token is only in
    the deleted page's `PreviousFields`), and `nft_info`'s `ledger_index`
    moved only by a change of owner or burned flag, not by a URI change alone.
    A version 2 XQIX still serves everything else, but `nft_info` and
    `nfts_by_issuer` answer that NFT state is not indexed until `index-pieces`
    (run by every sync) has rebuilt it, which decodes every range again. The
    NFTs-by-issuer XQOX files went to version 2 with it (their candidates are
    the tokens the postings list, and the snapshot's) and are rebuilt from
    the same decode.
- **XQSK**, one per XQIX, derived from it alone: for every account and NFT
  posting list longer than two strides (256 postings), the list reader's state
  before every 128th posting (payload offset, previous ledger and
  TransactionIndex: 12 bytes). `account_tx` and `nft_history` start each
  range's read at the last checkpoint before the first posting a page can
  want, so a page deep in a busy account's million-ledger bundle reads about
  a page and 128 postings instead of every posting before it; a read that
  passes a later checkpoint must arrive at its state. The file names the
  SHA-256 of the XQIX it was built from, so a rebuilt XQIX makes it stale.
  `index-pieces` (and every sync) builds it for any range that lacks it,
  including caches indexed by older builds, in one pass over the XQIX's
  posting tables; without it lists are read from their start, with the same
  answers. It is small: for the million-ledger cache measured below, at most
  222.7M / 128 checkpoints of 12 bytes (21 MB) plus 16 bytes per long list,
  about 0.6% of its 3.85 GB XQIX; a range whose lists are all short has a
  112-byte file.

- **XQNF**, one per base snapshot: the owner of every NFToken in it, so
  `nft_info` can find tokens that no cached transaction has touched.
- **XQOX**, the optional indexes, one per container and one per base snapshot
  each, on unless `[index]` in `xrpla.toml` (or `--indexes`) turns them off:
  - `.xqmh`, for Clio's `mpt_holders`: per MPT issuance, the holders of every
    MPToken the snapshot holds or the container changes;
  - `.xqni`, for Clio's `nfts_by_issuer`: per issuer, every token the snapshot
    holds or the container's transactions name, sorted by (taxon, token ID).
  Both list candidates, as Clio's holder and issuer tables do: a query reads
  each at the requested ledger and leaves out those that do not exist there.
  They are built from the same decode as XQIX (or, for an XQIX already built,
  from its NFT postings, and from the container's object patches decoded
  without metadata). A method whose index is missing answers `notSupported`
  and says how to turn it on; turning an index off removes its files at the
  next `index-pieces` or sync.

State at any cached ledger is each key's newest version at or before that
ledger, decoded from its 64-ledger page, or else the snapshot's value. The
snapshot is the newest one the cache keeps at or before that ledger with a
range starting right after it, and only the ranges since it are searched. A
cache keeps the base snapshot and the newest interval snapshot the ranges pass
and, with `[cache] snapshots_every = N` (`xrpla sync --snapshots-every N`), every
interval snapshot at a multiple of N; each one's index is checked against the
signed catalog (sequence, ledger hash, account hash) as the base's is, and a
snapshot no longer selected loses its parts and XQSS at the next sync. Without
them a ledger years back reads from the 2013 base snapshot and searches every
bundle since (cold `account_lines`, `account_objects` and `book_offers` take
seconds); every 10,000,000 ledgers costs about 21 GB more for mainnet's full
history (10.1 GB downloaded), every 5,000,000 about 42 GB (20.0 GB), every
1,000,000 about 206 GB (99.0 GB). Answers do not depend on the choice. Object
reads decode only a page's object patches, skipping transaction metadata:
17 ms per modern page against 45 ms for a full decode. A page decoded in full
for a transaction query serves object reads too.
Transactions are found through the container's XTXI index. Decoded pages,
segments and snapshot blocks are held in bounded LRU caches.

**Methods**, subscriptions, forwarding of current-ledger methods and the known differences
from xrpld and Clio are in the [API reference](reference/api.md). The piece cache and the
RocksDB store answer the same set.

**Verification:**

- **Compared with a live xrpld 3.3.0** (`tools/xrpld_parity.py`). The live
  publisher's cache for ledgers 107,184,129–107,189,760 was served next to
  the node, which still held those ledgers. Over 26 sampled ledgers,
  **6,012 of 6,013** responses across 21 methods were identical, including
  paging through markers. The only difference was `ping`'s `role: admin`,
  which xrpld reports to admin connections only.
- **Compared with the RocksDB store.** `tests/piece_query_test.py`
  (CTest `piece_queries`) publishes an archive, builds both backends and
  requires identical answers to requests derived from the data (699 on the
  fixture).
- **Indexes are deterministic.** Rebuilding the live cache's eight indexes
  produced byte-identical files.
- **NFTs.** `tests/nft_lifecycle.py` drives a standalone xrpld 3.3.0 through
  mints that split pages, offers, a URI change, burns and a transfer then burn
  in one ledger; the archive is the `standalone-nfts` fixture (CTest
  `piece_queries_nfts`). Both backends agree and match the answers the
  scenario implies. NFT state follows transaction metadata in TransactionIndex
  order, as Clio records it.

**Measured on the million-ledger cache** (972,189 ledgers, 111,889,144
transactions; host shared with a full verification and a NuDB import):

| | Result |
|---|---|
| `index-pieces`, 6 jobs | 36 min (decode 34 min), 6.2 GB peak RSS, on a host with a load average near 10. The first build peaked at 26.7 GB, mostly container pages left mapped; they are now released after each segment. |
| XQNF | 440 MB for 8,457,876 NFTokens, built in 7 s |
| XQIX | 3.85 GB: object versions 2.77 GB (66.4M keys, 288M versions), account postings 0.78 GB (222.7M, 3.5 B each), headers 0.11 GB, tx order 0.14 GB, NFT postings 0.03 GB, ledger hashes 0.01 GB, NFT states 7.9 MB (250,019 changes of 143,847 tokens) |
| XQSS | 2.77 GB for 19,241,572 objects (published parts: 2.57 GB) |
| Local query footprint | 6.6 GB on top of the 25.4 GB synced, about 26% |

Latency for random ledgers and accounts across the whole million, in
milliseconds (`tools/query_bench.py`, 40 samples, busy host). Cold is the first
request in a fresh server; warm repeats it.

| Method | cold p50 | cold p95 | warm p50 |
|---|---|---|---|
| `tx` | 1.8 | 15 | 0.6 |
| `account_info` | 1.9 | 286 | 0.7 |
| `account_tx` (20) | 73 | 545 | 9.4 |
| `ledger` (expanded) | 255 | 360 | 25 |
| `account_lines` (50) | 181 | 1,063 | 1.2 |
| `ledger_data` (100) | 133 | 469 | 1.1 |
| `book_offers` (20) | 513 | 1,296 | 7.5 |

A cold read pays for opening each segment it touches (its codec dictionary:
about 130 ms, mostly zstd and parsing 2–3M entries) and for decoding pages
(20–45 ms each, stopping at the ledger read). So handlers plan their reads:
owner directories, books and `account_tx` load the objects they will need,
snapshot blocks included, in one concurrent batch. Decoded pages, open
segments and snapshot blocks stay in bounded caches, sized by
`xrpla serve --cache-mb` (`[storage] cache_mb`; default a quarter of memory, between
3,072 and 65,536 MB: 30% pages, 60% open segments, 10% snapshot blocks);
`xrpla_cache_stats` (not a rippled method) reports their hit rates and the time spent
loading. These measurements predate that default.

`nft_info` for a token with hundreds of offers since the snapshot answers in
27 ms from a fresh process.

The Python client took 8–18 s for a single ledger or transaction lookup.

## Known gaps

- **Cold reads across many segments.** Each segment a request touches
  first costs about 130 ms to open, so a cold walk touching many segments
  takes 0.5–1.3 s. Even at the tip, an object's newest version can be in any
  segment since the interval's snapshot, so warming the newest segments does
  not help (`--warm-segments`, off by default). A per-range state of final
  object values would make such reads single block reads; it belongs with the
  live-ingest state. Smaller or shared dictionaries would need a format
  revision.
- **Object keys are most of XQIX** (66M × 32 bytes per million). They are
  uniformly random hashes, so sorted neighbours share only about 3 bytes;
  prefix compression would save about 7% and is not worth a format change.
- **Snapshots block publication.** A publish run waits for any new interval
  snapshot (about 10 minutes at 20M objects) before signing a generation;
  pieces could be published first and the snapshot added later.
- **Restores start at interval boundaries.** Starting mid-interval needs
  state rebuilt from the interval snapshot first.
