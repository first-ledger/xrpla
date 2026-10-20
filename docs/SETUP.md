# Setup profiles

`xrpla setup` asks how a machine will use XRPLA and writes `/etc/xrpla/xrpla.toml`;
`xrpla run --config /etc/xrpla/xrpla.toml` carries the profile out (a systemd unit runs it
for the persistent profiles). The same commands can be run by hand.

This is the full reference for the profiles. For step-by-step instructions, start with the
guide for your goal ([documentation index](README.md)); every setting is documented in
[config/xrpla.example.toml](../config/xrpla.example.toml).

| Profile | Ledgers come from | Runs | Status |
|---|---|---|---|
| `restore` | a publication (R2 or a bucket directory) | once, then exits | available |
| `mirror` | a publication, checked again on a schedule | as a service | available |
| `connected` | a local or remote xrpld | as a service | available |
| `standalone` | a publication, then the XRPL peer network (no xrpld) | as a service | available |
| `publisher` | a node, publishing pieces to R2/S3 | as a service | `tools/live_publisher.py` |

## Trust

Every profile that reads a publication pins its publisher key (32 bytes, or 64 hex
characters): the one the [approved publication list](reference/publications.md) gives for the
publication named (`[publication] name`, `--publication`), or one given by hand
(`publisher_key`, `--publisher-key`). The signed catalog is checked against it, each container directory against the
catalog, each segment against its directory row and each decoded 64-ledger page against its
canonical digest; every ledger is then replayed through the verifier, which checks its
transaction and state tree roots and parent hash, and each range's last ledger hash against
the catalog. A publication read over HTTPS needs no credentials: range requests fetch one
container directory and then one segment at a time.

A publication can have several mirrors (the list's `mirrors`, or `[publication] source` given as
an array). The catalog comes from the mirror claiming the newest generation; everything else
from the first in order that is not resting after a failure. One out of reach, answering with
an HTTP error, serving an object that does not match the signed catalog, or offering an older
catalog generation than the cache holds is passed over for the next (event `mirror_failed`).
A mirror is trusted with nothing: the worst one can do is withhold the publication or hold it
back ([the publication list](reference/publications.md#trust-model) gives the details).

### Validator signatures

A node's word is not needed for which ledgers are validated. `xrpla validators --list
https://vl.ripple.com --list-key ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734`
checks a validator list against its publisher's key (the publisher's manifest names the key
that signed the list, and each validator's manifest names its current signing key) and prints
its size and quorum (ceil(0.8 × validators), as xrpld computes it). With `--node
ws[s]://HOST:PORT` it follows that node's validations stream, checks every validation's
signature, counts each trusted validator once per ledger hash, and reports each ledger that
reaches quorum; on mainnet with vl.ripple.com's 35 validators that is 28 signatures, about
1.5 s after a ledger closes.

`xrpla publish` takes the same list with `--validator-list URL --validator-list-key HEX
--validations-node ws[s]://...`: each batch then ends at a ledger a quorum signed (catching up
ends at the first such ledger past the batch window), its hash is pinned before the batch is
committed, and the parent chain authenticates every earlier ledger. The node supplying ledger
data may then be remote or untrusted. Followed live against mainnet, every one of 278 ledgers
was published this way, most within one ledger of the network.

## Restore: fill an xrpld or Clio database

```
xrpla restore --publication NAME | --source <PUBLICATION_URL> --publisher-key publisher.pub
              --to nudb|rocksdb|store|clio --out DIR [--range A-B|A-]
```

- `nudb`, `rocksdb`: an xrpld node database plus `ledger.db` and `transaction.db`, written
  straight from the publication with nothing staged, and a `rippled.cfg` pointing at them.
  Rows and node objects match `xrpla import rippled` from a store of the same ledgers.
- `store`: an xrpla RocksDB store (`xrpla serve --dir DIR`).
- `clio`: a store is built first (Clio's tables derive from its indexes), then Clio's import
  plan is written and loaded when `--host` is given (`XRPLA_CQL_PASSWORD` for the password).

The Clio plan (`DIR/plan/clio-plan.jsonl`, one JSON row a line) is written by `--jobs`
threads (default one a core, at most 32): they read the store, a ledger or a key range each,
and prepare its rows, and one thread keeps the live keys the successor rows need and writes
everything in ledger and key order, so the file is the same, byte for byte, for any number of
jobs (`tests/clio_plan_test.py` holds the plans the single-threaded writer wrote). The store's
block cache grows to an eighth of the available memory (at most 32 GB) meanwhile. The object
and NFT versions are cut into key ranges of about 8 MB of the store each (up to 2^20), and a
range's rows past 256 MB (one key can have a version per ledger) wait in a file beside the
plan (`clio-plan.jsonl.part.N`) until their turn, so memory stays bounded. With `--host`, the
plan is loaded as it is written (the store's ledgers were verified as it was built):
`tools/clio_load.py` follows the file and, once xrpla renames it into place complete, loads
the rest and publishes the range; it gives up if the plan stops growing for 30 minutes
(`--stall-seconds`). `xrpla import clio --host` from a store it verifies again loads only
the complete plan, so nothing unverified reaches the keyspace.

The loader writes with `--load-jobs` processes (default 8, at most the cores), each with its
own connection, prepared inserts and up to 128 requests in flight; rows of one table and
partition in a 4,096-line block go as one unlogged batch. Each row's write timestamp is its
plan line, so rows applied in any order leave what writing them in order would (a repeated
pseudo-transaction ID keeps its later ledger's row), and sending a row again changes nothing.
A busy node does not end the load: writes refused as overloaded, unavailable or timed out are
sent again after a backoff; a connection that times out (`--timeout`, 120 s) or drops is
opened again and every unacknowledged request sent again on it; each of these halves the
worker's requests in flight, which then grow back by one per window of answers. The load fails
only after 10 minutes without an answer (`--patience`). Recoveries are logged, once a minute.
The timestamps start 2^40 µs (about 13 days) before the load, below Clio's own later writes;
a keyspace an earlier xrpla loaded with the server's timestamps is refused while those are
newer (load it into a new keyspace).
Its progress, the first line every worker has acknowledged, is kept in the keyspace with the
SHA-256 of the plan before that line: an interrupted load resumes there when the plan starts
with the same bytes (the same plan, or the same plan written again by a resumed restore), and
sends every row again otherwise. The keyspace lease is renewed every 30 seconds; the range is
published only once every line is acknowledged.

Starting at ledger A replays from the newest snapshot below A; ledgers before A are verified
but not written. An interrupted restore continues after its last committed 256-ledger window
when run again, and a restore to a later ledger extends an earlier one.

NuDB is built directly rather than through its insert path, which takes one node at a time
under a lock and reads its key file to reject duplicates (about 100k nodes/s, however many
threads call it). Node values are encoded as xrpld's NuDB backend encodes them, in batches on
many threads, and one thread appends the batches to the data file as they come (ext4 lets one
thread write into a file at a time), starting each batch's writeback as it lands. Each record
is logged as 24 bytes: its key's hash (with the key file's salt, fixed when the build starts),
offset, size and the key's first 8 bytes. The key file is written once at the end: its
buckets are cut into regions of contiguous buckets small enough to sort in memory, the log is
distributed to a file per region, and every region is sorted (repeated nodes dropped: same
hash and key bytes), filled (full buckets spilled as NuDB's own `rekey` spills them) and
written on a thread of its own. No sorted run is written and nothing is merged, so the
temporary space stays one more log's worth (about 4 TB for a 40 TB full-history database).
The store starts as an empty one created by xrpld's backend, so its header is the one xrpld
expects. Each 256-ledger window checkpoints the data file and the log, so an interrupted
restore continues from the last window, and a later restore to a newer ledger extends a
finished one (its key file is rebuilt).

A new `transaction.db` is loaded without indexes: xrpld's schema indexes transactions by ID
and account transactions by ID, account and ledger, and rows arriving in ledger order would
rewrite pages all over those B-trees. The rows are appended to bare tables, and when the
ledgers are done every index is built by sorting, alongside NuDB's key file: the tables'
pages are read once, straight from the file, for all five indexes, and every index B-tree is
written from its sorted entries at once (47 s for 200,000 recent ledgers). The table's
primary key is then declared by rewriting the schema to exactly what xrpld's statements
create. `XRPLA_SQL_BULK=0` creates xrpld's schema from the start instead, and
`XRPLA_SQL_INDEX=sqlite` builds the indexes with SQLite's `CREATE INDEX`.

Ledgers are replayed on the restore's own hash tree rather than libxrpl's SHAMap: the state
is kept in memory as hashes only (each object's key and leaf hash, each inner node's
children; about 4 GB for mainnet's 20M objects), each ledger's changes are applied to it with
SHAMap's placement rules, the paths they touch rehashed (each first nibble's subtree on its
own thread) and the new nodes written. The checks are the verifier's: every object and
transaction (by the wire reader, falling back to libxrpl), each ledger's parent, transaction
root and state root against its header, so a node written for a ledger is the canonical one
once its root matches. A ledger's SQL rows are prepared and inserted on two more threads, and
records are read ahead on another. `XRPLA_RESTORE_VERIFIER=libxrpl` restores through
libxrpl's SHAMap instead (the tests compare the two). An interrupted restore replays the
ledgers it had written, writing nothing, then carries on.

The snapshot loads as a pipeline: parts download, decompress and parse ahead; objects are
checked, hashed and written on every core; the tree's inner nodes are then built with each
first byte's subtree on its own thread. Measured on NVMe (Hetzner, 24 cores): the mainnet
state at 107,214,590 (28.3M nodes) and the 514 ledgers after it restored in 18 s: 10 s for
the state, 3 s for the ledgers and 4 s for the key file (29.2M keys); 2,306 ledgers replayed
at about 295 a second, SQL inserts then being the limit. xrpld 3.3.0 opened a restore of the
same nodes (`--standalone --ledger`) and answered 47 ledger, state and transaction requests
identically to a live mainnet node. Through libxrpl's SHAMap the same restore took 27 s with
the state pipelined, 298 s with it loaded on one thread, 514 s through NuDB's insert path, and
ran at 35 ledgers a second.

A NuDB restore spanning several published snapshot intervals (one per 1,000,000 ledgers on
mainnet) restores them side by side, `--jobs` at a time (default: a third of the cores, and
no more than one per 5 GB of available memory): each
starts from its own interval's snapshot, into the same data file and SQLite databases, so a
full-history restore takes about as long as its longest interval. Every interval after the
first loads its snapshot's whole state first (10 s for mainnet's alone, some 3,000 ledgers'
work), so without `--jobs` intervals shorter than 8,192 ledgers join the one before them;
`--jobs N` splits at every snapshot, and `--jobs 1` restores in order. A directory restored by intervals
is resumed and extended by them. `xrpla compare-nodes --dir A --against B` reports the nodes
two node databases do not share or hold differently.

## Connected: follow a node and serve its history

```toml
profile = "connected"

[node]
rpc = "http://127.0.0.1:5005"          # xrpld JSON-RPC; local or remote
# grpc = "grpc://127.0.0.1:50051"      # optional: ledger diffs over gRPC
# validations = "ws://127.0.0.1:6006"  # with [validators]
# delay_ms = 0                         # pause before each request (raise for a shared public node)

# [validators]                         # accept only ledgers a quorum of these signed
# list = "https://vl.ripple.com"
# list_key = "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734"

[connected]
start = "tip"                          # or a ledger the node still holds

[storage]
dir = "/var/lib/xrpla"
cache_mb = 0                           # decode cache; 0: a quarter of memory, 3-64 GB

[serve]                                # xrpld itself usually has 5005 and 6006
http = "127.0.0.1:5105"
ws = "127.0.0.1:6106"
```

`xrpla run` keeps the node's history from `start` on, a few seconds behind it:

1. The follower (`xrpla publish`, run by `tools/live_publisher.py`) exports the node's state at
   the ledger before `start` once, then commits each new validated ledger to a signed spool,
   checking its transaction and state tree roots and parent hash. Every 16 and 256 ledgers its
   small chunks are merged, so polling every few seconds keeps the spool's manifest small.
2. Each complete 50,000-ledger piece is sealed into `STORAGE/bucket`, signed by this machine's
   key (`STORAGE/keys/local.pub`); intervals of 20 pieces become bundles, as a publisher's
   are, and the spool is then cut at the interval boundary.
3. The piece cache (`STORAGE/cache`) is synced from that bucket whenever a new catalog
   generation appears, and served together with the spool: requests for ledgers after the
   last sealed piece are answered from the spool, as a live tail.

Before the first piece is sealed the cache holds only the baseline snapshot, and the tail
serves everything after it. The first start therefore waits for the node's state export and
its snapshot (tens of minutes on mainnet) before listening. A local node can be trusted as
it is; with a remote one, set `[validators]` so ledgers are accepted only once a quorum of
the list's validators signed them (see Trust above).

Over WebSocket, `subscribe` takes the `ledger`, `transactions`, `accounts` and `books`
streams, as xrpld does (validated transactions only); see
[the API reference](reference/api.md#subscriptions). `serve --max-connections`
(default 1,024) caps open HTTP and WebSocket connections.

### With a publication

With a `[publication]` section (`name`, or `source` and `publisher_key`) instead of `connected.start`,
connected mode keeps the publication's whole history, as `mirror` and `standalone` do: the
piece cache follows the publication, the spool starts from its newest snapshot, and the node
supplies only the ledgers after the publication's tip. A node usually keeps about a day of
ledgers while a publication can be one piece (50,000 ledgers) behind the network; any ledgers
the node no longer holds are first fetched from the peer network (`publish --peers`). The
spool is rebuilt from each newer snapshot the publication gains, while the follower keeps
running until the swap. Nothing is sealed locally, so no Python pipeline runs.

Without a publication, the pipeline below keeps the history from `start` on:

The live publisher and the follower need `python3` with the system `libzstd` and `libsodium`.
SIGTERM stops the server, the live publisher and the follower; everything resumes from
the spool and bucket on the next start.

## Standalone: no xrpld

```toml
profile = "standalone"

[publication]
name = "xrpla-mainnet"                 # or source = "<PUBLICATION_URL>" and publisher_key = FILE

[storage]
dir = "/var/lib/xrpla"
cache_mb = 0                           # decode cache; 0: a quarter of memory, 3-64 GB

[serve]
http = "127.0.0.1:5005"
ws = "127.0.0.1:6006"

# [peers]                                # all settings: docs/PEER-SERVING.md
# endpoints = ["r.ripple.com:51235"]    # kept connected (default: join through mainnet's public hubs)
# outbound = 10; max_peers = 21; private = false
# relay = true; relay_validations = "all"; relay_proposals = "trusted"; squelch = true; squelch_sources = 5
# serve = false                          # true: serve the newest ledgers to peers
# ledgers = 256; listen = "0.0.0.0:51235"; max_inbound = 11; ip_limit = 2; crawl = false
# [validators]                           # default: vl.ripple.com and its key
# [standalone]
# sync_seconds = 300; rotate_seconds = 3600; poll_seconds = 3
```

History comes from the publication, as for `mirror`; the ledgers after it come from the XRPL
peer network, with no xrpld anywhere:

1. The first start syncs the piece cache, then writes a spool from the publication's newest
   snapshot and the ledgers after it (`xrpla restore --to spool`), replaying them through the
   verifier so the follower can continue from the publication's tip.
2. The follower (`xrpla publish --peers`) joins the peer network as a node of its own
   (`STORAGE/keys/node.key`): a TLS session signed with the node key, as xrpld's overlay
   requires. Peers relay validations; each is checked against the validator list, and a batch
   ends at a ledger that a quorum of the list signed (or at an earlier ledger that one's parent
   chain reaches). Each ledger is fetched by hash: its header, every transaction and its
   metadata from the transaction tree, and along its path in the state tree each object those
   transactions touched and those the ledger's close changes outside any transaction (the
   skip lists, and on flag ledgers the negative UNL, whose voted changes take effect then),
   every node checked against the hash that commits to it. Should a ledger's state root still
   not match (a change nothing names, as a future amendment might make), its state tree is
   compared with the verified parent's instead, fetching only the subtrees whose hashes
   differ, which finds every change (`state_changes_from_tree` in the log names what was
   missed). A ledger that keeps failing is reported as `follower_stuck` (after 10 attempts,
   then every 100) and `follower_recovered` once it passes. The
   verifier then checks the whole ledger, as it does for ledgers from a node. The next eight
   ledgers are fetched while one is verified, about two ledgers a second against mainnet's
   public hubs, so a new install catches up with the network within minutes.
3. The server answers from the cache and the spool (the spool as a live tail), and the cache
   follows the publication. Each time the publication gains a newer snapshot (and the cache
   holds it), the spool is rebuilt from it (`xrpla rotate-spool`), starting where the cache
   ends, so it keeps about one publication interval of ledgers. The swap takes the spool's
   lock (a `rotate-spool` run by hand while the follower runs is refused) and survives a crash
   between its two renames (the next start, or the next hourly check, finishes it; a swap that
   fails is finished or undone at once). The spool it replaces is kept beside it as
   `spool.retired-*` until the cache holds its ledgers, and at least a day: the server's tail
   moves onto the cache's newer pieces only once it is rebuilt over them, and until then reads
   the ledgers before the new spool's first from the retired one.

With `[peers] serve = true` the node also serves: it accepts inbound peers and answers their
requests for the newest `ledgers` ledgers (headers, transaction and state trees) as a
non-validating xrpld does, and tells its peers which ledgers it has; otherwise it answers
every request as a node without ledgers does (docs/PEER-SERVING.md). Either way it relays
validators' validations and proposals among its peers as xrpld does (each checked first),
but never transactions, and takes no part in consensus: to submit transactions, run an xrpld
alongside (`[serve] forward`).

On mainnet, peers' ledgers built this way were byte-identical to what a local xrpld supplied
over gRPC for the same ledgers, and the resulting transaction and ledger streams matched that
xrpld's message for message. `xrpla peers` shows the peer side on its own: it connects,
counts validations and fetches each quorum-validated ledger's header.

## Mirror: serve published history

```
xrpla sync --publication NAME --dir CACHE          # or --source <PUBLICATION_URL> --publisher-key publisher.pub
xrpla serve --dir CACHE
```

`sync` downloads what the selected ledgers need (the base snapshot and the containers after
it, each checked against the catalog), builds the local query index for anything new, then
switches `selection.json` and removes files no longer selected; a running `serve` picks up
the new selection. The `mirror` profile does both, syncing again every `mirror.sync_seconds`.

## Publishing: where ledgers come from

Only a publisher reads a node for history; everyone else restores or mirrors a publication.
`xrpla publish` (and `tools/live_publisher.py --node`) takes its ledgers from one of:

| `--source` | Node | Each ledger | State (a snapshot) |
|---|---|---|---|
| `grpc://HOST:50051` | xrpld | `GetLedger` with its object diff: one request | `GetLedgerData`, the key space in `--jobs` ranges |
| `http://HOST:PORT` | Clio | `ledger` with Clio's `diff`: one request | `ledger_data`, split at keys the base ledger holds |
| `http://HOST:PORT` | xrpld | `ledger`, then `ledger_entry` for each object its transactions touched | `ledger_data` |
| `http://…` with `--grpc-source grpc://…` | xrpld | over gRPC (everything gRPC serves) | over gRPC |
| `http://…` with `--grpc-source grpc://…` and `--grpc-jobs N` | Clio or xrpld, and an xrpld | from both: `--jobs` connections to the first, N to the gRPC node | over gRPC |
| `--peers` | the peer network | headers, transaction trees and state paths from peers | — (extends an existing spool) |

`--jobs N` is the number of connections: state ranges, and ledgers fetched ahead of
verification. With `--grpc-jobs N` beside a JSON-RPC `--source` and `--grpc-source`, ledgers are
fetched from both nodes at once, on `--jobs` connections to the first and N to the gRPC node, to
read faster than either allows a client alone; each ledger is verified the same whichever node
sent it. From an xrpld, gRPC is the one to use: over one connection it gave 100–140
ledgers a second (with transactions and diffs) and about 110,000 state objects a second; a
full mainnet state (20M objects) came over four in under three minutes.

An xrpld limits each client's load. To read a full-history xrpld without its limits, its
operator opens gRPC to the publisher's address and names it a secure gateway:

```
[port_grpc]
port = 50051
ip = 0.0.0.0
secure_gateway = PUBLISHER_IP
```

with the port firewalled to that address (gRPC here is plaintext, without authentication).
xrpld exempts a request from its limits when it comes from a `secure_gateway` address and
names a user, which XRPLA's requests do; `xrpla grpc-probe --source grpc://HOST:50051 --ledger
N` reports `"unlimited": true` once it applies.

## Configuration

`config/xrpla.example.toml` (installed as `/usr/share/xrpla/xrpla.example.toml`) documents
every setting `xrpla run` reads: what it does, which profiles read it, its default and the
values it accepts. `xrpla setup` writes that file with its answers filled in, every other
setting kept as a commented line with its default, so `/etc/xrpla/xrpla.toml` documents
itself; to change a setting, uncomment it (and its `[section]` line) and edit it. A name
`xrpla run` does not know is logged as `config_unknown_setting` at start (usually a typing
mistake) and otherwise ignored. `tests/config_reference_test.py` keeps the reference and
the code in step.

The sections, by profile:

| Section | restore | mirror | connected | standalone |
|---|---|---|---|---|
| `profile` | required | required | required | required |
| `[publication]` | required | required | optional | required |
| `[history]` | first, last | first, last | first (with a publication) | first |
| `[restore]` | target, out, clio_* | | | |
| `[storage]`, `[serve]`, `[network]`, `[index]`, `[cache]` | | yes | yes | yes |
| `[mirror]` | | sync_seconds | | |
| `[node]`, `[connected]` | | | yes | |
| `[validators]` | | | optional | optional (default vl.ripple.com) |
| `[standalone]` | | | | yes |
| `[peers]` | | | when filling a gap from peers | yes |
| `[log]` | optional | optional | optional | optional |

`xrpla run` checks the serving settings before its first sync, to the bounds of the matching
`xrpla serve` flags: `[serve] threads` 1-32, `max_connections` 1-1048576,
`max_connections_per_ip` 0-1048576 (0: no limit), and `[storage] cache_mb` at most 1073741824.

### Logs

What `xrpla run` reports (one JSON object a line, its own and the processes it starts) goes to
stdout and stderr by default: under the systemd unit, the journal keeps it within its own
limits (`journalctl -u xrpla`). With `[log] file`, it goes to that file instead, which starts
over at `max_mb` (default 100), keeping `keep` older files (default 5): FILE.1 is the newest,
and the oldest beyond `keep` is deleted. xrpld's own `debug.log` is rotated by the logrotate
rule its package installs (`/etc/logrotate.d/xrpld`).

`[index]` turns the optional local indexes on or off (both on by default): `mpt_holders` and
`nfts_by_issuer`, which answer Clio's methods of those names. They are built as history is
synced; one turned off is removed at the next sync and its method answers `notSupported`.
`xrpla sync` and `xrpla index-pieces` take the same choice as `--indexes
all|none|mpt_holders,nfts_by_issuer`.

`[storage] cache_mb` is the decode cache for a piece cache: open segments, decoded ledgers and
snapshot blocks, charged by what each holds. Under heavy load the process uses about a quarter
more than the cache (requests in flight, key lookup maps, the allocator's own), so a machine
shared with other services should leave that room. `xrpla_cache_stats` reports the cache's use,
what the open segments hold by part, and on Linux the allocator's own figures.

`[cache] snapshots_every` picks which of the publication's state snapshots (one every
1,000,000 ledgers) the local copy keeps besides the first and the newest: every one at a
multiple of that many ledgers (0, the default: none). A ledger reads each object from the
newest kept snapshot at or before it plus the changes since, so without them a cold
`account_lines`, `account_objects` or `book_offers` at a ledger years back searches dozens of
million-ledger bundles and can take seconds; with them it searches at most N ledgers' worth.
Answers are identical either way. Disk for mainnet's full history today:

| `snapshots_every` | Snapshots kept | Disk added | Download added |
|---|---|---|---|
| 0 (default) | the first and the newest | | |
| 10,000,000 | also every 10 millionth ledger's | about 21 GB | 10.1 GB |
| 5,000,000 | also every 5 millionth ledger's | about 42 GB | 20.0 GB |
| 1,000,000 | all of them | about 206 GB | 99.0 GB |

A change takes effect at the next sync, which downloads and indexes the snapshots now wanted
and removes the ones no longer wanted. `xrpla sync` takes the same choice as
`--snapshots-every N`.

## The service account and its directories

In short: the service runs as the `xrpla` system account, which may write only the storage
directory and the log file's directory. The rest of this section is what setup checks before it
hands a directory to that account, what it refuses, and the commands it prints to fix a
directory a root-run profile left behind.

The unit setup installs (`/etc/systemd/system/xrpla.service`) runs as a system account,
`xrpla`, not root: run as root, setup creates the account when it is missing and gives it the
storage directory and the log file's directory, before it writes the configuration, a publisher
key given as hex (`publisher.pub` beside it) or the unit. It gives those directories alone,
never what is in them: each is made when missing (with any missing directories above it, which
stay root's, mode 0755), then owned by `xrpla`, without POSIX ACLs (a default ACL inherited from
a parent, `setfacl -d -m u:alice:rwx /srv` say, would pass to all the service makes there) and
writable by its owner alone. Setup changes only what is not so already, so a re-run over an
install the account owns changes nothing (and passes on a read-only mount). The unit lets the
service write those directories alone, reading the rest of the file system, and bind ports
below 1024 (an HTTPS listener on 443). A TLS key must be readable by `xrpla`.

What is in a directory must be the account's already, and written by it alone: a new or empty
directory needs nothing more, and one the service has written is its. Setup reads the whole tree
first, changing nothing (a regular file is opened for reading only, to read its ACLs, and what it
opens is opened with `O_NOATIME` where the file system takes it, so no access time changes), and
refuses one holding anything `xrpla` does not own, a device node, or a directory or file with a
POSIX ACL (an access or a default one, inherited from a `setfacl -d` above, say) or that its
group or all may write, naming a few of each kind it found. When all it found is what a profile
run as root leaves (root's own entries, none with other hard links, and ACLs or group and other
write), it prints the commands that put that right, to run if it is XRPLA's, once whatever writes
in the directory is stopped (a file opened before them keeps the access it was opened with):

```sh
cd / &&
find '/var/lib/xrpla' -xdev -uid 0 \( -type d -o -links 1 \) -execdir chown -h xrpla: {} + &&
runuser -u xrpla -- find '/var/lib/xrpla' -mindepth 1 -xdev \( -type d -o -type f \) -execdir setfacl -b {} + &&
runuser -u xrpla -- find '/var/lib/xrpla' -mindepth 1 -xdev \( -type d -o -type f \) -execdir chmod go-w {} +
```

(those that apply, on one line), preceded by the `useradd` setup would run when there is no
`xrpla` account yet; when nothing in the directory is root's but the directory itself, a plain
`chown -h xrpla: '/var/lib/xrpla'` comes first instead, so the account can enter it. They cross
into no other file system (`-xdev`; `chown -R` would), act on each entry from its own directory
(`-execdir`), and leave out the `lost+found` of a file system mounted at the directory (with
`-prune`). Only `chown` runs as root, and only on root's entries (`-uid 0`): a file another user
puts in the directory after setup read it stays theirs, and the next setup refuses it. It changes
a symbolic link itself rather than what it names (`-h`) and leaves out a file with other hard
links. `setfacl` and `chmod` follow a link, so they run as `xrpla` (`runuser`, part of
util-linux) once it owns the tree: should anyone who may write in the directory replace an entry
with a link meanwhile (to `/etc`, say), they change only what the account could change anyway.
They start from `/` (`cd /`), as `find` returns to the directory it started in, which root's
(`/root`, say) the account cannot enter. The directory itself is left to setup, which takes away its ACLs
and group and other write when it gives it. Setup prints the `chown` only while the kernel's
`fs.protected_hardlinks` is on (as it is by default on current distributions): without it, an
account that may write in the directory could put a hard link to a file of root's there while
`chown` runs, which it would then give to `xrpla`. Setup then says so instead; turn it on
(`sysctl -w fs.protected_hardlinks=1`, and in `/etc/sysctl.d` to keep it) or move root's entries
out, and run setup again. For anything else (an entry on another
file system or bind mount inside the directory, a device node, another user's file, or a file of
root's with other hard links, perhaps to one outside, such as `/etc/shadow`) setup prints no
command, as one would hand over or change what is not XRPLA's: move it out (or unmount it) and run
setup again. A profile run by hand afterwards should run as the account
(`sudo -u xrpla xrpla run --config ...`).

The tree is read without following links. A directory on another device (a file system mounted
inside, or a btrfs subvolume) is judged by its own owner and mode and not entered, as
`find -xdev` enters none; a bind mount of the same device as the directory is entered, as `find -xdev`
enters it too, and anything in it that is not the account's is reported as on another mount
(setup tells one by its mount id).

Every directory is checked, its contents included, before the account is made or any directory
is given, so a refusal leaves the machine as it was; a failure after the checks (`useradd`
failing, a directory changed meanwhile, a full disk) can leave the account made and the
directories before it given. Setup refuses a system directory or one in it (`/usr`, `/etc`,
`/boot`, `/dev`, `/proc`, `/sys`, `/run`, `/bin`, `/sbin`, `/lib*`, and the directories in
root's `PATH`), a shared parent itself (`/`, `/var`, `/var/lib`, `/var/log`, `/home`, `/opt`,
`/srv`, `/tmp`, ...), a directory in `/tmp` or `/var/tmp` (the unit's `PrivateTmp=yes` gives the
service an empty one of its own), a path with a symbolic link in it, a directory on a network,
cluster or FUSE file system or with a parent on one (NFS, SMB, 9p, Ceph, AFS, Lustre, OCFS2,
GFS2, or FUSE ones such as sshfs, GlusterFS, mergerfs and virtiofs, whose server, other machines
or daemon decide who may write it, not the owners and modes setup reads; other kinds are not
recognized), one with an NFSv4 ACL (an OpenZFS dataset's with `acltype=nfsv4`, say) or a CIFS
mount's Windows ACL (setup does not read those), and one whose parents are not all root's or
that another account may write without the sticky bit: one any account may write, one with an
access ACL whose mask lets a named user other than root or a named group write (read-only
entries such as `setfacl -m u:datadog:rx` are fine), or one a group may write unless it is a
system group (gid below 1000) that names no members, is no login account's primary group (uid
1000 or above, as openSUSE's `users` is) and is not the `xrpla` account's own, as the `syslog`
group that writes Ubuntu's `/var/log` is. It refuses a directory owned by another user, and one
holding anything at its top that is not part of XRPLA's layouts (`cache`, `spool`,
`spool.next`, `keys`, `pieces`, `selection.json`, ..., the log file and its older files, or the
directory holding the log file when it is further down the storage directory); the `lost+found`
of a file system (or bind mount) mounted there, root's directory on that file system, is left
alone, and one in a directory that is not where something is mounted is refused as any entry. Paths are answered as absolute ones (`~` is
`$HOME`), a publisher key file and the `publisher.pub` written beside a relative `--config`
included; one a unit cannot name as it is (anything but ASCII letters, digits and
`. _ / @ + : -`: a space, `%`, `$`, a quote or a backslash, say) is asked again, and a
`--config` path or a program path (the unit's `ExecStart=`) with one is refused.

The configuration is written afresh from the answers, but the `[log]` section of the one it
replaces is kept: to log to a file, set `[log] file` and run setup again, and the unit lets the
service write that file's directory. The file must be an absolute path (the service runs from
`/`; a relative one is not kept), and is kept normal. It goes in a directory of its own, such
as `/var/log/xrpla/xrpla.log`, or in the storage directory or one under it
(`STORAGE/log/xrpla.log`); a shared directory such as `/var/log` itself is refused, as is a
storage directory inside the log file's directory.

The unit runs the program setup ran as: setup warns, naming `ExecStart=`, when that is not
under `/usr` (or is in a home directory), as a build tree's is. Install xrpla
(`cmake --install BUILD --prefix /usr`) and run setup again, or edit the unit.

The archive units in `config/` (`xrpla-serve.service`, `xrpla-tail.service`) are another way to
run XRPLA, not to be combined with this one: they run with `DynamicUser=yes` as `xrpla`, and
keep their state in `/var/lib/xrpla`, a link into `/var/lib/private`. Setup refuses to give the
service such a directory, and refuses an `xrpla`
account systemd lent one of those units while it runs (uid 61184-65519): stop them, and choose
a storage directory other than theirs.
