# Command-line reference

`xrpla --help` prints the full synopsis; `xrpla --version` prints the version and the libxrpl it
was built with. Every command prints its result as JSON on stdout, and errors as
`{"error": "..."}` on stderr with exit status 1. Options take one value each (`--name value`),
except the flags noted; an unknown or repeated option is an error.

Common conventions:

- **Publisher key** (`--publisher-key FILE`): a file holding 32 raw bytes or 64 hex characters,
  or the 64 hex characters themselves (always taken as the key: a key file so named is given
  with a path, `./NAME`), obtained independently of the publication. Commands that read signed history require it;
  automatic trust is not supported.
- **Source** (`--source`): a publication's HTTPS base URL (the directory holding
  `catalog/current.json`) or a local bucket directory.
- **Publication** (`--publication NAME`): instead of `--source` and `--publisher-key`, a
  publication from the [approved list](publications.md) installed with XRPLA, its key and mirrors
  (tried in order) taken from the list. With it, `--source` replaces the listed mirrors, and a
  `--publisher-key` given must be the listed key.
- **Ranges** (`--range`): `A-B`, or `A-` for A to the publication's tip. The v2 archive commands
  also take `latest-N` and `Nd`.
- **Network** (`--network-id`): `mainnet` (default), `testnet`, `devnet`, or a numeric network
  ID. The named networks bring their public hubs and validator list.
- `--jobs` values are 1-32.

## Profiles

| Command | Purpose |
|---|---|
| `xrpla setup [--config /etc/xrpla/xrpla.toml] [--unit-dir /etc/systemd/system]` | Ask how this machine uses XRPLA and write the configuration (and, for mirror, connected and standalone, the systemd unit). Run as root to install the unit and create the `xrpla` account. See [SETUP.md](../SETUP.md). |
| `xrpla run --config /etc/xrpla/xrpla.toml` | Carry out the configured profile: `restore` runs once and exits; `mirror`, `connected` and `standalone` run until stopped. |

Settings: [config/xrpla.example.toml](../../config/xrpla.example.toml).

## Publications: download, serve, restore

`PUB` below is `--publication NAME`, or `--source SRC --publisher-key FILE`.

| Command | Purpose |
|---|---|
| `xrpla publications` | List the approved publications installed with XRPLA (`XRPLA_PUBLICATIONS` names another list file): each one's name, network, first ledger, mirrors and publisher key, and any entry left out as invalid, with why. |
| `xrpla sync PUB --dir CACHE [--range A-B\|A-] [--jobs 4] [--indexes all\|none\|mpt_holders,nfts_by_issuer] [--snapshots-every N]` | Bring a piece cache in step with a publication: download what the range needs, check it against the signed catalog, build the query indexes, switch the selection. `--snapshots-every N` also keeps every published snapshot at a multiple of N ledgers (0, the default: only the base and the newest). |
| `xrpla index-pieces --dir CACHE [--jobs N] [--indexes ...]` | Build any missing local query index of a piece cache (every sync runs it). Default jobs: every core. |
| `xrpla serve --dir DIR [options]` | Serve the API over HTTP and WebSocket from a piece cache or a store. Options below. |
| `xrpla rpc --dir DIR --request FILE\|- [--forward URL] [--tail SPOOL --tail-key FILE] [--network-id ID]` | Answer one JSON-RPC request (at most 64 KiB, `-` for stdin) without a server. |
| `xrpla restore PUB --to nudb\|rocksdb\|store\|clio\|spool --out DIR [--range A-B\|A-] [--jobs N] [--load-jobs N] [--host HOST --port 9042 --keyspace xrpla --tls --username USER] [--signing-key FILE] [--replay]` | Restore a publication into xrpld's databases (`nudb`, `rocksdb`), an XRPLA store (`store`), Clio (`clio`), or a spool (`spool`). See the [restore guides](../README.md#guides). |
| `xrpla rotate-spool --dir SPOOL --signing-key FILE PUB [--check] [--start N]` | Restart a spool from the publication's newer snapshot, keeping its later ledgers. `--check` reports whether a rotation is due without doing it. |
| `xrpla compare-nodes --dir NODE_DB --against NODE_DB [--backend nudb\|rocksdb]` | Report the nodes two node databases do not share or hold differently. |

`restore` details:

- `--to spool` builds the state at the publication's tip (for `publish --peers`); with
  `--replay`, or a range start, it replays the snapshot and every later ledger instead.
  `--signing-key` signs the spool.
- `--jobs`: for `nudb`, snapshot intervals restored side by side (default a third of the cores,
  at most one per 5 GB of available memory); for `clio`, threads writing the import plan
  (default one per core; the plan is the same for any number).
- `--load-jobs` (`clio` only): loader processes, each with its own connection (default 8, at
  most the cores). Password: `XRPLA_CQL_PASSWORD`.
- Environment: `XRPLA_SQL_BULK=0` creates xrpld's SQL schema with its indexes from the start;
  `XRPLA_SQL_INDEX=sqlite` builds the indexes with SQLite's `CREATE INDEX`;
  `XRPLA_RESTORE_VERIFIER=libxrpl` replays through libxrpl's SHAMap instead of XRPLA's own hash
  tree.

### `serve` options

| Option | Default | Meaning |
|---|---|---|
| `--http HOST:PORT` | `127.0.0.1:5005` | JSON-RPC listener (also answers WebSocket upgrades) |
| `--ws HOST:PORT` | `127.0.0.1:6006` | WebSocket listener |
| `--https`, `--wss HOST:PORT` with `--tls-cert CHAIN.pem --tls-key KEY.pem` | none | TLS listeners; renewed certificates are reloaded |
| `--forward URL`, `--forward-ws URL` | none | upstream xrpld for current-ledger methods ([API reference](api.md#forwarded-methods)) |
| `--threads N` | 4 | request threads (1-32) |
| `--cache-mb N` | a quarter of memory, 3,072-65,536 | decode cache for a piece cache |
| `--tail SPOOL\|FOLLOWER_DIR --tail-key FILE` | none | also serve a spool's newer ledgers after the piece cache |
| `--max-connections N` | 1024 | open HTTP and WebSocket connections |
| `--max-connections-per-ip N` | 64 | per client address (0: no limit) |
| `--no-loopback-trust` | | as `[serve] loopback_trusted = false` |
| `--warm-segments N` | 0 | open the N newest segments in the background at start |
| `--network-id ID` | mainnet | the network reported in answers |
| `--config FILE` | | a flat archive configuration ([v2 archive tools](archive-v2.md#archive-services)) |

## Publishing and following

| Command | Purpose |
|---|---|
| `xrpla keygen --out FILE` | Create an Ed25519 signing seed (owner-readable only; never overwritten). |
| `xrpla publish --source URL --out ARCHIVE --signing-key FILE [options]` | Append verified ledgers from a node (or, with `--peers`, from the peer network) to a signed v2 archive or spool, resuming its committed tip. |
| `xrpla export --source URL --range A-B --out ARCHIVE --signing-key FILE [options]` | Export a range once into a new signed v2 archive. |
| `xrpla grpc-probe --source grpc://HOST:PORT --ledger N [--count 200] [--pages 20]` | Measure a gRPC source and report whether xrpld's client limits apply (`"unlimited": true` once the source treats this host as a secure gateway). |

Main `publish` options (`export` shares the source options):

| Option | Meaning |
|---|---|
| `--source URL` | `grpc://HOST:PORT` (xrpld) or `http(s)://` JSON-RPC (xrpld or Clio); see [SETUP.md](../SETUP.md#publishing-where-ledgers-come-from) |
| `--grpc-source grpc://...` [`--grpc-jobs N`] | an xrpld's gRPC beside a JSON-RPC source; with `--grpc-jobs`, ledgers come from both |
| `--jobs N` | connections to the source (default 4 for publish, 1 for export) |
| `--start N`, `--until N` | first ledger of a new archive; last ledger to follow |
| `--batch-ledgers 64`, `--interval 3`, `--once` | ledgers per batch; seconds between polls; one batch only |
| `--delay-ms 200` | pause before each request (0-60000) |
| `--compression-level 3` | zstd level, 1-19 |
| `--merge-chunks` | merge small chunks (for a follower polling every few seconds) |
| `--interval-snapshots N` | write a state snapshot every N ledgers |
| `--full-state`, `--no-rpc-cache` | compare complete states (no diffs); do not cache RPC responses |
| `--history-peers default\|none\|HOST:PORT,...` [`--history-identity FILE`] | where to fetch raw transaction trees a gRPC source omits (a few 2013 ledgers) |
| `--validator-list URL --validator-list-key HEX --validations-node ws[s]://...` | end each batch at a ledger a quorum of the list signed |
| `--peers default\|HOST:PORT,...` | follow the peer network instead of a node (no `--source`); the `--peer-*` options are those of [PEER-SERVING.md](../PEER-SERVING.md#settings) |

## Network checks

| Command | Purpose |
|---|---|
| `xrpla validators --list URL\|FILE --list-key HEX [--node ws[s]://HOST:PORT] [--ledgers 5]` | Check a validator list against its publisher key and print its size and quorum; with `--node`, count signed validations from that node's stream. |
| `xrpla peers [--peers HOST:PORT,...] [--list URL --list-key HEX] [--identity FILE] [--network-id ID] [--connections 3] [--ledgers 5] [--seconds 120]` | Connect to peers, count validations and fetch each quorum-validated ledger's header. |
| `xrpla peer-ledger --hash LEDGER_HASH [--peers ...] [--identity FILE] [--network-id ID]` | Fetch a ledger's header and raw transactions from peers and report whether libxrpl parses each. |
| `xrpla peer-compare --reference XRPLD_HOST:PORT --subject XRPLA_HOST:PORT [--ledgers 8] [--spacing 7] [--timeout-ms 10000] [--seed N]` | Send the same peer requests to both and compare the answers. |

## v2 archives and stores

| Command | Purpose |
|---|---|
| `xrpla fetch --archive URL\|PATH --range A-B\|latest-N\|Nd --publisher-key FILE [--dir ./data] [--jobs 8]` | Authenticate the publisher and download or resume a range. |
| `xrpla verify --dir ./data --publisher-key FILE [--full] [--trust-ledger-hash HASH]` | Check sizes and SHA-256; with `--full`, replay and check every ledger's roots and parent hashes. |
| `xrpla build --dir ./data --publisher-key FILE [--trust-ledger-hash HASH]` | Admit verified history to a RocksDB store, resuming a matching one. |
| `xrpla query --dir ./data --kind ledger\|tx\|object\|account_tx --key KEY [--ledger N]` | Query a store from the command line. |
| `xrpla status [--dir ./data]` | Report a download directory's or store's state. |
| `xrpla tail --archive URL\|PATH --range ... --publisher-key FILE [--dir ./data] [--interval 15] [--state-window N] [--once]` | Follow a signed archive and append newly published ledgers. |
| `xrpla prune --dir ./data --state-first N` | Keep state from ledger N on, keeping all transaction history. |
| `xrpla compact --dir ./data [--native]` | Compact an offline store, or an imported native RocksDB. |
| `xrpla import rippled --dir ./data --out DIR [--range A-B] [--backend nudb\|rocksdb]` | Write xrpld databases from a store. |
| `xrpla import clio --dir ./data --out PLAN_DIR [--range A-B] [--jobs N] [--host HOST --port 9042 --keyspace xrpla --tls --username USER] [--load-jobs N]` | Write (and with `--host`, load) a Clio import plan from a store. |
| `xrpla check-keys --archive DIR --publisher-key FILE` | Authenticate a v2 archive and, for every ledger, report the changed state objects whose keys neither its transactions touch nor the ledger's own closing objects (the skip lists, and the negative UNL on a flag ledger) explain. `missing` lists each such ledger and key; an empty list means every change is accounted for. |

See [v2 archive tools](archive-v2.md) for examples.

## Python tools

Installed under `/usr/share/xrpla/tools` (the Clio loader under `/usr/share/xrpla`). Each has
`--help`.

| Tool | Purpose |
|---|---|
| `live_publisher.py` | The publisher pipeline ([Run a publisher](../guides/publish.md)); `--health --workdir DIR` checks a running one. |
| `piece_publisher.py` | One publishing pass over a v2 archive into a bucket. |
| `s3_remote.py push --bucket DIR [--adopt-remote]` | Push a bucket's newest generation to R2/S3 (`XRPLA_S3_*` variables). |
| `publication_merge.py --bucket DIR --signing-key SEED --from-prefix PREFIX [--apply] [--abandon]` | Join an earlier publication onto this one. |
| `publication_audit.py --workdir DIR --xrpla-dir DIR --publisher-key FILE [...]` | Audit a published bucket as users read it. |
| `piece_client.py sync\|query\|restore` | Development client for pieces. |
| `xrpld_parity.py`, `xrpla_invariants.py`, `query_bench.py` | Comparison with xrpld, invariant checks, latency benchmarks. |
| `clio_load.py` | The Clio loader `restore --to clio` runs. |

### Contributor tools

In the source tree only (not installed):

| Tool | Purpose |
|---|---|
| `tools/fetch-fixtures.sh [DIR]` | Writes the synthetic canonical v2 fixture archive (default `.cache/fixtures-v2`) with `xrpla_core_tests`' `CoreFixture.Export`; set `XRPLA_CORE_TEST_BINARY` to that binary if it is not in `build-core/Release`. No public ledger data. |
| `tools/stream_soak.py --ws URL [--subscribers 20] [--out stream_soak.csv] [--alerts stream_alerts.log]` | Soak test for a server's ledger and transaction streams: many subscribers check for gaps, repeats, `txn_count` mismatches and differences between subscribers, with a summary row every 5 minutes. |
