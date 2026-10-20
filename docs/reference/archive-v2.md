# v2 archive tools

Before publications of pieces and bundles, XRPLA's unit of exchange was the signed **v2
archive**: a state snapshot plus chunks of ledgers, described by a signed `manifest.json`. The
commands below read and write v2 archives. Spools (the local archives of the newest ledgers that
standalone and connected nodes keep) are v2 archives too. For serving or restoring published
history, use the [guides](../README.md#guides) instead.

## Try the included public fixture

Run these commands with an installed `xrpla`, from the repository root:

```sh
xrpla fetch --archive tests/fixtures/mainnet-transactions --range 1000000-1000004 \
  --publisher-key tests/fixtures/mainnet-transactions/publisher.pub --dir ./data
xrpla verify --full --dir ./data --publisher-key tests/fixtures/mainnet-transactions/publisher.pub
xrpla build --dir ./data --publisher-key tests/fixtures/mainnet-transactions/publisher.pub
xrpla query --dir ./data --kind ledger --key 1000000
xrpla serve --dir ./data --http 127.0.0.1:5005 --ws 127.0.0.1:6006
```

The fixture key authenticates these checked-in files only. Pin a production publisher's key
independently before trusting its archives. Root and hash consistency alone is not independent
consensus validation: an independently obtained descendant ledger hash can be supplied with
`--trust-ledger-hash`.

## Export from a node

```sh
xrpla keygen --out publisher.seed
xrpla export --source https://s1.ripple.com:51234/ --range 32571-33570 \
  --out archive --signing-key publisher.seed --delay-ms 200
```

The exporter pins requests to ledger hashes, obtains binary state and full object diffs,
verifies every root, then signs the manifest and writes `archive/publisher.pub`. RPC responses
are cached for retry (`--no-rpc-cache` turns that off). Keep `publisher.seed` private and
distribute `publisher.pub` independently. A completed export directory is immutable.
`--full-state` compares complete ledger states when a source cannot provide full diffs; it is
substantially more expensive.

For repeated publication, use `publish` with the same key and directory:

```sh
xrpla publish --source http://127.0.0.1:5005 --grpc-source grpc://127.0.0.1:50051 \
  --start 1000000 --until 1009999 --batch-ledgers 64 --out archive \
  --signing-key publisher.seed --compression-level 3
```

The gRPC transport obtains full object diffs from xrpld; HTTP Clio sources can supply binary
diffs as well. `--once` publishes at most one batch. Compression levels 1-19 keep the same
format; higher levels use more CPU. Publishing commits immutable signatures before replacing the
manifest, so new readers can follow a publication while it is written. The manifest is limited
to 16 MiB.

## Follow, prune and query

```sh
xrpla tail --archive https://YOUR-ARCHIVE/v2 --range 7d --publisher-key publisher.pub \
  --dir ./data --interval 15 --state-window 100000
xrpla prune --dir ./data --state-first 1000002
printf '%s' '{"method":"account_tx","params":{"account":"CLASSIC_ADDRESS","limit":20}}' \
  | xrpla rpc --dir ./data --request -
```

- `--range` takes `A-B`, `latest-N` or `Nd`. `Nd` means the last N times 86,400 seconds
  **relative to the archive tip** and requires signed close-time indexes; older v2 archives work
  with numeric ranges.
- State pruning keeps an object baseline at the retention boundary and all transaction history.
  Downloaded archives and verification scratch nodes are separate caches; pruning does not
  delete them. Deleting scratch nodes while the service is stopped forces a safe full replay on
  the next build.
- `xrpla compact --dir ./data` flushes and compacts an offline store; `--native` compacts an
  imported native RocksDB.

## Native imports from a store

`xrpla restore` (see [Restore into xrpld](../guides/restore-xrpld.md)) writes xrpld and Clio
databases straight from a publication. From a verified v2 store, the older import commands do
the same:

```sh
xrpla import rippled --dir ./data --out ./native-rippled
xrpla import rippled --dir ./data --out ./native-rocksdb --backend rocksdb
xrpla import clio --dir ./data --out ./clio-plan
xrpla import clio --dir ./data --out ./clio-plan --host CASSANDRA_HOST --keyspace xrpla
```

- NuDB is the default node store; `--backend rocksdb` writes RocksDB instead. Both use
  libxrpl's own backends. The destination must be empty or a matching earlier XRPLA import with
  the same backend, and the target node must be stopped. Ledgers commit in windows of 256 after
  their nodes are durable. The generated `rippled.cfg` uses absolute paths.
- Clio without `--host` writes a reviewable JSONL plan. With `--host`, it loads an empty or
  matching XRPLA keyspace. Set `XRPLA_CQL_PASSWORD` for `--username`; `--tls` validates the
  server certificate. Progress is restartable, a renewable lease prevents concurrent importers
  (a crashed import's lease expires within 180 seconds), and the ledger range is published after
  all writes are acknowledged. A new keyspace uses SimpleStrategy with replication factor 1;
  provision production replication separately. `--jobs N` sets the threads writing the plan,
  `--load-jobs N` the loader's connections.

## Archive services

For a long-running archive server, adapt `config/xrpla-archive.example.toml` into
`/etc/xrpla/archive.toml` (with an absolute data path under `/var/lib/xrpla`) and install
`xrpla-serve.service` and `xrpla-tail.service` from `config/`. They share that state directory
and run with `DynamicUser=yes`. Do not combine them with the unit `xrpla setup` installs: both
use an `xrpla` account and `/var/lib/xrpla` (see [SETUP.md](../SETUP.md#the-service-account-and-its-directories)).

`xrpla serve --config FILE` and `fetch`/`tail --config FILE` read that flat file's keys:
`archive`, `range`, `dir`, `publisher_key`, `jobs`, `http`, `ws`, `forward`, `forward_ws`,
`threads`, `interval`, `state_window`.
