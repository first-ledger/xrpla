# Restore history into Clio (Cassandra or ScyllaDB)

`xrpla restore --to clio` downloads history from a publication, verifies every ledger, and loads
it into a Cassandra or ScyllaDB keyspace in Clio's schema. Clio then serves that history and
follows new ledgers through its own ETL from an xrpld, as if it had built the database itself.

A Clio restored this way was compared with a Clio whose database Clio's own ETL built: across
906 ledgers, 14,092 answers were identical (the only differences were version numbers).

## Before you start

| | Needed |
|---|---|
| XRPLA | installed ([Install XRPLA](install.md)); `python3` (the loader, `clio_load.py`, needs no third-party packages) |
| Cassandra or ScyllaDB | reachable from this machine on its CQL port (9042 by default) |
| Clio | the schema written is Clio 2.7.1's; Clio 2.8.0 has been run on it |
| Disk on the XRPLA machine | a RocksDB store of the restored range (about 1 KB per transaction) plus the import plan, under `--out` |
| Publication | one from the [approved list](../reference/publications.md) installed with XRPLA (`xrpla publications` lists them: `xrpla-mainnet` for mainnet), or another publication's base URL, `<PUBLICATION_URL>`, and publisher key, `<PUBLISHER_KEY>` |

> TODO(maintainer): give the store and plan sizes, and the database size, for full history.

**What to expect.** For about 700,000 recent mainnet ledgers (107,000,001 to 107,697,250), on a
48-thread, 125 GB server shared with Cassandra 4.1.10 (G1 collector, 24 GB heap): writing the
import plan took 58 minutes, and loading its 951,674,801 rows took 1 hour 35 minutes. Raw logs:
[docs/benchmarks/2026-10-audit/clio-restore/](../benchmarks/2026-10-audit/clio-restore/).

> TODO(maintainer): no full-history Clio restore has been timed yet.

## How it works

1. XRPLA first restores the range into a local RocksDB store (Clio's tables are derived from its
   indexes), verifying every ledger.
2. It writes Clio's rows to an import plan, `OUT/plan/clio-plan.jsonl` (one JSON row per line),
   on `--jobs` threads. The plan is byte for byte the same for any number of jobs.
3. With `--host`, `tools/clio_load.py` loads the plan as it is written, on `--load-jobs`
   connections. Only when every row is acknowledged does it publish the ledger range, so Clio
   never sees a partial range.

Without `--host`, only the plan is written; you can review it and load it later.

## Restore with the wizard

```sh
sudo xrpla setup
```

| Question | Answer |
|---|---|
| How will this machine use XRPLA? | `2` or `restore` |
| Which publication? | `1` or `xrpla-mainnet` (or `other`, then the publication's URL and publisher key, `<PUBLICATION_URL>` and `<PUBLISHER_KEY>`) |
| First ledger | `earliest`, or the first ledger you need |
| Restore into | `clio` |
| Output directory | a directory for the store and the plan, for example `/var/lib/xrpla/restore` |
| Cassandra/ScyllaDB host | the database host (empty: write the plan only) |
| Port | `9042` |
| Keyspace | `xrpla` (created if missing) |
| Start the restore now? | `y` |

For a user name or TLS, add `clio_username` and `clio_tls = true` to `[restore]` in
`/etc/xrpla/xrpla.toml` and run `xrpla run --config /etc/xrpla/xrpla.toml`. The password comes
from the `XRPLA_CQL_PASSWORD` environment variable.

## Restore by hand

```sh
export XRPLA_CQL_PASSWORD=...        # only with --username
xrpla restore --publication xrpla-mainnet \
              --to clio --out /var/lib/xrpla/restore \
              --host CASSANDRA_HOST [--port 9042] [--keyspace xrpla] [--username USER] [--tls] \
              [--range A-B|A-] [--jobs N] [--load-jobs N]
```

For a publication not in the list, give `--source <PUBLICATION_URL> --publisher-key publisher.pub`
(the key saved with `echo <PUBLISHER_KEY> > publisher.pub`) instead of `--publication`.

| Option | Meaning |
|---|---|
| `--host`, `--port`, `--keyspace` | where to load (keyspace default `xrpla`) |
| `--username`, `--tls` | authentication (password in `XRPLA_CQL_PASSWORD`); `--tls` validates the server certificate |
| `--range` | as for the xrpld restore; omit for all of the publication |
| `--jobs N` | threads writing the plan (default: one per core, at most 32) |
| `--load-jobs N` | loader processes, each with its own connection (default 8, at most the cores) |

## The keyspace

- A missing keyspace is created with `SimpleStrategy` and replication factor 1. For production,
  create the keyspace yourself first with the replication you want; XRPLA loads into an empty
  keyspace or one it loaded before.
- Rows carry write timestamps that start about 13 days before the load, below Clio's own later
  writes. A keyspace an older XRPLA loaded with the server's timestamps is refused while those
  are newer: load into a new keyspace.
- A lease in the keyspace, renewed every 30 seconds, keeps two loads from running at once. A
  crashed load's lease expires within 180 seconds.

## Check the result

1. Point Clio at the keyspace and at an xrpld for new ledgers (its ETL source), and start it.
   A minimal `clio.json` for a keyspace `xrpla` on a local Cassandra, following an xrpld on the
   same host:

   ```json
   {
     "database": {
       "type": "cassandra",
       "cassandra": {
         "contact_points": "127.0.0.1",
         "port": 9042,
         "keyspace": "xrpla",
         "replication_factor": 1
       }
     },
     "etl_sources": [
       { "ip": "127.0.0.1", "ws_port": "6006", "grpc_port": "50051" }
     ],
     "server": { "ip": "127.0.0.1", "port": 51233 }
   }
   ```

   Use the keyspace you passed to `--keyspace`. The xrpld needs its WebSocket and gRPC ports
   open to Clio, with Clio's address under `secure_gateway` in `[port_grpc]`. Clio continues
   from the newest restored ledger. Every other setting keeps Clio's default; Clio's
   `example-config.json` lists them.

2. Ask Clio for its range and for a restored ledger, and compare the ledger hash with a public
   server:

   ```sh
   curl -s -d '{"method":"server_info"}' http://CLIO_HOST:PORT/
   curl -s -d '{"method":"ledger","params":[{"ledger_index":N}]}' http://CLIO_HOST:PORT/
   ```

## Resume and extend

- **Interrupted load?** Run the same command again. The load resumes from the first line every
  worker had acknowledged, provided the plan starts with the same bytes; otherwise every row is
  sent again (sending a row twice changes nothing).
- **Busy database?** Writes refused as overloaded, unavailable or timed out are retried with
  backoff; the load fails only after 10 minutes without any answer. Recoveries are logged once a
  minute.

## Troubleshooting

| Symptom | What to do |
|---|---|
| The load stops after a long garbage-collection pause | Cassandra with its default collector paused for 21 minutes during the 2026-10 test; switching to G1 fixed it. |
| The loader gives up waiting for the plan | It stops when the plan has not grown for 30 minutes (`clio_load.py --stall-seconds`). Look at the restore's own output for why the plan writer stopped. |
| Keyspace refused | It holds rows from another load with newer timestamps, or another importer holds its lease. Use a new keyspace, or wait for the lease to expire. |

More detail on the plan writer and the loader: [SETUP.md](../SETUP.md#restore-fill-an-xrpld-or-clio-database).
