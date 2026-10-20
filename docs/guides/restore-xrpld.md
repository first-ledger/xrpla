# Restore history into xrpld (NuDB or RocksDB)

`xrpla restore --to nudb` (or `--to rocksdb`) downloads history from a publication, verifies
every ledger, and writes xrpld's own databases directly: the node store (NuDB, xrpld's default,
or RocksDB), `ledger.db` and `transaction.db` (SQLite), plus a `rippled.cfg` that points at
them. Nothing is staged in between. Start xrpld on the result and it holds that history as if it
had acquired it from the network.

Use this when you want a full-history (or long-history) xrpld without spending months fetching
history from peers. If you only need to *serve* history over the API, a
[standalone XRPLA](standalone.md) needs about 1.3 TB instead of 40 TB or more.

## Before you start

| | Needed |
|---|---|
| XRPLA | installed ([Install XRPLA](install.md)); Linux x86-64, glibc 2.31 or later |
| xrpld | a 3.4.x xrpld to run on the result (the restore writes with libxrpl 3.4.1's own backends). Stop it while the restore writes. |
| Disk | xrpld-sized: about 40 TB or more for mainnet's full history (see below), on NVMe, plus temporary space of about one-tenth of the node store while the NuDB key file is built (about 4 TB for a 40 TB database) |
| Memory | the restore keeps the ledger state as hashes in memory, about 4 GB per restore job for today's mainnet state; parallel jobs (`--jobs`) need that each |
| Publication | one from the [approved list](../reference/publications.md) installed with XRPLA (`xrpla publications` lists them: `xrpla-mainnet` for mainnet), or another publication's base URL, `<PUBLICATION_URL>`, and publisher key, `<PUBLISHER_KEY>` |

**What to expect.** Measured on a 48-thread, 125 GB NVMe server with `--jobs 8`, restoring
100,000-ledger stretches of mainnet:

| Stretch | Restore time | xrpld database | Peak memory |
|---|---|---|---|
| 14,000,001-14,100,000 (2015) | 25 s | 18 GB | 4.3 GB |
| 38,000,001-38,100,000 (2018) | 51 s | 44 GB | 11.6 GB |
| 63,000,001-63,100,000 (2021) | 57 s | 55 GB | 11.4 GB |
| 88,000,001-88,100,000 (2024) | 1 min 16 s | 48 GB | 13.8 GB |

Source: [docs/benchmarks/2026-10-audit/NOTES.md](../benchmarks/2026-10-audit/NOTES.md). The
recent million-ledger bundle (972,189 ledgers from 106,027,812) restored in about an hour into
about 1 TB, with a peak of 28.8 GB of memory.

> TODO(maintainer): the million-ledger figure comes from an internal run (2026-09-27, build
> b8eecec); add its receipt to docs/benchmarks or drop it. No full-history restore has been
> timed yet: add the measured time and size once one has run.

## Restore with the wizard

1. Run setup and choose the restore profile:

   ```sh
   sudo xrpla setup
   ```

   | Question | Answer |
   |---|---|
   | How will this machine use XRPLA? | `2` or `restore` |
   | Which publication? | `1` or `xrpla-mainnet` (or `other`, then the next two) |
   | Publication (HTTPS base URL or bucket directory) | only for `other`: `<PUBLICATION_URL>` |
   | Publisher key (file path, or 64 hex characters) | only for `other`: `<PUBLISHER_KEY>` |
   | First ledger (a sequence, or earliest) | `earliest` for full history, or the first ledger you need |
   | Restore into | `nudb` (or `rocksdb`) |
   | Output directory | an empty directory on the disk xrpld will use, for example `/var/lib/xrpld-restore` |
   | Start the restore now? | `y` |

   Setup writes `/etc/xrpla/xrpla.toml` (a listed publication by its name, so its mirrors and
   key come from the installed list) and, with `y`, starts the restore in the foreground. A
   restore of long history runs for hours: start it inside `tmux` or `screen`, or answer `n` and
   run it later with `xrpla run --config /etc/xrpla/xrpla.toml`.

## Restore by hand

1. Run the restore from a listed publication:

   ```sh
   xrpla restore --publication xrpla-mainnet --to nudb --out /var/lib/xrpld-restore
   ```

   For a publication not in the list, save its publisher key and name its URL instead:

   ```sh
   echo <PUBLISHER_KEY> > publisher.pub
   xrpla restore --source <PUBLICATION_URL> --publisher-key publisher.pub \
                 --to nudb --out /var/lib/xrpld-restore
   ```

   | Option | Meaning |
   |---|---|
   | `--to nudb` / `--to rocksdb` | the xrpld node store backend |
   | `--range A-B` | restore ledgers A to B; `--range A-` from A to the publication's tip; omit for all of it |
   | `--jobs N` | snapshot intervals restored side by side (default: a third of the cores, at most one per 5 GB of available memory); `--jobs 1` restores in order |

   Starting at ledger A replays from the newest snapshot below A: ledgers before A are verified
   but not written. A full-history restore runs each million-ledger interval from its own
   snapshot in parallel, so it takes about as long as its longest interval.

2. When it finishes, the restore prints a JSON summary (ledgers written, transactions, the last
   ledger and its hash, timings) and `config`, the path of the generated `rippled.cfg`.

The output directory holds:

| Path | What |
|---|---|
| `node-db/` | the node store (NuDB or RocksDB) |
| `ledger.db`, `transaction.db` | xrpld's SQLite databases, with xrpld's exact schemas |
| `rippled.cfg` | a minimal xrpld configuration using absolute paths to the above: `[node_db]`, `[database_path]`, `[ledger_history] full`, `[fetch_depth] full`, an admin JSON-RPC port on 127.0.0.1:5007, and `rippled.log` |

## Check the result

1. Open it with xrpld in standalone mode (offline, no peers) using the generated configuration,
   with LAST the last ledger the restore wrote:

   ```sh
   xrpld --conf /var/lib/xrpld-restore/rippled.cfg --standalone --ledger LAST --valid
   ```

   A standalone xrpld counts only the loaded ledger as present unless told otherwise; add
   `--force_ledger_present_range=FIRST,LAST` to query the whole restored range.

2. Ask it for a ledger and compare the hash with a public server:

   ```sh
   curl -s -d '{"method":"ledger","params":[{"ledger_index":LAST}]}' http://127.0.0.1:5007/
   curl -s -d '{"method":"ledger","params":[{"ledger_index":LAST}]}' https://s2.ripple.com:51234/
   ```

3. Optionally compare two restores node by node:
   `xrpla compare-nodes --dir A/node-db --against B/node-db [--backend nudb|rocksdb]` reports
   the nodes they do not share or hold differently.

The restore itself already checked every ledger: each object and transaction, each ledger's
parent hash, transaction tree root and state tree root against its header, and each range's last
ledger hash against the signed catalog. See [Security model](../reference/security.md).

## Run xrpld on the restored history

> TODO(maintainer): confirm and document the production procedure (which settings to carry
> into an existing xrpld.cfg, and how xrpld fills the gap between the restore's tip and the
> network). The outline below follows the generated `rippled.cfg`.

1. Stop xrpld.
2. In your `xrpld.cfg`, set `[node_db]` (`type = NuDB` or `RocksDB`, `path =` the restore's
   `node-db`), `[database_path]` (the restore directory) and `[ledger_history] full` from the
   generated `rippled.cfg`. Do not enable `online_delete` if you want to keep the history.
3. Start xrpld. It joins the network and acquires ledgers newer than the restore's tip from
   peers.

## Extend or resume

- **Interrupted?** Run the same command again. The restore continues after its last committed
  256-ledger window (an interval restore resumes by interval).
- **Newer history later?** Stop xrpld and run the restore again into the same directory with a
  later (or open-ended) range: it extends the earlier restore and rebuilds the NuDB key file.
- Start a new restore in an empty directory; a directory XRPLA restored before is resumed or
  extended. Anything else is refused before a byte is written:
  - a non-empty directory without XRPLA's receipt (`xrpla-import.json`): "Import destination
    must be empty or an xrpla import directory";
  - a restore with the other backend, or of other history: "Native import target version
    differs", "This directory was restored by intervals" or "Import destination belongs to
    different history". Use a new directory.

## Troubleshooting

| Symptom | What to do |
|---|---|
| Out of memory | Lower `--jobs`; each job holds the state's hash tree (about 4 GB). |
| Disk full near the end | NuDB's key file is built at the end from a log of about one-tenth of the data file; leave that space free. |
| Slow at the end | `transaction.db`'s indexes are built by sorting once all ledgers are written (alongside NuDB's key file). `XRPLA_SQL_INDEX=sqlite` builds them with SQLite's `CREATE INDEX` instead; `XRPLA_SQL_BULK=0` creates xrpld's schema with its indexes from the start. |
| "verifier state ... is missing" when resuming | The directory was restored with a different verifier; resume without `XRPLA_RESTORE_VERIFIER=libxrpl`, or use a fresh directory. |

How the restore works inside (NuDB built directly, SQLite bulk loading, the in-memory hash
tree) is described in [SETUP.md](../SETUP.md#restore-fill-an-xrpld-or-clio-database).
