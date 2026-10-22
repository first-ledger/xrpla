# Serve full history without xrpld (standalone)

A standalone XRPLA machine serves the full XRP Ledger history over HTTP and WebSocket JSON-RPC
(the xrpld and Clio APIs) with no xrpld anywhere:

- **History** comes from a publication. XRPLA downloads it once into a *piece cache* (see the
  [glossary](../reference/glossary.md)) and keeps it compressed; the API answers straight from
  the compressed data.
- **The newest ledgers** (those the publication does not hold yet) come from the XRPL peer
  network. XRPLA joins it as a non-validating node, checks each ledger against a quorum of the
  validator list and appends it to a local *spool*.

It does not take part in consensus and does not relay transactions. Methods that need the
current open ledger (`submit`, `fee`, `path_find`, ...) can be forwarded to an xrpld you run
elsewhere (see [Forwarding](#forwarding-submit-and-other-current-ledger-methods)).

## Before you start

| | Needed |
|---|---|
| OS | Linux x86-64 with glibc 2.31 or later (for example Ubuntu 22.04 or later), systemd |
| XRPLA | installed under `/usr` ([Install XRPLA](install.md)) |
| Disk | about 1.3 TB for mainnet's full history with its indexes; use a 2 TB NVMe drive to leave room for growth |
| Memory | the decode cache defaults to a quarter of RAM (between 3 and 64 GB); the process uses about a quarter more than the cache under heavy load. The full-history test node runs with 61 GB of RAM and a 32 GB cache. |
| Network | HTTPS to the publication; outbound TCP to peers on port 51235 |
| Publication | one from the [approved list](../reference/publications.md) installed with XRPLA (`xrpla publications` lists them: `xrpla-mainnet` for mainnet), or another publication's base URL, `<PUBLICATION_URL>`, and publisher key, `<PUBLISHER_KEY>` (64 hex characters) |

> TODO(maintainer): publish the `xrpla-mainnet` publisher key
> (`1b4adf5cccad231179f5eaad2ede3f1927908d9cee0966f0b559a617264e6ce6`) somewhere users can check it
> independently of this repository, and link it from the entry's homepage.

**Time.** The first start downloads about 1.2 TB and builds the local query indexes. From R2
at the measured 120 MB/s the download alone takes about 3 hours; index building adds hours more,
depending on cores and disk. While that runs, nothing is served yet. Later starts are quick.

> TODO(maintainer): replace the estimate above with a measured first-start time for a full
> mainnet sync on stated hardware.

Smaller histories are possible: set `[history] first` to a later ledger and XRPLA downloads only
what it needs from that ledger on (each interval's snapshot plus the pieces after it).

## Set it up with the wizard

1. Run the setup wizard as root, so it can create the service account and install the unit:

   ```sh
   sudo xrpla setup
   ```

2. Answer the questions:

   | Question | Answer |
   |---|---|
   | How will this machine use XRPLA? | `4` or `standalone` |
   | Which network? | asked only when the list has testnet or devnet publications: `mainnet` |
   | Which publication? | `1` or `xrpla-mainnet` (or `other`, then the next two) |
   | Publication (HTTPS base URL or bucket directory) | only for `other`: `<PUBLICATION_URL>` |
   | Publisher key (file path, or 64 hex characters) | only for `other`: `<PUBLISHER_KEY>` |
   | First ledger (a sequence, or earliest) | `earliest` for full history |
   | Storage directory | `/var/lib/xrpla` (or the mount point of your data disk) |
   | Decode cache (MB; 0: a quarter of memory, 3-64 GB) | `0` |
   | HTTP JSON-RPC listener | `127.0.0.1:5005` |
   | WebSocket listener | `127.0.0.1:6006` |

   Setup writes `/etc/xrpla/xrpla.toml` (with a listed publication by its name, so a package
   upgrade's list, with any new mirrors, applies; for `other`, `/etc/xrpla/publisher.pub` when
   you gave the key as hex), creates the `xrpla` system account, gives it the storage directory and installs
   `/etc/systemd/system/xrpla.service`. If it refuses the storage directory, it says why and,
   where it can, prints the commands that fix it; see
   [The service account](../SETUP.md#the-service-account-and-its-directories).

3. Start the service and follow its log:

   ```sh
   sudo systemctl daemon-reload && sudo systemctl enable --now xrpla
   journalctl -u xrpla -f
   ```

## Set it up by hand

The wizard only writes a configuration file. You can write it yourself:

1. Copy the configuration reference and edit it:

   ```sh
   sudo mkdir -p /etc/xrpla
   sudo cp /usr/share/xrpla/xrpla.example.toml /etc/xrpla/xrpla.toml
   ```

   Every setting in that file is commented out with its default. Uncomment and set at least:

   ```toml
   profile = "standalone"

   [publication]
   name = "xrpla-mainnet"   # from the approved list: `xrpla publications`

   [storage]
   dir = "/var/lib/xrpla"
   ```

   For a publication not in the list, give its URL (or an array of mirror URLs, tried in order)
   and its publisher key instead of `name`:

   ```toml
   [publication]
   source = "<PUBLICATION_URL>"
   publisher_key = "/etc/xrpla/publisher.pub"   # a file: 32 raw bytes or 64 hex characters
   ```

   and put the key in `/etc/xrpla/publisher.pub`, for example
   `echo <PUBLISHER_KEY> | sudo tee /etc/xrpla/publisher.pub`.

2. Run it in the foreground:

   ```sh
   xrpla run --config /etc/xrpla/xrpla.toml
   ```

   Run it as the account that owns the storage directory (for example
   `sudo -u xrpla xrpla run --config /etc/xrpla/xrpla.toml`), not as root. To run it as a
   service, run `sudo xrpla setup` once: it keeps nothing from your file except `[log]`, so
   answer its questions with the same values.

All settings, with defaults and accepted values, are in
[config/xrpla.example.toml](../../config/xrpla.example.toml).

## What happens on the first start

`xrpla run` logs one JSON object per line. On the first start it:

1. **Syncs the piece cache** (`STORAGE/cache`): downloads the snapshot and the pieces and
   bundles the selected ledgers need, checks each against the signed catalog, and builds the
   query indexes. While it works it logs `sync_progress` once a minute: while downloading,
   the objects and bytes done out of the total, the speed and `eta_seconds`; then, while
   indexing, the step and how many ranges are done. It ends with a summary line (`generation`,
   `fetched_bytes`, `seconds`). A failed sync is retried every 30 seconds (`sync_failed`).

   ```sh
   journalctl -u xrpla -f | grep sync_progress
   ```
2. **Starts an interim server** on your listeners that answers from the piece cache alone.
3. **Builds the spool** (`STORAGE/spool`, event `spool_restored`): the ledger state at the
   publication's tip, built from its newest snapshot. On the 16-thread test machine this took
   about 7 minutes.
4. **Starts the follower** (`peer_follower`), which joins the peer network with its own node
   key (`STORAGE/keys/node.key`), and catches up from the publication's tip to the network.
   SETUP.md gives about two ledgers a second against mainnet's public hubs.

   > TODO(maintainer): one measured new install caught up a 5,054-ledger gap in 2 h 10 min
   > (about 0.65 ledgers a second). Confirm the expected catch-up rate and update this step.

5. **Swaps in the full server**, which answers from the piece cache and, for the newest
   ledgers, from the spool.

After that it keeps going on its own: it syncs the cache every 300 seconds
(`[standalone] sync_seconds`), checks hourly for a newer published snapshot to restart the spool
from (`rotate_seconds`), and polls peers every 3 seconds (`poll_seconds`).

## Check that it works

1. Ask for the server's state and range:

   ```sh
   curl -s -H 'Content-Type: application/json' -d '{"method":"server_info"}' http://127.0.0.1:5005/
   ```

   `complete_ledgers` should run from the first ledger you selected to a ledger a few seconds
   old, and `validated_ledger.seq` should move as you repeat the request. Compare it with a
   public server, for example `https://s2.ripple.com:51234/`.

   A full mainnet node reports `complete_ledgers` from 32570, the publication's base snapshot
   and the oldest ledger mainnet still has.

2. Ask for something old, for example a 2013 ledger:

   ```sh
   curl -s -H 'Content-Type: application/json' \
     -d '{"method":"ledger","params":[{"ledger_index":100000}]}' http://127.0.0.1:5005/
   ```

3. Watch the log for problems: `follower_stuck` (a ledger failed 10 times; it is retried and
   reported again every 100 attempts, then `follower_recovered`), `sync_failed`,
   `config_unknown_setting` (a misspelt setting, otherwise ignored).

4. `xrpla_cache_stats` (an XRPLA method, not an xrpld one) reports the decode cache's use and
   hit rates.

## Expose it to other machines

The defaults listen on 127.0.0.1 only. To answer other machines:

- set `[serve] http` and `ws` to `0.0.0.0:PORT`, or use the TLS listeners (`https`, `wss` with
  `tls_cert` and `tls_key`; renewed certificates are picked up within a minute);
- put a reverse proxy or rate limiter in front of public listeners. Behind a proxy on the same
  machine set `[serve] loopback_trusted = false` and limit clients at the proxy;
- `max_connections` (1,024) and `max_connections_per_ip` (64) cap open connections.

See [Security model](../reference/security.md).

## Forwarding submit and other current-ledger methods

XRPLA answers from validated history. To accept `submit`, `fee`, `ledger_current`,
`server_state`, `ripple_path_find` and the other current-ledger methods, point it at an xrpld:

```toml
[serve]
forward = "http://XRPLD_HOST:PORT"      # a public (non-admin) port
forward_ws = "ws://XRPLD_HOST:PORT"     # for path_find, which xrpld streams over WebSocket only
```

XRPLA refuses to forward to an admin port: it checks with `get_counts` before sending anything.
The full rules are in [config/xrpla.example.toml](../../config/xrpla.example.toml) (`forward`)
and [API reference](../reference/api.md#forwarded-methods).

## Optional settings

- **Faster old state reads.** `[cache] snapshots_every = 10000000` keeps one extra snapshot
  every 10 million ledgers (about 21 GB more disk for mainnet), so cold `account_lines`,
  `account_objects` and `book_offers` at old ledgers take milliseconds rather than seconds.
  See [SETUP.md](../SETUP.md#configuration) for the table of costs.
- **Clio's MPT and NFT methods.** `[index] mpt_holders` and `nfts_by_issuer` are on by default.
- **Serving peers.** `[peers] serve = true` makes the node answer peers for the newest 256
  ledgers, as a non-validating xrpld does; open the peer port (`listen`, default
  `0.0.0.0:51235`). See [PEER-SERVING.md](../PEER-SERVING.md).
- **Another network.** `[network] id = "testnet"` or `"devnet"` picks that network's hubs and
  validator list; you need a publication of that network too.

## Upgrade

1. Install the new release ([Install XRPLA](install.md)).
2. Restart: `sudo systemctl restart xrpla`.

Everything resumes from the piece cache and the spool. When a release changes the local query
index format, the next sync rebuilds the indexes before serving, which can take hours on full
history; the release notes say so.

> TODO(maintainer): confirm that release notes will flag index-format changes, and whether the
> interim server answers during such a rebuild.

## Troubleshooting

| Symptom | What to do |
|---|---|
| `xrpla setup` refuses the storage directory | Read its message: it lists what it found (files owned by another user, ACLs, group-writable entries, a network file system, ...) and, for what a root-run profile leaves, prints the commands that fix it. Details: [SETUP.md](../SETUP.md#the-service-account-and-its-directories). |
| Setup warns that `ExecStart` is not an installed program | You ran setup from a build tree. Install XRPLA under `/usr` and run setup again. |
| Nothing listens for a long time after the first start | The first sync is still downloading or indexing: its `sync_progress` lines, once a minute, show how far it has got and `eta_seconds` for the download. |
| `follower_stuck` repeats | The node cannot fetch or verify a ledger from peers. Check outbound access to port 51235; set `[peers] endpoints` to peers you know. |
| A method answers `notSupported` | `mpt_holders` / `nfts_by_issuer`: the index is turned off in `[index]`. `submit` and other current-ledger methods: set `[serve] forward`. |
| Requests answer `tooBusy` | Raise `[serve] threads` (about one per core for a busy server). |
| Old `book_offers` or `account_lines` are slow when cold | Set `[cache] snapshots_every` (above). |

The full behaviour of the standalone profile is in [SETUP.md](../SETUP.md#standalone-no-xrpld).
