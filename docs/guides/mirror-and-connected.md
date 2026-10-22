# Mirror a publication, or run beside your own xrpld

Two more ways to serve history with XRPLA:

- **Mirror** (`profile = "mirror"`): keep a local copy of a publication and serve the API from
  it. No peers, no xrpld. The served history ends where the publication ends, so it trails the
  network by however far the publication does. Use it for archive queries, or behind a
  load balancer next to standalone nodes.
- **Connected** (`profile = "connected"`): follow an xrpld you run (or a remote one) a few
  seconds behind it, keep its history compressed, and serve it. Optionally take older history
  from a publication.

If you want current ledgers without running xrpld, use [standalone](standalone.md) instead.

## Mirror

### Before you start

The same as for a standalone node ([Before you start](standalone.md#before-you-start)): about
1.3 TB of disk for mainnet's full history, a quarter of RAM for the decode cache by default,
HTTPS to the publication, and a publication: one from the
[approved list](../reference/publications.md) (`xrpla publications`; `xrpla-mainnet` for
mainnet), or another's URL and key, `<PUBLICATION_URL>` and `<PUBLISHER_KEY>`. No peer network
access is needed.

### With the wizard

```sh
sudo xrpla setup
```

| Question | Answer |
|---|---|
| How will this machine use XRPLA? | `1` or `mirror` (the default) |
| Which publication? | `1` or `xrpla-mainnet` (or `other`, then the next two) |
| Publication (HTTPS base URL or bucket directory) | only for `other`: `<PUBLICATION_URL>`, or a local bucket directory |
| Publisher key | only for `other`: `<PUBLISHER_KEY>` |
| First ledger | `earliest`, or the first ledger you want |
| Storage directory | `/var/lib/xrpla` (the piece cache lives here) |
| Decode cache (MB) | `0` (a quarter of memory, 3-64 GB) |
| HTTP JSON-RPC listener / WebSocket listener | `127.0.0.1:5005` / `127.0.0.1:6006` |
| Check the publication every (seconds) | `300` |

Then start it:

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now xrpla
journalctl -u xrpla -f
```

### By hand

The mirror profile is two commands, which you can also run yourself:

```sh
xrpla sync --publication xrpla-mainnet --dir /var/lib/xrpla/cache
xrpla serve --dir /var/lib/xrpla/cache --http 127.0.0.1:5005 --ws 127.0.0.1:6006
```

For a publication not in the list, save its key (`echo <PUBLISHER_KEY> > publisher.pub`) and give
`--source <PUBLICATION_URL> --publisher-key publisher.pub` instead of `--publication`.

`sync` downloads what the selected ledgers need (the base snapshot and the containers after
it, each checked against the signed catalog, from the publication's mirrors in turn), builds the local query index for anything new,
then switches `selection.json` and removes files no longer selected. A running `serve` picks up
the new selection. Run `sync` again (from cron or a timer) to stay current; the mirror profile
does that every `[mirror] sync_seconds`. A sync that runs longer than a minute (the first one
takes hours) logs `sync_progress` to stderr once a minute: bytes and objects done out of the
total and `eta_seconds` while downloading, then the ranges indexed.

Useful `sync` options: `--range A-B` or `--range A-` (which ledgers), `--jobs N` (parallel
downloads, default 4), `--indexes all|none|mpt_holders,nfts_by_issuer`, `--snapshots-every N`
(extra snapshots for faster old state reads; see
[SETUP.md](../SETUP.md#configuration)).

### Check it

```sh
curl -s -H 'Content-Type: application/json' -d '{"method":"server_info"}' http://127.0.0.1:5005/
```

`complete_ledgers` should match the publication's range.

## Connected

### Before you start

| | Needed |
|---|---|
| xrpld | JSON-RPC reachable from this machine; gRPC (`[port_grpc]`) recommended, so each ledger's changes come in one request; its WebSocket if you use `[validators]` |
| Python | `python3` with the system `libzstd` and `libsodium`, for the local publishing pipeline (only without a `[publication]`) |
| Disk | the compressed history it keeps, plus the follower's verified state tree (its node store grows by about 0.7 MB per mainnet ledger and is compacted every 16,384 ledgers) |

> TODO(maintainer): give disk and memory guidance for a connected node, with and without a
> publication.

### With the wizard

```sh
sudo xrpla setup
```

| Question | Answer |
|---|---|
| How will this machine use XRPLA? | `3` or `connected` |
| Node JSON-RPC URL | `http://127.0.0.1:5005` |
| Node gRPC URL for ledger diffs | `grpc://127.0.0.1:50051` (empty: JSON-RPC only) |
| Accept only ledgers a quorum of validators signed...? | `y` for a remote or shared node; then the validator list, its key and the node's WebSocket URL |
| Which publication? | `xrpla-mainnet` to serve full history, `none` (the default) to keep history from now on, or `other` |
| Publication for earlier history | only for `other` (or when no publication is listed): `<PUBLICATION_URL>`, or empty to keep history from now on |
| Publisher key | `<PUBLISHER_KEY>` (only with a publication given by URL) |
| First ledger to keep | `tip`, or a ledger the node still holds (only without a publication) |
| Storage directory, decode cache | as for mirror |
| HTTP JSON-RPC listener / WebSocket listener | `127.0.0.1:5105` / `127.0.0.1:6106` (xrpld itself usually has 5005 and 6006) |

Then start it with `systemctl` as above.

### By hand

Write `/etc/xrpla/xrpla.toml` from [config/xrpla.example.toml](../../config/xrpla.example.toml);
the minimum is:

```toml
profile = "connected"

[node]
rpc = "http://127.0.0.1:5005"
# grpc = "grpc://127.0.0.1:50051"

[connected]
start = "tip"

[storage]
dir = "/var/lib/xrpla"

[serve]
http = "127.0.0.1:5105"
ws = "127.0.0.1:6106"
```

and run `xrpla run --config /etc/xrpla/xrpla.toml` as the account that owns the storage
directory. For history from a publication, add a `[publication]` section (`name = "xrpla-mainnet"`,
or `source` and `publisher_key`) in place of `[connected] start`.

### What it does

**Without a publication** XRPLA keeps the node's history from `start` on. The first start
exports the node's state once (tens of minutes on mainnet) before it listens. After that the
follower commits each new validated ledger to a spool, complete 50,000-ledger pieces are sealed
into `STORAGE/bucket` and signed with this machine's own key (`STORAGE/keys/local.pub`), and the
server answers from those pieces and the spool.

**With a publication** XRPLA keeps the publication's whole history, as mirror and standalone do,
and the node supplies only the ledgers after the publication's tip. Ledgers the node no longer
holds are fetched from the peer network first (`[peers]` settings apply then).

A local node can be trusted as it is. With a remote one, set `[validators]` so ledgers are
accepted only once a quorum of the list's validators signed them.

Full details: [SETUP.md](../SETUP.md#connected-follow-a-node-and-serve-its-history).

### Check it

```sh
curl -s -H 'Content-Type: application/json' -d '{"method":"server_info"}' http://127.0.0.1:5105/
```

`validated_ledger.seq` should be within a few ledgers of the node's.

## Upgrade and troubleshooting

Upgrading and the common problems are the same as for a standalone node: see
[Upgrade](standalone.md#upgrade) and [Troubleshooting](standalone.md#troubleshooting). For a
connected node, also check that the node answers on the configured URLs and, with
`[validators]`, that its WebSocket validations stream is reachable.
