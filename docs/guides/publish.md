# Run a publisher

A publisher follows an xrpld node, verifies every ledger, cuts history into pieces and bundles,
signs the catalog with its publisher key and uploads everything to an R2 (or other
S3-compatible) bucket. Everyone else only reads publications; very few operators need to run
one. This guide is for those who do.

Terms used below (piece, bundle, fragment, snapshot, catalog, spool) are explained in the
[glossary](../reference/glossary.md). The formats and every publisher tool are described in
[PUBLICATION.md](../PUBLICATION.md).

## How it fits together

`tools/live_publisher.py` runs the whole pipeline and restarts cleanly at any point:

1. **Follow.** `xrpla publish` follows the node into a signed local *spool*, checking each
   ledger's transaction and state tree roots and parent hash.
2. **Publish.** `piece_publisher.py` seals each complete 50,000-ledger piece (every segment is
   decoded again and compared byte for byte), rolls each complete 1,000,000-ledger interval into
   a bundle, writes each interval's state snapshot, and signs a new catalog generation in the
   local *bucket* directory.
3. **Push.** With `--push`, `s3_remote.py push` uploads each generation to R2, reads every
   object back and checks its SHA-256, and moves the remote `catalog/current.json` pointer last.
   Local copies R2 then holds are deleted, so the publisher's disk holds about one interval.
4. **Rotate.** At each interval boundary the spool restarts from the published snapshot, so it
   stays about one interval long.

The `publisher` profile of `xrpla setup` does not run this yet: `xrpla run` refuses it and
points to `tools/live_publisher.py`, run by `config/xrpla-publisher.service`.

## Before you start

| | Needed |
|---|---|
| A node | xrpld with gRPC (`[port_grpc]`) and JSON-RPC; for a backfill of old history, a full-history node (see [Reading a full-history xrpld](#reading-a-full-history-xrpld)). A Clio's JSON-RPC also works as a source. |
| XRPLA | installed under `/usr`, including the compiled tools (`xrpla_history_decode`, `xrpla_history_encode`, `xrpla_snapshot`, `xrpla_tx_index`) and `/usr/share/xrpla/tools`; `python3` with the system `libzstd` and `libsodium` |
| An S3-compatible bucket | for example Cloudflare R2, with an access key that may read, write and delete objects in it, and public read access over HTTPS for your users |
| Disk | about one interval of local work (spool, pieces, the next snapshot) with `--push`; keep `--min-free-gb` of headroom (the production publisher uses 50) |
| CPU | encoding is CPU-bound: about 30 ms of CPU per modern ledger with the compiled encoder at zstd 19 |

> TODO(maintainer): state disk, memory and CPU sizing for a mainnet publisher from the
> production publisher's figures.

## 1. Create the keys

The publisher key signs every catalog generation; the spool key signs the local spool.

```sh
sudo useradd --system --home-dir /var/lib/xrpla-publisher --shell /usr/sbin/nologin xrpla-publisher
sudo mkdir -p /etc/xrpla
sudo xrpla keygen --out /etc/xrpla/publisher.seed
sudo xrpla keygen --out /etc/xrpla/spool.seed
sudo chown xrpla-publisher: /etc/xrpla/publisher.seed /etc/xrpla/spool.seed
```

`keygen` creates the file readable by its owner only and refuses to overwrite one. Back up
`publisher.seed` somewhere safe and offline: anyone holding it can sign history your users will
accept, and losing it means starting a new publication under a new key.

Your users need the matching **public key**. The first publish writes it to the local bucket as
`publisher.pub` (32 raw bytes). Print it as hex with:

```sh
python3 -c "import sys; print(open(sys.argv[1], 'rb').read().hex())" /var/lib/xrpla-publisher/bucket/publisher.pub
```

Distribute it through a channel your users trust that is independent of the bucket (clients
never trust the copy inside the publication). `xrpla keygen` also prints the new seed's public
key, as `public_key` (hex), so you can publish it before the first push; the hex form is
accepted wherever a publisher key file is.

## 2. Configure the bucket credentials

Create `/etc/xrpla/publisher.env`, readable by `xrpla-publisher` only:

```sh
XRPLA_S3_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
XRPLA_S3_BUCKET=<BUCKET>
XRPLA_S3_ACCESS_KEY_ID=<ACCESS_KEY_ID>
XRPLA_S3_SECRET_ACCESS_KEY=<SECRET_ACCESS_KEY>
XRPLA_S3_PREFIX=mainnet/
# XRPLA_S3_REGION=auto      (the default, as R2 expects)
```

`XRPLA_S3_PREFIX` lets several publications share a bucket. Only one publisher may write a
prefix: pushes refuse a prefix signed by another key, and the pointer is moved with a
conditional write so two publishers cannot overwrite each other.

Your users' `<PUBLICATION_URL>` is the public HTTPS address of that prefix: the directory that
holds `catalog/current.json`. Users choose it by name once it is in the approved list (step 5).

> TODO(maintainer): document how the public read URL is set up for R2 (custom domain or
> r2.dev), and the URL of the official publication.

## 3. Run it as a service

1. Install the unit (installed by the package as `/usr/lib/systemd/system/xrpla-publisher.service`;
   from a source install it is under `PREFIX/lib/systemd/system`). Review its `ExecStart`: it
   follows `http://127.0.0.1:5005` with gRPC on `127.0.0.1:50051` and pushes to R2.
2. The first start of a new publication needs the first ledger once. Add
   `--start FIRST_LEDGER` to `ExecStart` (`sudo systemctl edit --full xrpla-publisher`), start
   it, and remove the option after the spool exists: later starts resume the spool named in
   `/var/lib/xrpla-publisher/current`.

   The spool's starting state is ledger `FIRST_LEDGER - 1`'s, read from the node. Mainnet's
   oldest available ledger is 32,570, so a mainnet publication of full history uses
   `--start 32571` and needs a full-history node (see
   [Reading a full-history xrpld](#reading-a-full-history-xrpld)); its base snapshot is
   ledger 32,570. A publication of recent history starts one ledger after any ledger the node
   holds in full.

3. Start it:

   ```sh
   sudo systemctl daemon-reload && sudo systemctl enable --now xrpla-publisher
   journalctl -u xrpla-publisher -f
   ```

Options the production publisher adds (see `live_publisher.py --help`):

| Option | Effect |
|---|---|
| `--fragment-ledgers 5000` | publish the piece still filling every 5,000 ledgers (about 5.4 hours), so the publication trails the network by less than that |
| `--tip-fragment-ledgers 250` | and the ledgers after the newest fragment every 250 ledgers (about 15 minutes), so new standalone nodes catch up from peers in minutes |
| `--min-free-gb 50` | pause the follower when free space falls below 50 GB (default 5) |
| `--grace-seconds N` | how long replaced pieces and fragments stay in the bucket for readers still fetching them (default 7 days) |
| `--until N` | follow no further than ledger N (for a backfill of an earlier range); publishing goes on |

## 4. Check that it publishes

1. Health check: exits non-zero and lists the problems when the runner, follower, spool,
   publishing or pushing has stalled for 30 minutes (`--max-age` seconds):

   ```sh
   sudo -u xrpla-publisher python3 /usr/share/xrpla/tools/live_publisher.py --health --workdir /var/lib/xrpla-publisher
   ```

   `WORKDIR/status.json` holds the details.

2. Read the publication as a user would, with the key pinned from your own copy:

   ```sh
   xrpla sync --source <PUBLICATION_URL> --publisher-key publisher.pub --dir /tmp/check --range A-B
   ```

3. For a full audit, `tools/publication_audit.py` downloads each bundle from the bucket as
   users do, restores it (`--restores nudb,store,clio`) and compares answers with a
   full-history xrpld (`--reference`). It only reads the bucket.

## 5. Offer it to users by name

`xrpla setup` offers the publications of the approved list installed with XRPLA
(`config/publications.toml`), and a configuration can name one (`[publication] name`), so users
need neither your URL nor your key by hand. To be listed, publish your publisher key where users
can check it independently (your own site, say), then open a pull request adding a
`[[publication]]` entry with that key and your mirrors: HTTPS base URLs that each serve the
publication's exact objects (a copy of the bucket prefix, or another public address of it).
[The publication list](../reference/publications.md) gives the format and what reviewers check.
Mirrors need no trust: readers check everything against your key, and move to the next mirror
when one is out of reach, failing, or behind.

## Reading a full-history xrpld

An xrpld limits each client's load. To read a full-history node without those limits, its
operator opens gRPC to the publisher's address and names it a secure gateway:

```
[port_grpc]
port = 50051
ip = 0.0.0.0
secure_gateway = PUBLISHER_IP
```

Firewall the port to that address: gRPC here is plaintext and unauthenticated.
`xrpla grpc-probe --source grpc://HOST:50051 --ledger N` reports `"unlimited": true` once the
exemption applies.

A few 2013 ledgers hold transactions current xrpld cannot decode, and its gRPC omits them.
`--history-peers default` fetches those ledgers' raw transaction trees from peers that hold
them. More source options (Clio as a source, combining sources with `--grpc-jobs`) are in
[SETUP.md](../SETUP.md#publishing-where-ledgers-come-from).

## Validator signatures instead of the node's word

`--validator-list https://vl.ripple.com --validator-list-key HEX --validations-node ws://...`
makes each batch end at a ledger a quorum of the list signed, so the node supplying ledger data
may be remote or untrusted. See [SETUP.md](../SETUP.md#validator-signatures).

## Operations

- **Stopping.** SIGTERM (`systemctl stop`) is safe at any point: a publish run writes its
  catalog generation last.
- **Moving to another machine.** Stop the publisher (and any push or merge), then copy the
  whole local bucket including its dotfiles (`.pushed`, `.remote-retired`, `.merging`) before
  starting on the new machine. Without them a push could delete remote objects readers are still
  fetching, so a push refuses a remote pointer that `.pushed` does not record. If they are lost,
  push once with `s3_remote.py push --bucket BUCKET --adopt-remote`.
- **Filling history from both ends.** A backfill of early history can run as a second
  publication under its own prefix (for example `mainnet-early/`), signed with the same key,
  and be joined onto the main one with `tools/publication_merge.py --bucket BUCKET
  --signing-key SEED --from-prefix mainnet-early/` (prints the plan; add `--apply` to copy,
  sign and push). Stop both publishers first.

  > TODO(maintainer): the merge's copy path has not yet been run against R2.

Details of every tool, the push protocol and grace periods: [PUBLICATION.md](../PUBLICATION.md#operation).
