# XRPLA

XRPLA is a verifiable archive of the full XRP Ledger history. It lets you:

- **serve full history** over HTTP and WebSocket JSON-RPC (the xrpld and Clio APIs) from a
  compressed local copy, with no xrpld, following new ledgers from the XRPL peer network;
- **restore history** into xrpld's databases (NuDB or RocksDB, plus SQLite) or into Clio
  (Cassandra or ScyllaDB), without months of syncing from peers.

History is distributed as a *publication*: signed, compressed pieces of history in a Cloudflare
R2 bucket that anyone can download over HTTPS. Mainnet's full history is about 1.3 TB as a
servable local copy, against roughly 40 TB for a full-history xrpld
([benchmarks](docs/benchmarks/2026-10-audit/report/XRPLA-benchmarks-2026-10.pdf)).

## Why you can trust it

You pin the publisher's public key, obtained independently of the bucket. Every download is
checked against the catalog the publisher signed, every segment against its SHA-256, and every
ledger is replayed: XRPLA recomputes its transaction and state tree roots and checks them and
its parent hash against its header, and each range's last ledger hash against the catalog. New
ledgers from the peer network are accepted only once a quorum of the validator list signed them.
Sampled across all of mainnet's history and compared with a full-history xrpld, its answers were
identical (120,617 of 120,617). See the
[security model](docs/reference/security.md).

## Choose your path

| I want to... | Guide |
|---|---|
| Serve full history without running xrpld | [Standalone](docs/guides/standalone.md) |
| Restore history into xrpld (NuDB or RocksDB) | [Restore into xrpld](docs/guides/restore-xrpld.md) |
| Restore history into Clio (Cassandra or ScyllaDB) | [Restore into Clio](docs/guides/restore-clio.md) |
| Serve a copy of a publication, or run beside my own xrpld | [Mirror and connected](docs/guides/mirror-and-connected.md) |
| Publish signed history to R2/S3 | [Run a publisher](docs/guides/publish.md) |

Most machines are set up with one command, `sudo xrpla setup`, which asks a few questions,
writes `/etc/xrpla/xrpla.toml` and installs a systemd service. Every guide also shows the
manual commands.

All documentation: [docs/README.md](docs/README.md).

## Install

XRPLA runs on Linux x86-64 with glibc 2.31 or later (Ubuntu 20.04+, Debian 12+). Install it
from the XRPLA apt repository:

```sh
curl -fsSL https://first-ledger.github.io/xrpla/xrpla-archive-keyring.gpg   | sudo tee /usr/share/keyrings/xrpla-archive-keyring.gpg >/dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/xrpla-archive-keyring.gpg] https://first-ledger.github.io/xrpla stable main"   | sudo tee /etc/apt/sources.list.d/xrpla.list
sudo apt update
sudo apt install xrpla
```

See [Install XRPLA](docs/guides/install.md) for the packages, building with Docker or from
source, and the runtime dependencies. XRPLA 0.4.0 is built on libxrpl 3.4.1 and RocksDB 10.5.1, and
restores into xrpld 3.4 and Clio 2.7/2.8 databases.

## Licence

MIT; see [LICENSE](LICENSE). Third-party licence notices are in [licenses/](licenses/).
