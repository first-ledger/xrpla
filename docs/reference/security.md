# Security model

What XRPLA checks, what it trusts, and how to run it safely.

## What you trust

When you restore or serve a publication, you trust exactly two things:

1. **The publisher key you pinned.** Every client is given the publisher's public key
   (`--publisher-key`, `[publication] publisher_key`, or the key the
   [approved publication list](publications.md) installed with XRPLA gives for a publication
   chosen by name) and never trusts a key found inside the publication. Get it from a source
   independent of the bucket.
2. **The XRP Ledger's own hashes**, which XRPLA recomputes for every ledger.

You do not need to trust the bucket, the CDN, HTTPS, or the network path: everything downloaded
is checked before it is used, and a publication read over HTTPS needs no credentials.

## The verification chain

For every profile that reads a publication:

1. The **catalog** (the publication's signed table of contents) is checked against the pinned
   publisher key (Ed25519).
2. Each **container directory** (the index at the front of a piece or bundle) is checked against
   the hash the catalog lists for it.
3. Each **segment** is checked against its SHA-256 in the directory, and each decoded 64-ledger
   **page** against its canonical digest.
4. Every **ledger is replayed** through the verifier, which recomputes its transaction tree root
   and state tree root and checks them, and its parent hash, against its header.
5. Each **range's last ledger hash** is checked against the catalog, and consecutive ranges must
   form a hash chain.

So a single signed hash per range, together with the parent-hash chain, authenticates every
ledger in it. A restore writes a ledger's nodes into xrpld's database only once its root
matches, so what it writes is the canonical data.

## Where the publisher's hashes come from

A publisher signs only ledgers it has verified itself. It can take the node's word that a
ledger is validated, or (`--validator-list`, `--validator-list-key`, `--validations-node`) end
each batch only at a ledger that a quorum of a validator list signed: it checks every
validation's signature against the list (whose own signature is checked against its publisher's
key), counts each trusted validator once per ledger hash, and pins that hash before committing.
The node supplying ledger data may then be remote or untrusted.

Root and hash consistency alone is not independent consensus validation. To check a publication
against the network yourself, compare a ledger hash from it with one from public servers, for
example `ledger` at a recent and an old sequence on `https://s2.ripple.com:51234/`. The v2
archive commands also accept an independently obtained descendant ledger hash with
`--trust-ledger-hash`.

## New ledgers on standalone and connected nodes

- **Standalone** accepts a ledger from the peer network only when a quorum of its validator list
  signed it (default for mainnet: `https://vl.ripple.com`, publisher key
  `ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734`). Every header, tree
  node and object fetched from peers is checked against the hash that commits to it, and the
  verifier then checks the whole ledger.
- **Connected** trusts its node for which ledgers are validated, unless `[validators]` is set
  (recommended for a remote or shared node).

`xrpla validators --list https://vl.ripple.com --list-key ED2677...` checks a validator list and
prints its size and quorum; with `--node ws://...` it counts validations from that node's
stream.

## Running the service safely

- **Its own account.** The unit `xrpla setup` installs runs as the `xrpla` system account, not
  root. It may write only the storage directory and the log file's directory; the rest of the
  file system is read-only to it. Setup refuses directories another account could tamper with
  (other owners, group- or world-writable entries, POSIX ACLs, network file systems) and, where
  it can, prints the commands that fix them. Details:
  [SETUP.md](../SETUP.md#the-service-account-and-its-directories).
- **Listeners.** The defaults listen on 127.0.0.1 only. XRPLA has no admin methods, but put
  public listeners behind a proxy or rate limiter. `max_connections` (1,024) and
  `max_connections_per_ip` (64) bound open connections; a WebSocket subscriber that falls 4,096
  messages behind is disconnected.
- **Loopback trust.** Clients on this machine's own addresses may call the internal `_` methods
  and are not held to `max_connections_per_ip`. Behind a reverse proxy on the same machine every
  client arrives from loopback: set `[serve] loopback_trusted = false` there.
- **TLS.** `[serve] https`/`wss` with `tls_cert` and `tls_key`; the key must be readable by the
  `xrpla` account. Certificates are reloaded within a minute of renewal.
- **Forwarding.** XRPLA forwards current-ledger methods only to an upstream that refuses it
  admin rights (checked with `get_counts` before anything is sent, and again every 5 minutes),
  strips signing fields and credentials, and never retries a submission. See
  [API reference](api.md#forwarded-methods).
- **Peer network.** A standalone node relays validators' validations and proposals after
  checking them, as xrpld does, never transactions, and takes no part in consensus. Its peer
  identity is `STORAGE/keys/node.key`. See [PEER-SERVING.md](../PEER-SERVING.md).

## Keys and secrets

| File | What | Who needs it |
|---|---|---|
| `publisher.seed` | the publisher's private key; anyone holding it can sign history clients accept | the publisher only; keep an offline backup |
| `publisher.pub` | the publisher's public key | every client, pinned |
| `spool.seed`, `STORAGE/keys/spool.seed` | signs a local spool | the machine that writes it |
| `STORAGE/keys/node.key` | the node's identity on the peer network | that node |
| `/etc/xrpla/publisher.env` | R2/S3 credentials | the publisher account only |
| `XRPLA_CQL_PASSWORD` | the Cassandra password for a Clio restore | the restore |

`xrpla keygen` creates a seed readable by its owner only and refuses to overwrite one.

## Reporting a vulnerability

Report vulnerabilities privately by email to **95x2t773@xrpla.anonaddy.com**, not in a public
issue. Include the version or commit, what you found, and how to reproduce it. We will
acknowledge the report, work on a fix, and agree a disclosure date with you; please keep the
issue private until a fixed release is out.

Integrity problems are the most serious: anything that lets a client accept history the
publisher did not sign, or a ledger that does not match the hash chain.
