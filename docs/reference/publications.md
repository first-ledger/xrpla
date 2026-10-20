# The publication list

XRPLA ships a list of approved publications, `config/publications.toml`, installed as
`/usr/share/xrpla/publications.toml` (under the install prefix). `xrpla setup` offers its
publications by name, a configuration can name one (`[publication] name = "xrpla-mainnet"`), and
`sync`, `restore` and `rotate-spool` take one with `--publication NAME`. Each entry gives the
publication's publisher key and its mirrors, so users type neither, and a configuration that
names a publication takes the mirrors of whatever list the installed package carries: an
upgrade that adds or replaces a mirror applies on the next start.

`xrpla publications` prints the installed list as JSON: each entry's name, network, first
ledger, mirrors and key, and any entry left out as invalid, with why. The environment variable
`XRPLA_PUBLICATIONS` names another list file (for tests, or a list of your own). The service
`xrpla setup` installs does not set it: `xrpla setup` warns when it is set, as the service then
reads the installed list, not that file.

## Trust model

What is trusted is the publisher key, and only it. Everything a client downloads is checked
against it (see [Security model](security.md)): the catalog's Ed25519 signature, every object's
SHA-256 against the signed catalog, every ledger's state and transaction roots and its parent
hash, and each range's last ledger hash against the catalog. A catalog generation older than
the one a piece cache already holds is refused.

So a mirror is trusted with nothing. A bad mirror can withhold the publication, fail, serve
bytes that do not check out, or serve an older (once validly signed) copy of it; in each case
the client logs `mirror_failed` and moves on to the next mirror in the list.

Which mirror is asked first:

- **The catalog.** Every mirror is asked which catalog generation it holds (its
  `catalog/current.json`, a few hundred bytes), and the catalog is taken from the mirror claiming
  the newest, checked as always; one that fails the checks is passed over for the next highest.
  A claim only orders the mirrors, so a lagging mirror is not followed while another is newer.
  A sync refuses a generation older than the one its piece cache holds; when that is all every
  mirror offers, the cache keeps its own (`catalog_refused_older`), and when another mirror
  failed instead, that failure is reported. A restore takes the newest catalog that holds the
  ledgers asked for.
- **Everything else** is checked against that catalog, so it comes from any mirror: the first
  in the list's order that is not resting.
- **A mirror that fails rests**, for the whole process: 30 s after its first failure in a row,
  twice as long after each next one, up to an hour. Meanwhile it is asked only after the others,
  so a mirror that is down costs a running service its few seconds once, not at every sync. An
  answer ends its rest.
- Before the last mirror left, a mirror is given 5 s to connect and a transient failure is tried
  again twice; the last is given the full retries (about 4 minutes) a single source is.

What a reviewer of the list vouches for is therefore that each key belongs to the publisher it
names. An entry with a wrong key would make clients accept that key's signatures: this is what a
review must rule out.

## Format

One `[[publication]]` table per publication:

```toml
[[publication]]
name = "example-mainnet"
network = "mainnet"
publisher_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
mirrors = ["https://history.example.com/mainnet", "https://mirror.example.org/xrpl/mainnet"]
first_ledger = 32570
description = "Full XRP Ledger mainnet history, published by Example"
homepage = "https://example.com/xrpl-history"
contact = "history@example.com"
```

| Field | Required | Value |
|---|---|---|
| `name` | yes | a short id, lowercase letters, digits and `-`; unique in the list (`other` and `none` are kept for `xrpla setup`). What a configuration names. |
| `network` | yes | `mainnet`, `testnet` or `devnet`. `xrpla setup` offers the entries of the network chosen, and writes `[network] id` for testnet and devnet. |
| `publisher_key` | yes | the publisher's Ed25519 public key, 64 hexadecimal characters |
| `mirrors` | yes | HTTPS base URLs, each the directory holding the publication's `catalog/current.json`, tried in order. No credentials, query or fragment. |
| `first_ledger` | no | the first ledger the publication holds; shown, not checked |
| `description` | no | one line on what the publication is |
| `homepage` | no | where the publisher publishes the key and describes the publication |
| `contact` | no | how to reach the publisher |

An entry that breaks these rules is left out (`xrpla publications` names it and why), and a
configuration naming it fails with that reason; the other entries are unaffected. A file that is
not valid TOML is an error.

### In a configuration

- `name` alone: the listed key and mirrors.
- `name` with `source`: `source` (one URL or directory, or an array of them) replaces the
  listed mirrors, for a mirror of your own; it is still checked against the listed key.
- `name` with `publisher_key`: allowed only when it is the listed key; any other is refused,
  since mixing one publication's name with another's key is almost certainly a mistake.
- `source` and `publisher_key` without `name`: a publication not in the list, as before.
  `source` may be an array of mirrors here too.

The command line is the same with `--publication`, `--source` and `--publisher-key`.

## Proposing a publication

A publisher who wants their publication offered opens a pull request adding one
`[[publication]]` entry to `config/publications.toml`. Before proposing:

1. **Publish the key out of band.** Put the publisher key where users and reviewers can find it
   independently of the bucket and of the pull request: your own website (the entry's
   `homepage`), signed release notes, or another channel already tied to you.
2. **Serve exact copies.** Every mirror must serve the publication's exact objects, with
   `catalog/current.json` at its base, over HTTPS with range requests and without redirects
   (XRPLA does not follow them). A mirror may lag a little: a sync passes over one whose
   catalog is older than the piece cache it updates.
3. **Pick a new name**: short, and naming the publisher and network (`example-mainnet`).

What reviewers check:

- The key in the entry is the key published at the homepage (or the other channel given), and
  that channel plausibly belongs to the publisher named.
- Every mirror serves a catalog signed by that key: `xrpla sync --publication NAME --dir
  /tmp/check --range A-B` with the entry in a test list (`XRPLA_PUBLICATIONS=FILE`), once for
  each mirror alone (`--source MIRROR`), succeeds; and the mirrors serve the same catalog
  generation, or one is only briefly behind.
- `first_ledger` and `network` match what the catalog holds.
- The contact answers.

A key that changes is a new entry (or a reviewed change of the entry, with the new key published
as the old one was). A client that names the publication pins whatever key the list it runs
with gives, so a package upgrade that changes an entry's key re-pins it without asking. A piece
cache synced under the old key keeps refusing catalog generations below the one it holds: if the
new key's publication starts its generations again, remove `CACHE/selection.json` (the next sync
selects afresh, keeping the objects that still match) to follow it.
