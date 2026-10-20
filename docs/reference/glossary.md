# Glossary

Terms used across the XRPLA documentation, in the order you are likely to meet them.

**Publication.** A directory of signed, compressed XRP Ledger history that XRPLA clients
download: a Cloudflare R2 (or other S3-compatible) bucket prefix read over HTTPS, or a local
directory with the same layout (a *bucket directory*). Its address is the directory that holds
`catalog/current.json`. See [PUBLICATION.md](../PUBLICATION.md) for the layout.

**Publisher.** Whoever writes a publication. A publisher follows an xrpld node (or the peer
network), verifies every ledger, cuts history into pieces and bundles, signs the catalog and
uploads it. See [Run a publisher](../guides/publish.md).

**Publisher key.** The publisher's Ed25519 public key: a file holding 32 raw bytes or 64 hex
characters. Every client pins it and checks the catalog's signature against it. XRPLA never
trusts a key it finds inside the publication itself; get it from a source you trust. The
matching private key (the *seed*, `publisher.seed`) stays with the publisher.

**Mirror.** Another address serving a publication's exact objects. A client given several takes
the catalog from the one claiming the newest, everything else from the first that answers, and
moves to the next when one is out of reach, fails, or serves something that does not check out
against the publisher key; a mirror itself is trusted with nothing.

**Publication list.** The approved publications installed with XRPLA
(`/usr/share/xrpla/publications.toml`), each with its publisher key and mirrors, which
`xrpla setup` offers by name. See [The publication list](publications.md).

**Catalog.** The signed table of contents of a publication: `catalog/NNNNNNNN.json` (a
*generation*) and its `.sig`, listing every piece, bundle, fragment and snapshot with its size,
SHA-256 and ledger range. `catalog/current.json` points at the newest generation and is written
last, so readers only ever see a complete one.

**Interval.** A fixed, aligned block of 1,000,000 ledgers. Each interval starts with a state
snapshot.

**Piece.** 50,000 ledgers of history (twenty pieces per interval), compressed into one
container file. The interval being filled is published piece by piece.

**Bundle.** A completed interval: its twenty pieces joined into one 1,000,000-ledger container
made from the same compressed bytes (nothing is re-encoded).

**Fragment.** Part of the piece still filling, published early (for example every 5,000
ledgers, with smaller *tip fragments* of 250 ledgers after it) so a publication trails the
network by minutes or hours rather than up to a piece. Fragments are listed in the catalog's
*tail*.

**Segment.** The unit inside a piece or bundle that a reader fetches and checks on its own (one
ranged HTTP read). Ledgers inside a segment are grouped into 64-ledger *pages*.

**Snapshot.** The complete ledger state (every ledger object) at one ledger, the ledger before
an interval's first. A restore starts from a snapshot and applies history forward, so any
interval can be restored without earlier history.

**Piece cache.** A local copy of a publication's pieces, bundles and snapshots, kept compressed,
plus the local query indexes XRPLA builds from them. `xrpla sync` maintains it; `xrpla serve`
answers API requests straight from it. Mirror and standalone machines run on a piece cache.

**Query index.** Local files XRPLA derives from a piece cache (`xrpla index-pieces`, run by every
sync) so that `tx`, `account_tx`, state reads and the NFT and MPT methods do not scan the
compressed data. They are not published and can be rebuilt at any time.

**Spool.** A small signed local archive of the newest ledgers: those after the publication's
tip, fetched from an xrpld or from the peer network. The server answers from the piece cache
and, for ledgers after it, from the spool (the *live tail*). The spool restarts from each newer
snapshot the publication gains, so it stays about one interval long.

**Follower.** The process that keeps the spool current (`xrpla publish`, started by
`xrpla run`): it fetches each new validated ledger, verifies it and appends it to the spool.

**Store.** An XRPLA RocksDB database built by replaying history (`xrpla restore --to store` or
`xrpla build`). Faster for some queries than a piece cache, but much larger (about 1 KB per
transaction). The Clio restore builds one as an intermediate step.

**Validator list and quorum.** A list of trusted validators, signed by its publisher (for
mainnet, `https://vl.ripple.com`). A ledger is accepted as validated once validations from a
quorum of the list's validators (80%, as xrpld counts) name its hash.

**Profile.** What `xrpla run` does on a machine: `restore`, `mirror`, `connected`, `standalone`
or `publisher`. `xrpla setup` writes the profile and its settings to `/etc/xrpla/xrpla.toml`.

**Decode cache.** Memory XRPLA uses to keep decompressed history while serving (`[storage]
cache_mb`, `xrpla serve --cache-mb`).

**v2 archive.** The older signed archive format (snapshot plus chunks, `manifest.json`) used by
`fetch`, `verify`, `build`, `tail` and `export`. Spools use it; publications use pieces and
bundles. See [v2 archive tools](archive-v2.md).
