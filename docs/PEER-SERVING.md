# Peer serving: a standalone node on the XRPL peer network

The standalone profile follows the network from peers with no xrpld (docs/SETUP.md). By
default it only takes: it connects out to peers, asks for ledgers, and answers every request
for data with "don't have it" (the reply xrpld gives for data it lacks). With `[peers] serve =
true` it also serves, as a non-validating xrpld does: it accepts inbound connections and
answers requests for the newest ledgers it holds. Either way it relays validators'
validations and proposals. This document is the reference for the `[peers]` settings and
for how the node behaves toward peers; its last sections record the implementation plan.

## Settings

Off by default; everything below is in the standalone profile's `xrpla.toml`:

```toml
[peers]
# endpoints = ["r.ripple.com:51235"]   # peers kept connected (default: none; join through mainnet's hubs)
outbound = 10            # peers this node connects to itself
# max_peers = 21         # all peers at once, inbound and outbound (xrpld's [peers_max]); inbound get what outbound leave
private = false          # true: connect only to `endpoints`, never advertise this node, keep its address out of crawls
relay = true             # relay validators' validations and proposals, as xrpld does
relay_validations = "all"        # unlisted validators' validations: all | trusted | drop_untrusted (xrpld's default: all)
relay_proposals = "trusted"      # unlisted validators' proposals: trusted | all | drop_untrusted (xrpld's default: trusted)
squelch = true           # ask peers to stop sending a validator's messages beyond `squelch_sources` of them
squelch_sources = 5      # peers kept sending each validator's messages (xrpld's reduce-relay default)
serve = false            # true: serve ledger data; false: answer as a node with no ledgers (default)
ledgers = 256            # with serve: how many of the newest validated ledgers are served in full
listen = "0.0.0.0:51235" # with serve: accept inbound connections here (omit to connect out only)
max_inbound = 11         # with serve: inbound peers at once; a full node redirects others
ip_limit = 2             # with serve: inbound peers from one address (xrpld's [overlay] ip_limit)
crawl = false            # with serve: answer /crawl on the peer port and let crawlers see this node's address
reserved = []            # with serve: node keys (n9...) always admitted and never charged
```

`xrpla run` passes these to the follower (`xrpla publish --peers ... --peer-connections N
--peer-max N --peer-private --peer-no-relay --peer-relay-validations P --peer-relay-proposals P
--peer-no-squelch --peer-squelch-sources N --peer-serve --peer-ledgers N --peer-listen
HOST:PORT --peer-max-inbound N --peer-ip-limit N --peer-crawl --peer-reserved KEYS`).

### Compared with xrpld's settings

| xrpld | XRPLA `[peers]` | Notes |
|---|---|---|
| `[peers_max]` | `max_peers` | outbound first, inbound get the rest (or `max_inbound`, if smaller) |
| `[peers_out_max]`, `[peers_in_max]` | `outbound`, `max_inbound` | defaults 10 and 11, as xrpld's |
| `[ips]` | (built in) | mainnet's hubs, kept to one session once the network names other peers |
| `[ips_fixed]` | `endpoints` | kept connected, reconnected when dropped |
| `[peer_private]` | `private` | |
| `[peer_reservations]` | `reserved` | also never charged, as xrpld's cluster peers |
| `[overlay] ip_limit` | `ip_limit` | |
| `[relay_validations]`, `[relay_proposals]` | `relay_validations`, `relay_proposals` | same values and defaults |
| `[reduce_relay] vp_base_squelch_enable`, `vp_base_squelch_max_selected_peers` | `squelch`, `squelch_sources` | on by default here (off in xrpld) |
| `[port_peer]` | `listen` | |
| `[crawl]` | `crawl` | `overlay` and `server` only |
| `[network_id]` | `[network] id` | |
| `[compression]` | — | not offered (messages go uncompressed, as with xrpld's default) |
| `[reduce_relay] tx_*`, `[max_transactions]` | — | transactions are not relayed (below) |
| `[overlay] max_unknown_time`, `max_diverged_time` | — | peers that stop answering are dropped instead |
| `[cluster_nodes]`, `[node_seed]` | — | the node key is `STORAGE/keys/node.key` |

`ledgers` is the window: the newest N validated ledgers, each served in full (header,
transaction tree, state tree). The node announces exactly that range to its peers, so they
ask it only for what it has. Anything older is answered "don't have it".

## What is served

| Request | Served from | Notes |
|---|---|---|
| `TMGetLedger` `liBASE` (header, tree roots) | the spool's headers; roots of the kept trees | any ledger in the window |
| `TMGetLedger` `liTX_NODE` | the kept transaction tree, read from the follower's node store | as xrpld's `getNodeFat` answers |
| `TMGetLedger` `liAS_NODE` | the kept state tree, read from the follower's node store | as xrpld's `getNodeFat` answers |
| `TMGetObjectByHash` (ledger, state and transaction nodes; fetch packs) | headers, node store | nodes of ledgers in the window |
| `TMProofPathRequest`, `TMReplayDeltaRequest` | kept state and transaction trees | when the peer negotiated ledger replay |
| everything else (`liTS_CANDIDATE`, relayed transactions) | not served | "don't have it" / ignored |

This node never proposes or validates; it relays validators' messages (below). It announces
itself as `xrpla-<version>`.

## Relaying

Every standalone node (serving or not) relays validators' validations and proposals among
its peers, inbound and outbound, as a non-validating xrpld does, so a peer connected to it
loses no share of the network's consensus traffic (`relay = false` turns it off). As xrpld's
`PeerImp` checks them before relaying (and charges a peer that relays one failing them):

- **Validations**: well formed, signed by the key they name (checked once, by libxrpl), and
  current: signed from just under 3 minutes ago to just under 5 minutes ahead (xrpld's window
  is 3 and 5). Relayed per `relay_validations`: all (xrpld's default), trusted only, or
  unlisted validators' dropped unread. Only trusted ones count toward this node's quorum.
- **Proposals**: well formed (a secp256k1 key, a 64–72 byte signature, 32-byte hashes), a
  close time within 3 minutes, and signed (the signing hash over sequence, close time,
  previous ledger and position). Relayed per `relay_proposals`: trusted only (xrpld's
  default), all, or unlisted validators' dropped. This node takes no part in consensus, so a
  proposal it will not relay is not checked.
- **Once**: messages are keyed as xrpld keys them (the SHA-512 half of a validation; a
  proposal's unique ID over its fields and signature) for five minutes. A copy seen again is a
  duplicate, neither checked nor relayed, and nothing goes back to a peer that sent it.
- **Squelch**: both sides offer `vprr` in the handshake. A peer's `TMSquelch` (5 minutes to an
  hour, as xrpld takes it) stops this node relaying that validator's messages to it; any
  other duration, or `squelch: false`, lifts it. In turn, once a validator's messages arrive
  from more than `squelch_sources` peers, each further one is asked to stop sending them for 5
  to 10 minutes (xrpld's reduce-relay, which it leaves off by default); validators whose
  messages are not relayed or counted here are squelched on every peer. On mainnet each
  message arrives from about twenty peers, so this cuts what peers send this node.
- **Transactions are not relayed.** xrpld relays a transaction only after applying it to its
  open ledger, which this node does not keep: relaying unchecked ones would pass on invalid
  ones (and get this node charged). Peers' transaction relay is about 20-fold redundant.

The follower logs `peer_relay` every five minutes: per kind, messages received, duplicates,
invalid, stale, dropped, relayed, messages and bytes sent; squelches sent and received.

## Outbound peers

Configured `endpoints` are always kept connected. Otherwise the node joins through
mainnet's public hubs and then prefers peers the network names (`TMEndpoints`, redirects):
once some are known, at most one hub session stays, another giving way each minute, so
XRPLA nodes do not pile onto the hubs. A peer that fails is tried again after 5 seconds,
doubling to 5 minutes; so is one whose session lasted under a minute, or that this node
closed (after 3 ledger requests in a row went unanswered), so a peer that drops sessions
soon after the handshake is not dialled again and again. The delay resets after a session
that lasted and that the peer ended. With `private = true` the node uses only `endpoints`
(the hubs when none are given), never advertises itself and keeps its address out of
crawls, as xrpld's `peer_private`.

The network is mainnet unless `[network] id` says otherwise (`"testnet"`, `"devnet"` or another
network's ID). Every handshake, in and out, carries it and refuses a peer of another network.
For mainnet, testnet and devnet it also picks the public hubs the node joins through and the
validator list it trusts; another network needs `endpoints` and `[validators]` set. A
network's nodes keep dialling an address they once knew on it (after a node of that network
ran there): each such dial is refused once its handshake shows the network, and counted in
`handshake_failures` (`Refused: Peer is on network N`). A refusal costs one TLS handshake, about
a millisecond; the address is not blocked, as a node of this network may share it.

Every session that ends is logged as `peer_closed`: `direction` (`out` or `in`), endpoint,
server, `seconds` open, `reason` (the peer closed the connection, and how; or `Closed by
this node:` and why), and the messages and bytes `received` and `sent`. The five-minute
`peer_relay` line's `outbound` counts sessions opened (`connects`), ended ones by reason
(`closed`), those under a minute (`short_sessions`), failed connections by reason
(`dial_failures`, the endpoint left out) and ledger requests a peer left unanswered for 10
seconds (`unanswered`); `peer_serving` counts the same for inbound peers (`closed`,
`short_sessions`) and connections that never became sessions (`handshake_failures`: TLS,
the request, or a refused upgrade, by reason).
Transactions (`submit`), `fee`, `path_find` and `ledger_current` stay forwarded to an xrpld
(`[serve] forward`): run one alongside to submit.

## Behaving as xrpld peers expect

From xrpld 3.3.0 and 3.4.0's overlay (`src/xrpld/overlay/detail/`):

- **Protocol.** 3.3 speaks XRPL/2.1–2.2 and 3.4 speaks 2.2–2.3; this node speaks 2.2, so node
  references in replies are `nodeid` (33 bytes: key prefix, then depth). TLS 1.2, a
  self-signed certificate; the session is authenticated by both sides signing the TLS
  Finished messages, as for outbound sessions today.
- **Inbound handshake.** `GET` with `Connection: upgrade`, `Upgrade` listing `XRPL/2.2`
  (else 400 "Unable to agree on a protocol version"), `Connect-As: peer` (else 503), and the
  same header checks xrpld makes: `Network-ID` matching when present, `Network-Time` within
  20 s, a secp256k1 `Public-Key` that is not our own, a valid `Session-Signature`. The 101
  answer carries `Connection`, `Upgrade: XRPL/2.2`, `Connect-As`, `Server: xrpla-<version>`,
  `Crawl`, `X-Protocol-Ctl` (features both sides enable: `ledgerreplay=1` when serving),
  `Network-ID`, `Network-Time`, `Public-Key`, `Session-Signature`, `Instance-Cookie`, and
  `Closed-Ledger`/`Previous-Ledger` (uppercase hex). A full node answers 503 with
  `{"peer-ips": [...]}` (up to 10 known peers).
- **Staying connected.** Answer pings (xrpld pings every 60 s and drops a peer that misses
  one). On connections xrpld opened, it drops a peer still not "converged" after 10 minutes:
  converged means our `TMStatusChange` names a ledger within 24 of its validated one. So a
  status goes out on connect and after every new validated ledger: `neACCEPTED_LEDGER`, the
  ledger's sequence, hash and parent, network time, and `firstSeq`/`lastSeq` = the served
  window. xrpld then prefers this node for ledgers in that range (`hasLedger`).
- **Not having something.** xrpld never sends an error-only `TMLedgerData` and charges the
  sender of one (400): a request for a ledger not served gets no reply (earlier followers sent error
  replies; that was fixed first). `TMGetObjectByHash` is always answered, with
  the objects found.
- **Replies.** `liBASE`: the 118-byte header (no prefix), then the state and transaction tree
  roots in wire format. Node requests: `getNodeFat` per ID (fat leaves; depth 1 by default,
  at most 3), stopping at 8192 nodes; more than 12288 IDs is refused. Objects by hash: the
  node-store blob with its hash prefix (`LWR\0`, `MIN\0`, `MLN\0`, `SND\0`). Fetch packs:
  from the requested ledger backwards, each earlier ledger's header, the state nodes it has
  that its child lacks, and its transaction nodes, until 512 objects or one second. Ledger
  replay (only when negotiated; off by default in xrpld): proof paths and replay deltas.
- **Discovery.** Every 151 s a `TMEndpoints` (version 2) with this node's own listening
  endpoint (hops 0) while it has free inbound slots; endpoints peers relay become candidates
  for its own outbound connections.
- **HTTP on the peer port.** `GET /crawl` (when `crawl = true`): xrpld's layout — `overlay`
  (each peer's key, direction, uptime, version, and address when that peer is public) and
  `server` (`build_version`, `complete_ledgers` = the window, `server_state` = `tracking`, as
  it follows but cannot take part in consensus, `pubkey_node`, `peers`, `validated_ledger`).
  `GET /health` as xrpld answers it, except that the newest ledger may be older (a warning from
  20 s, critical from 60 s, against xrpld's 7 s and 20 s): this node follows validated ledgers
  a few seconds behind the network.
- **Budgets.** Each peer's requests are charged as xrpld charges them (a balance decaying by
  1/32 a second); a peer whose balance reaches 25,000 is disconnected. A node syncing from
  this one alone asks faster than that allows (xrpld charges 250 for each of its own ledger
  requests, which carry no cookie), as it would from a single xrpld: list such a node's key in
  `reserved` (xrpld's `[peer_reservations]` and cluster), which also admits it when the
  inbound slots are full.
- **Counts.** Every five minutes the follower logs `peer_serving` with its counts since it
  started: requests, answered, unanswered, nodes and bytes sent, peers dropped. Unanswered
  requests are also counted by kind (`unanswered_kinds`): `tx_set` (a consensus transaction
  set; this node takes no part in consensus), `transactions` (transaction relay),
  `ledger_newer` and `ledger_older` (outside the window; a newer one is usually the network's
  newest ledger before this node has it), `ledger_unknown` (asked for by hash alone, or not
  the ledger served at its sequence), `tree_nodes`, `fetch_pack`, `proof_path` and
  `replay_delta`. What reserved peers ask for is also counted apart (`reserved`, included in
  the totals), so the network's share is the rest. Of the ledgers other peers ask for by hash
  while not served (newer ones, or by hash alone), `unserved_ledgers` counts how many this
  node served afterwards, by how long after the first request (`0-5s`, `5-15s`, `15-60s`,
  `60s+`), how many it had not served five minutes on (`not_served`), and those still
  `pending`: requests for the network's newest ledger, which this node has a few seconds
  later, are served soon after; others are not.

## How it fits

- **One process holds the network side.** The follower (`xrpla publish --peers`) already
  holds the peer sessions, the verified state tree and the spool, and runs for a publication
  interval at a time (about 44 days on mainnet), so the overlay lives there: its listener,
  inbound and outbound sessions, and the request handlers. The server process (RPC/WS) is
  unchanged.
- **The state window.** The follower keeps its verifier open from batch to batch. The verifier
  stores each ledger's transaction tree beside its state tree, remembers both roots of the
  newest `ledgers` ledgers, and its node store's compaction keeps every node reachable from
  them (the newest state tree whole, then only what each earlier ledger changed). Only
  headers are held in memory. The serving threads read the same node store; a compaction swaps the store
  under a lock they share. After a restart the roots are taken again from the spool's newest
  headers.
- **Sessions.** Inbound sessions are accepted with the same TLS 1.2 upgrade the follower uses
  outbound (both sides sign the session with their node keys), from the server side. Every
  session, inbound or outbound, is served; validations and manifests from inbound peers
  count toward the quorum as outbound peers' do.

## Plan (implemented)

The steps this feature was built in, kept for reference.

1. **Server handshake and overlay.** Listener, server-side upgrade (checks and response
   headers as xrpld's), slots and redirects, status messages announcing the served range,
   pings, per-peer request budgets, endpoint exchange, `/crawl`.
2. **Serving.** Request handlers over a ledger source (headers, transaction trees, the kept
   state trees), with xrpld's reply limits; "don't have it" outside the window.
3. **State window.** Verifier kept across batches; roots of the window kept through
   compaction; readers across compaction; window restored after a restart.
4. **Settings and status.** `[peers]` settings through `xrpla run`; counts of peers,
   requests served and bytes in the follower's status output.
5. **Tests.**
   - In process: an XRPLA outbound session against an XRPLA listener; `PeerLedgers` (which
     checks every node against its parent's hash) fetches whole ledgers from the listener
     and they verify; requests outside the window get "don't have it"; limits hold.
   - On the OVH test server, against its mainnet xrpld 3.3: xrpld connects to XRPLA
     (its admin `connect` command) and keeps the connection; a second xrpld with only XRPLA as its peer
     acquires ledgers in the window by hash (`ledger_request`) and they match the network's;
     `/crawl` answers as xrpld's does. With relaying, that second xrpld follows the network
     through XRPLA alone: validations and proposals reach it only through XRPLA, and within
     minutes it validated ledgers a few behind the network, reached `full`, and kept the
     connection (before relaying it never validated, and every ten minutes dropped XRPLA as
     "Not useful": it could not tell that XRPLA tracks the network). Mainnet nodes that connect
     to XRPLA keep it.
   - A soak with inbound peers from mainnet.

## Out of scope

- Relaying transactions (see Relaying).
- Serving ledgers older than the window (xrpld nodes backfilling history fetch a ledger's
  state tree too; a publication restore does that job far faster).
- Native `submit`, `fee`, `path_find`, `ledger_current` (forwarded to an xrpld).
