# API reference: methods, streams and known differences

XRPLA serves the XRP Ledger's public JSON-RPC and WebSocket API over validated history. The same
methods are answered from a piece cache (mirror, standalone, connected) and from a RocksDB store
(`xrpla serve --dir STORE`). xrpld's methods answer as xrpld 3.4.1 does (XRPLA links libxrpl
3.4.1); Clio's methods follow Clio 2.7 (2.8 for the MPT account methods and `nft_history`).

XRPLA has no admin methods. Requests are answered for validated ledgers only; there is no open
ledger and no consensus.

## Methods

Methods follow the rippled 3.x handlers, including their limits, markers and error shapes:

| Area | Methods |
|---|---|
| Ledgers | `ledger`, `ledger_closed`, `ledger_range`, `ledger_data` (with Clio's `out_of_order`), `ledger_entry` (every selector of xrpld 3.4, and Clio's `include_deleted`), `book_changes` |
| Transactions | `tx` (with `ctid`), `transaction_entry`, `account_tx`, `nft_history` (Clio) |
| Accounts | `account_info`, `account_lines`, `account_objects`, `account_offers`, `account_channels`, `account_currencies`, `account_nfts`, `gateway_balances`, `noripple_check`, `deposit_authorized`, `account_mptokens` and `account_mptoken_issuances` (Clio) |
| DEX, AMM, NFT, MPT | `book_offers`, `amm_info`, `nft_buy_offers`, `nft_sell_offers`, `nft_info`, `nfts_by_issuer` and `mpt_holders` (Clio), `vault_info`, `get_aggregate_price` |
| Server | `server_info`, `version`, `ping`, `random`, `feature` (at any ledger, as Clio), `ledger_index` (Clio), `subscribe`, `unsubscribe` |
| XRPLA only | `xrpla_cache_stats`: the decode cache's use, what open segments hold, hit rates and time spent loading, and on Linux the allocator's own figures |

`mpt_holders` and `nfts_by_issuer` need their optional local indexes (`[index]` in
`xrpla.toml`, on by default); with an index turned off the method answers `notSupported`.

Methods whose names start with `_` are internal (used by XRPLA's stream parity tool) and are
answered only to clients on this machine's own addresses while `[serve] loopback_trusted` is
true; everyone else gets `unknownCmd`.

## Subscriptions

Over WebSocket, `subscribe` takes the `ledger` and `transactions` streams and `accounts`, as xrpld
does (in the request's `api_version`): each new ledger's `ledgerClosed` message, then its
transactions in TransactionIndex order, with `owner_funds` on offers. `books` delivers the
transactions that created, changed or removed an offer in a book (`both` adds the reverse book,
`domain` a permissioned domain's book), and with `snapshot` the answer carries the book's offers
now (`offers`, or `bids` and `asks` with `both`).

They are validated transactions only, so `transactions_proposed` and `accounts_proposed` are not
offered, nor are `server`, `validations` or `manifests`. Each ledger's messages are rendered once
and the same text is sent to every subscriber; a subscriber that falls 4,096 messages behind is
disconnected. Streams publish ledgers as the server gains them: every few seconds on standalone
and connected nodes, which follow the network.

## Forwarded methods

Current-ledger methods are forwarded to an upstream xrpld with `[serve] forward`
(`xrpla serve --forward`), as Clio forwards them: `submit`, `submit_multisigned`, `fee`,
`ripple_path_find`, `ledger_current`, `server_state`, `server_definitions`, `manifest`,
`channel_authorize`, `channel_verify`, `simulate`, and any request for the `current` ledger (not
`batch`, whose elements xrpld runs as requests of their own). Without an upstream they answer
`notSupported`.

- Signing fields and admin credentials are dropped, and a limit above the public maximum is
  lowered.
- Before anything is sent, the upstream is asked `get_counts`, which xrpld answers only to an
  admin. An upstream that answers it would run every client's request as an admin, so it is sent
  nothing (at start the server refuses to run). One that cannot be asked yet is sent nothing
  until it can (forwarded requests answer `notReady`). It is asked again every 5 minutes, and
  within 10 s after a forwarded request could not reach it. Use a public (non-admin) port of the
  upstream.
- Each request is sent once over HTTP JSON-RPC and never retried (a submission must not be
  repeated behind the client's back), on threads of its own so a slow upstream cannot hold up
  history queries. An upstream that does not answer gives `notReady`.
- A forwarded request carries the client's address as `X-Forwarded-For`: list the XRPLA
  machines in the upstream's `secure_gateway` and its rate limits apply per client rather than
  to XRPLA.
- `path_find` streams its updates, so xrpld serves it over WebSocket only: a WebSocket client's
  path searches are relayed to the upstream's WebSocket (`[serve] forward_ws`,
  `--forward-ws`); over HTTP it answers `noEvents`, as xrpld does. Ripple's public servers
  (s1, s2) turn `path_find` off.

The detailed probe rules are documented with `forward` in
[config/xrpla.example.toml](../../config/xrpla.example.toml).

## Known differences

### From xrpld

- `ping` does not report `role: admin` (xrpld reports it to admin connections only; XRPLA has
  none). In a comparison of 6,013 responses across 21 methods with a live xrpld 3.3.0, this was
  the only difference.
- `server_state`, `fee` and the other current-ledger methods are forwarded (above), so
  `server_state` describes the upstream; use `server_info` for XRPLA's own tip.
- A few 2013 transactions cannot be decoded by current xrpld (an XRP amount with a bit the MPT
  format now reads). As xrpld, `tx` answers `txnNotFound` for them and listings leave them out,
  while the archive and every restore keep them. `book_changes` for such a ledger reports the
  crossings of the transactions it can show, as Clio does, where xrpld fails the request
  (`internal`).
- Answers follow xrpld 3.4.0's changes: ledger listings carry the same synthetic metadata fields
  as `tx` (`delivered_amount` also for AccountDelete, `nftoken_id(s)`, `offer_id`,
  `mpt_issuance_id`); `account_info` always lists the DisallowIncoming and Clawback flags;
  `book_changes` keys a book by its asset pair and skips a crossing whose rate or volume cannot
  be represented; `vault_info` keeps each parse error's own code and message and answers
  `entryNotFound` for a zero key; `get_aggregate_price` counts a repeated oracle once.

### From Clio

- `ledger_data` with `out_of_order` walks the ledger's keys in order (Clio walks its newest
  ledger's keys, then pages later ledgers' deletions): its marker is the last key returned,
  which XRPLA also accepts back with `out_of_order` (Clio 2.7 refuses it), and no marker is left
  once the walk is done. An integer marker D returns, as Clio does, the objects ledger D deleted
  that exist at the requested ledger, then D - 1.
- `ledger_entry` `include_deleted` finds a deletion within the cached history (a Clio with longer
  history can find older ones).
- `mpt_holders` lists every holder the history holds; Clio's own ingestion records only holders
  created by MPTokenAuthorize. Its pages, like those of `nfts_by_issuer`, depend on the
  candidates each server has seen, so page boundaries can differ while the concatenated answer
  is the same.
- `feature` lists every amendment libxrpl 3.4 knows, `supported` meaning this build implements
  it, with xrpld's `majority` time where the ledger records one.
- `nft_info` answers from the token's newest recorded state at or before the requested ledger
  (owner, URI and burned flag); its `ledger_index` is the ledger where that state began (the
  snapshot ledger for a token unchanged since), moved only by a change of owner or burned flag,
  as in Clio 2.8.

## Evidence

- Compared with stock xrpld 3.4.1 over four 100,000-ledger stretches from 2015 to 2024: 53,665
  answers, 0 different ([NOTES.md](../benchmarks/2026-10-audit/NOTES.md)).
- A parity campaign across all of mainnet's history: 108 intervals, 120,617 answers the same,
  0 different (`docs/benchmarks/2026-10-audit/parity-campaign/`).
- How the comparisons are run: [internal/PARITY-PLAN.md](../internal/PARITY-PLAN.md).
