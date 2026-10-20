#!/usr/bin/env python3
"""Check XRPLA's derived indexes against the ledger state and transactions they derive from.

Each ledger's state objects hash into its state root and its transactions (with metadata) into
its transaction root, so `ledger`, `ledger_entry`, `ledger_data` and `account_nfts` answer from
verified records. The indexes built from those records are not covered by the hashes: account_tx
and nft_history postings, transaction-by-hash locations, NFT state (nft_info, nfts_by_issuer),
mpt_holders, account_mptokens and the order book directories book_offers walks. This tool samples
ledgers of one XRPLA server and checks, per ledger L:

  account_tx      every account xrpld's TxMeta::getAffectedAccounts finds in a transaction's
                  metadata lists that transaction in account_tx at L, and nothing else in L (also
                  asked for accounts no transaction of L touches); listings equal the ledger's
  tx              tx(hash) gives the ledger listing's ledger, TransactionIndex, tx_json and meta
  nft_info        tokens touched in L, and some of their issuers' others: nft_info at L agrees
                  with the owner's NFToken pages (present iff not burned, same URI and ID fields)
                  and with the state the ledger's NFTokenPage changes leave
  nfts_by_issuer  each entry is nft_info's answer at L, in (taxon, ID) order, and lists every
                  touched token of the issuer nft_info knows at L
  nft_history     at L it lists every transaction Clio 2.8's ETL (etl/NFTHelpers.cpp) records
                  for the token, and nothing beyond those that name it or change its page entry
  mpt_holders     each holder is the issuance's MPToken in state (ledger_entry), holders the
                  ledger's MPToken changes leave are listed and deleted ones not; with
                  --scan-state, exactly the MPTokens ledger_data finds
  account_mptokens  each holder's listing holds the issuance with the MPToken's amount and flags
  book_offers     the offers, in order, that xrpld's getBookPage walk over the book's directories
                  in state yields (directories found by successor through ledger_data), each the
                  Offer in state, its quality its directory's

Prints one JSON line per invariant with checked and violated counts and the first violations,
each with the exact requests that show it, then a summary; exits 1 on any violation or failed
request. A check that needs a listing longer than --page-cap pages (an account holding hundreds of
thousands of NFTs) is counted as skipped, neither passed nor violated. No reference server is involved, so whole ranges can be swept at the node's own pace.

  xrpla_invariants.py --xrpla http://127.0.0.1:5015 [--range 80000000-80001000] [--ledgers 16]
                      [--seed 1] [--in-flight 8] [--scan-state]
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import random
import sys
import threading
import time
import urllib.request

TIMEOUT = 300
# account_mptokens entries checked one by one against state (without --scan-state) per holder.
MPT_STATE_CHECKS = 32
ALPHABET = 'rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz'
# Every STAccount field xrpld 3.4.1 defines (include/xrpl/protocol/detail/sfields.macro, type
# ACCOUNT): TxMeta::getAffectedAccounts adds each one it finds at the top of a node's fields.
ACCOUNT_FIELDS = {'Account', 'Owner', 'Destination', 'Issuer', 'Authorize', 'Unauthorize', 'RegularKey', 'NFTokenMinter',
                  'EmitCallback', 'Holder', 'Delegate', 'OtherChainSource', 'OtherChainDestination', 'AttestationSignerAccount',
                  'AttestationRewardAccount', 'LockingChainDoor', 'IssuingChainDoor', 'Subject', 'Borrower', 'Counterparty',
                  'Sponsor', 'HighSponsor', 'LowSponsor', 'CounterpartySponsor', 'Sponsee'}
# The amounts whose issuer it adds (when not zero), and MPTokenIssuanceID's issuer.
ISSUER_AMOUNTS = {'LowLimit', 'HighLimit', 'TakerPays', 'TakerGets'}
ZERO_ACCOUNT = 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
# Fields a listing adds to tx_json around the transaction itself (account_tx and tx add the
# ledger, close time and compact ID; api_version 1 the hash): left out of comparisons.
ANNOTATIONS = {'ledger_index', 'date', 'ctid', 'inLedger', 'hash'}
NFT_INFO_FIELDS = ('nft_id', 'ledger_index', 'owner', 'is_burned', 'uri', 'flags', 'transfer_fee', 'issuer', 'nft_taxon', 'nft_serial')
INVARIANTS = ('account_tx', 'tx', 'nft_info', 'nfts_by_issuer', 'nft_history', 'mpt_holders', 'account_mptokens', 'book_offers')


def address(account_id):
    """A classic address (XRPL base58check) for 20 AccountID bytes."""
    body = b'\x00' + account_id
    data = body + hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    n, text = int.from_bytes(data, 'big'), ''
    while n: n, rest = divmod(n, 58); text = ALPHABET[rest] + text
    return ALPHABET[0] * (len(data) - len(data.lstrip(b'\x00'))) + text


def account_id(text):
    """The 20 AccountID bytes of a classic address."""
    n = 0
    for c in text: n = n * 58 + ALPHABET.index(c)
    data = n.to_bytes(25, 'big')
    return data[1:21]


def mpt_issuer(issuance_id):
    """The issuer an MPTokenIssuanceID names (its last 20 of 24 bytes), None when zero."""
    raw = bytes.fromhex(issuance_id)[4:24]
    return address(raw) if any(raw) else None


def affected_accounts(meta):
    """The accounts xrpld indexes a transaction under: TxMeta::getAffectedAccounts
    (src/libxrpl/protocol/TxMeta.cpp, 3.4.1) on the JSON metadata. For each affected node it reads
    NewFields (a CreatedNode) or FinalFields (otherwise; a node without them adds nothing), and
    of their top-level fields takes every STAccount, the issuer of LowLimit, HighLimit,
    TakerPays and TakerGets when not zero (an MPT amount's is its issuance's issuer), and the
    issuer of MPTokenIssuanceID. PreviousFields and nested objects are not read."""
    found = set()
    for wrapper in meta.get('AffectedNodes', []):
        for kind, node in wrapper.items():
            fields = node.get('NewFields' if kind == 'CreatedNode' else 'FinalFields')
            if not isinstance(fields, dict): continue
            for name, value in fields.items():
                if name in ACCOUNT_FIELDS:
                    if isinstance(value, str) and value: found.add(value)
                elif name in ISSUER_AMOUNTS and isinstance(value, dict):
                    if 'mpt_issuance_id' in value:
                        issuer = mpt_issuer(value['mpt_issuance_id'])
                        if issuer: found.add(issuer)
                    elif value.get('issuer') and value['issuer'] != ZERO_ACCOUNT: found.add(value['issuer'])
                elif name == 'MPTokenIssuanceID' and isinstance(value, str):
                    issuer = mpt_issuer(value)
                    if issuer: found.add(issuer)
    return found


def nft_fields(nft_id):
    """What an NFTokenID encodes (xrpld's nft::getFlags, getTransferFee, getIssuer, getTaxon with
    its ciphered taxon, getSequence), as nft_info names them."""
    raw = bytes.fromhex(nft_id)
    serial = int.from_bytes(raw[28:32], 'big')
    taxon = int.from_bytes(raw[24:28], 'big') ^ ((384160001 * serial + 2459) & 0xFFFFFFFF)
    return {'flags': int.from_bytes(raw[0:2], 'big'), 'transfer_fee': int.from_bytes(raw[2:4], 'big'),
            'issuer': address(raw[4:24]), 'nft_taxon': taxon, 'nft_serial': serial}


def page_owner(key):
    """The account an NFTokenPage belongs to: the first 20 bytes of its key."""
    return address(bytes.fromhex(key)[:20])


def page_tokens(fields):
    """{NFTokenID: URI} of an NFTokens array (URI '' when absent), None when the field is not there."""
    if not isinstance(fields, dict) or 'NFTokens' not in fields: return None
    return {t['NFToken']['NFTokenID'].upper(): t['NFToken'].get('URI', '').upper() for t in fields['NFTokens']}


def page_changes(meta):
    """The NFTokenPage contents a transaction's metadata shows: (owner, before, after) per page,
    each {NFTokenID: URI}. A page created has nothing before; one deleted nothing after (its
    FinalFields, or PreviousFields when changed first, are before); one modified without
    PreviousFields.NFTokens holds the same tokens after as before."""
    for wrapper in meta.get('AffectedNodes', []):
        for kind, node in wrapper.items():
            if node.get('LedgerEntryType') != 'NFTokenPage': continue
            owner = page_owner(node['LedgerIndex'])
            if kind == 'CreatedNode': yield owner, {}, page_tokens(node.get('NewFields')) or {}
            elif kind == 'DeletedNode':
                yield owner, page_tokens(node.get('PreviousFields')) or page_tokens(node.get('FinalFields')) or {}, {}
            else:
                after = page_tokens(node.get('FinalFields')) or {}
                before = page_tokens(node.get('PreviousFields'))
                yield owner, after if before is None else before, after


def changed_tokens(meta):
    """Tokens a transaction added to pages, removed from them, or changed in them (another
    owner's page, another URI); a page split leaves its tokens unchanged."""
    before, after = {}, {}
    for owner, b, a in page_changes(meta):
        before.update((t, (owner, u)) for t, u in b.items()); after.update((t, (owner, u)) for t, u in a.items())
    return {t for t in before.keys() | after.keys() if before.get(t) != after.get(t)}


def named_tokens(value, inside_page=False):
    """NFTokenIDs a transaction or its metadata names, outside NFToken pages' NFTokens arrays."""
    found = set()
    if isinstance(value, dict):
        for k, v in value.items():
            if k == 'NFTokenID' and isinstance(v, str) and not inside_page: found.add(v.upper())
            elif isinstance(v, (dict, list)): found |= named_tokens(v, inside_page or k == 'NFTokens')
    elif isinstance(value, list):
        for v in value: found |= named_tokens(v, inside_page)
    return found


def node_fields(node):
    kind, body = next(iter(node.items()))
    return kind, body, body.get('NewFields' if kind == 'CreatedNode' else 'FinalFields') or {}


def clio_nft_tokens(tx, meta):
    """The tokens Clio 2.8's ETL records a transaction under in nft_history (getNFTDataFromTx in
    src/etl/NFTHelpers.cpp): none unless tesSUCCESS; NFTokenMint the token its page changes add
    (all NFTokenIDs after minus all before, over pages created or modified with
    PreviousFields.NFTokens); NFTokenBurn, NFTokenCreateOffer and NFTokenModify the transaction's
    NFTokenID; NFTokenAcceptOffer the accepted buy offer's NFTokenID (the sell offer's without a
    buy offer); NFTokenCancelOffer every affected NFTokenOffer's."""
    if meta.get('TransactionResult') != 'tesSUCCESS': return set()
    kind = tx.get('TransactionType')
    if kind == 'NFTokenMint':
        before, after = Counter(), Counter()
        for wrapper in meta.get('AffectedNodes', []):
            for node_kind, node in wrapper.items():
                if node.get('LedgerEntryType') != 'NFTokenPage': continue
                if node_kind == 'CreatedNode': after.update((page_tokens(node.get('NewFields')) or {}).keys())
                elif node_kind == 'ModifiedNode' and page_tokens(node.get('PreviousFields')) is not None:
                    before.update(page_tokens(node['PreviousFields']).keys()); after.update((page_tokens(node.get('FinalFields')) or {}).keys())
        return set((after - before).keys())
    if kind in ('NFTokenBurn', 'NFTokenCreateOffer', 'NFTokenModify'): return {tx['NFTokenID'].upper()}
    if kind == 'NFTokenAcceptOffer':
        offer = tx.get('NFTokenBuyOffer') or tx.get('NFTokenSellOffer')
        for node in meta.get('AffectedNodes', []):
            _, body, _ = node_fields(node)
            if body.get('LedgerIndex', '').upper() == offer.upper(): return {body['FinalFields']['NFTokenID'].upper()}
        return set()
    if kind == 'NFTokenCancelOffer':
        found = set()
        for node in meta.get('AffectedNodes', []):
            _, body, _ = node_fields(node)
            if body.get('LedgerEntryType') == 'NFTokenOffer': found.add(body['FinalFields']['NFTokenID'].upper())
        return found
    return set()


def final_nft_states(transactions):
    """{NFTokenID: (owner, burned, URI)} the ledger's NFToken page changes leave, transactions
    taken in TransactionIndex order: a token in a page after a transaction is held by that page's
    owner; one only in pages before it was burned (the owner it left)."""
    states = {}
    for t in transactions:
        held, left = {}, {}
        for owner, before, after in page_changes(t['meta']):
            for token, uri in after.items(): held[token] = (owner, False, uri)
            for token, uri in before.items(): left.setdefault(token, (owner, True, uri))
        for token, state in left.items():
            if token not in held: states[token] = state
        states.update(held)
    return states


def exchange_rate(directory):
    """The quality a book directory's key ends with (getQuality), as a float."""
    q = int(directory[-16:], 16)
    return (q & ((1 << 56) - 1)) * 10.0 ** ((q >> 56) - 100)


def dir_page(root, index):
    """keylet::page: a directory's page `index` (0 is the root), sha512Half of the DIR_NODE space
    ('d'), the root and the index."""
    if not index: return root.upper()
    return hashlib.sha512(b'\x00d' + bytes.fromhex(root) + index.to_bytes(8, 'big')).digest()[:32].hex().upper()


def ctid_parts(ctid):
    value = int(ctid, 16)
    return (value >> 32) & 0x0FFFFFFF, (value >> 16) & 0xFFFF


def strip(tx):
    return {k: v for k, v in tx.items() if k not in ANNOTATIONS} if isinstance(tx, dict) else tx


def entry_hash(entry):
    if 'hash' in entry: return entry['hash'].upper()
    return (entry.get('tx_json') or entry.get('tx') or {}).get('hash', '').upper()


def issue(amount):
    if isinstance(amount, str): return {'currency': 'XRP'}
    return {'currency': amount['currency'], 'issuer': amount['issuer']} if 'issuer' in amount else None


class Client:
    """JSON-RPC to one server, at most `in_flight` requests at once."""
    def __init__(self, url, in_flight):
        self.url, self.gate, self.lock = url, threading.BoundedSemaphore(in_flight), threading.Lock()
        self.requests, self.seconds = 0, 0.0

    def call(self, method, params):
        body = json.dumps({'method': method, 'params': [params]}).encode()
        request = urllib.request.Request(self.url, body, {'Content-Type': 'application/json', 'User-Agent': 'xrpla-invariants'})
        with self.gate:
            started = time.time()
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response: result = json.loads(response.read())['result']
        with self.lock: self.requests += 1; self.seconds += time.time() - started
        return result

    def pages(self, method, params, key, cap):
        """Every entry of a paged answer (following markers, at most `cap` pages), the requests
        made, and the first error answer (None when there was none)."""
        entries, asked, marker = [], [], None
        for _ in range(cap):
            p = {**params, **({'marker': marker} if marker is not None else {})}
            asked.append({'method': method, 'params': p})
            result = self.call(method, p)
            if 'error' in result: return entries, asked, result
            entries += result.get(key, [])
            marker = result.get('marker')
            if marker is None: return entries, asked, None
        return entries, asked, {'error': 'truncated', 'error_message': f'more than {cap} pages'}


def truncated(error):
    """Whether a pages() error is only the page cap reached, not an error answer."""
    return bool(error) and error.get('error') == 'truncated'


class Invariants:
    def __init__(self, a):
        self.a, self.client = a, Client(a.xrpla, a.in_flight)
        self.pool = ThreadPoolExecutor(max_workers=a.in_flight)
        self.lock = threading.Lock()
        self.stats = {name: {'checked': 0, 'violated': 0, 'errors': 0, 'skipped': 0, 'examples': []} for name in INVARIANTS}
        self.probes = []  # accounts seen in earlier ledgers: account_tx asked about them where nothing touched them

    def ok(self, name, n=1):
        with self.lock: self.stats[name]['checked'] += n

    def bad(self, name, ledger, requests, detail):
        with self.lock:
            s = self.stats[name]; s['checked'] += 1; s['violated'] += 1
            if len(s['examples']) < self.a.examples: s['examples'].append({'ledger': ledger, 'requests': requests, 'detail': detail})

    def skip(self, name):
        """A check left undecided: the listing it needs is longer than --page-cap pages, or what
        it asks about cannot be told apart from a token that never existed."""
        with self.lock: self.stats[name]['skipped'] += 1

    def failed(self, name, ledger, requests, error):
        with self.lock:
            s = self.stats[name]; s['errors'] += 1
            if len(s['examples']) < self.a.examples: s['examples'].append({'ledger': ledger, 'requests': requests, 'error': str(error)})

    def each(self, name, ledger, function, items):
        """function(item) for every item on the pool; an exception is that check's error."""
        def run(item):
            try: function(item)
            except Exception as error: self.failed(name, ledger, [{'item': item}], f'{type(error).__name__}: {error}')
        list(self.pool.map(run, items))

    def ask(self, method, params):
        return self.client.call(method, params), [{'method': method, 'params': params}]

    def sample(self, rng, items, n):
        items = sorted(items)
        return items if n <= 0 or len(items) <= n else sorted(rng.sample(items, n))

    # account_tx -------------------------------------------------------------------------------
    def check_account_tx(self, seq, txs, rng):
        expected = defaultdict(set)
        for t in txs:
            for account in affected_accounts(t['meta']): expected[account].add(t['hash'])
        by_hash = {t['hash']: t for t in txs}
        accounts = self.sample(rng, expected, self.a.accounts)
        probes = [a for a in self.probes if a not in expected][:self.a.probes]
        with self.lock: self.probes = (sorted(expected) + self.probes)[:256]

        def check(account):
            params = {'account': account, 'ledger_index_min': seq, 'ledger_index_max': seq, 'limit': 400, 'api_version': 2}
            entries, asked, error = self.client.pages('account_tx', params, 'transactions', self.a.page_cap)
            if error and not (account in probes and error.get('error') == 'actNotFound'):
                return self.bad('account_tx', seq, asked, f"error {error.get('error')}: {error.get('error_message', '')}")
            got = [entry_hash(e) for e in entries]
            want = expected.get(account, set())
            problems = []
            if len(got) != len(set(got)): problems.append(f'listed twice: {sorted(h for h, n in Counter(got).items() if n > 1)[:4]}')
            if want - set(got): problems.append(f'missing: {sorted(want - set(got))[:4]}')
            if set(got) - want: problems.append(f'not affecting the account in {seq}: {sorted(set(got) - want)[:4]}')
            for e in entries:
                t = by_hash.get(entry_hash(e))
                if e.get('ledger_index') != seq: problems.append(f'{entry_hash(e)} listed at ledger {e.get("ledger_index")}')
                elif t and (strip(e.get('tx_json')) != strip(t['tx']) or e.get('meta') != t['meta']):
                    problems.append(f'{entry_hash(e)}: tx_json or meta differ from the ledger listing')
            if problems: return self.bad('account_tx', seq, asked, '; '.join(problems[:6]))
            self.ok('account_tx')
        self.each('account_tx', seq, check, accounts + probes)

    # tx -----------------------------------------------------------------------------------------
    def check_tx(self, seq, ledger, txs, rng):
        def check(t):
            result, asked = self.ask('tx', {'transaction': t['hash'], 'api_version': 2})
            problems = []
            if 'error' in result: problems.append(f"error {result['error']}")
            else:
                if result.get('ledger_index') != seq: problems.append(f"ledger_index {result.get('ledger_index')}, listed in {seq}")
                if entry_hash(result) != t['hash']: problems.append(f'hash {entry_hash(result)}')
                if 'ledger_hash' in result and result['ledger_hash'].upper() != ledger['ledger_hash'].upper(): problems.append(f"ledger_hash {result['ledger_hash']}")
                if result.get('meta', {}).get('TransactionIndex') != t['meta'].get('TransactionIndex'):
                    problems.append(f"TransactionIndex {result.get('meta', {}).get('TransactionIndex')}, listed {t['meta'].get('TransactionIndex')}")
                elif result.get('meta') != t['meta']: problems.append('meta differs from the ledger listing')
                if strip(result.get('tx_json')) != strip(t['tx']): problems.append('tx_json differs from the ledger listing')
                if 'ctid' in result and ctid_parts(result['ctid']) != (seq, t['meta'].get('TransactionIndex')):
                    problems.append(f"ctid {result['ctid']} names {ctid_parts(result['ctid'])}")
            if problems: return self.bad('tx', seq, asked, '; '.join(problems))
            self.ok('tx')
        self.each('tx', seq, check, [txs[i] for i in self.sample(rng, range(len(txs)), self.a.txs)])

    # NFTs -----------------------------------------------------------------------------------------
    def check_nfts(self, seq, txs, rng):
        touched, required, allowed = set(), defaultdict(set), defaultdict(set)
        for t in txs:
            names = named_tokens(t['tx']) | named_tokens(t['meta']) | changed_tokens(t['meta'])
            synthetic = {t['meta'][k].upper() for k in ('nftoken_id',) if isinstance(t['meta'].get(k), str)}
            synthetic |= {i.upper() for i in t['meta'].get('nftoken_ids', []) if isinstance(i, str)}
            touched |= names | synthetic
            for token in clio_nft_tokens(t['tx'], t['meta']): required[token].add(t['hash'])
            for token in names: allowed[token].add(t['hash'])
        for token, hashes in required.items(): allowed[token] |= hashes
        states = final_nft_states(txs)
        tokens = set(self.sample(rng, touched, self.a.nfts))
        issuers = self.sample(rng, {nft_fields(t)['issuer'] for t in tokens}, self.a.issuers)

        listings = {}
        def by_issuer(issuer):
            listings[issuer] = self.client.pages('nfts_by_issuer', {'issuer': issuer, 'ledger_index': seq, 'limit': 100}, 'nfts', self.a.page_cap)
        self.each('nfts_by_issuer', seq, by_issuer, issuers)
        # Some of each issuer's other tokens, checked the same way (their nft_history at L as well:
        # a token nothing in L names must list nothing there).
        for issuer, (entries, _, _) in listings.items():
            others = [e['nft_id'].upper() for e in entries if e['nft_id'].upper() not in tokens]
            tokens |= set(self.sample(rng, others, self.a.issuer_sample))

        infos = {}
        def info(token): infos[token] = self.ask('nft_info', {'nft_id': token, 'ledger_index': seq})
        self.each('nft_info', seq, info, sorted(tokens))
        owners = {r['owner'] for r, _ in infos.values() if 'owner' in r} | {s[0] for s in states.values()}
        holdings = {}
        def holding(owner):
            entries, asked, error = self.client.pages('account_nfts', {'account': owner, 'ledger_index': seq, 'limit': 400}, 'account_nfts', self.a.page_cap)
            # An owner no longer in state (a burned token's, the account deleted) holds nothing.
            if error and error.get('error') == 'actNotFound': error = None
            holdings[owner] = ({e['NFTokenID'].upper(): e for e in entries}, asked, error)
        self.each('nft_info', seq, holding, sorted(owners))

        for token in sorted(tokens):
            if token not in infos: continue
            result, asked = infos[token]
            # Known to exist: its page changes, an issuer listing, or a successful NFToken
            # transaction Clio records for it (an offer's creation or cancellation changes no page).
            known = token in states or bool(required[token]) or any(token in {e['nft_id'].upper() for e in entries} for entries, _, _ in listings.values())
            if 'error' in result:
                if result['error'] == 'objectNotFound' and not known: self.skip('nft_info'); continue  # named, never minted by L
                self.bad('nft_info', seq, asked, f"error {result['error']}: {result.get('error_message', '')}"); continue
            problems = []
            for k, v in nft_fields(token).items():
                if result.get(k) != v: problems.append(f'{k} {result.get(k)!r}, the ID encodes {v!r}')
            if result.get('nft_id', '').upper() != token: problems.append(f"nft_id {result.get('nft_id')}")
            if not isinstance(result.get('ledger_index'), int) or result['ledger_index'] > seq: problems.append(f"ledger_index {result.get('ledger_index')} after {seq}")
            held, held_asked, error = holdings.get(result.get('owner'), ({}, [], {'error': 'no owner'}))
            if truncated(error): pass  # the owner's pages are too many to follow: membership is undecided
            elif error: problems.append(f"account_nfts of the owner: {error.get('error')}")
            elif result.get('is_burned'):
                if token in held: problems.append(f"burned, but in {result['owner']}'s NFToken pages")
            elif token not in held: problems.append(f"not burned, but not in owner {result.get('owner')}'s NFToken pages")
            else:
                page = held[token]
                if page.get('URI', '').upper() != result.get('uri', '').upper(): problems.append(f"uri {result.get('uri')!r}, the page holds {page.get('URI', '')!r}")
                for ours, theirs in (('flags', 'Flags'), ('transfer_fee', 'TransferFee'), ('issuer', 'Issuer'), ('nft_taxon', 'NFTokenTaxon'), ('nft_serial', 'nft_serial')):
                    if page.get(theirs, 0 if theirs == 'TransferFee' else None) != result.get(ours): problems.append(f'{ours} {result.get(ours)!r}, the page entry {page.get(theirs)!r}')
            if token in states:
                owner, burned, uri = states[token]
                if (result.get('owner'), bool(result.get('is_burned'))) != (owner, burned):
                    problems.append(f"owner {result.get('owner')} burned {result.get('is_burned')}; ledger {seq}'s NFToken page changes leave owner {owner} burned {burned}")
                elif not burned and result.get('uri', '').upper() != uri: problems.append(f"uri {result.get('uri')!r}; the page changes leave {uri!r}")
            if problems: self.bad('nft_info', seq, asked + held_asked[:2], '; '.join(problems[:6]))
            elif truncated(error): self.skip('nft_info')
            else: self.ok('nft_info')

        for issuer, (entries, asked, error) in sorted(listings.items()):
            if truncated(error): self.skip('nfts_by_issuer'); continue
            if error: self.bad('nfts_by_issuer', seq, asked, f"error {error.get('error')}: {error.get('error_message', '')}"); continue
            problems = []
            ids = [e.get('nft_id', '').upper() for e in entries]
            order = [(e.get('nft_taxon'), e.get('nft_id', '').upper()) for e in entries]
            if len(set(ids)) != len(ids): problems.append('a token listed twice')
            if order != sorted(order): problems.append('not in (taxon, token ID) order')
            for e in entries:
                token = e.get('nft_id', '').upper()
                if e.get('issuer') != issuer: problems.append(f"{token}: issuer {e.get('issuer')}")
                if token in infos and 'error' not in infos[token][0]:
                    expected = {k: infos[token][0].get(k) for k in NFT_INFO_FIELDS}
                    got = {k: e.get(k) for k in NFT_INFO_FIELDS}
                    expected['nft_id'], got['nft_id'] = token, token
                    if got != expected: problems.append(f'{token}: {json.dumps({k: got[k] for k in got if got[k] != expected[k]})}, nft_info says {json.dumps({k: expected[k] for k in got if got[k] != expected[k]})}')
            for token in tokens:
                if nft_fields(token)['issuer'] == issuer and token in infos and 'error' not in infos[token][0] and token not in ids:
                    problems.append(f'{token} missing (nft_info knows it at {seq})')
            if problems: self.bad('nfts_by_issuer', seq, asked, '; '.join(problems[:6]))
            else: self.ok('nfts_by_issuer')

        def history(token):
            params = {'nft_id': token, 'ledger_index_min': seq, 'ledger_index_max': seq, 'limit': 100, 'api_version': 2}
            entries, asked, error = self.client.pages('nft_history', params, 'transactions', self.a.page_cap)
            if truncated(error): return self.skip('nft_history')
            if error: return self.bad('nft_history', seq, asked, f"error {error.get('error')}: {error.get('error_message', '')}")
            got = [entry_hash(e) for e in entries]
            problems = []
            if len(got) != len(set(got)): problems.append('a transaction listed twice')
            if required[token] - set(got): problems.append(f'missing (Clio records it): {sorted(required[token] - set(got))[:4]}')
            if set(got) - allowed[token]: problems.append(f'neither names the token nor changes its page entry: {sorted(set(got) - allowed[token])[:4]}')
            if any(e.get('ledger_index', seq) != seq for e in entries): problems.append('a transaction listed outside the ledger asked about')
            if problems: return self.bad('nft_history', seq, asked, '; '.join(problems))
            self.ok('nft_history')
        self.each('nft_history', seq, history, sorted(tokens))

    # MPTs -----------------------------------------------------------------------------------------
    def check_mpts(self, seq, txs, rng):
        issuances, holders = set(), {}
        def visit(value):
            if isinstance(value, dict):
                for k, v in value.items():
                    if k in ('MPTokenIssuanceID', 'mpt_issuance_id') and isinstance(v, str) and len(v) == 48: issuances.add(v.upper())
                    else: visit(v)
            elif isinstance(value, list):
                for v in value: visit(v)
        for t in txs:
            visit(t['tx']); visit(t['meta'])
            for node in t['meta'].get('AffectedNodes', []):
                kind, body, fields = node_fields(node)
                if body.get('LedgerEntryType') == 'MPTokenIssuance' and 'Issuer' in fields and 'Sequence' in fields:
                    issuances.add((fields['Sequence'].to_bytes(4, 'big') + account_id(fields['Issuer'])).hex().upper())
                elif body.get('LedgerEntryType') == 'MPToken' and 'MPTokenIssuanceID' in fields:
                    holders[(fields['MPTokenIssuanceID'].upper(), fields['Account'])] = kind != 'DeletedNode'
        if not issuances: return
        scanned = None
        if self.a.scan_state:
            entries, asked, error = self.client.pages('ledger_data', {'ledger_index': seq, 'type': 'mptoken', 'limit': 256}, 'state', 100000)
            if error: return self.bad('mpt_holders', seq, asked, f"ledger_data: {error.get('error')}")
            scanned = defaultdict(dict)
            for o in entries: scanned[o['MPTokenIssuanceID'].upper()][o['Account']] = o

        listed = {}
        def check(issuance):
            entry, asked = self.ask('ledger_entry', {'mpt_issuance': issuance, 'ledger_index': seq})
            entries, more, error = self.client.pages('mpt_holders', {'mpt_issuance_id': issuance, 'ledger_index': seq, 'limit': 100}, 'mptokens', self.a.page_cap)
            asked += more
            if 'error' in entry:
                if error and error.get('error') == 'objectNotFound': return self.ok('mpt_holders')
                return self.bad('mpt_holders', seq, asked, f"the issuance is not in state, but mpt_holders answers {json.dumps(error) if error else len(entries)}")
            if truncated(error): return self.skip('mpt_holders')
            if error: return self.bad('mpt_holders', seq, asked, f"error {error.get('error')}: {error.get('error_message', '')}")
            problems, accounts = [], [e.get('account') for e in entries]
            if len(set(accounts)) != len(accounts): problems.append('a holder listed twice')
            if [account_id(a) for a in accounts] != sorted(account_id(a) for a in accounts): problems.append('not in AccountID order')
            for e in entries:
                token, more = self.ask('ledger_entry', {'mptoken': {'mpt_issuance_id': issuance, 'account': e['account']}, 'ledger_index': seq})
                node = token.get('node')
                if not node: problems.append(f"{e['account']}: no MPToken in state ({token.get('error')})"); asked += more; continue
                want = {'account': node['Account'], 'flags': node.get('Flags', 0), 'mpt_amount': str(node.get('MPTAmount', '0')), 'mptoken_index': token.get('index', '').upper()}
                got = {'account': e.get('account'), 'flags': e.get('flags'), 'mpt_amount': str(e.get('mpt_amount')), 'mptoken_index': e.get('mptoken_index', '').upper()}
                if got != want: problems.append(f"{e['account']}: {json.dumps(got)}, state holds {json.dumps(want)}"); asked += more
            for (i, account), present in holders.items():
                if i != issuance: continue
                if present and account not in accounts: problems.append(f'{account} missing (its MPToken changed in {seq} and stayed)')
                if not present and account in accounts: problems.append(f'{account} listed (its MPToken was deleted in {seq})')
            if scanned is not None and set(accounts) != set(scanned.get(issuance, {})):
                problems.append(f"holders {sorted(set(accounts) ^ set(scanned.get(issuance, {})))[:4]} differ from ledger_data's MPTokens")
            listed[issuance] = entries
            if problems: return self.bad('mpt_holders', seq, asked, '; '.join(problems[:6]))
            self.ok('mpt_holders')
        self.each('mpt_holders', seq, check, self.sample(rng, issuances, self.a.mpts))

        wanted = defaultdict(dict)  # holder -> {issuance: mpt_holders entry}
        for issuance, entries in listed.items():
            for e in entries: wanted[e['account']][issuance] = e
        def account(holder):
            entries, asked, error = self.client.pages('account_mptokens', {'account': holder, 'ledger_index': seq, 'limit': 100}, 'mptokens', self.a.page_cap)
            if truncated(error): return self.skip('account_mptokens')
            if error: return self.bad('account_mptokens', seq, asked, f"error {error.get('error')}: {error.get('error_message', '')}")
            problems, mine = [], defaultdict(list)
            for e in entries:
                mine[e.get('mpt_issuance_id', '').upper()].append(e)
                if e.get('account') != holder: problems.append(f"{e.get('mpt_issuance_id')}: account {e.get('account')}")
            for issuance, holder_entry in wanted[holder].items():
                found = mine.get(issuance, [])
                if len(found) != 1: problems.append(f'{issuance}: listed {len(found)} times, mpt_holders lists the holder'); continue
                flags = holder_entry.get('flags', 0)
                got = (str(found[0].get('mpt_amount', 0)), bool(found[0].get('mpt_locked')), bool(found[0].get('mpt_authorized')))
                want = (str(holder_entry.get('mpt_amount')), bool(flags & 1), bool(flags & 2))
                if got != want: problems.append(f'{issuance}: (amount, locked, authorized) {got}, the MPToken has {want}')
            if scanned is not None:
                held = {i for i, accounts in scanned.items() if holder in accounts}
                if set(mine) != held: problems.append(f'issuances {sorted(set(mine) ^ held)[:4]} differ from ledger_data\'s MPTokens')
            else:
                # Without the scan, a listed issuance mpt_holders did not vouch for (one not
                # sampled, or not touched in this ledger) is checked against its MPToken in state,
                # up to MPT_STATE_CHECKS of them; a holder with more is also counted skipped.
                others = sorted(i for i in mine if i not in wanted[holder])
                for issuance in others[:MPT_STATE_CHECKS]:
                    if len(mine[issuance]) != 1: problems.append(f'{issuance}: listed {len(mine[issuance])} times'); continue
                    token, more = self.ask('ledger_entry', {'mptoken': {'mpt_issuance_id': issuance, 'account': holder}, 'ledger_index': seq})
                    node = token.get('node')
                    if not node: problems.append(f"{issuance}: listed, but no MPToken in state ({token.get('error')})"); asked += more; continue
                    flags, found = node.get('Flags', 0), mine[issuance][0]
                    got = (str(found.get('mpt_amount', 0)), bool(found.get('mpt_locked')), bool(found.get('mpt_authorized')))
                    want = (str(node.get('MPTAmount', '0')), bool(flags & 1), bool(flags & 2))
                    if got != want: problems.append(f'{issuance}: (amount, locked, authorized) {got}, its MPToken in state has {want}'); asked += more
                if len(others) > MPT_STATE_CHECKS: self.skip('account_mptokens')
            if problems: return self.bad('account_mptokens', seq, asked, '; '.join(problems[:6]))
            self.ok('account_mptokens')
        self.each('account_mptokens', seq, account, self.sample(rng, wanted, self.a.mpts * 4))

    # Books ----------------------------------------------------------------------------------------
    def check_books(self, seq, txs, rng):
        books = {}
        for t in txs:
            for node in t['meta'].get('AffectedNodes', []):
                _, body, fields = node_fields(node)
                if body.get('LedgerEntryType') != 'Offer' or 'BookDirectory' not in fields: continue
                pays, gets = issue(fields['TakerPays']), issue(fields['TakerGets'])
                if not pays or not gets: continue  # an MPT book: book_offers takes currency/issuer books only
                key = json.dumps([pays, gets, fields.get('DomainID')], sort_keys=True)
                books[key] = (pays, gets, fields.get('DomainID'), fields['BookDirectory'].upper()[:48])
        limit = 100

        def check(key):
            pays, gets, domain, prefix = books[key]
            params = {'taker_pays': pays, 'taker_gets': gets, 'ledger_index': seq, 'limit': limit, **({'domain': domain} if domain else {})}
            result, asked = self.ask('book_offers', params)
            if 'error' in result: return self.bad('book_offers', seq, asked, f"error {result['error']}: {result.get('error_message', '')}")
            # The book's directories in state, in key order, by successor: ledger_data from the
            # book's base (no directory has quality zero) until a key leaves the book.
            directories, marker, done = [], prefix + '0' * 16, False
            while not done and len(directories) < limit:
                page, more = self.ask('ledger_data', {'ledger_index': seq, 'marker': marker, 'limit': 256, 'binary': True})
                asked += more
                if 'error' in page: return self.bad('book_offers', seq, asked, f"ledger_data: {page['error']}")
                for o in page.get('state', []):
                    if not o['index'].upper().startswith(prefix): done = True; break
                    directories.append(o['index'].upper())
                marker = page.get('marker')
                if not page.get('state') or marker is None: done = True
            # xrpld's getBookPage over them: each turn reads one directory entry (an empty
            # directory still takes one), `limit` turns.
            expected, objects, turns = [], {}, limit
            for root in directories:
                if turns <= 0: break
                entries, index = [], 0
                while True:
                    node, more = self.ask('ledger_entry', {'index': dir_page(root, index), 'ledger_index': seq})
                    if 'error' in node: asked += more; return self.bad('book_offers', seq, asked, f'directory page {index} of {root} missing: {node["error"]}')
                    entries += [(i.upper(), root) for i in node['node'].get('Indexes', [])]
                    index = int(node['node'].get('IndexNext', '0'), 16)
                    if not index or len(entries) >= turns: break
                turns -= 1
                for n, entry in enumerate(entries):
                    if n: turns -= 1
                    if turns < 0: break
                    expected.append(entry)
            problems = []
            got = [o.get('index', '').upper() for o in result.get('offers', [])]
            if got != [i for i, _ in expected]:
                problems.append(f'offers {got[:6]} differ from the book walk in state {[i for i, _ in expected][:6]}')
            directory_of = dict(expected)
            for o in result.get('offers', [])[:self.a.book_objects]:
                index = o.get('index', '').upper()
                node, more = self.ask('ledger_entry', {'index': index, 'ledger_index': seq})
                if 'error' in node: problems.append(f'{index}: not in state'); asked += more; continue
                state = node['node']
                differs = sorted(k for k in state if k != 'index' and o.get(k) != state[k])
                if differs: problems.append(f'{index}: {differs} differ from the Offer in state'); asked += more
                if index in directory_of and state.get('BookDirectory', '').upper() != directory_of[index]:
                    problems.append(f'{index}: BookDirectory {state.get("BookDirectory")}, found in {directory_of[index]}')
                rate = exchange_rate(state.get('BookDirectory', '0' * 64))
                try: quality = float(o.get('quality'))
                except (TypeError, ValueError): quality = None
                if quality is None or abs(quality - rate) > 1e-9 * max(abs(rate), 1e-300): problems.append(f"{index}: quality {o.get('quality')}, its directory's {rate:.16g}")
            if problems: return self.bad('book_offers', seq, asked, '; '.join(problems[:6]))
            self.ok('book_offers')
        self.each('book_offers', seq, check, self.sample(rng, books, self.a.books))

    def check_ledger(self, seq):
        rng = random.Random(f'{self.a.seed}:{seq}')
        ledger = self.client.call('ledger', {'ledger_index': seq, 'transactions': True, 'expand': True, 'api_version': 2})
        if 'error' in ledger: raise RuntimeError(f"ledger {seq}: {ledger['error']}")
        ledger = ledger['ledger']
        txs = []
        for entry in ledger.get('transactions', []):
            tx = entry.get('tx_json') or entry
            txs.append({'hash': entry_hash(entry) or tx.get('hash', '').upper(), 'tx': tx, 'meta': entry.get('meta') or entry.get('metaData') or {}})
        txs.sort(key=lambda t: t['meta'].get('TransactionIndex', 0))
        self.check_account_tx(seq, txs, rng)
        self.check_tx(seq, ledger, txs, rng)
        self.check_nfts(seq, txs, rng)
        self.check_mpts(seq, txs, rng)
        self.check_books(seq, txs, rng)
        return len(txs)

    def run(self):
        started = time.time()
        if self.a.range: first, last = self.a.range
        else:
            span = self.client.call('ledger_range', {})
            first, last = span['ledger_index_min'], span['ledger_index_max']
        rng = random.Random(self.a.seed)
        span = range(first, last + 1)
        ledgers = list(span) if self.a.ledgers <= 0 or self.a.ledgers >= len(span) else sorted(rng.sample(span, self.a.ledgers))
        transactions = 0
        for seq in ledgers: transactions += self.check_ledger(seq)
        self.pool.shutdown()
        bad = 0
        for name in INVARIANTS:
            s = self.stats[name]; bad += s['violated'] + s['errors']
            print(json.dumps({'invariant': name, **s}), flush=True)
        print(json.dumps({'summary': True, 'range': [first, last], 'ledgers': len(ledgers), 'transactions': transactions,
                          'checked': sum(s['checked'] for s in self.stats.values()), 'violated': sum(s['violated'] for s in self.stats.values()),
                          'errors': sum(s['errors'] for s in self.stats.values()), 'skipped': sum(s['skipped'] for s in self.stats.values()),
                          'requests': self.client.requests,
                          'seconds': round(time.time() - started, 1)}), flush=True)
        # A run that checked nothing (every sampled ledger empty, a wrong range) proves nothing.
        return 1 if bad or not sum(s['checked'] for s in self.stats.values()) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--xrpla', required=True, help='XRPLA JSON-RPC URL')
    parser.add_argument('--range', type=lambda s: tuple(map(int, s.split('-'))), help='FIRST-LAST (default: the server\'s ledger_range)')
    parser.add_argument('--ledgers', type=int, default=16, help='ledgers sampled from the range (0: every one)')
    parser.add_argument('--seed', type=int, default=1)
    parser.add_argument('--in-flight', type=int, default=8, help='most requests outstanding at once')
    parser.add_argument('--accounts', type=int, default=100, help='affected accounts checked per ledger (0: all)')
    parser.add_argument('--probes', type=int, default=4, help='accounts from other ledgers checked to list nothing per ledger')
    parser.add_argument('--txs', type=int, default=200, help='transactions looked up by hash per ledger (0: all)')
    parser.add_argument('--nfts', type=int, default=64, help='touched NFTs checked per ledger (0: all)')
    parser.add_argument('--issuers', type=int, default=8, help='NFT issuers whose nfts_by_issuer is checked per ledger')
    parser.add_argument('--issuer-sample', type=int, default=4, help='further tokens of each such issuer checked')
    parser.add_argument('--mpts', type=int, default=16, help='MPT issuances checked per ledger')
    parser.add_argument('--books', type=int, default=8, help='order books checked per ledger')
    parser.add_argument('--book-objects', type=int, default=100, help='offers of each book compared with state')
    parser.add_argument('--page-cap', type=int, default=200, help='most pages followed for one paged answer')
    parser.add_argument('--scan-state', action='store_true', help='also compare MPT holders with every MPToken ledger_data finds (walks the whole state)')
    parser.add_argument('--examples', type=int, default=3, help='violations shown per invariant')
    sys.exit(Invariants(parser.parse_args()).run())


if __name__ == '__main__':
    main()
