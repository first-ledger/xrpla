#!/usr/bin/env python3
"""Compare `xrpla serve` with a live xrpld on the ledgers both hold.

Samples validated ledgers in the overlap of the two servers' ranges, derives
requests from their contents (transactions, accounts, books, AMMs, NFTs, owned
objects) and sends each request to both. Responses are compared after removing
fields that describe the server rather than the ledger (status, warnings).
Prints one JSON line per method with match counts, the first differences, and each server's
response times (median, 95th percentile and maximum, in milliseconds), so the same run is a
per-method benchmark when both servers answer from the same machine.

  xrpld_parity.py --xrpld http://127.0.0.1:5005 --xrpla http://127.0.0.1:5015 [--ledgers 8]

With --transports, every request XRPLA answers over --xrpla is also sent to each of its other
endpoints (ws://, wss://, https://) and must get the same answer there: one server, every
protocol it serves.

A standalone xrpld (`--standalone --ledger N --valid` on a restored database) answers older
ledgers but reports only its tip as complete: give the range with --range. --clio-methods also
times the methods Clio adds (nft_info, nft_history, nfts_by_issuer, mpt_holders,
account_mptokens, ledger_range, server_info), on XRPLA alone; with --clio URL, a Clio server,
those of them that describe ledgers rather than the server are compared with its answers.

--xrpld may list several servers (comma-separated), asked in turn. Full-history public
servers that are not rate limited are mostly Clio (s1/s2.ripple.com), whose answers differ from
xrpld's in places: with --arbiter URL, an xrpld, an answer of the reference that XRPLA's differs
from is asked of the arbiter too, and one XRPLA matches there counts as the reference's own
difference (reported apart, as `reference_differs`), not XRPLA's. The arbiter is asked only
about differences, so a rate-limited xrpld serves.

A run passes when no answer differs and at least one answer about a ledger matches that is not
an error: version and ping, which every run asks, describe the server and do not count. Requests
XRPLA did not answer in time and those the reference could not answer are reported apart; more
than --max-unanswered of the ledger requests (default 0.1) fail the run.
"""
import argparse
from collections import defaultdict
import http.client
import json
from pathlib import Path
import random
import re
import socket
import statistics
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))

TIMEOUT = 120
SERVER_FIELDS = {'status', 'warnings', 'warning', 'api_version'}
# Errors that say the reference could not answer (a load-balanced server's node without the
# ledger, or a busy one), not what the ledger holds. `internal` is xrpld failing the request
# (book_changes on a ledger with a transaction it cannot decode), which there is no matching.
UNAVAILABLE = {'lgrNotFound', 'notReady', 'tooBusy', 'slowDown', 'noNetwork', 'noCurrent', 'noClosed', 'internal'}
ACCOUNT_FIELDS = {'Account', 'Destination', 'Owner', 'Issuer', 'issuer', 'RegularKey', 'Authorize', 'Holder'}
# Methods compared whose answers describe the server, not a ledger: they are reported, but a
# run's pass needs matching answers about ledgers (xrpld_parity's every run asks these).
SERVER_METHODS = {'version', 'ping'}
# The share of the ledger requests that may go unanswered, by XRPLA in time or by the reference,
# before a run proves too little to pass (--max-unanswered).
MAX_UNANSWERED = 0.1


def call(url, method, params):
    body = json.dumps({'method': method, 'params': [params]}).encode()
    request = urllib.request.Request(url, body, {'Content-Type': 'application/json', 'User-Agent': 'xrpla-parity'})
    started = time.time()
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        result = json.loads(response.read())['result']
    return result, time.time() - started


def reference_call(url, method, params, waited):
    """The reference's answer, waiting out its rate limit: a public server answers tooBusy or
    slowDown with "retry in ~N ms" once a client's quota is spent (xrplcluster.com: 500,000 units
    an hour), and 5xx when a node behind it is down. `waited` sums the seconds spent waiting."""
    for attempt in range(12):
        try: result, seconds = call(url, method, params)
        except urllib.error.HTTPError as error:
            if error.code < 500 or attempt == 11: raise
            time.sleep(min(60, 5 * (attempt + 1))); waited[0] += min(60, 5 * (attempt + 1)); continue
        hint = re.search(r'retry in ~(\d+)ms', result.get('error_message', '')) if result.get('error') in ('tooBusy', 'slowDown') else None
        if not hint: return result, seconds
        pause = min(1200, int(hint.group(1)) / 1000 + 1)
        time.sleep(pause); waited[0] += pause
    return result, seconds


def server_call(url, method, params, waited, patience=180):
    """XRPLA's answer, waiting while it cannot be reached (a restart) for up to `patience`
    seconds; a request it accepts but does not answer in time still raises."""
    deadline = time.time() + patience
    while True:
        try: return call(url, method, params)
        except urllib.error.URLError as error:
            refused = isinstance(error.reason, ConnectionRefusedError) or 'refused' in str(error.reason).lower()
            if not refused or time.time() > deadline: raise
        except (ConnectionResetError, http.client.RemoteDisconnected):
            if time.time() > deadline: raise
        time.sleep(3); waited[0] += 3


http_errors = (http.client.HTTPException, urllib.error.URLError)


class Transport:
    """One more of XRPLA's endpoints: http(s):// asked per request, ws(s):// over one connection
    (opened when first needed, and again after the server closed it)."""
    def __init__(self, url):
        self.url, self.ws, self.next_id = url, None, 0

    def ask(self, method, params):
        """The answer as HTTP's `result` would carry it."""
        if not self.url.startswith(('ws://', 'wss://')): return call(self.url, method, params)[0]
        if self.ws is None:
            from capture_validations import WebSocket
            self.ws = WebSocket(self.url); self.ws.sock.settimeout(TIMEOUT)
        self.next_id += 1
        self.ws.send({**params, 'id': self.next_id, 'command': method})
        response = self.ws.receive()
        if response.get('id') != self.next_id: raise ValueError(f'{self.url}: answer to request {response.get("id")}, not {self.next_id}')
        if 'result' in response: return {**response['result'], 'status': response.get('status')}
        # An error comes unwrapped, beside the envelope xrpld's ServerHandler adds to every WebSocket
        # response: the request's id, type, and its echoed jsonrpc, ripplerpc and api_version.
        return {k: v for k, v in response.items() if k not in ('id', 'type', 'jsonrpc', 'ripplerpc', 'api_version')}


def same_answer(http, other):
    """HTTP's answer and another protocol's, compared whole but for the echoed request (which a
    WebSocket error repeats with its id and command)."""
    strip = lambda v: {k: x for k, x in v.items() if k != 'request'} if isinstance(v, dict) else v
    return differences(strip(http), strip(other), limit=6)


def normalize(value):
    if isinstance(value, dict):
        # xrpld repeats the compact transaction ID inside tx_json for some transactions still in
        # its cache of recent ones (two xrpld nodes differ on the same transaction); the
        # top-level ctid is the answer. A tx_json ctid that disagrees with it still counts.
        tx = value.get('tx_json')
        if isinstance(tx, dict) and 'ctid' in tx and tx.get('ctid') == value.get('ctid'):
            value = {**value, 'tx_json': {k: v for k, v in tx.items() if k != 'ctid'}}
        return {k: normalize(v) for k, v in value.items() if k not in SERVER_FIELDS}
    if isinstance(value, list): return [normalize(v) for v in value]
    return value


def differences(a, b, path='', out=None, limit=6, ignore=()):
    """Up to `limit` differences, those matching an `ignore` pattern left out before they count
    (else a large answer's ignored ones could fill the limit and hide a real one)."""
    out = [] if out is None else out
    def note(text):
        if len(out) < limit and not any(re.search(i, text) for i in ignore): out.append(text)
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if len(out) >= limit: break
            if k not in a: note(f'{path}/{k}: only xrpla')
            elif k not in b: note(f'{path}/{k}: only xrpld')
            else: differences(a[k], b[k], f'{path}/{k}', out, limit, ignore)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b): note(f'{path}: xrpld {len(a)} items, xrpla {len(b)}')
        for i, (x, y) in enumerate(zip(a, b)):
            if len(out) >= limit: break
            differences(x, y, f'{path}[{i}]', out, limit, ignore)
    elif a != b: note(f'{path}: xrpld {json.dumps(a)[:160]} xrpla {json.dumps(b)[:160]}')
    return out


def spread(items, wanted):
    """`wanted` of `items` evenly across them, both ends included (the last listed is the newest
    ledger's): all of them when there are no more than that."""
    if len(items) <= wanted: return list(items)
    if wanted <= 0: return []
    if wanted == 1: return [items[-1]]
    return [items[round(i * (len(items) - 1) / (wanted - 1))] for i in range(wanted)]


def walk(value, visit):
    if isinstance(value, dict):
        for k, v in value.items(): visit(k, v); walk(v, visit)
    elif isinstance(value, list):
        for v in value: walk(v, visit)


def issue(amount):
    if isinstance(amount, str): return {'currency': 'XRP'}
    return {'currency': amount['currency'], 'issuer': amount['issuer']} if 'issuer' in amount else None


class Parity:
    def __init__(self, a):
        self.a = a; self.stats = defaultdict(lambda: {'same': 0, 'different': 0, 'xrpld_seconds': 0.0, 'xrpla_seconds': 0.0, 'examples': []})
        self.times = defaultdict(lambda: {'xrpld': [], 'xrpla': []})  # each compared request's seconds, per server
        self.slowest = defaultdict(list)  # (XRPLA seconds, params) of each method's slowest requests
        self.transports = [Transport(url) for url in a.transports]
        self.waited = [0.0]  # seconds spent waiting for the reference's rate limit or XRPLA's restarts
        self.references = [u for u in a.xrpld.split(',') if u]
        self.asked = 0  # requests sent to the references, which take turns

    def reference(self):
        self.asked += 1
        return self.references[self.asked % len(self.references)]

    def compare(self, method, _reference=None, **params):
        # Clio's methods alone: the other requests only find what to ask (from XRPLA's answers).
        if self.a.clio_only and not _reference: return server_call(self.a.xrpla, method, params, self.waited)[0]
        s = self.stats[method]
        reference = _reference or self.reference()
        # A public xrpld raises smaller limits to its minimum (10 for account_tx): ask both for at least that.
        if 'limit' in params and params['limit'] < self.a.min_limit: params = {**params, 'limit': self.a.min_limit}
        expected, t0 = reference_call(reference, method, params, self.waited)
        try: actual, t1 = server_call(self.a.xrpla, method, params, self.waited)
        except (OSError, http.client.HTTPException) as error:
            # A request XRPLA accepted but did not answer in time is slowness, reported apart; any
            # other failure (an HTTP error, a reset, still refused after a restart's wait) fails the run.
            # (Before Python 3.10, socket.timeout is not a TimeoutError.)
            timeout = (TimeoutError, socket.timeout)
            timed_out = isinstance(error, timeout) or isinstance(getattr(error, 'reason', None), timeout)
            key = 'timeouts' if timed_out else 'xrpla_errors'
            s[key] = s.get(key, 0) + 1
            if len(s['examples']) < 3: s['examples'].append({'params': params, 'differences': [f'xrpla: {error}']})
            return expected
        # A load-balanced reference (xrplcluster.com) sometimes routes a request to a node that lacks
        # the ledger: an error where XRPLA answers is asked again before it counts.
        for attempt in range(self.a.reference_retries if 'error' in expected and 'error' not in actual else 0):
            time.sleep(min(10, 2 * (attempt + 1)))
            again, _ = reference_call(reference, method, params, self.waited)
            if 'error' not in again: expected = again; break
        # The reference could not answer (busy, rate-limited, without the ledger): not compared,
        # whatever XRPLA said.
        if expected.get('error') in UNAVAILABLE and expected.get('error') != actual.get('error'):
            s['reference_errors'] = s.get('reference_errors', 0) + 1  # reported, not compared
            if len(s['examples']) < 3: s['examples'].append({'params': params, 'differences': [f"xrpld: {expected['error']}"]})
            return expected
        self.across_transports(method, params, actual)
        s['xrpld_seconds'] += t0; s['xrpla_seconds'] += t1; s['xrpla_max_seconds'] = round(max(s.get('xrpla_max_seconds', 0), t1), 3)
        self.times[method]['xrpld'].append(t0); self.times[method]['xrpla'].append(t1)
        self.note_slow(method, t1, params, t0)
        found = differences(normalize(expected), normalize(actual), ignore=self.a.ignore)
        # A load-balanced reference's nodes do not all answer alike (xrplcluster.com mixes xrpld
        # versions): an answer XRPLA matches from any of them counts, reported apart.
        for attempt in range(self.a.reference_retries if found else 0):
            time.sleep(min(10, 2 * (attempt + 1)))
            again, _ = reference_call(reference, method, params, self.waited)
            if not differences(normalize(again), normalize(actual), limit=1, ignore=self.a.ignore):
                s['reference_inconsistent'] = s.get('reference_inconsistent', 0) + 1
                found = []
                break
        # The reference is not xrpld (a Clio): an xrpld arbiter whose answer XRPLA matches makes
        # the difference the reference's.
        if found and self.a.arbiter and not _reference:
            judged, _ = reference_call(self.a.arbiter, method, params, self.waited)
            if judged.get('error') in UNAVAILABLE and judged.get('error') != actual.get('error'):
                # The arbiter could not answer (busy, rate-limited): not compared, as for the reference.
                s['reference_errors'] = s.get('reference_errors', 0) + 1
                if len(s['examples']) < 3: s['examples'].append({'params': params, 'differences': [f"arbiter: {judged['error']}"]})
                return expected
            if not differences(normalize(judged), normalize(actual), limit=1, ignore=self.a.ignore):
                s['reference_differs'] = s.get('reference_differs', 0) + 1
                if len(s.setdefault('reference_examples', [])) < 3: s['reference_examples'].append({'params': params, 'differences': found})
                found = []
        if found:
            s['different'] += 1
            if len(s['examples']) < 3: s['examples'].append({'params': params, 'differences': found})
        # Both answered with the same error (both lgrNotFound): alike, but nothing about the ledger
        # was compared, so it is counted apart and does not make a run that compared nothing pass.
        elif 'error' in actual: s['same_errors'] = s.get('same_errors', 0) + 1
        else: s['same'] += 1
        return expected

    def across_transports(self, method, params, http):
        """The same request over each other endpoint: its answer must be HTTP's."""
        s = self.stats[method]
        for transport in self.transports:
            # A connection the server closed (a restart) is opened again and the request asked
            # again, for up to three minutes; one that still fails is a transport error, not a
            # different answer.
            deadline, error = time.time() + 180, None
            while True:
                try: found = same_answer(http, transport.ask(method, params)); error = None; break
                except (OSError, ValueError, ConnectionError, *http_errors) as e:
                    error = e; transport.ws = None
                    if time.time() > deadline: break
                    time.sleep(3); self.waited[0] += 3
            if error is not None:
                s['transport_errors'] = s.get('transport_errors', 0) + 1
                if len(s.setdefault('transport_examples', [])) < 3: s['transport_examples'].append({'url': transport.url, 'params': params, 'error': str(error)})
                continue
            key = 'transports_different' if found else 'transports_same'
            s[key] = s.get(key, 0) + 1
            if found and len(s.setdefault('transport_examples', [])) < 3:
                s['transport_examples'].append({'url': transport.url, 'params': params, 'differences': found})

    def follow(self, method, pages=3, _reference=None, **params):
        """Compare a paged request and up to `pages` continuations through the reference's markers."""
        page = self.compare(method, _reference, **params)
        for _ in range(pages):
            if 'marker' not in page: break
            page = self.compare(method, _reference, **{**params, 'marker': page['marker']})

    def xrpla_only(self, method, **params):
        """Time a method xrpld lacks (Clio's) on XRPLA alone; an error answer is counted apart."""
        result, seconds = server_call(self.a.xrpla, method, params, self.waited)
        s = self.stats[method]; s['xrpla_only'] = True
        if 'error' in result:
            s['errors'] = s.get('errors', 0) + 1
            if len(s['examples']) < 3: s['examples'].append({'params': params, 'differences': [f"xrpla: {result['error']}"]})
        else: s['same'] += 1
        s['xrpla_seconds'] += seconds; self.times[method]['xrpla'].append(seconds)
        self.note_slow(method, seconds, params)
        return result

    def note_slow(self, method, seconds, params, reference=None):
        kept = self.slowest[method]
        kept.append((seconds, reference, params)); kept.sort(key=lambda x: -x[0]); del kept[3:]

    def overlap(self):
        if self.a.range: return self.a.range
        info, _ = call(self.references[0], 'server_info', {})
        spans = [tuple(map(int, part.split('-'))) for part in info['info']['complete_ledgers'].split(',') if '-' in part]
        ours, _ = call(self.a.xrpla, 'ledger_range', {})
        best = max(spans, key=lambda s: min(s[1], ours['ledger_index_max']) - max(s[0], ours['ledger_index_min']))
        return max(best[0], ours['ledger_index_min']), min(best[1], ours['ledger_index_max'])

    def run(self):
        first, last = self.overlap()
        if first > last: raise SystemExit('The servers share no ledgers')
        rng = random.Random(self.a.seed)
        ledgers = sorted({first, last, *rng.sample(range(first, last + 1), min(self.a.ledgers, last - first + 1)),
                          *(seq for seq in self.a.include_ledgers if first <= seq <= last)})
        accounts, books, amms, nfts, txs, keys = set(), {}, {}, set(), [], []
        self.issuers, self.mpts = set(), set()
        for seq in ledgers:
            self.compare('ledger', ledger_index=seq)
            self.compare('ledger', ledger_index=seq, transactions=True)
            ledger = self.compare('ledger', ledger_index=seq, transactions=True, expand=True, api_version=2)
            self.compare('ledger', ledger_index=seq, transactions=True, expand=True, api_version=1)
            self.compare('ledger', ledger_index=seq, transactions=True, expand=True, binary=True)
            self.compare('book_changes', ledger_index=seq)
            for tx in ledger.get('ledger', {}).get('transactions', [])[:self.a.transactions]:
                txs.append((tx['hash'], seq))
                def visit(k, v):
                    if k in ACCOUNT_FIELDS and isinstance(v, str) and v.startswith('r'): accounts.add(v)
                    if k in ('NFTokenID', 'nftoken_id') and isinstance(v, str): nfts.add(v)
                    if k in ('MPTokenIssuanceID', 'mpt_issuance_id') and isinstance(v, str): self.mpts.add(v)
                walk(tx, visit)
                if tx.get('TransactionType', tx.get('tx_json', {}).get('TransactionType')) == 'NFTokenMint':
                    self.issuers.add(tx.get('Issuer') or tx.get('Account') or tx.get('tx_json', {}).get('Account'))
                for node in tx.get('meta', {}).get('AffectedNodes', []):
                    body = next(iter(node.values())); fields = body.get('FinalFields') or body.get('NewFields') or {}
                    keys.append((body['LedgerIndex'], seq))
                    if body.get('LedgerEntryType') == 'Offer' and 'TakerGets' in fields:
                        pays, gets = issue(fields['TakerPays']), issue(fields['TakerGets'])
                        if pays and gets: books[json.dumps([pays, gets], sort_keys=True)] = seq
                    if body.get('LedgerEntryType') == 'AMM' and 'Account' in fields: amms[fields['Account']] = seq
        # Named transactions first: ones a ledger listing would not show (xrpld cannot decode them).
        # The rest evenly across the sampled ledgers, not the first ones listed: busy early
        # ledgers would otherwise leave the newest (the range's last among them) unchecked.
        txs = [(tx, None) for tx in self.a.include_transactions] + spread(txs, self.a.transactions * 4)
        for tx, seq in txs:
            for version in (1, 2):
                self.compare('tx', transaction=tx, api_version=version)
            self.compare('tx', transaction=tx, binary=True)
            if seq is not None: self.compare('transaction_entry', tx_hash=tx, ledger_index=seq)
        sampled = rng.sample(sorted(accounts), min(self.a.accounts, len(accounts)))
        for account in sampled:
            at = rng.choice(ledgers)
            for method in ('account_info', 'account_lines', 'account_offers', 'account_currencies', 'account_nfts', 'account_channels', 'gateway_balances'):
                self.compare(method, account=account, ledger_index=at)
            self.compare('account_info', account=account, ledger_index=at, signer_lists=True, api_version=2)
            self.follow('account_objects', account=account, ledger_index=at, limit=10)
            self.follow('account_lines', account=account, ledger_index=at, limit=10)
            self.follow('account_offers', account=account, ledger_index=at, limit=10)
            self.follow('account_nfts', account=account, ledger_index=at, limit=20)
            self.follow('account_tx', account=account, ledger_index_min=first, ledger_index_max=last, limit=5, api_version=2)
            self.follow('account_tx', account=account, ledger_index_min=first, ledger_index_max=last, limit=5, forward=True, binary=True)
            for role in ('user', 'gateway'):
                self.compare('noripple_check', account=account, ledger_index=at, role=role, transactions=True, limit=20)
            self.compare('account_tx', account=account, ledger_index_min=first, ledger_index_max=last, limit=10, api_version=2)
            self.compare('account_tx', account=account, ledger_index_min=first, ledger_index_max=last, limit=10, forward=True)
            self.compare('ledger_entry', account_root=account, ledger_index=at)
            other = rng.choice(sampled)
            self.compare('deposit_authorized', source_account=other, destination_account=account, ledger_index=at)
        for key, seq in rng.sample(keys, min(len(keys), self.a.objects)):
            self.compare('ledger_entry', index=key, ledger_index=seq)
            self.compare('ledger_entry', index=key, ledger_index=seq, binary=True)
        for book, seq in list(books.items())[:self.a.books]:
            pays, gets = json.loads(book)
            self.compare('book_offers', taker_pays=pays, taker_gets=gets, ledger_index=seq, limit=10)
        for amm, seq in list(amms.items())[:self.a.books]:
            self.compare('amm_info', amm_account=amm, ledger_index=seq)
        for nft in sorted(nfts)[:self.a.books]:
            for method in ('nft_buy_offers', 'nft_sell_offers'):
                self.compare(method, nft_id=nft, ledger_index=last)
        for at in (first, last):
            self.follow('ledger_data', ledger_index=at, limit=16, binary=True)
            self.follow('ledger_data', ledger_index=at, limit=8)
            self.follow('ledger_data', ledger_index=at, limit=200, type='offer', pages=1)
        for version in (1, 2): self.compare('version', api_version=version)
        self.compare('ping')
        if self.a.clio_methods:
            # ledger_range and server_info describe the server; the rest are compared with a Clio
            # given one, at ledgers it holds, paged through its markers.
            self.xrpla_only('ledger_range'); self.xrpla_only('server_info')
            clio = (lambda method, pages=0, **params: self.follow(method, pages=pages, _reference=self.a.clio, **params)) if self.a.clio                 else (lambda method, pages=0, **params: self.xrpla_only(method, **params))
            for nft in sorted(nfts)[:self.a.books]:
                clio('nft_info', nft_id=nft, ledger_index=last)
                clio('nft_info', nft_id=nft, ledger_index=rng.choice(ledgers))
                clio('nft_history', nft_id=nft, limit=20, pages=3)
                clio('nft_history', nft_id=nft, limit=5, forward=True, pages=3)
            issuers = sorted(self.issuers - {None})[:self.a.books]
            for issuer in issuers:
                clio('nfts_by_issuer', issuer=issuer, ledger_index=last, limit=50, pages=3)
                clio('nfts_by_issuer', issuer=issuer, ledger_index=rng.choice(ledgers), limit=10, pages=2)
            for issuance in sorted(self.mpts)[:self.a.books]: clio('mpt_holders', mpt_issuance_id=issuance, ledger_index=last, limit=50, pages=3)
            for account in sampled[:self.a.books]:
                clio('account_mptokens', account=account, ledger_index=rng.choice(ledgers))
            # Clio's ledger_index: the ledger closed at a time (each sampled ledger's own close time).
            for at in (first, last, *rng.sample(ledgers, min(4, len(ledgers)))):
                closed, _ = call(self.a.xrpla, 'ledger', {'ledger_index': at})
                if 'ledger' in closed: clio('ledger_index', date=closed['ledger']['close_time_iso'])
        def summary(values):
            if not values: return None
            ordered = sorted(values)
            return {'n': len(ordered), 'median': round(1000 * statistics.median(ordered), 2),
                    'p95': round(1000 * ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 2), 'max': round(1000 * ordered[-1], 2)}
        for method, s in sorted(self.stats.items()):
            print(json.dumps({'method': method, **s, 'xrpld_seconds': round(s['xrpld_seconds'], 3), 'xrpla_seconds': round(s['xrpla_seconds'], 3),
                              'xrpla_ms': summary(self.times[method]['xrpla']), 'xrpld_ms': summary(self.times[method]['xrpld']),
                              'slowest': [{'xrpla_ms': round(1000 * a, 2), 'xrpld_ms': None if b is None else round(1000 * b, 2), 'params': c}
                                          for a, b, c in self.slowest[method]]}))
        # Answers XRPLA alone gave (xrpla_only) were timed, not compared; answers about the server
        # (SERVER_METHODS) were compared but are counted apart, as they say nothing of a ledger.
        compared = [s for m, s in self.stats.items() if not s.get('xrpla_only') and m not in SERVER_METHODS]
        server = [s for m, s in self.stats.items() if m in SERVER_METHODS]
        total = sum(s['same'] for s in compared), sum(s['different'] for s in compared + server)
        same_errors = sum(s.get('same_errors', 0) for s in compared)
        server_same = sum(s['same'] for s in server)
        across = sum(s.get('transports_same', 0) for s in self.stats.values()), sum(s.get('transports_different', 0) for s in self.stats.values())
        transport_errors = sum(s.get('transport_errors', 0) for s in self.stats.values())
        timeouts = sum(s.get('timeouts', 0) for s in self.stats.values())
        xrpla_errors = sum(s.get('xrpla_errors', 0) for s in self.stats.values())
        unavailable = sum(s.get('reference_errors', 0) for s in self.stats.values())
        inconsistent = sum(s.get('reference_inconsistent', 0) for s in self.stats.values())
        reference_differs = sum(s.get('reference_differs', 0) for s in self.stats.values())
        # The ledger requests sent, answered or not, and those one of the servers left unanswered.
        unanswered = sum(s.get('timeouts', 0) + s.get('reference_errors', 0) for s in compared)
        requests = unanswered + sum(s['same'] + s['different'] + s.get('same_errors', 0) + s.get('xrpla_errors', 0) for s in compared)
        print(json.dumps({'overlap': [first, last], 'ledgers': len(ledgers), 'same': total[0], 'same_errors': same_errors, 'different': total[1], 'timeouts': timeouts, 'xrpla_errors': xrpla_errors,
                          'reference_errors': unavailable, 'reference_inconsistent': inconsistent, 'reference_differs': reference_differs,
                          'server_same': server_same, 'requests': requests, 'unanswered': unanswered, 'unanswered_allowed': int(self.a.max_unanswered * requests),
                          'transports': {'endpoints': [t.url for t in self.transports], 'same': across[0], 'different': across[1], 'errors': transport_errors},
                          'waited_seconds': round(self.waited[0])}))
        # A run without one matching answer about a ledger that is not an error (a wrong range, a
        # reference that answers every request with an error, both servers answering lgrNotFound
        # alike, every request timing out) proves nothing and fails, whatever version and ping
        # said. Timeouts and the reference's errors are reported apart, but past --max-unanswered
        # of the ledger requests they leave too little compared and fail the run too.
        return (total[0] > 0 and total[1] == 0 and across[1] == 0 and xrpla_errors == 0
                and unanswered <= int(self.a.max_unanswered * requests))


def share(text):
    """A command-line share: a number from 0 to 1."""
    value = float(text)
    if not 0 <= value <= 1: raise argparse.ArgumentTypeError(f'{text} is not between 0 and 1')
    return value


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--xrpld', required=True); p.add_argument('--xrpla', required=True)
    p.add_argument('--ledgers', type=int, default=8); p.add_argument('--transactions', type=int, default=40)
    p.add_argument('--accounts', type=int, default=60); p.add_argument('--objects', type=int, default=200)
    p.add_argument('--books', type=int, default=30); p.add_argument('--seed', type=int, default=1)
    numbers = lambda text: [int(n) for n in text.split(',') if n]
    p.add_argument('--include-ledgers', type=numbers, default=[], help='ledgers to compare besides the sample (comma-separated)')
    p.add_argument('--include-transactions', type=lambda text: [h for h in text.split(',') if h], default=[],
                   help='transaction hashes to look up besides those the sampled ledgers list')
    # xrplcluster.com answered one ledger listing the 3.4 way on 2 of 6 asks (2026-09-30): ten more
    # asks leave a mixed reference's wrong answer about a 1% chance.
    p.add_argument('--reference-retries', type=int, default=10,
                   help='times to ask the reference again (2 to 10 s apart) when its answer differs or it errs where XRPLA answers')
    p.add_argument('--min-limit', type=int, default=1, help='raise smaller limits to this (a public xrpld enforces 10 for account_tx)')
    p.add_argument('--range', type=lambda text: tuple(int(n) for n in text.split('-')), help="FIRST-LAST: the ledgers both hold (a standalone xrpld reports only its tip)")
    p.add_argument('--clio-methods', action='store_true', help='also time the methods Clio adds, on XRPLA alone')
    p.add_argument('--clio', help="a Clio server's JSON-RPC URL: with --clio-methods, Clio's methods are compared with its answers")
    p.add_argument('--arbiter', help="an xrpld asked about each difference from a reference that is not xrpld (see above)")
    p.add_argument('--clio-only', action='store_true',
                   help="with --clio: compare only Clio's methods; XRPLA's other answers just find the tokens, issuers and accounts to ask about")
    p.add_argument('--transports', type=lambda text: [u for u in text.split(',') if u], default=[],
                   help="XRPLA's other endpoints (ws://, wss://, https://, comma-separated): each request is asked there too and must get HTTP's answer")
    p.add_argument('--ignore', action='append', default=[], metavar='REGEX',
                   help='leave out differences matching REGEX (known differences of the reference server version)')
    p.add_argument('--max-unanswered', type=share, default=MAX_UNANSWERED,
                   help='the share of the ledger requests that may time out at XRPLA or go unanswered by the reference '
                        f'(default {MAX_UNANSWERED}); more fails the run')
    sys.exit(0 if Parity(p.parse_args()).run() else 1)


if __name__ == '__main__':
    main()
