"""Queries over compressed pieces answer exactly like the RocksDB store.

Publishes an archive as pieces, syncs a piece cache, builds its query index with
`xrpla index-pieces`, then serves the cache and a RocksDB store built from the same
archive side by side and requires identical responses to requests derived from the
data itself: every ledger, transaction, account, book, object and NFT it contains.

Usage: piece_query_test.py [ARCHIVE PIECE_LEDGERS BUNDLE_PIECES [MAX_LEDGERS [TAIL_AFTER]]]; defaults
to the mainnet-transactions fixture. Needs XRPLA_BINARY, XRPLA_HISTORY_DECODER and
XRPLA_SNAPSHOT_TOOL; XRPLA_HISTORY_ENCODER publishes with the compiled encoder, and
XRPLA_SNAPSHOTS_EVERY syncs the cache with `--snapshots-every` (interval snapshots kept).
"""
import http.client
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = Path(sys.argv.pop(1)) if len(sys.argv) > 1 else ROOT / 'tests' / 'fixtures' / 'mainnet-transactions'
PIECE = int(sys.argv.pop(1)) if len(sys.argv) > 1 else 2
BUNDLE = int(sys.argv.pop(1)) if len(sys.argv) > 1 else 2
MAX_LEDGERS = int(sys.argv.pop(1)) if len(sys.argv) > 1 else 64
# With TAIL_AFTER, pieces are published only up to that ledger and the archive (a signed v2
# spool) serves the later ledgers as a live tail over them.
TAIL_AFTER = int(sys.argv.pop(1)) if len(sys.argv) > 1 else None
BINARY, DECODER, SNAPSHOT = (os.environ.get(k) for k in ('XRPLA_BINARY', 'XRPLA_HISTORY_DECODER', 'XRPLA_SNAPSHOT_TOOL'))
ENCODER = os.environ.get('XRPLA_HISTORY_ENCODER')
SNAPSHOTS_EVERY = int(os.environ.get('XRPLA_SNAPSHOTS_EVERY') or 0)
OPENSSL = os.environ.get('XRPLA_OPENSSL')  # set for one variant: the TLS test waits on a certificate reload
ACCOUNT_FIELDS = {'Account', 'Destination', 'Owner', 'Issuer', 'issuer', 'RegularKey', 'Authorize', 'Holder'}


def address(account_id):
    """A classic address (XRPL base58check) for 20 AccountID bytes."""
    import hashlib
    alphabet = 'rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz'
    body = b'\x00' + account_id
    data = body + hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    n, text = int.from_bytes(data, 'big'), ''
    while n: n, rest = divmod(n, 58); text = alphabet[rest] + text
    return alphabet[0] * (len(data) - len(data.lstrip(b'\x00'))) + text


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]


class Server:
    def __init__(self, directory, extra=()):
        self.port, self.ws_port = free_port(), free_port()
        self.process = subprocess.Popen([BINARY, 'serve', '--dir', str(directory), '--http', f'127.0.0.1:{self.port}',
                                         '--ws', f'127.0.0.1:{self.ws_port}', '--threads', '4', *map(str, extra)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for _ in range(400):
            try: socket.create_connection(('127.0.0.1', self.port), .1).close(); return
            except OSError:
                if self.process.poll() is not None: raise RuntimeError(self.process.communicate()[1].decode())
                time.sleep(.05)
        raise RuntimeError('Server did not start')

    def call(self, method, params):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=600)
        connection.request('POST', '/', json.dumps({'method': method, 'params': [params]}), {'Content-Type': 'application/json'})
        return json.loads(connection.getresponse().read())['result']

    def close(self):
        self.process.terminate()
        try: self.process.communicate(timeout=30)
        except subprocess.TimeoutExpired: self.process.kill(); self.process.communicate()


def walk(value, visit, name=None):
    if isinstance(value, dict):
        for k, v in value.items(): visit(k, v); walk(v, visit, k)
    elif isinstance(value, list):
        for v in value: walk(v, visit, name)


def issue(amount):
    return {'currency': 'XRP'} if isinstance(amount, str) else {'currency': amount['currency'], 'issuer': amount['issuer']} if 'currency' in amount else None


@unittest.skipUnless(BINARY and DECODER and SNAPSHOT, 'Compiled xrpla, decoder and snapshot tool supplied by CTest')
class PieceQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # XRPLA_PIECE_QUERY_WORK keeps the bucket, cache and store between runs; finished steps are skipped.
        keep = os.environ.get('XRPLA_PIECE_QUERY_WORK')
        cls.tmp = None if keep else tempfile.TemporaryDirectory(prefix='xrpla-piece-query-')
        root = Path(keep or cls.tmp.name); root.mkdir(parents=True, exist_ok=True)
        manifest = json.loads((ARCHIVE / 'manifest.json').read_bytes())
        first = manifest['chunks'][0]['first']
        cls.timings = {}
        done = {'publish': root / 'bucket' / 'catalog' / 'current.json', 'sync': root / 'cache' / 'selection.json',
                'fetch': root / 'store' / 'archive' / 'selection.json', 'build': root / 'store' / 'store'}

        def step(name, command):
            if name in done and done[name].exists(): return []
            started = time.time()
            result = subprocess.run([str(x) for x in command], capture_output=True, text=True, timeout=36000)
            if result.returncode: raise RuntimeError(f'{name} failed: {result.stdout[-3000:]}{result.stderr[-3000:]}')
            cls.timings[name] = round(time.time() - started, 2)
            try: return [json.loads(result.stdout)]
            except ValueError: return [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]

        seed = root / 'publisher.seed'
        if not seed.exists(): seed.write_bytes(os.urandom(32))
        bucket, cache, store = root / 'bucket', root / 'cache', root / 'store'
        encoder = ['--encoder', ENCODER] if ENCODER else []
        step('publish', [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', ARCHIVE / 'publisher.pub',
                         '--bucket', bucket, '--signing-key', seed, '--scratch', root / 'scratch', '--decoder', DECODER, '--snapshot-tool', SNAPSHOT,
                         '--piece-ledgers', PIECE, '--bundle-pieces', BUNDLE, '--workers', os.cpu_count() or 2, '--grace-seconds', 0, *encoder,
                         *(('--until', TAIL_AFTER) if TAIL_AFTER else ())])
        # xrpla sync: besides the base snapshot, the newest interval snapshot the ranges pass and,
        # with SNAPSHOTS_EVERY, every one at a multiple of it; each ledger reads its state from the
        # newest kept at or before it (tested by every comparison at those ledgers).
        step('sync', [BINARY, 'sync', '--source', bucket, '--publisher-key', bucket / 'publisher.pub', '--dir', cache,
                      *(('--snapshots-every', SNAPSHOTS_EVERY) if SNAPSHOTS_EVERY else ())])
        cls.index = step('index', [BINARY, 'index-pieces', '--dir', cache])[0]
        # The store holds exactly the cached ledgers, so range-dependent answers are comparable.
        cached_last = json.loads((cache / 'selection.json').read_bytes())['last']
        if TAIL_AFTER: cached_last = manifest['tip']['seq']  # the tail serves the rest of the archive
        step('fetch', [BINARY, 'fetch', '--archive', ARCHIVE, '--range', f'{first}-{cached_last}', '--dir', store, '--publisher-key', ARCHIVE / 'publisher.pub'])
        step('build', [BINARY, 'build', '--dir', store, '--publisher-key', ARCHIVE / 'publisher.pub'])
        tail = ('--tail', ARCHIVE, '--tail-key', ARCHIVE / 'publisher.pub') if TAIL_AFTER else ()
        cls.store, cls.pieces = Server(store), Server(cache, tail)
        cls.tail_args = tail
        cls.pieces_dir, cls.bucket = cache, bucket
        a, b = cls.store.call('ledger_range', {}), cls.pieces.call('ledger_range', {})
        cls.first, cls.last = max(a['ledger_index_min'], b['ledger_index_min']), min(a['ledger_index_max'], b['ledger_index_max'])
        if TAIL_AFTER:
            sealed = json.loads((cache / 'selection.json').read_bytes())['last']
            assert sealed < b['ledger_index_max'] == a['ledger_index_max'], (sealed, a, b)
            cls.tail = [sealed + 1, b['ledger_index_max']]
        cls.compared = 0

    @classmethod
    def tearDownClass(cls):
        print(json.dumps({'compared': cls.compared, 'range': [cls.first, cls.last], 'nft_info': getattr(cls, 'nft_tokens', None), 'tail': getattr(cls, 'tail', None), 'timings': cls.timings, 'index': cls.index}), file=sys.stderr)
        cls.store.close(); cls.pieces.close()
        if cls.tmp: cls.tmp.cleanup()

    def same(self, method, **params):
        expected, actual = self.store.call(method, params), self.pieces.call(method, params)
        type(self).compared += 1
        self.assertEqual(actual, expected, f'{method} {json.dumps(params)}')
        return expected

    def ledgers(self):
        span = list(range(self.first, self.last + 1))
        if len(span) <= MAX_LEDGERS: return span
        rng = random.Random(7)
        return sorted({self.first, self.last, *rng.sample(span, MAX_LEDGERS - 2)})

    def survey(self):
        """Accounts, books, objects, NFTs, AMMs and transactions found in the sampled ledgers."""
        if hasattr(type(self), 'found'): return type(self).found
        found = {'accounts': set(), 'books': {}, 'keys': [], 'deleted': [], 'nfts': set(), 'amms': {}, 'txs': []}
        for seq in self.ledgers():
            ledger = self.store.call('ledger', {'ledger_index': seq, 'transactions': True, 'expand': True, 'api_version': 2})['ledger']
            for tx in ledger['transactions']:
                found['txs'].append((tx['hash'], seq))
                def visit(k, v):
                    if k in ACCOUNT_FIELDS and isinstance(v, str) and v.startswith('r'): found['accounts'].add(v)
                    if k in ('NFTokenID', 'nftoken_id') and isinstance(v, str): found['nfts'].add(v)
                walk(tx, visit)
                for node in tx['meta'].get('AffectedNodes', []):
                    body = next(iter(node.values())); fields = body.get('FinalFields') or body.get('NewFields') or {}
                    if body.get('LedgerEntryType') == 'Offer' and 'TakerGets' in fields:
                        pays, gets = issue(fields['TakerPays']), issue(fields['TakerGets'])
                        if pays and gets: found['books'][json.dumps([pays, gets], sort_keys=True)] = seq
                    if body.get('LedgerEntryType') == 'AMM' and 'Asset' in fields:
                        found['amms'][fields['Account']] = (fields['Asset'], fields.get('Asset2'), seq)
            diff = self.store.call('ledger', {'ledger_index': seq, 'diff': True, 'binary': True}).get('ledger', {}).get('diff', [])  # none for a baseline ledger
            found['keys'] += [(d['object_id'], seq) for d in diff]
            found['deleted'] += [(d['object_id'], seq) for d in diff if not d['object']]
        type(self).found = found
        return found

    @staticmethod
    def usable_snapshots(selection):
        """The interval snapshots a cache can keep: after the base, before the last ledger, a range starting right after."""
        snapshots = sorted(s['seq'] for s in selection['catalog']['snapshots'])
        return [s for s in snapshots if selection['snapshot']['seq'] < s < selection['last'] and any(r['first'] == s + 1 for r in selection['ranges'])]

    def test_recent_snapshot_is_selected(self):
        selection = json.loads((self.pieces_dir / 'selection.json').read_bytes())
        later = self.usable_snapshots(selection)
        if not later: self.skipTest('the publication has one snapshot within the cached ledgers')
        self.assertEqual(selection.get('recent', {}).get('seq'), later[-1])
        # Every one at a multiple of SNAPSHOTS_EVERY as well, each with its state index.
        kept = sorted({later[-1], *(s for s in later if SNAPSHOTS_EVERY and s % SNAPSHOTS_EVERY == 0)})
        self.assertEqual([s['seq'] for s in selection['snapshots']], kept)
        for seq in kept: self.assertTrue((self.pieces_dir / 'query' / f'snapshot-{seq}.xqss').exists(), seq)

    def test_state_around_interval_snapshots(self):
        # Ledgers before, at and after each kept snapshot read their state from different
        # snapshots (the one before, or the base, then that one): the answers are the store's.
        selection = json.loads((self.pieces_dir / 'selection.json').read_bytes())
        if not selection.get('snapshots'): self.skipTest('the cache keeps no interval snapshot')
        found = self.survey()
        for snapshot in selection['snapshots']:
            for seq in sorted({max(self.first, snapshot['seq'] - 1), snapshot['seq'], min(self.last, snapshot['seq'] + 1)}):
                for account in sorted(found['accounts'])[:40]:
                    for method in ('account_info', 'account_lines', 'account_objects', 'account_offers', 'gateway_balances'):
                        self.same(method, account=account, ledger_index=seq)
                for key, _ in found['keys'][:400]:
                    self.same('ledger_entry', index=key, ledger_index=seq, binary=True)
                for book in list(found['books'])[:20]:
                    pays, gets = json.loads(book)
                    self.same('book_offers', taker_pays=pays, taker_gets=gets, ledger_index=seq, limit=20)
                page = self.same('ledger_data', ledger_index=seq, binary=True, limit=256)
                while 'marker' in page: page = self.same('ledger_data', ledger_index=seq, binary=True, limit=256, marker=page['marker'])

    def test_interval_snapshots_reselected(self):
        # A copy of the cache synced to keep every interval snapshot, then none but the newest:
        # the others' files go (their index, state index and parts no kept snapshot shares). A
        # cache synced before `snapshots` was written (the newest as `recent` alone) still opens.
        later = self.usable_snapshots(json.loads((self.pieces_dir / 'selection.json').read_bytes()))
        if len(later) < 2: self.skipTest('the publication has fewer than two interval snapshots within the cached ledgers')
        copy = Path(tempfile.mkdtemp(prefix='xrpla-snapshots-'))
        try:
            cache = copy / 'cache'
            shutil.copytree(self.pieces_dir, cache)
            def run(*args): return json.loads(subprocess.run([str(a) for a in args], capture_output=True, text=True, check=True).stdout)
            def sync(every): return run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.bucket / 'publisher.pub', '--dir', cache, '--snapshots-every', every)
            self.assertEqual(sync(1)['interval_snapshots'], later)
            every = json.loads((cache / 'selection.json').read_bytes())
            self.assertEqual([s['seq'] for s in every['snapshots']], later)
            for seq in later: self.assertTrue((cache / 'query' / f'snapshot-{seq}.xqss').exists(), seq)
            result = sync(0)
            self.assertEqual(result['interval_snapshots'], [later[-1]])
            selection = json.loads((cache / 'selection.json').read_bytes())
            self.assertEqual(([s['seq'] for s in selection['snapshots']], selection['recent']['seq']), ([later[-1]], later[-1]))
            for s in every['snapshots'][:-1]:
                self.assertFalse((cache / 'query' / f'snapshot-{s["seq"]}.xqss').exists(), s['seq'])
                self.assertFalse((cache / s['index']['object']).exists(), s['seq'])
            kept = {selection['snapshot']['index']['object'], selection['recent']['index']['object']}
            for s in (selection['snapshot'], selection['recent']):
                kept |= {p['object'] for p in json.loads((cache / s['index']['object']).read_bytes())['parts']}
            self.assertEqual(sorted(f.relative_to(cache).as_posix() for f in (cache / 'snapshots').rglob('*') if f.is_file()), sorted(kept))
            self.assertTrue(result['removed'])
            # As the previous build wrote it: the newest snapshot as `recent` alone.
            del selection['snapshots']
            (cache / 'selection.json').write_text(json.dumps(selection))
            request = copy / 'request.json'
            for account in sorted(self.survey()['accounts'])[:10]:
                for seq in sorted({self.first, later[0], later[-1], selection['last']}):  # without a tail
                    params = {'account': account, 'ledger_index': seq}
                    request.write_text(json.dumps({'method': 'account_objects', 'params': [params]}))
                    self.assertEqual(run(BINARY, 'rpc', '--dir', cache, '--request', request), self.store.call('account_objects', params), params)
        finally:
            shutil.rmtree(copy, ignore_errors=True)

    def test_ledgers(self):
        for seq in self.ledgers():
            header = self.same('ledger', ledger_index=seq)
            self.same('ledger', ledger_hash=header['ledger_hash'])
            for version in (1, 2):
                self.same('ledger', ledger_index=seq, transactions=True, api_version=version)
                self.same('ledger', ledger_index=seq, transactions=True, expand=True, api_version=version)
                self.same('ledger', ledger_index=seq, transactions=True, expand=True, binary=True, api_version=version)
            self.same('ledger', ledger_index=seq, diff=True)
            self.same('ledger', ledger_index=seq, diff=True, binary=True)
            self.same('ledger_closed', ledger_index=seq)
            self.same('_ledger_event', ledger_index=seq)
        self.same('ledger_range')
        self.same('ledger', ledger_index=self.last + 1)

    def test_baseline_ledger_without_transactions_is_served(self):
        # The base snapshot's ledger is served whole by both servers when it has no transactions
        # (mainnet's first, 32,570), and not at all when it has some.
        snapshot = json.loads((self.pieces_dir / 'selection.json').read_bytes())['snapshot']
        index = json.loads((self.pieces_dir / snapshot['index']['object']).read_bytes())
        empty = index['ledger_header'][2 * 44:2 * 76] == '0' * 64  # the header's transaction tree hash (after sequence, drops, parent)
        seq = snapshot['seq']
        self.assertEqual(self.pieces.call('ledger_range', {})['ledger_index_min'], seq if empty else seq + 1)
        answer = self.same('ledger', ledger_index=seq, transactions=True)
        if not empty: self.assertEqual(answer.get('error'), 'lgrNotFound'); return
        self.assertEqual((answer['ledger']['ledger_hash'].lower(), answer['ledger']['transactions']), (snapshot['ledger_hash'], []))
        self.same('ledger', ledger_hash=answer['ledger']['ledger_hash'])
        self.same('ledger_data', ledger_index=seq, limit=50)
        accounts = [o['Account'] for o in self.store.call('ledger_data', {'ledger_index': seq, 'type': 'account', 'limit': 5}).get('state', [])]
        for account in accounts:
            self.same('account_info', account=account, ledger_index=seq)
            self.same('account_tx', account=account, ledger_index_min=seq, ledger_index_max=seq + 1)
        self.same('book_changes', ledger_index=seq)
        self.same('ledger', ledger_index=seq, diff=True)  # its changes from its parent are not held
        self.same('ledger', ledger_index=seq - 1)

    def test_transactions(self):
        txs = self.survey()['txs']
        self.assertTrue(txs, 'archive has no transactions to compare')
        for tx, _ in txs[:4000]:
            for version in (1, 2):
                self.same('tx', transaction=tx, api_version=version)
                self.same('tx', transaction=tx, binary=True, api_version=version)
        self.same('tx', transaction='00' * 32)

    def test_account_history(self):
        accounts = sorted(self.survey()['accounts'])
        for account in accounts[:300]:
            for forward in (False, True):
                page = self.same('account_tx', account=account, limit=3, forward=forward, api_version=2)
                for _ in range(3):
                    if 'marker' not in page: break
                    page = self.same('account_tx', account=account, limit=3, forward=forward, marker=page['marker'], api_version=2)
            self.same('account_tx', account=account, api_version=1)
            self.same('account_tx', account=account, binary=True, limit=20)
            self.same('account_tx', account=account, ledger_index_min=self.first, ledger_index_max=(self.first + self.last) // 2)
        for nft in sorted(self.survey()['nfts'])[:100]:
            self.same('nft_history', nft_id=nft, limit=5)

    def test_transaction_reads_match_whole_ledgers(self):
        # tx, account_tx and nft_history decode their ledgers without object changes, and a ledger
        # decoded whole serves them from then on. On a server of its own, so the order is known:
        # every read first from transaction-only decodes, then each ledger they name decoded whole
        # (its diff), then every read again from the whole ledgers; both equal to the store's.
        server = Server(self.pieces_dir, self.tail_args)
        try:
            found = self.survey()
            reads = [('tx', {'transaction': tx, 'binary': binary, 'api_version': 2}) for tx, _ in found['txs'][:200] for binary in (False, True)]
            reads += [('account_tx', {'account': account, 'limit': 400, 'forward': True, 'binary': binary, 'api_version': 2})
                      for account in sorted(found['accounts'])[:20] for binary in (False, True)]
            reads += [('nft_history', {'nft_id': nft, 'limit': 100, 'forward': True, 'api_version': 2}) for nft in sorted(found['nfts'])[:20]]
            cold = [server.call(method, params) for method, params in reads]
            ledgers = {seq for _, seq in found['txs'][:200]}
            for (method, params), answer in zip(reads, cold):
                if method != 'tx': ledgers.update(entry['ledger_index'] for entry in answer.get('transactions', []))
            for seq in sorted(ledgers): server.call('ledger', {'ledger_index': seq, 'diff': True, 'binary': True})
            for (method, params), answer in zip(reads, cold):
                self.assertEqual(server.call(method, params), answer, f'{method} {json.dumps(params)}')
                self.assertEqual(answer, self.store.call(method, params), f'{method} {json.dumps(params)}')
        finally:
            server.close()

    def test_nft_history_as_clio(self):
        # Clio 2.8's nft_history (NFTHistory.cpp): a marker follows every full page, whether or not
        # more remain (forward the last entry's TransactionIndex plus one, back the last entry's);
        # the limit is clamped to 1-100, default 50, and answered only when asked; a bound outside
        # the held ledgers is refused and -1 is none; no ctid, inLedger or NFToken synthetic fields.
        nfts = sorted(self.survey()['nfts'])[:20]
        if not nfts: self.skipTest('archive has no NFT transactions')
        def walk(nft, limit, forward):
            listed, params = [], {'nft_id': nft, 'limit': limit, 'forward': forward, 'api_version': 2}
            for _ in range(1000):
                page = self.same('nft_history', **params)
                entries = page['transactions']
                listed += [(e['ledger_index'], e['meta']['TransactionIndex'], e['hash']) for e in entries]
                self.assertEqual('marker' in page, len(entries) == limit, page.get('marker'))
                if 'marker' not in page: return listed
                last = entries[-1]
                self.assertEqual(page['marker'], {'ledger': last['ledger_index'], 'seq': last['meta']['TransactionIndex'] + (1 if forward else 0)})
                params['marker'] = page['marker']
            self.fail(f'{nft}: pages do not end')
        for nft in nfts:
            whole = walk(nft, 100, True)
            self.assertEqual(whole, sorted(whole))
            self.assertEqual(len(set(whole)), len(whole))
            for limit in (1, 2, 3):
                self.assertEqual(walk(nft, limit, True), whole)
                self.assertEqual(walk(nft, limit, False), whole[::-1])
            page = self.same('nft_history', nft_id=nft)
            self.assertNotIn('limit', page)
            self.assertLessEqual(len(page['transactions']), 50)
            self.assertEqual(self.same('nft_history', nft_id=nft, limit=5000)['limit'], 100)
            self.assertEqual(self.same('nft_history', nft_id=nft, ledger_index_min=-1, ledger_index_max=-1)['transactions'], page['transactions'])
            for entry in self.same('nft_history', nft_id=nft, limit=10)['transactions']:
                self.assertFalse({'ctid', 'inLedger'} & set(entry['tx']), entry['tx'])
                self.assertFalse({'nftoken_id', 'nftoken_ids', 'offer_id'} & set(entry['meta']), entry['meta'])
            for entry in self.same('nft_history', nft_id=nft, limit=10, binary=True)['transactions']:
                self.assertIsInstance(entry['date'], int)
        nft = nfts[0]
        self.assertEqual(self.same('nft_history', nft_id=nft, limit=0)['error'], 'invalidParams')
        self.assertEqual(self.same('nft_history', nft_id=nft, ledger_index_min=self.last + 1)['error_message'], 'ledgerSeqMinOutOfRange')
        self.assertEqual(self.same('nft_history', nft_id=nft, ledger_index_max=self.last + 1)['error_message'], 'ledgerSeqMaxOutOfRange')
        self.assertEqual(self.same('nft_history', nft_id=nft, ledger_index_min=self.last, ledger_index_max=self.first)['error'], 'lgrIdxsInvalid')
        self.assertEqual(self.same('nft_history', nft_id=nft, ledger_index=self.last, ledger_index_min=self.first)['error_message'], 'containsLedgerSpecifierAndRange')

    def test_ledger_named_as_xrpld_reads_it(self):
        # The deprecated `ledger` field, as xrpld's ledgerFromRequest reads it: an index (number or
        # string) or a 64-character hash, and only one of `ledger`, `ledger_hash` and `ledger_index`.
        seq = (self.first + self.last) // 2
        by_index = self.pieces.call('ledger', {'ledger_index': seq})
        ledger_hash = by_index['ledger_hash']
        for named in ({'ledger': seq}, {'ledger': str(seq)}, {'ledger': ledger_hash}):
            self.assertEqual(self.same('ledger', **named), by_index, named)
            self.assertEqual(self.pieces.call('ledger', {**named, 'transactions': True})['ledger_index'], seq)
        self.same('account_info', account=sorted(self.survey()['accounts'])[0], ledger=seq)
        both = self.pieces.call('ledger', {'ledger': seq, 'ledger_index': seq})
        self.assertEqual((both['error'], both['error_message']), ('invalidParams', "Exactly one of 'ledger', 'ledger_hash', or 'ledger_index' can be specified."))
        both = self.pieces.call('ledger', {'ledger_hash': ledger_hash, 'ledger_index': seq})
        self.assertEqual((both['error'], both['error_message']), ('invalidParams', "Exactly one of 'ledger_hash' or 'ledger_index' can be specified."))
        for value, message in (([seq], "Invalid field 'ledger', not string or number."), ('abc', "Invalid field 'ledger', not string or number."),
                               (-1, "Invalid field 'ledger', not string or number."), ('z' * 64, "Invalid field 'ledger', not hex string.")):
            wrong = self.pieces.call('ledger', {'ledger': value})
            self.assertEqual((wrong['error'], wrong['error_message']), ('invalidParams', message), value)
        # Methods xrpld answers without ledgerFromRequest take no `ledger`: account_tx reads its own
        # range and ledger_closed none.
        account = sorted(self.survey()['accounts'])[0]
        self.assertEqual(self.same('account_tx', account=account, ledger=[seq], limit=5), self.same('account_tx', account=account, limit=5))
        self.assertEqual(self.same('ledger_closed', ledger=[seq]), self.same('ledger_closed'))

    def test_account_history_filters_as_xrpld(self):
        account = sorted(self.survey()['accounts'])[0]
        # A ledger and a range together: refused in API v2, the range read in v1.
        refused = self.pieces.call('account_tx', {'account': account, 'ledger_index': self.last, 'ledger_index_min': self.first, 'api_version': 2})
        self.assertEqual((refused['error'], refused['error_message']), ('invalidParams', 'invalidParams'))
        ranged = self.same('account_tx', account=account, ledger_index=self.last, ledger_index_min=self.first, ledger_index_max=self.last, api_version=1)
        self.assertEqual((ranged['ledger_index_min'], ranged['ledger_index_max']), (self.first, self.last))
        # In v1 the ledger beside a range is not even looked up; in v2 the pair is refused first.
        for ledger in ('abc', 4000000000):
            ranged = self.same('account_tx', account=account, ledger_index=ledger, ledger_index_min=self.first, ledger_index_max=self.last, api_version=1)
            self.assertEqual((ranged['ledger_index_min'], ranged['ledger_index_max']), (self.first, self.last))
            refused = self.pieces.call('account_tx', {'account': account, 'ledger_index': ledger, 'ledger_index_min': self.first, 'api_version': 2})
            self.assertEqual((refused['error'], refused['error_message']), ('invalidParams', 'invalidParams'))
        # The delegate filter is checked before the named ledger is looked up.
        early = self.pieces.call('account_tx', {'account': account, 'ledger_index': 4000000000, 'delegate': {}})
        self.assertEqual(early['error_message'], "Invalid field 'delegate_filter'.")
        # The delegate filter (xrpld 3.4.1): checked as xrpld checks it, and its markers kept apart.
        for delegate, error, message in (('x', 'invalidParams', "Invalid field 'delegate'."),
                                         ({}, 'invalidParams', "Invalid field 'delegate_filter'."),
                                         ({'delegate_filter': 'both'}, 'invalidParams', "Invalid field 'delegate_filter'."),
                                         ({'delegate_filter': 'actor', 'counter_party': 7}, 'invalidParams', "Invalid field 'counter_party'."),
                                         ({'delegate_filter': 'actor', 'counter_party': 'r'}, 'actMalformed', None)):
            answer = self.pieces.call('account_tx', {'account': account, 'delegate': delegate})
            self.assertEqual(answer['error'], error, delegate)
            if message: self.assertEqual(answer['error_message'], message, delegate)
        plain = self.pieces.call('account_tx', {'account': account, 'limit': 1, 'forward': True})
        if 'marker' in plain:
            # Either way round: a plain marker with a delegate filter, a delegate marker without one.
            for params in ({'marker': plain['marker'], 'delegate': {'delegate_filter': 'actor'}}, {'marker': {**plain['marker'], 'delegate': True}}):
                mixed = self.pieces.call('account_tx', {'account': account, **params})
                self.assertEqual(mixed['error'], 'invalidParams', params)
                self.assertIn('Do not mix delegate and non-delegate pagination markers', mixed['error_message'])
        for kind in ('actor', 'authorizer'):
            # This archive's transactions carry no Delegate field: every page is empty, and a full
            # read returns the last row read as the marker, flagged as a delegate marker, which
            # continues the walk to the end of the account's history.
            params = {'account': account, 'delegate': {'delegate_filter': kind}, 'limit': 2, 'forward': True, 'api_version': 2}
            # Each read is 3 rows from the marker's own (skipped), so the markers step 2 rows on and
            # the walk ends; the live comparison with xrpld covers transactions that pass the filter.
            page, markers = self.pieces.call('account_tx', params), []
            while 'marker' in page:
                self.assertEqual(page['transactions'], [])
                self.assertIs(page['marker']['delegate'], True)
                markers.append((page['marker']['ledger'], page['marker']['seq']))
                page = self.pieces.call('account_tx', {**params, 'marker': page['marker']})
            self.assertEqual(page['transactions'], [])
            self.assertEqual(markers, sorted(set(markers)))

    def refused(self, method, code, message=None, **params):
        """An error, alike from both backends, with xrpld 3.4.1's code (and message)."""
        answer = self.same(method, **params)
        self.assertEqual(answer.get('error'), code, f'{method} {json.dumps(params)}: {answer}')
        if message is not None: self.assertEqual(answer.get('error_message'), message, f'{method} {json.dumps(params)}')
        return answer

    def existing(self):
        """The surveyed accounts that exist at the newest ledger."""
        return [a for a in sorted(self.survey()['accounts']) if 'error' not in self.store.call('account_info', {'account': a})][:20]

    def test_limits_as_xrpld(self):
        # readLimitField for a public caller: zero refused, out of range clamped (account_tx 10-400,
        # of which a JSON page reads at most 200 rows).
        account = self.existing()[0]
        for method, extra in (('account_lines', {}), ('account_objects', {}), ('account_offers', {}), ('account_channels', {}),
                              ('account_nfts', {}), ('account_tx', {}), ('noripple_check', {'role': 'user'})):
            self.refused(method, 'invalidParams', "Invalid field 'limit'.", account=account, limit=0, **extra)
            self.refused(method, 'invalidParams', "Invalid field 'limit', not unsigned integer.", account=account, limit=-1, **extra)
        self.refused('book_offers', 'invalidParams', "Invalid field 'limit'.", taker_pays={'currency': 'XRP'},
                     taker_gets={'currency': 'USD', 'issuer': account}, limit=0)
        self.refused('nft_buy_offers', 'invalidParams', "Invalid field 'limit'.", nft_id='00' * 32, limit=0)
        self.assertEqual(self.same('account_tx', account=account, limit=1)['limit'], 10)
        self.assertEqual(self.same('account_tx', account=account, limit=1000)['limit'], 400)
        self.assertLessEqual(len(self.same('account_tx', account=account, limit=400)['transactions']), 200)
        # Clio's own account_mptokens refuses zero as Clio does.
        self.refused('account_mptokens', 'invalidParams', 'Invalid parameters.', account=account, limit=0)

    def test_book_offers_errors_as_xrpld(self):
        account = self.existing()[0]
        usd = {'currency': 'USD', 'issuer': account}
        xrp = {'currency': 'XRP'}
        one = 'rrrrrrrrrrrrrrrrrrrrBZbvji'  # ACCOUNT_ONE
        cases = [
            ({'taker_gets': xrp}, 'invalidParams', "Missing field 'taker_pays'."),
            ({'taker_pays': xrp}, 'invalidParams', "Missing field 'taker_gets'."),
            ({'taker_pays': 'XRP', 'taker_gets': usd}, 'invalidParams', "Invalid field 'taker_pays', not object."),
            ({'taker_pays': xrp, 'taker_gets': 5}, 'invalidParams', "Invalid field 'taker_gets', not object."),
            ({'taker_pays': {}, 'taker_gets': usd}, 'invalidParams', "Missing field 'taker_pays.currency'."),
            ({'taker_pays': None, 'taker_gets': usd}, 'invalidParams', "Missing field 'taker_pays.currency'."),
            ({'taker_pays': {'currency': 5}, 'taker_gets': usd}, 'invalidParams', "Invalid field 'taker_pays.currency', not string."),
            ({'taker_pays': {'mpt_issuance_id': '00' * 24, 'currency': 'USD'}, 'taker_gets': usd}, 'invalidParams', "Invalid field 'taker_pays'."),
            ({'taker_pays': {'currency': 'XXXX'}, 'taker_gets': usd}, 'srcCurMalformed', "Invalid field 'taker_pays.currency', bad currency."),
            ({'taker_pays': xrp, 'taker_gets': {'currency': 'XXXX', 'issuer': account}}, 'dstAmtMalformed', "Invalid field 'taker_gets.currency', bad currency."),
            ({'taker_pays': {'mpt_issuance_id': 'zz'}, 'taker_gets': usd}, 'srcCurMalformed', "Invalid field 'taker_pays.mpt_issuance_id'"),
            ({'taker_pays': {'currency': 'USD'}, 'taker_gets': xrp}, 'srcIsrMalformed', "Invalid field 'taker_pays.issuer', expected non-XRP issuer."),
            ({'taker_pays': {'currency': 'USD', 'issuer': 'bad'}, 'taker_gets': xrp}, 'srcIsrMalformed', "Invalid field 'taker_pays.issuer', bad issuer."),
            ({'taker_pays': {'currency': 'USD', 'issuer': 7}, 'taker_gets': xrp}, 'invalidParams', "Invalid field 'taker_pays.issuer', not string."),
            ({'taker_pays': {'currency': 'USD', 'issuer': one}, 'taker_gets': xrp}, 'srcIsrMalformed', "Invalid field 'taker_pays.issuer', bad issuer account one."),
            ({'taker_pays': {'currency': 'XRP', 'issuer': account}, 'taker_gets': usd}, 'srcIsrMalformed', "Unneeded field 'taker_pays.issuer' for XRP currency specification."),
            ({'taker_pays': usd, 'taker_gets': {'currency': 'EUR'}}, 'dstIsrMalformed', "Invalid field 'taker_gets.issuer', expected non-XRP issuer."),
            ({'taker_pays': usd, 'taker_gets': xrp, 'taker': 5}, 'invalidParams', "Invalid field 'taker', not string."),
            ({'taker_pays': usd, 'taker_gets': xrp, 'taker': 'bad'}, 'invalidParams', "Invalid field 'taker'."),
            ({'taker_pays': usd, 'taker_gets': xrp, 'domain': 'zz'}, 'domainMalformed', 'Unable to parse domain.'),
            ({'taker_pays': usd, 'taker_gets': xrp, 'domain': 5}, 'domainMalformed', 'Unable to parse domain.'),
            ({'taker_pays': usd, 'taker_gets': usd}, 'badMarket', None),
        ]
        for params, code, message in cases:
            self.refused('book_offers', code, message, ledger_index=self.last, **params)
        self.assertNotIn('error', self.same('book_offers', taker_pays=xrp, taker_gets=usd, taker=account))

    def test_account_methods_as_xrpld(self):
        accounts = self.existing()
        account = accounts[0]
        # account_channels: an empty destination filters nothing; one not a string is refused.
        self.assertEqual(self.same('account_channels', account=account, destination_account=''), self.same('account_channels', account=account))
        self.refused('account_channels', 'invalidParams', "Invalid field 'destination_account'.", account=account, destination_account=5)
        self.refused('account_channels', 'actMalformed', None, account=account, destination_account='bad')
        # gateway_balances' hotwallet: invalidHotWallet in API v1, invalidParams in v2, on top of
        # the ledger and account; null is no hot wallet.
        for wallets in ('bad', [5], [account, 'bad'], 5, {}):
            answer = self.refused('gateway_balances', 'invalidHotWallet', None, account=account, hotwallet=wallets)
            self.assertEqual(answer['account'], account)
            self.assertIn('ledger_index', answer)
            self.refused('gateway_balances', 'invalidParams', None, account=account, hotwallet=wallets, api_version=2)
        self.assertEqual(self.same('gateway_balances', account=account, hotwallet=None), self.same('gateway_balances', account=account))
        self.assertNotIn('error', self.same('gateway_balances', account=account, hotwallet=accounts[-1]))
        # Booleans xrpld reads with asBool, and those it checks in API v2.
        self.assertEqual(self.same('account_lines', account=account, ignore_default=1), self.same('account_lines', account=account, ignore_default=True))
        self.assertEqual(self.same('account_objects', account=account, deletion_blockers_only='x'),
                         self.same('account_objects', account=account, deletion_blockers_only=True))
        self.assertEqual(self.same('account_objects', account=account, deletion_blockers_only=0), self.same('account_objects', account=account))
        self.refused('account_info', 'invalidParams', None, account=account, signer_lists='x', api_version=2)
        self.assertIn('signer_lists', self.same('account_info', account=account, signer_lists='x')['account_data'])
        self.refused('account_info', 'invalidParams', None, account=account, queue=True, ledger_index=self.last)
        self.refused('noripple_check', 'invalidParams', "Invalid field 'transactions'.", account=account, role='user', transactions='x', api_version=2)
        self.assertIn('transactions', self.same('noripple_check', account=account, role='user', transactions='x'))
        # noripple_check's checks come before the ledger is looked up, in xrpld's order.
        self.refused('noripple_check', 'invalidParams', "Invalid field 'account'.", account=5, role='user', ledger_index=4000000000)
        self.refused('noripple_check', 'invalidParams', "Invalid field 'role'.", account=account, role='x', ledger_index=4000000000)
        self.refused('noripple_check', 'invalidParams', "Invalid field 'limit'.", account=account, role='user', limit=0, ledger_index=4000000000)
        malformed = self.refused('noripple_check', 'actMalformed', None, account='bad', role='user', transactions=True)
        self.assertEqual((malformed['transactions'], malformed['ledger_index']), ([], self.last))
        key = self.same('ledger_entry', account_root=account)['index']
        self.assertIn('node_binary', self.same('ledger_entry', index=key, binary=1))
        self.assertIn('node_binary', self.same('ledger_entry', index=key, binary='x', api_version=2))
        self.assertIn('node', self.same('ledger_entry', index=key, binary=0))
        # deposit_authorized with credentials, checked as xrpld checks them.
        both = {'source_account': accounts[0], 'destination_account': accounts[-1]}
        self.refused('deposit_authorized', 'actMalformed', None, source_account='bad')
        for credentials, message in (([], 'is non-empty array of CredentialID(hash256)'), ('x', 'is non-empty array of CredentialID(hash256)'),
                                     (['zz'], 'an array of CredentialID(hash256)'), ([5], 'an array of CredentialID(hash256)'),
                                     (['00' * 32] * 9, 'array too long')):
            self.refused('deposit_authorized', 'invalidParams', f"Invalid field 'credentials', not {message}.", credentials=credentials, **both)
        missing = self.refused('deposit_authorized', 'badCredentials', "credentials don't exist", credentials=['00' * 32], **both)
        self.assertEqual(missing['ledger_index'], self.last)
        # Clio's ledger_index: validated, and its message for a date before the first ledger.
        self.assertIs(self.same('ledger_index')['validated'], True)
        self.refused('ledger_index', 'lgrNotFound', 'ledgerNotInRange', date='2000-01-02T00:00:00Z')

    def test_tx_as_xrpld(self):
        txs = self.survey()['txs']
        self.assertTrue(txs)
        tx, seq = txs[0]
        self.refused('tx', 'invalidParams', 'Invalid parameters.')
        self.refused('tx', 'notImpl', None, transaction='zz')
        self.refused('tx', 'notImpl', None, transaction=5)
        found = self.same('tx', transaction=tx)
        self.assertEqual(self.same('tx', transaction=tx, binary=1), self.same('tx', transaction=tx, binary=True))
        self.assertEqual(self.same('tx', transaction=tx, binary='x', api_version=2), self.same('tx', transaction=tx, binary=True, api_version=2))
        if 'ctid' in found:
            ctid = found['ctid']
            self.refused('tx', 'invalidParams', 'Invalid parameters.', transaction=tx, ctid=ctid)
            for version in (1, 2):
                self.assertEqual(self.same('tx', ctid=ctid, api_version=version), self.same('tx', transaction=tx, api_version=version))
            self.assertEqual(self.same('tx', ctid=ctid.lower()), found)
            other = ctid[:12] + '0001'
            self.refused('tx', 'wrongNetwork', 'Wrong network. You should submit this request to a node running on NetworkID: 1', ctid=other)
            missing = ctid[:8] + 'FFFF' + ctid[12:]  # an index no transaction has
            self.refused('tx', 'txnNotFound', None, ctid=missing)
        for bad in ('zz', 'C' * 15, '8' + '0' * 15, 12):
            self.refused('tx', 'invalidParams', 'Invalid parameters.', ctid=bad)
        # min_ledger and max_ledger: checked, and a transaction not found says whether every
        # ledger between holds rows (xrpld counts the ledgers its Transactions table has rows of).
        unknown = '00' * 32
        self.refused('tx', 'invalidLgrRange', None, transaction=unknown, min_ledger=self.last, max_ledger=self.last - 1)
        self.refused('tx', 'excessiveLgrRange', None, transaction=unknown, min_ledger=1, max_ledger=1002)
        self.refused('tx', 'invalidLgrRange', None, transaction=unknown, min_ledger='abc', max_ledger=self.last)
        self.refused('tx', 'invalidLgrRange', None, transaction=unknown, min_ledger=-1, max_ledger=self.last)
        self.assertNotIn('searched_all', self.same('tx', transaction=unknown))
        self.assertIs(self.refused('tx', 'txnNotFound', None, transaction=unknown, min_ledger=self.last + 1, max_ledger=self.last + 2)['searched_all'], False)
        if self.last - self.first <= 1000:
            nonempty = all(int(self.same('ledger', ledger_index=s)['ledger']['transaction_hash'], 16) for s in range(self.first, self.last + 1))
            self.assertIs(self.refused('tx', 'txnNotFound', None, transaction=unknown, min_ledger=self.first, max_ledger=self.last)['searched_all'], nonempty)
        self.assertEqual(self.same('tx', transaction=tx, min_ledger=self.first, max_ledger=self.first)['hash'], found['hash'])

    def test_account_tx_as_xrpld(self):
        account = max(sorted(self.survey()['accounts']), key=lambda a: len(self.store.call('account_tx', {'account': a, 'limit': 400})['transactions']))
        whole = self.same('account_tx', account=account, limit=400, forward=True, api_version=2)['transactions']
        # Bounds: in API v2 one outside the ledgers held is lgrIdxMalformed (0 and -1 are not), and an
        # inverted range lgrIdxsInvalid in v1, invalidLgrRange in v2; other negative bounds are defaults.
        self.refused('account_tx', 'lgrIdxMalformed', None, account=account, ledger_index_max=self.last + 5, api_version=2)
        if self.first > 1: self.refused('account_tx', 'lgrIdxMalformed', None, account=account, ledger_index_min=self.first - 1, api_version=2)
        self.assertEqual(self.same('account_tx', account=account, ledger_index_max=self.last + 5)['ledger_index_max'], self.last)
        for version, code in ((1, 'lgrIdxsInvalid'), (2, 'invalidLgrRange')):
            self.refused('account_tx', code, None, account=account, ledger_index_min=self.last, ledger_index_max=self.last - 1, api_version=version)
        defaults = self.same('account_tx', account=account, api_version=2)
        for low in (0, -1, -7):
            self.assertEqual(self.same('account_tx', account=account, ledger_index_min=low, ledger_index_max=-3, api_version=2), defaults)
        # The marker: checked as xrpld checks it; ledger 0 is none; one naming no row reads nothing.
        message = 'invalid marker. Provide ledger index via ledger field, and transaction sequence number via seq field'
        for marker in ('x', {'ledger': self.first}, {'ledger': 'x', 'seq': 0}, {'ledger': self.first, 'seq': -1}):
            self.refused('account_tx', 'invalidParams', message, account=account, marker=marker)
        self.assertEqual(self.same('account_tx', account=account, marker={'ledger': 0, 'seq': 5}), self.same('account_tx', account=account))
        nothing = self.same('account_tx', account=account, marker={'ledger': self.last, 'seq': 65535}, forward=True)
        self.assertEqual(nothing['transactions'], [])
        self.assertNotIn('marker', nothing)
        if 1 < len(whole) < 400:
            # A marker past the range's bound on its side is read from all the same.
            row = whole[-1]
            marker = {'ledger': row['ledger_index'], 'seq': self.same('tx', transaction=row['hash'])['meta']['TransactionIndex']}
            page = self.same('account_tx', account=account, marker=marker, forward=True, ledger_index_max=self.first, api_version=2)
            self.assertEqual([t['hash'] for t in page['transactions']], [row['hash']])
            self.assertEqual(page['ledger_index_max'], self.first)
        # Booleans: v2 checks them, v1 reads them with asBool.
        self.refused('account_tx', 'invalidParams', "Invalid field 'binary'.", account=account, binary='x', api_version=2)
        self.refused('account_tx', 'invalidParams', "Invalid field 'forward'.", account=account, forward=1, api_version=2)
        self.assertEqual(self.same('account_tx', account=account, binary='x', forward=1), self.same('account_tx', account=account, binary=True, forward=True))
        # A named ledger, checked as parseLedgerArgs checks it.
        self.refused('account_tx', 'invalidParams', 'ledgerHashNotString', account=account, ledger_hash=5)
        self.refused('account_tx', 'invalidParams', 'ledgerHashMalformed', account=account, ledger_hash='zz')
        self.refused('account_tx', 'invalidParams', 'ledger_index string malformed', account=account, ledger_index='abc')
        self.refused('account_tx', 'invalidParams', 'ledger_index string malformed', account=account, ledger_index=str(self.last))
        self.refused('account_tx', 'lgrNotFound', 'ledgerNotFound', account=account, ledger_index=self.last + 5)
        self.refused('account_tx', 'lgrNotValidated', None, account=account, ledger_index='')
        self.assertEqual(self.same('account_tx', account=account, ledger_index='validated')['ledger_index_min'], self.last)

    def test_state(self):
        found = self.survey()
        samples = sorted({self.first, (self.first + self.last) // 2, self.last})
        for account in sorted(found['accounts'])[:150]:
            for seq in samples:
                for method in ('account_info', 'account_lines', 'account_objects', 'account_offers', 'account_currencies', 'account_nfts', 'gateway_balances'):
                    self.same(method, account=account, ledger_index=seq)
                self.same('account_objects', account=account, ledger_index=seq, limit=2)
                self.same('ledger_entry', account_root=account, ledger_index=seq)
        for key, seq in found['keys'][:3000]:
            for at in {seq, max(self.first, seq - 1), self.last}:
                self.same('ledger_entry', index=key, ledger_index=at)
                self.same('ledger_entry', index=key, ledger_index=at, binary=True)
        for book, seq in list(found['books'].items())[:100]:
            pays, gets = json.loads(book)
            for at in {seq, self.last}:
                self.same('book_offers', taker_pays=pays, taker_gets=gets, ledger_index=at, limit=20)

    def test_book_snapshots(self):
        # subscribe with books and `snapshot`: the book's offers now (`bids` and `asks` with
        # `both`), from the pieces as from the store.
        sys.path.insert(0, str(ROOT / 'tools'))
        from capture_validations import WebSocket
        books = list(self.survey()['books'])[:20]
        if not books: self.skipTest('no offers in this fixture')
        def subscribe(server, request):
            ws = WebSocket(f'ws://127.0.0.1:{server.ws_port}/'); ws.sock.settimeout(60)
            ws.send({'id': 1, 'command': 'subscribe', **request})
            answer = ws.receive(); ws.sock.close()
            return answer
        for book in books:
            pays, gets = json.loads(book)
            for both in (False, True):
                request = {'books': [{'taker_pays': pays, 'taker_gets': gets, 'snapshot': True, 'both': both}]}
                expected, actual = subscribe(self.store, request), subscribe(self.pieces, request)
                type(self).compared += 1
                self.assertEqual(actual, expected, book)
                self.assertEqual(actual['status'], 'success', actual)
                self.assertIn('bids' if both else 'offers', actual['result'])
        bad = subscribe(self.pieces, {'books': [{'taker_pays': {'currency': 'XRP'}}]})
        self.assertEqual(bad['status'], 'error')

    def test_more_methods(self):
        found = self.survey()
        accounts = sorted(found['accounts'])[:80]
        for seq in self.ledgers()[:12]:
            self.same('book_changes', ledger_index=seq)
        for tx, seq in found['txs'][:300]:
            self.same('transaction_entry', tx_hash=tx, ledger_index=seq)
            self.same('transaction_entry', tx_hash=tx, ledger_index=seq, api_version=2)
        for i, account in enumerate(accounts):
            for seq in sorted({self.first, self.last}):
                self.same('account_channels', account=account, ledger_index=seq)
                self.same('account_channels', account=account, ledger_index=seq, limit=10)
                for role in ('user', 'gateway'):
                    self.same('noripple_check', account=account, role=role, transactions=True, ledger_index=seq)
                self.same('deposit_authorized', source_account=accounts[i - 1], destination_account=account, ledger_index=seq)
        for amm, (asset, asset2, seq) in list(found['amms'].items())[:50]:
            self.same('amm_info', amm_account=amm, ledger_index=seq)
            if asset2: self.same('amm_info', asset=asset, asset2=asset2, ledger_index=self.last)
        for nft in sorted(found['nfts'])[:50]:
            for method in ('nft_buy_offers', 'nft_sell_offers'):
                self.same(method, nft_id=nft, ledger_index=self.last)
        middle = self.pieces.call('ledger', {'ledger_index': (self.first + self.last) // 2})['ledger']['close_time_iso']
        self.same('ledger_index', date=middle)
        self.same('ledger_index')
        for version in (1, 2): self.same('version', api_version=version)
        self.same('ping')
        # The decode caches' statistics: a piece cache's, under a live tail too (its base's); a store keeps none.
        self.assertIn('budget', self.pieces.call('xrpla_cache_stats', {}))
        self.assertEqual(self.store.call('xrpla_cache_stats', {}).get('error'), 'notSupported')

    @unittest.skipUnless(OPENSSL, 'XRPLA_OPENSSL (an openssl program, supplied by CTest) makes the test certificates')
    def test_tls_listeners(self):
        # HTTPS and WSS answer as HTTP does, a plain request to a TLS port gets no answer, and a
        # renewed certificate is served without a restart.
        import base64, ssl, struct
        work = Path(tempfile.mkdtemp(prefix='xrpla-tls-'))
        (work / 'req.cnf').write_text('[req]\ndistinguished_name = dn\n[dn]\n')  # no system configuration needed
        def certificate(name):
            subprocess.run([OPENSSL, 'req', '-config', str(work / 'req.cnf'), '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-days', '1',
                            '-subj', f'/CN={name}', '-keyout', str(work / 'key.pem'), '-out', str(work / 'cert.pem')], check=True, capture_output=True)
        def served_name(port):
            with socket.create_connection(('127.0.0.1', port), 10) as raw:
                with insecure.wrap_socket(raw, server_hostname='localhost') as tls:
                    return tls.getpeercert(binary_form=True)
        insecure = ssl.create_default_context(); insecure.check_hostname = False; insecure.verify_mode = ssl.CERT_NONE
        certificate('first')
        https, wss = free_port(), free_port()
        server = Server(self.pieces_dir, ('--https', f'127.0.0.1:{https}', '--wss', f'127.0.0.1:{wss}',
                                          '--tls-cert', work / 'cert.pem', '--tls-key', work / 'key.pem'))
        try:
            for _ in range(100):
                try: socket.create_connection(('127.0.0.1', https), .1).close(); break
                except OSError: time.sleep(.05)
            params = {'ledger_index': self.last, 'transactions': True, 'expand': True}
            expected = server.call('ledger', params)
            connection = http.client.HTTPSConnection('127.0.0.1', https, timeout=60, context=insecure)
            connection.request('POST', '/', json.dumps({'method': 'ledger', 'params': [params]}), {'Content-Type': 'application/json'})
            self.assertEqual(json.loads(connection.getresponse().read())['result'], expected)
            # Kept open (HTTP/1.1): the next request goes over the same TLS connection.
            first_socket = connection.sock
            connection.request('POST', '/', json.dumps({'method': 'ping', 'params': [{}]}), {'Content-Type': 'application/json'})
            self.assertEqual(json.loads(connection.getresponse().read())['result']['status'], 'success')
            self.assertIs(connection.sock, first_socket)
            # WSS: the same answer as a WebSocket response, then the ledger stream.
            with insecure.wrap_socket(socket.create_connection(('127.0.0.1', wss), 10), server_hostname='localhost') as tls:
                tls.sendall((f'GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                             f'Sec-WebSocket-Key: {base64.b64encode(os.urandom(16)).decode()}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
                head = b''
                while b'\r\n\r\n' not in head: head += tls.recv(1)
                self.assertIn(b' 101 ', head.split(b'\r\n')[0])
                def send(value):
                    data, mask = json.dumps(value).encode(), os.urandom(4)
                    size = bytes([0x80 | len(data)]) if len(data) < 126 else bytes([0x80 | 126]) + struct.pack('>H', len(data))
                    tls.sendall(bytes([0x81]) + size + mask + bytes(x ^ mask[i % 4] for i, x in enumerate(data)))
                def receive():
                    def exactly(n):
                        out = b''
                        while len(out) < n: out += tls.recv(n - len(out))
                        return out
                    first, second = exactly(2)
                    n = second & 0x7f
                    n = struct.unpack('>H', exactly(2))[0] if n == 126 else struct.unpack('>Q', exactly(8))[0] if n == 127 else n
                    return json.loads(exactly(n))
                send({'id': 1, 'command': 'ledger', **params})
                answer = receive()
                self.assertEqual((answer['id'], answer['status'], answer['result']), (1, 'success', {k: v for k, v in expected.items() if k != 'status'}))
                send({'id': 2, 'command': 'subscribe', 'streams': ['ledger']})
                self.assertEqual(receive()['result']['ledger_index'], self.last)
            # Plain HTTP to a TLS listener is not answered.
            with self.assertRaises((http.client.HTTPException, OSError)):
                plain = http.client.HTTPConnection('127.0.0.1', https, timeout=5)
                plain.request('POST', '/', json.dumps({'method': 'ping', 'params': [{}]}), {'Content-Type': 'application/json'})
                json.loads(plain.getresponse().read())
            # A renewal: new sessions get the new certificate within the minute's check.
            before = served_name(https)
            certificate('renewed')
            for _ in range(80):
                if served_name(https) != before: break
                time.sleep(1)
            self.assertNotEqual(served_name(https), before)
            self.assertEqual(server.call('ping', {}).get('status'), 'success')
        finally:
            server.close(); shutil.rmtree(work, ignore_errors=True)

    def test_nft_info(self):
        ids = set(sorted(self.survey()['nfts'])[:120])
        # Tokens no cached transaction touches: their state comes from the snapshot's NFT map.
        pages = self.store.call('ledger_data', {'ledger_index': self.first, 'type': 'nft_page', 'limit': 40}).get('state', [])
        for page in pages:
            for token in page.get('NFTokens', [])[:4]: ids.add(token['NFToken']['NFTokenID'])
        samples = sorted({self.first, (self.first + self.last) // 2, self.last})
        found = 0
        for nft in sorted(ids):
            ledgers = set(samples)
            for tx in self.store.call('nft_history', {'nft_id': nft, 'limit': 8, 'api_version': 2}).get('transactions', []):
                ledgers |= {tx['ledger_index'], max(self.first, tx['ledger_index'] - 1)}
            for seq in sorted(ledgers):
                found += 'error' not in self.same('nft_info', nft_id=nft, ledger_index=seq)
        self.same('nft_info', nft_id='00' * 32, ledger_index=self.last)
        # Fixtures generated by tests/nft_lifecycle.py also say what the answers must be.
        expectations = ARCHIVE / 'nft-expectations.json'
        for e in json.loads(expectations.read_text()) if expectations.exists() else []:
            answer = self.pieces.call('nft_info', {'nft_id': e['nft_id'], 'ledger_index': e['ledger_index']})
            self.assertEqual({k: answer.get(k) for k in ('owner', 'uri', 'is_burned', 'ledger_index')},
                             {'owner': e['owner'], 'uri': e['uri'], 'is_burned': e['is_burned'], 'ledger_index': e['since']}, json.dumps(e))
            found += 1
        type(self).nft_tokens = {'ids': len(ids), 'found': found}
        if ids: self.assertTrue(found, 'no nft_info request found a token')

    def test_clio_methods(self):
        found = self.survey()
        samples = sorted({self.first, (self.first + self.last) // 2, self.last})
        # feature: every amendment libxrpl knows, enabled as of each ledger.
        for seq in samples: features = self.same('feature', ledger_index=seq)['features']
        self.assertTrue(features)
        key, entry = sorted(features.items())[0]
        self.assertEqual(self.same('feature', feature=entry['name'], ledger_index=self.last)[key]['name'], entry['name'])
        self.same('feature', feature=key.lower(), ledger_index=self.last)
        self.assertEqual(self.same('feature', feature='NoSuchAmendment')['error'], 'badFeature')
        self.assertEqual(self.same('feature', feature=entry['name'], vetoed=True)['error'], 'noPermission')
        # ledger_entry with include_deleted: an object a sampled ledger deleted is answered as it
        # was before, with the ledger that deleted it.
        deleted = found['deleted'][:300]
        for key, seq in deleted:
            for at in sorted({seq, self.last}):
                answer = self.same('ledger_entry', index=key, ledger_index=at, include_deleted=True)
                if at == seq: self.assertEqual(answer.get('deleted_ledger_index'), seq, answer)
                self.same('ledger_entry', index=key, ledger_index=at, include_deleted=True, binary=True)
            self.assertEqual(self.same('ledger_entry', index=key, ledger_index=seq)['error'], 'entryNotFound')
        # Selectors as xrpld 3.3 parses them.
        accounts = sorted(found['accounts'])
        if accounts:
            self.same('ledger_entry', account=accounts[0], ledger_index=self.last)
            self.assertEqual(self.same('ledger_entry', account_root=accounts[0], offer={'account': accounts[0], 'seq': 1})['error'], 'invalidParams')
            self.assertEqual(self.same('ledger_entry', offer=self.same('ledger_entry', account_root=accounts[0])['index'])['error'], 'unexpectedLedgerType')
            self.assertEqual(self.same('ledger_entry', vault={'owner': accounts[0], 'seq': 1})['error'], 'entryNotFound')
            self.assertEqual(self.same('ledger_entry', delegate={'account': accounts[0], 'authorize': accounts[-1]})['error'], 'entryNotFound')
            self.assertEqual(self.same('ledger_entry', mpt_issuance='00' * 24)['error'], 'entryNotFound')
            self.assertEqual(self.same('ledger_entry', mpt_issuance='00' * 32)['error'], 'malformedMPTokenIssuance')
        self.same('ledger_entry', fee=True, ledger_index=self.last)
        self.same('ledger_entry', amendments=True, ledger_index=self.last)
        self.assertEqual(self.same('ledger_entry', ledger_index=self.last, api_version=2)['error'], 'invalidParams')
        self.assertEqual(self.same('ledger_entry', ledger_index=self.last)['error'], 'unknownOption')
        for account in accounts[:40]:
            for method in ('account_mptokens', 'account_mptoken_issuances'):
                self.same(method, account=account, ledger_index=self.last)
        # xrpld 3.4.0: a zero vault key is entryNotFound; parse errors keep their own codes.
        self.assertEqual(self.same('vault_info', vault_id='00' * 32)['error'], 'entryNotFound')
        self.assertEqual(self.same('vault_info', vault_id='11' * 32)['error'], 'entryNotFound')
        self.assertEqual(self.same('vault_info', vault_id='zz')['error_message'], "Invalid field 'vault_id', not hex string.")
        self.assertEqual(self.same('vault_info', vault_id='11' * 32, seq=1)['error_message'],
                         "Must specify either 'vault_id' or both 'owner' and 'seq'.")
        if accounts:
            self.assertEqual(self.same('get_aggregate_price', base_asset='XRP', quote_asset='USD',
                                       oracles=[{'account': accounts[0], 'oracle_document_id': 4000000000}])['error'], 'objectNotFound')
        self.assertEqual(self.same('get_aggregate_price', base_asset='XRP', quote_asset='USD', oracles=[])['error'], 'oracleMalformed')
        values = {self.pieces.call('random', {})['random'] for _ in range(3)}
        self.assertEqual(len(values), 3)
        self.assertTrue(all(len(v) == 64 and int(v, 16) >= 0 for v in values))

    def test_clio_indexes(self):
        # nfts_by_issuer, every issuer of the fixture's tokens, paged two at a time.
        issuers = set()
        for nft in sorted(self.survey()['nfts'])[:60]:
            answer = self.pieces.call('nft_info', {'nft_id': nft, 'ledger_index': self.last})
            if 'issuer' in answer: issuers.add((answer['issuer'], answer['nft_taxon']))
        samples = sorted({self.first, (self.first + self.last) // 2, self.last})
        listed = 0
        for issuer, taxon in sorted(issuers):
            for seq in samples:
                page = self.same('nfts_by_issuer', issuer=issuer, ledger_index=seq, limit=2)
                listed += len(page['nfts'])
                for _ in range(20):
                    if 'marker' not in page: break
                    page = self.same('nfts_by_issuer', issuer=issuer, ledger_index=seq, limit=2, marker=page['marker'])
                    listed += len(page['nfts'])
            self.same('nfts_by_issuer', issuer=issuer, nft_taxon=taxon, ledger_index=self.last)
            whole = self.same('nfts_by_issuer', issuer=issuer, ledger_index=self.last, limit=100)
            ids = [n['nft_id'] for n in whole['nfts']]
            self.assertEqual(ids, sorted(ids, key=lambda i: (next(n['nft_taxon'] for n in whole['nfts'] if n['nft_id'] == i), i)))
        if issuers: self.assertTrue(listed, 'nfts_by_issuer listed no token')
        accounts = sorted(self.survey()['accounts'])
        if accounts: self.same('nfts_by_issuer', issuer=accounts[0], ledger_index=self.last)
        self.assertEqual(self.same('nfts_by_issuer', issuer=address(bytes([1]) * 20))['error'], 'actNotFound')
        self.assertEqual(self.same('mpt_holders', mpt_issuance_id='00' * 24)['error'], 'objectNotFound')
        self.assertEqual(self.same('mpt_holders', mpt_issuance_id='00' * 24, limit=0)['error'], 'invalidParams')
        # Every issuance's holders at every ledger, as the generating xrpld held them
        # (tests/token_lifecycle.py), paged two at a time.
        expectations = ARCHIVE / 'mpt-expectations.json'
        checked = 0
        for e in json.loads(expectations.read_text()) if expectations.exists() else []:
            if not self.first <= e['ledger_index'] <= self.last: continue
            holders, page = [], self.same('mpt_holders', mpt_issuance_id=e['mpt_issuance_id'], ledger_index=e['ledger_index'], limit=2)
            while True:
                if 'error' in page:
                    self.assertEqual((page['error'], e['holders']), ('objectNotFound', []), json.dumps(e)); break
                holders += [m['account'] for m in page['mptokens']]
                for m in page['mptokens']:
                    # Clio 2.8's fields beyond the amount: the locked amount, as the MPToken holds it.
                    node = self.same('ledger_entry', index=m['mptoken_index'], ledger_index=e['ledger_index'])['node']
                    self.assertEqual(m.get('locked_amount'), node.get('LockedAmount'), json.dumps(m))
                if 'marker' not in page: break
                page = self.same('mpt_holders', mpt_issuance_id=e['mpt_issuance_id'], ledger_index=e['ledger_index'], limit=2, marker=page['marker'])
            self.assertEqual(sorted(holders), e['holders'], json.dumps(e))
            checked += 1
        if expectations.exists(): self.assertTrue(checked)

    def test_xrpld_answers(self):
        # What the generating xrpld answered (tests/token_lifecycle.py): both backends answer alike.
        answers = ARCHIVE / 'xrpld-answers.json'
        if not answers.exists(): self.skipTest('this fixture records no xrpld answers')
        items = json.loads(answers.read_text())
        self.assertTrue(all(self.first <= i['params']['ledger_index'] <= self.last for i in items), 'answers outside the published ledgers')
        for item in items:
            for server in (self.pieces, self.store):
                actual = server.call(item['method'], item['params'])
                for field in ('status', 'warnings', 'request'): actual.pop(field, None)
                type(self).compared += 1
                self.assertEqual(actual, item['result'], json.dumps({'method': item['method'], **item['params']}))

    def test_optional_indexes_turned_off(self):
        # A copy of the cache indexed without the optional indexes: their files go, the methods
        # say how to turn them back on, and turning them on builds only them.
        copy = Path(tempfile.mkdtemp(prefix='xrpla-no-optional-'))
        try:
            shutil.copytree(self.pieces_dir, copy / 'cache')
            def run(*args): return json.loads(subprocess.run([str(a) for a in args], capture_output=True, text=True, check=True).stdout)
            removed = run(BINARY, 'index-pieces', '--dir', copy / 'cache', '--indexes', 'none')['removed']
            self.assertTrue(any(name.endswith(('.xqmh', '.xqni')) for name in removed), removed)
            self.assertFalse([f for f in (copy / 'cache' / 'query').iterdir() if f.suffix in ('.xqmh', '.xqni')])
            issuer = next((a for a in sorted(self.survey()['accounts']) if 'error' not in self.store.call('account_info', {'account': a})), None)
            if issuer:
                request = copy / 'request.json'
                request.write_text(json.dumps({'method': 'nfts_by_issuer', 'params': [{'issuer': issuer}]}))
                answer = run(BINARY, 'rpc', '--dir', copy / 'cache', '--request', request)
                self.assertEqual(answer['error'], 'notSupported', answer)
                self.assertIn('[index]', answer['error_message'])
            result = run(BINARY, 'index-pieces', '--dir', copy / 'cache')
            self.assertEqual({f.suffix for f in (copy / 'cache' / 'query').iterdir() if f.suffix in ('.xqmh', '.xqni')}, {'.xqmh', '.xqni'})
            self.assertTrue(all(r['status'] == 'present' for r in result['ranges'] if 'object' in r), result)
        finally:
            shutil.rmtree(copy, ignore_errors=True)

    def test_seek_index(self):
        # The fixtures' posting lists are far shorter than two default strides, so copies of the
        # cache get seek indexes (XQSK) of strides 1 and 3 (XRPLA_TEST_SEEK_STRIDE), and a third
        # copy none. Every account_tx and nft_history page of the busiest keys, walked to the end
        # in both directions with several limits and ledger bounds, answers alike from all of them
        # and from the store: checkpoints only move where a read starts.
        root = Path(tempfile.mkdtemp(prefix='xrpla-seek-'))
        servers = []
        try:
            def run(*args, env=None):
                return json.loads(subprocess.run([str(a) for a in args], capture_output=True, text=True, check=True, env=env).stdout)
            plain = root / 'plain'
            shutil.copytree(self.pieces_dir, plain)
            for seek in (plain / 'query').glob('*.xqsk'): seek.unlink()
            servers.append(Server(plain, self.tail_args))
            for stride in (1, 3):
                copy = root / f'stride-{stride}'
                shutil.copytree(self.pieces_dir, copy)
                result = run(BINARY, 'index-pieces', '--dir', copy, env={**os.environ, 'XRPLA_TEST_SEEK_STRIDE': str(stride)})
                seeks = [r['seek'] for r in result['ranges'] if 'seek' in r]
                self.assertTrue(seeks and all(s['stride'] == stride for s in seeks), result)
                if stride == 1 and not sum(s['checkpoints'] for s in seeks): self.skipTest('no posting list is longer than two postings')
                self.assertTrue(result['rebuilt'], result)  # the default stride's files were replaced
                servers.append(Server(copy, self.tail_args))
            # The keys with the most postings, by the store's count.
            def count(method, **key):
                return len(self.store.call(method, {**key, 'limit': 400}).get('transactions', []))
            accounts = sorted(sorted(self.survey()['accounts']), key=lambda a: -count('account_tx', account=a))[:8]
            nfts = sorted(sorted(self.survey()['nfts']), key=lambda n: -count('nft_history', nft_id=n))[:4]
            middle = (self.first + self.last) // 2
            bounds = [{}, {'ledger_index_min': self.first + 1, 'ledger_index_max': middle}, {'ledger_index_min': middle, 'ledger_index_max': self.last - 1}]
            for method, field, keys in (('account_tx', 'account', accounts), ('nft_history', 'nft_id', nfts)):
                for key in keys:
                    for extra in bounds:
                        for forward in (False, True):
                            for limit in (1, 2, 5):
                                params = {field: key, 'limit': limit, 'forward': forward, 'api_version': 2, **extra}
                                for _ in range(400):
                                    expected = self.store.call(method, params)
                                    for server in (self.pieces, *servers):
                                        self.assertEqual(server.call(method, params), expected, f'{method} {json.dumps(params)}')
                                    type(self).compared += 1
                                    if 'marker' not in expected: break
                                    params = {**params, 'marker': expected['marker']}
        finally:
            for server in servers: server.close()
            shutil.rmtree(root, ignore_errors=True)

    def test_state_pages_out_of_order(self):
        # Clio's out_of_order walks the same keys (this node reads any ledger's keys in order),
        # its marker the last key returned, and none once the walk is done.
        for seq in sorted({self.first, self.last}):
            def walk(**extra):
                keys, page = [], self.same('ledger_data', ledger_index=seq, binary=True, limit=2048, **extra)
                keys += [s['index'] for s in page['state']]
                while 'marker' in page:
                    if extra: self.assertIsInstance(page['marker'], str)
                    page = self.same('ledger_data', ledger_index=seq, binary=True, limit=2048, marker=page['marker'], **extra)
                    keys += [s['index'] for s in page['state']]
                if extra: self.assertIs(page['cache_full'], True)
                return keys
            self.assertEqual(walk(out_of_order=True), walk())
        # An integer marker D pages Clio's deletions: objects ledger D deleted that exist at the
        # ledger asked about, then D - 1.
        for key, d in self.survey()['deleted'][:30]:
            if d <= self.first: continue
            answer = self.same('ledger_data', ledger_index=self.first, out_of_order=True, marker=d, binary=True)
            self.assertEqual(answer['marker'], d - 1)
            self.assertIs(answer['cache_full'], True)
            gone = {k for k, at in self.survey()['deleted'] if at == d}
            for item in answer['state']: self.assertIn(item['index'], gone)
            self.assertIn(key, {item['index'] for item in answer['state']} | {k for k in gone if 'error' in self.same('ledger_entry', index=k, ledger_index=self.first)})
        self.assertEqual(self.same('ledger_data', ledger_index=self.last, out_of_order=True, marker=self.last)['state'], [])

    def test_state_pages(self):
        for seq in sorted({self.first, self.last}):
            for binary in (True, False):
                page = self.same('ledger_data', ledger_index=seq, limit=64, binary=binary)
                for _ in range(4):
                    if 'marker' not in page: break
                    page = self.same('ledger_data', ledger_index=seq, limit=64, binary=binary, marker=page['marker'])
            self.same('ledger_data', ledger_index=seq, limit=100, type='offer')


if __name__ == '__main__':
    unittest.main()
