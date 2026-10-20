"""The connected profile follows a node, seals pieces locally and serves them with a live tail.

A mock node replays a fixture archive (captured from a store built from it), its validated
tip advancing a ledger at a time. `xrpla run` with profile = "connected" follows it with
small pieces and 20-ledger intervals; once the served range reaches the node's tip and the
spool has been cut at the first interval, answers must equal a server over the store, the
history must be split between sealed pieces and the spool tail, the spool's small chunks
must have been merged, and SIGTERM must stop everything.

Needs XRPLA_BINARY (with xrpla_history_decode, xrpla_history_encode, xrpla_tx_index and
xrpla_snapshot beside it).
"""
import copy
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from capture_validations import WebSocket

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / 'tests' / 'fixtures' / 'standalone-nfts'
BINARY = os.environ.get('XRPLA_BINARY')
ACCOUNT_FIELDS = {'Account', 'Destination', 'Owner', 'Issuer'}


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]


def call(port, method, params, timeout=120):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=timeout)
    connection.request('POST', '/', json.dumps({'method': method, 'params': [params]}), {'Content-Type': 'application/json'})
    return json.loads(connection.getresponse().read())['result']


def wait_port(port, process, seconds):
    for _ in range(int(seconds * 20)):
        try: socket.create_connection(('127.0.0.1', port), .1).close(); return
        except OSError:
            if process.poll() is not None: raise RuntimeError(f'exited with {process.returncode}')
            time.sleep(.05)
    raise RuntimeError(f'port {port} did not open')


@unittest.skipUnless(BINARY, 'Compiled xrpla supplied by CTest')
class Connected(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='xrpla-connected-'); root = Path(cls.tmp.name)
        manifest = json.loads((ARCHIVE / 'manifest.json').read_bytes())
        first, cls.tip = manifest['chunks'][0]['first'], manifest['tip']['seq']
        store = cls.store_dir = root / 'store'
        for command in (['fetch', '--archive', ARCHIVE, '--range', f'{first}-{cls.tip}', '--dir', store, '--publisher-key', ARCHIVE / 'publisher.pub'],
                        ['build', '--dir', store, '--publisher-key', ARCHIVE / 'publisher.pub']):
            subprocess.run([BINARY, *map(str, command)], check=True, capture_output=True, timeout=600)
        cls.store_port = free_port()
        cls.store = subprocess.Popen([BINARY, 'serve', '--dir', str(store), '--http', f'127.0.0.1:{cls.store_port}', '--ws', f'127.0.0.1:{free_port()}'],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_port(cls.store_port, cls.store, 60)

        # The node: every ledger after the first (whose state the follower starts from), its
        # state pages, and a validated tip that advances while the test runs.
        cls.start = first + 1
        cls.ledgers, cls.pages, cls.hashes = {}, {}, {}
        for seq in range(first, cls.tip + 1):
            ledger = call(cls.store_port, 'ledger', {'ledger_index': seq, 'binary': True, 'transactions': True, 'expand': True, 'diff': True})
            cls.ledgers[seq] = ledger; cls.hashes[ledger['ledger_hash'].lower()] = seq
            marker = None
            while True:
                params = {'ledger_index': seq, 'binary': True, 'limit': 2048, **({'marker': marker} if marker is not None else {})}
                page = call(cls.store_port, 'ledger_data', params)
                cls.pages[(seq, json.dumps(marker))] = page
                if 'marker' not in page: break
                marker = page['marker']
        cls.node_tip, cls.requests, cls.fetched = cls.start, 0, 0
        test = cls

        class Node(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                method, params = request['method'], (request.get('params') or [{}])[0]
                test.requests += 1
                seq = params.get('ledger_index', test.node_tip)
                if isinstance(seq, str): seq = test.node_tip
                if 'ledger_hash' in params: seq = test.hashes[params['ledger_hash'].lower()]
                if seq > test.node_tip: result = {'error': 'lgrNotFound', 'status': 'error'}
                elif method == 'ledger':
                    result = copy.deepcopy(test.ledgers[seq])
                    if params.get('diff'): test.fetched = max(test.fetched, seq)  # the follower exported it
                elif method == 'ledger_data':
                    # A state download split across connections starts pages at other markers.
                    recorded = (seq, json.dumps(params.get('marker')))
                    result = test.pages[recorded] if recorded in test.pages else call(test.store_port, 'ledger_data', {
                        'ledger_index': seq, 'binary': True, 'limit': params.get('limit', 2048), 'marker': params['marker']})
                else: result = {'error': 'unknownCmd', 'status': 'error'}
                body = json.dumps({'result': result}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                try: self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass

            def log_message(self, *args): pass

        cls.node = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Node)
        threading.Thread(target=cls.node.serve_forever, daemon=True).start()
        cls.stop_ticking = threading.Event()

        # A new ledger once the follower has exported the tip, so it commits one ledger per batch.
        def tick():
            while cls.node_tip < cls.tip and not cls.stop_ticking.wait(1.2):
                if cls.fetched >= cls.node_tip: cls.node_tip += 1
        threading.Thread(target=tick, daemon=True).start()

        # The account with the most transactions, for an account subscription.
        counts = {}
        for seq in range(first + 1, cls.tip + 1):
            for tx in call(cls.store_port, 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True, 'api_version': 2})['ledger']['transactions']:
                counts[tx['tx_json']['Account']] = counts.get(tx['tx_json']['Account'], 0) + 1
        cls.account = max(counts, key=counts.get)
        cls.storage, cls.port, cls.ws_port = root / 'connected', free_port(), free_port()
        config = root / 'xrpla.toml'
        config.write_text(f'''profile = "connected"
[node]
rpc = "http://127.0.0.1:{cls.node.server_port}/"
[storage]
dir = "{cls.storage.as_posix()}"
cache_mb = 256
[serve]
http = "127.0.0.1:{cls.port}"
ws = "127.0.0.1:{cls.ws_port}"
[connected]
start = {cls.start}
poll_seconds = 1
publish_seconds = 2
sync_seconds = 2
piece_ledgers = 5
bundle_pieces = 4
''')
        cls.log = open(root / 'run.log', 'w+')
        cls.runner = subprocess.Popen([BINARY, 'run', '--config', str(config)], stdout=cls.log, stderr=subprocess.STDOUT)
        cls.started = time.time()
        try: wait_port(cls.port, cls.runner, 300)
        except RuntimeError as error:
            cls.log.seek(0)
            raise RuntimeError(f'{error}; the run log ends: {cls.log.read()[-3000:]}') from error
        # Streams from as soon as the server listens: every later ledger and its transactions.
        # Many subscribers to the same streams: each ledger is rendered once for all of them.
        everything = {'command': 'subscribe', 'streams': ['ledger', 'transactions'], 'api_version': 2}
        subscriptions = [('all', everything), ('account', {'command': 'subscribe', 'accounts': [cls.account]})]
        subscriptions += [(f'all-{n}', everything) for n in range(24)]
        cls.streams = {name: [] for name, _ in subscriptions}
        for name, request in subscriptions:
            ws = WebSocket(f'ws://127.0.0.1:{cls.ws_port}/'); ws.sock.settimeout(None); ws.send({'id': 1, **request})
            def listen(ws=ws, into=cls.streams[name]):
                try:
                    while True: into.append(ws.receive())
                except (OSError, ValueError, ConnectionError): pass
            threading.Thread(target=listen, daemon=True).start()
        deadline = time.time() + 300
        while time.time() < deadline:
            served = call(cls.port, 'ledger_range', {})
            sealed = json.loads((cls.storage / 'cache' / 'selection.json').read_bytes())['last']
            rotated = (cls.storage / 'follower' / 'current').read_text().strip() != f'spool-{cls.start}'
            # Everything the node has, with some of it sealed and some still in the tail, and the
            # spool cut once its first 20-ledger interval was bundled.
            if served.get('ledger_index_max') == cls.tip and cls.start + 4 <= sealed < cls.tip and rotated: break
            time.sleep(1)
        else:
            cls.log.seek(0); raise AssertionError(f'did not catch up: {served} sealed {sealed} rotated {rotated}\n{cls.log.read()[-6000:]}')
        cls.caught_up = round(time.time() - cls.started, 1)
        cls.sealed, cls.compared = sealed, 0

    @classmethod
    def tearDownClass(cls):
        cls.stop_ticking.set()
        if cls.runner.poll() is None: cls.runner.kill(); cls.runner.wait()
        cls.store.terminate(); cls.store.wait()
        cls.node.shutdown(); cls.node.server_close()
        print(json.dumps({'caught_up_seconds': cls.caught_up, 'sealed_through': cls.sealed, 'tail': [cls.sealed + 1, cls.tip],
                          'compared': cls.compared, 'node_requests': cls.requests, 'streamed': getattr(cls, 'streamed', None)}), file=sys.stderr)
        cls.log.close(); cls.tmp.cleanup()

    def same(self, method, **params):
        expected, actual = call(self.store_port, method, params), call(self.port, method, params)
        type(self).compared += 1
        self.assertEqual(actual, expected, f'{method} {json.dumps(params)}')
        return expected

    def test_1_answers_match_the_store(self):
        self.assertEqual(call(self.port, 'ledger_range', {})['ledger_index_min'], self.start)
        accounts = set()
        for seq in range(self.start, self.tip + 1):
            ledger = self.same('ledger', ledger_index=seq, transactions=True, expand=True, api_version=2)['ledger']
            self.same('ledger', ledger_index=seq, diff=True, binary=True)
            for tx in ledger['transactions']:
                self.same('tx', transaction=tx['hash'], api_version=2)
                for field in ACCOUNT_FIELDS:
                    if isinstance(tx['tx_json'].get(field), str): accounts.add(tx['tx_json'][field])
        self.assertTrue(accounts)
        for account in sorted(accounts):
            self.same('account_tx', account=account, ledger_index_min=self.start, api_version=2)
            self.same('account_tx', account=account, ledger_index_min=self.start, forward=True, limit=2, api_version=2)
            for seq in (self.sealed, self.tip):
                self.same('account_info', account=account, ledger_index=seq)
                self.same('account_objects', account=account, ledger_index=seq)
                self.same('account_nfts', account=account, ledger_index=seq)
        page = self.same('ledger_data', ledger_index=self.tip, limit=16)
        while 'marker' in page: page = self.same('ledger_data', ledger_index=self.tip, limit=16, marker=page['marker'])

    def test_1_network_id_is_configurable(self):
        tx = next(t for seq in range(self.tip, self.start - 1, -1) for t in call(self.port, 'ledger', {'ledger_index': seq, 'transactions': True})['ledger']['transactions'])
        self.assertTrue(call(self.port, 'tx', {'transaction': tx})['ctid'].endswith('0000'))  # mainnet by default
        def rpc(method, params):
            return json.loads(subprocess.run([BINARY, 'rpc', '--dir', str(self.store_dir), '--network-id', '21338', '--request', '-'],
                                             input=json.dumps({'method': method, 'params': params}), capture_output=True, text=True, check=True).stdout)
        self.assertTrue(rpc('tx', {'transaction': tx})['ctid'].endswith('535A'))
        self.assertEqual(rpc('server_info', {})['info']['network_id'], 21338)

    def test_2_spool_chunks_are_merged(self):
        # The first spool (still in place for an hour if the publisher has since rotated it).
        spool = self.storage / 'follower' / 'spools' / f'spool-{self.start}'
        chunks = json.loads((spool / 'manifest.json').read_bytes())['chunks']
        # The follower committed one ledger per batch; inside each completed 16-ledger block
        # those chunks are now one.
        tip = chunks[-1]['last']
        for block in range(self.start // 16, tip // 16):
            inside = [c for c in chunks if c['first'] >= block * 16 and c['last'] <= block * 16 + 15]
            self.assertLessEqual(len(inside), 1, chunks)
        self.assertEqual((chunks[0]['first'], chunks[0]['last']), (self.start, 15), chunks)
        self.assertTrue(all(c['first'] == c['last'] for c in chunks[1:]), chunks)

    def test_3_streams_match_the_store(self):
        time.sleep(3)  # the server polls for new ledgers every second
        messages = self.streams['all']
        self.assertEqual((messages[0]['type'], messages[0]['status']), ('response', 'success'), messages[:1])
        subscribed = messages[0]['result']['ledger_index']
        self.assertLess(subscribed, self.tip)
        ledgers, current = [], None
        for message in messages[1:]:
            if message['type'] == 'ledgerClosed':
                ledgers.append(message['ledger_index']); current = message
                ledger = call(self.store_port, 'ledger', {'ledger_index': message['ledger_index'], 'transactions': True})
                self.assertEqual((message['ledger_hash'], message['txn_count']), (ledger['ledger_hash'], len(ledger['ledger']['transactions'])))
                continue
            self.assertEqual(message['type'], 'transaction')
            self.assertEqual(message['ledger_index'], current['ledger_index'], 'a ledger\'s transactions follow its ledgerClosed')
            expected = call(self.store_port, 'tx', {'transaction': message['hash'], 'api_version': 2})
            for field in ('meta', 'hash', 'ledger_index', 'ledger_hash', 'close_time_iso', 'validated'): self.assertEqual(message[field], expected[field], field)
            tx_json = dict(expected['tx_json']); tx_json.pop('ledger_index')
            actual = dict(message['tx_json']); actual.pop('owner_funds', None)
            self.assertEqual(actual, tx_json)
            self.assertEqual((message['engine_result'], message['status']), (expected['meta']['TransactionResult'], 'closed'))
        if ledgers != list(range(subscribed + 1, self.tip + 1)):
            stalled = (ledgers or [subscribed])[-1] + 1
            self.fail(f'streamed {ledgers}; at {stalled}: ' + json.dumps([call(self.port, m, {'ledger_index': stalled, 'api_version': 2}) for m in ('_ledger_event', '_transaction_events')])[:3000])
        counted = sum(1 for m in messages if m['type'] == 'transaction')
        self.assertEqual(counted, sum(m['txn_count'] for m in messages if m['type'] == 'ledgerClosed'))
        type(self).streamed = [subscribed + 1, self.tip, counted]
        # Every other subscriber to the same streams got the same messages for the ledgers they share.
        later = max(self.streams[f'all-{n}'][0]['result']['ledger_index'] for n in range(24))
        shared = [m for m in messages[1:] if m['ledger_index'] > later]
        self.assertTrue(shared)
        for n in range(24):
            self.assertEqual([m for m in self.streams[f'all-{n}'][1:] if m['ledger_index'] > later], shared, n)
        # The account stream (API version 1): exactly the account's transactions in those ledgers.
        account = self.streams['account']
        self.assertEqual(account[0]['status'], 'success')
        first = account[0].get('result', {}).get('ledger_index')
        hashes = [m['transaction']['hash'] for m in account[1:]]
        history = call(self.store_port, 'account_tx', {'account': self.account, 'ledger_index_min': subscribed + 1, 'forward': True, 'limit': 400})
        self.assertEqual(hashes, [t['tx']['hash'] for t in history['transactions']][-len(hashes):] if hashes else [], (first, self.account))
        self.assertTrue(hashes, f'no transactions of {self.account} were streamed')

    def test_9_sigterm_stops_everything(self):
        self.runner.send_signal(signal.SIGTERM)
        self.assertEqual(self.runner.wait(timeout=120), 0)
        left = subprocess.run(['pgrep', '-f', str(self.storage / 'follower')], capture_output=True, text=True).stdout.split()
        self.assertEqual(left, [], 'the live publisher or follower outlived xrpla run')


if __name__ == '__main__':
    unittest.main()
