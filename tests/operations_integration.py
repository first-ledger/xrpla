"""Local end-to-end regression tests: RPC, tail, pruning, recovery and import plans."""
import base64
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import threading
import unittest

BINARY, FIXTURE = map(os.path.abspath, sys.argv[1:3])
sys.argv = sys.argv[:1]


class WebSocket:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), 10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        response = b''
        while not response.endswith(b'\r\n\r\n'):
            response += self.read(1)
        assert response.startswith(b'HTTP/1.1 101'), response
        accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
        assert accept in response

    def read(self, count):
        data = b''
        while len(data) < count:
            part = self.sock.recv(count - len(data))
            if not part:
                raise EOFError('WebSocket closed')
            data += part
        return data

    def send(self, value): self.sock.sendall(self.frame(json.dumps(value)))

    @staticmethod
    def frame(text):
        data = text.encode(); mask = os.urandom(4)
        header = bytes([0x81, 0x80 | len(data)]) if len(data) < 126 else bytes([0x81, 0xfe]) + struct.pack('>H', len(data))
        return header + mask + bytes(x ^ mask[i % 4] for i, x in enumerate(data))

    def receive(self):
        # A large message comes in fragments: a text frame, then continuations up to the last (FIN).
        message, opcode = b'', 1
        while True:
            first, length = self.read(2)
            length &= 127
            if length == 126:
                length = struct.unpack('>H', self.read(2))[0]
            elif length == 127:
                length = struct.unpack('>Q', self.read(8))[0]
            assert first & 15 == opcode, first
            message += self.read(length); opcode = 0
            if first & 0x80:
                return json.loads(message)



def public_upstream(handler, request):
    """A fake upstream's answer to the probe `xrpla serve` sends a forward upstream (get_counts,
    which xrpld answers only an admin): refused, as xrpld 3.4.1's public port refuses it (403
    Forbidden, its httpReply). True when `request` was that probe."""
    if request.get('method') != 'get_counts': return False
    payload = b'Forbidden\r\n'
    handler.send_response(403); handler.send_header('Content-Length', str(len(payload))); handler.end_headers(); handler.wfile.write(payload)
    return True

class FakeUpstreamWebSocket:
    """An xrpld WebSocket for path_find: answers each request, then streams one path_find update,
    and refuses get_counts (`xrpla serve`'s probe) as xrpld refuses anyone but an admin (answers
    it, as to an admin, once `admin` is set; with a binary frame of bytes that are not UTF-8 while
    `garbled` is). Records every other request received, and the X-Forwarded-For of each
    connection that sent one; a request with `drop` closes its connection, one with `close_code`
    sends a close frame with that code (0: none) first. Counts the probes; answers the next
    `declining` upgrades 503."""
    def __init__(self):
        self.listener = socket.socket(); self.listener.bind(('127.0.0.1', 0)); self.listener.listen()
        self.port = self.listener.getsockname()[1]
        self.forwarded_for, self.requests, self.admin, self.garbled = [], [], False, False
        self.probes, self.declining = 0, 0
        threading.Thread(target=self.serve, daemon=True).start()
    def serve(self):
        while True:
            try: conn, _ = self.listener.accept()
            except OSError: return
            threading.Thread(target=self.session, args=(conn,), daemon=True).start()
    def session(self, conn):
        head = b''
        while b'\r\n\r\n' not in head: head += conn.recv(4096)
        headers = {k.strip().lower(): v.strip() for k, _, v in (l.partition(':') for l in head.decode().split('\r\n')[1:] if ':' in l)}
        relayed = False
        if self.declining:
            self.declining -= 1
            conn.sendall(b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'); conn.close(); return
        accept = base64.b64encode(hashlib.sha1((headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        conn.sendall(f'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n'.encode())
        def read(n):
            data = b''
            while len(data) < n:
                part = conn.recv(n - len(data))
                if not part: raise EOFError
                data += part
            return data
        def send(value):
            data = json.dumps(value).encode()
            conn.sendall((bytes([0x81, len(data)]) if len(data) < 126 else bytes([0x81, 126]) + struct.pack('>H', len(data))) + data)
        try:
            while True:
                first, length = read(2); length &= 127
                if length == 126: length = struct.unpack('>H', read(2))[0]
                mask = read(4); data = bytes(b ^ mask[i % 4] for i, b in enumerate(read(length)))
                if first & 15 != 1: continue
                request = json.loads(data)
                if request.get('command') == 'get_counts':
                    self.probes += 1
                    if self.garbled: conn.sendall(bytes([0x82, 6]) + b'\xff\xfe\x00{"x')
                    elif self.admin: send({'id': request.get('id'), 'type': 'response', 'status': 'success', 'result': {'uptime': 5}})
                    else: send({'id': request.get('id'), 'type': 'response', 'status': 'error', 'error': 'forbidden', 'error_code': 3,
                               'error_message': 'Bad credentials.', 'request': request})
                    continue
                if request.get('drop'): conn.close(); return
                if 'close_code' in request:
                    # As a server closes (RFC 6455 7.1.1): its close frame, the client's close frame
                    # back, then the server ends the connection.
                    payload = struct.pack('>H', request['close_code']) if request['close_code'] else b''
                    conn.sendall(bytes([0x88, len(payload)]) + payload); conn.settimeout(5)
                    try: conn.recv(4096)
                    except OSError: pass
                    conn.close(); return
                if not relayed: self.forwarded_for.append(headers.get('x-forwarded-for')); relayed = True
                self.requests.append(request)
                send({'id': request.get('id'), 'status': 'success', 'type': 'response', 'result': {'alternatives': [], 'full_reply': False}})
                send({'id': request.get('id'), 'type': 'path_find', 'alternatives': [], 'full_reply': True})
        except (EOFError, OSError):
            conn.close()


class Operations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_dir = tempfile.TemporaryDirectory(prefix='xrpla-operations-source-')
        cls.source = Path(cls.fixture_dir.name) / 'archive'
        cls.export(cls.source, 8)
        cls.expected = json.loads((cls.source / 'expected.json').read_text())

    @classmethod
    def tearDownClass(cls):
        cls.fixture_dir.cleanup()

    @staticmethod
    def export(path, count):
        env = dict(os.environ, XRPLA_CORE_FIXTURE_DIR=str(path), XRPLA_CORE_FIXTURE_COUNT=str(count))
        result = subprocess.run([FIXTURE, '--gtest_filter=CoreFixture.Export'], env=env, capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='xrpla-operations-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.data = self.root / 'data'
        self.fetch('65536-65538')
        self.build()

    def cli(self, *args, ok=True, stdin=None):
        result = subprocess.run([BINARY, *map(str, args)], input=stdin, capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode == 0, ok, result.stdout + result.stderr)
        return json.loads(result.stdout if ok else result.stderr)

    def fetch(self, range, source=None):
        source = source or self.source
        return self.cli('fetch', '--archive', source, '--range', range, '--dir', self.data, '--publisher-key', source / 'publisher.pub')

    def build(self):
        return self.cli('build', '--dir', self.data, '--publisher-key', self.source / 'publisher.pub')

    def rpc(self, method, **params):
        return self.cli('rpc', '--dir', self.data, '--request', '-', stdin=json.dumps({'method': method, 'params': params}))

    def test_rpc_pagination_binary_versions_and_errors(self):
        account = self.expected['address']
        for version in (1, 2):
            info = self.rpc('account_info', account=account, api_version=version)
            self.assertEqual(info['account_data']['Balance'], '900000')
            self.assertEqual(info['ledger_index'], 65538)
            self.assertEqual(info['status'], 'success')
            tx = self.rpc('tx', transaction=self.expected['tx0'], api_version=version)
            self.assertEqual(tx['ledger_index'], 65536)
            self.assertEqual(tx['meta']['TransactionIndex'], 0)
            for forward in (False, True):
                # As a public xrpld: at least 10 a page, so both transactions and no marker.
                first = self.rpc('account_tx', account=account, limit=1, forward=forward, api_version=version)
                self.assertEqual(first['limit'], 10)
                self.assertNotIn('marker', first)
                items = first['transactions']
                self.assertEqual([x['meta']['TransactionIndex'] for x in items], [0, 1] if forward else [1, 0])
                self.assertIn('tx' if version == 1 else 'tx_json', items[0])
                # A marker naming the second: the page from its row on.
                ledger = items[1]['tx']['ledger_index'] if version == 1 else items[1]['ledger_index']
                marker = {'ledger': ledger, 'seq': items[1]['meta']['TransactionIndex']}
                second = self.rpc('account_tx', account=account, limit=1, forward=forward, api_version=version, marker=marker)
                self.assertEqual([x['meta']['TransactionIndex'] for x in second['transactions']], [1] if forward else [0])
                self.assertNotIn('marker', second)
        first = self.rpc('ledger_data', ledger_index=65537, limit=1, binary=True)
        found = list(first['state'])
        while 'marker' in first:
            first = self.rpc('ledger_data', ledger_index=65537, limit=1, binary=True, marker=first['marker'])
            found += first['state']
        self.assertEqual(len(found), 3)
        self.assertEqual(len({x['index'] for x in found}), 3)
        entry = self.rpc('ledger_entry', account_root=account, binary=True)
        self.assertEqual(entry['index'].lower(), self.expected['account_key'])
        for method in ('account_objects', 'account_lines', 'account_offers', 'account_nfts', 'account_currencies', 'gateway_balances'):
            self.assertEqual(self.rpc(method, account=account)['status'], 'success', method)
        # rippled treats a negative ledger_data limit as the maximum; a non-integer limit is an error.
        cases = [('ledger_data', {'marker': 'bad'}), ('ledger_data', {'limit': 'x'}), ('account_tx', {'account': account, 'marker': {}}), ('account_info', {'account': 'bad'})]
        for method, params in cases:
            self.assertEqual(self.rpc(method, **params)['status'], 'error')
        self.assertEqual(self.rpc('submit', tx_blob='00')['error'], 'notSupported')
        self.assertEqual(self.rpc('unknown')['error'], 'unknownCmd')
        # Checked before the ledger is looked up, as xrpld does: refused so for a ledger not held.
        missing = self.rpc('deposit_authorized', destination_account=account, ledger_index=1)
        self.assertEqual((missing['error'], missing['error_message']), ('invalidParams', "Missing field 'source_account'."))
        self.assertEqual(self.rpc('deposit_authorized', source_account='bad', ledger_index=1)['error'], 'actMalformed')
        for method in ('nft_buy_offers', 'nft_sell_offers'):
            self.assertEqual(self.rpc(method, nft_id='00', ledger_index=1)['error_message'], "Invalid field 'nft_id'.")
            self.assertEqual(self.rpc(method, nft_id='0' * 64, limit=0, ledger_index=1)['error_message'], "Invalid field 'limit'.")
        # xrpld's JSON reader keeps 2147483647 a signed integer, which its asInt reads.
        self.assertNotEqual(self.rpc('account_tx', account=account, ledger_index_min=2147483647).get('error'), 'internal')
        self.assertEqual(self.rpc('account_tx', account=account, ledger_index_min=2147483648)['error'], 'internal')
        # The legacy `ledger` names a ledger for transaction_entry as ledger_index does.
        self.assertNotEqual(self.rpc('transaction_entry', tx_hash=self.expected['tx0'], ledger=65536).get('error'), 'notYetImplemented')

    def test_incremental_prune_and_checkpoint_recovery(self):
        before = self.cli('status', '--dir', self.data)
        self.cli('compact', '--dir', self.data)
        self.assertEqual(self.cli('status', '--dir', self.data), before)
        self.fetch('65536-65539')
        result = self.build()
        self.assertEqual(result['resumed_from'], 65538)
        self.assertEqual(result['ledgers_added'], 1)
        # Losing scratch nodes must not make the canonical store irrecoverable.
        shutil.rmtree(self.data / '.verification-nodes')
        result = self.build()
        self.assertEqual(result['resumed_from'], 0)
        self.assertEqual(result['ledgers_added'], 0)
        self.cli('prune', '--dir', self.data, '--state-first', 65538)
        self.assertEqual(self.rpc('ledger_data', ledger_index=65537)['error'], 'lgrNotFound')
        self.assertEqual(self.rpc('tx', transaction=self.expected['tx0'])['status'], 'success')
        self.fetch('65536-65540')
        self.assertEqual(self.build()['ledgers_added'], 1)
        self.assertEqual(self.rpc('account_info', account=self.expected['address'])['status'], 'success')

    def test_day_range_and_tail_once(self):
        self.data = self.root / 'tail-data'
        result = self.cli('tail', '--once', '--archive', self.source, '--range', '1d', '--dir', self.data,
                          '--publisher-key', self.source / 'publisher.pub', '--state-window', '3')
        self.assertEqual(result['ledgers_added'], 8)
        self.assertEqual(result['store']['state_ledgers'], {'first': 65541, 'last': 65543})
        result = self.cli('tail', '--once', '--archive', self.source, '--dir', self.data,
                          '--publisher-key', self.source / 'publisher.pub')
        self.assertEqual(result['ledgers_added'], 0)
        older = self.root / 'older'
        self.export(older, 3)
        self.cli('tail', '--once', '--archive', older, '--dir', self.data,
                 '--publisher-key', older / 'publisher.pub', ok=False)

    def test_day_range_boundary(self):
        source = self.root / 'days'; self.export(source, 10000)
        self.data = self.root / 'day-data'
        result = self.fetch('1d', source)
        self.assertEqual(result['range'], {'first': 66895, 'last': 75535})

    def test_forwarding_keeps_request_and_requires_configuration(self):
        requests = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                requests.append(request)
                payload = json.dumps({'result': {'status': 'success', 'drops': {'base_fee': '10'}}}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args):
                pass
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        result = self.cli('rpc', '--dir', self.data, '--request', '-', '--forward', f'http://127.0.0.1:{server.server_port}/',
                          stdin=json.dumps({'method': 'fee', 'params': {'api_version': 2}}))
        self.assertEqual(result['drops']['base_fee'], '10')
        self.assertEqual(requests[0]['method'], 'fee')
        self.assertEqual(requests[0]['params'][0]['api_version'], 2)

    def test_native_import_and_clio_plan_are_idempotent(self):
        # NuDB is the default rippled target; RocksDB remains selectable.
        target = self.root / 'native'
        result = self.cli('import', 'rippled', '--dir', self.data, '--out', target)
        self.assertEqual(result['transactions'], 2)
        self.assertEqual(result['format'], 'rippled-3.3.0-nudb')
        self.assertTrue(list((target / 'node-db').glob('*.dat')))
        self.assertIn('type = NuDB', (target / 'rippled.cfg').read_text())
        self.cli('import', 'rippled', '--dir', self.data, '--out', target)
        error = self.cli('import', 'rippled', '--dir', self.data, '--out', target, '--backend', 'rocksdb', ok=False)
        self.assertIn('version differs', error['error'])
        rocks = self.root / 'native-rocksdb'
        rocks_result = self.cli('import', 'rippled', '--dir', self.data, '--out', rocks, '--backend', 'rocksdb')
        self.assertEqual(rocks_result['format'], 'rippled-3.3.0-rocksdb')
        self.assertEqual(rocks_result['last_hash'], result['last_hash'])
        self.assertTrue((rocks / 'node-db' / 'CURRENT').exists())
        self.cli('compact', '--dir', rocks, '--native')
        self.cli('import', 'rippled', '--dir', self.data, '--out', rocks, '--backend', 'rocksdb')
        for native in (target, rocks):
            with sqlite3.connect(native / 'ledger.db') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM Ledgers').fetchone()[0], 3)
            with sqlite3.connect(native / 'transaction.db') as db:
                self.assertEqual(db.execute('SELECT count(*) FROM Transactions').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT count(*) FROM AccountTransactions').fetchone()[0], 2)
        receipt = target / 'xrpla-import.json'
        current = receipt.read_bytes()
        old = json.loads(current); old['format'] = 'rippled-3.2.0-rocksdb'
        receipt.write_text(json.dumps(old))
        before = receipt.read_bytes()
        error = self.cli('import', 'rippled', '--dir', self.data, '--out', target, ok=False)
        self.assertIn('version differs', error['error'])
        self.assertEqual(receipt.read_bytes(), before)
        receipt.write_bytes(current)
        plan = self.root / 'clio'
        self.cli('import', 'clio', '--dir', self.data, '--out', plan)
        original = (plan / 'clio-plan.jsonl').read_bytes()
        self.cli('import', 'clio', '--dir', self.data, '--out', plan)
        self.assertEqual(original, (plan / 'clio-plan.jsonl').read_bytes())
        records = [json.loads(x) for x in original.splitlines()]
        self.assertEqual(records[0]['last'], 65538)
        self.assertEqual(len([x for x in records if x.get('table') == 'transactions']), 2)
        self.assertEqual(len([x for x in records if x.get('table') == 'ledgers']), 3)
        # Existing keys whose contents change do not gain successor versions.
        # A deletion only rewires its predecessor; it does not write a new row
        # for the deleted key (Clio 2.7.1 SuccessorExt contract).
        successors = [x['values'] for x in records if x.get('table') == 'successor']
        self.assertFalse(any(x['seq'] == 65538 and x['key']['blob'] == self.expected['directory_key'] for x in successors))

    def serve(self, *extra, tries=100):
        """`xrpla serve` on this data with `extra` options; its HTTP and WebSocket port (waited for
        `tries` times, 50 ms apart)."""
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        server = subprocess.Popen([BINARY, 'serve', '--dir', str(self.data), '--http', f'127.0.0.1:{port}', '--ws', f'127.0.0.1:{port}', *map(str, extra)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if server.poll() is None:
                server.terminate()
                try: server.communicate(timeout=15)
                except subprocess.TimeoutExpired: server.kill(); server.communicate()
        self.addCleanup(cleanup)
        for _ in range(tries):
            try:
                conn = socket.create_connection(('127.0.0.1', port), .1); conn.close(); return port
            except OSError:
                if server.poll() is not None: self.fail(server.communicate()[1].decode())
                time.sleep(.05)
        self.fail('server did not start')

    def post(self, port, method, params):
        client = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
        client.request('POST', '/', json.dumps({'method': method, 'params': [params]}))
        result = json.loads(client.getresponse().read())['result']; client.close()
        return result

    def test_forwarded_requests_go_once_with_the_client_address(self):
        requests = []
        class Failing(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if public_upstream(self, request): return
                requests.append((request, self.headers.get('X-Forwarded-For')))
                self.send_response(503); self.send_header('Content-Length', '0'); self.end_headers()
            def log_message(self, *args): pass
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Failing)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown)
        with socket.socket() as sock:  # a WebSocket upstream that is not there
            sock.bind(('127.0.0.1', 0)); closed = sock.getsockname()[1]
        port = self.serve('--forward', f'http://127.0.0.1:{upstream.server_port}/', '--forward-ws', f'ws://127.0.0.1:{closed}')
        # A failed submission is not repeated: the client gets notReady and decides.
        result = self.post(port, 'submit', {'tx_blob': '1200'})
        self.assertEqual(result['error'], 'notReady')
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0][0]['method'], 'submit')
        self.assertEqual(requests[0][1], '127.0.0.1')
        # path_find streams: over HTTP it answers as xrpld does there.
        self.assertEqual(self.post(port, 'path_find', {'subcommand': 'create'})['error'], 'noEvents')
        self.assertEqual(len(requests), 1)
        # Only public methods this node answers go upstream for the open ledger: an admin command or
        # an unknown name never does, whatever ledger it names (the upstream would run it as this
        # server, often an admin address).
        for method in ('stop', 'ledger_accept', 'validation_create', 'log_level', 'no_such_method'):
            for params in ({'ledger_index': 'current'}, {'ledger': 'current'}, {'ledger': ''}):
                self.assertNotEqual(self.post(port, method, params).get('error'), 'notReady', (method, params))
        self.assertEqual(len(requests), 1)
        self.assertEqual(self.post(port, 'account_info', {'account': 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh', 'ledger_index': 'current'})['error'], 'notReady')
        self.assertEqual([r[0]['method'] for r in requests], ['submit', 'account_info'])
        # batch is not forwarded (xrpld runs each element as a request of its own, any method), and
        # what xrpld allows only an admin is answered as for anyone else, upstream never asked.
        self.assertNotEqual(self.post(port, 'batch', {'method': 'stop'}).get('error'), 'notReady')
        self.assertEqual(self.post(port, 'submit', {'secret': 's', 'tx_json': {}})['error_message'], 'Signing is not supported by this server.')
        self.assertEqual(self.post(port, 'channel_authorize', {'secret': 's'})['error'], 'notSupported')
        self.assertEqual(self.post(port, 'ledger', {'ledger_index': 'current', 'full': True})['error'], 'noPermission')
        # simulate refuses the keys signing would take as xrpld's doSimulate does, after its check
        # of binary, the first one given named: they never go upstream.
        refused = self.post(port, 'simulate', {'tx_json': {}, 'seed': 's', 'secret': 's'})
        self.assertEqual((refused['error'], refused['error_message'], refused['request']['secret']), ('invalidParams', "Invalid field 'secret'.", '<masked>'))
        self.assertEqual(self.post(port, 'simulate', {'tx_json': {}, 'passphrase': 'p', 'binary': 1})['error_message'], "Invalid field 'binary'.")
        self.assertEqual(len(requests), 2)
        self.post(port, 'account_lines', {'account': 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh', 'ledger_index': 'current', 'limit': 100000})
        self.assertEqual(requests[-1][0]['params'][0]['limit'], 400)
        # An empty ledger_index is the open ledger too (xrpld's ledgerFromIndex).
        self.assertEqual(self.post(port, 'account_info', {'account': 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh', 'ledger_index': ''})['error'], 'notReady')
        self.assertEqual(requests[-1][0]['params'][0]['ledger_index'], '')
        # A limit above the public maximum is lowered; any other goes as sent, for the upstream to
        # judge as xrpld's readLimitField does (0 and negative ones refused, small ones raised).
        for limit in (0, -1, 5):
            self.post(port, 'account_lines', {'account': 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh', 'ledger_index': 'current', 'limit': limit})
            self.assertEqual(requests[-1][0]['params'][0]['limit'], limit)
        # What only the client's role, keys or envelope use is not sent on; a ripplerpc 2.0 client
        # still gets its answer in that shape (from this server, not the upstream).
        status, text = self.raw_post(port, {'method': 'submit', 'params': [{'tx_blob': '1200', 'secret': 's', 'seed': 's', 'admin_user': 'a',
                                                                                'admin_password': 'p', 'key_type': 'secp256k1', 'ripplerpc': '2.0', 'id': 4}]})
        self.assertEqual(sorted(requests[-1][0]['params'][0]), ['tx_blob'])
        answer = json.loads(text)
        self.assertEqual((status, answer['ripplerpc'], answer['id'], answer['error']['error']), (200, '2.0', 4, 'notReady'))
        # The ledger's seven flags are checked before its whole state is refused (LedgerHandler::check).
        sent = len(requests)
        self.assertEqual(self.post(port, 'ledger', {'ledger_index': 'current', 'full': True, 'queue': 1})['error'], 'invalidParams')
        self.assertEqual(self.post(port, 'ledger', {'ledger_index': 'current', 'accounts': True, 'binary': False})['error'], 'noPermission')
        self.assertEqual(len(requests), sent)
        # Over WebSocket it is relayed; an upstream that cannot be reached answers notReady.
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        ws.send({'id': 3, 'command': 'path_find', 'subcommand': 'create'})
        answer = ws.receive()
        self.assertEqual((answer['id'], answer['status'], answer['error']), (3, 'error', 'notReady'))

    def test_an_upstream_that_grants_admin_is_refused(self):
        # get_counts answered: this server is the upstream's admin, and so would every client be.
        class Admin(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                payload = json.dumps({'result': {'status': 'success', 'uptime': 5}}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args): pass
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Admin)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        run = subprocess.run([BINARY, 'serve', '--dir', str(self.data), '--http', f'127.0.0.1:{port}', '--forward', f'http://127.0.0.1:{upstream.server_port}/'],
                             capture_output=True, text=True, timeout=120)
        self.assertNotEqual(run.returncode, 0)
        self.assertIn('answers this server as an admin', run.stdout + run.stderr)

    def test_an_upstream_not_checked_is_sent_nothing(self):
        # Started before its upstream (nothing on the port yet), the server sends it nothing until it
        # has been asked again and refused get_counts: while it grants admin, nothing but the probe.
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); upstream_port = sock.getsockname()[1]
        methods, admin = [], threading.Event(); admin.set()
        class Upstream(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                methods.append(request['method'])
                if not admin.is_set() and public_upstream(self, request): return
                payload = json.dumps({'result': {'status': 'success', 'drops': {'base_fee': '10'}}}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args): pass
        port = self.serve('--forward', f'http://127.0.0.1:{upstream_port}/')
        self.assertEqual(self.post(port, 'fee', {})['error'], 'notReady')
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', upstream_port), Upstream)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown)
        # Asked again at most every 10 s, as requests come.
        for _ in range(60):
            self.assertEqual(self.post(port, 'fee', {})['error'], 'notReady')
            if methods: break
            time.sleep(.5)
        self.assertEqual(methods, ['get_counts'])
        self.assertEqual(self.post(port, 'fee', {})['error'], 'notReady')
        # Refusing it now (a public port), it is sent requests once asked again.
        admin.clear()
        for _ in range(60):
            result = self.post(port, 'fee', {})
            if 'fee' in methods: break
            self.assertEqual(result['error'], 'notReady')
            time.sleep(.5)
        self.assertEqual(result['drops']['base_fee'], '10')
        self.assertEqual(methods.count('fee'), 1)

    def restarting_upstream(self, methods, state):
        """An upstream that passed its first probe (403), answers get_counts after that with
        state['counts'] (403 refused as a public port; 200 answered, as to an admin), closes the
        connection of the next state['failing'] other requests unanswered (as a process
        restarting does), and answers the next state['refusing'] after those 503 (busy). Records
        every method asked; its port."""
        class Upstream(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                methods.append(request['method'])
                if request['method'] == 'get_counts':
                    if state['counts'] == 403 and public_upstream(self, request): return
                    status = state['counts']
                elif state['failing']: state['failing'] -= 1; self.close_connection = True; return
                elif state.get('refusing'): state['refusing'] -= 1; status = 503
                else: status = 200
                payload = json.dumps({'result': {'status': 'success', 'drops': {'base_fee': '10'}}}).encode() if status == 200 else b''
                self.send_response(status); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args): pass
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown)
        return upstream.server_port

    def test_an_upstream_found_an_admin_later_is_sent_nothing_until_found_public(self):
        # Restarted with this server among its admins: a forwarded request that cannot reach it
        # has it asked again within 10 s, without holding the requests meanwhile; once the probe
        # finds it an admin nothing more is forwarded, until a later probe finds it public.
        methods, state = [], {'counts': 403, 'failing': 1}
        port = self.serve('--forward', f'http://127.0.0.1:{self.restarting_upstream(methods, state)}/')
        state['counts'] = 200
        started = time.monotonic()
        self.assertEqual(self.post(port, 'fee', {})['error'], 'notReady')  # failing while it restarts
        self.assertEqual(self.post(port, 'fee', {})['drops']['base_fee'], '10')  # not held
        self.assertLess(time.monotonic() - started, 3)
        for _ in range(60):
            result = self.post(port, 'fee', {})
            if methods.count('get_counts') == 2 and 'error' in result: break
            time.sleep(.5)
        self.assertEqual(result['error'], 'notReady')
        sent = methods.count('fee')
        for _ in range(3):
            self.assertEqual(self.post(port, 'fee', {})['error'], 'notReady')
        self.assertEqual(methods.count('fee'), sent)
        state['counts'] = 403
        for _ in range(60):
            result = self.post(port, 'fee', {})
            if 'error' not in result: break
            time.sleep(.5)
        self.assertEqual(result['drops']['base_fee'], '10')
        self.assertEqual(methods.count('get_counts'), 3)

    def test_an_upstream_that_answers_an_error_is_not_asked_again(self):
        # What the upstream answers (503: busy, say) is an answer: it has the upstream asked again
        # for nothing, so a client cannot have it asked every 10 s.
        methods, state = [], {'counts': 403, 'failing': 0, 'refusing': 1}
        port = self.serve('--forward', f'http://127.0.0.1:{self.restarting_upstream(methods, state)}/')
        self.assertEqual(self.post(port, 'fee', {})['error'], 'notReady')
        time.sleep(1.5)
        self.assertEqual(self.post(port, 'fee', {})['drops']['base_fee'], '10')
        self.assertEqual((methods.count('get_counts'), methods.count('fee')), (1, 2))

    def test_requests_go_only_to_the_addresses_checked(self):
        # "localhost" is 127.0.0.1, where the upstream refuses get_counts, and ::1, where another
        # server answers it 503 (busy: its role unknown). Requests are pinned to 127.0.0.1, the
        # address found a public client; ::1 is sent none.
        if {'127.0.0.1', '::1'} - {info[4][0] for info in socket.getaddrinfo('localhost', None, type=socket.SOCK_STREAM)}:
            self.skipTest('localhost is not both 127.0.0.1 and ::1 here')
        methods, state, other = [], {'counts': 403, 'failing': 0}, []
        upstream_port = self.restarting_upstream(methods, state)
        class Other(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                other.append(json.loads(self.rfile.read(int(self.headers['Content-Length'])))['method'])
                self.send_response(503); self.send_header('Content-Length', '0'); self.end_headers()
            def log_message(self, *args): pass
        class V6(http.server.ThreadingHTTPServer): address_family = socket.AF_INET6
        try: v6 = V6(('::1', upstream_port), Other)
        except OSError: self.skipTest('no IPv6 loopback here')
        threading.Thread(target=v6.serve_forever, daemon=True).start()
        self.addCleanup(v6.server_close); self.addCleanup(v6.shutdown)
        port = self.serve('--forward', f'http://localhost:{upstream_port}/')
        for _ in range(3):
            self.assertEqual(self.post(port, 'fee', {})['drops']['base_fee'], '10')
        self.assertEqual((methods.count('fee'), other), (3, ['get_counts']))

    def test_an_upstream_without_get_counts_passes_unverified(self):
        # A Clio upstream (s1.ripple.com, xrplcluster.com) has no get_counts: it is forwarded to, and
        # the role it gives this server, which cannot be confirmed, is logged once as such.
        class Clio(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                result = ({'status': 'error', 'error': 'unknownCmd', 'error_code': 32, 'error_message': 'Unknown command.'} if request['method'] == 'get_counts'
                          else {'status': 'success', 'drops': {'base_fee': '10'}})
                payload = json.dumps({'result': result}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args): pass
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Clio)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        server = subprocess.Popen([BINARY, 'serve', '--dir', str(self.data), '--http', f'127.0.0.1:{port}', '--forward', f'http://127.0.0.1:{upstream.server_port}/'],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(lambda: server.poll() is None and (server.kill(), server.communicate()))
        for _ in range(100):
            try:
                conn = socket.create_connection(('127.0.0.1', port), .1); conn.close(); break
            except OSError:
                if server.poll() is not None: self.fail(server.communicate()[1].decode())
                time.sleep(.05)
        for _ in range(2):
            self.assertEqual(self.post(port, 'fee', {})['drops']['base_fee'], '10')
        server.terminate(); _, err = server.communicate(timeout=30)
        events = []
        for line in err.decode().splitlines():
            try: events.append(json.loads(line).get('event'))
            except (ValueError, AttributeError): pass
        self.assertEqual(events.count('forward_unverifiable'), 1)
        self.assertNotIn('forward_checked', events)

    def test_forward_urls_must_name_their_protocol(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        for option, url in (('--forward', 'ws://127.0.0.1:9/'), ('--forward-ws', 'http://127.0.0.1:9/')):
            run = subprocess.run([BINARY, 'serve', '--dir', str(self.data), '--http', f'127.0.0.1:{port}', option, url], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn('must be a', run.stdout + run.stderr)

    def test_path_find_is_relayed_to_the_upstream_websocket(self):
        upstream = FakeUpstreamWebSocket(); self.addCleanup(upstream.listener.close)
        port = self.serve('--forward', 'http://127.0.0.1:9/', '--forward-ws', f'ws://127.0.0.1:{upstream.port}')
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        ws.send({'id': 7, 'command': 'path_find', 'subcommand': 'create', 'source_account': 'r1', 'destination_account': 'r2', 'destination_amount': '1'})
        answer = ws.receive()
        self.assertEqual((answer['id'], answer['status'], answer['type']), (7, 'success', 'response'))
        update = ws.receive()  # the search's streamed update follows its answer
        self.assertEqual((update['id'], update['type']), (7, 'path_find'))
        ws.send({'id': 8, 'command': 'path_find', 'subcommand': 'close'})
        self.assertEqual(ws.receive()['id'], 8)
        self.assertEqual([r['subcommand'] for r in upstream.requests], ['create', 'close'])  # one upstream connection
        self.assertEqual(upstream.forwarded_for, ['127.0.0.1'])

    def test_an_open_path_search_outlives_a_failed_check(self):
        # The relayed request carries no credentials (xrpld reads a WebSocket request's role from
        # admin_user and admin_password) nor keys.
        upstream = FakeUpstreamWebSocket(); self.addCleanup(upstream.listener.close)
        port = self.serve('--forward', 'http://127.0.0.1:9/', '--forward-ws', f'ws://127.0.0.1:{upstream.port}')
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        ws.send({'id': 1, 'command': 'path_find', 'subcommand': 'create', 'admin_user': 'a', 'admin_password': 'p', 'secret': 's', 'seed': 's',
                 'seed_hex': '00', 'passphrase': 'p', 'source_account': 'r1'})
        self.assertEqual((ws.receive()['id'], ws.receive()['type']), (1, 'path_find'))
        self.assertEqual(sorted(upstream.requests[0]), ['command', 'id', 'source_account', 'subcommand'])
        # The upstream now answers this server as an admin: another session's relay failing has it
        # asked again within 10 s, and from then on no search is started...
        upstream.admin = True
        other = WebSocket(port); self.addCleanup(other.sock.close)
        other.send({'id': 2, 'command': 'path_find', 'subcommand': 'create', 'drop': True})
        for _ in range(120):
            time.sleep(.25)
            answer = self.search(port)
            if answer.get('error') == 'notReady': break
        self.assertEqual(answer.get('error'), 'notReady')
        ws.send({'id': 4, 'command': 'path_find', 'subcommand': 'create'})
        self.assertEqual(ws.receive()['error'], 'notReady')
        # ...but the one open can still be closed, on the connection that was checked.
        ws.send({'id': 5, 'command': 'path_find', 'subcommand': 'close'})
        answer = ws.receive()
        self.assertEqual((answer['id'], answer['type'], answer['status']), (5, 'response', 'success'))
        self.assertIn({'id': 5, 'command': 'path_find', 'subcommand': 'close'}, upstream.requests)
        # Found public again by a later probe, searches start again.
        upstream.admin = False
        for _ in range(120):
            time.sleep(.25)
            answer = self.search(port)
            if answer.get('status') == 'success': break
        self.assertEqual((answer['id'], answer['status']), (2, 'success'))

    def search(self, port):
        """A path search in a session of its own; its answer."""
        ws = WebSocket(port)
        try:
            ws.send({'id': 2, 'command': 'path_find', 'subcommand': 'create'})
            return ws.receive()
        finally: ws.sock.close()

    def test_a_probe_answered_with_bytes_that_are_not_text_tells_nothing(self):
        # A binary frame that is not UTF-8 (nor JSON) answering the probe once ended the process
        # (its parse error, quoting the bytes, could not be logged as JSON). It tells nothing: the
        # finding before holds, and the server goes on answering.
        upstream = FakeUpstreamWebSocket(); self.addCleanup(upstream.listener.close)
        port = self.serve('--forward', 'http://127.0.0.1:9/', '--forward-ws', f'ws://127.0.0.1:{upstream.port}')
        probes = upstream.probes
        upstream.garbled = True
        failing = WebSocket(port); self.addCleanup(failing.sock.close)
        failing.send({'id': 1, 'command': 'path_find', 'subcommand': 'create', 'drop': True})  # a relay failing has it asked again
        for _ in range(120):
            time.sleep(.25)
            self.assertEqual(self.search(port).get('status'), 'success')
            if upstream.probes > probes: break
        self.assertGreater(upstream.probes, probes)
        time.sleep(1)  # the probe's finding logged
        answer = self.search(port)
        self.assertEqual((answer['id'], answer['status']), (2, 'success'))

    def raw_post(self, port, body):
        client = http.client.HTTPConnection('127.0.0.1', port, timeout=30)
        client.request('POST', '/', body if isinstance(body, str) else json.dumps(body))
        response = client.getresponse(); text = response.read().decode(); client.close()
        return response.status, text

    def test_requests_xrpld_refuses_before_running_them(self):
        # As xrpld 3.4.1's ServerHandler: over HTTP a 400 and a line of text, over WebSocket an
        # error response; the server goes on answering.
        port = self.serve()
        deep = '{"method": "account_info", "params": [{"x": ' + '[' * 30 + ']' * 30 + '}]}'
        for body, status, text in [
                (deep, 400, 'Unable to parse request: * Line 1, Column 68\n  Syntax error: maximum nesting depth exceeded\n\r\n'),
                ('{"method": "ledger", "params": [{"ledger_index": 5000000000}]}', 400,
                 "Unable to parse request: * Line 1, Column 50\n  '5000000000' exceeds the allowable range.\n\r\n"),
                ('"text"', 400, 'Unable to parse request: * Line 1, Column 1\n  A valid JSON document must be either an array or an object value.\n\r\n'),
                ('[' * 26 + ']' * 26, 400, 'Unable to parse request: \r\n'),
                # An empty object is false to xrpld's `!jsonOrig`, refused before its method is looked at.
                ({}, 400, 'Unable to parse request: \r\n'),
                # A key twice in one object (nlohmann would keep the last), named as decoded.
                ('{"method": "ledger", "params": [{"ledger_index": 1, "ledger_index": 2}]}', 400,
                 "Unable to parse request: * Line 1, Column 53\n  Key 'ledger_index' appears twice.\n\r\n"),
                ('{"method": "ledger_range", "params": [{"x": 1, "\\u0078": 2}]}', 400,
                 "Unable to parse request: * Line 1, Column 48\n  Key 'x' appears twice.\n\r\n"),
                ({'method': 'ledger', 'params': [{'api_version': 3}]}, 400, 'invalid_API_version\r\n'),
                ({'params': [{}]}, 400, 'Null method\r\n'),
                ({'method': 5}, 400, 'method is not string\r\n'),
                ({'method': 'ledger', 'params': {'ledger_index': 1}}, 400, 'params unparsable\r\n')]:
            self.assertEqual(self.raw_post(port, body), (status, text), body)
        status, text = self.raw_post(port, {'method': 'ledger', 'params': [{'ledger_index': 'abc', 'secret': 's', 'id': 7}]})
        answer = json.loads(text)
        self.assertEqual((status, answer['id'], answer['result']['error_message'], answer['result']['request']['secret']),
                         (200, 7, "Invalid field 'ledger_index', not string or number.", '<masked>'))
        # The same keys in different objects are no duplicates.
        status, text = self.raw_post(port, '{"method": "ledger_range", "params": [{"a": {"id": 1}, "id": 2}]}')
        self.assertEqual((status, json.loads(text)['id']), (200, 2))
        # ripplerpc 2.0 (xrpld's processRequest): an error under `error`, with code and message, no echo.
        status, text = self.raw_post(port, {'method': 'ledger', 'params': [{'ledger_index': 'abc', 'ripplerpc': '2.0', 'id': 9}]})
        answer = json.loads(text)
        self.assertEqual((status, answer['id'], answer['ripplerpc']), (200, 9, '2.0'))
        self.assertNotIn('result', answer)
        error = answer['error']
        self.assertEqual((error['status'], error['error'], error['code'], error['message']),
                         ('error', 'invalidParams', error['error_code'], "Invalid field 'ledger_index', not string or number."))
        self.assertNotIn('error_message', error); self.assertNotIn('request', error)
        self.assertEqual(json.loads(self.raw_post(port, {'method': 'ledger_range', 'params': [{'ripplerpc': '2.0'}]})[1])['result']['status'], 'success')
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        ws.send({'id': 1, 'command': 'ledger', 'api_version': '2'})
        self.assertEqual({k: v for k, v in ws.receive().items() if k != 'request'},
                         {'type': 'response', 'status': 'error', 'error': 'invalid_API_version', 'id': 1, 'api_version': '2'})
        ws.send({'id': 2, 'ledger_index': 5})
        self.assertEqual(ws.receive()['error'], 'missingCommand')
        ws.sock.sendall(ws.frame('[' * 30 + ']' * 30))
        self.assertEqual(ws.receive(), {'type': 'error', 'error': 'jsonInvalid', 'value': '[' * 30 + ']' * 30})
        ws.send({'id': 3, 'method': 'ledger_range'})
        self.assertEqual(ws.receive()['result']['ledger_index_max'], 65538)
        # An error echoes the request as sent: `method` is not turned into `command`.
        ws.send({'id': 4, 'method': 'ledger', 'ledger_index': 'abc'})
        answer = ws.receive()
        self.assertEqual((answer['id'], answer['error'], answer['request']), (4, 'invalidParams', {'id': 4, 'method': 'ledger', 'ledger_index': 'abc'}))
        ws.sock.sendall(ws.frame('{"id": 5, "command": "ping", "id": 6}'))
        self.assertEqual(ws.receive(), {'type': 'error', 'error': 'jsonInvalid', 'value': '{"id": 5, "command": "ping", "id": 6}'})

    def test_websocket_requests_are_answered_as_each_is_ready(self):
        # A session's requests run alongside each other and are answered by id as each is ready:
        # a quick one is not held behind a slower one sent before it (here a forwarded fee whose
        # upstream holds it), while subscribe and unsubscribe still apply in the order sent.
        release = threading.Event()
        class Held(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                if public_upstream(self, json.loads(self.rfile.read(int(self.headers['Content-Length'])))): return
                release.wait(30)
                payload = json.dumps({'result': {'status': 'success', 'drops': {'base_fee': '10'}}}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args): pass
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Held)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown); self.addCleanup(release.set)
        port = self.serve('--forward', f'http://127.0.0.1:{upstream.server_port}/')
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        ws.send({'id': 1, 'command': 'fee'})
        ws.send({'id': 2, 'command': 'ledger_range'})
        ws.send({'id': 3, 'command': 'subscribe', 'streams': ['ledger']})
        ws.send({'id': 4, 'command': 'unsubscribe', 'streams': ['ledger']})
        answers = [ws.receive() for _ in range(3)]
        ids = [a['id'] for a in answers]
        self.assertEqual(sorted(ids), [2, 3, 4])
        self.assertLess(ids.index(3), ids.index(4))
        self.assertTrue(all(a['status'] == 'success' for a in answers))
        self.assertEqual(answers[ids.index(2)]['result']['ledger_index_max'], 65538)
        release.set()
        answer = ws.receive()
        self.assertEqual((answer['id'], answer['result']['drops']['base_fee']), (1, '10'))

    def test_pipelined_large_replies_are_all_sent(self):
        # Eight replies of 3 MB answered at once, to a client not reading yet: more than the
        # 16 MB a subscriber may fall behind by, which counts stream messages only.
        held, release = [], threading.Event()
        padding = 'x' * (3 << 20)
        class Large(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                if public_upstream(self, json.loads(self.rfile.read(int(self.headers['Content-Length'])))): return
                held.append(1); release.wait(30)
                payload = json.dumps({'result': {'status': 'success', 'drops': {'base_fee': '10'}, 'padding': padding}}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
            def log_message(self, *args): pass
        upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Large)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.addCleanup(upstream.server_close); self.addCleanup(upstream.shutdown); self.addCleanup(release.set)
        port = self.serve('--forward', f'http://127.0.0.1:{upstream.server_port}/')
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        for n in range(8):
            ws.send({'id': n, 'command': 'fee'})
        for _ in range(100):
            if len(held) == 8: break
            time.sleep(.05)
        self.assertEqual(len(held), 8)
        release.set(); time.sleep(2)  # all eight queued on the server before the client reads
        ws.sock.settimeout(30)
        answers = [ws.receive() for _ in range(8)]
        self.assertEqual(sorted(a['id'] for a in answers), list(range(8)))
        self.assertTrue(all(len(a['result']['padding']) == len(padding) for a in answers))
        # The session still answers: the next request is read once the replies are written.
        ws.send({'id': 8, 'command': 'ledger_range'})
        self.assertEqual(ws.receive()['result']['ledger_index_max'], 65538)

    def test_http_websocket_subscription_and_graceful_shutdown(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        server = subprocess.Popen([BINARY, 'serve', '--dir', str(self.data), '--http', f'127.0.0.1:{port}', '--ws', f'127.0.0.1:{port}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if server.poll() is None:
                server.terminate()
                try:
                    server.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill(); server.communicate(); raise
        self.addCleanup(cleanup)
        for _ in range(100):
            try:
                conn = socket.create_connection(('127.0.0.1', port), .1); conn.close(); break
            except OSError:
                if server.poll() is not None:
                    self.fail(server.communicate()[1].decode())
                time.sleep(.05)
        client = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
        client.request('POST', '/', json.dumps({'method': 'ledger_range', 'params': [{}]}))
        result = json.loads(client.getresponse().read())['result']; client.close()
        self.assertEqual(result['ledger_index_max'], 65538)
        ws = WebSocket(port); self.addCleanup(ws.sock.close)
        ws.send({'id': 0, 'command': 'path_find', 'subcommand': 'create'})
        self.assertEqual(ws.receive()['error'], 'notSupported')  # no [serve] forward_ws
        ws.send({'id': 1, 'command': 'subscribe', 'streams': ['ledger']})
        result = ws.receive(); self.assertEqual(result['status'], 'success')
        self.fetch('65536-65540'); self.build()
        events = [ws.receive(), ws.receive()]
        self.assertEqual([x['ledger_index'] for x in events], [65539, 65540])
        self.assertTrue(all(x['type'] == 'ledgerClosed' for x in events))
        ws.send({'id': 2, 'command': 'unsubscribe', 'streams': ['ledger']})
        self.assertEqual(ws.receive()['id'], 2)
        ws.sock.close(); cleanup(); self.assertEqual(server.returncode, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
