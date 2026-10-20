"""Connected with a publication: history from the publication, newer ledgers from a node.

The fixture archive is published only up to ledger 14; a mock node replays the rest, its tip
advancing a ledger at a time. `xrpla run` (profile = "connected" with [publication]) mirrors
the publication, starts its spool from the publication's newest snapshot and follows the node
from the publication's tip. Answers over the whole range must equal a server over a store of
the archive. Publishing the rest of the archive then gives the publication a newer snapshot:
the spool must rotate to it (with the follower running until the swap) and answers must still
match. SIGTERM must stop everything.

Needs XRPLA_BINARY, XRPLA_HISTORY_DECODER and XRPLA_SNAPSHOT_TOOL.
"""
import copy
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connected_test import ACCOUNT_FIELDS, ARCHIVE, call, free_port, wait_port

ROOT = Path(__file__).resolve().parents[1]
BINARY, DECODER, SNAPSHOT = (os.environ.get(k) for k in ('XRPLA_BINARY', 'XRPLA_HISTORY_DECODER', 'XRPLA_SNAPSHOT_TOOL'))
UNTIL = 14


@unittest.skipUnless(BINARY and DECODER and SNAPSHOT, 'Compiled xrpla, decoder and snapshot tool supplied by CTest')
class ConnectedWithPublication(unittest.TestCase):
    @classmethod
    def publish(cls, *extra):
        subprocess.run([str(x) for x in [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', ARCHIVE / 'publisher.pub',
                        '--bucket', cls.bucket, '--signing-key', cls.root / 'publisher.seed', '--scratch', cls.root / 'scratch', '--decoder', DECODER,
                        '--snapshot-tool', SNAPSHOT, '--piece-ledgers', 4, '--bundle-pieces', 2, '--grace-seconds', 0, *extra]], check=True, capture_output=True)
        pointer = json.loads((cls.bucket / 'catalog' / 'current.json').read_text())
        return json.loads((cls.bucket / pointer['object']).read_text())

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='xrpla-connected-pub-'); root = cls.root = Path(cls.tmp.name)
        manifest = json.loads((ARCHIVE / 'manifest.json').read_bytes())
        first, cls.tip = manifest['chunks'][0]['first'], manifest['tip']['seq']
        store = root / 'store'
        for command in (['fetch', '--archive', ARCHIVE, '--range', f'{first}-{cls.tip}', '--dir', store, '--publisher-key', ARCHIVE / 'publisher.pub'],
                        ['build', '--dir', store, '--publisher-key', ARCHIVE / 'publisher.pub']):
            subprocess.run([BINARY, *map(str, command)], check=True, capture_output=True, timeout=600)
        cls.store_port = free_port()
        cls.store = subprocess.Popen([BINARY, 'serve', '--dir', str(store), '--http', f'127.0.0.1:{cls.store_port}', '--ws', f'127.0.0.1:{free_port()}'],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_port(cls.store_port, cls.store, 60)
        (root / 'publisher.seed').write_bytes(os.urandom(32))
        cls.bucket = root / 'bucket'
        catalog = cls.publish('--until', UNTIL)
        cls.published = catalog['tip']['seq']

        # The node holds every ledger; its validated tip starts just past the publication's and
        # advances once the follower has exported it.
        cls.ledgers, cls.hashes = {}, {}
        for seq in range(first, cls.tip + 1):
            cls.ledgers[seq] = call(cls.store_port, 'ledger', {'ledger_index': seq, 'binary': True, 'transactions': True, 'expand': True, 'diff': True})
            cls.hashes[cls.ledgers[seq]['ledger_hash'].lower()] = seq
        cls.first, cls.node_tip, cls.fetched = first, cls.published + 1, 0
        test = cls

        class Node(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                method, params = request['method'], (request.get('params') or [{}])[0]
                seq = params.get('ledger_index', test.node_tip)
                if isinstance(seq, str): seq = test.node_tip
                if 'ledger_hash' in params: seq = test.hashes[params['ledger_hash'].lower()]
                if method == 'server_info': result = {'info': {'complete_ledgers': f'{test.first}-{test.node_tip}'}}
                elif seq > test.node_tip: result = {'error': 'lgrNotFound', 'status': 'error'}
                elif method == 'ledger':
                    result = copy.deepcopy(test.ledgers[seq])
                    if params.get('diff'): test.fetched = max(test.fetched, seq)
                else: result = {'error': 'unknownCmd', 'status': 'error'}
                body = json.dumps({'result': result}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                try: self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass

            def log_message(self, *args): pass

        cls.node = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Node)
        threading.Thread(target=cls.node.serve_forever, daemon=True).start()
        cls.stop_ticking = threading.Event()

        def tick():
            while cls.node_tip < cls.tip and not cls.stop_ticking.wait(1):
                if cls.fetched >= cls.node_tip: cls.node_tip += 1
        threading.Thread(target=tick, daemon=True).start()

        cls.storage, cls.port = root / 'connected', free_port()
        config = root / 'xrpla.toml'
        config.write_text(f'''profile = "connected"
[node]
rpc = "http://127.0.0.1:{cls.node.server_port}/"
[publication]
source = "{cls.bucket.as_posix()}"
publisher_key = "{(cls.bucket / 'publisher.pub').as_posix()}"
[storage]
dir = "{cls.storage.as_posix()}"
cache_mb = 256
[serve]
http = "127.0.0.1:{cls.port}"
ws = "127.0.0.1:{free_port()}"
[connected]
poll_seconds = 1
sync_seconds = 2
rotate_seconds = 3
''')
        cls.log = open(root / 'run.log', 'w+')
        cls.runner = subprocess.Popen([BINARY, 'run', '--config', str(config)], stdout=cls.log, stderr=subprocess.STDOUT)
        wait_port(cls.port, cls.runner, 300)
        cls.wait_for(lambda: call(cls.port, 'ledger_range', {}).get('ledger_index_max') == cls.tip, 'the node tip')
        cls.compared = 0

    @classmethod
    def wait_for(cls, condition, what, seconds=300):
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                if condition(): return
            except (OSError, ValueError, KeyError): pass
            time.sleep(1)
        cls.log.seek(0)
        raise AssertionError(f'timed out waiting for {what}\n{cls.log.read()[-6000:]}')

    @classmethod
    def tearDownClass(cls):
        cls.stop_ticking.set()
        if cls.runner.poll() is None: cls.runner.kill(); cls.runner.wait()
        cls.store.terminate(); cls.store.wait()
        cls.node.shutdown(); cls.node.server_close()
        print(json.dumps({'published_through': cls.published, 'tip': cls.tip, 'compared': cls.compared}), file=sys.stderr)
        cls.log.close(); cls.tmp.cleanup()

    def same(self, method, **params):
        expected, actual = call(self.store_port, method, params), call(self.port, method, params)
        type(self).compared += 1
        self.assertEqual(actual, expected, f'{method} {json.dumps(params)}')
        return expected

    def compare_everything(self):
        self.same('ledger_range')
        accounts = set()
        for seq in range(self.first, self.tip + 1):
            ledger = self.same('ledger', ledger_index=seq, transactions=True, expand=True, api_version=2)['ledger']
            self.same('ledger', ledger_index=seq, diff=True, binary=True)
            for tx in ledger['transactions']:
                self.same('tx', transaction=tx['hash'], api_version=2)
                for field in ACCOUNT_FIELDS:
                    if isinstance(tx['tx_json'].get(field), str): accounts.add(tx['tx_json'][field])
        for account in sorted(accounts):
            self.same('account_tx', account=account, api_version=2)
            for seq in (self.published, self.tip):
                self.same('account_info', account=account, ledger_index=seq)
                self.same('account_objects', account=account, ledger_index=seq)

    def spool_snapshot(self):
        return json.loads((self.storage / 'spool' / 'manifest.json').read_text())['snapshots'][0]['seq']

    def test_1_history_and_tail_match_the_store(self):
        # The publication's history plus the node's ledgers, as one range.
        self.assertLess(self.published, self.tip)
        self.compare_everything()

    def test_2_spool_rotates_to_the_publications_newer_snapshot(self):
        before = self.spool_snapshot()
        catalog = self.publish()
        newest = max(s['seq'] for s in catalog['snapshots'] if s['seq'] < catalog['tip']['seq'])
        self.assertGreater(newest, before)
        type(self).wait_for(lambda: self.spool_snapshot() >= newest, 'the spool rotation', 120)  # the state at the tip
        self.assertTrue(any(p.name.startswith('spool.retired-') for p in self.storage.iterdir()))
        self.wait_for(lambda: call(self.port, 'ledger_range', {}).get('ledger_index_max') == self.tip, 'the tip after rotation', 60)
        self.compare_everything()

    def test_9_sigterm_stops_everything(self):
        self.runner.send_signal(signal.SIGTERM)
        self.assertEqual(self.runner.wait(timeout=120), 0)
        left = subprocess.run(['pgrep', '-f', str(self.storage)], capture_output=True, text=True).stdout.split()
        self.assertEqual(left, [], 'a follower outlived xrpla run')


if __name__ == '__main__':
    unittest.main()
