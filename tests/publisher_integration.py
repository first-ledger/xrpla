"""Real canonical publishing, concurrent extraction and interrupted-batch recovery."""
import copy
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

BINARY, FIXTURE = map(os.path.abspath, sys.argv[1:3])
sys.argv = sys.argv[:1]
SNAPSHOT = os.environ.get('XRPLA_SNAPSHOT_TOOL')


class Publisher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='xrpla-publisher-source-')
        root = Path(cls.tmp.name)
        source, data = root / 'source', root / 'data'
        env = dict(os.environ, XRPLA_CORE_FIXTURE_DIR=str(source), XRPLA_CORE_FIXTURE_COUNT='12')
        subprocess.run([FIXTURE, '--gtest_filter=CoreFixture.Export'], env=env, check=True, capture_output=True)
        def cli(*args, request=None):
            return json.loads(subprocess.check_output([BINARY, *map(str, args)], input=request, text=True))
        cli('fetch', '--archive', source, '--range', '65536-65547', '--dir', data, '--publisher-key', source / 'publisher.pub')
        cli('build', '--dir', data, '--publisher-key', source / 'publisher.pub')
        cls.ledgers, cls.states, cls.hashes = {}, {}, {}
        for sequence in range(65536, 65548):
            for method, extra, destination in [('ledger', {'transactions': True, 'expand': True, 'diff': True}, cls.ledgers), ('ledger_data', {}, cls.states)]:
                destination[sequence] = cli('rpc', '--dir', data, '--request', '-', request=json.dumps({'method': method, 'params': {'ledger_index': sequence, 'binary': True, **extra}}))
            cls.hashes[cls.ledgers[sequence]['ledger_hash'].lower()] = sequence

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        self.work = tempfile.TemporaryDirectory(prefix='xrpla-publisher-')
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name)
        self.archive = self.root / 'archive'
        self.seed = self.root / 'publisher.seed'
        self.cli('keygen', '--out', self.seed)
        self.tip, self.bad, self.block, self.diff = 65538, 0, 0, True
        self.entered, self.release = threading.Event(), threading.Event()
        self.active, self.peak, self.entries = 0, 0, 0
        # page: ledger_data pages of this many objects, paged as Clio pages them (0: one page).
        self.page, self.paging, self.page_peak, self.tip_queries = 0, 0, 0, 0
        mutex = threading.Lock()
        test = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                method, params = request['method'], request['params'][0]
                sequence = params.get('ledger_index', test.tip)
                if isinstance(sequence, str): sequence = test.tip
                if 'ledger_hash' in params: sequence = test.hashes[params['ledger_hash'].lower()]
                if method == 'ledger' and isinstance(params.get('ledger_index'), str):
                    with mutex: test.tip_queries += 1
                if method == 'ledger':
                    if sequence == test.block:
                        test.entered.set(); test.release.wait(30)
                    result = copy.deepcopy(test.ledgers[sequence])
                    if not test.diff: result['ledger'].pop('diff', None)
                    if sequence == test.bad: result['ledger']['diff'] = [{'object_id': '00' * 32, 'object': ''}]
                elif method == 'ledger_data':
                    # Each page starts after its marker, which must be a key the ledger holds.
                    full = test.states[sequence]
                    items = sorted(full['state'], key=lambda item: item['index'].lower())
                    keys = [item['index'].lower() for item in items]
                    result = {k: full[k] for k in ('ledger_index', 'ledger_hash')}
                    marker, size = params.get('marker'), test.page or len(items)
                    if marker is not None and marker.lower() not in keys:
                        result.update(error='invalidParams', error_message='markerDoesNotExist')
                    else:
                        with mutex:
                            test.paging += 1; test.page_peak = max(test.page_peak, test.paging)
                        if test.page: time.sleep(.02)
                        with mutex: test.paging -= 1
                        start = keys.index(marker.lower()) + 1 if marker is not None else 0
                        result['state'] = items[start:start + size]
                        if start + size < len(items): result['marker'] = items[start + size - 1]['index']
                elif method == 'ledger_entry':
                    with mutex:
                        test.active += 1; test.entries += 1; test.peak = max(test.peak, test.active)
                    time.sleep(.03)
                    item = next((x for x in test.states[sequence]['state'] if x['index'].lower() == params['index'].lower()), None)
                    result = {k: test.states[sequence][k] for k in ('ledger_index', 'ledger_hash')}
                    result.update({'node_binary': item['data']} if item else {'error': 'entryNotFound'})
                    with mutex: test.active -= 1
                else: raise AssertionError(method)
                body = json.dumps({'result': result}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers()
                try: self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError): pass
            def log_message(self, *args): pass
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close); self.addCleanup(self.server.shutdown)
        self.addCleanup(self.release.set)
        self.url = f'http://127.0.0.1:{self.server.server_port}/'

    def cli(self, *args, ok=True):
        result = subprocess.run([BINARY, *map(str, args)], capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode == 0, ok, result.stdout + result.stderr)
        return json.loads(result.stdout if ok else result.stderr.splitlines()[-1])

    def command(self, *extra):
        return ['publish', '--source', self.url, '--out', self.archive, '--signing-key', self.seed, '--delay-ms', '0', *extra]

    def publish(self, *extra, ok=True):
        return self.cli(*self.command('--once', *extra), ok=ok)

    def manifest(self): return json.loads((self.archive / 'manifest.json').read_text())

    def follow(self, data):
        return self.cli('tail', '--once', '--archive', self.archive, '--range', '65537-' + str(self.manifest()['tip']['seq']), '--dir', data, '--publisher-key', self.archive / 'publisher.pub')

    def test_append_tail_immutable_signature_and_missing_nodes(self):
        self.publish('--start', '65537')
        old = self.manifest()
        snapshot = (self.archive / old['snapshots'][0]['index']['object']).read_bytes()
        data = self.root / 'data'
        self.assertEqual(self.follow(data)['ledgers_added'], 2)
        self.tip = 65540
        result = self.publish()
        self.assertTrue(result['last_batch']['resumed_nodes'])
        self.assertEqual(result['ledgers_published'], 2)
        self.assertEqual(self.manifest()['chunks'][:len(old['chunks'])], old['chunks'])
        self.assertEqual((self.archive / old['snapshots'][0]['index']['object']).read_bytes(), snapshot)
        # Simulate the crash window after manifest commit, before legacy signature.
        (self.archive / 'manifest.json.sig').write_bytes(b'\0' * 64)
        self.assertEqual(self.follow(data)['ledgers_added'], 2)
        shutil.rmtree(self.archive / '.verification-nodes')
        self.tip = 65542
        recovered = self.publish()
        self.assertFalse(recovered['last_batch']['resumed_nodes'])
        self.assertEqual(self.follow(data)['ledgers_added'], 2)
        self.assertEqual(self.publish()['ledgers_published'], 0)

    def test_bad_batch_and_wrong_publisher_never_commit(self):
        self.publish('--start', '65537')
        before = (self.archive / 'manifest.json').read_bytes()
        self.tip = self.bad = 65540
        self.publish(ok=False)
        self.assertEqual((self.archive / 'manifest.json').read_bytes(), before)
        wrong = self.root / 'wrong.seed'; self.cli('keygen', '--out', wrong)
        self.seed = wrong
        self.publish(ok=False)
        self.assertEqual((self.archive / 'manifest.json').read_bytes(), before)

    def test_without_rpc_cache_preserves_roots_and_resumes(self):
        result = self.publish('--start', '65537', '--no-rpc-cache')
        self.assertFalse(result['last_batch']['rpc_cache'])
        self.assertFalse((self.archive / '.rpc-cache').exists())
        self.assertTrue(result['last_batch']['ledger_hashes_verified'])
        self.tip = 65540
        resumed = self.publish('--no-rpc-cache')
        self.assertTrue(resumed['last_batch']['resumed_nodes'])
        self.assertEqual(self.follow(self.root / 'no-cache-data')['ledgers_added'], 4)
        before = (self.archive / 'manifest.json').read_bytes()
        self.tip = self.bad = 65542
        self.publish('--no-rpc-cache', ok=False)
        self.assertEqual((self.archive / 'manifest.json').read_bytes(), before)

    def test_kill_partial_batch_then_resume(self):
        self.publish('--start', '65537')
        before = (self.archive / 'manifest.json').read_bytes()
        self.tip = self.block = 65540
        process = subprocess.Popen([BINARY, *map(str, self.command('--once'))], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertTrue(self.entered.wait(20), 'Publisher did not reach partial batch')
            process.kill(); process.communicate(timeout=15)
        finally:
            if process.poll() is None: process.kill(); process.communicate()
            self.release.set(); self.block = 0
        self.assertEqual((self.archive / 'manifest.json').read_bytes(), before)
        self.assertEqual(self.publish()['ledgers_published'], 2)
        self.assertEqual(self.follow(self.root / 'recovered')['ledgers_added'], 4)

    def test_kill_without_rpc_cache_then_resume(self):
        original = self.command
        self.command = lambda *extra: original('--no-rpc-cache', *extra)
        self.test_kill_partial_batch_then_resume()
        self.assertFalse((self.archive / '.rpc-cache').exists())

    def test_parallel_postimages_equal_serial_chunks(self):
        self.tip, self.diff = 65543, False
        self.publish('--start', '65539', '--jobs', '1')
        serial = self.manifest()['chunks']
        self.assertEqual(self.peak, 1)
        self.archive = self.root / 'parallel'
        self.publish('--start', '65539', '--jobs', '4')
        self.assertEqual(self.manifest()['chunks'], serial)
        self.assertGreaterEqual(self.peak, 2)

    def test_state_split_across_connections_writes_the_same_archive(self):
        # Clio pages state only from a marker the ledger holds: two connections split the key
        # space at a key from the base ledger's diff and must write what one connection writes.
        self.page = 7
        self.publish('--start', '65537', '--jobs', '1')
        single = self.manifest()
        self.assertEqual(self.page_peak, 1)
        self.archive, self.page_peak = self.root / 'split', 0
        self.publish('--start', '65537', '--jobs', '2')
        self.assertEqual(self.page_peak, 2)
        self.assertEqual(self.manifest()['snapshots'], single['snapshots'])
        self.assertEqual(self.manifest()['chunks'], single['chunks'])

    @unittest.skipUnless(SNAPSHOT, 'xrpla_snapshot supplied by CTest')
    def test_interval_snapshots_match_replayed_ones(self):
        # The follower writes each boundary's state as it passes it; the snapshot tool copies
        # those (checked) instead of replaying, and they equal the replayed ones byte for byte.
        self.tip = 65542
        self.publish('--start', '65537', '--interval-snapshots', '2')
        made = self.archive / 'interval-snapshots'
        self.assertEqual(sorted(p.name for p in made.glob('*.json')), ['65538.json', '65540.json', '65542.json'])
        def snapshot(archive, name, ok=True):
            result = subprocess.run([SNAPSHOT, '--archive', archive, '--publisher-key', archive / 'publisher.pub', '--at', '65540,65542',
                                     '--out', self.root / name, '--scratch', self.root / (name + '-scratch')], capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, ok, result.stderr)
            return [json.loads(line) for line in result.stdout.splitlines()], result.stderr
        copied, log = snapshot(self.archive, 'copied')
        self.assertIn('"copied"', log)
        self.assertNotIn('"load"', log)
        replayed_archive = self.root / 'replayed'
        shutil.copytree(self.archive, replayed_archive, ignore=shutil.ignore_patterns('interval-snapshots', '.verification-nodes', '.xrpla.lock'))
        replayed, log = snapshot(replayed_archive, 'replayed')
        self.assertIn('"load"', log)
        self.assertEqual(copied, replayed)
        for item in copied:
            index = json.loads((self.root / 'copied' / item['index']['object']).read_bytes())
            for name in [item['index']['object']] + [part['object'] for part in index['parts']]:
                self.assertEqual((self.root / 'copied' / name).read_bytes(), (self.root / 'replayed' / name).read_bytes())
        # A snapshot whose objects changed after the follower wrote them is refused.
        part = json.loads((made / '65540.json').read_bytes())['index']['object']
        (made / part).write_bytes((made / part).read_bytes() + b' ')
        snapshot(self.archive, 'tampered', ok=False)

    def test_catch_up_without_polling_pause(self):
        # Behind a known tip, batches follow one another at once, and the tip is asked for only
        # when no whole batch is known to be validated.
        self.tip = 65543
        process = subprocess.Popen([BINARY, *map(str, self.command('--start', '65537', '--interval', '60', '--batch-ledgers', '2'))],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None: self.fail(str(process.communicate()))
                if (self.archive / 'manifest.json').exists() and self.manifest()['tip']['seq'] == 65543: break
                time.sleep(.05)
            self.assertEqual(self.manifest()['tip']['seq'], 65543)
            self.assertEqual(self.tip_queries, 2)
            process.terminate(); stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, stderr.decode())
        finally:
            if process.poll() is None: process.kill(); process.communicate()

    def test_continuous_tip_polling_and_graceful_stop(self):
        process = subprocess.Popen([BINARY, *map(str, self.command('--start', '65537', '--interval', '1', '--batch-ledgers', '2'))], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def wait_tip(expected):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if process.poll() is not None: self.fail(str(process.communicate()))
                if (self.archive / 'manifest.json').exists() and self.manifest()['tip']['seq'] == expected: return
                time.sleep(.05)
            self.fail('Publisher did not reach tip ' + str(expected))
        try:
            wait_tip(65538); self.tip = 65542; wait_tip(65542)
            process.terminate(); stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, stderr.decode())
            self.assertIn(b'"stopped": true', stdout)
        finally:
            if process.poll() is None: process.kill(); process.communicate()
        self.assertEqual(self.publish()['ledgers_published'], 0)


if __name__ == '__main__': unittest.main(verbosity=2)
