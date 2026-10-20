"""`xrpla restore`: a piece publication streamed into each target equals the staged paths.

Publishes an archive as pieces, then restores it straight from the bucket (and over HTTP
with range requests) into an xrpla store, xrpld RocksDB and NuDB node databases and a Clio
import plan. Each must match what `fetch` + `build` + `import` produce from the archive.

Usage: restore_test.py [ARCHIVE PIECE_LEDGERS BUNDLE_PIECES]; needs XRPLA_BINARY,
XRPLA_HISTORY_DECODER and XRPLA_SNAPSHOT_TOOL.
"""
from contextlib import closing
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from fake_cassandra import FakeCassandra, expected  # noqa: E402
ARCHIVE = Path(sys.argv.pop(1)) if len(sys.argv) > 1 else ROOT / 'tests' / 'fixtures' / 'standalone-nfts'
PIECE = int(sys.argv.pop(1)) if len(sys.argv) > 1 else 4
BUNDLE = int(sys.argv.pop(1)) if len(sys.argv) > 1 else 2
BINARY, DECODER, SNAPSHOT = (os.environ.get(k) for k in ('XRPLA_BINARY', 'XRPLA_HISTORY_DECODER', 'XRPLA_SNAPSHOT_TOOL'))


class RangeHandler(http.server.SimpleHTTPRequestHandler):
    """Static files with single-range support, as an R2 public bucket serves them."""
    def log_message(self, *_): pass

    def do_GET(self):
        path = Path(self.translate_path(self.path))
        if not path.is_file(): self.send_error(404); return
        data = path.read_bytes(); header = self.headers.get('Range')
        if header:
            start, end = header.removeprefix('bytes=').split('-')
            start, end = int(start), min(int(end), len(data) - 1)
            self.send_response(206); self.send_header('Content-Range', f'bytes {start}-{end}/{len(data)}'); data = data[start:end + 1]
        else: self.send_response(200)
        self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)


def run(*command, env=None):
    result = subprocess.run([str(x) for x in command], capture_output=True, text=True, timeout=1800, env=env)
    if result.returncode: raise RuntimeError(f'{command[1]} failed: {result.stdout[-2000:]}{result.stderr[-3000:]}')
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return json.loads('\n'.join(lines)) if lines else None


def rows(database, query):
    with closing(sqlite3.connect(database)) as db: return sorted(db.execute(query).fetchall())


@unittest.skipUnless(BINARY and DECODER and SNAPSHOT, 'Compiled xrpla, decoder and snapshot tool supplied by CTest')
class Restore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='xrpla-restore-'); root = cls.root = Path(cls.tmp.name)
        seed = root / 'publisher.seed'; seed.write_bytes(os.urandom(32))
        cls.bucket = root / 'bucket'
        publish = [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', ARCHIVE / 'publisher.pub',
                   '--bucket', cls.bucket, '--signing-key', seed, '--scratch', root / 'scratch', '--decoder', DECODER, '--snapshot-tool', SNAPSHOT,
                   '--piece-ledgers', PIECE, '--bundle-pieces', BUNDLE, '--grace-seconds', 0]
        subprocess.run([str(x) for x in publish], check=True, capture_output=True)
        catalog_pointer = json.loads((cls.bucket / 'catalog' / 'current.json').read_text())
        cls.catalog = json.loads((cls.bucket / catalog_pointer['object']).read_text())
        cls.key = cls.bucket / 'publisher.pub'
        # The staged reference: fetch and build the same ledgers from the archive.
        first, last = cls.catalog['ranges'][0]['first'], cls.catalog['tip']['seq']
        cls.reference = root / 'reference'
        run(BINARY, 'fetch', '--archive', ARCHIVE, '--range', f'{first}-{last}', '--dir', cls.reference, '--publisher-key', ARCHIVE / 'publisher.pub')
        cls.built = run(BINARY, 'build', '--dir', cls.reference, '--publisher-key', ARCHIVE / 'publisher.pub')
        cls.first, cls.last = first, last

    @classmethod
    def tearDownClass(cls): cls.tmp.cleanup()

    def restore(self, target, out, source=None, *extra):
        return run(BINARY, 'restore', '--source', source or self.bucket, '--publisher-key', self.key, '--to', target, '--out', out, *extra)

    def test_replay_matches_libxrpl_verifier(self):
        # A restore replays ledgers on its own hash tree (StateReplay) rather than libxrpl's
        # SHAMap; both must write the same nodes, in order and by intervals, fresh and resumed.
        # The reference also creates xrpld's SQL schema up front rather than loading bare tables
        # and indexing them at the end: the schemas must come out identical.
        libxrpl = dict(os.environ, XRPLA_RESTORE_VERIFIER='libxrpl', XRPLA_SQL_BULK='0')
        middle = (self.first + self.last) // 2
        for backend in ('nudb', 'rocksdb'):
            for jobs in (('--jobs', '1'), ('--jobs', '4')) if backend == 'nudb' else ((),):
                name = f'{backend}{"-".join(jobs)}'
                reference = run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', backend,
                                '--out', self.root / f'libxrpl-{name}', *jobs, env=libxrpl)
                self.restore(backend, self.root / f'replay-{name}', None, '--range', f'{self.first}-{middle}', *jobs)
                replayed = self.restore(backend, self.root / f'replay-{name}', None, *jobs)  # resumes by replaying
                self.assertEqual(replayed['last_hash'], reference['last_hash'], name)
                same = run(BINARY, 'compare-nodes', '--dir', self.root / f'replay-{name}' / 'node-db', '--against',
                           self.root / f'libxrpl-{name}' / 'node-db', '--backend', backend)
                self.assertTrue(same['same'], (name, same))
                for db, query in (('ledger.db', 'SELECT * FROM Ledgers'), ('transaction.db', 'SELECT * FROM Transactions'),
                                  ('transaction.db', 'SELECT * FROM AccountTransactions')):
                    self.assertEqual(rows(self.root / f'replay-{name}' / db, query), rows(self.root / f'libxrpl-{name}' / db, query), (name, query))
                for db in ('ledger.db', 'transaction.db'):
                    schema = 'SELECT type, name, tbl_name, sql FROM sqlite_master'
                    self.assertEqual(rows(self.root / f'replay-{name}' / db, schema), rows(self.root / f'libxrpl-{name}' / db, schema), (name, db))
                    self.assertEqual(rows(self.root / f'replay-{name}' / db, 'PRAGMA integrity_check'), [('ok',)], (name, db))
                # The primary key works as one: a repeated TransID is refused.
                with closing(sqlite3.connect(self.root / f'replay-{name}' / 'transaction.db')) as db:
                    some = db.execute('SELECT TransID FROM Transactions LIMIT 1').fetchone()[0]
                    with self.assertRaises(sqlite3.IntegrityError):
                        db.execute("INSERT INTO Transactions (TransID) VALUES (?)", (some,))

    def test_store_matches_build(self):
        result = self.restore('store', self.root / 'restored-store')
        reference = run(BINARY, 'status', '--dir', self.reference)
        self.assertEqual(result['complete_ledgers'], reference['complete_ledgers'])
        self.assertEqual(result['last_ledger_hash'], reference['last_ledger_hash'])
        # Same state and history answers from both stores.
        for method, params in (('ledger_data', {'ledger_index': self.last, 'binary': True, 'limit': 2048}),
                               ('ledger', {'ledger_index': self.last, 'transactions': True, 'expand': True, 'binary': True})):
            request = json.dumps({'method': method, 'params': [params]})
            answers = [subprocess.run([BINARY, 'rpc', '--dir', d, '--request', '-'], input=request, capture_output=True, text=True, check=True).stdout
                       for d in (self.root / 'restored-store', self.reference)]
            self.assertEqual(answers[0], answers[1], method)

    def test_spool_starts_from_the_state_at_the_tip(self):
        # By default the state at the tip (the newest snapshot updated by the ledgers since, checked
        # against the account hash of the ledger before the tip); with --replay, every ledger after
        # the snapshot replayed. Both end on the publication's tip with the same state.
        newest = max(s['seq'] for s in self.catalog['snapshots'] if s['seq'] < self.last)
        for name, extra, first in (('fast', (), self.last), ('replay', ('--replay',), newest + 1)):
            spool, seed = self.root / f'{name}-spool', self.root / f'{name}-spool.seed'
            seed.write_bytes(os.urandom(32))
            result = self.restore('spool', spool, None, '--signing-key', seed, *extra)
            self.assertEqual((result['range']['first'], result['range']['last']), (first, self.last), name)
            self.assertEqual(result['last_ledger_hash'].lower(), self.catalog['tip']['ledger_hash'].lower(), name)
            self.assertEqual(json.loads((spool / 'manifest.json').read_text())['snapshots'][0]['seq'], first - 1, name)
            self.assertTrue((spool / '.verification-nodes').is_dir())  # `publish` resumes without replaying
            # A signed v2 archive that fetch and build accept, ending where the publication does.
            store = self.root / f'from-{name}-spool'
            run(BINARY, 'fetch', '--archive', spool, '--range', f'{first}-{self.last}', '--dir', store, '--publisher-key', spool / 'publisher.pub')
            built = run(BINARY, 'build', '--dir', store, '--publisher-key', spool / 'publisher.pub')
            self.assertEqual(built['last_ledger_hash'], self.built['last_ledger_hash'], name)
            request = json.dumps({'method': 'ledger_data', 'params': [{'ledger_index': self.last, 'binary': True, 'limit': 2048}]})
            answers = [subprocess.run([BINARY, 'rpc', '--dir', d, '--request', '-'], input=request, capture_output=True, text=True, check=True).stdout
                       for d in (store, self.reference)]
            self.assertEqual(answers[0], answers[1], name)
            # Restored again, an existing spool is extended to the publication's tip: nothing newer here.
            self.assertEqual(self.restore('spool', spool, None, '--signing-key', seed)['extended'], 0, name)

    @classmethod
    def short_publication(cls):
        """A shorter publication of the same archive, whose newest snapshot is later than the
        first (a spool from the first then holds ledgers the short publication does not)."""
        short = cls.root / 'short-bucket'
        if not short.exists():
            snapshots = sorted(s['seq'] for s in cls.catalog['snapshots'])
            (cls.root / 'short.seed').write_bytes(os.urandom(32))
            subprocess.run([str(x) for x in [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', ARCHIVE / 'publisher.pub',
                            '--bucket', short, '--signing-key', cls.root / 'short.seed', '--scratch', cls.root / 'short-scratch', '--decoder', DECODER,
                            '--snapshot-tool', SNAPSHOT, '--piece-ledgers', PIECE, '--bundle-pieces', BUNDLE, '--grace-seconds', 0, '--until', snapshots[-2] + 2]],
                           check=True, capture_output=True)
        return short

    def test_spool_rotates_to_a_newer_snapshot(self):
        snapshots = sorted(s['seq'] for s in self.catalog['snapshots'])
        self.assertGreaterEqual(len(snapshots), 3, snapshots)
        short = self.short_publication()
        spool, seed = self.root / 'rotating-spool', self.root / 'rotating.seed'
        seed.write_bytes(os.urandom(32))
        self.restore('spool', spool, None, '--signing-key', seed, '--range', f'{snapshots[0] + 1}-')
        self.assertEqual(json.loads((spool / 'manifest.json').read_text())['snapshots'][0]['seq'], snapshots[0])
        result = run(BINARY, 'rotate-spool', '--dir', spool, '--signing-key', seed, '--source', short, '--publisher-key', short / 'publisher.pub')
        self.assertTrue(result['rotated'], result)
        manifest = json.loads((spool / 'manifest.json').read_text())
        self.assertGreaterEqual(manifest['snapshots'][0]['seq'], result['publication_snapshot'])  # the state at its tip
        self.assertGreater(result['publication_snapshot'], snapshots[0])
        self.assertEqual(manifest['tip']['seq'], self.last)  # the old spool's later ledgers carried over
        self.assertEqual(result['extended'][-1]['last'], self.last)
        store = self.root / 'from-rotated'
        run(BINARY, 'fetch', '--archive', spool, '--range', f"{manifest['snapshots'][0]['seq'] + 1}-{self.last}", '--dir', store, '--publisher-key', spool / 'publisher.pub')
        self.assertEqual(run(BINARY, 'build', '--dir', store, '--publisher-key', spool / 'publisher.pub')['last_ledger_hash'], self.built['last_ledger_hash'])
        self.assertFalse(run(BINARY, 'rotate-spool', '--dir', spool, '--signing-key', seed, '--source', short, '--publisher-key', short / 'publisher.pub')['rotated'])

    def test_a_served_tail_keeps_its_ledgers_across_a_rotation(self):
        # A server's tail follows a spool over pieces that end before the snapshot the spool
        # rotates to. Once the tail adopts the rotated spool, and until a newer cache holds the
        # ledgers in between, they come from the retired spool.
        from connected_test import call, free_port, wait_port
        snapshots = sorted(s['seq'] for s in self.catalog['snapshots'])
        short = self.short_publication()
        cache = self.root / 'tail-cache'
        run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache, '--range', f'{self.first}-{max(self.first, snapshots[0] + 1)}')
        spool, seed = self.root / 'served-spool', self.root / 'served.seed'
        seed.write_bytes(os.urandom(32))
        self.restore('spool', spool, None, '--signing-key', seed, '--range', f'{snapshots[0] + 1}-')
        port = free_port()
        server = subprocess.Popen([str(x) for x in [BINARY, 'serve', '--dir', cache, '--http', f'127.0.0.1:{port}', '--ws', f'127.0.0.1:{free_port()}',
                                   '--tail', spool, '--tail-key', spool / 'publisher.pub']], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            wait_port(port, server, 60)
            pieces_last = json.loads((cache / 'selection.json').read_text())['last']
            # With their transactions, which the spool's chunk files hold (headers are in memory).
            ledger = lambda seq: call(port, 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True, 'binary': True})
            before = {seq: ledger(seq) for seq in range(pieces_last + 1, self.last + 1)}
            self.assertTrue(any(result['ledger']['transactions'] for result in before.values()))
            result = run(BINARY, 'rotate-spool', '--dir', spool, '--signing-key', seed, '--source', short, '--publisher-key', short / 'publisher.pub')
            self.assertTrue(result['rotated'], result)
            self.assertGreater(result['publication_snapshot'], pieces_last)  # ledgers only the retired spool holds
            for _ in range(12):  # the tail refreshes every 2 s
                for seq, expected in before.items():
                    self.assertEqual(ledger(seq), expected, f'ledger {seq}')
                time.sleep(0.5)
        finally:
            server.terminate(); server.wait()

    def tail_rotation(self, name, cache_last=None):
        """A cache to `cache_last` and a spool from the first snapshot, which the short
        publication has a rotation due for (given with a trailing separator); and a fresh tail's
        answer for a ledger past the pieces (`rpc --tail` builds its tail anew each time, as a
        server restarting does)."""
        snapshots = sorted(s['seq'] for s in self.catalog['snapshots'])
        short = self.short_publication()
        cache = self.root / f'{name}-cache'
        cache_last = cache_last or max(self.first, snapshots[0] + 1)
        run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache, '--range', f'{self.first}-{cache_last}')
        spool, seed = self.root / f'{name}-spool', self.root / f'{name}.seed'
        seed.write_bytes(os.urandom(32))
        self.restore('spool', spool, None, '--signing-key', seed, '--range', f'{snapshots[0] + 1}-')
        def ledger(seq):
            request = self.root / f'{name}-request.json'
            request.write_text(json.dumps({'method': 'ledger', 'params': [{'ledger_index': seq, 'transactions': True, 'expand': True, 'binary': True}]}))
            return run(BINARY, 'rpc', '--dir', cache, '--tail', spool, '--tail-key', spool / 'publisher.pub', '--request', request)
        rotate = lambda *extra: run(BINARY, 'rotate-spool', '--dir', f'{spool}/', '--signing-key', seed, '--source', short, '--publisher-key', short / 'publisher.pub', *extra)
        pieces_last = json.loads((cache / 'selection.json').read_text())['last']
        return spool, short, seed, pieces_last, ledger, rotate

    def test_a_rotation_started_after_the_cache_leaves_the_tail_no_gap(self):
        # Rotated to start at the ledger after the cache's last (as the service does: the cache
        # holds the newest snapshot), the new spool continues the pieces: the tail needs nothing
        # from the retired spool. The spool's siblings are made beside it, not inside it.
        import shutil
        snapshots = sorted(s['seq'] for s in self.catalog['snapshots'])
        spool, _, _, pieces_last, ledger, rotate = self.tail_rotation('gapless', snapshots[-2])
        before = {seq: ledger(seq) for seq in range(pieces_last + 1, self.last + 1)}
        result = rotate('--start', pieces_last + 1)
        self.assertTrue(result['rotated'], result)
        # At the cache's last ledger + 1, or the publication's tip if the cache already reaches it.
        self.assertLessEqual(result['start'], pieces_last + 1)
        self.assertGreater(result['start'], result['publication_snapshot'])
        retired = Path(result['retired'])
        self.assertEqual((retired.parent, retired.name.startswith('gapless-spool.retired-')), (spool.parent, True))
        self.assertFalse((spool / '.next').exists())
        self.assertFalse(Path(f'{spool}.next').exists())
        self.assertLessEqual(json.loads((spool / 'manifest.json').read_text())['chunks'][0]['first'], pieces_last + 1)
        shutil.rmtree(retired)
        for seq, expected in before.items():
            self.assertEqual(ledger(seq), expected, f'ledger {seq}')

    def test_a_tail_built_after_a_rotation_reads_the_retired_spool(self):
        # Rotated to the publication's tip, past the cache's last ledger: a tail built afterwards
        # (a restart) knows none of the replaced spool's chunks, and reads the ledgers in between
        # from the retired spool's signed manifest. Without it they cannot be served.
        import shutil
        spool, _, _, pieces_last, ledger, rotate = self.tail_rotation('restarted')
        before = {seq: ledger(seq) for seq in range(pieces_last + 1, self.last + 1)}
        result = rotate()
        self.assertTrue(result['rotated'], result)
        self.assertGreater(json.loads((spool / 'manifest.json').read_text())['chunks'][0]['first'], pieces_last + 1)  # a gap
        for seq, expected in before.items():
            self.assertEqual(ledger(seq), expected, f'ledger {seq}')
        shutil.rmtree(result['retired'])
        with self.assertRaisesRegex(RuntimeError, 'not contiguous'):
            ledger(pieces_last + 1)

    def test_a_tail_built_after_two_rotations_reads_both_retired_spools(self):
        # A cache short of two rotations' spools: the ledgers between its last and the spool's
        # first are in two retired spools, the older ending where the newer begins, and neither
        # holding them all. A tail built afterwards reads them from both.
        import shutil
        snapshots = sorted(s['seq'] for s in self.catalog['snapshots'])
        if len(snapshots) < 3: self.skipTest(f'two rotations need three snapshots: {snapshots}')
        def publication(name, until):
            """A publication of the archive to `until`, its tip and the newest snapshot before the
            tip (a publication ends on a piece; the rotation starts from that snapshot)."""
            bucket = self.root / f'{name}-bucket'
            (self.root / f'{name}.seed').write_bytes(os.urandom(32))
            subprocess.run([str(x) for x in [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', ARCHIVE / 'publisher.pub',
                            '--bucket', bucket, '--signing-key', self.root / f'{name}.seed', '--scratch', self.root / f'{name}-scratch', '--decoder', DECODER,
                            '--snapshot-tool', SNAPSHOT, '--piece-ledgers', PIECE, '--bundle-pieces', BUNDLE, '--grace-seconds', 0, '--until', until]],
                           check=True, capture_output=True)
            pointer = json.loads((bucket / 'catalog' / 'current.json').read_text())
            catalog = json.loads((bucket / pointer['object']).read_text())
            tip = catalog['tip']['seq']
            return bucket, tip, max(s['seq'] for s in catalog['snapshots'] if s['seq'] < tip)
        # Each a piece past a later snapshot of the archive's, so each has a newer snapshot than
        # the spool it rotates.
        middle, middle_tip, middle_snapshot = publication('middle', snapshots[1] + PIECE)
        late, late_tip, late_snapshot = publication('late', snapshots[2] + PIECE)
        if not snapshots[0] < middle_snapshot < middle_tip - 1 < late_snapshot < late_tip <= self.last:
            self.skipTest(f'no two rotations due: snapshots {snapshots}, middle {middle_snapshot}..{middle_tip}, late {late_snapshot}..{late_tip}')
        cache = self.root / 'stitched-cache'
        run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache, '--range', f'{self.first}-{max(self.first, snapshots[0] + 1)}')
        spool, seed = self.root / 'stitched-spool', self.root / 'stitched.seed'
        seed.write_bytes(os.urandom(32))
        # The first spool ends just before the middle publication's tip, where the spool replacing
        # it starts; that one, extended from the full publication, holds the ledgers after, past
        # the late publication's tip, where the last spool starts.
        self.restore('spool', spool, None, '--signing-key', seed, '--range', f'{snapshots[0] + 1}-{middle_tip - 1}')
        rotate = lambda source: run(BINARY, 'rotate-spool', '--dir', spool, '--signing-key', seed, '--source', source, '--publisher-key', source / 'publisher.pub')
        first_rotation = rotate(middle)
        self.assertTrue(first_rotation['rotated'], first_rotation)
        self.restore('spool', spool, None, '--signing-key', seed)
        def ledger(seq):
            request = self.root / 'stitched-request.json'
            request.write_text(json.dumps({'method': 'ledger', 'params': [{'ledger_index': seq, 'transactions': True, 'expand': True, 'binary': True}]}))
            return run(BINARY, 'rpc', '--dir', cache, '--tail', spool, '--tail-key', spool / 'publisher.pub', '--request', request)
        pieces_last = json.loads((cache / 'selection.json').read_text())['last']
        before = {seq: ledger(seq) for seq in range(pieces_last + 1, self.last + 1)}
        time.sleep(1.1)  # retired spools are named by the second
        result = rotate(late)
        self.assertTrue(result['rotated'], result)
        first = json.loads((spool / 'manifest.json').read_text())['chunks'][0]['first']
        retired = sorted(spool.parent.glob('stitched-spool.retired-*'))
        self.assertEqual(len(retired), 2, retired)
        older, newer = (json.loads((r / 'manifest.json').read_text()) for r in retired)
        self.assertGreater(newer['chunks'][0]['first'], pieces_last + 1)  # the newer does not reach the pieces
        self.assertLess(older['tip']['seq'] + 1, first)                  # nor the older the spool
        for seq, expected in before.items():
            self.assertEqual(ledger(seq), expected, f'ledger {seq}')
        shutil.rmtree(retired[0])
        with self.assertRaisesRegex(RuntimeError, 'not contiguous'):
            ledger(pieces_last + 1)

    def test_a_rotation_is_refused_while_the_spool_is_written(self):
        # A follower holds the spool's lock while it runs: a rotation by hand is refused before it
        # builds anything, rather than swapping the spool from under the follower's next batch.
        import fcntl
        spool, _, _, _, _, rotate = self.tail_rotation('locked')
        manifest = (spool / 'manifest.json').read_bytes()
        with open(spool / '.xrpla.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, 'in use'):
                rotate()
        self.assertFalse(Path(f'{spool}.next').exists())
        self.assertEqual((spool / 'manifest.json').read_bytes(), manifest)
        # As is one while another rotation builds the new spool: what it built is left alone.
        next_spool = Path(f'{spool}.next'); next_spool.mkdir()
        (next_spool / 'building').write_text('')
        with open(next_spool / '.xrpla.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, 'in use'):
                rotate()
        self.assertTrue((next_spool / 'building').exists())
        self.assertEqual((spool / 'manifest.json').read_bytes(), manifest)
        self.assertTrue(rotate()['rotated'])  # once the follower and the other rotation have stopped
        self.assertFalse(next_spool.exists())

    def test_a_rotation_stopped_between_its_renames_is_finished(self):
        # A crash after the spool was renamed away and before spool.next took its place: the next
        # rotation (or the service's start) puts spool.next in place, once its manifest is found
        # signed by the spool's key; one signed by another key is left where it is.
        spool, short, seed, _, _, rotate = self.tail_rotation('crashed')
        next_spool, retired = Path(f'{spool}.next'), Path(f'{spool}.retired-1')
        other = self.root / 'crashed-other.seed'; other.write_bytes(os.urandom(32))
        restore_next = lambda key: run(BINARY, 'restore', '--source', short, '--publisher-key', short / 'publisher.pub', '--to', 'spool',
                                       '--out', next_spool, '--signing-key', key)
        restore_next(other)
        spool.rename(retired)
        with self.assertRaisesRegex(RuntimeError, 'signature|differs'):
            rotate()
        self.assertTrue((next_spool / 'manifest.json').exists())
        self.assertFalse((spool / 'manifest.json').exists())
        import shutil
        shutil.rmtree(next_spool)
        restore_next(seed)
        tip = json.loads((next_spool / 'manifest.json').read_text())['tip']['seq']
        result = rotate()
        self.assertFalse(result['rotated'], result)  # it starts at the short publication's tip: nothing newer
        self.assertFalse(next_spool.exists())
        self.assertEqual(json.loads((spool / 'manifest.json').read_text())['tip']['seq'], tip)

    def test_rippled_databases_match_import(self):
        for backend in ('rocksdb', 'nudb'):
            reference = run(BINARY, 'import', 'rippled', '--dir', self.reference, '--out', self.root / f'import-{backend}', '--backend', backend)
            # NuDB by intervals: each snapshot interval on its own thread (without --jobs, only
            # intervals of 8192 ledgers or more, and the fixture's are shorter).
            restored = self.restore(backend, self.root / f'restored-{backend}', None, *(('--jobs', '4') if backend == 'nudb' else ()))
            self.assertEqual(restored['last_hash'], reference['last_hash'], backend)
            self.assertEqual(restored['transactions'], reference['transactions'], backend)
            for name, query in (('ledger.db', 'SELECT * FROM Ledgers'), ('transaction.db', 'SELECT * FROM Transactions'),
                                ('transaction.db', 'SELECT * FROM AccountTransactions')):
                self.assertEqual(rows(self.root / f'restored-{backend}' / name, query), rows(self.root / f'import-{backend}' / name, query), f'{backend} {query}')
            # Rows name each transaction as its own JSON does (TransactionType, Account, Sequence).
            for tx_hash, kind, account, sequence in rows(self.root / f'restored-{backend}' / 'transaction.db', 'SELECT TransID, TransType, FromAcct, FromSeq FROM Transactions'):
                request = json.dumps({'method': 'tx', 'params': [{'transaction': tx_hash}]})
                tx = json.loads(subprocess.run([BINARY, 'rpc', '--dir', self.reference, '--request', '-'], input=request, capture_output=True, text=True, check=True).stdout)
                self.assertEqual((kind, account, sequence), (tx['TransactionType'], tx['Account'], tx['Sequence']), tx_hash)
            # Every node xrpld's own insert path wrote, identical (NuDB: built directly, by
            # intervals); the restore also holds its baseline snapshot's whole tree.
            same = run(BINARY, 'compare-nodes', '--dir', self.root / f'restored-{backend}' / 'node-db', '--against', self.root / f'import-{backend}' / 'node-db', '--backend', backend)
            self.assertEqual((same['b']['missing_count'], same['a']['different'], same['b']['different']), (0, 0, 0), same)
            if backend == 'nudb':
                # By intervals or in order, the same nodes.
                sequential = self.restore('nudb', self.root / 'restored-nudb-sequential', None, '--jobs', '1')
                self.assertEqual(sequential['last_hash'], restored['last_hash'])
                self.assertTrue(run(BINARY, 'compare-nodes', '--dir', self.root / 'restored-nudb' / 'node-db',
                                    '--against', self.root / 'restored-nudb-sequential' / 'node-db')['same'])
                self.assertGreater(len(restored.get('intervals', [])), 1, 'the fixture publication has several intervals')
            # A finished restore is a no-op when run again (by intervals if it was restored so).
            again = self.restore(backend, self.root / f'restored-{backend}')
            self.assertEqual(again['ledgers_written'], restored['ledgers_written'])

    def test_clio_plan_matches_import(self):
        reference = run(BINARY, 'import', 'clio', '--dir', self.reference, '--out', self.root / 'import-clio')
        restored = self.restore('clio', self.root / 'restored-clio')['clio']
        self.assertEqual(restored['rows'], reference['rows'])
        plans = [Path(p).read_text().splitlines() for p in (restored['plan'], reference['plan'])]
        self.assertEqual(sorted(plans[0][1:]), sorted(plans[1][1:]))  # the first line names the source directory
        # The restore's plan skipped the independent replay (its own replay verified the store):
        # an import asking for it writes the plan again, verified, and that one is reused.
        summary = self.root / 'restored-clio' / 'plan' / 'clio-plan.json'
        self.assertFalse(json.loads(summary.read_text())['verified'])
        staging = self.root / 'restored-clio' / 'store-staging'
        imported = run(BINARY, 'import', 'clio', '--dir', staging, '--out', self.root / 'restored-clio' / 'plan')
        self.assertFalse(imported.get('reused', False))
        self.assertTrue(json.loads(summary.read_text())['verified'])
        self.assertTrue(run(BINARY, 'import', 'clio', '--dir', staging, '--out', self.root / 'restored-clio' / 'plan')['reused'])

    def test_clio_restore_loads_as_it_writes(self):
        # With a host, the plan is loaded as it is written (by several jobs and loader
        # workers): the keyspace ends with what the plan's rows written in order leave, the
        # range published, and the plan on disk is the one a plan-only restore writes.
        server = FakeCassandra()
        try:
            out = self.root / 'streamed-clio'
            clio = self.restore('clio', out, None, '--host', '127.0.0.1', '--port', server.port, '--keyspace', 'xrpla_stream',
                                '--jobs', '3', '--load-jobs', '2')['clio']
            self.assertTrue(clio['applied'] and clio['streamed'] and clio['load']['published'], clio)
            lines = Path(clio['plan']).read_text().splitlines(keepends=True)
            self.assertEqual(clio['load']['rows_sent'], clio['rows'])
            self.assertEqual(clio['load']['plan_sha256'], hashlib.sha256(''.join(lines).encode()).hexdigest())
            for table, rows in expected(lines).items():
                self.assertEqual(server.contents(table), rows, table)
            self.assertEqual(server.text_row('xrpla_import_status', 0)['last'], self.last)
            alone = self.restore('clio', self.root / 'plan-only-clio')['clio']
            self.assertEqual(Path(alone['plan']).read_bytes(), Path(clio['plan']).read_bytes())
            # Run again: the complete plan is reused and every line is acknowledged already.
            again = self.restore('clio', out, None, '--host', '127.0.0.1', '--port', server.port, '--keyspace', 'xrpla_stream')['clio']
            self.assertTrue(again['reused'])
            self.assertEqual(again['load']['rows_sent'], 0)
        finally:
            server.close()

    def test_over_http_with_ranges(self):
        handler = lambda *a, **k: RangeHandler(*a, directory=str(self.bucket), **k)
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            result = self.restore('rocksdb', self.root / 'restored-http', f'http://127.0.0.1:{server.server_address[1]}')
            self.assertEqual(result['last_hash'], self.restore('rocksdb', self.root / 'restored-rocksdb-2')['last_hash'])
            self.assertGreater(result['bytes_read'], 0)
        finally:
            server.shutdown(); server.server_close()

    def test_resumes_and_extends(self):
        # A restore that stopped part way (here: at the end of a shorter range) continues from
        # its last committed ledger and ends identical to one uninterrupted restore.
        middle = (self.first + self.last) // 2
        for target, key, jobs in (('rocksdb', 'last_hash', ()), ('nudb', 'last_hash', ()), ('nudb', 'last_hash', ('--jobs', '4')), ('store', 'last_ledger_hash', ())):
            out = self.root / f'resumed-{target}-{len(jobs)}'
            first = self.restore(target, out, None, '--range', f'{self.first}-{middle}', *jobs)
            self.assertEqual('intervals' in first, bool(jobs), target)
            resumed = self.restore(target, out)  # by intervals again if it began so
            self.assertEqual('intervals' in resumed, bool(jobs), target)
            whole = self.restore(target, self.root / f'whole-{target}-{len(jobs)}')
            self.assertEqual(resumed[key], whole[key], target)
            if target in ('rocksdb', 'nudb'):
                self.assertEqual(resumed['ledgers_written'], self.last - middle)
                self.assertEqual(rows(out / 'transaction.db', 'SELECT * FROM Transactions'), rows(self.root / f'whole-{target}-{len(jobs)}' / 'transaction.db', 'SELECT * FROM Transactions'))
            else:
                self.assertEqual(resumed['ledgers_applied'], self.last - middle)

    def test_transaction_indexes_match_sqlites_own(self):
        # transaction.db's indexes are built outside SQLite; XRPLA_SQL_INDEX=sqlite builds them
        # with CREATE INDEX. Both pass integrity_check, record the same schema and answer the
        # same through each index.
        outside, inside = self.root / 'indexes-outside', self.root / 'indexes-sqlite'
        self.restore('nudb', outside)
        run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', 'nudb', '--out', inside,
            env=dict(os.environ, XRPLA_SQL_INDEX='sqlite'))
        def query(out, sql):
            with closing(sqlite3.connect(out / 'transaction.db')) as db: return db.execute(sql).fetchall()
        for out in (outside, inside): self.assertEqual(query(out, 'PRAGMA integrity_check'), [('ok',)], out)
        schema = 'SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name'
        self.assertEqual(query(outside, schema), query(inside, schema))
        for sql in ('SELECT TransID, rowid FROM Transactions INDEXED BY sqlite_autoindex_Transactions_1 ORDER BY TransID',
                    'SELECT TransID, rowid FROM AccountTransactions INDEXED BY AcctTxIDIndex ORDER BY TransID, rowid',
                    'SELECT Account, LedgerSeq, TxnSeq, TransID FROM AccountTransactions INDEXED BY AcctTxIndex ORDER BY Account, LedgerSeq, TxnSeq, TransID, rowid'):
            self.assertEqual(query(outside, sql), query(inside, sql), sql)
            self.assertTrue(query(outside, sql), sql)

    def test_resume_after_rows_outran_the_receipt(self):
        # A crash after a window's rows committed and before its receipt recorded them: the
        # resume writes those ledgers again, and while loading (no unique keys yet) the first
        # copies must go, or the final unique index fails. At the first window the receipt has
        # no rows to name; at a later one (by intervals) it names the rowids to keep.
        for target, jobs, window in (('nudb', (), 1), ('nudb', ('--jobs', '4'), 1), ('nudb', ('--jobs', '4'), 2), ('rocksdb', (), 1)):
            out, whole = self.root / f'outran-{target}-{len(jobs)}-{window}', self.root / f'outran-whole-{target}-{len(jobs)}-{window}'
            command = [str(x) for x in (BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', target, '--out', out, *jobs)]
            crashed = subprocess.run(command, capture_output=True, text=True, timeout=600, env=dict(os.environ, XRPLA_TEST_EXIT_AFTER_ROWS=str(window)))
            self.assertEqual(crashed.returncode, 75, crashed.stderr[-2000:])
            if window > 1: self.assertIn('sql_rowids', json.loads((out / 'xrpla-import.json').read_text()))
            resumed = self.restore(target, out, None, *jobs)
            self.assertEqual(resumed['last_hash'], self.restore(target, whole, None, *jobs)['last_hash'], target)
            for query in ('SELECT * FROM Transactions', 'SELECT * FROM AccountTransactions'):
                self.assertEqual(rows(out / 'transaction.db', query), rows(whole / 'transaction.db', query), f'{target} {query}')

    def test_resume_after_an_interrupted_finish(self):
        # A crash in the finish phase, after the last window committed: once transaction.db's
        # index pages are written but before SQLite records them (NuDB's key file part built),
        # or once the key file and indexes are complete but before the receipt says so. The
        # resume applies no ledger again, only finishes, and ends identical to one uninterrupted
        # restore: the same nodes, rows, schema and a sound database.
        jobs = {'nudb': ('--jobs', '1'), 'rocksdb': ()}  # NuDB in order, not by intervals
        wholes = {}
        for target in ('nudb', 'rocksdb'):
            wholes[target] = self.root / f'finish-whole-{target}'
            self.restore(target, wholes[target], None, *jobs[target])
        # 'pages': stopped with the index pages written but not yet counted in the header.
        for target, stage in (('nudb', 'pages'), ('nudb', 'indexes'), ('nudb', 'receipt'), ('rocksdb', 'pages'), ('rocksdb', 'indexes'), ('rocksdb', 'receipt')):
            out, whole, name = self.root / f'finish-{target}-{stage}', wholes[target], f'{target} {stage}'
            command = [str(x) for x in (BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', target, '--out', out, *jobs[target])]
            crashed = subprocess.run(command, capture_output=True, text=True, timeout=600, env=dict(os.environ, XRPLA_TEST_EXIT_IN_FINISH=stage))
            self.assertEqual(crashed.returncode, 75, crashed.stderr[-2000:])
            receipt = json.loads((out / 'xrpla-import.json').read_text())
            self.assertEqual(receipt['last'], self.last, name)
            self.assertNotIn('finished', receipt, name)
            if stage in ('pages', 'indexes'): self.assertTrue((out / 'transaction.db-xrpla-index').exists(), name)  # the build was under way
            resumed = self.restore(target, out, None, *jobs[target])
            self.assertEqual(resumed['phase_seconds']['read'], 0, name)  # no ledger read again
            # The finished receipt describes the restore, its counts carried from the last window's.
            whole_receipt = json.loads((whole / 'xrpla-import.json').read_text())
            for key in ('ledgers_written', 'transactions', 'last_hash'):
                self.assertEqual(resumed[key], whole_receipt[key], f'{name} {key}')
                self.assertEqual(json.loads((out / 'xrpla-import.json').read_text())[key], whole_receipt[key], f'{name} {key}')
            self.assertTrue(json.loads((out / 'xrpla-import.json').read_text())['finished'], name)
            self.assertFalse((out / 'transaction.db-xrpla-index').exists(), name)
            same = run(BINARY, 'compare-nodes', '--dir', out / 'node-db', '--against', whole / 'node-db', '--backend', target)
            self.assertTrue(same['same'], (name, same))
            for db, query in (('ledger.db', 'SELECT * FROM Ledgers'), ('transaction.db', 'SELECT * FROM Transactions'),
                              ('transaction.db', 'SELECT * FROM AccountTransactions'), ('transaction.db', 'SELECT type, name, tbl_name, sql FROM sqlite_master')):
                self.assertEqual(rows(out / db, query), rows(whole / db, query), f'{name} {query}')
            self.assertEqual(rows(out / 'transaction.db', 'PRAGMA integrity_check'), [('ok',)], name)
            # The indexes answer as those of the uninterrupted restore.
            query = 'SELECT Account, LedgerSeq, TxnSeq, TransID FROM AccountTransactions INDEXED BY AcctTxIndex ORDER BY Account, LedgerSeq, TxnSeq, TransID, rowid'
            with closing(sqlite3.connect(out / 'transaction.db')) as a, closing(sqlite3.connect(whole / 'transaction.db')) as b:
                self.assertEqual(a.execute(query).fetchall(), b.execute(query).fetchall(), name)

    def test_resume_after_an_interrupted_sqlite_index_build(self):
        # XRPLA_SQL_INDEX=sqlite: CREATE INDEX and the schema rewrite that makes the TransID index
        # the primary key's commit at once. Stopped before that commit, the file holds the loading
        # schema again (no index, xrpla_transaction_id included), and the resume ends identical
        # to one uninterrupted restore.
        env = dict(os.environ, XRPLA_SQL_INDEX='sqlite')
        jobs = {'nudb': ('--jobs', '1'), 'rocksdb': ()}
        for target in ('nudb', 'rocksdb'):
            out, whole = self.root / f'sqlite-finish-{target}', self.root / f'sqlite-finish-whole-{target}'
            run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', target, '--out', whole, *jobs[target], env=env)
            command = [str(x) for x in (BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', target, '--out', out, *jobs[target])]
            crashed = subprocess.run(command, capture_output=True, text=True, timeout=600, env=dict(env, XRPLA_TEST_EXIT_IN_FINISH='sqlite-indexes'))
            self.assertEqual(crashed.returncode, 75, crashed.stderr[-2000:])
            self.assertNotIn('finished', json.loads((out / 'xrpla-import.json').read_text()), target)
            self.assertEqual(rows(out / 'transaction.db', "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name IN ('Transactions', 'AccountTransactions')"), [], target)  # rolled back
            resumed = run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', target, '--out', out, *jobs[target], env=env)
            self.assertEqual(resumed['last_hash'], json.loads((whole / 'xrpla-import.json').read_text())['last_hash'], target)
            for query in ('SELECT type, name, tbl_name, sql FROM sqlite_master', 'SELECT * FROM Transactions', 'SELECT * FROM AccountTransactions'):
                self.assertEqual(rows(out / 'transaction.db', query), rows(whole / 'transaction.db', query), f'{target} {query}')
            self.assertEqual(rows(out / 'transaction.db', 'PRAGMA integrity_check'), [('ok',)], target)

    def test_an_older_finished_restore_xrpld_wrote_to_is_refused(self):
        # A restore finished by a release before receipts said "finished": its receipt names the
        # last ledger, its header and the finish's output, and a rerun only finishes (the same
        # nodes, the counts kept). Once xrpld has appended to the NuDB data file, a rerun would
        # cut its nodes off: it is refused, the file left as it is.
        out = self.root / 'older-finished-nudb'
        restored = self.restore('nudb', out, None, '--jobs', '1')
        receipt_path = out / 'xrpla-import.json'
        def as_older():
            receipt = json.loads(receipt_path.read_text())
            self.assertTrue(receipt.pop('finished')); self.assertIn('last_header', receipt); self.assertIn('nudb', receipt)
            receipt_path.write_text(json.dumps(receipt))
        as_older()
        again = self.restore('nudb', out, None, '--jobs', '1')
        self.assertEqual((again['ledgers_written'], again['transactions'], again['last_hash']),
                         (restored['ledgers_written'], restored['transactions'], restored['last_hash']))
        self.assertEqual(again['phase_seconds']['read'], 0)
        as_older()
        data = next((out / 'node-db').glob('*.dat'))
        with data.open('ab') as f: f.write(os.urandom(4096))  # as xrpld appends nodes
        size = data.stat().st_size
        with self.assertRaisesRegex(RuntimeError, 'xrpld has since written to'):
            self.restore('nudb', out, None, '--jobs', '1')
        self.assertEqual(data.stat().st_size, size)

    def test_extending_a_finished_restore_xrpld_wrote_to_is_refused(self):
        # A finished NuDB restore, in order or by intervals, extended later: once xrpld has
        # appended to its data file, the extension would cut those nodes off, and is refused
        # before it writes. An extension stopped part way leaves the file longer than the finish
        # did too, and goes on: it ends identical to one uninterrupted restore.
        middle = (self.first + self.last) // 2
        for jobs in (('--jobs', '1'), ('--jobs', '4')):
            out, whole, name = self.root / f'extended-written-{jobs[1]}', self.root / f'extended-whole-{jobs[1]}', ' '.join(jobs)
            self.restore('nudb', out, None, '--range', f'{self.first}-{middle}', *jobs)
            data = next((out / 'node-db').glob('*.dat'))
            size = data.stat().st_size
            with data.open('ab') as f: f.write(os.urandom(4096))  # as xrpld appends nodes
            with self.assertRaisesRegex(RuntimeError, 'xrpld has since written to'):
                self.restore('nudb', out, None, *jobs)
            self.assertEqual(data.stat().st_size, size + 4096, name)
            os.truncate(data, size)  # as the restore left it
            command = [str(x) for x in (BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', 'nudb', '--out', out, *jobs)]
            crashed = subprocess.run(command, capture_output=True, text=True, timeout=600, env=dict(os.environ, XRPLA_TEST_EXIT_AFTER_ROWS='1'))
            self.assertEqual(crashed.returncode, 75, crashed.stderr[-2000:])
            self.assertNotIn('nudb', json.loads((out / 'xrpla-import.json').read_text()), name)  # no longer names the finish
            extended = self.restore('nudb', out, None, *jobs)
            self.assertEqual(extended['last_hash'], self.restore('nudb', whole, None, *jobs)['last_hash'], name)
            same = run(BINARY, 'compare-nodes', '--dir', out / 'node-db', '--against', whole / 'node-db', '--backend', 'nudb')
            self.assertTrue(same['same'], (name, same))
            for query in ('SELECT * FROM Transactions', 'SELECT * FROM AccountTransactions'):
                self.assertEqual(rows(out / 'transaction.db', query), rows(whole / 'transaction.db', query), f'{name} {query}')

    def test_an_interval_restore_stopped_in_its_finish_finishes(self):
        # By intervals, stopped in the finish phase run after run (every window committed): each
        # run rewrites the receipt for its intervals before it starts, which must go on naming
        # where the NuDB build stopped, or the next run would cut the data file back to its
        # header. It ends identical to one uninterrupted restore.
        jobs = ('--jobs', '4')
        out, whole = self.root / 'interval-finish', self.root / 'interval-finish-whole'
        expected = self.restore('nudb', whole, None, *jobs)
        command = [str(x) for x in (BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', 'nudb', '--out', out, *jobs)]
        receipt_path = out / 'xrpla-import.json'
        for stage in ('pages', 'indexes', 'receipt'):
            crashed = subprocess.run(command, capture_output=True, text=True, timeout=600, env=dict(os.environ, XRPLA_TEST_EXIT_IN_FINISH=stage))
            self.assertEqual(crashed.returncode, 75, f'{stage}: {crashed.stderr[-2000:]}')
            receipt = json.loads(receipt_path.read_text())
            self.assertNotIn('finished', receipt, stage)
            self.assertIn('nudb_data_size', receipt, stage)
        # A receipt naming committed intervals but not the checkpoint they end at is refused,
        # the data file left as it is.
        kept = receipt_path.read_text()
        receipt = json.loads(kept); del receipt['nudb_data_size']; receipt_path.write_text(json.dumps(receipt))
        data = next((out / 'node-db').glob('*.dat'))
        size = data.stat().st_size
        with self.assertRaisesRegex(RuntimeError, 'not where its NuDB build stopped'):
            self.restore('nudb', out, None, *jobs)
        self.assertEqual(data.stat().st_size, size)
        receipt_path.write_text(kept)
        resumed = self.restore('nudb', out, None, *jobs)
        # The finished receipt describes the restore, its counts carried from the windows' receipts.
        for key in ('last_hash', 'ledgers_written', 'transactions'):
            self.assertEqual(resumed[key], expected[key], key)
            self.assertEqual(json.loads(receipt_path.read_text())[key], expected[key], key)
        self.assertTrue(json.loads(receipt_path.read_text())['finished'])
        self.assertFalse((out / 'transaction.db-xrpla-index').exists())
        same = run(BINARY, 'compare-nodes', '--dir', out / 'node-db', '--against', whole / 'node-db', '--backend', 'nudb')
        self.assertTrue(same['same'], same)
        for db, query in (('ledger.db', 'SELECT * FROM Ledgers'), ('transaction.db', 'SELECT * FROM Transactions'),
                          ('transaction.db', 'SELECT * FROM AccountTransactions'), ('transaction.db', 'SELECT type, name, tbl_name, sql FROM sqlite_master')):
            self.assertEqual(rows(out / db, query), rows(whole / db, query), query)
        self.assertEqual(rows(out / 'transaction.db', 'PRAGMA integrity_check'), [('ok',)])

    def test_an_interval_extension_without_its_verifier_state_is_refused(self):
        # By intervals, libxrpl's verifier goes on from the state it kept for an interval, which
        # one restored with the replay verifier has none of: the extension is refused, before it
        # writes, rather than failing on an empty store. Without the setting the replay goes on,
        # identical to one uninterrupted restore.
        jobs = ('--jobs', '4')
        snapshots = sorted(s['seq'] for s in self.catalog['snapshots'])
        out, whole = self.root / 'interval-verifier-state', self.root / 'interval-verifier-state-whole'
        self.restore('nudb', out, None, '--range', f'{self.first}-{snapshots[-2] + 2}', *jobs)  # its last interval goes on later
        data = next((out / 'node-db').glob('*.dat'))
        size, receipt = data.stat().st_size, (out / 'xrpla-import.json').read_text()
        self.assertTrue(json.loads(receipt)['finished'])
        self.assertIn('intervals', json.loads(receipt))
        with self.assertRaisesRegex(RuntimeError, "libxrpl's verifier state for it"):
            run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', 'nudb', '--out', out, *jobs,
                env=dict(os.environ, XRPLA_RESTORE_VERIFIER='libxrpl'))
        self.assertEqual((data.stat().st_size, (out / 'xrpla-import.json').read_text()), (size, receipt))
        extended = self.restore('nudb', out, None, *jobs)
        self.assertEqual(extended['last_hash'], self.restore('nudb', whole, None, *jobs)['last_hash'])
        same = run(BINARY, 'compare-nodes', '--dir', out / 'node-db', '--against', whole / 'node-db', '--backend', 'nudb')
        self.assertTrue(same['same'], same)
        for query in ('SELECT * FROM Transactions', 'SELECT * FROM AccountTransactions'):
            self.assertEqual(rows(out / 'transaction.db', query), rows(whole / 'transaction.db', query), query)

    def test_an_in_order_extension_without_its_verifier_state_is_refused(self):
        # In order too: a NuDB restore made with the replay verifier keeps no state for libxrpl's,
        # and extended with it would start over, discarding the finished database. It is refused
        # before it writes; without the setting the replay goes on.
        jobs = ('--jobs', '1')
        middle = (self.first + self.last) // 2
        out, whole = self.root / 'in-order-verifier-state', self.root / 'in-order-verifier-state-whole'
        self.restore('nudb', out, None, '--range', f'{self.first}-{middle}', *jobs)
        data = next((out / 'node-db').glob('*.dat'))
        size, receipt = data.stat().st_size, (out / 'xrpla-import.json').read_text()
        self.assertTrue(json.loads(receipt)['finished'])
        self.assertNotIn('intervals', json.loads(receipt))
        with self.assertRaisesRegex(RuntimeError, "libxrpl's verifier state for it"):
            run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', self.key, '--to', 'nudb', '--out', out, *jobs,
                env=dict(os.environ, XRPLA_RESTORE_VERIFIER='libxrpl'))
        self.assertEqual((data.stat().st_size, (out / 'xrpla-import.json').read_text()), (size, receipt))
        extended = self.restore('nudb', out, None, *jobs)
        self.assertEqual(extended['last_hash'], self.restore('nudb', whole, None, *jobs)['last_hash'])

    def test_sync_accepts_a_publication_of_only_its_baseline(self):
        # A new publication (a connected install before its first piece is sealed) holds its
        # baseline snapshot alone: a cache syncs from it, for a live tail to serve after it.
        bucket, seed = self.root / 'baseline-bucket', self.root / 'baseline.seed'
        seed.write_bytes(os.urandom(32))
        subprocess.run([str(x) for x in [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', ARCHIVE / 'publisher.pub',
                        '--bucket', bucket, '--signing-key', seed, '--scratch', self.root / 'baseline-scratch', '--decoder', DECODER,
                        '--snapshot-tool', SNAPSHOT, '--piece-ledgers', 1000, '--bundle-pieces', 2, '--grace-seconds', 0]], check=True, capture_output=True)
        pointer = json.loads((bucket / 'catalog' / 'current.json').read_text())
        self.assertEqual(json.loads((bucket / pointer['object']).read_text())['ranges'], [])
        result = run(BINARY, 'sync', '--source', bucket, '--publisher-key', bucket / 'publisher.pub', '--dir', self.root / 'baseline-cache')
        self.assertEqual((result['ranges'], result['last']), (0, result['snapshot']))

    def test_sync_builds_a_servable_cache(self):
        cache = self.root / 'synced'
        first = run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache)
        self.assertGreater(first['fetched_bytes'], 0)
        self.assertTrue((cache / 'selection.json').exists())
        request = json.dumps({'method': 'ledger', 'params': [{'ledger_index': self.last, 'transactions': True, 'expand': True, 'binary': True}]})
        answers = [subprocess.run([BINARY, 'rpc', '--dir', d, '--request', '-'], input=request, capture_output=True, text=True, check=True).stdout
                   for d in (cache, self.reference)]
        self.assertEqual(json.loads(answers[0])['ledger'], json.loads(answers[1])['ledger'])
        self.assertEqual(run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache)['fetched_bytes'], 0)

    def test_sync_refuses_an_older_catalog(self):
        # The catalog pointer is not signed: a stale copy of it (a cache in front of the origin,
        # a mirror replaying it) names an older generation, signed as it once was. A cache that
        # holds a newer one takes it as nothing newer (its selection kept, and served), and
        # refuses its own generation come back different.
        sys.path.insert(0, str(ROOT / 'tools'))
        from fast_archive import Signer, digest, encoded
        import shutil
        bucket, cache = self.root / 'replayed-bucket', self.root / 'replayed-cache'
        shutil.copytree(self.bucket, bucket)
        pointer = bucket / 'catalog' / 'current.json'; older = pointer.read_bytes()
        def sign(catalog):
            raw = encoded(catalog); name = f'catalog/{catalog["generation"]:08d}.json'
            (bucket / name).write_bytes(raw); (bucket / (name + '.sig')).write_bytes(Signer().sign(raw, self.root / 'publisher.seed')[1])
            pointer.write_bytes(encoded({'generation': catalog['generation'], 'object': name, 'sha256': digest(raw)}))
        sync = lambda: run(BINARY, 'sync', '--source', bucket, '--publisher-key', self.key, '--dir', cache)
        newer = dict(self.catalog, generation=self.catalog['generation'] + 1, published_at='2026-10-15T00:00:00Z')
        sign(newer); sync()
        pointer.write_bytes(older)
        selection = (cache / 'selection.json').stat()
        result = sync()
        self.assertEqual((result['generation'], result['refused_older']), (newer['generation'], self.catalog['generation']))
        self.assertEqual(((cache / 'selection.json').stat().st_ino, (cache / 'selection.json').stat().st_mtime_ns), (selection.st_ino, selection.st_mtime_ns))
        sign(dict(newer, published_at='2026-10-16T00:00:00Z'))
        with self.assertRaisesRegex(RuntimeError, 'differs from the one this cache holds'): sync()
        sign(newer); sync()  # the generation it holds, as it was
        self.assertEqual(json.loads((cache / 'selection.json').read_text())['catalog']['generation'], newer['generation'])

    def test_a_locked_cache_is_left_alone(self):
        # A sync or index-pieces run while another holds the cache (the service's background
        # sync, one run by hand) is refused: they would share staging files and indexes.
        import fcntl
        cache = self.root / 'locked-cache'; cache.mkdir()
        with open(cache / '.xrpla.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, 'locked'): run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache)
            with self.assertRaisesRegex(RuntimeError, 'locked'): run(BINARY, 'index-pieces', '--dir', cache)
        self.assertFalse((cache / 'selection.json').exists())

    def test_a_damaged_index_is_rebuilt(self):
        # An index file is checked whole (its SHA-256 trailer) once; one changed since is checked
        # again, and rebuilt when it differs.
        cache = self.root / 'damaged-cache'
        sync = lambda: run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', cache)
        built = lambda result: [i for i in result['indexes'] if i.get('status') == 'built']
        self.assertTrue(built(sync()))
        self.assertEqual(built(sync()), [])
        index = sorted((cache / 'query').glob('*.xqix'))[0]
        data = bytearray(index.read_bytes()); data[len(data) // 2] ^= 1; index.write_bytes(bytes(data))
        # Rebuilt into a file renamed over the damaged one (never deleted first, so a server
        # starting meanwhile finds it), and selection.json written again though unchanged:
        # servers reopen the cache only when it changes, and still map the damaged file.
        selection = (cache / 'selection.json').stat().st_ino
        self.assertEqual(len(built(sync())), 1)
        self.assertNotEqual((cache / 'selection.json').stat().st_ino, selection)
        selection = (cache / 'selection.json').stat().st_ino
        self.assertEqual(built(sync()), [])
        self.assertEqual((cache / 'selection.json').stat().st_ino, selection)  # nothing damaged: left as it is
        rebuilt = index.read_bytes()
        self.assertNotEqual(rebuilt, bytes(data))
        self.assertEqual(hashlib.sha256(rebuilt[:-32]).digest(), rebuilt[-32:])

    def setup(self, config, answers):
        # An empty publication list: these answer the publication by URL and key.
        (self.root / 'no-publications.toml').write_text('')
        result = subprocess.run([BINARY, 'setup', '--config', config, '--unit-dir', self.root / 'units'], input='\n'.join(answers) + '\n',
                                capture_output=True, text=True, timeout=600, env={**os.environ, 'XRPLA_PUBLICATIONS': str(self.root / 'no-publications.toml')})
        if result.returncode: raise RuntimeError(result.stdout[-2000:] + result.stderr[-2000:])
        return result.stdout

    def test_setup_restore_runs_once(self):
        config, out = self.root / 'etc-restore' / 'xrpla.toml', self.root / 'setup-restore'
        printed = self.setup(config, ['restore', str(self.bucket), str(self.key), 'earliest', 'rocksdb', str(out), 'y'])
        self.assertEqual(tomllib.loads(config.read_text())['restore'], {'target': 'rocksdb', 'out': str(out)})
        result = json.loads(printed[printed.index('{\n'):])
        self.assertEqual(result['last_hash'], self.restore('rocksdb', self.root / 'restored-rocksdb-3')['last_hash'])
        again = json.loads(subprocess.run([BINARY, 'run', '--config', config], capture_output=True, text=True, check=True).stdout)
        self.assertEqual(again['last'], result['last'])

    def test_setup_mirror_writes_service(self):
        config = self.root / 'etc-mirror' / 'xrpla.toml'
        hex_key = self.key.read_bytes().hex()
        printed = self.setup(config, ['mirror', str(self.bucket), hex_key, '', str(self.root / 'mirror-store'), '1024', '127.0.0.1:0', '', '60'])
        # The unit runs the program setup ran as: a build tree's is named, as one to replace.
        program = Path(BINARY).resolve()
        if not str(program).startswith('/usr/'):
            self.assertIn(f'Warning: its ExecStart is {program},', printed)
        text = config.read_text()
        written = tomllib.loads(text)
        self.assertEqual(written['profile'], 'mirror')
        self.assertEqual((written['storage']['cache_mb'], written['mirror']['sync_seconds'], written['history']['first']), (1024, 60, 'earliest'))
        # The file documents every setting: the chosen ones set, the rest commented with their defaults.
        reference = (ROOT / 'config' / 'xrpla.example.toml').read_text()
        self.assertIn('cache_mb = 1024\n', text)
        for line in ('#nfts_by_issuer = true', '#forward = "http://127.0.0.1:51234"', '#[peers]', '# Clio\'s nfts_by_issuer: every NFToken of an issuer'):
            self.assertIn(line, text)
        self.assertEqual(len(text.splitlines()), len(reference.splitlines()) + 2)
        self.assertEqual((config.parent / 'publisher.pub').read_text().strip(), hex_key)
        unit = (self.root / 'units' / 'xrpla.service').read_text()
        self.assertIn(f'xrpla run --config {config}', unit)
        # A dedicated account, writing only its data directory.
        for line in ('User=xrpla', 'ProtectSystem=strict', f"ReadWritePaths={written['storage']['dir']}"):
            self.assertIn(line + '\n', unit)
        self.assertNotIn('User=root', unit)

    def test_run_refuses_serving_settings_out_of_bounds(self):
        # Held to `xrpla serve`'s flag bounds before the first sync, not after it (when an interim
        # server given them as flags would fail to start every 30 s).
        for key, value in (('serve.threads', 0), ('serve.threads', 33), ('serve.max_connections', 0),
                           ('serve.max_connections', (1 << 20) + 1), ('serve.max_connections_per_ip', (1 << 20) + 1),
                           ('storage.cache_mb', (1 << 30) + 1)):
            section, name = key.split('.')
            config = self.root / 'etc-bounds' / 'xrpla.toml'
            config.parent.mkdir(exist_ok=True)
            settings = {'serve': {}, 'storage': {'dir': str(self.root / 'bounds-store')}}
            settings[section][name] = value
            config.write_text(f'profile = "mirror"\n\n[publication]\nsource = "{self.bucket}"\npublisher_key = "{self.key}"\n\n'
                              + ''.join(f'[{s}]\n' + ''.join(f'{k} = {json.dumps(v)}\n' for k, v in table.items()) + '\n' for s, table in settings.items()))
            result = subprocess.run([BINARY, 'run', '--config', config], capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(f'Config {key} must be from', result.stdout + result.stderr)
            self.assertFalse((self.root / 'bounds-store' / 'selection.json').exists())  # refused before any sync

    def test_setup_connected_writes_service(self):
        config = self.root / 'etc-connected' / 'xrpla.toml'
        self.setup(config, ['connected', 'http://127.0.0.1:5005', '', 'y', '', '', '', '', '123', str(self.root / 'connected-store'), '', '', ''])
        written = tomllib.loads(config.read_text())
        self.assertEqual(written['profile'], 'connected')
        self.assertNotIn('publication', written)
        self.assertEqual(written['node'], {'rpc': 'http://127.0.0.1:5005', 'validations': 'ws://127.0.0.1:6006'})
        self.assertEqual(written['validators']['list'], 'https://vl.ripple.com')
        self.assertEqual((written['connected']['start'], written['serve']['http'], written['storage']['cache_mb']), (123, '127.0.0.1:5105', 0))
        self.assertIn(f'xrpla run --config {config}', (self.root / 'units' / 'xrpla.service').read_text())

    def test_setup_connected_with_a_publication(self):
        config = self.root / 'etc-connected-pub' / 'xrpla.toml'
        hex_key = self.key.read_bytes().hex()
        self.setup(config, ['connected', 'http://127.0.0.1:5005', '', 'n', str(self.bucket), hex_key, str(self.root / 'connected-pub-store'), '', '', ''])
        written = tomllib.loads(config.read_text())
        self.assertEqual(written['publication']['source'], str(self.bucket))
        self.assertEqual((config.parent / 'publisher.pub').read_text().strip(), hex_key)
        self.assertNotIn('start', written.get('connected', {}))

    def test_setup_standalone_writes_service(self):
        config = self.root / 'etc-standalone' / 'xrpla.toml'
        self.setup(config, ['standalone', str(self.bucket), str(self.key), '', str(self.root / 'standalone-store'), '', '', ''])
        written = tomllib.loads(config.read_text())
        self.assertEqual((written['profile'], written['publication']['source'], written['serve']['http']), ('standalone', str(self.bucket), '127.0.0.1:5005'))
        self.assertNotIn('mirror', written)
        self.assertIn(f'xrpla run --config {config}', (self.root / 'units' / 'xrpla.service').read_text())

    # Mirrors: a publication served from several places, tried in order.
    def serve(self, directory):
        """`directory` over HTTP with range requests, as an R2 public bucket serves it, until the test ends."""
        handler = lambda *a, **k: RangeHandler(*a, directory=str(directory), **k)
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        return f'http://127.0.0.1:{server.server_address[1]}'

    @staticmethod
    def unreachable():
        """A loopback URL nothing listens on."""
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            return f'http://127.0.0.1:{probe.getsockname()[1]}'

    def tampered(self, name):
        """A copy of the bucket whose every container has one byte changed (its catalog intact)."""
        copy = self.root / name
        shutil.copytree(self.bucket, copy)
        for r in self.catalog['ranges']:
            container = copy / r['object']
            data = bytearray(container.read_bytes()); data[len(data) // 2] ^= 1; container.write_bytes(bytes(data))
        return copy

    def listed(self, *mirrors):
        """An environment whose publication list holds `test-pub`: this bucket's key, served by `mirrors`."""
        path = self.root / f'publications-{len(list(self.root.glob("publications-*")))}.toml'
        path.write_text('[[publication]]\nname = "test-pub"\nnetwork = "mainnet"\n'
                        f'publisher_key = "{self.key.read_bytes().hex()}"\nmirrors = [{", ".join(json.dumps(m) for m in mirrors)}]\n')
        return dict(os.environ, XRPLA_PUBLICATIONS=str(path))

    def sync_listed(self, cache, *mirrors):
        result = subprocess.run([BINARY, 'sync', '--publication', 'test-pub', '--dir', str(cache)], env=self.listed(*mirrors),
                                capture_output=True, text=True, timeout=1800)
        self.assertEqual(result.returncode, 0, result.stdout[-2000:] + result.stderr[-3000:])
        return json.loads(result.stdout), result.stderr

    def test_sync_moves_to_the_next_mirror(self):
        # A mirror out of reach, or one serving objects that differ from the signed catalog, is
        # passed over (and logged); the publication comes whole from the next.
        direct = run(BINARY, 'sync', '--source', self.bucket, '--publisher-key', self.key, '--dir', self.root / 'mirror-cache-direct')
        same = lambda result: (result['generation'], result['first'], result['last'], result['cached_bytes'])
        good, tampered = self.serve(self.bucket), self.serve(self.tampered('mirror-tampered'))
        result, log = self.sync_listed(self.root / 'mirror-cache-listed', good)
        self.assertEqual(same(result), same(direct))
        self.assertNotIn('mirror_failed', log)
        for name, first in (('unreachable', self.unreachable()), ('tampered', tampered)):
            result, log = self.sync_listed(self.root / f'mirror-cache-{name}', first, good)
            self.assertEqual(same(result), same(direct), name)
            self.assertIn('"event":"mirror_failed"', log, name)
            failures = [line for line in log.splitlines() if f'"mirror":"{first}"' in line]
            self.assertTrue(failures, name)
            if name == 'unreachable':
                # Found out of reach once (asked for its catalog's generation), then left to rest:
                # neither the catalog nor any download asks it again.
                self.assertEqual(len(failures), 1, log)
        self.assertIn('Object SHA-256 or size mismatch', log)

    def test_a_mirror_behind_the_cache_is_passed_over(self):
        # A mirror offering an older generation than the cache holds is refused as a stale
        # pointer is, and the next mirror asked; with no other mirror, the cache keeps its own.
        sys.path.insert(0, str(ROOT / 'tools'))
        from fast_archive import Signer, digest, encoded
        newer_bucket = self.root / 'mirror-newer'
        shutil.copytree(self.bucket, newer_bucket)
        newer = dict(self.catalog, generation=self.catalog['generation'] + 1, published_at='2026-10-15T00:00:00Z')
        raw = encoded(newer); name = f'catalog/{newer["generation"]:08d}.json'
        (newer_bucket / name).write_bytes(raw); (newer_bucket / (name + '.sig')).write_bytes(Signer().sign(raw, self.root / 'publisher.seed')[1])
        (newer_bucket / 'catalog' / 'current.json').write_bytes(encoded({'generation': newer['generation'], 'object': name, 'sha256': digest(raw)}))
        behind, current = self.serve(self.bucket), self.serve(newer_bucket)
        # A first mirror behind the next (though not behind the new cache) is not followed: the
        # mirror claiming the newest generation is tried first.
        cache = self.root / 'mirror-cache-behind'
        result, log = self.sync_listed(cache, behind, current)
        self.assertEqual(result['generation'], newer['generation'])
        self.assertNotIn('mirror_failed', log)
        result, log = self.sync_listed(cache, behind, current)
        self.assertEqual((result['generation'], result['fetched_bytes']), (newer['generation'], 0))
        self.assertNotIn('refused_older', result)
        # Only mirrors behind the cache: nothing newer, and the cache keeps its own.
        result, log = self.sync_listed(cache, behind)
        self.assertEqual((result['generation'], result['refused_older']), (newer['generation'], self.catalog['generation']))
        self.assertIn('catalog_refused_older', log)
        # One behind and one failing (serving nothing): the failure is reported (it may have been
        # newer), with the older generation beside it, not taken for nothing newer.
        empty = self.root / 'mirror-empty'; empty.mkdir()
        failed = subprocess.run([BINARY, 'sync', '--publication', 'test-pub', '--dir', str(cache)], env=self.listed(behind, self.serve(empty)),
                                capture_output=True, text=True, timeout=600)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn(f"another mirror's catalog generation {self.catalog['generation']} is older than generation {newer['generation']}", failed.stderr)

    def test_a_key_given_as_hex_is_never_a_file(self):
        # 64 hexadecimal characters are the key itself: a file of that name where the command
        # runs (another user's, in /tmp) is not read in its place.
        hex_key = self.key.read_bytes().hex()
        here = self.root / 'key-cwd'; here.mkdir()
        (here / hex_key).write_bytes(os.urandom(32))
        good = self.serve(self.bucket)
        for args, env in (([ '--source', str(self.bucket), '--publisher-key', hex_key], None), (['--publication', 'test-pub'], self.listed(good))):
            result = subprocess.run([BINARY, 'sync', *args, '--dir', str(self.root / f'key-cache-{args[0][2:]}')], cwd=here, env=env,
                                    capture_output=True, text=True, timeout=600)
            self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        # A key file so named is still read when given with a path.
        result = subprocess.run([BINARY, 'sync', '--source', str(self.bucket), '--publisher-key', f'./{hex_key}', '--dir', str(self.root / 'key-cache-file')],
                                cwd=here, capture_output=True, text=True, timeout=600)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('signature', result.stderr)

    def test_restore_reads_an_array_of_mirrors(self):
        # [publication] source as an array: a segment that differs on the first mirror is read
        # from the next; the restore equals one from the bucket alone.
        config, out = self.root / 'etc-mirrors' / 'xrpla.toml', self.root / 'restored-mirrors'
        config.parent.mkdir()
        config.write_text(f'profile = "restore"\n\n[publication]\nsource = [{json.dumps(str(self.tampered("mirror-tampered-dir")))}, {json.dumps(str(self.bucket))}]\n'
                          f'publisher_key = "{self.key}"\n\n[restore]\ntarget = "rocksdb"\nout = "{out}"\n')
        result = subprocess.run([BINARY, 'run', '--config', config], capture_output=True, text=True, timeout=1800)
        self.assertEqual(result.returncode, 0, result.stdout[-2000:] + result.stderr[-3000:])
        self.assertIn('"event":"mirror_failed"', result.stderr)
        self.assertEqual(json.loads(result.stdout)['last_hash'], self.restore('rocksdb', self.root / 'restored-mirrors-reference')['last_hash'])

    def test_detects_tampered_segments(self):
        import shutil
        tampered = self.root / 'tampered-bucket'; shutil.copytree(self.bucket, tampered)
        container = tampered / self.catalog['ranges'][0]['object']
        data = bytearray(container.read_bytes()); data[len(data) // 2] ^= 1; container.write_bytes(bytes(data))
        with self.assertRaisesRegex(RuntimeError, 'differs'):
            self.restore('rocksdb', self.root / 'restored-tampered', tampered)

    def test_rejects_a_different_publisher(self):
        other = self.root / 'other.pub'; other.write_bytes(os.urandom(32))
        with self.assertRaisesRegex(RuntimeError, 'signature'):
            run(BINARY, 'restore', '--source', self.bucket, '--publisher-key', other, '--to', 'store', '--out', self.root / 'rejected')


if __name__ == '__main__':
    unittest.main()
