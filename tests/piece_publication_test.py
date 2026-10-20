"""Piece publisher and client: layout, rollover, reclamation, queries, restore and tampering.

Needs XRPLA_HISTORY_DECODER and XRPLA_SNAPSHOT_TOOL. With XRPLA_BINARY, the
restored archive is also fetched and fully verified by `xrpla`.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
FIXTURE = ROOT / 'tests' / 'fixtures' / 'mainnet-transactions'
TIP_HASH = 'cea057a66cd8d4e51c9c12b971f9ea7772c47a7b15d573dcb1857458dd307503'
DECODER, SNAPSHOT = os.environ.get('XRPLA_HISTORY_DECODER'), os.environ.get('XRPLA_SNAPSHOT_TOOL')
ENCODER = os.environ.get('XRPLA_HISTORY_ENCODER')


def run(*args, ok=True):
    result = subprocess.run([sys.executable, *map(str, args)], capture_output=True, text=True, timeout=600)
    if (result.returncode == 0) != ok: raise AssertionError(result.stdout + result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')], result.stderr


class BucketFiles(unittest.TestCase):
    def test_a_file_from_another_file_system_is_copied_and_synced(self):
        # Scratch and bucket on different file systems: the object is copied under a temporary
        # name, synced, then renamed, so its final name never holds a partial copy.
        import errno
        import piece_publisher as pp
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root / 'scratch' / 'piece.xrpp'; source.parent.mkdir(); source.write_bytes(b'piece bytes')
            bucket = pp.Bucket(root / 'bucket'); replace, fsync = os.replace, os.fsync; renamed, synced = [], []
            def across(a, b):
                if Path(a) == source: raise OSError(errno.EXDEV, 'Invalid cross-device link')
                renamed.append((Path(a).name, Path(b).name)); return replace(a, b)
            with mock.patch.object(pp.os, 'replace', across), mock.patch.object(pp.os, 'fsync', lambda fd: synced.append(fd) or fsync(fd)):
                bucket.put_file('pieces/1-2.p.xrpp', source)
            self.assertEqual(renamed, [('1-2.p.xrpp.part', '1-2.p.xrpp')])
            self.assertEqual(len(synced), 2)  # the copy, then its directory
            self.assertEqual(bucket.get('pieces/1-2.p.xrpp'), b'piece bytes')
            self.assertEqual(sorted(p.name for p in (root / 'bucket' / 'pieces').iterdir()), ['1-2.p.xrpp'])
            self.assertFalse(source.exists())
            # On one file system it is renamed.
            source.write_bytes(b'other piece'); bucket.put_file('pieces/3-4.p.xrpp', source)
            self.assertEqual(bucket.get('pieces/3-4.p.xrpp'), b'other piece'); self.assertFalse(source.exists())

    def test_a_run_clears_the_temporary_copies_a_crash_left_in_the_bucket(self):
        import argparse
        import piece_publisher as pp
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); bucket = root / 'bucket'
            kept = ['pieces/1-2.p.xrpp', 'snapshots/2.s/index.json', 'catalog/current.json', 'notes.part']
            stale = ['pieces/3-4.p.xrpp.part', 'snapshots/2.s/part-0.xrps.part', 'catalog/current.json.part']
            for name in kept + stale: (bucket / name).parent.mkdir(parents=True, exist_ok=True); (bucket / name).write_bytes(b'x')
            seen = []
            with mock.patch.object(pp, 'run_once', lambda args: seen.append(sorted(str(p.relative_to(bucket)).replace(os.sep, '/')
                                                                                     for p in bucket.rglob('*') if p.is_file()))):
                pp.publish(argparse.Namespace(bucket=bucket, scratch=root / 'scratch'))
            # Only the objects' temporary copies under the bucket's object directories go, before the run.
            self.assertEqual(seen, [sorted(kept + ['.publisher.lock'])])


@unittest.skipUnless(DECODER and SNAPSHOT, 'Compiled decoder and snapshot tool supplied by CTest')
class PiecePublication(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        self.seed = self.root / 'publisher.seed'; self.seed.write_bytes(os.urandom(32))
        self.bucket = self.root / 'bucket'

    def tearDown(self): self.tmp.cleanup()

    def publish(self, *extra, ok=True):
        return run(ROOT / 'tools' / 'piece_publisher.py', '--source', FIXTURE, '--source-key', FIXTURE / 'publisher.pub',
                   '--bucket', self.bucket, '--signing-key', self.seed, '--scratch', self.root / 'scratch', '--decoder', DECODER,
                   '--snapshot-tool', SNAPSHOT, '--piece-ledgers', 2, '--bundle-pieces', 2, '--workers', 2,
                   '--grace-seconds', 0, *extra, ok=ok)

    def client(self, *args, ok=True):
        return run(ROOT / 'tools' / 'piece_client.py', *args, ok=ok)

    def catalog(self):
        pointer = json.loads((self.bucket / 'catalog/current.json').read_bytes())
        return json.loads((self.bucket / pointer['object']).read_bytes())

    def test_publish_rollover_query_restore(self):
        # Ledger 1,000,000 alone completes the first interval; the next piece is the active interval.
        out, _ = self.publish('--until', 1000002)
        catalog = self.catalog()
        self.assertEqual([(r['kind'], r['first'], r['last']) for r in catalog['ranges']],
                         [('bundle', 1000000, 1000000), ('piece', 1000001, 1000002)])
        self.assertEqual(sorted(s['seq'] for s in catalog['snapshots']), [999999, 1000000])
        piece = catalog['ranges'][1]['object']
        key = self.bucket / 'publisher.pub'
        cache = self.root / 'cache'
        synced, _ = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', cache, '--from', 1000001)
        self.assertEqual((synced[0]['snapshot'], synced[0]['first'], synced[0]['last']), (1000000, 1000001, 1000002))
        ledger, _ = self.client('query', '--cache', cache, '--ledger', 1000002)
        self.assertEqual(ledger[0]['ledger_hash'], catalog['ranges'][1]['last_hash'])
        # Keep this generation's pointer and signed catalog, to replay them once a newer one is out.
        pointer = self.bucket / 'catalog/current.json'; old_object = json.loads(pointer.read_bytes())['object']
        replay = {name: (self.bucket / name).read_bytes() for name in ('catalog/current.json', old_object, old_object + '.sig')}

        # The last piece completes the second interval: its pieces become one bundle and are reclaimed.
        out, _ = self.publish()
        self.assertEqual(out[-1]['bundles'], 1); self.assertEqual(out[-1]['reclaimed'], 2)
        catalog = self.catalog()
        self.assertEqual([(r['kind'], r['first'], r['last']) for r in catalog['ranges']],
                         [('bundle', 1000000, 1000000), ('bundle', 1000001, 1000004)])
        self.assertEqual(catalog['tip'], {'seq': 1000004, 'ledger_hash': TIP_HASH})
        self.assertFalse((self.bucket / piece).exists())
        again, _ = self.publish()
        self.assertEqual(again[-1]['generation'], catalog['generation'])  # nothing new to publish

        # The cache follows the newer generation; a mirror replaying the older one (validly signed
        # as it once was) is refused, and the cache keeps its selection.
        synced, _ = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', cache, '--from', 1000001)
        self.assertEqual((synced[0]['generation'], synced[0]['last']), (catalog['generation'], 1000004))
        current = pointer.read_bytes()
        for name, data in replay.items(): (self.bucket / name).write_bytes(data)
        refused, error = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', cache, '--from', 1000001)
        self.assertLess(refused[0]['refused_older'], catalog['generation'])
        self.assertEqual((refused[0]['generation'], refused[0]['fetched_bytes']), (catalog['generation'], 0))
        self.assertIn('catalog_refused_older', error)
        self.assertEqual(json.loads((cache / 'selection.json').read_bytes())['catalog']['generation'], catalog['generation'])
        pointer.write_bytes(current)

        full = self.root / 'full'
        synced, _ = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', full)
        self.assertEqual((synced[0]['first'], synced[0]['last']), (1000000, 1000004))
        tx = self.first_transaction_hash()
        found, _ = self.client('query', '--cache', full, '--tx', tx)
        self.assertEqual(hashlib.sha512(b'TXN\0' + bytes.fromhex(found[0]['tx_blob'])).digest()[:32].hex(), tx)
        missing, _ = self.client('query', '--cache', full, '--tx', '00' * 32)
        self.assertFalse(missing[0]['found'])

        restored = self.root / 'restored'
        result, _ = self.client('restore', '--cache', full, '--out', restored, '--scratch', self.root, '--decoder', DECODER)
        self.assertEqual((result[0]['first'], result[0]['last'], result[0]['tip_hash']), (1000000, 1000004, TIP_HASH))
        if os.environ.get('XRPLA_BINARY'):
            xrpla, data = os.environ['XRPLA_BINARY'], self.root / 'data'
            for command in (['fetch', '--archive', restored, '--range', '1000000-1000004'], ['verify', '--full']):
                subprocess.run([xrpla, *map(str, command), '--dir', data, '--publisher-key', restored / 'publisher.pub'],
                               check=True, capture_output=True, timeout=600)
        # The restored archive is signed with a new key, so a restore checks every cached file
        # whole first: a snapshot part or a range changed since the sync is refused, not signed.
        selection = json.loads((full / 'selection.json').read_bytes())
        index = json.loads((full / selection['snapshot']['index']['object']).read_bytes())
        for i, name in enumerate((index['parts'][0]['object'], selection['ranges'][-1]['object'])):
            path = full / name; original = path.read_bytes()
            data = bytearray(original); data[len(data) // 2] ^= 1; path.write_bytes(bytes(data))
            _, error = self.client('restore', '--cache', full, '--out', self.root / f'tampered-{i}', '--scratch', self.root, '--decoder', DECODER, ok=False)
            self.assertIn('differs', error, name)
            path.write_bytes(original)

    def test_fragments_publish_the_filling_piece_and_join_into_it(self):
        # Pieces of 2 ledgers, fragments of 1: ledger 1,000,003 is published as the tail before its
        # piece completes, and readers follow it; the completed piece is joined from its fragments.
        self.publish('--until', 1000003, '--fragment-ledgers', 1)
        catalog = self.catalog()
        self.assertEqual([(r['kind'], r['first'], r['last']) for r in catalog['ranges']],
                         [('bundle', 1000000, 1000000), ('piece', 1000001, 1000002)])
        tail = catalog['tail']
        self.assertEqual((tail['fragment_ledgers'], [(f['kind'], f['first'], f['last']) for f in tail['ranges']]), (1, [('fragment', 1000003, 1000003)]))
        self.assertEqual((catalog['tip']['seq'], tail['tip']['seq']), (1000002, 1000003))  # ranges alone end at the newest piece
        fragment, fragment_hash = tail['ranges'][0]['object'], tail['ranges'][0]['last_hash']
        self.assertTrue(fragment.startswith('fragments/') and (self.bucket / fragment).exists())
        key = self.bucket / 'publisher.pub'
        cache = self.root / 'cache'
        synced, _ = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', cache, '--from', 1000001)
        self.assertEqual((synced[0]['first'], synced[0]['last']), (1000001, 1000003))
        ledger, _ = self.client('query', '--cache', cache, '--ledger', 1000003)
        self.assertEqual(ledger[0]['ledger_hash'], fragment_hash)
        if os.environ.get('XRPLA_BINARY'):
            native = self.root / 'native'
            run = subprocess.run([os.environ['XRPLA_BINARY'], 'sync', '--source', str(self.bucket), '--publisher-key', str(key), '--dir', str(native)],
                                 capture_output=True, text=True, timeout=600)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertEqual(json.loads((native / 'selection.json').read_text())['ranges'][-1]['last'], 1000003)

        # The source completes the piece and its interval: the piece is joined from its two
        # fragments (one published, one encoded now), bundled, and the tail retires.
        out, _ = self.publish('--fragment-ledgers', 1)
        self.assertEqual([l.get('fragments') for l in out if l.get('piece') == [1000003, 1000004]], [2])
        catalog = self.catalog()
        self.assertNotIn('tail', catalog)
        self.assertEqual([(r['kind'], r['first'], r['last']) for r in catalog['ranges']],
                         [('bundle', 1000000, 1000000), ('bundle', 1000001, 1000004)])
        self.assertEqual(catalog['tip'], {'seq': 1000004, 'ledger_hash': TIP_HASH})
        self.assertFalse((self.bucket / fragment).exists())  # retired, and past its grace period
        self.assertEqual(list((self.bucket / 'fragments').glob('*')), [])
        full = self.root / 'full'
        synced, _ = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', full)
        self.assertEqual((synced[0]['first'], synced[0]['last']), (1000000, 1000004))
        for seq, expected in ((1000003, fragment_hash), (1000004, TIP_HASH)):  # from the joined piece, now in the bundle
            ledger, _ = self.client('query', '--cache', full, '--ledger', seq)
            self.assertEqual(ledger[0]['ledger_hash'], expected)
        tx = self.first_transaction_hash()
        found, _ = self.client('query', '--cache', full, '--tx', tx)
        self.assertEqual(hashlib.sha512(b'TXN\0' + bytes.fromhex(found[0]['tx_blob'])).digest()[:32].hex(), tx)

    def test_small_fragments_end_the_tail_and_retire_unjoined(self):
        # Pieces of 4 ledgers, fragments of 2, small fragments of 1: ledger 1,000,003 is a small
        # fragment after the fragment 1,000,001-1,000,002, readers follow both, and when the piece
        # completes it is joined from its two fragments (the second encoded whole) while the small
        # one retires.
        layout = ('--piece-ledgers', 4, '--fragment-ledgers', 2, '--tip-fragment-ledgers', 1, '--tip-grace-seconds', 0)
        self.publish('--until', 1000003, *layout)
        tail = self.catalog()['tail']
        self.assertEqual([(f['first'], f['last']) for f in tail['ranges']], [(1000001, 1000002), (1000003, 1000003)])
        self.assertEqual(tail['tip']['seq'], 1000003)
        small, small_hash = tail['ranges'][1]['object'], tail['ranges'][1]['last_hash']
        key = self.bucket / 'publisher.pub'
        synced, _ = self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', self.root / 'cache', '--from', 1000001)
        self.assertEqual((synced[0]['first'], synced[0]['last']), (1000001, 1000003))
        ledger, _ = self.client('query', '--cache', self.root / 'cache', '--ledger', 1000003)
        self.assertEqual(ledger[0]['ledger_hash'], small_hash)
        out, _ = self.publish(*layout)
        self.assertEqual([l.get('fragments') for l in out if l.get('piece') == [1000001, 1000004]], [2])
        catalog = self.catalog()
        self.assertNotIn('tail', catalog)
        self.assertEqual(catalog['tip'], {'seq': 1000004, 'ledger_hash': TIP_HASH})
        self.assertFalse((self.bucket / small).exists())  # retired, past its (small fragment's) grace period
        full = self.root / 'full'
        self.client('sync', '--bucket', self.bucket, '--publisher-key', key, '--cache', full)
        for seq, expected in ((1000003, small_hash), (1000004, TIP_HASH)):
            ledger, _ = self.client('query', '--cache', full, '--ledger', seq)
            self.assertEqual(ledger[0]['ledger_hash'], expected)
        _, error = self.publish('--fragment-ledgers', 2, '--tip-fragment-ledgers', 2, ok=False)
        self.assertIn('smaller divisor', error)

    @unittest.skipUnless(ENCODER, 'Compiled encoder supplied by CTest')
    def test_segments_encoded_together_give_the_same_piece(self):
        import piece_publisher as pp
        pieces = []
        with mock.patch.object(pp, 'SEGMENT_LEDGERS', 1):  # one ledger a segment: five segments
            for jobs in (1, 4):
                scratch = self.root / f'scratch-{jobs}'; scratch.mkdir()
                result = pp.encode_piece((str(FIXTURE), str(FIXTURE / 'publisher.pub'), 1000000, 1000004, DECODER,
                                          str(scratch), ENCODER, 4, jobs))
                pieces.append((result.pop('staged'), result))
        self.assertEqual(pieces[0][1]['segments'], 5)
        self.assertEqual(pieces[0][1], pieces[1][1])
        self.assertEqual(Path(pieces[0][0]).read_bytes(), Path(pieces[1][0]).read_bytes())

    def test_one_publisher_per_bucket(self):
        import fcntl
        self.bucket.mkdir(parents=True)
        with open(self.bucket / '.publisher.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            _, error = self.publish(ok=False)
            self.assertIn('Another publisher is running', error)
        (self.root / 'scratch').mkdir(); (self.root / 'scratch' / 'piece-1-2.xrpp').write_bytes(b'stale')
        self.publish()
        self.assertFalse((self.root / 'scratch' / 'piece-1-2.xrpp').exists())

    def test_a_failed_run_keeps_the_pieces_it_finished(self):
        # The snapshot tool fails once every piece is encoded: the pieces stay in the bucket,
        # recorded, and the next run takes each as it is once checked against itself and the source.
        failing = self.root / 'failing-snapshot'; failing.write_text('#!/bin/sh\nexit 3\n'); failing.chmod(0o755)
        self.publish('--until', 1000002, '--snapshot-tool', failing, ok=False)
        scratch = self.root / 'scratch'
        self.assertFalse((self.bucket / 'catalog/current.json').exists())
        self.assertEqual(sorted(p.name for p in scratch.glob('kept-*.json')), ['kept-1000000-1000000.json', 'kept-1000001-1000002.json'])
        # A record that no longer matches its object is dropped with the object, and encoded again.
        record = scratch / 'kept-1000001-1000002.json'
        changed = json.loads(record.read_bytes()); changed['transactions'] += 1; record.write_bytes(json.dumps(changed).encode())
        out, error = self.publish('--until', 1000002)
        self.assertIn('kept_range_discarded', error)
        self.assertEqual({tuple(line['piece']): line.get('kept', False) for line in out if 'piece' in line},
                         {(1000000, 1000000): True, (1000001, 1000002): False})
        self.assertEqual(list(scratch.glob('kept-*.json')), [])
        catalog = self.catalog()
        self.assertEqual([(r['kind'], r['first'], r['last']) for r in catalog['ranges']],
                         [('bundle', 1000000, 1000000), ('piece', 1000001, 1000002)])
        self.assertEqual(catalog['ranges'][1]['transactions'], changed['transactions'] - 1)

    def test_a_run_drops_only_the_kept_records_of_ranges_it_took(self):
        # A run stopped after encoding every piece; the next one publishes less (--until): the
        # record of the piece past it stays, and the run that reaches it takes it as it is.
        failing = self.root / 'failing-snapshot'; failing.write_text('#!/bin/sh\nexit 3\n'); failing.chmod(0o755)
        self.publish('--snapshot-tool', failing, ok=False)
        scratch = self.root / 'scratch'
        self.assertEqual(sorted(p.name for p in scratch.glob('kept-*.json')),
                         ['kept-1000000-1000000.json', 'kept-1000001-1000002.json', 'kept-1000003-1000004.json'])
        self.publish('--until', 1000002)
        self.assertEqual([p.name for p in scratch.glob('kept-*.json')], ['kept-1000003-1000004.json'])
        out, _ = self.publish()
        self.assertEqual({tuple(line['piece']): line.get('kept', False) for line in out if 'piece' in line}, {(1000003, 1000004): True})
        self.assertEqual(list(scratch.glob('kept-*.json')), [])
        self.assertEqual(self.catalog()['tip'], {'seq': 1000004, 'ledger_hash': TIP_HASH})

    @unittest.skipUnless(os.environ.get('XRPLA_TX_INDEX'), 'xrpla_tx_index supplied')
    def test_the_index_tool_builds_the_same_bundles(self):
        # The compiled index runs in a directory of the run's own, which it leaves nothing in.
        self.publish()
        python = self.catalog(); self.bucket = self.root / 'bucket-tool'
        self.publish('--index-tool', os.environ['XRPLA_TX_INDEX'])
        self.assertEqual([r['sha256'] for r in self.catalog()['ranges']], [r['sha256'] for r in python['ranges']])
        self.assertEqual([p.name for p in (self.root / 'scratch').iterdir()], [])

    def test_tampering_and_wrong_keys_are_rejected(self):
        self.publish()
        catalog = self.catalog(); target = self.bucket / catalog['ranges'][-1]['object']
        other = self.root / 'other.pub'; other.write_bytes(os.urandom(32))
        self.client('sync', '--bucket', self.bucket, '--publisher-key', other, '--cache', self.root / 'c1', ok=False)
        data = bytearray(target.read_bytes()); data[len(data) // 2] ^= 1; target.write_bytes(bytes(data))
        _, error = self.client('sync', '--bucket', self.bucket, '--publisher-key', self.bucket / 'publisher.pub', '--cache', self.root / 'c2', ok=False)
        self.assertIn('differs', error)
        # A different signing key cannot extend the bucket.
        self.seed.write_bytes(os.urandom(32))
        self.publish(ok=False)

    @unittest.skipUnless(os.environ.get('XRPLA_BINARY'), 'xrpla binary supplied by CTest')
    def test_a_spool_extends_from_the_publication_and_its_tail(self):
        # A node behind the network catches up from the publication (its tail too) before peers:
        # a restore to an existing spool appends what the publication holds past its tip.
        xrpla, key = os.environ['XRPLA_BINARY'], self.bucket / 'publisher.pub'
        spool, spool_seed = self.root / 'spool', self.root / 'spool.seed'
        spool_seed.write_bytes(os.urandom(32))
        def restore():
            run = subprocess.run([xrpla, 'restore', '--source', str(self.bucket), '--publisher-key', str(key), '--to', 'spool',
                                  '--out', str(spool), '--signing-key', str(spool_seed)], capture_output=True, text=True, timeout=600)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            tip = json.loads((spool / 'manifest.json').read_text())['tip']
            return json.loads(run.stdout), tip['seq'], tip['ledger_hash']
        self.publish('--until', 1000002)
        _, seq, _ = restore()
        self.assertEqual(seq, 1000002)
        self.publish('--until', 1000003, '--fragment-ledgers', 1)  # ledger 1,000,003 only as the tail
        result, seq, ledger_hash = restore()
        self.assertEqual((result['extended'], seq, ledger_hash), (1, 1000003, self.catalog()['tail']['ranges'][0]['last_hash']))
        self.publish('--fragment-ledgers', 1)
        result, seq, ledger_hash = restore()
        self.assertEqual((result['extended'], seq, ledger_hash), (1, 1000004, TIP_HASH))
        result, seq, _ = restore()  # nothing newer
        self.assertEqual((result['extended'], seq), (0, 1000004))

    def test_spool_rotation_cuts_at_a_published_boundary(self):
        from live_publisher import cut_spool
        from piece_publisher import Bucket, source_manifest
        self.publish()
        catalog = self.catalog()
        snapshot = next(s for s in catalog['snapshots'] if s['seq'] == 1000000)
        spool_seed = self.root / 'spool.seed'; spool_seed.write_bytes(os.urandom(32))
        new = self.root / 'spools' / 'spool-1000001'
        # The fixture's single chunk straddles the boundary, so the cut splits it.
        cut_spool(FIXTURE, FIXTURE / 'publisher.pub', new, 1000000, snapshot, Bucket(self.bucket), spool_seed)
        manifest = source_manifest(new, new / 'publisher.pub')
        self.assertEqual((manifest['chunks'][0]['first'], manifest['tip']['seq'], manifest['snapshots'][0]['seq']), (1000001, 1000004, 1000000))
        rebuilt = subprocess.run([SNAPSHOT, '--archive', new, '--publisher-key', new / 'publisher.pub', '--at', '1000004',
                                  '--out', self.root / 'check', '--scratch', self.root / 'check-scratch'], check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(rebuilt.stdout)['ledger_hash'], TIP_HASH)
        # The publisher accepts the rotated spool as a continuation of the same catalog.
        out, _ = run(ROOT / 'tools' / 'piece_publisher.py', '--source', new, '--source-key', new / 'publisher.pub',
                     '--bucket', self.bucket, '--signing-key', self.seed, '--scratch', self.root / 'scratch', '--decoder', DECODER,
                     '--snapshot-tool', SNAPSHOT, '--piece-ledgers', 2, '--bundle-pieces', 2, '--workers', 2, '--grace-seconds', 0)
        self.assertEqual(out[-1]['generation'], catalog['generation'])

    def test_spool_rotation_carries_the_verified_state(self):
        # The new spool has the old one's tip, so the follower's node store and the interval
        # snapshots it wrote move with it (the follower resumes instead of rebuilding). The
        # snapshot's copies are staged first, as while the follower still runs.
        import shutil
        from live_publisher import ROTATED_FROM, cut_spool, stage_spool
        from piece_publisher import Bucket
        self.publish()
        snapshot = next(s for s in self.catalog()['snapshots'] if s['seq'] == 1000000)
        old = self.root / 'spools' / 'spool-old'; shutil.copytree(FIXTURE, old)
        (old / '.verification-nodes').mkdir(); (old / '.verification-nodes' / 'CURRENT').write_bytes(b'nodes')
        (old / '.verification-nodes.compacted').write_bytes(b'1000002')
        (old / 'interval-snapshots').mkdir(); (old / 'interval-snapshots' / '1000002.json').write_bytes(b'{}')
        spool_seed = self.root / 'spool.seed'; spool_seed.write_bytes(os.urandom(32))
        new = self.root / 'spools' / 'spool-1000001'
        staged = stage_spool(old, old / 'publisher.pub', new, 1000000, snapshot, Bucket(self.bucket))
        self.assertIn(snapshot['index']['object'], staged)
        self.assertFalse(new.exists())
        cut_spool(old, old / 'publisher.pub', new, 1000000, snapshot, Bucket(self.bucket), spool_seed, staged)
        self.assertEqual((new / ROTATED_FROM).read_text(), 'spool-old')
        rebuilt = subprocess.run([SNAPSHOT, '--archive', new, '--publisher-key', new / 'publisher.pub', '--at', '1000004',
                                  '--out', self.root / 'check', '--scratch', self.root / 'check-scratch'], check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(rebuilt.stdout)['ledger_hash'], TIP_HASH)
        for name in ('.verification-nodes', '.verification-nodes.compacted', 'interval-snapshots'):
            self.assertTrue((new / name).exists(), name)
            self.assertFalse((old / name).exists(), name)
        self.assertEqual((new / '.verification-nodes' / 'CURRENT').read_bytes(), b'nodes')

    def first_transaction_hash(self):
        from compression_probe import Zstd
        from piece_publisher import ledger_rows, source_manifest
        import history_codec as hc
        manifest = source_manifest(FIXTURE, FIXTURE / 'publisher.pub')
        for seq, prefix, patches in ledger_rows(FIXTURE, manifest, 1000000, 1000004, Zstd()):
            r = hc.R(prefix); r.u32(); r.record()
            if r.u32(): return hashlib.sha512(b'TXN\0' + r.record()).digest()[:32].hex()
        raise AssertionError('Fixture has no transactions')


if __name__ == '__main__':
    unittest.main()
