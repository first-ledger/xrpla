"""Pushed publications: what a push records as verified remotely, which local copies a publisher
then deletes (only those the remote holds, and only those nothing local reads again), what a
push replacing another publication removes remotely (a prefix holding another publisher's key is
refused), how long the objects a pointer move leaves stay remotely (their grace period from the
move, whatever came before or after it), a remote pointer no push from the bucket recorded being
refused unless adopted, the bucket lock a push takes, and the follower pausing when the disk is
short of space."""
import argparse
import hashlib
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import time
import unittest
import urllib.error
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import live_publisher  # noqa: E402
import s3_remote  # noqa: E402
from s3_remote import DEFAULT_GRACE, PUSHED, REMOTE_RETIRED, push, reclaim, verified_remotely  # noqa: E402


class FakeS3:
    """An in-memory remote with what push and reclaim use."""
    def __init__(self): self.objects = {}; self.reads = []
    def request(self, method, key='', query=None, body=b'', headers=None, stream=False):
        assert method == 'GET' and key
        if key not in self.objects: raise urllib.error.HTTPError(key, 404, 'Not Found', {}, None)
        return 200, {'ETag': self.etag(key)}, self.objects[key]
    def etag(self, key): return '"' + hashlib.md5(self.objects[key]).hexdigest() + '"'
    def abort_incomplete_uploads(self): return 0
    def list(self, prefix=''): return [k for k in self.objects if k.startswith(prefix)]
    def exists(self, key): return key in self.objects
    def get(self, key): return self.objects[key]
    def put(self, key, data, if_none_match=False, if_match=None):
        if (if_none_match and key in self.objects) or (if_match and (key not in self.objects or if_match != self.etag(key))):
            raise urllib.error.HTTPError(key, 412, 'Precondition Failed', {}, None)
        self.objects[key] = bytes(data)
    def put_file(self, key, path): self.objects[key] = Path(path).read_bytes()
    def sha256(self, key): self.reads.append(key); return hashlib.sha256(self.objects[key]).hexdigest()
    def delete(self, key): del self.objects[key]


def sha(data): return hashlib.sha256(data).hexdigest()


def write(root, name, data):
    path = Path(root) / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data); return sha(data)


def make_bucket(root, generation=3):
    """A bucket whose catalog holds two snapshots (at 1,000 and 2,000), a bundle and a piece."""
    catalog = {'generation': generation, 'snapshots': [], 'ranges': []}
    write(root, 'publisher.pub', b'key')
    for seq in (1000, 2000):
        parts = [{'object': f'snapshots/{seq}.s/part-{i}', 'sha256': write(root, f'snapshots/{seq}.s/part-{i}', f'{seq} part {i}'.encode())} for i in range(2)]
        index = json.dumps({'parts': parts}).encode()
        catalog['snapshots'].append({'seq': seq, 'index': {'object': f'snapshots/{seq}.s/index.json', 'sha256': write(root, f'snapshots/{seq}.s/index.json', index)}})
    catalog['ranges'].append({'kind': 'bundle', 'first': 1001, 'last': 2000, 'object': 'bundles/1001-2000.b.xrpp',
                              'sha256': write(root, 'bundles/1001-2000.b.xrpp', b'bundle' * 100)})
    catalog['ranges'].append({'kind': 'piece', 'first': 2001, 'last': 2500, 'object': 'pieces/2001-2500.p.xrpp',
                              'sha256': write(root, 'pieces/2001-2500.p.xrpp', b'piece' * 50)})
    return generation_of(root, catalog)


def generation_of(root, catalog):
    """Write `catalog` as the bucket's current generation (signed with a stand-in signature)."""
    raw = json.dumps(catalog).encode(); name = f"catalog/{catalog['generation']:08d}.json"
    write(root, name, raw); write(root, name + '.sig', b'signature')
    write(root, 'catalog/current.json', json.dumps({'generation': catalog['generation'], 'object': name, 'sha256': sha(raw)}).encode())
    return catalog


class Push(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name) / 'bucket'; self.root.mkdir()
        self.catalog = make_bucket(self.root); self.s3 = FakeS3()

    def tearDown(self): self.tmp.cleanup()

    def quiet_push(self, **options):
        with redirect_stdout(io.StringIO()): return push(self.root, self.s3, **options)

    def test_replacing_a_publication_then_reclaiming_local_copies(self):
        # The remote holds an older publication of the same publisher on the prefix, which no push
        # from this bucket recorded: refused, then adopted. Its bundle and snapshot stay through
        # the grace period, then go; an object it shares a name with is replaced.
        for key in ('bundles/old.xrpp', 'snapshots/old.s/index.json', 'snapshots/old.s/part-0', 'snapshots/1000.s/part-0'):
            self.s3.objects[key] = b'old'
        self.s3.objects['publisher.pub'] = b'key'
        self.s3.objects['catalog/current.json'] = json.dumps({'generation': 1, 'object': 'catalog/00000001.json', 'sha256': '0'}).encode()
        before = dict(self.s3.objects)
        with self.assertRaisesRegex(ValueError, 'generation 1, which no push from this bucket recorded.*--adopt-remote'): self.quiet_push()
        self.assertEqual(self.s3.objects, before)
        result = self.quiet_push(adopt=True)
        self.assertEqual((result['uploaded'], result['verified'], result['removed'], result['adopted']), (8, 9, 0, 3))
        self.assertIn('bundles/old.xrpp', self.s3.objects)
        with mock.patch.object(s3_remote.time, 'time', return_value=time.time() + DEFAULT_GRACE + 60):
            self.assertEqual(self.quiet_push()['removed'], 3)
        self.assertNotIn('bundles/old.xrpp', self.s3.objects); self.assertNotIn('snapshots/old.s/part-0', self.s3.objects)
        self.assertEqual(self.s3.objects['snapshots/1000.s/part-0'], b'1000 part 0')
        self.assertEqual(json.loads(self.s3.objects['catalog/current.json'])['generation'], 3)
        # Every object read back intact is recorded, publisher.pub (kept: the same key) included.
        verified = verified_remotely(self.root)
        self.assertEqual(verified['bundles/1001-2000.b.xrpp'], self.catalog['ranges'][0]['sha256'])
        self.assertIn('snapshots/1000.s/part-1', verified); self.assertIn('publisher.pub', verified)

        bundle = ('bundles/1001-2000.b.xrpp', self.catalog['ranges'][0]['sha256'])
        part = ('snapshots/1000.s/part-0', verified['snapshots/1000.s/part-0'])
        files, size = reclaim(self.root, self.s3, [bundle, part])
        self.assertEqual((files, size), (2, 600 + len(b'1000 part 0')))
        self.assertFalse((self.root / bundle[0]).exists()); self.assertFalse((self.root / part[0]).exists())
        # A later push re-uploads and reads back nothing, and removes nothing the generation uses.
        self.s3.reads.clear()
        again = self.quiet_push()
        self.assertEqual((again['uploaded'], again['verified'], again['removed']), (0, 0, 0))
        self.assertEqual(self.s3.reads, []); self.assertIn(bundle[0], self.s3.objects)

    def test_a_remote_pointer_this_bucket_did_not_record_is_refused(self):
        # Generation 3 pushed, then the publisher moved to a bucket without its records (or with
        # stale ones), at generation 4: nothing is written or deleted.
        self.quiet_push(); pushed = (self.root / PUSHED).read_bytes()
        self.catalog['generation'] = 4; generation_of(self.root, self.catalog)
        for record in (None, pushed.splitlines()[0] + b'\n'):
            if record is None: (self.root / PUSHED).unlink()
            else: (self.root / PUSHED).write_bytes(record + json.dumps({'pointer': {'generation': 2, 'sha256': '0'}}).encode() + b'\n')
            before = dict(self.s3.objects)
            with self.assertRaisesRegex(ValueError, f'generation 3, which no push from this bucket recorded.*{REMOTE_RETIRED}'): self.quiet_push()
            self.assertEqual(self.s3.objects, before)
        # With the records copied over, it carries on.
        (self.root / PUSHED).write_bytes(pushed)
        self.assertEqual(self.quiet_push()['generation'], 4)
        # A bucket whose records predate the pointer's is taken at its word, and records it from then on.
        (self.root / PUSHED).write_bytes(b''.join(l + b'\n' for l in pushed.splitlines() if b'pointer' not in l))
        self.assertEqual(self.quiet_push()['generation'], 4)
        self.assertEqual(s3_remote.pushed_pointers(self.root), {(4, json.loads(self.s3.objects['catalog/current.json'])['sha256'])})
        # Not while a merge is under way: it records its copies in PUSHED as well, so objects
        # alone beside its record are no older push's.
        (self.root / PUSHED).write_bytes(b''.join(l + b'\n' for l in pushed.splitlines() if b'pointer' not in l))
        (self.root / s3_remote.MERGING).write_text(json.dumps({'objects': [], 'source': 'early/', 'started': 1}))
        with self.assertRaisesRegex(ValueError, 'generation 4, which no push from this bucket recorded'): self.quiet_push()

    def test_adopted_objects_are_kept_by_a_push_that_stopped_part_way(self):
        # Adopting, the push records the remote pointer as found by a push from here, then stops
        # while uploading: the next push, which no longer adopts, still keeps what it adopted.
        self.s3.objects.update({'publisher.pub': b'key', 'bundles/old.xrpp': b'old'})
        self.s3.objects['catalog/current.json'] = json.dumps({'generation': 1, 'object': 'catalog/00000001.json', 'sha256': '0'}).encode()
        put_file = self.s3.put_file
        def stopping(key, path): raise urllib.error.URLError('connection lost')
        self.s3.put_file = stopping
        with self.assertRaises(urllib.error.URLError): self.quiet_push(adopt=True)
        self.s3.put_file = put_file
        self.assertIn((1, '0'), s3_remote.pushed_pointers(self.root))
        self.assertIn('bundles/old.xrpp', s3_remote.remotely_retired(self.root))
        self.assertEqual(self.quiet_push()['removed'], 0); self.assertIn('bundles/old.xrpp', self.s3.objects)

    def test_pushed_is_compacted_to_what_is_read_from_it(self):
        self.quiet_push()
        for generation in range(4, 30):
            self.catalog['generation'] = generation; generation_of(self.root, self.catalog); self.quiet_push()
        held = verified_remotely(self.root)
        with open(self.root / PUSHED, 'a') as ledger:  # objects read back again, others since deleted remotely, a line cut short
            for _ in range(20): ledger.write(json.dumps({'object': 'publisher.pub', 'sha256': sha(b'key')}) + '\n')
            for i in range(200): ledger.write(json.dumps({'object': f'fragments/{i}.f.xrpp', 'sha256': sha(b'%d' % i)}) + '\n')
            ledger.write('{"obj')
        before = json.loads(self.s3.objects['catalog/current.json']); remote = set(self.s3.list())
        pointers = s3_remote.pushed_pointers(self.root)
        size = (self.root / PUSHED).stat().st_size
        s3_remote.compact_pushed(self.root, before, remote, limit=size)  # not past the limit: as it is
        self.assertEqual((self.root / PUSHED).stat().st_size, size)
        s3_remote.compact_pushed(self.root, before, remote, limit=0)
        compacted = (self.root / PUSHED).stat().st_size
        self.assertLess(2 * compacted, size)
        self.assertEqual(verified_remotely(self.root), held)  # the objects gone remotely are no longer recorded
        kept = s3_remote.pushed_pointers(self.root)
        self.assertEqual(len(kept), s3_remote.PUSHED_POINTERS_KEPT); self.assertLessEqual(kept, pointers)
        self.assertIn((before['generation'], before['sha256']), kept)
        # Not rewritten again until compacting would halve it.
        with open(self.root / PUSHED, 'a') as ledger:
            for _ in range(5): ledger.write(json.dumps({'object': 'publisher.pub', 'sha256': sha(b'key')}) + '\n')
        grown = (self.root / PUSHED).stat().st_size
        s3_remote.compact_pushed(self.root, before, remote, limit=0)
        self.assertEqual((self.root / PUSHED).stat().st_size, grown)
        self.catalog['generation'] = 30; generation_of(self.root, self.catalog)
        self.assertEqual(self.quiet_push()['generation'], 30)
        # A push compacts with what it listed; an object deleted remotely is uploaded again, and recorded once more.
        with open(self.root / PUSHED, 'a') as ledger:
            for i in range(200): ledger.write(json.dumps({'object': f'fragments/{i}.f.xrpp', 'sha256': sha(b'%d' % i)}) + '\n')
        del self.s3.objects['pieces/2001-2500.p.xrpp']
        with mock.patch.object(s3_remote.compact_pushed, '__defaults__', (0,)):
            self.catalog['generation'] = 31; generation_of(self.root, self.catalog)
            self.assertEqual(self.quiet_push()['uploaded'], 1)
        verified = verified_remotely(self.root)
        self.assertFalse(any(name.startswith('fragments/') for name in verified))
        self.assertEqual(verified['pieces/2001-2500.p.xrpp'], self.catalog['ranges'][1]['sha256'])
        self.assertIn((31, json.loads(self.s3.objects['catalog/current.json'])['sha256']), s3_remote.pushed_pointers(self.root))

    def test_an_object_without_a_retired_entry_gets_at_least_the_default_grace(self):
        # Generation 4 drops the piece without retiring it, and retires only a tip fragment, with
        # the short tip grace: the piece still stays the default grace period from the move.
        piece = self.stalled(); now = int(time.time())
        self.catalog['retired'] = [{'object': 'fragments/2501-2510.f.xrpp', 'sha256': '0', 'retired_at': now, 'delete_after': now + 600}]
        generation_of(self.root, self.catalog)
        self.assertEqual(self.quiet_push()['removed'], 0)
        self.assertGreaterEqual(s3_remote.remotely_retired(self.root)[piece], now + DEFAULT_GRACE)

    def test_another_publishers_prefix_is_refused(self):
        self.s3.objects['publisher.pub'] = b'another key'
        with self.assertRaisesRegex(ValueError, 'publisher key differs'): self.quiet_push()
        self.assertEqual(sorted(self.s3.objects), ['publisher.pub'])

    def test_retired_objects_stay_remotely_through_their_grace_period(self):
        self.quiet_push()
        # Generation 4 retires the piece (as when a bundle covers it), and its local copy goes early.
        piece = self.catalog['ranges'].pop(); now = int(time.time())
        self.catalog['retired'] = [{'object': piece['object'], 'sha256': piece['sha256'], 'retired_at': now, 'delete_after': now + 3600}]
        self.catalog['generation'] = 4; generation_of(self.root, self.catalog); (self.root / piece['object']).unlink()
        self.assertEqual(self.quiet_push()['removed'], 0)
        # Past the generation that named it, its grace period alone keeps it.
        self.catalog['generation'] = 5; generation_of(self.root, self.catalog)
        self.assertEqual(self.quiet_push()['removed'], 0); self.assertIn(piece['object'], self.s3.objects)
        # Once it has passed since the pointer left generation 3, the piece goes.
        self.catalog['generation'] = 6; generation_of(self.root, self.catalog)
        with mock.patch.object(s3_remote.time, 'time', return_value=now + 3602):
            self.assertEqual(self.quiet_push()['removed'], 1)
        self.assertNotIn(piece['object'], self.s3.objects)

    def stalled(self):
        """Generation 3 pushed; generation 4 drops the piece, retired so long ago that the catalog
        no longer lists it (pushes failed for longer than its grace period), and gone locally."""
        self.quiet_push()
        piece = self.catalog['ranges'].pop(); self.catalog['generation'] = 4; generation_of(self.root, self.catalog)
        (self.root / piece['object']).unlink()
        return piece['object']

    def test_what_a_pointer_move_leaves_stays_through_its_grace_period_from_the_move(self):
        piece = self.stalled(); now = time.time()
        self.assertEqual(self.quiet_push()['removed'], 0)
        recorded = json.loads((self.root / REMOTE_RETIRED).read_bytes())['objects']
        self.assertEqual([(e['object'], e['generation']) for e in recorded], [(piece, 3)])
        self.assertGreaterEqual(recorded[0]['delete_after'], int(now + DEFAULT_GRACE))
        # The publisher resumes: the next generation, minutes later, keeps it too.
        self.catalog['generation'] = 5; generation_of(self.root, self.catalog)
        self.assertEqual(self.quiet_push()['removed'], 0); self.assertIn(piece, self.s3.objects)
        with mock.patch.object(s3_remote.time, 'time', return_value=now + DEFAULT_GRACE + 60):
            self.assertEqual(self.quiet_push()['removed'], 1)
        self.assertNotIn(piece, self.s3.objects)

    def test_a_push_that_failed_after_moving_the_pointer_is_covered(self):
        # The pointer moves, then the push fails: the next push (a retry, or a restarted runner's)
        # finds the generation it pushes already current, and keeps what the move left all the same.
        piece = self.stalled(); put = self.s3.put
        def failing_after_the_pointer(key, data, **kwargs):
            put(key, data, **kwargs)
            if key == 'catalog/current.json': raise urllib.error.URLError('connection lost')
        self.s3.put = failing_after_the_pointer
        with self.assertRaises(urllib.error.URLError): self.quiet_push()
        self.s3.put = put
        self.assertEqual(json.loads(self.s3.objects['catalog/current.json'])['generation'], 4)
        self.assertEqual(self.quiet_push()['removed'], 0); self.assertIn(piece, self.s3.objects)

    def test_a_push_from_the_command_line_takes_the_bucket_lock(self):
        import fcntl
        command = ['s3_remote.py', 'push', '--bucket', str(self.root)]
        with mock.patch.object(sys, 'argv', command), mock.patch.object(s3_remote, 'push') as pushed, mock.patch.object(s3_remote.S3, 'from_environment'):
            with open(self.root / '.publisher.lock', 'w') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                with self.assertRaisesRegex(SystemExit, "holds this bucket's lock"): s3_remote.main()
                pushed.assert_not_called()
            s3_remote.main()
            pushed.assert_called_once()

    def test_an_object_the_remote_generation_names_is_not_replaced_by_other_bytes(self):
        self.quiet_push(); pointer = self.s3.objects['catalog/current.json']
        # Generation 4 names a snapshot's index and part (names that do not carry their hash) with
        # other bytes than generation 3, which readers may be fetching, does.
        snapshot = self.catalog['snapshots'][0]; part = 'snapshots/1000.s/part-0'
        parts = [{'object': part, 'sha256': write(self.root, part, b'other state')}, {'object': 'snapshots/1000.s/part-1', 'sha256': sha(b'1000 part 1')}]
        snapshot['index']['sha256'] = write(self.root, snapshot['index']['object'], json.dumps({'parts': parts}).encode())
        index = self.s3.objects[snapshot['index']['object']]
        self.catalog['generation'] = 4; generation_of(self.root, self.catalog)
        with self.assertRaisesRegex(ValueError, 'Remote generation 3 names .* with other bytes .* fresh prefix'): self.quiet_push()
        self.assertEqual((self.s3.objects[part], self.s3.objects[snapshot['index']['object']]), (b'1000 part 0', index))
        self.assertEqual(self.s3.objects['catalog/current.json'], pointer)

    def test_catalog_files_are_written_only_over_what_was_read(self):
        # A push that stopped left generation 3's catalog; another writer replaces it between this
        # push's read and its write: the write fails rather than replace what that one wrote.
        self.s3.objects['catalog/00000003.json'] = b'left by a push that stopped'
        read = self.s3.request
        def racing(method, key='', **kwargs):
            answer = read(method, key, **kwargs)
            if key == 'catalog/00000003.json': self.s3.objects[key] = b'written by another publisher'
            return answer
        self.s3.request = racing
        with self.assertRaisesRegex(ValueError, 'catalog/00000003.json changed during this push'): self.quiet_push()
        self.assertEqual(self.s3.objects['catalog/00000003.json'], b'written by another publisher')
        self.assertNotIn('catalog/current.json', self.s3.objects)
        # One absent when listed that appears before the write is not replaced either.
        self.s3.request = read; del self.s3.objects['catalog/00000003.json.sig']
        listing = self.s3.list
        def appearing(prefix=''):
            keys = listing(prefix); self.s3.objects['catalog/00000003.json.sig'] = b'another signature'; return keys
        self.s3.list = appearing
        with self.assertRaisesRegex(ValueError, 'catalog/00000003.json.sig changed during this push'): self.quiet_push()
        self.assertEqual(self.s3.objects['catalog/00000003.json.sig'], b'another signature')
        self.assertNotIn('catalog/current.json', self.s3.objects)
        # With the remote as this push left it, the next one replaces the stale files it reads.
        self.s3.list = listing; self.s3.objects['catalog/00000003.json'] = b'left by a push that stopped'
        self.quiet_push()
        self.assertEqual(self.s3.objects['catalog/00000003.json'], (self.root / 'catalog/00000003.json').read_bytes())
        self.assertEqual(self.s3.objects['catalog/00000003.json.sig'], b'signature')

    def test_an_object_read_without_an_etag_is_not_written_over(self):
        # A store that answers without an ETag: the pointer and catalog files cannot be written
        # only if unchanged, so the push is refused rather than writing them unconditionally.
        read = self.s3.request
        def without_etags(method, key='', **kwargs):
            status, headers, body = read(method, key, **kwargs)
            return status, {k: v for k, v in headers.items() if k.lower() != 'etag'}, body
        self.s3.objects['catalog/00000003.json'] = b'left by a push that stopped'
        self.s3.request = without_etags
        with self.assertRaisesRegex(ValueError, 'catalog/00000003.json came back without an ETag'): self.quiet_push()
        self.assertEqual(self.s3.objects['catalog/00000003.json'], b'left by a push that stopped')
        self.assertNotIn('catalog/current.json', self.s3.objects)
        self.s3.request = read; self.quiet_push(); pointer = self.s3.objects['catalog/current.json']
        # Generation 4: the pointer it would move came back without an ETag. Nothing is written.
        self.catalog['generation'] = 4; generation_of(self.root, self.catalog)
        self.s3.request = without_etags
        with self.assertRaisesRegex(ValueError, 'catalog/current.json came back without an ETag'): self.quiet_push()
        self.assertEqual(self.s3.objects['catalog/current.json'], pointer)
        self.assertNotIn('catalog/00000004.json', self.s3.objects)
        # The generation the pointer already names is pushed again without writing it.
        self.catalog['generation'] = 3; generation_of(self.root, self.catalog)
        self.assertEqual(self.quiet_push()['generation'], 3)

    def test_only_copies_the_remote_holds_are_deleted(self):
        self.quiet_push()
        piece = ('pieces/2001-2500.p.xrpp', self.catalog['ranges'][1]['sha256'])
        part = ('snapshots/2000.s/part-0', sha(b'2000 part 0'))
        other = ('snapshots/2000.s/part-1', sha(b'2000 part 1'))
        # Only the piece stays recorded, and its remote copy has gone since; the snapshot parts are
        # unrecorded (read back before deleting), one of them differing remotely.
        (self.root / PUSHED).write_text(json.dumps({'object': piece[0], 'sha256': piece[1]}) + '\n')
        del self.s3.objects[piece[0]]
        self.s3.objects[other[0]] = b'something else'
        self.s3.reads.clear()
        files, _ = reclaim(self.root, self.s3, [piece, part, other])
        self.assertEqual(files, 1)
        self.assertTrue((self.root / piece[0]).exists()); self.assertTrue((self.root / other[0]).exists())
        self.assertFalse((self.root / part[0]).exists())
        self.assertEqual(self.s3.reads, [part[0], other[0]])
        self.assertEqual(verified_remotely(self.root), {piece[0]: piece[1], part[0]: part[1]})  # the part is now recorded


class Runner(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); base = Path(self.tmp.name)
        self.bucket = base / 'bucket'; self.bucket.mkdir(); self.catalog = make_bucket(self.bucket)
        self.work = base / 'work'; spool = self.work / 'spools' / 'spool-1001'; spool.mkdir(parents=True)
        (spool / 'manifest.json').write_text('{}'); (self.work / 'current').write_text('spool-1001')
        (self.work / 'spool.pub').write_bytes(b'k' * 32)
        args = argparse.Namespace(workdir=self.work, spool_seed=base / 'seed', bucket=self.bucket, signing_key=base / 'key',
                                  piece_ledgers=500, bundle_pieces=2, push=True, keep_pushed=False, min_free_gb=50.0, once=True,
                                  publish_seconds=60, start=None)
        with mock.patch('signal.signal'): self.runner = live_publisher.Runner(args)
        self.events = []; self.runner.log = lambda **event: self.events.append(event)

    def tearDown(self): self.tmp.cleanup()

    def test_bundles_and_snapshots_before_the_rotation_boundary_are_reclaimed(self):
        # The current spool starts at 1,001, so its next rotation needs the snapshot at 2,000.
        manifest = {'chunks': [{'first': 1001}]}
        with mock.patch.object(live_publisher, 'load_catalog', return_value=self.catalog), \
             mock.patch.object(live_publisher, 'source_manifest', return_value=manifest), \
             mock.patch.object(live_publisher, 'S3'), mock.patch.object(live_publisher, 'reclaim', return_value=(3, 30)) as reclaimed, \
             mock.patch.object(live_publisher, 'Signer'):
            self.runner.reclaim_pushed()
        names = [name for name, _ in reclaimed.call_args[0][2]]
        self.assertEqual(sorted(names), ['bundles/1001-2000.b.xrpp', 'snapshots/1000.s/part-0', 'snapshots/1000.s/part-1'])
        self.assertEqual(self.events, [{'event': 'reclaimed_local', 'files': 3, 'bytes': 30}])

    def test_a_push_waits_while_another_holds_the_bucket_lock(self):
        # A merge (or a publish run) holds the bucket's lock: neither push nor reclaim runs, and
        # both hold it themselves when they do.
        import fcntl
        def holding_the_lock(*_):
            with open(self.bucket / '.publisher.lock', 'w') as other:
                with self.assertRaises(BlockingIOError): fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return {'generation': 3}
        with mock.patch.object(live_publisher, 'push', side_effect=holding_the_lock) as pushed, mock.patch.object(live_publisher, 'S3'), \
             mock.patch.object(self.runner, 'reclaim_pushed') as reclaimed:
            with open(self.bucket / '.publisher.lock', 'w') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                self.runner.mirror(3)
                pushed.assert_not_called(); reclaimed.assert_not_called()
            self.runner.mirror(3)
            pushed.assert_called_once(); reclaimed.assert_called_once()
        self.assertEqual([e['event'] for e in self.events], ['push_deferred', 'pushed'])
        self.assertEqual(self.runner.pushed, 3)

    def test_the_follower_reads_the_node_or_only_peers(self):
        a = self.runner.args
        common = dict(xrpla='xrpla', batch_ledgers=256, poll_seconds=600, merge_chunks=True, until=None, validator_list=None)
        node = dict(node='http://node:51233', peers=None, rpc_jobs=2, grpc_source='grpc://node:50051', grpc_jobs=10, history_peers=None,
                    history_identity=None, delay_ms=0, fallback='wss://api.example.com')
        peers = dict(node=None, peers='default', peer_identity=self.work / 'node.key', network_id=0, fallback=None)
        commands = []
        for mode in (node, peers):
            vars(a).update(common, **mode)
            with mock.patch.object(live_publisher.subprocess, 'Popen') as popen, mock.patch('builtins.open'):
                self.runner.follow(self.work / 'spools' / 'spool-1001')
            commands.append(popen.call_args[0][0])
        self.assertIn('--grpc-source', commands[0]); self.assertNotIn('--peers', commands[0])
        self.assertEqual(commands[0][commands[0].index('--fallback') + 1], 'wss://api.example.com')
        self.assertEqual(commands[1][commands[1].index('--peers') + 1], 'default')
        self.assertEqual(commands[1][commands[1].index('--network-id') + 1], '0')
        # From peers, nothing reaches a node: each ledger is trusted on the network's validators.
        for flag in ('--source', '--jobs', '--grpc-source', '--grpc-jobs', '--history-peers', '--delay-ms', '--validations-node', '--fallback'):
            self.assertNotIn(flag, commands[1])
        # A new spool cannot start from peers.
        (self.work / 'current').unlink(); vars(a).update(start=5)
        with self.assertRaises(SystemExit): self.runner.run()

    def test_a_cut_interrupted_after_its_commit_point_is_completed(self):
        # The new spool is in place (renamed from its staging directory), but a crash came before
        # the follower's state moved and the current pointer changed: a restart completes the cut.
        old = self.work / 'spools' / 'spool-1001'; new = self.work / 'spools' / 'spool-2001'
        (old / '.verification-nodes').mkdir(); (old / '.verification-nodes' / 'CURRENT').write_bytes(b'nodes')
        (old / 'interval-snapshots').mkdir()
        new.mkdir(); (new / 'manifest.json').write_text('{}'); (new / live_publisher.ROTATED_FROM).write_text('spool-1001')
        (new / 'interval-snapshots').mkdir()  # moved before the crash
        self.assertEqual(self.runner.resume_rotation(), new)
        self.assertEqual((self.work / 'current').read_text(), 'spool-2001')
        self.assertEqual((new / '.verification-nodes' / 'CURRENT').read_bytes(), b'nodes')
        self.assertFalse((old / '.verification-nodes').exists())
        self.assertTrue((old / 'retired').exists())
        self.assertEqual(self.events, [{'event': 'rotation_completed', 'spool': 'spool-2001'}])
        # Nothing more to complete: the current spool is followed as it is.
        self.assertEqual(self.runner.resume_rotation(), new)

    def test_a_spool_retired_while_still_current_is_kept_and_the_cut_completed(self):
        # A crash between retiring the old spool and making the new one current: the old spool is
        # still current, so it is not purged, and the restart completes the cut (retired before
        # current, the replaced spool is never left unretired, which nothing would remove).
        old = self.work / 'spools' / 'spool-1001'; new = self.work / 'spools' / 'spool-2001'
        (old / 'retired').write_text('1')
        new.mkdir(); (new / 'manifest.json').write_text('{}'); (new / live_publisher.ROTATED_FROM).write_text('spool-1001')
        self.runner.purge_retired(grace=0)
        self.assertTrue(old.exists())
        self.assertEqual(self.runner.resume_rotation(), new)
        self.assertEqual((self.work / 'current').read_text(), 'spool-2001')
        self.runner.purge_retired(grace=0)
        self.assertFalse(old.exists())

    def test_a_short_disk_pauses_the_follower_until_space_returns(self):
        free = {'value': 10 * 2**30}
        usage = lambda _: mock.Mock(free=free['value'])
        follower = mock.Mock(); follower.poll.return_value = None
        self.runner.follower = follower
        with mock.patch.object(live_publisher.shutil, 'disk_usage', usage), mock.patch.object(self.runner, 'follow') as follow, \
             mock.patch.object(self.runner, 'publish', return_value=None):
            self.runner.run()  # --once: one pass of the loop
            follower.send_signal.assert_called()  # halted
            follow.assert_not_called()
            self.assertTrue(self.runner.status['disk_low'])
            free['value'] = 55 * 2**30  # above the floor, but not yet a quarter above it
            self.assertTrue(self.runner.disk_low())
            free['value'] = 70 * 2**30
            self.assertFalse(self.runner.disk_low())
        self.assertEqual([e['event'] for e in self.events], ['disk_low', 'disk_recovered'])


if __name__ == '__main__':
    unittest.main()
