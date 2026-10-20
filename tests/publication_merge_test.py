"""Joining an earlier publication onto the front of a later one (tools/publication_merge.py):
what is copied between prefixes and verified, the joined catalog and that the later publisher
carries on from it, a merge run again after stopping part way or abandoned, and what is refused."""
import copy
import hashlib
import io
import json
import os
import shutil
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest
import urllib.error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from fast_archive import Signer  # noqa: E402
from piece_publisher import Bucket, load_catalog, sign_catalog  # noqa: E402
from publication_merge import abandon, merge, remote_catalog  # noqa: E402
from s3_remote import push, verified_remotely  # noqa: E402

LAYOUT = {'piece_ledgers': 10, 'bundle_pieces': 2}
CODEC = {'format': 'xrph-v2', 'zstd_level': 19, 'hash_recipes': 10, 'index': 'xtxi-v1'}


class FakeS3:
    """One prefix of an in-memory store, with what push and merge use."""
    def __init__(self, objects, prefix): self.objects = objects; self.prefix = prefix; self.copied = []; self.damage = None; self.fail = None
    def request(self, method, key='', query=None, body=b'', headers=None, stream=False):
        assert method == 'GET' and key
        if self.prefix + key not in self.objects: raise urllib.error.HTTPError(key, 404, 'Not Found', {}, None)
        data = self.objects[self.prefix + key]
        return 200, {'ETag': '"' + hashlib.md5(data).hexdigest() + '"'}, data
    def abort_incomplete_uploads(self): return 0
    def list(self, prefix=''): return [k[len(self.prefix):] for k in self.objects if k.startswith(self.prefix + prefix)]
    def exists(self, key): return self.prefix + key in self.objects
    def get(self, key): return self.objects[self.prefix + key]
    def put(self, key, data, if_none_match=False, if_match=None):
        held = self.objects.get(self.prefix + key)
        if (if_none_match and held is not None) or (if_match and (held is None or if_match != '"' + hashlib.md5(held).hexdigest() + '"')):
            raise urllib.error.HTTPError(key, 412, 'Precondition Failed', {}, None)
        self.objects[self.prefix + key] = bytes(data)
    def put_file(self, key, path): self.objects[self.prefix + key] = Path(path).read_bytes()
    def sha256(self, key): return hashlib.sha256(self.objects[self.prefix + key]).hexdigest()
    def delete(self, key): del self.objects[self.prefix + key]
    def copy_from(self, source_prefix, key, size):
        if key == self.fail: raise OSError('connection lost')
        data = self.objects[source_prefix + key]; assert len(data) == size
        self.objects[self.prefix + key] = data[:-1] + b'!' if key == self.damage else data
        self.copied.append(key)


def sha(data): return hashlib.sha256(data).hexdigest()
def ledger(seq): return sha(b'ledger %d' % seq)


def write(root, name, data):
    path = Path(root) / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data); return sha(data)


def snapshot(root, seq, salt=b''):
    prefix = f'snapshots/{seq}.{ledger(seq)[:16]}'; parts = []
    for i in range(2):
        data = b'state %d part %d ' % (seq, i) + salt
        parts.append({'object': f'{prefix}/part-{i}.xrps', 'records': 1, 'sha256': write(root, f'{prefix}/part-{i}.xrps', data), 'size': len(data)})
    index = json.dumps({'seq': seq, 'parts': parts}).encode()
    return {'seq': seq, 'ledger_hash': ledger(seq), 'account_hash': sha(b'state %d' % seq), 'prefix': prefix, 'parts': 2, 'objects': 2,
            'total_size': sum(p['size'] for p in parts), 'index': {'object': prefix + '/index.json', 'sha256': write(root, prefix + '/index.json', index), 'size': len(index)}}


def span(root, kind, first, last):
    data = b'%s %d-%d' % (kind.encode(), first, last) * 20; digest = sha(data); name = f'{kind}s/{first}-{last}.{digest[:16]}.xrpp'
    write(root, name, data)
    return {'kind': kind, 'first': first, 'last': last, 'object': name, 'sha256': digest, 'size': len(data), 'directory_sha256': sha(b'd' + data),
            'parent_hash': ledger(first - 1), 'last_hash': ledger(last), 'transactions': last - first + 1, 'segments': 1}


def publication(root, seed, first, snapshots, ranges, generation):
    """A signed local bucket: snapshots at `snapshots`, and `ranges` as (kind, first, last)."""
    catalog = {'format': 'xrpla-catalog-v1', 'generation': generation, 'network_id': 0, 'layout': LAYOUT, 'codec': CODEC, 'first': first,
               'snapshots': [snapshot(root, s, salt=str(root).encode()) for s in snapshots], 'ranges': [span(root, *r) for r in ranges], 'retired': [],
               'published_at': '2026-01-01T00:00:00Z'}
    catalog['tip'] = {'seq': catalog['ranges'][-1]['last'], 'ledger_hash': catalog['ranges'][-1]['last_hash']}
    sign_catalog(Bucket(root), catalog, seed)
    return catalog


def quiet(function, *args, **kwargs):
    with redirect_stdout(io.StringIO()): return function(*args, **kwargs)


class Merge(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        self.seed = self.root / 'publisher.seed'; self.seed.write_bytes(os.urandom(32))
        self.key = Signer().sign(b'', self.seed)[0]
        self.objects = {}; self.early = FakeS3(self.objects, 'early/'); self.main = FakeS3(self.objects, 'main/')
        self.bucket = self.root / 'main'

    def tearDown(self): self.tmp.cleanup()

    def publish(self, early_ranges=(('bundle', 11, 20), ('bundle', 21, 40)), early_snapshots=(10, 20, 40)):
        """The earlier publication (11-40, pushed under early/) and the later one (41-70, under main/), afresh."""
        self.objects.clear()
        for name in ('early', 'main'): shutil.rmtree(self.root / name, ignore_errors=True)
        self.earlier = publication(self.root / 'early', self.seed, 11, early_snapshots, early_ranges, 7)
        self.later = publication(self.bucket, self.seed, 41, (40, 60), (('bundle', 41, 60), ('piece', 61, 70)), 3)
        quiet(push, self.root / 'early', self.early); quiet(push, self.bucket, self.main)

    def test_the_plan_writes_nothing(self):
        self.publish(); before = dict(self.objects); files = sorted(p for p in self.bucket.rglob('*') if p.is_file() and p.name != '.publisher.lock')
        report = quiet(merge, self.bucket, self.main, self.early, self.seed)
        # Two bundles; the snapshots at 10 and 20, each an index and two parts. The snapshot at 40 is this publication's own.
        self.assertEqual((report['objects'], report['to_copy'], report['first'], report['tip'], report['generation']), (8, 8, 11, 70, 4))
        self.assertFalse(report['applied'])
        self.assertEqual(self.objects, before)
        self.assertEqual(sorted(p for p in self.bucket.rglob('*') if p.is_file() and p.name != '.publisher.lock'), files)

    def test_joined_publication_and_the_publisher_carrying_on(self):
        self.publish(); own = self.main.get(self.later['snapshots'][0]['index']['object'])
        report = quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual((report['copied'], report['generation'], report['pushed']['uploaded']), (8, 4, 0))
        catalog = remote_catalog(self.main, self.key)
        self.assertEqual((catalog['generation'], catalog['first'], catalog['tip']['seq']), (4, 11, 70))
        self.assertEqual([(r['first'], r['last']) for r in catalog['ranges']], [(11, 20), (21, 40), (41, 60), (61, 70)])
        self.assertEqual([s['seq'] for s in catalog['snapshots']], [10, 20, 40, 60])
        self.assertEqual(self.main.get(catalog['snapshots'][2]['index']['object']), own)  # the boundary snapshot is untouched
        # Every object the catalog names is under this prefix with the catalog's hash, and recorded as verified.
        verified = verified_remotely(self.bucket)
        for r in catalog['ranges']: self.assertEqual(self.main.sha256(r['object']), r['sha256'])
        for s in catalog['snapshots']:
            for p in json.loads(self.main.get(s['index']['object']))['parts']:
                self.assertEqual(self.main.sha256(p['object']), p['sha256']); self.assertEqual(verified[p['object']], p['sha256'])
        # Locally: the joined catalog and the gained snapshot indexes, not the earlier bundles or parts.
        self.assertEqual(load_catalog(Bucket(self.bucket), self.key), catalog)
        self.assertTrue((self.bucket / catalog['snapshots'][0]['index']['object']).exists())
        self.assertFalse((self.bucket / catalog['ranges'][0]['object']).exists())
        # The earlier prefix is as it was.
        self.assertEqual(remote_catalog(self.early, self.key), self.earlier)
        # Run again: nothing to do.
        self.main.copied.clear()
        again = quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual((again['to_copy'], again['copied'], again['generation'], self.main.copied), (0, 0, 4, []))
        # The publisher's next generation keeps the earlier objects.
        following = load_catalog(Bucket(self.bucket), self.key); following['ranges'].append(span(self.bucket, 'piece', 71, 80))
        following.update(generation=5, tip={'seq': 80, 'ledger_hash': ledger(80)}); sign_catalog(Bucket(self.bucket), following, self.seed)
        pushed = quiet(push, self.bucket, self.main)
        self.assertEqual((pushed['uploaded'], pushed['removed']), (1, 0))
        self.assertTrue(self.main.exists(catalog['ranges'][0]['object']))

    def test_a_merge_that_stopped_is_run_again(self):
        self.publish(); pointer = self.main.get('catalog/current.json')
        self.main.fail = self.earlier['ranges'][1]['object']
        with self.assertRaises(OSError): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual(self.main.get('catalog/current.json'), pointer)  # readers still see the publication as it was
        # Nothing was signed: a push by the publisher, were it running, pushes its own catalog,
        # whose objects it holds, and readers still see the publication as it was. The copies
        # already made are named by no catalog and held nowhere locally, but the merge's record
        # names them: the push keeps them.
        copies = list(self.main.copied); self.assertTrue(copies)
        record = json.loads((self.bucket / '.merging').read_bytes())
        self.assertTrue(set(copies) <= set(record['objects'])); self.assertEqual(record['source'], 'early/')
        pushed = quiet(push, self.bucket, self.main)
        self.assertEqual(self.main.get('catalog/current.json'), pointer)
        self.assertEqual(pushed['removed'], 0)
        # Each push reports the unfinished merge that it keeps the copies of.
        self.assertEqual((pushed['merging']['source'], pushed['merging']['objects']), ('early/', len(record['objects'])))
        self.assertGreaterEqual(pushed['merging']['age_seconds'], 0)
        for name in copies: self.assertTrue(self.main.exists(name), name)
        self.assertTrue((self.bucket / '.merging').exists())  # the pushed catalog does not name them yet
        done = len(self.main.copied); self.main.fail = None
        report = quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual((report['generation'], len(self.main.copied)), (4, report['copied'] + done))
        self.assertEqual(report['copied'], report['objects'] - done)  # what survived the push is not copied again
        self.assertEqual(remote_catalog(self.main, self.key)['first'], 11)
        self.assertFalse((self.bucket / '.merging').exists())

    def test_a_push_drops_the_record_once_its_catalog_names_the_copies(self):
        # A merge that signed the joined catalog but whose own push failed: the publisher's next
        # push carries it, and the record goes with it.
        self.publish(); real = self.main.request
        def failing(method, key='', **kwargs):  # once the merge has checked the pointer and begun
            if key == 'catalog/current.json' and (self.bucket / '.merging').exists(): raise urllib.error.URLError('connection lost')
            return real(method, key, **kwargs)
        self.main.request = failing
        with self.assertRaises(urllib.error.URLError): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.main.request = real
        self.assertTrue((self.bucket / '.merging').exists())
        self.assertEqual(load_catalog(Bucket(self.bucket), self.key)['first'], 11)  # signed locally
        with self.assertRaisesRegex(SystemExit, 'names every copy'): quiet(abandon, self.bucket, self.seed)  # done but for its push
        pushed = quiet(push, self.bucket, self.main)
        self.assertEqual((pushed['generation'], pushed['removed']), (4, 0))
        self.assertFalse((self.bucket / '.merging').exists())

    def test_a_finished_merge_clears_a_record_an_earlier_attempt_widened(self):
        # An earlier attempt recorded a copy this one does not use; this one's push moves the
        # pointer and then fails. The record names this run's copies alone, so the next push,
        # whose catalog names them all, drops it.
        self.publish()
        (self.bucket / '.merging').write_text(json.dumps({'objects': ['bundles/1-10.earlier.xrpp'], 'source': 'early/', 'started': 1}))
        put = self.main.put
        def failing_after_the_pointer(key, data, **kwargs):
            put(key, data, **kwargs)
            if key == 'catalog/current.json': raise urllib.error.URLError('connection lost')
        self.main.put = failing_after_the_pointer
        with self.assertRaises(urllib.error.URLError): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.main.put = put
        self.assertEqual(remote_catalog(self.main, self.key)['first'], 11)
        record = json.loads((self.bucket / '.merging').read_bytes())
        self.assertNotIn('bundles/1-10.earlier.xrpp', record['objects']); self.assertEqual(record['started'], 1)
        quiet(push, self.bucket, self.main)
        self.assertFalse((self.bucket / '.merging').exists())

    def test_an_abandoned_merge_leaves_its_copies_to_the_pushes(self):
        self.publish(); pointer = self.main.get('catalog/current.json')
        self.main.fail = self.earlier['ranges'][1]['object']
        with self.assertRaises(OSError): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        copies = list(self.main.copied); self.assertTrue(copies)
        indexes = [s['index']['object'] for s in self.earlier['snapshots'] if s['seq'] in (10, 20)]
        for name in indexes: self.assertTrue((self.bucket / name).exists(), name)  # written locally by the merge
        report = quiet(abandon, self.bucket, self.seed)
        self.assertEqual((report['abandoned'], report['source'], report['local_deleted']), (8, 'early/', 2))
        self.assertFalse((self.bucket / '.merging').exists())
        for name in indexes: self.assertFalse((self.bucket / name).exists(), name)
        # The next push deletes the copies, neither named nor held here, and reports no merge.
        pushed = quiet(push, self.bucket, self.main)
        self.assertEqual(pushed['removed'], len(copies)); self.assertNotIn('merging', pushed)
        for name in copies: self.assertFalse(self.main.exists(name), name)
        self.assertEqual(self.main.get('catalog/current.json'), pointer)
        with self.assertRaisesRegex(SystemExit, 'No merge'): quiet(abandon, self.bucket, self.seed)

    def test_a_copy_that_differs_stops_the_merge(self):
        self.publish(); pointer = self.main.get('catalog/current.json')
        self.main.damage = self.earlier['ranges'][0]['object']
        with self.assertRaisesRegex(ValueError, 'Copy differs'): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual(self.main.get('catalog/current.json'), pointer)
        self.assertNotIn(self.main.damage, verified_remotely(self.bucket))
        self.main.damage = None  # run again: the damaged copy is read back, found to differ and replaced
        quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual(self.main.sha256(self.earlier['ranges'][0]['object']), self.earlier['ranges'][0]['sha256'])

    def test_a_generation_left_before_the_pointer_moved_is_signed_again(self):
        # A publisher stopped after writing generation 4's files, before moving the pointer (and
        # a push that stopped there too): the next run signs generation 4 again, with other
        # times, and both the bucket and the remote take it.
        self.publish(); bucket = Bucket(self.bucket)
        catalog = load_catalog(bucket, self.key); catalog.update(generation=4, published_at='2026-01-02T00:00:00Z')
        for name in ('catalog/00000004.json', 'catalog/00000004.json.sig'):
            write(self.bucket, name, b'left by a run that stopped'); self.main.put(name, b'left by a push that stopped')
        catalog['published_at'] = '2026-01-03T00:00:00Z'; sign_catalog(bucket, catalog, self.seed)
        self.assertEqual(load_catalog(bucket, self.key), catalog)
        quiet(push, self.bucket, self.main)
        self.assertEqual(remote_catalog(self.main, self.key), catalog)
        # A generation the pointer has reached stays as it is.
        catalog['published_at'] = '2026-01-04T00:00:00Z'
        with self.assertRaisesRegex(ValueError, 'Immutable object differs'): sign_catalog(bucket, catalog, self.seed)

    def test_a_remote_pointer_the_bucket_did_not_record_is_refused_before_anything_is_written(self):
        # The bucket's records are lost: the merge's own records of its copies must not make it
        # look like a bucket whose records predate the pointer records.
        self.publish(); pushed = (self.bucket / '.pushed').read_bytes(); (self.bucket / '.pushed').unlink()
        before = dict(self.objects)
        with self.assertRaisesRegex(ValueError, 'which no push from this bucket recorded'):
            quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual(self.objects, before)
        self.assertFalse((self.bucket / '.pushed').exists()); self.assertFalse((self.bucket / '.merging').exists())
        # Records that predate the pointer records are taken at their word, and the pointer recorded.
        (self.bucket / '.pushed').write_bytes(b''.join(l + b'\n' for l in pushed.splitlines() if b'pointer' not in l))
        report = quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
        self.assertEqual(report['generation'], 4)
        self.assertEqual(remote_catalog(self.main, self.key)['first'], 11)

    def test_refusals(self):
        def refused(message, change=None, **published):
            self.publish(**published)
            if change:
                catalog = copy.deepcopy(self.earlier); change(catalog); catalog['generation'] += 1
                sign_catalog(Bucket(self.root / 'early'), catalog, self.seed); quiet(push, self.root / 'early', self.early)
            before = dict(self.objects)
            with self.assertRaisesRegex(ValueError, message): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)
            self.assertEqual(self.objects, before)
        refused('ends at 20; this one starts at 41', early_ranges=(('bundle', 11, 20),), early_snapshots=(10, 20))
        refused('still has pieces', early_ranges=(('bundle', 11, 20), ('piece', 21, 30), ('piece', 31, 40)))
        refused('not a chain', lambda c: c['ranges'][-1].update(last_hash='00' * 32))
        refused('differ in layout', lambda c: c.update(layout={'piece_ledgers': 5, 'bundle_pieces': 4}))
        refused('different ledgers', lambda c: c['snapshots'][-1].update(ledger_hash='11' * 32))
        # A catalog another key signed is not this publisher's.
        self.publish(); other = self.root / 'other.seed'; other.write_bytes(os.urandom(32))
        forged = copy.deepcopy(self.earlier); forged['generation'] += 1
        sign_catalog(Bucket(self.root / 'forged'), forged, other)
        for name in ('catalog/current.json', f'catalog/{forged["generation"]:08d}.json', f'catalog/{forged["generation"]:08d}.json.sig'):
            self.early.put(name, (self.root / 'forged' / name).read_bytes())
        with self.assertRaisesRegex(ValueError, 'signature differs'): quiet(merge, self.bucket, self.main, self.early, self.seed, apply=True)


if __name__ == '__main__':
    unittest.main()
