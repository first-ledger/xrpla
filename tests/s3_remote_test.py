"""R2/S3 mirror: signed requests, multipart, ranges, conditional puts, push and remote sync.

Needs an S3-compatible endpoint (for example MinIO): XRPLA_TEST_S3_ENDPOINT,
XRPLA_TEST_S3_ACCESS_KEY_ID and XRPLA_TEST_S3_SECRET_ACCESS_KEY. The end-to-end
case also needs XRPLA_HISTORY_DECODER and XRPLA_SNAPSHOT_TOOL.
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import urllib.error
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import s3_remote  # noqa: E402
from s3_remote import S3, push  # noqa: E402

FIXTURE = ROOT / 'tests' / 'fixtures' / 'mainnet-transactions'
TIP_HASH = 'cea057a66cd8d4e51c9c12b971f9ea7772c47a7b15d573dcb1857458dd307503'
E = os.environ
CONFIGURED = all(E.get(k) for k in ('XRPLA_TEST_S3_ENDPOINT', 'XRPLA_TEST_S3_ACCESS_KEY_ID', 'XRPLA_TEST_S3_SECRET_ACCESS_KEY'))


class Retries(unittest.TestCase):
    """Reads survive a store or connection failing part way; writes are sent once."""
    class Response:
        status = 200
        def __init__(self, data, headers=None): self.data = data; self.headers = headers or {}
        def read(self):
            if isinstance(self.data, Exception): raise self.data
            return self.data

    def answers(self, *outcomes):
        """Patches urlopen to give `outcomes` in turn (an exception is raised, bytes answered);
        returns the methods it was called with."""
        calls, queue = [], list(outcomes)
        def urlopen(request, timeout):
            calls.append(request.get_method()); outcome = queue.pop(0)
            if isinstance(outcome, urllib.error.URLError): raise outcome
            return outcome if isinstance(outcome, self.Response) else self.Response(outcome)
        for patch in (mock.patch.object(s3_remote.urllib.request, 'urlopen', urlopen), mock.patch.object(s3_remote.time, 'sleep', lambda seconds: None)):
            patch.start(); self.addCleanup(patch.stop)
        return calls

    def http_error(self, code): return urllib.error.HTTPError('https://store/x', code, 'error', {}, None)

    def setUp(self): self.s3 = S3('https://store.example', 'bucket', 'key', 'secret')

    def test_a_read_cut_short_or_refused_is_tried_again(self):
        calls = self.answers(http.client.IncompleteRead(b'part', 5), self.http_error(502), urllib.error.URLError('reset'), b'whole')
        self.assertEqual(self.s3.get('bundles/x.xrpp', 0, 4), b'whole')
        self.assertEqual(calls, ['GET'] * 4)

    def test_reads_give_up_and_answers_are_not_retried(self):
        calls = self.answers(*[self.http_error(503)] * S3.READ_ATTEMPTS)
        with self.assertRaises(urllib.error.HTTPError): self.s3.get('x')
        self.assertEqual(len(calls), S3.READ_ATTEMPTS)
        calls = self.answers(self.http_error(404))
        self.assertFalse(self.s3.exists('missing'))
        self.assertEqual(calls, ['HEAD'])

    def test_writes_are_sent_once(self):
        calls = self.answers(self.http_error(503))
        with self.assertRaises(urllib.error.HTTPError): self.s3.put('catalog/current.json', b'{}', if_match='"etag"')
        self.assertEqual(calls, ['PUT'])

    def test_a_failed_part_is_sent_again(self):
        # A part sent again replaces the one before: one failing must not end the upload.
        s3 = S3('https://store.example', 'bucket', 'key', 'secret', part_size=4, jobs=1)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'big'; path.write_bytes(b'12345678')  # two parts
            started = b'<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><UploadId>u</UploadId></InitiateMultipartUploadResult>'
            calls = self.answers(started, self.http_error(503), self.Response(b'', {'ETag': '"a"'}), self.Response(b'', {'ETag': '"b"'}), b'')
            s3.put_file('bundles/big.xrpp', path)
        self.assertEqual(calls, ['POST', 'PUT', 'PUT', 'PUT', 'POST'])


class Uploads(unittest.TestCase):
    """Aborting interrupted multipart uploads walks every page of the listing, and stops on one
    that does not move on."""
    def page(self, keys, truncated, key_marker='', upload_marker=''):
        uploads = ''.join(f'<Upload><Key>{k}</Key><UploadId>id-{k}</UploadId></Upload>' for k in keys)
        return (f'<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{uploads}<IsTruncated>{truncated}</IsTruncated>'
                f'<NextKeyMarker>{key_marker}</NextKeyMarker><NextUploadIdMarker>{upload_marker}</NextUploadIdMarker></ListMultipartUploadsResult>').encode()

    def aborting(self, *pages):
        s3 = S3('https://store.example', 'bucket', 'key', 'secret'); listed, deleted = [], []
        def request(method, key='', query=None, **_):
            if method == 'DELETE': deleted.append(key); return 204, {}, b''
            listed.append(query); return 200, {}, pages[len(listed) - 1]
        s3.request = request
        return s3, listed, deleted

    def test_every_page_is_walked(self):
        s3, listed, deleted = self.aborting(self.page(['bundles/a'], 'true', 'bundles/a', 'id-bundles/a'), self.page(['pieces/b', 'other/c'], 'false'))
        self.assertEqual(s3.abort_incomplete_uploads(), 2)
        self.assertEqual(deleted, ['bundles/a', 'pieces/b'])
        self.assertEqual((listed[1]['key-marker'], listed[1]['upload-id-marker']), ('bundles/a', 'id-bundles/a'))

    def test_a_listing_that_does_not_advance_stops(self):
        same = self.page(['bundles/a'], 'true', 'bundles/a', 'id-bundles/a')
        s3, listed, _ = self.aborting(same, same, same)
        with self.assertRaisesRegex(ValueError, 'does not advance'): s3.abort_incomplete_uploads()
        self.assertEqual(len(listed), 2)
        s3, listed, _ = self.aborting(self.page([], 'true'))  # truncated, without markers
        with self.assertRaisesRegex(ValueError, 'does not advance'): s3.abort_incomplete_uploads()
        self.assertEqual(len(listed), 1)


@unittest.skipUnless(CONFIGURED, 'No S3-compatible test endpoint configured')
class Remote(unittest.TestCase):
    def setUp(self):
        self.name = 'xrpla-test-' + uuid.uuid4().hex[:12]
        self.s3 = S3(E['XRPLA_TEST_S3_ENDPOINT'], self.name, E['XRPLA_TEST_S3_ACCESS_KEY_ID'],
                     E['XRPLA_TEST_S3_SECRET_ACCESS_KEY'], E.get('XRPLA_TEST_S3_REGION', 'us-east-1'), part_size=5 * 1024 * 1024)
        self.s3.request('PUT')  # create the bucket
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)

    def tearDown(self):
        for key in self.s3.list(): self.s3.delete(key)
        self.s3.request('DELETE'); self.tmp.cleanup()

    def test_objects_multipart_ranges_and_conditions(self):
        self.s3.put('a/b.bin', b'hello world')
        self.assertEqual(self.s3.get('a/b.bin'), b'hello world')
        self.assertEqual(self.s3.get('a/b.bin', 6, 10), b'world')
        self.assertTrue(self.s3.exists('a/b.bin')); self.assertFalse(self.s3.exists('missing'))
        with self.assertRaises(urllib.error.HTTPError) as error: self.s3.put('a/b.bin', b'x', if_none_match=True)
        self.assertEqual(error.exception.code, 412)
        big = self.root / 'big.bin'; data = os.urandom(12 * 1024 * 1024 + 7); big.write_bytes(data)
        self.s3.put_file('bundles/big.xrpp', big)  # three parts
        self.assertEqual(self.s3.sha256('bundles/big.xrpp'), hashlib.sha256(data).hexdigest())
        self.assertEqual(self.s3.get('bundles/big.xrpp', 5 * 1024 * 1024 - 2, 5 * 1024 * 1024 + 1), data[5 * 1024 * 1024 - 2:5 * 1024 * 1024 + 2])
        self.assertEqual(sorted(self.s3.list('a/')), ['a/b.bin'])
        with self.assertRaises(ValueError): self.s3.download('a/b.bin', self.root / 'x', '00' * 32)
        # With its size known (as the catalog gives it), a download sends no HEAD.
        methods, request = [], self.s3.request
        self.s3.request = lambda method, *args, **kwargs: methods.append(method) or request(method, *args, **kwargs)
        sized = self.root / 'sized.bin'
        self.assertEqual(self.s3.download('bundles/big.xrpp', sized, hashlib.sha256(data).hexdigest(), len(data)), len(data))
        self.assertEqual(sized.read_bytes(), data)
        self.assertEqual(methods, ['GET'] * 3)
        self.s3.request = request
        # An interrupted multipart upload is aborted by the next push's cleanup.
        self.s3.request('POST', 'bundles/orphan.xrpp', {'uploads': ''})
        self.assertEqual(self.s3.abort_incomplete_uploads(), 1); self.assertEqual(self.s3.abort_incomplete_uploads(), 0)

    @unittest.skipUnless(E.get('XRPLA_HISTORY_DECODER') and E.get('XRPLA_SNAPSHOT_TOOL'), 'Compiled tools not supplied')
    def test_push_then_remote_sync_and_restore(self):
        bucket, seed = self.root / 'bucket', self.root / 'publisher.seed'; seed.write_bytes(os.urandom(32))
        publish = [sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', FIXTURE, '--source-key', FIXTURE / 'publisher.pub',
                   '--bucket', bucket, '--signing-key', seed, '--scratch', self.root / 'scratch', '--decoder', E['XRPLA_HISTORY_DECODER'],
                   '--snapshot-tool', E['XRPLA_SNAPSHOT_TOOL'], '--piece-ledgers', 2, '--bundle-pieces', 2, '--grace-seconds', 0]
        subprocess.run([str(x) for x in publish] + ['--until', '1000002'], check=True, capture_output=True)
        first = push(bucket, self.s3)
        subprocess.run([str(x) for x in publish], check=True, capture_output=True)  # rolls the pieces into a bundle
        second = push(bucket, self.s3)
        self.assertGreater(second['generation'], first['generation'])
        # Only the first piece was ever pushed (the second was bundled before a push), and generation
        # 1, which the pointer just left, names it: it stays through its grace period from the move.
        self.assertEqual(second['removed'], 0)
        pieces = self.s3.list('pieces/'); self.assertEqual(len(pieces), 1)
        self.assertIn(pieces[0], s3_remote.remotely_retired(bucket))
        self.assertEqual(push(bucket, self.s3)['uploaded'], 0)  # pushing the same generation again is a no-op
        # The pointer never moves backwards, and a concurrent pointer change fails the push.
        current = self.s3.get('catalog/current.json')
        older = json.loads(current); older['generation'] -= 1
        stale = self.root / 'stale'; (stale / 'catalog').mkdir(parents=True)
        (stale / 'catalog/current.json').write_text(json.dumps(older))
        with self.assertRaisesRegex(ValueError, 'newer than local'): push(stale, self.s3)
        forked = dict(json.loads(current), sha256='00' * 32); (stale / 'catalog/current.json').write_text(json.dumps(forked))
        with self.assertRaisesRegex(ValueError, 'differs from the local one'): push(stale, self.s3)
        self.s3.put('catalog/current.json', current)  # a new ETag: some other writer
        with self.assertRaises(urllib.error.HTTPError) as error: self.s3.put('catalog/current.json', current, if_match='"stale"')
        self.assertEqual(error.exception.code, 412)
        self.assertEqual(self.s3.get('catalog/current.json'), current)
        with mock.patch.object(s3_remote.time, 'time', return_value=time.time() + s3_remote.DEFAULT_GRACE + 60):
            self.assertEqual(push(bucket, self.s3)['removed'], 1)
        self.assertFalse([k for k in self.s3.list('pieces/')])
        # A prefixed publication shares the bucket without seeing the other one.
        other = S3(self.s3.endpoint, self.name, self.s3.access_key, self.s3.secret_key, self.s3.region, prefix='other')
        other.put('catalog/current.json', b'{}')
        self.assertEqual(other.list(), ['catalog/current.json']); self.assertIn('other/catalog/current.json', self.s3.list())
        other.delete('catalog/current.json')
        env = {**E, 'XRPLA_S3_ENDPOINT': E['XRPLA_TEST_S3_ENDPOINT'], 'XRPLA_S3_BUCKET': self.name, 'XRPLA_S3_REGION': self.s3.region,
               'XRPLA_S3_ACCESS_KEY_ID': E['XRPLA_TEST_S3_ACCESS_KEY_ID'], 'XRPLA_S3_SECRET_ACCESS_KEY': E['XRPLA_TEST_S3_SECRET_ACCESS_KEY']}
        client = [sys.executable, str(ROOT / 'tools' / 'piece_client.py')]
        synced = subprocess.run(client + ['sync', '--remote', '--publisher-key', str(bucket / 'publisher.pub'), '--cache', str(self.root / 'cache')],
                                check=True, capture_output=True, text=True, env=env)
        self.assertEqual(json.loads(synced.stdout)['last'], 1000004)
        restored = subprocess.run(client + ['restore', '--cache', str(self.root / 'cache'), '--out', str(self.root / 'restored'),
                                            '--scratch', str(self.root), '--decoder', E['XRPLA_HISTORY_DECODER']],
                                  check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(restored.stdout)['tip_hash'], TIP_HASH)


if __name__ == '__main__':
    unittest.main()
