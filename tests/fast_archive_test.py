"""tools/fast_archive.py's shared helpers: signing, safe paths, checked reads, durable writes,
the ledger-header check and manifest authentication."""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import fast_archive as fast  # noqa: E402


def ledger_result(sequence, transactions=None, diff=None):
    header = sequence.to_bytes(4, 'big') + bytes(8) + bytes(range(32)) + bytes(118 - 44)
    ledger = {'ledger_data': header.hex()}
    if transactions is not None: ledger['transactions'] = transactions
    if diff is not None: ledger['diff'] = diff
    return {'validated': True, 'ledger_index': sequence, 'ledger': ledger,
            'ledger_hash': hashlib.sha512(b'LWR\0' + header).digest()[:32].hex().upper()}


class Helpers(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)

    def seed(self, name='publisher.seed', hex_text=False):
        path = self.root / name
        value = os.urandom(32)
        path.write_bytes(value.hex().encode() + b'\n' if hex_text else value)
        return path

    def test_digest_encoded_stamp(self):
        self.assertEqual(fast.digest(b''), hashlib.sha256(b'').hexdigest())
        self.assertEqual(fast.encoded({'a': 1, 'b': [2, 3]}), b'{"a":1,"b":[2,3]}')
        self.assertRegex(fast.stamp(), re.compile(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$'))

    def test_durable_replaces_without_leftovers(self):
        path = self.root / 'nested' / 'file.json'
        fast.durable(path, b'one'); fast.durable(path, b'two')
        self.assertEqual(path.read_bytes(), b'two')
        self.assertEqual(sorted(p.name for p in path.parent.iterdir()), ['file.json'])

    def test_signer_round_trip_and_rejections(self):
        signer = fast.Signer()
        for seed in (self.seed(), self.seed('hex.seed', hex_text=True)):
            key, signature = signer.sign(b'manifest', seed)
            self.assertEqual((len(key), len(signature)), (32, 64))
            signer.verify(b'manifest', signature, key)
            with self.assertRaises(ValueError): signer.verify(b'manifesT', signature, key)
            other, _ = signer.sign(b'manifest', self.seed('other.seed'))
            with self.assertRaises(ValueError): signer.verify(b'manifest', signature, other)
        bad = self.root / 'short.seed'; bad.write_bytes(b'00')
        with self.assertRaises(ValueError): signer.sign(b'manifest', bad)

    def test_safe_and_checked(self):
        (self.root / 'objects').mkdir(); data = b'chunk bytes'
        (self.root / 'objects' / 'a').write_bytes(data)
        item = {'object': 'objects/a', 'size': len(data), 'sha256': fast.digest(data)}
        self.assertEqual(fast.checked(self.root, item), data)
        for name in ('../escape', 'objects\\a', '.', 7):
            with self.assertRaises(ValueError): fast.safe(self.root, name)
        with self.assertRaises(ValueError): fast.checked(self.root, dict(item, size=len(data) + 1))
        with self.assertRaises(ValueError): fast.checked(self.root, dict(item, sha256=fast.digest(b'x')))

    def test_record_limits(self):
        self.assertEqual(fast.record(b'abc'), b'\x03\0\0\0abc')
        with self.assertRaises(ValueError): fast.record(b'')

    def test_check_header(self):
        result = ledger_result(70000, [{'tx_blob': 'AA', 'meta': 'BB'}],
                               [{'object_id': '11' * 32, 'object': 'CC'}])
        digest, parent = fast.check_header(result, 70000)
        self.assertEqual(digest, result['ledger_hash'].lower())
        self.assertEqual(parent, bytes(range(32)).hex())
        self.assertEqual(fast.check_header(result, 70000, expected=digest.upper())[0], digest)
        bare = ledger_result(70000)
        self.assertEqual(fast.check_header(bare, 70000, complete=False)[0], bare['ledger_hash'].lower())
        rejected = [
            (dict(result, validated=False), {}),
            (result, {'sequence': 70001}),
            (result, {'expected': '00' * 32}),
            (dict(result, ledger_hash='00' * 32), {}),
            (bare, {}),
            (ledger_result(70000, [], [{'object_id': '11' * 32, 'object': ''}] * 2), {}),
            (ledger_result(70000, [], [{'object_id': '11' * 31, 'object': ''}]), {}),
            (ledger_result(70000, [{'tx_blob': '', 'meta': 'BB'}], []), {}),
        ]
        for value, options in rejected:
            with self.assertRaises(fast.IntegrityError):
                fast.check_header(value, options.get('sequence', 70000), expected=options.get('expected'))
        short = ledger_result(70000, [], []); short['ledger']['ledger_data'] = '00' * 117
        with self.assertRaises(fast.IntegrityError): fast.check_header(short, 70000)

    def test_authenticate(self):
        manifest = {'format_version': 2, 'dicts': [], 'snapshots': [], 'tip': {'seq': 20},
                    'chunks': [{'first': 1, 'last': 10}, {'first': 11, 'last': 20}]}
        seed = self.seed()

        def publish(value):
            raw = fast.encoded(value)
            key, signature = fast.Signer().sign(raw, seed)
            (self.root / 'manifest.json').write_bytes(raw)
            (self.root / 'signatures').mkdir(exist_ok=True)
            (self.root / 'signatures' / (fast.digest(raw) + '.sig')).write_bytes(signature)
            (self.root / 'publisher.pub').write_bytes(key)
            return raw
        raw = publish(manifest)
        self.assertEqual(fast.authenticate(self.root, self.root / 'publisher.pub')[1], raw)
        hex_key = self.root / 'publisher.hex'
        hex_key.write_text((self.root / 'publisher.pub').read_bytes().hex() + '\n')
        self.assertEqual(fast.authenticate(self.root, hex_key)[0], manifest)
        other, _ = fast.Signer().sign(b'x', self.seed('other.seed'))
        wrong = self.root / 'wrong.pub'; wrong.write_bytes(other)
        with self.assertRaises(ValueError): fast.authenticate(self.root, wrong)
        (self.root / 'manifest.json').write_bytes(raw.replace(b'"seq":20', b'"seq":21'))
        with self.assertRaises((ValueError, FileNotFoundError)):
            fast.authenticate(self.root, self.root / 'publisher.pub')
        for broken in (dict(manifest, tip={'seq': 19}),
                       dict(manifest, chunks=[{'first': 1, 'last': 10}, {'first': 12, 'last': 20}]),
                       dict(manifest, format_version=1)):
            publish(broken)
            with self.assertRaises(ValueError): fast.authenticate(self.root, self.root / 'publisher.pub')
        duplicate = b'{"format_version":2,"format_version":2}'
        key, signature = fast.Signer().sign(duplicate, seed)
        (self.root / 'manifest.json').write_bytes(duplicate)
        (self.root / 'signatures' / (fast.digest(duplicate) + '.sig')).write_bytes(signature)
        with self.assertRaisesRegex(ValueError, 'Duplicate JSON field'):
            fast.authenticate(self.root, self.root / 'publisher.pub')
        self.assertEqual(json.loads(raw), manifest)


if __name__ == '__main__':
    unittest.main()
