"""Publication audit: runners split by ledger range, and parity results reused only for the very
bytes another audit compared (the same SHA-256)."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import publication_audit as pa  # noqa: E402


def bundle(first, last, tag='aa', rest='00'):
    return {'kind': 'bundle', 'first': first, 'last': last, 'object': f'bundles/{first}-{last}.{tag * 8}.xrpp', 'sha256': tag * 8 + rest * 24}


class Audit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)

    def tearDown(self): self.tmp.cleanup()

    def audit(self, **options):
        a = argparse.Namespace(workdir=self.root / 'work', xrpla_dir=self.root, from_ledger=0, until_ledger=0, parity_from=None)
        for key, value in options.items(): setattr(a, key, value)
        return pa.Audit(a)

    def write(self, workdir, *entries):
        workdir.mkdir(parents=True, exist_ok=True)
        (workdir / 'audited.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in entries))

    def test_runners_split_by_range_and_skip_what_is_done(self):
        ranges = [bundle(1, 10), bundle(11, 20), bundle(21, 30), {'kind': 'piece', 'first': 31, 'last': 32, 'object': 'pieces/x'}]
        self.write(self.root / 'work', {'object': ranges[0]['object'], 'result': 'passed'})
        self.assertEqual([r['first'] for r in self.audit().wanted(ranges)], [11, 21])
        self.assertEqual([r['first'] for r in self.audit(until_ledger=20).wanted(ranges)], [11])
        self.assertEqual([r['first'] for r in self.audit(from_ledger=21).wanted(ranges)], [21])

    def test_parity_is_reused_only_for_the_same_object_that_passed(self):
        parity = {'step': 'parity', 'ok': True, 'seconds': 300, 'summary': {'same': 900, 'different': 0}, 'peer_filled': []}
        def entry(time, rng, result, steps, hashed=True):
            return {'time': time, 'object': rng['object'], **({'sha256': rng['sha256']} if hashed else {}), 'result': result, 'steps': steps}
        self.write(self.root / 'early',
                   entry('t1', bundle(1, 10), 'passed', [{'step': 'sync', 'ok': True}, parity]),
                   entry('t2', bundle(11, 20), 'failed', [parity]),
                   entry('t3', bundle(21, 30), 'passed', [{'step': 'sync', 'ok': True}]),
                   entry('t4', bundle(31, 40), 'passed', [{'step': 'sync', 'ok': True}, parity], hashed=False))
        audit = self.audit(parity_from=[self.root / 'early'])
        reused = audit.earlier_parity(bundle(1, 10))
        self.assertEqual((reused['ok'], reused['summary'], reused['reused']['audited']), (True, parity['summary'], 't1'))
        self.assertIsNone(audit.earlier_parity(bundle(1, 10, tag='bb')))  # other bytes, other name
        self.assertIsNone(audit.earlier_parity(bundle(1, 10, rest='11')))  # same name, other bytes
        self.assertIsNone(audit.earlier_parity(bundle(31, 40)))           # recorded without its SHA-256
        self.assertIsNone(audit.earlier_parity(bundle(11, 20)))           # that audit failed
        self.assertIsNone(audit.earlier_parity(bundle(21, 30)))           # it had no parity step
        self.assertIsNone(self.audit().earlier_parity(bundle(1, 10)))     # none given


if __name__ == '__main__':
    unittest.main()
