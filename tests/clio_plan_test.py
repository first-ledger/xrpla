"""`xrpla import clio`: the plan is the same rows, in the same file, however many jobs write it.

For each fixture archive (fetched and built), the whole range and a range in its middle:
plans written with one thread and with several, with the final tables cut into other
numbers of key ranges and their rows spilled to files, are byte-identical, and their rows,
sorted, are exactly those xrpla wrote with its single-threaded plan writer (93923cd), as
is the file itself.

Usage: clio_plan_test.py; needs XRPLA_BINARY. `clio_plan_test.py --digests` prints the
table below for the binary XRPLA_BINARY names, as the test computes it (the plans it
writes with one job); it was produced so from xrpla built at 93923cd, before plans were
written by several jobs:
    git worktree add /tmp/xrpla-93923cd 93923cd   # then build it as usual
    XRPLA_BINARY=/tmp/xrpla-93923cd/build/xrpla python3 tests/clio_plan_test.py --digests
(that binary knows no --jobs; --digests passes none).
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
BINARY = os.environ.get('XRPLA_BINARY')
# fixture, range (None: the whole archive) -> SHA-256 of the plan file, of its rows sorted
# (without the identity line), as xrpla 93923cd wrote them (single-threaded, the old writer).
SINGLE_THREADED = {
    # Rewritten with the NFT burn fix: a burn that merges its page away (token ...6E5D19C600000010
    # at ledger 23) gains its nf_tokens, nf_token_uris and issuer_nf_tokens_v2 rows; 93923cd's
    # store never recorded the burn. Every other row, and every other plan here, is unchanged.
    ('standalone-nfts', None): ('f89c9a2639957c3dc40289e26533968570b24f953f0d50ac50074a661ab65e28', 'f383ffa531dfab494aeaee0b74584bd6372badd8ab9ba3e7ce39342efaf26749'),
    # Rewritten when the baseline stopped writing deletions: keys deleted before ledger 12 lose
    # their five empty objects rows (Clio's own load writes none); no other row changes.
    ('standalone-nfts', '12-18'): ('d70528fedd145d0d11d3e50567be7488f00bc3b19bd3122af425b21dc601d773', 'e832662388cdf4568724437f30eed3f3c6b774056a710cccb52aab7472972615'),
    ('standalone-tokens', None): ('ea0d7b2b2bd626278258709df310a2ed3dee82f999ae2df3027d5a1b80a4f130', 'a201d4c278b3d03e496ae3c83e4b1fb055f7672cf5fad6f8db9500f9116df4ea'),
    ('standalone-tokens', '20-28'): ('6b6ac46f7d0fab715aa2f6e7e3696e6f2f2f1713a1c82c7521cc6283bd663113', '6ac0e945150c0636d62d9e3362ff707ee3d53052f0876a20252fb1518075395b'),
    ('mainnet-early', None): ('1f0867dc95b8373b9554094173c80baa5cfd6ef2b662fe318ccf99c5c8f32f01', '62a958d2af91d23efcc815d365d19864ddc226cd77dc5ef1a5a997f29602e75b'),
    ('mainnet-early', '32904-33237'): ('f80d16fd5351451cc4abf4cd5ace9b0f4cd554111dd69064cd2d1657db0a2122', '7bb8c71a378e89927db5e5bf55ecc828fb47532c6cccae5ce08fd581e41f23f4'),
}
# Ways to write each plan: --jobs, and the environment's range bits and spill size
# (import_clio.cpp's test settings: other cuts of the final tables, rows in spill files).
WAYS = [('1', {}), ('2', {}), ('5', {}), ('32', {}), (None, {}),
        ('4', {'XRPLA_CLIO_RANGE_BITS': '1'}), ('4', {'XRPLA_CLIO_RANGE_BITS': '16'}),
        ('3', {'XRPLA_CLIO_SPILL_BYTES': '700'}), ('1', {'XRPLA_CLIO_SPILL_BYTES': '1', 'XRPLA_CLIO_RANGE_BITS': '2'})]


def run(*command, env=None):
    result = subprocess.run([str(x) for x in command], capture_output=True, text=True, timeout=900, env=env)
    if result.returncode:
        raise RuntimeError(f'{command[1]} failed: {result.stdout[-2000:]}{result.stderr[-3000:]}')
    return json.loads(result.stdout) if result.stdout.strip() else None


def plans(tmp, ways):
    """Builds each fixture's store and writes its plans: yields (fixture, range, way, result, plan bytes)."""
    for fixture in sorted({name for name, _ in SINGLE_THREADED}):
        archive, data = ROOT / 'tests' / 'fixtures' / fixture, Path(tmp) / fixture
        chunks = json.loads((archive / 'manifest.json').read_text())['chunks']
        run(BINARY, 'fetch', '--archive', archive, '--range', f'{chunks[0]["first"]}-{chunks[-1]["last"]}', '--dir', data, '--publisher-key', archive / 'publisher.pub')
        run(BINARY, 'build', '--dir', data, '--publisher-key', archive / 'publisher.pub')
        for name, ledgers in SINGLE_THREADED:
            if name != fixture:
                continue
            for n, (jobs, settings) in enumerate(ways):
                out = Path(tmp) / f'{fixture}-{ledgers}-{n}'
                result = run(BINARY, 'import', 'clio', '--dir', data, '--out', out, *(('--range', ledgers) if ledgers else ()),
                             *(('--jobs', jobs) if jobs else ()), env={**os.environ, **settings})
                yield fixture, ledgers, (jobs, settings), result, (out / 'clio-plan.jsonl').read_bytes()


def digests(plan):
    return hashlib.sha256(plan).hexdigest(), hashlib.sha256(b'\n'.join(sorted(plan.splitlines()[1:]))).hexdigest()


@unittest.skipUnless(BINARY, 'Compiled xrpla supplied by CTest')
class ClioPlan(unittest.TestCase):
    def test_same_rows_for_any_jobs(self):
        with tempfile.TemporaryDirectory(prefix='xrpla-clio-plan-') as tmp:
            written = {}
            for fixture, ledgers, way, result, plan in plans(tmp, WAYS):
                case = fixture, ledgers, way
                self.assertEqual(result['rows'], len(plan.splitlines()) - 17, case)  # the identity line and 16 schemas
                self.assertEqual(digests(plan)[1], SINGLE_THREADED[fixture, ledgers][1], case)
                self.assertFalse([p.name for p in Path(result['plan']).parent.iterdir() if '.part' in p.name], case)  # no spill file left
                written.setdefault((fixture, ledgers), set()).add(plan)
            for key, files in written.items():
                self.assertEqual(len(files), 1, key)
                self.assertEqual(digests(next(iter(files)))[0], SINGLE_THREADED[key][0], key)


if __name__ == '__main__':
    if sys.argv[1:] == ['--digests']:
        with tempfile.TemporaryDirectory(prefix='xrpla-clio-plan-') as tmp:
            for fixture, ledgers, _, _, plan in plans(tmp, [(None, {})]):
                print(f'    {(fixture, ledgers)!r}: {digests(plan)!r},')
    else:
        unittest.main()
