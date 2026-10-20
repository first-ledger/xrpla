"""Compiled XRPH encoder: uncompressed sections byte-identical to the Python reference.

Usage: history_writer_test.py ENCODER [ARCHIVE LEDGERS]. The synthetic fixtures always
run; with a v2 archive (and its publisher.pub) the first LEDGERS ledgers are encoded
in 4,096-ledger segments too. Both decoders must reproduce the canonical bytes.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
# Read arguments before importing history_codec_test, which consumes argv[1] itself.
ENCODER = sys.argv.pop(1) if len(sys.argv) > 1 else os.environ.get('XRPLA_HISTORY_ENCODER')
ARCHIVE = Path(sys.argv.pop(1)) if len(sys.argv) > 1 else None
LEDGERS = int(sys.argv.pop(1)) if len(sys.argv) > 1 else 0
sys.path.insert(0, str(ROOT / 'tools')); sys.path.insert(0, str(ROOT / 'tests'))
import history_codec as hc
from compression_probe import Zstd
from field_codec import R
import history_codec_test as fixtures_module


class Sections:
    def __init__(self, zstd): self.z, self.sections = zstd, []
    def compress(self, data, level): self.sections.append(data); return self.z.compress(data, 1)


def sections_of(dump):
    r = R(dump); out = []
    while r.pos < len(dump): out.append(r.blob())
    return out


@unittest.skipUnless(ENCODER, 'Compiled encoder supplied by CTest')
class Writer(unittest.TestCase):
    def check(self, rows, **options):
        z = Zstd(); ref = Sections(z)
        python, _ = hc.encode(rows, ref, options.get('page', 64), True, 19, extra_recipes=options.get('extra', True))
        body = hc.original_body(rows)
        with tempfile.TemporaryDirectory() as d:
            d = Path(d); (d / 'body').write_bytes(body)
            command = [ENCODER, '--body', d / 'body', '--count', len(rows), '--out', d / 'seg', '--sections', d / 'sections',
                       '--page-ledgers', options.get('page', 64), '--check']
            if not options.get('extra', True): command.append('--no-extra-recipes')
            result = json.loads(subprocess.run([str(x) for x in command], check=True, capture_output=True, text=True).stdout)
            compiled = sections_of((d / 'sections').read_bytes())
            self.assertEqual(len(compiled), len(ref.sections))
            for i, (a, b) in enumerate(zip(compiled, ref.sections)): self.assertEqual(a, b, f'section {i} differs')
            # The Python reader decodes the compiled output too.
            self.assertEqual(hc.Archive((d / 'seg').read_bytes(), z).restore(), [(r[0], hc.raw_record(r)) for r in rows])
            self.assertTrue(result['checked'])
            # Same zstd level and library version would make whole outputs equal; sections are the format.
            self.assertEqual(len(python) > 0, True)

    def test_synthetic_fixtures(self):
        self.check(fixtures_module.fixtures(30), page=8)
        self.check(fixtures_module.fixtures(18), page=4, extra=False)
        self.check(fixtures_module.keylet_fixtures(), page=2)

    @unittest.skipUnless(ARCHIVE and LEDGERS, 'No archive supplied')
    def test_real_archive(self):
        from piece_publisher import ledger_rows, segments, source_manifest
        manifest = source_manifest(ARCHIVE, ARCHIVE / 'publisher.pub'); first = manifest['chunks'][0]['first']
        for group in segments(ledger_rows(ARCHIVE, manifest, first, first + LEDGERS - 1, Zstd())):
            self.check(group)


if __name__ == '__main__':
    unittest.main()
