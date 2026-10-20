"""The compiled bundle index (xrpla_tx_index) writes the same bytes as the Python builder.

Publishes the mainnet-transactions fixture with tiny pieces, keeping retired pieces,
then rebuilds every container's XTXI index (pieces singly, each bundle from its
pieces) with the tool and compares it with the index the publisher wrote.
Needs XRPLA_TX_INDEX, XRPLA_HISTORY_DECODER and XRPLA_SNAPSHOT_TOOL.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import xrpp

FIXTURE = ROOT / 'tests' / 'fixtures' / 'mainnet-transactions'
TOOL, DECODER, SNAPSHOT = (os.environ.get(k) for k in ('XRPLA_TX_INDEX', 'XRPLA_HISTORY_DECODER', 'XRPLA_SNAPSHOT_TOOL'))


@unittest.skipUnless(TOOL and DECODER and SNAPSHOT, 'Compiled tools supplied by CTest')
class TxIndexTool(unittest.TestCase):
    def test_matches_python_indexes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); bucket = root / 'bucket'; seed = root / 'seed'; seed.write_bytes(os.urandom(32))
            subprocess.run([sys.executable, str(ROOT / 'tools' / 'piece_publisher.py'), '--source', str(FIXTURE),
                            '--source-key', str(FIXTURE / 'publisher.pub'), '--bucket', str(bucket), '--signing-key', str(seed),
                            '--scratch', str(root / 'scratch'), '--decoder', DECODER, '--snapshot-tool', SNAPSHOT,
                            '--piece-ledgers', '1', '--bundle-pieces', '2', '--workers', '1', '--grace-seconds', '3600'],
                           check=True, capture_output=True)
            pointer = json.loads((bucket / 'catalog/current.json').read_bytes())
            catalog = json.loads((bucket / pointer['object']).read_bytes())
            retired = [r['object'] for r in catalog['retired']]
            def index_of(path):
                return xrpp.Container.open(path).index_bytes()
            def tool(paths):
                out = root / 'index'
                subprocess.run([TOOL, '--out', str(out), *map(str, paths)], check=True, capture_output=True)
                return out.read_bytes()
            checked = 0
            for name in retired + [r['object'] for r in catalog['ranges']]:
                self.assertEqual(tool([bucket / name]), index_of(bucket / name), name); checked += 1
            for bundle in (r for r in catalog['ranges'] if r['kind'] == 'bundle'):
                pieces = sorted((p for p in retired if bundle['first'] <= int(Path(p).name.split('-')[0]) <= bundle['last']),
                                key=lambda p: int(Path(p).name.split('-')[0]))
                if len(pieces) > 1:
                    self.assertEqual(tool([bucket / p for p in pieces]), index_of(bucket / bundle['object'])); checked += 1
            self.assertGreater(checked, 3)


if __name__ == '__main__':
    unittest.main()
