"""Guard qualification evidence against history gaps and canonical-data mismatches."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from node_probe import choose_range, pinned
from node_conformance import collect, project


class Evidence(unittest.TestCase):
    def test_nft_pagination_uses_its_own_cursor_and_rejects_duplicates(self):
        first = {'validated': True, 'ledger_index': 20, 'ledger_hash': 'ab' * 32,
                 'account_nfts': [{'NFTokenID': '01'}], 'marker': 'upstream-cursor'}
        last = dict(first, account_nfts=[{'NFTokenID': '02'}]); last.pop('marker')
        calls = []
        def request(method, params):
            calls.append(params)
            return last if 'marker' in params else first
        full = collect('account_nfts', {'account': 'sample'}, request)
        self.assertEqual(len(full['account_nfts']), 2)
        self.assertNotIn('marker', full)
        self.assertEqual(calls[1]['marker'], 'upstream-cursor')
        last['account_nfts'] = first['account_nfts']
        with self.assertRaises(ValueError):
            collect('account_nfts', {'account': 'sample'}, request)

    def test_range_reserves_snapshot_and_does_not_cross_a_gap(self):
        self.assertEqual(choose_range('10-30,40-50', 50, 10, 0), (41, 50))
        self.assertEqual(choose_range('10-30,40-50', 50, 11, 0), (20, 30))
        for complete in ('empty', '10-20,22-25', '1'):
            with self.assertRaises(ValueError):
                choose_range(complete, 25, 11, 0)
        with self.assertRaises(ValueError):
            choose_range('0-25', 25, 10, 0)

    def test_reference_requires_validated_exact_sequence_and_hash(self):
        good = {'validated': True, 'ledger_index': 20, 'ledger_hash': 'ab' * 32}
        self.assertEqual(pinned(good, 20, 'AB' * 32), 'AB' * 32)
        for change in ({'validated': False}, {'ledger_index': 19}, {'ledger_hash': 'cd' * 32}):
            with self.assertRaises(ValueError):
                pinned(dict(good, **change), 20, 'AB' * 32)

    def test_projection_ignores_envelope_fields_but_detects_changed_metadata(self):
        source = {'validated': True, 'ledger_index': 20, 'ledger_hash': 'ab' * 32,
                  'ledger': {'ledger_data': 'aa', 'transactions': [{'tx_blob': 'bb', 'meta': 'cc'}]}}
        same = dict(copy.deepcopy(source), status='success', warnings=[{'id': 2001}])
        self.assertEqual(project('ledger', source), project('ledger', same))
        changed = copy.deepcopy(source)
        changed['ledger']['transactions'][0]['meta'] = 'dd'
        self.assertNotEqual(project('ledger', source), project('ledger', changed))
        changed['ledger']['transactions'] = []
        self.assertNotEqual(project('ledger', source), project('ledger', changed))
        same['validated'] = False
        with self.assertRaises(ValueError):
            project('ledger', same)


if __name__ == '__main__':
    unittest.main()
