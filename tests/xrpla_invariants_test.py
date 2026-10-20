"""tools/xrpla_invariants.py: its xrpld and Clio rules on hand-made metadata, and no violations
on the fixtures.

Usage: xrpla_invariants_test.py [ARCHIVE]. Without ARCHIVE only the rules are tested. With it
(a fixture archive), the archive is served twice, as a RocksDB store (`xrpla fetch` and
`xrpla build`) and as a piece cache (published with tools/piece_publisher.py, `xrpla sync`,
`xrpla index-pieces`), and the tool must check every ledger of each with no violation. Needs
XRPLA_BINARY, XRPLA_HISTORY_DECODER and XRPLA_SNAPSHOT_TOOL (XRPLA_HISTORY_ENCODER optional).
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
sys.path.insert(0, str(ROOT / 'tests'))
import xrpla_invariants as inv  # noqa: E402

ARCHIVE = Path(sys.argv.pop(1)) if len(sys.argv) > 1 else None
BINARY, DECODER, SNAPSHOT = (os.environ.get(k) for k in ('XRPLA_BINARY', 'XRPLA_HISTORY_DECODER', 'XRPLA_SNAPSHOT_TOOL'))
ENCODER = os.environ.get('XRPLA_HISTORY_ENCODER')

ALICE, BOB, CAROL, DAVE = (inv.address(bytes([n]) * 20) for n in (1, 2, 3, 4))
# An MPTokenIssuanceID: sequence 5, then DAVE's AccountID.
ISSUANCE = (5).to_bytes(4, 'big').hex().upper() + inv.account_id(DAVE).hex().upper()


def token_id(flags, fee, issuer, taxon, serial):
    """An NFTokenID as xrpld's NFTokenMint makes it (taxon ciphered with the serial)."""
    ciphered = taxon ^ ((384160001 * serial + 2459) & 0xFFFFFFFF)
    return (flags.to_bytes(2, 'big') + fee.to_bytes(2, 'big') + inv.account_id(issuer) + ciphered.to_bytes(4, 'big') + serial.to_bytes(4, 'big')).hex().upper()


class Rules(unittest.TestCase):
    def test_addresses_round_trip(self):
        for a in (ALICE, BOB, CAROL, DAVE, inv.ZERO_ACCOUNT, 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh'): self.assertEqual(inv.address(inv.account_id(a)), a)
        self.assertEqual(inv.account_id(inv.ZERO_ACCOUNT), bytes(20))

    def test_affected_accounts(self):
        meta = {'AffectedNodes': [
            # Every STAccount at the top of FinalFields; PreviousFields are not read.
            {'ModifiedNode': {'LedgerEntryType': 'AccountRoot', 'FinalFields': {'Account': ALICE, 'RegularKey': BOB, 'Balance': '1'},
                              'PreviousFields': {'RegularKey': CAROL}}},
            # A trust line: the limits' issuers, not the balance's (always the zero account).
            {'CreatedNode': {'LedgerEntryType': 'RippleState', 'NewFields': {
                'Balance': {'currency': 'USD', 'issuer': inv.ZERO_ACCOUNT, 'value': '0'},
                'LowLimit': {'currency': 'USD', 'issuer': CAROL, 'value': '0'}, 'HighLimit': {'currency': 'USD', 'issuer': DAVE, 'value': '9'}}}},
            # A deleted node: its FinalFields; TakerPays XRP (no issuer), TakerGets an issued
            # currency; an account nested in an object is not read.
            {'DeletedNode': {'LedgerEntryType': 'Offer', 'FinalFields': {'Account': BOB, 'TakerPays': '100',
                             'TakerGets': {'currency': 'EUR', 'issuer': ALICE, 'value': '1'}, 'Nested': {'Account': 'rNestedNotRead'}}}},
            # MPTokenIssuanceID gives its issuer; a node without FinalFields adds nothing.
            {'ModifiedNode': {'LedgerEntryType': 'MPToken', 'FinalFields': {'Account': CAROL, 'MPTokenIssuanceID': ISSUANCE}}},
            {'ModifiedNode': {'LedgerEntryType': 'DirectoryNode', 'PreviousFields': {'Owner': 'rOnlyPrevious'}}},
        ]}
        self.assertEqual(inv.affected_accounts(meta), {ALICE, BOB, CAROL, DAVE})
        # An MPT amount in TakerGets: the issuance's issuer; the Destination of a created check.
        meta = {'AffectedNodes': [
            {'CreatedNode': {'LedgerEntryType': 'Offer', 'NewFields': {'Account': ALICE, 'TakerPays': '5', 'TakerGets': {'mpt_issuance_id': ISSUANCE, 'value': '3'}}}},
            {'CreatedNode': {'LedgerEntryType': 'Check', 'NewFields': {'Account': ALICE, 'Destination': BOB, 'SendMax': {'currency': 'USD', 'issuer': CAROL, 'value': '1'}}}}]}
        self.assertEqual(inv.affected_accounts(meta), {ALICE, BOB, DAVE})  # SendMax's issuer is not one of the four amounts
        self.assertEqual(inv.affected_accounts({}), set())

    def test_nft_id_fields(self):
        token = token_id(8, 500, ALICE, 7, 12)
        self.assertEqual(inv.nft_fields(token), {'flags': 8, 'transfer_fee': 500, 'issuer': ALICE, 'nft_taxon': 7, 'nft_serial': 12})

    def page(self, kind, owner, before=None, after=None):
        """An NFTokenPage node of `owner` with the tokens ({id: uri}) before and after."""
        tokens = lambda m: [{'NFToken': {'NFTokenID': t, **({'URI': u} if u else {})}} for t, u in m.items()]
        key = inv.account_id(owner).hex().upper() + 'F' * 24
        body = {'LedgerEntryType': 'NFTokenPage', 'LedgerIndex': key}
        if kind == 'CreatedNode': body['NewFields'] = {'NFTokens': tokens(after)}
        elif kind == 'DeletedNode': body['FinalFields'] = {'NFTokens': tokens(before)}
        else:
            body['FinalFields'] = {'NFTokens': tokens(after)}
            if before is not None: body['PreviousFields'] = {'NFTokens': tokens(before)}
        return {kind: body}

    def test_nft_rules(self):
        a, b, c = token_id(8, 0, ALICE, 1, 1), token_id(8, 0, ALICE, 1, 2), token_id(8, 0, ALICE, 1, 3)
        ok = 'tesSUCCESS'
        mint = {'tx': {'TransactionType': 'NFTokenMint', 'Account': ALICE}, 'hash': 'M',
                'meta': {'TransactionResult': ok, 'TransactionIndex': 0, 'AffectedNodes': [self.page('ModifiedNode', ALICE, {a: ''}, {a: '', b: 'AB'})]}}
        sell = 'S' * 64
        accept = {'tx': {'TransactionType': 'NFTokenAcceptOffer', 'Account': BOB, 'NFTokenSellOffer': sell}, 'hash': 'A',
                  'meta': {'TransactionResult': ok, 'TransactionIndex': 1, 'AffectedNodes': [
                      self.page('ModifiedNode', ALICE, {a: '', b: 'AB'}, {a: ''}), self.page('CreatedNode', BOB, after={b: 'AB'}),
                      {'DeletedNode': {'LedgerEntryType': 'NFTokenOffer', 'LedgerIndex': sell, 'FinalFields': {'NFTokenID': b, 'Owner': ALICE}}}]}}
        burn = {'tx': {'TransactionType': 'NFTokenBurn', 'Account': ALICE, 'NFTokenID': a}, 'hash': 'B',
                'meta': {'TransactionResult': ok, 'TransactionIndex': 2, 'AffectedNodes': [self.page('DeletedNode', ALICE, {a: ''})]}}
        cancel = {'tx': {'TransactionType': 'NFTokenCancelOffer', 'Account': CAROL, 'NFTokenOffers': ['X' * 64]}, 'hash': 'C',
                  'meta': {'TransactionResult': ok, 'TransactionIndex': 3, 'AffectedNodes': [
                      {'DeletedNode': {'LedgerEntryType': 'NFTokenOffer', 'LedgerIndex': 'X' * 64, 'FinalFields': {'NFTokenID': c, 'Owner': CAROL}}}]}}
        failed = {'tx': {'TransactionType': 'NFTokenBurn', 'Account': CAROL, 'NFTokenID': c}, 'hash': 'F',
                  'meta': {'TransactionResult': 'tecNO_PERMISSION', 'TransactionIndex': 4, 'AffectedNodes': []}}
        # A page split moving `b` between two of BOB's pages changes no token entry.
        split = {'meta': {'AffectedNodes': [self.page('ModifiedNode', BOB, {b: 'AB', c: ''}, {c: ''}), self.page('CreatedNode', BOB, after={b: 'AB'})]}}
        self.assertEqual(inv.clio_nft_tokens(mint['tx'], mint['meta']), {b})
        self.assertEqual(inv.clio_nft_tokens(accept['tx'], accept['meta']), {b})
        self.assertEqual(inv.clio_nft_tokens(burn['tx'], burn['meta']), {a})
        self.assertEqual(inv.clio_nft_tokens(cancel['tx'], cancel['meta']), {c})
        self.assertEqual(inv.clio_nft_tokens(failed['tx'], failed['meta']), set())  # Clio records only tesSUCCESS
        self.assertEqual(inv.named_tokens(failed['tx']), {c})  # though it names the token
        self.assertEqual(inv.changed_tokens(mint['meta']), {b})
        self.assertEqual(inv.changed_tokens(accept['meta']), {b})
        self.assertEqual(inv.changed_tokens(split['meta']), set())
        # Page tokens are not "named": only NFTokenID fields outside NFTokens arrays are.
        self.assertEqual(inv.named_tokens(mint['meta']), set())
        self.assertEqual(inv.final_nft_states([mint, accept, burn, cancel, failed]), {b: (BOB, False, 'AB'), a: (ALICE, True, '')})

    def test_directory_pages(self):
        root = 'AB' * 32
        self.assertEqual(inv.dir_page(root, 0), root)
        self.assertEqual(len(inv.dir_page(root, 1)), 64)
        self.assertNotEqual(inv.dir_page(root, 1), inv.dir_page(root, 2))
        # Quality 1: mantissa 10^15, exponent -15 (stored as 85).
        self.assertAlmostEqual(inv.exchange_rate('0' * 48 + ((85 << 56) | 10 ** 15).to_bytes(8, 'big').hex()), 1.0)
        self.assertEqual(inv.ctid_parts('C000000500020000'), (5, 2))

    def test_page_cap_is_skipped_not_violated(self):
        # A listing longer than the page cap is undecided: counted as skipped, never as a violation.
        client = inv.Client('http://127.0.0.1:1', 1)
        client.call = lambda method, params: {'account_nfts': [{'NFTokenID': '00'}], 'marker': 'm'}
        entries, asked, error = client.pages('account_nfts', {'account': ALICE}, 'account_nfts', 3)
        self.assertEqual((len(entries), len(asked)), (3, 3))
        self.assertTrue(inv.truncated(error))
        self.assertFalse(inv.truncated({'error': 'actNotFound'}) or inv.truncated(None))
        checks = inv.Invariants(type('Args', (), {'xrpla': 'http://127.0.0.1:1', 'in_flight': 1, 'examples': 3})())
        checks.skip('nfts_by_issuer')
        self.assertEqual({k: checks.stats['nfts_by_issuer'][k] for k in ('checked', 'violated', 'skipped')}, {'checked': 0, 'violated': 0, 'skipped': 1})
        checks.pool.shutdown()

    def test_unvouched_account_mptokens_are_checked_against_state(self):
        # Without --scan-state, an issuance account_mptokens lists that mpt_holders did not vouch
        # for is compared with its MPToken in state: here its amount and lock differ.
        other = (6).to_bytes(4, 'big').hex().upper() + inv.account_id(DAVE).hex().upper()
        tokens = {ISSUANCE: {'Account': ALICE, 'MPTAmount': '5'}, other: {'Account': ALICE, 'MPTAmount': '7', 'Flags': 1}}
        def call(method, params):
            if method == 'ledger_entry' and 'mpt_issuance' in params: return {'node': {'LedgerEntryType': 'MPTokenIssuance'}}
            if method == 'ledger_entry': return {'node': tokens[params['mptoken']['mpt_issuance_id']], 'index': 'AA'}
            if method == 'mpt_holders': return {'mptokens': [{'account': ALICE, 'flags': 0, 'mpt_amount': '5', 'mptoken_index': 'AA'}]}
            if method == 'account_mptokens':
                return {'mptokens': [{'account': ALICE, 'mpt_issuance_id': ISSUANCE, 'mpt_amount': '5'},
                                     {'account': ALICE, 'mpt_issuance_id': other, 'mpt_amount': '9'}]}
            raise AssertionError(method)
        args = {'xrpla': 'http://127.0.0.1:1', 'in_flight': 1, 'examples': 3, 'scan_state': False, 'mpts': 0, 'page_cap': 3}
        checks = inv.Invariants(type('Args', (), args)()); checks.client.call = call
        txs = [{'tx': {'TransactionType': 'MPTokenAuthorize', 'Account': ALICE, 'MPTokenIssuanceID': ISSUANCE},
                'meta': {'AffectedNodes': [{'ModifiedNode': {'LedgerEntryType': 'MPToken', 'FinalFields': {'Account': ALICE, 'MPTokenIssuanceID': ISSUANCE}}}]}}]
        import random
        checks.check_mpts(7, txs, random.Random(1))
        checks.pool.shutdown()
        self.assertEqual({k: checks.stats['mpt_holders'][k] for k in ('checked', 'violated')}, {'checked': 1, 'violated': 0})
        stats = checks.stats['account_mptokens']
        self.assertEqual((stats['checked'], stats['violated'], stats['errors']), (1, 1, 0))
        self.assertIn(f"{other}: (amount, locked, authorized) ('9', False, False), its MPToken in state has ('7', True, False)", stats['examples'][0]['detail'])


@unittest.skipUnless(ARCHIVE and BINARY and DECODER and SNAPSHOT, 'A fixture archive and the compiled tools supplied by CTest')
class Fixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from piece_query_test import Server
        cls.tmp = tempfile.TemporaryDirectory(prefix='xrpla-invariants-')
        root = Path(cls.tmp.name)
        manifest = json.loads((ARCHIVE / 'manifest.json').read_bytes())
        first, last = manifest['chunks'][0]['first'], manifest['tip']['seq']

        def run(*command):
            result = subprocess.run([str(x) for x in command], capture_output=True, text=True, timeout=3600)
            if result.returncode: raise RuntimeError(f'{command[:2]} failed: {result.stdout[-3000:]}{result.stderr[-3000:]}')

        key = ARCHIVE / 'publisher.pub'
        run(BINARY, 'fetch', '--archive', ARCHIVE, '--range', f'{first}-{last}', '--dir', root / 'store', '--publisher-key', key)
        run(BINARY, 'build', '--dir', root / 'store', '--publisher-key', key)
        seed = root / 'publisher.seed'; seed.write_bytes(os.urandom(32))
        run(sys.executable, ROOT / 'tools' / 'piece_publisher.py', '--source', ARCHIVE, '--source-key', key, '--bucket', root / 'bucket',
            '--signing-key', seed, '--scratch', root / 'scratch', '--decoder', DECODER, '--snapshot-tool', SNAPSHOT, '--piece-ledgers', 2,
            '--bundle-pieces', 2, '--workers', os.cpu_count() or 2, '--grace-seconds', 0, *(('--encoder', ENCODER) if ENCODER else ()))
        run(BINARY, 'sync', '--source', root / 'bucket', '--publisher-key', root / 'bucket' / 'publisher.pub', '--dir', root / 'cache')
        run(BINARY, 'index-pieces', '--dir', root / 'cache')
        cls.servers = {'store': Server(root / 'store'), 'pieces': Server(root / 'cache')}

    @classmethod
    def tearDownClass(cls):
        for server in cls.servers.values(): server.close()
        cls.tmp.cleanup()

    def check(self, backend):
        server = self.servers[backend]
        result = subprocess.run([sys.executable, ROOT / 'tools' / 'xrpla_invariants.py', '--xrpla', f'http://127.0.0.1:{server.port}', '--ledgers', '0',
                                 '--accounts', '0', '--txs', '0', '--nfts', '0', '--scan-state', '--examples', '6'],
                                capture_output=True, text=True, timeout=1800)
        lines = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
        print(backend, *(json.dumps({k: v for k, v in line.items() if k != 'examples'}) for line in lines), sep='\n', file=sys.stderr)
        stats = {line['invariant']: line for line in lines if 'invariant' in line}
        summary = next(line for line in lines if line.get('summary'))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((summary['violated'], summary['errors']), (0, 0), result.stdout)
        # Each fixture exercises its own indexes: every invariant it has data for was checked.
        self.assertGreater(stats['account_tx']['checked'], 0)
        self.assertGreater(stats['tx']['checked'], 0)
        name = ARCHIVE.name
        if name == 'standalone-nfts':
            for invariant in ('nft_info', 'nfts_by_issuer', 'nft_history'): self.assertGreater(stats[invariant]['checked'], 0, invariant)
        if name == 'standalone-tokens':
            for invariant in ('mpt_holders', 'account_mptokens'): self.assertGreater(stats[invariant]['checked'], 0, invariant)
        if name == 'mainnet-transactions': self.assertGreater(stats['book_offers']['checked'], 0)

    def test_store(self): self.check('store')

    def test_pieces(self): self.check('pieces')


if __name__ == '__main__':
    unittest.main()
