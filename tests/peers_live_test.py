"""The peer protocol against the live XRPL network (opt-in: XRPLA_LIVE_PEERS=1).

`xrpla peers` must complete the handshake with mainnet's public hubs, count validations
relayed by peers against vl.ripple.com, and fetch the header of each quorum-validated
ledger from a peer, hashing to the validated hash. Needs XRPLA_BINARY and network access.
"""
import json
import os
import subprocess
import unittest

BINARY = os.environ.get('XRPLA_BINARY')


@unittest.skipUnless(BINARY and os.environ.get('XRPLA_LIVE_PEERS') == '1', 'Live network test: set XRPLA_LIVE_PEERS=1')
class LivePeers(unittest.TestCase):
    def test_validated_headers_from_peers(self):
        result = subprocess.run([BINARY, 'peers', '--ledgers', '3', '--seconds', '120'], capture_output=True, text=True, timeout=300)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        report = json.loads(result.stdout)
        self.assertTrue(report['peers'], report)
        self.assertGreaterEqual(len(report['ledgers']), 3, report)
        for ledger in report['ledgers']:
            self.assertTrue(ledger['header_verified'], ledger)
            self.assertGreaterEqual(ledger['trusted_validations'], report['quorum'])


if __name__ == '__main__':
    unittest.main()
