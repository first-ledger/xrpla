"""Offline real-mainnet replay plus recorded Clio 2.7.1 RPC conformance.

xrpla answers like rippled/xrpld (tools/xrpld_parity.py compares it with a live
xrpld). Where Clio 2.7.1 answers differently the recorded responses are compared
without those fields: rippled's ledger_data marker is the next key minus one;
account_objects markers name a directory entry; tx carries ctid; limit is echoed
only alongside a marker; gateway_balances and book_offers report validated;
account_info lists the DisallowIncoming and Clawback account flags always (xrpld
3.3 and 3.4), where Clio 2.7.1 lists them once their amendments are enabled.
Continuations that pass a Clio-format account_objects marker are skipped.
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

binary = sys.argv[1]
fixtures = Path(__file__).parent / 'fixtures'
RIPPLED_OVER_CLIO = {'ledger_data': {'marker'}, 'account_objects': {'marker', 'limit'}, 'tx': {'ctid'},
                     'account_lines': {'limit'}, 'account_nfts': {'limit'}, 'account_offers': {'limit'},
                     'gateway_balances': {'validated'}, 'book_offers': {'validated'}}
ALWAYS_LISTED_FLAGS = {'allowTrustLineClawback', 'disallowIncomingCheck', 'disallowIncomingNFTokenOffer',
                       'disallowIncomingPayChan', 'disallowIncomingTrustline'}

def clean(value, method=None):
    if isinstance(value, dict):
        drop = {'status', 'warnings'} | RIPPLED_OVER_CLIO.get(method, set())
        return {k: clean(v) for k, v in value.items() if k not in drop}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value

with tempfile.TemporaryDirectory(prefix='xrpla-mainnet-') as tmp:
    comparisons = 0
    for name in ('mainnet-early', 'mainnet-transactions'):
        source = fixtures / name; data = Path(tmp) / name
        expected = json.loads((source / 'export-report.json').read_text())
        bounds = expected['range']; range = f'{bounds["first"]}-{bounds["last"]}'
        common = ['--dir', str(data), '--publisher-key', str(source / 'publisher.pub')]
        def cli(*args, stdin=None):
            p = subprocess.run([binary, *args], input=stdin, capture_output=True, text=True, timeout=180)
            assert p.returncode == 0, p.stderr
            return json.loads(p.stdout)
        cli('fetch', '--archive', str(source), '--range', range, *common)
        verified = cli('verify', '--full', '--trust-ledger-hash', expected['last_ledger_hash'], *common)
        assert verified['ledgers_replayed'] == bounds['last'] - bounds['first'] + 1, verified
        built = cli('build', *common)
        assert built['store']['last_ledger_hash'] == expected['last_ledger_hash'], built
        golden = source / 'clio-2.7.1-responses.json'
        if golden.exists():
            for row in json.loads(golden.read_text())['comparisons']:
                if row['method'] == 'account_objects' and 'marker' in row['params']: continue
                actual = cli('rpc', '--dir', str(data), '--request', '-', stdin=json.dumps({'method': row['method'], 'params': row['params']}))
                if row['method'] == 'account_info' and 'account_flags' in actual:
                    extra = set(actual['account_flags']) - set(row['expected'].get('account_flags', {}))
                    assert extra <= ALWAYS_LISTED_FLAGS, (row['params'], extra)
                    for name in extra: del actual['account_flags'][name]
                assert clean(actual, row['method']) == clean(row['expected'], row['method']), (row['method'], row['params'], actual)
                comparisons += 1
    print(json.dumps({'mainnet_ledgers': 1005, 'recorded_clio_responses': comparisons, 'passed': True}))
