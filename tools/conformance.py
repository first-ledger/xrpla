#!/usr/bin/env python3
"""Compare historical XRPLA RPC with a pinned upstream server at explicit ledgers."""
import argparse
import json
from pathlib import Path
import subprocess
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--dir', required=True)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--first', type=int, required=True)
    parser.add_argument('--last', type=int, required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    def local(method, params):
        result = subprocess.run([args.binary, 'rpc', '--dir', args.dir, '--request', '-'], input=json.dumps({'method': method, 'params': params}), capture_output=True, text=True, check=True)
        return json.loads(result.stdout)

    def remote(method, params):
        body = json.dumps({'method': method, 'params': [params]}).encode()
        request = urllib.request.Request(args.endpoint, body, {'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)['result']

    def clean(value):
        if isinstance(value, dict):
            return {k: clean(v) for k, v in value.items() if k not in ('warnings', 'status')}
        if isinstance(value, list):
            return [clean(v) for v in value]
        return value

    records = []
    def differences(a, b, path=''):
        if type(a) is not type(b):
            return [f'{path}: type {type(a).__name__} != {type(b).__name__}']
        if isinstance(a, dict):
            out = [f'{path}/{k}: missing locally' for k in b.keys() - a.keys()]
            out += [f'{path}/{k}: extra locally' for k in a.keys() - b.keys()]
            for k in a.keys() & b.keys():
                out += differences(a[k], b[k], path + '/' + k)
            return out
        if isinstance(a, list):
            out = [] if len(a) == len(b) else [f'{path}: length {len(a)} != {len(b)}']
            for i, (x, y) in enumerate(zip(a, b)):
                out += differences(x, y, path + '/' + str(i))
            return out
        return [] if a == b else [f'{path}: {str(a)[:80]} != {str(b)[:80]}']
    def check(method, params):
        actual = local(method, params); expected = remote(method, params)
        equal = clean(actual) == clean(expected)
        records.append({'method': method, 'params': params, 'equal': equal, 'differences': differences(clean(actual), clean(expected))[:40], 'actual': actual, 'expected': expected})
        print(method, 'PASS' if equal else 'DIFFERENT', flush=True)
        return actual

    txs = local('ledger', {'ledger_index': args.first, 'transactions': True})['ledger']['transactions']
    account = local('tx', {'transaction': txs[0]})['Account'] if txs else None
    offers = [x for x in local('ledger_data', {'ledger_index': args.first, 'limit': 200})['state'] if x['LedgerEntryType'] == 'Offer']
    for version in (1, 2):
        base = {'ledger_index': args.first, 'api_version': version}
        for binary in (False, True):
            check('ledger', dict(base, transactions=True, expand=True, binary=binary))
            page = check('ledger_data', dict(base, limit=16, binary=binary))
            if 'marker' in page:
                check('ledger_data', dict(base, limit=16, binary=binary, marker=page['marker']))
            if account:
                check('ledger_entry', dict(base, account_root=account, binary=binary))
                check('account_tx', {'account': account, 'api_version': version, 'ledger_index_min': args.first,
                                    'ledger_index_max': args.last, 'limit': 100, 'binary': binary})
            for tx in txs:
                check('tx', {'transaction': tx, 'api_version': version, 'binary': binary})
        if account:
            for method in ('account_info', 'account_objects', 'account_lines', 'account_offers', 'account_nfts', 'account_currencies', 'gateway_balances'):
                check(method, dict(base, account=account))
            for method in ('account_objects', 'account_lines'):
                first_page = check(method, dict(base, account=account, limit=10))
                if 'marker' in first_page:
                    check(method, dict(base, account=account, limit=10, marker=first_page['marker']))
        for offer in offers[:3]:
            def issue(value):
                return {k: value[k] for k in ('currency', 'issuer')} if isinstance(value, dict) else {'currency': 'XRP'}
            check('book_offers', dict(base, taker_pays=issue(offer['TakerPays']), taker_gets=issue(offer['TakerGets']), limit=10))
            check('account_offers', dict(base, account=offer['Account'], limit=10))
    report = {'endpoint': args.endpoint, 'first': args.first, 'last': args.last,
              'passed': sum(x['equal'] for x in records), 'total': len(records), 'comparisons': records}
    Path(args.out).write_text(json.dumps(report, indent=2))
    Path(args.out + '.summary.json').write_text(json.dumps({k: v for k, v in report.items() if k != 'comparisons'} | {'differences': [{k: v for k, v in row.items() if k not in ('actual', 'expected')} for row in records if not row['equal']]}, indent=2))
    return 0 if report['passed'] == report['total'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
