#!/usr/bin/env python3
"""Record xrpld evidence, then compare canonical data with an offline XRPLA store.

This deliberately compares a documented data projection, not full Clio/xrpld API
envelopes. Capture while source ledgers remain available; check can run offline.
"""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

from node_probe import rpc, pinned


def collect(method, params, request):
    """Drain NFT pages independently: xrpld and Clio use different limit units."""
    result = request(method, params)
    if method != 'account_nfts' or 'error' in result:
        return result
    combined = dict(result)
    tokens, markers, ids = [], set(), set()
    for _ in range(1024):
        pinned(result, int(combined['ledger_index']), combined['ledger_hash'])
        for token in result['account_nfts']:
            nft = token['NFTokenID'].upper()
            if nft in ids:
                raise ValueError('NFT pagination repeated a token')
            ids.add(nft); tokens.append(token)
        if 'marker' not in result:
            combined['account_nfts'] = tokens
            combined.pop('marker', None)
            return combined
        marker = json.dumps(result['marker'], sort_keys=True)
        if marker in markers:
            raise ValueError('NFT pagination marker repeated')
        markers.add(marker)
        result = request(method, dict(params, marker=result['marker']))
    raise ValueError('NFT pagination exceeds 1024 pages')


def project(method, result):
    if 'error' in result:
        return {'error': result['error']}
    if result.get('validated') is not True:
        raise ValueError(f'{method}: result is not validated')
    if method == 'ledger':
        ledger = result['ledger']
        return {'ledger_hash': result['ledger_hash'].upper(), 'ledger_index': int(result['ledger_index']),
                'header': ledger['ledger_data'].upper(),
                'transactions': sorted((tx.get('tx_blob', tx.get('tx')).upper(), tx['meta'].upper())
                                       for tx in ledger['transactions'])}
    if method == 'ledger_entry':
        return {key: result[key].upper() for key in ('index', 'node_binary', 'ledger_hash')}
    if method == 'account_info':
        return {'account_data': result['account_data'], 'ledger_hash': result['ledger_hash'].upper()}
    if method == 'account_nfts':
        return {'account_nfts': sorted(result['account_nfts'], key=lambda x: x['NFTokenID']),
                'ledger_hash': result['ledger_hash'].upper(), 'more': 'marker' in result}
    if method == 'tx':
        return {'tx_blob': result.get('tx_blob', result.get('tx')).upper(),
                'metadata': result.get('meta', result.get('meta_blob')).upper(),
                'ledger_index': int(result['ledger_index']), 'hash': result['hash'].upper()}
    raise ValueError(f'No projection defined for {method}')


def capture(args):
    report = json.loads(args.probe.read_text())
    first, last = map(int, report['range'].split('-'))
    rows, accounts, nft_accounts = [], set(), set()
    objects, transaction_samples = defaultdict(dict), defaultdict(dict)
    for sequence in range(first, last + 1):
        with gzip.open(args.source_data / f'{sequence}.json.gz', 'rt', encoding='utf-8') as source:
            data = json.load(source)
        binary, expanded = data['binary'], data['json']
        expected_hash = next(x['hash'] for x in report['ledgers'] if x['sequence'] == sequence)
        pinned(binary, sequence, expected_hash)
        pinned(expanded, sequence, expected_hash)
        rows.append({'method': 'ledger', 'params': {'ledger_index': sequence, 'binary': True,
                    'transactions': True, 'expand': True}, 'expected': binary})
        for tx in expanded['ledger']['transactions']:
            transaction = tx.get('tx_json', tx)
            kind = transaction['TransactionType']
            tx_hash = tx.get('hash', transaction.get('hash'))
            if len(transaction_samples[kind]) < 2:
                transaction_samples[kind][tx_hash] = sequence
                if 'Account' in transaction:
                    accounts.add(transaction['Account'])
            if kind.startswith('NFToken'):
                nft_accounts.add(transaction['Account'])
            for wrapper in tx.get('metaData', tx.get('meta', {})).get('AffectedNodes', []):
                for node in wrapper.values():
                    kind = node['LedgerEntryType']
                    if len(objects[kind]) < 3:
                        objects[kind][node['LedgerIndex']] = sequence
    # Everything below is explicitly pinned before the source prunes it.
    last_hash = report['ledgers'][-1]['hash']
    def add(method, params):
        expected = collect(method, params, lambda m, p: rpc(args.source, m, p))
        if method != 'tx':
            pinned(expected, last, last_hash)
        elif int(expected['ledger_index']) != params['min_ledger']:
            raise ValueError('Transaction ledger mismatch')
        row = {'method': method, 'params': params, 'expected': expected}
        if method == 'account_nfts':
            row['pagination'] = 'complete-collection'
        rows.append(row)
        project(method, expected)
    for samples in transaction_samples.values():
        for tx_hash, sequence in samples.items():
            add('tx', {'transaction': tx_hash, 'binary': True, 'min_ledger': sequence, 'max_ledger': sequence})
    for samples in objects.values():
        for key in samples:
            params = {'ledger_hash': last_hash, 'index': key, 'binary': True}
            try:
                add('ledger_entry', params)
            except ValueError as error:
                if str(error) != 'ledger_entry: entryNotFound':
                    raise
    for account in sorted(accounts)[:16]:
        add('account_info', {'ledger_hash': last_hash, 'account': account})
    for account in sorted(nft_accounts)[:16]:
        add('account_nfts', {'ledger_hash': last_hash, 'account': account, 'limit': 32})
    result = {'profile': 'xrpld-canonical-data-v1', 'source_build_version': report['build_version'],
              'range': report['range'], 'reference': report.get('reference'), 'comparisons': rows}
    args.out.write_text(json.dumps(result, separators=(',', ':')) + '\n', encoding='utf-8')
    print(json.dumps({'recorded': len(rows), 'methods': sorted({x['method'] for x in rows})}))


def check(args):
    recorded = json.loads(args.recorded.read_text())
    rows = []
    def local(method, params):
        process = subprocess.run([args.binary, 'rpc', '--dir', str(args.dir), '--request', '-'],
                                 input=json.dumps({'method': method, 'params': params}),
                                 capture_output=True, text=True, timeout=120, check=True)
        return json.loads(process.stdout)
    for row in recorded['comparisons']:
        method, params = row['method'], row['params']
        if method == 'account_nfts' and row.get('pagination') != 'complete-collection':
            raise ValueError('Recapture NFT evidence with complete pagination')
        actual = collect(method, params, local)
        # JSON round-trip gives tuples/lists the same representation as stored evidence.
        expected = json.loads(json.dumps(project(method, row['expected'])))
        projected = json.loads(json.dumps(project(method, actual)))
        result = {'method': method, 'params': params, 'equal': projected == expected}
        if not result['equal']:
            result.update(actual=projected, expected=expected)
        rows.append(result)
        print(f'{method}: {"PASS" if result["equal"] else "DIFFERENT"}', flush=True)
    report = {'profile': recorded['profile'], 'range': recorded['range'],
              'recording_sha256': hashlib.sha256(args.recorded.read_bytes()).hexdigest(),
              'passed': sum(x['equal'] for x in rows), 'total': len(rows), 'comparisons': rows}
    args.out.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in report.items() if k != 'comparisons'}))
    return 0 if report['passed'] == report['total'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    record = modes.add_parser('capture')
    record.add_argument('--source', default='http://127.0.0.1:5005')
    record.add_argument('--source-data', type=Path, required=True)
    record.add_argument('--probe', type=Path, required=True)
    compare = modes.add_parser('check')
    compare.add_argument('--recorded', type=Path, required=True)
    compare.add_argument('--binary', required=True)
    compare.add_argument('--dir', type=Path, required=True)
    for sub in (record, compare):
        sub.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    return capture(args) if args.mode == 'capture' else check(args)


if __name__ == '__main__':
    raise SystemExit(main())
