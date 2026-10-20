#!/usr/bin/env python3
"""Capture bounded, hash-pinned xrpld coverage before a qualification run (stdlib only)."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.request


def rpc(url, method, params=None, attempts=4):
    body = json.dumps({'method': method, 'params': [params or {}]}).encode()
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, body, {'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=60) as response:
                data = response.read(128 * 1024 * 1024 + 1)
            if len(data) > 128 * 1024 * 1024:
                raise ValueError('RPC response exceeds 128 MiB')
            result = json.loads(data)['result']
            if result.get('error') not in ('tooBusy', 'slowDown'):
                if 'error' in result:
                    raise ValueError(f'{method}: {result["error"]}')
                return result
        except (urllib.error.URLError, TimeoutError):
            if attempt + 1 == attempts:
                raise
        if attempt + 1 < attempts:
            time.sleep(min(2 ** attempt, 8))
    raise ValueError(f'{method}: source remains busy')


def choose_range(complete, tip, count, lag):
    """Choose one contiguous range including its preceding snapshot."""
    intervals = []
    for part in complete.split(','):
        if part == 'empty':
            continue
        ends = [int(n) for n in part.split('-')]
        if len(ends) not in (1, 2) or ends[0] < 1 or ends[-1] < ends[0]:
            raise ValueError('Invalid complete_ledgers interval')
        intervals.append((ends[0], ends[-1]))
    for low, high in sorted(intervals, reverse=True):
        last = min(high, tip - lag)
        first = last - count + 1
        if first >= 2 and first - 1 >= low:
            return first, last
    raise ValueError('Insufficient contiguous history including the snapshot ledger')


def pinned(result, sequence, ledger_hash=None):
    if result.get('validated') is not True or int(result['ledger_index']) != sequence:
        raise ValueError('Source returned an unvalidated or different ledger')
    actual = result['ledger_hash'].upper()
    if ledger_hash and actual != ledger_hash.upper():
        raise ValueError('Source returned a different ledger hash')
    return actual


def probe(url, count=16, lag=4, reference=None, capture=None):
    info = rpc(url, 'server_info')['info']
    first, last = choose_range(info['complete_ledgers'], info['validated_ledger']['seq'], count, lag)
    report = {'captured_at': datetime.now(timezone.utc).isoformat(), 'source': url,
              'build_version': info['build_version'], 'git': info.get('git'),
              'reported_network_id': info.get('network_id'),
              'server_state': info['server_state'], 'complete_ledgers': info['complete_ledgers'],
              'range': f'{first}-{last}', 'snapshot_sequence': first - 1,
              'independent_consensus_validation': False, 'ledgers': []}
    types, objects, samples = Counter(), Counter(), {}
    previous = None
    if capture:
        capture.mkdir(parents=True, exist_ok=False)
    for sequence in range(first - 1, last + 1):
        binary = rpc(url, 'ledger', {'ledger_index': sequence, 'binary': True,
                                     'transactions': True, 'expand': True, 'diff': True})
        ledger_hash = pinned(binary, sequence)
        raw = bytes.fromhex(binary['ledger']['ledger_data'])
        if (len(raw) != 118 or int.from_bytes(raw[:4], 'big') != sequence
                or hashlib.sha512(b'LWR\0' + raw).hexdigest()[:64].upper() != ledger_hash):
            raise ValueError('Canonical header hash or sequence mismatch')
        if previous and raw[12:44].hex().upper() != previous:
            raise ValueError('Ledger parent chain mismatch')
        previous = ledger_hash
        document = rpc(url, 'ledger', {'ledger_hash': ledger_hash, 'transactions': True, 'expand': True})
        pinned(document, sequence, ledger_hash)
        txs = document['ledger']['transactions']
        if len(txs) != len(binary['ledger']['transactions']):
            raise ValueError('Binary/JSON transaction counts differ')
        if capture:
            with gzip.open(capture / f'{sequence}.json.gz', 'wt', encoding='utf-8') as out:
                json.dump({'binary': binary, 'json': document}, out, separators=(',', ':'))
        if sequence >= first:
            for tx in txs:
                transaction = tx.get('tx_json', tx)
                kind = transaction['TransactionType']
                types[kind] += 1
                samples.setdefault(kind, {'ledger_index': sequence, 'hash': tx.get('hash', transaction.get('hash'))})
                meta = tx.get('metaData', tx.get('meta', {}))
                for wrapper in meta.get('AffectedNodes', []):
                    for node in wrapper.values():
                        objects[node['LedgerEntryType']] += 1
        report['ledgers'].append({'sequence': sequence, 'hash': ledger_hash,
                                  'transactions': len(txs), 'role': 'snapshot' if sequence < first else 'replay'})
    if reference:
        result = rpc(reference, 'ledger', {'ledger_index': last})
        pinned(result, last, previous)
        report['reference'] = {'source': reference, 'sequence': last, 'hash': previous, 'matched': True}
    features = rpc(url, 'feature').get('features', {})
    report['enabled_amendments'] = sorted(v.get('name', k) for k, v in features.items() if v.get('enabled'))
    start = time.monotonic()
    state = rpc(url, 'ledger_data', {'ledger_hash': report['ledgers'][0]['hash'], 'binary': True, 'limit': 2048})
    pinned(state, first - 1, report['ledgers'][0]['hash'])
    page = state['state']
    sample = {'objects': len(page), 'seconds': time.monotonic() - start,
              'binary_bytes': sum(len(x['data']) // 2 for x in page), 'has_more': 'marker' in state}
    if page and 'marker' in state:
        fraction = int(page[-1]['index'], 16) / 2 ** 256
        sample['estimated_total_objects'] = round(len(page) / fraction) if fraction else None
        sample['estimate_method'] = 'First keyspace prefix; assumes uniformly distributed keys; not a count.'
    report['state_sample'] = sample
    report['transaction_types'] = dict(sorted(types.items()))
    report['affected_object_types'] = dict(sorted(objects.items()))
    report['sample_transactions'] = samples
    report['transaction_count'] = sum(types.values())
    report['coverage_note'] = 'Object counts are metadata occurrences, not unique objects. Enabled amendments do not establish transaction coverage.'
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default='http://127.0.0.1:5005')
    parser.add_argument('--reference', help='Independent RPC endpoint; the final ledger hash must match')
    parser.add_argument('--count', type=int, default=16)
    parser.add_argument('--lag', type=int, default=4)
    parser.add_argument('--capture', type=Path, help='New directory for compressed binary/JSON source evidence')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.count <= 4096 or not 0 <= args.lag <= 100000:
        parser.error('count must be 1..4096 and lag must be 0..100000')
    report = probe(args.source, args.count, args.lag, args.reference, args.capture)
    args.out.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in report.items() if k not in ('ledgers', 'enabled_amendments', 'sample_transactions')}, indent=2))


if __name__ == '__main__':
    main()
