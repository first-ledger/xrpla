#!/usr/bin/env python3
"""Latency of common queries against an `xrpla serve` endpoint.

Samples random ledgers from the server's range, takes transactions and accounts
from them, then times each method twice (cold, then warm). Prints one JSON line
per method with median, 95th percentile and maximum milliseconds.

  query_bench.py --url http://127.0.0.1:5015 [--samples 40]
"""
import argparse
from collections import defaultdict
import json
import random
import statistics
import time
import urllib.request


def call(url, method, params):
    body = json.dumps({'method': method, 'params': [params]}).encode()
    started = time.perf_counter()
    with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=600) as r:
        result = json.loads(r.read())['result']
    return result, (time.perf_counter() - started) * 1000


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--url', required=True); p.add_argument('--samples', type=int, default=40); p.add_argument('--seed', type=int, default=3)
    a = p.parse_args(); rng = random.Random(a.seed)
    span, _ = call(a.url, 'ledger_range', {})
    first, last = span['ledger_index_min'], span['ledger_index_max']
    times = defaultdict(lambda: {'cold': [], 'warm': []}); errors = defaultdict(int)

    def timed(method, **params):
        result = None
        for phase in ('cold', 'warm'):
            result, ms = call(a.url, method, params)
            times[method][phase].append(ms)
            if 'error' in result: errors[method] += 1
        return result

    txs, accounts = [], []
    for _ in range(a.samples):
        seq = rng.randint(first, last)
        ledger = timed('ledger', ledger_index=seq, transactions=True, expand=True, api_version=2)['ledger']
        for tx in rng.sample(ledger['transactions'], min(2, len(ledger['transactions']))):
            txs.append(tx['hash']); accounts.append((tx['tx_json']['Account'], seq))
    for h in txs: timed('tx', transaction=h, api_version=2)
    for account, seq in accounts:
        timed('account_info', account=account, ledger_index=seq)
        timed('account_lines', account=account, ledger_index=seq, limit=50)
        timed('account_tx', account=account, limit=20, api_version=2)
        timed('account_tx', account=account, limit=20, ledger_index_min=max(first, seq - 1000), ledger_index_max=seq, api_version=2)
    for _ in range(a.samples // 4):
        timed('ledger_data', ledger_index=rng.randint(first, last), limit=100, binary=True)
        timed('book_offers', taker_gets={'currency': 'XRP'}, taker_pays={'currency': 'USD', 'issuer': 'rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq'},
              ledger_index=rng.randint(first, last), limit=20)
    for method, t in sorted(times.items()):
        row = {'method': method, 'calls': len(t['cold']), 'errors': errors[method]}
        for phase, values in t.items():
            values = sorted(values)
            row[phase] = {'p50_ms': round(statistics.median(values), 1), 'p95_ms': round(values[int(0.95 * (len(values) - 1))], 1), 'max_ms': round(values[-1], 1)}
        print(json.dumps(row))


if __name__ == '__main__':
    main()
