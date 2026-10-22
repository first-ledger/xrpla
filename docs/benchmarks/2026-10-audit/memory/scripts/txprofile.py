#!/usr/bin/env python3
"""Where an account_tx request's time goes, one request at a time against a quiet server: for a
sample of accounts, limit 20/200/400, newest first (wallets) and oldest first (indexers), each
cold and then repeated warm. Per request: seconds, transactions, distinct segments the answer's
ledgers fall in, and the decode caches' deltas (dictionaries loaded, segment opens, page decodes,
milliseconds in each) from xrpla_cache_stats.

  txprofile.py URL CORPUS.json OUT.jsonl [ACCOUNTS] [LEDGERS_PER_SEGMENT]
"""
import json, random, sys, time, urllib.request

url, corpus_path, out = sys.argv[1], sys.argv[2], sys.argv[3]
count = int(sys.argv[4]) if len(sys.argv) > 4 else 30
per_segment = int(sys.argv[5]) if len(sys.argv) > 5 else 2870


def call(method, params):
    body = json.dumps({'method': method, 'params': [params]}).encode()
    with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=600) as r:
        return json.load(r)['result']


def stats():
    s = call('xrpla_cache_stats', {})
    d, ms = s.get('dictionaries', {}), s.get('load_ms', {})
    return {'dictionaries': d.get('loaded', 0), 'dict_read_ms': d.get('read_ms', 0), 'dict_parse_ms': d.get('parse_ms', 0),
            'segment_open_ms': ms.get('segment_open', 0), 'page_decode_ms': ms.get('page_decode', 0),
            'dictionary_ms': ms.get('dictionary', 0), 'segment_misses': s['archives']['misses'], 'page_misses': s['pages']['misses']}


corpus = json.load(open(corpus_path))
rng = random.Random(5)
accounts = rng.sample(corpus['accounts'], count)
with open(out, 'w') as f:
    for account in accounts:
        for limit in (20, 200, 400):
            for forward in (False, True):
                for run in ('cold', 'warm'):
                    before = stats(); started = time.monotonic()
                    result = call('account_tx', {'account': account, 'limit': limit, 'forward': forward})
                    seconds = time.monotonic() - started; after = stats()
                    ledgers = [t.get('ledger_index') or t.get('tx', {}).get('ledger_index') for t in result.get('transactions', [])]
                    ledgers = [l for l in ledgers if l]
                    row = {'account': account, 'limit': limit, 'forward': forward, 'run': run, 'seconds': round(seconds, 3),
                           'transactions': len(ledgers), 'segments': len({l // per_segment for l in ledgers}),
                           'span': [min(ledgers), max(ledgers)] if ledgers else None,
                           **{k: round(after[k] - before[k], 1) for k in after}}
                    f.write(json.dumps(row) + '\n'); f.flush()
