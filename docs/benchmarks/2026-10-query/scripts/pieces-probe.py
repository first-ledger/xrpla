#!/usr/bin/env python3
"""Where a piece cache's slow requests spend their time: starts `xrpla serve` on p14, runs the
all-commands benchmark once (cold), prints the cache statistics (hit rates, and load time by
stage: segment open, dictionary, page decode, snapshot block), then times individual slow
requests on a fresh server with the statistics before and after each."""
import json, subprocess, time, urllib.request, os
B = os.path.expanduser('~/store-bench/new/bin/xrpla')
def call(port, method, params):
    req = urllib.request.Request(f'http://127.0.0.1:{port}', json.dumps({'method': method, 'params': [params]}).encode(), {'Content-Type': 'application/json'})
    s = time.perf_counter()
    with urllib.request.urlopen(req, timeout=300) as r: out = json.load(r)['result']
    return out, time.perf_counter() - s
def serve(port):
    p = subprocess.Popen([B, 'serve', '--dir', 'p14', '--http', f'127.0.0.1:{port}', '--ws', f'127.0.0.1:{port + 1000}'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(120):
        try: call(port, 'server_info', {}); return p
        except Exception: time.sleep(1)
os.chdir(os.path.expanduser('~/store-bench'))
p = serve(15800)
subprocess.run(['python3', 'xrpld_parity.py', '--xrpld', 'http://127.0.0.1:15300', '--xrpla', 'http://127.0.0.1:15800', '--range', '8000001-8300000', '--clio-methods',
                '--ledgers', '40', '--transactions', '40', '--accounts', '150', '--objects', '400', '--books', '40', '--reference-retries', '0'],
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print('after one benchmark pass:', json.dumps(call(15800, 'xrpla_cache_stats', {})[0]))
p.terminate(); p.wait()
requests = [
    ('account_info', {'account': 'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYM', 'ledger_index': 8255778}),
    ('account_mptokens', {'account': 'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYM', 'ledger_index': 8255778}),
    ('account_lines', {'account': 'rM3X3QSr8icjTGpaF52dozhbT2BZSXJQYM', 'ledger_index': 8113561}),
    ('ledger_data', {'ledger_index': 8300000, 'limit': 200, 'type': 'offer'}),
    ('ledger_data', {'ledger_index': 8300000, 'limit': 200}),
    ('noripple_check', {'account': 'rMAz5ZnK73nyNUL4foAvaxdreczCkG3vA6', 'ledger_index': 8221311, 'role': 'gateway', 'transactions': True, 'limit': 20}),
    ('gateway_balances', {'account': 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B', 'ledger_index': 8133730}),
    ('book_offers', {'taker_pays': {'currency': 'USD', 'issuer': 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'}, 'taker_gets': {'currency': 'XRP'}, 'ledger_index': 8200000, 'limit': 20}),
    ('account_tx', {'account': 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B', 'ledger_index_min': 8000001, 'ledger_index_max': 8300000, 'limit': 10}),
]
p = serve(15810)
def load(stats): return {k: round(v, 1) for k, v in stats['load_ms'].items()}
for method, params in requests:
    for attempt in ('first', 'again'):
        before = call(15810, 'xrpla_cache_stats', {})[0]
        out, seconds = call(15810, method, params)
        after = call(15810, 'xrpla_cache_stats', {})[0]
        delta = {k: round(after['load_ms'][k] - before['load_ms'][k], 1) for k in after['load_ms']}
        misses = {c: after[c].get('misses', 0) - before[c].get('misses', 0) for c in ('archives', 'pages', 'snapshot_blocks') if isinstance(after.get(c), dict)}
        print(f"{method:18s} {attempt:5s} {1000 * seconds:9.1f} ms  load {delta}  misses {misses}  {'error ' + str(out.get('error')) if 'error' in out else ''}", flush=True)
p.terminate(); p.wait()
print('DONE')
