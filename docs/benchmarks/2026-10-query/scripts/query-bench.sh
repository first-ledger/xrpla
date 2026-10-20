#!/usr/bin/env bash
# Query speed of an xrpla store: 8,000,001-8,300,000 restored with the audit's build (old,
# no filters, 128 open files) and with ~/store-bench/new (bloom filters, open-file limit),
# each served over HTTP; the same 2,000 random transactions by hash, 500 ledgers by hash and
# 500 account_info, one request at a time.
set -eu
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
for build in old new; do
  bin=$([ $build = old ] && echo ~/xrpla-audit/inst34/bin/xrpla || echo ~/store-bench/new/bin/xrpla)
  rm -rf q-$build; s=$(date +%s)
  nice -n 10 $bin restore --source bucket --publisher-key ~/xrpla-audit/publisher.pub --to store --out q-$build --range 8000001-8300000 > /dev/null 2> q-$build.err
  echo "$build restore $(( $(date +%s) - s )) s; store $(du -sh q-$build/store | cut -f1); files $(ls q-$build/store/*.sst | wc -l)"
done
python3 - <<'EOF'
import json, random, subprocess, time, urllib.request, os
env = dict(os.environ)
def call(port, method, params):
    req = urllib.request.Request(f'http://127.0.0.1:{port}', json.dumps({'method': method, 'params': [params]}).encode(), {'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as r: return json.load(r)['result']
servers = {}
for i, build in enumerate(('old', 'new')):
    binary = os.path.expanduser('~/xrpla-audit/inst34/bin/xrpla' if build == 'old' else '~/store-bench/new/bin/xrpla')
    port = 15100 + i
    servers[build] = (port, subprocess.Popen([binary, 'serve', '--dir', f'q-{build}', '--http', f'127.0.0.1:{port}', '--ws', f'127.0.0.1:{16100 + i}'],
                                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env))
for build, (port, _) in servers.items():
    for _ in range(120):
        try: call(port, 'server_info', {}); break
        except Exception: time.sleep(1)
random.seed(7)
port = servers['new'][0]
hashes, ledgers, accounts = [], [], []
for seq in random.sample(range(8000001, 8300001), 700):
    r = call(port, 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True})
    ledgers.append(r['ledger_hash'])
    for tx in r['ledger'].get('transactions', []):
        hashes.append(tx['hash']); accounts.append(tx['Account'])
hashes = random.sample(hashes, min(2000, len(hashes))); ledgers = ledgers[:500]; accounts = random.sample(sorted(set(accounts)), min(500, len(set(accounts))))
print('requests: %d tx, %d ledgers, %d accounts' % (len(hashes), len(ledgers), len(accounts)))
for build in ('old', 'new', 'old', 'new'):
    port = servers[build][0]; out = []
    for name, method, items, make in (('tx', 'tx', hashes, lambda h: {'transaction': h}),
                                      ('ledger by hash', 'ledger', ledgers, lambda h: {'ledger_hash': h}),
                                      ('account_info', 'account_info', accounts, lambda a: {'account': a, 'ledger_index': 8299999})):
        times = []
        for item in items:
            s = time.perf_counter(); r = call(port, method, make(item)); times.append(time.perf_counter() - s)
            assert r.get('status') == 'success' or 'error' not in r, (method, r)
        times.sort()
        out.append('%s %.2f ms median, %.2f ms p99' % (name, 1000 * times[len(times) // 2], 1000 * times[int(len(times) * 0.99)]))
    print(build + ':', '; '.join(out))
for _, (port, process) in servers.items(): process.terminate()
EOF
echo DONE
