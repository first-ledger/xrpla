#!/usr/bin/env bash
# Per-method query timings: XRPLA serving q-new (store, 8,000,001-8,300,000) against xrpld 3.4.0
# standalone on the same ledgers restored to NuDB (127.0.0.1:15300), every method the parity
# tool covers plus Clio's on XRPLA alone. Prints a table sorted by XRPLA's p95.
set -eu
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
(setsid nohup new/bin/xrpla serve --dir q-new --http 127.0.0.1:15400 --ws 127.0.0.1:16400 > serve-q-new.log 2>&1 < /dev/null &)
for i in $(seq 1 60); do curl -s -m 5 http://127.0.0.1:15400 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
python3 xrpld_parity.py --xrpld http://127.0.0.1:15300 --xrpla http://127.0.0.1:15400 --range 8000001-8300000 --clio-methods \
  --ledgers 40 --transactions 40 --accounts 150 --objects 400 --books 40 --reference-retries 0 > parity-bench.jsonl 2> parity-bench.err || true
pkill -f "serve --dir q-new --http 127.0.0.1:15400" || true
python3 - <<'EOF'
import json
rows = [json.loads(l) for l in open('parity-bench.jsonl') if l.startswith('{"method"')]
summary = [json.loads(l) for l in open('parity-bench.jsonl') if l.startswith('{"overlap"')]
rows.sort(key=lambda r: -((r.get('xrpla_ms') or {}).get('p95') or 0))
print('%-26s %5s %9s %9s %9s | %9s %9s %9s | %s' % ('method', 'n', 'xrpla med', 'p95', 'max', 'xrpld med', 'p95', 'max', 'same/diff'))
for r in rows:
    a, d = r.get('xrpla_ms') or {}, r.get('xrpld_ms') or {}
    print('%-26s %5s %9s %9s %9s | %9s %9s %9s | %s/%s%s' % (r['method'], a.get('n', 0), a.get('median', '-'), a.get('p95', '-'), a.get('max', '-'),
          d.get('median', '-'), d.get('p95', '-'), d.get('max', '-'), r['same'], r['different'], ' (xrpla only)' if r.get('xrpla_only') else ''))
print(summary)
EOF
tail -3 parity-bench.err
