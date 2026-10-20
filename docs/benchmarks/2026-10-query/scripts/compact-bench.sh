#!/usr/bin/env bash
# The all-commands benchmark on q-new as restored, then on a compacted copy of it (q-compact),
# to see what the restore's file layout costs reads. Results in parity-new.jsonl and
# parity-compact.jsonl; tables in compact-bench.log.
set -eu
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
table() { python3 - "$1" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.startswith('{"method"')]
rows.sort(key=lambda r: -((r.get('xrpla_ms') or {}).get('p95') or 0))
for r in rows[:9]:
    a, d = r.get('xrpla_ms') or {}, r.get('xrpld_ms') or {}
    print('%-22s xrpla med %8s p95 %8s max %8s | xrpld med %7s p95 %7s' % (r['method'], a.get('median'), a.get('p95'), a.get('max'), d.get('median'), d.get('p95')))
    for s in r.get('slowest', [])[:2]: print('     slow: %s ms (xrpld %s) %s' % (s['xrpla_ms'], s['xrpld_ms'], json.dumps(s['params'])[:150]))
PY
}
run() {  # dir port out
  (setsid nohup new/bin/xrpla serve --dir $1 --http 127.0.0.1:$2 --ws 127.0.0.1:$(( $2 + 1000 )) > /dev/null 2>&1 < /dev/null &)
  for i in $(seq 1 60); do curl -s -m 5 http://127.0.0.1:$2 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
  python3 xrpld_parity.py --xrpld http://127.0.0.1:15300 --xrpla http://127.0.0.1:$2 --range 8000001-8300000 --clio-methods \
    --ledgers 40 --transactions 40 --accounts 150 --objects 400 --books 40 --reference-retries 0 > $3 2> $3.err || true
  pkill -f "serve --dir $1 --http 127.0.0.1:$2" || true
}
: run q-new 15410 parity-new.jsonl
: skipped
: kept
: already compacted
run q-compact 15420 parity-compact.jsonl
echo "== q-compact"; table parity-compact.jsonl
echo DONE
