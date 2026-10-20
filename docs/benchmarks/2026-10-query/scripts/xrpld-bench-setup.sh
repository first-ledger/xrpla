#!/usr/bin/env bash
# Restores 8,000,001-8,300,000 into an xrpld database (NuDB) and starts xrpld 3.4.0 on it,
# standalone at ledger 8,300,000, answering RPC on 127.0.0.1:15300 (admin, so no client limits),
# for comparing query speed with XRPLA serving the same ledgers (q-new).
set -eu
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
if [ ! -f xrpld-8m/rippled.cfg ]; then
  rm -rf xrpld-8m; s=$(date +%s)
  nice -n 10 new/bin/xrpla restore --source bucket --publisher-key ~/xrpla-audit/publisher.pub --to nudb --out xrpld-8m --range 8000001-8300000 > xrpld-restore.out 2> xrpld-restore.err
  echo "nudb restore $(( $(date +%s) - s )) s; $(du -sh xrpld-8m | cut -f1)"
fi
sed -i 's/^port = 5007$/port = 15300/' xrpld-8m/rippled.cfg
grep -n "port\|node_db\|type" xrpld-8m/rippled.cfg | head
HASH=$(python3 -c "import json; r=json.load(open('xrpld-restore.out')); print(r.get('last_hash') or r.get('tip_hash') or '')")
echo "last hash $HASH"
(setsid nohup ~/xrpld-3.4.0/root/usr/bin/xrpld --conf ~/store-bench/xrpld-8m/rippled.cfg --standalone --ledger 8300000 --valid > xrpld-run.log 2>&1 < /dev/null &)
for i in $(seq 1 120); do
  if curl -s -m 5 http://127.0.0.1:15300 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers; then break; fi; sleep 5
done
curl -s -m 10 http://127.0.0.1:15300 -d '{"method":"server_info","params":[{}]}' | python3 -c "import json,sys; i=json.load(sys.stdin)['result']['info']; print(i.get('build_version'), i.get('complete_ledgers'), i.get('server_state'))"
curl -s -m 30 http://127.0.0.1:15300 -d '{"method":"ledger","params":[{"ledger_index":8123456,"transactions":true}]}' | cut -c1-200
