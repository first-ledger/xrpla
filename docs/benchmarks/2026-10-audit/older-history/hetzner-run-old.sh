#!/usr/bin/env bash
# Older history against stock xrpld (docs/internal/PARITY-PLAN.md, after comparison 5): for each of four
# 100,000-ledger stretches across the eras, restore it into xrpld's NuDB and SQLite (xrpla restore
# --to nudb, 543c523, timed), sync the same range as a local piece cache, serve both locally (xrpld
# 3.4.1 standalone on the restored tip; xrpla serve on the cache), and compare them with
# tools/xrpld_parity.py: three seeds of 300 ledgers, every request also over XRPLA's WebSocket.
# xrpld's ports are public (no admin): XRPLA answers as a public xrpld does (an admin has no minimum
# limits, for one). xrpld starts with --force_ledger_present_range over the stretch: standalone, it otherwise counts only the
# loaded ledger as present (and every older one unvalidated), where a networked xrpld finds the restored
# range by walking back through its database. A local xrpld answers alike every time, so a difference is
# asked again once. Cores 16-39; the comparison's client on 40-47. Stops if free disk falls under 150 GB.
cd ~/parity-old
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1 XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
SOURCE=https://xrpla-data.firstledger.net/mainnet
free_gb() { df --output=avail -BG / | tail -1 | tr -dc 0-9; }
k=0
for first in 14000001 38000001 63000001 88000001; do
  last=$((first + 99999)); name=r$((first / 1000000))m; k=$((k + 1)); d=~/parity-old/$name; mkdir -p $d
  [ "$(free_gb)" -lt 150 ] && { echo "$name skipped: low disk" >> run-old.done; continue; }
  # 1. xrpld's database (kept from an earlier run when it finished).
  if ! grep -q "^exit 0" $d/restore.finished 2>/dev/null; then
  date -u +%FT%TZ > $d/restore.started
  taskset -c 16-39 nice -n 10 ionice -c2 -n7 /usr/bin/time -v b/bin/xrpla restore --source $SOURCE --publisher-key publisher.pub \
    --to nudb --out $d/node --range $first-$last --jobs 8 > $d/restore.log 2> $d/restore.err &
  rpid=$!
  # Low disk stops this restore alone (its time(1) wrapper's child), nothing else on the box.
  while kill -0 $rpid 2>/dev/null; do
    f=$(free_gb); echo "{\"utc\":\"$(date -u +%FT%TZ)\",\"free_gb\":$f,\"load\":\"$(cut -d' ' -f1-3 /proc/loadavg)\"}" >> $d/restore-watch.jsonl
    [ "$f" -lt 150 ] && { kill -INT $(pgrep -P $rpid) 2>/dev/null; echo low-disk >> $d/restore-watch.jsonl; }
    sleep 30
  done
  wait $rpid; code=$?
  echo "exit $code $(date -u +%FT%TZ)" > $d/restore.finished; du -sb $d/node >> $d/restore.finished
  [ $code = 0 ] || { echo "$name restore failed ($code)" >> run-old.done; continue; }
  # 2. XRPLA's piece cache of the same range.
  taskset -c 16-39 /usr/bin/time -v b/bin/xrpla sync --source $SOURCE --publisher-key publisher.pub --dir $d/cache --range $first-$last --jobs 8 > $d/sync.log 2> $d/sync.err
  echo "sync exit $?" >> $d/restore.finished
  fi
  # 3. Both servers.
  cat > $d/xrpld.cfg <<C
[server]
port_rpc
port_ws
[port_rpc]
port = $((5300 + k))
ip = 127.0.0.1
protocol = http
[port_ws]
port = $((6300 + k))
ip = 127.0.0.1
protocol = ws
[database_path]
$d/node
[node_db]
type = NuDB
path = $d/node/node-db
[ledger_history]
full
[fetch_depth]
full
[debug_logfile]
$d/xrpld.log
[workers]
16
C
  (cd $d && setsid nohup taskset -c 16-39 /usr/bin/xrpld --conf $d/xrpld.cfg --standalone --ledger $last --valid --force_ledger_present_range=$first,$last > $d/xrpld.out 2>&1 < /dev/null &)
  (cd $d && setsid nohup taskset -c 16-39 ~/parity-old/b/bin/xrpla serve --dir $d/cache --http 127.0.0.1:$((5400 + k)) --ws 127.0.0.1:$((6400 + k)) --threads 8 --cache-mb 8192 > $d/serve.log 2>&1 < /dev/null &)
  for i in $(seq 1 240); do curl -s -m 3 http://127.0.0.1:$((5300 + k)) -d '{"method":"server_info","params":[{}]}' | grep -q "\"complete_ledgers\":\"$first-$last\"" && break; sleep 5; done
  for i in $(seq 1 60); do curl -s -m 3 http://127.0.0.1:$((5400 + k)) -d '{"method":"ledger_range"}' | grep -q success && break; sleep 2; done
  curl -s -m 10 http://127.0.0.1:$((5300 + k)) -d '{"method":"server_info","params":[{}]}' > $d/xrpld-up.json
  curl -s -m 10 http://127.0.0.1:$((5400 + k)) -d '{"method":"ledger_range"}' > $d/xrpla-up.json
  # 4. The comparison.
  for seed in 1 2 3; do
    taskset -c 40-47 python3 tools/xrpld_parity.py --xrpld http://127.0.0.1:$((5300 + k)) --xrpla http://127.0.0.1:$((5400 + k)) \
      --transports ws://127.0.0.1:$((6400 + k)) --range $first-$last --ledgers 300 --seed $seed --reference-retries 1 > $d/compare-seed$seed.jsonl 2> $d/compare-seed$seed.err
    echo "$name seed $seed exit $? $(date -u +%FT%TZ)" >> run-old.done
  done
  for p in $(pgrep -f "^/usr/bin/xrpld --conf $d/xrpld.cfg") $(pgrep -f "^/home/ubuntu/parity-old/b/bin/xrpla serve --dir $d/cache"); do kill $p; done; sleep 10
done
echo ALL-DONE >> run-old.done
