#!/usr/bin/env bash
# Throughput under concurrent WebSocket load (load-bench.py), on the same 2014 data (ledgers
# 8,000,001-8,300,000): XRPLA serving the piece cache p14 and the RocksDB store q-compact, and
# xrpld 3.4.0 standalone holding the same ledgers. Each server is pinned to CPUs 0-15 and the
# load generator to CPUs 16-39, one server under load at a time. The machine's other cores run
# the backfills and audits throughout. Results in ~/store-bench/results-load/.
set -u
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
B=~/store-bench/r6/bin/xrpla
R=~/store-bench/results-load; mkdir -p $R
SECONDS_EACH=30; LEVELS=16,64,256
log() { echo "$(date -u +%FT%TZ) $*" | tee -a $R/run.log; }
log "build $($B --version); load $(cut -d' ' -f1-3 /proc/loadavg)"

# xrpld: a WebSocket admin port next to the HTTP one, then restarted pinned to 0-15.
cfg=xrpld-8m/rippled.cfg
grep -q port_ws_admin_local $cfg || {
  cp -a $cfg $cfg.before-load
  sed -i 's/^port_rpc_admin_local$/port_rpc_admin_local\nport_ws_admin_local/' $cfg
  printf '\n[port_ws_admin_local]\nport = 16300\nip = 127.0.0.1\nadmin = 127.0.0.1\nprotocol = ws\n' >> $cfg
}
for p in $(pgrep -f "xrpld-8m/rippled.cfg"); do kill $p; done; sleep 10
(setsid nohup taskset -c 0-15 ~/xrpld-3.4.0/root/usr/bin/xrpld --conf ~/store-bench/$cfg --standalone --ledger 8300000 --valid > xrpld-run.log 2>&1 < /dev/null &)
for i in $(seq 1 120); do curl -s -m 5 http://127.0.0.1:15300 -d '{"method":"server_info","params":[{}]}' | grep -q '"complete_ledgers"' && break; sleep 5; done
log "xrpld up: $(curl -s -m 5 http://127.0.0.1:15300 -d '{"method":"server_info","params":[{}]}' | grep -o '"complete_ledgers":"[^"]*"')"

serve() {  # dir port
  (setsid nohup taskset -c 0-15 $B serve --dir $1 --http 127.0.0.1:$(( $2 - 1000 )) --ws 127.0.0.1:$2 --threads 32 > $R/serve-$1.log 2>&1 < /dev/null &)
  for i in $(seq 1 90); do curl -s -m 5 http://127.0.0.1:$(( $2 - 1000 )) -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
}
stop() { for p in $(pgrep -f "serve --dir $1 "); do kill $p; done; sleep 3; }
G="taskset -c 16-39 python3 load-bench.py"

serve p14 16910
$G corpus 16910 8000001 8300000 $R/corpus.json | tee -a $R/run.log
pid=$(pgrep -f "serve --dir p14 " | head -1)
$G run 16910 $pid $R/corpus.json pieces-warmup common 30 64 > /dev/null
$G run 16910 $pid $R/corpus.json pieces common $SECONDS_EACH $LEVELS | tee -a $R/results.jsonl
$G run 16910 $pid $R/corpus.json pieces full $SECONDS_EACH 64,256 | tee -a $R/results.jsonl
stop p14

serve q-compact 16920
pid=$(pgrep -f "serve --dir q-compact " | head -1)
$G run 16920 $pid $R/corpus.json store-warmup common 30 64 > /dev/null
$G run 16920 $pid $R/corpus.json store common $SECONDS_EACH $LEVELS | tee -a $R/results.jsonl
$G run 16920 $pid $R/corpus.json store full $SECONDS_EACH 64,256 | tee -a $R/results.jsonl
stop q-compact

pid=$(pgrep -f "xrpld-8m/rippled.cfg" | head -1)
$G run 16300 $pid $R/corpus.json xrpld-warmup common 30 64 > /dev/null
$G run 16300 $pid $R/corpus.json xrpld common $SECONDS_EACH $LEVELS | tee -a $R/results.jsonl
log DONE
