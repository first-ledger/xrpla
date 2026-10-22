#!/usr/bin/env bash
# Memory under concurrent public load: a second `xrpla serve` on the test node's cache, on
# 127.0.0.1:5205 under a memory cap, loaded by scale_load.py in phases.
#   campaign.sh LABEL CACHE_MB MEMORY_MAX PHASE...    (PHASE = SECONDS:CLIENTS:MIX[:SLOW])
# XRPLA_BIN picks the build tested (default: the public node's own).
set -u
bin=${XRPLA_BIN:-/home/ubuntu/xrpla-test/bin/xrpla}
cd ~/scale
label=$1 cache=$2 max=$3; shift 3
out=runs/$label; mkdir -p "$out"
unit=xrpla-scale-$label
echo "$(date -u +%FT%TZ) scale test $label start (cache $cache MB, MemoryMax $max, $bin)" >> ~/xrpla-test/results/experiments.log
sudo systemd-run --scope -p MemoryMax="$max" -p MemorySwapMax=0 --uid=ubuntu --unit="$unit" \
  taskset -c 0-9 "$bin" serve --dir /home/ubuntu/xrpla-test/data/cache \
  --http 127.0.0.1:5205 --ws 127.0.0.1:6206 --threads 16 --cache-mb "$cache" --no-loopback-trust --network-id 0 \
  > "$out/serve.log" 2>&1 < /dev/null &
for i in $(seq 1 60); do curl -s -m 3 -d '{"method":"ledger_range"}' http://127.0.0.1:5205/ | grep -q ledger_index_max && break; sleep 2; done
# The server itself: the newest process with its command line (sudo, its parent, has it too).
pid=$(pgrep -n -f -- "serve --dir /home/ubuntu/xrpla-test/data/cache --http 127.0.0.1:5205")
python3 memwatch.py "$pid" http://127.0.0.1:5205/ "/sys/fs/cgroup/system.slice/$unit.scope" "$out/mem.jsonl" &
for phase in "$@"; do
  IFS=: read -r seconds clients mix slow <<< "$phase"
  kill -0 "$pid" 2>/dev/null || { echo "{\"phase\": \"$phase\", \"server\": \"gone\"}" >> "$out/load.jsonl"; continue; }
  echo "$(date -u +%s) $phase start" >> "$out/phases.log"
  taskset -c 10-15 python3 scale_load.py 127.0.0.1 5205 corpus.json "$seconds" "$clients" "$mix" --slow-readers "${slow:-0}" >> "$out/load.jsonl"
  echo "$(date -u +%s) $phase end" >> "$out/phases.log"
  sleep 20  # the quiet after a phase: what memory goes back
done
sudo systemctl stop "$unit.scope" 2>/dev/null
sudo journalctl -u "$unit.scope" --no-pager | tail -3 > "$out/unit.log"
echo "$(date -u +%FT%TZ) scale test $label end" >> ~/xrpla-test/results/experiments.log
echo done > "$out/done"
