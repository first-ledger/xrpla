#!/usr/bin/env bash
# The piece cache benchmarks of round 4 again, after round 5: several resume points per
# segment, leaner dictionaries (values as offsets, 12-byte atoms) loaded with decompression and
# checksum overlapping the parse, book_offers price levels from one key walk, and TCP_NODELAY.
# At the default budget (an eighth of memory: 15.6 GB here) and at 3072 MB.
# Results in ~/store-bench/results-pieces5/.
set -u
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
B=~/store-bench/r5/bin/xrpla
R=~/store-bench/results-pieces5; mkdir -p $R
log() { echo "$(date -u +%FT%TZ) $*" | tee -a $R/run.log; }
log "build $(sha256sum $B | cut -c1-16); load $(cut -d' ' -f1-3 /proc/loadavg)"
start() {  # dir port cache_mb(or 0)
  local extra=""; [ "$3" != 0 ] && extra="--cache-mb $3"
  (setsid nohup $B serve --dir $1 --http 127.0.0.1:$2 --ws 127.0.0.1:$(( $2 + 1000 )) $extra > $R/serve-$1-$3.log 2>&1 < /dev/null &)
  for i in $(seq 1 90); do curl -s -m 5 http://127.0.0.1:$2 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
}
stop() { pkill -f "serve --dir $1 --http 127.0.0.1:$2" || true; sleep 2; }
for budget in 0 3072; do
  label=$([ $budget = 0 ] && echo auto || echo 3gb)
  start p14 15910 $budget
  for phase in cold warm; do
    s=$(date +%s)
    python3 xrpld_parity.py --xrpld http://127.0.0.1:15300 --xrpla http://127.0.0.1:15910 --range 8000001-8300000 --clio-methods \
      --ledgers 40 --transactions 40 --accounts 150 --objects 400 --books 40 --reference-retries 0 > $R/parity-pieces-$label-$phase.jsonl 2> $R/parity-pieces-$label-$phase.err
    log "parity pieces $label $phase: $(( $(date +%s) - s )) s; $(tail -1 $R/parity-pieces-$label-$phase.jsonl | cut -c1-120)"
  done
  curl -s -m 10 http://127.0.0.1:15910 -d '{"method":"xrpla_cache_stats","params":[{}]}' > $R/cache-stats-$label.json
  log "cache stats $label: $(cat $R/cache-stats-$label.json | cut -c1-300)"
  stop p14 15910
  start p25 15920 $budget
  for phase in cold warm; do python3 nft-bench.py http://127.0.0.1:15920 91000001 91050000 > $R/nft-pieces-$label-$phase.txt 2>&1; log "nft pieces $label $phase done"; done
  stop p25 15920
done
log "latency"
python3 latency-bench.py $B 8000001 8300000 pieces=p14 > $R/latency-pieces-auto.jsonl 2> $R/latency-pieces-auto.err
log DONE
