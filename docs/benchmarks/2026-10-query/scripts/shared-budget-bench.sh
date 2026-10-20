#!/usr/bin/env bash
# The piece cache with one shared budget (segments 60%, pages 30%, snapshot blocks 10%
# guaranteed, each borrowing what the others leave free), the all-commands benchmark cold then
# warm at 3,072 and 1,536 MB, to compare with the fixed split in ../page-share/.
# Results in ~/store-bench/results-shared-budget/.
set -u
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
B=~/store-bench/sb/bin/xrpla
R=~/store-bench/results-shared-budget; mkdir -p $R
log() { echo "$(date -u +%FT%TZ) $*" | tee -a $R/run.log; }
log "build $(sha256sum $B | cut -c1-16); load $(cut -d' ' -f1-3 /proc/loadavg)"
for mb in 3072 1536; do
  for pct in shared; do
    (setsid nohup $B serve --dir p14 --http 127.0.0.1:15930 --ws 127.0.0.1:16930 --cache-mb $mb > $R/serve-$mb-$pct.log 2>&1 < /dev/null &)
    for i in $(seq 1 90); do curl -s -m 5 http://127.0.0.1:15930 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
    for phase in cold warm; do
      s=$(date +%s)
      python3 xrpld_parity.py --xrpld http://127.0.0.1:15300 --xrpla http://127.0.0.1:15930 --range 8000001-8300000 --clio-methods \
        --ledgers 40 --transactions 40 --accounts 150 --objects 400 --books 40 --reference-retries 0 > $R/parity-$mb-$pct-$phase.jsonl 2> $R/parity-$mb-$pct-$phase.err
      log "$mb MB pages $pct% $phase: $(( $(date +%s) - s )) s; $(tail -1 $R/parity-$mb-$pct-$phase.jsonl | cut -c1-100)"
    done
    curl -s -m 10 http://127.0.0.1:15930 -d '{"method":"xrpla_cache_stats","params":[{}]}' > $R/cache-stats-$mb-$pct.json
    log "cache stats $mb $pct: $(cat $R/cache-stats-$mb-$pct.json)"
    pkill -f "serve --dir p14 --http 127.0.0.1:15930" || true; sleep 3
  done
done
log DONE
