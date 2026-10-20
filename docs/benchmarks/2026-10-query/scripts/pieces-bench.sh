#!/usr/bin/env bash
# Compressed piece cache against the RocksDB store and xrpld 3.4.0, on the same ledgers, with
# the same requests (build: ~/store-bench/new, 6e084e3). Results (JSON lines and logs) go to
# ~/store-bench/results-pieces/ for the benchmark documentation.
#  1. Piece caches: 8,000,001-8,300,000 (2014) from `bucket` and 91,000,001-91,050,000 (2025)
#     from `bucket25`: sync and index-pieces times and sizes, each index file type.
#  2. All-commands benchmark against xrpld (127.0.0.1:15300) for the 2014 piece cache and store
#     q-book: a fresh server (cold), then the same requests again (warm).
#  3. HTTP/WebSocket latency (latency-bench.py), piece cache and store.
#  4. NFT benchmark on the 2025 piece cache and store n25, cold then warm.
set -u
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
B=~/store-bench/new/bin/xrpla
R=~/store-bench/results-pieces; mkdir -p $R
log() { echo "$(date -u +%FT%TZ) $*" | tee -a $R/run.log; }
log "build $(sha256sum $B | cut -c1-16); load $(cut -d' ' -f1-3 /proc/loadavg); cores $(nproc); memory $(free -g | awk '/Mem/{print $2}') GB"
cache() {  # dir bucket range
  rm -rf $1; local s=$(date +%s)
  nice -n 10 $B sync --source $2 --publisher-key ~/xrpla-audit/publisher.pub --dir $1 --range $3 > $R/$1-sync.json 2> $R/$1-sync.err
  local synced=$(( $(date +%s) - s )); s=$(date +%s)
  nice -n 10 $B index-pieces --dir $1 > $R/$1-index.json 2> $R/$1-index.err
  log "$1 ($3): sync $synced s, index-pieces $(( $(date +%s) - s )) s; total $(du -s --block-size=1M $1 | cut -f1) MB:" \
      "$(for d in bundles pieces snapshots query; do [ -d $1/$d ] && echo -n "$d $(du -s --block-size=1M $1/$d | cut -f1) MB; "; done)" \
      "$(for e in $(ls $1/query 2>/dev/null | sed 's/.*\.//' | sort -u); do echo -n ".$e $(du -c --block-size=1M $1/query/*.$e | tail -1 | cut -f1) MB; "; done)"
}
cache p14 bucket 8000001-8300000
cache p25 bucket25 91000001-91050000
log "store q-book $(du -s --block-size=1M q-book/store | cut -f1) MB; store n25 $(du -s --block-size=1M n25/store | cut -f1) MB; xrpld-8m $(du -s --block-size=1M xrpld-8m | cut -f1) MB"
parity() {  # label dir port
  (setsid nohup $B serve --dir $2 --http 127.0.0.1:$3 --ws 127.0.0.1:$(( $3 + 1000 )) > $R/serve-$1.log 2>&1 < /dev/null &)
  for i in $(seq 1 90); do curl -s -m 5 http://127.0.0.1:$3 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
  for phase in cold warm; do
    local s=$(date +%s)
    python3 xrpld_parity.py --xrpld http://127.0.0.1:15300 --xrpla http://127.0.0.1:$3 --range 8000001-8300000 --clio-methods \
      --ledgers 40 --transactions 40 --accounts 150 --objects 400 --books 40 --reference-retries 0 > $R/parity-$1-$phase.jsonl 2> $R/parity-$1-$phase.err
    log "parity $1 $phase: $(( $(date +%s) - s )) s; $(tail -1 $R/parity-$1-$phase.jsonl | cut -c1-160)"
  done
  pkill -f "serve --dir $2 --http 127.0.0.1:$3" || true; sleep 2
}
parity pieces p14 15610
parity store q-book 15620
log "latency"
python3 latency-bench.py $B 8000001 8300000 pieces=p14 store=q-book > $R/latency-2014.jsonl 2> $R/latency-2014.err
nft() {  # label dir port
  (setsid nohup $B serve --dir $2 --http 127.0.0.1:$3 --ws 127.0.0.1:$(( $3 + 1000 )) > $R/serve-$1.log 2>&1 < /dev/null &)
  for i in $(seq 1 90); do curl -s -m 5 http://127.0.0.1:$3 -d '{"method":"server_info","params":[{}]}' | grep -q complete_ledgers && break; sleep 2; done
  for phase in cold warm; do python3 nft-bench.py http://127.0.0.1:$3 91000001 91050000 > $R/nft-$1-$phase.txt 2>&1; log "nft $1 $phase done"; done
  pkill -f "serve --dir $2 --http 127.0.0.1:$3" || true; sleep 2
}
nft pieces p25 15630
nft store n25 15640
log DONE
