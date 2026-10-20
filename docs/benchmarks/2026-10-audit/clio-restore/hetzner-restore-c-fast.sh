#!/usr/bin/env bash
# Comparison 5 with the fast Clio path (parallel plan, streaming parallel load): the same out
# directory, so the verified staging store of the baseline run is reused (extended to the tip).
cd ~/parity-c
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1 XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
date -u +%FT%TZ > restore.started
taskset -c 16-39 nice -n 10 ionice -c2 -n7 /usr/bin/time -v inst-fast/bin/xrpla restore --source https://xrpla-data.firstledger.net/mainnet \
  --publisher-key publisher.pub --to clio --out /home/ubuntu/parity-c/restore --range 107000001- \
  --host 127.0.0.1 --port 9042 --keyspace clio --jobs 16 --load-jobs 4 > restore.log 2> restore.err
echo "exit $? $(date -u +%FT%TZ)" > restore.finished
