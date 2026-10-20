#!/usr/bin/env bash
# C (docs/internal/PARITY-PLAN.md comparison 5): Clio 2.8.0 on the keyspace xrpla restore --to clio loaded
# from R2 (107,000,001-107,707,250), taking new ledgers from this box's xrpld (ws 6006, gRPC 50051),
# on cores 16-39 as X and A' were.
cd ~/parity-c
date -u +%FT%TZ > clio.started
exec taskset -c 16-39 clio/clio_server --conf /home/ubuntu/parity-c/clio.json >> clio.out 2>&1 < /dev/null
