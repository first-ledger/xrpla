#!/usr/bin/env bash
# C's database (docs/internal/PARITY-PLAN.md comparison 5): Apache Cassandra 4.1.10 in user space, localhost
# only, on cores 16-39 (the cores X and A' were given), a 24 GB heap, G1 (CMS/ParNew paused 21 minutes under the import's writes).
cd ~/parity-c
export JAVA_HOME=/home/ubuntu/parity-c/jdk PATH=/home/ubuntu/parity-c/jdk/bin:$PATH
export MAX_HEAP_SIZE=24G CASSANDRA_LOG_DIR=/home/ubuntu/parity-c/logs LOCAL_JMX=yes
exec taskset -c 16-39 cassandra/bin/cassandra -f > logs/cassandra.out 2>&1 < /dev/null
