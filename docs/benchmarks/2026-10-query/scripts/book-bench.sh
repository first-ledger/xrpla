#!/usr/bin/env bash
# 8,000,001-8,300,000 restored with the book index build (q-book: index, then compaction),
# then the all-commands benchmark against the standalone xrpld on the same ledgers.
set -eu
cd ~/store-bench
export XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1
export XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23
rm -rf q-book; s=$(date +%s)
nice -n 10 new/bin/xrpla restore --source bucket --publisher-key ~/xrpla-audit/publisher.pub --to store --out q-book --range 8000001-8300000 > q-book.out 2> q-book.err
echo "restore $(( $(date +%s) - s )) s; $(tail -1 q-book.err | cut -c1-150); compact $(python3 -c "import json; print(round(json.load(open('q-book.out'))['timings']['compact']))") s; store $(du -sh q-book/store | cut -f1)"
sed -n '/^table()/,/^}/p;/^run()/,/^}/p' compact-bench.sh > bench-functions.sh
. ./bench-functions.sh
run q-book 15430 parity-book.jsonl
table parity-book.jsonl
python3 -c "
import json
for l in open('parity-book.jsonl'):
    r = json.loads(l)
    if r.get('method') == 'book_offers': print('book_offers same', r['same'], 'different', r['different'], json.dumps(r['examples'])[:300])"
echo DONE
