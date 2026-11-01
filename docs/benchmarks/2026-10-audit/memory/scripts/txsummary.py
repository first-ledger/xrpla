#!/usr/bin/env python3
"""Summary of txprofile.py's rows: per limit, direction and cold/warm, latency percentiles,
segments touched, dictionaries loaded and where the milliseconds went.

  txsummary.py TXPROFILE.jsonl
"""
import json, statistics, sys

rows = [json.loads(l) for l in open(sys.argv[1])]


def q(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(p * len(values)))] if values else None


for limit in (20, 200, 400):
    for forward in (False, True):
        for run in ('cold', 'warm'):
            r = [x for x in rows if x['limit'] == limit and x['forward'] == forward and x['run'] == run]
            if not r:
                continue
            s = [x['seconds'] for x in r]
            mean = lambda k: round(statistics.mean(x[k] for x in r))
            print(f"limit {limit:3} {'oldest' if forward else 'newest'} {run:4}: p50 {q(s, .5)}s p90 {q(s, .9)}s max {max(s)}s"
                  f" | segments p50 {q([x['segments'] for x in r], .5)} max {max(x['segments'] for x in r)}"
                  f" | dictionaries p50 {q([x['dictionaries'] for x in r], .5)}"
                  f" | mean ms read {mean('dict_read_ms')} parse {mean('dict_parse_ms')} decode {mean('page_decode_ms')}"
                  f" | txs p50 {q([x['transactions'] for x in r], .5)}")
cold = [x for x in rows if x['run'] == 'cold' and x['dictionaries'] > 0]
n = sum(x['dictionaries'] for x in cold)
print('per dictionary: read ms', round(sum(x['dict_read_ms'] for x in cold) / n), 'parse ms', round(sum(x['dict_parse_ms'] for x in cold) / n))
for x in sorted(rows, key=lambda x: -x['seconds'])[:6]:
    print('slowest', x['seconds'], 'limit', x['limit'], 'oldest' if x['forward'] else 'newest', x['run'], 'segments', x['segments'],
          'dictionaries', x['dictionaries'], 'decode ms', x['page_decode_ms'], 'span', x['span'])
