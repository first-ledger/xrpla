#!/usr/bin/env python3
"""Each phase of one or more campaign runs (runs/LABEL from campaign.sh), side by side: requests a
second, latency, errors, and memory (cgroup anonymous memory, its peak and mean; the allocator's
peak; the decode cache's budget) while it ran and 15-20 s after.

  summarize.py RUN_DIR...
"""
import json, os, sys


def phases(run):
    rows = [json.loads(l) for l in open(os.path.join(run, 'mem.jsonl'))]
    marks = [l.split() for l in open(os.path.join(run, 'phases.log'))]
    loads = [json.loads(l) for l in open(os.path.join(run, 'load.jsonl'))]
    starts = [(float(m[0]), m[1]) for m in marks if m[2] == 'start']
    ends = [float(m[0]) for m in marks if m[2] == 'end']
    for (start, name), end, load in zip(starts, ends, loads):
        window = [r for r in rows if start <= r['t'] <= end]
        after = [r['anon_mb'] for r in rows if end + 15 <= r['t'] <= end + 20 and 'anon_mb' in r]
        anon = [r['anon_mb'] for r in window if 'anon_mb' in r]
        stats = [r for r in window if 'alloc_mb' in r]
        yield name, {'per_s': load['per_second'], 'p50_ms': load['ms_p50'], 'p95_ms': load['ms_p95'],
                     'errors': sum(v for k, v in load['errors'].items() if k != 'actNotFound'),
                     'anon_max': max(anon), 'anon_mean': round(sum(anon) / len(anon)), 'anon_after': after[-1] if after else None,
                     'alloc_max': max((r['alloc_mb'] for r in stats), default=None),
                     'budget_max': max((r['budget_used_mb'] for r in stats), default=None)}


if __name__ == '__main__':
    runs = {os.path.basename(r.rstrip('/')): dict(phases(r)) for r in sys.argv[1:]}
    names = list(dict.fromkeys(n for r in runs.values() for n in r))
    keys = ['per_s', 'p50_ms', 'p95_ms', 'errors', 'anon_max', 'anon_mean', 'anon_after', 'alloc_max', 'budget_max']
    for name in names:
        print(name)
        print('  %-10s' % 'run' + ''.join('%11s' % k for k in keys))
        for label, r in runs.items():
            if name in r:
                print('  %-10s' % label + ''.join('%11s' % r[name][k] for k in keys))
