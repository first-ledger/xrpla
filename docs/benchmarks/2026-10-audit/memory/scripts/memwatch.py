#!/usr/bin/env python3
"""A test server's memory, one JSON line a second to OUT: the cgroup's anonymous and file memory
(what MemoryMax counts) every second, and every 5th second the decode cache's accounting
(xrpla_cache_stats) and its TCP connections.

  memwatch.py PID URL CGROUP_DIR OUT
"""
import json, os, subprocess, sys, time, urllib.request

pid, url, cgroup, out = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
port = url.rsplit(':', 1)[1].strip('/')
n = 0
while os.path.exists(f'/proc/{pid}'):
    row = {'t': round(time.time(), 1)}
    try:
        with open(os.path.join(cgroup, 'memory.stat')) as f:
            stat = dict(line.split() for line in f)
        row['anon_mb'] = int(stat['anon']) >> 20
        row['file_mb'] = int(stat['file']) >> 20
    except OSError:
        pass
    if n % 5 == 0:
        try:
            body = json.dumps({'method': 'xrpla_cache_stats', 'params': [{}]}).encode()
            with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=10) as r:
                s = json.load(r)['result']
            a = s.get('allocator', {})
            row.update({'alloc_mb': a.get('allocated', 0) >> 20, 'resident_mb': a.get('resident', 0) >> 20,
                        'budget_used_mb': s['budget']['used'] >> 20, 'budget_cached_mb': s['budget'].get('cached', s['budget']['used']) >> 20,
                        'budget_held_mb': s['budget'].get('held', 0) >> 20, 'segments': s['open_segments']['segments'],
                        'segments_mb': s['open_segments']['charged_bytes'] >> 20, 'pages_mb': s['pages']['bytes'] >> 20})
        except Exception as error:
            row['stats_error'] = str(error)[:80]
        lines = subprocess.run(['ss', '-Htn', 'state', 'established', f'( sport = :{port} )'], capture_output=True, text=True).stdout
        row['conns'] = len(lines.splitlines())
    with open(out, 'a') as f:
        f.write(json.dumps(row) + '\n')
    n += 1
    time.sleep(1)
