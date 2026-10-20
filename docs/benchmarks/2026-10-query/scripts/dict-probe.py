#!/usr/bin/env python3
"""For segments of the 8-9M bundle: the dictionary section's compressed and decompressed size,
and the time to decompress it (zstd through ctypes, so C speed), to compare with the C++
reader's measured load time per segment (decompress + parse)."""
import glob, sys, time, os, statistics
sys.path.insert(0, os.path.expanduser('~/xrpla-audit/inst34/share/xrpla/tools'))
import xrpp
from compression_probe import Zstd
import history_codec as hc

path = glob.glob(os.path.expanduser('~/store-bench/p14/bundles/*.xrpp'))[0]
c = xrpp.Container.open(path)
z = Zstd()
rows = c.directory.rows
print('segments', len(rows))
sizes, raw_sizes, times = [], [], []
for i in range(0, len(rows), max(1, len(rows) // 25)):
    seg = c.segment(i)
    a = hc.Archive(seg, z)
    sec = a.sections[0] if hasattr(a, 'sections') else None
    s = time.perf_counter()
    d = a.section(0)
    times.append(time.perf_counter() - s)
    raw_sizes.append(len(d))
    sizes.append(sec[1] if sec and isinstance(sec, (tuple, list)) else None)
print('dictionary decompressed MB: median %.1f max %.1f' % (statistics.median(raw_sizes) / 2**20, max(raw_sizes) / 2**20))
print('decompress ms: median %.1f max %.1f' % (1000 * statistics.median(times), 1000 * max(times)))
print('segment sizes MB: median %.1f' % (statistics.median([r[2] for r in rows]) / 2**20))
