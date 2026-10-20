#!/usr/bin/env python3
"""Entry counts of a segment dictionary's tables (the XHD2 layout history_reader.hpp parses):
field headers, values (and their bytes), hash recipes (by mode, with argument counts), atoms
(by kind, with argument counts) and shapes (count and total elements), for a few segments of
the 8-9M bundle, to see where parsing time and memory go."""
import glob, os, sys, collections
sys.path.insert(0, os.path.expanduser('~/xrpla-audit/inst34/share/xrpla/tools'))
import xrpp, history_codec as hc
from compression_probe import Zstd

class R:
    def __init__(self, d): self.d, self.p = d, 0
    def take(self, n): v = self.d[self.p:self.p + n]; self.p += n; return v
    def byte(self): b = self.d[self.p]; self.p += 1; return b
    def var(self):
        v = s = 0
        while True:
            b = self.byte(); v |= (b & 127) << s; s += 7
            if not b & 128: return v
    def bytes(self): return self.take(self.var())

path = glob.glob(os.path.expanduser('~/store-bench/p14/bundles/*.xrpp'))[0]
c = xrpp.Container.open(path); z = Zstd()
sizes = [0, 1, 1, 2, 3, 2, 2, 1, 5, 1, 1]
for i in (0, 100, 200):
    d = hc.Archive(c.segment(i), z).section(0)
    r = R(d); assert r.take(4) == b'XHD2'
    out = collections.OrderedDict(dictionary_mb=round(len(d) / 2**20, 1))
    n = r.var(); [r.bytes() for _ in range(n)]; out['headers'] = n
    n = r.var(); vb = 0
    for _ in range(n): vb += len(r.bytes())
    out['values'] = n; out['value_mb'] = round(vb / 2**20, 1)
    n = r.var(); modes = collections.Counter(); args = 0
    for _ in range(n):
        m = r.byte(); modes[m] += 1
        if not m: r.take(32)
        else:
            for _ in range(sizes[m]): r.var(); args += 1
            if m == 9: r.var(); args += 1
    out['hashes'] = n; out['hash_args'] = args; out['hash_modes'] = dict(sorted(modes.items()))
    n = r.var(); kinds = collections.Counter(); args = 0
    for _ in range(n):
        r.var(); k = r.var(); kinds[k] += 1
        if k == 4:
            r.var(); args += 1; cnt = r.var()
            for _ in range(cnt): r.var(); args += 1
        else:
            for _ in range(3 if k == 3 else 1): r.var(); args += 1
    out['atoms'] = n; out['atom_args'] = args; out['atom_kinds'] = dict(sorted(kinds.items()))
    n = r.var(); total = 0
    for _ in range(n):
        size = r.var(); total += size
        for _ in range(size): r.var()
    out['shapes'] = n; out['shape_elements'] = total
    assert r.p == len(d)
    print(i, dict(out), flush=True)
