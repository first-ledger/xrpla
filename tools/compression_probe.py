#!/usr/bin/env python3
"""Measure actual archive components and lossless zstd levels; never rewrite archives."""
import argparse
import ctypes as c
import ctypes.util
import hashlib
import json
import os
from pathlib import Path
import struct
import time


class Zstd:
    def __init__(self):
        self.lib = c.CDLL(os.environ.get('XRPLA_ZSTD_LIBRARY') or
                          ctypes.util.find_library('zstd') or 'libzstd.so.1')
        for name, result, arguments in [
            ('ZSTD_getFrameContentSize', c.c_ulonglong, [c.c_void_p, c.c_size_t]),
            ('ZSTD_decompress', c.c_size_t, [c.c_void_p, c.c_size_t, c.c_void_p, c.c_size_t]),
            ('ZSTD_compress', c.c_size_t, [c.c_void_p, c.c_size_t, c.c_void_p, c.c_size_t, c.c_int]),
            ('ZSTD_compressBound', c.c_size_t, [c.c_size_t]),
            ('ZSTD_isError', c.c_uint, [c.c_size_t]),
            ('ZSTD_getErrorName', c.c_char_p, [c.c_size_t]),
            ('ZSTD_versionString', c.c_char_p, []),
        ]:
            function = getattr(self.lib, name); function.restype = result; function.argtypes = arguments
    def check(self, value):
        if self.lib.ZSTD_isError(value): raise ValueError(self.lib.ZSTD_getErrorName(value).decode())
        return value
    def decompress(self, data):
        size = self.lib.ZSTD_getFrameContentSize(data, len(data))
        if size > 128 * 1024 * 1024: raise ValueError('Probe accepts known-size frames up to 128 MiB')
        output = c.create_string_buffer(size)
        actual = self.check(self.lib.ZSTD_decompress(output, size, data, len(data)))
        if actual != size: raise ValueError('Frame size mismatch')
        return output.raw[:actual]
    def compress(self, data, level):
        size = self.lib.ZSTD_compressBound(len(data)); output = c.create_string_buffer(size)
        actual = self.check(self.lib.ZSTD_compress(output, size, data, len(data), level))
        return output.raw[:actual]


def components(body, count):
    """Read bounded v2 records to separate raw headers/transactions/patches."""
    pos, headers, transactions, patches = 0, 0, 0, 0
    tx_count, patch_count = 0, 0
    projection = bytearray()
    def number():
        nonlocal pos
        if pos + 4 > len(body): raise ValueError('Truncated field')
        result = struct.unpack_from('<I', body, pos)[0]; pos += 4; return result
    def record():
        nonlocal pos
        size = number()
        if size > 16 * 1024 * 1024 or pos + size > len(body): raise ValueError('Invalid record size')
        pos += size
    offsets = []
    for _ in range(count):
        offsets.append(len(projection)); start = pos; sequence = number(); record(); headers += pos - start
        start_tx = pos; n = number(); tx_count += n
        if n > 1000000: raise ValueError('Invalid transaction count')
        for _ in range(n): record(); record()
        transactions += pos - start_tx
        projection.extend(body[start:pos]); projection.extend(b'\0' * 4)
        start_patch = pos; n = number(); patch_count += n
        if n > 1000000: raise ValueError('Invalid patch count')
        for _ in range(n): pos += 32; record()
        patches += pos - start_patch
    if pos + 12 * count != len(body): raise ValueError('Unexpected chunk body length')
    for i, offset in enumerate(offsets):
        sequence = struct.unpack_from('<I', body, pos + i * 12)[0]
        projection.extend(struct.pack('<IQ', sequence, offset))
    return {'headers_bytes': headers, 'transactions_bytes': transactions, 'patches_bytes': patches,
            'offset_table_bytes': 12 * count, 'transactions': tx_count, 'patches': patch_count}, bytes(projection)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--levels', default='3,9,19')
    args = parser.parse_args(); root = args.archive.resolve()
    levels = list(map(int, args.levels.split(',')))
    if any(level < 1 or level > 19 for level in levels): parser.error('levels must be 1..19')
    manifest_bytes = (root / 'manifest.json').read_bytes(); manifest = json.loads(manifest_bytes)
    if manifest['format_version'] != 2 or manifest['dicts']: parser.error('Probe requires dictionary-free v2')
    zstd = Zstd()
    def read(item):
        path = (root / item['object']).resolve()
        if not path.is_relative_to(root): raise ValueError('Unsafe object path')
        data = path.read_bytes()
        if len(data) != item['size'] or hashlib.sha256(data).hexdigest() != item['sha256']: raise ValueError('Archive object differs')
        return data
    def probe(raw):
        rows = []
        for level in levels:
            start = time.perf_counter(); compressed = zstd.compress(raw, level)
            elapsed = time.perf_counter() - start
            if zstd.decompress(compressed) != raw: raise ValueError('Compression round trip differs')
            rows.append({'level': level, 'bytes': len(compressed), 'seconds': elapsed})
        return rows
    report = {'format': 'xrpla-compression-probe-v1', 'zstd': zstd.lib.ZSTD_versionString().decode(),
              'manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(), 'chunks': [], 'snapshot_samples': [],
              'note': 'The no-patch projection is a non-reconstructible diagnostic, not a valid replacement archive.'}
    snapshot_bytes = index_bytes = 0
    for snapshot in manifest['snapshots']:
        index_raw = read(snapshot['index']); index_bytes += len(index_raw); parts = json.loads(index_raw)['parts']
        snapshot_bytes += sum(part['size'] for part in parts)
        for i in sorted({0, len(parts) // 2, len(parts) - 1}):
            part = parts[i]; data = read(part); raw = zstd.decompress(data)
            report['snapshot_samples'].append({'object': part['object'], 'objects': part['records'],
                                               'current_bytes': len(data), 'raw_bytes': len(raw), 'levels': probe(raw)})
    for chunk in manifest['chunks']:
        data = read(chunk); raw = zstd.decompress(data[51:-40]); breakdown, projected = components(raw, chunk['last'] - chunk['first'] + 1)
        report['chunks'].append({'first': chunk['first'], 'last': chunk['last'], 'current_bytes': len(data),
                                  'raw_bytes': len(raw), 'components': breakdown, 'levels': probe(raw),
                                  'without_patches_diagnostic': probe(projected)})
    report['stored_bytes'] = {'snapshot_parts': snapshot_bytes, 'snapshot_indexes': index_bytes,
                              'chunks': sum(row['current_bytes'] for row in report['chunks']),
                              'time_indexes': sum(x.get('time_index', {}).get('size', 0) for x in manifest['chunks']),
                              'manifest_and_key_signature': len(manifest_bytes) + 32 + 64}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'report': str(args.out), 'stored_bytes': report['stored_bytes']}))


if __name__ == '__main__': main()
