#!/usr/bin/env python3
"""Pull published pieces/bundles back to a configured ledger, query them, or restore them.

  sync     authenticate the catalog, then cache the snapshot and every range up to
           --to (from the interval start at or before --from); every object is
           checked against the signed catalog
  query    answer --ledger N or --tx HASH straight from the cached compressed data
  restore  decode a cached range into a local v2 archive (snapshot + chunks) that
           `xrpla fetch --archive` and `xrpla verify --full` accept; it is signed
           with a local key created for this restore

The bucket is a local directory laid out like the intended R2 bucket.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile

from compression_probe import Zstd
from delta_codec import split_v2
from fast_archive import Signer, digest, durable, encoded, stamp
import history_codec as hc
from piece_publisher import Bucket, load_catalog, load_key, published_ranges, unique
from s3_remote import S3
import tx_fingerprint_index as ti
import xrpp


def half(data): return hashlib.sha512(data).digest()[:32]


class RemoteBucket:
    """Read side of an R2/S3 publication bucket, with the same interface as the local Bucket."""
    def __init__(self, s3): self.s3 = s3
    def exists(self, name): return self.s3.exists(name)
    def get(self, name): return self.s3.get(name)
    def copy_to(self, name, target, expected_sha256, expected_size=None):
        return self.s3.download(name, target, expected_sha256, expected_size)


def select(catalog, first, last):
    """The snapshot and ranges needed to cover [first, last], starting at an interval snapshot
    (the ranges, then the tail's fragments)."""
    ranges = published_ranges(catalog)
    if not ranges or last > ranges[-1]['last'] or first < ranges[0]['first']: raise ValueError('Requested ledgers are not published')
    snapshots = sorted((s for s in catalog['snapshots'] if s['seq'] < first), key=lambda s: s['seq'])
    if not snapshots: raise ValueError('No published snapshot precedes the requested ledgers')
    base = snapshots[-1]
    chosen = [r for r in ranges if r['last'] > base['seq'] and r['first'] <= last]
    if chosen[0]['first'] != base['seq'] + 1: raise ValueError('Snapshot does not start a published range')
    return base, chosen


def sync(args):
    bucket = RemoteBucket(S3.from_environment()) if args.remote else Bucket(args.bucket); key = load_key(args.publisher_key)
    catalog = load_catalog(bucket, key)
    if catalog is None: raise ValueError('Bucket has no catalog')
    cache = Path(args.cache); cache.mkdir(parents=True, exist_ok=True)
    target = cache / 'selection.json'
    if target.exists():
        # As `xrpla sync` does: the pointer is not signed, so a stale copy of it (a cache in
        # front of the origin, or a mirror replaying an old one) can name an older generation,
        # signed as it once was. Generations only move forward: an older one is refused and the
        # cache keeps its selection until the publication offers a newer one; one with the same
        # generation that came back different is refused outright.
        held = json.loads(target.read_bytes())['catalog']
        have, now = held['generation'], catalog['generation']
        if now < have:
            print(json.dumps({'event': 'catalog_refused_older', 'offered': now, 'held': have, 'cache': str(cache),
                              'note': f'a stale or replayed catalog; remove {target} to follow another publication'}), file=sys.stderr)
            result = {'generation': have, 'refused_older': now, 'fetched_bytes': 0}
            print(json.dumps(result)); return result
        if now == have and held != catalog:
            raise ValueError(f'Catalog generation {now} differs from the one this cache holds; it is refused')
    chain = published_ranges(catalog)
    last = args.to or chain[-1]['last']; first = args.from_ or chain[0]['first']
    base, chosen = select(catalog, first, last)
    index_bytes = bucket.get(base['index']['object'])
    if hashlib.sha256(index_bytes).hexdigest() != base['index']['sha256']: raise ValueError('Snapshot index differs')
    index = json.loads(index_bytes, object_pairs_hook=unique)
    # (object, sha256, size): with the size known, a download needs no HEAD first.
    names = [(base['index']['object'], base['index']['sha256'], base['index'].get('size'))]
    names += [(p['object'], p['sha256'], p.get('size')) for p in index['parts']]
    names += [(r['object'], r['sha256'], r.get('size')) for r in chosen]
    # A snapshot is dozens of parts; fetch several objects at once (large ones also transfer
    # in parallel ranges inside copy_to).
    todo = [item for item in names if not ((cache / item[0]).exists() and xrpp.file_digests(cache / item[0])[0] == item[1])]
    with ThreadPoolExecutor(max_workers=8) as pool:
        fetched = sum(pool.map(lambda item: bucket.copy_to(item[0], cache / item[0], item[1], item[2]), todo))
    # Whole-file hashes were checked while copying; check each directory against the catalog too.
    for r in chosen: xrpp.Container.open(cache / r['object'], directory_sha256=r['directory_sha256'], full=False)
    state = {'catalog': catalog, 'snapshot': base, 'ranges': chosen, 'first': base['seq'] + 1, 'last': min(last, chosen[-1]['last'])}
    durable(target, encoded(state))
    result = {'generation': catalog['generation'], 'snapshot': base['seq'], 'first': state['first'], 'last': state['last'],
              'ranges': len(chosen), 'fetched_bytes': fetched,
              'cached_bytes': sum(p.stat().st_size for p in cache.rglob('*') if p.is_file())}
    print(json.dumps(result)); return result


def cached(cache, full=False):
    """The selection and its containers; `full` checks each whole file against the catalog too
    (a query reads only the segments it needs, each checked against its directory row)."""
    state = json.loads((Path(cache) / 'selection.json').read_bytes())
    return state, [(r, xrpp.Container.open(Path(cache) / r['object'], expected_sha256=r['sha256'] if full else None,
                                           directory_sha256=r['directory_sha256'], full=full)) for r in state['ranges']]


def cached_object(cache, item):
    """A cached snapshot object's bytes, checked against the hash the catalog gives it."""
    data = (Path(cache) / item['object']).read_bytes()
    if hashlib.sha256(data).hexdigest() != item['sha256']: raise ValueError(f"Cached {item['object']} differs from the catalog")
    return data


def query(args):
    state, containers = cached(args.cache); z = Zstd()
    if args.ledger is not None:
        if not state['first'] <= args.ledger <= state['last']: raise ValueError('Ledger outside the synced range')
        for r, c in containers:
            if r['first'] <= args.ledger <= r['last']:
                archive = hc.Archive(c.segment(c.directory.segment_for(args.ledger)), z)
                page = next(i for i, p in enumerate(archive.pages) if p[0] <= args.ledger < p[0] + p[1])
                record = dict(archive.page(page))[args.ledger]
                rows, _ = archive.tx_page(page)
                seq, header, txs = next(row for row in rows if row[0] == args.ledger)
                result = {'ledger_index': seq, 'ledger_hash': half(b'LWR\0' + header).hex(), 'transactions': len(txs),
                          'canonical_record_bytes': len(record)}
                print(json.dumps(result)); return result
    tx_hash = bytes.fromhex(args.tx)
    for r, c in containers:
        index = ti.Index(c.index_bytes())
        def fetch(ledger, position):
            archive = hc.Archive(c.segment(c.directory.segment_for(ledger)), z)
            page = next(i for i, p in enumerate(archive.pages) if p[0] <= ledger < p[0] + p[1])
            rows, _ = archive.tx_page(page)
            return next(row for row in rows if row[0] == ledger)[2][position]
        found = index.lookup(tx_hash, fetch)
        if found:
            ledger, position = found
            result = {'hash': args.tx.upper(), 'ledger_index': ledger, 'position': position, 'tx_blob': fetch(ledger, position).hex()}
            print(json.dumps(result)); return result
    print(json.dumps({'hash': args.tx.upper(), 'found': False})); return None


def restore(args):
    """Write a v2 archive for [first, --to]: the cached snapshot plus one chunk per decoded segment.
    Every cached file is checked whole against the catalog first, and each range restored to its
    end must end at the ledger hash the catalog gives it: the archive is signed with a new key,
    so nothing after this vouches for what it holds."""
    state, containers = cached(args.cache, full=True); z = Zstd(); out = Path(args.out)
    last = min(args.to or state['last'], state['last'])
    if out.exists() and any(out.iterdir()): raise ValueError('Restore output must be empty')
    snapshot = state['snapshot']
    index = cached_object(args.cache, snapshot['index'])
    durable(out / snapshot['index']['object'], index)
    for part in json.loads(index)['parts']: durable(out / part['object'], cached_object(args.cache, part))
    chunks = []; tip = None
    with tempfile.TemporaryDirectory(dir=args.scratch) as work:
        segment_path, canonical_path = Path(work) / 'segment.xrph', Path(work) / 'segment.canonical'
        for r, c in containers:
            for i, (start, count, *_rest) in enumerate(c.directory.rows):
                if start > last: break
                segment_path.write_bytes(c.segment(i))
                subprocess.run([args.decoder, str(segment_path), '--output', str(canonical_path)], check=True, capture_output=True)
                body = canonical_path.read_bytes()
                end = min(start + count - 1, last)
                if end < start + count - 1:  # trim a partial final segment to the requested ledger
                    rows = [row for row in split_v2(body, count) if row[0] <= end]
                    body = hc.original_body(rows)
                n = end - start + 1; table = len(body) - 12 * n
                header_at = struct.unpack_from('<Q', body, table + 12 * (n - 1) + 4)[0]
                header_len = struct.unpack_from('<I', body, header_at + 4)[0]
                tip = half(b'LWR\0' + body[header_at + 8:header_at + 8 + header_len]).hex()
                data = b'XRPA' + struct.pack('<HIII', 2, start, end, n) + bytes(33) + z.compress(body, 3)
                data += struct.pack('<Q', table) + hashlib.sha256(data).digest()
                name = f'chunks/{start}-{end}.{digest(data)[:16]}.xrpa'
                durable(out / name, data)
                chunks.append({'object': name, 'size': len(data), 'sha256': digest(data), 'first': start, 'last': end,
                               'ledger_hash_of_last': tip, 'amendments': []})
            if chunks and chunks[-1]['last'] == r['last'] and tip != r['last_hash'].lower():
                raise ValueError(f"Range {r['first']}-{r['last']} ends at ledger hash {tip}, not the catalog's {r['last_hash']}")
    if not chunks or chunks[-1]['last'] != last: raise ValueError('Restore did not reach the requested ledger')
    document = {'format_version': 2, 'chunk_width': 65536, 'dicts': [], 'chunks': chunks,
                'snapshots': [{k: snapshot[k] for k in ('seq', 'prefix', 'account_hash', 'index', 'parts', 'total_size')}],
                'published_at': stamp(), 'tip': {'seq': last, 'ledger_hash': tip}, 'signature_format': 'sha256-path-v1',
                'integrity': {'mode': 'restored-from-signed-catalog', 'catalog_generation': state['catalog']['generation']}}
    raw = encoded(document)
    seed = Path(args.scratch) / f'restore-{os.getpid()}.seed'; seed.write_bytes(os.urandom(32))
    try: public, signature = Signer().sign(raw, seed)
    finally: seed.unlink()
    durable(out / 'publisher.pub', public); durable(out / 'signatures' / (digest(raw) + '.sig'), signature)
    durable(out / 'manifest.json', raw); durable(out / 'manifest.json.sig', signature)
    result = {'archive': str(out), 'first': chunks[0]['first'], 'last': last, 'chunks': len(chunks), 'tip_hash': tip,
              'publisher_key': str(out / 'publisher.pub')}
    print(json.dumps(result)); return result


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='command', required=True)
    s = sub.add_parser('sync'); s.add_argument('--publisher-key', type=Path, required=True)
    where = s.add_mutually_exclusive_group(required=True); where.add_argument('--bucket', type=Path)
    where.add_argument('--remote', action='store_true', help='read from R2/S3 configured by XRPLA_S3_* variables')
    s.add_argument('--cache', type=Path, required=True); s.add_argument('--from', dest='from_', type=int); s.add_argument('--to', type=int)
    q = sub.add_parser('query'); q.add_argument('--cache', type=Path, required=True)
    g = q.add_mutually_exclusive_group(required=True); g.add_argument('--ledger', type=int); g.add_argument('--tx')
    r = sub.add_parser('restore'); r.add_argument('--cache', type=Path, required=True); r.add_argument('--out', type=Path, required=True)
    r.add_argument('--scratch', type=Path, required=True); r.add_argument('--decoder', required=True); r.add_argument('--to', type=int)
    args = p.parse_args()
    {'sync': sync, 'query': query, 'restore': restore}[args.command](args)


if __name__ == '__main__':
    main()
