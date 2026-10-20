#!/usr/bin/env python3
"""Publish verified history as signed 50,000-ledger pieces and 1,000,000-ledger bundles.

Layout (docs/internal/SIZE-AND-FOOTPRINT.md): completed intervals of `bundle_pieces`
pieces are published as one bundle; the active interval is published as pieces.
Each interval has a state snapshot at its start so a restore can begin there.

One `publish` run is idempotent. It encodes every complete, unpublished piece
(history codec, zstd 19, every segment decoded back to its canonical bytes by
the C++ reader), rolls each complete interval into a bundle made from the same
segment bytes, writes interval snapshots, signs a new catalog generation and
reclaims retired pieces after their grace period. Run it again, or with
--watch, as the source grows.

With --fragment-ledgers N, the complete N-ledger fragments of the piece still
filling are published too, in the catalog's `tail` (readers that know only
`ranges` ignore it), so the publication trails its source by under N ledgers
rather than a piece. Every piece is then joined from its fragments' segments:
those published as the tail, and the rest encoded when the piece completes. A
piece is encoded once either way, and the tail fragments it covers retire after
the grace period, as pieces a bundle covers do.

With --tip-fragment-ledgers M as well, the complete M-ledger fragments after the
newest N-ledger one end the tail, so the publication trails its source by under
M ledgers. They are never joined: each N-ledger fragment is encoded whole when
it completes (small segments would compress the piece worse), and the small
fragments it covers retire after --tip-grace-seconds.

The source is a signed v2 archive. The bucket is a local directory laid out
like the intended R2 bucket.
"""
import argparse
from array import array
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import ctypes
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import time

from compression_probe import Zstd
from delta_codec import split_v2
from fast_archive import Signer, checked, digest, durable, encoded, stamp
import history_codec as hc
import tx_fingerprint_index as ti
import xrpp

SEGMENT_LEDGERS = 4096
SEGMENT_BUDGET = 480 * 1024 ** 2
# Memory one segment encoding at a time can take at the budget: its rows and canonical body here,
# and the encoder's working set (measured on 2026 mainnet segments at the budget: about 0.6 GB
# here and 2.5 GB in the encoder). Bounds how many run at once, with what the rest of the machine
# keeps (a quarter of its memory, at least 16 GiB).
SEGMENT_MEMORY = 4 * 1024 ** 3


def half(data): return hashlib.sha512(data).digest()[:32]


_PRCTL = []


def die_with_parent():
    """Child setup: a snapshot run can take an hour, and a pool worker holds the publisher's
    pipes and bucket lock, so neither may outlive a stopped publisher; nor may the encoder and
    decoder a worker runs outlive the worker (a failed run stops its workers at once). The
    handle is resolved once, in the parent (a pool worker's initializer): a forked child of a
    threaded worker calls it without loading anything."""
    prctl()(1, 15)  # PR_SET_PDEATHSIG, SIGTERM (Linux)


def prctl():
    """The prctl handle, resolved on first use: call it in a parent before forking a child whose
    preexec_fn is die_with_parent."""
    if not _PRCTL: _PRCTL.append(ctypes.CDLL(None, use_errno=True).prctl)
    return _PRCTL[0]


def stop_workers(pool):
    """Shut a process pool down without waiting for the tasks it is running: queued ones are
    cancelled and the workers terminated (their encoders and decoders follow, die_with_parent)."""
    terminate = getattr(pool, 'terminate_workers', None)  # Python 3.14
    if terminate: terminate(); return
    processes = list((getattr(pool, '_processes', None) or {}).values())
    for process in processes: process.terminate()
    pool.shutdown(wait=False, cancel_futures=True)


def unique(pairs):
    result = {}
    for k, v in pairs:
        if k in result: raise ValueError('Duplicate JSON field')
        result[k] = v
    return result


def load_key(path):
    key = Path(path).read_bytes()
    return key if len(key) == 32 else bytes.fromhex(key.decode().strip())


def source_manifest(root, key_path):
    """Authenticate a v2 archive (either signature layout) and return its manifest."""
    raw = (root / 'manifest.json').read_bytes()
    if len(raw) > 16 * 1024 ** 2: raise ValueError('Manifest exceeds reader limit')
    sig = root / 'signatures' / (digest(raw) + '.sig')
    Signer().verify(raw, (sig if sig.exists() else root / 'manifest.json.sig').read_bytes(), load_key(key_path))
    manifest = json.loads(raw, object_pairs_hook=unique)
    if manifest['format_version'] != 2 or manifest['dicts'] or not manifest['chunks']: raise ValueError('Requires dictionary-free v2 archive')
    return manifest


class Bucket:
    """Local stand-in for the R2 bucket: immutable objects plus one mutable pointer."""
    def __init__(self, root): self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True)
    def path(self, name):
        path = (self.root / name).resolve()
        if not path.is_relative_to(self.root.resolve()) or path == self.root.resolve(): raise ValueError('Unsafe object name')
        return path
    def exists(self, name): return self.path(name).exists()
    def get(self, name): return self.path(name).read_bytes()
    def put(self, name, data):
        path = self.path(name)
        if path.exists():
            if path.read_bytes() != data: raise ValueError('Immutable object differs: ' + name)
            return
        durable(path, data)
    def put_file(self, name, source):
        """Move a finished local file into the bucket as an immutable object."""
        path, source = self.path(name), Path(source)
        if path.exists():
            if xrpp.file_digests(path)[0] != xrpp.file_digests(source)[0]: raise ValueError('Immutable object differs: ' + name)
            source.unlink(); return
        path.parent.mkdir(parents=True, exist_ok=True)
        # A rename where scratch and bucket share a file system (the writers sync what they
        # write). Across file systems the bytes are copied under a temporary name in the bucket
        # and synced before the final name appears, so a crash never leaves a partial object
        # under it.
        copied = False
        try: os.replace(source, path)
        except OSError as error:
            if error.errno != errno.EXDEV: raise
            temporary = path.with_name(path.name + '.part')
            with source.open('rb') as data, temporary.open('wb') as out:
                shutil.copyfileobj(data, out, xrpp.STEP); out.flush(); os.fsync(out.fileno())
            os.replace(temporary, path); copied = True
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
        if copied: source.unlink()  # once the object's name is durable
    def copy_to(self, name, target, expected_sha256, expected_size=None):
        """Stream an object to `target`, checking it against the catalog's SHA-256."""
        target = Path(target); target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + '.part'); hasher = hashlib.sha256(); size = 0
        with self.path(name).open('rb') as source, temporary.open('wb') as out:
            while data := source.read(xrpp.STEP): hasher.update(data); out.write(data); size += len(data)
            out.flush(); os.fsync(out.fileno())
        if hasher.hexdigest() != expected_sha256: temporary.unlink(); raise ValueError('Object differs from catalog: ' + name)
        os.replace(temporary, target); return size
    def put_pointer(self, name, data): durable(self.path(name), data)
    def delete(self, name):
        path = self.path(name)
        if path.exists(): path.unlink()


# Layout ---------------------------------------------------------------------

def plan(first, tip, piece_ledgers, bundle_pieces):
    """Complete pieces from `first` through `tip`, grouped by interval. The earliest piece may start late."""
    pieces = []; j = (first - 1) // piece_ledgers
    while (j + 1) * piece_ledgers <= tip:
        start = max(first, j * piece_ledgers + 1); end = (j + 1) * piece_ledgers
        pieces.append((j // bundle_pieces, start, end)); j += 1
    return pieces


def fragments_of(a, b, ledgers):
    """The fragments (first, last) covering ledgers a through b, aligned to multiples of `ledgers`
    (the first may start late, the last end early)."""
    out = []; j = (a - 1) // ledgers
    while j * ledgers + 1 <= b:
        out.append((max(a, j * ledgers + 1), min(b, (j + 1) * ledgers))); j += 1
    return out


def published_ranges(catalog):
    """The catalog's ranges, then its tail fragments: one chain from `first` to the newest ledger."""
    chain = catalog['ranges'] + catalog.get('tail', {}).get('ranges', [])
    check_chain(chain)
    return chain


def interval_bounds(k, first, piece_ledgers, bundle_pieces):
    width = piece_ledgers * bundle_pieces
    return max(first, k * width + 1), (k + 1) * width


# Encoding a piece -------------------------------------------------------------

def ledger_rows(root, manifest, first, last, zstd):
    """Yield (seq, prefix, patches) for [first, last] from authenticated v2 chunks."""
    for chunk in manifest['chunks']:
        if chunk['last'] < first: continue
        if chunk['first'] > last: break
        body = zstd.decompress(checked(root, chunk)[51:-40])
        for row in split_v2(body, chunk['last'] - chunk['first'] + 1):
            if first <= row[0] <= last: yield row


def segments(rows):
    group = []; size = 0
    for row in rows:
        n = len(row[1]) + 16 + sum(36 + len(v) for k, v in row[2])
        if group and (len(group) >= SEGMENT_LEDGERS or size + n > SEGMENT_BUDGET): yield group; group = []; size = 0
        group.append(row); size += n
    if group: yield group


# Size limits of the segment format (tools/history_reader.hpp): a segment whose encoding would
# exceed one is split in two and each half encoded, until it fits (at worst a ledger apiece).
# Ledger size alone cannot predict them: a segment's dictionary section holds what its ledgers
# share and hit the 128 MiB section bound on busy late-2023 ledgers (83,654,097-83,658,104).
SIZE_BOUNDS = ('Section bound', 'Encoded segment bound', 'Segment decoded bound')


class SegmentTooLarge(Exception):
    pass


def encode_segment(group, work, scratch, encoder, decoder, threads, z):
    """The encoded segment for `group` (checked by decoding it back); SegmentTooLarge when a
    size limit of the format refuses it."""
    canonical = hc.original_body(group)
    # Every segment is decoded by the C++ reader and compared byte for byte.
    path, out = work / 'segment.xrph', work / 'segment.canonical'
    if encoder:
        # The compiled encoder writes the same format (tests/history_writer_test.py) and
        # --check decodes its output with the reader before reporting success.
        out.write_bytes(canonical)
        run = subprocess.run([encoder, '--body', str(out), '--count', str(len(group)), '--out', str(path), '--check',
                              '--threads', str(threads)], capture_output=True, text=True, preexec_fn=die_with_parent)
        if run.returncode:
            said = (run.stdout + run.stderr).strip()
            if any(bound in said for bound in SIZE_BOUNDS): raise SegmentTooLarge(said[-300:])
            # The encoder reports why on stdout; the body is kept (one per segment) to reproduce it.
            kept = Path(scratch) / f'failed-segment-{group[0][0]}-{group[-1][0]}.canonical'
            shutil.copyfile(out, kept)
            raise ValueError(f'Encoder failed on segment {group[0][0]}-{group[-1][0]} ({len(group)} ledgers): {said[-600:]}; body kept at {kept}')
        report = json.loads(run.stdout)
        if not report.get('checked'): raise ValueError(f'Encoder did not check segment {group[0][0]}')
        return path.read_bytes()
    try: data, stats = hc.encode(group, z)
    except ValueError as error:
        if any(bound in str(error) for bound in SIZE_BOUNDS): raise SegmentTooLarge(str(error)) from error
        raise
    path.write_bytes(data)
    subprocess.run([decoder, str(path), '--output', str(out)], check=True, capture_output=True, preexec_fn=die_with_parent)
    if out.read_bytes() != canonical: raise ValueError(f'Decoded segment {group[0][0]} differs from source')
    return data


def encode_fitting(group, *args):
    """[(group, encoded)] for `group`, split in halves (recursively) where a size limit refuses it."""
    try: return [(group, encode_segment(group, *args))]
    except SegmentTooLarge:
        if len(group) < 2: raise
        middle = len(group) // 2
        return encode_fitting(group[:middle], *args) + encode_fitting(group[middle:], *args)


def in_order(work, items, jobs):
    """work(item) for each item, up to `jobs` at once on threads, yielding results in item order
    (and holding at most `jobs` items: memory stays bounded however long `items` runs)."""
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        pending = deque()
        for item in items:
            pending.append(pool.submit(work, item))
            if len(pending) >= jobs: yield pending.popleft().result()
        while pending: yield pending.popleft().result()


def default_segment_jobs(meminfo=None, cores=None):
    """Segments encoding at once across a run: one per core, within the memory available beyond
    what the machine keeps for everything else (/proc/meminfo, or `meminfo`'s text)."""
    cores = cores or os.cpu_count() or 1
    try:
        if meminfo is None:
            with open('/proc/meminfo') as info: meminfo = info.read()
        fields = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in meminfo.splitlines() if line.split()[1:2]}
        total, available = fields['MemTotal'], fields['MemAvailable']
    except (OSError, KeyError, ValueError): return max(1, cores // 4)  # no figures: a modest share
    spare = available - max(total // 4, 16 * 1024 ** 3)
    return max(1, min(cores, spare // SEGMENT_MEMORY))


def encode_piece(job):
    """Worker: encode, verify and package one piece. Returns its container and descriptor."""
    source, key_path, first, last, decoder, scratch, encoder, threads, segment_jobs = job
    root = Path(source); manifest = source_manifest(root, key_path); z = Zstd()
    # Index inputs stay compact: per-ledger counts and each transaction's first 8 hash bytes.
    parts = []; counts, prefixes = array('I'), array('Q'); parent = previous = None; transactions = 0
    work = Path(tempfile.mkdtemp(prefix=f'piece-{first}-', dir=scratch))
    # The compiled encoder spends most of a segment compressing one large section on one core,
    # so up to `segment_jobs` segments encode at once, each in its own directory with an equal
    # share of `threads`. Results are taken in ledger order: the piece's bytes do not depend on
    # how many ran together. (The Python reference encoder shares one zstd context: one at a time.)
    jobs = max(1, segment_jobs) if encoder else 1
    def encode(item):
        index, planned = item; directory = work / str(index); directory.mkdir()
        try: return encode_fitting(planned, directory, scratch, encoder, decoder, max(1, threads // jobs), z)
        finally: shutil.rmtree(directory, ignore_errors=True)
    try:
        for encoded in in_order(encode, enumerate(segments(ledger_rows(root, manifest, first, last, z))), jobs):
            for group, data in encoded:
                for seq, prefix, patches in group:
                    r = hc.R(prefix); r.u32(); header = r.record(); n = r.u32()
                    if len(header) != 118: raise ValueError('Unexpected ledger header size')
                    if previous is not None and header[12:44] != previous: raise ValueError(f'Parent hash differs at {seq}')
                    if parent is None: parent = header[12:44]
                    for _ in range(n):
                        tx = r.record(); r.record(); prefixes.append(int.from_bytes(half(b'TXN\0' + tx)[:8], 'big'))
                    counts.append(n); transactions += n; previous = half(b'LWR\0' + header)
                parts.append((group[0][0], len(group), data))
        if not parts or parts[0][0] != first or parts[-1][0] + parts[-1][1] - 1 != last: raise ValueError('Source does not cover the piece')
        path = Path(scratch) / f'piece-{first}-{last}.xrpp'
        rows = [(s, n, len(d), xrpp.sha(d), (lambda d=d: d)) for s, n, d in parts]
        whole, directory = xrpp.write(path, 'piece', first, last, parent, previous, transactions, rows, ti.build_compact(first, counts, prefixes))
        return {'kind': 'piece', 'first': first, 'last': last, 'object': f'pieces/{first}-{last}.{whole[:16]}.xrpp', 'sha256': whole,
                'size': path.stat().st_size, 'directory_sha256': directory, 'parent_hash': parent.hex(), 'last_hash': previous.hex(),
                'transactions': transactions, 'segments': len(parts), 'staged': str(path)}
    finally:
        shutil.rmtree(work, ignore_errors=True)


# A piece (or fragment) a run encoded, checked and moved into the bucket is recorded in the
# scratch directory until a catalog names it, so a run that stops before signing one (another
# piece failed, the snapshot tool failed, the machine stopped) loses none of the finished ones.
def kept_record(scratch, first, last): return Path(scratch) / f'kept-{first}-{last}.json'


def keep(scratch, result): durable(kept_record(scratch, result['first'], result['last']), encoded(result))


def kept(bucket, scratch, root, manifest, first, last, kind, z, named):
    """The descriptor of [first, last] as an earlier run encoded it, or None (then it is encoded
    again). The record is only a hint: the object is checked in full against it (every byte
    against the SHA-256 its name and the record carry, the directory, and the ledgers, parent
    hash, last hash and transactions it states), and the source must still hold the same
    ledgers (its parent hash at `first` and ledger hash at `last`, which commit to every ledger
    between). The bytes were decoded back and compared with the source before they were moved
    into the bucket, as any piece's are; these checks establish that they are those bytes.
    A record that fails them is dropped with its object, unless the catalog (`named`) holds
    it: the range is encoded again, and the bucket would refuse different bytes under the
    name of ones left behind."""
    record = kept_record(scratch, first, last)
    if not record.exists(): return None
    result = None
    try:
        result = json.loads(record.read_bytes(), object_pairs_hook=unique)
        prefix = 'fragments' if kind == 'fragment' else 'pieces'
        if (result['kind'], result['first'], result['last'], result['object']) != (kind, first, last, f"{prefix}/{first}-{last}.{result['sha256'][:16]}.xrpp"):
            raise ValueError('Kept record names another range')
        c = xrpp.Container.open(bucket.path(result['object']), result['sha256'], result['directory_sha256'], full=True)
        d = c.directory
        if (d.kind, d.first, d.last, d.parent_hash.hex(), d.last_hash.hex(), d.transactions, len(d.rows), len(c.data)) != \
                ('piece', first, last, result['parent_hash'], result['last_hash'], result['transactions'], result['segments'], result['size']):
            raise ValueError('Kept object differs from its record')
        def header(seq):
            for _, prefix_bytes, _ in ledger_rows(root, manifest, seq, seq, z):
                r = hc.R(prefix_bytes); r.u32(); return r.record()
            raise ValueError(f'Source does not hold ledger {seq}')
        if header(first)[12:44].hex() != result['parent_hash'] or half(b'LWR\0' + header(last)).hex() != result['last_hash']:
            raise ValueError('Source ledgers differ from the kept range')
        return result
    except (OSError, ValueError, KeyError, TypeError, struct.error) as error:
        print(json.dumps({'event': 'kept_range_discarded', 'range': [first, last], 'reason': str(error)[:300]}), file=sys.stderr, flush=True)
        name = result.get('object') if isinstance(result, dict) else None
        if isinstance(name, str) and name.startswith(('pieces/', 'fragments/')) and name not in named:
            try: bucket.delete(name)
            except (OSError, ValueError): pass
        record.unlink(missing_ok=True); return None


def build_bundle(bucket, pieces, zstd, scratch, index_tool=None, kind='bundle'):
    """Concatenate the pieces' segments unchanged; rebuild the index from their transaction pages
    (with the compiled xrpla_tx_index when given; it writes the same bytes). With kind 'piece',
    the parts are fragments."""
    # The parts must chain: each one's parent the last ledger of the one before (only each
    # part's own ledgers were checked when it was encoded, and the catalog checks the joined
    # whole's ends alone).
    for before, after in zip(pieces, pieces[1:]):
        if after['first'] != before['last'] + 1 or after['parent_hash'] != before['last_hash']:
            raise ValueError(f"{kind} parts do not chain: {before['first']}-{before['last']} then {after['first']}-{after['last']}")
    rows = []; counts, prefixes = array('I'), array('Q')
    for piece in pieces:
        c = xrpp.Container.open(bucket.path(piece['object']), piece['sha256'], piece['directory_sha256'])
        for i, (start, count, at, length, digest) in enumerate(c.directory.rows):
            if not index_tool:
                archive = hc.Archive(c.segment(i), zstd)
                for page in range(len(archive.pages)):
                    tx_rows, _ = archive.tx_page(page)
                    for _, _, txs in tx_rows:
                        counts.append(len(txs)); prefixes.extend(int.from_bytes(half(b'TXN\0' + tx)[:8], 'big') for tx in txs)
            rows.append((start, count, length, digest, (lambda c=c, i=i: c.segment(i))))
    first, last = pieces[0]['first'], pieces[-1]['last']
    if index_tool:
        # In a directory of this run's own (a stopped run's is cleared by the next, as pieces'
        # are), and the tool stops with this process.
        work = Path(tempfile.mkdtemp(prefix=f'{kind}-{first}-{last}-', dir=scratch))
        try:
            index_path = work / 'index.xtxi'; prctl()
            subprocess.run([index_tool, '--out', str(index_path), *(str(bucket.path(p['object'])) for p in pieces)], check=True,
                           capture_output=True, preexec_fn=die_with_parent)
            index = index_path.read_bytes()
        finally: shutil.rmtree(work, ignore_errors=True)
    else:
        index = ti.build_compact(first, counts, prefixes)
    path = Path(scratch) / f'{kind}-{first}-{last}.xrpp'
    whole, directory = xrpp.write(path, kind, first, last, bytes.fromhex(pieces[0]['parent_hash']), bytes.fromhex(pieces[-1]['last_hash']),
                                  sum(p['transactions'] for p in pieces), rows, index)
    name = f'{kind}s/{first}-{last}.{whole[:16]}.xrpp'; size = path.stat().st_size
    bucket.put_file(name, path)
    return {'kind': kind, 'first': first, 'last': last, 'object': name, 'sha256': whole, 'size': size,
            'directory_sha256': directory, 'parent_hash': pieces[0]['parent_hash'], 'last_hash': pieces[-1]['last_hash'],
            'transactions': sum(p['transactions'] for p in pieces), 'segments': len(rows)}


# Catalog ------------------------------------------------------------------------

def load_catalog(bucket, key):
    if not bucket.exists('catalog/current.json'): return None
    pointer = json.loads(bucket.get('catalog/current.json'))
    raw = bucket.get(pointer['object'])
    if hashlib.sha256(raw).hexdigest() != pointer['sha256']: raise ValueError('Catalog pointer differs from generation')
    Signer().verify(raw, bucket.get(pointer['object'] + '.sig'), key)
    catalog = json.loads(raw, object_pairs_hook=unique)
    if catalog['generation'] != pointer['generation']: raise ValueError('Catalog generation differs')
    return catalog


def check_chain(ranges):
    for a, b in zip(ranges, ranges[1:]):
        if b['first'] != a['last'] + 1 or b['parent_hash'] != a['last_hash']: raise ValueError('Catalog ranges are not a chain')


def sign_catalog(bucket, catalog, seed):
    published_ranges(catalog)  # the ranges, then the tail, are one chain
    raw = encoded(catalog)
    public, signature = Signer().sign(raw, seed)
    name = f'catalog/{catalog["generation"]:08d}.json'
    bucket.put('publisher.pub', public)
    # A generation the pointer has reached is immutable. One above it is not yet referenced: a run
    # stopped before it moved the pointer leaves that generation's files, which the next run signs
    # again with other times (published_at, retired_at), so they are replaced.
    pointer = json.loads(bucket.get('catalog/current.json')) if bucket.exists('catalog/current.json') else None
    write = bucket.put if pointer and pointer['generation'] >= catalog['generation'] else bucket.put_pointer
    write(name + '.sig', signature); write(name, raw)
    # The pointer moves last, so readers only ever see a complete generation.
    bucket.put_pointer('catalog/current.json', encoded({'generation': catalog['generation'], 'object': name, 'sha256': digest(raw)}))


# Publishing ---------------------------------------------------------------------

def publish(args):
    """One publish run while holding the bucket's exclusive publisher lock."""
    Path(args.bucket).mkdir(parents=True, exist_ok=True)
    with open(Path(args.bucket) / '.publisher.lock', 'w') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise SystemExit('Another publisher is running on this bucket') from None
        # With the lock held, staging left by an interrupted run is safe to clear.
        scratch = Path(args.scratch)
        if scratch.exists():
            for stale in scratch.iterdir():
                # (kept-*.json records stay: run_once checks each before using it.)
                if stale.name.startswith(('piece-', 'bundle-', 'snapshots-')) or (stale.name.startswith('kept-') and stale.name.endswith('.part')):
                    shutil.rmtree(stale) if stale.is_dir() else stale.unlink()
        # So is an object's temporary copy a crash left in the bucket (`<object>.part`, from
        # Bucket.put_file across file systems or a durable write): no catalog names it, and
        # nothing pushes or reads it.
        for directory in ('pieces', 'fragments', 'bundles', 'snapshots', 'catalog'):
            for stale in (Path(args.bucket) / directory).rglob('*.part'):
                if stale.is_file(): stale.unlink()
        return run_once(args)


def run_once(args):
    root = args.source.resolve(); manifest = source_manifest(root, args.source_key)
    bucket = Bucket(args.bucket); signer_key = Signer().sign(b'', args.signing_key)[0]
    catalog = load_catalog(bucket, signer_key)
    source_first = manifest['chunks'][0]['first']; tip = min(manifest['tip']['seq'], args.until or manifest['tip']['seq'])
    layout = {'piece_ledgers': args.piece_ledgers, 'bundle_pieces': args.bundle_pieces}
    if catalog is None:
        catalog = {'format': 'xrpla-catalog-v1', 'generation': 0, 'network_id': args.network_id, 'layout': layout,
                   'codec': {'format': 'xrph-v2', 'zstd_level': 19, 'hash_recipes': 10, 'index': 'xtxi-v1'},
                   'first': source_first, 'snapshots': [], 'ranges': [], 'retired': []}
    elif catalog['layout'] != layout: raise ValueError('Bucket layout differs')
    # The catalog fixes piece alignment. A rotated spool may start later, but never past the published tip.
    first = catalog['first']
    report = {'pieces': 0, 'bundles': 0, 'snapshots': 0, 'fragments': 0, 'reclaimed': 0}
    covered = catalog['ranges'][-1]['last'] if catalog['ranges'] else first - 1
    if source_first > covered + 1: raise ValueError('Source starts after the published tip')
    todo = [(k, a, b) for k, a, b in plan(first, tip, args.piece_ledgers, args.bundle_pieces) if b > covered]
    scratch = Path(args.scratch); scratch.mkdir(parents=True, exist_ok=True)
    z = Zstd()

    # Fragments: each completed piece's (joined below), and the complete ones after the newest
    # piece (the tail). Published tail fragments still held here are used again.
    fragment_ledgers = args.fragment_ledgers
    old_tail = catalog.get('tail', {}).get('ranges', [])
    held = {(f['first'], f['last']): f for f in old_tail if bucket.path(f['object']).exists()}
    piece_parts = {a: fragments_of(a, b, fragment_ledgers) for k, a, b in todo} if fragment_ledgers else {}
    newest = todo[-1][2] if todo else covered
    tail_wanted = [r for r in fragments_of(newest + 1, tip, fragment_ledgers) if r[1] % fragment_ledgers == 0] if fragment_ledgers and tip > newest else []
    # Then the small fragments after the newest complete one (none ends where a fragment does).
    tip_ledgers = args.tip_fragment_ledgers
    after = tail_wanted[-1][1] if tail_wanted else newest
    tail_wanted += [r for r in fragments_of(after + 1, tip, tip_ledgers) if r[1] % tip_ledgers == 0] if tip_ledgers and tip > after else []

    # A snapshot at every interval start the source has reached, so a restore (and spool
    # rotation) can begin at any interval as soon as its boundary ledger exists.
    width = args.piece_ledgers * args.bundle_pieces
    intervals = range((first - 1) // width, tip // width + 1)
    have = {s['seq'] for s in catalog['snapshots']}
    wanted = [interval_bounds(k, first, args.piece_ledgers, args.bundle_pieces)[0] - 1 for k in intervals]
    missing = [s for s in wanted if s not in have]
    if any(s < source_first - 1 for s in missing): raise ValueError('A missing snapshot precedes the source baseline')
    snapshot_job = None
    if missing:
        out = Path(tempfile.mkdtemp(prefix='snapshots-', dir=scratch)); work = out / 'scratch'; prctl()
        snapshot_job = (subprocess.Popen([args.snapshot_tool, '--archive', str(root), '--publisher-key', str(args.source_key),
                                          '--at', ','.join(map(str, missing)), '--out', str(out / 'objects'), '--scratch', str(work)],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, preexec_fn=die_with_parent), out)

    # Encode complete pieces (or, with fragments, the fragments not held yet) in parallel; the
    # catalog advances along the contiguous prefix.
    done, encoded = {}, {}
    if fragment_ledgers:
        wanted = [r for k, a, b in todo for r in piece_parts[a]] + tail_wanted
        spans = [(x, y, True) for x, y in wanted if (x, y) not in held]
    else: spans = [(a, b, False) for k, a, b in todo]

    recorded = set()  # the ranges this run takes, whose kept records it drops once a catalog names them

    def take(result, reused=False):
        recorded.add((result['first'], result['last']))
        if fragment_ledgers: encoded[(result['first'], result['last'])] = result
        else: done[result['first']] = result; report['pieces'] += 1
        print(json.dumps({result['kind']: [result['first'], result['last']], 'bytes': result['size'], **({'kept': True} if reused else {})}), flush=True)

    def finished(result):
        staged = Path(result.pop('staged'))
        if fragment_ledgers: result.update(kind='fragment', object='fragments/' + result['object'].split('/', 1)[1])
        bucket.put_file(result['object'], staged); keep(scratch, result); take(result)

    # Ranges an earlier run finished before it stopped are used as they are, once checked.
    unfinished = []
    named = {r['object'] for r in catalog['ranges'] + old_tail + catalog['retired']}
    for a, b, fragment in spans:
        result = kept(bucket, scratch, root, manifest, a, b, 'fragment' if fragment else 'piece', z, named)
        if result: take(result, reused=True)
        else: unfinished.append((a, b, fragment))
    # The cores and the segment encodings the run allows are shared among the pieces at work.
    pieces = max(1, min(args.workers, len(unfinished) or 1))
    threads = max(1, (os.cpu_count() or 1) // pieces)
    segment_jobs = max(1, (args.segment_jobs or default_segment_jobs()) // pieces)
    jobs = {(a, b): (str(root), str(args.source_key), a, b, args.decoder, str(scratch), args.encoder, threads, segment_jobs) for a, b, _ in unfinished}
    # One failed piece stops the run at once: leaving a pool's `with` would first wait for every
    # other encode in flight (hours, on a long history). What finished before the failure was
    # seen is moved into the bucket and recorded first, for the next run to use (`kept`).
    futures, taken = {}, set()
    pool = ProcessPoolExecutor(max_workers=args.workers, initializer=die_with_parent)
    try:
        futures = {pool.submit(encode_piece, job): span for span, job in jobs.items()}
        for future in as_completed(futures): taken.add(future); finished(future.result())
    except BaseException:
        for future in futures: future.cancel()
        for future in futures:
            if future not in taken and future.done() and not future.cancelled() and future.exception() is None:
                try: finished(future.result())
                except Exception: pass  # not kept; the next run encodes it again
        stop_workers(pool)
        if snapshot_job: snapshot_job[0].kill()
        raise
    pool.shutdown()
    now = int(time.time())
    retire = lambda ranges, grace=args.grace_seconds: [{'object': r['object'], 'sha256': r['sha256'], 'retired_at': now, 'delete_after': now + grace} for r in ranges]
    for k, a, b in todo:
        if fragment_ledgers:
            parts = [held.get(r) or encoded[r] for r in piece_parts[a]]
            piece = build_bundle(bucket, parts, z, scratch, args.index_tool, kind='piece'); report['pieces'] += 1
            # Fragments never published go now; published ones a reader may still use retire later.
            for r in piece_parts[a]:
                if r in encoded: bucket.delete(encoded[r]['object'])
            done[a] = piece
            print(json.dumps({'piece': [a, b], 'bytes': piece['size'], 'fragments': len(parts)}), flush=True)
        catalog['ranges'].append(done[a])
    tail = [held.get(r) or encoded[r] for r in tail_wanted]
    report['fragments'] = sum(1 for r in tail_wanted if r not in held)
    tail_changed = [f['object'] for f in old_tail] != [f['object'] for f in tail]
    gone = [f for f in old_tail if f not in tail]
    # A small fragment is replaced within one fragment's time: its shorter grace keeps the catalog's
    # retired list short.
    small = lambda f: bool(fragment_ledgers) and f['last'] % fragment_ledgers != 0
    catalog['retired'] += retire([f for f in gone if not small(f)]) + retire([f for f in gone if small(f)], args.tip_grace_seconds)
    if tail:
        catalog['tail'] = {'fragment_ledgers': fragment_ledgers, 'tip': {'seq': tail[-1]['last'], 'ledger_hash': tail[-1]['last_hash']}, 'ranges': tail}
    else: catalog.pop('tail', None)

    if snapshot_job:
        process, out = snapshot_job
        stdout, stderr = process.communicate()
        if process.returncode: raise RuntimeError('Snapshot tool failed: ' + stderr[-2000:])
        for line in stdout.splitlines():
            snapshot = json.loads(line)
            names = [snapshot['index']['object']] + [p['object'] for p in json.loads((out / 'objects' / snapshot['index']['object']).read_bytes())['parts']]
            for name in names: bucket.put(name, (out / 'objects' / name).read_bytes())
            catalog['snapshots'].append(snapshot); report['snapshots'] += 1
        shutil.rmtree(out, ignore_errors=True)
    if first - 1 not in {s['seq'] for s in catalog['snapshots']}:
        raise ValueError('The first interval needs the source baseline snapshot')

    # Roll complete intervals into bundles; their pieces retire after the grace period.
    by_interval = {}
    for r in catalog['ranges']:
        by_interval.setdefault((r['first'] - 1) // (args.piece_ledgers * args.bundle_pieces), []).append(r)
    for k, ranges in sorted(by_interval.items()):
        a, b = interval_bounds(k, first, args.piece_ledgers, args.bundle_pieces)
        if len(ranges) == 1 and ranges[0]['kind'] == 'bundle': continue
        if ranges[-1]['last'] != b or ranges[0]['first'] != a: continue  # interval still active
        bundle = build_bundle(bucket, ranges, z, scratch, args.index_tool); report['bundles'] += 1
        at = catalog['ranges'].index(ranges[0])
        catalog['ranges'][at:at + len(ranges)] = [bundle]
        catalog['retired'] += retire(ranges)
        print(json.dumps({'bundle': [a, b], 'bytes': bundle['size'], 'pieces': len(ranges)}), flush=True)

    # Reclaim retired pieces and fragments past their grace period that nothing references.
    live = {r['object'] for r in published_ranges(catalog)}; still_retired = []
    for r in catalog['retired']:
        if r['delete_after'] <= now and r['object'] not in live: bucket.delete(r['object']); report['reclaimed'] += 1
        else: still_retired.append(r)
    catalog['retired'] = still_retired

    if any(report.values()) or tail_changed:
        last = catalog['ranges'][-1] if catalog['ranges'] else None
        catalog.update(generation=catalog['generation'] + 1, published_at=stamp(),
                       tip={'seq': last['last'], 'ledger_hash': last['last_hash']} if last else None)
        sign_catalog(bucket, catalog, args.signing_key)
    # Every range this run took is now named by the catalog (or joined into a piece it names). The
    # records of ranges it did not take (past an --until lower than a stopped run's) stay for the
    # run that does.
    for a, b in recorded: kept_record(scratch, a, b).unlink(missing_ok=True)
    report.update(generation=catalog['generation'], tip=catalog.get('tip'), published_bytes=published_bytes(bucket, catalog))
    print(json.dumps(report), flush=True)
    return report


SNAPSHOT_BYTES = {}  # snapshot index SHA-256: its parts' bytes, for entries without total_size


def published_bytes(bucket, catalog):
    """Bytes of the objects the catalog names, from the sizes it signs (a walk of the bucket
    stats every file in it, hundreds of thousands for a full history, on every run of --watch;
    and a bucket whose remote holds its objects keeps few of them locally)."""
    total = sum(r['size'] for r in published_ranges(catalog))
    for s in catalog['snapshots']:
        index = s['index']
        if 'total_size' not in s and index['sha256'] not in SNAPSHOT_BYTES:
            SNAPSHOT_BYTES[index['sha256']] = sum(p['size'] for p in json.loads(bucket.get(index['object']))['parts'])
        total += index['size'] + s.get('total_size', SNAPSHOT_BYTES.get(index['sha256'], 0))
    return total


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--source', type=Path, required=True, help='signed v2 archive')
    p.add_argument('--source-key', type=Path, required=True, help='public key of the source archive')
    p.add_argument('--bucket', type=Path, required=True); p.add_argument('--signing-key', type=Path, required=True)
    p.add_argument('--scratch', type=Path, required=True)
    p.add_argument('--decoder', required=True, help='xrpla_history_decode binary')
    p.add_argument('--snapshot-tool', required=True, help='xrpla_snapshot binary')
    p.add_argument('--encoder', help='xrpla_history_encode binary; the Python reference encoder when omitted')
    p.add_argument('--index-tool', help='xrpla_tx_index binary for bundle indexes; built in Python when omitted')
    p.add_argument('--piece-ledgers', type=int, default=50_000); p.add_argument('--bundle-pieces', type=int, default=20)
    p.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 2) - 2))
    p.add_argument('--segment-jobs', type=int, help='segments encoding at once across all pieces (with --encoder); '
                   'default: one per core, within the memory available beyond a quarter of the machine\'s')
    p.add_argument('--grace-seconds', type=int, default=7 * 86400); p.add_argument('--network-id', type=int, default=0)
    p.add_argument('--until', type=int, help='publish no further than this ledger')
    p.add_argument('--fragment-ledgers', type=int, default=0, help='publish the piece still filling in fragments of this many ledgers '
                   '(a divisor of --piece-ledgers; 0: none)')
    p.add_argument('--tip-fragment-ledgers', type=int, default=0, help='and the fragments after the newest one in fragments of this many '
                   'ledgers (a divisor of --fragment-ledgers; 0: none)')
    p.add_argument('--tip-grace-seconds', type=int, default=86400, help='how long a retired small fragment is kept')
    p.add_argument('--watch', type=int, help='repeat every N seconds')
    args = p.parse_args()
    if args.piece_ledgers < 1 or args.bundle_pieces < 1: p.error('Layout must be positive')
    if args.fragment_ledgers < 0 or (args.fragment_ledgers and args.piece_ledgers % args.fragment_ledgers):
        p.error('--fragment-ledgers must divide --piece-ledgers')
    if args.tip_fragment_ledgers < 0 or (args.tip_fragment_ledgers and (not args.fragment_ledgers or args.fragment_ledgers % args.tip_fragment_ledgers
                                                                        or args.tip_fragment_ledgers == args.fragment_ledgers)):
        p.error('--tip-fragment-ledgers must be a smaller divisor of --fragment-ledgers')
    while True:
        publish(args)
        if not args.watch: break
        time.sleep(args.watch)


if __name__ == '__main__':
    main()
