#!/usr/bin/env python3
"""Join an earlier publication onto the front of this one.

Two publishers can fill one history from both ends under separate prefixes of a bucket (one
publisher per prefix): this one from some interval boundary on, and an earlier one from the
start of history up to that boundary. When the earlier one is complete, `publication_merge.py`
makes this publication cover both:

  * the earlier catalog is read from its prefix and checked against the publisher key; it must
    have the same network, layout and codec, consist of whole bundles, and end at the ledger
    before this publication's first, with that ledger's hash this publication's parent hash;
  * its bundles and snapshots are copied to this prefix inside the store (nothing is downloaded
    to be uploaded again), and each copy is read back and checked against its SHA-256;
  * its snapshot indexes are written to the local bucket (a push lists snapshot parts from them);
  * the joined catalog is signed as this publication's next generation, and pushed: the
    pointer moves only after every object is verified under this prefix.

This publication's publisher then carries on from the joined catalog. Stop both publishers
first (this holds the local bucket's publisher lock); start this one again afterwards. The
earlier prefix is left as it is. A merge that stops part way is run again: copies already
verified are not repeated, and until it finishes the local bucket's `.merging` record names
them (with the prefix they come from and when the merge started), so a push meanwhile keeps
them remotely, and reports that it does. A merge that will not be finished is abandoned with
--abandon: the record goes, with the snapshot indexes the merge wrote locally that the local
catalog does not name, and the copies are then deleted by the pushes that follow, as any
object neither named nor held here is.

Without --apply nothing is written, locally or remotely: the plan is printed.

  publication_merge.py --bucket DIR --signing-key SEED --from-prefix mainnet-early/ [--apply]
  publication_merge.py --bucket DIR --signing-key SEED --abandon

Credentials and this publication's prefix come from the XRPLA_S3_* variables (s3_remote.py).
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
from pathlib import Path
import threading
import time

from fast_archive import Signer, durable, encoded, stamp
from piece_publisher import Bucket, check_chain, load_catalog, sign_catalog, unique
from s3_remote import MERGING, PUSHED, S3, catalog_objects, check_recorded, merging_record, push, record_pointer, remote_pointer, verified_remotely


def remote_catalog(s3, key):
    """The current catalog generation under a prefix, checked against the publisher key."""
    pointer = json.loads(s3.get('catalog/current.json'))
    raw = s3.get(pointer['object'])
    if hashlib.sha256(raw).hexdigest() != pointer['sha256']: raise ValueError('Catalog pointer differs from generation')
    Signer().verify(raw, s3.get(pointer['object'] + '.sig'), key)
    catalog = json.loads(raw, object_pairs_hook=unique)
    if catalog['generation'] != pointer['generation']: raise ValueError('Catalog generation differs')
    return catalog


def joined(earlier, later):
    """`later` with `earlier` on its front, and the earlier snapshots it gains. When `later`
    already begins with `earlier` (a merge run again), it is returned unchanged."""
    for field in ('format', 'network_id', 'layout', 'codec'):
        if earlier[field] != later[field]: raise ValueError(f'The publications differ in {field}')
    if not earlier['ranges'] or not later['ranges']: raise ValueError('A publication without ranges cannot be joined')
    if any(r['kind'] != 'bundle' for r in earlier['ranges']):
        raise ValueError('The earlier publication still has pieces: its last interval is not complete')
    # A snapshot both hold (at the boundary) stays this publication's own: the two may have
    # written the same state as different parts under the same names.
    seqs = {s['seq']: s for s in later['snapshots']}
    gained = [s for s in earlier['snapshots'] if s['seq'] not in seqs]
    for s in earlier['snapshots']:
        if s['seq'] in seqs and (seqs[s['seq']]['ledger_hash'], seqs[s['seq']]['account_hash']) != (s['ledger_hash'], s['account_hash']):
            raise ValueError(f"The publications' snapshots at {s['seq']} are of different ledgers")
    if later['first'] == earlier['first']:
        if later['ranges'][:len(earlier['ranges'])] != earlier['ranges'] or gained: raise ValueError('This publication starts where the earlier one does, but does not begin with it')
        return later, [s for s in earlier['snapshots'] if seqs[s['seq']] == s]
    end = earlier['ranges'][-1]['last']
    if end != later['first'] - 1 or later['ranges'][0]['first'] != later['first']:
        raise ValueError(f"The earlier publication ends at {end}; this one starts at {later['first']}")
    catalog = dict(later, first=earlier['first'], snapshots=sorted(gained + later['snapshots'], key=lambda s: s['seq']),
                   ranges=earlier['ranges'] + later['ranges'])
    check_chain(catalog['ranges'])  # the earlier tip is this publication's parent ledger
    return catalog, gained


def merge(bucket_root, s3, source, signing_key, apply=False):
    """Join the publication under `source` (an S3 on the earlier prefix) onto the local bucket's,
    whose remote is `s3`. Returns the report it prints."""
    root = Path(bucket_root); bucket = Bucket(root); key = Signer().sign(b'', signing_key)[0]
    with open(root / '.publisher.lock', 'w') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise SystemExit('A publisher is running on this bucket: stop it first') from None
        later = load_catalog(bucket, key)
        if later is None: raise ValueError('The local bucket has no catalog')
        # The push that ends the merge would refuse a remote pointer no push from this bucket
        # recorded: refused before anything is written, as the copies' records (PUSHED) must not
        # make a bucket that recorded nothing look like one whose records predate the pointers.
        before, _ = remote_pointer(s3); pointers = check_recorded(root, before)
        earlier = remote_catalog(source, key)
        catalog, snapshots = joined(earlier, later)
        # What this prefix must hold of the earlier publication: (name, sha256, size).
        indexes = {}; objects = [(r['object'], r['sha256'], r['size']) for r in earlier['ranges']]
        for s in snapshots:
            raw = source.get(s['index']['object'])
            if hashlib.sha256(raw).hexdigest() != s['index']['sha256']: raise ValueError('Snapshot index differs from catalog: ' + s['index']['object'])
            indexes[s['index']['object']] = raw
            objects.append((s['index']['object'], s['index']['sha256'], len(raw)))
            objects += [(p['object'], p['sha256'], p['size']) for p in json.loads(raw)['parts']]
        remote = set(s3.list()); known = verified_remotely(root)
        todo = [o for o in objects if o[0] not in remote or known.get(o[0]) != o[1]]
        report = {'earlier': {'generation': earlier['generation'], 'first': earlier['first'], 'last': earlier['ranges'][-1]['last']},
                  'first': catalog['first'], 'tip': catalog['ranges'][-1]['last'], 'ranges': len(catalog['ranges']), 'snapshots': len(catalog['snapshots']),
                  'objects': len(objects), 'to_copy': len(todo), 'bytes_to_copy': sum(o[2] for o in todo),
                  'generation': later['generation'] + (catalog is not later), 'applied': bool(apply)}
        if not apply: print(json.dumps(report)); return report

        # A bucket whose records predate the pointer records is taken at its word, as a push takes
        # it, and the pointer recorded now: while the merge's record lasts, a push takes PUSHED's
        # objects for this merge's, not an older push's (check_recorded).
        if before and not pointers: record_pointer(root, before)
        # The earlier publication's objects are copied and verified first; only then is the joined
        # catalog signed and the local pointer moved, so a merge that stops partway leaves the
        # publisher on its own catalog (whose objects it holds), not on one it cannot push. Until
        # a catalog names them, the copies are recorded (MERGING) before the first is made, so a
        # push meanwhile keeps them rather than deleting them as neither named nor held here.
        record = merging_record(root) or {}; started = record.get('started', int(time.time()))
        durable(root / MERGING, encoded({'objects': sorted(set(record.get('objects', [])) | {o[0] for o in objects}),
                                         'source': source.prefix, 'started': started}))
        for name, raw in indexes.items(): bucket.put(name, raw)
        recording = threading.Lock(); copied = 0
        with open(root / PUSHED, 'a') as ledger:
            def copy(item):
                nonlocal copied
                name, sha, size = item
                # Already under this prefix but not recorded (a merge that stopped between the
                # copy and its record): kept when intact.
                if name not in remote or s3.sha256(name) != sha:
                    s3.copy_from(source.prefix, name, size)
                    if s3.sha256(name) != sha: raise ValueError('Copy differs from catalog: ' + name)
                    with recording: copied += 1
                with recording:
                    ledger.write(json.dumps({'object': name, 'sha256': sha}) + '\n'); ledger.flush()
                    print(json.dumps({'verified': name, 'bytes': size}), flush=True)
            with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(copy, todo))
        # Every copy this run uses is verified: the record keeps only those, not what an earlier
        # attempt copied that this one does not use, so the first push whose catalog names them
        # all (this merge's, or the publisher's next when this one's fails) drops it.
        durable(root / MERGING, encoded({'objects': sorted({o[0] for o in objects}), 'source': source.prefix, 'started': started}))
        if catalog is not later:
            catalog.update(generation=later['generation'] + 1, published_at=stamp())
            sign_catalog(bucket, catalog, signing_key)
        report.update(copied=copied, pushed=push(root, s3))
        # The pushed catalog names every copy: the record is no longer needed (push drops it too).
        (root / MERGING).unlink(missing_ok=True)
        print(json.dumps(report)); return report


def abandon(bucket_root, signing_key):
    """Give up a merge that will not be finished: its record (MERGING) goes, and with it the
    snapshot indexes it wrote locally that the local catalog does not name (a push keeps the
    remote copy of whatever is held here), so the pushes that follow delete its copies under
    the usual rules. Refused once the local catalog names every copy: that merge is done but for
    its push. Returns the report it prints."""
    root = Path(bucket_root); bucket = Bucket(root); key = Signer().sign(b'', signing_key)[0]
    with open(root / '.publisher.lock', 'w') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise SystemExit('A publisher is running on this bucket: stop it first') from None
        record = merging_record(root)
        if record is None: raise SystemExit('No merge is under way on this bucket')
        catalog = load_catalog(bucket, key); pending = set(record['objects'])
        named = {name for name, _ in catalog_objects(root, catalog)} if catalog else set()
        if pending <= named: raise SystemExit('The local catalog names every copy the merge made: push it (s3_remote.py push) to finish the merge')
        local = [name for name in sorted(pending - named) if bucket.path(name).exists()]
        for name in local: bucket.delete(name)
        (root / MERGING).unlink()
        report = {'abandoned': len(pending), 'source': record.get('source'), 'local_deleted': len(local),
                  'age_seconds': int(time.time() - record['started']) if 'started' in record else None}
        print(json.dumps(report)); return report


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--bucket', type=Path, required=True, help="this publication's local bucket")
    p.add_argument('--signing-key', type=Path, required=True, help='publisher seed (both publications are signed with it)')
    p.add_argument('--from-prefix', help="the earlier publication's prefix in the same store, for example mainnet-early/")
    p.add_argument('--apply', action='store_true', help='copy, sign and push (default: print the plan)')
    p.add_argument('--abandon', action='store_true', help='give up an unfinished merge: its copies are left to the pushes that follow to delete')
    a = p.parse_args()
    if a.abandon:
        if a.from_prefix or a.apply: p.error('--abandon takes neither --from-prefix nor --apply')
        abandon(a.bucket, a.signing_key); return
    if not a.from_prefix: p.error('--from-prefix is required')
    s3 = S3.from_environment(); source = S3.from_environment(prefix=a.from_prefix)
    if source.prefix == s3.prefix: p.error('--from-prefix is this publication\'s own prefix')
    merge(a.bucket, s3, source, a.signing_key, a.apply)


if __name__ == '__main__':
    main()
