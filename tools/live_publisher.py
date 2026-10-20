#!/usr/bin/env python3
"""Follow a node or the peer network and publish pieces/bundles continuously
(docs/internal/SIZE-AND-FOOTPRINT.md).

  xrpla publish     follows the node (--node), or the network's peers (--peers: each
                    ledger trusted on a quorum of the network's listed validators,
                    no node involved), into a signed v2 spool (restartable)
  piece_publisher   seals complete pieces, rolls complete intervals into bundles,
                    writes interval snapshots and reclaims retired pieces
  rotation          once an interval is bundled and the next interval's snapshot
                    exists, the spool is cut at that boundary: the new spool is the
                    published snapshot plus the old spool's later chunks, so the
                    node never has to re-export state and the spool stays bounded

With --push each new catalog generation is mirrored to R2/S3, and the local copies
the remote then holds (each read back intact after its upload, and still there) that
nothing here reads again are deleted: bundles, and the snapshots before the next
rotation boundary. Pieces go as before, once bundled and past their grace period. A
publisher's disk then holds about one interval (--keep-pushed keeps everything, for a
bucket something else reads). Below --min-free-gb free space (5 by default; a publisher
sets more) the follower pauses, so a remote that stays unreachable fills the disk no
further, and resumes once space returns.

Everything is idempotent; stop and restart at any time. The runner keeps
WORKDIR/status.json current; `live_publisher.py --health --workdir WORKDIR` reads it
and exits non-zero, listing the problems, when publishing has stalled.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import sys
import time

from compression_probe import Zstd
from delta_codec import split_v2
from fast_archive import Signer, checked, digest, durable, encoded, stamp
import history_codec as hc
from piece_publisher import Bucket, load_catalog, source_manifest
from s3_remote import S3, push, reclaim


TOOLS = Path(__file__).resolve().parent
MANIFEST_WARNING = 12 * 1024 * 1024  # xrpla publish rejects manifests over 16 MiB
STATUS = 'status.json'


def without_credentials():
    """This process's environment less the object store's credentials (XRPLA_S3_*): only this
    process pushes, and the follower (which talks to the peer network) and the encoders need none."""
    return {k: v for k, v in os.environ.items() if not k.startswith('XRPLA_S3_')}


def signal_group(process, sig):
    """Signal a child started in its own session, and everything it started."""
    try: os.killpg(process.pid, sig)
    except ProcessLookupError: pass


def write_chunk(root, rows, zstd):
    """A v2 chunk (and its time index) for consecutive rows, as fast_archive writes them."""
    body = bytearray(); offsets = []; times = bytearray()
    for seq, prefix, patches in rows:
        offsets.append(len(body)); body += hc.raw_record((seq, prefix, patches))
        r = hc.R(prefix); r.u32(); header = r.record(); times += struct.pack('>II', seq, int.from_bytes(header[112:116], 'big'))
    table = len(body)
    for i, offset in enumerate(offsets): body += struct.pack('<IQ', rows[0][0] + i, offset)
    first, last = rows[0][0], rows[-1][0]
    data = b'XRPA' + struct.pack('<HIII', 2, first, last, len(rows)) + bytes(33) + zstd.compress(bytes(body), 3)
    data += struct.pack('<Q', table) + hashlib.sha256(data).digest()
    name, time_name = f'chunks/{first}-{last}.{digest(data)[:16]}.xrpa', f'times/{first}-{last}.{digest(times)[:16]}.xrpt'
    durable(root / name, data); durable(root / time_name, bytes(times))
    r = hc.R(rows[-1][1]); r.u32(); last_header = r.record()
    return {'object': name, 'size': len(data), 'sha256': digest(data), 'first': first, 'last': last,
            'ledger_hash_of_last': hashlib.sha512(b'LWR\0' + last_header).digest()[:32].hex(), 'amendments': [],
            'time_index': {'object': time_name, 'size': len(times), 'sha256': digest(times)},
            'first_close_time': int.from_bytes(times[4:8], 'big'), 'last_close_time': int.from_bytes(times[-4:], 'big')}


# The follower's own state in a spool: its verified state tree, and the interval snapshots it wrote.
STATE = ('.verification-nodes', '.verification-nodes.compacted', 'interval-snapshots')
ROTATED_FROM = 'rotated-from'  # in a spool cut from another: that spool's name


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def copy_immutable(old, manifest, staging, boundary, snapshot, bucket, staged):
    """Copy into `staging` what a cut at `boundary` takes unchanged: the published snapshot (its
    index and parts, checked against the catalog) and the old spool's whole chunks after the
    boundary (checked against its manifest). Objects `staged` (object -> SHA-256) already holds
    are not copied again; those copied are added."""
    index = bucket.get(snapshot['index']['object'])
    if hashlib.sha256(index).hexdigest() != snapshot['index']['sha256']: raise ValueError('Snapshot index differs from catalog')
    if staged.get(snapshot['index']['object']) != snapshot['index']['sha256']:
        durable(staging / snapshot['index']['object'], index); staged[snapshot['index']['object']] = snapshot['index']['sha256']
    for part in json.loads(index)['parts']:
        if staged.get(part['object']) == part['sha256']: continue
        bucket.copy_to(part['object'], staging / part['object'], part['sha256']); staged[part['object']] = part['sha256']
    for chunk in manifest['chunks']:
        if chunk['first'] <= boundary: continue
        for item in (chunk, chunk.get('time_index')):
            if item and staged.get(item['object']) != item['sha256']:
                durable(staging / item['object'], checked(old, item)); staged[item['object']] = item['sha256']


def stage_spool(old, old_key, new, boundary, snapshot, bucket):
    """The bulk of a cut, done while the old spool's follower carries on: the immutable copies
    (gigabytes: the snapshot's parts) go into the new spool's staging directory, so the follower
    stops only for cut_spool itself. Returns what was staged, for cut_spool."""
    staging = new.with_name(new.name + '.staging'); shutil.rmtree(staging, ignore_errors=True)
    staged = {}
    copy_immutable(old, source_manifest(old, old_key), staging, boundary, snapshot, bucket, staged)
    return staged


def move_state(old, new):
    """The follower's state moves from the spool a cut was made from to the new spool, which has
    the old one's tip, so the follower resumes there at once instead of rebuilding its tree from
    the snapshot. What already moved stays: run again after a crash, it finishes the move."""
    moved = False
    for name in STATE:
        if (old / name).exists() and not (new / name).exists(): os.replace(old / name, new / name); moved = True
    if moved: sync_directory(old); sync_directory(new)


def cut_spool(old, old_key, new, boundary, snapshot, bucket, seed, staged=None):
    """New spool = the published snapshot at `boundary` + the old spool's ledgers after it. With
    `staged` (from stage_spool), only what the old spool gained since is copied."""
    manifest = source_manifest(old, old_key); z = Zstd()
    if not manifest['chunks'][0]['first'] <= boundary < manifest['tip']['seq']: raise ValueError('Cut boundary outside the spool')
    if new.exists(): raise ValueError('New spool directory already exists')
    staging = new.with_name(new.name + '.staging')
    if staged is None: shutil.rmtree(staging, ignore_errors=True); staged = {}
    copy_immutable(old, manifest, staging, boundary, snapshot, bucket, staged)
    chunks = []
    for chunk in manifest['chunks']:
        if chunk['last'] <= boundary: continue
        if chunk['first'] > boundary: chunks.append(chunk); continue  # whole chunk: copied unchanged
        body = z.decompress(checked(old, chunk)[51:-40])
        chunks.append(write_chunk(staging, [row for row in split_v2(body, chunk['last'] - chunk['first'] + 1) if row[0] > boundary], z))
    # Chunks staged that the old spool merged into others since are not part of the new one.
    kept = {snapshot['index']['object']} | {part['object'] for part in json.loads((staging / snapshot['index']['object']).read_bytes())['parts']}
    kept |= {item['object'] for chunk in chunks for item in (chunk, chunk.get('time_index')) if item}
    for name in staged:
        if name not in kept: (staging / name).unlink(missing_ok=True)
    document = {'format_version': 2, 'chunk_width': 65536, 'dicts': [], 'chunks': chunks,
                'snapshots': [{k: snapshot[k] for k in ('seq', 'prefix', 'account_hash', 'index', 'parts', 'total_size')}],
                'published_at': stamp(), 'tip': manifest['tip'], 'signature_format': 'sha256-path-v1',
                'integrity': {'mode': 'rotated-spool', 'boundary': boundary}}
    raw = encoded(document); public, signature = Signer().sign(raw, seed)
    durable(staging / 'publisher.pub', public); durable(staging / 'signatures' / (digest(raw) + '.sig'), signature)
    durable(staging / 'manifest.json', raw); durable(staging / 'manifest.json.sig', signature)
    durable(staging / ROTATED_FROM, old.name.encode())
    # The rename is the cut's commit point: the new spool is complete before the follower's state
    # leaves the old one, and a crash after it is completed on restart (Runner.resume_rotation).
    # The old spool is retired, and must not be followed again.
    os.replace(staging, new); sync_directory(new.parent)
    move_state(old, new)


class Runner:
    def __init__(self, args):
        self.args = args; self.work = Path(args.workdir); (self.work / 'spools').mkdir(parents=True, exist_ok=True)
        self.spool_key = self.work / 'spool.pub'
        if not self.spool_key.exists(): durable(self.spool_key, Signer().sign(b'', args.spool_seed)[0])
        self.follower = None; self.stopping = False; self.warned = 0
        self.pushed = None  # last generation mirrored; a failed push leaves it unchanged, so it is retried
        now = int(time.time()); self.status = {'started': now, 'push': bool(args.push), 'spool_tip': None, 'spool_tip_since': now}
        signal.signal(signal.SIGTERM, self.stop); signal.signal(signal.SIGINT, self.stop)

    def stop(self, *_): self.stopping = True

    def log(self, **event):
        print(json.dumps({'time': stamp(), **event}), flush=True)
        if 'error' in event or event['event'] == 'follower_exited': self.touch(error={**event, 'time': int(time.time())})

    def touch(self, **fields):
        """Rewrite status.json (times are Unix seconds); called on events and every loop."""
        self.status.update(fields, updated=int(time.time()), follower_running=self.follower is not None and self.follower.poll() is None)
        temporary = self.work / (STATUS + '.part'); temporary.write_bytes(encoded(self.status)); os.replace(temporary, self.work / STATUS)

    def note_spool(self, spool):
        try: tip = json.loads((spool / 'manifest.json').read_bytes())['tip']['seq']
        except (OSError, ValueError, KeyError): return
        if tip != self.status['spool_tip']: self.touch(spool=spool.name, spool_tip=tip, spool_tip_since=int(time.time()))

    def current(self):
        pointer = self.work / 'current'
        return self.work / 'spools' / pointer.read_text().strip() if pointer.exists() else None

    def follow(self, spool, start=None):
        """Run `xrpla publish` on the spool until stopped."""
        a = self.args
        command = [a.xrpla, 'publish', '--out', str(spool), '--signing-key', str(a.spool_seed),
                   '--batch-ledgers', str(a.batch_ledgers), '--interval', str(a.poll_seconds)]
        if a.peers:
            # Peers keep recent ledgers only: a spool far behind the network catches up from a node.
            command += ['--peers', a.peers, '--network-id', str(a.network_id)]
            if a.peer_identity: command += ['--peer-identity', str(a.peer_identity)]
            if a.validator_list: command += ['--validator-list', a.validator_list, '--validator-list-key', a.validator_list_key]
        else:
            command += ['--source', a.node, '--jobs', str(a.rpc_jobs)]
            if a.grpc_source: command += ['--grpc-source', a.grpc_source]
            if a.grpc_jobs: command += ['--grpc-jobs', str(a.grpc_jobs)]
            if a.history_peers: command += ['--history-peers', a.history_peers]
            if a.history_identity: command += ['--history-identity', str(a.history_identity)]
            if a.validator_list:
                command += ['--validator-list', a.validator_list, '--validator-list-key', a.validator_list_key, '--validations-node', a.validations_node]
            if a.delay_ms is not None: command += ['--delay-ms', str(a.delay_ms)]
        if a.merge_chunks: command += ['--merge-chunks']
        # The follower writes each interval's starting snapshot as it passes the boundary.
        command += ['--interval-snapshots', str(a.piece_ledgers * a.bundle_pieces)]
        if start: command += ['--start', str(start)]
        if a.until: command += ['--until', str(a.until)]
        with open(self.work / 'follower.log', 'a') as log:  # the child keeps its own descriptor
            self.follower = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=without_credentials())
        self.log(event='following', spool=spool.name, start=start)

    def reached_until(self, spool):
        """With --until, whether the spool holds that ledger (the follower is then done)."""
        if not self.args.until: return False
        try: return json.loads((spool / 'manifest.json').read_bytes())['tip']['seq'] >= self.args.until
        except (OSError, ValueError, KeyError): return False

    def keep_following(self, spool):
        """Short of space the follower pauses; otherwise one that exited is started again (unless
        the spool already holds --until)."""
        low = self.disk_low()
        if low and self.follower is not None: self.halt_follower()
        elif not low and not self.reached_until(spool) and (self.follower is None or self.follower.poll() is not None):
            if self.follower is not None: self.log(event='follower_exited', code=self.follower.returncode)
            self.follow(spool, None if (spool / 'manifest.json').exists() else self.args.start)

    def halt_follower(self):
        if self.follower and self.follower.poll() is None:
            self.follower.send_signal(signal.SIGTERM)
            try: self.follower.wait(timeout=300)
            except subprocess.TimeoutExpired: self.follower.kill(); self.follower.wait()
        self.follower = None

    def publish(self, spool):
        """One piece_publisher run on the spool; the follower is looked after meanwhile."""
        a = self.args
        command = [sys.executable, str(TOOLS / 'piece_publisher.py'), '--source', str(spool), '--source-key', str(self.spool_key),
                   '--bucket', str(a.bucket), '--signing-key', str(a.signing_key), '--scratch', str(self.work / 'scratch'),
                   '--decoder', a.decoder, '--snapshot-tool', a.snapshot_tool, '--piece-ledgers', str(a.piece_ledgers),
                   '--bundle-pieces', str(a.bundle_pieces), '--workers', str(a.workers), '--grace-seconds', str(a.grace_seconds),
                   '--fragment-ledgers', str(a.fragment_ledgers), '--tip-fragment-ledgers', str(a.tip_fragment_ledgers)]
        if a.encoder: command += ['--encoder', a.encoder]
        if a.index_tool: command += ['--index-tool', a.index_tool]
        # A publish run writes its catalog generation last, so stopping it midway is safe. It runs
        # in its own process group: its pool workers and tools hold its pipes and bucket lock, so a
        # stop signals all of them, and kills any still running a minute later.
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True, env=without_credentials())
        stop_sent = None
        while True:
            try: stdout, stderr = process.communicate(timeout=5); break
            except subprocess.TimeoutExpired:
                if self.stopping and stop_sent is None: stop_sent = time.monotonic(); signal_group(process, signal.SIGTERM)
                elif stop_sent is not None and time.monotonic() - stop_sent > 60: signal_group(process, signal.SIGKILL)
                # A publish that writes a snapshot runs for many minutes: a follower exiting
                # meanwhile is started again now, not once it ends.
                if not self.stopping: self.keep_following(spool)
                self.touch()
        if process.returncode:
            self.log(event='publish_stopped' if self.stopping else 'publish_failed', error=stderr[-2000:]); return None
        report = json.loads(stdout.splitlines()[-1]); self.log(event='published', **report)
        self.touch(published={'generation': report.get('generation'), 'tip': (report.get('tip') or {}).get('seq'), 'time': int(time.time())})
        if a.push: self.mirror(report.get('generation'))
        return report

    def mirror(self, generation):
        """Push `generation` (unless pushed already) and reclaim what the remote then holds, holding
        the bucket's publisher lock (piece_publisher's, and publication_merge's: a merge's copies
        must not be pushed over, nor its catalog pushed by two at once). While another holds it,
        both wait for the next publish."""
        a = self.args
        with open(Path(a.bucket) / '.publisher.lock', 'w') as lock:
            try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError: self.log(event='push_deferred', reason='another process holds the bucket lock'); return
            if generation and generation != self.pushed:
                try:
                    result = push(a.bucket, S3.from_environment()); self.pushed = result['generation']; self.log(event='pushed', **result)
                    # An unfinished merge's copies are kept remotely while its record lasts: shown
                    # (source, objects, age) until it is run again or abandoned.
                    self.touch(pushed={'generation': result['generation'], 'time': int(time.time())}, merging=result.get('merging'))
                except Exception as error: self.log(event='push_failed', error=str(error)[-2000:])  # retried after the next publish
            if not a.keep_pushed and self.pushed is not None and self.pushed == generation:
                try: self.reclaim_pushed()
                except Exception as error: self.log(event='reclaim_failed', error=str(error)[-2000:])  # tried again after the next push

    def reclaim_pushed(self):
        """Delete the local copies R2 holds that nothing here reads again: bundles (bundling reads
        pieces, never bundles) and the parts of snapshots before the next rotation boundary
        (rotation copies that snapshot from the bucket; a spool keeps a copy of the one it
        started from). Snapshot indexes stay: a push lists snapshot parts from them."""
        a = self.args; bucket = Bucket(a.bucket); spool = self.current()
        catalog = load_catalog(bucket, Signer().sign(b'', a.signing_key)[0])
        if not catalog or spool is None or not (spool / 'manifest.json').exists(): return
        manifest = source_manifest(spool, self.spool_key); width = a.piece_ledgers * a.bundle_pieces
        boundary = ((manifest['chunks'][0]['first'] - 1) // width + 1) * width
        objects = [(r['object'], r['sha256']) for r in catalog['ranges'] if r['kind'] == 'bundle']
        for snapshot in catalog['snapshots']:
            index = bucket.path(snapshot['index']['object'])
            if snapshot['seq'] < boundary and index.exists():
                objects += [(p['object'], p['sha256']) for p in json.loads(index.read_bytes())['parts']]
        files, size = reclaim(a.bucket, S3.from_environment(), objects)
        if files: self.log(event='reclaimed_local', files=files, bytes=size)

    def disk_low(self):
        """Whether the work directory's file system is below --min-free-gb free (and not yet a
        quarter above it again), logged once each way."""
        free = shutil.disk_usage(self.work).free / 2**30; floor = self.args.min_free_gb
        if not self.status.get('disk_low') and free < floor:
            self.log(event='disk_low', free_gb=round(free, 1), min_free_gb=floor); self.touch(disk_low=True)
        elif self.status.get('disk_low') and free >= floor * 1.25:
            self.log(event='disk_recovered', free_gb=round(free, 1)); self.touch(disk_low=False)
        return bool(self.status.get('disk_low'))

    def maybe_rotate(self, spool):
        """Cut the spool once its first interval is bundled and the next interval's snapshot is published."""
        a = self.args; bucket = Bucket(a.bucket)
        catalog = load_catalog(bucket, Signer().sign(b'', a.signing_key)[0])
        if not catalog: return spool
        manifest = source_manifest(spool, self.spool_key); width = a.piece_ledgers * a.bundle_pieces
        boundary = ((manifest['chunks'][0]['first'] - 1) // width + 1) * width
        bundled = any(r['kind'] == 'bundle' and r['last'] == boundary for r in catalog['ranges'])
        snapshot = next((s for s in catalog['snapshots'] if s['seq'] == boundary), None)
        if not (bundled and snapshot and manifest['tip']['seq'] > boundary): return spool
        new = self.work / 'spools' / f'spool-{boundary + 1}'
        # The copies are made while the follower runs; it stops for the cut alone.
        staged = stage_spool(spool, self.spool_key, new, boundary, snapshot, bucket)
        self.halt_follower()
        cut_spool(spool, self.spool_key, new, boundary, snapshot, bucket, a.spool_seed, staged)
        self.complete_rotation(spool, new)
        self.log(event='rotated', boundary=boundary, spool=new.name)
        return new

    def complete_rotation(self, old, new):
        """After a cut's commit point (its new spool in place): the follower's state moves to the
        new spool, which becomes current, and the old one retires."""
        move_state(old, new)
        # Everything in it through the boundary is published and signed. Readers of its tail (a
        # connected server, until its piece cache has the new bundle) get an hour to move on.
        # It retires before the new spool becomes current: a crash between leaves it current and
        # retired (purge_retired keeps the current spool, and resume_rotation completes the cut),
        # never replaced but not retired, which nothing would remove.
        if not (old / 'retired').exists(): durable(old / 'retired', str(int(time.time())).encode())
        durable(self.work / 'current', new.name.encode())

    def resume_rotation(self):
        """A cut a crash interrupted after its commit point (a spool in place that was cut from
        the current one) is completed before anything follows; returns the current spool."""
        spool = self.current()
        if spool is None: return None
        for marker in sorted((self.work / 'spools').glob('*/' + ROTATED_FROM)):
            new = marker.parent
            if new != spool and (new / 'manifest.json').exists() and marker.read_text().strip() == spool.name:
                self.complete_rotation(spool, new); self.log(event='rotation_completed', spool=new.name)
                return new
        return spool

    def purge_retired(self, grace=3600):
        """Remove rotated spools an hour after they were retired."""
        current = self.current()
        for old in (self.work / 'spools').glob('*/retired'):
            try: retired_at = int(old.read_text())
            except (OSError, ValueError): continue
            if old.parent != current and time.time() - retired_at > grace: shutil.rmtree(old.parent, ignore_errors=True)

    def run(self):
        a = self.args; spool = self.resume_rotation()
        if spool is None:
            if not a.start: raise SystemExit('No spool yet: pass --start with the first ledger to follow')
            if a.peers: raise SystemExit('No spool yet: a spool followed from peers starts from a node (--node) or a restore (xrpla restore --to spool)')
            spool = self.work / 'spools' / f'spool-{a.start}'; durable(self.work / 'current', spool.name.encode())
            self.follow(spool, a.start)
        last_publish = 0
        while not self.stopping:
            # Short of space the follower pauses; publishing, pushing and reclaiming go on.
            self.keep_following(spool)
            if (spool / 'manifest.json').exists() and time.time() - last_publish >= a.publish_seconds:
                last_publish = time.time(); self.note_spool(spool)
                if self.publish(spool) is not None: spool = self.maybe_rotate(spool)
                # A stop may arrive mid-publish, and rotation stops the follower.
                if self.follower is None and not self.stopping and not self.disk_low() and not self.reached_until(spool): self.follow(spool)
            manifest = spool / 'manifest.json'
            if manifest.exists() and manifest.stat().st_size > MANIFEST_WARNING and time.time() - self.warned > 3600:
                self.warned = time.time(); self.log(event='spool_manifest_large', bytes=manifest.stat().st_size, limit=16 * 1024 * 1024)
            self.touch()
            self.purge_retired()
            if a.once: break
            time.sleep(5)
        self.halt_follower()
        self.touch()


def health(workdir, max_age):
    """Problems with a runner, from its status.json: 0 when healthy, 1 otherwise."""
    path = Path(workdir) / STATUS; now = time.time(); problems = []; status = {}
    if not path.exists(): problems.append('no status.json: the runner has not started here')
    else:
        status = json.loads(path.read_bytes()); published = status.get('published')
        if now - status['updated'] > max_age: problems.append(f"status not updated for {int(now - status['updated'])} s: the runner is stopped or stuck")
        if status.get('disk_low'): problems.append('the disk is short of free space: the follower is paused')
        elif not status.get('follower_running'): problems.append('the follower is not running')
        if now - status.get('spool_tip_since', now) > max_age: problems.append(f"the spool tip has not advanced past {status['spool_tip']} for {int(now - status['spool_tip_since'])} s")
        if not published or now - published['time'] > max_age: problems.append(f'no successful publish run in the last {max_age} s')
        elif status.get('push') and (status.get('pushed') or {}).get('generation') != published['generation'] and now - published['time'] > max_age:
            problems.append(f"generation {published['generation']} is not pushed")
    print(json.dumps({'healthy': not problems, 'problems': problems, 'status': status}))
    return 1 if problems else 0


def main():
    if '--health' in sys.argv:
        h = argparse.ArgumentParser(description='Report whether a live runner is publishing.')
        h.add_argument('--health', action='store_true'); h.add_argument('--workdir', type=Path, required=True)
        h.add_argument('--max-age', type=int, default=1800, help='seconds without progress before a check fails')
        a = h.parse_args(); raise SystemExit(health(a.workdir, a.max_age))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--node', help='the node: grpc://HOST:PORT (xrpld gRPC) or http(s):// JSON-RPC (xrpld or Clio); docs/SETUP.md')
    source.add_argument('--peers', help="default (the network's hubs) or HOST:PORT,...: follow the peer network instead of a node "
                        "(xrpla publish --peers; the spool must exist and be within the peers' history of the network)")
    p.add_argument('--peer-identity', type=Path, help='with --peers: the node key file used toward peers (default: a new key each run)')
    p.add_argument('--network-id', type=int, default=0, help='with --peers: the network (0: mainnet, whose validator list is built in)')
    p.add_argument('--grpc-source', help='xrpld gRPC URL beside a JSON-RPC --node (gRPC then serves ledgers and state)')
    p.add_argument('--grpc-jobs', type=int, help='with --grpc-source: fetch ledgers on this many connections to it as well as on --rpc-jobs to --node (xrpla publish --grpc-jobs)')
    p.add_argument('--history-peers', help="default (peers the network's crawl lists as holding the ledger), none, or HOST:PORT,...: where raw transaction trees come from for ledgers a gRPC node omits, a few of 2013 (xrpla publish --history-peers; default: none)")
    p.add_argument('--history-identity', type=Path, help='node key file this node uses toward history peers (default: a new key each run)')
    p.add_argument('--workdir', type=Path, required=True); p.add_argument('--spool-seed', type=Path, required=True)
    p.add_argument('--start', type=int, help='first ledger for a new spool'); p.add_argument('--xrpla', required=True)
    p.add_argument('--until', type=int, help='follow no further than this ledger (a backfill of an earlier range); publishing goes on')
    p.add_argument('--bucket', type=Path, required=True); p.add_argument('--signing-key', type=Path, required=True)
    p.add_argument('--decoder', required=True); p.add_argument('--snapshot-tool', required=True)
    p.add_argument('--encoder', help='xrpla_history_encode binary (compiled encoder)')
    p.add_argument('--index-tool', help='xrpla_tx_index binary (compiled bundle index)')
    p.add_argument('--piece-ledgers', type=int, default=50_000); p.add_argument('--bundle-pieces', type=int, default=20)
    p.add_argument('--workers', type=int, default=4); p.add_argument('--grace-seconds', type=int, default=7 * 86400)
    # The piece still filling is published in fragments of this many ledgers (0: none), so the
    # publication trails the network by under that many (5,000: about 5.4 hours) rather than up to
    # a piece. For a publication others read (R2); a local one serves its spool as the live tail.
    p.add_argument('--fragment-ledgers', type=int, default=0)
    # And the fragments after the newest one in smaller fragments (250: about 15 minutes), so a new
    # node catches up the rest from peers in minutes. Each is replaced when its fragment completes.
    p.add_argument('--tip-fragment-ledgers', type=int, default=0)
    # xrpla publish writes one chunk per poll when it keeps up with the node, and the spool manifest
    # is limited to 16 MiB. Polling every 10 minutes keeps ~150 ledgers per chunk: ~3 MB per million.
    p.add_argument('--batch-ledgers', type=int, default=256); p.add_argument('--poll-seconds', type=int, default=600)
    p.add_argument('--rpc-jobs', type=int, default=4); p.add_argument('--publish-seconds', type=int, default=60)
    # The follower's connections to the node (--rpc-jobs) each wait this long before a request.
    p.add_argument('--delay-ms', type=int, help='pause before each request to the node (xrpla publish default: 200)')
    p.add_argument('--push', action='store_true', help='mirror each catalog generation to R2/S3 (XRPLA_S3_* variables), then delete local copies R2 holds')
    p.add_argument('--keep-pushed', action='store_true', help='with --push, keep every local copy (for a bucket something else reads)')
    # Low by default: the connected profile runs this on machines of any size; a publisher sets
    # a margin for what a push can leave behind while R2 is unreachable.
    p.add_argument('--min-free-gb', type=float, default=5, help='pause the follower while the work directory has less free space (default 5)')
    p.add_argument('--merge-chunks', action='store_true', help='merge small spool chunks (for a follower polling every few seconds)')
    # Trust a quorum of listed validators instead of the node's "validated" flag (xrpla publish).
    p.add_argument('--validator-list'); p.add_argument('--validator-list-key'); p.add_argument('--validations-node')
    p.add_argument('--once', action='store_true', help='one follow/publish/rotate pass, for tests')
    Runner(p.parse_args()).run()


if __name__ == '__main__':
    main()
