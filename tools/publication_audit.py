#!/usr/bin/env python3
"""Audit a publication in R2/S3 as its users use it, one bundle at a time.

For each million-ledger bundle the remote catalog lists that this workdir has not audited:
downloads the bundle and the snapshot it starts from (each checked against the SHA-256 the
catalog lists; the catalog's signature is checked by sync), then, with the publisher key pinned
here (never the remote copy):

  sync      xrpla sync into a piece cache, which verifies signatures and builds the query indexes
  restore   xrpla restore to each of --restores (nudb for xrpld, store, clio), which replays and
            checks every ledger
  parity    xrpla serve over the cache, against a full-history xrpld (tools/xrpld_parity.py):
            sampled ledgers, transactions, accounts and objects, plus any ledger whose
            transactions the publisher read from peers (--follower-log)

Reads the bucket only (XRPLA_S3_* variables; nothing is written or deleted remotely). Each
result is a line in WORKDIR/audited.jsonl; WORKDIR/status.json holds the latest. A failed
bundle's files stay in WORKDIR/failed-FIRST-LAST for inspection and it is not retried; a
parity run the reference server could not complete is retried later.

Several runners can share a workdir on disjoint ranges (--from-ledger, --until-ledger), each
auditing one bundle at a time. A bundle whose object passed parity in another audit
(--parity-from: its workdir, e.g. the audit of a publication it was copied from) reuses that
result when that audit recorded the same SHA-256 (older entries, without one, are not reused):
the download is checked against the catalog's SHA-256 and sync, which checks the catalog's
signature with the pinned key, must pass first, so the bytes are the ones compared then.
Download and sync still run.

  publication_audit.py --workdir DIR --xrpla-dir INSTALL --publisher-key FILE \\
      [--restores nudb,store,clio] [--reference https://xrplcluster.com] [--watch 1800]
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))
from s3_remote import S3, remote_pointer  # noqa: E402

# Differences from the reference that are not about the data: xrplcluster.com's book_changes has
# no "type", which stock xrpld 3.4.0 (rpc/BookChanges.h) sets, as XRPLA does.
KNOWN = [r'^/type: only xrpla$']


def stamp(): return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def log(**event): print(json.dumps({'time': stamp(), **event}), flush=True)


def without_credentials():
    """This process's environment less the object store's credentials (XRPLA_S3_*): the servers,
    syncs and restores it runs read local files and public endpoints only."""
    return {k: v for k, v in os.environ.items() if not k.startswith('XRPLA_S3_')}


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]


class Audit:
    def __init__(self, a):
        self.a = a; self.work = a.workdir; self.work.mkdir(parents=True, exist_ok=True)
        self.xrpla = str(a.xrpla_dir / 'bin' / 'xrpla')
        self.done = {}
        path = self.work / 'audited.jsonl'
        if path.exists():
            for line in path.read_text().splitlines():
                entry = json.loads(line)
                if entry['result'] in ('passed', 'failed'): self.done[entry['object']] = entry['result']
        # Parity results of other audits, by object: from bundles that passed whole.
        self.earlier = {}
        for other in a.parity_from or []:
            for line in (other / 'audited.jsonl').read_text().splitlines() if (other / 'audited.jsonl').exists() else []:
                entry = json.loads(line)
                parity = next((s for s in entry.get('steps', []) if s['step'] == 'parity'), None)
                if entry['result'] == 'passed' and parity and parity['ok']: self.earlier[entry['object']] = (other, entry, parity)

    def wanted(self, ranges):
        """The bundles of `ranges` this runner audits: not yet done, within its ledgers."""
        return [r for r in ranges if r['kind'] == 'bundle' and r['object'] not in self.done
                and r['last'] >= self.a.from_ledger and (not self.a.until_ledger or r['last'] <= self.a.until_ledger)]

    def earlier_parity(self, rng):
        """The parity step of another audit of these very bytes (the same SHA-256), or None."""
        found = self.earlier.get(rng['object'])
        if not found: return None
        other, entry, parity = found
        if entry.get('sha256') != rng['sha256']: return None
        return {'step': 'parity', 'ok': True, 'seconds': 0, 'reused': {'workdir': str(other), 'audited': entry['time']},
                **{k: parity[k] for k in ('summary', 'peer_filled') if k in parity}}

    def record(self, **entry):
        entry = {'time': stamp(), **entry}
        with (self.work / 'audited.jsonl').open('a') as out: out.write(json.dumps(entry) + '\n')
        status = {'updated': int(time.time()), 'last': entry,
                  'passed': sum(1 for r in self.done.values() if r == 'passed'),
                  'failed': sorted(o for o, r in self.done.items() if r == 'failed')}
        (self.work / 'status.json').write_text(json.dumps(status, indent=1))
        log(event='audited', **entry)

    def step(self, name, command, run, env=None):
        started = time.time()
        with (run / f'{name}.out').open('w') as out, (run / f'{name}.err').open('w') as err:
            code = subprocess.run(command, stdout=out, stderr=err, env=env or without_credentials()).returncode
        seconds = round(time.time() - started)
        tail = (run / f'{name}.err').read_text(errors='replace')[-800:] if code else ''
        return {'step': name, 'ok': code == 0, 'seconds': seconds, **({'error': tail} if tail else {})}

    def peer_filled(self, first, last):
        """Ledgers in [first, last] whose transactions the publisher read from peers."""
        found = set()
        if self.a.follower_log and self.a.follower_log.exists():
            for line in self.a.follower_log.read_text(errors='replace').splitlines():
                if '"transactions_from_peers"' in line:
                    try: seq = json.loads(line)['ledger']
                    except (ValueError, KeyError): continue
                    if first <= seq <= last: found.add(seq)
        return sorted(found)

    def parity(self, cache, first, last, run):
        """One parity run; None when the reference server could not be reached."""
        http, ws = free_port(), free_port()
        server = subprocess.Popen([self.xrpla, 'serve', '--dir', str(cache), '--http', f'127.0.0.1:{http}', '--ws', f'127.0.0.1:{ws}'],
                                  stdout=(run / 'serve.log').open('w'), stderr=subprocess.STDOUT, env=without_credentials())
        try:
            for _ in range(120):
                try:
                    with socket.create_connection(('127.0.0.1', http), timeout=1): break
                except OSError: time.sleep(1)
            include = self.peer_filled(first, last)
            command = [sys.executable, str(TOOLS / 'xrpld_parity.py'), '--xrpld', self.a.reference, '--xrpla', f'http://127.0.0.1:{http}',
                       '--ledgers', str(self.a.ledgers), '--min-limit', '10', '--seed', str(first)]
            if include: command += ['--include-ledgers', ','.join(map(str, include))]
            for pattern in KNOWN: command += ['--ignore', pattern]
            result = self.step('parity', command, run)  # fails when any answer differs
            lines = (run / 'parity.out').read_text().splitlines()
            summary = json.loads(lines[-1]) if lines and lines[-1].startswith('{"overlap"') else None
            if summary is None: return None  # the reference (or the network) failed mid-run
            result['summary'] = summary; result['peer_filled'] = include
            result['slow_requests'] = summary.get('timeouts', 0)  # XRPLA took over 120 s: speed, not correctness
            result['reference_errors'] = summary.get('reference_errors', 0)  # the reference could not answer
            result['reference_inconsistent'] = summary.get('reference_inconsistent', 0)  # matched only on asking again
            result['differences'] = [json.loads(l) for l in lines[:-1] if json.loads(l).get('different') or json.loads(l).get('timeouts')][:8]
            return result
        finally:
            server.terminate()
            try: server.wait(timeout=30)
            except subprocess.TimeoutExpired: server.kill()

    def bundle(self, s3, pointer, catalog, rng):
        first, last = rng['first'], rng['last']
        run = self.work / f'run-{first}-{last}'
        shutil.rmtree(run, ignore_errors=True); run.mkdir()
        mirror = run / 'bucket'
        started = time.time()
        # The catalog and its signature (checked by sync, with the pinned key), and the objects this
        # range needs, each checked against the SHA-256 the catalog lists.
        for name in ('publisher.pub', pointer['object'], pointer['object'] + '.sig'):
            (mirror / name).parent.mkdir(parents=True, exist_ok=True); (mirror / name).write_bytes(s3.get(name))
        (mirror / 'catalog/current.json').write_text(json.dumps(pointer))
        snapshot = next((s for s in catalog['snapshots'] if s['seq'] == first - 1), None)
        if snapshot is None: raise ValueError(f'No snapshot at {first - 1} for the bundle {first}-{last}')
        index = snapshot['index']
        s3.download(index['object'], mirror / index['object'], index['sha256'], index.get('size'))
        fetched = rng['size']
        for part in json.loads((mirror / index['object']).read_bytes())['parts']:
            s3.download(part['object'], mirror / part['object'], part['sha256'], part.get('size')); fetched += part['size']
        s3.download(rng['object'], mirror / rng['object'], rng['sha256'], rng['size'])
        steps = [{'step': 'download', 'ok': True, 'seconds': round(time.time() - started), 'bytes': fetched}]
        span = f'{first}-{last}'
        key = str(self.a.publisher_key)
        steps.append(self.step('sync', [self.xrpla, 'sync', '--source', str(mirror), '--publisher-key', key, '--dir', str(run / 'cache'),
                                        '--range', span, '--jobs', str(self.a.jobs)], run))
        for kind in self.a.restores:
            if not steps[-1]['ok']: break
            command = [self.xrpla, 'restore', '--source', str(mirror), '--publisher-key', key, '--to', kind, '--out', str(run / kind), '--range', span]
            if kind == 'nudb': command += ['--jobs', str(self.a.jobs)]
            steps.append(self.step(f'restore-{kind}', command, run))
            shutil.rmtree(run / kind, ignore_errors=True)  # checked; the output is not kept
        reused = self.earlier_parity(rng) if all(s['ok'] for s in steps) else None
        if reused: steps.append(reused)
        elif all(s['ok'] for s in steps) and self.a.reference:
            parity = None
            for attempt in range(3):
                parity = self.parity(run / 'cache', first, last, run)
                if parity is not None: break
                time.sleep(600)
            if parity is None:
                error = (run / 'parity.err').read_text(errors='replace')[-800:] if (run / 'parity.err').exists() else ''
                self.record(object=rng['object'], sha256=rng['sha256'], range=[first, last], result='parity_unavailable', steps=steps, error=error)
                shutil.rmtree(run, ignore_errors=True)
                return
            steps.append(parity)
        ok = all(s['ok'] for s in steps)
        self.done[rng['object']] = 'passed' if ok else 'failed'
        self.record(object=rng['object'], sha256=rng['sha256'], range=[first, last], generation=catalog['generation'], result='passed' if ok else 'failed',
                    seconds=round(time.time() - started), steps=steps)
        if ok: shutil.rmtree(run, ignore_errors=True)
        else:
            shutil.rmtree(run / 'bucket', ignore_errors=True)  # R2 holds it; keep the logs and cache
            os.replace(run, self.work / f'failed-{first}-{last}')

    def once(self):
        s3 = S3.from_environment()
        pointer, _ = remote_pointer(s3)
        if not pointer: log(event='no_publication', prefix=s3.prefix); return 0
        catalog = json.loads(s3.get(pointer['object']))
        todo = self.wanted(catalog['ranges'])
        log(event='pending', prefix=s3.prefix, generation=catalog['generation'], bundles=len(todo))
        for rng in todo[:self.a.max_bundles]:
            if (shutil.disk_usage(self.work).free >> 30) < self.a.min_free_gb:
                log(event='disk_low', free_gb=shutil.disk_usage(self.work).free >> 30); break
            try: self.bundle(s3, pointer, catalog, rng)
            except Exception as error:  # a download or catalog problem: recorded, retried next pass
                self.record(object=rng['object'], sha256=rng.get('sha256'), range=[rng['first'], rng['last']], result='error', error=str(error)[-1000:])
        return len(todo)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--workdir', type=Path, required=True)
    p.add_argument('--xrpla-dir', type=Path, required=True, help='install directory (bin/xrpla)')
    p.add_argument('--publisher-key', type=Path, required=True, help='the publisher key, pinned (not read from the bucket)')
    p.add_argument('--restores', default='nudb,store,clio', type=lambda t: [k for k in t.split(',') if k and k != 'none'],
                   help='restore targets to check (none: sync and parity only)')
    p.add_argument('--reference', default='https://xrplcluster.com', help="a full-history xrpld's JSON-RPC URL (empty: no parity)")
    p.add_argument('--ledgers', type=int, default=12, help='ledgers sampled per bundle for parity')
    p.add_argument('--follower-log', type=Path, help="the publisher's follower.log: its transactions_from_peers ledgers are always compared")
    p.add_argument('--jobs', type=int, default=4)
    p.add_argument('--from-ledger', type=int, default=0, help='only bundles ending at or after this ledger')
    p.add_argument('--until-ledger', type=int, default=0, help='only bundles ending at or before this ledger')
    p.add_argument('--parity-from', type=Path, action='append', help="another audit's workdir whose parity results its objects reuse")
    p.add_argument('--max-bundles', type=int, default=1, help='bundles audited per pass')
    p.add_argument('--min-free-gb', type=int, default=200, help='skip a pass while less space is free')
    p.add_argument('--watch', type=int, help='repeat every N seconds')
    a = p.parse_args()
    audit = Audit(a)
    while True:
        pending = audit.once()
        if not a.watch: break
        time.sleep(a.watch if pending <= a.max_bundles else 60)


if __name__ == '__main__':
    main()
