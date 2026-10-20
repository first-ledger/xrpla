#!/usr/bin/env python3
"""Parity of a full-history XRPLA server with a reference xrpld across all of history
(docs/internal/PARITY-PLAN.md, comparisons 1 and 2).

Every million-ledger interval the server holds (a bundle, and the newest one still filling)
is compared on its own with tools/xrpld_parity.py: random ledgers, plus chosen ones (the
interval's first and last ledger, the first and last of a randomly picked 50,000-ledger piece,
two flag ledgers, and any --include ledger inside it), with requests derived from their
contents. With --transports, each request is also asked over the server's other endpoints and
must get the HTTP answer.

  parity_campaign.py --xrpla http://127.0.0.1:5005 --reference https://xrplcluster.com --out DIR
                     [--transports ws://127.0.0.1:6006,https://HOST:51234,wss://HOST:51233] [--jobs 4]

DIR/run.json records the run (servers' versions, publication generation, settings);
DIR/intervals/FIRST-LAST.jsonl holds each interval's per-method results, summary and exit code,
and DIR/progress.jsonl one line per finished interval. Run again with the same DIR to resume:
finished intervals are kept, except one that failed only because too many of its requests went
unanswered (a reference out or rate-limited, a slow stretch at XRPLA), which is run again, up to
RETRIES attempts in all (the earlier files are kept beside it as FIRST-LAST.jsonl.unanswered-N).
DIR/summary.json totals everything at the end.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import random
import subprocess
import sys
import threading
import time
import urllib.request

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))
from xrpld_parity import SERVER_METHODS, call, share

INTERVAL, PIECE = 1_000_000, 50_000
# Known differences of the reference's xrpld version, not of the data (publication_audit.py).
KNOWN = [r'^/type: only xrpla$']
# 2013 ledgers whose transactions current xrpld cannot decode (docs: undecodable history).
UNDECODABLE = [625681, 625753]
# Attempts at an interval whose runs fail only on unanswered requests, across resumes.
RETRIES = 3


def stamp(): return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def progress_records(out):
    """DIR/progress.jsonl's entries with a summary, by interval, and the count of lines that do not
    parse (one cut short by a stop)."""
    records, unreadable = {}, 0
    if (out / 'progress.jsonl').exists():
        for line in (out / 'progress.jsonl').read_text().splitlines():
            try: entry = json.loads(line) if line.startswith('{') else {}
            except ValueError: unreadable += 1; continue
            if entry.get('summary') is not None: records.setdefault(tuple(entry['interval']), []).append(entry)
    return records, unreadable


def recorded_exit(lines, entries):
    """An interval's exit code: the one its file records (its last line), else, for a file written
    before the files kept it, that of its last progress entry with a summary; None when neither has it."""
    if lines and 'exit' in lines[-1]: return lines[-1]['exit']
    return entries[-1].get('exit') if entries else None


def unanswered_only(exit, summary):
    """Whether a run failed only because too many of its requests went unanswered: no difference
    and no error at XRPLA, over any endpoint (none another endpoint failed to answer either)."""
    transports = summary.get('transports', {})
    return (exit not in (0, None) and summary.get('unanswered', 0) > summary.get('unanswered_allowed', summary.get('unanswered', 0))
            and not summary.get('different') and not summary.get('xrpla_errors') and not transports.get('different') and not transports.get('errors'))


def unanswered_attempts(path):
    """The earlier attempts at an interval set aside because they failed only on unanswered requests."""
    return len(list(path.parent.glob(path.name + '.unanswered-*')))


def run_again(path, entries):
    """Why an interval's file is not kept on resume, or None when it stands: 'unrecorded' when no
    exit is recorded for it (a file from before files kept their exit, whose progress entry a stop
    cut short or never wrote), 'unanswered'
    when it failed only on unanswered requests and fewer than RETRIES attempts were made."""
    lines = [json.loads(l) for l in path.read_text().splitlines()]
    exit = recorded_exit(lines, entries)
    if exit is None: return 'unrecorded'
    summary = next(l for l in reversed(lines) if 'overlap' in l)
    if unanswered_only(exit, summary) and unanswered_attempts(path) + 1 < RETRIES: return 'unanswered'
    return None


def intervals(first, last):
    out, k = [], (first - 1) // INTERVAL
    while k * INTERVAL + 1 <= last:
        out.append((max(first, k * INTERVAL + 1), min(last, (k + 1) * INTERVAL))); k += 1
    return out


def chosen(a, b, rng, include):
    """The interval's chosen ledgers: its ends, one piece's ends, two flag ledgers, and any named."""
    points = {a, b}
    pieces = [(max(a, p + 1), min(b, p + PIECE)) for p in range((a - 1) // PIECE * PIECE, b, PIECE)]
    piece = rng.choice(pieces); points |= {piece[0], piece[1]}
    flags = [n for n in range((a + 255) // 256 * 256, b + 1, 256)]
    points |= set(rng.sample(flags, min(2, len(flags))))
    points |= {n for n in include if a <= n <= b}
    return sorted(points)


def generation(source):
    try:
        with urllib.request.urlopen(urllib.request.Request(source.rstrip('/') + '/catalog/current.json', headers={'User-Agent': 'xrpla-parity'}), timeout=30) as r:
            return json.load(r)['generation']
    except (OSError, ValueError, KeyError): return None


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--xrpla', required=True)
    p.add_argument('--reference', required=True, help='the reference server, or several (comma-separated) asked in turn')
    p.add_argument('--arbiter', help='an xrpld asked about each difference when the reference is not xrpld (xrpld_parity.py --arbiter)')
    p.add_argument('--reference-retries', type=int, help='passed to xrpld_parity.py: one Clio answers alike, a load-balanced cluster may not')
    p.add_argument('--max-unanswered', type=share, help="passed to xrpld_parity.py: the share of an interval's ledger requests that may go unanswered")
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--transports', default='', help="the server's other endpoints, comma-separated (xrpld_parity.py --transports)")
    p.add_argument('--jobs', type=int, default=2, help='intervals compared at once: requests in flight to the reference (xrplcluster.com allows '
                   '500,000 units an hour; 2 keeps within it, and a spent quota is waited out)')
    p.add_argument('--random-ledgers', type=int, default=8, help='random ledgers per interval, besides the chosen ones')
    p.add_argument('--transactions', type=int, default=20); p.add_argument('--accounts', type=int, default=20)
    p.add_argument('--objects', type=int, default=60); p.add_argument('--books', type=int, default=10)
    p.add_argument('--include', default='', help='ledgers always compared, comma-separated (besides the 2013 ones xrpld cannot decode)')
    p.add_argument('--seed', type=int, default=1)
    p.add_argument('--source', default='https://xrpla-data.firstledger.net/mainnet', help='the publication served, for its generation in run.json')
    a = p.parse_args()
    out = a.out; (out / 'intervals').mkdir(parents=True, exist_ok=True)
    served = call(a.xrpla, 'ledger_range', {})[0]
    first, last = served['ledger_index_min'], served['ledger_index_max']
    include = UNDECODABLE + [int(n) for n in a.include.split(',') if n]
    run = {'started': stamp(), 'xrpla': a.xrpla, 'reference': a.reference, 'arbiter': a.arbiter, 'transports': [u for u in a.transports.split(',') if u],
           'served': [first, last], 'publication_generation': generation(a.source), 'seed': a.seed, 'jobs': a.jobs,
           'per_interval': {'random_ledgers': a.random_ledgers, 'transactions': a.transactions, 'accounts': a.accounts, 'objects': a.objects, 'books': a.books},
           'known_differences': KNOWN, 'include': include}
    for name, url in (('xrpla_server', a.xrpla), ('reference_server', a.reference.split(',')[0])):
        try: info = call(url, 'server_info', {})[0]['info']; run[name] = {'build_version': info.get('build_version'), 'complete_ledgers': info.get('complete_ledgers')}
        except (OSError, ValueError, KeyError) as error: run[name] = {'error': str(error)}
    if (out / 'run.json').exists():
        # A resumed run keeps its record; a change of reference, of XRPLA's build or of the
        # publication served is noted with the intervals done before it.
        earlier = json.loads((out / 'run.json').read_text())
        changed = {k: run[k] for k in ('reference', 'arbiter', 'publication_generation') if earlier.get(k) != run[k]}
        if (earlier.get('xrpla_server') or {}).get('build_version') != run['xrpla_server'].get('build_version'): changed['xrpla_server'] = run['xrpla_server']
        run = {**earlier, 'resumed': stamp()}
        if changed: run.setdefault('reference_changes', []).append({'at': stamp(), **changed, 'intervals_done': len(list((out / 'intervals').glob('*.jsonl')))})
    (out / 'run.json').write_text(json.dumps(run, indent=1))
    # A run stopped while writing a line of progress.jsonl leaves it unfinished: end it, so the
    # next line is not appended to it (summarize skips the broken one).
    progress = out / 'progress.jsonl'
    if progress.exists() and progress.stat().st_size and not progress.read_bytes().endswith(b'\n'):
        with open(progress, 'a') as f: f.write('\n')
    records, _ = progress_records(out)
    lock = threading.Lock()

    def compare(a_first, a_last, attempt=1):
        target = out / 'intervals' / f'{a_first}-{a_last}.jsonl'
        again = run_again(target, records.get((a_first, a_last), [])) if target.exists() else None
        if target.exists():
            if again is None: return None
            # Set aside, not replaced: those failed on unanswered requests count the attempts made.
            n = unanswered_attempts(target) + 1 if again == 'unanswered' else 1
            target.replace(target.with_name(f'{target.name}.{again}-{n}'))
        rng = random.Random(a.seed * 1_000_003 + a_first)
        points = chosen(a_first, a_last, rng, include)
        command = [sys.executable, str(TOOLS / 'xrpld_parity.py'), '--xrpld', a.reference, '--xrpla', a.xrpla, '--range', f'{a_first}-{a_last}',
                   '--ledgers', str(a.random_ledgers), '--transactions', str(a.transactions), '--accounts', str(a.accounts), '--objects', str(a.objects),
                   '--books', str(a.books), '--min-limit', '10', '--seed', str(a.seed * 1_000_003 + a_first), '--include-ledgers', ','.join(map(str, points))]
        if a.transports: command += ['--transports', a.transports]
        if a.arbiter: command += ['--arbiter', a.arbiter]
        if a.reference_retries is not None: command += ['--reference-retries', str(a.reference_retries)]
        if a.max_unanswered is not None: command += ['--max-unanswered', str(a.max_unanswered)]
        for pattern in KNOWN: command += ['--ignore', pattern]
        started = time.time()
        result = subprocess.run(command, capture_output=True, text=True)
        lines = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
        summary = next((l for l in reversed(lines) if 'overlap' in l), None)
        entry = {'time': stamp(), 'interval': [a_first, a_last], 'chosen': points, 'seconds': round(time.time() - started),
                 'exit': result.returncode, 'summary': summary, 'attempt': attempt}
        if again: entry['run_again'] = again
        if summary is None: entry['error'] = result.stderr[-2000:]
        else:
            # The file records its exit itself: a stop before the progress entry below leaves it readable.
            lines.append({'exit': result.returncode, 'attempt': attempt, 'time': entry['time']})
            temporary = target.parent / (target.name + '.tmp')
            temporary.write_text('\n'.join(map(json.dumps, lines)) + '\n'); temporary.replace(target)
        with lock:
            with open(out / 'progress.jsonl', 'a') as f: f.write(json.dumps(entry) + '\n')
        print(json.dumps(entry), flush=True)
        # An interval that ended without a result (the run itself failed) is run again, twice at most.
        if summary is None and attempt < 3:
            time.sleep(60)
            return compare(a_first, a_last, attempt + 1)
        return entry

    todo = intervals(first, last)
    with ThreadPoolExecutor(max_workers=a.jobs) as pool:
        for future in as_completed([pool.submit(compare, x, y) for x, y in todo]): future.result()

    summary, passed = summarize(out, todo)
    (out / 'summary.json').write_text(json.dumps(summary, indent=1))
    print(json.dumps({k: v for k, v in summary.items() if k != 'differing'}))
    sys.exit(0 if passed else 1)


def summarize(out, todo):
    """The run's totals from DIR's interval files and progress.jsonl, and whether it passed: every
    interval compared, each one's xrpld_parity.py run passing (exit 0) and comparing something, no
    differences, and at least one matching answer about a ledger in all. Intervals run again after
    failing only on unanswered requests are listed (`run_again`), with the attempts made at each."""
    totals = {'same': 0, 'same_errors': 0, 'different': 0, 'timeouts': 0, 'xrpla_errors': 0, 'reference_errors': 0, 'reference_inconsistent': 0, 'transports_same': 0,
              'transports_different': 0, 'transport_errors': 0, 'waited_seconds': 0}
    # Each interval's exit code: the one its file records, else (a file written before files kept
    # it) that of the attempt that wrote it (the last with a summary). A progress line that does not
    # parse (one cut short by a stop) is skipped and counted: an interval whose file records no exit,
    # if that was its only record, then fails for want of one.
    records, unreadable = progress_records(out)
    differing, missing, failed, retried = [], [], [], []
    for x, y in todo:
        path = out / 'intervals' / f'{x}-{y}.jsonl'
        if not path.exists(): missing.append([x, y]); continue
        lines = [json.loads(l) for l in path.read_text().splitlines()]
        s = next(l for l in reversed(lines) if 'overlap' in l); exit = recorded_exit(lines, records.get((x, y), []))
        earlier = unanswered_attempts(path)
        if earlier: retried.append({'interval': [x, y], 'attempts': earlier + 1, 'passed': exit == 0})
        for k in ('same_errors', 'different', 'timeouts', 'xrpla_errors', 'reference_errors', 'reference_inconsistent'): totals[k] += s.get(k, 0)
        # Matching answers about ledgers, from the per-method lines: an interval file written before
        # xrpld_parity.py counted version and ping apart has them in its summary's `same`.
        same = sum(l.get('same', 0) for l in lines if 'method' in l and l['method'] not in SERVER_METHODS and not l.get('xrpla_only'))
        totals['same'] += same
        totals['transports_same'] += s.get('transports', {}).get('same', 0); totals['transports_different'] += s.get('transports', {}).get('different', 0)
        totals['transport_errors'] += s.get('transports', {}).get('errors', 0); totals['waited_seconds'] += s.get('waited_seconds', 0)
        # An interval whose run failed (an exit not 0, or none recorded) or that compared nothing
        # does not pass because the others did; one that failed only on unanswered requests says
        # so, with the attempts made at it.
        reasons = ([f'exit {exit}'] if exit != 0 else []) + (['compared nothing'] if same + s.get('different', 0) == 0 else [])
        if unanswered_only(exit, s):
            reasons.append(f"{s['unanswered']} of {s.get('requests')} requests unanswered ({s['unanswered_allowed']} allowed), after {earlier + 1} attempts")
        if reasons: failed.append({'interval': [x, y], 'reasons': reasons})
        for l in lines:
            if 'method' in l and (l.get('different') or l.get('transports_different') or l.get('xrpla_errors')):
                differing.append({'interval': [x, y], 'method': l['method'], 'different': l.get('different', 0),
                                  'transports_different': l.get('transports_different', 0), 'examples': l.get('examples', []) + l.get('transport_examples', [])})
    summary = {'finished': stamp(), 'intervals': len(todo), 'missing': missing, 'failed': failed, 'run_again': retried, **totals, 'differing': differing}
    if unreadable: summary['progress_unreadable_lines'] = unreadable
    passed = (not missing and not failed and totals['same'] > 0 and not totals['different'] and not totals['transports_different']
              and not totals['xrpla_errors'])
    return summary, passed


if __name__ == '__main__':
    main()
