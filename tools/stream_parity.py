#!/usr/bin/env python3
"""Compare xrpla's ledger and transaction streams with an xrpld node's.

  stream_parity.py capture --node ws://127.0.0.1:6006 --seconds 90 --out FILE
  stream_parity.py compare --capture FILE --xrpla http://127.0.0.1:5105
  stream_parity.py books --node ws://127.0.0.1:6006 --xrpla ws://127.0.0.1:6106 --book JSON

`capture` records the node's `ledger` and `transactions` stream messages, once in API
version 1 and once in version 2. A connection the node closes is opened again and subscribed
anew; the ledgers around the gap are left out of the comparison. A capture file ending in .gz
is read compressed. `compare` asks an xrpla server (which must hold those
ledgers, e.g. a piece cache with the live spool as its tail) for the messages its own
streams would send for the same ledgers, and reports every difference by path.
"""
import argparse
import gzip
import json
from pathlib import Path
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture_validations import WebSocket
from xrpld_parity import share


def capture(a):
    messages = {1: [], 2: []}

    def listen(version):
        deadline = time.time() + a.seconds
        while time.time() < deadline:
            try:
                ws = WebSocket(a.node); ws.sock.settimeout(120)
                ws.send({'id': 1, 'command': 'subscribe', 'streams': ['ledger', 'transactions'], 'api_version': version})
                while time.time() < deadline: messages[version].append(ws.receive())
            except (OSError, ConnectionError, ValueError) as error:
                # A gap: the ledgers around it are not compared (compare's `interrupted`).
                messages[version].append({'type': 'reconnected', 'time': time.time(), 'error': str(error)})
                time.sleep(5)
    threads = [threading.Thread(target=listen, args=(v,), daemon=True) for v in (1, 2)]
    for t in threads: t.start()
    for t in threads: t.join(a.seconds + 30)
    Path(a.out).write_text(json.dumps({'1': messages[1], '2': messages[2]}))
    closed = [m['ledger_index'] for m in messages[1] if m.get('type') == 'ledgerClosed']
    print(json.dumps({'ledgers': [min(closed), max(closed)] if closed else None,
                      'transactions': sum(1 for m in messages[1] if m.get('type') == 'transaction'),
                      'reconnects': {v: sum(1 for m in messages[v] if m.get('type') == 'reconnected') for v in (1, 2)}}))


def call(url, method, params, patience=180):
    """xrpla's answer, waiting up to `patience` seconds while it cannot be reached (a restart)."""
    body = json.dumps({'method': method, 'params': [params]}).encode()
    deadline = time.time() + patience
    while True:
        try:
            with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=300) as r:
                return json.load(r)['result']
        except (urllib.error.URLError, ConnectionError):
            if time.time() > deadline: raise
            time.sleep(3)


def differences(expected, actual, path=''):
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            if key not in actual: yield f'{path}/{key}: missing (xrpld {json.dumps(expected[key])[:80]})'
            elif key not in expected: yield f'{path}/{key}: extra (xrpla {json.dumps(actual[key])[:80]})'
            else: yield from differences(expected[key], actual[key], f'{path}/{key}')
    elif isinstance(expected, list) and isinstance(actual, list) and len(expected) == len(actual):
        for i, (e, x) in enumerate(zip(expected, actual)): yield from differences(e, x, f'{path}[{i}]')
    elif expected != actual:
        yield f'{path}: xrpld {json.dumps(expected)[:80]} xrpla {json.dumps(actual)[:80]}'


def settled(url, method, params, seconds=180):
    """xrpla's answer once it holds the ledger: it trails the network by a ledger or two."""
    deadline = time.time() + seconds
    while True:
        result = call(url, method, params)
        if 'error' not in result or time.time() > deadline: return result
        time.sleep(5)


def compare(a):
    opener = gzip.open if a.capture.endswith('.gz') else open
    with opener(a.capture, 'rt') as f: captured = json.load(f)
    report = {'ledgers': 0, 'transactions': 0, 'identical': 0, 'interrupted': 0, 'incomplete': 0, 'unavailable': [], 'differences': {}}
    for version in ('1', '2'):
        # A ledger's messages, whatever order they came in: a load-balanced node (xrplcluster.com)
        # can send some of a ledger's transactions before its ledgerClosed, or a message twice.
        closed_by, txs_by, interrupted, current = {}, {}, set(), None
        for m in captured[version]:
            if m.get('type') == 'ledgerClosed': current = m['ledger_index']; closed_by.setdefault(current, m)
            elif m.get('type') == 'transaction': txs_by.setdefault(m['ledger_index'], {})[m.get('hash') or m['transaction']['hash']] = m
            elif m.get('type') == 'reconnected' and current is not None: interrupted |= {current, current + 1}
        index = lambda m: (m.get('meta') or {}).get('TransactionIndex', 0)
        # The capture may start or end inside a ledger's transactions, and a reconnection may cut
        # through the ledgers around it: only ledgers whose transactions all came are compared.
        for seq, closed_message in sorted(closed_by.items())[1:-1]:
            expected = {'closed': closed_message, 'txs': sorted(txs_by.get(seq, {}).values(), key=index)}
            if seq in interrupted: report['interrupted'] += 1; continue
            if len(expected['txs']) != closed_message.get('txn_count', len(expected['txs'])): report['incomplete'] += 1; continue
            closed = settled(a.xrpla, '_ledger_event', {'ledger_index': seq})
            if 'error' in closed: report['unavailable'].append(seq); continue
            closed.pop('status', None); closed['type'] = 'ledgerClosed'
            # The servers hold different ranges; each must end at the ledger itself.
            for m in (closed, expected['closed']): m['validated_ledgers'] = m['validated_ledgers'].rsplit('-', 1)[-1]
            events = [e['event'] for e in call(a.xrpla, '_transaction_events', {'ledger_index': seq, 'api_version': int(version)})['events']]
            report['ledgers'] += 1
            pairs = [(expected['closed'], closed)]
            if len(events) != len(expected['txs']): report['differences'].setdefault(f'v{version} transaction count', []).append(seq)
            pairs += list(zip(expected['txs'], events))
            for e, x in pairs:
                found = list(differences(e, x))
                report['transactions'] += e.get('type') == 'transaction'
                if not found: report['identical'] += 1
                for d in found:
                    key = f'v{version} ' + ''.join(c if not c.isdigit() else '#' for c in d.split(':')[0])
                    report['differences'].setdefault(key, []).append(d if len(report['differences'].get(key, [])) < 3 else seq)
    report['differences'] = {k: v[:3] + ([f'... {len(v)} in all'] if len(v) > 3 else []) for k, v in report['differences'].items()}
    report['unavailable'] = len(report['unavailable'])
    # A ledger xrpla never answered, or one the capture lost messages of (a reconnection, a load-
    # balanced node's missing transactions), is not compared: more than a few of them is a
    # failure, not a pass on the rest.
    left_out = report['unavailable'] + report['interrupted'] + report['incomplete']
    report['not_compared_allowed'] = int(a.max_unavailable * (report['ledgers'] + left_out))
    print(json.dumps(report, indent=1))
    too_many = left_out > report['not_compared_allowed']
    return 1 if report['differences'] or not report['ledgers'] or too_many else 0  # nothing compared proves nothing


def books(a):
    """Subscribes xrpld and xrpla to the same book at once; compares what each delivers per ledger."""
    book = json.loads(a.book)
    got = {'xrpld': {}, 'xrpla': {}}

    def listen(name, url):
        ws = WebSocket(url); ws.sock.settimeout(None)
        ws.send({'id': 1, 'command': 'subscribe', 'books': [book]})
        deadline = time.time() + a.seconds
        while time.time() < deadline:
            m = ws.receive()
            if m.get('type') == 'transaction':
                got[name].setdefault(m['ledger_index'], set()).add(m['transaction']['hash'])
    threads = [threading.Thread(target=listen, args=(n, u), daemon=True) for n, u in (('xrpld', a.node), ('xrpla', a.xrpla))]
    for t in threads: t.start()
    for t in threads: t.join(a.seconds + 60)
    # The ledgers inside xrpld's window: its first and last may be cut by the start and end of
    # listening, and xrpla trails the network by a ledger or two, so its last is left out too.
    # Bounded by xrpld's alone: a books stream that delivers nothing must not shrink the window.
    seqs = sorted(set(got['xrpld']) | set(got['xrpla']))
    newest = max(got['xrpld'] or [0]) - 2
    shared = [s for s in seqs if min(got['xrpld'] or [0]) < s < newest]
    differ = {s: {'xrpld_only': sorted(got['xrpld'].get(s, set()) - got['xrpla'].get(s, set())), 'xrpla_only': sorted(got['xrpla'].get(s, set()) - got['xrpld'].get(s, set()))}
              for s in shared if got['xrpld'].get(s, set()) != got['xrpla'].get(s, set())}
    print(json.dumps({'ledgers': len(shared), 'transactions': sum(len(got['xrpld'].get(s, ())) for s in shared), 'differ': differ}, indent=1))
    return 1 if differ or not shared else 0  # nothing compared proves nothing


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='mode', required=True)
    b = sub.add_parser('books'); b.add_argument('--node', default='ws://127.0.0.1:6006'); b.add_argument('--xrpla', required=True)
    b.add_argument('--seconds', type=int, default=120); b.add_argument('--book', required=True, help='a subscribe books entry, as JSON')
    c = sub.add_parser('capture'); c.add_argument('--node', default='ws://127.0.0.1:6006'); c.add_argument('--seconds', type=int, default=90); c.add_argument('--out', required=True)
    m = sub.add_parser('compare'); m.add_argument('--capture', required=True); m.add_argument('--xrpla', default='http://127.0.0.1:5105')
    m.add_argument('--max-unavailable', type=share, default=0.1,
                   help="the share of the captured ledgers left uncompared, unanswered by xrpla or cut short in the capture "
                        "(default 0.1); more fails the comparison")
    a = p.parse_args()
    raise SystemExit(capture(a) if a.mode == 'capture' else books(a) if a.mode == 'books' else compare(a))


if __name__ == '__main__':
    main()
