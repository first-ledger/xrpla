#!/usr/bin/env python3
"""Watches a server's ledger and transaction streams for hours through many subscribers.

  stream_soak.py --ws ws://127.0.0.1:6106 [--subscribers 20] [--out stream_soak.csv] [--alerts stream_alerts.log]

Every subscriber must see ledgers one after another (no gap, no repeat), each ledger's
transactions right after its ledgerClosed message and as many as its txn_count, and the same
messages as every other subscriber. A summary row is appended every 5 minutes; anything
wrong is appended to the alerts file as it happens. A subscriber that is disconnected
reconnects (and is reported).
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture_validations import WebSocket


class Subscriber:
    def __init__(self, name, url, alert):
        self.name, self.url, self.alert = name, url, alert
        self.digests = {}  # ledger -> digest of its messages
        self.last = None; self.ledgers = 0; self.transactions = 0; self.reconnects = 0
        self.current = None; self.pending = []
        threading.Thread(target=self.run, daemon=True).start()

    def close_ledger(self):
        if self.current is None: return
        seq, count = self.current['ledger_index'], self.current['txn_count']
        if len(self.pending) != count: self.alert(f'{self.name}: ledger {seq} had {len(self.pending)} transactions, txn_count {count}')
        digest = hashlib.sha256(json.dumps([self.current] + self.pending, sort_keys=True).encode()).hexdigest()
        self.digests[seq] = digest
        if len(self.digests) > 2000: self.digests.pop(min(self.digests))
        self.current, self.pending = None, []

    def run(self):
        while True:
            try:
                ws = WebSocket(self.url); ws.sock.settimeout(120)
                ws.send({'id': 1, 'command': 'subscribe', 'streams': ['ledger', 'transactions'], 'api_version': 2})
                reply = ws.receive()
                if reply.get('status') != 'success': self.alert(f'{self.name}: subscribe failed {reply}'); time.sleep(10); continue
                self.last = reply['result'].get('ledger_index', self.last)
                while True:
                    message = ws.receive()
                    if message.get('type') == 'ledgerClosed':
                        self.close_ledger()
                        seq = message['ledger_index']
                        if self.last is not None and seq != self.last + 1: self.alert(f'{self.name}: ledger {seq} after {self.last}')
                        self.last = seq; self.ledgers += 1; self.current = message
                    elif message.get('type') == 'transaction':
                        self.transactions += 1
                        if self.current is None or message.get('ledger_index') != self.current['ledger_index']:
                            self.alert(f'{self.name}: transaction of ledger {message.get("ledger_index")} outside its ledger')
                        else: self.pending.append(message)
            except Exception as error:
                self.reconnects += 1; self.current, self.pending = None, []
                self.alert(f'{self.name}: disconnected ({type(error).__name__}: {str(error)[:120]}); reconnecting')
                time.sleep(5)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--ws', required=True); p.add_argument('--subscribers', type=int, default=20)
    p.add_argument('--out', default='stream_soak.csv'); p.add_argument('--alerts', default='stream_alerts.log')
    p.add_argument('--interval', type=int, default=300)
    a = p.parse_args()
    lock = threading.Lock()
    def alert(text):
        with lock, open(a.alerts, 'a') as f: f.write(f'{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())} ALERT {text}\n')
    subscribers = [Subscriber(f's{n}', a.ws, alert) for n in range(a.subscribers)]
    out = Path(a.out)
    if not out.exists(): out.write_text('time,last_ledger,ledgers,transactions,reconnects,compared,differing\n')
    while True:
        time.sleep(a.interval)
        # Ledgers every subscriber has finished (the newest may still be arriving) must match.
        common = set.intersection(*(set(s.digests) for s in subscribers)) - {max(s.digests, default=0) for s in subscribers}
        differing = [seq for seq in sorted(common) if len({s.digests[seq] for s in subscribers}) > 1]
        for seq in differing[:5]: alert(f'subscribers differ on ledger {seq}')
        for s in subscribers:
            for seq in common: s.digests.pop(seq, None)
        last = max((s.last or 0) for s in subscribers)
        with open(out, 'a') as f:
            f.write(f'{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},{last},{sum(s.ledgers for s in subscribers)},'
                    f'{sum(s.transactions for s in subscribers)},{sum(s.reconnects for s in subscribers)},{len(common)},{len(differing)}\n')


if __name__ == '__main__':
    main()
