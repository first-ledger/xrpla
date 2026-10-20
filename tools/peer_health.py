#!/usr/bin/env python3
"""How a node that follows the XRPL peer network treats its peers, from its log: whether it is a
good citizen (it asks for little that goes unanswered, duplicates few requests, and peers do not
end its sessions early), as one line of JSON per run.

  peer_health.py LOG [--hours 1] [--out FILE]

LOG is `xrpla run`'s or the follower's output (JSON lines among others): `peer_connected` and
`peer_closed` events, and every 5 minutes `peer_relay` with the outbound sessions' totals
(requests, hedged copies, retries, unanswered, connects, dial failures, closings by reason,
each peer's answer time) and, on a node serving the peer network, `peer_serving` with the
inbound side (sessions accepted and why they ended, requests answered and unanswered, peers
dropped for their charges, the ledgers served). Totals restart with the process; deltas are
summed across restarts.

Flags (`concerns`): sessions peers ended within a minute (a peer dropping this node, as xrpld
does when a peer costs it too much), closings for load, more than 5% of requests unanswered,
more than 25% sent twice.
"""
import argparse
import json
from pathlib import Path
import sys
import time


def events(path):
    with open(path, 'rb') as f:
        for raw in f:
            if not raw.startswith(b'{'): continue
            try: event = json.loads(raw)
            except ValueError: continue
            if not isinstance(event, dict) or 'event' not in event: continue
            yield event


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('log', type=Path)
    p.add_argument('--hours', type=float, default=1.0, help='the last N hours of the log, by its peer_relay samples (every 5 minutes)')
    p.add_argument('--out', type=Path, help='append the result to this file as well')
    a = p.parse_args()
    samples, closed, connected, serving = [], [], 0, []
    for event in events(a.log):
        kind = event['event']
        if kind == 'peer_relay' and 'outbound' in event: samples.append(event['outbound'])
        elif kind == 'peer_serving' and 'status' in event: serving.append(event['status'])
        elif kind == 'peer_closed' and event.get('direction') == 'out': closed.append(event)
        elif kind == 'peer_connected': connected += 1
    window = max(1, int(a.hours * 12))  # samples every 5 minutes
    recent = samples[-(window + 1):]
    # Sums of the counters' growth between samples; a drop is a restart (the new value is growth).
    totals = {k: 0 for k in ('requests', 'hedged', 'retries', 'unanswered', 'connects', 'short_sessions')}
    reasons = {}
    for before, after in zip(recent, recent[1:]):
        for k in totals:
            b, x = before.get(k) or 0, after.get(k) or 0
            totals[k] += x - b if x >= b else x
        for reason, count in (after.get('closed') or {}).items():
            b = (before.get('closed') or {}).get(reason, 0)
            reasons[reason] = reasons.get(reason, 0) + (count - b if count >= b else count)
    # Sessions ended in the window (the newest `peer_closed` events, as many as there were connects).
    ended = closed[-max(totals['connects'], 0):] if totals['connects'] else []
    by_peer_short = [e for e in ended if not e.get('reason', '').startswith('Closed by this node') and e.get('seconds', 0) < 60]
    load_reasons = {r: n for r, n in reasons.items() if any(w in r.lower() for w in ('charge', 'resource', 'busy', 'load', 'too many'))}
    latest = recent[-1] if recent else {}
    answer_ms = sorted(p.get('answer_ms') or 0 for p in latest.get('peers', []) if p.get('answer_ms'))
    requests = totals['requests'] or 1
    result = {
        'time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'log': str(a.log), 'hours': a.hours, 'samples': len(recent),
        'requests': totals['requests'], 'hedged': totals['hedged'], 'retries': totals['retries'], 'unanswered': totals['unanswered'],
        'unanswered_rate': round(totals['unanswered'] / requests, 4), 'hedged_rate': round(totals['hedged'] / requests, 4),
        'sessions_opened': totals['connects'], 'short_sessions': totals['short_sessions'], 'closed_by_reason': reasons,
        'ended_by_peers_within_a_minute': len(by_peer_short), 'open_peers': len(latest.get('peers', [])),
        'answer_ms': {'median': answer_ms[len(answer_ms) // 2] if answer_ms else None, 'max': answer_ms[-1] if answer_ms else None},
        'dial_failures': latest.get('dial_failures', {}),
    }
    # The serving side, when the node serves: the same deltas over the window.
    served = serving[-(window + 1):]
    if served:
        sums = {k: 0 for k in ('accepted', 'short_sessions', 'requests', 'answered', 'unanswered', 'peers_dropped', 'busy', 'bytes_sent')}
        inbound_closed = {}
        for before, after in zip(served, served[1:]):
            for k in sums:
                b = before.get(k, before.get('serving', {}).get(k)) or 0
                x = after.get(k, after.get('serving', {}).get(k)) or 0
                sums[k] += x - b if x >= b else x
            for reason, count in (after.get('closed') or {}).items():
                b = (before.get('closed') or {}).get(reason, 0)
                inbound_closed[reason] = inbound_closed.get(reason, 0) + (count - b if count >= b else count)
        latest_served = served[-1]
        result['serving'] = {**sums, 'closed_by_reason': inbound_closed, 'inbound_now': len(latest_served.get('inbound', [])),
                             'ledgers': latest_served.get('serving', {}).get('ledgers'),
                             'answered_rate': round(sums['answered'] / sums['requests'], 4) if sums['requests'] else None}
    concerns = []
    if by_peer_short: concerns.append(f'{len(by_peer_short)} sessions ended by peers within a minute')
    if load_reasons: concerns.append(f'closed for load: {load_reasons}')
    if result['unanswered_rate'] > 0.05: concerns.append(f"{result['unanswered_rate']:.1%} of requests unanswered")
    if result['hedged_rate'] > 0.25: concerns.append(f"{result['hedged_rate']:.1%} of requests sent twice")
    if not recent: concerns.append('no peer_relay samples in the log')
    if result.get('serving', {}).get('peers_dropped'): concerns.append(f"serving dropped {result['serving']['peers_dropped']} peers for their charges")
    result['concerns'] = concerns
    line = json.dumps(result)
    print(line)
    if a.out:
        with open(a.out, 'a') as f: f.write(line + '\n')
    sys.exit(1 if concerns else 0)


if __name__ == '__main__':
    main()
