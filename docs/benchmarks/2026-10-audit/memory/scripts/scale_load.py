#!/usr/bin/env python3
"""Many public clients at once against an `xrpla serve`, each from its own loopback address
(127.0.0.2, 127.0.0.3, ...), so per-address limits treat them as different hosts.

  scale_load.py HOST PORT CORPUS.json SECONDS CLIENTS MIX [--slow-readers N] [--processes P]

MIX:
  realistic  account_info, tx, ledger headers, account_lines/objects at old ledgers, some
             account_tx and ledgers with transactions (what explorers and wallets ask)
  heavy      account_tx pages followed by marker (limit 400, over all history), ledgers with
             expanded transactions, ledger_data pages (what an indexer backfilling asks)
--slow-readers N: N of the clients read their replies at 64 KB a second.

Prints one JSON line: requests, errors by kind, latency percentiles, bytes received.
"""
import argparse, http.client, json, multiprocessing as mp, random, socket, time

MIXES = {
    'realistic': {'account_info': 25, 'tx': 20, 'ledger': 15, 'account_lines': 10, 'account_objects': 8,
                  'account_tx': 10, 'ledger_txs': 7, 'book_offers': 5},
    'heavy': {'account_tx_pages': 45, 'ledger_txs': 35, 'ledger_data': 20},
    'only_account_tx_pages': {'account_tx_pages': 1},
    'only_ledger_txs': {'ledger_txs': 1},
    'only_ledger_data': {'ledger_data': 1},
}
XRP_USD = {'taker_gets': {'currency': 'XRP'},
           'taker_pays': {'currency': 'USD', 'issuer': 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'}}


def body(rng, corpus, kind, marker=None):
    ledger = rng.choice(corpus['ledgers'])['seq']
    account = rng.choice(corpus['accounts'])
    if kind == 'account_info': return 'account_info', {'account': account, 'ledger_index': ledger}
    if kind == 'tx': return 'tx', {'transaction': rng.choice(corpus['hashes'])}
    if kind == 'ledger': return 'ledger', {'ledger_index': ledger}
    if kind == 'account_lines': return 'account_lines', {'account': account, 'ledger_index': ledger}
    if kind == 'account_objects': return 'account_objects', {'account': account, 'ledger_index': ledger}
    if kind == 'account_tx': return 'account_tx', {'account': account, 'limit': 20}
    if kind == 'ledger_txs': return 'ledger', {'ledger_index': ledger, 'transactions': True, 'expand': True}
    if kind == 'book_offers': return 'book_offers', dict(XRP_USD, ledger_index=ledger, limit=50)
    if kind == 'ledger_data': return 'ledger_data', {'ledger_index': ledger, 'limit': 2048}
    if kind == 'account_tx_pages':
        params = {'account': account, 'limit': 400, 'forward': True}
        if marker: params['marker'] = marker
        return 'account_tx', params
    raise ValueError(kind)


def client(index, host, port, corpus, mix, until, slow, queue):
    rng = random.Random(index)
    kinds = [k for k, w in MIXES[mix].items() for _ in range(w)]
    source = ('127.0.%d.%d' % ((index + 2) // 254, (index + 2) % 254 + 1), 0)
    stats = {'requests': 0, 'errors': {}, 'ms': [], 'bytes': 0}
    conn, pages, marker = None, 0, None
    while time.time() < until:
        kind = 'account_tx_pages' if marker else rng.choice(kinds)
        method, params = body(rng, corpus, kind, marker)
        payload = json.dumps({'method': method, 'params': [params]})
        started = time.monotonic()
        try:
            if conn is None:
                conn = http.client.HTTPConnection(host, port, timeout=120, source_address=source)
            conn.request('POST', '/', payload, {'Content-Type': 'application/json'})
            response = conn.getresponse()
            if slow:
                chunks = []
                while True:
                    chunk = response.read(65536)
                    if not chunk: break
                    chunks.append(chunk); time.sleep(1)
                data = b''.join(chunks)
            else:
                data = response.read()
            stats['bytes'] += len(data)
            stats['ms'].append((time.monotonic() - started) * 1000)
            stats['requests'] += 1
            if response.status != 200 or b'"status":"error"' in data[-400:] or b'"error"' in data[:200]:
                head = data[:300].decode(errors='replace')
                reason = head.split('"error":"', 1)[1].split('"', 1)[0] if '"error":"' in head else f'http {response.status}'
                stats['errors'][reason] = stats['errors'].get(reason, 0) + 1
                marker = None
            elif kind == 'account_tx_pages':
                result = json.loads(data)['result']
                pages += 1
                marker = result.get('marker') if pages < 25 else None
                if marker is None: pages = 0
        except (OSError, http.client.HTTPException, ValueError) as error:
            stats['errors'][type(error).__name__] = stats['errors'].get(type(error).__name__, 0) + 1
            conn, marker = None, None
            time.sleep(0.5)
    queue.put(stats)


def group(clients, host, port, corpus, mix, until, slow_set, queue):
    import threading
    threads = [threading.Thread(target=client, args=(i, host, port, corpus, mix, until, i in slow_set, queue)) for i in clients]
    for t in threads: t.start()
    for t in threads: t.join()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('host'); p.add_argument('port', type=int); p.add_argument('corpus')
    p.add_argument('seconds', type=int); p.add_argument('clients', type=int); p.add_argument('mix', choices=MIXES)
    p.add_argument('--slow-readers', type=int, default=0); p.add_argument('--processes', type=int, default=6)
    a = p.parse_args()
    corpus = json.load(open(a.corpus))
    until = time.time() + a.seconds
    queue = mp.Queue()
    slow = set(range(a.slow_readers))
    procs = [mp.Process(target=group, args=(list(range(i, a.clients, a.processes)), a.host, a.port, corpus, a.mix, until, slow, queue))
             for i in range(a.processes)]
    for proc in procs: proc.start()
    total = {'requests': 0, 'errors': {}, 'ms': [], 'bytes': 0}
    for _ in range(a.clients):
        s = queue.get()
        total['requests'] += s['requests']; total['bytes'] += s['bytes']; total['ms'] += s['ms']
        for k, v in s['errors'].items(): total['errors'][k] = total['errors'].get(k, 0) + v
    for proc in procs: proc.join()
    ms = sorted(total['ms'])
    q = lambda f: round(ms[min(len(ms) - 1, int(f * len(ms)))]) if ms else None
    print(json.dumps({'mix': a.mix, 'clients': a.clients, 'slow_readers': a.slow_readers, 'seconds': a.seconds,
                      'requests': total['requests'], 'per_second': round(total['requests'] / a.seconds, 1),
                      'errors': total['errors'], 'ms_p50': q(.5), 'ms_p95': q(.95), 'ms_p99': q(.99), 'ms_max': q(1.0),
                      'received_mb': round(total['bytes'] / 1e6)}))


if __name__ == '__main__':
    main()
