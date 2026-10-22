#!/usr/bin/env python3
"""Request material spread over all of history, for scale_load.py.

  corpus.py URL OUT.json [LEDGERS]

Reads LEDGERS ledgers (default 400) at random across the server's range, with their
transactions, and keeps the ledgers, transaction hashes and accounts they name.
"""
import json, random, sys, urllib.request


def call(url, method, params):
    body = json.dumps({'method': method, 'params': [params]}).encode()
    with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=120) as r:
        return json.load(r)['result']


def main(url, out, count=400):
    first, last = (lambda r: (r['ledger_index_min'], r['ledger_index_max']))(call(url, 'ledger_range', {}))
    rng = random.Random(7)
    ledgers, hashes, accounts = [], [], set()
    for seq in sorted(rng.randint(first + 1, last - 300) for _ in range(count)):
        result = call(url, 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True})
        txs = result['ledger'].get('transactions', [])
        ledgers.append({'seq': seq, 'transactions': len(txs)})
        for tx in txs[:20]:
            hashes.append(tx['hash'])
            for field in ('Account', 'Destination'):
                if isinstance(tx.get(field), str):
                    accounts.add(tx[field])
    corpus = {'first': first, 'last': last, 'ledgers': ledgers, 'hashes': hashes, 'accounts': sorted(accounts)}
    json.dump(corpus, open(out, 'w'))
    print(json.dumps({'ledgers': len(ledgers), 'hashes': len(hashes), 'accounts': len(accounts), 'range': [first, last]}))


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 400)
