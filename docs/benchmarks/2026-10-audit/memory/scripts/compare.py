#!/usr/bin/env python3
"""The same requests to two servers on the same data, answers compared exactly: a change that
must not alter any answer (a memory fix) against the build before it.

  compare.py OLD_URL NEW_URL CORPUS.json [OUT.jsonl]

Covers every ledger in the corpus (header, transactions expanded and binary, a ledger_data page
both ways), its transactions by hash, and its accounts' account_tx pages (forward and back,
markers followed), account_info, account_lines and account_objects at old ledgers.
"""
import json, random, sys, urllib.request


def call(url, method, params):
    body = json.dumps({'method': method, 'params': [params]}).encode()
    with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=300) as r:
        return json.load(r)['result']


def requests(corpus):
    rng = random.Random(11)
    for entry in corpus['ledgers']:
        seq = entry['seq']
        yield 'ledger', {'ledger_index': seq}
        yield 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True}
        yield 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True, 'binary': True}
        yield 'ledger_data', {'ledger_index': seq, 'limit': 256}
        yield 'ledger_data', {'ledger_index': seq, 'limit': 2048, 'binary': True}
    for tx in corpus['hashes'][::5]:
        yield 'tx', {'transaction': tx}
        yield 'tx', {'transaction': tx, 'binary': True}
    for account in rng.sample(corpus['accounts'], min(150, len(corpus['accounts']))):
        seq = rng.choice(corpus['ledgers'])['seq']
        yield 'account_info', {'account': account, 'ledger_index': seq}
        yield 'account_lines', {'account': account, 'ledger_index': seq}
        yield 'account_objects', {'account': account, 'ledger_index': seq}
        for forward in (True, False):
            for binary in (False, True):
                yield 'account_tx_pages', {'account': account, 'limit': 400, 'forward': forward, 'binary': binary}


def main(old, new, corpus_path, out_path=None):
    corpus = json.load(open(corpus_path))
    same = different = 0
    out = open(out_path, 'w') if out_path else None
    for method, params in requests(corpus):
        pages = 3 if method == 'account_tx_pages' else 1
        method = 'account_tx' if method == 'account_tx_pages' else method
        marker = None
        for _ in range(pages):
            p = dict(params, **({'marker': marker} if marker else {}))
            a, b = call(old, method, p), call(new, method, p)
            if a == b:
                same += 1
            else:
                different += 1
                if out:
                    out.write(json.dumps({'method': method, 'params': p, 'old': str(a)[:2000], 'new': str(b)[:2000]}) + '\n')
            marker = a.get('marker')
            if not marker:
                break
    print(json.dumps({'same': same, 'different': different}))
    return different == 0


if __name__ == '__main__':
    sys.exit(0 if main(*sys.argv[1:5]) else 1)
