#!/usr/bin/env python3
"""NFT query speed of `xrpla serve` (methods xrpld lacks, so XRPLA alone): NFTs, their issuers,
owners and offers are collected from every 25th ledger of the range's transactions, then
nft_info (at several ledgers), nft_history (pages of 20 and 100, both directions),
nfts_by_issuer (pages of 50 and 100, with and without a taxon, continued through markers),
account_nfts (pages of 20 and 400) and nft_buy_offers / nft_sell_offers are timed one request
at a time. Prints median, p95, max and the slowest requests per method.

  nft-bench.py URL FIRST LAST
"""
import json, random, statistics, sys, time, urllib.request
from collections import defaultdict

URL, FIRST, LAST = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
def call(method, params):
    req = urllib.request.Request(URL, json.dumps({'method': method, 'params': [params]}).encode(), {'Content-Type': 'application/json'})
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=120) as r: result = json.load(r)['result']
    return result, time.perf_counter() - started

times, slowest, errors = defaultdict(list), defaultdict(list), defaultdict(int)
def timed(method, **params):
    result, seconds = call(method, params)
    times[method].append(seconds)
    slowest[method].append((seconds, params)); slowest[method].sort(key=lambda x: -x[0]); del slowest[method][3:]
    if 'error' in result: errors[method] += 1
    return result

rng = random.Random(3)
nfts, issuers, owners, offered = set(), set(), set(), set()
for seq in range(FIRST, LAST + 1, 25):
    ledger, _ = call('ledger', {'ledger_index': seq, 'transactions': True, 'expand': True, 'api_version': 2})
    for tx in ledger.get('ledger', {}).get('transactions', []):
        t, meta = tx.get('tx_json', {}), tx.get('meta', {})
        kind = t.get('TransactionType', '')
        if not kind.startswith('NFToken'): continue
        for n in [meta.get('nftoken_id')] + meta.get('nftoken_ids', []) + [t.get('NFTokenID')]:
            if n: nfts.add(n)
        if kind == 'NFTokenMint': issuers.add(t.get('Issuer') or t['Account'])
        if kind == 'NFTokenCreateOffer' and t.get('NFTokenID'): offered.add(t['NFTokenID'])
        owners.add(t['Account'])
nfts, issuers, owners, offered = sorted(nfts), sorted(issuers), sorted(owners), sorted(offered)
print(f'sampled {len(nfts)} NFTs, {len(issuers)} issuers, {len(owners)} accounts, {len(offered)} NFTs with offers', flush=True)

for n in rng.sample(nfts, min(150, len(nfts))):
    for at in (FIRST + (LAST - FIRST) // 2, LAST): timed('nft_info', nft_id=n, ledger_index=at)
    page = timed('nft_history', nft_id=n, limit=20)
    if 'marker' in page: timed('nft_history', nft_id=n, limit=20, marker=page['marker'])
    timed('nft_history', nft_id=n, limit=100, forward=True)
for issuer in issuers[:60]:
    page = timed('nfts_by_issuer', issuer=issuer, ledger_index=LAST, limit=50)
    for _ in range(3):
        if 'marker' not in page: break
        page = timed('nfts_by_issuer', issuer=issuer, ledger_index=LAST, limit=50, marker=page['marker'])
    taxa = sorted({n.get('nft_taxon') for n in page.get('nfts', []) if 'nft_taxon' in n})
    if taxa: timed('nfts_by_issuer', issuer=issuer, ledger_index=LAST, limit=100, nft_taxon=taxa[0])
for owner in rng.sample(owners, min(150, len(owners))):
    page = timed('account_nfts', account=owner, ledger_index=LAST, limit=20)
    if 'marker' in page: timed('account_nfts', account=owner, ledger_index=LAST, limit=20, marker=page['marker'])
    timed('account_nfts', account=owner, ledger_index=LAST, limit=400)
for n in rng.sample(offered, min(150, len(offered))):
    for method in ('nft_sell_offers', 'nft_buy_offers'): timed(method, nft_id=n, ledger_index=LAST)

print('%-16s %6s %10s %10s %10s %7s' % ('method', 'n', 'median ms', 'p95 ms', 'max ms', 'errors'))
for method in sorted(times):
    v = sorted(times[method])
    print('%-16s %6d %10.2f %10.2f %10.2f %7d' % (method, len(v), 1000 * statistics.median(v), 1000 * v[min(len(v) - 1, int(len(v) * 0.95))], 1000 * v[-1], errors[method]))
    for seconds, params in slowest[method][:2]: print('    slow: %.1f ms %s' % (1000 * seconds, json.dumps(params)[:160]))
print('DONE')
