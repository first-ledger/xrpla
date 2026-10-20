#!/usr/bin/env python3
"""Generate real NFToken transactions on an isolated xrpld process and export them as an archive.

Runs only a fresh, owned standalone node on loopback ephemeral ports; it never
submits to the user's mainnet node. Requires Linux, xrpld 3.3.0 and XRPLA 0.4.0.
The signed archive written to OUT/archive is tests/fixtures/standalone-nfts.

Tokens held since the baseline snapshot, mints that split pages, transfers
through sell and buy offers, cancelled offers, a URI change, burns by owner and
by issuer, and a transfer followed by a burn inside one ledger.
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

TRANSFERABLE, BURNABLE, MUTABLE, SELL = 8, 1, 16, 1


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--node-binary', default='/usr/bin/xrpld')
    parser.add_argument('--binary', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    if root.exists() and any(root.iterdir()): parser.error('--out must be empty')
    root.mkdir(parents=True, exist_ok=True)
    rpc_port, grpc_port = port(), port()
    while grpc_port == rpc_port: grpc_port = port()
    cfg = root / 'xrpld.cfg'
    cfg.write_text(f'''[server]
rpc
[rpc]
port = {rpc_port}
ip = 127.0.0.1
admin = 127.0.0.1
protocol = http
[port_grpc]
port = {grpc_port}
ip = 127.0.0.1
[database_path]
{root}/node
[node_db]
type = NuDB
path = {root}/node/nudb
[ledger_history]
full
[signing_support]
true
[features]
NonFungibleTokensV1_1
fixNFTokenRemint
DynamicNFT
[debug_logfile]
{root}/xrpld.log
''')
    report = {'network': 'isolated-standalone', 'versions': {}, 'transactions': [], 'tokens': {}}
    report['versions']['xrpld'] = subprocess.check_output([args.node_binary, '--version'], text=True).strip()
    report['versions']['xrpla'] = subprocess.check_output([args.binary, '--version'], text=True).strip()

    def rpc(method, **params):
        body = json.dumps({'method': method, 'params': [params]}).encode()
        with urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{rpc_port}/', body, {'Content-Type': 'application/json'}), timeout=30) as response:
            result = json.load(response)['result']
        if 'error' in result: raise RuntimeError(method + ': ' + result['error'])
        return result

    def cli(*command):
        result = subprocess.run([args.binary, *map(str, command)], capture_output=True, text=True, timeout=600)
        if result.returncode: raise RuntimeError(result.stderr)
        return json.loads(result.stdout)

    with (root / 'node-console.log').open('w') as log:
        node = subprocess.Popen([args.node_binary, '--conf', str(cfg), '--standalone', '--start'], stdout=log, stderr=log)
    try:
        deadline = time.monotonic() + 90
        while True:
            try: rpc('server_info'); break
            except Exception:
                if node.poll() is not None or time.monotonic() > deadline: raise
                time.sleep(.2)
        master, issuer, a, b = (rpc('wallet_propose', passphrase=name) for name in ('masterpassphrase', 'xrpla-nft-issuer', 'xrpla-nft-a', 'xrpla-nft-b'))
        sequences = {}
        pending = []

        def submit(wallet, kind, **fields):
            """Apply to the open ledger; the caller closes it with close()."""
            address = wallet['account_id']
            if address not in sequences: sequences[address] = rpc('account_info', account=address, ledger_index='current')['account_data']['Sequence']
            tx = {'TransactionType': kind, 'Account': address, 'Sequence': sequences[address], 'Fee': '12', **fields}
            response = rpc('submit', secret=wallet['master_seed'], tx_json=tx)
            assert response['engine_result'] == 'tesSUCCESS', (kind, fields, response['engine_result'])
            sequences[address] += 1
            pending.append((kind, response['tx_json']['hash']))
            return response['tx_json']['hash']

        def close():
            rpc('ledger_accept')
            results = []
            for kind, tx_hash in pending:
                result = rpc('tx', transaction=tx_hash)
                assert result['validated'] and result['meta']['TransactionResult'] == 'tesSUCCESS', result
                report['transactions'].append({'type': kind, 'hash': tx_hash, 'ledger': result['ledger_index']})
                results.append(result)
            pending.clear()
            return results

        def one(wallet, kind, **fields):
            submit(wallet, kind, **fields)
            return close()[0]

        def mint(taxon, uri=None, flags=TRANSFERABLE | BURNABLE):
            fields = {'NFTokenTaxon': taxon, 'Flags': flags}
            if uri: fields['URI'] = uri.encode().hex().upper()
            submit(issuer, 'NFTokenMint', **fields)

        def minted(results): return [r['meta']['nftoken_id'] for r in results]

        rpc('ledger_accept')
        for wallet in (issuer, a, b): submit(master, 'Payment', Destination=wallet['account_id'], Amount='10000000000')
        close()
        # Held since the baseline snapshot: one is transferred later, the others never change.
        for taxon, uri in ((1, 'ipfs://kept-since-snapshot'), (2, None), (3, 'ipfs://moved-after-snapshot')):
            mint(taxon, uri, TRANSFERABLE | BURNABLE | MUTABLE)
        kept, plain, moved_later = minted(close())
        snapshot = rpc('ledger', ledger_index='validated')['ledger_index']
        first = snapshot + 1
        # Forty mints over two ledgers split the issuer's pages.
        for taxon in range(100, 120): mint(taxon, f'ipfs://bulk-{taxon}')
        bulk = minted(close())
        for taxon in range(120, 140): mint(taxon)
        results = close(); bulk += minted(results); plain_bulk_at = results[0]['ledger_index']
        mint(10, 'ipfs://traded', TRANSFERABLE | BURNABLE | MUTABLE)
        mint(11, 'ipfs://same-ledger-burn')
        results = close(); traded, same_ledger = minted(results); minted_at = results[0]['ledger_index']
        # traded: sold to a, a's own offer cancelled, then bought by b, then its URI changed.
        offer = one(issuer, 'NFTokenCreateOffer', NFTokenID=traded, Amount='1000000', Flags=SELL)['meta']['offer_id']
        sold_at = one(a, 'NFTokenAcceptOffer', NFTokenSellOffer=offer)['ledger_index']
        cancelled = one(a, 'NFTokenCreateOffer', NFTokenID=traded, Amount='5000000', Flags=SELL)['meta']['offer_id']
        one(a, 'NFTokenCancelOffer', NFTokenOffers=[cancelled])
        buy = one(b, 'NFTokenCreateOffer', NFTokenID=traded, Amount='2000000', Owner=a['account_id'])['meta']['offer_id']
        bought_at = one(a, 'NFTokenAcceptOffer', NFTokenBuyOffer=buy)['ledger_index']
        modified_at = one(issuer, 'NFTokenModify', NFTokenID=traded, Owner=b['account_id'], URI='ipfs://traded-v2'.encode().hex().upper())['ledger_index']
        # moved_later: held since the snapshot, then given to b.
        gift = one(issuer, 'NFTokenCreateOffer', NFTokenID=moved_later, Amount='0', Flags=SELL, Destination=b['account_id'])['meta']['offer_id']
        gifted_at = one(b, 'NFTokenAcceptOffer', NFTokenSellOffer=gift)['ledger_index']
        # same_ledger: bought by a and burned by a in one ledger.
        sale = one(issuer, 'NFTokenCreateOffer', NFTokenID=same_ledger, Amount='0', Flags=SELL, Destination=a['account_id'])['meta']['offer_id']
        submit(a, 'NFTokenAcceptOffer', NFTokenSellOffer=sale)
        submit(a, 'NFTokenBurn', NFTokenID=same_ledger)
        same_ledger_at = close()[0]['ledger_index']
        # An issuer burns a token b holds, then its own bulk tokens (pages merge).
        handed = one(issuer, 'NFTokenCreateOffer', NFTokenID=bulk[0], Amount='0', Flags=SELL, Destination=b['account_id'])['meta']['offer_id']
        one(b, 'NFTokenAcceptOffer', NFTokenSellOffer=handed)
        issuer_burned_at = one(issuer, 'NFTokenBurn', NFTokenID=bulk[0], Owner=b['account_id'])['ledger_index']
        for token in bulk[1:25]: submit(issuer, 'NFTokenBurn', NFTokenID=token)
        bulk_burned_at = close()[0]['ledger_index']
        rpc('ledger_accept')  # a ledger without transactions
        last = rpc('ledger', ledger_index='validated')['ledger_index']
        report['range'] = {'first': first, 'last': last}
        report['tokens'] = {'kept': kept, 'plain': plain, 'moved_later': moved_later, 'traded': traded,
                            'same_ledger_burn': same_ledger, 'issuer_burned': bulk[0], 'bulk': bulk}
        (root / 'lifecycle.json').write_text(json.dumps(report, indent=2))
        # nft_info answers implied by the scenario (Clio semantics: each transaction's result counts,
        # ledger_index is where the state began), checked by tests/piece_query_test.py.
        hexed = lambda text: text.encode().hex().upper()
        owner = {'issuer': issuer['account_id'], 'a': a['account_id'], 'b': b['account_id']}
        def expect(token, at, who, uri, burned, since):
            return {'nft_id': token, 'ledger_index': at, 'owner': owner[who], 'uri': hexed(uri), 'is_burned': burned, 'since': since}
        expectations = [
            expect(kept, last, 'issuer', 'ipfs://kept-since-snapshot', False, snapshot),
            expect(plain, last, 'issuer', '', False, snapshot),
            expect(moved_later, gifted_at - 1, 'issuer', 'ipfs://moved-after-snapshot', False, snapshot),
            expect(moved_later, last, 'b', 'ipfs://moved-after-snapshot', False, gifted_at),
            expect(traded, minted_at, 'issuer', 'ipfs://traded', False, minted_at),
            expect(traded, sold_at, 'a', 'ipfs://traded', False, sold_at),
            expect(traded, bought_at - 1, 'a', 'ipfs://traded', False, sold_at),
            expect(traded, bought_at, 'b', 'ipfs://traded', False, bought_at),
            # A URI change alone keeps ledger_index where the owner last changed (Clio).
            expect(traded, modified_at, 'b', 'ipfs://traded-v2', False, bought_at),
            expect(traded, last, 'b', 'ipfs://traded-v2', False, bought_at),
            expect(same_ledger, same_ledger_at - 1, 'issuer', 'ipfs://same-ledger-burn', False, minted_at),
            expect(same_ledger, last, 'a', 'ipfs://same-ledger-burn', True, same_ledger_at),
            expect(bulk[0], last, 'b', 'ipfs://bulk-100', True, issuer_burned_at),
            # Burns whose page merged into the next one and was deleted: the token is in the deleted
            # page's PreviousFields alone (once read as unburned). bulk[25] is never burned.
            expect(bulk[9], bulk_burned_at, 'issuer', 'ipfs://bulk-109', True, bulk_burned_at),
            *(expect(token, last, 'issuer', f'ipfs://bulk-{100 + i}' if i < 20 else '', True, bulk_burned_at) for i, token in enumerate(bulk[1:25], 1)),
            expect(bulk[25], last, 'issuer', '', False, plain_bulk_at),
        ]
        seed = root / 'publisher.seed'; cli('keygen', '--out', seed)
        archive = root / 'archive'
        report['publication'] = cli('publish', '--source', f'http://127.0.0.1:{rpc_port}/', '--grpc-source', f'grpc://127.0.0.1:{grpc_port}',
                                    '--out', archive, '--signing-key', seed, '--delay-ms', '0', '--start', first, '--until', last, '--once')
        (archive / 'nft-expectations.json').write_text(json.dumps(expectations, indent=1) + '\n')
        report['passed'] = True
    except Exception as error:
        report['passed'] = False; report['error'] = str(error); raise
    finally:
        node.terminate()
        try: node.wait(timeout=30)
        except subprocess.TimeoutExpired: node.kill(); node.wait(timeout=10)
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': True, 'range': report['range'], 'archive': str(root / 'archive')}))


if __name__ == '__main__': main()
