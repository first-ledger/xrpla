#!/usr/bin/env python3
"""Generate MPT, price oracle and vault activity on an isolated xrpld and export it as an archive.

Runs only a fresh, owned standalone node on loopback ephemeral ports; it never submits to the
user's mainnet node. Requires Linux, xrpld 3.3.0 and XRPLA. The signed archive written to
OUT/archive is tests/fixtures/standalone-tokens, with what xrpld itself answered:

- mpt-expectations.json: every MPT issuance's holders at every ledger of the archive, from
  xrpld's own state (the answers Clio's mpt_holders must give, as sets);
- xrpld-answers.json: xrpld's answers to get_aggregate_price, vault_info and ledger_entry
  requests (oracle, vault, mptoken, mpt_issuance selectors), which XRPLA must repeat.

The scenario: an issuance with a holder before the archive's first ledger (so the baseline
snapshot holds MPTokens), holders joining, trading and leaving two issuances (one with a
transfer fee), a holder locked and unlocked, a clawback, two price oracles updated over
several ledgers, and a vault with a deposit.
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

LOCK, REQUIRE_AUTH, TRANSFER, CLAWBACK = 2, 4, 32, 64


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
MPTokensV1
PriceOracle
fixPriceOracleOrder
SingleAssetVault
[debug_logfile]
{root}/xrpld.log
''')
    report = {'network': 'isolated-standalone', 'versions': {}, 'transactions': []}
    report['versions']['xrpld'] = subprocess.check_output([args.node_binary, '--version'], text=True).strip()
    report['versions']['xrpla'] = subprocess.check_output([args.binary, '--version'], text=True).strip()

    def call(method, **params):
        body = json.dumps({'method': method, 'params': [params]}).encode()
        with urllib.request.urlopen(urllib.request.Request(f'http://127.0.0.1:{rpc_port}/', body, {'Content-Type': 'application/json'}), timeout=30) as response:
            return json.load(response)['result']

    def rpc(method, **params):
        result = call(method, **params)
        if 'error' in result: raise RuntimeError(method + ': ' + json.dumps(result)[:500])
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
        names = ('masterpassphrase', 'xrpla-token-issuer', 'xrpla-token-a', 'xrpla-token-b', 'xrpla-token-c', 'xrpla-token-oracle', 'xrpla-token-vault')
        master, issuer, a, b, c, oracle, vault_owner = (rpc('wallet_propose', passphrase=name) for name in names)

        def transact(wallet, kind, **fields):
            """Submit, close the ledger, and return the validated result."""
            address = wallet['account_id']
            sequence = rpc('account_info', account=address, ledger_index='current')['account_data']['Sequence']
            tx = {'TransactionType': kind, 'Account': address, 'Sequence': sequence, **fields}
            # The node fills in the fee: some of these transactions cost an owner reserve.
            response = rpc('submit', secret=wallet['master_seed'], tx_json=tx, fee_mult_max=1000000)
            assert response['engine_result'] == 'tesSUCCESS', (kind, fields, response['engine_result'], response.get('engine_result_message'))
            rpc('ledger_accept')
            result = rpc('tx', transaction=response['tx_json']['hash'])
            assert result['validated'] and result['meta']['TransactionResult'] == 'tesSUCCESS', result
            report['transactions'].append({'type': kind, 'hash': result['hash'], 'ledger': result['ledger_index']})
            return result

        rpc('ledger_accept')
        for wallet in (issuer, a, b, c, oracle, vault_owner): transact(master, 'Payment', Destination=wallet['account_id'], Amount='1000000000')
        # Before the archive: an issuance with one authorized holder, in the baseline snapshot.
        created = transact(issuer, 'MPTokenIssuanceCreate', Flags=LOCK | REQUIRE_AUTH | TRANSFER | CLAWBACK, AssetScale=2, MaximumAmount='1000000',
                           MPTokenMetadata='5852504C4120746F6B656E73')
        first_issuance = created['meta']['mpt_issuance_id']
        transact(a, 'MPTokenAuthorize', MPTokenIssuanceID=first_issuance)
        transact(issuer, 'MPTokenAuthorize', MPTokenIssuanceID=first_issuance, Holder=a['account_id'])
        rpc('ledger_accept')
        first = rpc('ledger', ledger_index='validated')['ledger_index'] + 1

        def amount(issuance, value): return {'mpt_issuance_id': issuance, 'value': str(value)}
        transact(b, 'MPTokenAuthorize', MPTokenIssuanceID=first_issuance)
        transact(issuer, 'MPTokenAuthorize', MPTokenIssuanceID=first_issuance, Holder=b['account_id'])
        transact(issuer, 'Payment', Destination=a['account_id'], Amount=amount(first_issuance, 1000))
        transact(a, 'Payment', Destination=b['account_id'], Amount=amount(first_issuance, 250))
        transact(issuer, 'MPTokenIssuanceSet', MPTokenIssuanceID=first_issuance, Holder=a['account_id'], Flags=1)
        transact(issuer, 'MPTokenIssuanceSet', MPTokenIssuanceID=first_issuance, Holder=a['account_id'], Flags=2)
        transact(issuer, 'Clawback', Amount=amount(first_issuance, 50), Holder=b['account_id'])
        second = transact(issuer, 'MPTokenIssuanceCreate', Flags=TRANSFER, TransferFee=100, AssetScale=0)['meta']['mpt_issuance_id']
        for holder in (c, a, b): transact(holder, 'MPTokenAuthorize', MPTokenIssuanceID=second)
        transact(issuer, 'Payment', Destination=c['account_id'], Amount=amount(second, 500))
        transact(c, 'Payment', Destination=issuer['account_id'], Amount=amount(second, 500))
        left_at = transact(c, 'MPTokenAuthorize', MPTokenIssuanceID=second, Flags=1)['ledger_index']

        # Two price oracles, the first updated twice; LastUpdateTime is Unix time, near the ledger's.
        def price(value, scale): return [{'PriceData': {'BaseAsset': 'XRP', 'QuoteAsset': 'USD', 'AssetPrice': format(value, 'x'), 'Scale': scale}}]
        oracle_ledgers = []
        for document, value in ((1, 74000), (2, 74500), (1, 74250), (1, 73900)):
            result = transact(oracle, 'OracleSet', OracleDocumentID=document, Provider='70726F7669646572', AssetClass='63757272656E6379',
                              LastUpdateTime=int(time.time()) + len(oracle_ledgers), PriceDataSeries=price(value, 5))
            oracle_ledgers.append(result['ledger_index'])
            time.sleep(1.1)
        # A vault holding XRP, and a deposit (its shares are an MPT issuance of the vault's pseudo-account).
        vault_created = transact(vault_owner, 'VaultCreate', Asset={'currency': 'XRP'})
        vault_id = next(n['CreatedNode']['LedgerIndex'] for n in vault_created['meta']['AffectedNodes']
                        if n.get('CreatedNode', {}).get('LedgerEntryType') == 'Vault')
        vault_seq = vault_created['Sequence'] if 'Sequence' in vault_created else vault_created['tx_json']['Sequence']
        transact(a, 'VaultDeposit', VaultID=vault_id, Amount='5000000')
        rpc('ledger_accept')
        last = rpc('ledger', ledger_index='validated')['ledger_index']
        report['range'] = {'first': first, 'last': last}

        # What xrpld holds: every issuance's holders at every ledger.
        expectations = []
        for seq in range(first - 1, last + 1):
            holders, marker = {}, None
            while True:
                page = rpc('ledger_data', ledger_index=seq, type='mptoken', limit=256, **({'marker': marker} if marker else {}))
                for item in page['state']: holders.setdefault(item['MPTokenIssuanceID'], []).append(item['Account'])
                marker = page.get('marker')
                if not marker: break
            for issuance in dict.fromkeys((first_issuance, second, *holders.keys())):
                expectations.append({'mpt_issuance_id': issuance, 'ledger_index': seq, 'holders': sorted(set(holders.get(issuance, [])))})
        # xrpld's answers to the requests XRPLA must answer alike.
        answers = []
        def record(method, **params):
            result = call(method, **params)
            result.pop('status', None); result.pop('warnings', None); result.pop('request', None)
            answers.append({'method': method, 'params': params, 'result': result})
        oracles = [{'account': oracle['account_id'], 'oracle_document_id': 1}, {'account': oracle['account_id'], 'oracle_document_id': 2}]
        for seq in sorted({*oracle_ledgers, last}):
            record('get_aggregate_price', ledger_index=seq, base_asset='XRP', quote_asset='USD', oracles=oracles)
            record('get_aggregate_price', ledger_index=seq, base_asset='XRP', quote_asset='USD', oracles=oracles, trim=20)
            record('get_aggregate_price', ledger_index=seq, base_asset='XRP', quote_asset='USD', oracles=oracles, time_threshold=1)
        record('get_aggregate_price', ledger_index=last, base_asset='XRP', quote_asset='EUR', oracles=oracles)
        record('get_aggregate_price', ledger_index=last, base_asset='XRP', quote_asset='USD', oracles=oracles, trim=0)
        record('vault_info', ledger_index=last, vault_id=vault_id)
        record('vault_info', ledger_index=last, owner=vault_owner['account_id'], seq=vault_seq)
        record('vault_info', ledger_index=last, vault_id='11' * 32)
        record('vault_info', ledger_index=last, vault_id=vault_id, seq=vault_seq)
        record('ledger_entry', ledger_index=last, vault=vault_id)
        record('ledger_entry', ledger_index=last, vault={'owner': vault_owner['account_id'], 'seq': vault_seq})
        record('ledger_entry', ledger_index=last, oracle={'account': oracle['account_id'], 'oracle_document_id': 1})
        record('ledger_entry', ledger_index=oracle_ledgers[0], oracle={'account': oracle['account_id'], 'oracle_document_id': 1})
        for issuance in (first_issuance, second):
            record('ledger_entry', ledger_index=last, mpt_issuance=issuance)
            for holder in (a, b, c):
                record('ledger_entry', ledger_index=last, mptoken={'mpt_issuance_id': issuance, 'account': holder['account_id']})
                record('ledger_entry', ledger_index=left_at - 1, mptoken={'mpt_issuance_id': issuance, 'account': holder['account_id']})
        record('ledger_entry', ledger_index=last, mpt_issuance='00' * 32)
        record('ledger_entry', ledger_index=last, offer=vault_id)
        (root / 'lifecycle.json').write_text(json.dumps(report, indent=2))
        seed = root / 'publisher.seed'; cli('keygen', '--out', seed)
        archive = root / 'archive'
        report['publication'] = cli('publish', '--source', f'http://127.0.0.1:{rpc_port}/', '--grpc-source', f'grpc://127.0.0.1:{grpc_port}',
                                    '--out', archive, '--signing-key', seed, '--delay-ms', '0', '--start', first, '--until', last, '--once')
        (archive / 'mpt-expectations.json').write_text(json.dumps(expectations, indent=1) + '\n')
        (archive / 'xrpld-answers.json').write_text(json.dumps(answers, indent=1) + '\n')
        (archive / 'lifecycle.json').write_text(json.dumps(report, indent=1) + '\n')
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
