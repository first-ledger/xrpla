#!/usr/bin/env python3
"""Isolated native-server integration. Requires Docker and built XRPLA images.

python3 tests/native_integration.py --core-image xrpla-core:local --server-image xrpla-integration:local
"""
import argparse
from contextlib import ExitStack
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--core-image', default='xrpla-core:local')
    parser.add_argument('--server-image', default='xrpla-integration:local')
    parser.add_argument('--out', default='.cache/native-integration.json')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    cache = repo / '.cache'; cache.mkdir(exist_ok=True)
    token = 'xrpla-test-' + uuid.uuid4().hex[:10]
    network = token + '-net'; containers = []
    # Keep mounted test files owned by the invoking Linux CI user.
    run_user = f'{os.getuid()}:{os.getgid()}' if hasattr(os, 'getuid') else '0'

    def docker(*command, check=True):
        result = subprocess.run(['docker', *map(str, command)], capture_output=True, text=True)
        if check and result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        return result.stdout.strip()

    def wait_for(command, seconds=150):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                return command()
            except Exception:
                time.sleep(1)
        raise TimeoutError('Native integration service did not become ready')

    def cleanup():
        for name in reversed(containers):
            docker('rm', '-fv', name, check=False)
        containers.clear()

    versions = {
        'xrpla': docker('run', '--rm', args.core_image, '--version'),
        'xrpld': docker('run', '--rm', args.server_image, '-lc', 'xrpld --version'),
        'clio': docker('run', '--rm', args.server_image, '-lc', 'clio_server --version'),
    }
    assert '(libxrpl 3.3.0)' in versions['xrpla'], versions
    assert re.search(r'\b3\.3\.0\b', versions['xrpld']), versions
    assert re.search(r'\b2\.7\.1\b', versions['clio']), versions
    docker('network', 'create', network)
    try:
        with ExitStack() as stack:
            tmp = stack.enter_context(tempfile.TemporaryDirectory(prefix=token + '-', dir=cache))
            # Stop mounted database processes before removing their files.
            stack.callback(cleanup)
            work = Path(tmp)
            mounts = ['-v', f'{repo}:/src:ro', '-v', f'{work}:/work']
            def core(*command, entrypoint=None):
                options = ['run', '--rm', '--user', run_user, '--network', network, *mounts]
                if entrypoint:
                    options += ['--entrypoint', entrypoint]
                return docker(*options, args.core_image, *command)
            cassandra, native, clio = (token + '-' + x for x in ('cassandra', 'native', 'clio'))
            containers.append(cassandra)
            docker('run', '-d', '--name', cassandra, '--network', network, '--memory', '1536m', '--cpus', '2',
                   '-e', 'MAX_HEAP_SIZE=512M', '-e', 'HEAP_NEWSIZE=100M',
                   'cassandra:4.1.10@sha256:5e620e10efaeaf936ae73963e594c98df88f0cd468582d7ceb324cbb864f0604')
            wait_for(lambda: docker('exec', cassandra, 'cqlsh', '-e', 'SELECT release_version FROM system.local;'))
            fixture = '/src/tests/fixtures/mainnet-transactions'
            core('fetch', '--archive', fixture, '--range', '1000000-1000004', '--publisher-key', fixture + '/publisher.pub', '--dir', '/work/data')
            core('build', '--publisher-key', fixture + '/publisher.pub', '--dir', '/work/data')
            native_import = json.loads(core('import', 'rippled', '--dir', '/work/data', '--out', '/work/native'))
            clio_import = json.loads(core('import', 'clio', '--dir', '/work/data', '--out', '/work/clio', '--host', cassandra, '--keyspace', 'xrpla_validation'))
            repeated = json.loads(core('import', 'clio', '--dir', '/work/data', '--out', '/work/clio', '--host', cassandra, '--keyspace', 'xrpla_validation'))
            assert repeated['load']['rows_sent'] == 0, repeated
            config = work / 'native' / 'rippled.cfg'
            config.write_text(config.read_text().replace('ip = 127.0.0.1', 'ip = 0.0.0.0'))
            containers.append(native)
            docker('run', '-d', '--name', native, '--user', run_user, '--network', network, '--memory', '1536m', '--cpus', '2', *mounts,
                   args.server_image, '-lc', 'exec xrpld --conf /work/native/rippled.cfg --standalone --ledger 1000004 --valid')
            def ledger():
                result = json.loads(docker('exec', native, 'xrpld', '--conf', '/work/native/rippled.cfg', 'ledger', '1000000'))['result']
                assert result['ledger_hash'].lower() == native_import['first_hash'], result
                return result
            native_ledger = wait_for(ledger)
            config_clio = json.loads((repo / 'config/clio-validation.json').read_text())
            config_clio['database']['cassandra']['contact_points'] = cassandra
            config_clio['log'] = {'level': 'error', 'enable_console': True}
            (work / 'clio.json').write_text(json.dumps(config_clio))
            containers.append(clio)
            docker('run', '-d', '--name', clio, '--user', run_user, '--network', network, '--memory', '768m', '--cpus', '2', *mounts,
                   args.server_image, '-lc', 'exec clio_server --conf /work/clio.json')
            # Check server readiness before the full conformance run.
            probe = f"import json,urllib.request; r=json.load(urllib.request.urlopen(urllib.request.Request('http://{clio}:51233/',json.dumps({{'method':'ledger','params':[{{'ledger_index':1000000}}]}}).encode(),{{'Content-Type':'application/json'}}))); assert 'ledger' in r['result'],r"
            wait_for(lambda: core('-c', probe, entrypoint='python3'))
            core('/src/tools/conformance.py', '--binary', '/usr/local/bin/xrpla', '--dir', '/work/data',
                 '--endpoint', f'http://{clio}:51233/', '--first', '1000000', '--last', '1000004',
                 '--out', '/work/conformance.json', entrypoint='python3')
            core('keygen', '--out', '/work/export.seed')
            exported = json.loads(core('export', '--source', f'http://{clio}:51233/', '--range', '1000001-1000004',
                                      '--out', '/work/export', '--signing-key', '/work/export.seed', '--delay-ms', '0'))
            core('fetch', '--archive', '/work/export', '--range', '1d', '--publisher-key', '/work/export/publisher.pub', '--dir', '/work/roundtrip')
            roundtrip = json.loads(core('build', '--publisher-key', '/work/export/publisher.pub', '--dir', '/work/roundtrip'))
            assert roundtrip['last_ledger_hash'] == native_import['last_hash'], roundtrip
            report = {'versions': versions, 'native_ledger': native_ledger, 'rippled_import': native_import,
                      'clio_import': clio_import, 'clio_restart': repeated,
                      'export': exported, 'roundtrip': roundtrip,
                      'conformance': json.loads((work / 'conformance.json').read_text())}
            output = Path(args.out); output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, indent=2))
            print(json.dumps({'passed': True, 'report': str(output)}))
    finally:
        cleanup()
        docker('network', 'rm', network, check=False)


if __name__ == '__main__':
    main()
