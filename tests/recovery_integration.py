"""Kill a real admission process, then recover its partial RocksDB store."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

binary, fixture = map(os.path.abspath, sys.argv[1:3])
with tempfile.TemporaryDirectory(prefix='xrpla-kill-recovery-') as tmp:
    root = Path(tmp); source = root / 'source'; data = root / 'data'
    env = dict(os.environ, XRPLA_CORE_FIXTURE_DIR=str(source), XRPLA_CORE_FIXTURE_COUNT='8192')
    subprocess.run([fixture, '--gtest_filter=CoreFixture.Export'], env=env, check=True, stdout=subprocess.DEVNULL)
    common = ['--dir', str(data), '--publisher-key', str(source / 'publisher.pub')]
    subprocess.run([binary, 'fetch', '--archive', str(source), '--range', '65536-73727', *common], check=True, stdout=subprocess.DEVNULL)
    process = subprocess.Popen([binary, 'build', *common], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    deadline = time.monotonic() + 60
    while not (data / 'store' / 'CURRENT').exists():
        if process.poll() is not None:
            raise AssertionError('Build completed before crash point: ' + process.communicate()[1].decode())
        if time.monotonic() > deadline:
            process.kill(); process.wait(); raise TimeoutError('Admission never started')
        time.sleep(.005)
    process.kill(); process.communicate(timeout=10)
    recovered = subprocess.run([binary, 'build', *common], check=True, capture_output=True, text=True, timeout=180)
    result = json.loads(recovered.stdout)
    assert result['store']['complete_ledgers'] == {'first': 65536, 'last': 73727}, result
    expected = json.loads((source / 'expected.json').read_text())
    assert result['store']['last_ledger_hash'] == expected['anchor'], result
    # The killed process must not leave a lock that blocks subsequent commands.
    again = json.loads(subprocess.check_output([binary, 'build', *common], text=True, timeout=180))
    assert again['ledgers_added'] == 0
    print(json.dumps({'crash_recovery': 'passed', 'complete_ledgers': result['store']['complete_ledgers']}))
