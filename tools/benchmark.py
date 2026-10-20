#!/usr/bin/env python3
"""Linux benchmark: per-process elapsed time, peak RSS and logical disk bytes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import tempfile
import time


def run(command, log_prefix=None, timeout=7200):
    output = open(str(log_prefix) + '.stdout', 'w+b') if log_prefix else tempfile.TemporaryFile()
    errors = open(str(log_prefix) + '.stderr', 'w+b') if log_prefix else tempfile.TemporaryFile()
    with output as out, errors as error:
        start = time.monotonic(); pid = os.fork()
        if pid == 0:
            os.setsid()
            os.dup2(out.fileno(), 1); os.dup2(error.fileno(), 2)
            try:
                os.execvp(command[0], command)
            except BaseException as exc:
                os.write(2, str(exc).encode())
                os._exit(127)
        timed_out = False
        try:
            while True:
                finished, status, usage = os.wait4(pid, os.WNOHANG)
                if finished:
                    break
                if time.monotonic() - start >= timeout:
                    timed_out = True
                    os.killpg(pid, signal.SIGKILL)
                    _, status, usage = os.wait4(pid, 0)
                    break
                time.sleep(0.1)
        except BaseException:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.wait4(pid, 0)
            raise
        out.seek(0); error.seek(0)
        result = {'seconds': time.monotonic() - start, 'peak_rss_kib': usage.ru_maxrss,
                'user_seconds': usage.ru_utime, 'system_seconds': usage.ru_stime,
                'exit_code': os.waitstatus_to_exitcode(status), 'timed_out': timed_out}
        stdout = out.read().decode(errors='replace')
        try:
            result['result'] = json.loads(stdout)
        except ValueError:
            result['stdout_tail'] = stdout[-4096:]
        if status:
            result['stderr_tail'] = error.read().decode(errors='replace')[-4096:]
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'archive', 'range', 'dir', 'out'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--trust-ledger-hash', help='Independently obtained hash at the requested range end')
    parser.add_argument('--phase-timeout', type=int, default=7200)
    parser.add_argument('--skip-verify', action='store_true',
                        help='Measure build directly; build still performs full canonical verification')
    args = parser.parse_args()
    root, source = Path(args.dir), Path(args.archive)
    if root.exists():
        raise ValueError('Benchmark directory must be new')
    if args.phase_timeout < 1:
        parser.error('phase-timeout must be positive')
    binary = str(Path(args.binary).resolve()); key = str(source / 'publisher.pub')
    import subprocess
    version = subprocess.check_output([binary, '--version'], text=True).strip()
    report_path = Path(args.out)
    if report_path.exists():
        raise ValueError('Benchmark report must be new')
    logs = report_path.with_suffix('.logs'); logs.mkdir(parents=True, exist_ok=False)
    report = {'platform': platform.platform(), 'cpu_count': os.cpu_count(), 'binary_version': version,
              'manifest_sha256': hashlib.sha256((source / 'manifest.json').read_bytes()).hexdigest(),
              'trust_anchor': args.trust_ledger_hash, 'range': args.range, 'phases': {}}
    anchor = ['--trust-ledger-hash', args.trust_ledger_hash] if args.trust_ledger_hash else []
    phases = [('fetch', ['fetch', '--archive', str(source), '--range', args.range])]
    if not args.skip_verify:
        phases.append(('verify', ['verify', '--full', *anchor]))
    phases.extend([('build', ['build', *anchor]), ('repeat_build', ['build', *anchor])])
    for name, command in phases:
        print(f'Starting {name}; logs: {logs / name}', flush=True)
        report['phases'][name] = run([binary, *command, '--dir', str(root), '--publisher-key', key], logs / name, args.phase_timeout)
        report_path.write_text(json.dumps(report, indent=2))
        if report['phases'][name]['exit_code'] or 'result' not in report['phases'][name]:
            raise RuntimeError(f'{name} failed; see {report_path} and {logs}')
    report['logical_bytes'] = {name: sum(p.stat().st_size for p in (root / name).rglob('*') if p.is_file())
                               for name in ('archive', 'store', '.verification-nodes')}
    report['passed'] = True
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
