#!/usr/bin/env python3
"""Bounded HTTP load test, checking every response against recorded node evidence."""
import argparse
import gzip
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time
import threading
import urllib.request

from node_conformance import collect, project


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default='http://127.0.0.1:5025')
    parser.add_argument('--recorded', type=Path, required=True)
    parser.add_argument('--requests', type=int)
    parser.add_argument('--duration-seconds', type=int, default=0)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if not 0 <= args.duration_seconds <= 3600:
        parser.error('duration-seconds must be 0..3600')
    if args.requests is None: args.requests = 1000000 if args.duration_seconds else 1000
    if not 1 <= args.workers <= 32 or not 1 <= args.requests <= 1000000:
        parser.error('workers must be 1..32 and requests must be 1..1000000')
    corpus = json.loads(gzip.decompress(args.recorded.read_bytes()) if args.recorded.suffix == '.gz' else args.recorded.read_bytes())
    cases = corpus['comparisons']
    if not cases:
        parser.error('recording contains no comparisons')
    # Fail early if the recording cannot be projected.
    expected = [project(row['method'], row['expected']) for row in cases]
    def request(index):
        case_index = index % len(cases)
        row = cases[case_index]
        start = time.perf_counter()
        http_count, total_bytes = 0, 0
        try:
            def remote(method, params):
                nonlocal http_count, total_bytes
                http_count += 1
                body = json.dumps({'method': method, 'params': [params]}).encode()
                req = urllib.request.Request(args.endpoint, body, {'Content-Type': 'application/json'})
                with urllib.request.urlopen(req, timeout=60) as response:
                    raw = response.read(128 * 1024 * 1024 + 1)
                if len(raw) > 128 * 1024 * 1024:
                    raise ValueError('Response exceeds 128 MiB')
                total_bytes += len(raw)
                return json.loads(raw)['result']
            result = collect(row['method'], row['params'], remote)
            if project(row['method'], result) != expected[case_index]:
                raise ValueError('Canonical response differs from recording')
            return {'case': case_index, 'method': row['method'], 'seconds': time.perf_counter() - start,
                    'bytes': total_bytes, 'http_requests': http_count, 'passed': True}
        except Exception as error:
            return {'case': case_index, 'method': row['method'], 'seconds': time.perf_counter() - start,
                    'http_requests': http_count, 'passed': False, 'error': str(error)}
    start = time.perf_counter()
    deadline = start + args.duration_seconds if args.duration_seconds else float('inf')
    counter, lock = 0, threading.Lock()
    def worker():
        nonlocal counter
        rows = []
        while time.perf_counter() < deadline:
            with lock:
                if counter >= args.requests: break
                index = counter; counter += 1
            rows.append(request(index))
        return rows
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = [row for batch in pool.map(lambda _: worker(), range(args.workers)) for row in batch]
    elapsed = time.perf_counter() - start
    times = sorted(row['seconds'] for row in results)
    def percentile(p):
        return times[min(len(times) - 1, int((len(times) - 1) * p))] * 1000
    report = {'endpoint': args.endpoint, 'profile': corpus['profile'], 'range': corpus['range'],
              'workers': args.workers, 'requests': len(results), 'seconds': elapsed,
              'duration_requested_seconds': args.duration_seconds,
              'queries_per_second': len(results) / elapsed,
              'http_requests': sum(row['http_requests'] for row in results),
              'requests_per_second': sum(row['http_requests'] for row in results) / elapsed,
              'passed': sum(row['passed'] for row in results),
              'response_bytes': sum(row.get('bytes', 0) for row in results),
              'latency_ms': {'p50': percentile(.50), 'p95': percentile(.95), 'p99': percentile(.99),
                             'max': times[-1] * 1000},
              'failures': [row for row in results if not row['passed']]}
    args.out.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] == report['requests'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
