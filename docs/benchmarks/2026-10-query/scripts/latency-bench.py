#!/usr/bin/env python3
"""Request latency of `xrpla serve` over HTTP and WebSocket, for several data directories (a
piece cache or a store) holding the same ledgers. Each is served by the same build; the same
random transactions by hash, ledgers by hash and account_info are asked of each: one at a time
over HTTP, then over one WebSocket one at a time and with 16 in flight. Each directory gets a
fresh server (cold) and is then asked again (warm). Prints one JSON line per measurement.

  latency-bench.py BINARY FIRST LAST LABEL=DIR [LABEL=DIR ...]
"""
import base64, json, os, random, socket, statistics, struct, subprocess, sys, time, urllib.request

BINARY, FIRST, LAST = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
TARGETS = [arg.split('=', 1) for arg in sys.argv[4:]]

def http_call(port, method, params):
    req = urllib.request.Request(f'http://127.0.0.1:{port}', json.dumps({'method': method, 'params': [params]}).encode(), {'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=120) as r: return json.load(r)['result']

class WebSocket:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=120)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                           f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        head = b''
        while b'\r\n\r\n' not in head: head += self.sock.recv(1)
        if b' 101 ' not in head.split(b'\r\n')[0]: raise RuntimeError(head.decode(errors='replace'))
        self.buffer = b''
    def send(self, value):
        data = json.dumps(value).encode(); mask = os.urandom(4)
        n = len(data)
        header = bytes([0x81]) + (bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + struct.pack('>H', n) if n < 65536 else bytes([0x80 | 127]) + struct.pack('>Q', n))
        self.sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))
    def read(self, n):
        while len(self.buffer) < n:
            chunk = self.sock.recv(1 << 20)
            if not chunk: raise RuntimeError('connection closed')
            self.buffer += chunk
        out, self.buffer = self.buffer[:n], self.buffer[n:]; return out
    def receive(self):
        message = b''
        while True:
            b0, b1 = self.read(2); length = b1 & 0x7f
            if length == 126: length = struct.unpack('>H', self.read(2))[0]
            elif length == 127: length = struct.unpack('>Q', self.read(8))[0]
            payload = self.read(length)
            if (b0 & 0x0f) == 0x9: continue
            message += payload
            if b0 & 0x80: return json.loads(message)

def stats(values):
    v = sorted(values)
    return {'n': len(v), 'median_ms': round(1000 * statistics.median(v), 3), 'p95_ms': round(1000 * v[min(len(v) - 1, int(len(v) * 0.95))], 3),
            'p99_ms': round(1000 * v[min(len(v) - 1, int(len(v) * 0.99))], 3), 'max_ms': round(1000 * v[-1], 3)}

def start(directory, port):
    process = subprocess.Popen([BINARY, 'serve', '--dir', directory, '--http', f'127.0.0.1:{port}', '--ws', f'127.0.0.1:{port + 1000}'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(180):
        try: http_call(port, 'server_info', {}); return process
        except Exception: time.sleep(1)
    raise RuntimeError('server did not start: ' + directory)

# The requests, sampled once from the first directory.
first = start(TARGETS[0][1], 15700)
rng = random.Random(7)
hashes, ledgers, accounts = [], [], []
for seq in rng.sample(range(FIRST, LAST + 1), 700):
    r = http_call(15700, 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True})
    ledgers.append(r['ledger_hash'])
    for tx in r['ledger'].get('transactions', []): hashes.append(tx['hash']); accounts.append(tx['Account'])
first.terminate(); first.wait()
hashes = rng.sample(hashes, min(2000, len(hashes))); ledgers = ledgers[:500]; accounts = rng.sample(sorted(set(accounts)), min(500, len(set(accounts))))
sets = [('tx', 'tx', [{'transaction': h} for h in hashes]), ('ledger by hash', 'ledger', [{'ledger_hash': h} for h in ledgers]),
        ('account_info', 'account_info', [{'account': a, 'ledger_index': LAST - 1} for a in accounts])]
print(json.dumps({'requests': {name: len(items) for name, _, items in sets}}), flush=True)

for i, (label, directory) in enumerate(TARGETS):
    port = 15710 + 10 * i
    process = start(directory, port)
    for phase in ('cold', 'warm'):
        for name, method, items in sets:
            times = []
            for params in items:
                s = time.perf_counter(); r = http_call(port, method, params); times.append(time.perf_counter() - s)
                if 'error' in r: raise RuntimeError(f'{label} {method}: {r}')
            print(json.dumps({'target': label, 'phase': phase, 'transport': 'http', 'request': name, **stats(times)}), flush=True)
        ws = WebSocket(port + 1000)
        for name, method, items in sets:
            times = []
            for n, params in enumerate(items):
                s = time.perf_counter(); ws.send({'command': method, 'id': n, **params}); reply = ws.receive(); times.append(time.perf_counter() - s)
                if reply.get('status') != 'success': raise RuntimeError(f'{label} ws {method}: {str(reply)[:200]}')
            s = time.perf_counter(); sent = received = 0
            while received < len(items):
                while sent < len(items) and sent - received < 16: ws.send({'command': method, 'id': sent, **items[sent]}); sent += 1
                ws.receive(); received += 1
            rate = len(items) / (time.perf_counter() - s)
            print(json.dumps({'target': label, 'phase': phase, 'transport': 'ws', 'request': name, **stats(times), 'per_second_16_in_flight': round(rate)}), flush=True)
    process.terminate(); process.wait()
print('DONE')
