#!/usr/bin/env python3
"""WebSocket query speed of the two 8,000,001-8,300,000 stores (q-old: the audit's build;
q-new: bloom filters and open-file limit), each served by its own build: the same random
transactions by hash, ledgers by hash and account_info as the HTTP benchmark, over one
connection per server, first one request at a time (latency), then 16 in flight (throughput).
A minimal RFC 6455 client: the standard library has none."""
import base64, json, os, random, socket, struct, subprocess, time, urllib.request

def http_call(port, method, params):
    req = urllib.request.Request(f'http://127.0.0.1:{port}', json.dumps({'method': method, 'params': [params]}).encode(), {'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as r: return json.load(r)['result']

class WebSocket:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=60)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                           f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        head = b''
        while b'\r\n\r\n' not in head: head += self.sock.recv(1)
        if b' 101 ' not in head.split(b'\r\n')[0]: raise RuntimeError(head.decode(errors='replace'))
        self.buffer = b''
    def send(self, value):
        data = json.dumps(value).encode(); mask = os.urandom(4)
        header = bytes([0x81]) + (bytes([0x80 | len(data)]) if len(data) < 126 else bytes([0x80 | 126]) + struct.pack('>H', len(data)) if len(data) < 65536 else bytes([0x80 | 127]) + struct.pack('>Q', len(data)))
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
            payload = self.read(length); opcode = b0 & 0x0f
            if opcode == 0x9: continue  # ping (no pong needed for a benchmark)
            message += payload
            if b0 & 0x80: return json.loads(message)

os.chdir(os.path.expanduser('~/store-bench'))
servers = {}
for i, build in enumerate(('old', 'new')):
    binary = os.path.expanduser('~/xrpla-audit/inst34/bin/xrpla' if build == 'old' else '~/store-bench/new/bin/xrpla')
    servers[build] = (15200 + i, 16200 + i, subprocess.Popen([binary, 'serve', '--dir', f'q-{build}', '--http', f'127.0.0.1:{15200 + i}', '--ws', f'127.0.0.1:{16200 + i}'],
                                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
for build, (http, ws, _) in servers.items():
    for _ in range(120):
        try: http_call(http, 'server_info', {}); break
        except Exception: time.sleep(1)
random.seed(7)
http = servers['new'][0]
hashes, ledgers, accounts = [], [], []
for seq in random.sample(range(8000001, 8300001), 700):
    r = http_call(http, 'ledger', {'ledger_index': seq, 'transactions': True, 'expand': True})
    ledgers.append(r['ledger_hash'])
    for tx in r['ledger'].get('transactions', []): hashes.append(tx['hash']); accounts.append(tx['Account'])
hashes = random.sample(hashes, min(2000, len(hashes))); ledgers = ledgers[:500]; accounts = random.sample(sorted(set(accounts)), min(500, len(set(accounts))))
sets = (('tx', [{'command': 'tx', 'transaction': h} for h in hashes]),
        ('ledger by hash', [{'command': 'ledger', 'ledger_hash': h} for h in ledgers]),
        ('account_info', [{'command': 'account_info', 'account': a, 'ledger_index': 8299999} for a in accounts]))
print('requests: %d tx, %d ledgers, %d accounts' % (len(hashes), len(ledgers), len(accounts)))
for build in ('old', 'new', 'old', 'new'):
    socket_ = WebSocket(servers[build][1]); out = []
    for name, requests in sets:
        times = []
        for n, request in enumerate(requests):
            s = time.perf_counter(); socket_.send(dict(request, id=n)); reply = socket_.receive(); times.append(time.perf_counter() - s)
            assert reply.get('status') == 'success', (name, str(reply)[:300])
        times.sort()
        # Throughput: 16 requests in flight.
        s = time.perf_counter(); sent = received = 0
        while received < len(requests):
            while sent < len(requests) and sent - received < 16: socket_.send(dict(requests[sent], id=sent)); sent += 1
            socket_.receive(); received += 1
        rate = len(requests) / (time.perf_counter() - s)
        out.append('%s %.2f ms median, %.2f ms p99, %.0f/s with 16 in flight' % (name, 1000 * times[len(times) // 2], 1000 * times[int(len(times) * 0.99)], rate))
    print(build + ' (ws):', '; '.join(out))
for _, (_, _, process) in servers.items(): process.terminate()
print('DONE')
