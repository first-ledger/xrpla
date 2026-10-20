#!/usr/bin/env python3
"""Throughput of a JSON-RPC server under concurrent WebSocket load: many connections, each with
requests pipelined, from several processes, for a fixed time per concurrency level. Reports
requests a second, latency percentiles, errors by method and the server process's CPU use
(cores busy), so throughput per core can be compared between servers.

  load-bench.py corpus PORT FIRST LAST OUT.json          build a request corpus from a server
  load-bench.py run PORT PID CORPUS.json LABEL MIX SECONDS INFLIGHT[,INFLIGHT...] > results.jsonl

MIX is `common` (methods every compared server answers) or `full` (adds account_tx). PID is the
server process whose CPU time is sampled. The generator should run on CPUs the server does not
(taskset), or it competes with what it measures.
"""
import base64, json, multiprocessing as mp, os, random, socket, struct, sys, time

PROCESSES = int(os.environ.get('LOAD_PROCESSES', '16'))
MIXES = {
    'common': {'account_info': 25, 'tx': 20, 'ledger': 10, 'ledger_entry': 10, 'account_lines': 10,
               'book_offers': 5, 'account_objects': 5, 'server_info': 5},
}
MIXES['full'] = dict(MIXES['common'], account_tx=10)


class WebSocket:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=60)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                           f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        self.buffer = b''
        while b'\r\n\r\n' not in self.buffer: self.buffer += self.sock.recv(65536)
        head, self.buffer = self.buffer.split(b'\r\n\r\n', 1)
        if b' 101 ' not in head.split(b'\r\n')[0]: raise RuntimeError('WebSocket upgrade refused: ' + head[:80].decode())

    def send(self, text):
        data = text.encode(); mask = os.urandom(4); n = len(data)
        header = bytes([0x81]) + (bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + struct.pack('>H', n) if n < 65536 else bytes([0x80 | 127]) + struct.pack('>Q', n))
        self.sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def frames(self):
        """Whole messages already received, after one recv."""
        chunk = self.sock.recv(1 << 20)
        if not chunk: raise RuntimeError('connection closed')
        self.buffer += chunk; out = []
        while len(self.buffer) >= 2:
            b1 = self.buffer[1] & 0x7F; at = 2
            if b1 == 126:
                if len(self.buffer) < 4: break
                b1 = struct.unpack('>H', self.buffer[2:4])[0]; at = 4
            elif b1 == 127:
                if len(self.buffer) < 10: break
                b1 = struct.unpack('>Q', self.buffer[2:10])[0]; at = 10
            if len(self.buffer) < at + b1: break
            fin, opcode = self.buffer[0] & 0x80, self.buffer[0] & 0x0F
            payload = self.buffer[at:at + b1]; self.buffer = self.buffer[at + b1:]
            if opcode == 9: self.sock.sendall(bytes([0x8A, 0x80]) + os.urandom(4)); continue  # pong
            if opcode in (0, 1):  # a message may come in fragments: text, then continuations
                self.partial = (b'' if opcode == 1 else getattr(self, 'partial', b'')) + payload
                if fin: out.append(self.partial); self.partial = b''
        return out


def request(method, case):
    if method == 'account_info': return {'command': method, 'account': case['account'], 'ledger_index': case['ledger']}
    if method == 'account_lines': return {'command': method, 'account': case['account'], 'ledger_index': case['ledger']}
    if method == 'account_objects': return {'command': method, 'account': case['account'], 'ledger_index': case['ledger'], 'limit': 50}
    if method == 'account_tx': return {'command': method, 'account': case['account'], 'ledger_index_min': case['ledger'] - 20000, 'ledger_index_max': case['ledger'], 'limit': 20}
    if method == 'ledger_entry': return {'command': method, 'account_root': case['account'], 'ledger_index': case['ledger']}
    if method == 'tx': return {'command': method, 'transaction': case['tx']}
    if method == 'ledger': return {'command': method, 'ledger_index': case['ledger']}
    if method == 'book_offers': return {'command': method, 'taker_pays': case['pays'], 'taker_gets': case['gets'], 'ledger_index': case['ledger'], 'limit': 20}
    return {'command': 'server_info'}


def worker(port, corpus, mix, seconds, inflight, connections, seed, queue):
    rng = random.Random(seed)
    methods = [m for m, w in mix.items() for _ in range(w)]
    sockets = [WebSocket(port) for _ in range(connections)]
    sent = {}; latencies = []; done = 0; errors = {}; samples = []; next_id = [seed * 10_000_000]
    def fire(ws):
        method = rng.choice(methods)
        pool = corpus['books'] if method == 'book_offers' else corpus['txs'] if method == 'tx' else corpus['accounts']
        body = request(method, rng.choice(pool)); next_id[0] += 1; body['id'] = next_id[0]
        sent[next_id[0]] = (time.perf_counter(), method); ws.send(json.dumps(body))
    per = max(1, inflight // connections)
    for ws in sockets:
        for _ in range(per): fire(ws)
    import selectors
    sel = selectors.DefaultSelector()
    for ws in sockets: sel.register(ws.sock, selectors.EVENT_READ, ws)
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        for key, _ in sel.select(timeout=1):
            ws = key.data
            for payload in ws.frames():
                now = time.perf_counter()
                i = payload.find(b'"id":')
                rid = int(payload[i + 5:payload.index(b',', i) if payload.find(b',', i) > 0 else payload.index(b'}', i)].strip()) if i >= 0 else None
                t0, method = sent.pop(rid, (now, '?'))
                if b'"status":"success"' not in payload and b'"status": "success"' not in payload:
                    errors[method] = errors.get(method, 0) + 1
                    if len(samples) < 3: samples.append(payload[:300].decode(errors='replace'))
                done += 1
                if len(latencies) < 200000: latencies.append(now - t0)
                fire(ws)
    queue.put({'done': done, 'latencies': latencies, 'errors': errors, 'samples': samples, 'cpu': time.process_time()})


def cpu_seconds(pid):
    fields = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def run(port, pid, corpus_path, label, mix_name, seconds, levels):
    corpus = json.load(open(corpus_path)); mix = {mix_name[5:]: 1} if mix_name.startswith('only:') else MIXES[mix_name]
    for inflight in levels:
        connections = max(1, min(8, inflight // PROCESSES))  # per process
        queue = mp.Queue()
        procs = [mp.Process(target=worker, args=(port, corpus, mix, seconds, max(1, inflight // PROCESSES), connections, s + 1, queue)) for s in range(PROCESSES)]
        c0, t0 = cpu_seconds(pid), time.time()
        for p in procs: p.start()
        results = [queue.get() for _ in procs]
        for p in procs: p.join()
        c1, t1 = cpu_seconds(pid), time.time()
        done = sum(r['done'] for r in results)
        lat = sorted(x for r in results for x in r['latencies'])
        errors = {}
        for r in results:
            for k, v in r['errors'].items(): errors[k] = errors.get(k, 0) + v
        pct = lambda q: round(1000 * lat[min(len(lat) - 1, int(q * len(lat)))], 2) if lat else None
        print(json.dumps({'label': label, 'mix': mix_name, 'inflight': inflight, 'seconds': seconds, 'requests': done,
                          'per_second': round(done / seconds), 'p50_ms': pct(0.5), 'p90_ms': pct(0.9), 'p99_ms': pct(0.99),
                          'server_cores_busy': round((c1 - c0) / (t1 - t0), 2),
                          'generator_cores_busy': round(sum(r['cpu'] for r in results) / (t1 - t0), 2), 'processes': PROCESSES,
                          'errors': errors, 'error_samples': [s for r in results for s in r['samples']][:2]}), flush=True)


def build_corpus(port, first, last, out):
    ws = WebSocket(port); rng = random.Random(1)
    def call(body):
        ws.send(json.dumps(body))
        while True:
            for payload in ws.frames(): return json.loads(payload)
    accounts, txs, books = [], [], []
    for seq in rng.sample(range(first + 25000, last + 1), 300):
        r = call({'command': 'ledger', 'ledger_index': seq, 'transactions': True, 'expand': True})
        for t in r.get('result', {}).get('ledger', {}).get('transactions', []):
            accounts.append({'account': t['Account'], 'ledger': seq})
            if t.get('Destination'): accounts.append({'account': t['Destination'], 'ledger': seq})
            txs.append({'tx': t['hash']})
            if t.get('TransactionType') == 'OfferCreate':
                side = lambda a: {'currency': 'XRP'} if isinstance(a, str) else {'currency': a['currency'], 'issuer': a['issuer']}
                books.append({'pays': side(t['TakerPays']), 'gets': side(t['TakerGets']), 'ledger': seq})
    json.dump({'accounts': accounts, 'txs': txs, 'books': books}, open(out, 'w'))
    print(f'corpus: {len(accounts)} account cases, {len(txs)} transactions, {len(books)} books')


if __name__ == '__main__':
    if sys.argv[1] == 'corpus': build_corpus(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
    else: run(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5], sys.argv[6], int(sys.argv[7]), [int(x) for x in sys.argv[8].split(',')])
