#!/usr/bin/env python3
"""Capture a validator list and live validations as a test fixture.

Fetches a signed validator list (default https://vl.ripple.com), then subscribes to an
xrpld node's `validations` and `manifests` streams and records every message for a while,
plus the headers of the ledgers they validate. The result lets tests check validator-list
verification and quorum counting against real mainnet data.

  capture_validations.py --node ws://127.0.0.1:6006 --rpc http://127.0.0.1:5005 --seconds 40 --out FILE
"""
import argparse
import base64
import json
import os
import socket
import ssl
import struct
import time
import urllib.parse
import urllib.request


class WebSocket:
    """Just enough of RFC 6455 for an xrpld or xrpla WebSocket: ws://, or wss:// with the
    server's certificate checked."""
    def __init__(self, url):
        parts = urllib.parse.urlparse(url)
        if parts.scheme not in ('ws', 'wss'): raise ValueError('Only ws:// and wss:// are supported')
        self.sock = socket.create_connection((parts.hostname, parts.port or (443 if parts.scheme == 'wss' else 80)), timeout=30)
        if parts.scheme == 'wss': self.sock = ssl.create_default_context().wrap_socket(self.sock, server_hostname=parts.hostname)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f'GET {parts.path or "/"} HTTP/1.1\r\nHost: {parts.netloc}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                           f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        response = b''
        while b'\r\n\r\n' not in response: response += self.sock.recv(4096)
        if b' 101 ' not in response.split(b'\r\n', 1)[0]: raise ValueError('WebSocket upgrade refused')
        self.buffer = response.split(b'\r\n\r\n', 1)[1]

    def send(self, message):
        self.frame(0x1, json.dumps(message).encode())

    def frame(self, opcode, data):
        mask = os.urandom(4)
        header = bytes([0x80 | opcode]) + (bytes([0x80 | len(data)]) if len(data) < 126 else bytes([0x80 | 126]) + struct.pack('>H', len(data)))
        self.sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def take(self, n):
        while len(self.buffer) < n:
            chunk = self.sock.recv(65536)
            if not chunk: raise ConnectionError('WebSocket closed')
            self.buffer += chunk
        out, self.buffer = self.buffer[:n], self.buffer[n:]
        return out

    def receive(self):
        """The next text message, joined from its continuation frames."""
        message = b''
        while True:
            first, second = self.take(2)
            length = second & 127
            if length == 126: length = struct.unpack('>H', self.take(2))[0]
            elif length == 127: length = struct.unpack('>Q', self.take(8))[0]
            payload = self.take(length)
            if first & 15 == 9: self.frame(0xA, payload); continue  # a ping is answered, as RFC 6455 requires
            if first & 15 == 10: continue  # pong
            if first & 15 == 8: raise ConnectionError('WebSocket closed')
            message += payload
            if first & 0x80: return json.loads(message)


def rpc(url, method, params):
    body = json.dumps({'method': method, 'params': [params]}).encode()
    with urllib.request.urlopen(urllib.request.Request(url, body, {'Content-Type': 'application/json'}), timeout=30) as r:
        return json.load(r)['result']


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--node', default='ws://127.0.0.1:6006'); p.add_argument('--rpc', default='http://127.0.0.1:5005')
    p.add_argument('--list', default='https://vl.ripple.com'); p.add_argument('--seconds', type=int, default=40)
    p.add_argument('--out', required=True)
    a = p.parse_args()
    with urllib.request.urlopen(a.list, timeout=30) as r: validator_list = json.load(r)
    ws = WebSocket(a.node)
    ws.send({'id': 1, 'command': 'subscribe', 'streams': ['validations', 'manifests']})
    messages, deadline = [], time.time() + a.seconds
    while time.time() < deadline:
        message = ws.receive()
        if message.get('type') in ('validationReceived', 'manifestReceived'): messages.append(message)
    ledgers = {}
    for m in messages:
        if m.get('type') == 'validationReceived' and m['ledger_hash'] not in ledgers:
            header = rpc(a.rpc, 'ledger', {'ledger_hash': m['ledger_hash'], 'binary': True})
            if 'ledger_data' in header.get('ledger', {}): ledgers[m['ledger_hash']] = {'ledger_index': int(m['ledger_index']), 'header': header['ledger']['ledger_data']}
    server = rpc(a.rpc, 'server_info', {})['info']
    out = {'captured_at': int(time.time()), 'list_site': a.list, 'validator_list': validator_list, 'messages': messages,
           'ledgers': ledgers, 'node_quorum': server.get('validation_quorum'), 'build_version': server.get('build_version')}
    with open(a.out, 'w') as f: json.dump(out, f, indent=1)
    print(json.dumps({'messages': len(messages), 'validations': sum(m['type'] == 'validationReceived' for m in messages),
                      'ledgers': len(ledgers), 'node_quorum': out['node_quorum']}))


if __name__ == '__main__':
    main()
