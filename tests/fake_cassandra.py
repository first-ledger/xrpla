"""A Cassandra native protocol v4 server holding tables in memory, for tools/clio_load.py's tests.

It answers the statements the loader sends (its keyspace lease, status and range rows by
text; the plan's rows as prepared inserts, alone or in unlogged batches) and keeps, for
each primary key, the write with the highest timestamp, as Cassandra does. It can refuse
some writes as overloaded, or drop every connection after a number of writes.
"""
import json
import random
import re
import select
import socket
import socketserver
import struct
import threading
import time

TYPES = {'blob': 0x03, 'bigint': 0x02, 'boolean': 0x04, 'int': 0x09, 'text': 0x0D, 'uuid': 0x0C}


def short_string(value):
    value = value.encode()
    return struct.pack('>H', len(value)) + value


def type_option(kind):
    code, inner = kind
    return struct.pack('>H', code) + (struct.pack('>H', len(inner)) + b''.join(type_option(k) for k in inner) if code == 0x31 else b'')


def parse_type(text):
    text = text.strip().lower()
    if text.startswith('tuple<'):
        return 0x31, [parse_type(part) for part in text[6:-1].split(',')]
    return TYPES[text], []


def decode(kind, raw):
    code, inner = kind
    if code == 0x03:
        return {'blob': raw.hex()}
    if code in (0x02, 0x09):
        return int.from_bytes(raw, 'big', signed=True)
    if code == 0x04:
        return raw != b'\0'
    if code == 0x31:
        values, offset = [], 0
        for element in inner:
            size = struct.unpack('>i', raw[offset:offset + 4])[0]; offset += 4
            values.append(decode(element, raw[offset:offset + size])); offset += size
        if offset != len(raw):
            raise ValueError('tuple length')
        return values
    return raw.decode()


def created(statement):
    """A CREATE TABLE statement's keyspace (or $KEYSPACE), table and Table."""
    match = re.match(r'CREATE TABLE IF NOT EXISTS (\$?\w+)\.(\w+) \((.*?)\)(?: WITH .*)?$', statement, re.S)
    return match.group(1), match.group(2), Table(match.group(3))


def now():
    return time.time_ns() // 1000


class Table:
    def __init__(self, definition):
        columns, key = [], None
        for part in re.split(r',(?![^<(]*[>)])', definition):
            part = part.strip()
            if part.upper().startswith('PRIMARY KEY'):
                key = part[len('PRIMARY KEY'):].strip(' ()').replace(' ', '').split(',')
                continue
            name, kind = part.split(None, 1)
            if kind.upper().endswith(' PRIMARY KEY'):
                kind = kind[:-len(' PRIMARY KEY')]; key = [name]
            columns.append((name, parse_type(kind)))
        self.columns, self.key, self.rows = dict(columns), key, {}

    def write(self, values, timestamp):
        """values: column -> plan value."""
        key = json.dumps([values[name] for name in self.key], sort_keys=True)
        held = self.rows.get(key)
        if held is None or timestamp >= held[0]:
            self.rows[key] = (timestamp, {**(held[1] if held else {}), **values})


class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class FakeCassandra:
    def __init__(self, overload_every=0, fail_after_writes=None, delay=0, reorder=False, unprepare_every=0,
                 timeout_every=0, drop_every=0, stall_at=(), stall_seconds=0):
        """reorder: answer writes out of order, as a node does (held a few at a time, released
        shuffled); unprepare_every: every so many writes, forget every prepared statement, so
        that write and the others in flight are refused as unprepared; timeout_every: refuse
        a write as timed out; drop_every: close the connection instead of answering a write;
        stall_at: before the write after that many, pause every connection stall_seconds."""
        self.timeout_every, self.drop_every = timeout_every, drop_every
        self.stall_at, self.stall_seconds = set(stall_at), stall_seconds
        self.tables, self.prepared, self.lock = {}, {}, threading.Lock()
        self.overload_every, self.fail_after_writes, self.delay = overload_every, fail_after_writes, delay
        self.reorder, self.unprepare_every, self.shuffle = reorder, unprepare_every, random.Random(7)
        self.requests, self.writes, self.batches, self.unprepared, self.ids, self.statements = 0, 0, 0, 0, 0, []
        self.connections = []
        fake = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                connection, buffer, held = self.request, bytearray(), []
                with fake.lock:
                    fake.connections.append(connection)

                def release():
                    fake.shuffle.shuffle(held)
                    connection.sendall(b''.join(held))
                    held.clear()

                def take(size):
                    while len(buffer) < size:
                        if held and not select.select([connection], [], [], 0.02)[0]:
                            release()  # the client waits for these
                        part = connection.recv(1 << 16)
                        if not part:
                            return None
                        buffer.extend(part)
                    data = bytes(buffer[:size]); del buffer[:size]
                    return data

                try:
                    while True:
                        head = take(9)
                        if head is None:
                            return
                        version, flags, number, opcode, length = struct.unpack('>BBhBI', head)
                        body = take(length) if length else b''
                        if body is None:
                            return
                        answer = fake.answer(opcode, body)
                        if opcode in (10, 13) and fake.delay:
                            time.sleep(fake.delay)  # a slow node: the load takes a while
                        if answer is None:
                            connection.shutdown(socket.SHUT_RDWR)
                            return
                        code, payload = answer
                        held.append(struct.pack('>BBhBI', 0x84, 0, number, code, len(payload)) + payload)
                        if not fake.reorder or opcode not in (10, 13) or len(held) >= 4:
                            release()
                except OSError:
                    return

        self.server = Server(('127.0.0.1', 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown(); self.server.server_close()
        for connection in self.connections:
            try:
                connection.close()
            except OSError:
                pass

    # Results
    @staticmethod
    def void():
        return 8, struct.pack('>i', 1)

    @staticmethod
    def rows(columns, rows):
        payload = struct.pack('>iii', 2, 1, len(columns)) + short_string('ks') + short_string('t')
        for name, code in columns:
            payload += short_string(name) + struct.pack('>H', code)
        payload += struct.pack('>i', len(rows))
        for row in rows:
            for value in row:
                payload += struct.pack('>i', -1) if value is None else struct.pack('>i', len(value)) + value
        return 8, payload

    @staticmethod
    def error(code, message):
        return 0, struct.pack('>i', code) + short_string(message)

    def applied(self, value, extra=()):
        return self.rows([('[applied]', 4), *[(name, 13) for name, _ in extra]], [[b'\1' if value else b'\0', *[v.encode() for _, v in extra]]])

    def text_row(self, table, key):
        held = self.tables[table].rows.get(json.dumps([key])) if table in self.tables else None
        return held[1] if held else None

    def answer(self, opcode, body):
        with self.lock:
            self.requests += 1
            if opcode == 1:
                return 2, b''
            if opcode == 7:
                size = struct.unpack('>I', body[:4])[0]
                return self.query(body[4:4 + size].decode())
            if opcode == 9:
                size = struct.unpack('>I', body[:4])[0]
                return self.prepare(body[4:4 + size].decode())
            if opcode in (10, 13):
                if self.fail_after_writes is not None and self.writes >= self.fail_after_writes:
                    return None
                if self.overload_every and self.requests % self.overload_every == 0:
                    return self.error(0x1001, 'Overloaded')
                if self.unprepare_every and self.requests % self.unprepare_every == 0:
                    self.prepared.clear()
                if self.timeout_every and self.requests % self.timeout_every == 0:
                    return self.error(0x1100, 'Operation timed out - received only 0 responses.')
                if self.drop_every and self.requests % self.drop_every == 0:
                    return None
                if any(self.writes >= at for at in self.stall_at):
                    self.stall_at = {at for at in self.stall_at if self.writes < at}
                    time.sleep(self.stall_seconds)  # a node's pause (GC, compaction): every connection waits
                return self.execute(body) if opcode == 10 else self.batch(body)
            return self.error(0x000A, 'Unsupported')

    def query(self, text):
        self.statements.append(text)
        if text.startswith('SELECT table_name FROM system_schema.tables'):
            return self.rows([('table_name', 13)], [[name.encode()] for name in self.tables])
        if text.startswith('CREATE KEYSPACE'):
            return self.void()
        if text.startswith('CREATE TABLE'):
            _, name, table = created(text)
            self.tables.setdefault(name, table)
            return self.void()
        lock = self.text_row('xrpla_import_lock', 0)
        match = re.match(r"INSERT INTO \w+\.xrpla_import_lock \(id,owner\) VALUES \(0,'([^']+)'\) IF NOT EXISTS", text)
        if match:
            if lock:
                return self.applied(False)
            self.tables['xrpla_import_lock'].write({'id': 0, 'owner': match.group(1)}, 0)
            return self.applied(True)
        match = re.match(r"UPDATE \w+\.xrpla_import_lock USING TTL 180 SET owner='([^']+)' WHERE id=0 IF owner='([^']+)'", text)
        if match:
            return self.applied(bool(lock) and lock['owner'] == match.group(2))
        match = re.match(r"DELETE FROM \w+\.xrpla_import_lock WHERE id=0 IF owner='([^']+)'", text)
        if match:
            if lock and lock['owner'] == match.group(1):
                self.tables['xrpla_import_lock'].rows.clear()
                return self.applied(True)
            return self.applied(False)
        if text.startswith('SELECT source,first,last,plan,next_line FROM'):
            status = self.text_row('xrpla_import_status', 0)
            if not status:
                return self.rows([('source', 13), ('first', 2), ('last', 2), ('plan', 13), ('next_line', 2)], [])
            return self.rows([('source', 13), ('first', 2), ('last', 2), ('plan', 13), ('next_line', 2)],
                             [[status['source'].encode(), struct.pack('>q', status['first']), struct.pack('>q', status['last']),
                               status['plan'].encode(), struct.pack('>q', status['next_line'])]])
        if re.match(r'SELECT writetime\(next_line\) FROM \w+\.xrpla_import_status WHERE id=0$', text):
            held = self.tables['xrpla_import_status'].rows.get(json.dumps([0]))
            return self.rows([('writetime(next_line)', 2)], [[struct.pack('>q', held[0])]] if held else [])
        if re.match(r'SELECT base FROM \w+\.xrpla_import_writes WHERE id=0$', text):
            held = self.text_row('xrpla_import_writes', 0)
            return self.rows([('base', 2)], [[struct.pack('>q', held['base'])]] if held else [])
        match = re.match(r'INSERT INTO \w+\.xrpla_import_writes \(id,base\) VALUES \(0,(\d+)\)$', text)
        if match:
            self.tables['xrpla_import_writes'].write({'id': 0, 'base': int(match.group(1))}, now())
            return self.void()
        match = re.match(r'SELECT header FROM \w+\.ledgers WHERE sequence=(\d+)', text)
        if match:
            held = self.tables['ledgers'].rows.get(json.dumps([int(match.group(1))]))
            return self.rows([('header', 3)], [[bytes.fromhex(held[1]['header']['blob'])]] if held else [])
        match = re.match(r"INSERT INTO \w+\.xrpla_import_status \(id,source,first,last,plan,next_line\) VALUES \(0,'(\w+)',(\d+),(\d+),'(\w*)',(\d+)\)", text)
        if match:
            self.tables['xrpla_import_status'].rows.clear()
            self.tables['xrpla_import_status'].write({'id': 0, 'source': match.group(1), 'first': int(match.group(2)), 'last': int(match.group(3)),
                                                      'plan': match.group(4), 'next_line': int(match.group(5))}, now())
            return self.void()
        match = re.match(r"UPDATE \w+\.xrpla_import_status SET next_line=(\d+),plan='(\w+)' WHERE id=0", text)
        if match:
            self.tables['xrpla_import_status'].write({'id': 0, 'next_line': int(match.group(1)), 'plan': match.group(2)}, now())
            return self.void()
        match = re.match(r'BEGIN BATCH INSERT INTO \w+\.ledger_range \(is_latest,sequence\) VALUES \(false,(\d+)\); '
                         r'INSERT INTO \w+\.ledger_range \(is_latest,sequence\) VALUES \(true,(\d+)\); '
                         r'UPDATE \w+\.xrpla_import_status SET last=(\d+) WHERE id=0; APPLY BATCH', text)
        if match:
            self.tables['ledger_range'].write({'is_latest': False, 'sequence': int(match.group(1))}, 1 << 62)
            self.tables['ledger_range'].write({'is_latest': True, 'sequence': int(match.group(2))}, 1 << 62)
            self.tables['xrpla_import_status'].write({'id': 0, 'last': int(match.group(3))}, now())
            return self.void()
        return self.error(0x2000, 'Unexpected statement: ' + text[:200])

    def prepare(self, text):
        match = re.fullmatch(r'INSERT INTO (\w+)\.(\w+) \(([\w,]+)\) VALUES \(([?,]+)\) USING TIMESTAMP \?', text)
        if not match or match.group(2) not in self.tables:
            return self.error(0x2200, 'Cannot prepare ' + text)
        table, columns = self.tables[match.group(2)], match.group(3).split(',')
        if match.group(4).count('?') != len(columns):
            return self.error(0x2200, 'Bind markers')
        self.ids += 1
        identifier = struct.pack('>I', self.ids) + b'id'
        self.prepared[identifier] = match.group(2), columns
        partition = [columns.index(table.key[0])]
        payload = struct.pack('>iH', 4, len(identifier)) + identifier
        payload += struct.pack('>iii', 1, len(columns) + 1, len(partition)) + b''.join(struct.pack('>H', i) for i in partition)
        payload += short_string(match.group(1)) + short_string(match.group(2))
        for name in columns:
            payload += short_string(name) + type_option(table.columns[name])
        payload += short_string('[timestamp]') + type_option((0x02, []))
        payload += struct.pack('>ii', 4, 0)
        return 8, payload

    def bound(self, body, offset, identifier):
        name, columns = self.prepared[identifier]
        table = self.tables[name]
        count = struct.unpack('>H', body[offset:offset + 2])[0]; offset += 2
        if count != len(columns) + 1:
            raise ValueError('values')
        raw = []
        for _ in range(count):
            size = struct.unpack('>i', body[offset:offset + 4])[0]; offset += 4
            raw.append(body[offset:offset + size]); offset += size
        values = {name: decode(table.columns[name], value) for name, value in zip(columns, raw)}
        timestamp = int.from_bytes(raw[-1], 'big', signed=True)
        table.write(values, timestamp)
        self.writes += 1
        return offset

    def execute(self, body):
        size = struct.unpack('>H', body[:2])[0]
        identifier = body[2:2 + size]
        if identifier not in self.prepared:
            self.unprepared += 1
            return self.error(0x2500, 'Unprepared')
        consistency, flags = struct.unpack('>HB', body[2 + size:5 + size])
        if not flags & 1:
            return self.error(0x000A, 'Values expected')
        self.bound(body, 5 + size, identifier)
        return self.void()

    def batch(self, body):
        kind, count = struct.unpack('>BH', body[:3])
        if kind != 1:
            return self.error(0x2200, 'Only unlogged batches of rows are expected')
        offset = 3
        for _ in range(count):
            if body[offset] != 1:
                return self.error(0x2200, 'Prepared statements expected')
            size = struct.unpack('>H', body[offset + 1:offset + 3])[0]
            identifier = body[offset + 3:offset + 3 + size]
            if identifier not in self.prepared:
                self.unprepared += 1
                return self.error(0x2500, 'Unprepared')
            offset = self.bound(body, offset + 3 + size, identifier)
        self.batches += 1
        return self.void()

    def contents(self, table):
        """The table's rows, each as a JSON text of its values, sorted."""
        return sorted(json.dumps(values, sort_keys=True) for _, values in self.tables[table].rows.values())


def expected(plan_lines):
    """What a plan's rows leave in each table written in order: per primary key, the last."""
    tables, result = {}, {}
    for line in plan_lines[1:]:
        record = json.loads(line)
        if 'schema' in record:
            _, name, table = created(record['schema'])
            tables[name] = table
            continue
        table = tables[record['table']]
        result.setdefault(record['table'], {})[json.dumps([record['values'][k] for k in table.key], sort_keys=True)] = record['values']
    return {name: sorted(json.dumps(values, sort_keys=True) for values in rows.values()) for name, rows in result.items()}
