#!/usr/bin/env python3
"""Restartable Clio 2.7.1 import using Cassandra native protocol v4.

No third-party Python packages. Ledger availability is published only after every
row is acknowledged. Rows are idempotent and progress belongs to the plan's bytes.

The rows are written by several processes (--workers), each with its own connection and
many requests in flight, as prepared statements: the plan is cut into blocks of lines
dealt to the workers in turn, and in a block the rows of one table and partition go
together as an unlogged batch (a single-partition batch is one mutation). Rows land in
any order, so each carries its plan line as its write timestamp (`USING TIMESTAMP`):
where two rows share a primary key (a pseudo-transaction repeated in a later ledger), the
later line wins, as when they were sent in order, and a row sent again is the same
write. The timestamps start 2^40 microseconds (about 13 days) before the import started,
so they stay below any write Clio makes after it, and above those of an earlier run of
this loader (recorded in xrpla_import_writes; a later clock is required). A keyspace an
earlier xrpla loaded with the server's timestamps is refused while its writes are newer
than these: rows loaded now would not replace them.

A busy node does not end the load: writes refused as overloaded or timed out are sent
again after a backoff, a connection that times out (--timeout, default 120 s) or drops is
opened again and its unacknowledged requests sent again, and either halves the worker's
requests in flight, which then grow back slowly. Only --patience (default 600 s) without
an answer fails it.

Progress is the first line not yet acknowledged by every worker, kept with the SHA-256
of the plan's bytes before it: a resumed import skips those lines only when the plan it
is given starts with the same bytes (the same plan, or the same plan written again), and
otherwise sends every row again. With --follow the plan is read as xrpla writes it
(PLAN.part, renamed to PLAN once complete), so loading overlaps its writing; the range
is published only once the complete plan is acknowledged.
"""
import argparse
import binascii
import hashlib
import json
import multiprocessing
import multiprocessing.connection
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import struct
import sys
import time
import uuid

LOCAL_QUORUM = 6
# Unavailable, overloaded, bootstrapping, write and read timeouts, write failure: worth
# sending again (the rows are the same writes), for as long as --patience allows.
RETRYABLE = (0x1000, 0x1001, 0x1002, 0x1100, 0x1200, 0x1500)
# A socket that times out or drops: the connection is opened again.
TRANSIENT = (OSError, ConnectionError)
UNPREPARED = 0x2500
BATCH_STATEMENTS, BATCH_BYTES = 32, 64 * 1024
FORMAT = 'xrpla-clio-2.7.1-v1'


def short_string(value):
    value = value.encode()
    return struct.pack('>H', len(value)) + value


class Reader:
    def __init__(self, data):
        self.data, self.offset = data, 0

    def take(self, size):
        if size < 0 or self.offset + size > len(self.data):
            raise ValueError('Truncated Cassandra response')
        value = self.data[self.offset:self.offset + size]
        self.offset += size
        return value

    def integer(self):
        return struct.unpack('>i', self.take(4))[0]

    def short(self):
        return struct.unpack('>H', self.take(2))[0]

    def string(self):
        return self.take(self.short()).decode()

    def bytes(self):
        size = self.integer()
        return None if size == -1 else self.take(size)

    def data_type(self):
        """A column type: its code and its element types (a tuple's, a collection's)."""
        code, inner = self.short(), []
        if code == 0:
            self.string()
        elif code in (0x20, 0x22):
            inner.append(self.data_type())
        elif code == 0x21:
            inner.extend((self.data_type(), self.data_type()))
        elif code == 0x30:
            self.string(); self.string()
            for _ in range(self.short()):
                self.string(); inner.append(self.data_type())
        elif code == 0x31:
            inner.extend(self.data_type() for _ in range(self.short()))
        return code, inner


class CassandraError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(f'Cassandra error {code:#x}: {message}')
        self.code = code


def error_of(body):
    error = Reader(body)
    return CassandraError(error.integer(), error.string())


class Connection:
    def __init__(self, host, port=9042, tls=False, username=None, password=None, timeout=120):
        self.socket = socket.create_connection((host, port), timeout=min(timeout, 30))
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if tls:
            self.socket = ssl.create_default_context().wrap_socket(self.socket, server_hostname=host)
        self.socket.settimeout(timeout)
        self.input = self.socket.makefile('rb', buffering=1 << 16)
        self.queued, self.queued_bytes = [], 0
        opcode, body = self.exchange(1, struct.pack('>H', 1) + short_string('CQL_VERSION') + short_string('3.0.0'))
        if opcode == 3:
            if not username or password is None:
                raise RuntimeError('Cassandra requires credentials; set --username and the password environment variable')
            token = b'\0' + username.encode() + b'\0' + password.encode()
            opcode, body = self.exchange(15, struct.pack('>i', len(token)) + token)
            if opcode != 16:
                raise RuntimeError('Unsupported Cassandra authentication exchange')
        elif opcode != 2:
            raise RuntimeError('Cassandra did not return READY')

    def receive(self, size):
        data = self.input.read(size)
        if len(data) != size:
            raise ConnectionError('Cassandra closed the connection')
        return data

    def send(self, stream, opcode, body):
        """Queues a request; requests go out together, before the next response is read."""
        self.queued.append(struct.pack('>BBhBI', 4, 0, stream, opcode, len(body)) + body)
        self.queued_bytes += len(body) + 9
        if self.queued_bytes >= 1 << 16:
            self.flush()

    def flush(self):
        if self.queued:
            self.socket.sendall(b''.join(self.queued))
            self.queued, self.queued_bytes = [], 0

    def read_frame(self):
        """The next response on any stream: (stream, opcode, body); an error is opcode 0."""
        self.flush()
        version, flags, stream, opcode, length = struct.unpack('>BBhBI', self.receive(9))
        if version != 0x84 or length > 256 * 1024 * 1024 or flags & ~0x08:
            raise RuntimeError('Unsupported Cassandra response framing')
        reader = Reader(self.receive(length))
        if flags & 0x08:
            for _ in range(reader.short()):
                reader.string()  # Server warnings do not change result framing.
        return stream, opcode, reader.data[reader.offset:]

    def exchange(self, opcode, body, other=None):
        """A request on stream 0, answered; responses to other streams on the way go to `other`."""
        self.send(0, opcode, body)
        while True:
            stream, code, answer = self.read_frame()
            if stream == 0:
                break
            if other is None:
                raise RuntimeError('Unsupported Cassandra response framing')
            other(stream, code, answer)
        if code == 0:
            raise error_of(answer)
        return code, answer

    def query(self, statement, consistency=LOCAL_QUORUM):
        value = statement.encode()
        for attempt in range(5):
            try:
                opcode, body = self.exchange(7, struct.pack('>I', len(value)) + value + struct.pack('>HB', consistency, 0))
                if opcode != 8:
                    raise RuntimeError('Unexpected Cassandra query response')
                break
            except CassandraError as error:
                if error.code not in RETRYABLE or attempt == 4:
                    raise
                time.sleep(min(2 ** attempt, 8))
        reader = Reader(body)
        kind = reader.integer()
        if kind != 2:
            return []
        flags, count = reader.integer(), reader.integer()
        if flags & 2:
            raise RuntimeError('Unexpected paginated metadata query')
        if flags & 4:
            raise RuntimeError('Missing Cassandra column metadata')
        if flags & 1:
            reader.string(); reader.string()
        columns = []
        for _ in range(count):
            if not flags & 1:
                reader.string(); reader.string()
            columns.append((reader.string(), reader.data_type()[0]))
        rows = []
        for _ in range(reader.integer()):
            row = {}
            for name, code in columns:
                raw = reader.bytes()
                if raw is None:
                    row[name] = None
                elif code in (1, 10, 13):
                    row[name] = raw.decode()
                elif code in (2, 7, 9, 11):
                    row[name] = int.from_bytes(raw, 'big', signed=True)
                elif code == 4:
                    row[name] = raw != b'\0'
                else:
                    row[name] = raw.hex()
            rows.append(row)
        return rows

    def close(self):
        self.input.close()
        self.socket.close()


def connect(args):
    return Connection(args.host, args.port, args.tls, args.username, os.getenv(args.password_env), args.timeout)


class Recoveries:
    """Says, at most once a minute, that connections were opened again and why."""

    def __init__(self, who):
        self.who, self.count, self.said = who, 0, 0.0

    def __call__(self, error):
        self.count += 1
        if time.monotonic() - self.said >= 60:
            print(f'Clio import {self.who}: {error or type(error).__name__}; connecting again ({self.count} so far)', file=sys.stderr)
            self.said = time.monotonic()


class Session:
    """The coordinator's connection: a query that times out or loses its connection is sent
    again on a new one (its statements are the lease, the status and the range: the same
    statement again has the same effect), backing off, for up to `patience` seconds."""

    def __init__(self, args):
        self.args, self.db, self.recovered = args, connect(args), Recoveries('coordinator')

    def query(self, statement, consistency=LOCAL_QUORUM):
        started, delay = time.monotonic(), 0.5
        while True:
            try:
                return self.db.query(statement, consistency)
            except TRANSIENT as error:
                if time.monotonic() - started > self.args.patience:
                    raise
                self.recovered(error)
                self.close()
                time.sleep(delay)
                delay = min(delay * 2, 30)
                try:
                    self.db = connect(self.args)
                except TRANSIENT:
                    pass  # the next query fails on the closed connection, and tries again

    def close(self):
        try:
            self.db.close()
        except OSError:
            pass


def prepared(body):
    """A PREPARE result: the statement's ID, its bind markers' types and partition key markers."""
    reader = Reader(body)
    if reader.integer() != 4:
        raise RuntimeError('Unexpected Cassandra prepare response')
    statement = reader.take(reader.short())
    flags, count, keys = reader.integer(), reader.integer(), reader.integer()
    partition = [reader.short() for _ in range(keys)]
    if flags & 1:
        reader.string(); reader.string()
    types = []
    for _ in range(count):
        if not flags & 1:
            reader.string(); reader.string()
        reader.string(); types.append(reader.data_type())
    return statement, types, partition


def encode(kind, value):
    """A plan value as a CQL value of the column's type (blob, bigint, int, boolean, text, tuple)."""
    code, inner = kind
    if code == 0x03:
        if not isinstance(value, dict) or set(value) != {'blob'} or not isinstance(value['blob'], str):
            raise ValueError('Invalid CQL blob')
        try:
            return binascii.unhexlify(value['blob'])
        except binascii.Error:
            raise ValueError('Invalid CQL blob') from None
    if code == 0x04:
        if not isinstance(value, bool):
            raise ValueError('Invalid CQL boolean')
        return b'\1' if value else b'\0'
    if code in (0x02, 0x09):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError('Invalid CQL integer')
        try:
            return struct.pack('>q' if code == 0x02 else '>i', value)
        except struct.error:
            raise ValueError('CQL integer out of range') from None
    if code in (0x0A, 0x0D):
        if not isinstance(value, str):
            raise ValueError('Invalid CQL text')
        return value.encode()
    if code == 0x31:
        if not isinstance(value, list) or len(value) != len(inner):
            raise ValueError('Invalid CQL tuple')
        return b''.join(struct.pack('>i', len(part)) + part for part in (encode(k, v) for k, v in zip(inner, value)))
    raise ValueError('Unsupported CQL value')


def bound(row):
    return struct.pack('>H', len(row)) + b''.join(struct.pack('>i', len(value)) + value for value in row)


def identifier(name):
    if not isinstance(name, str) or not re.fullmatch('[a-z][a-z0-9_]*', name):
        raise ValueError('Invalid CQL identifier')
    return name


class PlanLines:
    """The plan's lines (bytes, newline included), its identity line first. Following a plan
    being written (`follow`), the reader waits at its end until xrpla renames the file it
    reads to the plan's name (it does so once the plan is complete and closed), calling
    `idle` while it waits; a writer that stops first (the process `writer` is no longer this
    one's parent) fails the read, as does a plan that stops growing for `stall` seconds (a
    writer that hangs)."""
    stall = 1800

    def __init__(self, path, follow=False, writer=None, idle=None):
        self.path, self.follow, self.writer, self.idle = Path(path), follow, writer, idle
        try:
            self.stream = open(f'{path}.part' if follow else path, 'rb')
        except FileNotFoundError:
            if not follow:
                raise
            self.stream = open(path, 'rb')  # complete already

    def written(self):
        """When the writer last wrote: the plan, or a file a range's rows wait in before they
        join it (PLAN.part.N: one key's versions can take long to read)."""
        times = [os.fstat(self.stream.fileno()).st_mtime]
        for spill in self.path.parent.glob(f'{self.path.name}.part.*'):
            try:
                times.append(spill.stat().st_mtime)
            except FileNotFoundError:
                pass
        return max(times)

    def complete(self):
        try:
            return os.path.samestat(os.fstat(self.stream.fileno()), os.stat(self.path))
        except FileNotFoundError:
            return False

    def __iter__(self):
        partial, grew = b'', time.monotonic()
        while self.follow:
            line = self.stream.readline()
            if line:
                grew = time.monotonic()
            if line.endswith(b'\n'):
                yield partial + line
                partial = b''
                continue
            partial += line
            if self.complete():
                break  # renamed once closed: what is left to read is the rest of the plan
            if self.writer is not None and os.getppid() != self.writer:
                raise RuntimeError('The plan writer stopped before the plan was complete')
            if time.monotonic() - grew > self.stall and time.time() - self.written() > self.stall:
                raise RuntimeError(f'The plan has not grown for {self.stall} seconds; its writer seems stuck')
            if self.idle:
                self.idle()
            time.sleep(0.2)
        for line in self.stream:
            line, partial = partial + line, b''
            if not line.endswith(b'\n'):
                partial = line
                break
            yield line
        if partial:
            raise ValueError('The import plan ends in a partial line')

    def close(self):
        self.stream.close()


class Writer:
    """A worker's connection: prepared inserts, up to `window` requests outstanding, each
    counted against the plan block whose rows it carries.

    Nothing a busy node does ends the load while it still makes progress: a request refused
    as overloaded or timed out is sent again after a backoff, and a connection that times out
    or drops is opened again and every unacknowledged request sent again on it (the same
    rows and timestamps: the same writes). Either halves the window, which then grows back by
    one for each window's worth of answers, so the load settles at what the node sustains.
    Only `patience` seconds without an answer while requests wait fail the worker."""

    def __init__(self, args, index, base):
        self.args, self.namespace, self.base = args, args.keyspace, base
        self.db = connect(args)
        self.statements = {}
        self.free = list(range(args.in_flight, 0, -1))
        self.window, self.answered = args.in_flight, 0
        self.pending = {}
        self.outstanding = {}  # a block's first line -> its requests not yet acknowledged, in block order
        self.unprepared = []  # streams whose statement the server no longer knew, to send again
        self.later = []  # (when, stream): refused as busy, to send again then
        self.broken = None  # the error that broke the connection, until it is opened again
        self.progressed = time.monotonic()
        self.recovered = Recoveries(f'worker {index}')
        self.rows = 0

    def statement(self, key):
        if key not in self.statements:
            table, columns = key
            text = (f'INSERT INTO {self.namespace}.{identifier(table)} ({",".join(identifier(c) for c in columns)}) '
                    f'VALUES ({",".join("?" * len(columns))}) USING TIMESTAMP ?').encode()
            opcode, body = self.db.exchange(9, struct.pack('>I', len(text)) + text, self.answer)
            if opcode != 8:
                raise RuntimeError('Unexpected Cassandra prepare response')
            statement, types, partition = prepared(body)
            if len(types) != len(columns) + 1 or any(i >= len(columns) for i in partition):
                raise RuntimeError('Unexpected Cassandra prepared statement')
            self.statements[key] = statement, types, partition
        return self.statements[key]

    def prepare(self, key):
        while True:
            try:
                return self.statement(key)
            except TRANSIENT as error:
                self.reconnect(error)

    def block(self, first, lines):
        """Sends one block's rows; `lines` are (plan line number, line) pairs."""
        groups = {}
        for number, line in lines:
            record = json.loads(line)
            if 'schema' in record:
                raise ValueError('The import plan has a schema after its rows')
            if set(record) != {'table', 'values'} or not isinstance(record['values'], dict):
                raise ValueError('Invalid import plan row')
            key = record['table'], tuple(record['values'])
            _, types, partition = self.prepare(key)
            row = [encode(t, v) for t, v in zip(types, [*record['values'].values(), self.base + number])]
            groups.setdefault((key, tuple(row[i] for i in partition)), []).append(row)
            self.rows += 1
        # Counted from 1 while its requests go out: answers to the first ones (they come in any
        # order) cannot empty it, and a block enters `outstanding` once, in order (frontier()).
        self.outstanding[first] = 1
        for (key, _), rows in groups.items():
            chunk, size = [], 0
            for row in rows:
                length = sum(len(value) + 4 for value in row)
                if chunk and (len(chunk) == BATCH_STATEMENTS or size + length > BATCH_BYTES):
                    self.submit(first, key, chunk)
                    chunk, size = [], 0
                chunk.append(row); size += length
            self.submit(first, key, chunk)
        self.flush()
        self.acknowledged(first)

    def acknowledged(self, first):
        self.outstanding[first] -= 1
        if not self.outstanding[first]:
            del self.outstanding[first]

    def send(self, stream):
        """Sends a pending request with its statement's current ID (on a broken connection,
        it is sent once the connection is opened again)."""
        if self.broken:
            return
        request = self.pending[stream]
        if request[1] not in self.statements:
            self.statement(request[1])
        statement = self.statements[request[1]][0]
        request[4] = statement
        identifier, rows = struct.pack('>H', len(statement)) + statement, request[2]
        try:
            if len(rows) == 1:  # EXECUTE: values, result metadata skipped
                self.db.send(stream, 10, identifier + struct.pack('>HB', LOCAL_QUORUM, 0x03) + bound(rows[0]))
            else:  # an unlogged BATCH of the prepared statement
                self.db.send(stream, 13, struct.pack('>BH', 1, len(rows)) + b''.join(b'\1' + identifier + bound(row) for row in rows) + struct.pack('>HB', LOCAL_QUORUM, 0))
        except TRANSIENT as error:
            self.broken = error

    def flush(self):
        try:
            if not self.broken:
                self.db.flush()
        except TRANSIENT as error:
            self.broken = error

    def submit(self, first, key, rows):
        while len(self.pending) >= self.window:
            self.receive()
        if not self.pending:
            self.progressed = time.monotonic()  # idle until now: patience counts from here
        stream = self.free.pop()
        self.pending[stream] = [first, key, rows, 0, None]
        self.outstanding[first] += 1
        try:
            self.send(stream)
        except TRANSIENT as error:
            self.broken = error

    def receive(self):
        """Sends again what is due (unprepared, refused as busy), then takes the next answer;
        a broken connection is opened again first."""
        try:
            if self.broken:
                raise self.broken
            self.prepare_again()
            now = time.monotonic()
            for when, stream in [x for x in self.later if x[0] <= now]:
                self.later.remove((when, stream))
                self.send(stream)
            if self.broken:
                raise self.broken
            if not self.pending:
                return
            if len(self.pending) == len(self.later):  # all waiting to be sent again
                time.sleep(max(0.0, min(when for when, _ in self.later) - now))
                return
            self.answer(*self.db.read_frame())
        except TRANSIENT as error:
            self.reconnect(error)

    def prepare_again(self):
        while self.unprepared:
            stream = self.unprepared.pop()
            request = self.pending[stream]
            key, used = request[1], request[4]
            # Prepared again once, however many of its requests the server refused meanwhile
            # (answers read while preparing only queue more here).
            if key in self.statements and self.statements[key][0] == used:
                del self.statements[key]
            self.statement(key)
            self.send(stream)

    def patient(self, error, since):
        if time.monotonic() - since > self.args.patience:
            raise RuntimeError(f'No answer from Cassandra for {self.args.patience} seconds: {error}')

    def slower(self):
        self.window, self.answered = max(1, self.window // 2), 0

    def reconnect(self, error):
        """Opens the connection again, backing off, and sends every unacknowledged request."""
        delay, since = 0.5, self.progressed if self.pending else time.monotonic()
        while True:
            self.patient(error, since)
            self.recovered(error)
            self.slower()
            try:
                self.db.close()
            except OSError:
                pass
            time.sleep(delay)
            delay = min(delay * 2, 30)
            try:
                self.db, self.broken = connect(self.args), None
                self.statements.clear()
                self.unprepared.clear(); self.later.clear()
                for stream in sorted(self.pending):
                    if stream in self.pending:  # (answers read while preparing may take some)
                        self.pending[stream][3] = 0
                        self.send(stream)
                self.flush()
                if not self.broken:
                    return
                error = self.broken
            except TRANSIENT as failure:
                error = failure

    def answer(self, stream, opcode, body):
        request = self.pending.get(stream)
        if request is None or stream in self.unprepared or any(s == stream for _, s in self.later):
            raise RuntimeError('Unexpected Cassandra response stream')
        first, attempts = request[0], request[3]
        if opcode == 0:
            error = error_of(body)
            if error.code == UNPREPARED and attempts < 8:
                self.unprepared.append(stream)
            elif error.code in RETRYABLE:
                self.patient(error, self.progressed)
                if error.code in (0x1001, 0x1100, 0x1200):  # overloaded or timed out: send less at once
                    self.slower()
                self.later.append((time.monotonic() + min(0.1 * 2 ** attempts, 10), stream))
            else:
                raise error
            request[3] += 1
            return
        if opcode != 8:
            raise RuntimeError('Unexpected Cassandra write response')
        del self.pending[stream]
        self.free.append(stream)
        self.acknowledged(first)
        self.progressed = time.monotonic()
        self.answered += 1
        if self.answered >= self.window and self.window < self.args.in_flight:
            self.window, self.answered = self.window + 1, 0

    def frontier(self, reading):
        """No line of this worker's before it is unacknowledged (`reading`: the block it reads)."""
        return min(next(iter(self.outstanding), reading), reading)

    def drain(self):
        while self.pending:
            self.receive()


def work(index, count, args, start, base, report):
    """A worker: the plan's blocks from line `start`, every `count`-th from block `index`.
    Reports, about every second, the line no row of its before is unacknowledged."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    try:
        coordinator = os.getppid()
        writer = Writer(args, index, base)
        block, current, reported, number, rows = [], start, 0.0, -1, False

        def progress():
            nonlocal reported
            if time.monotonic() - reported >= 1:
                if os.getppid() != coordinator:
                    raise RuntimeError('The import stopped')
                report.send(('progress', writer.frontier(current), writer.rows))
                reported = time.monotonic()

        def idle():
            """Waiting for the plan's writer: what was sent is acknowledged meanwhile."""
            writer.drain()
            progress()

        for number, line in enumerate(PlanLines(args.plan, args.follow, coordinator, idle)):
            # The coordinator ran the schemas, which lead the plan.
            if number and line.startswith(b'{"schema"'):
                if rows:
                    raise ValueError('The import plan has a schema after its rows')
                continue
            rows = rows or number > 0
            if number < start:
                continue
            first = start + (number - start) // args.block_lines * args.block_lines
            if first != current:
                if block:
                    writer.block(current, block)
                    block = []
                current = first
                progress()
            if (first - start) // args.block_lines % count == index:
                block.append((number, line))
        if block:
            writer.block(current, block)
        writer.drain()
        report.send(('done', max(number + 1, start), writer.rows))
    except BaseException as error:
        try:
            report.send(('error', str(error) or type(error).__name__))
        except OSError:
            pass  # the coordinator has stopped
        sys.exit(1)


def load(args):
    if not re.fullmatch('[a-z][a-z0-9_]{0,47}', args.keyspace):
        raise ValueError('Keyspace must be a lowercase CQL identifier')
    path, ns = Path(args.plan), args.keyspace
    writer = os.getppid() if args.follow else None
    lines = PlanLines(path, args.follow, writer)
    header = json.loads(next(iter(lines)))
    lines.close()
    if header.get('format') != FORMAT:
        raise ValueError('Unsupported import plan format')
    if not re.fullmatch('[0-9a-f]{64}', header.get('first_hash', '')) or not isinstance(header.get('first'), int) or not isinstance(header.get('last'), int) or not 1 <= header['first'] <= header['last'] <= 0xffffffff:
        raise ValueError('Invalid import plan identity')
    db = Session(args)
    lease, renewed, workers = None, 0.0, []

    def keep(now=False):
        """Renews the keyspace lease (it lasts 180 seconds) every 30 seconds of the import."""
        nonlocal renewed
        if lease is None or (not now and time.monotonic() - renewed < 30):
            return
        applied = db.query(f"UPDATE {ns}.xrpla_import_lock USING TTL 180 SET owner='{lease}' WHERE id=0 IF owner='{lease}'")
        if not applied or not applied[0].get('[applied]'):
            raise RuntimeError('Import lease was lost; ledger range was not advanced')
        renewed = time.monotonic()

    class Position:
        """The plan read in order: `line` lines read, their bytes hashed."""

        def __init__(self):
            self.lines = PlanLines(path, args.follow, writer, keep)
            self.iterator, self.line, self.digest = iter(self.lines), 0, hashlib.sha256()

        def advance(self, line=None):
            """To `line` (None: the end); False if the plan ends before it."""
            while line is None or self.line < line:
                try:
                    self.digest.update(next(self.iterator))
                except StopIteration:
                    return line is None
                self.line += 1
                if not self.line % 65536:
                    keep()
            return True

    try:
        tables = {x['table_name'] for x in db.query(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{ns}'", 1)}
        if tables and 'xrpla_import_status' not in tables:
            raise RuntimeError('Destination keyspace is not an xrpla import; choose an empty keyspace')
        db.query(f"CREATE KEYSPACE IF NOT EXISTS {ns} WITH replication={{'class':'SimpleStrategy','replication_factor':{args.replication_factor}}}")
        db.query(f'CREATE TABLE IF NOT EXISTS {ns}.xrpla_import_status (id int PRIMARY KEY, source text, first bigint, last bigint, plan text, next_line bigint)')
        db.query(f'CREATE TABLE IF NOT EXISTS {ns}.xrpla_import_lock (id int PRIMARY KEY, owner text)')
        token = str(uuid.uuid4())
        claimed = db.query(f"INSERT INTO {ns}.xrpla_import_lock (id,owner) VALUES (0,'{token}') IF NOT EXISTS USING TTL 180")
        if not claimed or not (claimed[0].get('[applied]') or claimed[0].get('owner') == token):  # (or claimed by a try that timed out)
            raise RuntimeError('Another importer owns this keyspace; after a crash its lease expires within 180 seconds')
        lease, renewed = token, time.monotonic()
        metadata = db.query(f'SELECT source,first,last,plan,next_line FROM {ns}.xrpla_import_status WHERE id=0')
        old = metadata[0] if metadata else {}
        if old and (old['source'] != header['first_hash'] or old['first'] != header['first']):
            raise RuntimeError('Destination contains different ledger history')
        if old.get('last', 0) > header['last']:
            raise RuntimeError('Import would shrink the published ledger range')
        if old.get('last', 0) >= header['first']:
            prior = db.query(f'SELECT header FROM {ns}.ledgers WHERE sequence={old["last"]}')
            if not prior:
                raise RuntimeError('Published Clio range is missing its last header')
            prior_hash = hashlib.sha512(b'LWR\0' + bytes.fromhex(prior[0]['header'])[:118]).digest()[:32].hex()
            matches, lines = False, PlanLines(path, args.follow, writer, keep)
            for number, line in enumerate(lines):
                if b'"ledger_hashes"' in line:
                    row = json.loads(line)
                    if row.get('table') == 'ledger_hashes' and row['values']['sequence'] == old['last']:
                        matches = row['values']['hash']['blob'] == prior_hash
                        break
                if not number % 65536:
                    keep()
            lines.close()
            if not matches:
                raise RuntimeError('Import would replace previously published history')
        # The rows' timestamps start here (see the docstring), recorded so the next import can
        # check its own come later. A keyspace loaded by a loader that wrote with the server's
        # timestamps (xrpla before this one: the status table, but no record) holds rows newer
        # than these for about 13 days, which rows written now would not replace.
        base = time.time_ns() // 1000 - (1 << 40)
        db.query(f'CREATE TABLE IF NOT EXISTS {ns}.xrpla_import_writes (id int PRIMARY KEY, base bigint)')
        earlier = db.query(f'SELECT base FROM {ns}.xrpla_import_writes WHERE id=0')
        if earlier and earlier[0]['base'] >= base:
            raise RuntimeError("An earlier import's write timestamps are not older than this one's; check the clock")
        if not earlier and old:
            written = db.query(f'SELECT writetime(next_line) FROM {ns}.xrpla_import_status WHERE id=0')
            newest = next(iter(written[0].values())) if written else None
            if newest is None or newest >= base:
                raise RuntimeError('This keyspace was loaded by an earlier xrpla, whose rows carry the server\'s write timestamps: '
                                   'rows loaded now would not replace them. Load into a new keyspace.')
        db.query(f'INSERT INTO {ns}.xrpla_import_writes (id,base) VALUES (0,{base})')
        # Lines acknowledged before (the status's next_line) are skipped if this plan starts
        # with the same bytes.
        position = Position()
        start = old.get('next_line') or 1
        if not (old.get('plan') and position.advance(start) and position.digest.hexdigest() == old['plan']):
            position.lines.close()
            position, start = Position(), 1
            position.advance(1)
        # The schemas lead the plan; workers send its rows.
        schemas = PlanLines(path, args.follow, writer, keep)
        for number, line in enumerate(schemas):
            record = json.loads(line) if number else {}
            if number and 'schema' not in record:
                break
            if number:
                db.query(record['schema'].replace('$KEYSPACE', ns))
        schemas.close()
        old_last = old.get('last', header['first'] - 1)
        db.query(f"INSERT INTO {ns}.xrpla_import_status (id,source,first,last,plan,next_line) VALUES (0,'{header['first_hash']}',{header['first']},{old_last},'{position.digest.hexdigest()}',{start})")
        context = multiprocessing.get_context('fork')
        reports = []
        for index in range(args.workers):
            receive, send = context.Pipe(duplex=False)
            process = context.Process(target=work, args=(index, args.workers, args, start, base, send), daemon=True)
            process.start(); send.close()
            workers.append(process); reports.append(receive)
        frontiers, rows, done = [start] * args.workers, [0] * args.workers, [False] * args.workers
        committed, said = time.monotonic(), time.monotonic()

        def commit():
            """Records progress: the first line some worker has not acknowledged."""
            position.advance(min(frontiers))
            keep(True)
            db.query(f"UPDATE {ns}.xrpla_import_status SET next_line={position.line},plan='{position.digest.hexdigest()}' WHERE id=0")

        while not all(done):
            waiting = [reports[i] for i in range(args.workers) if not done[i]]
            for ready in multiprocessing.connection.wait(waiting + [workers[i].sentinel for i in range(args.workers) if not done[i]], timeout=1):
                if ready not in reports:
                    continue
                index = reports.index(ready)
                try:
                    kind, *value = ready.recv()
                except EOFError:
                    kind, value = 'error', ['A Clio import worker stopped']
                if kind == 'error':
                    raise RuntimeError(value[0])
                frontiers[index], rows[index] = value
                done[index] = kind == 'done'
            for index, process in enumerate(workers):
                if not done[index] and not process.is_alive() and not reports[index].poll():
                    raise RuntimeError('A Clio import worker stopped')
            if writer is not None and os.getppid() != writer:
                raise RuntimeError('The plan writer stopped before the plan was complete')
            keep()
            if min(frontiers) > position.line and time.monotonic() - committed >= 1:
                commit(); committed = time.monotonic()
            if time.monotonic() - said >= 10:
                print(f'Clio import: {min(frontiers)} plan lines acknowledged, {sum(rows)} rows sent', file=sys.stderr)
                said = time.monotonic()
        for process in workers:
            process.join()
        if not position.advance():
            raise ValueError('The import plan ends in a partial line')
        if position.line != min(frontiers):
            raise RuntimeError('The Clio import workers did not read the whole plan')
        commit()
        # A logged batch publishes both range endpoints and the completion marker.
        db.query(f'BEGIN BATCH INSERT INTO {ns}.ledger_range (is_latest,sequence) VALUES (false,{header["first"]}); '
                 f'INSERT INTO {ns}.ledger_range (is_latest,sequence) VALUES (true,{header["last"]}); '
                 f'UPDATE {ns}.xrpla_import_status SET last={header["last"]} WHERE id=0; APPLY BATCH')
        return {'keyspace': ns, 'first': header['first'], 'last': header['last'], 'rows_sent': sum(rows), 'plan_lines': position.line,
                'resumed_from_line': start, 'workers': args.workers, 'plan_sha256': position.digest.hexdigest(), 'clio_version': '2.7.1', 'published': True}
    finally:
        for process in workers:
            if process.is_alive():
                process.terminate()
            process.join()
        if lease is not None:
            try:
                db.db.query(f"DELETE FROM {ns}.xrpla_import_lock WHERE id=0 IF owner='{lease}'")
            except Exception:
                pass  # Crash recovery is bounded by the lease TTL.
        db.close()


def positive(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError('must be at least 1')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--follow', action='store_true', help='load the plan as xrpla writes it (PLAN.part until complete)')
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, default=9042)
    parser.add_argument('--keyspace', default='xrpla')
    parser.add_argument('--replication-factor', type=int, choices=range(1, 6), default=1)
    parser.add_argument('--tls', action='store_true')
    parser.add_argument('--username')
    parser.add_argument('--password-env', default='XRPLA_CQL_PASSWORD')
    parser.add_argument('--workers', type=positive, default=min(8, os.cpu_count() or 1), help='processes, each with a connection (default 8, at most the cores)')
    parser.add_argument('--in-flight', type=positive, default=128, help="each worker's requests outstanding")
    parser.add_argument('--block-lines', type=positive, default=4096, help='plan lines a worker takes at a time')
    parser.add_argument('--timeout', type=positive, default=120, help='seconds a connection may wait for an answer before it is opened again')
    parser.add_argument('--patience', type=positive, default=600, help='seconds without an answer, through retries and new connections, before the load fails')
    parser.add_argument('--stall-seconds', type=positive, default=1800, help='with --follow: fail once the plan has not grown this long')
    parser.add_argument('--result')
    args = parser.parse_args()
    PlanLines.stall = args.stall_seconds
    if args.in_flight > 32767:
        parser.error('--in-flight must be at most 32767')
    # Stopped (xrpla stops it when the plan's writing fails), it stops its workers and
    # releases its lease.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    result = load(args)
    if args.result:
        Path(args.result).write_text(json.dumps(result))
    else:
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        sys.exit(1)
