"""XRPP piece/bundle container (docs/internal/SIZE-AND-FOOTPRINT.md, publication plan).

A piece covers one aligned range of ledgers (50,000 by default); a bundle
covers a whole interval (1,000,000) and is built from its pieces' segment
bytes unchanged. Both use this layout:

  directory: header, one table row per XRPH segment, index descriptor
  body:      XRPH segments (history_codec), then the XTXI transaction index
  trailer:   SHA-256 of everything before it

The catalog records the SHA-256 of the directory as well as of the whole file,
so a reader can fetch the directory with one byte-range read, authenticate it,
then fetch and check single segments. Bundles reach tens of GB, so containers
are written and read as files without holding them in memory.
"""
import hashlib
import mmap
import os
from pathlib import Path
import struct

MAGIC = b'XRPP'
HEAD = struct.Struct('<4sHBBIII32s32sQ')  # magic, version, kind, pad, first, last, segments, parent hash, last hash, transactions
ROW = struct.Struct('<IIQQ32s')  # first ledger, ledgers, offset, length, SHA-256
INDEX = struct.Struct('<QQ32s')  # offset, length, SHA-256
KINDS = {'piece': 0, 'bundle': 1}
MAX_SEGMENTS = 65536
STEP = 8 * 1024 * 1024


def sha(data): return hashlib.sha256(data).digest()


def write(path, kind, first, last, parent_hash, last_hash, transactions, segments, index):
    """Write a container file. `segments` lists (first ledger, ledgers, length, sha256, read) where
    read() returns that segment's bytes; segments are read one at a time. Returns (file sha256, directory sha256)."""
    if kind not in KINDS or not segments or len(segments) > MAX_SEGMENTS: raise ValueError('Container bounds')
    if len(parent_hash) != 32 or len(last_hash) != 32: raise ValueError('Hash length')
    expected = first
    for start, count, *_rest in segments:
        if start != expected or count < 1: raise ValueError('Segments must cover the range contiguously')
        expected += count
    if expected != last + 1: raise ValueError('Segments must end at the range end')
    offset = HEAD.size + ROW.size * len(segments) + INDEX.size
    rows = bytearray()
    for start, count, length, digest, _ in segments:
        rows += ROW.pack(start, count, offset, length, digest); offset += length
    directory = HEAD.pack(MAGIC, 1, KINDS[kind], 0, first, last, len(segments), parent_hash, last_hash, transactions) \
        + bytes(rows) + INDEX.pack(offset, len(index), sha(index))
    body, whole = hashlib.sha256(), hashlib.sha256()
    temporary = Path(str(path) + '.part'); temporary.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open('wb') as out:
        def emit(data): out.write(data); body.update(data); whole.update(data)
        emit(directory)
        for start, count, length, digest, read in segments:
            data = read()
            if len(data) != length or sha(data) != digest: raise ValueError(f'Segment {start} changed while writing')
            emit(data)
        emit(index)
        trailer = body.digest(); out.write(trailer); whole.update(trailer)
        out.flush(); os.fsync(out.fileno())
    os.replace(temporary, path)
    return whole.hexdigest(), sha(directory).hex()


def build(kind, first, last, parent_hash, last_hash, transactions, segments, index):
    """In-memory form for small containers: `segments` lists (first ledger, ledgers, bytes)."""
    rows = [(s, n, len(d), sha(d), (lambda d=d: d)) for s, n, d in segments]
    import tempfile
    with tempfile.TemporaryDirectory() as work:
        path = Path(work) / 'container'
        _, directory = write(path, kind, first, last, parent_hash, last_hash, transactions, rows, index)
        return path.read_bytes(), directory


class Directory:
    """Parsed, authenticated directory; enough to locate every segment and the index."""
    def __init__(self, data, expected_sha256=None):
        if len(data) < HEAD.size + INDEX.size: raise ValueError('Truncated container directory')
        magic, version, kind, _, self.first, self.last, n, self.parent_hash, self.last_hash, self.transactions = HEAD.unpack_from(data)
        if magic != MAGIC or version != 1 or kind not in KINDS.values() or not 1 <= n <= MAX_SEGMENTS or self.last < self.first:
            raise ValueError('Invalid container header')
        self.kind = 'piece' if kind == 0 else 'bundle'
        self.size = HEAD.size + ROW.size * n + INDEX.size
        if len(data) < self.size: raise ValueError('Truncated container directory')
        if expected_sha256 is not None and sha(data[:self.size]).hex() != expected_sha256: raise ValueError('Directory differs from catalog')
        self.rows = [ROW.unpack_from(data, HEAD.size + ROW.size * i) for i in range(n)]
        self.index = INDEX.unpack_from(data, HEAD.size + ROW.size * n)
        expected, offset = self.first, self.size
        for start, count, at, length, _ in self.rows:
            if start != expected or not count or at != offset: raise ValueError('Invalid segment table')
            expected += count; offset += length
        if expected != self.last + 1 or self.index[0] != offset: raise ValueError('Invalid segment table')
        self.body_size = offset + self.index[1]

    def segment_for(self, ledger):
        for i, (start, count, *_rest) in enumerate(self.rows):
            if start <= ledger < start + count: return i
        raise ValueError('Ledger outside container')


def file_digests(path):
    """(SHA-256 of the whole file, SHA-256 of all but its last 32 bytes, trailer) in one streaming pass."""
    size = os.path.getsize(path); whole, body = hashlib.sha256(), hashlib.sha256()
    with open(path, 'rb') as f:
        remaining = size - 32
        while remaining > 0:
            data = f.read(min(STEP, remaining)); whole.update(data); body.update(data); remaining -= len(data)
        trailer = f.read(32); whole.update(trailer)
    return whole.hexdigest(), body.digest(), trailer


class Container:
    """A container read in place. Segments and the index are checked against the directory on every read;
    `full=True` also checks the whole file against its trailer (and the catalog's file hash, if given)."""
    def __init__(self, data, expected_sha256=None, directory_sha256=None, full=True):
        if len(data) < 32: raise ValueError('Container checksum differs')
        if full:
            whole, body = hashlib.sha256(), hashlib.sha256()
            view = memoryview(data)
            for at in range(0, len(data) - 32, STEP):
                part = view[at:min(at + STEP, len(data) - 32)]; whole.update(part); body.update(part)
            whole.update(view[len(data) - 32:])
            if body.digest() != bytes(view[len(data) - 32:]): raise ValueError('Container checksum differs')
            if expected_sha256 is not None and whole.hexdigest() != expected_sha256: raise ValueError('Container differs from catalog')
        self.data = data
        self.directory = Directory(data[:min(len(data), HEAD.size + ROW.size * MAX_SEGMENTS + INDEX.size)], directory_sha256)
        if self.directory.body_size != len(data) - 32: raise ValueError('Container length differs')

    @classmethod
    def open(cls, path, expected_sha256=None, directory_sha256=None, full=True):
        handle = open(path, 'rb')
        data = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ); handle.close()
        return cls(data, expected_sha256, directory_sha256, full)

    def segment(self, i):
        start, count, at, length, digest = self.directory.rows[i]
        data = bytes(self.data[at:at + length])
        if sha(data) != digest: raise ValueError('Segment checksum differs')
        return data

    def index_bytes(self):
        at, length, digest = self.directory.index
        data = bytes(self.data[at:at + length])
        if sha(data) != digest: raise ValueError('Index checksum differs')
        return data
