"""Compact static transaction-hash index (size experiment E2, docs/internal/SIZE-AND-FOOTPRINT.md).

Stores a fingerprint and an exact location per transaction instead of the full
32-byte hash. A lookup reads one hash-prefix bucket and returns candidate
(ledger, position) pairs; the caller confirms a candidate by hashing the
fetched transaction, so fingerprint collisions cost an extra read, never a
wrong answer. About 4.75 bytes per transaction versus 44.6 for SQLite.

Layout (little-endian): header, bucket bitmap, bucket samples, fingerprints,
locations, per-ledger transaction counts, SHA-256 of everything before it.
The bitmap writes each bucket as one 1-bit per entry then a 0-bit (the high
half of an Elias-Fano code); samples record the bitmap offset of every 256th
bucket so a lookup scans at most 255 bucket terminators.
"""
from array import array
import hashlib
import itertools
import math
import struct

MAGIC = b'XTXI'
HEAD = struct.Struct('<4sHBBBBIQQ')  # magic, version, bucket bits, fingerprint bits, location bits, pad, first ledger, entries, ledgers
SAMPLE = 256
MAX_ENTRIES = 1 << 40


class Bits:
    """Append-only little-endian bit writer."""
    def __init__(self): self.buf = bytearray(); self.acc = 0; self.pending = 0
    @property
    def size(self): return 8 * len(self.buf) + self.pending
    def put(self, value, width):
        if width and value >> width: raise ValueError('Bit field overflow')
        self.acc |= value << self.pending; self.pending += width
        while self.pending >= 8: self.buf.append(self.acc & 255); self.acc >>= 8; self.pending -= 8
    def bytes(self): return bytes(self.buf) + (bytes([self.acc]) if self.pending else b'')


def get(buf, at, width):
    """Read `width` bits starting at bit `at` of a little-endian bit string."""
    if not width: return 0
    first, last = at // 8, (at + width - 1) // 8
    if last >= len(buf): raise ValueError('Truncated bit field')
    return (int.from_bytes(buf[first:last + 1], 'little') >> (at % 8)) & ((1 << width) - 1)


def vu(n):
    out = bytearray()
    while True:
        b = n & 127; n >>= 7
        out.append(b | (128 if n else 0))
        if not n: return bytes(out)


def build(first_ledger, ledgers, fingerprint_bits=16):
    """`ledgers` lists, per consecutive ledger from first_ledger, its transaction hashes in position order."""
    counts, prefixes = array('I'), array('Q')
    for txs in ledgers:
        counts.append(len(txs))
        for h in txs:
            if len(h) != 32: raise ValueError('Hash length')
            prefixes.append(int.from_bytes(h[:8], 'big'))
    return build_compact(first_ledger, counts, prefixes, fingerprint_bits)


def build_compact(first_ledger, counts, prefixes, fingerprint_bits=16):
    """The same index from per-ledger transaction counts and each transaction's first 8 hash bytes
    (as big-endian integers, in ledger/position order). Entries are packed into 64-bit integers and
    sorted one hash-prefix partition at a time, so memory stays near 16 bytes per transaction."""
    n = len(prefixes)
    if n > MAX_ENTRIES or not 1 <= fingerprint_bits <= 32 or sum(counts) != n: raise ValueError('Index bounds')
    q = max(1, math.ceil(math.log2(max(n, 2))))
    location_bits = max(1, math.ceil(math.log2(max(n, 2))))
    width = q + fingerprint_bits
    partition_bits = min(q, max(8, width + location_bits - 64))
    rest_bits = width - partition_bits
    if width > 64 or rest_bits + location_bits > 64: raise ValueError('Index too large for 64-bit entries')
    parts = [array('Q') for _ in range(1 << partition_bits)]
    rest_mask, location_mask = (1 << rest_bits) - 1, (1 << location_bits) - 1
    for ordinal, prefix in enumerate(prefixes):
        value = prefix >> (64 - width)
        parts[value >> rest_bits].append(((value & rest_mask) << location_bits) | ordinal)
    bitmap, fingerprints, locations = Bits(), Bits(), Bits()
    samples = bytearray(); mask = (1 << fingerprint_bits) - 1
    low_bits = q - partition_bits; current = None; entries = []; cursor = 0
    for bucket in range(1 << q):
        if bucket % SAMPLE == 0: samples += struct.pack('<Q', bitmap.size)
        partition = bucket >> low_bits
        if partition != current:
            current = partition; entries = sorted(parts[partition]); parts[partition] = None; cursor = 0
        low = bucket & ((1 << low_bits) - 1)
        while cursor < len(entries) and (entries[cursor] >> location_bits) >> fingerprint_bits == low:
            entry = entries[cursor]
            bitmap.put(1, 1); fingerprints.put((entry >> location_bits) & mask, fingerprint_bits)
            locations.put(entry & location_mask, location_bits)
            cursor += 1
        bitmap.put(0, 1)
    ledger_counts = b''.join(vu(c) for c in counts)
    sections = [bitmap.bytes(), bytes(samples), fingerprints.bytes(), locations.bytes(), ledger_counts]
    out = HEAD.pack(MAGIC, 1, q, fingerprint_bits, location_bits, 0, first_ledger, n, len(counts))
    out += b''.join(struct.pack('<Q', len(s)) for s in sections) + b''.join(sections)
    return out + hashlib.sha256(out).digest()


class Index:
    def __init__(self, data):
        if len(data) < HEAD.size + 5 * 8 + 32 or hashlib.sha256(data[:-32]).digest() != data[-32:]:
            raise ValueError('Index checksum differs')
        magic, version, self.q, self.fbits, self.lbits, _, self.first, self.n, ledgers = HEAD.unpack_from(data)
        if magic != MAGIC or version != 1 or not 1 <= self.q <= 41 or not 1 <= self.fbits <= 32 or not 1 <= self.lbits <= 41:
            raise ValueError('Invalid index header')
        if self.n > MAX_ENTRIES or (self.n and (self.n - 1) >> self.lbits): raise ValueError('Invalid index size')
        lengths = struct.unpack_from('<5Q', data, HEAD.size); at = HEAD.size + 40
        if at + sum(lengths) != len(data) - 32: raise ValueError('Index section lengths differ')
        self.bitmap, self.samples, self.fingerprints, self.locations, counts = \
            (data[a:a + l] for a, l in zip(itertools.accumulate([at, *lengths[:-1]]), lengths))
        if len(self.bitmap) != (self.n + (1 << self.q) + 7) // 8 or len(self.samples) != 8 * -(-(1 << self.q) // SAMPLE):
            raise ValueError('Index section sizes differ')
        if len(self.fingerprints) != (self.n * self.fbits + 7) // 8 or len(self.locations) != (self.n * self.lbits + 7) // 8:
            raise ValueError('Index section sizes differ')
        self.starts = []; total = 0; pos = 0
        for _ in range(ledgers):
            c = 0; shift = 0
            while True:
                if pos >= len(counts) or shift > 35: raise ValueError('Invalid ledger counts')
                b = counts[pos]; pos += 1; c |= (b & 127) << shift; shift += 7
                if not b & 128: break
            self.starts.append(total); total += c
        if pos != len(counts) or total != self.n: raise ValueError('Ledger counts differ from entries')

    def location(self, ordinal):
        """Map an ordinal to (ledger, position) via the per-ledger counts."""
        lo, hi = 0, len(self.starts) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self.starts[mid] <= ordinal: lo = mid
            else: hi = mid - 1
        return self.first + lo, ordinal - self.starts[lo]

    def candidates(self, tx_hash):
        """(ledger, position) pairs whose fingerprint matches; confirm by hashing the transaction."""
        if len(tx_hash) != 32: raise ValueError('Hash length')
        value = int.from_bytes(tx_hash, 'big') >> (256 - self.q - self.fbits)
        bucket, fingerprint = value >> self.fbits, value & ((1 << self.fbits) - 1)
        base = bucket - bucket % SAMPLE
        pos = struct.unpack_from('<Q', self.samples, 8 * (base // SAMPLE))[0]
        entry = pos - base  # ones before this bitmap offset
        for _ in range(bucket - base):  # skip whole buckets up to the target
            while get(self.bitmap, pos, 1): pos += 1; entry += 1
            pos += 1
        out = []
        while get(self.bitmap, pos, 1):
            if get(self.fingerprints, entry * self.fbits, self.fbits) == fingerprint:
                ordinal = get(self.locations, entry * self.lbits, self.lbits)
                if ordinal >= self.n: raise ValueError('Location out of range')
                out.append(self.location(ordinal))
            pos += 1; entry += 1
        return out

    def lookup(self, tx_hash, fetch):
        """Exact lookup: `fetch(ledger, position)` returns the raw transaction bytes."""
        for ledger, position in self.candidates(tx_hash):
            if hashlib.sha512(b'TXN\0' + fetch(ledger, position)).digest()[:32] == tx_hash:
                return ledger, position
        return None
