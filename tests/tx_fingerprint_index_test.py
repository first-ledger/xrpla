"""Compact transaction-hash index: exact lookups, collisions, empty ledgers and corruption."""
import hashlib
from pathlib import Path
import random
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import tx_fingerprint_index as ti


def half(data): return hashlib.sha512(data).digest()[:32]


def corpus(ledgers, seed=7):
    rng = random.Random(seed); txs = []
    for i in range(ledgers):
        txs.append([rng.randbytes(rng.randrange(20, 60)) for _ in range(rng.choice((0, 0, 1, 3, 40)))])
    return txs


class FingerprintIndex(unittest.TestCase):
    def check(self, txs, bits):
        index = ti.Index(ti.build(1000, [[half(b'TXN\0' + t) for t in ledger] for ledger in txs], bits))
        fetch = lambda ledger, position: txs[ledger - 1000][position]
        for i, ledger in enumerate(txs):
            for position, tx in enumerate(ledger):
                self.assertEqual(index.lookup(half(b'TXN\0' + tx), fetch), (1000 + i, position))
        rng = random.Random(1)
        for _ in range(500): self.assertIsNone(index.lookup(rng.randbytes(32), fetch))
        return index

    def test_every_present_hash_and_no_absent_hash(self):
        index = self.check(corpus(300), 16)
        rng = random.Random(2)
        extra = sum(len(index.candidates(rng.randbytes(32))) for _ in range(20000))
        self.assertLess(extra, 5)  # 16-bit fingerprints: rare wasted reads

    def test_collisions_resolve_by_hashing(self):
        index = self.check(corpus(200, 3), 4)
        rng = random.Random(3)
        self.assertGreater(sum(len(index.candidates(rng.randbytes(32))) for _ in range(5000)), 50)

    def test_empty_index_and_empty_ledgers(self):
        index = ti.Index(ti.build(5, [[], [], []]))
        self.assertEqual(index.candidates(bytes(32)), [])
        self.check([[], [b'a'], [], [], [b'b', b'c'], []], 16)

    def test_size_near_five_bytes_per_transaction(self):
        rng = random.Random(4)
        ledgers = [[rng.randbytes(32) for _ in range(100)] for _ in range(1000)]
        data = ti.build(1, ledgers)
        self.assertLess(len(data) / 100000, 5.5)
        index = ti.Index(data)
        self.assertEqual(index.candidates(ledgers[-1][-1]), [(1000, 99)])

    def test_corruption_is_rejected(self):
        data = ti.build(1, [[half(b'TXN\0' + bytes([i])) for i in range(10)]])
        for bad in (data[:-1], data[:20] + bytes([data[20] ^ 1]) + data[21:], b'XXXX' + data[4:], data + b'\0'):
            with self.assertRaises(ValueError): ti.Index(bad)
        with self.assertRaises(ValueError): ti.Index(data).candidates(b'short')


if __name__ == '__main__':
    unittest.main()
