"""Segments a size limit of the format refuses are split in halves until they fit: the parts cover
the planned segment in order, a single ledger that still does not fit fails, and other encoder
errors are not retried."""
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import piece_publisher as pp  # noqa: E402


def rows(first, count):
    return [(seq, b'', []) for seq in range(first, first + count)]


class SplitOnBounds(unittest.TestCase):
    def test_a_segment_over_a_bound_is_split_until_it_fits(self):
        calls = []
        def encode(group, *args):
            calls.append(len(group))
            if len(group) > 3: raise pp.SegmentTooLarge('Section bound')
            return f'{group[0][0]}-{group[-1][0]}'.encode()
        with mock.patch.object(pp, 'encode_segment', side_effect=encode):
            parts = pp.encode_fitting(rows(100, 10))
        self.assertEqual([p[0][0][0] for p in parts], [100, 102, 105, 107])  # starts, in order
        self.assertEqual(sum(len(g) for g, _ in parts), 10)
        self.assertEqual([seq for g, _ in parts for seq, _, _ in g], list(range(100, 110)))
        self.assertEqual([d for _, d in parts], [b'100-101', b'102-104', b'105-106', b'107-109'])
        self.assertEqual(calls[0], 10)

    def test_a_single_ledger_over_a_bound_fails(self):
        with mock.patch.object(pp, 'encode_segment', side_effect=pp.SegmentTooLarge('Section bound')):
            with self.assertRaises(pp.SegmentTooLarge):
                pp.encode_fitting(rows(7, 1))

    def test_other_encoder_errors_are_not_split(self):
        with mock.patch.object(pp, 'encode_segment', side_effect=ValueError('Encoder failed: Truncated data')) as encode:
            with self.assertRaises(ValueError):
                pp.encode_fitting(rows(1, 8))
            self.assertEqual(encode.call_count, 1)


class SegmentsInParallel(unittest.TestCase):
    def test_results_come_in_order_with_a_bounded_number_at_work(self):
        running, most, lock = [0], [0], threading.Lock()
        def work(n):
            with lock: running[0] += 1; most[0] = max(most[0], running[0])
            time.sleep(0.01 * (5 - n % 5))  # later items finish first
            with lock: running[0] -= 1
            return n * n
        self.assertEqual(list(pp.in_order(work, range(12), 4)), [n * n for n in range(12)])
        self.assertLessEqual(most[0], 4); self.assertGreater(most[0], 1)

    def test_the_default_leaves_the_machine_its_memory(self):
        meminfo = lambda total, available: f'MemTotal: {total << 20} kB\nMemFree: 1 kB\nMemAvailable: {available << 20} kB\n'
        # 128 GiB with 96 available: 32 are kept, 64 spare allow 16 segments (4 GiB each).
        self.assertEqual(pp.default_segment_jobs(meminfo(128, 96), cores=48), 16)
        self.assertEqual(pp.default_segment_jobs(meminfo(128, 96), cores=8), 8)
        # With most of it in use, one at a time; 16 GiB are kept even on a small machine.
        self.assertEqual(pp.default_segment_jobs(meminfo(128, 30), cores=48), 1)
        self.assertEqual(pp.default_segment_jobs(meminfo(32, 28), cores=48), 3)
        self.assertEqual(pp.default_segment_jobs('MemTotal: 1 kB\n', cores=48), 12)  # no MemAvailable

    def test_a_failure_stops_the_run(self):
        def work(n):
            if n == 3: raise ValueError('Encoder failed')
            return n
        with self.assertRaises(ValueError): list(pp.in_order(work, range(8), 3))


if __name__ == '__main__':
    unittest.main()
