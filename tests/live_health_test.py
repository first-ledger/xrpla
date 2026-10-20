"""live_publisher.py: --health verdicts from the runner's status.json, and stopping a publish run."""
import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import live_publisher  # noqa: E402
from live_publisher import STATUS, health  # noqa: E402

# Stands in for piece_publisher.py: pool workers that would run for ten minutes and, like the
# real ones, hold the run's pipes. Each notes its PID in the scratch directory.
FAKE_PUBLISHER = r"""
import os, sys, time
from concurrent.futures import ProcessPoolExecutor

def work(path):
    with open(path, 'a') as f: f.write(f'{os.getpid()}\n')
    time.sleep(600)

if __name__ == '__main__':
    path = sys.argv[sys.argv.index('--scratch') + 1] + '/workers'
    with ProcessPoolExecutor(2) as pool: list(pool.map(work, [path, path]))
"""


def running(pid):
    try: return Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1][0] != 'Z'
    except FileNotFoundError: return False


class Health(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.work = Path(self.tmp.name)

    def tearDown(self): self.tmp.cleanup()

    def check(self, status):
        if status is None: (self.work / STATUS).unlink(missing_ok=True)
        else: (self.work / STATUS).write_text(json.dumps(status))
        result = subprocess.run([sys.executable, str(ROOT / 'tools' / 'live_publisher.py'), '--health', '--workdir', str(self.work), '--max-age', '600'],
                                capture_output=True, text=True)
        report = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0 if report['healthy'] else 1)
        return report['problems']

    def test_verdicts(self):
        now = int(time.time())
        good = {'started': now - 5000, 'updated': now - 3, 'push': True, 'follower_running': True, 'spool': 'spool-1',
                'spool_tip': 1200, 'spool_tip_since': now - 100, 'published': {'generation': 7, 'tip': 1024, 'time': now - 60},
                'pushed': {'generation': 7, 'time': now - 50}}
        self.assertEqual(self.check(good), [])
        self.assertIn('not started', self.check(None)[0])
        self.assertIn('stopped or stuck', self.check({**good, 'updated': now - 900})[0])
        self.assertEqual(self.check({**good, 'follower_running': False}), ['the follower is not running'])
        self.assertIn('has not advanced', self.check({**good, 'spool_tip_since': now - 900})[0])
        self.assertIn('no successful publish', self.check({**good, 'published': {'generation': 7, 'tip': 1024, 'time': now - 900}})[0])
        # A new generation waits for its push only as long as the age limit allows.
        self.assertEqual(self.check({**good, 'pushed': {'generation': 6, 'time': now - 900}}), [])
        self.assertEqual(health(self.work, 30), 1)  # published 60 s ago is too old for a 30 s limit


class Stop(unittest.TestCase):
    def test_a_stop_ends_the_publish_run_and_its_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            tools, work = Path(tmp) / 'tools', Path(tmp) / 'work'
            tools.mkdir(); (work / 'scratch').mkdir(parents=True)
            (tools / 'piece_publisher.py').write_text(FAKE_PUBLISHER)
            runner = live_publisher.Runner.__new__(live_publisher.Runner)
            runner.args = argparse.Namespace(bucket=work / 'bucket', signing_key='key', decoder='decoder', snapshot_tool='snapshot',
                                             piece_ledgers=16, bundle_pieces=4, workers=2, grace_seconds=0, fragment_ledgers=0, tip_fragment_ledgers=0, encoder=None, index_tool=None, push=False)
            runner.work, runner.spool_key, runner.follower = work, work / 'spool.pub', None
            runner.status = {'started': 0, 'push': False, 'spool_tip': None, 'spool_tip_since': 0}
            runner.stopping = True  # as if SIGTERM arrived during the run
            started = time.monotonic()
            with mock.patch.object(live_publisher, 'TOOLS', tools), redirect_stdout(io.StringIO()) as out:
                self.assertIsNone(runner.publish(work / 'spool'))
            self.assertLess(time.monotonic() - started, 30)
            self.assertEqual(json.loads(out.getvalue())['event'], 'publish_stopped')
            workers = [int(pid) for pid in (work / 'scratch' / 'workers').read_text().split()]
            self.assertTrue(workers)
            self.assertFalse([pid for pid in workers if running(pid)])

    def test_the_follower_is_looked_after_during_a_long_publish_run(self):
        # A publish run writing a snapshot takes many minutes: a follower that exits meanwhile is
        # started again while it runs, not once it ends.
        with tempfile.TemporaryDirectory() as tmp:
            tools, work = Path(tmp) / 'tools', Path(tmp) / 'work'
            tools.mkdir(); (work / 'scratch').mkdir(parents=True)
            (tools / 'piece_publisher.py').write_text("import time\ntime.sleep(6)\nprint('{}')\n")
            runner = live_publisher.Runner.__new__(live_publisher.Runner)
            runner.args = argparse.Namespace(bucket=work / 'bucket', signing_key='key', decoder='decoder', snapshot_tool='snapshot',
                                             piece_ledgers=16, bundle_pieces=4, workers=2, grace_seconds=0, fragment_ledgers=0, tip_fragment_ledgers=0, encoder=None, index_tool=None, push=False)
            runner.work, runner.spool_key, runner.follower = work, work / 'spool.pub', None
            runner.status = {'started': 0, 'push': False, 'spool_tip': None, 'spool_tip_since': 0}
            runner.stopping = False
            with mock.patch.object(live_publisher, 'TOOLS', tools), mock.patch.object(runner, 'keep_following') as tended, \
                 redirect_stdout(io.StringIO()):
                self.assertEqual(runner.publish(work / 'spool'), {})
            tended.assert_called_with(work / 'spool')


if __name__ == '__main__':
    unittest.main()
