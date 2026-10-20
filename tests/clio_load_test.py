"""tools/clio_load.py against an in-memory Cassandra (tests/fake_cassandra.py).

Several workers, small blocks and few requests in flight, so rows land out of order: the
tables must still hold what the plan's rows written one after another would leave (a key
written twice keeps its later row), the range is published last, and an interrupted load
resumes where its acknowledged lines end, only for a plan with the same bytes before them.
A plan followed while it is written (--follow) loads the same.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_cassandra import FakeCassandra, expected  # noqa: E402

SCHEMAS = [
    ('objects', 'key blob, sequence bigint, object blob, PRIMARY KEY(key,sequence)) WITH CLUSTERING ORDER BY(sequence DESC)'),
    ('transactions', 'hash blob PRIMARY KEY, ledger_sequence bigint, date bigint, transaction blob, metadata blob)'),
    ('ledger_transactions', 'ledger_sequence bigint, hash blob, PRIMARY KEY(ledger_sequence,hash))'),
    ('diff', 'seq bigint, key blob, PRIMARY KEY(seq,key))'),
    ('account_tx', 'account blob, seq_idx tuple<bigint,bigint>, hash blob, PRIMARY KEY(account,seq_idx)) WITH CLUSTERING ORDER BY(seq_idx DESC)'),
    ('ledgers', 'sequence bigint PRIMARY KEY, header blob)'),
    ('ledger_hashes', 'hash blob PRIMARY KEY, sequence bigint)'),
    ('ledger_range', 'is_latest boolean PRIMARY KEY, sequence bigint)'),
    ('nf_tokens', 'token_id blob, sequence bigint, owner blob, is_burned boolean, PRIMARY KEY(token_id,sequence)) WITH CLUSTERING ORDER BY(sequence DESC)'),
]


def h(*parts):
    return hashlib.sha256(repr(parts).encode()).hexdigest()


def plan(first=1000, last=1039):
    """A plan shaped like xrpla's: identity, schemas, then rows (with a transaction ID in two ledgers)."""
    def row(table, **values):
        return json.dumps({'table': table, 'values': values}, sort_keys=True, separators=(',', ':'))
    lines = [json.dumps({'format': 'xrpla-clio-2.7.1-v1', 'first': first, 'last': last, 'first_hash': h('ledger', first), 'last_hash': h('ledger', last)})]
    lines += [json.dumps({'schema': f'CREATE TABLE IF NOT EXISTS $KEYSPACE.{table} ({fields}'}, separators=(',', ':')) for table, fields in SCHEMAS]
    for seq in range(first, last + 1):
        header = bytes.fromhex(h('header', seq) * 4)[:118]  # Clio's header row: the header, then its hash
        digest = hashlib.sha512(b'LWR\0' + header).digest()[:32]
        lines.append(row('ledgers', sequence=seq, header={'blob': (header + digest).hex()}))
        lines.append(row('ledger_hashes', hash={'blob': digest.hex()}, sequence=seq))
        for position in range(seq % 7):
            tx = h('tx', seq, position)
            lines.append(row('transactions', hash={'blob': tx}, ledger_sequence=seq, date=seq * 10, transaction={'blob': h('body', tx)}, metadata={'blob': ''}))
            lines.append(row('ledger_transactions', ledger_sequence=seq, hash={'blob': tx}))
            lines.append(row('account_tx', account={'blob': h('account', position)[:40]}, seq_idx=[seq, position], hash={'blob': tx}))
        if seq % 10 == 3:  # a repeated pseudo-transaction: its ID keeps the later ledger's row
            lines.append(row('transactions', hash={'blob': h('vote')}, ledger_sequence=seq, date=seq, transaction={'blob': 'aa'}, metadata={'blob': h(seq)}))
        for key in range(seq % 5):
            lines.append(row('diff', seq=seq, key={'blob': h('key', key)}))
    for key in range(50):
        for seq in range(first, last + 1, 9):
            lines.append(row('objects', key={'blob': h('key', key)}, sequence=seq, object={'blob': h('object', key, seq) * 3}))
        lines.append(row('nf_tokens', token_id={'blob': h('nft', key)}, sequence=first, owner={'blob': h('owner')[:40]}, is_burned=key % 2 == 0))
    return [line + '\n' for line in lines]


@unittest.skipUnless(os.name == 'posix', 'The loader runs on Linux')
class Loader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='xrpla-clio-load-')
        self.root = Path(self.tmp.name)
        self.servers = []

    def tearDown(self):
        for server in self.servers:
            server.close()
        self.tmp.cleanup()

    def server(self, **options):
        server = FakeCassandra(**options)
        self.servers.append(server)
        return server

    def load(self, server, path, *extra, check=True):
        result = self.root / 'result.json'
        result.unlink(missing_ok=True)
        command = [sys.executable, ROOT / 'tools' / 'clio_load.py', '--plan', path, '--host', '127.0.0.1', '--port', server.port,
                   '--keyspace', 'xrpla_test', '--result', result, '--workers', 3, '--in-flight', 4, '--block-lines', 50, *extra]
        done = subprocess.run([str(x) for x in command], capture_output=True, text=True, timeout=300)
        if check and done.returncode:
            raise AssertionError(done.stderr[-3000:])
        return json.loads(result.read_text()) if not done.returncode else None

    def check_loaded(self, server, lines):
        want = expected(lines)
        for table, rows in want.items():
            self.assertEqual(server.contents(table), rows, table)
        header = json.loads(lines[0])
        vote = [json.loads(x) for x in server.contents('transactions') if json.loads(x)['hash']['blob'] == h('vote')]
        self.assertEqual([x['ledger_sequence'] for x in vote], [max(s for s in range(header['first'], header['last'] + 1) if s % 10 == 3)])
        self.assertEqual(sorted((json.loads(x)['is_latest'], json.loads(x)['sequence']) for x in server.contents('ledger_range')),
                         [(False, header['first']), (True, header['last'])])
        self.assertEqual(server.tables['xrpla_import_lock'].rows, {}, 'the lease is released')

    def write(self, lines, name='clio-plan.jsonl'):
        path = self.root / name
        path.write_text(''.join(lines))
        return path

    def test_loads_as_if_in_order(self):
        lines = plan()
        server = self.server()
        result = self.load(server, self.write(lines))
        self.check_loaded(server, lines)
        rows = len(lines) - 1 - len(SCHEMAS)
        self.assertEqual((result['rows_sent'], result['plan_lines'], result['resumed_from_line']), (rows, len(lines), 1))
        self.assertEqual(result['plan_sha256'], hashlib.sha256(''.join(lines).encode()).hexdigest())
        self.assertGreater(server.batches, 0, 'rows of one partition go together')
        self.assertLess(server.writes - server.batches, rows)
        # Loaded again: every line acknowledged, nothing is sent.
        again = self.load(server, self.root / 'clio-plan.jsonl')
        self.assertEqual((again['rows_sent'], again['resumed_from_line']), (0, len(lines)))
        self.check_loaded(server, lines)

    def test_retries_overloaded_writes(self):
        lines = plan()
        server = self.server(overload_every=5)
        self.load(server, self.write(lines))
        self.check_loaded(server, lines)

    def test_resumes_after_interruption(self):
        lines = plan(1000, 1199)
        path = self.write(lines)
        server = self.server(fail_after_writes=1200, delay=0.005)
        self.assertIsNone(self.load(server, path, '--block-lines', 20, '--patience', 3, check=False))
        status = server.text_row('xrpla_import_status', 0)
        self.assertGreater(status['next_line'], 1)
        self.assertLess(status['next_line'], len(lines))
        self.assertEqual(status['plan'], hashlib.sha256(''.join(lines[:status['next_line']]).encode()).hexdigest())
        self.assertEqual(status['last'], 999, 'the range is not published')
        server.fail_after_writes = None
        server.tables['xrpla_import_lock'].rows.clear()  # the lease's TTL, run out
        result = self.load(server, path, '--block-lines', 20)
        self.assertEqual(result['resumed_from_line'], status['next_line'])
        self.assertLess(result['rows_sent'], len(lines) - 1 - len(SCHEMAS))
        self.check_loaded(server, lines)

    def test_different_plan_starts_again(self):
        lines = plan(1000, 1199)
        server = self.server(fail_after_writes=1200, delay=0.005)
        self.assertIsNone(self.load(server, self.write(lines), '--block-lines', 20, '--patience', 3, check=False))
        self.assertGreater(server.text_row('xrpla_import_status', 0)['next_line'], len(SCHEMAS) + 2)
        server.fail_after_writes = None
        server.tables['xrpla_import_lock'].rows.clear()
        changed = list(lines)
        line = len(SCHEMAS) + 3  # the first transaction's row
        changed[line] = changed[line].replace('"date":10000', '"date":10001')
        self.assertNotEqual(changed, lines)
        result = self.load(server, self.write(changed))
        self.assertEqual(result['resumed_from_line'], 1)
        self.assertEqual(server.contents('transactions'), expected(changed)['transactions'])

    def test_follows_a_plan_being_written(self):
        lines = plan()
        path = self.root / 'clio-plan.jsonl'
        part = Path(f'{path}.part')
        part.write_text(''.join(lines[:len(SCHEMAS) + 1]))

        def writer():
            with part.open('a') as stream:
                for i, line in enumerate(lines[len(SCHEMAS) + 1:]):
                    stream.write(line[:len(line) // 2]); stream.flush()  # lines arrive in pieces
                    stream.write(line[len(line) // 2:])
                    if i % 200 == 0:
                        stream.flush(); time.sleep(0.3)
            os.replace(part, path)
        thread = threading.Thread(target=writer)
        thread.start()
        server = self.server()
        result = self.load(server, path, '--follow')
        thread.join()
        self.assertEqual(result['plan_lines'], len(lines))
        self.check_loaded(server, lines)

    def test_answers_out_of_order(self):
        # A node answers in any order: a block's first answers may come while its later rows
        # are still being sent.
        lines = plan(1000, 1099)
        server = self.server(reorder=True)
        self.load(server, self.write(lines))
        self.check_loaded(server, lines)

    def test_prepares_again(self):
        # A node that forgets its prepared statements refuses every request in flight with them.
        lines = plan(1000, 1099)
        server = self.server(reorder=True, unprepare_every=25)
        self.load(server, self.write(lines))
        self.check_loaded(server, lines)
        self.assertGreater(server.unprepared, 10)

    def test_refuses_keyspace_loaded_with_server_timestamps(self):
        # An earlier xrpla's loader wrote with the server's timestamps and left no record of
        # its own: rows loaded now would lose to those for about 13 days.
        def earlier(server, written):
            for statement in ('CREATE TABLE IF NOT EXISTS xrpla_test.xrpla_import_status (id int PRIMARY KEY, source text, first bigint, last bigint, plan text, next_line bigint)',
                              'CREATE TABLE IF NOT EXISTS xrpla_test.xrpla_import_lock (id int PRIMARY KEY, owner text)'):
                server.query(statement)
            header = json.loads(lines[0])
            server.tables['xrpla_import_status'].write({'id': 0, 'source': header['first_hash'], 'first': header['first'], 'last': header['first'] - 1,
                                                        'plan': 'f' * 64, 'next_line': 5}, written)
        lines = plan()
        recent = self.server()
        earlier(recent, time.time_ns() // 1000 - 60_000_000)
        self.assertIsNone(self.load(recent, self.write(lines), check=False))
        self.assertIsNone(recent.text_row('xrpla_import_writes', 0))
        old = self.server()
        earlier(old, time.time_ns() // 1000 - (1 << 41))
        self.load(old, self.write(lines))
        self.check_loaded(old, lines)

    def test_follow_gives_up_on_a_stuck_writer(self):
        lines = plan()
        path = self.root / 'clio-plan.jsonl'
        Path(f'{path}.part').write_text(''.join(lines[:len(SCHEMAS) + 20]))
        started = time.monotonic()
        self.assertIsNone(self.load(self.server(), path, '--follow', '--stall-seconds', 2, check=False))
        self.assertLess(time.monotonic() - started, 60)

    def test_rides_out_a_busy_node(self):
        # Writes refused as timed out, connections dropped mid-block and pauses longer than
        # the client's timeout (every connection waits, the coordinator's too): the load
        # opens connections again, sends what was unacknowledged and completes the same.
        lines = plan(1000, 1149)
        server = self.server(reorder=True, timeout_every=11, drop_every=37, stall_at=(300, 1500), stall_seconds=2.5)
        result = self.load(server, self.write(lines), '--timeout', 1)
        self.check_loaded(server, lines)
        self.assertEqual(result['plan_lines'], len(lines))
        self.assertFalse(server.stall_at, 'both pauses happened')
        self.assertGreater(len(server.connections), 3 + 1 + 10, 'connections were opened again')

    def test_gives_up_without_progress(self):
        server = self.server(fail_after_writes=0)
        started = time.monotonic()
        self.assertIsNone(self.load(server, self.write(plan()), '--timeout', 1, '--patience', 3, check=False))
        self.assertLess(time.monotonic() - started, 60)

    def test_refuses_another_history(self):
        server = self.server()
        self.load(server, self.write(plan()))
        other = plan()
        other[0] = json.dumps({**json.loads(other[0]), 'first_hash': h('another')}) + '\n'
        self.assertIsNone(self.load(server, self.write(other, 'other.jsonl'), check=False))


if __name__ == '__main__':
    unittest.main()
