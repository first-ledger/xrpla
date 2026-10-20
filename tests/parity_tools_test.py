"""tools/xrpld_parity.py's comparison rules: ignored differences do not fill the limit, and only
a request XRPLA did not answer in time counts as a timeout (any other failure fails the run);
matching errors, and version and ping, are not a comparison of ledgers; too many unanswered
requests fail; sampled transactions reach the newest ledger.
tools/parity_campaign.py's summary: an interval that failed or compared nothing fails the run,
and a progress line cut short is skipped; on resume, an interval that failed only on unanswered
requests is run again a bounded number of times, and one with no exit recorded is run again."""
import contextlib
import http.client
import io
import json
from pathlib import Path
import socket
import sys
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import parity_campaign  # noqa: E402
import xrpld_parity  # noqa: E402


def options(**changed):
    return SimpleNamespace(**{'clio_only': False, 'min_limit': 1, 'reference_retries': 0, 'ignore': [], 'arbiter': None,
                              'transports': [], 'xrpld': 'http://reference', 'xrpla': 'http://xrpla', 'max_unanswered': 0.1, **changed})


class Differences(unittest.TestCase):
    def test_ignored_differences_do_not_hide_a_real_one(self):
        expected = {'a': list(range(100)), 'b': 2}
        actual = {'a': [n + 1 for n in range(100)], 'b': 3}
        self.assertEqual(xrpld_parity.differences(expected, actual, ignore=[r'^/a']), ['/b: xrpld 2 xrpla 3'])

    def test_the_limit_counts_what_is_kept(self):
        found = xrpld_parity.differences(list(range(10)), [n + 1 for n in range(10)], limit=3)
        self.assertEqual(len(found), 3)


class Failures(unittest.TestCase):
    def compare(self, error):
        parity = xrpld_parity.Parity(options())
        with mock.patch.object(xrpld_parity, 'reference_call', return_value=({'status': 'success'}, 0.01)), \
             mock.patch.object(xrpld_parity, 'server_call', side_effect=error):
            parity.compare('server_info')
        return parity.stats['server_info']

    def test_a_timeout_is_slowness(self):
        for error in (TimeoutError('timed out'), socket.timeout('timed out'), urllib.error.URLError(TimeoutError('timed out'))):
            stats = self.compare(error)
            self.assertEqual((stats.get('timeouts'), stats.get('xrpla_errors')), (1, None), error)

    def test_other_failures_are_errors(self):
        for error in (ConnectionResetError('reset'), urllib.error.HTTPError('http://xrpla', 500, 'Internal', {}, None),
                      http.client.RemoteDisconnected('closed'), urllib.error.URLError(ConnectionRefusedError('refused'))):
            stats = self.compare(error)
            self.assertEqual((stats.get('timeouts'), stats.get('xrpla_errors')), (None, 1), error)


LEDGER = {'status': 'success', 'ledger_index': 5}
SERVER = {'status': 'success', 'version': {'first': '1.0.0', 'good': '1.0.0', 'last': '2.0.0'}}


class MatchingErrors(unittest.TestCase):
    def run_with(self, answer, xrpla=None, reference=None, **changed):
        """A whole run in which both servers answer version and ping alike and successfully, and
        give `answer` to every other request (or XRPLA what `xrpla(method)` returns or raises, the
        reference `reference(method)`); whether it passed, and its summary."""
        a = options(range=(5, 5), ledgers=1, include_ledgers=[], include_transactions=[], seed=1, transactions=4, accounts=2,
                    objects=2, books=2, clio_methods=False, clio=None, **changed)
        parity, printed = xrpld_parity.Parity(a), io.StringIO()
        def answering(given):
            def answer_to(url, method, params, waited):
                if method in xrpld_parity.SERVER_METHODS: return dict(SERVER), 0.01
                found = given(method) if given else answer
                if isinstance(found, BaseException): raise found
                return dict(found), 0.01
            return answer_to
        with mock.patch.object(xrpld_parity, 'reference_call', side_effect=answering(reference)), \
             mock.patch.object(xrpld_parity, 'server_call', side_effect=answering(xrpla)), \
             contextlib.redirect_stdout(printed):
            passed = parity.run()
        return passed, json.loads(printed.getvalue().splitlines()[-1])

    def test_the_same_error_is_counted_apart(self):
        parity = xrpld_parity.Parity(options())
        error = {'error': 'lgrNotFound', 'status': 'error'}
        with mock.patch.object(xrpld_parity, 'reference_call', return_value=(error, 0.01)), \
             mock.patch.object(xrpld_parity, 'server_call', return_value=(dict(error), 0.01)):
            parity.compare('ledger', ledger_index=5)
        stats = parity.stats['ledger']
        self.assertEqual((stats['same'], stats['different'], stats.get('same_errors')), (0, 0, 1))

    def test_a_run_of_matching_errors_fails(self):
        # version and ping match, every ledger request errs alike: nothing about a ledger compared.
        passed, summary = self.run_with({'error': 'lgrNotFound', 'status': 'error'})
        self.assertFalse(passed)
        self.assertEqual((summary['same'], summary['server_same']), (0, 3))
        self.assertGreater(summary['same_errors'], 0)

    def test_a_run_that_only_timed_out_fails(self):
        passed, summary = self.run_with(LEDGER, xrpla=lambda method: TimeoutError('timed out'))
        self.assertFalse(passed)
        self.assertEqual((summary['same'], summary['server_same'], summary['xrpla_errors']), (0, 3, 0))
        self.assertEqual(summary['unanswered'], summary['requests'])

    def test_a_run_the_reference_could_not_answer_fails(self):
        passed, summary = self.run_with(LEDGER, reference=lambda method: {'error': 'tooBusy', 'status': 'error'})
        self.assertFalse(passed)
        self.assertEqual((summary['same'], summary['server_same']), (0, 3))
        self.assertGreater(summary['reference_errors'], 0)

    def test_too_many_unanswered_fails(self):
        # The ledger listings and book_changes match; ledger_data (6 of the run's 12 ledger requests)
        # times out.
        timing_out = lambda method: TimeoutError('timed out') if method == 'ledger_data' else LEDGER
        passed, summary = self.run_with(LEDGER, xrpla=timing_out)
        self.assertFalse(passed)
        self.assertEqual((summary['same'], summary['unanswered'], summary['requests'], summary['unanswered_allowed']), (6, 6, 12, 1))
        passed, _ = self.run_with(LEDGER, xrpla=timing_out, max_unanswered=0.5)
        self.assertTrue(passed)

    def test_a_run_of_matching_answers_passes(self):
        passed, summary = self.run_with(LEDGER)
        self.assertTrue(passed)
        self.assertEqual(summary['same_errors'], 0)
        self.assertEqual((summary['server_same'], summary['unanswered']), (3, 0))

    def test_shares_are_between_0_and_1(self):
        self.assertEqual(xrpld_parity.share('0.25'), 0.25)
        for text in ('-0.1', '1.5'):
            with self.assertRaises(Exception): xrpld_parity.share(text)


class Spread(unittest.TestCase):
    def test_both_ends_are_included(self):
        items = list(range(100))
        picked = xrpld_parity.spread(items, 7)
        self.assertEqual((len(picked), picked[0], picked[-1]), (7, 0, 99))
        self.assertEqual(len(set(picked)), 7)
        self.assertEqual(picked, sorted(picked))

    def test_the_newest_is_reached_whatever_the_counts(self):
        for n in range(2, 60):
            for wanted in range(2, n):
                picked = xrpld_parity.spread(list(range(n)), wanted)
                self.assertEqual((len(picked), len(set(picked)), picked[0], picked[-1]), (wanted, wanted, 0, n - 1), (n, wanted))

    def test_few_items_are_all_taken(self):
        self.assertEqual(xrpld_parity.spread([1, 2, 3], 5), [1, 2, 3])
        self.assertEqual(xrpld_parity.spread([1, 2, 3], 1), [3])
        self.assertEqual(xrpld_parity.spread([1, 2, 3], 0), [])

    def test_deterministic(self):
        self.assertEqual(xrpld_parity.spread(list(range(1000)), 80), xrpld_parity.spread(list(range(1000)), 80))


class CampaignSummary(unittest.TestCase):
    def setUp(self):
        self.out = Path(tempfile.mkdtemp()); (self.out / 'intervals').mkdir()
        self.todo = [(1, 1_000_000), (1_000_001, 2_000_000)]

    def tearDown(self):
        shutil.rmtree(self.out)

    def interval(self, first, last, exit=0, attempts=(), **counts):
        summary = {'overlap': [first, last], 'same': 0, 'different': 0, 'xrpla_errors': 0, **counts}
        (self.out / 'intervals' / f'{first}-{last}.jsonl').write_text(json.dumps({'method': 'ledger', 'same': summary['same']}) + '\n' + json.dumps(summary) + '\n')
        with open(self.out / 'progress.jsonl', 'a') as f:
            for earlier in attempts: f.write(json.dumps({'interval': [first, last], 'exit': earlier, 'summary': None}) + '\n')
            f.write(json.dumps({'interval': [first, last], 'exit': exit, 'summary': summary}) + '\n')

    def test_all_compared_passes(self):
        self.interval(*self.todo[0], same=10)
        self.interval(*self.todo[1], exit=0, attempts=(1,), same=5)
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertTrue(passed, summary)
        self.assertEqual((summary['same'], summary['failed'], summary['missing']), (15, [], []))

    def test_a_failed_interval_fails(self):
        self.interval(*self.todo[0], same=10)
        self.interval(*self.todo[1], exit=1, same=5)
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': ['exit 1']}])

    def test_an_interval_that_compared_nothing_fails(self):
        self.interval(*self.todo[0], same=10)
        self.interval(*self.todo[1], same_errors=40)
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': ['compared nothing']}])
        self.assertEqual(summary['same_errors'], 40)

    def test_an_interval_without_a_recorded_exit_fails(self):
        self.interval(*self.todo[0], same=10)
        self.interval(*self.todo[1], same=5)
        (self.out / 'progress.jsonl').write_text('\n'.join((self.out / 'progress.jsonl').read_text().splitlines()[:1]) + '\n')
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': ['exit None']}])

    def test_version_and_ping_alone_compare_nothing(self):
        # An interval file whose summary counted version and ping as matching ledger answers.
        self.interval(*self.todo[0], same=10)
        summary = {'overlap': list(self.todo[1]), 'same': 3, 'same_errors': 40, 'different': 0, 'xrpla_errors': 0}
        lines = [{'method': 'ledger', 'same': 0, 'same_errors': 40}, {'method': 'ping', 'same': 1}, {'method': 'version', 'same': 2}, summary]
        (self.out / 'intervals' / '1000001-2000000.jsonl').write_text(''.join(json.dumps(l) + '\n' for l in lines))
        with open(self.out / 'progress.jsonl', 'a') as f: f.write(json.dumps({'interval': list(self.todo[1]), 'exit': 0, 'summary': summary}) + '\n')
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': ['compared nothing']}])
        self.assertEqual(summary['same'], 10)

    def test_a_progress_line_cut_short_is_skipped(self):
        self.interval(*self.todo[0], same=10)
        self.interval(*self.todo[1], same=5)
        progress = self.out / 'progress.jsonl'
        progress.write_text(progress.read_text()[:-20])
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['progress_unreadable_lines'], 1)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': ['exit None']}])

    def test_a_missing_interval_fails(self):
        self.interval(*self.todo[0], same=10)
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['missing'], [list(self.todo[1])])

    def test_no_matching_answer_in_all_fails(self):
        summary, passed = parity_campaign.summarize(self.out, [])
        self.assertFalse(passed)
        self.assertEqual(summary['same'], 0)

    def test_a_file_that_records_its_exit_needs_no_progress_entry(self):
        # A stop between writing the interval file and its progress entry: the file says how it ended.
        for (x, y), exit in zip(self.todo, (0, 1)):
            lines = [{'method': 'ledger', 'same': 5}, {'overlap': [x, y], 'same': 5, 'different': 0, 'xrpla_errors': 0}, {'exit': exit, 'attempt': 1}]
            (self.out / 'intervals' / f'{x}-{y}.jsonl').write_text(''.join(json.dumps(l) + '\n' for l in lines))
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': ['exit 1']}])
        path = self.out / 'intervals' / '1-1000000.jsonl'
        self.assertIsNone(parity_campaign.run_again(path, []))
        # One from before files kept their exit, with no progress entry, is not done.
        self.interval(*self.todo[0], same=10); (self.out / 'progress.jsonl').unlink()
        self.assertEqual(parity_campaign.run_again(path, []), 'unrecorded')

    def test_an_interval_failed_only_on_unanswered_requests_is_run_again(self):
        unanswered = dict(same=6, unanswered=6, requests=12, unanswered_allowed=1)
        self.interval(*self.todo[0], same=10)
        self.interval(*self.todo[1], exit=1, **unanswered)
        path = self.out / 'intervals' / '1000001-2000000.jsonl'
        records, _ = parity_campaign.progress_records(self.out)
        self.assertEqual(parity_campaign.run_again(path, records[self.todo[1]]), 'unanswered')
        # Not one with a difference or an error at XRPLA, as well.
        for other in (dict(different=1), dict(xrpla_errors=1), dict(transports={'different': 1}), dict(transports={'errors': 1})):
            self.interval(*self.todo[1], exit=1, **unanswered, **other)
            records, _ = parity_campaign.progress_records(self.out)
            self.assertIsNone(parity_campaign.run_again(path, records[self.todo[1]]), other)
        # RETRIES attempts in all: then it stands, and fails the run.
        self.interval(*self.todo[1], exit=1, **unanswered)
        for n in range(1, parity_campaign.RETRIES): (path.parent / f'{path.name}.unanswered-{n}').write_text('')
        records, _ = parity_campaign.progress_records(self.out)
        self.assertIsNone(parity_campaign.run_again(path, records[self.todo[1]]))
        summary, passed = parity_campaign.summarize(self.out, self.todo)
        self.assertFalse(passed)
        self.assertEqual(summary['failed'], [{'interval': list(self.todo[1]), 'reasons': [
            'exit 1', f'6 of 12 requests unanswered (1 allowed), after {parity_campaign.RETRIES} attempts']}])
        self.assertEqual(summary['run_again'], [{'interval': list(self.todo[1]), 'attempts': parity_campaign.RETRIES, 'passed': False}])


class CampaignResume(unittest.TestCase):
    """A resumed campaign keeps the intervals that ended, and runs again those that failed only on
    unanswered requests (the earlier file set aside, the attempt recorded) or record no exit."""
    def setUp(self):
        self.out = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.out)

    def campaign(self, unanswered):
        """One run over a server holding one interval, whose comparison leaves `unanswered` of 12
        requests unanswered (more than 1 fails it); the number of comparisons run."""
        summary = {'overlap': [1, 1_000_000], 'same': 12 - unanswered, 'different': 0, 'xrpla_errors': 0,
                   'requests': 12, 'unanswered': unanswered, 'unanswered_allowed': 1}
        stdout = json.dumps({'method': 'ledger', 'same': 12 - unanswered}) + '\n' + json.dumps(summary) + '\n'
        answer = lambda url, method, params: ({'ledger_index_min': 1, 'ledger_index_max': 1_000_000} if method == 'ledger_range' else {'info': {}}, 0.01)
        run = mock.Mock(return_value=SimpleNamespace(stdout=stdout, stderr='', returncode=0 if unanswered <= 1 else 1))
        argv = ['parity_campaign.py', '--xrpla', 'http://xrpla', '--reference', 'http://reference', '--out', str(self.out)]
        with mock.patch.object(parity_campaign, 'call', side_effect=answer), mock.patch.object(parity_campaign, 'generation', return_value=1), \
             mock.patch.object(parity_campaign.subprocess, 'run', run), mock.patch.object(sys, 'argv', argv), \
             contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
            parity_campaign.main()
        return run.call_count

    def test_unanswered_failures_are_run_again_a_bounded_number_of_times(self):
        self.assertEqual(self.campaign(unanswered=6), 1)
        for attempt in range(2, parity_campaign.RETRIES + 1): self.assertEqual(self.campaign(unanswered=6), 1, attempt)
        self.assertEqual(self.campaign(unanswered=6), 0)  # RETRIES made: it stands
        intervals = self.out / 'intervals'
        self.assertEqual(len(list(intervals.glob('1-1000000.jsonl.unanswered-*'))), parity_campaign.RETRIES - 1)
        entries = [json.loads(l) for l in (self.out / 'progress.jsonl').read_text().splitlines()]
        self.assertEqual([e.get('run_again') for e in entries], [None] + ['unanswered'] * (parity_campaign.RETRIES - 1))
        self.assertTrue(json.loads((self.out / 'summary.json').read_text())['failed'])

    def test_a_pass_after_an_outage_stands(self):
        self.campaign(unanswered=6); self.campaign(unanswered=0)
        self.assertEqual(self.campaign(unanswered=0), 0)
        summary = json.loads((self.out / 'summary.json').read_text())
        self.assertEqual((summary['failed'], summary['run_again']), ([], [{'interval': [1, 1_000_000], 'attempts': 2, 'passed': True}]))

    def test_an_interval_without_a_recorded_exit_is_run_again(self):
        self.campaign(unanswered=0)
        path = self.out / 'intervals' / '1-1000000.jsonl'
        path.write_text(''.join(l + '\n' for l in path.read_text().splitlines()[:-1]))  # a file from before files kept their exit
        (self.out / 'progress.jsonl').write_text('')  # and its progress entry lost
        self.assertEqual(self.campaign(unanswered=0), 1)
        self.assertTrue((self.out / 'intervals' / '1-1000000.jsonl.unrecorded-1').exists())


if __name__ == '__main__':
    unittest.main()
