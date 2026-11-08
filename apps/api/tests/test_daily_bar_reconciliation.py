"""Next-day check that stored daily bars equal the KRX regular-session bars."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.orm import Session

from app.core.settings import settings
from app.db import engine
from app.db_models import DailyPriceBar, Symbol
from app.eod import daily_bar_reconciliation as recon
from app.eod import runner as runner_module
from app.eod.daily_bar_reconciliation import (
    MAX_ATTEMPTS_PER_DATE,
    SCHEMA_VERSION,
    DailyBarReconciliation,
    compare_bars,
    select_sample,
    summarize,
)
from app.eod.official_bar_restatement import OfficialReference
from app.eod.runner import DAILY_BAR_RECONCILIATION_MIN_INTERVAL, EODBackgroundRunner

DAY = date(2026, 11, 6)
NOW = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)  # Monday 17:00 KST
STORED = {
    '005930': (100, 110, 90, 105, 1_000),
    '000660': (200, 220, 190, 210, 2_000),
    '035720': (50, 55, 45, 52, 500),
}


class _Fetcher:
    """Fake KIS fetch that records calls; ``rows=None`` answers nothing."""

    def __init__(self, rows: dict | None) -> None:
        self.rows = rows
        self.calls: list[tuple[list[str], date]] = []

    def __call__(self, symbols, market_date):
        self.calls.append((list(symbols), market_date))
        if self.rows is None:
            return {}, {symbol: 'KISError: down' for symbol in symbols}
        return {s: self.rows[s] for s in symbols if s in self.rows}, {}


class SampleTests(unittest.TestCase):
    def test_sample_is_reproducible_and_bounded(self) -> None:
        symbols = [f'{n:06d}' for n in range(1000)]
        first = select_sample(symbols, DAY, 50)
        self.assertEqual(first, select_sample(reversed(symbols), DAY, 50))
        self.assertEqual(len(first), 50)
        self.assertEqual(first, sorted(first))

    def test_sample_changes_with_the_date(self) -> None:
        symbols = [f'{n:06d}' for n in range(1000)]
        self.assertNotEqual(select_sample(symbols, DAY, 50), select_sample(symbols, DAY - timedelta(days=1), 50))

    def test_small_universe_is_taken_whole(self) -> None:
        self.assertEqual(select_sample(STORED, DAY, 200), sorted(STORED))


class CompareBarsTests(unittest.TestCase):
    def test_identical_bars_have_no_mismatch(self) -> None:
        result = compare_bars(STORED, dict(STORED))
        self.assertEqual((result['compared_count'], result['price_mismatch_count']), (3, 0))

    def test_price_and_volume_differences_are_counted_apart(self) -> None:
        reference = dict(STORED)
        reference['005930'] = (100, 110, 90, 106, 1_000)  # price
        reference['000660'] = (200, 220, 190, 210, 2_045)  # volume only
        result = compare_bars(STORED, reference)
        self.assertEqual((result['price_mismatch_count'], result['volume_only_mismatch_count']), (1, 1))
        self.assertEqual(result['mismatch_by_field'], {'open': 0, 'high': 0, 'low': 0, 'close': 1, 'volume': 1})
        self.assertEqual([item['symbol'] for item in result['mismatch_samples']], ['005930'])
        self.assertEqual(result['mismatch_samples'][0]['reference']['close'], 106)
        self.assertEqual([item['symbol'] for item in result['volume_mismatch_samples']], ['000660'])

    def test_a_halted_symbol_is_compared_on_close_only(self) -> None:
        stored = {'009190': (0, 0, 0, 830, 0)}
        self.assertEqual(compare_bars(stored, {'009190': (830, 830, 830, 830, 0)})['price_mismatch_count'], 0)
        self.assertEqual(compare_bars(stored, {'009190': (835, 835, 835, 835, 0)})['price_mismatch_count'], 1)


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def _service(self, *, reference=STORED, finality_error=None, size=200):
        fetcher = _Fetcher(reference)

        def finality(day, now):
            if finality_error is not None:
                raise finality_error

        service = DailyBarReconciliation(
            reports_dir=self.root / 'reports',
            sample_size=lambda: size,
            session_loader=lambda before: DAY,
            stored_loader=lambda day: dict(STORED),
            reference_fetcher=fetcher,
            finality_validator=finality,
        )
        return service, fetcher

    def test_matching_session_is_ok_and_recorded(self) -> None:
        service, fetcher = self._service()
        with self.assertNoLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            result = service.run_if_due(now=NOW)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['reference'], 'naver_daily_chart')
        self.assertEqual(fetcher.calls, [(sorted(STORED), DAY)])
        on_disk = json.loads((self.root / 'reports' / '2026-11-06.json').read_text(encoding='utf-8'))
        self.assertEqual((on_disk['status'], on_disk['schema_version']), ('ok', SCHEMA_VERSION))

    def test_only_the_sample_is_fetched(self) -> None:
        service, fetcher = self._service(size=2)
        result = service.run_if_due(now=NOW)
        self.assertEqual(len(fetcher.calls[0][0]), 2)
        self.assertEqual((result['sample_count'], result['compared_count']), (2, 2))

    def test_a_wrong_stored_price_is_flagged_with_a_warning(self) -> None:
        reference = dict(STORED)
        reference['005930'] = (100, 110, 90, 104, 1_000)
        service, _ = self._service(reference=reference)
        with self.assertLogs('app.eod.daily_bar_reconciliation', level='WARNING') as logs:
            result = service.run_if_due(now=NOW)
        self.assertEqual(result['status'], 'mismatch')
        self.assertEqual(result['warnings'], ['stored_prices_differ_from_reference'])
        self.assertIn('005930', logs.output[0])
        self.assertEqual(summarize(result)['mismatch_symbols'], ['005930'])

    def test_a_volume_only_difference_is_noted_without_a_warning(self) -> None:
        reference = dict(STORED)
        reference['000660'] = (200, 220, 190, 210, 2_045)
        service, _ = self._service(reference=reference)
        with self.assertNoLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            result = service.run_if_due(now=NOW)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['warnings'], ['stored_volume_differs_from_reference'])

    def test_a_settled_session_is_not_fetched_again_even_after_restart(self) -> None:
        service, fetcher = self._service()
        service.run_if_due(now=NOW)
        service.run_if_due(now=NOW + timedelta(hours=1))
        restarted, restarted_fetcher = self._service()
        result = restarted.run_if_due(now=NOW + timedelta(hours=2))
        self.assertEqual(len(fetcher.calls), 1)
        self.assertEqual(restarted_fetcher.calls, [])
        self.assertEqual(result['status'], 'ok')

    def test_a_report_from_the_old_market_frame_reference_is_redone(self) -> None:
        (self.root / 'reports').mkdir(parents=True)
        (self.root / 'reports' / '2026-11-06.json').write_text(
            json.dumps({'schema_version': 'daily_bar_reconciliation_v1', 'status': 'mismatch'}), encoding='utf-8'
        )
        service, fetcher = self._service()
        self.assertEqual(service.run_if_due(now=NOW)['status'], 'ok')
        self.assertEqual(len(fetcher.calls), 1)

    def test_unavailable_reference_is_retried_a_bounded_number_of_times(self) -> None:
        service, fetcher = self._service(reference=None)
        with self.assertLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            for hour in range(MAX_ATTEMPTS_PER_DATE + 2):
                result = service.run_if_due(now=NOW + timedelta(hours=hour))
        self.assertEqual(len(fetcher.calls), MAX_ATTEMPTS_PER_DATE)
        self.assertEqual(result['status'], 'reference_unavailable')
        self.assertEqual(result['reference_error_count'], 3)

    def test_partial_reference_below_coverage_is_inconclusive(self) -> None:
        service, _ = self._service(reference={'005930': STORED['005930']})
        with self.assertLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            self.assertEqual(service.run_if_due(now=NOW)['status'], 'reference_unavailable')

    def test_unfinalized_session_is_not_checked(self) -> None:
        service, fetcher = self._service(finality_error=ValueError('not final'))
        self.assertIsNone(service.run_if_due(now=NOW))
        self.assertEqual(fetcher.calls, [])

    def test_no_stored_session_is_a_no_op(self) -> None:
        service, fetcher = self._service()
        service.session_loader = lambda before: None
        self.assertIsNone(service.run_if_due(now=NOW))
        self.assertEqual(fetcher.calls, [])


class _Response:
    def __init__(self, text: str) -> None:
        self._text = text

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._text.encode('utf-8')


NAVER_BODY = """ [['날짜', '시가', '고가', '저가', '종가', '거래량', '외국인소진율'],
\t\t
["20261105", 5250, 5400, 5100, 5270, 120543, 3.64],
\t\t
["20261106", 5050, 5250, 4980, 5110, 189025, 3.64]
]
"""


class NaverFetchTests(unittest.TestCase):
    def test_parses_the_row_of_the_requested_date_and_reports_failures(self) -> None:
        urls = []

        def opener(request, timeout):
            urls.append(request.full_url)
            if 'symbol=035720' in request.full_url:
                raise OSError('down')
            if 'symbol=000660' in request.full_url:
                return _Response("[['날짜'], [\"20261105\", 1, 1, 1, 1, 1, 0]]")
            return _Response(NAVER_BODY)

        sleeps = []
        bars, errors = recon.fetch_naver_bars(sorted(STORED), DAY, opener=opener, sleep=sleeps.append)
        self.assertEqual(bars, {'005930': (5050, 5250, 4980, 5110, 189025)})
        self.assertEqual(errors['000660'], 'no_row_for_date')
        self.assertIn('OSError', errors['035720'])
        self.assertEqual(len(sleeps), 2)
        self.assertTrue(all('startTime=20261106&endTime=20261106' in url for url in urls))

    def test_a_non_literal_body_is_an_error_not_code(self) -> None:
        def opener(request, timeout):
            return _Response("__import__('os').system('echo pwned')")

        bars, errors = recon.fetch_naver_bars(['005930'], DAY, opener=opener, sleep=lambda _: None)
        self.assertEqual(bars, {})
        self.assertIn('ValueError', errors['005930'])

    def test_malformed_numeric_row_does_not_abort_other_symbols(self) -> None:
        for bad_value in (None, 'not-a-price'):
            with self.subTest(bad_value=bad_value):
                def opener(request, timeout):
                    if 'symbol=000660' in request.full_url:
                        return _Response(repr([
                            ['date', 'open', 'high', 'low', 'close', 'volume'],
                            ['20261106', 100, 110, 90, bad_value, 1000],
                        ]))
                    return _Response(NAVER_BODY)

                bars, errors = recon.fetch_naver_bars(
                    ['000660', '005930'], DAY, opener=opener, sleep=lambda _: None
                )

                self.assertEqual(bars, {'005930': (5050, 5250, 4980, 5110, 189025)})
                self.assertEqual(set(errors), {'000660'})
                self.assertIn('Error:', errors['000660'])

    def test_incomplete_ohlcv_row_is_an_error_instead_of_a_bar(self) -> None:
        def opener(request, timeout):
            if 'symbol=000660' in request.full_url:
                return _Response("[['date'], ['20261106', 100, 110, 90, 105]]")
            return _Response(NAVER_BODY)

        bars, errors = recon.fetch_naver_bars(
            ['000660', '005930'], DAY, opener=opener, sleep=lambda _: None
        )

        self.assertEqual(bars, {'005930': (5050, 5250, 4980, 5110, 189025)})
        self.assertEqual(set(errors), {'000660'})
        self.assertIn('ValueError:', errors['000660'])


class DatabaseReaderTests(unittest.TestCase):
    """The DB readers against the test database (rolled back)."""

    def setUp(self) -> None:
        connection = engine.connect()
        transaction = connection.begin()
        self.session = Session(bind=connection)
        self.addCleanup(connection.close)
        self.addCleanup(transaction.rollback)
        self.addCleanup(self.session.close)
        session = self.session

        @contextmanager
        def rollback_only_session_scope(**_kwargs):
            yield session
            session.flush()

        scope = patch('app.db.session_scope', rollback_only_session_scope)
        scope.start()
        self.addCleanup(scope.stop)
        self.day = date(1999, 1, 4)

    def test_reads_stored_bars_as_integer_tuples(self) -> None:
        code = uuid4().hex[:6].upper()
        symbol = Symbol(symbol=code, name_ko='reconciliation', market='KOSDAQ', instrument_type='equity',
                        source='protected_symbol_source', is_active=True)
        self.session.add(symbol)
        self.session.flush()
        self.session.add(DailyPriceBar(symbol_id=symbol.id, trade_date=self.day, open_price=100, high_price=110,
                                       low_price=90, close_price=105, volume=1000, turnover=105_000, source='pykrx'))
        self.session.flush()
        self.assertEqual(recon.load_stored_bars(self.day)[code], (100, 110, 90, 105, 1000))
        self.assertGreaterEqual(recon.load_latest_stored_session(self.day + timedelta(days=1)), self.day)


class RunnerWiringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = EODBackgroundRunner(interval_seconds=60)
        idle = patch.object(runner_module.auto_pykrx_eod_scheduler.state, 'active_task', False)
        idle.start()
        self.addCleanup(idle.stop)

    def test_due_outside_the_close_window_only(self) -> None:
        for state in ('PREOPEN', 'OPEN', 'WEEKEND', 'HOLIDAY'):
            self.assertTrue(self.runner._daily_bar_reconciliation_due(SimpleNamespace(state=state), now=NOW), state)
        self.assertFalse(self.runner._daily_bar_reconciliation_due(SimpleNamespace(state='CLOSED'), now=NOW))
        self.assertFalse(self.runner._daily_bar_reconciliation_due(None, now=NOW))
        with patch.object(runner_module.auto_pykrx_eod_scheduler.state, 'active_task', True):
            self.assertFalse(self.runner._daily_bar_reconciliation_due(SimpleNamespace(state='OPEN'), now=NOW))
        with patch.object(settings, 'daily_bar_reconciliation_enabled', False):
            self.assertFalse(self.runner._daily_bar_reconciliation_due(SimpleNamespace(state='OPEN'), now=NOW))

    def test_spaced_by_the_minimum_interval(self) -> None:
        self.runner._last_daily_bar_reconciliation_at = NOW - DAILY_BAR_RECONCILIATION_MIN_INTERVAL + timedelta(minutes=1)
        self.assertFalse(self.runner._daily_bar_reconciliation_due(SimpleNamespace(state='OPEN'), now=NOW))
        self.runner._last_daily_bar_reconciliation_at = NOW - DAILY_BAR_RECONCILIATION_MIN_INTERVAL
        self.assertTrue(self.runner._daily_bar_reconciliation_due(SimpleNamespace(state='OPEN'), now=NOW))

    def test_result_is_exposed_in_the_snapshot(self) -> None:
        self.assertIsNone(self.runner.snapshot()['daily_bar_reconciliation'])
        result = {'market_date': '2026-11-06', 'status': 'mismatch', 'warnings': ['stored_prices_differ_from_reference'],
                  'compared_count': 3, 'price_mismatch_count': 1, 'volume_only_mismatch_count': 0,
                  'mismatch_samples': [{'symbol': '005930'}]}
        with patch('app.eod.daily_bar_reconciliation.run_if_due', return_value=result):
            self.runner._reconcile_daily_bars_sync()
        snapshot = self.runner.snapshot()['daily_bar_reconciliation']
        self.assertEqual(snapshot['status'], 'mismatch')
        self.assertEqual(snapshot['mismatch_symbols'], ['005930'])

    def test_runs_in_a_background_thread_one_at_a_time(self) -> None:
        calls = []

        def fake_sync():
            calls.append(1)
            return None

        async def scenario():
            with patch.object(self.runner, '_reconcile_daily_bars_sync', fake_sync):
                self.runner._start_daily_bar_reconciliation(now=NOW)
                self.runner._start_daily_bar_reconciliation(now=NOW)  # still in flight: ignored
                await self.runner._blocking_step_tasks[runner_module.DAILY_BAR_RECONCILIATION_STEP]

        asyncio.run(scenario())
        self.assertEqual(calls, [1])
        self.assertEqual(self.runner._last_daily_bar_reconciliation_at, NOW)


class OfficialReferenceRowsTests(unittest.TestCase):
    def test_rows_for_returns_only_that_date(self) -> None:
        other = date(2026, 11, 5)
        reference = OfficialReference(
            {('005930', DAY): (1, 2, 3, 4, 5, 6), ('005930', other): (1, 1, 1, 1, 1, 1)},
            covered_dates={DAY, other},
        )
        self.assertEqual(reference.rows_for(DAY), {'005930': (1, 2, 3, 4, 5, 6)})
        self.assertEqual(reference.rows_for(date(2026, 11, 4)), {})


if __name__ == '__main__':
    unittest.main()
