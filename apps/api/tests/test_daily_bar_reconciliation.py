"""Next-day check that stored daily bars equal the official KRX table."""

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
from app.db_models import DailyPipelineRun, DailyPipelineStageRun, DailyPriceBar, Symbol
from app.eod import daily_bar_reconciliation as recon
from app.eod import runner as runner_module
from app.eod.daily_bar_reconciliation import (
    MAX_ATTEMPTS_PER_DATE,
    DailyBarReconciliation,
    compare_bars,
    summarize,
)
from app.eod.official_bar_restatement import OfficialReference
from app.eod.runner import DAILY_BAR_RECONCILIATION_MIN_INTERVAL, EODBackgroundRunner

DAY = date(2026, 11, 6)
NOW = datetime(2026, 11, 9, 8, 0, tzinfo=timezone.utc)  # Monday 17:00 KST
OFFICIAL = {
    '005930': (100, 110, 90, 105, 1_000, 105_000),
    '000660': (200, 220, 190, 210, 2_000, 420_000),
    '035720': (50, 55, 45, 52, 500, 26_000),
}


class _Loader:
    """Fake OfficialReference.load that records calls."""

    def __init__(self, rows: dict | None) -> None:
        self.rows = rows
        self.calls: list[tuple[list[date], Path]] = []

    def __call__(self, dates, *, cache_dir):
        dates = list(dates)
        self.calls.append((dates, cache_dir))
        if self.rows is None:
            return OfficialReference({}, covered_dates=())
        return OfficialReference({(s, d): v for d in dates for s, v in self.rows.items()}, covered_dates=dates)


class CompareBarsTests(unittest.TestCase):
    def test_identical_bars_have_no_mismatch(self) -> None:
        result = compare_bars(dict(OFFICIAL), OFFICIAL)
        self.assertEqual(result['compared_count'], 3)
        self.assertEqual(result['mismatch_count'], 0)
        self.assertEqual(result['mismatch_samples'], [])

    def test_counts_every_field_that_differs(self) -> None:
        stored = dict(OFFICIAL)
        stored['005930'] = (100, 110, 90, 104, 1_000, 0)  # provisional close, missing turnover
        stored['000660'] = (200, 221, 190, 210, 2_000, 420_000)
        result = compare_bars(stored, OFFICIAL)
        self.assertEqual(result['mismatch_count'], 2)
        self.assertEqual(
            result['mismatch_by_field'],
            {'open': 0, 'high': 1, 'low': 0, 'close': 1, 'volume': 0, 'turnover': 1},
        )
        sample = {item['symbol']: item for item in result['mismatch_samples']}
        self.assertEqual(sample['005930']['stored']['close'], 104)
        self.assertEqual(sample['005930']['official']['close'], 105)

    def test_universe_differences_are_reported_but_not_mismatches(self) -> None:
        stored = {'005930': OFFICIAL['005930'], '999999': (1, 1, 1, 1, 1, 1)}
        result = compare_bars(stored, OFFICIAL)
        self.assertEqual(result['mismatch_count'], 0)
        self.assertEqual(result['official_not_stored_count'], 2)
        self.assertEqual(result['stored_not_official_samples'], ['999999'])

    def test_finds_price_source_counters_nested_in_stage_details(self) -> None:
        details = {'progress': {'extra': {'price_source': {'official_market_frame_bars': 2700, 'per_symbol_bars': 3}}}}
        self.assertEqual(recon._find_price_source(details), {'official_market_frame_bars': 2700, 'per_symbol_bars': 3})
        self.assertIsNone(recon._find_price_source({'bars_upserted': 10}))


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def _service(self, *, stored=None, official=OFFICIAL, collection=None, finality_error=None):
        loader = _Loader(official)

        def finality(day, now):
            if finality_error is not None:
                raise finality_error

        service = DailyBarReconciliation(
            reports_dir=self.root / 'reports',
            frames_dir=self.root / 'frames',
            session_loader=lambda before: DAY,
            stored_loader=lambda day: dict(OFFICIAL if stored is None else stored),
            collection_loader=lambda day: collection,
            reference_loader=loader,
            finality_validator=finality,
        )
        return service, loader

    def test_matching_session_is_ok_and_recorded(self) -> None:
        collection = {'stage_status': 'completed', 'price_source': {'official_market_frame_bars': 3, 'per_symbol_bars': 0}}
        service, loader = self._service(collection=collection)
        with self.assertNoLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            result = service.run_if_due(now=NOW)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['warnings'], [])
        self.assertEqual(loader.calls, [([DAY], self.root / 'frames')])
        on_disk = json.loads((self.root / 'reports' / '2026-11-06.json').read_text(encoding='utf-8'))
        self.assertEqual(on_disk['status'], 'ok')
        self.assertEqual(on_disk['schema_version'], 'daily_bar_reconciliation_v1')

    def test_provisional_bars_are_flagged_with_a_warning(self) -> None:
        stored = dict(OFFICIAL)
        stored['005930'] = (100, 110, 90, 104, 1_000, 105_000)
        service, _ = self._service(stored=stored)
        with self.assertLogs('app.eod.daily_bar_reconciliation', level='WARNING') as logs:
            result = service.run_if_due(now=NOW)
        self.assertEqual(result['status'], 'mismatch')
        self.assertIn('stored_bars_differ_from_official', result['warnings'])
        self.assertIn('005930', logs.output[0])
        self.assertEqual(summarize(result)['mismatch_symbols'], ['005930'])

    def test_per_symbol_fallback_warns_even_when_values_match(self) -> None:
        collection = {'stage_status': 'completed', 'price_source': {'official_market_frame_bars': 2, 'per_symbol_bars': 1}}
        service, _ = self._service(collection=collection)
        with self.assertLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            result = service.run_if_due(now=NOW)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['warnings'], ['collection_used_per_symbol_fallback'])

    def test_session_collected_without_counters_is_marked(self) -> None:
        service, _ = self._service(collection={'stage_status': 'completed', 'price_source': None})
        self.assertIn('collection_price_source_unrecorded', service.run_if_due(now=NOW)['warnings'])

    def test_a_settled_session_is_not_fetched_again_even_after_restart(self) -> None:
        service, loader = self._service()
        service.run_if_due(now=NOW)
        service.run_if_due(now=NOW + timedelta(hours=1))
        restarted, restarted_loader = self._service()
        result = restarted.run_if_due(now=NOW + timedelta(hours=2))
        self.assertEqual(len(loader.calls), 1)
        self.assertEqual(restarted_loader.calls, [])
        self.assertEqual(result['status'], 'ok')

    def test_unavailable_reference_is_retried_a_bounded_number_of_times(self) -> None:
        service, loader = self._service(official=None)
        with self.assertLogs('app.eod.daily_bar_reconciliation', level='WARNING'):
            for hour in range(MAX_ATTEMPTS_PER_DATE + 2):
                result = service.run_if_due(now=NOW + timedelta(hours=hour))
        self.assertEqual(len(loader.calls), MAX_ATTEMPTS_PER_DATE)
        self.assertEqual(result['status'], 'reference_unavailable')
        self.assertEqual(result['attempt'], MAX_ATTEMPTS_PER_DATE)

    def test_unfinalized_session_is_not_checked(self) -> None:
        service, loader = self._service(finality_error=ValueError('not final'))
        self.assertIsNone(service.run_if_due(now=NOW))
        self.assertEqual(loader.calls, [])

    def test_no_stored_session_is_a_no_op(self) -> None:
        service, loader = self._service()
        service.session_loader = lambda before: None
        self.assertIsNone(service.run_if_due(now=NOW))
        self.assertEqual(loader.calls, [])


class DatabaseReaderTests(unittest.TestCase):
    """The two DB readers against the test database (rolled back)."""

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
        stored = recon.load_stored_bars(self.day)
        self.assertEqual(stored[code], (100, 110, 90, 105, 1000, 105_000))
        self.assertEqual(recon.load_latest_stored_session(self.day + timedelta(days=1)) >= self.day, True)

    def test_reads_the_latest_price_collection_counters(self) -> None:
        run = DailyPipelineRun(market_date=self.day, trigger='test', status='completed')
        self.session.add(run)
        self.session.flush()
        self.session.add(DailyPipelineStageRun(
            pipeline_run_id=run.id, market_date=self.day, stage_id='price_collection', status='completed',
            details={'extra': {'price_source': {'official_market_frame_bars': 2690, 'per_symbol_bars': 4}}},
        ))
        self.session.flush()
        self.assertEqual(
            recon.load_collection_price_source(self.day),
            {'stage_status': 'completed', 'price_source': {'official_market_frame_bars': 2690, 'per_symbol_bars': 4}},
        )
        self.assertIsNone(recon.load_collection_price_source(self.day - timedelta(days=1)))


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
        result = {'market_date': '2026-11-06', 'status': 'mismatch', 'warnings': ['stored_bars_differ_from_official'],
                  'compared_count': 3, 'mismatch_count': 1, 'mismatch_samples': [{'symbol': '005930'}],
                  'collection': {'price_source': {'per_symbol_bars': 0}}}
        with patch('app.eod.daily_bar_reconciliation.run_if_due', return_value=result):
            self.runner._reconcile_daily_bars_sync()
        snapshot = self.runner.snapshot()['daily_bar_reconciliation']
        self.assertEqual(snapshot['status'], 'mismatch')
        self.assertEqual(snapshot['mismatch_symbols'], ['005930'])
        self.assertEqual(snapshot['collection_price_source'], {'per_symbol_bars': 0})

    def test_runs_in_a_background_thread_one_at_a_time(self) -> None:
        calls = []

        def fake_sync():
            calls.append(1)
            return None

        async def scenario():
            with patch.object(self.runner, '_reconcile_daily_bars_sync', fake_sync):
                self.runner._start_daily_bar_reconciliation(now=NOW)
                self.runner._start_daily_bar_reconciliation(now=NOW)  # still in flight: ignored
                task = self.runner._blocking_step_tasks[runner_module.DAILY_BAR_RECONCILIATION_STEP]
                await task

        asyncio.run(scenario())
        self.assertEqual(calls, [1])
        self.assertEqual(self.runner._last_daily_bar_reconciliation_at, NOW)


class OfficialReferenceRowsTests(unittest.TestCase):
    def test_rows_for_returns_only_that_date(self) -> None:
        other = date(2026, 11, 5)
        reference = OfficialReference(
            {('005930', DAY): OFFICIAL['005930'], ('005930', other): (1, 1, 1, 1, 1, 1)},
            covered_dates={DAY, other},
        )
        self.assertEqual(reference.rows_for(DAY), {'005930': OFFICIAL['005930']})
        self.assertEqual(reference.rows_for(date(2026, 11, 4)), {})


if __name__ == '__main__':
    unittest.main()
