import hashlib
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import delete, select
from sqlalchemy.dialects import postgresql

from app.db import engine, session_scope
from app.db_models import (
    CurrentKISIntradaySnapshotCache,
    DailyPriceBar,
    DailyPykrxEnrichmentRow,
    EODCollectionRun,
    KISIntradaySnapshot,
    Symbol,
)
from app.market_data_store import (
    _build_daily_market_relative_rows_query,
    _build_prior_market_turnover_bars_query,
    _build_prior_market_turnover_baselines_query,
    load_current_market_snapshot_context,
    load_daily_market_relative_contexts,
    load_daily_pykrx_enrichment_rows,
    load_prior_market_turnover_baselines,
    load_current_kis_intraday_snapshot,
    persist_daily_price_bar,
    persist_daily_price_bars,
    persist_kis_intraday_snapshot,
    upsert_daily_pykrx_enrichment_rows,
)


def _market_membership(by_market: dict[str, list[str]]) -> dict:
    return {
        'schema_version': 'pykrx_market_membership_v1',
        'membership_basis': 'end_date_snapshot',
        'symbol_count': sum(len(symbols) for symbols in by_market.values()),
        'markets': {
            market: {
                'symbols': symbols,
                'symbol_count': len(symbols),
                'membership_sha256': hashlib.sha256(
                    '\n'.join(sorted(symbols)).encode('utf-8')
                ).hexdigest(),
            }
            for market, symbols in by_market.items()
        },
    }


class HistoricalPriceBarPersistenceTests(unittest.TestCase):
    @staticmethod
    def _bar() -> dict:
        return {
            'trade_date': '2020-01-02',
            'open_price': 100,
            'high_price': 110,
            'low_price': 90,
            'close_price': 105,
            'volume': 1_000,
            'turnover': 105_000,
        }

    def test_historical_bar_write_preserves_existing_current_symbol_state(self) -> None:
        existing = SimpleNamespace(
            id=91,
            name_ko='Current name',
            standard_code='CURRENT-CODE',
            market='CURRENT-MARKET',
            instrument_type='current-type',
            source='current-source',
            is_active=False,
            last_refreshed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )

        class FakeSession:
            def execute(self, _statement):
                return None

        fake_session = FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with (
            patch('app.market_data_store.session_scope', fake_session_scope),
            patch('app.market_data_store._get_symbol', return_value=existing),
        ):
            persisted = persist_daily_price_bars(
                '005930',
                [self._bar()],
                market='HISTORICAL-MARKET',
                source='historical-source',
                name_ko='Historical name',
                standard_code='HISTORICAL-CODE',
                update_current_symbol_state=False,
            )

        self.assertEqual(persisted, 1)
        self.assertEqual(existing.name_ko, 'Current name')
        self.assertEqual(existing.standard_code, 'CURRENT-CODE')
        self.assertEqual(existing.market, 'CURRENT-MARKET')
        self.assertEqual(existing.instrument_type, 'current-type')
        self.assertEqual(existing.source, 'current-source')
        self.assertFalse(existing.is_active)
        self.assertEqual(
            existing.last_refreshed_at,
            datetime(2026, 9, 1, tzinfo=timezone.utc),
        )

    def test_historical_bar_write_creates_unknown_symbol_as_inactive(self) -> None:
        class FakeSession:
            def __init__(self) -> None:
                self.added = []

            def add(self, row) -> None:
                self.added.append(row)

            def flush(self) -> None:
                self.added[0].id = 92

            def execute(self, _statement):
                return None

        fake_session = FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with (
            patch('app.market_data_store.session_scope', fake_session_scope),
            patch('app.market_data_store._get_symbol', return_value=None),
        ):
            persisted = persist_daily_price_bars(
                '099999',
                [self._bar()],
                market='KOSPI',
                source='pykrx',
                name_ko='Historical listing',
                update_current_symbol_state=False,
            )

        self.assertEqual(persisted, 1)
        self.assertEqual(len(fake_session.added), 1)
        created_symbol = fake_session.added[0]
        self.assertEqual(created_symbol.symbol, '099999')
        self.assertEqual(created_symbol.name_ko, 'Historical listing')
        self.assertFalse(created_symbol.is_active)


class MarketRelativeQueryShapeTests(unittest.TestCase):
    def test_market_relative_rows_query_uses_bounded_per_market_trade_dates(self) -> None:
        statement = _build_daily_market_relative_rows_query(
            target_markets=['KOSPI', 'KOSDAQ'],
            market_date=date(2026, 4, 8),
        )

        compiled = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={'literal_binds': True},
            )
        ).lower()

        self.assertIn('join lateral', compiled)
        self.assertIn('limit 2', compiled)
        self.assertIn("daily_price_bars.trade_date <= '2026-04-08'", compiled)
        self.assertNotIn('row_number', compiled)

    def test_prior_market_turnover_queries_use_completed_run_and_eod_run_index(self) -> None:
        statement = _build_prior_market_turnover_baselines_query(
            markets=['KOSPI', 'KOSDAQ'],
            market_date=date(2026, 4, 8),
        )
        bars_statement = _build_prior_market_turnover_bars_query(
            run_id=73,
            trade_date=date(2026, 4, 7),
            markets=['KOSPI', 'KOSDAQ'],
        )

        compiled = str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={'literal_binds': True},
            )
        ).lower()

        compiled_bars = str(
            bars_statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={'literal_binds': True},
            )
        ).lower()

        self.assertNotIn('join lateral', compiled)
        self.assertIn("eod_collection_runs.trade_date < '2026-04-08'", compiled)
        self.assertIn("eod_collection_runs.source = 'pykrx'", compiled)
        self.assertIn("eod_collection_runs.status = 'completed'", compiled)
        self.assertIn('eod_collection_runs.symbols_failed = 0', compiled)
        self.assertIn("run_metadata ->> 'limited_run'", compiled)
        self.assertIn("run_metadata -> 'universe_snapshot'", compiled)
        self.assertIn('limit 8', compiled)
        self.assertIn('daily_price_bars.eod_run_id = 73', compiled_bars)
        self.assertNotIn("daily_price_bars.trade_date = '2026-04-07'", compiled_bars)
        self.assertIn('daily_price_bars.trade_date', compiled_bars)
        self.assertIn("daily_price_bars.source = 'pykrx'", compiled_bars)

    def test_load_current_market_snapshot_context_preserves_cache_provenance(self) -> None:
        cached_at = datetime(2026, 4, 8, 0, 10, tzinfo=timezone.utc)
        row = SimpleNamespace(
            payload={
                'status': 'ready',
                'as_of': '2026-04-08T09:10:00+09:00',
                'markets': {'kospi': {'index_code': '0001', 'value': 2700.0}},
            },
            snapshot_status='ready',
            stale_reason=None,
            as_of=cached_at,
            last_refresh_attempted_at=cached_at,
            last_refresh_succeeded_at=cached_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='background_runner:open_drive',
            updated_at=cached_at,
        )

        class FakeResult:
            def scalar_one_or_none(self):
                return row

        class FakeSession:
            def execute(self, _statement):
                return FakeResult()

        @contextmanager
        def fake_session_scope():
            yield FakeSession()

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_current_market_snapshot_context()

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result['source'], 'kis_market_snapshot_v1')
        self.assertEqual(result['source_table'], 'current_market_snapshot_cache')
        self.assertEqual(result['as_of'], '2026-04-08T00:10:00+00:00')
        self.assertEqual(result['last_refresh_status'], 'success')
        self.assertEqual(result['payload']['markets']['kospi']['index_code'], '0001')

    def test_load_prior_market_turnover_baselines_accepts_point_in_time_historical_run(self) -> None:
        run_metadata = {
            'mode': 'historical_backfill',
            'markets': ['KOSPI'],
            'symbol': None,
            'limited_run': False,
            'requested_symbols': ['005930', '000660'],
            'failed_symbols': [],
            'unattempted_symbols': [],
            'universe_snapshot': {
                'source': 'pykrx',
                'complete': True,
                'markets': ['KOSPI'],
                'symbol_count': 2,
                'persisted_count': 0,
                'latest_trade_date': '2026-04-07',
                'membership_basis': 'end_date_snapshot',
                'unavailable_markets': [],
                'point_in_time': True,
                'current_universe_mutated': False,
                'market_membership': _market_membership(
                    {'KOSPI': ['005930', '000660']}
                ),
            },
        }
        responses = iter(
            (
                [
                    SimpleNamespace(
                        _mapping={
                            'run_id': 73,
                            'trade_date': date(2026, 4, 7),
                            'status': 'completed',
                            'symbols_total': 2,
                            'symbols_succeeded': 2,
                            'symbols_failed': 0,
                            'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                            'run_metadata': run_metadata,
                        }
                    )
                ],
                [
                    SimpleNamespace(_mapping={'symbol': '005930', 'market': 'KOSPI', 'trade_date': date(2026, 4, 7), 'turnover_krw': 5_000}),
                    SimpleNamespace(_mapping={'symbol': '000660', 'market': 'KOSPI', 'trade_date': date(2026, 4, 7), 'turnover_krw': 3_000}),
                ],
            )
        )

        class FakeResult:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return self.rows

        class FakeSession:
            def execute(self, _statement):
                return FakeResult(next(responses))

        @contextmanager
        def fake_session_scope():
            yield FakeSession()

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI'],
            )

        self.assertEqual(result['KOSPI']['trade_date'], '2026-04-07')
        self.assertEqual(result['KOSPI']['turnover_krw'], 8_000)
        self.assertEqual(result['KOSPI']['coverage_ratio'], 1.0)
        self.assertEqual(result['KOSPI']['source'], 'pykrx')
        self.assertEqual(result['KOSPI']['source_table'], 'daily_price_bars')
        self.assertEqual(result['KOSPI']['eod_run_id'], 73)
        self.assertEqual(result['KOSPI']['coverage_proof']['membership_status'], 'exact')
        expected_membership_sha256 = hashlib.sha256(
            b'000660\n005930'
        ).hexdigest()
        self.assertEqual(
            result['KOSPI']['coverage_proof']['requested_membership_sha256'],
            expected_membership_sha256,
        )
        self.assertEqual(
            result['KOSPI']['coverage_proof']['persisted_membership_sha256'],
            expected_membership_sha256,
        )
        self.assertEqual(result['KOSPI']['coverage_proof']['universe_source'], 'pykrx')
        self.assertEqual(
            result['KOSPI']['coverage_proof']['membership_basis'],
            'end_date_snapshot',
        )

    def test_load_prior_market_turnover_baselines_rejects_same_count_wrong_symbol_set(self) -> None:
        run_metadata = {
            'mode': 'historical_backfill',
            'markets': ['KOSPI'],
            'symbol': None,
            'limited_run': False,
            'requested_symbols': ['005930', '000660'],
            'failed_symbols': [],
            'unattempted_symbols': [],
            'universe_snapshot': {
                'source': 'pykrx',
                'complete': True,
                'markets': ['KOSPI'],
                'symbol_count': 2,
                'persisted_count': 0,
                'latest_trade_date': '2026-04-07',
                'membership_basis': 'end_date_snapshot',
                'unavailable_markets': [],
                'point_in_time': True,
                'current_universe_mutated': False,
                'market_membership': _market_membership(
                    {'KOSPI': ['005930', '000660']}
                ),
            },
        }
        responses = iter(
            (
                [
                    SimpleNamespace(
                        _mapping={
                            'run_id': 73,
                            'trade_date': date(2026, 4, 7),
                            'status': 'completed',
                            'symbols_total': 2,
                            'symbols_succeeded': 2,
                            'symbols_failed': 0,
                            'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                            'run_metadata': run_metadata,
                        }
                    )
                ],
                [
                    SimpleNamespace(_mapping={'symbol': '000660', 'market': 'KOSPI', 'trade_date': date(2026, 4, 7), 'turnover_krw': 3_000}),
                    SimpleNamespace(_mapping={'symbol': '035420', 'market': 'KOSPI', 'trade_date': date(2026, 4, 7), 'turnover_krw': 5_000}),
                ],
            )
        )

        class FakeResult:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return self.rows

        class FakeSession:
            def execute(self, _statement):
                return FakeResult(next(responses))

        @contextmanager
        def fake_session_scope():
            yield FakeSession()

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI'],
            )

        self.assertEqual(result, {})

    def test_prior_turnover_uses_eod_market_membership_not_current_symbol_market(self) -> None:
        run_metadata = {
            'markets': ['KOSPI', 'KOSDAQ'],
            'symbol': None,
            'limited_run': False,
            'requested_symbols': ['005930', '247540'],
            'failed_symbols': [],
            'unattempted_symbols': [],
            'universe_snapshot': {
                'source': 'pykrx',
                'complete': True,
                'markets': ['KOSPI', 'KOSDAQ'],
                'symbol_count': 2,
                'persisted_count': 2,
                'latest_trade_date': '2026-04-07',
                'membership_basis': 'end_date_snapshot',
                'unavailable_markets': [],
                'market_membership': {
                    'schema_version': 'pykrx_market_membership_v1',
                    'membership_basis': 'end_date_snapshot',
                    'symbol_count': 2,
                    'markets': {
                        'KOSPI': {
                            'symbols': ['005930'],
                            'symbol_count': 1,
                            'membership_sha256': hashlib.sha256(b'005930').hexdigest(),
                        },
                        'KOSDAQ': {
                            'symbols': ['247540'],
                            'symbol_count': 1,
                            'membership_sha256': hashlib.sha256(b'247540').hexdigest(),
                        },
                    },
                },
            },
        }
        responses = iter(
            (
                [
                    SimpleNamespace(
                        _mapping={
                            'run_id': 73,
                            'trade_date': date(2026, 4, 7),
                            'status': 'completed',
                            'symbols_total': 2,
                            'symbols_succeeded': 2,
                            'symbols_failed': 0,
                            'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                            'run_metadata': run_metadata,
                        }
                    )
                ],
                [
                    SimpleNamespace(
                        _mapping={
                            'symbol': '005930',
                            'market': 'KOSDAQ',
                            'trade_date': date(2026, 4, 7),
                            'turnover_krw': 5_000,
                        }
                    ),
                    SimpleNamespace(
                        _mapping={
                            'symbol': '247540',
                            'market': 'KOSPI',
                            'trade_date': date(2026, 4, 7),
                            'turnover_krw': 3_000,
                        }
                    ),
                ],
            )
        )

        class FakeResult:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return self.rows

        class FakeSession:
            def execute(self, _statement):
                return FakeResult(next(responses))

        @contextmanager
        def fake_session_scope():
            yield FakeSession()

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI', 'KOSDAQ'],
            )

        self.assertEqual(result['KOSPI']['turnover_krw'], 5_000)
        self.assertEqual(result['KOSDAQ']['turnover_krw'], 3_000)

    def test_prior_turnover_rejects_legacy_run_without_market_membership_proof(self) -> None:
        run_metadata = {
            'markets': ['KOSPI'],
            'symbol': None,
            'limited_run': False,
            'requested_symbols': ['005930'],
            'failed_symbols': [],
            'unattempted_symbols': [],
            'universe_snapshot': {
                'source': 'pykrx',
                'complete': True,
                'markets': ['KOSPI'],
                'symbol_count': 1,
                'persisted_count': 1,
                'latest_trade_date': '2026-04-07',
                'membership_basis': 'end_date_snapshot',
                'unavailable_markets': [],
            },
        }
        candidate = SimpleNamespace(
            _mapping={
                'run_id': 73,
                'trade_date': date(2026, 4, 7),
                'status': 'completed',
                'symbols_total': 1,
                'symbols_succeeded': 1,
                'symbols_failed': 0,
                'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                'run_metadata': run_metadata,
            }
        )

        class FakeResult:
            def all(self):
                return [candidate]

        class FakeSession:
            def __init__(self):
                self.execute_count = 0

            def execute(self, _statement):
                self.execute_count += 1
                return FakeResult()

        fake_session = FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI'],
            )

        self.assertEqual(result, {})
        self.assertEqual(fake_session.execute_count, 1)

    def test_incomplete_eod_universe_is_rejected_before_bar_read(self) -> None:
        candidate = SimpleNamespace(
            _mapping={
                'run_id': 73,
                'trade_date': date(2026, 4, 7),
                'status': 'completed',
                'symbols_total': 2,
                'symbols_succeeded': 2,
                'symbols_failed': 0,
                'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                'run_metadata': {
                    'markets': ['KOSPI'],
                    'symbol': None,
                    'limited_run': False,
                    'requested_symbols': ['005930', '000660'],
                    'failed_symbols': [],
                    'unattempted_symbols': [],
                    'universe_snapshot': {
                        'source': 'pykrx',
                        'complete': False,
                        'markets': ['KOSPI'],
                        'symbol_count': 2,
                        'persisted_count': 2,
                        'latest_trade_date': '2026-04-07',
                        'membership_basis': 'end_date_snapshot',
                        'unavailable_markets': ['KOSPI'],
                    },
                },
            }
        )

        class FakeResult:
            def all(self):
                return [candidate]

        class FakeSession:
            def __init__(self):
                self.execute_count = 0

            def execute(self, _statement):
                self.execute_count += 1
                return FakeResult()

        fake_session = FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI'],
            )

        self.assertEqual(result, {})
        self.assertEqual(fake_session.execute_count, 1)

    def test_malformed_eod_coverage_metadata_fails_closed_before_bar_read(self) -> None:
        candidate = SimpleNamespace(
            _mapping={
                'run_id': 73,
                'trade_date': date(2026, 4, 7),
                'status': 'completed',
                'symbols_total': 'not-a-count',
                'symbols_succeeded': 2,
                'symbols_failed': 0,
                'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                'run_metadata': {
                    'markets': ['KOSPI'],
                    'symbol': None,
                    'limited_run': False,
                    'requested_symbols': ['005930', '000660'],
                    'failed_symbols': [],
                    'unattempted_symbols': [],
                    'universe_snapshot': {
                        'complete': True,
                        'markets': ['KOSPI'],
                        'symbol_count': 2,
                        'latest_trade_date': 'invalid-date',
                        'unavailable_markets': [],
                    },
                },
            }
        )

        class FakeResult:
            def all(self):
                return [candidate]

        class FakeSession:
            def __init__(self):
                self.execute_count = 0

            def execute(self, _statement):
                self.execute_count += 1
                return FakeResult()

        fake_session = FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI'],
            )

        self.assertEqual(result, {})
        self.assertEqual(fake_session.execute_count, 1)

    def test_completed_eod_turnover_loader_is_two_indexed_reads_at_operating_scale(self) -> None:
        requested_symbols = [f'{index:06d}' for index in range(4000)]
        run_metadata = {
            'markets': ['KOSPI', 'KOSDAQ'],
            'symbol': None,
            'limited_run': False,
            'requested_symbols': requested_symbols,
            'failed_symbols': [],
            'unattempted_symbols': [],
            'universe_snapshot': {
                'source': 'pykrx',
                'complete': True,
                'markets': ['KOSPI', 'KOSDAQ'],
                'symbol_count': 4000,
                'persisted_count': 4000,
                'latest_trade_date': '2026-04-07',
                'membership_basis': 'end_date_snapshot',
                'unavailable_markets': [],
                'market_membership': _market_membership(
                    {
                        'KOSPI': requested_symbols[:2000],
                        'KOSDAQ': requested_symbols[2000:],
                    }
                ),
            },
        }
        candidate = SimpleNamespace(
            _mapping={
                'run_id': 73,
                'trade_date': date(2026, 4, 7),
                'status': 'completed',
                'symbols_total': 4000,
                'symbols_succeeded': 4000,
                'symbols_failed': 0,
                'completed_at': datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc),
                'run_metadata': run_metadata,
            }
        )
        bars = [
            SimpleNamespace(
                _mapping={
                    'symbol': symbol,
                    'market': 'KOSPI' if index < 2000 else 'KOSDAQ',
                    'trade_date': date(2026, 4, 7),
                    'turnover_krw': 1_000,
                }
            )
            for index, symbol in enumerate(requested_symbols)
        ]
        responses = iter(([candidate], bars))

        class FakeResult:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return self.rows

        class FakeSession:
            def __init__(self):
                self.execute_count = 0

            def execute(self, _statement):
                self.execute_count += 1
                return FakeResult(next(responses))

        fake_session = FakeSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        started = perf_counter()
        with patch('app.market_data_store.session_scope', fake_session_scope):
            result = load_prior_market_turnover_baselines(
                market_date=date(2026, 4, 8),
                markets=['KOSPI', 'KOSDAQ'],
            )
        elapsed = perf_counter() - started

        self.assertEqual(fake_session.execute_count, 2)
        self.assertEqual(result['KOSPI']['expected_symbol_count'], 2000)
        self.assertEqual(result['KOSDAQ']['expected_symbol_count'], 2000)
        self.assertLess(elapsed, 0.5)


class MarketRelativeContextIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        EODCollectionRun.__table__.create(bind=engine, checkfirst=True)
        Symbol.__table__.create(bind=engine, checkfirst=True)
        DailyPriceBar.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.kospi_market = 'TEST_KOSPI_319'
        self.kosdaq_market = 'TEST_KOSDAQ_319'
        self.symbols = ['236101', '236102', '336101', '336102', '336103']
        self._seed_rows()

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(delete(DailyPriceBar).where(DailyPriceBar.symbol_id.in_(symbol_ids)))
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))

    def test_load_daily_market_relative_contexts_uses_current_and_prior_trade_dates_per_market(self) -> None:
        items = load_daily_market_relative_contexts(
            market_date='2026-04-08',
            symbols=['236101', '336101', '336102', '336103'],
        )
        items_by_symbol = {item['symbol']: item for item in items}

        kospi_item = items_by_symbol['236101']
        self.assertEqual(kospi_item['status'], 'available')
        self.assertEqual(kospi_item['market'], self.kospi_market)
        self.assertEqual(kospi_item['breadth']['market_symbol_count'], 2)
        self.assertEqual(kospi_item['breadth']['up_count'], 1)
        self.assertEqual(kospi_item['breadth']['down_count'], 1)
        self.assertEqual(kospi_item['breadth']['missing_previous_close_count'], 0)
        self.assertEqual(kospi_item['relative_strength']['return_pct'], 10.0)
        self.assertEqual(kospi_item['relative_strength']['return_rank'], 1)
        self.assertEqual(kospi_item['relative_strength']['return_population'], 2)
        self.assertEqual(kospi_item['relative_strength']['volume_rank'], 2)
        self.assertEqual(kospi_item['relative_strength']['volume_population'], 2)

        kosdaq_available_item = items_by_symbol['336101']
        self.assertEqual(kosdaq_available_item['status'], 'available')
        self.assertEqual(kosdaq_available_item['market'], self.kosdaq_market)
        self.assertEqual(kosdaq_available_item['breadth']['market_symbol_count'], 2)
        self.assertEqual(kosdaq_available_item['breadth']['up_count'], 1)
        self.assertEqual(kosdaq_available_item['breadth']['down_count'], 0)
        self.assertEqual(kosdaq_available_item['breadth']['missing_previous_close_count'], 1)
        self.assertEqual(kosdaq_available_item['relative_strength']['return_pct'], 5.0)
        self.assertEqual(kosdaq_available_item['relative_strength']['return_rank'], 1)
        self.assertEqual(kosdaq_available_item['relative_strength']['return_population'], 1)
        self.assertEqual(kosdaq_available_item['relative_strength']['volume_rank'], 2)
        self.assertEqual(kosdaq_available_item['relative_strength']['volume_population'], 2)

        kosdaq_partial_item = items_by_symbol['336102']
        self.assertEqual(kosdaq_partial_item['status'], 'partial')
        self.assertEqual(kosdaq_partial_item['market'], self.kosdaq_market)
        self.assertEqual(kosdaq_partial_item['breadth']['missing_previous_close_count'], 1)
        self.assertIsNone(kosdaq_partial_item['relative_strength']['return_pct'])
        self.assertIsNone(kosdaq_partial_item['relative_strength']['return_rank'])
        self.assertEqual(kosdaq_partial_item['relative_strength']['return_population'], 1)
        self.assertEqual(kosdaq_partial_item['relative_strength']['volume_rank'], 1)
        self.assertEqual(kosdaq_partial_item['relative_strength']['volume_population'], 2)

        kosdaq_unavailable_item = items_by_symbol['336103']
        self.assertEqual(kosdaq_unavailable_item['status'], 'unavailable')
        self.assertEqual(kosdaq_unavailable_item['market'], self.kosdaq_market)
        self.assertIsNone(kosdaq_unavailable_item['breadth'])
        self.assertIsNone(kosdaq_unavailable_item['relative_strength'])

    def _seed_rows(self) -> None:
        captured_at = datetime(2026, 4, 8, 6, 0, tzinfo=timezone.utc)
        with session_scope() as session:
            symbol_rows = [
                Symbol(
                    symbol='236101',
                    name_ko='Market Alpha',
                    market=self.kospi_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='236102',
                    name_ko='Market Beta',
                    market=self.kospi_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='336101',
                    name_ko='Market Gamma',
                    market=self.kosdaq_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='336102',
                    name_ko='Market Delta',
                    market=self.kosdaq_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
                Symbol(
                    symbol='336103',
                    name_ko='Market Epsilon',
                    market=self.kosdaq_market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                ),
            ]
            session.add_all(symbol_rows)
            session.flush()

            symbol_id_by_symbol = {row.symbol: row.id for row in symbol_rows}
            session.add_all(
                [
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236101'],
                        trade_date=date(2026, 4, 8),
                        open_price=105,
                        high_price=111,
                        low_price=104,
                        close_price=110,
                        volume=1500,
                        turnover=165000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236101'],
                        trade_date=date(2026, 4, 7),
                        open_price=98,
                        high_price=101,
                        low_price=97,
                        close_price=100,
                        volume=1400,
                        turnover=140000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236101'],
                        trade_date=date(2026, 4, 1),
                        open_price=68,
                        high_price=71,
                        low_price=67,
                        close_price=70,
                        volume=900,
                        turnover=63000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236102'],
                        trade_date=date(2026, 4, 8),
                        open_price=95,
                        high_price=96,
                        low_price=89,
                        close_price=90,
                        volume=2500,
                        turnover=225000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['236102'],
                        trade_date=date(2026, 4, 7),
                        open_price=102,
                        high_price=103,
                        low_price=99,
                        close_price=100,
                        volume=2400,
                        turnover=240000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336101'],
                        trade_date=date(2026, 4, 8),
                        open_price=205,
                        high_price=211,
                        low_price=204,
                        close_price=210,
                        volume=500,
                        turnover=105000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336101'],
                        trade_date=date(2026, 4, 6),
                        open_price=198,
                        high_price=201,
                        low_price=197,
                        close_price=200,
                        volume=450,
                        turnover=90000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336101'],
                        trade_date=date(2026, 4, 4),
                        open_price=148,
                        high_price=151,
                        low_price=147,
                        close_price=150,
                        volume=430,
                        turnover=64500,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336102'],
                        trade_date=date(2026, 4, 8),
                        open_price=175,
                        high_price=181,
                        low_price=174,
                        close_price=180,
                        volume=800,
                        turnover=144000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336102'],
                        trade_date=date(2026, 4, 4),
                        open_price=158,
                        high_price=161,
                        low_price=157,
                        close_price=160,
                        volume=700,
                        turnover=112000,
                        source='test',
                        captured_at=captured_at,
                    ),
                    DailyPriceBar(
                        symbol_id=symbol_id_by_symbol['336103'],
                        trade_date=date(2026, 4, 6),
                        open_price=118,
                        high_price=121,
                        low_price=117,
                        close_price=120,
                        volume=300,
                        turnover=36000,
                        source='test',
                        captured_at=captured_at,
                    ),
                ]
            )


class PykrxEnrichmentStoreIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Symbol.__table__.create(bind=engine, checkfirst=True)
        DailyPriceBar.__table__.create(bind=engine, checkfirst=True)
        DailyPykrxEnrichmentRow.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.symbols = ['661501', '661502']

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(
                session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all()
            )
            if symbol_ids:
                session.execute(delete(DailyPriceBar).where(DailyPriceBar.symbol_id.in_(symbol_ids)))
                session.execute(delete(DailyPykrxEnrichmentRow).where(DailyPykrxEnrichmentRow.symbol_id.in_(symbol_ids)))
            session.execute(delete(DailyPykrxEnrichmentRow).where(DailyPykrxEnrichmentRow.scope_key.in_(self.symbols)))

    def test_upsert_and_load_daily_pykrx_enrichment_rows(self) -> None:
        inserted = upsert_daily_pykrx_enrichment_rows(
            rows=[
                {
                    'market_date': '2026-04-21',
                    'market': 'KOSPI',
                    'category': 'valuation_snapshot',
                    'scope_type': 'symbol',
                    'scope_key': '661501',
                    'source': 'pykrx',
                    'symbol': '661501',
                    'name_ko': 'Value Alpha',
                    'standard_code': 'KR7661501001',
                    'payload': {'per': 11.2, 'pbr': 1.3},
                },
                {
                    'market_date': '2026-04-21',
                    'market': 'KOSPI',
                    'category': 'foreign_ownership',
                    'scope_type': 'symbol',
                    'scope_key': '661501',
                    'source': 'pykrx',
                    'symbol': '661501',
                    'name_ko': 'Value Alpha',
                    'standard_code': 'KR7661501001',
                    'payload': {'foreign_ownership_ratio_pct': 33.1},
                },
                {
                    'market_date': '2026-04-21',
                    'market': 'KOSPI',
                    'category': 'sector_classification',
                    'scope_type': 'symbol',
                    'scope_key': '661502',
                    'source': 'pykrx',
                    'symbol': '661502',
                    'name_ko': 'Sector Beta',
                    'standard_code': 'KR7661502009',
                    'payload': {'sector_name': '반도체'},
                },
            ]
        )
        self.assertEqual(inserted, 3)

        updated = upsert_daily_pykrx_enrichment_rows(
            rows=[
                {
                    'market_date': '2026-04-21',
                    'market': 'KOSPI',
                    'category': 'valuation_snapshot',
                    'scope_type': 'symbol',
                    'scope_key': '661501',
                    'source': 'pykrx',
                    'symbol': '661501',
                    'name_ko': 'Value Alpha',
                    'standard_code': 'KR7661501001',
                    'payload': {'per': 10.8, 'pbr': 1.1},
                }
            ]
        )
        self.assertEqual(updated, 1)

        items = load_daily_pykrx_enrichment_rows(
            market_date='2026-04-21',
            categories=['valuation_snapshot', 'foreign_ownership'],
            symbols=['661501'],
            limit=10,
        )

        self.assertEqual(len(items), 2)
        valuation_row = next(item for item in items if item['category'] == 'valuation_snapshot')
        foreign_row = next(item for item in items if item['category'] == 'foreign_ownership')
        self.assertEqual(valuation_row['symbol'], '661501')
        self.assertEqual(valuation_row['name_ko'], 'Value Alpha')
        self.assertEqual(valuation_row['payload']['per'], 10.8)
        self.assertEqual(foreign_row['payload']['foreign_ownership_ratio_pct'], 33.1)

    def test_valuation_snapshot_updates_daily_price_bar_fundamentals(self) -> None:
        persist_daily_price_bar(
            {
                'symbol': '661501',
                'market': 'KOSPI',
                'business_date': '2026-04-22',
                'open_price': 1000,
                'high_price': 1100,
                'low_price': 900,
                'close_price': 1050,
                'volume': 10000,
                'turnover': 10500000,
                'per': None,
                'pbr': None,
                'bps': None,
                'eps': None,
                'dividend_yield': None,
                'dps': None,
                'source': 'pykrx',
            }
        )

        inserted = upsert_daily_pykrx_enrichment_rows(
            rows=[
                {
                    'market_date': '2026-04-22',
                    'market': 'KOSPI',
                    'category': 'valuation_snapshot',
                    'scope_type': 'symbol',
                    'scope_key': '661501',
                    'source': 'pykrx',
                    'symbol': '661501',
                    'name_ko': 'Value Alpha',
                    'standard_code': 'KR7661501001',
                    'payload': {
                        'per': 10.8,
                        'pbr': 1.1,
                        'bps': 952.4,
                        'eps': 97.2,
                        'dividend_yield': 2.5,
                        'dps': 26.0,
                    },
                }
            ]
        )

        self.assertEqual(inserted, 1)
        with session_scope() as session:
            price_row = (
                session.execute(
                    select(DailyPriceBar)
                    .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                    .where(Symbol.symbol == '661501', DailyPriceBar.trade_date == date(2026, 4, 22))
                )
                .scalars()
                .one()
            )
            per = float(price_row.per)
            pbr = float(price_row.pbr)
            dividend_yield = float(price_row.dividend_yield)
        self.assertEqual(per, 10.8)
        self.assertEqual(pbr, 1.1)
        self.assertEqual(dividend_yield, 2.5)

    def test_sparse_valuation_snapshot_preserves_existing_daily_price_bar_fundamentals(self) -> None:
        persist_daily_price_bar(
            {
                'symbol': '661501',
                'market': 'KOSPI',
                'business_date': '2026-04-23',
                'open_price': 1000,
                'high_price': 1100,
                'low_price': 900,
                'close_price': 1050,
                'volume': 10000,
                'turnover': 10500000,
                'per': 9.9,
                'pbr': 1.4,
                'bps': 750.0,
                'eps': 106.0,
                'dividend_yield': 2.8,
                'dps': 31.0,
                'source': 'pykrx',
            }
        )

        inserted = upsert_daily_pykrx_enrichment_rows(
            rows=[
                {
                    'market_date': '2026-04-23',
                    'market': 'KOSPI',
                    'category': 'valuation_snapshot',
                    'scope_type': 'symbol',
                    'scope_key': '661501',
                    'source': 'daily_price_bars_carry_forward',
                    'symbol': '661501',
                    'name_ko': 'Value Alpha',
                    'standard_code': 'KR7661501001',
                    'payload': {
                        'per': 10.8,
                        'pbr': None,
                    },
                }
            ]
        )

        self.assertEqual(inserted, 1)
        with session_scope() as session:
            price_row = (
                session.execute(
                    select(DailyPriceBar)
                    .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                    .where(Symbol.symbol == '661501', DailyPriceBar.trade_date == date(2026, 4, 23))
                )
                .scalars()
                .one()
            )
            values = {
                'per': float(price_row.per),
                'pbr': float(price_row.pbr),
                'bps': float(price_row.bps),
                'eps': float(price_row.eps),
                'dividend_yield': float(price_row.dividend_yield),
                'dps': float(price_row.dps),
            }
            source = price_row.source

        self.assertEqual(
            values,
            {
                'per': 10.8,
                'pbr': 1.4,
                'bps': 750.0,
                'eps': 106.0,
                'dividend_yield': 2.8,
                'dps': 31.0,
            },
        )
        self.assertEqual(source, 'daily_price_bars_carry_forward')


class KISIntradaySnapshotStoreIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Symbol.__table__.create(bind=engine, checkfirst=True)
        KISIntradaySnapshot.__table__.create(bind=engine, checkfirst=True)
        CurrentKISIntradaySnapshotCache.__table__.create(bind=engine, checkfirst=True)

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(session.execute(select(Symbol.id).where(Symbol.symbol.in_(['771001']))).scalars().all())
            if symbol_ids:
                session.execute(delete(KISIntradaySnapshot).where(KISIntradaySnapshot.symbol_id.in_(symbol_ids)))
                session.execute(
                    delete(CurrentKISIntradaySnapshotCache).where(
                        CurrentKISIntradaySnapshotCache.symbol_id.in_(symbol_ids)
                    )
                )
            session.execute(delete(Symbol).where(Symbol.symbol == '771001'))

    def test_persist_and_load_current_kis_intraday_snapshot(self) -> None:
        persist_kis_intraday_snapshot(
            {
                'symbol': '771001',
                'name_ko': 'Intraday Alpha',
                'market': 'KOSPI',
                'market_code': 'J',
                'source': 'kis_intraday_snapshot_v1',
                'feature_version': 'kis_intraday_snapshot_v1',
                'availability_status': 'partial',
                'freshness_status': 'fresh',
                'as_of_market_date': '2026-05-07',
                'as_of_timestamp': '2026-05-07T10:15:30+09:00',
                'snapshot_payload': {
                    'symbol': '771001',
                    'market_state': 'OPEN',
                    'coverage': {'status': 'partial', 'available_blocks': 3, 'partial_blocks': 1, 'total_blocks': 5},
                    'quote': {'price': 71550},
                    'liquidity': {'volume_power_rank': 3},
                },
            }
        )

        row = load_current_kis_intraday_snapshot('771001')

        assert row is not None
        self.assertEqual(row['symbol'], '771001')
        self.assertEqual(row['name_ko'], 'Intraday Alpha')
        self.assertEqual(row['availability_status'], 'partial')
        self.assertEqual(row['freshness_status'], 'fresh')
        self.assertEqual(row['as_of_market_date'], '2026-05-07')
        self.assertEqual(row['quote']['price'], 71550)
        self.assertEqual(row['liquidity']['volume_power_rank'], 3)
        self.assertIsNotNone(row['captured_at'])
        self.assertIsNotNone(row['cached_at'])


if __name__ == '__main__':
    unittest.main()
