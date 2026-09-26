from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import DailyPriceBar, Symbol
from app.eod.turnover_repair import DailyTurnoverRepairService, main
from app.market_data_store import repair_daily_price_bar_turnovers


class _FakeSeries:
    def __init__(self, values: dict[str, object]):
        self._values = values

    def items(self):
        return self._values.items()


class _FakeFrame:
    def __init__(
        self,
        values: dict[str, object],
        *,
        volumes: dict[str, object] | None = None,
        include_turnover: bool = True,
    ):
        self.empty = not values
        self.columns = ['시가', '고가', '저가', '종가', '거래량']
        if include_turnover:
            self.columns.append('거래대금')
        self._turnovers = values
        self._volumes = volumes or {symbol: 1 for symbol in values}
        self._prices = {
            '시가': {symbol: 100 for symbol in values},
            '고가': {symbol: 110 for symbol in values},
            '저가': {symbol: 90 for symbol in values},
            '종가': {symbol: 105 for symbol in values},
        }

    def __getitem__(self, key: str):
        if key not in self.columns:
            raise KeyError(key)
        if key == '거래대금':
            return _FakeSeries(self._turnovers)
        if key == '거래량':
            return _FakeSeries(self._volumes)
        if key in self._prices:
            return _FakeSeries(self._prices[key])
        raise KeyError(key)


class _FakeResult:
    def __init__(self, rows=(), *, rowcount: int = 0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows
        for row in self.rows:
            for field, value in {
                'open_price': 100,
                'high_price': 110,
                'low_price': 90,
                'close_price': 105,
                'is_active': True,
            }.items():
                if not hasattr(row, field):
                    setattr(row, field, value)
        self.calls: list[tuple[object, object]] = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))
        if len(self.calls) == 1:
            return _FakeResult(self.rows)
        return _FakeResult(rowcount=len(parameters or []))


def _session_scope_for(session):
    @contextmanager
    def fake_session_scope():
        yield session

    return fake_session_scope


def _available_lock(**_kwargs):
    @contextmanager
    def lock():
        yield True

    return lock()


class DailyPriceBarTurnoverRepositoryTests(unittest.TestCase):
    provider_ohlc = {'005930': (100, 110, 90, 105)}

    def test_apply_updates_only_paired_liquidity_fields(self):
        session = _FakeSession(
            [
                SimpleNamespace(
                    bar_id=11,
                    symbol='005930',
                    volume=100,
                    turnover=1,
                )
            ]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 123_456},
                provider_volume_by_symbol={'005930': 101},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['updated_count'], 1)
        self.assertEqual(len(session.calls), 2)
        update_statement, update_parameters = session.calls[1]
        compiled_sql = str(update_statement.compile())
        self.assertIn('turnover=', compiled_sql)
        self.assertIn('volume=', compiled_sql)
        for prohibited_column in (
            'captured_at',
            'source',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'per=',
            'pbr=',
            'eod_run_id',
        ):
            self.assertNotIn(prohibited_column, compiled_sql)
        self.assertEqual(
            update_parameters,
            [
                {
                    'repair_bar_id': 11,
                    'repair_volume': 101,
                    'repair_turnover': 123_456,
                }
            ],
        )

    def test_missing_price_bar_blocks_all_updates(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=100, turnover=1)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930', '000660'],
                turnover_by_symbol={'005930': 10, '000660': 20},
                provider_volume_by_symbol={'005930': 100, '000660': 100},
                provider_ohlc_by_symbol={
                    '005930': (100, 110, 90, 105),
                    '000660': (100, 110, 90, 105),
                },
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['missing_price_bar_symbols'], ['000660'])
        self.assertEqual(len(session.calls), 1)

    def test_missing_positive_volume_turnover_blocks_all_updates(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=100, turnover=1)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 0},
                provider_volume_by_symbol={'005930': 100},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['invalid_provider_liquidity_symbols'],
            ['005930'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_provider_volume_increase_repairs_volume_and_turnover(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=0, turnover=0)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 123_456},
                provider_volume_by_symbol={'005930': 100},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['stored_volume_changed_symbols'], ['005930'])
        self.assertEqual(len(session.calls), 2)

    def test_provider_volume_decrease_blocks_updates(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=101, turnover=1)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 123_456},
                provider_volume_by_symbol={'005930': 100},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['provider_volume_decrease_symbols'],
            ['005930'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_provider_ohlc_mismatch_blocks_updates(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=100, turnover=1)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 123_456},
                provider_volume_by_symbol={'005930': 101},
                provider_ohlc_by_symbol={'005930': (101, 110, 90, 105)},
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['provider_stored_ohlc_mismatch_symbols'],
            ['005930'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_active_stored_row_outside_membership_blocks_updates(self):
        session = _FakeSession(
            [
                SimpleNamespace(
                    bar_id=11,
                    symbol='005930',
                    volume=100,
                    turnover=1,
                ),
                SimpleNamespace(
                    bar_id=12,
                    symbol='069500',
                    volume=100,
                    turnover=1,
                    is_active=True,
                ),
            ]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 123_456},
                provider_volume_by_symbol={'005930': 101},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['active_stored_outside_membership_symbols'],
            ['069500'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_missing_zero_volume_turnover_is_blocked(self):
        session = _FakeSession(
            [
                SimpleNamespace(bar_id=11, symbol='005930', volume=100, turnover=1),
                SimpleNamespace(bar_id=12, symbol='000660', volume=0, turnover=0),
            ]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930', '000660'],
                turnover_by_symbol={'005930': 10},
                provider_volume_by_symbol={'005930': 100, '000660': 0},
                provider_ohlc_by_symbol={
                    '005930': (100, 110, 90, 105),
                    '000660': (100, 110, 90, 105),
                },
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['missing_provider_symbols'],
            ['000660'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_zero_volume_positive_turnover_blocks_updates(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=0, turnover=0)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 123_456},
                provider_volume_by_symbol={'005930': 0},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['invalid_provider_liquidity_symbols'],
            ['005930'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_existing_nonzero_turnover_without_provider_evidence_blocks(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=0, turnover=1)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={},
                provider_volume_by_symbol={'005930': 0},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=True,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['missing_provider_symbols'],
            ['005930'],
        )
        self.assertEqual(len(session.calls), 1)

    def test_dry_run_never_executes_update(self):
        session = _FakeSession(
            [SimpleNamespace(bar_id=11, symbol='005930', volume=100, turnover=1)]
        )

        with patch(
            'app.market_data_store.session_scope',
            _session_scope_for(session),
        ):
            result = repair_daily_price_bar_turnovers(
                market_date='2026-08-19',
                expected_symbols=['005930'],
                turnover_by_symbol={'005930': 10},
                provider_volume_by_symbol={'005930': 100},
                provider_ohlc_by_symbol=self.provider_ohlc,
                apply=False,
            )

        self.assertEqual(result['status'], 'ready')
        self.assertFalse(result['applied'])
        self.assertEqual(result['would_update_count'], 1)
        self.assertEqual(len(session.calls), 1)


class DailyPriceBarTurnoverRepositoryPostgresTests(unittest.TestCase):
    def test_apply_updates_liquidity_pair_and_preserves_other_lineage(self):
        market_date = date(2099, 12, 27)
        symbol_code = f'{900000 + uuid4().int % 99_999:06d}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='Liquidity repair integration',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            bar = DailyPriceBar(
                symbol_id=symbol.id,
                trade_date=market_date,
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1_000,
                turnover=0,
                per=Decimal('10.5'),
                source='protected_existing',
            )
            session.add(bar)
            session.flush()
            before = (
                bar.open_price,
                bar.high_price,
                bar.low_price,
                bar.close_price,
                bar.per,
                bar.source,
                bar.captured_at,
                bar.eod_run_id,
            )

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                with patch(
                    'app.market_data_store.session_scope',
                    rollback_only_session_scope,
                ):
                    result = repair_daily_price_bar_turnovers(
                        market_date=market_date,
                        expected_symbols=[symbol_code],
                        turnover_by_symbol={symbol_code: 120_000},
                        provider_volume_by_symbol={symbol_code: 1_100},
                        provider_ohlc_by_symbol={
                            symbol_code: (100, 110, 90, 105)
                        },
                        apply=True,
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['updated_count'], 1)
                session.expire_all()
                persisted = session.execute(
                    select(DailyPriceBar).where(DailyPriceBar.id == bar.id)
                ).scalar_one()
                self.assertEqual(persisted.volume, 1_100)
                self.assertEqual(persisted.turnover, 120_000)
                self.assertEqual(
                    (
                        persisted.open_price,
                        persisted.high_price,
                        persisted.low_price,
                        persisted.close_price,
                        persisted.per,
                        persisted.source,
                        persisted.captured_at,
                        persisted.eod_run_id,
                    ),
                    before,
                )
            finally:
                session.close()
                transaction.rollback()


class DailyTurnoverRepairServiceTests(unittest.TestCase):
    def test_uses_exact_date_market_frames_and_passes_dry_run_plan(self):
        calls: list[tuple[str, str]] = []
        patch_calls: list[dict] = []
        frames = {
            'KOSPI': _FakeFrame({'005930': 100}),
            'KOSDAQ': _FakeFrame({'000660': 200}),
        }

        def load_market(trade_date: str, market: str):
            calls.append((trade_date, market))
            return frames[market]

        def patch_turnovers(**kwargs):
            patch_calls.append(kwargs)
            return {
                'status': 'ready',
                'applied': False,
                'would_update_count': 2,
            }

        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-19',
            market_ohlcv_loader=load_market,
            turnover_patcher=patch_turnovers,
            lock_factory=_available_lock,
            minimum_market_membership={'KOSPI': 1, 'KOSDAQ': 1},
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI', 'KOSDAQ'],
            apply=False,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(
            calls,
            [('20260819', 'KOSPI'), ('20260819', 'KOSDAQ')],
        )
        self.assertEqual(len(patch_calls), 1)
        self.assertEqual(
            patch_calls[0]['expected_symbols'],
            ['005930', '000660'],
        )
        self.assertEqual(
            patch_calls[0]['turnover_by_symbol'],
            {'005930': 100, '000660': 200},
        )
        self.assertEqual(
            patch_calls[0]['provider_volume_by_symbol'],
            {'005930': 1, '000660': 1},
        )
        self.assertEqual(
            patch_calls[0]['provider_ohlc_by_symbol'],
            {
                '005930': (100, 110, 90, 105),
                '000660': (100, 110, 90, 105),
            },
        )
        self.assertFalse(patch_calls[0]['apply'])
        self.assertEqual(result['membership_basis'], 'exact_market_date_ohlcv')

    def test_exact_minimum_membership_boundary_reaches_database_patcher(self):
        patch_calls: list[dict] = []
        frames = {
            'KOSPI': _FakeFrame(
                {
                    f'{symbol:06d}': 100
                    for symbol in range(100_000, 100_700)
                }
            ),
            'KOSDAQ': _FakeFrame(
                {
                    f'{symbol:06d}': 200
                    for symbol in range(200_000, 201_200)
                }
            ),
        }

        def patch_turnovers(**kwargs):
            patch_calls.append(kwargs)
            return {'status': 'ready', 'applied': False}

        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-19',
            market_ohlcv_loader=lambda _trade_date, market: frames[market],
            turnover_patcher=patch_turnovers,
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI', 'KOSDAQ'],
            apply=False,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(
            result['market_symbol_counts'],
            {'KOSPI': 700, 'KOSDAQ': 1200},
        )
        self.assertEqual(len(patch_calls), 1)
        self.assertEqual(len(patch_calls[0]['expected_symbols']), 1900)

    def test_partial_market_payload_is_blocked_before_database_patcher(self):
        frames = {
            'KOSPI': _FakeFrame(
                {
                    f'{symbol:06d}': 100
                    for symbol in range(100_000, 100_700)
                }
            ),
            'KOSDAQ': _FakeFrame(
                {
                    f'{symbol:06d}': 200
                    for symbol in range(200_000, 200_010)
                }
            ),
        }
        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-19',
            market_ohlcv_loader=lambda _trade_date, market: frames[market],
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI', 'KOSDAQ'],
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'market_membership_below_safety_floor')
        self.assertEqual(result['failed_market'], 'KOSDAQ')
        self.assertEqual(result['observed_symbol_count'], 10)
        self.assertEqual(result['minimum_symbol_count'], 1200)

    def test_mixed_alpha_code_is_skipped_and_reported(self):
        patch_calls: list[dict] = []
        values = {
            f'{symbol:06d}': 100
            for symbol in range(100_000, 100_700)
        }
        values['00104K'] = 999

        def patch_turnovers(**kwargs):
            patch_calls.append(kwargs)
            return {'status': 'ready', 'applied': False}

        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-18',
            market_ohlcv_loader=lambda *_args: _FakeFrame(values),
            turnover_patcher=patch_turnovers,
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-08-18',
            markets=['KOSPI'],
            apply=False,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['market_symbol_counts'], {'KOSPI': 700})
        self.assertEqual(result['provider_skipped_code_count'], 1)
        self.assertEqual(
            result['provider_skipped_code_preview'],
            [{'market': 'KOSPI', 'code': '00104K'}],
        )
        self.assertEqual(
            result['provider_skipped_code_counts_by_market'],
            {'KOSPI': 1},
        )
        self.assertEqual(len(patch_calls), 1)
        self.assertEqual(len(patch_calls[0]['expected_symbols']), 700)
        self.assertNotIn('00104K', patch_calls[0]['expected_symbols'])
        self.assertNotIn('00104K', patch_calls[0]['turnover_by_symbol'])

    def test_konex_market_is_rejected(self):
        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: self.fail('resolver must not run'),
            market_ohlcv_loader=lambda *_args: self.fail('loader must not run'),
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=_available_lock,
        )

        with self.assertRaisesRegex(ValueError, 'unsupported turnover repair'):
            service.repair_for_market_date(
                market_date='2026-08-19',
                markets=['KONEX'],
                apply=False,
            )

    def test_non_session_date_is_blocked_before_market_fetch(self):
        fetch_calls: list[tuple[str, str]] = []

        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-18',
            market_ohlcv_loader=lambda *args: fetch_calls.append(args),
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI'],
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'not_an_exact_trading_session')
        self.assertEqual(fetch_calls, [])

    def test_incomplete_market_payload_is_blocked_before_database_patch(self):
        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-19',
            market_ohlcv_loader=lambda *_args: _FakeFrame(
                {'005930': 100},
                include_turnover=False,
            ),
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI'],
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_market_payload')

    def test_zero_volume_positive_turnover_payload_is_blocked(self):
        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-19',
            market_ohlcv_loader=lambda *_args: _FakeFrame(
                {'005930': 100},
                volumes={'005930': 0},
            ),
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=_available_lock,
            minimum_market_membership={'KOSPI': 1, 'KOSDAQ': 1},
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI'],
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_market_payload')

    def test_missing_turnover_value_is_blocked_before_database_patch(self):
        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-08-19',
            market_ohlcv_loader=lambda *_args: _FakeFrame({'005930': None}),
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=_available_lock,
            minimum_market_membership={'KOSPI': 1, 'KOSDAQ': 1},
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI'],
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'provider_turnover_incomplete')
        self.assertEqual(result['provider_missing_turnover_symbols'], ['005930'])

    def test_unavailable_lock_is_blocked_before_provider_calls(self):
        @contextmanager
        def unavailable_lock(**_kwargs):
            yield False

        service = DailyTurnoverRepairService(
            trade_date_resolver=lambda **_kwargs: self.fail('resolver must not run'),
            market_ohlcv_loader=lambda *_args: self.fail('loader must not run'),
            turnover_patcher=lambda **_kwargs: self.fail('patcher must not run'),
            lock_factory=unavailable_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-08-19',
            markets=['KOSPI'],
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'advisory_lock_unavailable')


class DailyTurnoverRepairCliTests(unittest.TestCase):
    def test_cli_is_dry_run_by_default(self):
        with (
            patch(
                'app.eod.turnover_repair.daily_turnover_repair_service.repair_for_market_date',
                return_value={'status': 'ready', 'applied': False},
            ) as repair,
            patch('sys.stdout.write'),
        ):
            exit_code = main(
                ['--market-date', '2026-08-19', '--markets', 'KOSPI,KOSDAQ']
            )

        self.assertEqual(exit_code, 0)
        self.assertFalse(repair.call_args.kwargs['apply'])
