from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from decimal import Decimal
from json import JSONDecodeError
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import DailyPriceBar, Symbol
from app.eod.symbol_history_repair import (
    MAX_REPAIR_CALENDAR_DAYS,
    MAX_REPAIR_SYMBOLS,
    SymbolHistoryBar,
    SymbolHistoryRepairService,
    _normalize_date_bounds,
    _normalize_symbols,
    main,
    repair_missing_symbol_history,
)


class _FakeFrame:
    def __init__(self, rows: dict[object, dict[str, object]], columns=None):
        self._rows = rows
        self.empty = not rows
        if columns is None:
            columns = next(iter(rows.values())).keys() if rows else ()
        self.columns = list(columns)

    def iterrows(self):
        return iter(self._rows.items())


def _range_row(
    *,
    open_price: object = 100,
    high_price: object = 110,
    low_price: object = 90,
    close_price: object = 105,
    volume: object = 1_000,
    range_turnover: object = 1,
) -> dict[str, object]:
    return {
        '시가': open_price,
        '고가': high_price,
        '저가': low_price,
        '종가': close_price,
        '거래량': volume,
        '거래대금': range_turnover,
    }


def _market_row(
    *,
    open_price: object = 100,
    high_price: object = 110,
    low_price: object = 90,
    close_price: object = 105,
    volume: object | None = None,
    turnover: object = 105_000,
) -> dict[str, object]:
    if volume is None:
        volume = 0 if turnover == 0 else 1_000
    return {
        '시가': open_price,
        '고가': high_price,
        '저가': low_price,
        '종가': close_price,
        '거래량': volume,
        '거래대금': turnover,
    }


def _available_lock(**_kwargs):
    @contextmanager
    def lock():
        yield True

    return lock()


def _exact_resolver(*, end_date: str) -> str:
    return end_date


def _weekday_sessions(start_date: date, end_date: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _bar(
    symbol: str,
    trade_date: date,
    *,
    market: str = 'KOSPI',
    open_price: object = 100,
    high_price: object = 110,
    low_price: object = 90,
    close_price: object = 105,
    volume: object = 1_000,
    turnover: object = 105_000,
) -> SymbolHistoryBar:
    return SymbolHistoryBar(
        symbol=symbol,
        market=market,
        trade_date=trade_date,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        turnover=turnover,
    )


class SymbolHistoryRepairInputTests(unittest.TestCase):
    def test_requires_explicit_valid_bounded_symbols(self):
        with self.assertRaisesRegex(ValueError, '6-digit'):
            _normalize_symbols(['5930'])
        with self.assertRaisesRegex(ValueError, '6-digit'):
            _normalize_symbols(['00A930'])
        with self.assertRaisesRegex(ValueError, 'duplicates'):
            _normalize_symbols(['005930', '005930'])
        with self.assertRaisesRegex(ValueError, 'limited'):
            _normalize_symbols(
                [f'{index:06d}' for index in range(MAX_REPAIR_SYMBOLS + 1)]
            )

    def test_rejects_reversed_and_oversized_date_bounds(self):
        with self.assertRaisesRegex(ValueError, 'must not be after'):
            _normalize_date_bounds('2026-09-23', '2026-09-22')
        with self.assertRaisesRegex(ValueError, 'limited'):
            _normalize_date_bounds(
                date(2025, 1, 1),
                date(2025, 1, 1) + timedelta(days=MAX_REPAIR_CALENDAR_DAYS),
            )

    def test_database_writer_revalidates_every_numeric_bar_field(self):
        trade_date = date(2026, 9, 23)
        invalid_bars = (
            (_bar('005930', trade_date, open_price=True), 'open_price'),
            (
                _bar('005930', trade_date, high_price=Decimal('110.5')),
                'high_price',
            ),
            (_bar('005930', trade_date, low_price=-1), 'low_price'),
            (
                _bar('005930', trade_date, turnover=0),
                'non-positive turnover',
            ),
            (
                _bar('005930', trade_date, high_price=100),
                'invalid OHLC geometry',
            ),
            (
                _bar(
                    '005930',
                    trade_date,
                    volume=0,
                    turnover=1,
                ),
                'zero-volume row has non-zero turnover',
            ),
            (
                _bar(
                    '005930',
                    trade_date,
                    open_price=0,
                    high_price=0,
                    low_price=0,
                    close_price=100,
                    volume=0,
                    turnover=0,
                ),
                'incomplete halted OHLC',
            ),
        )

        for bar, message in invalid_bars:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    repair_missing_symbol_history(
                        symbols=['005930'],
                        start_date=trade_date,
                        end_date=trade_date,
                        bars=[bar],
                    )


class SymbolHistoryRepairServiceTests(unittest.TestCase):
    def _service(
        self,
        *,
        symbol_range_loader,
        market_ohlcv_loader,
        database_repairer,
        minimum_market_membership=None,
        lock_set_factory=None,
    ) -> SymbolHistoryRepairService:
        lock_kwargs = (
            {'lock_factory': _available_lock}
            if lock_set_factory is None
            else {'lock_set_factory': lock_set_factory}
        )
        return SymbolHistoryRepairService(
            trade_date_resolver=_exact_resolver,
            trading_dates_resolver=_weekday_sessions,
            symbol_range_loader=symbol_range_loader,
            market_ohlcv_loader=market_ohlcv_loader,
            database_repairer=database_repairer,
            minimum_market_membership=(
                minimum_market_membership
                or {'KOSPI': 1, 'KOSDAQ': 1}
            ),
            **lock_kwargs,
        )

    def test_dry_run_fetches_each_symbol_range_once_and_uses_raw_turnover(self):
        first_date = date(2026, 9, 22)
        second_date = date(2026, 9, 23)
        range_calls: list[tuple[str, str, str]] = []
        market_calls: list[tuple[str, str]] = []
        database_calls: list[dict] = []

        def range_loader(start: str, end: str, symbol: str):
            range_calls.append((start, end, symbol))
            return _FakeFrame(
                {
                    first_date: _range_row(range_turnover=1),
                    second_date: _range_row(range_turnover=2),
                }
            )

        def market_loader(trade_date: str, market: str):
            market_calls.append((trade_date, market))
            if market == 'KOSPI':
                turnover = 910_000 if trade_date == '20260922' else 920_000
                return _FakeFrame(
                    {
                        '005930': _market_row(turnover=turnover),
                        '00104K': _market_row(turnover=77),
                    }
                )
            return _FakeFrame({'035720': _market_row()})

        def repairer(**kwargs):
            database_calls.append(kwargs)
            return {
                'status': 'ready',
                'applied': False,
                'missing_bar_count': 2,
                'would_insert_count': 2,
            }

        service = self._service(
            symbol_range_loader=range_loader,
            market_ohlcv_loader=market_loader,
            database_repairer=repairer,
            lock_set_factory=lambda **_kwargs: self.fail(
                'dry-run must not request writer locks'
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date=first_date,
            end_date=second_date,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertFalse(result['applied'])
        self.assertEqual(
            range_calls,
            [('20260922', '20260923', '005930')],
        )
        self.assertEqual(
            market_calls,
            [
                ('20260922', 'KOSPI'),
                ('20260922', 'KOSDAQ'),
                ('20260923', 'KOSPI'),
                ('20260923', 'KOSDAQ'),
            ],
        )
        bars = database_calls[0]['bars']
        self.assertEqual([bar.turnover for bar in bars], [910_000, 920_000])
        self.assertEqual(result['provider_skipped_code_count'], 2)
        self.assertEqual(result['symbol_markets'], {'005930': 'KOSPI'})

    def test_symbol_range_loader_retries_error_and_empty_before_success(self):
        trade_date = date(2026, 9, 23)
        range_attempts = 0
        database_calls: list[dict] = []

        def range_loader(_start: str, _end: str, _symbol: str):
            nonlocal range_attempts
            range_attempts += 1
            if range_attempts == 1:
                raise JSONDecodeError('transient response', 'x', 0)
            if range_attempts == 2:
                return _FakeFrame({})
            return _FakeFrame({trade_date: _range_row()})

        service = self._service(
            symbol_range_loader=range_loader,
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {'005930': _market_row()}
                if market == 'KOSPI'
                else {'035720': _market_row()}
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date=trade_date,
            end_date=trade_date,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(range_attempts, 3)
        self.assertEqual(result['symbol_range_retry_count'], 2)
        self.assertEqual(
            [
                item['outcome']
                for item in result['symbol_range_retry_preview'][0]['attempts']
            ],
            ['loader_error', 'empty_payload', 'success'],
        )
        self.assertEqual(len(database_calls), 1)

    def test_symbol_range_loader_blocks_after_three_malformed_payloads(self):
        trade_date = date(2026, 9, 23)
        range_attempts = 0

        def range_loader(_start: str, _end: str, _symbol: str):
            nonlocal range_attempts
            range_attempts += 1
            return _FakeFrame(
                {trade_date: _range_row()},
                columns=['시가', '고가'],
            )

        service = self._service(
            symbol_range_loader=range_loader,
            market_ohlcv_loader=lambda *_args: self.fail(
                'market loader must not run'
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date=trade_date,
            end_date=trade_date,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_symbol_range_payload')
        self.assertEqual(result['failed_symbol'], '005930')
        self.assertEqual(result['load_attempt_count'], 3)
        self.assertEqual(range_attempts, 3)
        self.assertEqual(
            [item['outcome'] for item in result['load_attempts']],
            ['invalid_payload', 'invalid_payload', 'invalid_payload'],
        )
        self.assertTrue(
            all(
                'missing columns' in item['error_message']
                for item in result['load_attempts']
            )
        )

    def test_below_floor_blocks_before_database_repair(self):
        market_loader_calls: list[tuple[str, str]] = []

        def market_loader(trade_date: str, market: str):
            market_loader_calls.append((trade_date, market))
            return _FakeFrame({'005930': _market_row()})

        service = self._service(
            symbol_range_loader=lambda *_args: _FakeFrame(
                {date(2026, 9, 23): _range_row()}
            ),
            market_ohlcv_loader=market_loader,
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            minimum_market_membership={'KOSPI': 2, 'KOSDAQ': 1},
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date='2026-09-23',
            end_date='2026-09-23',
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'market_membership_below_safety_floor')
        self.assertEqual(result['failed_market'], 'KOSPI')
        self.assertEqual(
            market_loader_calls,
            [('20260923', 'KOSPI'), ('20260923', 'KOSDAQ')],
        )

    def test_positive_volume_without_raw_turnover_blocks(self):
        def market_loader(_trade_date: str, market: str):
            if market == 'KOSPI':
                return _FakeFrame(
                    {'005930': _market_row(volume=1_000, turnover=0)}
                )
            return _FakeFrame({'035720': _market_row()})

        service = self._service(
            symbol_range_loader=lambda *_args: _FakeFrame(
                {date(2026, 9, 23): _range_row(volume=1_000)}
            ),
            market_ohlcv_loader=market_loader,
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date='2026-09-23',
            end_date='2026-09-23',
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'invalid_exact_market_payload',
        )

    def test_exact_market_loader_retries_error_and_empty_before_success(self):
        trade_date = date(2026, 9, 23)
        kosdaq_attempts = 0
        database_calls: list[dict] = []

        def market_loader(_trade_date: str, market: str):
            nonlocal kosdaq_attempts
            if market == 'KOSPI':
                return _FakeFrame({'005930': _market_row()})
            kosdaq_attempts += 1
            if kosdaq_attempts == 1:
                raise KeyError('transient missing 거래대금')
            if kosdaq_attempts == 2:
                return _FakeFrame({})
            return _FakeFrame({'035720': _market_row()})

        service = self._service(
            symbol_range_loader=lambda *_args: _FakeFrame(
                {trade_date: _range_row()}
            ),
            market_ohlcv_loader=market_loader,
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date=trade_date,
            end_date=trade_date,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(kosdaq_attempts, 3)
        self.assertEqual(result['exact_market_retry_count'], 2)
        self.assertEqual(
            [
                item['outcome']
                for item in result['exact_market_retry_preview'][0]['attempts']
            ],
            ['loader_error', 'empty_payload', 'success'],
        )
        self.assertEqual(len(database_calls), 1)

    def test_exact_market_loader_blocks_after_three_malformed_payloads(self):
        trade_date = date(2026, 9, 23)
        kosdaq_attempts = 0

        def market_loader(_trade_date: str, market: str):
            nonlocal kosdaq_attempts
            if market == 'KOSPI':
                return _FakeFrame({'005930': _market_row()})
            kosdaq_attempts += 1
            return _FakeFrame(
                {'035720': _market_row()},
                columns=['시가', '고가', '저가', '종가', '거래량'],
            )

        service = self._service(
            symbol_range_loader=lambda *_args: _FakeFrame(
                {trade_date: _range_row()}
            ),
            market_ohlcv_loader=market_loader,
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date=trade_date,
            end_date=trade_date,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_exact_market_payload')
        self.assertEqual(result['failed_market'], 'KOSDAQ')
        self.assertEqual(result['load_attempt_count'], 3)
        self.assertEqual(kosdaq_attempts, 3)
        self.assertEqual(
            [item['outcome'] for item in result['load_attempts']],
            ['invalid_payload', 'invalid_payload', 'invalid_payload'],
        )
        self.assertTrue(
            all(
                'missing columns' in item['error_message']
                for item in result['load_attempts']
            )
        )

    def test_exact_market_ohlcv_is_authoritative_when_range_is_adjusted(self):
        database_calls: list[dict] = []

        def market_loader(_trade_date: str, market: str):
            if market == 'KOSPI':
                return _FakeFrame(
                    {'005930': _market_row(volume=1_001)}
                )
            return _FakeFrame({'035720': _market_row()})

        service = self._service(
            symbol_range_loader=lambda *_args: _FakeFrame(
                {date(2026, 9, 23): _range_row(volume=1_000)}
            ),
            market_ohlcv_loader=market_loader,
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date='2026-09-23',
            end_date='2026-09-23',
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['range_exact_ohlcv_mismatch_count'], 1)
        self.assertEqual(database_calls[0]['bars'][0].volume, 1_001)

    def test_absent_or_cross_market_ambiguous_target_blocks(self):
        range_loader = lambda *_args: _FakeFrame(  # noqa: E731
            {date(2026, 9, 23): _range_row()}
        )

        def absent_market_loader(_trade_date: str, market: str):
            code = '000660' if market == 'KOSPI' else '035720'
            return _FakeFrame({code: _market_row()})

        service = self._service(
            symbol_range_loader=range_loader,
            market_ohlcv_loader=absent_market_loader,
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date='2026-09-23',
            end_date='2026-09-23',
        )
        self.assertEqual(
            result['reason'],
            'target_absent_or_ambiguous_in_exact_membership',
        )

        service = self._service(
            symbol_range_loader=range_loader,
            market_ohlcv_loader=lambda _date, _market: _FakeFrame(
                {'005930': _market_row()}
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date='2026-09-23',
            end_date='2026-09-23',
        )
        self.assertEqual(result['reason'], 'duplicate_cross_market_membership')

    def test_provider_empty_calendar_candidate_is_treated_as_closed(self):
        first_date = date(2026, 9, 21)
        calendar_only_date = date(2026, 9, 22)
        last_date = date(2026, 9, 23)
        database_calls: list[dict] = []
        market_calls: list[tuple[str, str]] = []
        lock_calls: list[dict] = []
        lock_active = False

        def market_loader(trade_date: str, market: str):
            market_calls.append((trade_date, market))
            if trade_date == '20260922':
                code = '005930' if market == 'KOSPI' else '035720'
                return _FakeFrame({code: _market_row(turnover=0)})
            code = '005930' if market == 'KOSPI' else '035720'
            return _FakeFrame({code: _market_row()})

        @contextmanager
        def date_locks(**kwargs):
            nonlocal lock_active
            lock_calls.append(kwargs)
            self.assertEqual(len(market_calls), 6)
            self.assertFalse(database_calls)
            lock_active = True
            try:
                yield True, None
            finally:
                lock_active = False

        def repairer(**kwargs):
            self.assertTrue(lock_active)
            database_calls.append(kwargs)
            return {'status': 'completed', 'applied': True}

        service = self._service(
            symbol_range_loader=lambda *_args: _FakeFrame(
                {
                    first_date: _range_row(),
                    last_date: _range_row(),
                }
            ),
            market_ohlcv_loader=market_loader,
            database_repairer=repairer,
            lock_set_factory=date_locks,
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date=first_date,
            end_date=last_date,
            apply=True,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertTrue(result['applied'])
        self.assertEqual(result['provider_trade_date_count'], 2)
        self.assertEqual(result['provider_closed_calendar_date_count'], 1)
        self.assertEqual(
            result['provider_closed_calendar_dates'],
            [calendar_only_date.isoformat()],
        )
        self.assertEqual(len(database_calls[0]['bars']), 2)
        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_dates': (first_date, last_date),
                    'source': 'daily_price_bars',
                }
            ],
        )

    def test_apply_validates_provider_before_actual_date_lock_denial(self):
        first_date = date(2026, 9, 21)
        middle_date = date(2026, 9, 22)
        last_date = date(2026, 9, 23)
        lock_calls: list[dict] = []
        range_calls: list[tuple[str, str, str]] = []
        market_calls: list[tuple[str, str]] = []

        @contextmanager
        def date_locks(**kwargs):
            self.assertEqual(
                range_calls,
                [('20260921', '20260923', '005930')],
            )
            self.assertEqual(len(market_calls), 6)
            lock_calls.append(kwargs)
            yield False, middle_date.isoformat()

        service = SymbolHistoryRepairService(
            trade_date_resolver=_exact_resolver,
            trading_dates_resolver=_weekday_sessions,
            symbol_range_loader=lambda start, end, symbol: (
                range_calls.append((start, end, symbol))
                or _FakeFrame(
                    {
                        first_date: _range_row(),
                        middle_date: _range_row(),
                        last_date: _range_row(),
                    }
                )
            ),
            market_ohlcv_loader=lambda trade_date, market: (
                market_calls.append((trade_date, market))
                or _FakeFrame(
                    {'005930': _market_row()}
                    if market == 'KOSPI'
                    else {'035720': _market_row()}
                )
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run while a range lock is blocked'
            ),
            lock_set_factory=date_locks,
            minimum_market_membership={'KOSPI': 1, 'KOSDAQ': 1},
        )

        result = service.repair_range(
            symbols=['005930'],
            start_date=first_date,
            end_date=last_date,
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'advisory_lock_unavailable')
        self.assertEqual(result['blocked_trade_date'], middle_date.isoformat())
        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_dates': (first_date, middle_date, last_date),
                    'source': 'daily_price_bars',
                }
            ],
        )

    def test_incomplete_symbol_date_coverage_and_non_session_bound_block(self):
        first_date = date(2026, 9, 21)
        middle_date = date(2026, 9, 22)
        last_date = date(2026, 9, 23)

        def range_loader(_start: str, _end: str, symbol: str):
            rows = {
                first_date: _range_row(),
                middle_date: _range_row(),
                last_date: _range_row(),
            }
            if symbol == '000660':
                rows.pop(middle_date)
            return _FakeFrame(rows)

        service = self._service(
            symbol_range_loader=range_loader,
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {
                    '005930': _market_row(),
                    '000660': _market_row(),
                }
                if market == 'KOSPI'
                else {'035720': _market_row()}
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
        )
        result = service.repair_range(
            symbols=['005930', '000660'],
            start_date=first_date,
            end_date=last_date,
        )
        self.assertEqual(
            result['reason'],
            'incomplete_cross_symbol_date_coverage',
        )
        self.assertEqual(result['failed_symbol'], '000660')
        self.assertEqual(result['failed_date'], '2026-09-22')

        service = SymbolHistoryRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-22',
            symbol_range_loader=lambda *_args: self.fail(
                'range loader must not run'
            ),
            market_ohlcv_loader=lambda *_args: self.fail(
                'market loader must not run'
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            lock_factory=_available_lock,
        )
        result = service.repair_range(
            symbols=['005930'],
            start_date='2026-09-22',
            end_date='2026-09-23',
        )
        self.assertEqual(
            result['reason'],
            'date_bound_is_not_exact_trading_session',
        )


class SymbolHistoryRepairRepositoryPostgresTests(unittest.TestCase):
    def _new_symbol(self, session: Session, *, market: str = 'KOSPI') -> Symbol:
        code = f'{900000 + uuid4().int % 99_999:06d}'
        while session.execute(
            select(Symbol.id).where(Symbol.symbol == code)
        ).scalar_one_or_none() is not None:
            code = f'{900000 + uuid4().int % 99_999:06d}'
        symbol = Symbol(
            symbol=code,
            name_ko='Symbol history repair test',
            market=market,
            instrument_type='equity',
            source='protected_source',
            is_active=False,
        )
        session.add(symbol)
        session.flush()
        return symbol

    @contextmanager
    def _savepoint_scope(self, connection, session):
        savepoint = connection.begin_nested()
        try:
            yield session
            session.flush()
            savepoint.commit()
        except Exception:
            savepoint.rollback()
            session.expire_all()
            raise

    def test_dry_run_writes_nothing_and_missing_db_symbol_blocks(self):
        trade_date = date(2099, 12, 27)
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = self._new_symbol(session)

            def scope():
                return self._savepoint_scope(connection, session)

            try:
                with patch(
                    'app.eod.symbol_history_repair.session_scope',
                    scope,
                ):
                    result = repair_missing_symbol_history(
                        symbols=[symbol.symbol],
                        start_date=trade_date,
                        end_date=trade_date,
                        bars=[_bar(symbol.symbol, trade_date)],
                    )
                self.assertEqual(result['status'], 'ready')
                self.assertFalse(result['applied'])
                count = session.execute(
                    select(func.count(DailyPriceBar.id)).where(
                        DailyPriceBar.symbol_id == symbol.id,
                        DailyPriceBar.trade_date == trade_date,
                    )
                ).scalar_one()
                self.assertEqual(count, 0)

                with patch(
                    'app.eod.symbol_history_repair.session_scope',
                    scope,
                ):
                    result = repair_missing_symbol_history(
                        symbols=['899999'],
                        start_date=trade_date,
                        end_date=trade_date,
                        bars=[_bar('899999', trade_date)],
                    )
                self.assertEqual(result['status'], 'blocked')
                self.assertEqual(
                    result['reason'],
                    'requested_symbol_missing_from_symbols_table',
                )
            finally:
                session.close()
                transaction.rollback()

    def test_apply_preserves_existing_row_and_symbol_lifecycle(self):
        first_date = date(2099, 12, 27)
        second_date = date(2099, 12, 28)
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = self._new_symbol(session)
            existing = DailyPriceBar(
                symbol_id=symbol.id,
                eod_run_id=None,
                trade_date=first_date,
                open_price=777,
                high_price=888,
                low_price=666,
                close_price=800,
                volume=123,
                turnover=98_400,
                per=Decimal('7.7'),
                source='protected_existing',
            )
            session.add(existing)
            session.flush()
            before = (
                existing.open_price,
                existing.high_price,
                existing.low_price,
                existing.close_price,
                existing.volume,
                existing.turnover,
                existing.per,
                existing.source,
                existing.captured_at,
                symbol.market,
                symbol.source,
                symbol.is_active,
            )

            def scope():
                return self._savepoint_scope(connection, session)

            try:
                with patch(
                    'app.eod.symbol_history_repair.session_scope',
                    scope,
                ):
                    result = repair_missing_symbol_history(
                        symbols=[symbol.symbol],
                        start_date=first_date,
                        end_date=second_date,
                        bars=[
                            _bar(symbol.symbol, first_date, close_price=101),
                            _bar(
                                symbol.symbol,
                                second_date,
                                close_price=106,
                                turnover=106_000,
                            ),
                        ],
                        apply=True,
                        source='kis',
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['source'], 'kis')
                self.assertEqual(result['existing_bar_count'], 1)
                self.assertEqual(result['inserted_count'], 1)
                session.expire_all()
                persisted_existing = session.execute(
                    select(DailyPriceBar).where(
                        DailyPriceBar.symbol_id == symbol.id,
                        DailyPriceBar.trade_date == first_date,
                    )
                ).scalar_one()
                persisted_symbol = session.get(Symbol, symbol.id)
                after = (
                    persisted_existing.open_price,
                    persisted_existing.high_price,
                    persisted_existing.low_price,
                    persisted_existing.close_price,
                    persisted_existing.volume,
                    persisted_existing.turnover,
                    persisted_existing.per,
                    persisted_existing.source,
                    persisted_existing.captured_at,
                    persisted_symbol.market,
                    persisted_symbol.source,
                    persisted_symbol.is_active,
                )
                self.assertEqual(after, before)

                inserted = session.execute(
                    select(DailyPriceBar).where(
                        DailyPriceBar.symbol_id == symbol.id,
                        DailyPriceBar.trade_date == second_date,
                    )
                ).scalar_one()
                self.assertEqual(inserted.turnover, 106_000)
                self.assertEqual(inserted.source, 'kis')
                self.assertIsNone(inserted.per)
                self.assertIsNone(inserted.pbr)
                self.assertIsNone(inserted.eod_run_id)
            finally:
                session.close()
                transaction.rollback()

    def test_insert_count_mismatch_rolls_back_all_rows(self):
        first_date = date(2099, 12, 27)
        second_date = date(2099, 12, 28)
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = self._new_symbol(session)

            @contextmanager
            def scope():
                savepoint = connection.begin_nested()
                try:
                    yield session
                    session.flush()
                    savepoint.commit()
                except Exception:
                    savepoint.rollback()
                    session.expire_all()
                    raise

            real_execute = session.execute

            class _ShortScalarResult:
                def __init__(self, result):
                    self.result = result

                def all(self):
                    rows = self.result.all()
                    return rows[:-1]

            class _ShortResult:
                def __init__(self, result):
                    self.result = result

                def scalars(self):
                    return _ShortScalarResult(self.result.scalars())

            def execute(statement, *args, **kwargs):
                result = real_execute(statement, *args, **kwargs)
                if (
                    bool(getattr(statement, 'is_insert', False))
                    and getattr(getattr(statement, 'table', None), 'name', None)
                    == 'daily_price_bars'
                ):
                    return _ShortResult(result)
                return result

            try:
                with (
                    patch(
                        'app.eod.symbol_history_repair.session_scope',
                        scope,
                    ),
                    patch.object(session, 'execute', side_effect=execute),
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        'insert count changed',
                    ):
                        repair_missing_symbol_history(
                            symbols=[symbol.symbol],
                            start_date=first_date,
                            end_date=second_date,
                            bars=[
                                _bar(symbol.symbol, first_date),
                                _bar(symbol.symbol, second_date),
                            ],
                            apply=True,
                        )

                count = session.execute(
                    select(func.count(DailyPriceBar.id)).where(
                        DailyPriceBar.symbol_id == symbol.id,
                        DailyPriceBar.trade_date.in_([first_date, second_date]),
                    )
                ).scalar_one()
                self.assertEqual(count, 0)
            finally:
                session.close()
                transaction.rollback()


class SymbolHistoryRepairCliTests(unittest.TestCase):
    def test_cli_defaults_to_dry_run_and_propagates_blocked_status(self):
        with (
            patch(
                'app.eod.symbol_history_repair.symbol_history_repair_service.'
                'repair_range',
                return_value={'status': 'ready', 'applied': False},
            ) as repair,
            patch('sys.stdout.write'),
        ):
            self.assertEqual(
                main(
                    [
                        '--symbols',
                        '005930,000660',
                        '--start-date',
                        '2025-09-23',
                        '--end-date',
                        '2026-09-23',
                    ]
                ),
                0,
            )
        self.assertFalse(repair.call_args.kwargs['apply'])
        self.assertEqual(
            repair.call_args.kwargs['symbols'],
            ('005930', '000660'),
        )

        with (
            patch(
                'app.eod.symbol_history_repair.symbol_history_repair_service.'
                'repair_range',
                return_value={'status': 'blocked', 'applied': False},
            ),
            patch('sys.stdout.write'),
        ):
            self.assertEqual(
                main(
                    [
                        '--symbols',
                        '005930',
                        '--start-date',
                        '2026-09-23',
                        '--end-date',
                        '2026-09-23',
                    ]
                ),
                2,
            )


if __name__ == '__main__':
    unittest.main()
