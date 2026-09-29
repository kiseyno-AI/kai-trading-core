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
from app.eod.missing_bar_repair import (
    MISSING_BAR_INSERT_BATCH_SIZE,
    MissingDailyBar,
    MissingDailyBarRepairService,
    _extract_market_fundamentals,
    _extract_market_ohlcv,
    _missing_bar_insert_batches,
    main,
    repair_missing_daily_price_bars,
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


def _ohlcv_row(
    *,
    open_price: object = 100,
    high_price: object = 110,
    low_price: object = 90,
    close_price: object = 105,
    volume: object = 1_000,
    turnover: object = 105_000,
) -> dict[str, object]:
    return {
        '시가': open_price,
        '고가': high_price,
        '저가': low_price,
        '종가': close_price,
        '거래량': volume,
        '거래대금': turnover,
    }


def _fundamental_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        'PER': 10.5,
        'PBR': 1.2,
        'BPS': 50_000,
        'EPS': 5_000,
        'DIV': 2.3,
        'DPS': 1_200,
    }
    values.update(overrides)
    return values


def _available_lock(**_kwargs):
    @contextmanager
    def lock():
        yield True

    return lock()


class MissingBarProviderValidationTests(unittest.TestCase):
    def test_missing_bar_insert_batches_bound_statement_size(self):
        values = [
            {'symbol_id': index}
            for index in range(MISSING_BAR_INSERT_BATCH_SIZE * 2 + 1)
        ]

        batches = list(_missing_bar_insert_batches(values))

        self.assertEqual(
            [len(batch) for batch in batches],
            [MISSING_BAR_INSERT_BATCH_SIZE, MISSING_BAR_INSERT_BATCH_SIZE, 1],
        )
        self.assertEqual(
            [item for batch in batches for item in batch],
            values,
        )

    def test_extracts_required_ohlcv_and_skips_non_numeric_codes(self):
        frame = _FakeFrame(
            {
                '005930': _ohlcv_row(),
                '00104K': _ohlcv_row(),
            }
        )

        bars, skipped = _extract_market_ohlcv(frame, market='KOSPI')

        self.assertEqual([bar.symbol for bar in bars], ['005930'])
        self.assertEqual(bars[0].turnover, 105_000)
        self.assertEqual(skipped, ['00104K'])

    def test_rejects_positive_volume_without_positive_turnover(self):
        frame = _FakeFrame(
            {'005930': _ohlcv_row(volume=1, turnover=0)}
        )

        with self.assertRaisesRegex(
            ValueError,
            'positive-volume row has non-positive turnover',
        ):
            _extract_market_ohlcv(frame, market='KOSPI')

    def test_rejects_traded_row_with_non_positive_ohlc(self):
        frame = _FakeFrame(
            {
                '005930': _ohlcv_row(
                    open_price=0,
                    high_price=0,
                    low_price=0,
                    close_price=0,
                    volume=1,
                    turnover=1,
                )
            }
        )

        with self.assertRaisesRegex(
            ValueError,
            'positive-volume row has non-positive OHLC',
        ):
            _extract_market_ohlcv(frame, market='KOSPI')

    def test_zero_volume_requires_zero_turnover_but_allows_halted_quote_shape(self):
        invalid = _FakeFrame(
            {
                '005930': _ohlcv_row(
                    open_price=0,
                    high_price=0,
                    low_price=0,
                    close_price=105,
                    volume=0,
                    turnover=1,
                )
            }
        )
        with self.assertRaisesRegex(
            ValueError,
            'zero-volume row has non-zero turnover',
        ):
            _extract_market_ohlcv(invalid, market='KOSPI')

        valid = _FakeFrame(
            {
                '005930': _ohlcv_row(
                    open_price=0,
                    high_price=0,
                    low_price=0,
                    close_price=105,
                    volume=0,
                    turnover=0,
                )
            }
        )
        bars, _ = _extract_market_ohlcv(valid, market='KOSPI')
        self.assertEqual(bars[0].close_price, 105)

    def test_rejects_missing_required_column_and_non_integral_values(self):
        missing_column = _FakeFrame(
            {'005930': _ohlcv_row()},
            columns=['시가', '고가', '저가', '종가', '거래량'],
        )
        with self.assertRaisesRegex(ValueError, '거래대금'):
            _extract_market_ohlcv(missing_column, market='KOSPI')

        non_integral = _FakeFrame(
            {'005930': _ohlcv_row(close_price=105.5)}
        )
        with self.assertRaisesRegex(ValueError, 'close_price is not an integer'):
            _extract_market_ohlcv(non_integral, market='KOSPI')

    def test_rejects_partial_fundamental_schema(self):
        frame = _FakeFrame(
            {'005930': {'PER': 10.5}},
            columns=['PER'],
        )

        with self.assertRaisesRegex(ValueError, 'missing columns'):
            _extract_market_fundamentals(frame, market='KOSPI')


class MissingDailyBarRepairServiceTests(unittest.TestCase):
    def test_dry_run_uses_exact_two_markets_and_market_wide_fundamentals(self):
        ohlcv_calls: list[tuple[str, str]] = []
        fundamental_calls: list[tuple[str, str]] = []
        database_calls: list[dict] = []
        codes = {'KOSPI': '005930', 'KOSDAQ': '035720'}

        def load_ohlcv(trade_date: str, market: str):
            ohlcv_calls.append((trade_date, market))
            return _FakeFrame({codes[market]: _ohlcv_row()})

        def load_fundamentals(trade_date: str, market: str):
            fundamental_calls.append((trade_date, market))
            return _FakeFrame({codes[market]: _fundamental_row()})

        def database_repairer(**kwargs):
            database_calls.append(kwargs)
            return {
                'status': 'ready',
                'applied': False,
                'missing_bar_count': 2,
                'would_insert_count': 2,
                'inserted_count': 0,
            }

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=load_ohlcv,
            market_fundamental_loader=load_fundamentals,
            database_repairer=database_repairer,
            security_group_loader=lambda: self.fail(
                'complete fundamentals must not load current master'
            ),
            lock_factory=_available_lock,
            minimum_market_membership={'KOSPI': 1, 'KOSDAQ': 1},
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
        )

        self.assertEqual(result['status'], 'ready')
        self.assertFalse(result['applied'])
        self.assertEqual(
            ohlcv_calls,
            [('20260909', 'KOSPI'), ('20260909', 'KOSDAQ')],
        )
        self.assertEqual(fundamental_calls, ohlcv_calls)
        self.assertEqual(len(database_calls), 1)
        self.assertFalse(database_calls[0]['apply'])
        bars = database_calls[0]['bars']
        self.assertEqual([bar.symbol for bar in bars], ['005930', '035720'])
        self.assertEqual(bars[0].per, Decimal('10.5'))
        self.assertEqual(bars[0].dividend_yield, Decimal('2.3'))
        self.assertEqual(result['provider_missing_fundamental_count'], 0)
        self.assertEqual(len(result['membership_sha256']), 64)

    def test_fundamentals_are_optional_per_symbol_and_gaps_are_reported(self):
        frames = {
            'KOSPI': _FakeFrame(
                {
                    '005930': _ohlcv_row(),
                    '000660': _ohlcv_row(),
                    '00104K': _ohlcv_row(),
                }
            ),
            'KOSDAQ': _FakeFrame({'035720': _ohlcv_row()}),
        }
        fundamental_frames = {
            'KOSPI': _FakeFrame(
                {
                    '005930': _fundamental_row(),
                    '123456': _fundamental_row(),
                    '12AB56': _fundamental_row(),
                }
            ),
            'KOSDAQ': _FakeFrame({'035720': _fundamental_row()}),
        }
        database_calls: list[dict] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: frames[market],
            market_fundamental_loader=(
                lambda _date, market: fundamental_frames[market]
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
            security_group_loader=lambda: {
                '005930': 'ST',
                '000660': 'RT',
                '035720': 'ST',
            },
            lock_factory=_available_lock,
            minimum_market_membership={'KOSPI': 2, 'KOSDAQ': 1},
        )

        result = service.repair_for_market_date(market_date='20260909')

        self.assertEqual(result['provider_skipped_ohlcv_code_count'], 1)
        self.assertEqual(result['provider_skipped_fundamental_code_count'], 1)
        self.assertEqual(result['provider_missing_fundamental_count'], 1)
        self.assertEqual(
            result['provider_missing_fundamental_symbols'],
            ['000660'],
        )
        self.assertEqual(result['provider_extra_fundamental_count'], 1)
        self.assertEqual(
            result['provider_extra_fundamental_symbols'],
            ['123456'],
        )
        bars_by_symbol = {
            bar.symbol: bar for bar in database_calls[0]['bars']
        }
        self.assertIsNone(bars_by_symbol['000660'].per)

    def test_missing_fundamental_row_for_stock_blocks_repair(self):
        frames = {
            'KOSPI': _FakeFrame({'005930': _ohlcv_row()}),
            'KOSDAQ': _FakeFrame({'035720': _ohlcv_row()}),
        }
        fundamental_frames = {
            'KOSPI': _FakeFrame({'123456': _fundamental_row()}),
            'KOSDAQ': _FakeFrame({'035720': _fundamental_row()}),
        }
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: frames[market],
            market_fundamental_loader=(
                lambda _date, market: fundamental_frames[market]
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            security_group_loader=lambda: {
                '005930': 'ST',
                '035720': 'ST',
            },
            lock_factory=_available_lock,
            minimum_market_membership={'KOSPI': 1, 'KOSDAQ': 1},
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'required_fundamental_rows_missing')
        self.assertEqual(
            result['required_missing_fundamental_symbols'],
            ['005930'],
        )

    def test_membership_below_floor_blocks_before_fundamentals_or_database(self):
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda *_args: _FakeFrame(
                {'005930': _ohlcv_row()}
            ),
            market_fundamental_loader=lambda *_args: self.fail(
                'fundamental loader must not run'
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            security_group_loader=lambda: {},
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'market_membership_below_safety_floor')
        self.assertEqual(result['failed_market'], 'KOSPI')

    def test_non_session_and_unavailable_lock_block_before_provider_fetch(self):
        provider_calls: list[tuple[str, str]] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-08',
            market_ohlcv_loader=lambda *args: provider_calls.append(args),
            market_fundamental_loader=lambda *args: provider_calls.append(args),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            security_group_loader=lambda: {},
            lock_factory=_available_lock,
        )
        result = service.repair_for_market_date(market_date='2026-09-09')
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'not_an_exact_trading_session')
        self.assertEqual(provider_calls, [])

        @contextmanager
        def unavailable_lock(**_kwargs):
            yield False

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: self.fail(
                'resolver must not run'
            ),
            market_ohlcv_loader=lambda *args: provider_calls.append(args),
            market_fundamental_loader=lambda *args: provider_calls.append(args),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            lock_factory=unavailable_lock,
        )
        result = service.repair_for_market_date(market_date='2026-09-09')
        self.assertEqual(result['reason'], 'advisory_lock_unavailable')
        self.assertEqual(provider_calls, [])


class MissingDailyBarRepairRepositoryPostgresTests(unittest.TestCase):
    def test_apply_preserves_existing_row_and_inserts_only_missing_row(self):
        market_date = date(2099, 12, 29)
        first_code = f'{900000 + uuid4().int % 49_999:06d}'
        second_code = f'{950000 + uuid4().int % 49_999:06d}'
        while second_code == first_code:
            second_code = f'{950000 + uuid4().int % 49_999:06d}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            first = Symbol(
                symbol=first_code,
                name_ko='Existing row test',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            second = Symbol(
                symbol=second_code,
                name_ko='Missing row test',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([first, second])
            session.flush()
            existing = DailyPriceBar(
                symbol_id=first.id,
                eod_run_id=None,
                trade_date=market_date,
                open_price=777,
                high_price=888,
                low_price=666,
                close_price=800,
                volume=123,
                turnover=98_400,
                per=Decimal('7.7'),
                pbr=Decimal('0.8'),
                bps=Decimal('999.9'),
                eps=Decimal('101.1'),
                dividend_yield=Decimal('3.3'),
                dps=Decimal('44.4'),
                source='protected_existing',
            )
            session.add(existing)
            session.flush()

            before = (
                existing.eod_run_id,
                existing.open_price,
                existing.high_price,
                existing.low_price,
                existing.close_price,
                existing.volume,
                existing.turnover,
                existing.per,
                existing.pbr,
                existing.bps,
                existing.eps,
                existing.dividend_yield,
                existing.dps,
                existing.source,
                existing.captured_at,
            )

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            bars = [
                MissingDailyBar(
                    symbol=first_code,
                    market='KOSPI',
                    open_price=1,
                    high_price=2,
                    low_price=1,
                    close_price=2,
                    volume=999,
                    turnover=1_998,
                    per=Decimal('99'),
                ),
                MissingDailyBar(
                    symbol=second_code,
                    market='KOSDAQ',
                    open_price=100,
                    high_price=110,
                    low_price=90,
                    close_price=105,
                    volume=1_000,
                    turnover=105_000,
                    per=Decimal('10.5'),
                    pbr=Decimal('1.2'),
                    bps=Decimal('50000'),
                    eps=Decimal('5000'),
                    dividend_yield=Decimal('2.3'),
                    dps=Decimal('1200'),
                ),
            ]

            try:
                with patch(
                    'app.eod.missing_bar_repair.session_scope',
                    rollback_only_session_scope,
                ):
                    result = repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=bars,
                        apply=True,
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['missing_bar_count'], 1)
                self.assertEqual(result['inserted_count'], 1)
                self.assertEqual(result['current_market_mismatch_count'], 1)
                self.assertEqual(
                    result['current_market_mismatch_symbols'],
                    [second_code],
                )

                session.expire_all()
                persisted_existing = session.execute(
                    select(DailyPriceBar).where(
                        DailyPriceBar.symbol_id == first.id,
                        DailyPriceBar.trade_date == market_date,
                    )
                ).scalar_one()
                after = (
                    persisted_existing.eod_run_id,
                    persisted_existing.open_price,
                    persisted_existing.high_price,
                    persisted_existing.low_price,
                    persisted_existing.close_price,
                    persisted_existing.volume,
                    persisted_existing.turnover,
                    persisted_existing.per,
                    persisted_existing.pbr,
                    persisted_existing.bps,
                    persisted_existing.eps,
                    persisted_existing.dividend_yield,
                    persisted_existing.dps,
                    persisted_existing.source,
                    persisted_existing.captured_at,
                )
                self.assertEqual(after, before)

                inserted = session.execute(
                    select(DailyPriceBar).where(
                        DailyPriceBar.symbol_id == second.id,
                        DailyPriceBar.trade_date == market_date,
                    )
                ).scalar_one()
                self.assertEqual(inserted.source, 'pykrx')
                self.assertIsNone(inserted.eod_run_id)
                self.assertEqual(inserted.open_price, 100)
                self.assertEqual(inserted.turnover, 105_000)
                self.assertEqual(inserted.per, Decimal('10.500000'))
                self.assertEqual(inserted.dividend_yield, Decimal('2.300000'))
            finally:
                session.close()
                transaction.rollback()

    def test_stored_krx_row_outside_provider_membership_blocks_apply(self):
        market_date = date(2099, 12, 28)
        expected_code = f'{900000 + uuid4().int % 40_000:06d}'
        extra_code = f'{940000 + uuid4().int % 40_000:06d}'
        while extra_code == expected_code:
            extra_code = f'{940000 + uuid4().int % 40_000:06d}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            expected = Symbol(
                symbol=expected_code,
                name_ko='Expected',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            extra = Symbol(
                symbol=extra_code,
                name_ko='Extra',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([expected, extra])
            session.flush()
            session.add(
                DailyPriceBar(
                    symbol_id=extra.id,
                    trade_date=market_date,
                    open_price=100,
                    high_price=110,
                    low_price=90,
                    close_price=105,
                    volume=1_000,
                    turnover=105_000,
                    source='test',
                )
            )
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                with patch(
                    'app.eod.missing_bar_repair.session_scope',
                    rollback_only_session_scope,
                ):
                    result = repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=[
                            MissingDailyBar(
                                symbol=expected_code,
                                market='KOSPI',
                                open_price=100,
                                high_price=110,
                                low_price=90,
                                close_price=105,
                                volume=1_000,
                                turnover=105_000,
                            )
                        ],
                        apply=True,
                    )

                self.assertEqual(result['status'], 'blocked')
                self.assertEqual(
                    result['reason'],
                    'stored_rows_outside_provider_membership',
                )
                self.assertEqual(
                    result['stored_outside_provider_membership_symbols'],
                    [extra_code],
                )
                self.assertIsNone(
                    session.execute(
                        select(DailyPriceBar).where(
                            DailyPriceBar.symbol_id == expected.id,
                            DailyPriceBar.trade_date == market_date,
                        )
                    ).scalar_one_or_none()
                )
            finally:
                session.close()
                transaction.rollback()


class MissingDailyBarRepairCliTests(unittest.TestCase):
    def test_cli_defaults_to_dry_run_and_returns_nonzero_when_blocked(self):
        with (
            patch(
                'app.eod.missing_bar_repair.missing_daily_bar_repair_service.'
                'repair_for_market_date',
                return_value={'status': 'ready', 'applied': False},
            ) as repair,
            patch('sys.stdout.write'),
        ):
            self.assertEqual(main(['--market-date', '2026-09-09']), 0)
        self.assertFalse(repair.call_args.kwargs['apply'])

        with (
            patch(
                'app.eod.missing_bar_repair.missing_daily_bar_repair_service.'
                'repair_for_market_date',
                return_value={'status': 'blocked', 'applied': False},
            ),
            patch('sys.stdout.write'),
        ):
            self.assertEqual(main(['--market-date', '2026-09-09']), 2)


if __name__ == '__main__':
    unittest.main()
