from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import (
    DailyPipelineRun,
    DailyPriceBar,
    DailyPriceBarRepairBatch,
    EODCollectionRun,
    Symbol,
)
from app.eod.market_membership import (
    build_market_membership_snapshot,
    market_membership_sha256,
)
from app.eod.missing_bar_repair import (
    MISSING_BAR_INSERT_BATCH_SIZE,
    MissingDailyBar,
    MissingDailyBarRepairService,
    _downstream_fallback_bundle_sha256,
    _extract_market_fundamentals,
    _extract_market_ohlcv,
    _extract_symbol_fundamental,
    _missing_bar_insert_batches,
    _load_downstream_membership_manifests,
    _provider_payload_sha256,
    _skipped_provider_codes_sha256,
    main,
    prepare_downstream_fallback_membership_bundle,
    repair_missing_daily_price_bars,
)
from app.eod.price_bar_repair_state import (
    _missing_bar_insert_repair_evidence,
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


def _core_api_inactive() -> bool:
    return False


def _reviewed_apply_kwargs(
    *,
    market_date: date,
    bars: list[MissingDailyBar],
    missing_symbols: list[str],
    required_rebuild_trade_dates: list[date] | None = None,
    historical_symbol_identities: dict[str, dict[str, str]] | None = None,
    excluded_ohlcv_codes: list[dict[str, str]] | None = None,
    excluded_fundamental_codes: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        'expected_provider_membership_count': len(bars),
        'expected_membership_sha256': market_membership_sha256(
            [bar.symbol for bar in bars]
        ),
        'expected_provider_payload_sha256': _provider_payload_sha256(
            market_date=market_date,
            bars=bars,
            historical_symbol_identities=historical_symbol_identities,
            excluded_ohlcv_codes=excluded_ohlcv_codes,
            excluded_fundamental_codes=excluded_fundamental_codes,
        ),
        'expected_missing_bar_count': len(missing_symbols),
        'expected_missing_membership_sha256': market_membership_sha256(
            missing_symbols
        ),
        'required_rebuild_trade_dates': (
            required_rebuild_trade_dates or [market_date]
        ),
    }


def _strict_universe_snapshot_metadata(
    *,
    market_date: date,
    symbols_by_market: dict[str, list[str]],
) -> dict[str, object]:
    markets = list(symbols_by_market)
    members = [
        SimpleNamespace(symbol=symbol, market=market)
        for market, symbols in symbols_by_market.items()
        for symbol in symbols
    ]
    membership = build_market_membership_snapshot(
        symbols=members,
        markets=markets,
    )
    return {
        'universe_snapshot': {
            'complete': True,
            'latest_trade_date': market_date.isoformat(),
            'universe_source': 'pykrx_market_ohlcv',
            'source': 'pykrx',
            'markets': markets,
            'market_status': {
                market: 'pykrx_market_ohlcv' for market in markets
            },
            'market_errors': [],
            'unavailable_markets': [],
            'membership_basis': 'end_date_snapshot',
            'market_membership': membership,
            'symbol_count': len(members),
        }
    }


def _prepared_downstream_manifest(
    *,
    market_date: date,
) -> dict[str, object]:
    members = [
        SimpleNamespace(symbol='005930', market='KOSPI'),
        SimpleNamespace(symbol='035720', market='KOSDAQ'),
    ]
    symbols = sorted(member.symbol for member in members)
    return {
        'basis': 'reviewed_live_exact_date_pykrx_membership',
        'source': 'pykrx',
        'universe_source': 'pykrx_market_ohlcv',
        'market_date': market_date.isoformat(),
        'markets': ['KOSPI', 'KOSDAQ'],
        'market_membership': build_market_membership_snapshot(
            symbols=members,
            markets=('KOSPI', 'KOSDAQ'),
        ),
        'symbols': symbols,
        'symbol_count': len(symbols),
        'membership_sha256': market_membership_sha256(symbols),
        'excluded_ohlcv_codes': [],
        'excluded_ohlcv_code_sha256': _skipped_provider_codes_sha256([]),
        'daily_price_input_count': len(symbols),
        'daily_price_input_sha256': 'a' * 64,
    }


class MissingBarProviderValidationTests(unittest.TestCase):
    def test_fallback_bundle_uses_exact_date_krx_membership_only(self):
        target_date = date(2026, 9, 8)
        downstream_date = date(2026, 9, 9)
        resolver_calls: list[str] = []
        market_calls: list[tuple[str, str]] = []

        def resolve_date(*, end_date: str) -> str:
            resolver_calls.append(end_date)
            return downstream_date.isoformat()

        def load_market(trade_date: str, market: str):
            market_calls.append((trade_date, market))
            return _FakeFrame(
                {
                    ('005930' if market == 'KOSPI' else '035720'): (
                        _ohlcv_row()
                    )
                }
            )

        @contextmanager
        def fake_session_scope():
            yield MagicMock()

        with (
            patch.dict(
                'app.eod.missing_bar_repair.MINIMUM_MARKET_MEMBERSHIP',
                {'KOSPI': 1, 'KOSDAQ': 1, 'KONEX': 1},
            ),
            patch(
                'app.eod.missing_bar_repair.session_scope',
                side_effect=fake_session_scope,
            ),
            patch(
                'app.eod.missing_bar_repair.'
                '_select_strict_stored_membership',
                return_value=(None, None),
            ),
            patch(
                'app.eod.missing_bar_repair.'
                'exact_daily_price_bar_scope_fingerprint',
                return_value=('a' * 64, 2),
            ),
        ):
            bundle = prepare_downstream_fallback_membership_bundle(
                target_date=target_date,
                required_rebuild_dates=(target_date, downstream_date),
                trade_date_resolver=resolve_date,
                market_ohlcv_loader=load_market,
                minimum_market_membership={
                    'KOSPI': 1,
                    'KOSDAQ': 1,
                    'KONEX': 1,
                },
            )

        self.assertEqual(resolver_calls, ['20260909'])
        self.assertEqual(
            market_calls,
            [('20260909', 'KOSPI'), ('20260909', 'KOSDAQ')],
        )
        self.assertEqual(bundle['manifest_count'], 1)
        manifest = bundle['manifests'][downstream_date.isoformat()]
        self.assertEqual(manifest['markets'], ['KOSPI', 'KOSDAQ'])
        self.assertEqual(manifest['daily_price_input_count'], 2)
        self.assertEqual(
            bundle['bundle_sha256'],
            _downstream_fallback_bundle_sha256(bundle['manifests']),
        )

    def test_fallback_bundle_rejects_rolled_back_trade_date(self):
        target_date = date(2026, 9, 8)
        downstream_date = date(2026, 9, 9)

        @contextmanager
        def fake_session_scope():
            yield MagicMock()

        with (
            patch(
                'app.eod.missing_bar_repair.session_scope',
                side_effect=fake_session_scope,
            ),
            patch(
                'app.eod.missing_bar_repair.'
                '_select_strict_stored_membership',
                return_value=(None, None),
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'not an exact trading session',
            ):
                prepare_downstream_fallback_membership_bundle(
                    target_date=target_date,
                    required_rebuild_dates=(target_date, downstream_date),
                    trade_date_resolver=lambda **_kwargs: '2026-09-08',
                    market_ohlcv_loader=lambda *_args: self.fail(
                        'rolled date must block before market loading'
                    ),
                    minimum_market_membership={
                        'KOSPI': 1,
                        'KOSDAQ': 1,
                        'KONEX': 1,
                    },
                )

    def test_prepared_downstream_fallback_is_revalidated_in_transaction(self):
        target_date = date(2026, 9, 8)
        downstream_date = date(2026, 9, 9)
        manifest = _prepared_downstream_manifest(
            market_date=downstream_date
        )
        session = MagicMock()

        with (
            patch.dict(
                'app.eod.missing_bar_repair.MINIMUM_MARKET_MEMBERSHIP',
                {'KOSPI': 1, 'KOSDAQ': 1, 'KONEX': 1},
            ),
            patch(
                'app.eod.missing_bar_repair.'
                '_select_strict_stored_membership',
                return_value=(None, None),
            ),
            patch(
                'app.eod.missing_bar_repair.'
                'exact_daily_price_bar_scope_fingerprint',
                return_value=('a' * 64, 2),
            ),
        ):
            result = _load_downstream_membership_manifests(
                session,
                target_date=target_date,
                required_rebuild_dates=(target_date, downstream_date),
                prepared_fallback_manifests={
                    downstream_date.isoformat(): manifest
                },
            )

        self.assertEqual(
            result[downstream_date.isoformat()]['basis'],
            'reviewed_live_exact_date_pykrx_membership',
        )
        self.assertIsNone(
            result[downstream_date.isoformat()]['eod_run_id']
        )

    def test_prepared_downstream_fallback_rejects_price_input_drift(self):
        target_date = date(2026, 9, 8)
        downstream_date = date(2026, 9, 9)
        manifest = _prepared_downstream_manifest(
            market_date=downstream_date
        )

        with (
            patch.dict(
                'app.eod.missing_bar_repair.MINIMUM_MARKET_MEMBERSHIP',
                {'KOSPI': 1, 'KOSDAQ': 1, 'KONEX': 1},
            ),
            patch(
                'app.eod.missing_bar_repair.'
                '_select_strict_stored_membership',
                return_value=(None, None),
            ),
            patch(
                'app.eod.missing_bar_repair.'
                'exact_daily_price_bar_scope_fingerprint',
                return_value=('b' * 64, 2),
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'prepared downstream price input changed',
            ):
                _load_downstream_membership_manifests(
                    MagicMock(),
                    target_date=target_date,
                    required_rebuild_dates=(target_date, downstream_date),
                    prepared_fallback_manifests={
                        downstream_date.isoformat(): manifest
                    },
                )

    def test_repository_apply_requires_reviewed_membership_gate(self):
        with self.assertRaisesRegex(
            ValueError,
            'apply requires reviewed provider membership',
        ):
            repair_missing_daily_price_bars(
                market_date='2099-12-31',
                bars=[
                    MissingDailyBar(
                        symbol='005930',
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

    def test_rejects_arbitrary_empty_konex_fundamental_payload(self):
        empty_frame = _FakeFrame({}, columns=[])

        for market in ('KOSPI', 'KOSDAQ', 'KONEX'):
            with self.subTest(market=market), self.assertRaisesRegex(
                ValueError,
                'missing columns',
            ):
                _extract_market_fundamentals(empty_frame, market=market)

    def test_rejects_nonempty_konex_payload_with_partial_schema(self):
        frame = _FakeFrame(
            {'900001': {'PER': 10.5}},
            columns=['PER'],
        )

        with self.assertRaisesRegex(ValueError, 'missing columns'):
            _extract_market_fundamentals(frame, market='KONEX')

    def test_exact_symbol_fundamental_distinguishes_absence_and_schema_error(self):
        self.assertIsNone(
            _extract_symbol_fundamental(
                _FakeFrame({}, columns=[]),
                symbol='005930',
            )
        )
        values = _extract_symbol_fundamental(
            _FakeFrame({'2026-09-09': _fundamental_row()}),
            symbol='005930',
        )
        self.assertEqual(values['per'], Decimal('10.5'))
        with self.assertRaisesRegex(ValueError, 'missing columns'):
            _extract_symbol_fundamental(
                _FakeFrame(
                    {'2026-09-09': {'PER': 10.5}},
                    columns=['PER'],
                ),
                symbol='005930',
            )


class MissingDailyBarRepairServiceTests(unittest.TestCase):
    def test_dry_run_uses_all_official_raw_markets_and_fundamentals(self):
        ohlcv_calls: list[tuple[str, str]] = []
        fundamental_calls: list[tuple[str, str]] = []
        database_calls: list[dict] = []
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }

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
                'missing_bar_count': 3,
                'would_insert_count': 3,
                'inserted_count': 0,
            }

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=load_ohlcv,
            market_fundamental_loader=load_fundamentals,
            database_repairer=database_repairer,
            core_api_active_checker=lambda: self.fail(
                'dry-run must not inspect the Core API port'
            ),
            security_group_loader=lambda: self.fail(
                'complete fundamentals must not load current master'
            ),
            lock_factory=_available_lock,
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
        )

        self.assertEqual(result['status'], 'ready')
        self.assertFalse(result['applied'])
        self.assertEqual(
            ohlcv_calls,
            [
                ('20260909', 'KOSPI'),
                ('20260909', 'KOSDAQ'),
                ('20260909', 'KONEX'),
            ],
        )
        self.assertEqual(fundamental_calls, ohlcv_calls)
        self.assertEqual(len(database_calls), 1)
        self.assertFalse(database_calls[0]['apply'])
        bars = database_calls[0]['bars']
        self.assertEqual(
            [bar.symbol for bar in bars],
            ['005930', '035720', '900001'],
        )
        self.assertEqual(bars[0].per, Decimal('10.5'))
        self.assertEqual(bars[0].dividend_yield, Decimal('2.3'))
        self.assertEqual(result['provider_missing_fundamental_count'], 0)
        self.assertEqual(result['provider_membership_count'], 3)
        self.assertEqual(len(result['membership_sha256']), 64)
        self.assertEqual(
            result['membership_sha256'],
            market_membership_sha256(['005930', '035720', '900001']),
        )
        self.assertIsNone(
            database_calls[0]['expected_provider_membership_count']
        )
        self.assertIsNone(database_calls[0]['expected_membership_sha256'])

    def test_apply_holds_target_and_full_downstream_lock_scope(self):
        target = date(2026, 9, 9)
        downstream = date(2026, 9, 10)
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        expected_bars = [
            MissingDailyBar(
                symbol=codes[market],
                market=market,
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
            )
            for market in ('KOSPI', 'KOSDAQ', 'KONEX')
        ]
        lock_calls: list[dict] = []
        database_calls: list[dict] = []

        @contextmanager
        def lock_set(**kwargs):
            lock_calls.append(kwargs)
            yield True, None

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: target.isoformat(),
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _fundamental_row()}
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'completed', 'applied': True}
            ),
            core_api_active_checker=_core_api_inactive,
            lock_set_factory=lock_set,
            rebuild_date_loader=lambda **_kwargs: [target, downstream],
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(
            market_date=target,
            apply=True,
            expected_provider_membership_count=3,
            expected_membership_sha256=market_membership_sha256(
                list(codes.values())
            ),
            expected_provider_payload_sha256=_provider_payload_sha256(
                market_date=target,
                bars=expected_bars,
            ),
            expected_missing_bar_count=3,
            expected_missing_membership_sha256=market_membership_sha256(
                list(codes.values())
            ),
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_dates': (target, downstream),
                    'source': 'daily_price_bars',
                    'exclusive_admission': True,
                }
            ],
        )
        self.assertEqual(
            database_calls[0]['required_rebuild_trade_dates'],
            (target, downstream),
        )

    def test_apply_binds_reviewed_downstream_fallback_bundle(self):
        target = date(2026, 9, 8)
        downstream = date(2026, 9, 9)
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        bars = [
            MissingDailyBar(
                symbol=code,
                market=market,
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
            )
            for market, code in codes.items()
        ]
        fallback_manifests = {
            downstream.isoformat(): _prepared_downstream_manifest(
                market_date=downstream
            )
        }
        fallback_bundle = {
            'schema_version': 'missing_bar_downstream_membership_v1',
            'manifest_count': 1,
            'bundle_sha256': _downstream_fallback_bundle_sha256(
                fallback_manifests
            ),
            'manifests': fallback_manifests,
        }
        database_calls: list[dict] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: target.isoformat(),
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _fundamental_row()}
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'completed', 'applied': True}
            ),
            downstream_membership_bundle_preparer=(
                lambda **_kwargs: fallback_bundle
            ),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
            rebuild_date_loader=lambda **_kwargs: [target, downstream],
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )
        reviewed = {
            'expected_provider_membership_count': 3,
            'expected_membership_sha256': market_membership_sha256(
                list(codes.values())
            ),
            'expected_provider_payload_sha256': _provider_payload_sha256(
                market_date=target,
                bars=bars,
            ),
            'expected_missing_bar_count': 3,
            'expected_missing_membership_sha256': market_membership_sha256(
                list(codes.values())
            ),
        }

        blocked = service.repair_for_market_date(
            market_date=target,
            apply=True,
            **reviewed,
        )
        self.assertEqual(
            blocked['reason'],
            'reviewed_downstream_fallback_evidence_required',
        )

        result = service.repair_for_market_date(
            market_date=target,
            apply=True,
            expected_downstream_fallback_manifest_count=1,
            expected_downstream_fallback_bundle_sha256=(
                fallback_bundle['bundle_sha256']
            ),
            **reviewed,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['downstream_fallback_manifest_count'], 1)
        self.assertEqual(
            database_calls[0]['prepared_downstream_membership_manifests'],
            fallback_manifests,
        )

    def test_apply_hashes_canonical_alphanumeric_code_exclusions(self):
        target = date(2026, 9, 9)
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []

        def load_ohlcv(_date: str, market: str):
            rows = {codes[market]: _ohlcv_row()}
            if market == 'KOSPI':
                rows['00104K'] = _ohlcv_row()
            return _FakeFrame(rows)

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: target.isoformat(),
            market_ohlcv_loader=load_ohlcv,
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _fundamental_row()}
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'completed', 'applied': True}
            ),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
            rebuild_date_loader=lambda **_kwargs: [target],
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(
            market_date=target,
            apply=True,
            expected_provider_membership_count=3,
            expected_membership_sha256=market_membership_sha256(
                list(codes.values())
            ),
            expected_provider_payload_sha256=_provider_payload_sha256(
                market_date=target,
                bars=[
                    MissingDailyBar(
                        symbol=code,
                        market=market,
                        **{
                            key: value
                            for key, value in {
                                'open_price': 100,
                                'high_price': 110,
                                'low_price': 90,
                                'close_price': 105,
                                'volume': 1_000,
                                'turnover': 105_000,
                                'per': Decimal('10.5'),
                                'pbr': Decimal('1.2'),
                                'bps': Decimal('50000'),
                                'eps': Decimal('5000'),
                                'dividend_yield': Decimal('2.3'),
                                'dps': Decimal('1200'),
                            }.items()
                        },
                    )
                    for market, code in codes.items()
                ],
                excluded_ohlcv_codes=[
                    {'market': 'KOSPI', 'code': '00104K'}
                ],
            ),
            expected_missing_bar_count=3,
            expected_missing_membership_sha256='0' * 64,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['provider_excluded_ohlcv_code_count'], 1)
        self.assertEqual(
            len(result['provider_excluded_ohlcv_code_sha256']),
            64,
        )
        self.assertEqual(
            database_calls[0]['excluded_ohlcv_codes'],
            [{'market': 'KOSPI', 'code': '00104K'}],
        )

    def test_malformed_provider_code_still_blocks_repair(self):
        target = date(2026, 9, 9)
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []

        def load_ohlcv(_date: str, market: str):
            rows = {codes[market]: _ohlcv_row()}
            if market == 'KOSPI':
                rows['ABCDEF'] = _ohlcv_row()
            return _FakeFrame(rows)

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: target.isoformat(),
            market_ohlcv_loader=load_ohlcv,
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _fundamental_row()}
            ),
            database_repairer=lambda **kwargs: database_calls.append(kwargs),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(market_date=target)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_excluded_provider_code')
        self.assertIn('ABCDEF', result['error_message'])
        self.assertEqual(database_calls, [])

    def test_apply_requires_reviewed_membership_before_provider_fetch(self):
        provider_calls: list[tuple[str, str]] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: provider_calls.append(
                ('resolver', '')
            ),
            market_ohlcv_loader=lambda *args: provider_calls.append(args),
            market_fundamental_loader=lambda *args: provider_calls.append(args),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'reviewed_apply_evidence_required',
        )
        self.assertEqual(provider_calls, [])

    def test_apply_blocks_when_core_api_port_is_active_before_any_work(self):
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: self.fail(
                'trade-date resolver must not run'
            ),
            market_ohlcv_loader=lambda *_args: self.fail(
                'OHLCV provider must not run'
            ),
            market_fundamental_loader=lambda *_args: self.fail(
                'fundamental provider must not run'
            ),
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            rebuild_date_loader=lambda **_kwargs: self.fail(
                'rebuild-date loader must not run'
            ),
            core_api_active_checker=lambda: True,
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
            apply=True,
            expected_provider_membership_count=1,
            expected_membership_sha256='0' * 64,
            expected_provider_payload_sha256='1' * 64,
            expected_missing_bar_count=0,
            expected_missing_membership_sha256='2' * 64,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'core_api_port_active')
        self.assertFalse(result['applied'])
        self.assertEqual(result['core_api_host'], '127.0.0.1')
        self.assertEqual(result['core_api_port'], 8000)

    def test_apply_blocks_provider_membership_drift_before_database_write(self):
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _fundamental_row()}
            ),
            database_repairer=lambda **kwargs: database_calls.append(kwargs),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
            rebuild_date_loader=lambda **_kwargs: [date(2026, 9, 9)],
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
            apply=True,
            expected_provider_membership_count=3,
            expected_membership_sha256='0' * 64,
            expected_provider_payload_sha256='0' * 64,
            expected_missing_bar_count=3,
            expected_missing_membership_sha256='0' * 64,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'provider_membership_sha256_mismatch',
        )
        self.assertEqual(database_calls, [])

    def test_apply_blocks_provider_payload_drift_before_database_write(self):
        target = date(2026, 9, 9)
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: target.isoformat(),
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _fundamental_row()}
            ),
            database_repairer=lambda **kwargs: database_calls.append(kwargs),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
            rebuild_date_loader=lambda **_kwargs: [target],
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(
            market_date=target,
            apply=True,
            expected_provider_membership_count=3,
            expected_membership_sha256=market_membership_sha256(
                list(codes.values())
            ),
            expected_provider_payload_sha256='0' * 64,
            expected_missing_bar_count=3,
            expected_missing_membership_sha256=market_membership_sha256(
                list(codes.values())
            ),
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'provider_payload_sha256_mismatch')
        self.assertEqual(database_calls, [])

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
            'KONEX': _FakeFrame({'900001': _ohlcv_row()}),
        }
        fundamental_frames = {
            'KOSPI': _FakeFrame(
                {
                    '005930': _fundamental_row(),
                    '123456': _fundamental_row(),
                    '1234A6': _fundamental_row(),
                }
            ),
            'KOSDAQ': _FakeFrame({'035720': _fundamental_row()}),
            'KONEX': _FakeFrame({'900001': _fundamental_row()}),
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
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {
                '005930': 'ST',
                '000660': 'RT',
                '035720': 'ST',
                '900001': 'ST',
            },
            lock_factory=_available_lock,
            minimum_market_membership={
                'KOSPI': 2,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(market_date='20260909')

        self.assertEqual(result['provider_excluded_ohlcv_code_count'], 1)
        self.assertEqual(result['provider_excluded_fundamental_code_count'], 1)
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
            'KONEX': _FakeFrame({'900001': _ohlcv_row()}),
        }
        fundamental_frames = {
            'KOSPI': _FakeFrame({'123456': _fundamental_row()}),
            'KOSDAQ': _FakeFrame({'035720': _fundamental_row()}),
            'KONEX': _FakeFrame({'900001': _fundamental_row()}),
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
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {
                '005930': 'ST',
                '035720': 'ST',
                '900001': 'ST',
            },
            lock_factory=_available_lock,
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'required_fundamental_rows_missing')
        self.assertEqual(
            result['required_missing_fundamental_symbols'],
            ['005930'],
        )

    def test_exact_symbol_absence_allows_price_repair_with_explicit_gap(self):
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                (
                    {'123456': _fundamental_row()}
                    if market == 'KOSPI'
                    else {codes[market]: _fundamental_row()}
                )
            ),
            symbol_fundamental_loader=lambda _date, _symbol: _FakeFrame(
                {},
                columns=[],
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {symbol: 'ST' for symbol in codes.values()},
            lock_factory=_available_lock,
            minimum_market_membership={market: 1 for market in codes},
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(
            result['provider_symbol_fundamental_unavailable_symbols'],
            ['005930'],
        )
        self.assertEqual(result['required_missing_fundamental_count'], 0)
        bars_by_symbol = {
            bar.symbol: bar for bar in database_calls[0]['bars']
        }
        self.assertIsNone(bars_by_symbol['005930'].per)

    def test_exact_symbol_fundamental_fallback_error_blocks_repair(self):
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }

        def fail_symbol_fundamental(_date: str, _symbol: str):
            raise TimeoutError('provider timeout')

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: _FakeFrame(
                (
                    {'123456': _fundamental_row()}
                    if market == 'KOSPI'
                    else {codes[market]: _fundamental_row()}
                )
            ),
            symbol_fundamental_loader=fail_symbol_fundamental,
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {symbol: 'ST' for symbol in codes.values()},
            lock_factory=_available_lock,
            minimum_market_membership={market: 1 for market in codes},
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'symbol_fundamental_fallback_failed')

    def test_arbitrary_empty_konex_fundamentals_block_price_repair(self):
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []
        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=lambda _date, market: (
                _FakeFrame({}, columns=[])
                if market == 'KONEX'
                else _FakeFrame({codes[market]: _fundamental_row()})
            ),
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {
                '005930': 'ST',
                '035720': 'ST',
                '900001': 'ST',
            },
            lock_factory=_available_lock,
            minimum_market_membership={
                'KOSPI': 1,
                'KOSDAQ': 1,
                'KONEX': 1,
            },
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_fundamental_payload')
        self.assertEqual(result['failed_market'], 'KONEX')
        self.assertEqual(database_calls, [])

    def test_konex_provider_empty_table_keyerror_is_narrowly_accepted(self):
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }
        database_calls: list[dict] = []

        def load_fundamentals(_date: str, market: str):
            if market == 'KONEX':
                raise KeyError(
                    "None of [Index(['BPS', 'PER', 'PBR', 'EPS', 'DIV', "
                    "'DPS'], dtype='object')] are in the [columns]"
                )
            return _FakeFrame({codes[market]: _fundamental_row()})

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=load_fundamentals,
            database_repairer=lambda **kwargs: (
                database_calls.append(kwargs)
                or {'status': 'ready', 'applied': False}
            ),
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {symbol: 'ST' for symbol in codes.values()},
            lock_factory=_available_lock,
            minimum_market_membership={market: 1 for market in codes},
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(
            result['provider_fundamental_unavailable_markets'],
            ['KONEX'],
        )
        self.assertEqual(len(database_calls), 1)

    def test_unrelated_konex_keyerror_still_blocks_repair(self):
        codes = {
            'KOSPI': '005930',
            'KOSDAQ': '035720',
            'KONEX': '900001',
        }

        def load_fundamentals(_date: str, market: str):
            if market == 'KONEX':
                raise KeyError('unexpected provider schema regression')
            return _FakeFrame({codes[market]: _fundamental_row()})

        service = MissingDailyBarRepairService(
            trade_date_resolver=lambda **_kwargs: '2026-09-09',
            market_ohlcv_loader=lambda _date, market: _FakeFrame(
                {codes[market]: _ohlcv_row()}
            ),
            market_fundamental_loader=load_fundamentals,
            database_repairer=lambda **_kwargs: self.fail(
                'database repairer must not run'
            ),
            core_api_active_checker=_core_api_inactive,
            lock_factory=_available_lock,
            minimum_market_membership={market: 1 for market in codes},
        )

        result = service.repair_for_market_date(market_date='2026-09-09')

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_fundamental_payload')
        self.assertEqual(result['failed_market'], 'KONEX')

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
            core_api_active_checker=_core_api_inactive,
            security_group_loader=lambda: {},
            lock_factory=_available_lock,
            rebuild_date_loader=lambda **_kwargs: [date(2026, 9, 9)],
        )

        result = service.repair_for_market_date(
            market_date='2026-09-09',
            apply=True,
            expected_provider_membership_count=1900,
            expected_membership_sha256='0' * 64,
            expected_provider_payload_sha256='0' * 64,
            expected_missing_bar_count=0,
            expected_missing_membership_sha256=market_membership_sha256([]),
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
            core_api_active_checker=_core_api_inactive,
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
            core_api_active_checker=_core_api_inactive,
            lock_factory=unavailable_lock,
        )
        result = service.repair_for_market_date(market_date='2026-09-09')
        self.assertEqual(result['reason'], 'advisory_lock_unavailable')
        self.assertEqual(provider_calls, [])


class MissingDailyBarRepairRepositoryPostgresTests(unittest.TestCase):
    def test_exact_historical_identity_creates_inactive_symbol_and_bar(self):
        market_date = date(2099, 12, 25)
        historical_code = f'{760000 + uuid4().int % 10_000:06d}'
        anchor_code = f'{770000 + uuid4().int % 10_000:06d}'
        bar = MissingDailyBar(
            symbol=historical_code,
            market='KOSPI',
            open_price=100,
            high_price=110,
            low_price=90,
            close_price=105,
            volume=1_000,
            turnover=105_000,
        )
        identities = {
            historical_code: {
                'symbol': historical_code,
                'market': 'KOSPI',
                'name_ko': 'Historical exact member',
                'market_date': market_date.isoformat(),
            }
        }
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            anchor = Symbol(
                symbol=anchor_code,
                name_ko='Historical identity anchor',
                market='OTHER',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add(anchor)
            session.flush()
            session.add(
                DailyPriceBar(
                    symbol_id=anchor.id,
                    trade_date=market_date,
                    open_price=1,
                    high_price=1,
                    low_price=1,
                    close_price=1,
                    volume=0,
                    turnover=0,
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
                    dry_run = repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=[bar],
                        historical_symbol_identities=identities,
                    )
                    result = repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=[bar],
                        apply=True,
                        historical_symbol_identities=identities,
                        **_reviewed_apply_kwargs(
                            market_date=market_date,
                            bars=[bar],
                            missing_symbols=[historical_code],
                            historical_symbol_identities=identities,
                        ),
                    )

                self.assertEqual(dry_run['status'], 'ready')
                self.assertEqual(
                    dry_run['would_create_historical_symbols'],
                    [historical_code],
                )
                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['historical_symbol_create_count'], 1)
                created = session.scalar(
                    select(Symbol).where(Symbol.symbol == historical_code)
                )
                self.assertIsNotNone(created)
                self.assertFalse(created.is_active)
                self.assertEqual(created.market, 'KOSPI')
                self.assertEqual(created.name_ko, 'Historical exact member')
                self.assertEqual(created.source, 'pykrx_historical_repair')
                self.assertIsNotNone(
                    session.scalar(
                        select(DailyPriceBar).where(
                            DailyPriceBar.symbol_id == created.id,
                            DailyPriceBar.trade_date == market_date,
                        )
                    )
                )
            finally:
                session.close()
                transaction.rollback()

    def test_dry_run_emits_full_provider_payload_and_missing_scope_hashes(self):
        market_date = date(2099, 12, 24)
        existing_code = f'{850000 + uuid4().int % 20_000:06d}'
        missing_code = f'{870000 + uuid4().int % 20_000:06d}'
        bars = [
            MissingDailyBar(
                symbol=existing_code,
                market='KOSPI',
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1_000,
                turnover=105_000,
            ),
            MissingDailyBar(
                symbol=missing_code,
                market='KONEX',
                open_price=200,
                high_price=210,
                low_price=190,
                close_price=205,
                volume=2_000,
                turnover=410_000,
            ),
        ]
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            existing = Symbol(
                symbol=existing_code,
                name_ko='Dry run existing',
                market='OTHER',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            missing = Symbol(
                symbol=missing_code,
                name_ko='Dry run missing',
                market='KONEX',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([existing, missing])
            session.flush()
            session.add(
                DailyPriceBar(
                    symbol_id=existing.id,
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
                        bars=bars,
                        apply=False,
                    )

                self.assertEqual(result['missing_bar_count'], 1)
                self.assertEqual(result['missing_bar_symbols'], [missing_code])
                self.assertEqual(
                    result['missing_membership_sha256'],
                    market_membership_sha256([missing_code]),
                )
                self.assertEqual(
                    result['provider_payload_sha256'],
                    _provider_payload_sha256(
                        market_date=market_date,
                        bars=bars,
                    ),
                )
            finally:
                session.close()
                transaction.rollback()

    def test_missing_scope_mismatch_rolls_back_without_writes(self):
        market_date = date(2099, 12, 23)
        missing_code = f'{880000 + uuid4().int % 10_000:06d}'
        anchor_code = f'{890000 + uuid4().int % 10_000:06d}'
        bar = MissingDailyBar(
            symbol=missing_code,
            market='KOSPI',
            open_price=100,
            high_price=110,
            low_price=90,
            close_price=105,
            volume=1_000,
            turnover=105_000,
        )
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            missing = Symbol(
                symbol=missing_code,
                name_ko='Missing mismatch',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            anchor = Symbol(
                symbol=anchor_code,
                name_ko='Date anchor',
                market='OTHER',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([missing, anchor])
            session.flush()
            session.add(
                DailyPriceBar(
                    symbol_id=anchor.id,
                    trade_date=market_date,
                    open_price=1,
                    high_price=1,
                    low_price=1,
                    close_price=1,
                    volume=0,
                    turnover=0,
                    source='test',
                )
            )
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            reviewed = _reviewed_apply_kwargs(
                market_date=market_date,
                bars=[bar],
                missing_symbols=[],
            )
            try:
                with (
                    patch(
                        'app.eod.missing_bar_repair.session_scope',
                        rollback_only_session_scope,
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        'missing bar count changed',
                    ),
                ):
                    repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=[bar],
                        apply=True,
                        **reviewed,
                    )

                self.assertIsNone(
                    session.scalar(
                        select(DailyPriceBar).where(
                            DailyPriceBar.symbol_id == missing.id,
                            DailyPriceBar.trade_date == market_date,
                        )
                    )
                )
                matching_batches = [
                    batch
                    for batch in session.scalars(
                        select(DailyPriceBarRepairBatch)
                    ).all()
                    if batch.affected_trade_dates
                    == [market_date.isoformat()]
                ]
                self.assertEqual(matching_batches, [])
            finally:
                session.close()
                transaction.rollback()

    def test_active_no_bar_downstream_run_blocks_apply_before_any_write(self):
        market_date = date(2099, 12, 22)
        active_downstream_date = date(2099, 12, 23)
        missing_code = f'{810000 + uuid4().int % 10_000:06d}'
        anchor_code = f'{820000 + uuid4().int % 10_000:06d}'
        bar = MissingDailyBar(
            symbol=missing_code,
            market='KOSPI',
            open_price=100,
            high_price=110,
            low_price=90,
            close_price=105,
            volume=1_000,
            turnover=105_000,
        )
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            missing = Symbol(
                symbol=missing_code,
                name_ko='Active gate missing',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            anchor = Symbol(
                symbol=anchor_code,
                name_ko='Active gate anchor',
                market='OTHER',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([missing, anchor])
            session.flush()
            session.add_all(
                [
                    DailyPriceBar(
                        symbol_id=anchor.id,
                        trade_date=market_date,
                        open_price=1,
                        high_price=1,
                        low_price=1,
                        close_price=1,
                        volume=0,
                        turnover=0,
                        source='test',
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=active_downstream_date,
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='running',
                        symbols_total=1,
                        symbols_succeeded=0,
                        symbols_failed=0,
                    ),
                    DailyPipelineRun(
                        market_date=active_downstream_date,
                        trigger='auto_close_scheduler',
                        status='queued',
                        current_stage='price_collection',
                    ),
                ]
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
                        bars=[bar],
                        apply=True,
                        **_reviewed_apply_kwargs(
                            market_date=market_date,
                            bars=[bar],
                            missing_symbols=[missing_code],
                        ),
                    )

                self.assertEqual(result['status'], 'blocked')
                self.assertEqual(
                    result['reason'],
                    'official_daily_or_pykrx_task_active',
                )
                self.assertEqual(len(result['active_pykrx_eod_run_ids']), 1)
                self.assertEqual(
                    len(result['active_daily_pipeline_run_ids']),
                    1,
                )
                self.assertIsNone(
                    session.scalar(
                        select(DailyPriceBar).where(
                            DailyPriceBar.trade_date == active_downstream_date
                        )
                    )
                )
                self.assertIsNone(
                    session.scalar(
                        select(DailyPriceBar).where(
                            DailyPriceBar.symbol_id == missing.id,
                            DailyPriceBar.trade_date == market_date,
                        )
                    )
                )
            finally:
                session.close()
                transaction.rollback()

    def test_downstream_other_membership_is_frozen_from_exact_eod_proof(self):
        target_date = date(2099, 12, 20)
        downstream_date = date(2099, 12, 21)
        existing_code = f'{710000 + uuid4().int % 10_000:06d}'
        missing_code = f'{720000 + uuid4().int % 10_000:06d}'
        downstream_code = f'{730000 + uuid4().int % 10_000:06d}'
        bars = [
            MissingDailyBar(
                symbol=existing_code,
                market='KOSPI',
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1_000,
                turnover=105_000,
            ),
            MissingDailyBar(
                symbol=missing_code,
                market='KOSDAQ',
                open_price=200,
                high_price=210,
                low_price=190,
                close_price=205,
                volume=2_000,
                turnover=410_000,
            ),
        ]

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            existing = Symbol(
                symbol=existing_code,
                name_ko='Target existing',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            missing = Symbol(
                symbol=missing_code,
                name_ko='Target missing',
                market='KOSDAQ',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            downstream = Symbol(
                symbol=downstream_code,
                name_ko='Downstream reclassified',
                market='OTHER',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([existing, missing, downstream])
            session.flush()
            session.add_all(
                [
                    DailyPriceBar(
                        symbol_id=existing.id,
                        trade_date=target_date,
                        open_price=100,
                        high_price=110,
                        low_price=90,
                        close_price=105,
                        volume=1_000,
                        turnover=105_000,
                        source='test',
                    ),
                    DailyPriceBar(
                        symbol_id=downstream.id,
                        trade_date=downstream_date,
                        open_price=300,
                        high_price=310,
                        low_price=290,
                        close_price=305,
                        volume=3_000,
                        turnover=915_000,
                        source='test',
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=downstream_date,
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed',
                        symbols_total=1,
                        symbols_succeeded=1,
                        symbols_failed=0,
                        run_metadata=_strict_universe_snapshot_metadata(
                            market_date=downstream_date,
                            symbols_by_market={
                                'KOSPI': [downstream_code],
                                'KOSDAQ': [],
                            },
                        ),
                    ),
                ]
            )
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                with (
                    patch(
                        'app.eod.missing_bar_repair.session_scope',
                        rollback_only_session_scope,
                    ),
                    patch.dict(
                        'app.eod.missing_bar_repair.MINIMUM_MARKET_MEMBERSHIP',
                        {'KOSPI': 1, 'KOSDAQ': 0, 'KONEX': 0},
                        clear=True,
                    ),
                ):
                    result = repair_missing_daily_price_bars(
                        market_date=target_date,
                        bars=bars,
                        apply=True,
                        **_reviewed_apply_kwargs(
                            market_date=target_date,
                            bars=bars,
                            missing_symbols=[missing_code],
                            required_rebuild_trade_dates=[
                                target_date,
                                downstream_date,
                            ],
                        ),
                    )

                self.assertEqual(result['status'], 'completed')
                batch = session.get(
                    DailyPriceBarRepairBatch,
                    result['repair_batch_id'],
                )
                evidence = batch.verification_metadata['repair_evidence']
                frozen = evidence['price_dependent_membership_by_date']
                self.assertEqual(
                    frozen[downstream_date.isoformat()]['symbols'],
                    [downstream_code],
                )
                self.assertEqual(
                    frozen[downstream_date.isoformat()]['basis'],
                    'validated_eod_run_market_membership',
                )
                durable = _missing_bar_insert_repair_evidence(
                    session,
                    batch=batch,
                )
                self.assertTrue(durable['ready'])
                self.assertEqual(
                    durable['price_dependent_membership_by_date'][
                        downstream_date.isoformat()
                    ],
                    [downstream_code],
                )
                downstream_bar = session.scalar(
                    select(DailyPriceBar).where(
                        DailyPriceBar.symbol_id == downstream.id,
                        DailyPriceBar.trade_date == downstream_date,
                    )
                )
                downstream_bar.open_price = 301
                session.flush()
                changed = _missing_bar_insert_repair_evidence(
                    session,
                    batch=batch,
                )
                self.assertFalse(changed['ready'])
                self.assertIn(
                    'missing_bar_insert_frozen_membership_input_sha256_mismatch',
                    changed['issues'],
                )
            finally:
                session.close()
                transaction.rollback()

    def test_missing_downstream_membership_proof_fails_before_insert(self):
        target_date = date(2099, 12, 18)
        downstream_date = date(2099, 12, 19)
        existing_code = f'{740000 + uuid4().int % 10_000:06d}'
        missing_code = f'{750000 + uuid4().int % 10_000:06d}'
        downstream_code = f'{760000 + uuid4().int % 10_000:06d}'
        bars = [
            MissingDailyBar(
                symbol=existing_code,
                market='KOSPI',
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1_000,
                turnover=105_000,
            ),
            MissingDailyBar(
                symbol=missing_code,
                market='KOSDAQ',
                open_price=200,
                high_price=210,
                low_price=190,
                close_price=205,
                volume=2_000,
                turnover=410_000,
            ),
        ]
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbols = [
                Symbol(
                    symbol=code,
                    name_ko=code,
                    market=market,
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                )
                for code, market in (
                    (existing_code, 'KOSPI'),
                    (missing_code, 'KOSDAQ'),
                    (downstream_code, 'OTHER'),
                )
            ]
            session.add_all(symbols)
            session.flush()
            session.add_all(
                [
                    DailyPriceBar(
                        symbol_id=symbols[0].id,
                        trade_date=target_date,
                        open_price=100,
                        high_price=110,
                        low_price=90,
                        close_price=105,
                        volume=1_000,
                        turnover=105_000,
                        source='test',
                    ),
                    DailyPriceBar(
                        symbol_id=symbols[2].id,
                        trade_date=downstream_date,
                        open_price=300,
                        high_price=310,
                        low_price=290,
                        close_price=305,
                        volume=3_000,
                        turnover=915_000,
                        source='test',
                    ),
                ]
            )
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                with (
                    patch(
                        'app.eod.missing_bar_repair.session_scope',
                        rollback_only_session_scope,
                    ),
                    self.assertRaisesRegex(
                        RuntimeError,
                        'no strict same-date PyKRX membership proof',
                    ),
                ):
                    repair_missing_daily_price_bars(
                        market_date=target_date,
                        bars=bars,
                        apply=True,
                        **_reviewed_apply_kwargs(
                            market_date=target_date,
                            bars=bars,
                            missing_symbols=[missing_code],
                            required_rebuild_trade_dates=[
                                target_date,
                                downstream_date,
                            ],
                        ),
                    )
                self.assertIsNone(
                    session.scalar(
                        select(DailyPriceBar).where(
                            DailyPriceBar.symbol_id == symbols[1].id,
                            DailyPriceBar.trade_date == target_date,
                        )
                    )
                )
                matching_batches = [
                    batch
                    for batch in session.scalars(
                        select(DailyPriceBarRepairBatch)
                    ).all()
                    if batch.affected_trade_dates
                    == [target_date.isoformat()]
                ]
                self.assertEqual(matching_batches, [])
            finally:
                session.close()
                transaction.rollback()

    def test_historical_bar_reclassified_other_is_still_in_exact_membership(self):
        market_date = date(2099, 12, 27)
        symbol_code = f'{900000 + uuid4().int % 49_999:06d}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='Reclassified historical row',
                market='OTHER',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            session.add(
                DailyPriceBar(
                    symbol_id=symbol.id,
                    trade_date=market_date,
                    open_price=100,
                    high_price=110,
                    low_price=90,
                    close_price=105,
                    volume=1_000,
                    turnover=105_000,
                    source='historical',
                )
            )
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            bar = MissingDailyBar(
                symbol=symbol_code,
                market='KOSPI',
                open_price=999,
                high_price=999,
                low_price=999,
                close_price=999,
                volume=999,
                turnover=998_001,
            )
            try:
                with patch(
                    'app.eod.missing_bar_repair.session_scope',
                    rollback_only_session_scope,
                ):
                    result = repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=[bar],
                        apply=True,
                        **_reviewed_apply_kwargs(
                            market_date=market_date,
                            bars=[bar],
                            missing_symbols=[],
                        ),
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['stored_before_count'], 1)
                self.assertEqual(result['missing_bar_count'], 0)
                self.assertEqual(result['inserted_count'], 0)
                self.assertEqual(
                    result['historical_market_transition_count'],
                    1,
                )
                self.assertFalse(result['rebuild_debt_recorded'])
                persisted = session.execute(
                    select(DailyPriceBar).where(
                        DailyPriceBar.symbol_id == symbol.id,
                        DailyPriceBar.trade_date == market_date,
                    )
                ).scalar_one()
                self.assertEqual(persisted.source, 'historical')
                self.assertEqual(persisted.open_price, 100)
            finally:
                session.close()
                transaction.rollback()

    def test_membership_gate_mismatch_writes_no_bar_or_rebuild_batch(self):
        market_date = date(2099, 12, 26)
        symbol_code = f'{900000 + uuid4().int % 49_999:06d}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='Membership mismatch',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add(symbol)
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            bar = MissingDailyBar(
                symbol=symbol_code,
                market='KOSPI',
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1_000,
                turnover=105_000,
            )
            try:
                with (
                    patch(
                        'app.eod.missing_bar_repair.session_scope',
                        rollback_only_session_scope,
                    ),
                    self.assertRaisesRegex(
                        ValueError,
                        'SHA-256 changed',
                    ),
                ):
                    repair_missing_daily_price_bars(
                        market_date=market_date,
                        bars=[bar],
                        apply=True,
                        expected_provider_membership_count=1,
                        expected_membership_sha256='0' * 64,
                        expected_provider_payload_sha256=(
                            _provider_payload_sha256(
                                market_date=market_date,
                                bars=[bar],
                            )
                        ),
                        expected_missing_bar_count=1,
                        expected_missing_membership_sha256=(
                            market_membership_sha256([symbol_code])
                        ),
                        required_rebuild_trade_dates=[market_date],
                    )

                self.assertIsNone(
                    session.execute(
                        select(DailyPriceBar).where(
                            DailyPriceBar.symbol_id == symbol.id,
                            DailyPriceBar.trade_date == market_date,
                        )
                    ).scalar_one_or_none()
                )
                matching_batches = [
                    batch
                    for batch in session.scalars(
                        select(DailyPriceBarRepairBatch)
                    ).all()
                    if batch.affected_trade_dates
                    == [market_date.isoformat()]
                ]
                self.assertEqual(matching_batches, [])
            finally:
                session.close()
                transaction.rollback()

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
                        **_reviewed_apply_kwargs(
                            market_date=market_date,
                            bars=bars,
                            missing_symbols=[second_code],
                        ),
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['missing_bar_count'], 1)
                self.assertEqual(result['inserted_count'], 1)
                self.assertEqual(
                    result['historical_market_transition_count'],
                    1,
                )
                self.assertEqual(
                    result['historical_market_transition_preview'],
                    [
                        {
                            'symbol': second_code,
                            'current_market': 'KOSPI',
                            'historical_market': 'KOSDAQ',
                        }
                    ],
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
                self.assertTrue(result['rebuild_debt_recorded'])
                repair_batch = session.get(
                    DailyPriceBarRepairBatch,
                    result['repair_batch_id'],
                )
                self.assertIsNotNone(repair_batch)
                self.assertEqual(repair_batch.status, 'pending')
                self.assertEqual(repair_batch.affected_symbols, [second_code])
                self.assertEqual(
                    repair_batch.affected_trade_dates,
                    [market_date.isoformat()],
                )
                self.assertIn(
                    market_date.isoformat(),
                    repair_batch.required_rebuild_trade_dates,
                )
                repair_evidence = repair_batch.verification_metadata[
                    'repair_evidence'
                ]
                self.assertEqual(
                    repair_evidence['repair_kind'],
                    'missing_bar_insert',
                )
                self.assertEqual(
                    repair_evidence['inserted_symbols'],
                    [second_code],
                )
                self.assertEqual(
                    repair_evidence['expected_provider_membership_count'],
                    2,
                )
                self.assertEqual(
                    repair_evidence['provider_membership_manifest']['markets'][
                        'KOSDAQ'
                    ]['symbols'],
                    [second_code],
                )
                self.assertEqual(
                    repair_evidence['price_dependent_membership_symbols'],
                    sorted([first_code, second_code]),
                )
                second.market = 'OTHER'
                session.flush()
                durable_evidence = _missing_bar_insert_repair_evidence(
                    session,
                    batch=repair_batch,
                )
                self.assertTrue(durable_evidence['ready'])
                self.assertEqual(durable_evidence['issues'], [])
            finally:
                session.close()
                transaction.rollback()

    def test_stored_current_market_row_outside_provider_is_diagnostic_only(self):
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
                        **_reviewed_apply_kwargs(
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
                            missing_symbols=[expected_code],
                        ),
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(
                    result['stored_outside_provider_membership_symbols'],
                    [extra_code],
                )
                self.assertIsNotNone(
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

    def test_cli_forwards_reviewed_membership_gate_for_apply(self):
        membership_sha256 = 'a' * 64
        provider_payload_sha256 = 'b' * 64
        missing_membership_sha256 = 'c' * 64
        downstream_bundle_sha256 = 'd' * 64
        with (
            patch(
                'app.eod.missing_bar_repair.missing_daily_bar_repair_service.'
                'repair_for_market_date',
                return_value={'status': 'completed', 'applied': True},
            ) as repair,
            patch('sys.stdout.write'),
        ):
            exit_code = main(
                [
                    '--market-date',
                    '2026-09-09',
                    '--apply',
                    '--expected-provider-membership-count',
                    '2675',
                    '--expected-membership-sha256',
                    membership_sha256,
                    '--expected-provider-payload-sha256',
                    provider_payload_sha256,
                    '--expected-missing-bar-count',
                    '9728',
                    '--expected-missing-membership-sha256',
                    missing_membership_sha256,
                    '--expected-downstream-fallback-manifest-count',
                    '14',
                    '--expected-downstream-fallback-bundle-sha256',
                    downstream_bundle_sha256,
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            repair.call_args.kwargs,
            {
                'market_date': '2026-09-09',
                'apply': True,
                'expected_provider_membership_count': 2675,
                'expected_membership_sha256': membership_sha256,
                'expected_provider_payload_sha256': provider_payload_sha256,
                'expected_missing_bar_count': 9728,
                'expected_missing_membership_sha256': (
                    missing_membership_sha256
                ),
                'expected_downstream_fallback_manifest_count': 14,
                'expected_downstream_fallback_bundle_sha256': (
                    downstream_bundle_sha256
                ),
            },
        )


if __name__ == '__main__':
    unittest.main()
