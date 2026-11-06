from __future__ import annotations

from contextlib import contextmanager
import unittest
from unittest.mock import patch

from app.eod import stored_signal_feature_backfill as backfill
from app.signals.daily_signal_features import (
    STORED_FALLBACK_BACKFILL_MODE,
    DailySignalFeatureService,
)


class _NoKIS:
    async def fetch_volume_power_rank(self, **_kwargs):  # pragma: no cover
        raise AssertionError('backfill must never call KIS')


def _bar(trade_date: str) -> dict:
    return {
        'trade_date': trade_date,
        'open_price': 100,
        'high_price': 110,
        'low_price': 90,
        'close_price': 105,
        'volume': 1_000,
        'turnover': 105_000,
        'source': 'pykrx',
        'captured_at': f'{trade_date}T15:30:00+09:00',
    }


def _support_maps(symbols_with_bars: tuple[str, ...]) -> dict:
    return {
        'symbol_map': {
            symbol: {'symbol': symbol, 'name_ko': symbol, 'market': 'KOSPI'}
            for symbol in ('000660', '005930', '035420')
        },
        # A decision-usable same-day quote is present in the cache; it must
        # still be ignored because today's cache is not historical evidence.
        'quote_map': {
            '005930': {
                'symbol': '005930',
                'price': 999,
                'trade_date': '2026-11-02',
                'available': True,
            }
        },
        'price_bars_by_symbol': {
            symbol: [_bar('2026-11-02')] for symbol in symbols_with_bars
        },
        'value_map': {},
        'unified_value_map': {},
        'quality_map': {},
        'momentum_map': {},
        'advanced_value_map': {},
        'low_risk_map': {},
        'beta_map': {},
        'composite_map': {},
    }


class BuildStoredFallbackRowsTests(unittest.TestCase):
    def _build(self, symbols, *, bars=('000660', '005930')):
        service = DailySignalFeatureService(client=_NoKIS())
        with (
            patch(
                'app.signals.daily_signal_features.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['000660', '005930'],
            ),
            patch(
                'app.signals.daily_signal_features.'
                '_candidate_universe_metadata',
                return_value={
                    'availability': 'available',
                    'symbols': ['000660'],
                    'has_piotroski_rows': True,
                    'has_market_risk_rows': True,
                },
            ),
            patch(
                'app.signals.daily_signal_features._load_support_maps',
                return_value=_support_maps(bars),
            ),
        ):
            return service.build_stored_owned_fallback_rows(
                market_date='2026-11-02',
                symbols=symbols,
            )

    def test_rows_are_fallback_only_tagged_and_quote_free(self) -> None:
        rows, summary = self._build(['005930', '000660'])

        self.assertEqual({row['symbol'] for row in rows}, {'000660', '005930'})
        self.assertEqual(summary['built_row_count'], 2)
        for row in rows:
            payload = row['feature_payload']
            self.assertEqual(row['signal_count'], 0)
            self.assertEqual(payload['sources'], {})
            self.assertEqual(
                payload['row_provenance']['row_kind'], 'fallback_only'
            )
            self.assertEqual(
                payload['collection']['repair_mode'],
                STORED_FALLBACK_BACKFILL_MODE,
            )
            self.assertNotIn('quote_snapshot', payload['fallback_features'])
        by_symbol = {row['symbol']: row for row in rows}
        self.assertTrue(
            by_symbol['000660']['feature_payload']['row_provenance'][
                'candidate_universe_member'
            ]
        )

    def test_symbol_without_any_stored_input_is_reported_not_written(
        self,
    ) -> None:
        rows, summary = self._build(['005930', '000660'], bars=('005930',))

        self.assertEqual([row['symbol'] for row in rows], ['005930'])
        self.assertEqual(summary['without_stored_inputs'], ['000660'])

    def test_symbol_without_daily_bar_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self._build(['005930', '035420'])

    def test_invalid_symbol_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self._build(['not-a-symbol'])


class _FakeService:
    def __init__(self) -> None:
        self.built_for: list[str] = []

    def build_stored_owned_fallback_rows(self, *, market_date, symbols):
        self.built_for = list(symbols)
        rows = [{'symbol': symbol, 'market_date': market_date} for symbol in symbols]
        return rows, {'without_stored_inputs': []}


class PlanAndApplyTests(unittest.TestCase):
    def test_plan_only_builds_symbols_missing_a_row(self) -> None:
        service = _FakeService()
        rows, summary = backfill.plan_backfill(
            '2026-11-04',
            service=service,
            bar_symbols_loader=lambda _d: ['A00001', 'A00002', 'A00003'],
            stored_symbols_loader=lambda _d, _s: {'A00002'},
        )

        self.assertEqual(service.built_for, ['A00001', 'A00003'])
        self.assertEqual(len(rows), 2)
        self.assertEqual(summary['existing_row_count'], 1)
        self.assertEqual(summary['missing_symbol_count'], 2)

    def test_plan_with_nothing_missing_builds_nothing(self) -> None:
        service = _FakeService()
        rows, summary = backfill.plan_backfill(
            '2026-11-04',
            service=service,
            bar_symbols_loader=lambda _d: ['A00001'],
            stored_symbols_loader=lambda _d, _s: {'A00001'},
        )

        self.assertEqual(rows, [])
        self.assertEqual(service.built_for, [])
        self.assertEqual(summary['missing_symbol_count'], 0)

    def test_apply_writes_inside_the_fence_without_pruning(self) -> None:
        events: list[str] = []

        @contextmanager
        def guard(*, market_date):
            events.append(f'enter:{market_date}')
            yield
            events.append('exit')

        def persister(rows, *, delete_rows_outside_scope):
            events.append(f'persist:{len(rows)}:{delete_rows_outside_scope}')
            return len(rows)

        result = backfill.apply_backfill(
            '2026-11-02',
            guard_factory=guard,
            finality_validator=lambda value, **_kw: value,
            service=_FakeService(),
            persister=persister,
            bar_symbols_loader=lambda _d: ['A00001', 'A00002'],
            stored_symbols_loader=lambda _d, _s: set(),
        )

        self.assertEqual(
            events,
            ['enter:2026-11-02', 'persist:2:False', 'exit'],
        )
        self.assertEqual(result['persisted_row_count'], 2)
        self.assertTrue(result['applied'])

    def test_apply_fails_when_persisted_count_differs(self) -> None:
        @contextmanager
        def guard(*, market_date):
            yield

        with self.assertRaises(RuntimeError):
            backfill.apply_backfill(
                '2026-11-02',
                guard_factory=guard,
                finality_validator=lambda value, **_kw: value,
                service=_FakeService(),
                persister=lambda rows, **_kw: len(rows) - 1,
                bar_symbols_loader=lambda _d: ['A00001', 'A00002'],
                stored_symbols_loader=lambda _d, _s: set(),
            )

    def test_apply_refuses_unfinalized_date_before_taking_the_fence(self) -> None:
        entered: list[bool] = []

        @contextmanager
        def guard(*, market_date):
            entered.append(True)
            yield

        def reject(_value, **_kw):
            raise ValueError('same-day stored-data backfill is blocked')

        with self.assertRaises(ValueError):
            backfill.apply_backfill(
                '2026-11-06',
                guard_factory=guard,
                finality_validator=reject,
                service=_FakeService(),
                persister=lambda rows, **_kw: len(rows),
                bar_symbols_loader=lambda _d: ['A00001'],
                stored_symbols_loader=lambda _d, _s: set(),
            )
        self.assertEqual(entered, [])


class CompareWithStoredTests(unittest.TestCase):
    def _stored(self, quote=None, price=105):
        fallback = {'price_action': {'close': price}}
        if quote:
            fallback['quote_snapshot'] = quote
        return {'symbol': 'A00001', 'feature_payload': {'fallback_features': fallback}}

    def _run(self, stored, rebuilt_price):
        class Service:
            def build_stored_owned_fallback_rows(self, *, market_date, symbols):
                return (
                    [
                        {
                            'symbol': 'A00001',
                            'feature_payload': {
                                'fallback_features': {
                                    'price_action': {'close': rebuilt_price}
                                }
                            },
                        }
                    ],
                    {},
                )

        with patch.object(
            backfill,
            'load_raw_daily_signal_features_for_repair',
            return_value=[stored],
        ):
            return backfill.compare_with_stored(
                '2026-10-30',
                service=Service(),
                bar_symbols_loader=lambda _d: ['A00001'],
            )

    def test_quote_snapshot_is_ignored_when_comparing(self) -> None:
        result = self._run(self._stored(quote={'price': 1}), rebuilt_price=105)

        self.assertEqual(result['identical_fallback_rows'], 1)
        self.assertEqual(result['differing_row_count'], 0)

    def test_value_difference_is_reported_per_group(self) -> None:
        result = self._run(self._stored(price=105), rebuilt_price=106)

        self.assertEqual(result['identical_fallback_rows'], 0)
        self.assertEqual(result['differing_groups'], {'price_action': 1})
        self.assertEqual(result['differing_symbol_sample'], ['A00001'])


if __name__ == '__main__':
    unittest.main()
