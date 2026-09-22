import unittest
from datetime import date
from unittest.mock import patch

from app.signals.value_factor import (
    VALUE_FACTOR_VERSION,
    VALUE_STATUS_INVALID_PER,
    VALUE_STATUS_MISSING_METRIC,
    VALUE_STATUS_SCORED,
    DailyValueFactorService,
    ValueFactorInput,
)


class DailyValueFactorServiceTests(unittest.TestCase):
    def _input(self, *, symbol: str, per, pbr, dividend_yield, market_date: date) -> ValueFactorInput:
        return ValueFactorInput(
            symbol=symbol,
            name_ko=symbol,
            market='KOSPI',
            bars=[
                {
                    'trade_date': market_date.isoformat(),
                    'close_price': 100000,
                    'per': per,
                    'pbr': pbr,
                    'dividend_yield': dividend_yield,
                    'source': 'pykrx',
                    'captured_at': f'{market_date.isoformat()}T15:30:00+09:00',
                }
            ],
        )

    def test_rebuild_scores_cross_sectional_value_metrics(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            self._input(symbol='000001', per=5.0, pbr=0.5, dividend_yield=0.05, market_date=market_date),
            self._input(symbol='000002', per=10.0, pbr=1.0, dividend_yield=0.03, market_date=market_date),
            self._input(symbol='000003', per=20.0, pbr=2.0, dividend_yield=0.01, market_date=market_date),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002', '000003'], [])),
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(result['feature_version'], VALUE_FACTOR_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(upsert.call_count, 3)
        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}

        best = persisted_by_symbol['000001']
        mid = persisted_by_symbol['000002']
        worst = persisted_by_symbol['000003']

        self.assertEqual(best['per_rank'], 1)
        self.assertEqual(best['pbr_rank'], 1)
        self.assertEqual(best['dividend_yield_rank'], 1)
        self.assertEqual(best['value_rank'], 1)
        self.assertEqual(best['value_percentile'], 1.0)
        self.assertEqual(best['value_status'], VALUE_STATUS_SCORED)

        self.assertEqual(mid['per_rank'], 2)
        self.assertEqual(mid['pbr_rank'], 2)
        self.assertEqual(mid['dividend_yield_rank'], 2)
        self.assertEqual(mid['value_rank'], 2)
        self.assertAlmostEqual(mid['value_score'], 0.5, places=6)

        self.assertEqual(worst['per_rank'], 3)
        self.assertEqual(worst['pbr_rank'], 3)
        self.assertEqual(worst['dividend_yield_rank'], 3)
        self.assertEqual(worst['value_rank'], 3)
        self.assertEqual(worst['value_percentile'], 0.0)
        self.assertEqual(result['summary']['scored_count'], 3)

    def test_rebuild_marks_negative_per_as_invalid_without_attractive_score(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            self._input(symbol='000001', per=-3.0, pbr=0.7, dividend_yield=0.02, market_date=market_date),
            self._input(symbol='000002', per=8.0, pbr=1.0, dividend_yield=0.03, market_date=market_date),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001', '000002'], [])),
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}
        invalid = persisted_by_symbol['000001']
        scored = persisted_by_symbol['000002']

        self.assertEqual(invalid['per_status'], 'invalid_non_positive')
        self.assertEqual(invalid['value_status'], VALUE_STATUS_INVALID_PER)
        self.assertIsNone(invalid['per_rank'])
        self.assertIsNone(invalid['value_score'])
        self.assertIsNone(invalid['value_rank'])
        self.assertEqual(scored['value_status'], VALUE_STATUS_SCORED)
        self.assertEqual(result['summary']['invalid_per_count'], 1)

    def test_rebuild_marks_missing_dividend_yield_as_missing_metric(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            self._input(symbol='000001', per=7.0, pbr=0.8, dividend_yield=None, market_date=market_date),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['000001'], [])),
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['value_status'], VALUE_STATUS_MISSING_METRIC)
        self.assertIsNone(persisted['value_score'])
        self.assertEqual(result['summary']['missing_metric_count'], 1)

    def test_rebuild_prefers_same_day_valuation_snapshot_when_bar_metrics_are_null(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            ValueFactorInput(
                symbol='005930',
                name_ko='Samsung Electronics',
                market='KOSPI',
                bars=[
                    {
                        'trade_date': market_date.isoformat(),
                        'close_price': 100000,
                        'per': None,
                        'pbr': None,
                        'dividend_yield': None,
                        'source': 'pykrx',
                    }
                ],
                valuation_snapshot={
                    'per': 8.0,
                    'pbr': 0.9,
                    'dividend_yield': 0.03,
                },
            )
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['value_status'], VALUE_STATUS_SCORED)
        self.assertEqual(persisted['per'], 8.0)
        self.assertEqual(persisted['pbr'], 0.9)
        self.assertEqual(persisted['dividend_yield'], 0.03)
        self.assertEqual(
            persisted['factor_payload']['audit']['valuation_source'],
            'pykrx_valuation_snapshot',
        )
        self.assertEqual(
            persisted['factor_payload']['audit']['valuation_metric_sources'],
            {
                'per': 'pykrx_valuation_snapshot',
                'pbr': 'pykrx_valuation_snapshot',
                'dividend_yield': 'pykrx_valuation_snapshot',
            },
        )
        self.assertEqual(result['summary']['scored_count'], 1)

    def test_rebuild_tracks_mixed_sources_and_prefers_snapshot_values(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        inputs = [
            ValueFactorInput(
                symbol='005930',
                name_ko='Samsung Electronics',
                market='KOSPI',
                bars=[
                    {
                        'trade_date': market_date.isoformat(),
                        'close_price': 100000,
                        'per': 18.0,
                        'pbr': 0.95,
                        'dividend_yield': 0.02,
                        'source': 'pykrx',
                    }
                ],
                valuation_snapshot={'per': 7.0},
            )
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0]
        self.assertEqual(persisted['per'], 7.0)
        self.assertEqual(persisted['pbr'], 0.95)
        self.assertEqual(persisted['dividend_yield'], 0.02)
        self.assertEqual(persisted['factor_payload']['audit']['valuation_source'], 'mixed')
        self.assertEqual(
            persisted['factor_payload']['audit']['valuation_metric_sources'],
            {
                'per': 'pykrx_valuation_snapshot',
                'pbr': 'daily_price_bars',
                'dividend_yield': 'daily_price_bars',
            },
        )

    def test_load_inputs_attaches_same_day_pykrx_valuation_snapshot(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        valuation_snapshot = {
            'per': 8.0,
            'pbr': 0.9,
            'dividend_yield': 0.03,
        }

        with (
            patch(
                'app.signals.value_factor.list_symbols_with_daily_bar_on_date',
                return_value=['005930'],
            ),
            patch(
                'app.signals.value_factor.load_symbols',
                return_value=[
                    {
                        'symbol': '005930',
                        'name_ko': 'Samsung Electronics',
                        'market': 'KOSPI',
                    }
                ],
            ),
            patch(
                'app.signals.value_factor.load_recent_price_bars_by_symbol',
                return_value={'005930': []},
            ),
            patch(
                'app.signals.value_factor.load_daily_pykrx_enrichment_rows',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'pykrx',
                        'payload': valuation_snapshot,
                    }
                ],
            ) as load_enrichment,
        ):
            inputs, matched, missing = service._load_inputs(
                market_date=market_date,
                symbols=None,
            )

        self.assertEqual(matched, ['005930'])
        self.assertEqual(missing, [])
        self.assertEqual(inputs[0].valuation_snapshot, valuation_snapshot)
        load_enrichment.assert_called_once_with(
            market_date=market_date,
            categories=['valuation_snapshot'],
            scope_types=['symbol'],
            symbols=['005930'],
            source='pykrx',
            limit=None,
        )

    def test_subset_rerun_uses_full_universe_rankings(self) -> None:
        service = DailyValueFactorService()
        market_date = date(2026, 4, 24)
        all_inputs = [
            self._input(symbol='000001', per=5.0, pbr=0.5, dividend_yield=0.05, market_date=market_date),
            self._input(symbol='000002', per=10.0, pbr=1.0, dividend_yield=0.03, market_date=market_date),
            self._input(symbol='000003', per=20.0, pbr=2.0, dividend_yield=0.01, market_date=market_date),
        ]

        def load_inputs_side_effect(*, market_date, symbols):
            if symbols is None:
                return (all_inputs, ['000001', '000002', '000003'], [])
            return ([all_inputs[1]], ['000002'], [])

        with (
            patch.object(service, '_load_inputs', side_effect=load_inputs_side_effect),
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date, symbols=['000002'])

        persisted = upsert.call_args.args[0]
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['summary']['ranking_universe_counts']['per'], 3)
        self.assertEqual(result['summary']['ranking_universe_counts']['composite_score'], 3)
        self.assertEqual(persisted['symbol'], '000002')
        self.assertEqual(persisted['per_rank'], 2)
        self.assertEqual(persisted['pbr_rank'], 2)
        self.assertEqual(persisted['dividend_yield_rank'], 2)
        self.assertEqual(persisted['value_rank'], 2)

    def test_rebuild_with_empty_symbols_is_no_op(self) -> None:
        service = DailyValueFactorService()

        with (
            patch.object(service, '_load_inputs') as load_inputs,
            patch('app.signals.value_factor.upsert_daily_value_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-24',
                symbols=[],
            )

        load_inputs.assert_not_called()
        upsert.assert_not_called()
        self.assertEqual(result['requested_symbols'], [])
        self.assertEqual(result['matched_symbols'], [])
        self.assertEqual(result['missing_symbols'], [])
        self.assertEqual(result['results_persisted'], 0)
        self.assertEqual(result['results_failed'], 0)
        self.assertEqual(result['items'], [])
        self.assertEqual(
            result['summary'],
            {
                'requested_symbol_count': 0,
                'matched_symbol_count': 0,
                'missing_symbol_count': 0,
                'symbols_with_market_date_bar': 0,
                'missing_market_date_bar_count': 0,
                'invalid_per_count': 0,
                'missing_metric_count': 0,
                'scored_count': 0,
                'ranking_universe_counts': {
                    'per': 0,
                    'pbr': 0,
                    'dividend_yield': 0,
                    'composite_score': 0,
                },
            },
        )

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        service = DailyValueFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-24',
                'value_status': VALUE_STATUS_SCORED,
                'value_score': 0.8,
            }
        ]

        with patch('app.signals.value_factor.load_daily_value_factor_results', return_value=stored_items) as loader:
            result = service.inspect(market_date='2026-04-24', symbol='5930', limit=50)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['feature_version'], VALUE_FACTOR_VERSION)
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])
