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
