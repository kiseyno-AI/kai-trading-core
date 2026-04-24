import unittest
from datetime import date
from unittest.mock import patch

from app.signals.piotroski_filter import (
    LATEST_FALLBACK_POLICY,
    DailyPiotroskiFilterService,
    PiotroskiInputs,
)


def _build_input(
    *,
    symbol: str,
    pbr: float,
    return_20d: float,
    has_audit_issue: bool = False,
) -> PiotroskiInputs:
    return PiotroskiInputs(
        symbol=symbol,
        name_ko=symbol,
        market_date=date(2026, 4, 14),
        pbr=pbr,
        return_20d=return_20d,
        total_equity=100.0,
        net_income=20.0,
        prior_net_income=10.0,
        operating_cash_flow=30.0,
        total_assets=200.0,
        prior_total_assets=180.0,
        debt_ratio=0.3,
        prior_debt_ratio=0.4,
        current_ratio=1.8,
        prior_current_ratio=1.2,
        gross_margin=0.35,
        prior_gross_margin=0.30,
        asset_turnover=1.4,
        prior_asset_turnover=1.2,
        has_audit_issue=has_audit_issue,
        has_dilution_signal=False,
    )


class DailyPiotroskiFilterServiceTests(unittest.TestCase):
    def test_collect_and_persist_stores_passed_and_not_passed_symbols(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0, has_audit_issue=True),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch('app.signals.piotroski_filter.upsert_daily_piotroski_filter_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['symbols_passed'], 1)
        self.assertEqual(result['symbols_not_passed'], 1)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['results_failed'], 0)
        self.assertEqual(upsert.call_count, 2)

        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}
        self.assertTrue(persisted_by_symbol['005930']['passed_filter'])
        self.assertFalse(persisted_by_symbol['000660']['passed_filter'])
        self.assertTrue(persisted_by_symbol['005930']['passes_piotroski_score'])
        self.assertTrue(persisted_by_symbol['000660']['is_bottom_momentum'])

    def test_collect_and_persist_continues_when_one_symbol_write_fails(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
        ]

        def failing_upsert(payload: dict) -> None:
            if payload['symbol'] == '000660':
                raise RuntimeError('write failed')

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch(
                'app.signals.piotroski_filter.upsert_daily_piotroski_filter_result',
                side_effect=failing_upsert,
            ),
        ):
            result = service.rebuild_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['000660'])

    def test_inspect_returns_exact_market_date_when_requested_date_is_available(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-14',
                'passed_filter': True,
            },
            {
                'symbol': '000660',
                'market_date': '2026-04-14',
                'passed_filter': False,
            },
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-14',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(market_date='2026-04-14')

        latest_loader.assert_called_once_with(
            market_date=date(2026, 4, 14),
            symbols=None,
        )
        loader.assert_called_once_with(
            market_date='2026-04-14',
            symbols=None,
            limit=None,
        )
        self.assertEqual(result['requested_market_date'], '2026-04-14')
        self.assertEqual(result['resolved_market_date'], '2026-04-14')
        self.assertEqual(result['market_date'], '2026-04-14')
        self.assertFalse(result['latest_mode'])
        self.assertFalse(result['is_latest_fallback'])
        self.assertEqual(result['fallback_policy'], LATEST_FALLBACK_POLICY)
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['passed_count'], 1)
        self.assertEqual(result['summary']['not_passed_count'], 1)
        self.assertEqual(result['items'], stored_items)

    def test_inspect_uses_latest_mode_when_market_date_is_omitted(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'passed_filter': True,
            },
            {
                'symbol': '000660',
                'market_date': '2026-04-23',
                'passed_filter': False,
            },
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-23',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect()

        latest_loader.assert_called_once_with(
            market_date=None,
            symbols=None,
        )
        loader.assert_called_once_with(
            market_date='2026-04-23',
            symbols=None,
            limit=None,
        )
        self.assertIsNone(result['requested_market_date'])
        self.assertEqual(result['resolved_market_date'], '2026-04-23')
        self.assertEqual(result['market_date'], '2026-04-23')
        self.assertTrue(result['latest_mode'])
        self.assertFalse(result['is_latest_fallback'])
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['items'], stored_items)

    def test_inspect_uses_latest_fallback_when_requested_market_date_is_missing(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'passed_filter': True,
            },
            {
                'symbol': '000660',
                'market_date': '2026-04-23',
                'passed_filter': False,
            },
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-23',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(market_date='2026-04-24')

        latest_loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=None,
        )
        loader.assert_called_once_with(
            market_date='2026-04-23',
            symbols=None,
            limit=None,
        )
        self.assertEqual(result['requested_market_date'], '2026-04-24')
        self.assertEqual(result['resolved_market_date'], '2026-04-23')
        self.assertEqual(result['market_date'], '2026-04-23')
        self.assertFalse(result['latest_mode'])
        self.assertTrue(result['is_latest_fallback'])
        self.assertEqual(result['fallback_policy'], LATEST_FALLBACK_POLICY)
        self.assertEqual(result['items'], stored_items)

    def test_inspect_returns_single_item_when_symbol_is_requested(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'passed_filter': True,
            }
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-23',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(symbol='5930', limit=25)

        latest_loader.assert_called_once_with(
            market_date=None,
            symbols=['005930'],
        )
        loader.assert_called_once_with(
            market_date='2026-04-23',
            symbols=['005930'],
            limit=1,
        )
        self.assertTrue(result['latest_mode'])
        self.assertEqual(result['resolved_market_date'], '2026-04-23')
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['summary']['missing_symbols'], [])
        self.assertEqual(result['item'], stored_items[0])

    def test_inspect_rejects_blank_symbol(self):
        service = DailyPiotroskiFilterService()

        with self.assertRaisesRegex(ValueError, 'symbol must not be blank'):
            service.inspect(market_date='2026-04-14', symbol='   ')
