import unittest
from datetime import date, timedelta
from unittest.mock import patch

from app.signals.momentum_factor import (
    DailyMomentumFactorService,
    MomentumOverheatRule,
    build_daily_momentum_factor_rows,
)


def _build_bars(closes: list[int], *, start_date: date = date(2025, 1, 1)) -> list[dict]:
    bars: list[dict] = []
    for index, close in enumerate(closes):
        trade_date = (start_date + timedelta(days=index)).isoformat()
        bars.append(
            {
                'trade_date': trade_date,
                'open_price': close,
                'high_price': close + 1,
                'low_price': max(close - 1, 1),
                'close_price': close,
                'volume': 1_000_000 + index,
                'turnover': close * 1_000,
                'source': 'pykrx',
            }
        )
    return bars


class DailyMomentumFactorBuilderTests(unittest.TestCase):
    def test_build_rows_computes_expected_12_1_momentum(self) -> None:
        closes = [100] + [150] * 252

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={'005930': _build_bars(closes)},
        )

        self.assertEqual(rows[0]['symbol'], '005930')
        self.assertEqual(rows[0]['momentum_12_1'], 0.5)
        self.assertEqual(rows[0]['momentum_rank'], 1)
        self.assertEqual(rows[0]['momentum_percentile'], 1.0)
        self.assertTrue(rows[0]['has_sufficient_history'])
        self.assertEqual(rows[0]['factor_payload']['inputs']['reference_close_t_minus_21'], 150)
        self.assertEqual(rows[0]['factor_payload']['inputs']['reference_close_t_minus_252'], 100)

    def test_build_rows_marks_insufficient_history_without_momentum_rank(self) -> None:
        closes = [100] * 120

        rows = build_daily_momentum_factor_rows(
            market_date='2025-04-30',
            price_histories={'005930': _build_bars(closes)},
        )

        self.assertIsNone(rows[0]['momentum_12_1'])
        self.assertFalse(rows[0]['has_sufficient_history'])
        self.assertIsNone(rows[0]['momentum_rank'])
        self.assertIsNone(rows[0]['momentum_percentile'])
        self.assertEqual(
            rows[0]['factor_payload']['insufficient_history_reason'],
            'requires_at_least_253_stored_trading_bars',
        )

    def test_build_rows_computes_moving_average_flags(self) -> None:
        closes = ([100] * 133) + ([150] * 60) + ([200] * 60)

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={'005930': _build_bars(closes)},
        )

        self.assertEqual(rows[0]['ma_60'], 200.0)
        self.assertEqual(rows[0]['ma_120'], 175.0)
        self.assertTrue(rows[0]['is_price_above_ma120'])
        self.assertTrue(rows[0]['is_ma60_above_ma120'])
        self.assertTrue(rows[0]['is_momentum_eligible'])

    def test_build_rows_classifies_recent_overheat_from_configured_threshold(self) -> None:
        base = [100] * 247
        normal_closes = base + [100, 101, 102, 103, 104, 105]
        hot_closes = base + [100, 104, 108, 112, 120, 130]
        rule = MomentumOverheatRule(return_threshold=0.15)

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={
                '000660': _build_bars(normal_closes),
                '005930': _build_bars(hot_closes),
            },
            overheat_rule=rule,
        )

        by_symbol = {row['symbol']: row for row in rows}
        self.assertFalse(by_symbol['000660']['is_recent_overheated'])
        self.assertTrue(by_symbol['005930']['is_recent_overheated'])
        self.assertEqual(by_symbol['005930']['factor_payload']['overheat_rule']['threshold'], 0.15)
        self.assertFalse(by_symbol['005930']['is_momentum_eligible'])

    def test_build_rows_assigns_cross_sectional_rank_and_percentile(self) -> None:
        histories = {
            '005930': _build_bars([100] + [150] * 252),
            '000660': _build_bars([100] + [120] * 252),
            '035420': _build_bars([100] + [90] * 252),
        }

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories=histories,
        )

        by_symbol = {row['symbol']: row for row in rows}
        self.assertEqual(by_symbol['005930']['momentum_rank'], 1)
        self.assertEqual(by_symbol['005930']['momentum_percentile'], 1.0)
        self.assertEqual(by_symbol['000660']['momentum_rank'], 2)
        self.assertEqual(by_symbol['000660']['momentum_percentile'], 0.5)
        self.assertEqual(by_symbol['035420']['momentum_rank'], 3)
        self.assertEqual(by_symbol['035420']['momentum_percentile'], 0.0)


class DailyMomentumFactorServiceTests(unittest.TestCase):
    def test_rebuild_for_market_date_persists_rows_for_selected_universe(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(reversed(_build_bars([100] + [120] * 252))),
        }

        with (
            patch(
                'app.signals.momentum_factor.list_symbols_with_daily_bar_on_date',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
                return_value=histories,
            ),
            patch('app.signals.momentum_factor.upsert_daily_momentum_factor_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date='2025-09-10')

        self.assertEqual(result['requested_symbols'], 2)
        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['ranked_symbols'], 2)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['results_failed'], 0)
        self.assertEqual(upsert.call_count, 2)
        persisted = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}
        self.assertEqual(persisted['005930']['momentum_rank'], 1)
        self.assertEqual(persisted['000660']['momentum_rank'], 2)

    def test_evaluate_symbol_uses_requested_universe_for_cross_sectional_rank(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(reversed(_build_bars([100] + [120] * 252))),
        }

        with patch(
            'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
            return_value=histories,
        ):
            result = service.evaluate_symbol(
                symbol='5930',
                market_date='2025-09-10',
                universe_symbols=['005930', '000660'],
            )

        self.assertEqual(result['symbol'], '005930')
        self.assertEqual(result['momentum_rank'], 1)
        self.assertEqual(result['factor_payload']['ranking']['ranked_universe_size'], 2)

    def test_inspect_returns_summary_for_requested_symbols(self) -> None:
        service = DailyMomentumFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2025-09-10',
                'momentum_rank': 1,
                'is_momentum_eligible': True,
            }
        ]

        with patch(
            'app.signals.momentum_factor.load_daily_momentum_factor_results',
            return_value=stored_items,
        ) as loader:
            result = service.inspect(market_date='2025-09-10', symbols=['5930', '000660'], limit=10)

        loader.assert_called_once_with(
            market_date=date(2025, 9, 10),
            symbols=['005930', '000660'],
            limit=10,
        )
        self.assertEqual(result['summary']['entries'], 1)
        self.assertEqual(result['summary']['matched_symbols'], ['005930'])
        self.assertEqual(result['summary']['missing_symbols'], ['000660'])
        self.assertEqual(result['summary']['eligible_symbols'], 1)
