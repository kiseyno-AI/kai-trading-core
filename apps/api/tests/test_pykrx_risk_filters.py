import unittest
from datetime import date, timedelta
from unittest.mock import patch

from app.signals.pykrx_risk_filters import (
    LowLiquidityRuleConfig,
    PykrxRiskFilterService,
    TRADING_HALT_LIKE_HEURISTIC,
)


def _symbol(symbol: str, *, market: str = 'KOSPI') -> dict:
    return {
        'symbol': symbol,
        'name_ko': symbol,
        'market': market,
    }


def _bar(
    trade_date: str,
    *,
    open_price: int = 100,
    high_price: int = 110,
    low_price: int = 90,
    close_price: int = 105,
    volume: int = 1_000,
    turnover: int = 1_000_000,
) -> dict:
    return {
        'trade_date': trade_date,
        'open_price': open_price,
        'high_price': high_price,
        'low_price': low_price,
        'close_price': close_price,
        'volume': volume,
        'turnover': turnover,
        'source': 'pykrx',
        'captured_at': f'{trade_date}T15:30:00+09:00',
    }


def _history(
    market_date: date,
    *,
    days: int,
    turnover: int,
    current_bar_overrides: dict | None = None,
) -> list[dict]:
    bars = [
        _bar(
            (market_date - timedelta(days=offset)).isoformat(),
            turnover=turnover,
        )
        for offset in range(days)
    ]
    if current_bar_overrides:
        bars[0].update(current_bar_overrides)
    return bars


class PykrxRiskFilterServiceTests(unittest.TestCase):
    def test_inspect_flags_halt_like_heuristic_and_preserves_audit_evidence(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)
        symbols = [_symbol('000002'), _symbol('000001')]
        bars_by_symbol = {
            '000001': _history(
                market_date,
                days=20,
                turnover=500_000,
                current_bar_overrides={
                    'open_price': 0,
                    'high_price': 0,
                    'low_price': 0,
                    'close_price': 12_345,
                    'volume': 0,
                    'turnover': 0,
                },
            ),
            '000002': _history(market_date, days=20, turnover=5_000_000),
        }

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=symbols),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value=bars_by_symbol,
            ),
        ):
            result = service.inspect(market_date=market_date)

        self.assertEqual([item['symbol'] for item in result['items']], ['000001', '000002'])
        halt_item = result['items'][0]
        normal_item = result['items'][1]

        self.assertTrue(halt_item['is_trading_halt_like'])
        self.assertEqual(
            halt_item['trading_halt_like']['heuristic']['name'],
            TRADING_HALT_LIKE_HEURISTIC,
        )
        self.assertEqual(
            halt_item['trading_halt_like']['evidence'],
            {
                'trade_date': '2026-04-23',
                'open': 0,
                'high': 0,
                'low': 0,
                'close': 12345,
                'volume': 0,
                'trading_value': 0,
            },
        )
        self.assertFalse(normal_item['is_trading_halt_like'])
        self.assertEqual(result['summary']['trading_halt_like_count'], 1)

    def test_evaluate_symbol_returns_single_symbol_result(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=[_symbol('005930')]),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value={'005930': _history(market_date, days=20, turnover=9_000_000)},
            ),
        ):
            result = service.evaluate_symbol(symbol='5930', market_date=market_date)

        self.assertEqual(result['symbol'], '005930')
        self.assertIsNotNone(result['item'])
        self.assertEqual(result['item']['symbol'], '005930')
        self.assertFalse(result['item']['is_trading_halt_like'])

    def test_inspect_computes_20d_average_rank_percentile_and_percentile_rule(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)
        rule = LowLiquidityRuleConfig(kind='bottom_percentile', percentile_threshold=0.34)
        symbols = [_symbol('000003'), _symbol('000001'), _symbol('000002')]
        bars_by_symbol = {
            '000001': _history(market_date, days=20, turnover=100),
            '000002': _history(market_date, days=20, turnover=200),
            '000003': _history(market_date, days=20, turnover=300),
        }

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=symbols),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value=bars_by_symbol,
            ),
        ):
            result = service.inspect(market_date=market_date, liquidity_rule=rule)

        items_by_symbol = {item['symbol']: item for item in result['items']}
        self.assertEqual(items_by_symbol['000001']['low_liquidity_20d']['avg_trading_value_20d'], 100.0)
        self.assertEqual(items_by_symbol['000001']['low_liquidity_20d']['liquidity_rank'], 1)
        self.assertEqual(items_by_symbol['000001']['low_liquidity_20d']['liquidity_percentile'], 0.0)
        self.assertTrue(items_by_symbol['000001']['is_low_liquidity'])

        self.assertEqual(items_by_symbol['000002']['low_liquidity_20d']['avg_trading_value_20d'], 200.0)
        self.assertEqual(items_by_symbol['000002']['low_liquidity_20d']['liquidity_rank'], 2)
        self.assertEqual(items_by_symbol['000002']['low_liquidity_20d']['liquidity_percentile'], 0.5)
        self.assertFalse(items_by_symbol['000002']['is_low_liquidity'])

        self.assertEqual(items_by_symbol['000003']['low_liquidity_20d']['avg_trading_value_20d'], 300.0)
        self.assertEqual(items_by_symbol['000003']['low_liquidity_20d']['liquidity_rank'], 3)
        self.assertEqual(items_by_symbol['000003']['low_liquidity_20d']['liquidity_percentile'], 1.0)
        self.assertFalse(items_by_symbol['000003']['is_low_liquidity'])

    def test_get_low_liquidity_symbols_supports_bottom_rank_rule_with_stable_sort(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)
        rule = LowLiquidityRuleConfig(kind='bottom_rank', max_rank=2)
        symbols = [_symbol('000003'), _symbol('000002'), _symbol('000001')]
        bars_by_symbol = {
            '000001': _history(market_date, days=20, turnover=100),
            '000002': _history(market_date, days=20, turnover=100),
            '000003': _history(market_date, days=20, turnover=300),
        }

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=symbols),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value=bars_by_symbol,
            ),
        ):
            result = service.get_low_liquidity_symbols(
                market_date=market_date,
                liquidity_rule=rule,
            )

        self.assertEqual([item['symbol'] for item in result['items']], ['000001', '000002'])
        self.assertEqual(
            [item['low_liquidity_20d']['liquidity_rank'] for item in result['items']],
            [1, 2],
        )

    def test_inspect_supports_absolute_threshold_rule(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)
        rule = LowLiquidityRuleConfig(kind='max_avg_trading_value', max_avg_trading_value=150.0)
        symbols = [_symbol('000001'), _symbol('000002')]
        bars_by_symbol = {
            '000001': _history(market_date, days=20, turnover=100),
            '000002': _history(market_date, days=20, turnover=200),
        }

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=symbols),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value=bars_by_symbol,
            ),
        ):
            result = service.inspect(market_date=market_date, liquidity_rule=rule)

        items_by_symbol = {item['symbol']: item for item in result['items']}
        self.assertTrue(items_by_symbol['000001']['is_low_liquidity'])
        self.assertFalse(items_by_symbol['000002']['is_low_liquidity'])
        self.assertEqual(
            items_by_symbol['000001']['low_liquidity_20d']['reason'],
            'max_avg_trading_value_threshold_met',
        )

    def test_inspect_marks_insufficient_history_explicitly(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=[_symbol('005930')]),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value={'005930': _history(market_date, days=10, turnover=500_000)},
            ),
        ):
            result = service.inspect(market_date=market_date)

        item = result['items'][0]
        self.assertIsNone(item['is_low_liquidity'])
        self.assertFalse(item['low_liquidity_20d']['available'])
        self.assertEqual(item['low_liquidity_20d']['reason'], 'insufficient_history')
        self.assertEqual(item['low_liquidity_20d']['history_days_available'], 10)
        self.assertEqual(item['low_liquidity_20d']['required_history_days'], 20)
        self.assertEqual(result['summary']['insufficient_history_count'], 1)

    def test_inspect_marks_missing_market_date_bar_and_applies_market_filter(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)
        symbols = [_symbol('123456', market='KOSDAQ')]
        bars_by_symbol = {
            '123456': [_bar('2026-04-22', turnover=500_000)],
        }

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=symbols) as load_symbols_mock,
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value=bars_by_symbol,
            ),
        ):
            result = service.inspect(market_date=market_date, market='kosdaq')

        load_symbols_mock.assert_called_once_with(
            is_active=True,
            markets=['KOSDAQ'],
            symbols=None,
        )
        item = result['items'][0]
        self.assertIsNone(item['is_trading_halt_like'])
        self.assertEqual(item['trading_halt_like']['reason'], 'missing_market_date_bar')
        self.assertEqual(item['low_liquidity_20d']['reason'], 'missing_market_date_bar')
        self.assertEqual(result['summary']['symbols_missing_market_date_bar'], 1)

    def test_inspect_with_explicit_empty_symbols_returns_empty_result_without_loading(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols') as load_symbols_mock,
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol'
            ) as load_bars_mock,
        ):
            result = service.inspect(market_date=market_date, symbols=[])

        load_symbols_mock.assert_not_called()
        load_bars_mock.assert_not_called()
        self.assertEqual(result['market_date'], '2026-04-23')
        self.assertEqual(result['requested_symbols'], [])
        self.assertEqual(result['matched_symbols'], [])
        self.assertEqual(result['missing_symbols'], [])
        self.assertEqual(result['summary']['requested_symbol_count'], 0)
        self.assertEqual(result['summary']['matched_symbol_count'], 0)
        self.assertEqual(result['summary']['trading_halt_like_count'], 0)
        self.assertEqual(result['summary']['low_liquidity_count'], 0)
        self.assertEqual(result['items'], [])

    def test_get_trading_halt_like_symbols_returns_stably_sorted_items(self) -> None:
        service = PykrxRiskFilterService()
        market_date = date(2026, 4, 23)
        halt_row = {
            'open_price': 0,
            'high_price': 0,
            'low_price': 0,
            'close_price': 8_000,
            'volume': 0,
            'turnover': 0,
        }
        symbols = [_symbol('000020'), _symbol('000010')]
        bars_by_symbol = {
            '000010': _history(market_date, days=20, turnover=100, current_bar_overrides=halt_row),
            '000020': _history(market_date, days=20, turnover=100, current_bar_overrides=halt_row),
        }

        with (
            patch('app.signals.pykrx_risk_filters.load_symbols', return_value=symbols),
            patch(
                'app.signals.pykrx_risk_filters.load_recent_price_bars_by_symbol',
                return_value=bars_by_symbol,
            ),
        ):
            result = service.get_trading_halt_like_symbols(market_date=market_date)

        self.assertEqual([item['symbol'] for item in result['items']], ['000010', '000020'])
        self.assertEqual(result['summary']['trading_halt_like_count'], 2)


if __name__ == '__main__':
    unittest.main()
