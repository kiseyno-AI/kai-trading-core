import unittest
from unittest.mock import patch

from app.agent_context.read_service import AgentContextReadService, _build_price_context


class FakeFundamentalsRepository:
    def list_stored_fundamentals(self, *, stock_codes, fs_div=None):
        raise AssertionError('agent context should use summary fundamentals path')

    def list_stored_fundamentals_summary(self, *, stock_codes, fs_div=None):
        assert stock_codes == ['005930', '000660']
        assert fs_div == 'CFS'
        return [
            {
                'rcept_no': '20250312000123',
                'corp_code': '00126380',
                'stock_code': '005930',
                'corp_name': '삼성전자',
                'business_year': 2024,
                'report_code': '11011',
                'filing_category': 'annual_report',
                'fs_div': 'CFS',
                'statement_type': 'consolidated',
                'period_type': 'annual',
                'period_end_month': 12,
                'currency': 'KRW',
                'source_status': '000',
                'source_message': 'OK',
                'captured_at': '2025-03-12T01:02:03+00:00',
                'disclosure': {
                    'report_nm': '사업보고서 (2024.12)',
                    'receipt_date': '2025-03-12',
                },
                'highlights': [
                    {
                        'metric_key': 'revenue',
                        'metric_type': 'account',
                        'statement_type': 'IS',
                        'statement_name': '손익계산서',
                        'account_id': 'ifrs-full_Revenue',
                        'account_name': '매출액',
                        'current_amount': 100,
                        'current_cumulative_amount': 100,
                        'prior_amount': 90,
                        'prior_cumulative_amount': 90,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    }
                ],
                'statement_row_count': 1,
            },
            {
                'rcept_no': '20260410000123',
                'corp_code': '00126380',
                'stock_code': '005930',
                'corp_name': '삼성전자',
                'business_year': 2025,
                'report_code': '11013',
                'filing_category': 'quarterly_report',
                'fs_div': 'CFS',
                'statement_type': 'consolidated',
                'period_type': 'q1',
                'period_end_month': 3,
                'currency': 'KRW',
                'source_status': '000',
                'source_message': 'OK',
                'captured_at': '2026-04-10T01:02:03+00:00',
                'disclosure': {
                    'report_nm': '분기보고서 (2025.03)',
                    'receipt_date': '2026-04-10',
                },
                'highlights': [],
                'statement_row_count': 0,
            },
        ]


class FakeDisclosureRepository:
    def list_recent_events_by_stock_codes(self, *, stock_codes, receipt_date_to=None, limit_per_symbol=5):
        assert stock_codes == ['005930', '000660']
        assert receipt_date_to.isoformat() == '2026-04-08'
        assert limit_per_symbol == 3
        return {
            '005930': [
                {
                    'rcept_no': '20260407000001',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'report_nm': '사업보고서 (2025.12)',
                    'filing_category': 'annual_report',
                    'is_financial_report': True,
                    'receipt_date': '2026-04-07',
                    'remark': None,
                    'fundamentals_capture_status': 'captured',
                }
            ],
            '000660': [],
        }


class AgentContextReadServiceTests(unittest.TestCase):
    @patch('app.agent_context.read_service.load_daily_price_contexts')
    @patch('app.agent_context.read_service.load_daily_action_summaries')
    @patch('app.agent_context.read_service.load_daily_signal_features')
    @patch('app.agent_context.read_service.load_daily_technical_snapshots')
    @patch('app.agent_context.read_service.load_symbols')
    def test_get_symbol_day_context_aggregates_existing_blocks(
        self,
        load_symbols_mock,
        load_technical_mock,
        load_signal_features_mock,
        load_actions_mock,
        load_price_contexts_mock,
    ):
        load_symbols_mock.return_value = [
            {
                'id': 1,
                'symbol': '005930',
                'standard_code': 'KR7005930003',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'instrument_type': 'equity',
                'source': 'pykrx',
                'is_active': True,
                'last_refreshed_at': None,
            }
        ]
        load_technical_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'close_price': 71000,
                'volume': 100,
                'trend_state': 'uptrend',
                'ma_alignment': 'bullish',
                'breakout_20d': True,
                'breakdown_20d': False,
                'return_5d': 5.2,
                'return_20d': 12.1,
                'volume_ratio_20d': 1.8,
                'atr_14': 1200.0,
                'atr_pct_14': 1.7,
                'signal_payload': {},
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_actions_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'action_label': 'long_breakout',
                'bias_direction': 'long',
                'signal_score': 78,
                'confidence_tier': 'high',
                'risk_tier': 'low',
                'trigger_price': 71000.0,
                'invalidation_price': 69800.0,
                'support_price': 68000.0,
                'resistance_price': 70800.0,
                'reasons': ['fresh_20d_breakout'],
                'summary_payload': {},
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_signal_features_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'kis_ranking',
                'signal_count': 2,
                'volume_power_rank': 3,
                'volume_power': 140.5,
                'overtime_volume_rank': 7,
                'overtime_volume': 120000,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'feature_payload': {'sources': {'volume-power': {'rank': 3}}},
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_price_contexts_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'context_payload': {
                    'trade_date': '2026-04-08',
                    'source': 'pykrx',
                    'captured_at': '2026-04-08T15:30:00+09:00',
                    'open_price': 70000,
                    'high_price': 72000,
                    'low_price': 69950,
                    'close_price': 71500,
                    'volume': 123456,
                    'turnover': 8800000000,
                    'candle': {
                        'direction': 'bullish',
                        'body_points': 1500,
                        'body_percent_of_open': 2.1429,
                        'body_percent_of_range': 73.1707,
                        'body_size': 'large',
                        'upper_wick_points': 500,
                        'lower_wick_points': 50,
                        'wick_bias': 'upper',
                    },
                    'session_range': {
                        'points': 2050,
                        'percent_of_open': 2.9286,
                        'close_position_in_range': 0.7561,
                        'close_position_label': 'upper_half',
                    },
                    'gap': {
                        'previous_trade_date': '2026-04-07',
                        'previous_close_price': 69800,
                        'gap_points': 200,
                        'gap_percent': 0.2865,
                        'direction': 'up',
                        'filled_intraday': False,
                    },
                },
                'captured_at': '2026-04-08T15:30:00+09:00',
            }
        ]

        service = AgentContextReadService(
            fundamentals_repository=FakeFundamentalsRepository(),
            disclosure_repository=FakeDisclosureRepository(),
        )

        result = service.get_symbol_day_context(
            market_date='2026-04-08',
            symbols=['005930', '000660'],
            fs_div='cfs',
            disclosures_limit=3,
        )

        self.assertEqual(load_technical_mock.call_args.kwargs['fallback_window_days'], 0)

        self.assertEqual(result['summary']['requested_symbol_count'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['summary']['price_context_count'], 1)
        self.assertEqual(result['summary']['technical_snapshot_count'], 1)
        self.assertEqual(result['summary']['action_summary_count'], 1)
        self.assertEqual(result['summary']['signal_feature_count'], 1)
        self.assertEqual(result['summary']['fundamentals_count'], 1)
        self.assertEqual(result['summary']['missing_symbols'], ['000660'])

        first_item = result['items'][0]
        self.assertEqual(first_item['symbol'], '005930')
        self.assertEqual(first_item['company']['name_ko'], '삼성전자')
        self.assertEqual(first_item['company']['dart']['corp_code'], '00126380')
        self.assertEqual(first_item['price_context']['open_price'], 70000)
        self.assertEqual(first_item['price_context']['gap']['previous_close_price'], 69800)
        self.assertEqual(first_item['price_context']['gap']['direction'], 'up')
        self.assertFalse(first_item['price_context']['gap']['filled_intraday'])
        self.assertEqual(first_item['price_context']['session_range']['close_position_label'], 'upper_half')
        self.assertEqual(first_item['price_context']['candle']['wick_bias'], 'upper')
        self.assertEqual(first_item['technical_snapshot']['trend_state'], 'uptrend')
        self.assertEqual(first_item['action_summary']['action_label'], 'long_breakout')
        self.assertEqual(first_item['signal_features']['signal_count'], 2)
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['period']['period_end_date'], '2024-12-31')
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['statement']['row_count'], 1)
        self.assertEqual(first_item['recent_disclosures']['count'], 1)
        self.assertTrue(first_item['availability']['price_context'])

        second_item = result['items'][1]
        self.assertEqual(second_item['symbol'], '000660')
        self.assertIsNone(second_item['price_context'])
        self.assertIsNone(second_item['technical_snapshot'])
        self.assertIsNone(second_item['signal_features'])
        self.assertIsNone(second_item['fundamentals']['latest_snapshot'])
        self.assertEqual(second_item['recent_disclosures']['count'], 0)
        self.assertFalse(second_item['availability']['price_context'])

    def test_get_symbol_day_context_requires_at_least_one_symbol(self):
        service = AgentContextReadService(
            fundamentals_repository=FakeFundamentalsRepository(),
            disclosure_repository=FakeDisclosureRepository(),
        )

        with self.assertRaisesRegex(ValueError, 'At least one symbol is required'):
            service.get_symbol_day_context(market_date='2026-04-08', symbols=[])

    def test_build_price_context_marks_gap_fill_and_flat_candle_conservatively(self):
        context = _build_price_context(
            [
                {
                    'trade_date': '2026-04-08',
                    'open_price': 100,
                    'high_price': 108,
                    'low_price': 95,
                    'close_price': 100,
                    'volume': 10,
                    'turnover': 1000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-08T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-04-07',
                    'open_price': 95,
                    'high_price': 99,
                    'low_price': 94,
                    'close_price': 96,
                    'volume': 12,
                    'turnover': 1200,
                    'source': 'pykrx',
                    'captured_at': '2026-04-07T15:30:00+09:00',
                },
            ]
        )

        self.assertEqual(context['candle']['direction'], 'flat')
        self.assertEqual(context['candle']['body_points'], 0)
        self.assertEqual(context['gap']['direction'], 'up')
        self.assertTrue(context['gap']['filled_intraday'])
        self.assertEqual(context['session_range']['close_position_label'], 'lower_half')
