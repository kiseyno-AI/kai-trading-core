from contextlib import ExitStack
import unittest
from unittest.mock import patch

from app.agent_context.read_service import AgentContextReadService, PERSISTED_FACTOR_FAMILIES, _build_price_context

EMPTY_LATEST_PRIOR_ROWS = {
    'factor_rows': {family: {} for family in PERSISTED_FACTOR_FAMILIES},
    'signal_features': {},
}


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
    @patch('app.agent_context.read_service.load_daily_market_relative_contexts')
    @patch('app.agent_context.read_service.load_daily_composite_factor_results')
    @patch('app.agent_context.read_service.load_daily_beta_factor_results')
    @patch('app.agent_context.read_service.load_daily_low_risk_factor_results')
    @patch('app.agent_context.read_service.load_daily_advanced_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_momentum_factor_results')
    @patch('app.agent_context.read_service.load_daily_quality_factor_results')
    @patch('app.agent_context.read_service.load_daily_unified_value_factor_results')
    @patch('app.agent_context.read_service.load_current_quotes')
    @patch('app.agent_context.read_service.load_daily_price_context_history')
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
        load_price_context_history_mock,
        load_current_quotes_mock,
        load_unified_value_mock,
        load_quality_mock,
        load_momentum_mock,
        load_advanced_value_mock,
        load_low_risk_mock,
        load_beta_mock,
        load_composite_mock,
        load_market_relative_mock,
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
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'market_date': '2026-04-08',
                'source': 'kis_ranking',
                'signal_count': 2,
                'volume_power_rank': 3,
                'volume_power': 140.5,
                'overtime_volume_rank': 7,
                'overtime_volume': 120000,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'row_kind': 'direct_plus_fallback',
                'provenance': {
                    'row_kind': 'direct_plus_fallback',
                    'direct_signal_source_count': 2,
                    'fallback_groups_present': ['price_action', 'factor_scores'],
                },
                'freshness': {
                    'market_date': '2026-04-08',
                    'direct_sources_collected_at': '2026-04-08T15:00:00+09:00',
                },
                'fallback_features': {
                    'price_action': {'close_return_1d_pct': 2.43},
                },
                'feature_payload': {
                    'sources': {'volume-power': {'rank': 3}},
                    'row_provenance': {
                        'row_kind': 'direct_plus_fallback',
                        'direct_signal_source_count': 2,
                        'fallback_groups_present': ['price_action', 'factor_scores'],
                    },
                },
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_unified_value_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'unified_value_v1',
                'feature_version': 'unified_value_v1',
                'unified_value_score': 0.84,
                'unified_value_rank': 5,
                'unified_value_percentile': 0.96,
                'components_used': 5,
                'unified_value_status': 'scored',
                'per': 10.1,
                'pbr': 1.2,
                'dividend_yield': 2.1,
                'fcf_yield': 4.3,
                'ev_to_ebitda': 8.4,
                'per_status': 'available',
                'pbr_status': 'available',
                'dividend_yield_status': 'available',
                'fcf_yield_status': 'available',
                'ev_to_ebitda_status': 'available',
                'factor_payload': {
                    'availability': {
                        'components_used': 5,
                        'components_required': 5,
                        'valid_component_keys': ['per', 'pbr', 'dividend_yield', 'fcf_yield', 'ev_to_ebitda'],
                        'missing_component_keys': [],
                    },
                    'component_statuses': {
                        'per': 'available',
                        'pbr': 'available',
                        'dividend_yield': 'available',
                        'fcf_yield': 'available',
                        'ev_to_ebitda': 'available',
                    },
                    'methodology': {
                        'score_method': 'equal_weight_mean_of_five_component_percentiles',
                        'score_requirement': 'all_five_components_required',
                        'ranking_directions': {
                            'per': 'lower_better_positive_only',
                            'pbr': 'lower_better_positive_only',
                            'dividend_yield': 'higher_better_non_negative',
                            'fcf_yield': 'higher_better',
                            'ev_to_ebitda': 'lower_better',
                            'unified_value_score': 'higher_better',
                        },
                    },
                },
            }
        ]
        load_quality_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'quality_v1',
                'feature_version': 'quality_v1',
                'quality_score': 0.741,
                'quality_rank': 7,
                'quality_percentile': 0.88,
                'components_used': 5,
                'components_possible': 6,
                'is_quality_score_complete': False,
                'quality_status': 'scored_partial',
                'roe': 0.12,
                'roa': 0.08,
                'gross_profit_to_assets': 0.11,
                'operating_cash_flow_to_net_income': 1.2,
                'debt_ratio': 0.41,
                'accruals_ratio': None,
                'roe_status': 'available',
                'roa_status': 'available',
                'gross_profit_to_assets_status': 'available',
                'operating_cash_flow_to_net_income_status': 'available',
                'debt_ratio_status': 'available',
                'accruals_ratio_status': 'missing_total_assets',
                'factor_payload': {
                    'availability': {
                        'components_used': 5,
                        'components_possible': 6,
                        'is_quality_score_complete': False,
                        'valid_component_keys': [
                            'roe',
                            'roa',
                            'gross_profit_to_assets',
                            'operating_cash_flow_to_net_income',
                            'debt_ratio',
                        ],
                        'missing_component_keys': ['accruals_ratio'],
                    },
                    'component_statuses': {
                        'roe': 'available',
                        'roa': 'available',
                        'gross_profit_to_assets': 'available',
                        'operating_cash_flow_to_net_income': 'available',
                        'debt_ratio': 'available',
                        'accruals_ratio': 'missing_total_assets',
                    },
                    'methodology': {
                        'score_method': 'equal_weight_mean_of_available_component_percentiles',
                        'score_requirement': 'at_least_one_component_required',
                        'complete_requirement': 'all_six_components_required',
                        'ranking_directions': {
                            'roe': 'higher_better',
                            'roa': 'higher_better',
                            'gross_profit_to_assets': 'higher_better',
                            'operating_cash_flow_to_net_income': 'higher_better_positive_net_income_required',
                            'debt_ratio': 'lower_better',
                            'accruals_ratio': 'lower_better',
                            'quality_score': 'higher_better',
                        },
                    },
                },
            }
        ]
        load_momentum_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'daily_price_bars',
                'feature_version': 'price_momentum_v1',
                'close_price': 71500,
                'ma_60': 68500.0,
                'ma_120': 64100.0,
                'momentum_12_1': 0.421,
                'return_5d': 0.052,
                'momentum_rank': 12,
                'momentum_percentile': 0.91,
                'momentum_percentile_threshold': 0.8,
                'passes_momentum_top_percentile': True,
                'is_momentum_eligible': True,
                'factor_payload': {
                    'feature_version': 'price_momentum_v1',
                    'ranking': {
                        'momentum_percentile_threshold': 0.8,
                    },
                    'eligibility': {
                        'passes_trend_confirmation': True,
                        'passes_momentum_top_percentile': True,
                        'momentum_eligibility_status': 'eligible',
                        'is_momentum_eligible': True,
                    },
                    'eligibility_rule': {
                        'rule_version': 'momentum_top_percentile_v1',
                        'percentile_gate': {
                            'field': 'momentum_percentile',
                            'comparison': '>=',
                            'threshold': 0.8,
                        },
                    },
                    'overheat': {
                        'has_sufficient_cross_sectional_context': True,
                        'is_recent_overheated': False,
                    },
                    'overheat_rule': {
                        'rule_version': 'momentum_overheat_v2',
                    },
                    'volatility': {
                        'return_5d_vol_adjusted': 1.4,
                    },
                },
            }
        ]
        load_advanced_value_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'advanced_value_v1',
                'feature_version': 'advanced_value_v1',
                'advanced_value_score': 0.81,
                'advanced_value_rank': 9,
                'advanced_value_percentile': 0.92,
                'advanced_value_components_used': 2,
                'advanced_value_classification': 'eligible',
                'is_advanced_value_eligible': True,
                'market_cap_proxy': 1000000,
                'enterprise_value': 1200000,
                'ebitda': 50000,
                'free_cash_flow': 42000,
                'fcf_yield': 0.041,
                'ev_to_ebitda': 24.0,
                'fcf_yield_status': 'available',
                'ev_to_ebitda_status': 'available',
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_low_risk_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'low_risk_v1',
                'feature_version': 'low_risk_v1',
                'low_risk_score': 0.67,
                'low_risk_classification': 'watchlist',
                'is_low_risk_eligible': False,
                'volatility_60d': 0.12,
                'volatility_120d': 0.18,
                'max_drawdown_120d': 0.22,
                'avg_trading_value_20d': 1200000000,
                'volatility_60d_percentile': 0.81,
                'volatility_120d_percentile': 0.75,
                'max_drawdown_120d_percentile': 0.62,
                'liquidity_percentile': 0.91,
                'passes_liquidity_floor': True,
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_beta_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'beta_v1',
                'feature_version': 'beta_v1',
                'low_beta_score': 0.58,
                'low_beta_classification': 'eligible',
                'is_low_beta_eligible': True,
                'benchmark_id': 'KOSPI',
                'benchmark_market': 'KOSPI',
                'beta_60d': 0.84,
                'beta_120d': 0.92,
                'beta_60d_percentile': 0.77,
                'beta_120d_percentile': 0.69,
                'history_days_available': 120,
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_composite_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'composite_v1',
                'feature_version': 'composite_v1',
                'composite_score': 0.73,
                'composite_rank': 11,
                'composite_percentile': 0.89,
                'composite_classification': 'eligible',
                'is_composite_eligible': True,
                'valid_factor_count': 5,
                'available_base_weight': 1.0,
                'has_required_inputs': True,
                'is_hard_excluded': False,
                'momentum_input': 0.91,
                'value_input': 0.63,
                'advanced_value_input': 0.81,
                'low_risk_input': 0.67,
                'low_beta_input': 0.58,
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_current_quotes_mock.return_value = [
            {
                'symbol': '005930',
                'source': 'kis',
                'market_state': 'OPEN',
                'is_live': True,
                'fallback': False,
                'available': True,
                'price': 71550,
                'change': 50,
                'change_pct': 0.07,
                'volume': 150000,
                'turnover': 9000000000,
                'bid_price': 71500,
                'ask_price': 71600,
                'as_of': '095700',
                'trade_date': '2026-04-08',
                'cached_at': '2026-04-08T09:57:00+09:00',
                'captured_at': '2026-04-08T09:57:00+09:00',
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
                    'liquidity': {
                        'turnover_krw': 8800000000,
                        'turnover_source': 'provider',
                        'turnover_is_estimated': False,
                        'provider_turnover_krw': 8800000000,
                        'estimated_turnover_krw': 8827104000,
                    },
                    'price_bar_quality': {
                        'open_price_zero': False,
                        'turnover_missing_or_zero': False,
                        'volume_zero': False,
                        'gap_unreliable_due_to_open': False,
                        'possible_suspension_or_special_session': False,
                        'has_any_issue': False,
                        'issue_codes': [],
                    },
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
                        'reliable': True,
                    },
                },
                'captured_at': '2026-04-08T15:30:00+09:00',
            }
        ]
        load_price_context_history_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-07',
                'source': 'pykrx',
                'context_payload': {
                    'trade_date': '2026-04-07',
                    'open_price': 69400,
                    'close_price': 69800,
                    'candle': {'direction': 'bullish'},
                    'session_range': {'close_position_label': 'upper_half'},
                    'gap': {'direction': 'flat'},
                },
                'captured_at': '2026-04-07T15:30:00+09:00',
            },
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'context_payload': {
                    'trade_date': '2026-04-08',
                    'open_price': 70000,
                    'close_price': 71500,
                    'candle': {'direction': 'bullish'},
                    'session_range': {'close_position_label': 'upper_half'},
                    'gap': {'direction': 'up'},
                },
                'captured_at': '2026-04-08T15:30:00+09:00',
            },
        ]
        load_market_relative_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'status': 'available',
                'market': 'KOSPI',
                'breadth': {
                    'basis': 'close_vs_previous_close',
                    'market_symbol_count': 810,
                    'up_count': 470,
                    'down_count': 260,
                    'flat_count': 80,
                    'missing_previous_close_count': 0,
                },
                'relative_strength': {
                    'return_pct': 2.43,
                    'return_rank': 41,
                    'return_percentile': 0.95,
                    'return_population': 810,
                    'volume': 123456,
                    'volume_rank': 55,
                    'volume_percentile': 0.93,
                    'volume_population': 810,
                },
            }
        ]

        service = AgentContextReadService(
            fundamentals_repository=FakeFundamentalsRepository(),
            disclosure_repository=FakeDisclosureRepository(),
        )
        with patch(
            'app.agent_context.read_service._load_latest_symbol_day_block_history',
            return_value=EMPTY_LATEST_PRIOR_ROWS,
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930', '000660'],
                fs_div='cfs',
                disclosures_limit=3,
            )

        self.assertEqual(load_technical_mock.call_args.kwargs['fallback_window_days'], 0)

        self.assertEqual(result['summary']['requested_symbol_count'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['summary']['quote_context_count'], 1)
        self.assertEqual(result['summary']['price_context_count'], 1)
        self.assertEqual(result['summary']['technical_snapshot_count'], 1)
        self.assertEqual(result['summary']['action_summary_count'], 1)
        self.assertEqual(result['summary']['signal_feature_count'], 1)
        self.assertEqual(result['summary']['market_relative_context_count'], 1)
        self.assertEqual(result['summary']['fundamentals_count'], 1)
        self.assertEqual(result['summary']['missing_symbols'], ['000660'])
        self.assertEqual(result['summary']['factor_row_counts']['unified_value'], 1)
        self.assertEqual(result['summary']['context_completeness']['signal_features']['coverage_status'], 'sparse')
        self.assertEqual(result['summary']['context_completeness']['signal_features']['missing_rows'], 1)

        first_item = result['items'][0]
        self.assertEqual(first_item['symbol'], '005930')
        self.assertEqual(first_item['company']['name_ko'], '삼성전자')
        self.assertEqual(first_item['company']['dart']['corp_code'], '00126380')
        self.assertTrue(first_item['quote_context']['available'])
        self.assertEqual(first_item['quote_context']['source'], 'kis')
        self.assertEqual(first_item['quote_context']['spread']['spread_points'], 100)
        self.assertEqual(first_item['quote_context']['spread']['spread_bps_of_price'], 13.9762)
        self.assertEqual(
            first_item['quote_context']['freshness']['trade_date_relation_to_request'],
            'same_market_date',
        )
        self.assertEqual(first_item['quote_context']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(first_item['quote_context']['freshness_provenance']['provenance_status'], 'provider_sourced')
        self.assertEqual(first_item['price_context']['open_price'], 70000)
        self.assertEqual(first_item['price_context']['liquidity']['turnover_source'], 'provider')
        self.assertEqual(first_item['price_context']['price_bar_quality']['issue_codes'], [])
        self.assertEqual(first_item['price_context']['gap']['previous_close_price'], 69800)
        self.assertEqual(first_item['price_context']['gap']['direction'], 'up')
        self.assertFalse(first_item['price_context']['gap']['filled_intraday'])
        self.assertTrue(first_item['price_context']['gap']['reliable'])
        self.assertEqual(first_item['price_context']['session_range']['close_position_label'], 'upper_half')
        self.assertEqual(first_item['price_context']['candle']['wick_bias'], 'upper')
        self.assertEqual(first_item['price_context']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(first_item['price_context']['freshness_provenance']['provenance_status'], 'derived_from_provider_data')
        self.assertEqual(len(first_item['price_context_history']), 2)
        self.assertEqual(first_item['price_context_history'][0]['market_date'], '2026-04-07')
        self.assertEqual(first_item['price_context_history'][1]['market_date'], '2026-04-08')
        self.assertEqual(first_item['price_context_history'][1]['price_context']['trade_date'], '2026-04-08')
        self.assertEqual(first_item['technical_snapshot']['trend_state'], 'uptrend')
        self.assertEqual(first_item['action_summary']['action_label'], 'long_breakout')
        self.assertEqual(first_item['signal_features']['signal_count'], 2)
        self.assertEqual(first_item['signal_features']['provenance']['row_kind'], 'direct_plus_fallback')
        self.assertEqual(first_item['signal_features']['fallback_features']['price_action']['close_return_1d_pct'], 2.43)
        self.assertEqual(first_item['signal_features']['freshness_provenance']['provenance_status'], 'provider_plus_fallback')
        self.assertEqual(first_item['signal_features']['freshness_provenance']['issue_types'], ['derived_or_fallback'])
        self.assertEqual(first_item['scores']['unified_value']['status'], 'available')
        self.assertEqual(first_item['scores']['unified_value']['score'], 0.84)
        self.assertEqual(first_item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(first_item['scores']['quality']['status'], 'partial')
        self.assertEqual(first_item['scores']['quality']['components_used'], 5)
        self.assertEqual(first_item['scores']['quality']['components_possible'], 6)
        self.assertIn('Missing quality components: accruals_ratio.', first_item['scores']['quality']['coverage_notes'])
        self.assertEqual(first_item['scores']['momentum']['status'], 'available')
        self.assertEqual(first_item['scores']['momentum']['score'], 0.91)
        self.assertEqual(first_item['scores']['momentum']['components_possible'], 3)
        self.assertEqual(first_item['persisted_factor_rows']['advanced_value']['advanced_value_classification'], 'eligible')
        self.assertEqual(
            first_item['persisted_factor_rows']['advanced_value']['freshness_provenance']['freshness_status'],
            'fresh',
        )
        self.assertEqual(first_item['persisted_factor_rows']['low_risk']['low_risk_classification'], 'watchlist')
        self.assertEqual(first_item['persisted_factor_rows']['beta']['low_beta_classification'], 'eligible')
        self.assertEqual(first_item['persisted_factor_rows']['composite']['composite_classification'], 'eligible')
        self.assertEqual(first_item['market_relative_context']['relative_strength']['return_percentile'], 0.95)
        self.assertEqual(first_item['context_completeness']['factor_rows']['quality']['row_status'], 'present')
        self.assertEqual(
            first_item['context_completeness']['factor_rows']['quality']['null_submetric_fields'],
            ['accruals_ratio'],
        )
        self.assertEqual(first_item['context_completeness']['signal_features']['row_status'], 'present')
        self.assertEqual(first_item['context_completeness']['signal_features']['row_kind'], 'direct_plus_fallback')
        self.assertEqual(first_item['context_completeness']['signal_features']['fallback_groups_present'], ['price_action', 'factor_scores'])
        self.assertEqual(first_item['context_completeness']['market_relative_context']['status'], 'available')
        self.assertEqual(first_item['context_completeness']['requested_symbol_coverage']['signal_features']['present_rows'], 1)
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['period']['period_end_date'], '2024-12-31')
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['statement']['row_count'], 1)
        self.assertEqual(first_item['fundamentals']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertEqual(first_item['recent_disclosures']['count'], 1)
        self.assertEqual(first_item['recent_disclosures']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertEqual(
            first_item['context_provenance']['signal_features']['requested_symbol_coverage']['missing_rows'],
            1,
        )
        self.assertTrue(first_item['availability']['quote_context'])
        self.assertTrue(first_item['availability']['price_context'])

        second_item = result['items'][1]
        self.assertEqual(second_item['symbol'], '000660')
        self.assertIsNone(second_item['quote_context'])
        self.assertIsNone(second_item['price_context'])
        self.assertEqual(second_item['price_context_history'], [])
        self.assertIsNone(second_item['technical_snapshot'])
        self.assertIsNone(second_item['signal_features'])
        self.assertIsNone(second_item['market_relative_context'])
        self.assertEqual(second_item['scores']['unified_value']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['quality']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['momentum']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'missing')
        self.assertEqual(second_item['context_completeness']['signal_features']['row_status'], 'missing')
        self.assertEqual(second_item['context_completeness']['signal_features']['row_kind'], 'missing')
        self.assertEqual(second_item['context_completeness']['market_relative_context']['status'], 'unavailable')
        self.assertEqual(second_item['context_completeness']['sector_peer_mapping']['status'], 'unavailable')
        self.assertIsNone(second_item['fundamentals']['latest_snapshot'])
        self.assertEqual(second_item['context_provenance']['signal_features']['freshness_status'], 'missing')
        self.assertEqual(second_item['recent_disclosures']['count'], 0)
        self.assertFalse(second_item['availability']['quote_context'])
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

    def test_get_symbol_day_context_marks_stale_and_available_but_old_blocks_without_silently_omitting_them(self):
        latest_prior_rows = {
            'factor_rows': {
                family: (
                    {
                        '005930': {
                            'market_date': '2026-04-07',
                            'captured_at': '2026-04-07T15:00:00+09:00',
                            'source': 'unified_value_v1',
                        }
                    }
                    if family == 'unified_value'
                    else {}
                )
                for family in PERSISTED_FACTOR_FAMILIES
            },
            'signal_features': {
                '005930': {
                    'market_date': '2026-04-07',
                    'captured_at': '2026-04-07T15:00:00+09:00',
                    'source': 'kis_ranking',
                }
            },
        }

        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(self, *, stock_codes, fs_div=None):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(self, *, stock_codes, receipt_date_to=None, limit_per_symbol=5):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )

        with (
            patch(
                'app.agent_context.read_service.load_symbols',
                return_value=[
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
                ],
            ),
            patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[]),
            patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[]),
            patch('app.agent_context.read_service.load_daily_signal_features', return_value=[]),
            patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_momentum_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[]),
            patch(
                'app.agent_context.read_service.load_current_quotes',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'kis',
                        'market_state': 'CLOSED',
                        'is_live': False,
                        'fallback': False,
                        'available': True,
                        'price': 70100,
                        'change': -200,
                        'change_pct': -0.28,
                        'volume': 99000,
                        'turnover': 6400000000,
                        'as_of': '153000',
                        'trade_date': '2026-04-07',
                        'cached_at': '2026-04-07T15:30:00+09:00',
                        'captured_at': '2026-04-07T15:30:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_price_context_history',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-07',
                        'source': 'pykrx',
                        'context_payload': {
                            'trade_date': '2026-04-07',
                            'source': 'pykrx',
                            'captured_at': '2026-04-07T15:30:00+09:00',
                            'close_price': 70100,
                        },
                        'captured_at': '2026-04-07T15:30:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service._load_latest_symbol_day_block_history',
                return_value=latest_prior_rows,
            ),
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        self.assertEqual(item['quote_context']['freshness_provenance']['freshness_status'], 'stale')
        self.assertEqual(item['quote_context']['freshness_provenance']['issue_types'], ['stale'])
        self.assertIsNone(item['price_context'])
        self.assertEqual(item['context_provenance']['price_context']['freshness_status'], 'stale')
        self.assertEqual(item['context_provenance']['price_context']['latest_available_market_date'], '2026-04-07')
        self.assertIsNone(item['signal_features'])
        self.assertEqual(item['context_provenance']['signal_features']['freshness_status'], 'stale')
        self.assertEqual(item['context_provenance']['signal_features']['latest_available_market_date'], '2026-04-07')
        self.assertIsNone(item['persisted_factor_rows']['unified_value'])
        self.assertEqual(item['context_provenance']['factor_rows']['unified_value']['freshness_status'], 'available_but_old')
        self.assertEqual(item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'available_but_old')

    @patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[])
    @patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_momentum_factor_results')
    @patch('app.agent_context.read_service.load_daily_quality_factor_results')
    @patch('app.agent_context.read_service.load_daily_unified_value_factor_results')
    @patch('app.agent_context.read_service.load_current_quotes', return_value=[])
    @patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[])
    @patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[])
    @patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[])
    @patch('app.agent_context.read_service.load_daily_signal_features')
    @patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[])
    @patch('app.agent_context.read_service.load_symbols')
    def test_get_symbol_day_context_reports_factor_complete_and_signal_features_sparse_for_requested_universe(
        self,
        load_symbols_mock,
        _load_technical_mock,
        load_signal_features_mock,
        _load_actions_mock,
        _load_price_contexts_mock,
        _load_price_context_history_mock,
        _load_current_quotes_mock,
        load_unified_value_mock,
        load_quality_mock,
        load_momentum_mock,
        *_unused,
    ):
        symbols = [f'{index:06d}' for index in range(1, 77)]
        load_symbols_mock.return_value = [
            {
                'id': index,
                'symbol': symbol,
                'standard_code': f'KR{index:010d}',
                'name_ko': symbol,
                'market': 'KOSPI',
                'instrument_type': 'equity',
                'source': 'pykrx',
                'is_active': True,
                'last_refreshed_at': None,
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_signal_features_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'kis_ranking',
                'signal_count': 1,
                'volume_power_rank': offset,
                'volume_power': 120.0 + offset,
                'overtime_volume_rank': None,
                'overtime_volume': None,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'feature_payload': {},
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for offset, symbol in enumerate(symbols[:2], start=1)
        ]
        load_unified_value_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'unified_value_v1',
                'feature_version': 'unified_value_v1',
                'unified_value_score': 0.8,
                'unified_value_rank': index,
                'unified_value_percentile': 1.0,
                'components_used': 5,
                'unified_value_status': 'scored',
                'per': 10.0,
                'pbr': 1.0,
                'dividend_yield': 2.0,
                'fcf_yield': 3.0,
                'ev_to_ebitda': 8.0,
                'per_status': 'available',
                'pbr_status': 'available',
                'dividend_yield_status': 'available',
                'fcf_yield_status': 'available',
                'ev_to_ebitda_status': 'available',
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_quality_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'quality_v1',
                'feature_version': 'quality_v1',
                'quality_score': 0.7,
                'quality_rank': index,
                'quality_percentile': 1.0,
                'components_used': 6,
                'components_possible': 6,
                'is_quality_score_complete': True,
                'quality_status': 'scored_complete',
                'roe': 0.12,
                'roa': 0.08,
                'gross_profit_to_assets': 0.11,
                'operating_cash_flow_to_net_income': 1.1,
                'debt_ratio': 0.4,
                'accruals_ratio': 0.02,
                'roe_status': 'available',
                'roa_status': 'available',
                'gross_profit_to_assets_status': 'available',
                'operating_cash_flow_to_net_income_status': 'available',
                'debt_ratio_status': 'available',
                'accruals_ratio_status': 'available',
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_momentum_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'daily_price_bars',
                'feature_version': 'price_momentum_v1',
                'momentum_percentile': 0.9,
                'momentum_rank': index,
                'momentum_12_1': 0.31,
                'return_5d': 0.04,
                'ma_60': 100.0,
                'ma_120': 90.0,
                'momentum_percentile_threshold': 0.8,
                'passes_momentum_top_percentile': True,
                'is_momentum_eligible': True,
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]

        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(self, *, stock_codes, fs_div=None):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(self, *, stock_codes, receipt_date_to=None, limit_per_symbol=5):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )
        with patch(
            'app.agent_context.read_service._load_latest_symbol_day_block_history',
            return_value=EMPTY_LATEST_PRIOR_ROWS,
        ):
            result = service.get_symbol_day_context(
                market_date='2026-05-06',
                symbols=symbols,
                fs_div='CFS',
                disclosures_limit=3,
            )

        coverage = result['summary']['context_completeness']
        self.assertEqual(coverage['factor_rows']['unified_value']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['quality']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['momentum']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['unified_value']['coverage_status'], 'complete')
        self.assertEqual(coverage['signal_features']['present_rows'], 2)
        self.assertEqual(coverage['signal_features']['missing_rows'], 74)
        self.assertEqual(coverage['signal_features']['coverage_status'], 'sparse')
        self.assertAlmostEqual(coverage['signal_features']['coverage_ratio'], 0.0263, places=4)
        self.assertEqual(
            result['items'][0]['context_completeness']['requested_symbol_coverage']['signal_features']['missing_rows'],
            74,
        )
        self.assertEqual(
            result['items'][0]['context_provenance']['signal_features']['requested_symbol_coverage']['present_rows'],
            2,
        )

    def test_get_symbol_day_context_includes_pykrx_enrichment_blocks(self):
        captured_at = '2026-04-08T15:00:00+09:00'

        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(self, *, stock_codes, fs_div=None):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(self, *, stock_codes, receipt_date_to=None, limit_per_symbol=5):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    'app.agent_context.read_service.load_symbols',
                    return_value=[
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
                    ],
                )
            )
            stack.enter_context(patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_signal_features', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_current_quotes', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_momentum_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.build_signal_decider_scores', return_value={}))
            stack.enter_context(
                patch(
                    'app.agent_context.read_service._load_latest_symbol_day_block_history',
                    return_value=EMPTY_LATEST_PRIOR_ROWS,
                )
            )
            stack.enter_context(
                patch(
                    'app.agent_context.read_service.pykrx_enrichment_service.load_symbol_day_context_inputs',
                    return_value={
                        'by_symbol': {
                            '005930': {
                                'sector_classification': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'name_ko': '삼성전자',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'sector_name': '반도체',
                                        'close_price': 71000,
                                        'market_cap': 420000000000,
                                        'change_pct': 1.2,
                                    },
                                },
                                'investor_flow_symbol': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'investor_groups': {
                                            '외국인': {'net_value': 1200000},
                                            '기관합계': {'net_value': -350000},
                                        }
                                    },
                                },
                                'short_selling': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'short_volume': {'short_volume': 1000, 'short_volume_ratio_pct': 0.35},
                                        'short_balance': {'short_balance_ratio_pct': 0.45},
                                    },
                                },
                                'valuation_snapshot': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'market_cap': 420000000000,
                                        'per': 11.1,
                                        'pbr': 1.3,
                                    },
                                },
                                'foreign_ownership': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'foreign_ownership_ratio_pct': 51.2,
                                        'foreign_limit_exhaustion_rate_pct': 73.4,
                                    },
                                },
                            }
                        },
                        'by_market': {
                            'KOSPI': {
                                'market_breadth': {
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'market_symbol_count': 900,
                                        'up_count': 500,
                                        'down_count': 300,
                                        'flat_count': 100,
                                        'advance_decline_ratio': 1.6667,
                                    },
                                },
                                'investor_flow_market': {
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'investor_groups': [
                                            {'investor_type': '외국인', 'net_value': 800000000},
                                            {'investor_type': '기관합계', 'net_value': -240000000},
                                        ]
                                    },
                                },
                            }
                        },
                        'sector_rows_by_market': {
                            'KOSPI': [
                                {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'name_ko': '삼성전자',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'sector_name': '반도체',
                                        'market_cap': 420000000000,
                                        'change_pct': 1.2,
                                    },
                                },
                                {
                                    'symbol': '000660',
                                    'market': 'KOSPI',
                                    'name_ko': 'SK하이닉스',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'sector_name': '반도체',
                                        'market_cap': 180000000000,
                                        'change_pct': 0.9,
                                    },
                                },
                            ]
                        },
                    },
                )
            )

            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        self.assertEqual(item['sector_peer_context']['sector_name'], '반도체')
        self.assertEqual(item['sector_peer_context']['peer_count'], 1)
        self.assertEqual(item['sector_peer_context']['peers'][0]['symbol'], '000660')
        self.assertEqual(item['market_breadth_context']['advance_decline_ratio'], 1.6667)
        self.assertEqual(item['investor_flow_context']['symbol_flow']['investor_groups']['외국인']['net_value'], 1200000)
        self.assertEqual(item['investor_flow_context']['market_flow']['investor_groups'][0]['investor_type'], '외국인')
        self.assertEqual(item['short_selling_context']['short_balance']['short_balance_ratio_pct'], 0.45)
        self.assertEqual(item['valuation_context']['valuation_snapshot']['per'], 11.1)
        self.assertEqual(item['valuation_context']['foreign_ownership']['foreign_ownership_ratio_pct'], 51.2)
        self.assertEqual(item['context_provenance']['sector_peer_context']['freshness_status'], 'fresh')
        self.assertEqual(item['context_provenance']['market_breadth_context']['provenance_status'], 'derived_from_provider_data')
        self.assertEqual(item['context_provenance']['investor_flow_context']['source_tables'], ['daily_pykrx_enrichment_rows'])
        self.assertEqual(item['context_completeness']['sector_peer_mapping']['status'], 'available')
        self.assertEqual(item['context_completeness']['requested_symbol_coverage']['valuation_context']['coverage_status'], 'complete')
        self.assertEqual(result['summary']['sector_peer_context_count'], 1)
        self.assertEqual(result['summary']['investor_flow_context_count'], 1)
        self.assertEqual(result['summary']['context_completeness']['market_breadth_context']['coverage_status'], 'complete')
