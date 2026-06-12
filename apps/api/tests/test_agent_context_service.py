from contextlib import ExitStack
from datetime import date
import unittest
from unittest.mock import patch

from sqlalchemy.exc import SQLAlchemyError

from app.agent_context.read_service import (
    AgentContextReadService,
    PERSISTED_FACTOR_FAMILIES,
    _build_compact_liquidity_context,
    _build_decider_data_quality_context,
    _build_decider_intraday_quality_context,
    _build_factor_summary_entry,
    _build_intraday_execution_context,
    _build_intraday_execution_freshness_provenance,
    _build_pre_decider_enrichment_context,
    _build_price_context,
    _build_quote_context,
    _build_quote_freshness_provenance,
    _build_recent_dart_events_context,
    _build_recent_disclosures_freshness_provenance,
    _load_latest_symbol_day_block_history,
)

EMPTY_LATEST_PRIOR_ROWS = {
    'factor_rows': {family: {} for family in PERSISTED_FACTOR_FAMILIES},
    'signal_features': {},
}


def _closed_market_session() -> dict[str, object]:
    return {
        'status': 'weekend',
        'current_market_state': 'WEEKEND',
        'current_calendar_date': '2026-04-11',
        'latest_valid_trading_date': '2026-04-10',
        'recommendation_basis': 'latest_completed_eod',
        'quote_data_expected': False,
        'intraday_data_expected': False,
    }


def _make_intraday_execution_row(
    *,
    symbol: str = '005930',
    availability_status: str = 'partial',
    as_of_market_date: str = '2026-04-08',
    as_of_timestamp: str = '2026-04-08T09:57:00+09:00',
    captured_at: str = '2026-04-08T09:57:05+09:00',
    market_state: str = 'OPEN',
) -> dict[str, object]:
    block_statuses = {
        'quote': 'available',
        'orderbook': 'available',
        'liquidity': 'available',
        'investor_program_activity': availability_status,
        'news_hints': 'available',
    }
    return {
        'symbol': symbol,
        'market_code': 'J',
        'source': 'kis_intraday_snapshot_v1',
        'feature_version': 'kis_intraday_snapshot_v1',
        'availability_status': availability_status,
        'freshness_status': 'fresh',
        'market_state': market_state,
        'as_of_market_date': as_of_market_date,
        'as_of_timestamp': as_of_timestamp,
        'captured_at': captured_at,
        'coverage': {
            'status': availability_status,
            'available_blocks': 4 if availability_status == 'partial' else 5,
            'partial_blocks': 1 if availability_status == 'partial' else 0,
            'total_blocks': 5,
            'block_statuses': block_statuses,
        },
        'availability': {
            'quote': True,
            'orderbook': True,
            'liquidity': True,
            'investor_program_activity': availability_status != 'unavailable',
            'news_hints': True,
        },
        'freshness': {
            'market_date': as_of_market_date,
            'status': 'fresh',
        },
        'quote': {
            'price': 71550,
            'spread_points': 100,
            'change_pct': 0.7,
        },
        'orderbook': {
            'imbalance': 0.076923,
            'ask_total_size': 14000,
            'bid_total_size': 16300,
        },
        'liquidity': {
            'volume_power': 155.2,
            'volume_power_rank': 3,
            'volume_rank': 9,
            'quote_balance_rank': 5,
        },
        'investor_program_activity': {
            'status': availability_status,
            'program_net_quantity': 12000,
            'foreign_net_quantity': 3100,
            'institution_net_quantity': -2800,
        },
        'news_hints': {
            'status': 'available',
            'item_count': 1,
            'event_tags': ['contract', 'earnings'],
            'items': [
                {
                    'title': '삼성전자 반도체 공급 계약 체결',
                    'published_at': '2026-04-08T08:40:00+09:00',
                    'event_tags': ['contract'],
                }
            ],
        },
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
                    },
                    {
                        'metric_key': 'operating_income',
                        'metric_type': 'account',
                        'statement_type': 'IS',
                        'statement_name': '손익계산서',
                        'account_id': 'dart_OperatingIncomeLoss',
                        'account_name': '영업이익',
                        'current_amount': 20,
                        'current_cumulative_amount': 20,
                        'prior_amount': 18,
                        'prior_cumulative_amount': 18,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'net_income',
                        'metric_type': 'account',
                        'statement_type': 'IS',
                        'statement_name': '손익계산서',
                        'account_id': 'ifrs-full_ProfitLoss',
                        'account_name': '당기순이익',
                        'current_amount': 15,
                        'current_cumulative_amount': 15,
                        'prior_amount': 14,
                        'prior_cumulative_amount': 14,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'total_assets',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_Assets',
                        'account_name': '자산총계',
                        'current_amount': 500,
                        'current_cumulative_amount': None,
                        'prior_amount': 470,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'current_assets',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_CurrentAssets',
                        'account_name': '유동자산',
                        'current_amount': 240,
                        'current_cumulative_amount': None,
                        'prior_amount': 220,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'cash_and_cash_equivalents',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_CashAndCashEquivalents',
                        'account_name': '현금및현금성자산',
                        'current_amount': 90,
                        'current_cumulative_amount': None,
                        'prior_amount': 75,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'total_liabilities',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_Liabilities',
                        'account_name': '부채총계',
                        'current_amount': 260,
                        'current_cumulative_amount': None,
                        'prior_amount': 250,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'current_liabilities',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_CurrentLiabilities',
                        'account_name': '유동부채',
                        'current_amount': 120,
                        'current_cumulative_amount': None,
                        'prior_amount': 115,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'total_equity',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_Equity',
                        'account_name': '자본총계',
                        'current_amount': 240,
                        'current_cumulative_amount': None,
                        'prior_amount': 220,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'operating_cash_flow',
                        'metric_type': 'account',
                        'statement_type': 'CF',
                        'statement_name': '현금흐름표',
                        'account_id': 'ifrs-full_CashFlowsFromUsedInOperatingActivities',
                        'account_name': '영업활동순현금흐름',
                        'current_amount': 24,
                        'current_cumulative_amount': 24,
                        'prior_amount': 20,
                        'prior_cumulative_amount': 20,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
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
                    'report_nm': '[기재정정] 사업보고서 (2025.12)',
                    'filing_category': 'annual_report',
                    'is_financial_report': True,
                    'receipt_date': '2026-04-07',
                    'remark': '정',
                    'fundamentals_capture_status': 'captured',
                },
                {
                    'rcept_no': '20260406000055',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'report_nm': '주식등의 대량보유상황보고서',
                    'filing_category': 'other',
                    'is_financial_report': False,
                    'receipt_date': '2026-04-06',
                    'remark': None,
                    'fundamentals_capture_status': 'not_applicable',
                }
            ],
            '000660': [],
        }


class DartEventContextTests(unittest.TestCase):
    def test_recent_dart_events_distinguishes_empty_from_storage_unavailable(self):
        empty = _build_recent_dart_events_context(
            disclosures=[],
            fundamentals={'latest_snapshot': None},
            storage_unavailable=False,
        )
        unavailable = _build_recent_dart_events_context(
            disclosures=[],
            fundamentals={'latest_snapshot': None},
            storage_unavailable=True,
        )
        provenance = _build_recent_disclosures_freshness_provenance(
            disclosures=[],
            requested_market_date=date(2026, 4, 8),
            disclosures_limit=3,
            storage_unavailable=True,
        )

        self.assertEqual(empty['status'], 'empty')
        self.assertEqual(empty['reason'], 'no_recent_disclosures')
        self.assertEqual(unavailable['status'], 'unavailable')
        self.assertEqual(unavailable['reason'], 'dart_disclosure_storage_unavailable')
        self.assertEqual(provenance['provenance_status'], 'storage_unavailable')
        self.assertEqual(provenance['reason'], 'dart_disclosure_storage_unavailable')


class AgentContextReadServiceTests(unittest.TestCase):
    @patch('app.agent_context.read_service.load_daily_market_relative_contexts')
    @patch('app.agent_context.read_service.load_daily_composite_factor_results')
    @patch('app.agent_context.read_service.load_daily_beta_factor_results')
    @patch('app.agent_context.read_service.load_daily_low_risk_factor_results')
    @patch('app.agent_context.read_service.load_daily_advanced_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_momentum_factor_results')
    @patch('app.agent_context.read_service.load_daily_quality_factor_results')
    @patch('app.agent_context.read_service.load_daily_unified_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_value_factor_results')
    @patch('app.agent_context.read_service.load_current_quotes')
    @patch('app.agent_context.read_service.load_current_kis_intraday_snapshots')
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
        load_current_kis_intraday_snapshots_mock,
        load_current_quotes_mock,
        load_value_mock,
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
        load_value_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'value_factor_v1',
                'feature_version': 'value_factor_v1',
                'per': 10.1,
                'pbr': 1.2,
                'dividend_yield': 2.1,
                'per_status': 'available',
                'per_rank': 15,
                'pbr_rank': 22,
                'dividend_yield_rank': 31,
                'per_percentile': 0.97,
                'pbr_percentile': 0.92,
                'dividend_yield_percentile': 0.88,
                'value_score': 0.923333,
                'value_rank': 9,
                'value_percentile': 0.95,
                'value_status': 'scored',
                'factor_payload': {
                    'availability': {
                        'valid_component_keys': ['per', 'pbr', 'dividend_yield'],
                        'missing_component_keys': [],
                    }
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
        load_current_kis_intraday_snapshots_mock.return_value = [
            _make_intraday_execution_row()
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

        self.assertEqual(result['market_session']['requested_date'], '2026-04-08')
        self.assertEqual(result['market_session']['latest_valid_trading_date'], '2026-04-08')
        self.assertEqual(result['summary']['market_session']['requested_date'], '2026-04-08')
        self.assertEqual(result['summary']['requested_symbol_count'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['summary']['quote_context_count'], 1)
        self.assertEqual(result['summary']['price_context_count'], 1)
        self.assertEqual(result['summary']['intraday_execution_context_count'], 1)
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
        self.assertEqual(first_item['market_session']['requested_date'], '2026-04-08')
        self.assertEqual(first_item['market_session']['latest_valid_trading_date'], '2026-04-08')
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
        self.assertEqual(first_item['liquidity_context']['status'], 'available')
        self.assertEqual(first_item['liquidity_context']['turnover']['turnover_krw'], 8800000000)
        self.assertEqual(first_item['liquidity_context']['turnover']['turnover_source'], 'provider')
        self.assertEqual(first_item['liquidity_context']['ranking']['liquidity_percentile'], 0.91)
        self.assertEqual(first_item['liquidity_context']['ranking']['liquidity_rank'], None)
        self.assertEqual(first_item['liquidity_context']['intraday']['status'], 'partial')
        self.assertEqual(first_item['liquidity_context']['intraday']['quote_spread_points'], 100)
        self.assertEqual(first_item['liquidity_context']['intraday']['quote_balance_rank'], 5)
        self.assertEqual(first_item['decider_data_quality_context']['status'], 'partial')
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['liquidity_turnover']['status'],
            'available',
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['liquidity_turnover']['turnover']['status'],
            'available',
        )
        self.assertEqual(first_item['decision_input_quality_context']['schema_version'], 'decision_input_quality_v1')
        self.assertEqual(
            first_item['decision_input_quality_context']['data_quality']['blocks']['liquidity_turnover']['decision_effect'],
            'confirmed',
        )
        self.assertIn(
            'technical_evidence_available',
            first_item['decision_input_quality_context']['final_judgment']['positive_confirmations'],
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['decision_priority']['core_execution']['status'],
            'partial',
        )
        self.assertIn(
            'intraday_realtime',
            first_item['decider_data_quality_context']['decision_priority']['core_execution']['degraded_block_names'],
        )
        self.assertFalse(first_item['decider_data_quality_context']['decision_priority']['auxiliary_context']['blocking_by_default'])
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['intraday_realtime']['status'],
            'partial',
        )
        intraday_quality = first_item['decider_data_quality_context']['blocks']['intraday_realtime']['intraday_snapshot']
        self.assertTrue(intraday_quality['quote_available'])
        self.assertTrue(intraday_quality['orderbook_available'])
        self.assertTrue(intraday_quality['liquidity_available'])
        self.assertTrue(intraday_quality['investor_program_activity_available'])
        self.assertTrue(intraday_quality['news_hints_available'])
        self.assertEqual(first_item['intraday_execution_context']['status'], 'partial')
        self.assertEqual(first_item['intraday_execution_context']['quote']['spread_points'], 100)
        self.assertAlmostEqual(first_item['intraday_execution_context']['orderbook']['imbalance'], 0.076923, places=6)
        self.assertEqual(first_item['intraday_execution_context']['liquidity']['volume_power_rank'], 3)
        self.assertEqual(first_item['intraday_execution_context']['news_hints']['event_tags'], ['contract', 'earnings'])
        self.assertEqual(first_item['intraday_execution_context']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(
            first_item['intraday_execution_context']['freshness_provenance']['trade_date_relation_to_request'],
            'same_market_date',
        )
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
        self.assertEqual(first_item['factor_summary']['factor_scores']['value']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['factor_scores']['value']['source_status'], 'scored')
        self.assertAlmostEqual(first_item['factor_summary']['factor_scores']['value']['score'], 0.923333)
        self.assertEqual(
            first_item['factor_summary']['factor_scores']['value']['provenance']['freshness_status'],
            'fresh',
        )
        self.assertEqual(first_item['scores']['quality']['status'], 'partial')
        self.assertEqual(first_item['scores']['quality']['components_used'], 5)
        self.assertEqual(first_item['scores']['quality']['components_possible'], 6)
        self.assertIn('Missing quality components: accruals_ratio.', first_item['scores']['quality']['coverage_notes'])
        self.assertEqual(first_item['scores']['momentum']['status'], 'available')
        self.assertEqual(first_item['scores']['momentum']['score'], 0.91)
        self.assertEqual(first_item['scores']['momentum']['components_possible'], 3)
        self.assertEqual(first_item['factor_summary']['factor_scores']['advanced_value']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['factor_scores']['low_risk']['status'], 'not_eligible')
        self.assertEqual(first_item['factor_summary']['factor_scores']['beta']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['factor_scores']['composite']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['signal_features']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['signal_features']['row_kind'], 'direct_plus_fallback')
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
        self.assertEqual(first_item['context_completeness']['intraday_execution_context']['status'], 'partial')
        self.assertEqual(first_item['context_completeness']['intraday_execution_context']['available_blocks'], 4)
        self.assertEqual(
            first_item['context_completeness']['intraday_execution_context']['block_statuses']['investor_program_activity'],
            'partial',
        )
        self.assertEqual(first_item['context_completeness']['market_relative_context']['status'], 'available')
        self.assertEqual(first_item['context_completeness']['requested_symbol_coverage']['signal_features']['present_rows'], 1)
        self.assertEqual(
            first_item['context_completeness']['requested_symbol_coverage']['intraday_execution_context']['coverage_status'],
            'partial',
        )
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['period']['period_end_date'], '2024-12-31')
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['statement']['row_count'], 1)
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['normalized_facts']['balance_sheet']['current_assets']['current_value'],
            240,
        )
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['analysis']['leverage_liquidity']['metrics']['current_ratio'],
            '2.0000',
        )
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['analysis']['cash_flow_quality']['metrics']['operating_cash_flow_to_net_income'],
            '1.6000',
        )
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['source_provenance']['provider'],
            'dart',
        )
        self.assertEqual(first_item['fundamentals']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertIn(
            'operating_cash_flow',
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['available_core_fields'],
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['report_period_type'],
            'annual',
        )
        self.assertTrue(
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['annual_only_context']
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['decision_guidance']['decision_tier'],
            'auxiliary_context',
        )
        self.assertEqual(first_item['recent_disclosures']['count'], 2)
        self.assertEqual(first_item['recent_disclosures']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertEqual(first_item['recent_disclosures']['items'][0]['is_correction'], True)
        self.assertEqual(first_item['recent_disclosures']['items'][1]['is_large_holder_related'], True)
        self.assertEqual(
            first_item['fundamentals']['disclosure_context']['summary']['correction_count'],
            1,
        )
        self.assertEqual(
            first_item['fundamentals']['disclosure_context']['summary']['ownership_related_count'],
            1,
        )
        self.assertEqual(
            first_item['fundamentals']['disclosure_context']['recent_ownership_large_holder_changes'][0]['rcept_no'],
            '20260406000055',
        )
        self.assertEqual(
            first_item['context_provenance']['signal_features']['requested_symbol_coverage']['missing_rows'],
            1,
        )
        self.assertEqual(first_item['context_provenance']['intraday_execution_context']['freshness_status'], 'fresh')
        self.assertEqual(
            first_item['context_provenance']['intraday_execution_context']['requested_symbol_coverage']['partial_rows'],
            1,
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['technical_evidence']['support_price'],
            68000.0,
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['technical_evidence']['resistance_price'],
            70800.0,
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['factor_score_coverage']['required_scores']['quality']['status'],
            'available',
        )
        self.assertTrue(first_item['availability']['quote_context'])
        self.assertTrue(first_item['availability']['price_context'])
        self.assertTrue(first_item['availability']['intraday_execution_context'])

        second_item = result['items'][1]
        self.assertEqual(second_item['symbol'], '000660')
        self.assertEqual(second_item['quote_context']['status'], 'unavailable')
        self.assertEqual(second_item['quote_context']['reason'], 'no_current_quote_cache_row')
        self.assertEqual(second_item['quote_context']['freshness']['status'], 'missing')
        self.assertEqual(second_item['quote_context']['spread']['status'], 'unavailable')
        self.assertIsNone(second_item['price_context'])
        self.assertEqual(second_item['intraday_execution_context']['status'], 'unavailable')
        self.assertEqual(
            second_item['intraday_execution_context']['reason'],
            'no_current_kis_intraday_snapshot_cache_row',
        )
        self.assertEqual(second_item['intraday_execution_context']['quote']['status'], 'unavailable')
        self.assertEqual(second_item['intraday_execution_context']['orderbook']['status'], 'unavailable')
        self.assertEqual(second_item['intraday_execution_context']['liquidity']['status'], 'unavailable')
        self.assertEqual(
            second_item['intraday_execution_context']['investor_program_activity']['status'],
            'unavailable',
        )
        self.assertEqual(second_item['intraday_execution_context']['news_hints']['status'], 'unavailable')
        self.assertEqual(second_item['price_context_history'], [])
        self.assertIsNone(second_item['technical_snapshot'])
        self.assertIsNone(second_item['signal_features'])
        self.assertEqual(second_item['market_relative_context']['status'], 'unavailable')
        self.assertEqual(second_item['sector_peer_context']['status'], 'unavailable')
        self.assertEqual(second_item['market_breadth_context']['status'], 'unavailable')
        self.assertEqual(second_item['investor_flow_context']['status'], 'unavailable')
        self.assertEqual(second_item['short_selling_context']['status'], 'unavailable')
        self.assertEqual(second_item['valuation_context']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['unified_value']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['quality']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['momentum']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'missing')
        self.assertEqual(second_item['factor_summary']['factor_scores']['value']['status'], 'missing_source_row')
        self.assertEqual(second_item['factor_summary']['factor_scores']['unified_value']['status'], 'missing_source_row')
        self.assertEqual(second_item['factor_summary']['signal_features']['status'], 'missing_source_row')
        self.assertEqual(second_item['context_completeness']['signal_features']['row_status'], 'missing')
        self.assertEqual(second_item['context_completeness']['signal_features']['row_kind'], 'missing')
        self.assertEqual(second_item['context_completeness']['intraday_execution_context']['status'], 'unavailable')
        self.assertEqual(second_item['context_completeness']['market_relative_context']['status'], 'unavailable')
        self.assertEqual(second_item['context_completeness']['sector_peer_mapping']['status'], 'unavailable')
        self.assertIsNone(second_item['fundamentals']['latest_snapshot'])
        self.assertEqual(second_item['context_provenance']['signal_features']['freshness_status'], 'missing')
        self.assertEqual(second_item['context_provenance']['intraday_execution_context']['freshness_status'], 'missing')
        self.assertEqual(second_item['context_provenance']['market_relative_context']['freshness_status'], 'missing')
        self.assertEqual(second_item['recent_disclosures']['count'], 0)
        self.assertEqual(
            second_item['decider_data_quality_context']['blocks']['intraday_realtime']['status'],
            'unavailable',
        )
        missing_intraday_quality = second_item['decider_data_quality_context']['blocks']['intraday_realtime']['intraday_snapshot']
        self.assertFalse(missing_intraday_quality['quote_available'])
        self.assertFalse(missing_intraday_quality['orderbook_available'])
        self.assertFalse(missing_intraday_quality['liquidity_available'])
        self.assertFalse(missing_intraday_quality['investor_program_activity_available'])
        self.assertFalse(missing_intraday_quality['news_hints_available'])
        self.assertEqual(
            second_item['decider_data_quality_context']['blocks']['factor_score_coverage']['status'],
            'unavailable',
        )
        self.assertFalse(second_item['availability']['quote_context'])
        self.assertFalse(second_item['availability']['price_context'])
        self.assertFalse(second_item['availability']['intraday_execution_context'])
        self.assertFalse(second_item['availability']['market_relative_context'])
        self.assertFalse(second_item['availability']['sector_peer_context'])
        self.assertFalse(second_item['availability']['market_breadth_context'])
        self.assertFalse(second_item['availability']['investor_flow_context'])
        self.assertFalse(second_item['availability']['short_selling_context'])
        self.assertFalse(second_item['availability']['valuation_context'])

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

    def test_build_compact_liquidity_context_marks_derived_turnover_from_close_and_volume(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 120000,
                'turnover': 0,
                'liquidity': {
                    'turnover_krw': 8520000000,
                    'turnover_source': 'estimated',
                    'turnover_is_estimated': True,
                    'provider_turnover_krw': None,
                    'estimated_turnover_krw': 8520000000,
                },
                'price_bar_quality': {
                    'issue_codes': ['turnover_missing_or_zero'],
                },
            },
            price_context_history=[],
            price_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_plus_fallback',
                'source_tables': ['daily_price_contexts', 'daily_price_bars'],
            },
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
        )

        self.assertEqual(context['status'], 'derived')
        self.assertEqual(context['turnover']['status'], 'derived')
        self.assertEqual(context['turnover']['turnover_krw'], 8520000000)
        self.assertEqual(context['turnover']['turnover_source'], 'derived')
        self.assertEqual(context['turnover']['trading_value_source'], 'estimated')
        self.assertEqual(context['turnover']['estimated_trading_value_krw'], 8520000000)
        self.assertEqual(context['turnover']['reason'], 'derived_from_close_price_x_volume')

    def test_build_compact_liquidity_context_prefers_raw_pykrx_trading_value(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 120000,
                'turnover': 0,
                'liquidity': {
                    'provider_turnover_krw': None,
                    'estimated_turnover_krw': 8520000000,
                },
                'price_bar_quality': {
                    'issue_codes': ['turnover_missing_or_zero'],
                },
            },
            price_context_history=[],
            price_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_plus_fallback',
                'source_tables': ['daily_price_contexts', 'daily_price_bars'],
            },
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
            valuation_context={
                'status': 'available',
                'valuation_snapshot': {
                    'trading_value': 9870000000,
                    'close_price': 71000,
                    'volume': 120000,
                },
            },
            valuation_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_sourced',
                'as_of_market_date': '2026-04-08',
                'latest_available_timestamp': '2026-04-08T15:30:00+09:00',
                'source_tables': ['daily_pykrx_enrichment_rows'],
            },
        )

        self.assertEqual(context['status'], 'available')
        self.assertEqual(context['turnover']['turnover_krw'], 9870000000)
        self.assertEqual(context['turnover']['trading_value_source'], 'raw')
        self.assertEqual(context['turnover']['raw_trading_value_krw'], 9870000000)
        self.assertIsNone(context['turnover']['derived_turnover_krw'])
        self.assertEqual(context['turnover']['trading_value_source_detail'], 'pykrx_valuation_snapshot.trading_value')

    def test_build_compact_liquidity_context_marks_missing_price_or_volume_when_turnover_not_derivable(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 0,
                'turnover': 0,
                'liquidity': {
                    'turnover_source': 'missing',
                    'turnover_is_estimated': False,
                    'provider_turnover_krw': None,
                    'estimated_turnover_krw': None,
                },
                'price_bar_quality': {
                    'issue_codes': ['turnover_missing_or_zero', 'volume_zero'],
                },
            },
            price_context_history=[],
            price_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'derived_from_provider_data',
                'source_tables': ['daily_price_contexts', 'daily_price_bars'],
            },
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
        )

        self.assertEqual(context['status'], 'missing_price_or_volume')
        self.assertEqual(context['turnover']['status'], 'missing_price_or_volume')
        self.assertEqual(context['turnover']['trading_value_source'], 'missing')
        self.assertEqual(context['turnover']['reason'], 'volume_missing_or_zero')

    def test_build_pre_decider_enrichment_marks_closed_market_quote_snapshot_stale(self):
        market_date = date.fromisoformat('2026-04-11')
        market_session = _closed_market_session()
        quote_context = _build_quote_context(None, market_date=market_date, market_session=market_session)
        intraday_context = _build_intraday_execution_context(None, market_session=market_session)

        context = _build_pre_decider_enrichment_context(
            requested_market_date=market_date,
            market_session=market_session,
            quote_context=quote_context,
            quote_freshness_provenance={'freshness_status': 'not_applicable'},
            intraday_execution_context=intraday_context,
            intraday_execution_freshness_provenance={'freshness_status': 'not_applicable'},
            price_context={
                'trade_date': '2026-04-10',
                'close_price': 71000,
                'captured_at': '2026-04-10T15:30:00+09:00',
            },
            liquidity_context={
                'turnover': {
                    'status': 'available',
                    'trading_value_source': 'raw',
                    'trading_value_krw': 8200000000,
                    'raw_trading_value_krw': 8200000000,
                    'trade_date': '2026-04-10',
                    'freshness_status': 'fresh',
                    'provenance': {'freshness_status': 'fresh'},
                }
            },
            investor_flow_context={'status': 'unavailable'},
            investor_flow_provenance={},
            short_selling_context={'status': 'unavailable'},
            short_selling_provenance={},
            sector_peer_context={'status': 'unavailable'},
            sector_peer_provenance={},
            market_breadth_context={'status': 'unavailable'},
            market_breadth_provenance={},
        )

        snapshot = context['quote_orderbook_snapshot']
        phase_contract = context['market_data_phase_contract']
        self.assertEqual(snapshot['status'], 'stale_eod')
        self.assertEqual(snapshot['snapshot_type'], 'eod_stale')
        self.assertEqual(snapshot['current_price'], 71000)
        self.assertEqual(snapshot['quote_freshness_timestamp'], '2026-04-10T15:30:00+09:00')
        self.assertEqual(snapshot['freshness']['quote_context']['freshness_status'], 'not_applicable')
        self.assertEqual(phase_contract['recommendation_basis'], 'latest_completed_eod')
        self.assertFalse(phase_contract['realtime_quote_orderbook_required'])
        self.assertTrue(phase_contract['eod_reference_price_accepted'])
        self.assertTrue(phase_contract['agent_guidance']['daily_process_agents_use_completed_eod'])

    def test_get_symbol_day_context_marks_liquidity_context_storage_unavailable_when_price_context_store_fails(self):
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
            patch('app.agent_context.read_service.load_daily_value_factor_results', return_value=[]),
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
                        'market_state': 'OPEN',
                        'is_live': True,
                        'fallback': False,
                        'available': True,
                        'price': 71550,
                        'change': 50,
                        'change_pct': 0.07,
                        'volume': 150000,
                        'turnover': 9000000000,
                        'trade_date': '2026-04-08',
                        'cached_at': '2026-04-08T09:57:00+09:00',
                        'captured_at': '2026-04-08T09:57:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_price_contexts',
                side_effect=SQLAlchemyError('daily price contexts unavailable'),
            ),
            patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[]),
            patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service._load_latest_symbol_day_block_history',
                return_value=EMPTY_LATEST_PRIOR_ROWS,
            ),
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        self.assertEqual(item['liquidity_context']['status'], 'storage_unavailable')
        self.assertEqual(item['liquidity_context']['turnover']['status'], 'storage_unavailable')
        self.assertEqual(item['liquidity_context']['turnover']['reason'], 'daily_price_context_storage_unavailable')
        self.assertEqual(item['context_provenance']['price_context']['reason'], 'daily_price_context_storage_unavailable')

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
                    else {
                        '005930': {
                            'market_date': '2026-04-07',
                            'captured_at': '2026-04-07T15:00:00+09:00',
                            'source': 'low_risk_v1',
                            'avg_trading_value_20d': 6400000000.0,
                            'liquidity_rank': 17,
                            'liquidity_percentile': 0.82,
                            'passes_liquidity_floor': True,
                        }
                    }
                    if family == 'low_risk'
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
            patch('app.agent_context.read_service.load_daily_value_factor_results', return_value=[]),
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
            patch(
                'app.agent_context.read_service.load_current_kis_intraday_snapshots',
                return_value=[
                    _make_intraday_execution_row(
                        availability_status='available',
                        as_of_market_date='2026-04-07',
                        as_of_timestamp='2026-04-07T15:28:00+09:00',
                        captured_at='2026-04-07T15:28:05+09:00',
                        market_state='CLOSED',
                    )
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
        self.assertEqual(item['liquidity_context']['status'], 'stale')
        self.assertEqual(item['liquidity_context']['turnover']['trade_date'], '2026-04-07')
        self.assertEqual(
            item['liquidity_context']['turnover']['reason'],
            'latest_price_context_before_requested_market_date',
        )
        self.assertEqual(item['liquidity_context']['ranking']['status'], 'stale')
        self.assertEqual(
            item['liquidity_context']['ranking']['reason'],
            'latest_prior_low_risk_liquidity_row_available',
        )
        self.assertEqual(item['liquidity_context']['ranking']['as_of_market_date'], '2026-04-07')
        self.assertEqual(item['liquidity_context']['ranking']['avg_trading_value_20d'], 6400000000.0)
        self.assertEqual(item['liquidity_context']['ranking']['liquidity_rank'], 17)
        self.assertEqual(item['liquidity_context']['ranking']['liquidity_percentile'], 0.82)
        self.assertTrue(item['liquidity_context']['ranking']['passes_liquidity_floor'])
        self.assertEqual(item['decider_data_quality_context']['blocks']['liquidity_turnover']['status'], 'stale')
        self.assertEqual(item['decider_data_quality_context']['blocks']['intraday_realtime']['status'], 'stale')
        self.assertEqual(item['decision_input_quality_context']['final_judgment']['decision_band'], 'block')
        self.assertIn(
            'liquidity_turnover',
            item['decision_input_quality_context']['final_judgment']['missing_context'],
        )
        self.assertIsNone(item['signal_features'])
        self.assertEqual(item['context_provenance']['signal_features']['freshness_status'], 'stale')
        self.assertEqual(item['context_provenance']['signal_features']['latest_available_market_date'], '2026-04-07')
        self.assertEqual(item['intraday_execution_context']['status'], 'available')
        self.assertEqual(item['context_provenance']['intraday_execution_context']['freshness_status'], 'stale')
        self.assertEqual(item['context_provenance']['intraday_execution_context']['latest_available_market_date'], '2026-04-07')
        self.assertIsNone(item['persisted_factor_rows']['unified_value'])
        self.assertEqual(item['context_provenance']['factor_rows']['unified_value']['freshness_status'], 'available_but_old')
        self.assertEqual(item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertEqual(item['factor_summary']['factor_scores']['value']['status'], 'missing_source_row')
        self.assertEqual(item['factor_summary']['factor_scores']['unified_value']['status'], 'not_computed_for_market_date')
        self.assertEqual(item['factor_summary']['signal_features']['status'], 'not_computed_for_market_date')

    def test_get_symbol_day_context_factor_summary_distinguishes_storage_missing_non_eligible_and_history_states(self):
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
        latest_prior_rows = {
            'factor_rows': {family: {} for family in PERSISTED_FACTOR_FAMILIES},
            'factor_storage_unavailable': {family: False for family in PERSISTED_FACTOR_FAMILIES},
            'signal_features': {},
            'signal_features_storage_unavailable': False,
        }

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
            patch(
                'app.agent_context.read_service.load_daily_value_factor_results',
                side_effect=SQLAlchemyError('value table unavailable'),
            ),
            patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_momentum_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'daily_price_bars',
                        'feature_version': 'price_momentum_v1',
                        'close_price': 71500,
                        'ma_60': 68500.0,
                        'ma_120': 70000.0,
                        'momentum_12_1': 0.25,
                        'return_5d': 0.012,
                        'has_sufficient_history': True,
                        'is_price_above_ma120': True,
                        'is_ma60_above_ma120': False,
                        'is_recent_overheated': False,
                        'momentum_rank': 64,
                        'momentum_percentile': 0.62,
                        'momentum_percentile_threshold': 0.8,
                        'passes_momentum_top_percentile': False,
                        'is_momentum_eligible': False,
                        'factor_payload': {
                            'eligibility': {
                                'passes_trend_confirmation': False,
                                'passes_momentum_top_percentile': False,
                                'momentum_eligibility_status': 'trend_not_confirmed',
                                'is_momentum_eligible': False,
                            },
                            'ranking': {'momentum_percentile_threshold': 0.8},
                            'overheat': {
                                'has_sufficient_cross_sectional_context': True,
                                'is_recent_overheated': False,
                            },
                        },
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_advanced_value_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'advanced_value_v1',
                        'feature_version': 'advanced_value_v1',
                        'advanced_value_score': 0.54,
                        'advanced_value_rank': 90,
                        'advanced_value_percentile': 0.43,
                        'advanced_value_components_used': 2,
                        'advanced_value_classification': 'watchlist',
                        'is_advanced_value_eligible': False,
                        'market_cap_proxy': 10.0,
                        'enterprise_value': 12.0,
                        'ebitda': 3.0,
                        'free_cash_flow': 2.0,
                        'fcf_yield': 0.05,
                        'ev_to_ebitda': 4.0,
                        'fcf_yield_status': 'available',
                        'ev_to_ebitda_status': 'available',
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_low_risk_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'low_risk_v1',
                        'feature_version': 'low_risk_v1',
                        'low_risk_score': None,
                        'low_risk_classification': 'insufficient_history',
                        'is_low_risk_eligible': False,
                        'volatility_60d': None,
                        'volatility_120d': None,
                        'max_drawdown_120d': None,
                        'avg_trading_value_20d': None,
                        'volatility_60d_percentile': None,
                        'volatility_120d_percentile': None,
                        'max_drawdown_120d_percentile': None,
                        'liquidity_percentile': None,
                        'passes_liquidity_floor': None,
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_beta_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'beta_v1',
                        'feature_version': 'beta_v1',
                        'low_beta_score': None,
                        'low_beta_classification': 'missing_market_date_bar',
                        'is_low_beta_eligible': False,
                        'benchmark_id': 'KOSPI_EQW',
                        'benchmark_market': 'KOSPI',
                        'beta_60d': None,
                        'beta_120d': None,
                        'beta_60d_percentile': None,
                        'beta_120d_percentile': None,
                        'history_days_available': 0,
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_composite_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'composite_v1',
                        'feature_version': 'composite_v1',
                        'composite_score': 0.31,
                        'composite_rank': 155,
                        'composite_percentile': 0.18,
                        'composite_classification': 'hard_excluded',
                        'is_composite_eligible': False,
                        'valid_factor_count': 3,
                        'available_base_weight': 0.65,
                        'has_required_inputs': True,
                        'is_hard_excluded': True,
                        'momentum_input': 0.62,
                        'value_input': 0.71,
                        'advanced_value_input': 0.54,
                        'low_risk_input': None,
                        'low_beta_input': None,
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_current_quotes', return_value=[]),
            patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[]),
            patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[]),
            patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[]),
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
        summary = item['factor_summary']['factor_scores']
        self.assertEqual(summary['value']['status'], 'storage_unavailable')
        self.assertEqual(summary['unified_value']['status'], 'missing_source_row')
        self.assertEqual(summary['momentum']['status'], 'not_eligible')
        self.assertEqual(summary['momentum']['source_status'], 'trend_not_confirmed')
        self.assertEqual(summary['advanced_value']['status'], 'not_eligible')
        self.assertEqual(summary['advanced_value']['source_status'], 'watchlist')
        self.assertEqual(summary['low_risk']['status'], 'insufficient_history')
        self.assertEqual(summary['beta']['status'], 'not_computed_for_market_date')
        self.assertEqual(summary['beta']['source_status'], 'missing_market_date_bar')
        self.assertEqual(summary['composite']['status'], 'not_eligible')
        self.assertEqual(item['liquidity_context']['ranking']['status'], 'missing')
        self.assertEqual(item['liquidity_context']['ranking']['reason'], 'low_risk_liquidity_fields_missing')
        self.assertEqual(item['context_completeness']['factor_rows']['value']['row_status'], 'storage_unavailable')
        self.assertEqual(item['factor_summary']['signal_features']['status'], 'missing_source_row')

    def test_build_decider_data_quality_context_marks_factor_score_provider_error_for_factor_storage_failure(self):
        result = _build_decider_data_quality_context(
            market_date=date(2026, 4, 8),
            scores={
                'unified_value': {
                    'status': 'unavailable',
                    'freshness_provenance': {'provenance_status': 'storage_unavailable'},
                },
                'quality': {'status': 'unavailable'},
                'momentum': {'status': 'unavailable'},
            },
            factor_summary={
                'signal_features': {'status': 'missing_source_row'},
            },
            liquidity_context={},
            quote_context=None,
            quote_context_provenance={},
            intraday_execution_context=None,
            intraday_execution_provenance={},
            intraday_execution_completeness={'status': 'unavailable'},
            fundamentals={},
            fundamentals_provenance={},
            recent_dart_events={},
            recent_disclosures_provenance={},
            recent_external_news={},
            recent_external_news_provenance={},
            investor_flow_context=None,
            investor_flow_provenance={},
            short_selling_context=None,
            short_selling_provenance={},
            market_relative_context=None,
            market_relative_provenance=None,
            sector_peer_context=None,
            sector_peer_provenance=None,
            market_breadth_context=None,
            market_breadth_provenance=None,
            technical_snapshot=None,
            action_summary=None,
            unified_value_row=None,
            advanced_value_row=None,
        )

        factor_score_block = result['blocks']['factor_score_coverage']
        self.assertEqual(result['status'], 'provider_error')
        self.assertEqual(factor_score_block['status'], 'provider_error')
        self.assertEqual(factor_score_block['required_scores']['unified_value']['status'], 'provider_error')
        self.assertEqual(result['block_status_counts']['provider_error'], 1)
        self.assertEqual(result['block_status_counts']['unavailable'], 7)

    def test_build_decider_intraday_quality_context_keeps_structured_unavailable_blocks_false(self):
        intraday_execution_context = _build_intraday_execution_context(None)

        result = _build_decider_intraday_quality_context(
            quote_context=None,
            quote_provenance={'freshness_status': 'missing', 'provenance_status': 'provider_sourced'},
            intraday_execution_context=intraday_execution_context,
            intraday_execution_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_current_kis_intraday_snapshot_cache_row',
            },
            intraday_execution_completeness=intraday_execution_context['coverage'],
        )

        intraday_snapshot = result['intraday_snapshot']
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(intraday_snapshot['status'], 'unavailable')
        self.assertFalse(intraday_snapshot['quote_available'])
        self.assertFalse(intraday_snapshot['orderbook_available'])
        self.assertFalse(intraday_snapshot['liquidity_available'])
        self.assertFalse(intraday_snapshot['investor_program_activity_available'])
        self.assertFalse(intraday_snapshot['news_hints_available'])

    def test_build_decider_intraday_quality_context_uses_status_fallback_when_block_availability_is_missing(self):
        intraday_execution_context = _build_intraday_execution_context(_make_intraday_execution_row())
        intraday_execution_context.pop('availability', None)
        intraday_execution_context['quote']['status'] = 'derived'

        result = _build_decider_intraday_quality_context(
            quote_context={'status': 'available', 'trade_date': '2026-04-08'},
            quote_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_context=intraday_execution_context,
            intraday_execution_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_completeness=intraday_execution_context['coverage'],
        )

        intraday_snapshot = result['intraday_snapshot']
        self.assertTrue(intraday_snapshot['quote_available'])
        self.assertTrue(intraday_snapshot['orderbook_available'])
        self.assertTrue(intraday_snapshot['liquidity_available'])
        self.assertTrue(intraday_snapshot['investor_program_activity_available'])
        self.assertTrue(intraday_snapshot['news_hints_available'])

    def test_closed_market_quote_and_intraday_contexts_are_not_applicable(self):
        market_session = _closed_market_session()

        quote_context = _build_quote_context(
            None,
            market_date=date(2026, 4, 11),
            market_session=market_session,
        )
        quote_provenance = _build_quote_freshness_provenance(
            row=None,
            requested_market_date=date(2026, 4, 11),
            market_session=market_session,
        )
        intraday_execution_context = _build_intraday_execution_context(
            None,
            market_session=market_session,
        )
        intraday_execution_provenance = _build_intraday_execution_freshness_provenance(
            row=None,
            requested_market_date=date(2026, 4, 11),
            market_session=market_session,
        )

        self.assertEqual(quote_context['status'], 'not_applicable')
        self.assertEqual(quote_context['reason'], 'closed_market')
        self.assertEqual(quote_context['market_session_status'], 'weekend')
        self.assertEqual(quote_context['latest_valid_trading_date'], '2026-04-10')
        self.assertFalse(quote_context['data_expected'])
        self.assertEqual(quote_provenance['freshness_status'], 'not_applicable')
        self.assertEqual(quote_provenance['provenance_status'], 'not_applicable')
        self.assertEqual(intraday_execution_context['status'], 'not_applicable')
        self.assertEqual(intraday_execution_context['coverage']['status'], 'not_applicable')
        self.assertEqual(
            intraday_execution_context['coverage']['block_statuses']['quote'],
            'not_applicable',
        )
        self.assertEqual(intraday_execution_provenance['freshness_status'], 'not_applicable')
        self.assertEqual(intraday_execution_provenance['provenance_status'], 'not_applicable')

    def test_build_decider_intraday_quality_context_marks_closed_market_as_not_applicable(self):
        market_session = _closed_market_session()
        quote_context = _build_quote_context(
            None,
            market_date=date(2026, 4, 11),
            market_session=market_session,
        )
        intraday_execution_context = _build_intraday_execution_context(
            None,
            market_session=market_session,
        )

        result = _build_decider_intraday_quality_context(
            quote_context=quote_context,
            quote_provenance=_build_quote_freshness_provenance(
                row=None,
                requested_market_date=date(2026, 4, 11),
                market_session=market_session,
            ),
            intraday_execution_context=intraday_execution_context,
            intraday_execution_provenance=_build_intraday_execution_freshness_provenance(
                row=None,
                requested_market_date=date(2026, 4, 11),
                market_session=market_session,
            ),
            intraday_execution_completeness=intraday_execution_context['coverage'],
        )

        self.assertEqual(result['status'], 'not_applicable')
        self.assertEqual(result['market_session_status'], 'weekend')
        self.assertEqual(result['recommendation_basis'], 'latest_completed_eod')
        self.assertEqual(result['latest_valid_trading_date'], '2026-04-10')
        self.assertEqual(result['quote']['status'], 'not_applicable')
        self.assertEqual(result['intraday_snapshot']['status'], 'not_applicable')
        self.assertFalse(result['intraday_snapshot']['quote_available'])
        self.assertFalse(result['intraday_snapshot']['orderbook_available'])
        self.assertFalse(result['intraday_snapshot']['liquidity_available'])
        self.assertFalse(result['intraday_snapshot']['investor_program_activity_available'])
        self.assertFalse(result['intraday_snapshot']['news_hints_available'])

    def test_build_decider_data_quality_context_marks_factor_score_provider_error_for_signal_feature_storage_failure(self):
        result = _build_decider_data_quality_context(
            market_date=date(2026, 4, 8),
            scores={
                'unified_value': {'status': 'unavailable'},
                'quality': {'status': 'unavailable'},
                'momentum': {'status': 'unavailable'},
            },
            factor_summary={
                'signal_features': {
                    'status': 'storage_unavailable',
                    'provenance': {'provenance_status': 'storage_unavailable'},
                }
            },
            liquidity_context={},
            quote_context=None,
            quote_context_provenance={},
            intraday_execution_context=None,
            intraday_execution_provenance={},
            intraday_execution_completeness={'status': 'unavailable'},
            fundamentals={},
            fundamentals_provenance={},
            recent_dart_events={},
            recent_disclosures_provenance={},
            recent_external_news={},
            recent_external_news_provenance={},
            investor_flow_context=None,
            investor_flow_provenance={},
            short_selling_context=None,
            short_selling_provenance={},
            market_relative_context=None,
            market_relative_provenance=None,
            sector_peer_context=None,
            sector_peer_provenance=None,
            market_breadth_context=None,
            market_breadth_provenance=None,
            technical_snapshot=None,
            action_summary=None,
            unified_value_row=None,
            advanced_value_row=None,
        )

        factor_score_block = result['blocks']['factor_score_coverage']
        self.assertEqual(result['status'], 'provider_error')
        self.assertEqual(factor_score_block['status'], 'provider_error')
        self.assertEqual(factor_score_block['signal_features']['status'], 'provider_error')
        self.assertEqual(factor_score_block['signal_features']['source_status'], 'storage_unavailable')

    def test_build_decider_data_quality_context_marks_empty_news_and_annual_financials_as_auxiliary(self):
        result = _build_decider_data_quality_context(
            market_date=date(2026, 4, 8),
            scores={
                'unified_value': {'status': 'available'},
                'quality': {'status': 'available'},
                'momentum': {'status': 'available'},
            },
            factor_summary={
                'signal_features': {'status': 'available'},
            },
            liquidity_context={
                'turnover': {'status': 'available', 'provenance': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'}},
                'ranking': {'status': 'available', 'provenance': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'}},
                'intraday': {
                    'status': 'available',
                    'provenance': {
                        'intraday_execution_context': {
                            'freshness_status': 'fresh',
                            'provenance_status': 'provider_sourced',
                        }
                    },
                },
            },
            quote_context={'trade_date': '2026-04-08'},
            quote_context_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_context={'status': 'available'},
            intraday_execution_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_completeness={'status': 'available'},
            fundamentals={
                'latest_snapshot': {
                    'filing': {'receipt_date': '2025-03-12', 'report_name': '사업보고서 (2024.12)'},
                    'period': {'period_end_date': '2024-12-31', 'business_year': 2024, 'report_code': '11011'},
                    'statement': {'source_status': 'available'},
                    'fundamental_context': {
                        'normalized_facts': {
                            'income_statement': {
                                'gross_profit': {'status': 'available'},
                            },
                            'balance_sheet': {
                                'current_assets': {'status': 'available'},
                                'current_liabilities': {'status': 'available'},
                                'cash_and_cash_equivalents': {'status': 'available'},
                            },
                            'cash_flow_statement': {
                                'operating_cash_flow': {'status': 'available'},
                                'investing_cash_flow': {'status': 'missing'},
                                'financing_cash_flow': {'status': 'missing'},
                            },
                        }
                    },
                }
            },
            fundamentals_provenance={'freshness_status': 'available_but_old', 'provenance_status': 'provider_sourced'},
            recent_dart_events={'status': 'empty', 'reason': 'no_recent_disclosures', 'summary': {}, 'item_count': 0},
            recent_disclosures_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_disclosures_up_to_request_date',
            },
            recent_external_news={
                'status': 'empty',
                'reason': 'no_recent_external_news_found',
                'item_count': 0,
                'provider': {'provider': 'naver_news_search', 'enabled': True, 'status': 'available', 'reason': None},
            },
            recent_external_news_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_external_news_found',
            },
            investor_flow_context={'status': 'available', 'symbol_flow': {}, 'market_flow': {}},
            investor_flow_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            short_selling_context={'status': 'available', 'short_volume': {}, 'short_balance': {}},
            short_selling_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            market_relative_context={'status': 'available', 'relative_strength': {}, 'breadth': {}},
            market_relative_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            sector_peer_context={'status': 'available', 'sector_name': 'Semiconductor', 'peer_count': 5, 'same_sector_symbol_count': 6},
            sector_peer_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            market_breadth_context={'status': 'available', 'market_symbol_count': 900, 'up_count': 500, 'down_count': 300, 'flat_count': 100},
            market_breadth_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            technical_snapshot={'trend_state': 'uptrend'},
            action_summary={'support_price': 68000, 'resistance_price': 70800, 'invalidation_price': 66500},
            unified_value_row=None,
            advanced_value_row=None,
        )

        self.assertEqual(result['decision_priority']['core_execution']['status'], 'available')
        self.assertFalse(result['decision_priority']['auxiliary_context']['blocking_by_default'])
        self.assertTrue(result['blocks']['disclosures_news']['empty_news_is_neutral'])
        self.assertTrue(
            result['blocks']['disclosures_news']['decision_guidance']['empty_is_neutral_by_default']
        )
        self.assertIn(
            'neutral unless separate risk evidence',
            result['blocks']['disclosures_news']['decision_guidance']['interpretation'],
        )
        self.assertEqual(result['blocks']['financials_cashflow']['report_period_type'], 'annual')
        self.assertTrue(result['blocks']['financials_cashflow']['annual_only_context'])
        self.assertEqual(
            result['blocks']['financials_cashflow']['decision_guidance']['default_missing_effect'],
            'confidence_reducer',
        )
        self.assertIn(
            'Annual fundamentals are still usable context',
            result['blocks']['financials_cashflow']['decision_guidance']['interpretation'],
        )
        self.assertEqual(result['block_status_counts']['provider_error'], 0)
        self.assertEqual(result['block_status_counts']['empty'], 1)

    def test_load_latest_symbol_day_block_history_only_requests_low_risk_liquidity_fields(self):
        with patch('app.agent_context.read_service._load_latest_prior_rows_for_model', return_value={}) as load_prior_rows_mock:
            _load_latest_symbol_day_block_history(
                market_date=date(2026, 4, 8),
                symbols=['005930'],
            )

        self.assertEqual(load_prior_rows_mock.call_count, len(PERSISTED_FACTOR_FAMILIES) + 1)
        extra_fields_by_model = {
            call.kwargs['model'].__name__: call.kwargs.get('extra_fields', ())
            for call in load_prior_rows_mock.call_args_list
        }
        self.assertEqual(
            extra_fields_by_model['DailyLowRiskFactorResult'],
            (
                'avg_trading_value_20d',
                'liquidity_rank',
                'liquidity_percentile',
                'passes_liquidity_floor',
            ),
        )
        for model_name, extra_fields in extra_fields_by_model.items():
            if model_name != 'DailyLowRiskFactorResult':
                self.assertEqual(extra_fields, ())

    def test_build_factor_summary_entry_marks_unscored_value_families_not_eligible(self):
        cases = (
            (
                'value',
                {
                    'market_date': '2026-04-08',
                    'source': 'value_factor_v1',
                    'feature_version': 'value_factor_v1',
                    'value_status': 'missing_metric',
                    'value_score': None,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
                'missing_metric',
            ),
            (
                'unified_value',
                {
                    'market_date': '2026-04-08',
                    'source': 'unified_value_v1',
                    'feature_version': 'unified_value_v1',
                    'unified_value_status': 'insufficient_components',
                    'unified_value_score': None,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
                'insufficient_components',
            ),
            (
                'quality',
                {
                    'market_date': '2026-04-08',
                    'source': 'quality_v1',
                    'feature_version': 'quality_v1',
                    'quality_status': 'insufficient_components',
                    'quality_score': None,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
                'insufficient_components',
            ),
        )

        for family, row, expected_source_status in cases:
            with self.subTest(family=family, source_status=expected_source_status):
                entry = _build_factor_summary_entry(
                    family=family,
                    row=row,
                    score_context={},
                    freshness_provenance={'freshness_status': 'fresh'},
                    current_storage_unavailable=False,
                    prior_storage_unavailable=False,
                )
                self.assertEqual(entry['status'], 'not_eligible')
                self.assertEqual(entry['source_status'], expected_source_status)
                self.assertFalse(entry['score_available'])

    def test_build_factor_summary_entry_preserves_available_for_scored_value_families(self):
        cases = (
            (
                'value',
                {
                    'market_date': '2026-04-08',
                    'source': 'value_factor_v1',
                    'feature_version': 'value_factor_v1',
                    'value_status': 'scored',
                    'value_score': 0.91,
                    'value_rank': 8,
                    'value_percentile': 0.92,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
            ),
            (
                'unified_value',
                {
                    'market_date': '2026-04-08',
                    'source': 'unified_value_v1',
                    'feature_version': 'unified_value_v1',
                    'unified_value_status': 'scored',
                    'unified_value_score': 0.84,
                    'unified_value_rank': 15,
                    'unified_value_percentile': 0.79,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
            ),
            (
                'quality',
                {
                    'market_date': '2026-04-08',
                    'source': 'quality_v1',
                    'feature_version': 'quality_v1',
                    'quality_status': 'scored_partial',
                    'quality_score': 0.67,
                    'quality_rank': 22,
                    'quality_percentile': 0.58,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
            ),
        )

        for family, row in cases:
            with self.subTest(family=family):
                entry = _build_factor_summary_entry(
                    family=family,
                    row=row,
                    score_context={},
                    freshness_provenance={'freshness_status': 'fresh'},
                    current_storage_unavailable=False,
                    prior_storage_unavailable=False,
                )
                self.assertEqual(entry['status'], 'available')
                self.assertTrue(entry['score_available'])

    @patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[])
    @patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_momentum_factor_results')
    @patch('app.agent_context.read_service.load_daily_quality_factor_results')
    @patch('app.agent_context.read_service.load_daily_unified_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_value_factor_results')
    @patch('app.agent_context.read_service.load_current_quotes', return_value=[])
    @patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[])
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
        _load_current_kis_intraday_snapshots_mock,
        _load_current_quotes_mock,
        load_value_mock,
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
        load_value_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'value_factor_v1',
                'feature_version': 'value_factor_v1',
                'per': 10.0,
                'pbr': 1.0,
                'dividend_yield': 2.0,
                'per_status': 'available',
                'per_rank': index,
                'pbr_rank': index,
                'dividend_yield_rank': index,
                'per_percentile': 1.0,
                'pbr_percentile': 1.0,
                'dividend_yield_percentile': 1.0,
                'value_score': 0.82,
                'value_rank': index,
                'value_percentile': 1.0,
                'value_status': 'scored',
                'factor_payload': {},
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
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
        self.assertEqual(coverage['factor_rows']['value']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['unified_value']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['quality']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['momentum']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['value']['coverage_status'], 'complete')
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
        self.assertEqual(
            result['items'][0]['context_completeness']['requested_symbol_coverage']['intraday_execution_context']['coverage_status'],
            'missing',
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
            stack.enter_context(patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_value_factor_results', return_value=[]))
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
                                        'trading_value': 9870000000,
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
        self.assertEqual(item['liquidity_context']['turnover']['trading_value_source'], 'raw')
        self.assertEqual(item['liquidity_context']['turnover']['turnover_krw'], 9870000000)
        self.assertEqual(item['pre_decider_enrichment_context']['trading_value']['source'], 'raw')
        self.assertEqual(
            item['pre_decider_enrichment_context']['flow_short_sector_context']['investor_flow']['symbol_flow'][
                'investor_groups'
            ]['외국인']['net_value'],
            1200000,
        )
        self.assertEqual(
            item['pre_decider_enrichment_context']['flow_short_sector_context']['short_selling']['short_balance'][
                'short_balance_ratio_pct'
            ],
            0.45,
        )
        self.assertEqual(
            item['pre_decider_enrichment_context']['flow_short_sector_context']['sector_peers']['peer_count'],
            1,
        )
        self.assertEqual(item['context_provenance']['sector_peer_context']['freshness_status'], 'fresh')
        self.assertEqual(item['context_provenance']['market_breadth_context']['provenance_status'], 'derived_from_provider_data')
        self.assertEqual(item['context_provenance']['investor_flow_context']['source_tables'], ['daily_pykrx_enrichment_rows'])
        self.assertEqual(item['context_completeness']['sector_peer_mapping']['status'], 'available')
        self.assertEqual(item['context_completeness']['requested_symbol_coverage']['valuation_context']['coverage_status'], 'complete')
        self.assertEqual(result['summary']['sector_peer_context_count'], 1)
        self.assertEqual(result['summary']['investor_flow_context_count'], 1)
        self.assertEqual(result['summary']['pre_decider_enrichment_context_count'], 1)
        self.assertEqual(result['summary']['context_completeness']['market_breadth_context']['coverage_status'], 'complete')
