from datetime import date
import unittest

from app.agent_context.decider_coverage_report_service import DeciderCoverageReportService


def _decision_guidance(
    *,
    category: str,
    decision_tier: str,
    severity: str,
    default_missing_effect: str,
    blocking_by_default: bool,
    empty_is_neutral_by_default: bool = False,
) -> dict:
    return {
        'category': category,
        'decision_tier': decision_tier,
        'severity': severity,
        'default_missing_effect': default_missing_effect,
        'blocking_by_default': blocking_by_default,
        'empty_is_neutral_by_default': empty_is_neutral_by_default,
    }


def _base_symbol_day_context() -> dict:
    return {
        'quote_context': {'status': 'available'},
        'price_context': {'status': 'available'},
        'signal_features': {'status': 'available'},
        'fundamentals': {'status': 'available'},
        'recent_disclosures': {'status': 'available'},
        'recent_external_news': {'status': 'available'},
        'market_relative_context': {'status': 'available'},
        'sector_peer_context': {'status': 'available'},
        'market_breadth_context': {'status': 'available'},
        'investor_flow_context': {'status': 'available'},
        'short_selling_context': {'status': 'available'},
        'valuation_context': {'status': 'available'},
        'technical_snapshot': {'trend_state': 'uptrend'},
        'action_summary': {'support_price': 68000, 'resistance_price': 70800},
    }


class FakeDeciderCoverageReportRepository:
    def __init__(self, rows):
        self.rows = list(rows)
        self.list_args = None

    def list_feedback_rows(self, **kwargs):
        self.list_args = kwargs
        return list(self.rows)


class DeciderCoverageReportServiceTests(unittest.TestCase):
    def test_get_report_aggregates_daily_decider_coverage(self):
        signal_context = {
            'symbol_day_context': {
                **_base_symbol_day_context(),
                'intraday_execution_context': None,
            },
            'liquidity_context': {
                'turnover': {'status': 'available'},
                'ranking': {'status': 'available'},
                'intraday': {'status': 'available'},
            },
            'decider_data_quality_context': {
                'blocks': {
                    'liquidity_turnover': {
                        'status': 'partial',
                        'turnover': {
                            'source_status': 'derived',
                            'turnover_source': 'derived',
                        },
                        'decision_guidance': _decision_guidance(
                            category='liquidity_turnover_slippage',
                            decision_tier='core_execution',
                            severity='high',
                            default_missing_effect='core_blocker',
                            blocking_by_default=True,
                        ),
                    },
                    'intraday_realtime': {
                        'status': 'partial',
                        'decision_guidance': _decision_guidance(
                            category='after_hours_intraday_realtime',
                            decision_tier='core_execution',
                            severity='high',
                            default_missing_effect='core_blocker',
                            blocking_by_default=True,
                        ),
                    },
                    'factor_score_coverage': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='factor_score_coverage',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                    'financials_cashflow': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='financial_detail_quarter_cashflow',
                            decision_tier='auxiliary_context',
                            severity='low',
                            default_missing_effect='confidence_reducer',
                            blocking_by_default=False,
                        ),
                    },
                    'disclosures_news': {
                        'status': 'empty',
                        'decision_guidance': _decision_guidance(
                            category='disclosures_news_events',
                            decision_tier='auxiliary_context',
                            severity='low',
                            default_missing_effect='confidence_reducer',
                            blocking_by_default=False,
                            empty_is_neutral_by_default=True,
                        ),
                    },
                    'flows_short_selling': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='investor_flows_short_selling',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                    'sector_relative_strength': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='market_sector_peer_relative',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                    'technical_evidence': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='technical_evidence_support_resistance',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                }
            },
        }
        short_term_context = {
            'symbol_day_context': {
                **_base_symbol_day_context(),
                'intraday_execution_context': {'status': 'available'},
            },
            'liquidity_context': {
                'turnover': {'status': 'available'},
                'ranking': {'status': 'available'},
                'intraday': {'status': 'available'},
            },
            'decider_data_quality_context': {
                'blocks': {
                    'liquidity_turnover': {
                        'status': 'available',
                        'turnover': {
                            'source_status': 'available',
                            'turnover_source': 'provider',
                        },
                        'decision_guidance': _decision_guidance(
                            category='liquidity_turnover_slippage',
                            decision_tier='core_execution',
                            severity='high',
                            default_missing_effect='core_blocker',
                            blocking_by_default=True,
                        ),
                    },
                    'intraday_realtime': {
                        'status': 'stale',
                        'decision_guidance': _decision_guidance(
                            category='after_hours_intraday_realtime',
                            decision_tier='core_execution',
                            severity='high',
                            default_missing_effect='core_blocker',
                            blocking_by_default=True,
                        ),
                    },
                    'factor_score_coverage': {
                        'status': 'partial',
                        'decision_guidance': _decision_guidance(
                            category='factor_score_coverage',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                    'financials_cashflow': {
                        'status': 'unavailable',
                        'decision_guidance': _decision_guidance(
                            category='financial_detail_quarter_cashflow',
                            decision_tier='auxiliary_context',
                            severity='low',
                            default_missing_effect='confidence_reducer',
                            blocking_by_default=False,
                        ),
                    },
                    'disclosures_news': {
                        'status': 'provider_error',
                        'decision_guidance': _decision_guidance(
                            category='disclosures_news_events',
                            decision_tier='auxiliary_context',
                            severity='low',
                            default_missing_effect='confidence_reducer',
                            blocking_by_default=False,
                            empty_is_neutral_by_default=True,
                        ),
                    },
                    'flows_short_selling': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='investor_flows_short_selling',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                    'sector_relative_strength': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='market_sector_peer_relative',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                    'technical_evidence': {
                        'status': 'available',
                        'decision_guidance': _decision_guidance(
                            category='technical_evidence_support_resistance',
                            decision_tier='supporting_context',
                            severity='medium',
                            default_missing_effect='supporting_gap',
                            blocking_by_default=False,
                        ),
                    },
                }
            },
        }
        repository = FakeDeciderCoverageReportRepository(
            [
                {
                    'id': 1,
                    'market_date': date(2026, 5, 8),
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'run_id': 'symbol-run-2026-05-08-abc123',
                    'context_payload': signal_context,
                },
                {
                    'id': 2,
                    'market_date': date(2026, 5, 8),
                    'symbol': '000660',
                    'agent_name': 'short-term-decider',
                    'run_id': 'symbol-run-2026-05-08-abc123',
                    'context_payload': short_term_context,
                },
            ]
        )
        service = DeciderCoverageReportService(repository=repository)

        result = service.get_report(market_date='2026-05-08')

        self.assertEqual(repository.list_args['market_date'], date(2026, 5, 8))
        self.assertEqual(repository.list_args['agent_names'], ['short-term-decider', 'signal-decider'])
        summary = result['summary']
        self.assertEqual(summary['symbols_evaluated'], 2)
        self.assertEqual(summary['feedback_rows'], 2)
        self.assertEqual(summary['core_blocks']['counts']['available'], 1)
        self.assertEqual(summary['core_blocks']['counts']['derived'], 1)
        self.assertEqual(summary['core_blocks']['counts']['missing'], 1)
        self.assertEqual(summary['core_blocks']['counts']['stale'], 1)
        self.assertEqual(summary['optional_blocks']['counts']['available'], 8)
        self.assertEqual(summary['optional_blocks']['counts']['empty'], 1)
        self.assertEqual(summary['optional_blocks']['counts']['unavailable'], 2)
        self.assertEqual(summary['optional_blocks']['counts']['provider_error'], 1)
        self.assertEqual(summary['contract_regressions']['status'], 'alert')
        self.assertEqual(summary['contract_regressions']['raw_null_count'], 1)
        self.assertEqual(summary['contract_regressions']['unexpected_missing_core_block_count'], 1)
        self.assertEqual(
            summary['contract_regressions']['raw_null_paths'][0]['path'],
            'symbol_day_context.intraday_execution_context',
        )
        top_categories = {item['category']: item for item in summary['top_missing_categories']}
        self.assertEqual(top_categories['after_hours_intraday_realtime']['occurrences'], 2)
        self.assertEqual(top_categories['liquidity_turnover_slippage']['status_counts']['derived'], 1)
        self.assertEqual(top_categories['factor_score_coverage']['effect_counts']['confidence_reducer'], 1)
        self.assertEqual(top_categories['disclosures_news_events']['occurrences'], 1)
        self.assertEqual(
            summary['gap_effects']['decision_affected']['occurrences'],
            3,
        )
        self.assertEqual(
            summary['gap_effects']['confidence_reducer']['occurrences'],
            3,
        )
        self.assertEqual(
            summary['gap_effects']['neutral_auxiliary']['occurrences'],
            1,
        )
        self.assertEqual(result['per_agent']['signal-decider']['feedback_rows'], 1)
        self.assertEqual(result['per_agent']['signal-decider']['core_blocks']['counts']['derived'], 1)
        self.assertEqual(result['per_agent']['short-term-decider']['core_blocks']['counts']['stale'], 1)
        self.assertEqual(
            result['per_agent']['short-term-decider']['optional_blocks']['counts']['provider_error'],
            1,
        )

    def test_get_report_treats_closed_market_intraday_not_applicable_as_neutral(self):
        repository = FakeDeciderCoverageReportRepository(
            [
                {
                    'id': 1,
                    'market_date': date(2026, 5, 9),
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'run_id': 'symbol-run-2026-05-09-abc123',
                    'context_payload': {
                        'symbol_day_context': {
                            **_base_symbol_day_context(),
                            'intraday_execution_context': {'status': 'not_applicable'},
                        },
                        'liquidity_context': {
                            'turnover': {'status': 'available'},
                            'ranking': {'status': 'available'},
                            'intraday': {'status': 'not_applicable'},
                        },
                        'decider_data_quality_context': {
                            'blocks': {
                                'liquidity_turnover': {
                                    'status': 'available',
                                    'decision_guidance': _decision_guidance(
                                        category='liquidity_turnover_slippage',
                                        decision_tier='core_execution',
                                        severity='high',
                                        default_missing_effect='core_blocker',
                                        blocking_by_default=True,
                                    ),
                                },
                                'intraday_realtime': {
                                    'status': 'not_applicable',
                                    'decision_guidance': _decision_guidance(
                                        category='after_hours_intraday_realtime',
                                        decision_tier='core_execution',
                                        severity='high',
                                        default_missing_effect='core_blocker',
                                        blocking_by_default=True,
                                    ),
                                },
                            },
                        },
                    },
                }
            ]
        )
        service = DeciderCoverageReportService(repository=repository)

        result = service.get_report(market_date='2026-05-09', agent_names=['signal-decider'])

        summary = result['summary']
        self.assertEqual(summary['core_blocks']['counts']['available'], 2)
        self.assertEqual(summary['core_blocks']['counts']['missing'], 0)
        self.assertEqual(summary['core_blocks']['raw_status_counts']['not_applicable'], 1)
        self.assertEqual(summary['gap_effects']['decision_affected']['occurrences'], 0)
        self.assertEqual(summary['top_missing_categories'], [])

    def test_get_report_normalizes_filters_and_returns_empty_summary(self):
        repository = FakeDeciderCoverageReportRepository([])
        service = DeciderCoverageReportService(repository=repository)

        result = service.get_report(
            market_date='2026-05-08',
            run_id='  run-123  ',
            symbols=['005930,000660', '005930'],
            agent_names=['signal-decider', 'short-term-decider'],
        )

        self.assertEqual(repository.list_args['run_id'], 'run-123')
        self.assertEqual(repository.list_args['symbols'], ['005930', '000660'])
        self.assertEqual(repository.list_args['agent_names'], ['signal-decider', 'short-term-decider'])
        self.assertEqual(result['summary']['feedback_rows'], 0)
        self.assertEqual(result['summary']['symbols_evaluated'], 0)
        self.assertEqual(result['summary']['contract_regressions']['status'], 'pass')
        self.assertEqual(result['per_agent'], {})
