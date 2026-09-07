
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_eod


class FakeFreshnessService:
    def read(self):
        return {
            'today': {
                'trade_date': '2026-04-10',
                'price': {
                    'status': 'ready',
                    'symbols_total': 3,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                },
                'technical_analysis': {
                    'status': 'ready',
                    'symbols_total': 3,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                },
                'action_summary': {
                    'status': 'ready',
                    'symbols_total': 3,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                },
                'fundamentals': {
                    'status': 'ready',
                    'basis': 'same_day_snapshot_updates',
                    'symbols_total': 1,
                    'symbols_succeeded': 1,
                    'symbols_failed': 0,
                    'current_run': {
                        'basis': 'market_date_refresh_run',
                        'market_date': '2026-04-10',
                        'status': 'completed',
                        'fs_div': 'CFS',
                        'trigger': 'eod_pykrx_followup',
                        'updated_at': '2026-04-10T07:31:55+00:00',
                        'completed_at': '2026-04-10T07:31:55+00:00',
                        'symbols_total': 1,
                        'symbols_checked': 1,
                        'symbols_succeeded': 1,
                        'symbols_fresh': 0,
                        'symbols_refreshed': 1,
                        'symbols_failed': 0,
                        'symbols_remaining': 0,
                        'events_detected': 1,
                        'events_captured': 1,
                        'events_failed': 0,
                        'error_message': None,
                    },
                },
            },
            'latest': {
                'trade_date': '2026-04-10',
                'price_trade_date': '2026-04-10',
                'eod_status': 'completed',
                'updated_at': '2026-04-10T07:31:55+00:00',
                'succeeded': 3,
                'failed': 0,
                'total': 3,
                'summary': {
                    'basis': 'latest_eod_run_symbol_universe',
                    'updated_at': '2026-04-10T07:31:55+00:00',
                    'succeeded': 3,
                    'failed': 0,
                    'total': 3,
                },
                'symbols_total': 3,
                'symbols_succeeded': 3,
                'symbols_failed': 0,
                'technical_market_date': '2026-04-10',
                'action_market_date': '2026-04-10',
                'fundamentals': {
                    'basis': 'latest_available_refresh_run',
                    'market_date': '2026-04-10',
                    'status': 'completed',
                    'fs_div': 'CFS',
                    'trigger': 'eod_pykrx_followup',
                    'updated_at': '2026-04-10T07:31:55+00:00',
                    'completed_at': '2026-04-10T07:31:55+00:00',
                    'symbols_total': 1,
                    'symbols_checked': 1,
                    'symbols_succeeded': 1,
                    'symbols_fresh': 0,
                    'symbols_refreshed': 1,
                    'symbols_failed': 0,
                    'symbols_remaining': 0,
                    'events_detected': 1,
                    'events_captured': 1,
                    'events_failed': 0,
                    'error_message': None,
                },
            },
        }


class FakeScreeningResultReadService:
    def __init__(self):
        self.read_args = None

    def read(self, **kwargs):
        self.read_args = kwargs
        return {
            'family': kwargs['family'],
            'detail_route': '/eod/composite-factors',
            'read_mode': 'screening_compact',
            'requested_market_date': kwargs.get('market_date'),
            'resolved_market_date': kwargs.get('market_date') or '2026-04-24',
            'market_date': kwargs.get('market_date') or '2026-04-24',
            'latest_mode': kwargs.get('market_date') is None,
            'is_latest_fallback': False,
            'feature_version': 'composite_v1',
            'summary': {
                'entries': 2,
                'eligible_count': 1,
                'watchlist_count': 1,
            },
            'pagination': {
                'limit': kwargs['limit'],
                'offset': kwargs['offset'],
                'returned': 2,
                'total': 2,
                'has_more': False,
            },
            'items': [
                {
                    'symbol': '005930',
                    'name_ko': '삼성전자',
                    'market': 'KOSPI',
                    'market_date': kwargs.get('market_date') or '2026-04-24',
                    'source': 'composite_v1',
                    'composite_score': 0.91,
                    'composite_classification': 'eligible',
                },
                {
                    'symbol': '000660',
                    'name_ko': 'SK하이닉스',
                    'market': 'KOSPI',
                    'market_date': kwargs.get('market_date') or '2026-04-24',
                    'source': 'composite_v1',
                    'composite_score': 0.58,
                    'composite_classification': 'watchlist',
                },
            ],
        }


class FakeTechnicalSnapshotService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'snapshots_persisted': 2,
            'snapshots_failed': 0,
            'items': [{'symbol': '005930'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'trend_state': 'uptrend'}],
        }


class FakeActionSummaryService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'action_summaries_persisted': 2,
            'action_summaries_failed': 0,
            'items': [{'symbol': '005930', 'action_label': 'long_breakout'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'action_label': 'long_breakout'}],
        }


class FakePykrxEnrichmentService:
    def __init__(self):
        self.collect_args = None
        self.inspect_args = None

    def collect_for_market_date(self, **kwargs):
        self.collect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'status': 'completed',
            'categories_requested': kwargs.get('categories') or ['sector_classification'],
            'coverage': {
                'categories': {
                    'sector_classification': {
                        'present_rows': 1,
                        'expected_rows': 1,
                        'coverage_status': 'complete',
                    }
                }
            },
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'rows_returned': 1},
            'coverage': {
                'categories': {
                    'investor_flow_symbol': {
                        'present_rows': 1,
                        'expected_rows': 1,
                        'coverage_status': 'complete',
                    }
                }
            },
            'items': [
                {
                    'symbol': '005930',
                    'category': 'investor_flow_symbol',
                    'payload': {'investor_groups': {'외국인': {'net_value': 1200000}}},
                }
            ],
        }


class FakeSignalFeatureService:
    def __init__(self):
        self.collect_args = None
        self.inspect_args = None

    async def refresh_for_market_date(self, **kwargs):
        self.collect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'features_persisted': 2,
            'features_failed': 0,
            'items': [{'symbol': '005930', 'signal_count': 2}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'signal_count': 2}],
        }


class FakeMarketRiskSnapshotService:
    def __init__(self):
        self.collect_args = None
        self.inspect_args = None

    async def collect_for_market_date(self, **kwargs):
        self.collect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'snapshots_persisted': 2,
            'snapshots_failed': 0,
            'items': [{'symbol': '005930', 'is_trade_halted': True, 'market_warning_code': '01'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs.get('market_date') or '2026-04-14'
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'requested_market_date': market_date,
                'resolved_market_date': market_date,
                'feature_version': 'kis_quote2_snapshot_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'management_item_count': 0,
                    'warning_count': 1,
                    'trade_halted_count': 1,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'is_trade_halted': True,
                    'market_warning_code': '01',
                },
            }
        return {
            'market_date': '2026-04-14',
            'requested_market_date': None,
            'resolved_market_date': '2026-04-14',
            'latest_mode': True,
            'feature_version': 'kis_quote2_snapshot_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'management_item_count': 1,
                'warning_count': 1,
                'trade_halted_count': 1,
            },
            'items': [
                {'symbol': '000660', 'market_date': '2026-04-14', 'is_management_item': True},
                {'symbol': '005930', 'market_date': '2026-04-14', 'is_trade_halted': True},
            ],
        }


class FakeMarketRiskFilterService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    async def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'snapshot_collection': {'feature_version': 'kis_quote2_snapshot_v1', 'snapshots_persisted': 2},
            'items': [{'symbol': '005930', 'risk_state': 'watchlist', 'watchlist_flagged': True}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs.get('market_date') or '2026-04-14'
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'requested_market_date': market_date,
                'resolved_market_date': market_date,
                'feature_version': 'kis_market_risk_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'passed_count': 1,
                    'watchlist_count': 1,
                    'failed_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'risk_state': 'watchlist',
                    'passed_filter': True,
                    'watchlist_flagged': True,
                },
            }
        return {
            'market_date': '2026-04-14',
            'requested_market_date': None,
            'resolved_market_date': '2026-04-14',
            'latest_mode': True,
            'feature_version': 'kis_market_risk_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'passed_count': 1,
                'watchlist_count': 1,
                'failed_count': 1,
            },
            'items': [
                {'symbol': '000660', 'market_date': '2026-04-14', 'risk_state': 'fail', 'passed_filter': False},
                {'symbol': '005930', 'market_date': '2026-04-14', 'risk_state': 'watchlist', 'passed_filter': True},
            ],
        }


class FakeDailyPipelineRunStateService:
    def start_run(self, **kwargs):
        return {'id': 7001, **kwargs, 'status': 'running', 'stages': {}}

    def start_stage(self, **kwargs):
        return {'id': 1, **kwargs, 'status': 'running'}

    def update_stage(self, **kwargs):
        return {'id': 1, **kwargs}

    def finish_stage(self, **kwargs):
        return {'id': 1, **kwargs}

    def finish_run(self, **kwargs):
        return {'id': kwargs['pipeline_run_id'], **kwargs}


class FakeMomentumFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [
                {
                    'symbol': '005930',
                    'momentum_rank': 1,
                    'momentum_percentile_threshold': 0.8,
                    'passes_momentum_top_percentile': True,
                }
            ],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1, 'eligible_symbols': 1},
            'items': [
                {
                    'symbol': '005930',
                    'momentum_rank': 1,
                    'is_momentum_eligible': True,
                    'momentum_percentile_threshold': 0.8,
                    'passes_momentum_top_percentile': True,
                    'momentum_eligibility_status': 'eligible',
                }
            ],
        }


class FakeValueFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'value_score': 0.86}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'value_factor_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'scored_count': 1,
                    'invalid_per_count': 0,
                    'missing_metric_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'value_score': 0.91,
                    'value_status': 'scored',
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'value_factor_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'scored_count': 1,
                'invalid_per_count': 1,
                'missing_metric_count': 0,
            },
            'items': [
                {'symbol': '005930', 'market_date': market_date, 'value_score': 0.91, 'value_status': 'scored'},
                {'symbol': '000660', 'market_date': market_date, 'value_score': None, 'value_status': 'invalid_per'},
            ],
        }


class FakeAdvancedValueFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'advanced_value_score': 0.84}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'advanced_value_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'eligible_count': 1,
                    'watchlist_count': 0,
                    'ineligible_count': 0,
                    'insufficient_components_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'advanced_value_score': 0.91,
                    'advanced_value_classification': 'eligible',
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'advanced_value_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'eligible_count': 1,
                'watchlist_count': 1,
                'ineligible_count': 0,
                'insufficient_components_count': 0,
            },
            'items': [
                {
                    'symbol': '005930',
                    'market_date': market_date,
                    'advanced_value_score': 0.91,
                    'advanced_value_classification': 'eligible',
                },
                {
                    'symbol': '000660',
                    'market_date': market_date,
                    'advanced_value_score': 0.50,
                    'advanced_value_classification': 'watchlist',
                },
            ],
        }


class FakeUnifiedValueFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'unified_value_score': 0.87}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'unified_value_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'scored_count': 1,
                    'invalid_per_count': 0,
                    'missing_metric_count': 0,
                    'insufficient_components_count': 0,
                    'insufficient_fundamentals_count': 0,
                    'missing_market_date_bar_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'unified_value_score': 0.93,
                    'unified_value_status': 'scored',
                    'components_used': 5,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'unified_value_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'scored_count': 1,
                'invalid_per_count': 1,
                'missing_metric_count': 0,
                'insufficient_components_count': 0,
                'insufficient_fundamentals_count': 0,
                'missing_market_date_bar_count': 0,
            },
            'items': [
                {
                    'symbol': '005930',
                    'market_date': market_date,
                    'unified_value_score': 0.93,
                    'unified_value_status': 'scored',
                    'components_used': 5,
                },
                {
                    'symbol': '000660',
                    'market_date': market_date,
                    'unified_value_score': None,
                    'unified_value_status': 'invalid_per',
                    'components_used': 4,
                },
            ],
        }


class FakeQualityFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'quality_score': 0.87}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'quality_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'scored_complete_count': 0,
                    'scored_partial_count': 1,
                    'invalid_assets_count': 0,
                    'invalid_equity_count': 0,
                    'invalid_net_income_count': 0,
                    'missing_metric_count': 0,
                    'insufficient_components_count': 0,
                    'insufficient_fundamentals_count': 0,
                    'missing_market_date_bar_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'quality_score': 0.93,
                    'quality_status': 'scored_partial',
                    'components_possible': 6,
                    'components_used': 4,
                    'is_quality_score_complete': False,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'quality_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'scored_complete_count': 1,
                'scored_partial_count': 1,
                'invalid_assets_count': 0,
                'invalid_equity_count': 0,
                'invalid_net_income_count': 0,
                'missing_metric_count': 0,
                'insufficient_components_count': 0,
                'insufficient_fundamentals_count': 0,
                'missing_market_date_bar_count': 0,
            },
            'items': [
                {
                    'symbol': '005930',
                    'market_date': market_date,
                    'quality_score': 0.93,
                    'quality_status': 'scored_complete',
                    'components_possible': 6,
                    'components_used': 6,
                    'is_quality_score_complete': True,
                },
                {
                    'symbol': '000660',
                    'market_date': market_date,
                    'quality_score': 0.58,
                    'quality_status': 'scored_partial',
                    'components_possible': 6,
                    'components_used': 4,
                    'is_quality_score_complete': False,
                },
            ],
        }


class FakeCompositeFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'composite_score': 0.88}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'composite_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'scored_count': 1,
                    'eligible_count': 1,
                    'watchlist_count': 0,
                    'ineligible_count': 0,
                    'hard_excluded_count': 0,
                    'insufficient_inputs_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'composite_score': 0.91,
                    'composite_classification': 'eligible',
                    'is_composite_eligible': True,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'composite_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'scored_count': 2,
                'eligible_count': 1,
                'watchlist_count': 1,
                'ineligible_count': 0,
                'hard_excluded_count': 0,
                'insufficient_inputs_count': 0,
            },
            'items': [
                {
                    'symbol': '005930',
                    'market_date': market_date,
                    'composite_score': 0.91,
                    'composite_classification': 'eligible',
                    'is_composite_eligible': True,
                },
                {
                    'symbol': '000660',
                    'market_date': market_date,
                    'composite_score': 0.58,
                    'composite_classification': 'watchlist',
                    'is_composite_eligible': False,
                },
            ],
        }


class FakePiotroskiFilterService:
    def __init__(self):
        self.inspect_args = None

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        resolved_market_date = market_date or '2026-04-23'
        latest_mode = market_date is None
        is_latest_fallback = market_date == '2026-04-24'
        if is_latest_fallback:
            resolved_market_date = '2026-04-23'
        if symbol:
            return {
                'requested_market_date': market_date,
                'resolved_market_date': resolved_market_date,
                'market_date': resolved_market_date,
                'latest_mode': latest_mode,
                'is_latest_fallback': is_latest_fallback,
                'fallback_policy': 'latest_on_or_before_requested_market_date',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'passed_count': 1,
                    'not_passed_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': resolved_market_date,
                    'passed_filter': True,
                    'piotroski_score': 8,
                },
            }
        return {
            'requested_market_date': market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': latest_mode,
            'is_latest_fallback': is_latest_fallback,
            'fallback_policy': 'latest_on_or_before_requested_market_date',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'passed_count': 1,
                'not_passed_count': 1,
            },
            'items': [
                {'symbol': '005930', 'market_date': resolved_market_date, 'passed_filter': True, 'piotroski_score': 8},
                {'symbol': '000660', 'market_date': resolved_market_date, 'passed_filter': False, 'piotroski_score': 5},
            ],
        }


class FakeLowRiskFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'low_risk_score': 0.86}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'low_risk_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'eligible_count': 1,
                    'liquidity_floor_pass_count': 1,
                    'insufficient_history_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'low_risk_score': 0.91,
                    'is_low_risk_eligible': True,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'low_risk_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'eligible_count': 1,
                'liquidity_floor_pass_count': 2,
                'insufficient_history_count': 0,
            },
            'items': [
                {'symbol': '005930', 'market_date': market_date, 'low_risk_score': 0.91, 'is_low_risk_eligible': True},
                {'symbol': '000660', 'market_date': market_date, 'low_risk_score': 0.42, 'is_low_risk_eligible': False},
            ],
        }


class FakeBetaFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'low_beta_score': 0.84}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'beta_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'eligible_count': 1,
                    'watchlist_count': 0,
                    'insufficient_history_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'benchmark_id': 'KOSPI_EQW',
                    'low_beta_score': 0.91,
                    'is_low_beta_eligible': True,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'beta_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'eligible_count': 1,
                'watchlist_count': 1,
                'insufficient_history_count': 0,
            },
            'items': [
                {'symbol': '005930', 'market_date': market_date, 'low_beta_score': 0.91, 'is_low_beta_eligible': True},
                {'symbol': '000660', 'market_date': market_date, 'low_beta_score': 0.55, 'is_low_beta_eligible': False},
            ],
        }


class FakePipelineStatusService:
    def __init__(self):
        self.read_args = None

    def read(self, **kwargs):
        self.read_args = kwargs
        return {
            'market_date': kwargs.get('market_date') or '2026-04-21',
            'fs_div': kwargs.get('fs_div') or 'CFS',
            'status': 'running',
            'stage_order': ['price_collection', 'pykrx_enrichment', 'technical_snapshots', 'fundamentals_daily_refresh'],
            'stages': {
                'price_collection': {
                    'status': 'completed',
                    'source_run_ids': {'eod_run_id': 101},
                },
                'pykrx_enrichment': {
                    'status': 'partial',
                    'details': {
                        'categories': {
                            'sector_classification': {'coverage_status': 'complete'}
                        }
                    },
                },
                'technical_snapshots': {
                    'status': 'running',
                    'current_activity': 'Computing daily technical snapshots for 000660',
                },
                'fundamentals_daily_refresh': {
                    'status': 'pending',
                    'source_run_ids': {},
                },
            },
        }


class FakeDailyPipelineStageService:
    def __init__(self):
        self.describe_called = False
        self.run_args = None

    def describe(self):
        self.describe_called = True
        return {
            'stage_order': ['price_collection', 'technical_snapshots', 'signal_decider'],
            'default_daily_stage_order': ['price_collection', 'technical_snapshots', 'signal_decider'],
            'stages': [
                {
                    'id': 'price_collection',
                    'label': 'pykrx daily price collection',
                    'supports_symbols': True,
                }
            ],
        }

    def validate_run_request(self, **kwargs):
        from app.eod.daily_pipeline_stages import DailyPipelineStageService

        return DailyPipelineStageService().validate_run_request(**kwargs)

    def run(self, **kwargs):
        self.validate_run_request(
            market_date=kwargs.get('market_date'),
            stages=kwargs.get('stages'),
            resume_from_stage=kwargs.get('resume_from_stage'),
        )
        self.run_args = kwargs
        return {
            'market_date': kwargs.get('market_date') or '2026-04-21',
            'resolved_stages': kwargs.get('stages') or ['price_collection'],
            'status': 'completed',
            'stages': {
                'price_collection': {
                    'status': 'completed',
                    'result': {'bars_upserted': 2},
                }
            },
        }


class EODRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_eod.router)

    def test_get_eod_status_route_includes_market_session_metadata(self):
        class _FakeMarketState:
            def __init__(self) -> None:
                self.market = 'KRX'
                self.state = 'HOLIDAY'
                self.is_trading_day = False
                self.is_open = False
                self.reason = 'holiday'
                self.server_time = '2026-05-05T12:15:00+09:00'
                self.current_calendar_date = '2026-05-05'
                self.latest_valid_trading_date = '2026-05-04'
                self.market_session = 'holiday'

        client = TestClient(self.app)
        with patch('app.api.routes_eod.async_get_market_state', return_value=_FakeMarketState()):
            response = client.get('/eod/status')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['market']['state'], 'HOLIDAY')
        self.assertEqual(payload['market']['latest_valid_trading_date'], '2026-05-04')
        self.assertEqual(payload['market_session']['requested_date'], '2026-05-05')
        self.assertEqual(payload['market_session']['status'], 'holiday')
        self.assertEqual(payload['market_session']['latest_valid_trading_date'], '2026-05-04')
        self.assertFalse(payload['market_session']['intraday_data_expected'])

    def test_pykrx_backfill_omitted_dates_use_latest_krx_session(self):
        client = TestClient(self.app)
        with (
            patch(
                'app.api.routes_eod.async_latest_krx_trading_date',
                return_value=date(2026, 5, 4),
            ),
            patch.object(
                routes_eod.PYKRX_BACKFILL_SERVICE,
                'run_backfill',
                return_value={'status': 'completed'},
            ) as run_backfill,
        ):
            response = client.post('/eod/pykrx/backfill')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(run_backfill.call_args.kwargs['start_date'], '2026-05-04')
        self.assertEqual(run_backfill.call_args.kwargs['end_date'], '2026-05-04')

    def test_pykrx_backfill_rejects_explicit_krx_holiday(self):
        client = TestClient(self.app)
        with (
            patch(
                'app.api.routes_eod.async_get_krx_calendar_day',
                return_value=SimpleNamespace(is_open=False),
            ),
            patch.object(
                routes_eod.PYKRX_BACKFILL_SERVICE,
                'run_backfill',
            ) as run_backfill,
        ):
            response = client.post(
                '/eod/pykrx/backfill',
                params={'end_date': '2026-05-05'},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()['detail'],
            'end_date must be a KRX trading day; 2026-05-05 is a holiday',
        )
        run_backfill.assert_not_called()

    def test_pykrx_daily_rejects_explicit_weekend(self):
        client = TestClient(self.app)
        with (
            patch(
                'app.api.routes_eod.async_get_krx_calendar_day',
                return_value=SimpleNamespace(is_open=False),
            ),
            patch.object(
                routes_eod.PYKRX_BACKFILL_SERVICE,
                'run_daily_pipeline_if_due',
            ) as run_daily,
        ):
            response = client.post(
                '/eod/pykrx/daily/run-if-due',
                params={'market_date': '2026-05-09'},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()['detail'],
            'market_date must be a KRX trading day; 2026-05-09 is a weekend',
        )
        run_daily.assert_not_called()

    def test_pykrx_daily_omitted_date_uses_latest_krx_session(self):
        client = TestClient(self.app)
        with (
            patch(
                'app.api.routes_eod.async_latest_krx_trading_date',
                return_value=date(2026, 5, 4),
            ),
            patch.object(
                routes_eod.PYKRX_BACKFILL_SERVICE,
                'run_daily_pipeline_if_due',
                return_value={'status': 'completed'},
            ) as run_daily,
        ):
            response = client.post('/eod/pykrx/daily/run-if-due')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(run_daily.call_args.kwargs['market_date'], '2026-05-04')

    def test_get_eod_freshness_route_returns_service_payload(self):
        fake_service = FakeFreshnessService()
        original_service = routes_eod.eod_freshness_service
        routes_eod.eod_freshness_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/freshness')
        finally:
            routes_eod.eod_freshness_service = original_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['today']['trade_date'], '2026-04-10')
        self.assertEqual(payload['today']['price']['status'], 'ready')
        self.assertEqual(payload['today']['technical_analysis']['symbols_succeeded'], 3)
        self.assertEqual(payload['today']['fundamentals']['symbols_total'], 1)
        self.assertEqual(payload['today']['fundamentals']['basis'], 'same_day_snapshot_updates')
        self.assertEqual(payload['latest']['trade_date'], '2026-04-10')
        self.assertEqual(payload['latest']['updated_at'], '2026-04-10T07:31:55+00:00')
        self.assertEqual(payload['latest']['total'], 3)
        self.assertEqual(payload['latest']['summary']['basis'], 'latest_eod_run_symbol_universe')
        self.assertEqual(payload['latest']['symbols_succeeded'], 3)
        self.assertEqual(payload['latest']['action_market_date'], '2026-04-10')
        self.assertEqual(payload['latest']['fundamentals']['market_date'], '2026-04-10')

    def test_get_eod_screening_results_route_calls_service(self):
        fake_service = FakeScreeningResultReadService()
        original_service = routes_eod.screening_result_read_service
        routes_eod.screening_result_read_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/screening-results',
                params={
                    'family': 'composite',
                    'market_date': '2026-04-24',
                    'limit': 25,
                    'offset': 50,
                },
            )
        finally:
            routes_eod.screening_result_read_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.read_args,
            {'family': 'composite', 'market_date': '2026-04-24', 'limit': 25, 'offset': 50},
        )
        payload = response.json()
        self.assertEqual(payload['family'], 'composite')
        self.assertEqual(payload['detail_route'], '/eod/composite-factors')
        self.assertEqual(payload['summary']['entries'], 2)
        self.assertEqual(payload['pagination']['offset'], 50)
        self.assertEqual(payload['items'][0]['composite_classification'], 'eligible')

    def test_get_eod_pipeline_status_route_returns_service_payload(self):
        fake_service = FakePipelineStatusService()
        original_service = routes_eod.eod_pipeline_status_service
        routes_eod.eod_pipeline_status_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/pipeline/status',
                params={'market_date': '2026-04-21', 'fs_div': 'OFS'},
            )
        finally:
            routes_eod.eod_pipeline_status_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.read_args,
            {
                'market_date': '2026-04-21',
                'fs_div': 'OFS',
                'include_feedback_context': False,
                'include_feedback_stage': False,
                'include_pykrx_enrichment_coverage': False,
                'include_row_count_diagnostics': False,
            },
        )
        payload = response.json()
        self.assertEqual(payload['status'], 'running')
        self.assertEqual(payload['stages']['price_collection']['source_run_ids']['eod_run_id'], 101)
        self.assertEqual(
            payload['stages']['technical_snapshots']['current_activity'],
            'Computing daily technical snapshots for 000660',
        )

    def test_get_eod_pipeline_stages_route_returns_stage_catalog(self):
        fake_service = FakeDailyPipelineStageService()
        original_service = routes_eod.daily_pipeline_stage_service
        routes_eod.daily_pipeline_stage_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/pipeline/stages')
        finally:
            routes_eod.daily_pipeline_stage_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertTrue(fake_service.describe_called)
        payload = response.json()
        self.assertEqual(payload['stage_order'][0], 'price_collection')
        self.assertEqual(payload['stages'][0]['id'], 'price_collection')

    def test_run_eod_pipeline_route_calls_stage_service(self):
        fake_service = FakeDailyPipelineStageService()
        original_service = routes_eod.daily_pipeline_stage_service
        routes_eod.daily_pipeline_stage_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/pipeline/run',
                json={
                    'market_date': '2026-04-21',
                    'stages': ['signal_decider', 'technical_snapshots'],
                    'symbols': ['005930', '000660'],
                    'markets': ['KOSPI', 'KOSDAQ'],
                    'force': True,
                    'fs_div': 'OFS',
                    'symbol_limit': 25,
                    'capture_limit': 100,
                    'retry_cycles': 2,
                    'feedback_max_cycles': 4,
                    'feedback_outer_batch_size': 10,
                    'reuse_existing_feedback': False,
                },
            )
        finally:
            routes_eod.daily_pipeline_stage_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.run_args['market_date'], '2026-04-21')
        self.assertEqual(fake_service.run_args['stages'], ['signal_decider', 'technical_snapshots'])
        self.assertEqual(fake_service.run_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.run_args['markets'], ['KOSPI', 'KOSDAQ'])
        self.assertEqual(fake_service.run_args['feedback_max_cycles'], 4)
        self.assertFalse(fake_service.run_args['reuse_existing_feedback'])
        self.assertEqual(response.json()['status'], 'completed')

    def test_run_eod_pipeline_route_accepts_background_execution(self):
        fake_service = FakeDailyPipelineStageService()
        original_service = routes_eod.daily_pipeline_stage_service
        routes_eod.daily_pipeline_stage_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/pipeline/run',
                json={
                    'market_date': '2026-08-28',
                    'stages': ['signal_decider'],
                    'background': True,
                    'reuse_existing_feedback': True,
                },
            )
        finally:
            routes_eod.daily_pipeline_stage_service = original_service

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()['status'], 'accepted')
        self.assertTrue(response.json()['background'])
        self.assertEqual(fake_service.run_args['market_date'], '2026-08-28')
        self.assertNotIn('background', fake_service.run_args)

    def test_run_eod_pipeline_route_validates_background_input_before_accepting(self):
        original_service = routes_eod.daily_pipeline_stage_service
        try:
            for invalid_payload in (
                {
                    'market_date': 'not-a-date',
                    'stages': ['signal_decider'],
                },
                {
                    'market_date': '2026-08-28',
                    'stages': ['unknown_stage'],
                },
            ):
                with self.subTest(invalid_payload=invalid_payload):
                    synchronous_service = FakeDailyPipelineStageService()
                    routes_eod.daily_pipeline_stage_service = synchronous_service
                    client = TestClient(self.app)
                    synchronous_response = client.post('/eod/pipeline/run', json=invalid_payload)

                    background_service = FakeDailyPipelineStageService()
                    routes_eod.daily_pipeline_stage_service = background_service
                    background_response = client.post(
                        '/eod/pipeline/run',
                        json={**invalid_payload, 'background': True},
                    )

                    self.assertEqual(synchronous_response.status_code, 400)
                    self.assertEqual(background_response.status_code, 400)
                    self.assertEqual(background_response.json(), synchronous_response.json())
                    self.assertIsNone(background_service.run_args)
        finally:
            routes_eod.daily_pipeline_stage_service = original_service

    def test_collect_pykrx_enrichment_route_calls_service(self):
        fake_service = FakePykrxEnrichmentService()
        original_service = routes_eod.pykrx_enrichment_service
        routes_eod.pykrx_enrichment_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/pykrx/enrichment/collect',
                params={
                    'market_date': '2026-04-21',
                    'markets': 'KOSPI,KOSDAQ',
                    'symbols': '005930,000660',
                    'categories': 'sector_classification,investor_flow_symbol',
                },
            )
        finally:
            routes_eod.pykrx_enrichment_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.collect_args,
            {
                'market_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
                'symbols': ['005930', '000660'],
                'categories': ['sector_classification', 'investor_flow_symbol'],
            },
        )
        self.assertEqual(response.json()['coverage']['categories']['sector_classification']['coverage_status'], 'complete')

    def test_inspect_pykrx_enrichment_route_calls_service(self):
        fake_service = FakePykrxEnrichmentService()
        original_service = routes_eod.pykrx_enrichment_service
        routes_eod.pykrx_enrichment_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/pykrx/enrichment',
                params={
                    'market_date': '2026-04-21',
                    'markets': 'KOSPI',
                    'symbols': '005930',
                    'categories': 'investor_flow_symbol',
                    'limit': 5,
                },
            )
        finally:
            routes_eod.pykrx_enrichment_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {
                'market_date': '2026-04-21',
                'markets': ['KOSPI'],
                'symbols': ['005930'],
                'categories': ['investor_flow_symbol'],
                'limit': 5,
            },
        )
        self.assertEqual(response.json()['items'][0]['payload']['investor_groups']['외국인']['net_value'], 1200000)

    def test_compute_technical_snapshots_route_calls_service(self):
        fake_service = FakeTechnicalSnapshotService()
        original_service = routes_eod.daily_technical_snapshot_service
        routes_eod.daily_technical_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/technical-snapshots/compute',
                params={'market_date': '2026-04-07', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_technical_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['snapshots_persisted'], 2)

    def test_inspect_technical_snapshots_route_calls_service(self):
        fake_service = FakeTechnicalSnapshotService()
        original_service = routes_eod.daily_technical_snapshot_service
        routes_eod.daily_technical_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/technical-snapshots',
                params={'market_date': '2026-04-07', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_technical_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['trend_state'], 'uptrend')

    def test_compute_action_summaries_route_calls_service(self):
        fake_service = FakeActionSummaryService()
        original_service = routes_eod.daily_action_summary_service
        routes_eod.daily_action_summary_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/action-summaries/compute',
                params={'market_date': '2026-04-07', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_action_summary_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['action_summaries_persisted'], 2)

    def test_inspect_action_summaries_route_calls_service(self):
        fake_service = FakeActionSummaryService()
        original_service = routes_eod.daily_action_summary_service
        routes_eod.daily_action_summary_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/action-summaries',
                params={'market_date': '2026-04-07', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_action_summary_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['action_label'], 'long_breakout')

    def test_collect_signal_features_route_calls_service(self):
        fake_service = FakeSignalFeatureService()
        original_service = routes_eod.daily_signal_feature_service
        routes_eod.daily_signal_feature_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/signal-features/collect',
                params={
                    'market_date': '2026-04-14',
                    'sources': 'volume-power,overtime-volume',
                    'max_pages': 2,
                },
            )
        finally:
            routes_eod.daily_signal_feature_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.collect_args['sources'], ['volume-power', 'overtime-volume'])
        self.assertEqual(fake_service.collect_args['max_pages'], 2)
        self.assertEqual(response.json()['features_persisted'], 2)

    def test_inspect_signal_features_route_calls_service(self):
        fake_service = FakeSignalFeatureService()
        original_service = routes_eod.daily_signal_feature_service
        routes_eod.daily_signal_feature_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/signal-features',
                params={'market_date': '2026-04-14', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_signal_feature_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['signal_count'], 2)

    def test_compute_market_risk_filters_route_calls_service(self):
        fake_service = FakeMarketRiskFilterService()
        original_service = routes_eod.daily_market_risk_filter_service
        routes_eod.daily_market_risk_filter_service = fake_service
        try:
            with patch(
                'app.eod.daily_pipeline_tracking.daily_pipeline_run_state_service',
                FakeDailyPipelineRunStateService(),
            ):
                client = TestClient(self.app)
                response = client.post(
                    '/eod/market-risk-filters/compute',
                    params={
                        'market_date': '2026-04-14',
                        'symbols': '005930,000660',
                        'source': 'kis_market_risk_v1',
                        'market_code': 'UN',
                    },
                )
        finally:
            routes_eod.daily_market_risk_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-14')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'kis_market_risk_v1')
        self.assertEqual(fake_service.compute_args['market_code'], 'UN')
        self.assertIn('progress_callback', fake_service.compute_args)
        self.assertEqual(response.json()['pipeline_run_id'], 7001)
        self.assertEqual(response.json()['items'][0]['risk_state'], 'watchlist')

    def test_collect_market_risk_snapshots_route_calls_service(self):
        fake_service = FakeMarketRiskSnapshotService()
        original_service = routes_eod.daily_market_risk_snapshot_service
        routes_eod.daily_market_risk_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/market-risk-snapshots/collect',
                params={
                    'market_date': '2026-04-14',
                    'symbols': '005930,000660',
                    'source': 'kis_quote2_snapshot_v1',
                    'market_code': 'UN',
                },
            )
        finally:
            routes_eod.daily_market_risk_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.collect_args['market_date'], '2026-04-14')
        self.assertEqual(fake_service.collect_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.collect_args['source'], 'kis_quote2_snapshot_v1')
        self.assertEqual(fake_service.collect_args['market_code'], 'UN')
        self.assertEqual(response.json()['snapshots_persisted'], 2)
        self.assertTrue(response.json()['items'][0]['is_trade_halted'])

    def test_inspect_market_risk_snapshots_route_returns_single_item_for_symbol(self):
        fake_service = FakeMarketRiskSnapshotService()
        original_service = routes_eod.daily_market_risk_snapshot_service
        routes_eod.daily_market_risk_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/market-risk-snapshots',
                params={'market_date': '2026-04-14', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_market_risk_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-14')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertTrue(response.json()['item']['is_trade_halted'])
        self.assertEqual(response.json()['item']['market_warning_code'], '01')

    def test_inspect_market_risk_snapshots_route_allows_latest_mode_without_market_date(self):
        fake_service = FakeMarketRiskSnapshotService()
        original_service = routes_eod.daily_market_risk_snapshot_service
        routes_eod.daily_market_risk_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/market-risk-snapshots')
        finally:
            routes_eod.daily_market_risk_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(fake_service.inspect_args['market_date'])
        self.assertTrue(response.json()['latest_mode'])
        self.assertEqual(response.json()['summary']['trade_halted_count'], 1)

    def test_inspect_market_risk_filters_route_returns_single_item_for_symbol(self):
        fake_service = FakeMarketRiskFilterService()
        original_service = routes_eod.daily_market_risk_filter_service
        routes_eod.daily_market_risk_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/market-risk-filters',
                params={'market_date': '2026-04-14', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_market_risk_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-14')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['risk_state'], 'watchlist')
        self.assertTrue(response.json()['item']['watchlist_flagged'])

    def test_inspect_market_risk_filters_route_allows_latest_mode_without_market_date(self):
        fake_service = FakeMarketRiskFilterService()
        original_service = routes_eod.daily_market_risk_filter_service
        routes_eod.daily_market_risk_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/market-risk-filters')
        finally:
            routes_eod.daily_market_risk_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(fake_service.inspect_args['market_date'])
        self.assertTrue(response.json()['latest_mode'])
        self.assertEqual(response.json()['summary']['failed_count'], 1)

    def test_compute_low_risk_factors_route_calls_service(self):
        fake_service = FakeLowRiskFactorService()
        original_service = routes_eod.daily_low_risk_factor_service
        routes_eod.daily_low_risk_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/low-risk-factors/compute',
                params={
                    'market_date': '2026-04-23',
                    'symbols': '005930,000660',
                    'source': 'low_risk_v1',
                },
            )
        finally:
            routes_eod.daily_low_risk_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'low_risk_v1')
        self.assertEqual(response.json()['items'][0]['low_risk_score'], 0.86)

    def test_inspect_low_risk_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeLowRiskFactorService()
        original_service = routes_eod.daily_low_risk_factor_service
        routes_eod.daily_low_risk_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/low-risk-factors',
                params={'market_date': '2026-04-23', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_low_risk_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['low_risk_score'], 0.91)
        self.assertTrue(response.json()['item']['is_low_risk_eligible'])

    def test_compute_beta_factors_route_calls_service(self):
        fake_service = FakeBetaFactorService()
        original_service = routes_eod.daily_beta_factor_service
        routes_eod.daily_beta_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/beta-factors/compute',
                params={
                    'market_date': '2026-04-23',
                    'symbols': '005930,000660',
                    'source': 'beta_v1',
                },
            )
        finally:
            routes_eod.daily_beta_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'beta_v1')
        self.assertEqual(response.json()['items'][0]['low_beta_score'], 0.84)

    def test_inspect_beta_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeBetaFactorService()
        original_service = routes_eod.daily_beta_factor_service
        routes_eod.daily_beta_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/beta-factors',
                params={'market_date': '2026-04-23', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_beta_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['benchmark_id'], 'KOSPI_EQW')
        self.assertTrue(response.json()['item']['is_low_beta_eligible'])

    def test_compute_momentum_factors_route_calls_service(self):
        fake_service = FakeMomentumFactorService()
        original_service = routes_eod.daily_momentum_factor_service
        routes_eod.daily_momentum_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/momentum-factors/compute',
                params={'market_date': '2026-04-14', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_momentum_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['results_persisted'], 2)

    def test_inspect_momentum_factors_route_calls_service(self):
        fake_service = FakeMomentumFactorService()
        original_service = routes_eod.daily_momentum_factor_service
        routes_eod.daily_momentum_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/momentum-factors',
                params={'market_date': '2026-04-14', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_momentum_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['summary']['eligible_symbols'], 1)
        self.assertEqual(response.json()['items'][0]['momentum_rank'], 1)
        self.assertTrue(response.json()['items'][0]['passes_momentum_top_percentile'])

    def test_compute_value_factors_route_calls_service(self):
        fake_service = FakeValueFactorService()
        original_service = routes_eod.daily_value_factor_service
        routes_eod.daily_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'value_factor_v1',
                },
            )
        finally:
            routes_eod.daily_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'value_factor_v1')
        self.assertEqual(response.json()['items'][0]['value_score'], 0.86)

    def test_compute_value_factors_route_preserves_empty_symbol_subset(self):
        fake_service = FakeValueFactorService()
        original_service = routes_eod.daily_value_factor_service
        routes_eod.daily_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': ',',
                    'source': 'value_factor_v1',
                },
            )
        finally:
            routes_eod.daily_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], [])
        self.assertEqual(fake_service.compute_args['source'], 'value_factor_v1')

    def test_inspect_value_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeValueFactorService()
        original_service = routes_eod.daily_value_factor_service
        routes_eod.daily_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/value-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['value_score'], 0.91)
        self.assertEqual(response.json()['item']['value_status'], 'scored')

    def test_compute_advanced_value_factors_route_calls_service(self):
        fake_service = FakeAdvancedValueFactorService()
        original_service = routes_eod.daily_advanced_value_factor_service
        routes_eod.daily_advanced_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/advanced-value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'advanced_value_v1',
                },
            )
        finally:
            routes_eod.daily_advanced_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'advanced_value_v1')
        self.assertEqual(response.json()['items'][0]['advanced_value_score'], 0.84)

    def test_inspect_advanced_value_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeAdvancedValueFactorService()
        original_service = routes_eod.daily_advanced_value_factor_service
        routes_eod.daily_advanced_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/advanced-value-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_advanced_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['advanced_value_score'], 0.91)
        self.assertEqual(response.json()['item']['advanced_value_classification'], 'eligible')

    def test_compute_unified_value_factors_route_calls_service(self):
        fake_service = FakeUnifiedValueFactorService()
        original_service = routes_eod.daily_unified_value_factor_service
        routes_eod.daily_unified_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/unified-value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'unified_value_v1',
                },
            )
        finally:
            routes_eod.daily_unified_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'unified_value_v1')
        self.assertEqual(response.json()['items'][0]['unified_value_score'], 0.87)

    def test_inspect_unified_value_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeUnifiedValueFactorService()
        original_service = routes_eod.daily_unified_value_factor_service
        routes_eod.daily_unified_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/unified-value-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_unified_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['unified_value_score'], 0.93)
        self.assertEqual(response.json()['item']['unified_value_status'], 'scored')

    def test_compute_quality_factors_route_calls_service(self):
        fake_service = FakeQualityFactorService()
        original_service = routes_eod.daily_quality_factor_service
        routes_eod.daily_quality_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/quality-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'quality_v1',
                },
            )
        finally:
            routes_eod.daily_quality_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'quality_v1')
        self.assertEqual(response.json()['items'][0]['quality_score'], 0.87)

    def test_inspect_quality_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeQualityFactorService()
        original_service = routes_eod.daily_quality_factor_service
        routes_eod.daily_quality_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/quality-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_quality_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['quality_score'], 0.93)
        self.assertEqual(response.json()['item']['quality_status'], 'scored_partial')

    def test_compute_composite_factors_route_calls_service(self):
        fake_service = FakeCompositeFactorService()
        original_service = routes_eod.daily_composite_factor_service
        routes_eod.daily_composite_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/composite-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'composite_v1',
                },
            )
        finally:
            routes_eod.daily_composite_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'composite_v1')
        self.assertEqual(response.json()['items'][0]['composite_score'], 0.88)

    def test_inspect_composite_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeCompositeFactorService()
        original_service = routes_eod.daily_composite_factor_service
        routes_eod.daily_composite_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/composite-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_composite_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['composite_score'], 0.91)
        self.assertTrue(response.json()['item']['is_composite_eligible'])

    def test_inspect_piotroski_filter_results_route_returns_all_items(self):
        fake_service = FakePiotroskiFilterService()
        original_service = routes_eod.daily_piotroski_filter_service
        routes_eod.daily_piotroski_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/piotroski-filter-results', params={'market_date': '2026-04-14'})
        finally:
            routes_eod.daily_piotroski_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': '2026-04-14', 'symbol': None, 'limit': None},
        )
        self.assertEqual(response.json()['requested_market_date'], '2026-04-14')
        self.assertEqual(response.json()['resolved_market_date'], '2026-04-14')
        self.assertFalse(response.json()['latest_mode'])
        self.assertFalse(response.json()['is_latest_fallback'])
        self.assertEqual(response.json()['summary']['entries'], 2)
        self.assertEqual(response.json()['summary']['passed_count'], 1)
        self.assertEqual(response.json()['items'][0]['symbol'], '005930')

    def test_inspect_piotroski_filter_results_route_returns_single_item_for_symbol(self):
        fake_service = FakePiotroskiFilterService()
        original_service = routes_eod.daily_piotroski_filter_service
        routes_eod.daily_piotroski_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/piotroski-filter-results',
                params={'market_date': '2026-04-14', 'symbol': '005930', 'limit': 10},
            )
        finally:
            routes_eod.daily_piotroski_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': '2026-04-14', 'symbol': '005930', 'limit': 10},
        )
        self.assertEqual(response.json()['summary']['requested_symbol'], '005930')
        self.assertEqual(response.json()['resolved_market_date'], '2026-04-14')
        self.assertEqual(response.json()['item']['piotroski_score'], 8)

    def test_inspect_piotroski_filter_results_route_allows_latest_mode_without_market_date(self):
        fake_service = FakePiotroskiFilterService()
        original_service = routes_eod.daily_piotroski_filter_service
        routes_eod.daily_piotroski_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/piotroski-filter-results')
        finally:
            routes_eod.daily_piotroski_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': None, 'symbol': None, 'limit': None},
        )
        self.assertIsNone(response.json()['requested_market_date'])
        self.assertEqual(response.json()['resolved_market_date'], '2026-04-23')
        self.assertTrue(response.json()['latest_mode'])
        self.assertFalse(response.json()['is_latest_fallback'])
        self.assertEqual(response.json()['market_date'], '2026-04-23')
