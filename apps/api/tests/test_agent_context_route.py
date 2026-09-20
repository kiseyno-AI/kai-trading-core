from datetime import date, datetime
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_agent_context
from app.agent_context.prerequisite_service import FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
from app.agent_context.runtime_state import RuntimeFeedbackStateStore


def _feedback_prerequisites(*, status: str = 'ready', missing_stage_ids: list[str] | None = None) -> dict:
    missing_stage_ids = list(missing_stage_ids or [])
    ready = status == 'ready'
    return {
        'market_date': '2026-04-14',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER),
        'ready_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER) if ready else [],
        'waiting_stage_ids': missing_stage_ids if status == 'waiting' else [],
        'blocked_stage_ids': missing_stage_ids if status == 'blocked' else [],
        'missing_prerequisites': [
            {
                'stage': stage_id,
                'label': stage_id.replace('_', ' '),
                'status': 'running' if status == 'waiting' else 'partial',
                'gate_status': status,
                'current_activity': f'{status} {stage_id}',
                'details': {},
            }
            for stage_id in missing_stage_ids
        ],
        'stages': {},
    }


class FakeAgentContextReadService:
    def __init__(self):
        self.last_args = None

    def get_symbol_day_context(self, **kwargs):
        self.last_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'symbols': kwargs['symbols'],
            },
            'summary': {
                'requested_symbol_count': len(kwargs['symbols']),
                'matched_symbol_count': len(kwargs['symbols']),
                'missing_symbols': [],
            },
            'items': [{'symbol': kwargs['symbols'][0], 'market_date': kwargs['market_date'].isoformat()}],
        }


class ErrorAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        raise ValueError('At least one symbol is required')


class FakeAgentContextFeedbackService:
    def __init__(self):
        self.submitted = None
        self.list_args = None

    def submit_feedback(self, **kwargs):
        self.submitted = kwargs
        return {
            'accepted': True,
            'item': {
                'id': 7,
                'market_date': kwargs['market_date'].isoformat(),
                'symbol': kwargs['symbol'],
                'agent': {
                    'name': kwargs['agent_name'],
                    'version': kwargs['agent_version'],
                    'run_id': kwargs['run_id'],
                },
                'summary': kwargs['summary'],
                'feedback': kwargs['feedback'],
                'context': kwargs['context'],
                'received_at': '2026-04-14T15:00:00+00:00',
            },
            'summary': {
                'summary_keys': sorted(kwargs['summary'].keys()),
                'feedback_keys': sorted(kwargs['feedback'].keys()),
            },
        }

    def list_feedback(self, **kwargs):
        self.list_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'symbols': kwargs['symbols'],
                'agent_name': kwargs['agent_name'],
                'limit': kwargs['limit'],
            },
            'summary': {'count': 1},
            'items': [{'id': 7, 'symbol': '005930'}],
        }


class FakeAgentContextMissingDataReportService:
    def __init__(self):
        self.last_args = None

    def get_summary(self, **kwargs):
        self.last_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] is not None else None,
                'market_date_from': kwargs['market_date_from'].isoformat() if kwargs['market_date_from'] is not None else None,
                'market_date_to': kwargs['market_date_to'].isoformat() if kwargs['market_date_to'] is not None else None,
                'symbols': kwargs['symbols'],
                'agent_name': kwargs['agent_name'],
                'categories': kwargs['categories'],
                'persist_missing_categories': kwargs.get('persist_missing_categories', False),
            },
            'summary': {'feedback_rows_matched': 2},
            'categories': [{'category': 'factor_score_coverage'}],
            'date_breakdown': [],
            'uncategorized_examples': [],
        }


class FakeAgentContextFeedbackFailureService:
    def __init__(self):
        self.last_args = None

    def get_grouped_summary(self, **kwargs):
        self.last_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] is not None else None,
                'market_date_from': kwargs['market_date_from'].isoformat() if kwargs['market_date_from'] is not None else None,
                'market_date_to': kwargs['market_date_to'].isoformat() if kwargs['market_date_to'] is not None else None,
                'run_id': kwargs['run_id'],
                'agent_name': kwargs['agent_name'],
                'failure_kind': kwargs['failure_kind'],
                'limit': kwargs['limit'],
            },
            'summary': {'group_count': 1, 'failure_count': 2},
            'groups': [
                {
                    'market_date': '2026-05-05',
                    'run_id': kwargs['run_id'],
                    'agent_name': kwargs['agent_name'],
                    'failure_kind': kwargs['failure_kind'],
                    'failure_count': 2,
                }
            ],
        }


class FakeDeciderCoverageReportService:
    def __init__(self):
        self.last_args = None

    def get_report(self, **kwargs):
        self.last_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'run_id': kwargs['run_id'],
                'symbols': kwargs['symbols'],
                'agent_names': kwargs['agent_names'],
            },
            'summary': {
                'symbols_evaluated': 2,
                'contract_regressions': {'status': 'alert', 'raw_null_count': 1},
            },
            'per_agent': {
                'signal-decider': {'feedback_rows': 2},
            },
        }


class FakeAgentContextEodSummariesService:
    def __init__(self):
        self.inspect_args = None

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        requested_symbol = kwargs['symbol'].strip().upper() if kwargs['symbol'] else None
        item = {
            'symbol': requested_symbol or '005930',
            'market_date': kwargs['market_date'].isoformat(),
            'agent_name': 'eod-summarizer',
            'summary': 'Breadth improved into the close.',
            'confidence': 0.78,
            'risk_flags': ['narrow_leadership'],
            'received_at': '2026-04-21T06:10:00+00:00',
        }
        payload = {
            'market_date': kwargs['market_date'].isoformat(),
            'agent_name': 'eod-summarizer',
            'summary': {
                'entries': 1,
                'requested_symbol': requested_symbol,
                'matched_symbols': [item['symbol']],
                'missing_symbols': [],
                'risk_flagged_count': 1,
            },
        }
        if requested_symbol:
            payload['item'] = item
        else:
            payload['items'] = [item]
        return payload


class FakeAgentFeedbackRetryService:
    def __init__(self):
        self.last_config = None

    def run_retry_cycles(self, *, config, force: bool = False):
        self.last_config = config
        return {
            'market_date': config.market_date.isoformat(),
            'requested_symbols': list(config.symbols) if config.symbols is not None else None,
            'roles': list(config.roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'reuse_existing': config.reuse_existing,
            'feedback_scope_mode': config.feedback_scope_mode,
            'force': force,
            'cycles': [],
            'final_pending': 0,
            'completed': True,
            'progress': {
                'request': {
                    'market_date': config.market_date.isoformat(),
                    'symbols': list(config.symbols) if config.symbols is not None else None,
                    'expected_agents': list(config.roles),
                },
                'summary': {
                    'first_feedback_at': '2026-04-14T01:00:00+00:00',
                    'elapsed_seconds': 300,
                    'completed_feedback_per_minute': 0.6,
                    'completed_symbols_per_minute': 0.2,
                    'estimated_remaining_seconds': 0,
                    'estimated_completion_at': '2026-04-14T01:05:00+00:00',
                    'eta_confidence': 'high',
                },
            },
        }


class FakeBackgroundDispatchOrchestrator:
    def __init__(self):
        self.calls = []

    def dispatch_for_symbols(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'symbols': kwargs['symbols'],
                'roles': kwargs['roles'],
                'request_run_id': kwargs['request_run_id'],
            },
            'summary': {
                'status': 'completed',
                'completed': len(kwargs['symbols']),
                'failed': 0,
            },
            'items': [],
            'failures': [],
        }


class FailingBackgroundDispatchOrchestrator:
    def __init__(self, error: Exception):
        self.calls = []
        self.error = error

    def dispatch_for_symbols(self, **kwargs):
        self.calls.append(kwargs)
        raise self.error


class FakeRunStateService:
    def __init__(self):
        self.started = []
        self.finished = []
        self._runs_by_id = {}

    def start_run(self, **kwargs):
        self.started.append(kwargs)
        self._runs_by_id[kwargs['run_id']] = {
            'run_id': kwargs['run_id'],
            'status': kwargs['status'],
            'completed_count': 0,
            'failed_count': 0,
            'request_payload': kwargs['request_payload'],
            'summary_payload': kwargs.get('summary_payload'),
            'started_at': kwargs.get('started_at'),
            'completed_at': None,
        }
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': kwargs['status']}

    def get_run(self, *, run_id):
        return self._runs_by_id.get(run_id)

    def finish_run(self, **kwargs):
        self.finished.append(kwargs)
        current = dict(self._runs_by_id.get(kwargs['run_id']) or {})
        current.update(
            {
                'run_id': kwargs['run_id'],
                'status': kwargs['status'],
                'completed_count': kwargs['completed_count'],
                'failed_count': kwargs['failed_count'],
                'summary_payload': kwargs['summary_payload'],
                'completed_at': 'finished',
            }
        )
        self._runs_by_id[kwargs['run_id']] = current
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': kwargs['status']}


class FakeAgentContextProgressService:
    def __init__(self):
        self.last_args = None

    def get_feedback_progress(self, **kwargs):
        self.last_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'symbols': kwargs['symbols'],
                'run_id': kwargs['run_id'],
            },
            'result': {'market_date': kwargs['market_date'].isoformat(), 'roles': {}},
            'summary': {'run_id': kwargs['run_id'], 'run_status': 'running'},
        }


class FakeDeciderEvaluationService:
    def __init__(self):
        self.compute_args = None
        self.list_args = None

    def compute_evaluations(self, **kwargs):
        self.compute_args = kwargs
        return {
            'request': {
                'market_date_from': kwargs['market_date_from'].isoformat(),
                'market_date_to': kwargs['market_date_to'].isoformat(),
                'agent_names': kwargs['agent_names'],
                'symbols': kwargs['symbols'],
                'window_days': kwargs['window_days'],
                'force_recompute': kwargs['force_recompute'],
            },
            'summary': {'evaluations_upserted': 2},
            'items': [
                {
                    'evaluation_id': 7,
                    'source': {'feedback_id': 11, 'decision_snapshot_id': 84},
                    'agent_name': 'signal-decider',
                    'symbol': '005930',
                    'outcome_label': 'target_hit_before_stop',
                }
            ],
        }

    def list_evaluations(self, **kwargs):
        self.list_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] is not None else None,
                'market_date_from': kwargs['market_date_from'].isoformat() if kwargs['market_date_from'] is not None else None,
                'market_date_to': kwargs['market_date_to'].isoformat() if kwargs['market_date_to'] is not None else None,
                'agent_names': kwargs['agent_names'],
                'symbols': kwargs['symbols'],
                'outcome_labels': kwargs['outcome_labels'],
                'window_days': kwargs['window_days'],
                'limit': kwargs['limit'],
            },
            'summary': {'count': 1},
            'items': [
                {
                    'evaluation_id': 7,
                    'source': {'feedback_id': 11, 'decision_snapshot_id': 84},
                    'agent_name': 'signal-decider',
                    'symbol': '005930',
                    'outcome_label': 'target_hit_before_stop',
                }
            ],
        }


class FakeSignalDeciderLoopService:
    def __init__(self):
        self.decision_args = None
        self.detail_args = None
        self.calendar_args = None
        self.review_date_args = None
        self.review_detail_args = None
        self.diagnostics_args = None
        self.aggregate_diagnostics_args = None
        self.compute_args = None
        self.evaluation_args = None
        self.candidate_generate_args = None
        self.candidate_list_args = None
        self.policy_list_args = None
        self.policy_detail_args = None
        self.policy_activate_args = None

    def build_calendar_day_status(self, *, decision_count: int, reviewed_count: int) -> str:
        if decision_count <= 0:
            return 'empty'
        if reviewed_count >= decision_count:
            return 'reviewed'
        if reviewed_count > 0:
            return 'partial'
        return 'stored'

    def list_decisions(self, **kwargs):
        self.decision_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] is not None else None,
                'market_date_from': kwargs['market_date_from'].isoformat() if kwargs['market_date_from'] is not None else None,
                'market_date_to': kwargs['market_date_to'].isoformat() if kwargs['market_date_to'] is not None else None,
                'symbol': kwargs['symbol'],
                'action': kwargs['action'],
                'policy_version': kwargs['policy_version'],
                'verdict': kwargs['verdict'],
                'has_missed_opportunity': kwargs['has_missed_opportunity'],
                'horizons': kwargs['horizons'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'summary': {'count': 1},
            'items': [
                {
                    'feedback_id': 11,
                    'symbol': kwargs['symbol'] or '005930',
                    'policy_version': 'signal_decider_policy_v1',
                    'decision_delta': {
                        'has_previous_decision': True,
                        'transition': 'wait_to_wait',
                        'persistence_trend': 'persistent',
                        'current_wait_reason': {
                            'code': 'stale_quote_orderbook',
                            'summary': 'Realtime orderbook freshness is stale.',
                        },
                    },
                    'wait_reason': {
                        'code': 'stale_quote_orderbook',
                        'summary': 'Realtime orderbook freshness is stale.',
                    },
                    'explanation': {
                        'wait_reason': {
                            'code': 'stale_quote_orderbook',
                            'summary': 'Realtime orderbook freshness is stale.',
                        }
                    },
                    'evaluation_summary': {'evaluated': True},
                }
            ],
        }

    def get_decision_detail(self, **kwargs):
        self.detail_args = kwargs
        return {
            'request': {'snapshot_id': kwargs['snapshot_id']},
            'summary': {'evaluated_count': 1},
            'item': {
                'snapshot_id': kwargs['snapshot_id'],
                'decision_delta': {
                    'has_previous_decision': True,
                    'transition': 'wait_to_wait',
                    'persistence_trend': 'persistent',
                    'current_wait_reason': {
                        'code': 'stale_quote_orderbook',
                        'summary': 'Realtime orderbook freshness is stale.',
                    },
                },
                'wait_reason': {
                    'code': 'stale_quote_orderbook',
                    'summary': 'Realtime orderbook freshness is stale.',
                },
                'explanation': {
                    'wait_reason': {
                        'code': 'stale_quote_orderbook',
                        'summary': 'Realtime orderbook freshness is stale.',
                    }
                },
                'evaluation_summary': {'evaluated': True},
            },
        }

    def get_review_calendar(self, **kwargs):
        self.calendar_args = kwargs
        return {
            'request': {
                'month': kwargs['month'],
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {'name': 'signal_decider_replay_review'},
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {'day_count': 1, 'decision_count': 2, 'reviewed_decision_count': 2},
            'items': [
                {
                    'market_date': '2026-04-23',
                    'decision_count': 2,
                    'reviewed_decision_count': 2,
                    'scorecard': {'target_hit_count': 1},
                }
            ],
        }

    def get_review_date(self, **kwargs):
        self.review_date_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {'name': 'signal_decider_replay_review'},
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {'count': 1, 'evaluation_coverage': {'status': 'complete', 'complete_count': 1}},
            'items': [
                {
                    'snapshot_id': 84,
                    'decision_run_id': 'signal-review-run-84',
                    'market_date': kwargs['market_date'].isoformat(),
                    'symbol': '005930',
                    'policy_version': 'signal_decider_policy_v1',
                    'decision': {
                        'action': 'buy',
                        'preferred_action': 'buy',
                        'next_day_open_action': 'buy',
                        'target_price': 82000,
                        'stop_loss_price': 73500,
                        'expected_return_pct': 4.2,
                        'entry_price_basis': 'next_open',
                        'suggested_holding_period': '3d',
                        'confidence_score_10': 8,
                        'conviction_score_10': 7,
                    },
                    'decision_delta': {
                        'has_previous_decision': True,
                        'transition': 'wait_to_buy',
                        'persistence_trend': 'improving',
                        'previous_recommendation': 'wait',
                        'current_recommendation': 'buy',
                    },
                    'summary': 'Momentum and fundamentals align for a tactical next-day setup.',
                    'supporting_evidence': ['Relative strength remained constructive.'],
                    'decision_risks': ['Gap-down open'],
                    'received_at': '2026-04-23T07:45:00+00:00',
                    'review': {
                        'status': 'complete',
                        'insight': {'code': 'buy_should_have_taken', 'priority': 'low'},
                        'quality_verdict': 'good_buy',
                        'quality_reason': 'Target was touched and the stop-loss was never touched inside the review window.',
                        'review_conclusion': 'Buy held up on replay.',
                        'intraday_path_result': 'target_hit_first',
                        'narrative': {
                            'insight_code': 'buy_should_have_taken',
                            'insight_priority': 'low',
                            'headline': 'Buy held up on replay.',
                            'headline_key': 'signal_decider.review.headline.buy_should_have_taken',
                            'supporting_reasons': [
                                {
                                    'code': 'target_hit_without_stop',
                                    'message': 'Target was touched and the stop-loss was never touched inside the review window.',
                                    'message_key': 'signal_decider.review.reason.target_hit_without_stop',
                                }
                            ],
                            'counterfactual_summary': None,
                            'summary_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                        },
                        'narrative_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                        'path_analysis': {'path_marker': 'target_hit', 'trigger_sequence': 'target_hit_first'},
                        'outcome_horizons': [
                            {'horizon_days': 1, 'close_return_pct': 0.82, 'grade_label': 'good_buy'},
                            {'horizon_days': 5, 'close_return_pct': 3.14, 'grade_label': 'good_buy'},
                        ],
                    },
                }
            ],
        }

    def get_review_detail(self, **kwargs):
        self.review_detail_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'snapshot_id': kwargs['snapshot_id'],
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {'name': 'signal_decider_replay_review'},
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {
                'review_status': 'complete',
                'insight_code': 'buy_should_have_taken',
                'insight_priority': 'low',
                'headline': 'Buy held up on replay.',
                'primary_path_marker': 'target_hit',
            },
            'item': {
                'snapshot_id': kwargs['snapshot_id'],
                'symbol': '005930',
                'review': {
                    'status': 'complete',
                    'insight': {'code': 'buy_should_have_taken', 'priority': 'low'},
                    'narrative': {
                        'insight_code': 'buy_should_have_taken',
                        'insight_priority': 'low',
                        'headline': 'Buy held up on replay.',
                        'headline_key': 'signal_decider.review.headline.buy_should_have_taken',
                        'supporting_reasons': [
                            {
                                'code': 'target_hit_without_stop',
                                'message': 'Target was touched and the stop-loss was never touched inside the review window.',
                                'message_key': 'signal_decider.review.reason.target_hit_without_stop',
                            }
                        ],
                        'counterfactual_summary': None,
                        'summary_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                    },
                    'narrative_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                    'path_analysis': {'path_marker': 'target_hit'},
                },
                'observed_price_path': [{'trade_date': '2026-04-24'}],
            },
        }

    def get_diagnostics(self, **kwargs):
        self.diagnostics_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'symbol': kwargs['symbol'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'summary': {'count': 1},
            'items': [{'feedback_id': 11, 'symbol': kwargs['symbol'] or '005930', 'evaluations': []}],
        }

    def get_aggregate_diagnostics(self, **kwargs):
        self.aggregate_diagnostics_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] is not None else None,
                'market_date_from': kwargs['market_date_from'].isoformat() if kwargs['market_date_from'] is not None else None,
                'market_date_to': kwargs['market_date_to'].isoformat() if kwargs['market_date_to'] is not None else None,
                'symbols': kwargs['symbols'],
                'action': kwargs['action'],
                'policy_version': kwargs['policy_version'],
                'horizons': kwargs['horizons'],
                'evaluation_version': kwargs['evaluation_version'],
                'contributor_limit': kwargs['contributor_limit'],
            },
            'summary': {'decision_count': 2, 'evaluation_count': 4},
            'aggregates': {'by_horizon': [{'horizon_days': 5, 'buy_precision': 0.5}], 'by_policy_version': []},
            'contributors': {'factors': [{'key': 'technical.breakout_20d', 'sample_count': 2}], 'gates': []},
        }

    def compute_evaluations(self, **kwargs):
        self.compute_args = kwargs
        return {
            'request': {
                'market_date_from': kwargs['market_date_from'].isoformat(),
                'market_date_to': kwargs['market_date_to'].isoformat(),
                'symbols': kwargs['symbols'],
                'horizons': kwargs['horizons'],
                'force_recompute': kwargs['force_recompute'],
            },
            'summary': {'evaluations_upserted': 4},
            'items': [],
        }

    def list_evaluations(self, **kwargs):
        self.evaluation_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'symbol': kwargs['symbol'],
                'horizons': kwargs['horizons'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'summary': {
                'count': 1,
                'evaluated_count': 0,
                'stored_evaluation_count': 1,
                'coverage': {
                    'count': 1,
                    'computed_count': 0,
                    'pending_maturity_count': 1,
                    'insufficient_price_data_count': 0,
                    'not_computed_count': 0,
                    'status_reason_counts': {'pending_maturity': 1},
                    'coverage_ratio': 0.0,
                },
            },
            'items': [
                {
                    'feedback_id': 11,
                    'symbol': kwargs['symbol'] or '005930',
                    'coverage': [
                        {
                            'horizon_days': 5,
                            'status_reason': 'pending_maturity',
                            'decision_market_date': kwargs['market_date'].isoformat(),
                            'entry_trade_date': '2026-05-08',
                            'observed_bars_count': 1,
                            'required_bars_count': 5,
                            'earliest_computable_date': '2026-05-08',
                        }
                    ],
                    'coverage_summary': {
                        'count': 1,
                        'computed_count': 0,
                        'pending_maturity_count': 1,
                        'insufficient_price_data_count': 0,
                        'not_computed_count': 0,
                        'status_reason_counts': {'pending_maturity': 1},
                        'coverage_ratio': 0.0,
                    },
                    'evaluations': [
                        {
                            'horizon_days': 5,
                            'evaluation_status': 'pending',
                            'status_reason': 'pending_maturity',
                            'coverage': {
                                'horizon_days': 5,
                                'status_reason': 'pending_maturity',
                                'earliest_computable_date': '2026-05-08',
                            },
                        }
                    ],
                }
            ],
        }

    def generate_policy_candidates(self, **kwargs):
        self.candidate_generate_args = kwargs
        return {
            'request': {
                'market_date_from': kwargs['market_date_from'].isoformat(),
                'market_date_to': kwargs['market_date_to'].isoformat(),
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'min_sample_size': kwargs['min_sample_size'],
            },
            'summary': {'candidates_generated': 1},
            'items': [
                {
                    'candidate_version': 'signal_decider_policy_v1-h5-relax_wait_gate-abc123',
                    'policy_version': 'signal_decider_policy_v1-h5-relax_wait_gate-abc123',
                    'policy_status': 'candidate',
                }
            ],
        }

    def list_policy_candidates(self, **kwargs):
        self.candidate_list_args = kwargs
        return {
            'request': {'status': kwargs['status'], 'limit': kwargs['limit']},
            'summary': {'count': 1},
            'items': [
                {
                    'candidate_version': 'signal_decider_policy_v1-h5-relax_wait_gate-abc123',
                    'policy_version': 'signal_decider_policy_v1-h5-relax_wait_gate-abc123',
                    'policy_status': 'candidate',
                }
            ],
        }

    def list_policy_versions(self, **kwargs):
        self.policy_list_args = kwargs
        return {
            'request': {'status': kwargs['status'], 'limit': kwargs['limit']},
            'summary': {'count': 2, 'active_policy_version': 'signal_decider_policy_v1'},
            'items': [
                {'policy_version': 'signal_decider_policy_v1', 'status': 'active'},
                {'policy_version': 'signal_decider_policy_v2', 'status': 'candidate'},
            ],
        }

    def get_policy_version_detail(self, **kwargs):
        self.policy_detail_args = kwargs
        return {
            'request': {'policy_version': kwargs['policy_version']},
            'summary': {'is_active': False},
            'item': {'policy_version': kwargs['policy_version'], 'status': 'candidate'},
        }

    def activate_policy_version(self, **kwargs):
        self.policy_activate_args = kwargs
        return {
            'request': {
                'policy_version': kwargs['policy_version'],
                'activated_by': kwargs['activated_by'],
                'activation_note': kwargs['activation_note'],
            },
            'summary': {'activated': True, 'already_active': False},
            'item': {'policy_version': kwargs['policy_version'], 'status': 'active'},
        }


class FakeSignalDeciderCalendarReviewService:
    def __init__(self):
        self.calendar_args = None
        self.review_date_args = None
        self.review_detail_args = None
        self.maintenance_args = None

    def get_review_calendar(self, **kwargs):
        self.calendar_args = kwargs
        return {
            'request': {
                'month': kwargs['month'],
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {
                'name': 'signal_decider_replay_review',
                'artifact_source': 'signal_decider_calendar_review_items',
            },
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {
                'day_count': 1,
                'decision_count': 2,
                'reviewed_decision_count': 2,
                'calendar_status': 'reviewed',
            },
            'items': [
                {
                    'market_date': '2026-04-23',
                    'calendar_status': 'reviewed',
                    'decision_count': 2,
                    'reviewed_decision_count': 2,
                    'scorecard': {'target_hit_count': 1},
                }
            ],
        }

    def inspect_maintenance(self, **kwargs):
        self.maintenance_args = kwargs
        return {
            'request': kwargs,
            'summary': {'count': 1, 'latest_status': 'completed'},
            'items': [{'id': 7, 'status': 'completed'}],
        }

    def get_review_date(self, **kwargs):
        self.review_date_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {
                'name': 'signal_decider_replay_review',
                'artifact_source': 'signal_decider_calendar_review_items',
            },
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {
                'count': 1,
                'pending_review_count': 0,
                'review_status_counts': {'complete': 1},
                'evaluation_coverage': {
                    'status': 'complete',
                    'complete_count': 1,
                    'pending_count': 0,
                    'computed_count': 1,
                    'pending_maturity_count': 0,
                    'insufficient_price_data_count': 0,
                    'not_computed_count': 0,
                    'status_reason_counts': {'computed': 1},
                },
                'horizon_coverage': [
                    {
                        'horizon_days': 1,
                        'graded_count': 1,
                        'pending_count': 0,
                        'missing_count': 0,
                        'computed_count': 1,
                        'pending_maturity_count': 0,
                        'insufficient_price_data_count': 0,
                        'not_computed_count': 0,
                        'status_reason_counts': {'computed': 1},
                    },
                    {
                        'horizon_days': 5,
                        'graded_count': 1,
                        'pending_count': 0,
                        'missing_count': 0,
                        'computed_count': 1,
                        'pending_maturity_count': 0,
                        'insufficient_price_data_count': 0,
                        'not_computed_count': 0,
                        'status_reason_counts': {'computed': 1},
                    },
                ],
                'insight_counts': {'buy_should_have_taken': 1},
                'path_marker_counts': {'target_hit': 1},
            },
            'items': [
                {
                    'snapshot_id': 84,
                    'decision_run_id': 'signal-review-run-84',
                    'market_date': kwargs['market_date'].isoformat(),
                    'symbol': '005930',
                    'policy_version': 'signal_decider_policy_v1',
                    'decision': {
                        'action': 'buy',
                        'preferred_action': 'buy',
                        'next_day_open_action': 'buy',
                        'target_price': 82000,
                        'stop_loss_price': 73500,
                        'expected_return_pct': 4.2,
                        'entry_price_basis': 'next_open',
                        'suggested_holding_period': '3d',
                        'confidence_score_10': 8,
                        'conviction_score_10': 7,
                    },
                    'decision_delta': {
                        'has_previous_decision': True,
                        'transition': 'wait_to_buy',
                        'persistence_trend': 'improving',
                        'previous_recommendation': 'wait',
                        'current_recommendation': 'buy',
                    },
                    'summary': 'Momentum and fundamentals align for a tactical next-day setup.',
                    'supporting_evidence': ['Relative strength remained constructive.'],
                    'decision_risks': ['Gap-down open'],
                    'received_at': '2026-04-23T07:45:00+00:00',
                    'review': {
                        'status': 'complete',
                        'status_reason': 'computed',
                        'coverage': {
                            'horizon_days': 5,
                            'status_reason': 'computed',
                            'decision_market_date': kwargs['market_date'].isoformat(),
                            'entry_trade_date': '2026-04-24',
                            'observed_bars_count': 5,
                            'required_bars_count': 5,
                            'earliest_computable_date': '2026-04-30',
                        },
                        'insight': {'code': 'buy_should_have_taken', 'priority': 'low'},
                        'quality_verdict': 'buy_should_have_taken',
                        'quality_reason': 'Target was touched and the stop-loss was never touched inside the review window.',
                        'review_conclusion': 'Buy held up on replay.',
                        'path_outcome': 'target_hit',
                        'intraday_path_result': 'target_hit_first',
                        'narrative': {
                            'insight_code': 'buy_should_have_taken',
                            'insight_priority': 'low',
                            'headline': 'Buy held up on replay.',
                            'headline_key': 'signal_decider.review.headline.buy_should_have_taken',
                            'supporting_reasons': [
                                {
                                    'code': 'target_hit_without_stop',
                                    'message': 'Target was touched and the stop-loss was never touched inside the review window.',
                                    'message_key': 'signal_decider.review.reason.target_hit_without_stop',
                                }
                            ],
                            'counterfactual_summary': None,
                            'summary_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                        },
                        'narrative_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                        'path_analysis': {'path_marker': 'target_hit', 'trigger_sequence': 'target_hit_first'},
                        'outcome_horizons': [
                            {
                                'horizon_days': 1,
                                'close_return_pct': 0.82,
                                'grade_label': 'good_buy',
                                'status_reason': 'computed',
                                'coverage': {'horizon_days': 1, 'status_reason': 'computed'},
                            },
                            {
                                'horizon_days': 5,
                                'close_return_pct': 3.14,
                                'grade_label': 'good_buy',
                                'status_reason': 'computed',
                                'coverage': {
                                    'horizon_days': 5,
                                    'status_reason': 'computed',
                                    'earliest_computable_date': '2026-04-30',
                                },
                            },
                        ],
                    },
                }
            ],
        }

    def get_review_detail(self, **kwargs):
        self.review_detail_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'snapshot_id': kwargs['snapshot_id'],
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {
                'name': 'signal_decider_replay_review',
                'artifact_source': 'signal_decider_calendar_review_items',
            },
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {
                'review_status': 'complete',
                'insight_code': 'buy_should_have_taken',
                'insight_priority': 'low',
                'headline': 'Buy held up on replay.',
                'primary_path_marker': 'target_hit',
            },
            'item': {
                'snapshot_id': kwargs['snapshot_id'],
                'symbol': '005930',
                'review': {
                    'status': 'complete',
                    'insight': {'code': 'buy_should_have_taken', 'priority': 'low'},
                    'quality_verdict': 'buy_should_have_taken',
                    'quality_reason': 'Target was touched and the stop-loss was never touched inside the review window.',
                    'review_conclusion': 'Buy held up on replay.',
                    'path_outcome': 'target_hit',
                    'intraday_path_result': 'target_only',
                    'narrative': {
                        'insight_code': 'buy_should_have_taken',
                        'insight_priority': 'low',
                        'headline': 'Buy held up on replay.',
                        'headline_key': 'signal_decider.review.headline.buy_should_have_taken',
                        'supporting_reasons': [
                            {
                                'code': 'target_hit_without_stop',
                                'message': 'Target was touched and the stop-loss was never touched inside the review window.',
                                'message_key': 'signal_decider.review.reason.target_hit_without_stop',
                            }
                        ],
                        'counterfactual_summary': None,
                        'summary_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                    },
                    'narrative_text': 'Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.',
                    'path_analysis': {'path_marker': 'target_hit'},
                },
                'observed_price_path': [{'trade_date': '2026-04-24'}],
            },
        }


class EmptySignalDeciderCalendarReviewService(FakeSignalDeciderCalendarReviewService):
    def get_review_calendar(self, **kwargs):
        self.calendar_args = kwargs
        return {
            'request': {
                'month': kwargs['month'],
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {
                'name': 'signal_decider_replay_review',
                'artifact_source': 'signal_decider_calendar_review_items',
            },
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {'day_count': 0, 'decision_count': 0, 'reviewed_decision_count': 0, 'calendar_status': 'empty'},
            'items': [],
        }

    def get_review_date(self, **kwargs):
        self.review_date_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat(),
                'horizons': kwargs['horizons'],
                'primary_horizon_days': kwargs['primary_horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
            },
            'read_model': {
                'name': 'signal_decider_replay_review',
                'artifact_source': 'signal_decider_calendar_review_items',
            },
            'taxonomy': {'version': 'signal_decider_replay_review_v1'},
            'summary': {'count': 0, 'evaluation_coverage': {'status': 'empty', 'complete_count': 0}},
            'items': [],
        }


class FakeSignalDeciderExecutionOutcomeService:
    def __init__(self):
        self.list_args = None

    def list_outcomes(self, **kwargs):
        self.list_args = kwargs
        return {
            'request': {
                'snapshot_id': kwargs['snapshot_id'],
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] is not None else None,
                'market_date_from': kwargs['market_date_from'].isoformat() if kwargs['market_date_from'] is not None else None,
                'market_date_to': kwargs['market_date_to'].isoformat() if kwargs['market_date_to'] is not None else None,
                'symbol': kwargs['symbol'],
                'account_reference': kwargs['account_reference'],
                'policy_version': kwargs['policy_version'],
                'lifecycle_status': kwargs['lifecycle_status'],
                'exit_reason': kwargs['exit_reason'],
                'refresh': kwargs['refresh'],
                'limit': kwargs['limit'],
            },
            'summary': {'count': 1, 'closed_count': 1},
            'items': [
                {
                    'outcome_id': 7,
                    'decision_snapshot_id': kwargs['snapshot_id'] or 84,
                    'symbol': kwargs['symbol'] or '005930',
                    'account_reference': kwargs['account_reference'] or 'KIS:12345678-01',
                    'lifecycle_status': 'closed',
                    'exit_reason': 'target_hit',
                }
            ],
        }


class FakeSignalDeciderCalibrationService:
    def __init__(self):
        self.report_args = None

    def build_report(self, **kwargs):
        self.report_args = kwargs
        return {
            'request': {
                'market_date_from': kwargs['market_date_from'].isoformat(),
                'market_date_to': kwargs['market_date_to'].isoformat(),
                'symbols': kwargs['symbols'],
                'policy_version': kwargs['policy_version'],
                'horizon_days': kwargs['horizon_days'],
                'evaluation_version': kwargs['evaluation_version'],
                'min_sample_size': kwargs['min_sample_size'],
                'cluster_limit': kwargs['cluster_limit'],
            },
            'review_boundary': {
                'report_version': 'signal_decider_calibration_report_v1',
                'manual_review_only': True,
                'auto_apply_prompt_changes': False,
            },
            'summary': {
                'status': 'ready',
                'snapshots_considered': 4,
                'mature_computed_count': 3,
                'excluded_count': 1,
            },
            'exclusions': {
                'status_reason_counts': {'pending_maturity': 1},
                'items': [{'snapshot_id': 91, 'status_reason': 'pending_maturity'}],
            },
            'group_summaries': {
                'decision_actions': {
                    'label': 'Decision action buckets',
                    'items': [{'bucket_value': 'buy', 'sample_count': 2}],
                }
            },
            'condition_clusters': {
                'positive': [{'cluster_key': 'decision.confidence_bucket=high', 'sample_count': 2}],
                'negative': [{'cluster_key': 'wait_reason.code=stale_quote_orderbook', 'sample_count': 1}],
            },
            'calibration_hints': {
                'prompt_guidance': [],
                'scoring_guidance': [
                    {
                        'kind': 'scoring_guidance',
                        'manual_review_only': True,
                        'auto_apply': False,
                    }
                ],
                'operator_review': [],
            },
            'rule_candidates': {
                'schema_version': 'signal_decider_calibration_rule_candidates_v1',
                'status': 'generated',
                'summary': {
                    'candidate_count': 1,
                    'candidate_type_counts': {'prompt_warning': 1},
                    'activation_status_counts': {'needs_replay': 1},
                    'active_policy_count': 0,
                    'manual_review_only': True,
                },
                'thresholds': {'min_sample_size': 3},
                'items': [
                    {
                        'candidate_id': 'signal_decider_missed_opportunity_cluster_h5_test',
                        'candidate_type': 'prompt_warning',
                        'target_decider': 'signal-decider',
                        'predicate': {
                            'schema_version': 'signal_decider_rule_predicate_v1',
                            'operator': 'all',
                            'conditions': [
                                {
                                    'field': 'decision.action',
                                    'operator': 'eq',
                                    'value': 'wait',
                                }
                            ],
                        },
                        'reason': 'Replay-only missed opportunity warning.',
                        'support_sample_count': 3,
                        'negative_rate': 1.0,
                        'missed_opportunity_rate': 1.0,
                        'example_symbols': ['005930'],
                        'recommended_replay_config': {
                            'schema_version': 'signal_decider_rule_candidate_replay_config_v1',
                            'candidate_id': 'signal_decider_missed_opportunity_cluster_h5_test',
                            'candidate_type': 'prompt_warning',
                            'candidate_scope': 'missed_opportunity_cluster',
                            'activation_requires_replay': True,
                        },
                        'activation_status': 'needs_replay',
                    }
                ],
            },
        }


class AgentContextRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_agent_context.router)
        self.prerequisite_patcher = patch(
            'app.api.routes_agent_context.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(),
        )
        self.prerequisite_patcher.start()
        self.addCleanup(self.prerequisite_patcher.stop)

    def test_symbol_day_context_route_calls_service(self):
        fake_service = FakeAgentContextReadService()
        original_service = routes_agent_context.agent_context_read_service
        routes_agent_context.agent_context_read_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/symbol-day',
                params=[
                    ('market_date', '2026-04-08'),
                    ('symbols', '005930'),
                    ('symbols', '000660'),
                    ('fs_div', 'CFS'),
                    ('disclosures_limit', '3'),
                    ('knowledge_cutoff_at', '2026-04-08T15:15:00+09:00'),
                ],
            )
        finally:
            routes_agent_context.agent_context_read_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.last_args['fs_div'], 'CFS')
        self.assertEqual(fake_service.last_args['disclosures_limit'], 3)
        self.assertEqual(
            fake_service.last_args['knowledge_cutoff_at'],
            datetime.fromisoformat('2026-04-08T15:15:00+09:00'),
        )
        self.assertEqual(response.json()['items'][0]['symbol'], '005930')

    def test_feedback_retry_cycles_route_calls_service_and_returns_progress(self):
        fake_service = FakeAgentFeedbackRetryService()
        original_service = routes_agent_context.agent_feedback_retry_service
        routes_agent_context.agent_feedback_retry_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/feedback/retry-cycles',
                json={
                    'market_date': '2026-04-14',
                    'symbols': ['005930'],
                    'roles': ['technical-summarizer'],
                    'fs_div': 'OFS',
                    'max_cycles': 2,
                    'outer_batch_size': 10,
                    'feedback_scope_mode': 'broad_feedback_universe',
                    'knowledge_cutoff_at': '2026-04-14T15:30:00+09:00',
                    'reuse_existing': False,
                },
            )
        finally:
            routes_agent_context.agent_feedback_retry_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_config.market_date.isoformat(), '2026-04-14')
        self.assertEqual(fake_service.last_config.symbols, ('005930',))
        self.assertEqual(fake_service.last_config.roles, ('technical-summarizer',))
        self.assertEqual(fake_service.last_config.fs_div, 'OFS')
        self.assertEqual(fake_service.last_config.max_cycles, 2)
        self.assertEqual(fake_service.last_config.outer_batch_size, 10)
        self.assertEqual(fake_service.last_config.feedback_scope_mode, 'broad_feedback_universe')
        self.assertEqual(
            fake_service.last_config.knowledge_cutoff_at,
            datetime.fromisoformat('2026-04-14T15:30:00+09:00'),
        )
        self.assertFalse(fake_service.last_config.reuse_existing)
        self.assertEqual(response.json()['feedback_scope_mode'], 'broad_feedback_universe')
        self.assertEqual(response.json()['requested_symbols'], ['005930'])
        summary = response.json()['progress']['summary']
        self.assertEqual(summary['first_feedback_at'], '2026-04-14T01:00:00+00:00')
        self.assertEqual(summary['completed_feedback_per_minute'], 0.6)
        self.assertEqual(summary['estimated_completion_at'], '2026-04-14T01:05:00+00:00')

    def test_feedback_followup_route_preserves_knowledge_cutoff(self):
        fake_service = FakeAgentFeedbackRetryService()
        original_service = routes_agent_context.agent_feedback_retry_service
        routes_agent_context.agent_feedback_retry_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/feedback/followup',
                json={
                    'market_date': '2026-04-14',
                    'symbols': ['005930'],
                    'roles': ['signal-decider'],
                    'fs_div': 'OFS',
                    'knowledge_cutoff_at': '2026-04-14T15:30:00+09:00',
                    'max_cycles': 1,
                },
            )
        finally:
            routes_agent_context.agent_feedback_retry_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.last_config.knowledge_cutoff_at,
            datetime.fromisoformat('2026-04-14T15:30:00+09:00'),
        )
        self.assertEqual(fake_service.last_config.fs_div, 'OFS')
        self.assertEqual(response.json()['requested_symbols'], ['005930'])

    def test_feedback_run_routes_reject_invalid_cutoff_before_admission(self):
        client = TestClient(self.app)
        requests = (
            (
                '/agent-context/feedback/dispatch/symbols',
                {
                    'market_date': '2026-04-14',
                    'symbols': ['005930'],
                    'wait_for_completion': False,
                },
            ),
            (
                '/agent-context/feedback/retry-cycles',
                {
                    'market_date': '2026-04-14',
                    'symbols': ['005930'],
                },
            ),
            (
                '/agent-context/feedback/followup',
                {
                    'market_date': '2026-04-14',
                    'symbols': ['005930'],
                },
            ),
        )
        for endpoint, base_payload in requests:
            for cutoff, detail in (
                (
                    '2026-04-14T15:30:00',
                    'knowledge_cutoff_at must include a timezone offset',
                ),
                (
                    '2040-04-14T15:30:00+09:00',
                    'knowledge_cutoff_at must not be later than the current time',
                ),
            ):
                with self.subTest(endpoint=endpoint, cutoff=cutoff):
                    response = client.post(
                        endpoint,
                        json={
                            **base_payload,
                            'knowledge_cutoff_at': cutoff,
                        },
                    )
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(response.json()['detail'], detail)

    def test_symbol_dispatch_route_returns_waiting_gate_when_prerequisites_are_not_ready(self):
        threadpool_functions = []

        async def fake_run_in_threadpool(function, *args, **kwargs):
            threadpool_functions.append(function)
            return function(*args, **kwargs)

        with patch(
            'app.api.routes_agent_context.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(
                status='waiting',
                missing_stage_ids=['fundamentals_daily_refresh', 'quality_factors'],
            ),
        ), patch(
            'app.api.routes_agent_context.run_in_threadpool',
            side_effect=fake_run_in_threadpool,
        ):
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/feedback/dispatch/symbols',
                json={
                    'market_date': '2026-04-14',
                    'symbols': ['005930'],
                    'roles': ['technical-summarizer'],
                    'knowledge_cutoff_at': '2026-04-14T06:30:00+00:00',
                    'wait_for_completion': False,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            threadpool_functions,
            [routes_agent_context.resolve_external_news_knowledge_cutoff_for_symbols],
        )
        payload = response.json()
        self.assertTrue(payload['summary']['gated'])
        self.assertEqual(payload['summary']['status'], 'waiting')
        self.assertEqual(payload['items'], [])

    def test_symbol_dispatch_background_route_starts_durable_queued_run_and_reuses_same_run_id(self):
        fake_orchestrator = FakeBackgroundDispatchOrchestrator()
        fake_run_state_service = FakeRunStateService()
        runtime_store = RuntimeFeedbackStateStore()
        original_orchestrator = routes_agent_context.agent_feedback_orchestrator
        original_run_state_service = routes_agent_context.agent_context_feedback_run_state_service
        original_runtime_store = routes_agent_context.runtime_feedback_state_store
        routes_agent_context.agent_feedback_orchestrator = fake_orchestrator
        routes_agent_context.agent_context_feedback_run_state_service = fake_run_state_service
        routes_agent_context.runtime_feedback_state_store = runtime_store
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/feedback/dispatch/symbols',
                json={
                    'market_date': '2026-04-14',
                    'symbols': ['005930', '000660'],
                    'roles': ['eod-summarizer', 'technical-summarizer'],
                    'knowledge_cutoff_at': '2026-04-14T15:30:00+09:00',
                    'reuse_existing': False,
                    'wait_for_completion': False,
                },
            )
        finally:
            routes_agent_context.agent_feedback_orchestrator = original_orchestrator
            routes_agent_context.agent_context_feedback_run_state_service = original_run_state_service
            routes_agent_context.runtime_feedback_state_store = original_runtime_store

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        request_run_id = payload['request']['request_run_id']
        self.assertEqual(payload['summary']['status'], 'queued')
        self.assertEqual(fake_run_state_service.started[0]['run_id'], request_run_id)
        self.assertEqual(fake_run_state_service.started[0]['status'], 'queued')
        self.assertEqual(
            fake_run_state_service.started[0]['request_payload']['requested_symbols'],
            ['005930', '000660'],
        )
        self.assertEqual(
            fake_run_state_service.started[0]['request_payload']['scope'],
            'requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self.assertEqual(
            fake_run_state_service.started[0]['request_payload']['eligibility_filters'],
            ['market_risk_filter', 'piotroski_filter'],
        )
        self.assertEqual(
            fake_run_state_service.started[0]['request_payload'][
                'knowledge_cutoff_at'
            ],
            '2026-04-14T06:30:00+00:00',
        )
        self.assertEqual(
            payload['request']['knowledge_cutoff_at'],
            '2026-04-14T06:30:00+00:00',
        )
        self.assertEqual(
            payload['request']['scope'],
            'requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self.assertEqual(fake_orchestrator.calls[0]['request_run_id'], request_run_id)
        self.assertEqual(fake_orchestrator.calls[0]['symbols'], ['005930', '000660'])
        self.assertEqual(fake_orchestrator.calls[0]['roles'], ['eod-summarizer', 'technical-summarizer'])
        self.assertEqual(
            fake_orchestrator.calls[0]['knowledge_cutoff_at'],
            datetime.fromisoformat('2026-04-14T15:30:00+09:00'),
        )

    def test_symbol_dispatch_background_route_marks_queued_run_failed_when_startup_raises(self):
        fake_orchestrator = FailingBackgroundDispatchOrchestrator(RuntimeError('unknown roles during startup'))
        fake_run_state_service = FakeRunStateService()
        runtime_store = RuntimeFeedbackStateStore()
        original_orchestrator = routes_agent_context.agent_feedback_orchestrator
        original_run_state_service = routes_agent_context.agent_context_feedback_run_state_service
        original_runtime_store = routes_agent_context.runtime_feedback_state_store
        routes_agent_context.agent_feedback_orchestrator = fake_orchestrator
        routes_agent_context.agent_context_feedback_run_state_service = fake_run_state_service
        routes_agent_context.runtime_feedback_state_store = runtime_store
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/feedback/dispatch/symbols',
                json={
                    'market_date': '2026-04-14',
                    'symbols': ['005930', '000660'],
                    'roles': ['bogus-role'],
                    'reuse_existing': False,
                    'wait_for_completion': False,
                },
            )
        finally:
            routes_agent_context.agent_feedback_orchestrator = original_orchestrator
            routes_agent_context.agent_context_feedback_run_state_service = original_run_state_service
            routes_agent_context.runtime_feedback_state_store = original_runtime_store

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        request_run_id = payload['request']['request_run_id']
        self.assertEqual(payload['summary']['status'], 'queued')
        self.assertEqual(fake_run_state_service.started[0]['status'], 'queued')
        self.assertEqual(fake_run_state_service.finished[0]['run_id'], request_run_id)
        self.assertEqual(fake_run_state_service.finished[0]['status'], 'failed')
        self.assertEqual(
            fake_run_state_service.finished[0]['summary_payload']['lifecycle_stage'],
            'background_startup_failed',
        )
        self.assertIn('unknown roles during startup', fake_run_state_service.finished[0]['summary_payload']['fatal_error'])
        self.assertEqual(fake_run_state_service.get_run(run_id=request_run_id)['status'], 'failed')
        runtime_state = runtime_store.get_run(date.fromisoformat('2026-04-14'), run_id=request_run_id)
        self.assertIsNotNone(runtime_state)
        self.assertEqual(runtime_state.status, 'failed')

    def test_feedback_progress_route_passes_optional_run_id(self):
        fake_service = FakeAgentContextProgressService()
        original_service = routes_agent_context.agent_context_progress_service
        routes_agent_context.agent_context_progress_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/progress',
                params=[
                    ('market_date', '2026-04-14'),
                    ('symbols', '005930'),
                    ('run_id', 'symbol-run-2026-04-14-abc123'),
                ],
            )
        finally:
            routes_agent_context.agent_context_progress_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['run_id'], 'symbol-run-2026-04-14-abc123')
        self.assertEqual(fake_service.last_args['symbols'], ['005930'])
        self.assertEqual(response.json()['summary']['run_id'], 'symbol-run-2026-04-14-abc123')

    def test_feedback_calendar_summary_route_uses_projection_review_surface(self):
        fake_projection_service = FakeSignalDeciderCalendarReviewService()
        fake_loop_service = FakeSignalDeciderLoopService()
        original_projection_service = routes_agent_context.signal_decider_calendar_review_service
        original_loop_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_calendar_review_service = fake_projection_service
        routes_agent_context.signal_decider_loop_service = fake_loop_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/calendar',
                params=[
                    ('month', '2026-04'),
                    ('horizons', '1'),
                    ('horizons', '5'),
                    ('primary_horizon_days', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_projection_service
            routes_agent_context.signal_decider_loop_service = original_loop_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(fake_projection_service.calendar_args['month'], '2026-04')
        self.assertEqual(fake_projection_service.calendar_args['horizons'], [1, 5])
        self.assertIsNone(fake_loop_service.calendar_args)
        self.assertEqual(payload['summary']['decision_count'], 2)
        self.assertEqual(payload['summary']['reviewed_count'], 2)
        self.assertEqual(payload['summary']['source'], 'persisted_review_projection')
        self.assertEqual(payload['availability']['status'], 'ready')
        self.assertEqual(payload['items'][0]['review_status_counts']['reviewed'], 2)
        self.assertEqual(payload['items'][0]['calendar_status'], 'reviewed')

    def test_feedback_calendar_summary_route_falls_back_when_projection_is_empty(self):
        fake_projection_service = EmptySignalDeciderCalendarReviewService()
        fake_loop_service = FakeSignalDeciderLoopService()
        original_projection_service = routes_agent_context.signal_decider_calendar_review_service
        original_loop_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_calendar_review_service = fake_projection_service
        routes_agent_context.signal_decider_loop_service = fake_loop_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/calendar',
                params=[('month', '2026-04')],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_projection_service
            routes_agent_context.signal_decider_loop_service = original_loop_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(fake_projection_service.calendar_args['month'], '2026-04')
        self.assertEqual(fake_loop_service.calendar_args['month'], '2026-04')
        self.assertEqual(payload['summary']['source'], 'signal_decider_snapshot_fallback')
        self.assertEqual(payload['availability']['status'], 'fallback')
        self.assertEqual(payload['items'][0]['decision_count'], 2)

    def test_feedback_review_route_uses_projection_review_surface(self):
        fake_projection_service = FakeSignalDeciderCalendarReviewService()
        fake_loop_service = FakeSignalDeciderLoopService()
        original_projection_service = routes_agent_context.signal_decider_calendar_review_service
        original_loop_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_calendar_review_service = fake_projection_service
        routes_agent_context.signal_decider_loop_service = fake_loop_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/review',
                params=[
                    ('market_date', '2026-04-23'),
                    ('horizons', '1'),
                    ('horizons', '5'),
                    ('primary_horizon_days', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_projection_service
            routes_agent_context.signal_decider_loop_service = original_loop_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(fake_projection_service.review_date_args['market_date'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_projection_service.review_date_args['horizons'], [1, 5])
        self.assertIsNone(fake_loop_service.review_date_args)
        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['reviewed_count'], 1)
        self.assertEqual(payload['summary']['source'], 'persisted_review_projection')
        self.assertEqual(payload['availability']['status'], 'ready')
        self.assertEqual(payload['summary']['evaluation_coverage']['status_reason_counts'], {'computed': 1})
        self.assertEqual(payload['items'][0]['decision']['next_day_open_action'], 'buy')
        self.assertEqual(payload['items'][0]['decision']['decision_delta']['transition'], 'wait_to_buy')
        self.assertEqual(payload['items'][0]['decision']['evaluation']['status_reason'], 'computed')
        self.assertEqual(payload['items'][0]['decision']['evaluation']['target_stop_result'], 'target_hit_first')
        self.assertEqual(payload['items'][0]['review']['quality_reason'], 'Target was touched and the stop-loss was never touched inside the review window.')
        self.assertEqual(payload['items'][0]['review']['coverage']['status_reason'], 'computed')
        self.assertEqual(payload['items'][0]['review']['outcome_horizons'][1]['status_reason'], 'computed')
        self.assertEqual(len(payload['items'][0]['review']['outcome_horizons']), 2)

    def test_feedback_review_route_falls_back_when_projection_is_empty(self):
        fake_projection_service = EmptySignalDeciderCalendarReviewService()
        fake_loop_service = FakeSignalDeciderLoopService()
        original_projection_service = routes_agent_context.signal_decider_calendar_review_service
        original_loop_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_calendar_review_service = fake_projection_service
        routes_agent_context.signal_decider_loop_service = fake_loop_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/review',
                params=[('market_date', '2026-04-23')],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_projection_service
            routes_agent_context.signal_decider_loop_service = original_loop_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(fake_projection_service.review_date_args['market_date'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_loop_service.review_date_args['market_date'], date.fromisoformat('2026-04-23'))
        self.assertEqual(payload['summary']['source'], 'signal_decider_snapshot_fallback')
        self.assertEqual(payload['availability']['status'], 'fallback')
        self.assertEqual(payload['items'][0]['symbol'], '005930')

    def test_symbol_day_context_route_maps_validation_error_to_400(self):
        original_service = routes_agent_context.agent_context_read_service
        routes_agent_context.agent_context_read_service = ErrorAgentContextReadService()
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/symbol-day',
                params=[('market_date', '2026-04-08'), ('symbols', '')],
            )
        finally:
            routes_agent_context.agent_context_read_service = original_service

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'At least one symbol is required')

    def test_submit_agent_feedback_route_calls_service(self):
        fake_service = FakeAgentContextFeedbackService()
        original_service = routes_agent_context.agent_context_feedback_service
        routes_agent_context.agent_context_feedback_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/feedback',
                json={
                    'market_date': '2026-04-14',
                    'symbol': '005930',
                    'agent_name': 'openclaw',
                    'agent_version': 'daily-v1',
                    'run_id': 'run-123',
                    'summary': {'headline': 'watch breakout continuation'},
                    'feedback': {'confidence': 0.72, 'risk_notes': ['earnings volatility']},
                    'context': {'source_route': '/agent-context/symbol-day'},
                },
            )
        finally:
            routes_agent_context.agent_context_feedback_service = original_service

        self.assertEqual(response.status_code, 201)
        self.assertEqual(fake_service.submitted['market_date'].isoformat(), '2026-04-14')
        self.assertEqual(fake_service.submitted['symbol'], '005930')
        self.assertEqual(fake_service.submitted['agent_name'], 'openclaw')
        self.assertEqual(fake_service.submitted['summary']['headline'], 'watch breakout continuation')
        self.assertTrue(response.json()['accepted'])
        self.assertEqual(response.json()['item']['id'], 7)

    def test_submit_agent_feedback_route_rejects_empty_payload(self):
        client = TestClient(self.app)
        response = client.post(
            '/agent-context/feedback',
            json={
                'market_date': '2026-04-14',
                'symbol': '005930',
                'summary': {},
                'feedback': {},
            },
        )

        self.assertEqual(response.status_code, 422)

    def test_list_agent_feedback_route_calls_service(self):
        fake_service = FakeAgentContextFeedbackService()
        original_service = routes_agent_context.agent_context_feedback_service
        routes_agent_context.agent_context_feedback_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback',
                params=[
                    ('market_date', '2026-04-14'),
                    ('symbols', '005930,000660'),
                    ('agent_name', 'openclaw'),
                    ('limit', '10'),
                ],
            )
        finally:
            routes_agent_context.agent_context_feedback_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.list_args['market_date'].isoformat(), '2026-04-14')
        self.assertEqual(fake_service.list_args['symbols'], ['005930,000660'])
        self.assertEqual(fake_service.list_args['agent_name'], 'openclaw')
        self.assertEqual(fake_service.list_args['limit'], 10)
        self.assertEqual(response.json()['items'][0]['symbol'], '005930')

    def test_agent_feedback_data_gap_summary_route_calls_service(self):
        fake_service = FakeAgentContextMissingDataReportService()
        original_service = routes_agent_context.agent_context_missing_data_report_service
        routes_agent_context.agent_context_missing_data_report_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/data-gap-summary',
                params=[
                    ('market_date_from', '2026-05-05'),
                    ('market_date_to', '2026-05-06'),
                    ('symbols', '005930,000660'),
                    ('agent_name', 'technical-summarizer'),
                    ('categories', 'factor_score_coverage'),
                ],
            )
        finally:
            routes_agent_context.agent_context_missing_data_report_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['market_date_from'], date.fromisoformat('2026-05-05'))
        self.assertEqual(fake_service.last_args['market_date_to'], date.fromisoformat('2026-05-06'))
        self.assertEqual(fake_service.last_args['symbols'], ['005930,000660'])
        self.assertEqual(fake_service.last_args['agent_name'], 'technical-summarizer')
        self.assertEqual(fake_service.last_args['categories'], ['factor_score_coverage'])
        self.assertEqual(response.json()['summary']['feedback_rows_matched'], 2)

    def test_agent_feedback_failure_summary_route_calls_service(self):
        fake_service = FakeAgentContextFeedbackFailureService()
        original_service = routes_agent_context.agent_context_feedback_failure_service
        routes_agent_context.agent_context_feedback_failure_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/failure-summary',
                params=[
                    ('market_date_from', '2026-05-05'),
                    ('market_date_to', '2026-05-06'),
                    ('run_id', 'symbol-run-2026-05-05-auth'),
                    ('agent_name', 'openclaw-gateway'),
                    ('failure_kind', 'openclaw_auth_rejected'),
                    ('limit', '25'),
                ],
            )
        finally:
            routes_agent_context.agent_context_feedback_failure_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['market_date_from'], date.fromisoformat('2026-05-05'))
        self.assertEqual(fake_service.last_args['market_date_to'], date.fromisoformat('2026-05-06'))
        self.assertEqual(fake_service.last_args['run_id'], 'symbol-run-2026-05-05-auth')
        self.assertEqual(fake_service.last_args['agent_name'], 'openclaw-gateway')
        self.assertEqual(fake_service.last_args['failure_kind'], 'openclaw_auth_rejected')
        self.assertEqual(fake_service.last_args['limit'], 25)
        self.assertEqual(response.json()['summary']['group_count'], 1)

    def test_agent_feedback_decider_coverage_route_calls_service(self):
        fake_service = FakeDeciderCoverageReportService()
        original_service = routes_agent_context.decider_coverage_report_service
        routes_agent_context.decider_coverage_report_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/decider-coverage',
                params=[
                    ('market_date', '2026-05-08'),
                    ('run_id', 'symbol-run-2026-05-08-abc123'),
                    ('symbols', '005930,000660'),
                    ('agent_names', 'signal-decider'),
                    ('agent_names', 'short-term-decider'),
                ],
            )
        finally:
            routes_agent_context.decider_coverage_report_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['market_date'], date.fromisoformat('2026-05-08'))
        self.assertEqual(fake_service.last_args['run_id'], 'symbol-run-2026-05-08-abc123')
        self.assertEqual(fake_service.last_args['symbols'], ['005930,000660'])
        self.assertEqual(fake_service.last_args['agent_names'], ['signal-decider', 'short-term-decider'])
        self.assertEqual(response.json()['summary']['contract_regressions']['status'], 'alert')

    def test_compute_decider_evaluations_route_calls_service(self):
        fake_service = FakeDeciderEvaluationService()
        original_service = routes_agent_context.decider_post_decision_evaluation_service
        routes_agent_context.decider_post_decision_evaluation_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/decider/evaluations/compute',
                json={
                    'market_date_from': '2026-04-20',
                    'market_date_to': '2026-04-23',
                    'agent_names': ['signal-decider', 'short-term-decider'],
                    'symbols': ['005930'],
                    'window_days': 5,
                },
            )
        finally:
            routes_agent_context.decider_post_decision_evaluation_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date_from'].isoformat(), '2026-04-20')
        self.assertEqual(fake_service.compute_args['market_date_to'].isoformat(), '2026-04-23')
        self.assertEqual(fake_service.compute_args['agent_names'], ['signal-decider', 'short-term-decider'])
        self.assertEqual(fake_service.compute_args['symbols'], ['005930'])
        self.assertEqual(fake_service.compute_args['window_days'], 5)
        self.assertEqual(response.json()['summary']['evaluations_upserted'], 2)

    def test_list_decider_evaluations_route_calls_service(self):
        fake_service = FakeDeciderEvaluationService()
        original_service = routes_agent_context.decider_post_decision_evaluation_service
        routes_agent_context.decider_post_decision_evaluation_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/decider/evaluations',
                params=[
                    ('market_date', '2026-04-23'),
                    ('agent_names', 'signal-decider'),
                    ('agent_names', 'short-term-decider'),
                    ('symbols', '005930'),
                    ('outcome_labels', 'target_hit_before_stop'),
                    ('window_days', '5'),
                    ('limit', '25'),
                ],
            )
        finally:
            routes_agent_context.decider_post_decision_evaluation_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.list_args['market_date'].isoformat(), '2026-04-23')
        self.assertEqual(fake_service.list_args['agent_names'], ['signal-decider', 'short-term-decider'])
        self.assertEqual(fake_service.list_args['symbols'], ['005930'])
        self.assertEqual(fake_service.list_args['outcome_labels'], ['target_hit_before_stop'])
        self.assertEqual(fake_service.list_args['window_days'], 5)
        self.assertEqual(fake_service.list_args['limit'], 25)
        self.assertEqual(response.json()['items'][0]['outcome_label'], 'target_hit_before_stop')

    def test_inspect_technical_summarizer_feedback_route_calls_service(self):
        fake_service = FakeAgentContextFeedbackService()
        original_service = routes_agent_context.agent_context_feedback_service
        routes_agent_context.agent_context_feedback_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/technical-summaries',
                params={
                    'market_date': '2026-04-21',
                    'symbol': '005930',
                    'limit': '15',
                },
            )
        finally:
            routes_agent_context.agent_context_feedback_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.list_args,
            {
                'market_date': date.fromisoformat('2026-04-21'),
                'symbols': ['005930'],
                'agent_name': 'technical-summarizer',
                'limit': 15,
            },
        )
        self.assertEqual(response.json()['request']['agent_name'], 'technical-summarizer')
        self.assertEqual(response.json()['items'][0]['symbol'], '005930')

    def test_inspect_technical_summarizer_feedback_route_uses_default_limit(self):
        fake_service = FakeAgentContextFeedbackService()
        original_service = routes_agent_context.agent_context_feedback_service
        routes_agent_context.agent_context_feedback_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/technical-summaries',
                params={'market_date': '2026-04-21'},
            )
        finally:
            routes_agent_context.agent_context_feedback_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.list_args['market_date'], date.fromisoformat('2026-04-21'))
        self.assertIsNone(fake_service.list_args['symbols'])
        self.assertEqual(fake_service.list_args['agent_name'], 'technical-summarizer')
        self.assertEqual(fake_service.list_args['limit'], 50)

    def test_inspect_eod_summarizer_feedback_route_returns_all_items(self):
        fake_service = FakeAgentContextEodSummariesService()
        original_service = routes_agent_context.agent_context_eod_summaries_service
        routes_agent_context.agent_context_eod_summaries_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/eod-summaries',
                params={'market_date': '2026-04-21'},
            )
        finally:
            routes_agent_context.agent_context_eod_summaries_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': date.fromisoformat('2026-04-21'), 'symbol': None},
        )
        self.assertEqual(response.json()['summary']['entries'], 1)
        self.assertEqual(response.json()['items'][0]['agent_name'], 'eod-summarizer')

    def test_inspect_eod_summarizer_feedback_route_returns_single_item(self):
        fake_service = FakeAgentContextEodSummariesService()
        original_service = routes_agent_context.agent_context_eod_summaries_service
        routes_agent_context.agent_context_eod_summaries_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/feedback/eod-summaries',
                params={'market_date': '2026-04-21', 'symbol': '005930'},
            )
        finally:
            routes_agent_context.agent_context_eod_summaries_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': date.fromisoformat('2026-04-21'), 'symbol': '005930'},
        )
        self.assertEqual(response.json()['summary']['requested_symbol'], '005930')
        self.assertEqual(response.json()['item']['symbol'], '005930')

    def test_signal_decider_decisions_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/decisions',
                params={'market_date': '2026-04-23', 'symbol': '005930', 'limit': '10'},
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.decision_args['market_date'], date.fromisoformat('2026-04-23'))
        self.assertIsNone(fake_service.decision_args['market_date_from'])
        self.assertIsNone(fake_service.decision_args['market_date_to'])
        self.assertEqual(fake_service.decision_args['symbol'], '005930')
        self.assertEqual(fake_service.decision_args['limit'], 10)
        self.assertEqual(response.json()['items'][0]['policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(response.json()['items'][0]['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(response.json()['items'][0]['decision_delta']['transition'], 'wait_to_wait')

    def test_signal_decider_decisions_route_accepts_search_filters(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/decisions',
                params=[
                    ('market_date_from', '2026-04-20'),
                    ('market_date_to', '2026-04-23'),
                    ('action', 'wait'),
                    ('policy_version', 'signal_decider_policy_v1'),
                    ('verdict', 'missed_opportunity_after_wait'),
                    ('has_missed_opportunity', 'true'),
                    ('horizons', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                    ('limit', '25'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(fake_service.decision_args['market_date'])
        self.assertEqual(fake_service.decision_args['market_date_from'], date.fromisoformat('2026-04-20'))
        self.assertEqual(fake_service.decision_args['market_date_to'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_service.decision_args['action'], 'wait')
        self.assertEqual(fake_service.decision_args['policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(fake_service.decision_args['verdict'], 'missed_opportunity_after_wait')
        self.assertTrue(fake_service.decision_args['has_missed_opportunity'])
        self.assertEqual(fake_service.decision_args['horizons'], [5])
        self.assertEqual(fake_service.decision_args['evaluation_version'], 'signal_decider_eval_v1')

    def test_signal_decider_decision_detail_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/decisions/84',
                params=[('horizons', '1'), ('horizons', '5'), ('evaluation_version', 'signal_decider_eval_v1')],
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.detail_args['snapshot_id'], 84)
        self.assertEqual(fake_service.detail_args['horizons'], [1, 5])
        self.assertEqual(fake_service.detail_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(response.json()['item']['snapshot_id'], 84)
        self.assertEqual(response.json()['item']['explanation']['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertEqual(response.json()['item']['decision_delta']['transition'], 'wait_to_wait')

    def test_signal_decider_review_calendar_route_calls_service(self):
        fake_service = FakeSignalDeciderCalendarReviewService()
        original_service = routes_agent_context.signal_decider_calendar_review_service
        routes_agent_context.signal_decider_calendar_review_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/calendar',
                params=[
                    ('month', '2026-04'),
                    ('horizons', '1'),
                    ('horizons', '5'),
                    ('primary_horizon_days', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.calendar_args['month'], '2026-04')
        self.assertEqual(fake_service.calendar_args['horizons'], [1, 5])
        self.assertEqual(fake_service.calendar_args['primary_horizon_days'], 5)
        self.assertEqual(fake_service.calendar_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(response.json()['summary']['day_count'], 1)
        self.assertEqual(response.json()['items'][0]['calendar_status'], 'reviewed')
        self.assertEqual(response.json()['items'][0]['scorecard']['target_hit_count'], 1)

    def test_signal_decider_review_calendar_route_maps_zero_primary_horizon_to_400(self):
        client = TestClient(self.app)
        response = client.get(
            '/agent-context/signal-decider/calendar',
            params=[
                ('month', '2026-04'),
                ('primary_horizon_days', '0'),
            ],
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'primary_horizon_days must be drawn from [1, 3, 5, 10]')

    def test_signal_decider_review_calendar_maintenance_route_calls_service(self):
        fake_service = FakeSignalDeciderCalendarReviewService()
        original_service = routes_agent_context.signal_decider_calendar_review_service
        routes_agent_context.signal_decider_calendar_review_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/calendar/maintenance',
                params={'evaluation_version': 'signal_decider_eval_v1', 'limit': '5'},
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.maintenance_args, {'evaluation_version': 'signal_decider_eval_v1', 'limit': 5})
        self.assertEqual(response.json()['summary']['latest_status'], 'completed')
        self.assertEqual(response.json()['items'][0]['id'], 7)

    def test_signal_decider_review_date_route_calls_service(self):
        fake_service = FakeSignalDeciderCalendarReviewService()
        original_service = routes_agent_context.signal_decider_calendar_review_service
        routes_agent_context.signal_decider_calendar_review_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/calendar/2026-04-23',
                params=[
                    ('horizons', '1'),
                    ('horizons', '3'),
                    ('horizons', '5'),
                    ('primary_horizon_days', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.review_date_args['market_date'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_service.review_date_args['horizons'], [1, 3, 5])
        self.assertEqual(fake_service.review_date_args['primary_horizon_days'], 5)
        self.assertEqual(fake_service.review_date_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(response.json()['items'][0]['review']['insight']['code'], 'buy_should_have_taken')
        self.assertEqual(response.json()['items'][0]['decision_delta']['transition'], 'wait_to_buy')
        self.assertEqual(response.json()['items'][0]['review']['quality_verdict'], 'buy_should_have_taken')
        self.assertEqual(response.json()['items'][0]['review']['narrative']['headline'], 'Buy held up on replay.')

    def test_signal_decider_review_detail_route_calls_service(self):
        fake_service = FakeSignalDeciderCalendarReviewService()
        original_service = routes_agent_context.signal_decider_calendar_review_service
        routes_agent_context.signal_decider_calendar_review_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/calendar/2026-04-23/snapshots/84',
                params=[
                    ('horizons', '1'),
                    ('horizons', '5'),
                    ('primary_horizon_days', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_calendar_review_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.review_detail_args['market_date'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_service.review_detail_args['snapshot_id'], 84)
        self.assertEqual(fake_service.review_detail_args['horizons'], [1, 5])
        self.assertEqual(fake_service.review_detail_args['primary_horizon_days'], 5)
        self.assertEqual(fake_service.review_detail_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(response.json()['summary']['insight_priority'], 'low')
        self.assertEqual(response.json()['item']['review']['path_outcome'], 'target_hit')
        self.assertEqual(response.json()['item']['observed_price_path'][0]['trade_date'], '2026-04-24')

    def test_signal_decider_execution_outcomes_route_calls_service(self):
        fake_service = FakeSignalDeciderExecutionOutcomeService()
        original_service = routes_agent_context.signal_decider_execution_outcome_service
        routes_agent_context.signal_decider_execution_outcome_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/execution-outcomes',
                params=[
                    ('snapshot_id', '84'),
                    ('market_date_from', '2026-04-20'),
                    ('market_date_to', '2026-04-23'),
                    ('symbol', '005930'),
                    ('account_reference', 'KIS:12345678-01'),
                    ('policy_version', 'signal_decider_policy_v1'),
                    ('lifecycle_status', 'closed'),
                    ('exit_reason', 'target_hit'),
                    ('refresh', 'true'),
                    ('limit', '25'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_execution_outcome_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.list_args['snapshot_id'], 84)
        self.assertEqual(fake_service.list_args['market_date_from'], date.fromisoformat('2026-04-20'))
        self.assertEqual(fake_service.list_args['market_date_to'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_service.list_args['symbol'], '005930')
        self.assertEqual(fake_service.list_args['account_reference'], 'KIS:12345678-01')
        self.assertEqual(fake_service.list_args['policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(fake_service.list_args['lifecycle_status'], 'closed')
        self.assertEqual(fake_service.list_args['exit_reason'], 'target_hit')
        self.assertTrue(fake_service.list_args['refresh'])
        self.assertEqual(fake_service.list_args['limit'], 25)
        self.assertEqual(response.json()['items'][0]['lifecycle_status'], 'closed')

    def test_signal_decider_diagnostics_summary_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/diagnostics/summary',
                params=[
                    ('market_date_from', '2026-04-20'),
                    ('market_date_to', '2026-04-23'),
                    ('symbols', '005930'),
                    ('symbols', '000660'),
                    ('action', 'buy'),
                    ('policy_version', 'signal_decider_policy_v1'),
                    ('horizons', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                    ('contributor_limit', '15'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.aggregate_diagnostics_args['market_date_from'],
            date.fromisoformat('2026-04-20'),
        )
        self.assertEqual(
            fake_service.aggregate_diagnostics_args['market_date_to'],
            date.fromisoformat('2026-04-23'),
        )
        self.assertEqual(fake_service.aggregate_diagnostics_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.aggregate_diagnostics_args['action'], 'buy')
        self.assertEqual(fake_service.aggregate_diagnostics_args['policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(fake_service.aggregate_diagnostics_args['horizons'], [5])
        self.assertEqual(fake_service.aggregate_diagnostics_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(fake_service.aggregate_diagnostics_args['contributor_limit'], 15)
        self.assertEqual(response.json()['contributors']['factors'][0]['key'], 'technical.breakout_20d')

    def test_signal_decider_evaluation_compute_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/signal-decider/evaluations/compute',
                json={
                    'market_date_from': '2026-04-20',
                    'market_date_to': '2026-04-23',
                    'symbols': ['005930'],
                    'horizons': [1, 5],
                    'force_recompute': True,
                },
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date_from'], date.fromisoformat('2026-04-20'))
        self.assertEqual(fake_service.compute_args['market_date_to'], date.fromisoformat('2026-04-23'))
        self.assertEqual(fake_service.compute_args['symbols'], ['005930'])
        self.assertEqual(fake_service.compute_args['horizons'], [1, 5])
        self.assertTrue(fake_service.compute_args['force_recompute'])
        self.assertEqual(response.json()['summary']['evaluations_upserted'], 4)

    def test_signal_decider_evaluations_route_preserves_coverage_metadata(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/evaluations',
                params=[
                    ('market_date', '2026-04-29'),
                    ('symbol', '051910'),
                    ('horizons', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                    ('limit', '25'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.evaluation_args['market_date'], date.fromisoformat('2026-04-29'))
        self.assertEqual(fake_service.evaluation_args['symbol'], '051910')
        self.assertEqual(fake_service.evaluation_args['horizons'], [5])
        self.assertEqual(fake_service.evaluation_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(fake_service.evaluation_args['limit'], 25)
        payload = response.json()
        self.assertEqual(payload['summary']['coverage']['pending_maturity_count'], 1)
        self.assertEqual(payload['items'][0]['coverage'][0]['status_reason'], 'pending_maturity')
        self.assertEqual(payload['items'][0]['coverage'][0]['earliest_computable_date'], '2026-05-08')
        self.assertEqual(payload['items'][0]['evaluations'][0]['status_reason'], 'pending_maturity')

    def test_signal_decider_calibration_route_calls_service(self):
        fake_service = FakeSignalDeciderCalibrationService()
        original_service = routes_agent_context.signal_decider_calibration_service
        routes_agent_context.signal_decider_calibration_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/calibration',
                params=[
                    ('market_date_from', '2026-04-20'),
                    ('market_date_to', '2026-04-29'),
                    ('symbols', '005930'),
                    ('symbols', '000660'),
                    ('policy_version', 'signal_decider_policy_v1'),
                    ('horizon_days', '5'),
                    ('evaluation_version', 'signal_decider_eval_v1'),
                    ('min_sample_size', '2'),
                    ('cluster_limit', '5'),
                ],
            )
        finally:
            routes_agent_context.signal_decider_calibration_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.report_args['market_date_from'], date.fromisoformat('2026-04-20'))
        self.assertEqual(fake_service.report_args['market_date_to'], date.fromisoformat('2026-04-29'))
        self.assertEqual(fake_service.report_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.report_args['policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(fake_service.report_args['horizon_days'], 5)
        self.assertEqual(fake_service.report_args['evaluation_version'], 'signal_decider_eval_v1')
        self.assertEqual(fake_service.report_args['min_sample_size'], 2)
        self.assertEqual(fake_service.report_args['cluster_limit'], 5)
        payload = response.json()
        self.assertEqual(payload['summary']['status'], 'ready')
        self.assertEqual(payload['exclusions']['status_reason_counts']['pending_maturity'], 1)
        self.assertEqual(payload['condition_clusters']['positive'][0]['cluster_key'], 'decision.confidence_bucket=high')
        self.assertEqual(payload['calibration_hints']['scoring_guidance'][0]['manual_review_only'], True)
        self.assertEqual(payload['rule_candidates']['items'][0]['activation_status'], 'needs_replay')

    def test_signal_decider_policy_candidate_generation_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/signal-decider/policy-candidates/generate',
                json={
                    'market_date_from': '2026-04-20',
                    'market_date_to': '2026-04-23',
                    'symbols': ['005930'],
                    'horizons': [1, 3, 5, 10],
                    'primary_horizon_days': 5,
                    'min_sample_size': 2,
                },
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.candidate_generate_args['market_date_from'],
            date.fromisoformat('2026-04-20'),
        )
        self.assertEqual(fake_service.candidate_generate_args['primary_horizon_days'], 5)
        self.assertEqual(fake_service.candidate_generate_args['min_sample_size'], 2)
        self.assertEqual(
            response.json()['items'][0]['candidate_version'],
            'signal_decider_policy_v1-h5-relax_wait_gate-abc123',
        )

    def test_signal_decider_decision_policy_candidate_generation_alias_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/signal-decider/decision-policies/candidates/generate',
                json={
                    'market_date_from': '2026-04-20',
                    'market_date_to': '2026-04-23',
                    'symbols': ['005930'],
                    'horizons': [5],
                    'primary_horizon_days': 5,
                    'min_sample_size': 2,
                },
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.candidate_generate_args['primary_horizon_days'], 5)
        self.assertEqual(response.json()['items'][0]['policy_status'], 'candidate')

    def test_signal_decider_decision_policies_list_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/agent-context/signal-decider/decision-policies',
                params={'status': 'candidate', 'limit': '25'},
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.policy_list_args, {'status': 'candidate', 'limit': 25})
        self.assertEqual(response.json()['summary']['active_policy_version'], 'signal_decider_policy_v1')
        self.assertEqual(response.json()['items'][1]['status'], 'candidate')

    def test_signal_decider_decision_policy_detail_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/agent-context/signal-decider/decision-policies/signal_decider_policy_v2')
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.policy_detail_args, {'policy_version': 'signal_decider_policy_v2'})
        self.assertEqual(response.json()['item']['policy_version'], 'signal_decider_policy_v2')

    def test_signal_decider_decision_policy_activation_route_calls_service(self):
        fake_service = FakeSignalDeciderLoopService()
        replay_evidence = {
            'schema_version': 'signal_decider_policy_activation_evidence_v1',
            'candidate_policy_version': 'signal_decider_policy_v2',
            'replay_artifact': {'schema_version': 'decider_replay_v1'},
        }
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/signal-decider/decision-policies/signal_decider_policy_v2/activate',
                json={
                    'activated_by': 'human.reviewer',
                    'activation_note': 'Approved after diagnostics review.',
                    'replay_evidence': replay_evidence,
                },
            )
        finally:
            routes_agent_context.signal_decider_loop_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.policy_activate_args,
            {
                'policy_version': 'signal_decider_policy_v2',
                'activated_by': 'human.reviewer',
                'activation_note': 'Approved after diagnostics review.',
                'replay_evidence': replay_evidence,
            },
        )
        self.assertEqual(response.json()['item']['status'], 'active')
