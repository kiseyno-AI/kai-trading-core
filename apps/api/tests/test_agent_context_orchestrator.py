from contextlib import contextmanager
from datetime import date
import json
from pathlib import Path
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import AgentFeedbackOrchestrator, OpenClawAgentClient
from app.agent_context.orchestrator_helpers import (
    build_signal_decider_payload,
    coerce_decoded_batch_response,
    correlate_decoded_batch_response,
    repair_json_tail,
    trim_batch_item_for_prompt,
)
from app.agent_context.prerequisite_service import FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
from app.agent_context.runtime_state import RuntimeFeedbackRunState, RuntimeFeedbackStateStore


def _batch_result(symbol: str) -> dict:
    return {
        'decoded': {
            'items': [
                {
                    'symbol': symbol,
                    'feedback': {
                        'role': 'eod-summarizer',
                        'summary': f'{symbol} summary',
                        'confidence': 0.7,
                        'risk_flags': [],
                        'details': {},
                        'improvement_notes': [],
                    },
                }
            ]
        },
        'attempt': 1,
        'attempt_id': f'dispatch:{symbol}:attempt-1',
        'dispatch_id': f'dispatch:{symbol}',
        'request_run_id': 'request-run-test',
        'market_date': '2026-04-14',
        'batch_symbols': [symbol],
        'slot': 1,
        'started_at': '2026-04-14T01:00:00+00:00',
        'completed_at': '2026-04-14T01:01:00+00:00',
        'session_ref': {'session_key': 'feedback/eod-summarizer/slot-01'},
    }


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


class RepairJsonTailTests(unittest.TestCase):
    def test_build_signal_decider_payload_promotes_pre_decider_enrichment_context(self):
        pre_decider_context = {
            'schema_version': 'pre_decider_enrichment_v1',
            'status': 'available',
            'quote_orderbook_snapshot': {'status': 'available', 'current_price': 71550},
            'trading_value': {'status': 'available', 'source': 'raw', 'trading_value_krw': 9870000000},
            'flow_short_sector_context': {'status': 'available'},
        }

        payload = build_signal_decider_payload(
            item={
                'symbol': '005930',
                'market_date': '2026-04-14',
                'pre_decider_enrichment_context': pre_decider_context,
                'scores': {},
                'factor_summary': {},
                'liquidity_context': {},
                'decider_data_quality_context': {},
            },
            market_date=date(2026, 4, 14),
            request_run_id='request-run-test',
            market_context={},
            upstream_summaries={},
            upstream_traceability={},
            signal_decider_agent_name='short-term-decider',
        )

        self.assertEqual(payload['pre_decider_enrichment_context'], pre_decider_context)
        self.assertNotIn('pre_decider_enrichment_context', payload['symbol_day_context'])
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'pre_decider_enrichment_context_authoritative'
            ]
        )

    def test_coerce_decoded_batch_response_unwraps_batch_and_item_level_feedback(self):
        decoded = {
            'batch': {
                'items': [
                    {
                        'symbol': '005930',
                        'tactical_action': 'watch_for_trigger',
                        'summary': '확인 필요',
                    }
                ]
            }
        }

        result = coerce_decoded_batch_response(decoded)

        self.assertIn('items', result)
        self.assertTrue(result['_coerced_from_batch_key'])
        self.assertEqual(result['items'][0]['symbol'], '005930')
        self.assertEqual(result['items'][0]['feedback']['tactical_action'], 'watch_for_trigger')
        self.assertTrue(result['items'][0]['_coerced_feedback_from_item'])

    def test_repair_json_tail_drops_extra_closing_brace_before_items_close(self):
        malformed = '{"items":[{"symbol":"014190","feedback":{"summary":"ok","details":{},"improvement_notes":[]}}}]}'
        repaired = repair_json_tail(malformed)
        self.assertIsNotNone(repaired)
        parsed = json.loads(repaired)
        self.assertEqual(parsed['items'][0]['symbol'], '014190')

    def test_decode_candidate_json_recovers_tail_mismatch(self):
        malformed = '{"items":[{"symbol":"014190","feedback":{"role":"signal-decider","summary":"ok","confidence":0.6,"risk_flags":[],"details":{},"improvement_notes":[]}}}]}'
        client = OpenClawAgentClient()
        decoded, failure = client._decode_candidate_json(malformed)
        self.assertIsNone(failure)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded['items'][0]['symbol'], '014190')

    def test_repair_json_tail_does_not_hide_mid_object_syntax_errors(self):
        malformed = '{"items":[{"symbol":"014190" "feedback":{}}]}'
        repaired = repair_json_tail(malformed)
        self.assertIsNone(repaired)

    def test_trim_batch_item_for_prompt_preserves_compact_factor_summary(self):
        trimmed = trim_batch_item_for_prompt(
            {
                'symbol': '005930',
                'factor_summary': {
                    'requested_market_date': '2026-04-14',
                    'factor_scores': {
                        'value': {'status': 'available', 'score': 0.92},
                        'beta': {'status': 'not_eligible', 'source_status': 'watchlist'},
                    },
                    'signal_features': {
                        'status': 'available',
                        'row_kind': 'direct_plus_fallback',
                        'fallback_groups_present': ['price_action', 'factor_scores'],
                    },
                },
                'liquidity_context': {
                    'status': 'derived',
                    'turnover': {
                        'status': 'derived',
                        'turnover_krw': 8520000000,
                        'turnover_source': 'derived',
                    },
                    'intraday': {
                        'status': 'partial',
                        'quote_spread_points': 100,
                    },
                },
                'decider_data_quality_context': {
                    'status': 'partial',
                    'decision_priority': {
                        'core_execution': {'status': 'available', 'blocking_by_default': True},
                        'auxiliary_context': {'status': 'partial', 'blocking_by_default': False},
                    },
                    'blocks': {
                        'liquidity_turnover': {
                            'status': 'partial',
                            'turnover': {'status': 'partial', 'turnover_source': 'derived'},
                            'decision_guidance': {'decision_tier': 'core_execution'},
                        },
                        'disclosures_news': {
                            'status': 'provider_error',
                            'decision_guidance': {
                                'decision_tier': 'auxiliary_context',
                                'empty_is_neutral_by_default': True,
                            },
                            'providers': {
                                'external_news': {'status': 'provider_error', 'reason': 'naver_search_credentials_missing'}
                            },
                        },
                    },
                },
                'persisted_factor_rows': {
                    'value': {'value_score': 0.92, 'value_status': 'scored'},
                },
            }
        )

        self.assertEqual(trimmed['factor_summary']['factor_scores']['value']['status'], 'available')
        self.assertEqual(trimmed['factor_summary']['factor_scores']['beta']['status'], 'not_eligible')
        self.assertEqual(trimmed['factor_summary']['signal_features']['row_kind'], 'direct_plus_fallback')
        self.assertEqual(trimmed['liquidity_context']['turnover']['status'], 'derived')
        self.assertEqual(trimmed['liquidity_context']['intraday']['quote_spread_points'], 100)
        self.assertEqual(trimmed['decider_data_quality_context']['status'], 'partial')
        self.assertEqual(
            trimmed['decider_data_quality_context']['decision_priority']['core_execution']['status'],
            'available',
        )
        self.assertEqual(
            trimmed['decider_data_quality_context']['blocks']['disclosures_news']['providers']['external_news']['status'],
            'provider_error',
        )
        self.assertEqual(
            trimmed['decider_data_quality_context']['blocks']['disclosures_news']['decision_guidance']['decision_tier'],
            'auxiliary_context',
        )


class FakeFuture:
    def __init__(self, *, result, cancel_result):
        self._result = result
        self.cancel_result = cancel_result
        self.cancel_calls = 0

    def cancel(self):
        self.cancel_calls += 1
        return self.cancel_result

    def result(self):
        return self._result


class FakeThreadPoolExecutor:
    def __init__(self, futures):
        self.futures = list(futures)
        self.submissions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def submit(self, fn, **kwargs):
        self.submissions.append(kwargs)
        return self.futures[len(self.submissions) - 1]


class FakeAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        return {
            'items': [
                {'symbol': '005930'},
                {'symbol': '000660'},
            ]
        }


class FakeOpenClawClient:
    def dispatch_batch(self, **kwargs):
        raise AssertionError('fake executor should not call dispatch_batch')


class DynamicFakeAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        return {'items': [{'symbol': symbol} for symbol in kwargs['symbols']]}


class RecordingAgentContextReadService:
    def __init__(self):
        self.calls = []

    def get_symbol_day_context(self, **kwargs):
        self.calls.append(kwargs)
        return {'items': [{'symbol': symbol} for symbol in kwargs['symbols']]}


class DeciderReadyAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        return {
            'summary': {
                'context_completeness': {
                    'requested_symbol_count': len(kwargs['symbols']),
                    'factor_rows': {
                        'unified_value': {'present_rows': len(kwargs['symbols']), 'missing_rows': 0, 'coverage_status': 'complete'},
                        'quality': {'present_rows': len(kwargs['symbols']), 'missing_rows': 0, 'coverage_status': 'complete'},
                        'momentum': {'present_rows': len(kwargs['symbols']), 'missing_rows': 0, 'coverage_status': 'complete'},
                    },
                    'signal_features': {'present_rows': 1, 'missing_rows': max(len(kwargs['symbols']) - 1, 0), 'coverage_status': 'sparse'},
                }
            },
            'items': [
                {
                    'symbol': symbol,
                    'market_date': kwargs['market_date'].isoformat(),
                    'price_context': {'close_price': 85700},
                    'liquidity_context': {
                        'status': 'available',
                        'turnover': {
                            'status': 'available',
                            'turnover_krw': 8800000000,
                            'turnover_source': 'provider',
                        },
                        'ranking': {
                            'status': 'available',
                            'liquidity_percentile': 0.91,
                        },
                        'intraday': {
                            'status': 'partial',
                            'quote_spread_points': 100,
                            'quote_balance_rank': 5,
                        },
                    },
                    'decider_data_quality_context': {
                        'status': 'partial',
                        'blocks': {
                            'liquidity_turnover': {
                                'status': 'available',
                                'turnover': {
                                    'status': 'available',
                                    'turnover_source': 'provider',
                                },
                                'intraday_liquidity': {
                                    'status': 'partial',
                                    'quote_balance_rank': 5,
                                },
                            },
                            'financials_cashflow': {
                                'status': 'partial',
                                'available_core_fields': ['current_assets', 'operating_cash_flow'],
                                'missing_core_fields': ['financing_cash_flow'],
                            },
                            'disclosures_news': {
                                'status': 'partial',
                                'providers': {
                                    'dart_recent_events': {
                                        'status': 'available',
                                        'item_count': 1,
                                    },
                                    'external_news': {
                                        'status': 'provider_error',
                                        'reason': 'naver_search_credentials_missing',
                                    },
                                },
                            },
                        },
                    },
                    'technical_snapshot': {'trend_state': 'uptrend'},
                    'action_summary': {'action_label': 'watch_long'},
                    'market_relative_context': {
                        'status': 'available',
                        'market': 'KOSPI',
                        'breadth': {'up_count': 470, 'down_count': 260, 'flat_count': 80},
                        'relative_strength': {'return_percentile': 0.95, 'volume_percentile': 0.93},
                    },
                    'scores': {
                        'unified_value': {
                            'status': 'available',
                            'score': 0.87,
                            'higher_is_better': True,
                            'meaning': 'Composite value attractiveness score across stored cross-sectional value percentiles.',
                            'method': 'Equal-weight mean of PER, PBR, dividend yield, FCF yield, and EV/EBITDA percentiles.',
                            'components_used': 5,
                            'components_possible': 5,
                            'coverage_notes': [],
                            'as_of_market_date': kwargs['market_date'].isoformat(),
                        },
                        'quality': {
                            'status': 'partial',
                            'score': 0.74,
                            'higher_is_better': True,
                            'meaning': 'Business quality score across profitability, cash-flow quality, and balance-sheet quality inputs.',
                            'method': 'Equal-weight mean of the available quality component percentiles.',
                            'components_used': 5,
                            'components_possible': 6,
                            'coverage_notes': ['Missing quality components: accruals_ratio.'],
                            'as_of_market_date': kwargs['market_date'].isoformat(),
                        },
                        'momentum': {
                            'status': 'available',
                            'score': 0.91,
                            'higher_is_better': True,
                            'meaning': 'Medium-term price-strength score plus trend and overheat gating context from stored daily bars.',
                            'method': 'Ranks 12-1 momentum from stored daily closes, then records trend confirmation, a top-percentile eligibility gate, and the configured recent-overheat guard.',
                            'components_used': 3,
                            'components_possible': 3,
                            'coverage_notes': [],
                            'as_of_market_date': kwargs['market_date'].isoformat(),
                        },
                    },
                    'factor_summary': {
                        'requested_market_date': kwargs['market_date'].isoformat(),
                        'factor_scores': {
                            'value': {
                                'status': 'available',
                                'source_status': 'scored',
                                'score': 0.82,
                            },
                            'unified_value': {
                                'status': 'available',
                                'source_status': 'scored',
                                'score': 0.87,
                            },
                            'quality': {
                                'status': 'available',
                                'source_status': 'scored_partial',
                                'score': 0.74,
                            },
                            'momentum': {
                                'status': 'available',
                                'source_status': 'eligible',
                                'score': 0.91,
                            },
                            'advanced_value': {
                                'status': 'available',
                                'source_status': 'eligible',
                                'score': 0.81,
                            },
                            'low_risk': {
                                'status': 'not_eligible',
                                'source_status': 'watchlist',
                                'score': 0.67,
                            },
                            'beta': {
                                'status': 'available',
                                'source_status': 'eligible',
                                'score': 0.58,
                            },
                            'composite': {
                                'status': 'available',
                                'source_status': 'eligible',
                                'score': 0.73,
                            },
                        },
                        'signal_features': {
                            'status': 'missing_source_row',
                            'row_kind': 'missing',
                        },
                    },
                    'persisted_factor_rows': {
                        'value': {'value_score': 0.82, 'value_status': 'scored'},
                        'unified_value': {'unified_value_score': 0.87, 'unified_value_status': 'scored'},
                        'quality': {'quality_score': 0.74, 'quality_status': 'scored_partial', 'accruals_ratio': None},
                        'momentum': {'momentum_percentile': 0.91},
                        'advanced_value': {'advanced_value_score': 0.81, 'advanced_value_classification': 'eligible'},
                        'low_risk': {'low_risk_score': 0.67, 'low_risk_classification': 'watchlist'},
                        'beta': {'low_beta_score': 0.58, 'low_beta_classification': 'eligible'},
                        'composite': {'composite_score': 0.73, 'composite_classification': 'eligible'},
                    },
                    'context_completeness': {
                        'factor_rows': {
                            'quality': {'row_status': 'present', 'null_submetric_fields': ['accruals_ratio']},
                        },
                        'signal_features': {'row_status': 'missing'},
                        'market_relative_context': {'status': 'available'},
                        'sector_peer_mapping': {'status': 'unavailable'},
                        'requested_symbol_coverage': {
                            'requested_symbol_count': len(kwargs['symbols']),
                            'signal_features': {'present_rows': 1, 'missing_rows': max(len(kwargs['symbols']) - 1, 0), 'coverage_status': 'sparse'},
                        },
                    },
                    'fundamentals': {'latest_snapshot': {'highlights': {'operating_income': {'current_amount': 10}}}},
                    'recent_disclosures': {'count': 1, 'items': [{'rcept_no': '20260423000001'}]},
                }
                for symbol in kwargs['symbols']
            ]
        }


class FakeMarketContextBuilder:
    def __init__(self, payload: dict):
        self.payload = payload
        self.calls = []

    def build(self, *, market_date):
        self.calls.append(market_date.isoformat())
        return self.payload


class RecordingBatchClient:
    def __init__(self):
        self.calls = []
        self._lock = threading.Lock()

    def dispatch_batch(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
        spec = kwargs['spec']
        batch_items = kwargs['batch_items']
        dispatch_id = kwargs['batch_items'][0].get('dispatch_id')
        return {
            'decoded': {
                'items': [
                    {
                        'symbol': item['symbol'],
                        'feedback': {
                            'role': spec.role,
                            'summary': f"{item['symbol']} {spec.role} summary",
                            'confidence': 0.7,
                            'risk_flags': [],
                            'details': {},
                            'improvement_notes': [],
                        },
                    }
                    for item in kwargs['batch_items']
                ]
            },
            'attempt': 1,
            'attempt_id': f'{dispatch_id}:attempt-1' if dispatch_id is not None else None,
            'dispatch_id': dispatch_id,
            'request_run_id': kwargs['batch_items'][0].get('request_run_id'),
            'market_date': kwargs['market_date'].isoformat(),
            'batch_symbols': [item['symbol'] for item in batch_items],
            'slot': batch_items[0].get('session_slot'),
            'started_at': '2026-04-14T01:00:00+00:00',
            'completed_at': '2026-04-14T01:01:00+00:00',
            'session_ref': {'session_key': kwargs['batch_items'][0]['session_key']},
        }


class SignalDeciderBatchClient:
    def __init__(self):
        self.calls = []
        self.decider_payloads = []
        self._lock = threading.Lock()

    def dispatch_batch(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
        spec = kwargs['spec']
        batch_items = kwargs['batch_items']
        dispatch_id = batch_items[0].get('dispatch_id')
        if spec.role == 'eod-summarizer':
            feedback_by_symbol = {
                item['symbol']: {
                    'role': spec.role,
                    'summary': 'Breadth strengthened into the close.',
                    'confidence': 0.7,
                    'risk_flags': ['narrow_leadership'],
                    'details': {'breadth_context': {'market_bias': 'constructive'}},
                    'improvement_notes': ['Add sector breadth history.'],
                }
                for item in batch_items
            }
        elif spec.role == 'technical-summarizer':
            feedback_by_symbol = {
                item['symbol']: {
                    'role': spec.role,
                    'summary': 'Breakout attempt remains intact above support.',
                    'confidence': 0.68,
                    'risk_flags': [],
                    'details': {'support_resistance': {'support_levels': [80000], 'resistance_levels': [91200]}},
                    'improvement_notes': ['Add premarket gap data.'],
                }
                for item in batch_items
            }
        elif spec.role == 'fundamental-summarizer':
            feedback_by_symbol = {
                item['symbol']: {
                    'role': spec.role,
                    'summary': 'Fundamental backdrop is improving but still cyclical.',
                    'confidence': 0.66,
                    'risk_flags': [],
                    'details': {'fundamental_backdrop': {'quality': 'improving'}},
                    'improvement_notes': ['Add management guidance trend.'],
                }
                for item in batch_items
            }
        elif spec.role == 'short-term-decider':
            self.decider_payloads.extend(batch_items)
            feedback_by_symbol = {
                item['symbol']: {
                    'role': spec.role,
                    'agent_name': spec.agent_name,
                    'symbol': item['symbol'],
                    'market_date': item['market_date'],
                    'tactical_action': 'watch_for_trigger',
                    'preferred_action': 'watch_for_trigger',
                    'trigger_price': 87200,
                    'chase_limit_price': 88500,
                    'target_price': 91200,
                    'stop_loss_price': 82900,
                    'expected_holding_period': '1-2 trading days',
                    'confidence_score_10': 6,
                    'conviction_score_10': 5,
                    'thesis_summary': '단기 탄력은 있으나 돌파 확인이 필요합니다.',
                    'trigger_conditions': ['87200원 돌파와 거래량 동반'],
                    'supporting_evidence': ['Breadth improved.', 'Support held above 80000.'],
                    'tactical_risks': ['Leadership remains narrow.'],
                    'invalidation_conditions': ['82900원 이탈'],
                    'data_quality_warnings': ['Signal-feature coverage was partial.'],
                    'improvement_notes': ['Add premarket gap context.'],
                }
                for item in batch_items
            }
        else:
            self.decider_payloads.extend(batch_items)
            feedback_by_symbol = {
                item['symbol']: {
                    'role': spec.role,
                    'agent_name': spec.agent_name,
                    'symbol': item['symbol'],
                    'market_date': item['market_date'],
                    'preferred_action': 'buy',
                    'next_day_open_action': 'buy',
                    'entry_price_basis': 'next_open',
                    'target_price': 91200,
                    'expected_return_pct': 6.4,
                    'stop_loss_price': 82900,
                    'suggested_holding_period': '3-10 trading days',
                    'confidence_score_10': 7,
                    'conviction_score_10': 6,
                    'thesis_summary': 'Specialists align on a tactical next-day long setup.',
                    'supporting_evidence': ['Breadth improved.', 'Support held above 80000.'],
                    'decision_risks': ['Leadership remains narrow.'],
                    'invalidation_conditions': ['Gap below 82900 at the next open.'],
                    'data_quality_warnings': ['Signal-feature coverage was partial.'],
                    'specialist_disagreement': ['Fundamental view was constructive but not emphatic.'],
                    'improvement_notes': ['Add premarket gap context.'],
                }
                for item in batch_items
            }
        return {
            'decoded': {
                'items': [
                    {
                        'symbol': item['symbol'],
                        'feedback': feedback_by_symbol[item['symbol']],
                    }
                    for item in batch_items
                ]
            },
            'attempt': 1,
            'attempt_id': f'{dispatch_id}:attempt-1' if dispatch_id is not None else None,
            'dispatch_id': dispatch_id,
            'request_run_id': batch_items[0].get('request_run_id'),
            'market_date': kwargs['market_date'].isoformat(),
            'batch_symbols': [item['symbol'] for item in batch_items],
            'slot': batch_items[0].get('session_slot'),
            'started_at': '2026-04-14T01:00:00+00:00',
            'completed_at': '2026-04-14T01:01:00+00:00',
            'session_ref': {'session_key': batch_items[0]['session_key']},
            'response': {'model': f"openclaw/{spec.agent_name}", 'id': f"resp-{spec.role}"},
            'output_text': '{}',
        }


class FailingSignalDeciderBatchClient(SignalDeciderBatchClient):
    def dispatch_batch(self, **kwargs):
        spec = kwargs['spec']
        if spec.role == 'signal-decider':
            with self._lock:
                self.calls.append(kwargs)
            raise orchestrator_module.AgentDispatchRetryableError('gateway failed before feedback rows')
        return super().dispatch_batch(**kwargs)


class FakeHTTPResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class FakeHTTPClient:
    def __init__(self, *, response: FakeHTTPResponse, calls: list[dict], **kwargs):
        self.response = response
        self.calls = calls
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url, json):
        self.calls.append({'url': url, 'json': json, 'kwargs': self.kwargs})
        return self.response


class FakeFeedbackService:
    def __init__(self):
        self.bulk_items = []
        self.bulk_calls = []
        self.submitted = []

    def bulk_submit_feedback(self, *, items):
        self.bulk_items = list(items)
        self.bulk_calls.append(list(items))
        return {'accepted': True, 'count': len(self.bulk_items), 'items': self.bulk_items}

    def submit_feedback(self, **kwargs):
        self.submitted.append(kwargs)
        return {
            'accepted': True,
            'item': {
                'id': len(self.submitted),
                'agent_name': kwargs['agent_name'],
                'run_id': kwargs['run_id'],
            },
        }


class ObservingFeedbackService(FakeFeedbackService):
    def __init__(self, runtime_store):
        super().__init__()
        self.runtime_store = runtime_store
        self.observed_summaries = []

    def bulk_submit_feedback(self, *, items):
        snapshot = self.runtime_store.get_run(items[0]['market_date'], run_id=items[0]['run_id'])
        self.observed_summaries.append(snapshot.to_progress_summary() if snapshot is not None else None)
        return super().bulk_submit_feedback(items=items)


class FakeSessionCleanupService:
    def __init__(self):
        self.targets = []

    def delete_now(self, *, target):
        self.targets.append(target)
        return {'status': 'deleted', 'session_key': target.session_key}


class RecordingFailureService:
    def __init__(self):
        self.recorded = []

    def record_failure(self, **kwargs):
        self.recorded.append(kwargs)
        return {'id': len(self.recorded), **kwargs}


class RecordingRunStateService:
    def __init__(self):
        self.started = []
        self.checkpoints = []
        self.finished = []

    def start_run(self, **kwargs):
        self.started.append(kwargs)
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': 'running'}

    def checkpoint_run(self, **kwargs):
        self.checkpoints.append(kwargs)
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': kwargs['status']}

    def finish_run(self, **kwargs):
        self.finished.append(kwargs)
        return {'ok': True, 'run_id': kwargs['run_id'], 'status': kwargs['status']}


class StrictRuntimeStateStore:
    def __init__(self):
        self._delegate = RuntimeFeedbackStateStore()
        self.update_run_ids = []
        self.get_run_ids = []

    def start_run(self, state, *, replace=True, set_active=True, logical=False):
        return self._delegate.start_run(state, replace=replace, set_active=set_active, logical=logical)

    def get_run(self, market_date, run_id=None, logical=False):
        if not logical and run_id is None:
            raise AssertionError('expected explicit run_id for runtime get_run in normal dispatch')
        if run_id is not None:
            self.get_run_ids.append(run_id)
        return self._delegate.get_run(market_date, run_id=run_id, logical=logical)

    def update_run(self, market_date, updater, run_id=None, logical=False):
        if not logical and run_id is None:
            raise AssertionError('expected explicit run_id for runtime update_run in normal dispatch')
        if run_id is not None:
            self.update_run_ids.append(run_id)
        return self._delegate.update_run(market_date, updater, run_id=run_id, logical=logical)


class PartiallyFailingBatchClient(RecordingBatchClient):
    def dispatch_batch(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
        spec = kwargs['spec']
        batch_items = kwargs['batch_items']
        if spec.role == 'technical-summarizer':
            return {
                'decoded': {'items': []},
                'attempt': 1,
                'started_at': '2026-04-14T01:00:00+00:00',
                'completed_at': '2026-04-14T01:01:00+00:00',
                'session_ref': {'session_key': batch_items[0]['session_key']},
            }
        return super().dispatch_batch(**kwargs)


class AlwaysFailingBatchClient:
    def __init__(self):
        self.calls = []

    def dispatch_batch(self, **kwargs):
        self.calls.append(kwargs)
        raise orchestrator_module.AgentDispatchRetryableError('agent failed before feedback row')


class DetailedFailingBatchClient:
    def __init__(self):
        self.calls = []

    def dispatch_batch(self, **kwargs):
        self.calls.append(kwargs)
        batch_item = kwargs['batch_items'][0]
        dispatch_id = batch_item['dispatch_id']
        attempt_id = f'{dispatch_id}:attempt-1'
        raise orchestrator_module.AgentDispatchRetryableError(
            'OpenClaw batch response session_history_missing_or_empty',
            details={
                'failure_kind': 'session_history_missing_or_empty',
                'failure_class': 'session_history_missing_or_empty',
                'failure_mode': 'session_history_missing_or_empty',
                'session_key': batch_item['session_key'],
                'session_id': 'session-traj-1',
                'response_id': 'resp-decider-traj-1',
                'request_marker': f'batch-request::{attempt_id}',
                'dispatch_id': dispatch_id,
                'attempt_id': attempt_id,
                'slot': batch_item.get('session_slot'),
                'response_recovery': {
                    'session_key': batch_item['session_key'],
                    'session_id': 'session-traj-1',
                    'response_id': 'resp-decider-traj-1',
                    'request_marker': f'batch-request::{attempt_id}',
                    'history': {
                        'checked': True,
                        'failure_mode': 'session_history_missing_or_empty',
                    },
                    'trajectory': {
                        'checked': True,
                        'status': 'not_recovered',
                        'failure_mode': 'trajectory_artifacts_not_found',
                    },
                },
            },
        )


def _reuse_decision(should_dispatch: bool):
    return SimpleNamespace(should_dispatch=should_dispatch)


def _passed_filter_rows(*symbols: str) -> list[str]:
    return list(symbols)


@contextmanager
def _patch_feedback_eligibility(*symbols: str):
    with patch.object(
        orchestrator_module,
        'load_agent_feedback_eligible_symbols',
        return_value=_passed_filter_rows(*symbols),
    ):
        yield


class AgentFeedbackOrchestratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prerequisite_patcher = patch(
            'app.agent_context.orchestrator.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(),
        )
        self.prerequisite_patcher.start()
        self.addCleanup(self.prerequisite_patcher.stop)

    def test_openclaw_client_uses_default_timeout_when_configured_timeout_is_non_positive(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=0,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()

        self.assertEqual(
            client.timeout_seconds,
            orchestrator_module.DEFAULT_OPENCLAW_AGENT_TIMEOUT_SECONDS,
        )
        self.assertEqual(client.http_timeout_seconds, client.timeout_seconds)

    def test_signal_decider_batch_message_requires_exact_symbol_batch_envelope(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()
            message = client._build_batch_message(
                spec=orchestrator_module.DISPATCH_SPECS[-1],
                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                batch_items=[
                    {'symbol': '005930', 'market_date': '2026-04-14'},
                    {'symbol': '000660', 'market_date': '2026-04-14'},
                ],
                request_marker='batch-request::test',
            )
            one_symbol_message = client._build_batch_message(
                spec=orchestrator_module.DISPATCH_SPECS[-1],
                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                batch_items=[{'symbol': '005930', 'market_date': '2026-04-14'}],
                request_marker='batch-request::test-one',
            )

        self.assertIn('Assigned symbols (exact authoritative strings): ["005930", "000660"]', message)
        self.assertIn('output exactly 2 item(s)', message)
        self.assertIn('The only top-level JSON shape allowed is {"items":[...]}', message)
        self.assertIn('Every item.symbol is mandatory and must exactly match one assigned symbol string', message)
        self.assertIn('Every feedback.symbol and feedback.traceability.symbol is mandatory', message)
        self.assertIn('Self-check before final answer', message)
        self.assertIn('Immediately before sending, validate the final raw JSON text again', message)
        self.assertIn('no assigned symbol is missing', message)
        self.assertIn('Assigned symbols (exact authoritative strings): ["005930"]', one_symbol_message)
        self.assertIn('output exactly 1 item(s)', one_symbol_message)
        self.assertIn('One-symbol requests still require the same batch envelope with exactly one item', one_symbol_message)
        self.assertIn('Immediately before sending, re-check the final JSON formatting one last time', one_symbol_message)

    def test_signal_decider_prompt_trim_final_fallback_uses_one_item_rules(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=1,
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()
            message = client._build_batch_message(
                spec=orchestrator_module.DISPATCH_SPECS[-1],
                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                batch_items=[
                    {'symbol': '005930', 'market_date': '2026-04-14', 'large_context': 'x' * 1000},
                    {'symbol': '000660', 'market_date': '2026-04-14', 'large_context': 'y' * 1000},
                ],
                request_marker='batch-request::trim-fallback',
            )

        self.assertIn('Assigned symbols (exact authoritative strings): ["005930"]', message)
        self.assertIn('output exactly 1 item(s)', message)
        self.assertNotIn('Assigned symbols (exact authoritative strings): ["005930", "000660"]', message)
        self.assertNotIn('output exactly 2 item(s)', message)
        self.assertNotIn('"symbol": "000660"', message)

    def test_reuse_existing_skips_reusable_symbol_role_pairs(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        read_service = RecordingAgentContextReadService()
        reuse_service = SimpleNamespace(
            build_latest_decisions=lambda **kwargs: {
                '005930': {'eod-summarizer': _reuse_decision(False)},
                '000660': {'eod-summarizer': _reuse_decision(True)},
            }
        )
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=20)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with _patch_feedback_eligibility('005930', '000660'):
                                result = service.dispatch_for_symbols(
                                    market_date='2026-04-14',
                                    symbols=['005930', '000660'],
                                    roles=['eod-summarizer'],
                                    reuse_existing=True,
                                )

        dispatched_symbols = [
            item['symbol']
            for call in client.calls
            for item in call['batch_items']
        ]
        self.assertEqual(read_service.calls[0]['symbols'], ['000660'])
        self.assertEqual(dispatched_symbols, ['000660'])
        self.assertEqual([item['symbol'] for item in feedback_service.bulk_items], ['000660'])
        self.assertEqual([target.symbol for target in cleanup_service.targets], ['000660'])

    def test_dispatch_for_symbols_returns_blocked_gate_when_prerequisites_are_not_ready(self):
        service = AgentFeedbackOrchestrator()

        with patch(
            'app.agent_context.orchestrator.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            side_effect=lambda **_: _feedback_prerequisites(
                status='blocked',
                missing_stage_ids=['quality_factors', 'composite_factors'],
            ),
        ):
            result = service.dispatch_for_symbols(
                market_date='2026-04-14',
                symbols=['005930'],
                roles=['technical-summarizer'],
            )

        self.assertTrue(result['summary']['gated'])
        self.assertEqual(result['summary']['status'], 'blocked')
        self.assertEqual(result['items'], [])
        self.assertEqual(result['summary']['completed'], 0)

    def test_buffered_symbol_dispatch_cleans_each_batch_session_once(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=2, openclaw_agent_parallel_subtasks=20)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with _patch_feedback_eligibility('005930', '000660'):
                            result = service.dispatch_for_symbols(
                                market_date='2026-04-14',
                                symbols=['005930', '000660'],
                                roles=['eod-summarizer'],
                                reuse_existing=False,
                            )

        self.assertEqual([item['symbol'] for item in feedback_service.bulk_items], ['005930', '000660'])
        self.assertEqual(len(cleanup_service.targets), 1)
        self.assertEqual(cleanup_service.targets[0].session_key, client.calls[0]['batch_items'][0]['session_key'])
        self.assertEqual(result['summary']['flushed_to_db'], 2)

    def test_symbol_dispatch_checkpoints_each_persisted_batch_and_leaves_final_flush_as_safety(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with _patch_feedback_eligibility('005930', '000660'):
                                result = service.dispatch_for_symbols(
                                    market_date='2026-04-14',
                                    symbols=['005930', '000660'],
                                    roles=['eod-summarizer'],
                                    reuse_existing=False,
                                )

        batch_checkpoints = [
            checkpoint
            for checkpoint in run_state_service.checkpoints
            if checkpoint['summary_payload']['lifecycle_stage'] == 'batch_persisted'
        ]

        self.assertEqual([len(call) for call in feedback_service.bulk_calls], [1, 1])
        self.assertEqual(len(batch_checkpoints), 2)
        self.assertEqual(
            [checkpoint['summary_payload']['flush_result']['feedback_rows_seen'] for checkpoint in batch_checkpoints],
            [1, 1],
        )
        self.assertTrue(
            all(checkpoint['summary_payload']['flush_result']['status'] == 'feedback_flushed' for checkpoint in batch_checkpoints)
        )
        self.assertEqual(run_state_service.finished[0]['summary_payload']['flush_result']['status'], 'noop')
        self.assertEqual(result['summary']['flushed_to_db'], 2)

    def test_symbol_dispatch_progress_counts_only_persisted_feedback_between_batches(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        runtime_store = RuntimeFeedbackStateStore()
        feedback_service = ObservingFeedbackService(runtime_store)
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with _patch_feedback_eligibility('005930', '000660'):
                                result = service.dispatch_for_symbols(
                                    market_date='2026-04-14',
                                    symbols=['005930', '000660'],
                                    roles=['eod-summarizer'],
                                    reuse_existing=False,
                                )

        self.assertEqual(len(feedback_service.observed_summaries), 2)
        self.assertEqual(feedback_service.observed_summaries[0]['completed_feedback_count'], 0)
        self.assertEqual(feedback_service.observed_summaries[0]['pending_feedback_count'], 1)
        self.assertEqual(feedback_service.observed_summaries[0]['flushed_feedback_count'], 0)
        self.assertEqual(feedback_service.observed_summaries[1]['completed_feedback_count'], 1)
        self.assertEqual(feedback_service.observed_summaries[1]['buffered_feedback_count'], 1)
        self.assertEqual(feedback_service.observed_summaries[1]['pending_feedback_count'], 1)
        self.assertEqual(feedback_service.observed_summaries[1]['flushed_feedback_count'], 1)
        self.assertEqual(
            feedback_service.observed_summaries[1]['role_progress']['eod-summarizer']['completed_symbols'],
            1,
        )
        self.assertEqual(result['summary']['completed'], 2)
        self.assertEqual(result['summary']['flushed_to_db'], 2)

    def test_symbol_dispatch_uses_configured_parallel_subtasks(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=3)
        symbols = ['000001', '000002', '000003', '000004', '000005', '000006']

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with _patch_feedback_eligibility(*symbols):
                            result = service.dispatch_for_symbols(
                                market_date='2026-04-14',
                                symbols=symbols,
                                roles=['eod-summarizer'],
                                reuse_existing=False,
                            )

        slots = sorted(call['batch_items'][0]['session_slot'] for call in client.calls)
        self.assertEqual(slots, [1, 1, 2, 2, 3, 3])
        self.assertEqual(result['request']['parallel_subtasks'], 3)
        self.assertEqual(result['request']['role_parallel_subtasks'], 3)
        self.assertEqual(result['summary']['completed'], 6)

    def test_subagent_dispatch_uses_requested_per_role_parallelism(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        symbols = ['000001', '000002', '000003', '000004', '000005', '000006']

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    with _patch_feedback_eligibility(*symbols):
                        result = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=symbols,
                            roles=['eod-summarizer'],
                            per_role_parallelism=3,
                        )

        slots = sorted(
            call['batch_items'][0]['session_slot']
            for call in client.calls
        )
        self.assertEqual(slots, [1, 2, 3])
        self.assertEqual(result['request']['per_role_parallelism'], 3)
        self.assertEqual(result['request']['total_parallel_subtasks'], 3)
        self.assertEqual(result['summary']['completed'], 6)

    def test_timed_out_running_future_keeps_slot_until_it_completes(self):
        first_future = FakeFuture(result=_batch_result('005930'), cancel_result=False)
        second_future = FakeFuture(result=_batch_result('000660'), cancel_result=True)
        fake_executor = FakeThreadPoolExecutor([first_future, second_future])
        wait_calls = []

        def fake_wait(active_futures, *, timeout=None, return_when):
            wait_calls.append(list(active_futures))
            if len(wait_calls) == 1:
                return set(), set(active_futures)
            if len(wait_calls) == 2:
                self.assertEqual(list(active_futures), [first_future])
                return {first_future}, set()
            if len(wait_calls) == 3:
                self.assertEqual(list(active_futures), [second_future])
                return {second_future}, set()
            raise AssertionError('unexpected wait call')

        service = AgentFeedbackOrchestrator()
        service.client = FakeOpenClawClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()

        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
            openclaw_agent_timeout_seconds=1,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_retries=0,
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', FakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with _patch_feedback_eligibility('005930', '000660'):
                            with patch(
                                'app.agent_context.orchestrator.concurrent.futures.ThreadPoolExecutor',
                                side_effect=lambda max_workers: fake_executor,
                            ):
                                with patch('app.agent_context.orchestrator.concurrent.futures.wait', side_effect=fake_wait):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930', '000660'],
                                        roles=['eod-summarizer'],
                                        reuse_existing=False,
                                    )

        self.assertEqual(first_future.cancel_calls, 0)
        self.assertEqual(
            [submission['batch_items'][0]['symbol'] for submission in fake_executor.submissions],
            ['005930', '000660'],
        )
        self.assertEqual(
            [item['symbol'] for call in feedback_service.bulk_calls for item in call],
            ['005930', '000660'],
        )
        self.assertEqual(len(cleanup_service.targets), 1)
        self.assertEqual(result['summary']['failed'], 0)

    def test_dispatch_for_symbols_skips_symbols_outside_active_filter_intersection(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        read_service = RecordingAgentContextReadService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=20)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with _patch_feedback_eligibility('005930'):
                            result = service.dispatch_for_symbols(
                                market_date='2026-04-14',
                                symbols=['005930', '000660'],
                                roles=['eod-summarizer'],
                                reuse_existing=False,
                            )

        self.assertEqual(read_service.calls[0]['symbols'], ['005930'])
        self.assertEqual(
            [item['symbol'] for call in client.calls for item in call['batch_items']],
            ['005930'],
        )
        self.assertEqual(result['request']['symbols'], ['005930'])
        self.assertEqual(result['request']['skipped_symbols'], ['000660'])
        self.assertEqual(result['summary']['skipped'], 1)

    def test_dispatch_for_symbols_preserves_canonical_role_sequence_when_requested_out_of_order(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with _patch_feedback_eligibility('005930'):
                            result = service.dispatch_for_symbols(
                                market_date='2026-04-14',
                                symbols=['005930'],
                                roles=['fundamental-summarizer', 'eod-summarizer'],
                                reuse_existing=False,
                            )

        self.assertEqual(
            result['request']['roles'],
            ['eod-summarizer', 'fundamental-summarizer'],
        )
        self.assertEqual([call['spec'].role for call in client.calls], ['eod-summarizer', 'fundamental-summarizer'])
        self.assertEqual(result['summary']['completed'], 2)

    def test_default_symbol_dispatch_builds_signal_decider_payload_from_full_upstream_summaries(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=2)
        def build_latest_decisions(**kwargs):
            roles = tuple(kwargs['roles'])
            if set(roles) == set(orchestrator_module.UPSTREAM_SUMMARY_ROLES):
                return {}
            return {
                '005930': {
                    'short-term-decider': SimpleNamespace(
                        should_dispatch=True,
                        latest_feedback={
                            'id': 81,
                            'market_date': '2026-04-13',
                            'symbol': '005930',
                            'agent_name': 'short-term-decider',
                            'run_id': 'short-prev-81',
                            'received_at': '2026-04-13T06:00:00+00:00',
                            'feedback_payload': {
                                'role': 'short-term-decider',
                                'symbol': '005930',
                                'market_date': '2026-04-13',
                                'tactical_action': 'watch_for_trigger',
                                'preferred_action': 'watch_for_trigger',
                                'trigger_price': 86800,
                                'chase_limit_price': 88100,
                                'target_price': 91200,
                                'stop_loss_price': 82900,
                                'confidence_score_10': 6,
                                'conviction_score_10': 5,
                                'thesis_summary': '이전에는 돌파 확인이 더 필요했다.',
                                'trigger_conditions': ['86800원 돌파와 거래량 동반'],
                                'supporting_evidence': ['Momentum remained constructive.'],
                                'tactical_risks': ['Opening volatility remained elevated.'],
                                'invalidation_conditions': ['Breakout fails back under 82900.'],
                                'data_quality_warnings': [],
                            },
                        },
                    ),
                    'signal-decider': SimpleNamespace(
                        should_dispatch=True,
                        latest_feedback={
                            'id': 82,
                            'market_date': '2026-04-13',
                            'symbol': '005930',
                            'agent_name': 'signal-decider',
                            'run_id': 'signal-prev-82',
                            'received_at': '2026-04-13T06:05:00+00:00',
                            'feedback_payload': {
                                'role': 'signal-decider',
                                'symbol': '005930',
                                'market_date': '2026-04-13',
                                'preferred_action': 'wait',
                                'next_day_open_action': 'wait',
                                'wait_reason': {
                                    'code': 'stale_quote_orderbook',
                                    'summary': '호가 신선도 부족으로 대기했다.',
                                },
                                'target_price': None,
                                'expected_return_pct': None,
                                'stop_loss_price': None,
                                'confidence_score_10': 5,
                                'conviction_score_10': 4,
                                'thesis_summary': '이전에는 호가 신선도 부족으로 대기했다.',
                                'supporting_evidence': ['Breadth improved.'],
                                'decision_risks': ['Orderbook freshness remained stale.'],
                                'invalidation_conditions': ['Fresh realtime quote required.'],
                                'data_quality_warnings': ['Realtime orderbook freshness is stale.'],
                                'specialist_disagreement': [],
                            },
                        },
                    ),
                }
            }

        reuse_service = SimpleNamespace(build_latest_decisions=build_latest_decisions)
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'as_of': '2026-04-14T10:05:12+09:00',
                'summary': {
                    'market_bias': 'risk_on',
                    'line': 'KOSPI +0.45%; KOSDAQ +0.54%; USD/KRW 1444.5.',
                    'breadth_summary': 'KOSPI breadth 702/81/198; KOSDAQ breadth 901/92/501',
                },
                'indices': {
                    'kospi': {'level': 2642.07, 'change': 11.78, 'change_rate': 0.45},
                    'kosdaq': {'level': 857.60, 'change': 4.61, 'change_rate': 0.54},
                },
                'fx': {'usdkrw': {'value': 1444.5, 'effective_date': '2026-04-14', 'fallback_applied': False}},
                'flows': {
                    'foreign': {'value': 746146, 'unit': 'KIS_NET_VALUE_RAW'},
                    'institutional': {'value': 206217, 'unit': 'KIS_NET_VALUE_RAW'},
                },
                'missing_fields': [],
                'stale_reason': None,
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DeciderReadyAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with patch.object(
                                orchestrator_module,
                                'signal_decider_market_context_builder',
                                market_context_builder,
                            ):
                                with _patch_feedback_eligibility('005930'):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930'],
                                        reuse_existing=True,
                                    )

        self.assertEqual(
            result['request']['roles'],
            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer', 'short-term-decider', 'signal-decider'],
        )
        self.assertEqual(len(client.decider_payloads), 2)
        short_term_payload = next(payload for payload in client.decider_payloads if payload['role'] == 'short-term-decider')
        self.assertEqual(short_term_payload['orchestration']['request_model'], 'openclaw/short-term-decider')
        self.assertIn('trigger_price', short_term_payload['output_contract']['required_feedback_fields'])
        self.assertIn('wait_reason', short_term_payload['output_contract']['required_feedback_fields'])
        self.assertIn('decision_delta', short_term_payload['output_contract']['required_feedback_fields'])
        self.assertTrue(short_term_payload['previous_decision_context']['available'])
        self.assertEqual(short_term_payload['previous_decision_context']['recommendation'], 'watch_for_trigger')
        self.assertTrue(
            short_term_payload['decision_support_constraints']['missing_context_policy'][
                'auxiliary_context_absence_not_tradeability_blocker'
            ]
        )
        self.assertTrue(
            short_term_payload['decision_support_constraints']['decision_history_guidance'][
                'repeat_wait_requires_delta_summary'
            ]
        )
        self.assertEqual(short_term_payload['decider_data_quality_context']['status'], 'partial')
        self.assertEqual(
            short_term_payload['decider_data_quality_context']['blocks']['disclosures_news']['providers']['external_news']['status'],
            'provider_error',
        )
        decider_payload = next(payload for payload in client.decider_payloads if payload['role'] == 'signal-decider')
        self.assertEqual(decider_payload['upstream_summaries']['eod-summarizer']['details']['breadth_context']['market_bias'], 'constructive')
        self.assertEqual(
            decider_payload['upstream_summaries']['technical-summarizer']['details']['support_resistance']['support_levels'],
            [80000],
        )
        self.assertEqual(
            decider_payload['upstream_summaries']['fundamental-summarizer']['details']['fundamental_backdrop']['quality'],
            'improving',
        )
        self.assertEqual(decider_payload['scores']['unified_value']['status'], 'available')
        self.assertEqual(decider_payload['scores']['quality']['status'], 'partial')
        self.assertEqual(decider_payload['scores']['momentum']['score'], 0.91)
        self.assertEqual(decider_payload['factor_summary']['factor_scores']['value']['status'], 'available')
        self.assertEqual(decider_payload['factor_summary']['factor_scores']['low_risk']['status'], 'not_eligible')
        self.assertEqual(decider_payload['factor_summary']['signal_features']['status'], 'missing_source_row')
        self.assertEqual(decider_payload['liquidity_context']['turnover']['status'], 'available')
        self.assertEqual(decider_payload['liquidity_context']['intraday']['quote_balance_rank'], 5)
        self.assertEqual(decider_payload['decider_data_quality_context']['status'], 'partial')
        self.assertEqual(
            decider_payload['decider_data_quality_context']['blocks']['liquidity_turnover']['turnover']['turnover_source'],
            'provider',
        )
        self.assertEqual(
            decider_payload['decider_data_quality_context']['blocks']['disclosures_news']['providers']['external_news']['status'],
            'provider_error',
        )
        self.assertEqual(decider_payload['market_context']['status'], 'ready')
        self.assertEqual(decider_payload['market_context']['summary']['market_bias'], 'risk_on')
        self.assertEqual(decider_payload['market_context']['indices']['kospi']['level'], 2642.07)
        self.assertNotIn('scores', decider_payload['symbol_day_context'])
        self.assertNotIn('factor_summary', decider_payload['symbol_day_context'])
        self.assertNotIn('liquidity_context', decider_payload['symbol_day_context'])
        self.assertNotIn('decider_data_quality_context', decider_payload['symbol_day_context'])
        self.assertEqual(decider_payload['symbol_day_context']['market_relative_context']['relative_strength']['return_percentile'], 0.95)
        self.assertEqual(decider_payload['symbol_day_context']['persisted_factor_rows']['advanced_value']['advanced_value_classification'], 'eligible')
        self.assertEqual(decider_payload['symbol_day_context']['context_completeness']['sector_peer_mapping']['status'], 'unavailable')
        self.assertEqual(
            decider_payload['symbol_day_context']['context_completeness']['requested_symbol_coverage']['signal_features']['coverage_status'],
            'sparse',
        )
        self.assertEqual(decider_payload['orchestration']['request_model'], 'openclaw/signal-decider')
        self.assertEqual(
            decider_payload['orchestration']['required_upstream_roles'],
            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'],
        )
        self.assertTrue(decider_payload['previous_decision_context']['available'])
        self.assertEqual(decider_payload['previous_decision_context']['recommendation'], 'wait')
        self.assertTrue(
            decider_payload['decision_support_constraints']['missing_context_policy']['empty_recent_news_is_neutral']
        )
        self.assertTrue(
            decider_payload['decision_support_constraints']['missing_context_policy'][
                'quarterly_financial_detail_confidence_only_unless_thesis_depends_on_it'
            ]
        )
        self.assertEqual(decider_payload['output_contract']['assigned_symbol'], '005930')
        self.assertTrue(decider_payload['output_contract']['batch_envelope_required'])
        self.assertTrue(decider_payload['output_contract']['symbol_identity']['exact_match_required'])
        self.assertIn('wait_reason', decider_payload['output_contract']['required_feedback_fields'])
        self.assertIn('decision_delta', decider_payload['output_contract']['required_feedback_fields'])
        self.assertIn(
            'item_symbol_feedback_symbol_and_traceability_symbol_match_assigned_symbol',
            decider_payload['output_contract']['self_check_before_final_output'],
        )

        decider_row = next(item for item in feedback_service.bulk_items if item['agent_name'] == 'signal-decider')
        self.assertEqual(decider_row['feedback']['next_day_open_action'], 'buy')
        self.assertEqual(decider_row['feedback']['preferred_action'], 'buy')
        self.assertEqual(decider_row['summary']['model'], 'openclaw/signal-decider')
        self.assertIn('raw_response', decider_row['summary'])
        self.assertEqual(decider_row['context']['market_context']['status'], 'ready')
        self.assertEqual(decider_row['context']['market_context']['flows']['foreign']['value'], 746146)
        eod_call = next(call for call in client.calls if call['spec'].role == 'eod-summarizer')
        self.assertEqual(eod_call['batch_items'][0]['session_reuse_mode'], 'fresh_per_batch')
        self.assertTrue(eod_call['batch_items'][0]['session_key'].startswith('agent:eod-summarizer:feedback-fresh/eod-summarizer/'))
        self.assertIn(result['request']['request_run_id'], eod_call['batch_items'][0]['session_key'])
        decider_call = next(call for call in client.calls if call['spec'].role == 'signal-decider')
        self.assertEqual(decider_call['batch_items'][0]['session_reuse_mode'], 'fresh_per_batch')
        self.assertTrue(decider_call['batch_items'][0]['session_key'].startswith('agent:signal-decider:feedback-fresh/signal-decider/'))
        self.assertIn(result['request']['request_run_id'], decider_call['batch_items'][0]['session_key'])
        self.assertEqual(market_context_builder.calls, ['2026-04-14', '2026-04-14'])
        self.assertEqual(result['summary']['completed'], 5)

    def test_requested_short_term_decider_builds_tactical_payload_from_upstream_summaries(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=2)
        reuse_service = SimpleNamespace(build_latest_decisions=lambda **kwargs: {})
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {'foreign': {'value': 746146}},
                'missing_fields': [],
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DeciderReadyAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with patch.object(
                                orchestrator_module,
                                'signal_decider_market_context_builder',
                                market_context_builder,
                            ):
                                with _patch_feedback_eligibility('005930'):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930'],
                                        roles=['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer', 'short-term-decider'],
                                        reuse_existing=False,
                                    )

        self.assertEqual(
            result['request']['roles'],
            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer', 'short-term-decider'],
        )
        short_term_payload = client.decider_payloads[0]
        self.assertEqual(short_term_payload['role'], 'short-term-decider')
        self.assertEqual(short_term_payload['agent_name'], 'short-term-decider')
        self.assertEqual(short_term_payload['orchestration']['request_model'], 'openclaw/short-term-decider')
        self.assertEqual(short_term_payload['market_context']['status'], 'ready')
        self.assertIn('trigger_price', short_term_payload['output_contract']['required_feedback_fields'])
        self.assertIn('chase_limit_price', short_term_payload['output_contract']['required_feedback_fields'])
        self.assertNotIn('next_day_open_action', short_term_payload['output_contract']['required_feedback_fields'])
        short_term_row = next(item for item in feedback_service.bulk_items if item['agent_name'] == 'short-term-decider')
        self.assertEqual(short_term_row['feedback']['tactical_action'], 'watch_for_trigger')
        self.assertEqual(short_term_row['feedback']['preferred_action'], 'watch_for_trigger')
        self.assertEqual(short_term_row['feedback']['trigger_price'], 87200.0)
        self.assertEqual(short_term_row['context']['market_context']['summary']['market_bias'], 'risk_on')
        self.assertEqual(market_context_builder.calls, ['2026-04-14'])
        self.assertEqual(result['summary']['completed'], 4)

    def test_failed_fresh_signal_decider_batch_still_cleans_session(self):
        service = AgentFeedbackOrchestrator()
        client = FailingSignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        def build_latest_decisions(**kwargs):
            roles = tuple(kwargs['roles'])
            if set(roles) != set(orchestrator_module.UPSTREAM_SUMMARY_ROLES):
                return {}
            return {
                '005930': {
                    role: SimpleNamespace(
                        should_dispatch=False,
                        latest_feedback={
                            'id': len(role),
                            'market_date': '2026-04-14',
                            'run_id': f'upstream-{role}',
                            'received_at': '2026-04-14T00:30:00+00:00',
                            'feedback_payload': {
                                'role': role,
                                'summary': f'{role} summary',
                                'confidence': 0.7,
                                'risk_flags': [],
                                'details': {},
                                'improvement_notes': [],
                            },
                        },
                    )
                    for role in roles
                }
            }

        reuse_service = SimpleNamespace(build_latest_decisions=build_latest_decisions)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DeciderReadyAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                            with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                with _patch_feedback_eligibility('005930'):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930'],
                                        roles=['signal-decider'],
                                        reuse_existing=False,
                                    )

        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(len(failure_service.recorded), 1)
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['failed'], 1)
        self.assertEqual(len(cleanup_service.targets), 1)
        cleanup_target = cleanup_service.targets[0]
        self.assertEqual(cleanup_target.agent_name, 'signal-decider')
        self.assertEqual(cleanup_target.symbol, '005930')
        self.assertEqual(cleanup_target.run_id, result['request']['request_run_id'])
        self.assertIn(result['request']['request_run_id'], cleanup_target.session_key)
        self.assertIn('feedback-fresh/signal-decider/', cleanup_target.session_key)

    def test_build_upstream_feedback_bundle_does_not_reuse_same_day_persisted_summary_when_role_requires_redispatch(self):
        upstream_summaries, upstream_traceability, missing_roles = AgentFeedbackOrchestrator._build_upstream_feedback_bundle(
            symbol='005930',
            market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
            buffered_lookup={
                ('005930', 'eod-summarizer'): {
                    'feedback': {'role': 'eod-summarizer', 'summary': 'fresh eod'},
                    'run_id': 'run-1',
                },
                ('005930', 'fundamental-summarizer'): {
                    'feedback': {'role': 'fundamental-summarizer', 'summary': 'fresh fundamental'},
                    'run_id': 'run-1',
                },
            },
            upstream_reuse_decisions={
                '005930': {
                    'technical-summarizer': SimpleNamespace(
                        should_dispatch=True,
                        reason='source_updated',
                        latest_feedback={
                            'id': 99,
                            'market_date': '2026-04-14',
                            'run_id': 'stale-run',
                            'received_at': '2026-04-14T01:00:00+00:00',
                            'feedback_payload': {'role': 'technical-summarizer', 'summary': 'stale technical'},
                        },
                    )
                }
            },
        )

        self.assertEqual(set(upstream_summaries.keys()), {'eod-summarizer', 'fundamental-summarizer'})
        self.assertNotIn('technical-summarizer', upstream_summaries)
        self.assertEqual(missing_roles, ['technical-summarizer'])
        self.assertEqual(upstream_traceability['eod-summarizer']['source'], 'current_run_buffer')

    def test_signal_decider_missing_upstream_records_dependency_context_without_dispatch(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()

        def build_latest_decisions(**kwargs):
            roles = tuple(kwargs['roles'])
            if roles == ('signal-decider',):
                return {}
            return {
                '005930': {
                    'eod-summarizer': _reuse_decision(True),
                    'technical-summarizer': _reuse_decision(True),
                    'fundamental-summarizer': _reuse_decision(True),
                }
            }

        reuse_service = SimpleNamespace(build_latest_decisions=build_latest_decisions)
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                            with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                with _patch_feedback_eligibility('005930'):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930'],
                                        roles=['signal-decider'],
                                        reuse_existing=False,
                                    )

        self.assertEqual(client.calls, [])
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(len(failure_service.recorded), 1)
        self.assertEqual(
            failure_service.recorded[0]['context'],
            {
                'dependency_status': 'blocked_missing_upstream_summaries',
                'dependency_policy': 'record_failure_and_continue',
                'missing_roles': ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'],
            },
        )
        self.assertIn('missing_decider_upstream_summaries', failure_service.recorded[0]['failure_kind'])

    def test_dispatch_for_symbols_scopes_runtime_mutation_to_explicit_request_run_id(self):
        service = AgentFeedbackOrchestrator()
        service.client = RecordingBatchClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        runtime_store = StrictRuntimeStateStore()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=2)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
                with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                    with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                        with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                            with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                    with _patch_feedback_eligibility('005930'):
                                        result = service.dispatch_for_symbols(
                                            market_date='2026-04-14',
                                            symbols=['005930'],
                                            roles=['eod-summarizer'],
                                            reuse_existing=False,
                                        )

        request_run_id = result['request']['request_run_id']
        self.assertEqual(set(runtime_store.update_run_ids), {request_run_id})
        self.assertEqual(set(runtime_store.get_run_ids), {request_run_id})
        self.assertEqual(run_state_service.started[0]['run_id'], request_run_id)
        self.assertTrue(run_state_service.checkpoints)
        self.assertEqual(run_state_service.finished[0]['run_id'], request_run_id)
        self.assertEqual(run_state_service.finished[0]['status'], 'completed')

    def test_dispatch_for_symbols_persists_partial_run_when_later_role_raises(self):
        service = AgentFeedbackOrchestrator()
        service.client = RecordingBatchClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)
        original_prepare = service._prepare_role_dispatch_items

        def breaking_prepare(*, role, **kwargs):
            if role == 'technical-summarizer':
                raise RuntimeError('boom')
            return original_prepare(role=role, **kwargs)

        with patch.object(service, '_prepare_role_dispatch_items', side_effect=breaking_prepare):
            with patch.object(orchestrator_module, 'settings', fake_settings):
                with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                    with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                        with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                            with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                    with _patch_feedback_eligibility('005930'):
                                        with self.assertRaisesRegex(RuntimeError, 'boom'):
                                            service.dispatch_for_symbols(
                                                market_date='2026-04-14',
                                                symbols=['005930'],
                                                roles=['eod-summarizer', 'technical-summarizer'],
                                                reuse_existing=False,
                                            )

        self.assertEqual(len(feedback_service.bulk_calls), 1)
        self.assertEqual(feedback_service.bulk_calls[0][0]['agent_name'], 'eod-summarizer')
        batch_checkpoints = [
            checkpoint
            for checkpoint in run_state_service.checkpoints
            if checkpoint['summary_payload']['lifecycle_stage'] == 'batch_persisted'
        ]
        self.assertEqual(len(batch_checkpoints), 1)
        self.assertEqual(batch_checkpoints[0]['summary_payload']['flush_result']['feedback_rows_seen'], 1)
        self.assertEqual(run_state_service.finished[0]['status'], 'partial')
        self.assertEqual(run_state_service.finished[0]['completed_count'], 1)
        self.assertEqual(run_state_service.finished[0]['summary_payload']['lifecycle_stage'], 'finished')
        self.assertEqual(run_state_service.finished[0]['summary_payload']['flush_result']['status'], 'noop')
        self.assertIn('boom', run_state_service.finished[0]['summary_payload']['fatal_error'])

    def test_dispatch_for_symbols_continues_to_later_roles_after_role_failures(self):
        service = AgentFeedbackOrchestrator()
        client = PartiallyFailingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                            with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                with _patch_feedback_eligibility('005930', '000660'):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930', '000660'],
                                        roles=['technical-summarizer', 'fundamental-summarizer'],
                                        reuse_existing=False,
                                    )

        self.assertTrue(any(call['spec'].role == 'fundamental-summarizer' for call in client.calls))
        self.assertEqual(result['summary']['status'], 'partial')
        self.assertEqual(len(failure_service.recorded), 2)
        failure_checkpoints = [
            checkpoint
            for checkpoint in run_state_service.checkpoints
            if checkpoint['summary_payload']['lifecycle_stage'] == 'batch_persisted'
            and checkpoint['summary_payload']['flush_result']['failure_rows_seen'] > 0
        ]
        self.assertEqual(len(failure_checkpoints), 2)
        self.assertEqual(
            [checkpoint['summary_payload']['flush_result']['failure_rows_seen'] for checkpoint in failure_checkpoints],
            [1, 1],
        )
        self.assertEqual(run_state_service.finished[0]['status'], 'partial')
        self.assertEqual(run_state_service.finished[0]['summary_payload']['flush_result']['status'], 'noop')
        self.assertEqual(run_state_service.finished[0]['summary_payload']['role_statuses']['fundamental-summarizer'], 'completed')

    def test_dispatch_finalization_persists_failure_rows_when_batch_checkpoint_is_interrupted(self):
        service = AgentFeedbackOrchestrator()
        client = AlwaysFailingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        captured_logs = []
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)
        original_flush_and_checkpoint = service._flush_and_checkpoint_pending_run_buffers

        def interrupt_batch_checkpoint(**kwargs):
            if str(kwargs.get('reason') or '').startswith('batch_completed:'):
                raise RuntimeError('checkpoint interrupted after failure buffered')
            return original_flush_and_checkpoint(**kwargs)

        def capture_log(stage, **kwargs):
            captured_logs.append({'stage': stage, **kwargs})

        with patch.object(service, '_flush_and_checkpoint_pending_run_buffers', side_effect=interrupt_batch_checkpoint):
            with patch.object(orchestrator_module, 'settings', fake_settings):
                with patch.object(orchestrator_module, 'log_agent_feedback_event', side_effect=capture_log):
                    with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                                with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                        with _patch_feedback_eligibility('005930'):
                                            with self.assertRaisesRegex(RuntimeError, 'checkpoint interrupted after failure buffered'):
                                                service.dispatch_for_symbols(
                                                    market_date='2026-04-14',
                                                    symbols=['005930'],
                                                    roles=['eod-summarizer'],
                                                    reuse_existing=False,
                                                )

        self.assertEqual(len(feedback_service.bulk_calls), 0)
        self.assertEqual(len(failure_service.recorded), 1)
        self.assertEqual(failure_service.recorded[0]['agent_name'], 'eod-summarizer')
        self.assertIn('AgentDispatchRetryableError', failure_service.recorded[0]['failure_kind'])
        self.assertEqual(failure_service.recorded[0]['summary']['origin'], 'agent_dispatch_failure')
        self.assertEqual(run_state_service.finished[0]['status'], 'partial')
        self.assertEqual(run_state_service.finished[0]['summary_payload']['flush_result']['reason'], 'finalize')
        self.assertEqual(run_state_service.finished[0]['summary_payload']['flush_result']['failure_rows_persisted'], 1)

        success_log = next(entry for entry in captured_logs if entry['stage'] == 'failure_persistence_success')
        self.assertEqual(success_log['failure_origin'], 'agent_dispatch_failure')
        self.assertEqual(success_log['original_failure']['origin'], 'agent_dispatch_failure')

    def test_dispatch_persists_specific_recovery_diagnostics_in_failure_rows(self):
        service = AgentFeedbackOrchestrator()
        client = DetailedFailingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                            with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                with _patch_feedback_eligibility('005930'):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930'],
                                        roles=['eod-summarizer'],
                                        reuse_existing=False,
                                    )

        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(len(failure_service.recorded), 1)
        recorded = failure_service.recorded[0]
        self.assertEqual(recorded['failure_kind'], 'session_history_missing_or_empty')
        self.assertTrue(recorded['summary']['session_key'].startswith('agent:eod-summarizer:feedback-fresh/eod-summarizer/'))
        self.assertIn('/slot-01/', recorded['summary']['session_key'])
        self.assertEqual(recorded['summary']['session_id'], 'session-traj-1')
        self.assertEqual(recorded['summary']['response_id'], 'resp-decider-traj-1')
        self.assertTrue(str(recorded['summary']['request_marker']).startswith('batch-request::'))
        self.assertEqual(recorded['summary']['failure_mode'], 'session_history_missing_or_empty')
        self.assertEqual(recorded['summary']['failure_diagnostics']['history']['failure_mode'], 'session_history_missing_or_empty')
        self.assertEqual(recorded['summary']['failure_diagnostics']['trajectory']['failure_mode'], 'trajectory_artifacts_not_found')
        self.assertEqual(recorded['context']['dispatch_diagnostics']['failure_kind'], 'session_history_missing_or_empty')

    def test_failure_persistence_failure_log_separates_agent_failure_from_row_write_failure(self):
        service = AgentFeedbackOrchestrator()
        market_date = orchestrator_module.date.fromisoformat('2026-04-14')
        request_run_id = 'run-logging'
        runtime_store = RuntimeFeedbackStateStore()
        state = RuntimeFeedbackRunState(
            run_id=request_run_id,
            market_date=market_date,
            requested_symbols=['005930'],
            requested_roles=['eod-summarizer'],
            role_sequence=['eod-summarizer'],
            parallel_per_role=1,
            batch_size=1,
            started_at=orchestrator_module.datetime.now(orchestrator_module.timezone.utc),
            current_role='eod-summarizer',
        )
        state.pending_failures.append(
            {
                'role': 'eod-summarizer',
                'agent_name': 'eod-summarizer',
                'symbol': '005930',
                'status': 'failed',
                'retryable': True,
                'error': 'agent failed before feedback row',
                'failure_kind': 'agent_failed',
                'dispatch_id': 'dispatch-1',
                'attempt_id': 'dispatch-1:attempt-1',
                'slot': 1,
                'session_key': 'feedback/eod-summarizer/slot-01',
                'batch_symbols': ['005930'],
                'context': {'symbol': '005930'},
            }
        )
        runtime_store.start_run(state)
        captured_logs = []

        class FailingFailureService:
            def record_failure(self, **kwargs):
                raise RuntimeError('failure table unavailable')

        def capture_log(stage, **kwargs):
            captured_logs.append({'stage': stage, **kwargs})

        with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
            with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', FailingFailureService()):
                with patch.object(orchestrator_module, 'log_agent_feedback_event', side_effect=capture_log):
                    with self.assertRaisesRegex(RuntimeError, 'failure table unavailable'):
                        service._flush_pending_run_buffers(
                            market_date=market_date,
                            request_run_id=request_run_id,
                            reason='finalize',
                        )

        persistence_failure_log = next(entry for entry in captured_logs if entry['stage'] == 'failure_persistence_failure')
        self.assertEqual(persistence_failure_log['failure_origin'], 'agent_dispatch_failure')
        self.assertEqual(persistence_failure_log['persistence_error_origin'], 'failure_row_persistence')
        self.assertEqual(persistence_failure_log['original_failure']['error_message'], 'agent failed before feedback row')
        self.assertEqual(persistence_failure_log['error_type'], 'RuntimeError')

    def test_openclaw_batch_dispatch_logs_request_response_and_symbol_mismatch(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:eod-summarizer:slot-1:batch-1'
        http_calls = []
        captured_logs = []
        response_payload = {
            'id': 'resp-1',
            'status': 'completed',
            'model': 'openclaw/eod-summarizer',
            'output': [
                {
                    'type': 'message',
                    'content': [
                        {
                            'type': 'output_text',
                            'text': json.dumps(
                                {
                                    'items': [
                                        {
                                            'symbol': '005930',
                                            'feedback': {
                                                'role': 'eod-summarizer',
                                                'summary': 'Constructive close.',
                                                'confidence': 0.6,
                                                'risk_flags': [],
                                                'details': {},
                                                'improvement_notes': [],
                                            },
                                        },
                                        {
                                            'symbol': '999999',
                                            'feedback': {
                                                'role': 'eod-summarizer',
                                                'summary': 'Unexpected stale symbol.',
                                                'confidence': 0.2,
                                                'risk_flags': ['stale_session_reuse'],
                                                'details': {},
                                                'improvement_notes': [],
                                            },
                                        },
                                    ]
                                }
                            ),
                        }
                    ],
                }
            ],
        }
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )

        def fake_client_factory(*args, **kwargs):
            return FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                with patch.object(
                    orchestrator_module,
                    'log_agent_feedback_event',
                    side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                ):
                    client = OpenClawAgentClient()
                    result = client.dispatch_batch(
                        spec=orchestrator_module.DISPATCH_SPECS[0],
                        market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                        batch_items=[
                            {
                                'symbol': '005930',
                                'request_run_id': 'symbol-run-2026-04-14-abc123',
                                'dispatch_id': dispatch_id,
                                'session_slot': 1,
                                'session_key': 'agent:eod-summarizer:feedback/eod-summarizer/slot-01',
                            }
                        ],
                    )

        self.assertEqual(result['dispatch_id'], dispatch_id)
        self.assertEqual(result['attempt_id'], f'{dispatch_id}:attempt-1')
        self.assertEqual(http_calls[0]['url'], 'https://openclaw.example/v1/responses')
        self.assertEqual(http_calls[0]['json']['model'], 'openclaw/eod-summarizer')
        self.assertIn('dispatch_batch_request', [entry['stage'] for entry in captured_logs])
        self.assertIn('dispatch_batch_response', [entry['stage'] for entry in captured_logs])
        mismatch_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_symbol_mismatch')
        self.assertEqual(mismatch_log['dispatch_id'], dispatch_id)
        self.assertEqual(mismatch_log['unexpected_symbols'], ['999999'])
        self.assertEqual(mismatch_log['missing_symbols'], [])
        self.assertEqual(mismatch_log['failure_class'], 'batch_response_attribution_mismatch')
        self.assertEqual(mismatch_log['session_reuse_mode'], 'slot_reused')

    def test_signal_decider_one_symbol_batch_attributes_missing_symbol_under_reused_session(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:signal-decider:slot-1:batch-1'
        http_calls = []
        captured_logs = []
        response_payload = {
            'id': 'resp-decider-1',
            'status': 'completed',
            'model': 'openclaw/signal-decider',
            'output': [
                {
                    'type': 'message',
                    'content': [
                        {
                            'type': 'output_text',
                            'text': json.dumps(
                                {
                                    'items': [
                                        {
                                            'feedback': {
                                                'role': 'signal-decider',
                                                'agent_name': 'signal-decider',
                                                'market_date': '2026-04-14',
                                                'preferred_action': 'wait',
                                                'next_day_open_action': 'wait',
                                                'entry_price_basis': 'next_open',
                                                'target_price': None,
                                                'expected_return_pct': None,
                                                'stop_loss_price': None,
                                                'suggested_holding_period': '3-10 trading days',
                                                'confidence_score_10': 6,
                                                'conviction_score_10': 5,
                                                'thesis_summary': 'Valid one-symbol decision support.',
                                                'supporting_evidence': ['Specialists are aligned enough to wait.'],
                                                'decision_risks': [],
                                                'invalidation_conditions': [],
                                                'data_quality_warnings': [],
                                                'specialist_disagreement': [],
                                                'summary': 'Valid one-symbol decision support.',
                                                'confidence': 0.6,
                                                'risk_flags': [],
                                                'details': {'decision_support': {'decision_support_only': True}},
                                                'improvement_notes': [],
                                            },
                                        }
                                    ]
                                }
                            ),
                        }
                    ],
                }
            ],
        }
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )

        def fake_client_factory(*args, **kwargs):
            return FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                with patch.object(
                    orchestrator_module,
                    'log_agent_feedback_event',
                    side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                ):
                    client = OpenClawAgentClient()
                    result = client.dispatch_batch(
                        spec=orchestrator_module.DISPATCH_SPECS[-1],
                        market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                        batch_items=[
                            {
                                'symbol': '005930',
                                'market_date': '2026-04-14',
                                'request_run_id': 'symbol-run-2026-04-14-abc123',
                                'dispatch_id': dispatch_id,
                                'session_slot': 1,
                                'session_key': 'agent:signal-decider:feedback/signal-decider/slot-01',
                                'session_reuse_mode': 'slot_reused',
                            }
                        ],
                    )

        decoded_item = result['decoded']['items'][0]
        self.assertEqual(decoded_item['symbol'], '005930')
        self.assertEqual(result['response_correlation']['attribution_status'], 'fallback_attributed')
        self.assertEqual(result['response_correlation']['fallback_attributions'][0]['reason'], 'single_symbol_batch')
        self.assertIn('fallback_symbol_attribution', decoded_item['feedback']['risk_flags'])
        self.assertIn('response_symbol_missing', decoded_item['feedback']['risk_flags'])
        self.assertNotIn('batch_response_symbol_mismatch', [entry['stage'] for entry in captured_logs])
        fallback_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_symbol_attribution_fallback')
        self.assertEqual(fallback_log['session_reuse_mode'], 'slot_reused')
        self.assertEqual(fallback_log['fallback_attributions'][0]['symbol'], '005930')

    def test_single_symbol_batch_keeps_contradictory_envelope_symbol_as_mismatch(self):
        decoded, metadata = correlate_decoded_batch_response(
            decoded={
                'items': [
                    {
                        'symbol': '999999',
                        'feedback': {
                            'symbol': '005930',
                            'role': 'signal-decider',
                            'summary': 'Nested feedback named the requested symbol.',
                            'confidence': 0.6,
                            'risk_flags': [],
                            'details': {},
                            'improvement_notes': [],
                        },
                    }
                ]
            },
            expected_symbols=['005930'],
            session_reuse_mode='slot_reused',
        )

        self.assertEqual(decoded['items'][0]['symbol'], '999999')
        self.assertEqual(decoded['items'][0]['feedback']['risk_flags'], [])
        self.assertEqual(metadata['attribution_status'], 'mismatch')
        self.assertEqual(metadata['failure_class'], 'batch_response_attribution_mismatch')
        self.assertEqual(metadata['missing_symbols'], ['005930'])
        self.assertEqual(metadata['unexpected_symbols'], ['999999'])
        self.assertEqual(metadata['fallback_attributions'], [])

    def test_openclaw_batch_dispatch_classifies_invalid_json_parse_failures(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:signal-decider:slot-1:batch-1'
        request_marker = f'batch-request::{dispatch_id}:attempt-1'
        http_calls = []
        captured_logs = []
        response_payload = {
            'id': 'resp-decider-2',
            'status': 'completed',
            'model': 'openclaw/signal-decider',
            'output': [
                {
                    'type': 'message',
                    'content': [{'type': 'output_text', 'text': 'not json'}],
                }
            ],
        }
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
            openclaw_state_dir='/tmp/openclaw-test-state',
        )

        def fake_client_factory(*args, **kwargs):
            return FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                with patch.object(
                    OpenClawAgentClient,
                    'fetch_session_history',
                    return_value=[
                        {
                            'role': 'user',
                            'content': [{'type': 'input_text', 'text': f'Request marker: {request_marker}'}],
                        },
                        {
                            'role': 'assistant',
                            'content': [{'type': 'output_text', 'text': 'still not json'}],
                        },
                    ],
                ):
                    with patch.object(
                        orchestrator_module,
                        'log_agent_feedback_event',
                        side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                    ):
                        client = OpenClawAgentClient()
                        with self.assertRaisesRegex(orchestrator_module.AgentDispatchRetryableError, 'invalid_json'):
                            client.dispatch_batch(
                                spec=orchestrator_module.DISPATCH_SPECS[-1],
                                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                                batch_items=[
                                    {
                                        'symbol': '005930',
                                        'market_date': '2026-04-14',
                                        'request_run_id': 'symbol-run-2026-04-14-abc123',
                                        'dispatch_id': dispatch_id,
                                        'session_slot': 1,
                                        'session_key': 'agent:signal-decider:feedback/signal-decider/slot-01',
                                        'session_reuse_mode': 'slot_reused',
                                    }
                                ],
                            )

        parse_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_parse_failed')
        self.assertEqual(parse_log['failure_class'], 'invalid_json')
        self.assertEqual(parse_log['failure_mode'], 'session_history_assistant_non_json_after_marker')
        self.assertEqual(parse_log['json_failure_class'], 'json_object_not_found')
        self.assertEqual(
            parse_log['response_correlation']['response_recovery']['history']['failure_mode'],
            'session_history_assistant_non_json_after_marker',
        )
        self.assertNotIn('batch_response_symbol_mismatch', [entry['stage'] for entry in captured_logs])

    def test_openclaw_batch_dispatch_recovers_from_trajectory_when_session_history_is_missing(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:signal-decider:slot-1:batch-1'
        session_key = 'agent:signal-decider:feedback/signal-decider/slot-01'
        http_calls = []
        captured_logs = []
        response_payload = {
            'id': 'resp-decider-traj-1',
            'status': 'completed',
            'model': 'openclaw/signal-decider',
            'output': [
                {
                    'type': 'message',
                    'content': [{'type': 'output_text', 'text': 'not json'}],
                }
            ],
        }
        trajectory_text = json.dumps(
            {
                'items': [
                    {
                        'symbol': '005930',
                        'feedback': {
                            'role': 'signal-decider',
                            'agent_name': 'signal-decider',
                            'symbol': '005930',
                            'market_date': '2026-04-14',
                            'preferred_action': 'wait',
                            'next_day_open_action': 'wait',
                            'entry_price_basis': 'next_open',
                            'target_price': None,
                            'expected_return_pct': None,
                            'stop_loss_price': None,
                            'suggested_holding_period': '1-5 trading days',
                            'confidence_score_10': 6,
                            'conviction_score_10': 4,
                            'thesis_summary': 'Recovered from trajectory artifacts.',
                            'supporting_evidence': ['Trajectory artifacts preserved the JSON envelope.'],
                            'decision_risks': [],
                            'invalidation_conditions': [],
                            'data_quality_warnings': [],
                            'specialist_disagreement': [],
                            'traceability': {
                                'agent_name': 'signal-decider',
                                'symbol': '005930',
                                'market_date': '2026-04-14',
                            },
                            'summary': 'Recovered response.',
                            'confidence': 0.6,
                            'risk_flags': [],
                            'details': {'decision_support': {'decision_support_only': True}},
                            'improvement_notes': [],
                        },
                    }
                ]
            },
            ensure_ascii=False,
        )

        def fake_client_factory(*args, **kwargs):
            return FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            )

        with tempfile.TemporaryDirectory() as tempdir:
            sessions_dir = Path(tempdir) / 'agents' / 'signal-decider' / 'sessions'
            sessions_dir.mkdir(parents=True)
            trajectory_path = sessions_dir / 'session-traj-1.trajectory.jsonl'
            trajectory_path.write_text(
                '\n'.join(
                    [
                        json.dumps(
                            {
                                'type': 'session.started',
                                'sessionId': 'session-traj-1',
                                'sessionKey': session_key,
                                'runId': 'resp-decider-traj-1',
                                'data': {},
                            }
                        ),
                        json.dumps(
                            {
                                'type': 'trace.artifacts',
                                'sessionId': 'session-traj-1',
                                'sessionKey': session_key,
                                'runId': 'resp-decider-traj-1',
                                'data': {
                                    'assistantTexts': [trajectory_text],
                                    'finalStatus': 'success',
                                    'aborted': False,
                                    'externalAbort': False,
                                    'timedOut': False,
                                    'idleTimedOut': False,
                                    'timedOutDuringCompaction': False,
                                },
                            }
                        ),
                    ]
                )
                + '\n',
                encoding='utf-8',
            )
            fake_settings = SimpleNamespace(
                openclaw_base_url='https://openclaw.example',
                openclaw_gateway_token='gateway-token',
                openclaw_agent_timeout_seconds=30,
                openclaw_agent_max_retries=0,
                openclaw_agent_retry_backoff_seconds=0,
                openclaw_agent_max_prompt_chars=0,
                openclaw_state_dir=tempdir,
            )

            with patch.object(orchestrator_module, 'settings', fake_settings):
                with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                    with patch.object(OpenClawAgentClient, 'fetch_session_history', return_value=[]):
                        with patch.object(
                            orchestrator_module,
                            'log_agent_feedback_event',
                            side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                        ):
                            client = OpenClawAgentClient()
                            result = client.dispatch_batch(
                                spec=orchestrator_module.DISPATCH_SPECS[-1],
                                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                                batch_items=[
                                    {
                                        'symbol': '005930',
                                        'market_date': '2026-04-14',
                                        'request_run_id': 'symbol-run-2026-04-14-abc123',
                                        'dispatch_id': dispatch_id,
                                        'session_slot': 1,
                                        'session_key': session_key,
                                        'session_reuse_mode': 'fresh_per_batch',
                                    }
                                ],
                            )

        self.assertEqual(result['decoded']['items'][0]['symbol'], '005930')
        self.assertEqual(result['response_correlation']['decoded_source'], 'trajectory_artifacts')
        self.assertEqual(result['session_ref']['session_id'], 'session-traj-1')
        self.assertEqual(result['session_ref']['response_id'], 'resp-decider-traj-1')
        self.assertTrue(result['history_checked'])
        recovery = result['response_correlation']['response_recovery']
        self.assertEqual(recovery['history']['failure_mode'], 'session_history_missing_or_empty')
        self.assertEqual(recovery['trajectory']['status'], 'recovered')
        self.assertTrue(recovery['trajectory']['matched_response_id'])
        self.assertNotIn('batch_response_parse_failed', [entry['stage'] for entry in captured_logs])

    def test_openclaw_batch_dispatch_fails_with_trajectory_response_id_mismatch_when_only_stale_run_artifacts_exist(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:signal-decider:slot-1:batch-1'
        session_key = 'agent:signal-decider:feedback/signal-decider/slot-01'
        http_calls = []
        captured_logs = []
        response_payload = {
            'id': 'resp-decider-traj-expected',
            'status': 'completed',
            'model': 'openclaw/signal-decider',
            'output': [
                {
                    'type': 'message',
                    'content': [{'type': 'output_text', 'text': 'not json'}],
                }
            ],
        }
        stale_trajectory_text = json.dumps(
            {
                'items': [
                    {
                        'symbol': '005930',
                        'feedback': {
                            'role': 'signal-decider',
                            'summary': 'Stale trajectory response that must not be reused.',
                            'confidence': 0.6,
                            'risk_flags': [],
                            'details': {},
                            'improvement_notes': [],
                        },
                    }
                ]
            }
        )

        def fake_client_factory(*args, **kwargs):
            return FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            )

        with tempfile.TemporaryDirectory() as tempdir:
            sessions_dir = Path(tempdir) / 'agents' / 'signal-decider' / 'sessions'
            sessions_dir.mkdir(parents=True)
            trajectory_path = sessions_dir / 'session-traj-1.trajectory.jsonl'
            trajectory_path.write_text(
                '\n'.join(
                    [
                        json.dumps(
                            {
                                'type': 'session.started',
                                'sessionId': 'session-traj-1',
                                'sessionKey': session_key,
                                'runId': 'stale-run-id',
                                'data': {},
                            }
                        ),
                        json.dumps(
                            {
                                'type': 'trace.artifacts',
                                'sessionId': 'session-traj-1',
                                'sessionKey': session_key,
                                'runId': 'stale-run-id',
                                'data': {
                                    'assistantTexts': [stale_trajectory_text],
                                    'finalStatus': 'success',
                                },
                            }
                        ),
                    ]
                )
                + '\n',
                encoding='utf-8',
            )
            fake_settings = SimpleNamespace(
                openclaw_base_url='https://openclaw.example',
                openclaw_gateway_token='gateway-token',
                openclaw_agent_timeout_seconds=30,
                openclaw_agent_max_retries=0,
                openclaw_agent_retry_backoff_seconds=0,
                openclaw_agent_max_prompt_chars=0,
                openclaw_state_dir=tempdir,
            )

            with patch.object(orchestrator_module, 'settings', fake_settings):
                with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                    with patch.object(OpenClawAgentClient, 'fetch_session_history', return_value=[]):
                        with patch.object(
                            orchestrator_module,
                            'log_agent_feedback_event',
                            side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                        ):
                            client = OpenClawAgentClient()
                            with self.assertRaisesRegex(
                                orchestrator_module.AgentDispatchRetryableError,
                                'trajectory_response_id_mismatch',
                            ) as exc:
                                client.dispatch_batch(
                                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                                    market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                                    batch_items=[
                                        {
                                            'symbol': '005930',
                                            'market_date': '2026-04-14',
                                            'request_run_id': 'symbol-run-2026-04-14-abc123',
                                            'dispatch_id': dispatch_id,
                                            'session_slot': 1,
                                            'session_key': session_key,
                                            'session_reuse_mode': 'fresh_per_batch',
                                        }
                                    ],
                                )

        self.assertEqual(exc.exception.details['failure_mode'], 'trajectory_response_id_mismatch')
        self.assertEqual(exc.exception.details['failure_class'], 'trajectory_response_id_mismatch')
        self.assertEqual(
            exc.exception.details['response_recovery']['trajectory']['failure_mode'],
            'trajectory_response_id_mismatch',
        )
        self.assertFalse(exc.exception.details['response_recovery']['trajectory']['matched_response_id'])
        self.assertEqual(
            exc.exception.details['response_recovery']['trajectory']['candidate_run_ids'],
            ['stale-run-id'],
        )
        parse_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_parse_failed')
        self.assertEqual(parse_log['failure_mode'], 'trajectory_response_id_mismatch')
        self.assertEqual(parse_log['failure_class'], 'trajectory_response_id_mismatch')
        self.assertEqual(
            parse_log['response_correlation']['response_recovery']['trajectory']['failure_mode'],
            'trajectory_response_id_mismatch',
        )

    def test_spawn_symbol_subagent_terminal_error_logs_only_in_scope_fields(self):
        http_calls = []
        captured_logs = []
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )

        def fake_client_factory(*args, **kwargs):
            return FakeHTTPClient(
                response=FakeHTTPResponse(404, {'error': 'missing'}),
                calls=http_calls,
                **kwargs,
            )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                with patch.object(
                    orchestrator_module,
                    'log_agent_feedback_event',
                    side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                ):
                    client = OpenClawAgentClient()
                    with self.assertRaisesRegex(orchestrator_module.AgentDispatchTerminalError, 'sessions_spawn is not available'):
                        client.spawn_symbol_subagent(
                            spec=orchestrator_module.DISPATCH_SPECS[0],
                            market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                            symbol_item={'symbol': '005930'},
                            request_run_id='symbol-run-2026-04-14-abc123',
                        )

        self.assertEqual(http_calls[0]['url'], 'https://openclaw.example/tools/invoke')
        error_log = next(entry for entry in captured_logs if entry['stage'] == 'subagent_spawn_error')
        self.assertEqual(error_log['status'], 'terminal')
        self.assertEqual(error_log['request_run_id'], 'symbol-run-2026-04-14-abc123')
        self.assertEqual(error_log['agent_name'], 'eod-summarizer')
        self.assertEqual(error_log['role'], 'eod-summarizer')
        self.assertEqual(error_log['symbol'], '005930')
        self.assertTrue(str(error_log['session_key']).startswith('subagent-test/eod-summarizer/symbol-run-2026-04-14-abc123/005930-'))
        self.assertEqual(error_log['error_type'], 'AgentDispatchTerminalError')
        self.assertNotIn('dispatch_id', error_log)
        self.assertNotIn('attempt_id', error_log)
        self.assertEqual(error_log['spawn_target']['url'], 'https://openclaw.example/tools/invoke')

    def test_dispatch_for_symbols_logs_lifecycle_with_dispatch_correlation(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        captured_logs = []
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with _patch_feedback_eligibility('005930'):
                                with patch.object(
                                    orchestrator_module,
                                    'log_agent_feedback_event',
                                    side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                                ):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=['005930'],
                                        roles=['eod-summarizer'],
                                        reuse_existing=False,
                                    )

        request_run_id = result['request']['request_run_id']
        stages = {entry['stage'] for entry in captured_logs}
        self.assertTrue(
            {
                'run_started',
                'role_started',
                'normalized_response_accepted',
                'feedback_persistence_attempt',
                'feedback_persistence_success',
                'role_checkpoint_summary',
                'final_reconciliation',
                'run_finished',
            }.issubset(stages)
        )
        normalized_log = next(entry for entry in captured_logs if entry['stage'] == 'normalized_response_accepted')
        self.assertTrue(normalized_log['dispatch_id'].startswith(request_run_id))
        self.assertEqual(normalized_log['symbol'], '005930')
        self.assertEqual(normalized_log['attempt_id'], f"{normalized_log['dispatch_id']}:attempt-1")
        persistence_attempt = next(entry for entry in captured_logs if entry['stage'] == 'feedback_persistence_attempt')
        persistence_success = next(entry for entry in captured_logs if entry['stage'] == 'feedback_persistence_success')
        self.assertEqual(persistence_attempt['dispatch_id'], normalized_log['dispatch_id'])
        self.assertEqual(persistence_success['dispatch_id'], normalized_log['dispatch_id'])
        final_log = next(entry for entry in captured_logs if entry['stage'] == 'run_finished')
        self.assertEqual(final_log['request_run_id'], request_run_id)
        self.assertEqual(final_log['terminal_status'], 'completed')

    def test_dispatch_for_symbols_logs_stale_reuse_signal(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        captured_logs = []
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)
        reuse_service = SimpleNamespace(
            build_latest_decisions=lambda **kwargs: {
                '005930': {
                    'eod-summarizer': SimpleNamespace(
                        symbol='005930',
                        agent_name='eod-summarizer',
                        should_dispatch=True,
                        reason='stale_market_feedback',
                        latest_feedback_id=41,
                        latest_feedback_received_at='2026-04-13T06:00:00+00:00',
                        latest_source_updated_at='2026-04-14T06:05:00+00:00',
                    )
                }
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                    with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                        with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                            with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                with _patch_feedback_eligibility('005930'):
                                    with patch.object(
                                        orchestrator_module,
                                        'log_agent_feedback_event',
                                        side_effect=lambda stage, **payload: captured_logs.append({'stage': stage, **payload}),
                                    ):
                                        service.dispatch_for_symbols(
                                            market_date='2026-04-14',
                                            symbols=['005930'],
                                            roles=['eod-summarizer'],
                                            reuse_existing=True,
                                        )

        reuse_log = next(entry for entry in captured_logs if entry['stage'] == 'reuse_signal')
        self.assertEqual(reuse_log['symbol'], '005930')
        self.assertEqual(reuse_log['reuse_signal'], 'stale_feedback')
        self.assertEqual(reuse_log['status'], 'redispatch_required')
        self.assertEqual(reuse_log['reuse_reason'], 'stale_market_feedback')


if __name__ == '__main__':
    unittest.main()
