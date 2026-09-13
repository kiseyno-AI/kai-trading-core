from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import httpx
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import AgentFeedbackOrchestrator, OpenClawAgentClient
from app.agent_context.orchestrator_helpers import (
    build_durable_decider_input_generation,
    build_response_storage_payload,
    build_signal_decider_payload,
    coerce_decoded_batch_response,
    compressed_batch_item_for_prompt,
    correlate_decoded_batch_response,
    extract_response_mismatch_flags,
    trim_batch_item_for_prompt,
)
from app.agent_context.prerequisite_service import FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
from app.agent_context.runtime_state import RuntimeFeedbackRunState, RuntimeFeedbackStateStore
from app.agent_context.orchestrator_runtime_helpers import (
    batch_response_contract_failure,
    dispatch_and_store,
    normalized_feedback_contract_failure,
)
from app.agent_context.model_identity import build_observed_model_identity
from app.orders.signal_decider_performance_gates import evaluate_next_open_performance_gate


def _attach_response_correlation(result: dict, *, expected_symbols: list[str]) -> dict:
    decoded, response_correlation = correlate_decoded_batch_response(
        decoded=result.get('decoded'),
        expected_symbols=expected_symbols,
        session_reuse_mode='fresh_per_batch',
    )
    result['decoded'] = decoded
    result['response_correlation'] = response_correlation
    return result


def _batch_result(symbol: str) -> dict:
    return _attach_response_correlation({
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
    }, expected_symbols=[symbol])


def _complete_actionable_signal_feedback(*, symbol: str = '005930') -> dict:
    return {
        'role': 'signal-decider',
        'agent_name': 'signal-decider',
        'symbol': symbol,
        'market_date': '2026-04-23',
        'preferred_action': 'buy',
        'next_day_open_action': 'buy',
        'wait_reason': None,
        'entry_price_basis': 'next_open',
        'target_price': 91200,
        'expected_return_pct': 6.4,
        'stop_loss_price': 82900,
        'suggested_holding_period': '3-10 trading days',
        'confidence_score_10': 7,
        'conviction_score_10': 6,
        'thesis_summary': 'Momentum and execution conditions support a buy.',
        'supporting_evidence': ['Relative strength and volume improved together.'],
        'decision_risks': ['Opening volatility may expand.'],
        'decision_delta': {},
        'invalidation_conditions': ['The thesis fails below the stop price.'],
        'data_quality_warnings': [],
        'specialist_disagreement': [],
        'traceability': {'symbol': symbol},
        'summary': 'Momentum and execution conditions support a buy.',
        'confidence': 0.7,
        'risk_flags': [],
        'details': {'decision_support': {'decision_support_only': True}},
        'improvement_notes': [],
    }


def _runtime_contract_proof(digest: str) -> dict:
    return {
        'role': 'signal-decider',
        'version': 'v1',
        'expected_sha256': 'sha256:canonical',
        'live_sha256': f'sha256:{digest}',
        'verified': True,
        'status': 'verified',
        'reason': None,
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


class DurableDeciderInputGenerationTests(unittest.TestCase):
    @staticmethod
    def _source_context() -> dict:
        return {
            'symbol': '005930',
            'market_date': '2026-04-23',
            'company': {
                'name': 'Samsung Electronics',
                'market': 'KOSPI',
                'is_active': True,
                'source': 'pykrx',
                'last_refreshed_at': '2026-04-23T07:00:00Z',
            },
            'market_session': {'current_calendar_date': '2026-04-23'},
            'market_data_phase_contract': {'current_market_state': 'closed'},
            'quote_context': {'price': 85700, 'captured_at': '2026-04-23T06:20:00Z'},
            'intraday_execution_context': {
                'quote': {'price': 85700},
                'orderbook': {'best_bid_price': 85600, 'best_ask_price': 85700},
            },
            'liquidity_context': {
                'status': 'available',
                'turnover': {'turnover_krw': 8800000000, 'trade_date': '2026-04-23'},
                'ranking': {'liquidity_percentile': 0.91},
                'intraday': {'quote_spread_points': 100, 'quote_balance_rank': 5},
            },
            'pre_decider_enrichment_context': {
                'quote_orderbook_snapshot': {'current_price': 85700},
            },
            'market_regime_context': {
                'open_drive': {'index_change_percent': 0.7},
            },
            'decider_data_quality_context': {'status': 'available'},
            'decision_input_quality_context': {'status': 'available'},
            'context_completeness': {'quote_context': {'status': 'available'}},
            'availability': {
                'quote_context': True,
                'intraday_execution_context': True,
                'technical_snapshot': True,
            },
            'context_provenance': {
                'quote_context': {'as_of_timestamp': '2026-04-23T06:20:00Z'},
                'intraday_execution_context': {'as_of_timestamp': '2026-04-23T06:20:00Z'},
                'technical_snapshot': {'as_of_market_date': '2026-04-23'},
                'recent_external_news': {
                    'provider_status': {
                        'enabled': True,
                        'collection_enabled': True,
                        'status': 'ready',
                        'reason': None,
                    },
                },
            },
            'price_context': {'close_price': 85700, 'trade_date': '2026-04-23'},
            'technical_snapshot': {'trend_state': 'uptrend'},
            'action_summary': {'action_label': 'watch_long'},
            'macro_context': {
                'status': 'available',
                'generated_at': '2026-04-23T07:00:00Z',
                'series': [
                    {
                        'series_id': 'DEXKOUS',
                        'latest_value': 1380.5,
                        'freshness': {
                            'status': 'fresh',
                            'age_hours': 1.0,
                            'fetched_at': '2026-04-23T06:00:00Z',
                        },
                    }
                ],
            },
            'market_breadth_context': {
                'status': 'available',
                'source': 'pykrx_derived',
                'up_count': 470,
                'down_count': 260,
            },
            'agent_feedback_eligibility': {
                'eligible': True,
                'candidate_rank': 2,
            },
            'recent_external_news': {
                'status': 'available',
                'reason': None,
                'provider': {
                    'provider': 'naver_news',
                    'context_enabled': True,
                    'storage_read_enabled': True,
                    'enabled': True,
                    'collection_enabled': True,
                    'status': 'ready',
                    'reason': None,
                },
                'knowledge_cutoff_source': 'request_time',
                'item_count': 1,
                'items': [
                    {
                        'item_id': 77,
                        'title': 'Existing material filing coverage',
                        'materiality': 'material',
                    }
                ],
                'freshness_provenance': {
                    'provider_status': {
                        'enabled': True,
                        'collection_enabled': True,
                        'status': 'ready',
                        'reason': None,
                    },
                },
            },
        }

    def test_live_session_changes_do_not_revoke_durable_eod_generation(self):
        friday = self._source_context()
        monday = deepcopy(friday)
        monday['market_session'] = {'current_calendar_date': '2026-04-27'}
        monday['market_data_phase_contract'] = {'current_market_state': 'open'}
        monday['quote_context'] = {
            'price': 90100,
            'captured_at': '2026-04-27T00:01:00Z',
        }
        monday['intraday_execution_context'] = {
            'quote': {'price': 90100},
            'orderbook': {'best_bid_price': 90000, 'best_ask_price': 90100},
        }
        monday['liquidity_context']['intraday'] = {
            'quote_spread_points': 100,
            'quote_balance_rank': 1,
        }
        monday['pre_decider_enrichment_context'] = {
            'quote_orderbook_snapshot': {'current_price': 90100},
        }
        monday['market_regime_context'] = {
            'open_drive': {'index_change_percent': -1.3},
        }
        monday['decider_data_quality_context'] = {'status': 'partial'}
        monday['decision_input_quality_context'] = {'status': 'partial'}
        monday['context_completeness']['quote_context'] = {'status': 'fresh'}
        monday['availability']['quote_context'] = False
        monday['availability']['intraday_execution_context'] = False
        monday['context_provenance']['quote_context'] = {
            'as_of_timestamp': '2026-04-27T00:01:00Z',
        }
        monday['context_provenance']['intraday_execution_context'] = {
            'as_of_timestamp': '2026-04-27T00:01:00Z',
        }
        monday['company']['last_refreshed_at'] = '2026-04-27T00:00:00Z'
        monday['company']['source'] = 'kis_master'
        monday['macro_context']['generated_at'] = '2026-04-27T00:01:00Z'
        monday['macro_context']['series'][0]['freshness']['age_hours'] = 97.0
        monday['recent_external_news']['knowledge_cutoff_source'] = 'explicit'
        for provider_status in (
            monday['recent_external_news']['provider'],
            monday['recent_external_news']['freshness_provenance'][
                'provider_status'
            ],
            monday['context_provenance']['recent_external_news'][
                'provider_status'
            ],
        ):
            provider_status.update(
                {
                    'enabled': False,
                    'collection_enabled': False,
                    'status': 'disabled',
                    'reason': 'credentials_missing',
                }
            )

        self.assertEqual(
            build_durable_decider_input_generation(source_context=friday),
            build_durable_decider_input_generation(source_context=monday),
        )

    def test_eod_or_eligibility_changes_revoke_durable_generation(self):
        original = self._source_context()
        changed_eod = deepcopy(original)
        changed_eod['technical_snapshot']['trend_state'] = 'downtrend'
        changed_eligibility = deepcopy(original)
        changed_eligibility['agent_feedback_eligibility']['candidate_rank'] = 9
        changed_company = deepcopy(original)
        changed_company['company']['market'] = 'KOSDAQ'
        changed_news = deepcopy(original)
        changed_news['recent_external_news']['items'].append(
            {
                'item_id': 78,
                'title': 'New material event',
                'materiality': 'material',
            }
        )
        changed_news['recent_external_news']['item_count'] = 2

        expected = build_durable_decider_input_generation(
            source_context=original
        )
        self.assertNotEqual(
            expected,
            build_durable_decider_input_generation(source_context=changed_eod),
        )
        self.assertNotEqual(
            expected,
            build_durable_decider_input_generation(
                source_context=changed_eligibility
            ),
        )
        self.assertNotEqual(
            expected,
            build_durable_decider_input_generation(source_context=changed_company),
        )
        self.assertNotEqual(
            expected,
            build_durable_decider_input_generation(source_context=changed_news),
        )

    def test_live_open_drive_breadth_cannot_claim_durable_reuse(self):
        source = self._source_context()
        source['market_breadth_context'] = {
            'status': 'available',
            'source': 'kis_market_snapshot_v1',
            'up_count': 510,
            'down_count': 220,
            'freshness_provenance': {
                'source_provider': 'kis_market_snapshot_v1',
                'source_tables': ['current_market_snapshot_cache'],
            },
        }

        generation = build_durable_decider_input_generation(
            source_context=source
        )

        self.assertFalse(generation['durable_reuse_eligible'])
        self.assertEqual(
            generation['non_durable_reasons'],
            ['live_market_breadth_context'],
        )

    def test_scalar_live_breadth_source_table_cannot_claim_durable_reuse(self):
        source = self._source_context()
        source['market_breadth_context'] = {
            'status': 'available',
            'source': 'persisted_snapshot',
            'freshness_provenance': {
                'source_tables': 'current_market_snapshot_cache',
            },
        }

        generation = build_durable_decider_input_generation(
            source_context=source
        )

        self.assertFalse(generation['durable_reuse_eligible'])
        self.assertEqual(
            generation['non_durable_reasons'],
            ['live_market_breadth_context'],
        )

    def test_durable_generation_binds_the_original_news_cutoff(self):
        source = self._source_context()

        friday_close = build_durable_decider_input_generation(
            source_context=source,
            source_knowledge_cutoff_at='2026-04-23T06:30:00+00:00',
        )
        later_cutoff = build_durable_decider_input_generation(
            source_context=source,
            source_knowledge_cutoff_at='2026-04-26T23:00:00+00:00',
        )

        self.assertNotEqual(friday_close, later_cutoff)

    def test_actionable_legacy_snapshot_is_not_execution_valid(self):
        snapshot = SimpleNamespace(
            id=101,
            request_payload={},
            response_payload={'preferred_action': 'buy'},
            decision_run_id='legacy-run',
            agent_name='signal-decider',
            symbol='005930',
            decision_action='buy',
            next_day_open_action='buy',
        )

        result = AgentFeedbackOrchestrator().revalidate_persisted_decider_snapshots(
            market_date='2026-04-23',
            snapshots=[snapshot],
        )

        self.assertEqual(result['valid_snapshot_ids'], [])
        self.assertEqual(result['invalid_snapshot_ids'], [101])
        self.assertEqual(
            result['validations'][0]['reason'],
            'actionable_snapshot_missing_durable_generation_fence',
        )

    def test_json_string_snapshot_payloads_are_normalized_before_fence_validation(self):
        legacy_snapshot = SimpleNamespace(
            id=103,
            request_payload=json.dumps({}),
            response_payload=json.dumps(
                {'tactical_action': 'watch_for_trigger'}
            ),
            decision_run_id='legacy-short-term-run',
            agent_name='short-term-decider',
            symbol='005930',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
        )
        service = AgentFeedbackOrchestrator()

        legacy_result = service.revalidate_persisted_decider_snapshots(
            market_date='2026-04-23',
            snapshots=[legacy_snapshot],
        )

        self.assertEqual(legacy_result['valid_snapshot_ids'], [])
        self.assertEqual(legacy_result['invalid_snapshot_ids'], [103])
        self.assertEqual(
            legacy_result['validations'][0]['reason'],
            'actionable_snapshot_missing_durable_generation_fence',
        )

        fenced_snapshot = SimpleNamespace(
            id=104,
            request_payload=json.dumps(
                {
                    'durable_decision_input_generation': {
                        'schema_version': 'durable_decider_input_generation_v1',
                        'durable_reuse_eligible': True,
                        'non_durable_reasons': [],
                    },
                    'decision_input_generation_fence': {
                        'enforcement': 'required'
                    },
                    'source_revalidation_request': {
                        'source_context_scope_symbols': ['005930'],
                        'eligibility_scope_symbols': ['005930'],
                        'fs_div': 'CFS',
                        'disclosures_limit': 5,
                        'knowledge_cutoff_at': '2026-04-23T06:30:00+00:00',
                    },
                }
            ),
            response_payload=json.dumps({'preferred_action': 'wait'}),
            decision_run_id='fenced-string-run',
            agent_name='signal-decider',
            symbol='005930',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
        )

        with patch.object(
            service,
            '_revalidate_decider_dispatch_inputs',
            return_value={},
        ) as revalidate:
            fenced_result = service.revalidate_persisted_decider_snapshots(
                market_date='2026-04-23',
                snapshots=[fenced_snapshot],
            )

        revalidate.assert_called_once()
        self.assertEqual(fenced_result['valid_snapshot_ids'], [104])
        self.assertEqual(
            fenced_result['validations'][0]['reason'],
            'durable_decider_inputs_unchanged',
        )

    def test_invalid_json_string_snapshot_payloads_fail_closed(self):
        cases = (
            (
                105,
                json.dumps({}),
                '{"tactical_action":',
                {'response_payload': 'json_decode_error'},
            ),
            (
                106,
                json.dumps({}),
                json.dumps(['watch_for_trigger']),
                {'response_payload': 'json_shape_not_object'},
            ),
            (
                107,
                '{"decision_input_generation_fence":',
                json.dumps({'tactical_action': 'watch_for_trigger'}),
                {'request_payload': 'json_decode_error'},
            ),
        )
        service = AgentFeedbackOrchestrator()

        for snapshot_id, request_payload, response_payload, payload_errors in cases:
            with self.subTest(snapshot_id=snapshot_id):
                snapshot = SimpleNamespace(
                    id=snapshot_id,
                    request_payload=request_payload,
                    response_payload=response_payload,
                    decision_run_id=f'invalid-payload-{snapshot_id}',
                    agent_name='short-term-decider',
                    symbol='005930',
                    decision_action='wait',
                    preferred_action='wait',
                    next_day_open_action='wait',
                )

                result = service.revalidate_persisted_decider_snapshots(
                    market_date='2026-04-23',
                    snapshots=[snapshot],
                )

                self.assertEqual(result['valid_snapshot_ids'], [])
                self.assertEqual(result['invalid_snapshot_ids'], [snapshot_id])
                self.assertEqual(
                    result['validations'][0]['reason'],
                    'persisted_snapshot_payload_invalid',
                )
                self.assertEqual(
                    result['validations'][0]['payload_errors'],
                    payload_errors,
                )

    def test_oversized_json_string_snapshot_payload_fails_closed(self):
        snapshot = SimpleNamespace(
            id=108,
            request_payload=json.dumps({}),
            response_payload=json.dumps(
                {
                    'tactical_action': 'watch_for_trigger',
                    'padding': 'x' * (1024 * 1024),
                }
            ),
            decision_run_id='oversized-payload',
            agent_name='short-term-decider',
            symbol='005930',
            decision_action='wait',
            preferred_action='wait',
            next_day_open_action='wait',
        )

        result = AgentFeedbackOrchestrator().revalidate_persisted_decider_snapshots(
            market_date='2026-04-23',
            snapshots=[snapshot],
        )

        self.assertEqual(result['valid_snapshot_ids'], [])
        self.assertEqual(result['invalid_snapshot_ids'], [108])
        self.assertEqual(
            result['validations'][0]['reason'],
            'persisted_snapshot_payload_invalid',
        )
        self.assertEqual(
            result['validations'][0]['payload_errors'],
            {'response_payload': 'json_payload_too_large'},
        )

    def test_persisted_snapshot_revalidation_uses_durable_generation(self):
        snapshot = SimpleNamespace(
            id=102,
            request_payload={
                'decision_input_generation': {'schema_version': 'decider_input_generation_v1'},
                'durable_decision_input_generation': {
                    'schema_version': 'durable_decider_input_generation_v1',
                    'durable_reuse_eligible': True,
                    'non_durable_reasons': [],
                },
                'decision_input_generation_fence': {'enforcement': 'required'},
                'source_revalidation_request': {
                    'source_context_scope_symbols': ['005930'],
                    'eligibility_scope_symbols': ['005930'],
                    'fs_div': 'CFS',
                    'disclosures_limit': 5,
                    'knowledge_cutoff_at': '2026-04-23T06:30:00+00:00',
                },
            },
            response_payload={'preferred_action': 'buy'},
            decision_run_id='fenced-run',
            agent_name='signal-decider',
            symbol='005930',
            decision_action='buy',
            next_day_open_action='buy',
        )
        service = AgentFeedbackOrchestrator()

        with patch.object(
            service,
            '_revalidate_decider_dispatch_inputs',
            return_value={},
        ) as revalidate:
            result = service.revalidate_persisted_decider_snapshots(
                market_date='2026-04-23',
                snapshots=[snapshot],
            )

        self.assertEqual(result['valid_snapshot_ids'], [102])
        self.assertTrue(revalidate.call_args.kwargs['durable_source_generation'])
        self.assertFalse(revalidate.call_args.kwargs['revalidate_market_context'])
        self.assertTrue(
            revalidate.call_args.kwargs[
                'revalidate_post_decision_material_context'
            ]
        )
        self.assertEqual(
            revalidate.call_args.kwargs['source_knowledge_cutoff_at'],
            '2026-04-23T06:30:00+00:00',
        )


class ResponseIntegrityGuardTests(unittest.TestCase):
    def test_missing_batch_response_correlation_fails_closed(self):
        self.assertEqual(
            batch_response_contract_failure({'decoded': {'items': []}}),
            'batch_response_correlation_missing',
        )

    def test_missing_normalized_symbol_or_market_date_fails_closed(self):
        base = {
            'response_contract_valid': True,
            'structural_mismatch_flags': [],
            'symbol': '005930',
            'market_date': '2026-04-23',
        }

        self.assertEqual(
            normalized_feedback_contract_failure({**base, 'symbol': None}),
            'response_identity_missing:symbol',
        )
        self.assertEqual(
            normalized_feedback_contract_failure({**base, 'market_date': None}),
            'response_identity_missing:market_date',
        )

    def test_market_level_feedback_requires_market_date_but_not_symbol(self):
        base = {
            'response_contract_valid': True,
            'structural_mismatch_flags': [],
            'response_schema_violation_flags': [],
            'symbol': None,
            'market_date': '2026-04-23',
        }

        self.assertIsNone(
            normalized_feedback_contract_failure(base, require_symbol_identity=False)
        )
        self.assertEqual(
            normalized_feedback_contract_failure(
                {**base, 'market_date': None},
                require_symbol_identity=False,
            ),
            'response_identity_missing:market_date',
        )

    def test_missing_normalized_contract_verdict_fails_closed(self):
        self.assertEqual(
            normalized_feedback_contract_failure(
                {
                    'symbol': '005930',
                    'market_date': '2026-04-23',
                    'structural_mismatch_flags': [],
                    'response_schema_violation_flags': [],
                }
            ),
            'response_contract_verdict_missing',
        )

    def test_schema_contract_violations_are_rejected_by_every_dispatch_guard(self):
        normalized = {
            'response_contract_valid': False,
            'structural_mismatch_flags': [],
            'response_schema_violation_flags': ['required_field_missing:summary'],
            'risk_flags': ['required_field_missing:summary'],
            'symbol': '005930',
            'market_date': '2026-04-23',
        }

        self.assertEqual(
            normalized_feedback_contract_failure(normalized),
            'response_contract_invalid:required_field_missing:summary',
        )
        self.assertEqual(
            extract_response_mismatch_flags(normalized),
            ['required_field_missing:summary'],
        )

    def test_single_dispatch_rejects_invalid_contract_before_persistence_and_cleans_once(self):
        cleanup_calls: list[dict] = []
        store_calls: list[dict] = []

        class Client:
            @staticmethod
            def dispatch(**kwargs):
                return {
                    'normalized_feedback': {
                        'symbol': '__MARKET__',
                        'market_date': '2026-04-23',
                        'response_contract_valid': False,
                        'structural_mismatch_flags': [],
                        'response_schema_violation_flags': ['required_field_missing:summary'],
                    },
                    'session_ref': {'session_key': kwargs['payload']['session_key']},
                }

        class Orchestrator:
            @staticmethod
            def _get_client():
                return Client()

            @staticmethod
            def _store_feedback(**kwargs):
                store_calls.append(kwargs)
                return {'id': 1}

            @staticmethod
            def _cleanup_session_key(**kwargs):
                cleanup_calls.append(kwargs)
                return {'status': 'deleted', 'session_key': kwargs['session_key']}

        items, failures = dispatch_and_store(
            Orchestrator(),
            market_date=date(2026, 4, 23),
            request_run_id='single-invalid-contract',
            specs=[SimpleNamespace(role='eod-summarizer', agent_name='eod-summarizer')],
            payloads={'eod-summarizer': {'trade_date': '2026-04-23'}},
            symbol='__MARKET__',
        )

        self.assertEqual(items, [])
        self.assertEqual(store_calls, [])
        self.assertEqual(len(failures), 1)
        self.assertEqual(
            failures[0]['failure_kind'],
            'response_contract_invalid:required_field_missing:summary',
        )
        self.assertTrue(failures[0]['retryable'])
        self.assertEqual(len(cleanup_calls), 1)
        self.assertEqual(
            cleanup_calls[0]['session_key'],
            'agent:eod-summarizer:feedback/eod-summarizer/single-invalid-contract',
        )
        self.assertEqual(failures[0]['diagnostics']['session_cleanup']['status'], 'deleted')

    def test_single_dispatch_cleanup_failure_does_not_mask_contract_failure(self):
        class Client:
            @staticmethod
            def dispatch(**_kwargs):
                return {
                    'normalized_feedback': {
                        'symbol': '__MARKET__',
                        'market_date': '2026-04-23',
                        'response_contract_valid': False,
                        'structural_mismatch_flags': [],
                        'response_schema_violation_flags': ['required_field_type_invalid:details'],
                    },
                    'session_ref': {'session_key': 'feedback-fresh/eod/cleanup-error'},
                }

        class Orchestrator:
            @staticmethod
            def _get_client():
                return Client()

            @staticmethod
            def _store_feedback(**_kwargs):
                raise AssertionError('invalid feedback must not be persisted')

            @staticmethod
            def _cleanup_session_key(**_kwargs):
                raise RuntimeError('cleanup unavailable')

        items, failures = dispatch_and_store(
            Orchestrator(),
            market_date=date(2026, 4, 23),
            request_run_id='single-invalid-cleanup-error',
            specs=[SimpleNamespace(role='eod-summarizer', agent_name='eod-summarizer')],
            payloads={'eod-summarizer': {'trade_date': '2026-04-23'}},
            symbol='__MARKET__',
        )

        self.assertEqual(items, [])
        self.assertEqual(
            failures[0]['failure_kind'],
            'response_contract_invalid:required_field_type_invalid:details',
        )
        self.assertEqual(failures[0]['diagnostics']['session_cleanup']['status'], 'deferred')
        self.assertEqual(
            failures[0]['diagnostics']['session_cleanup']['reason'],
            'single_dispatch_session_cleanup_failed',
        )

    def test_market_level_single_dispatch_without_symbol_persists_and_cleans_exactly_once(self):
        cleanup_calls: list[dict] = []
        store_calls: list[dict] = []

        class Client:
            @staticmethod
            def dispatch(**_kwargs):
                return {
                    'normalized_feedback': {
                        'role': 'eod-summarizer',
                        'summary': '시장 요약',
                        'confidence': 0.7,
                        'risk_flags': [],
                        'details': {},
                        'improvement_notes': [],
                        'market_date': '2026-04-23',
                        'response_contract_valid': True,
                        'structural_mismatch_flags': [],
                        'response_schema_violation_flags': [],
                    },
                    'session_ref': {'session_key': 'feedback-fresh/eod/success'},
                }

        class Orchestrator:
            @staticmethod
            def _get_client():
                return Client()

            @staticmethod
            def _store_feedback(**kwargs):
                store_calls.append(kwargs)
                return {'id': 1}

            @staticmethod
            def _cleanup_session_key(**kwargs):
                cleanup_calls.append(kwargs)
                return {'status': 'deleted', 'session_key': kwargs['session_key']}

        items, failures = dispatch_and_store(
            Orchestrator(),
            market_date=date(2026, 4, 23),
            request_run_id='single-valid-contract',
            specs=[SimpleNamespace(role='eod-summarizer', agent_name='eod-summarizer')],
            payloads={'eod-summarizer': {'trade_date': '2026-04-23'}},
            symbol='__MARKET__',
        )

        self.assertEqual(failures, [])
        self.assertEqual(len(items), 1)
        self.assertEqual(len(store_calls), 1)
        self.assertFalse(store_calls[0]['cleanup_session'])
        self.assertEqual(len(cleanup_calls), 1)
        self.assertEqual(items[0]['storage']['session_cleanup']['status'], 'deleted')

    def test_decider_single_dispatch_without_trusted_model_identity_is_rejected_with_audit(self):
        store_calls: list[dict] = []

        class Client:
            @staticmethod
            def dispatch(**kwargs):
                return {
                    'normalized_feedback': {
                        **_complete_actionable_signal_feedback(),
                        'response_contract_valid': True,
                        'structural_mismatch_flags': [],
                        'response_schema_violation_flags': [],
                    },
                    'response': {'id': 'raw-response-kept', 'model': 'openclaw/signal-decider'},
                    'output_text': '{"next_day_open_action":"buy"}',
                    'session_ref': {'session_key': kwargs['payload']['session_key']},
                }

        class Orchestrator:
            @staticmethod
            def _get_client():
                return Client()

            @staticmethod
            def _store_feedback(**kwargs):
                store_calls.append(kwargs)
                return {'id': 1}

            @staticmethod
            def _cleanup_session_key(**kwargs):
                return {'status': 'deleted', 'session_key': kwargs['session_key']}

        items, failures = dispatch_and_store(
            Orchestrator(),
            market_date=date(2026, 4, 23),
            request_run_id='single-missing-model-identity',
            specs=[SimpleNamespace(role='signal-decider', agent_name='signal-decider')],
            payloads={'signal-decider': {'market_date': '2026-04-23', 'symbol': '005930'}},
            symbol='005930',
        )

        self.assertEqual(items, [])
        self.assertEqual(store_calls, [])
        self.assertEqual(failures[0]['failure_kind'], 'response_contract_invalid')
        self.assertEqual(
            failures[0]['context']['response_audit']['response']['id'],
            'raw-response-kept',
        )
        self.assertEqual(
            failures[0]['context']['response_audit']['model_identity_verdict']['status'],
            'actual_model_missing',
        )

    def test_symbol_single_dispatch_without_symbol_is_rejected_before_persistence(self):
        cleanup_calls: list[dict] = []

        class Client:
            @staticmethod
            def dispatch(**_kwargs):
                return {
                    'normalized_feedback': {
                        'role': 'technical-analyst',
                        'summary': '종목 기술 요약',
                        'confidence': 0.7,
                        'risk_flags': [],
                        'details': {},
                        'improvement_notes': [],
                        'market_date': '2026-04-23',
                        'response_contract_valid': True,
                        'structural_mismatch_flags': [],
                        'response_schema_violation_flags': [],
                    },
                    'session_ref': {'session_key': 'feedback-fresh/technical/missing-symbol'},
                }

        class Orchestrator:
            @staticmethod
            def _get_client():
                return Client()

            @staticmethod
            def _store_feedback(**_kwargs):
                raise AssertionError('symbol feedback without symbol identity must not be persisted')

            @staticmethod
            def _cleanup_session_key(**kwargs):
                cleanup_calls.append(kwargs)
                return {'status': 'deleted', 'session_key': kwargs['session_key']}

        items, failures = dispatch_and_store(
            Orchestrator(),
            market_date=date(2026, 4, 23),
            request_run_id='single-missing-symbol',
            specs=[SimpleNamespace(role='technical-analyst', agent_name='technical-analyst')],
            payloads={'technical-analyst': {'market_date': '2026-04-23', 'symbol': '005930'}},
            symbol='005930',
        )

        self.assertEqual(items, [])
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]['failure_kind'], 'response_identity_missing:symbol')
        self.assertEqual(len(cleanup_calls), 1)

    def test_single_dispatch_exception_uses_known_fresh_session_and_cleans_once(self):
        cleanup_calls: list[dict] = []
        dispatched_payloads: list[dict] = []

        class Client:
            @staticmethod
            def dispatch(**kwargs):
                dispatched_payloads.append(kwargs['payload'])
                raise RuntimeError('gateway unavailable')

        class Orchestrator:
            @staticmethod
            def _get_client():
                return Client()

            @staticmethod
            def _store_feedback(**_kwargs):
                raise AssertionError('failed dispatch must not be persisted')

            @staticmethod
            def _cleanup_session_key(**kwargs):
                cleanup_calls.append(kwargs)
                return {'status': 'deleted', 'session_key': kwargs['session_key']}

        items, failures = dispatch_and_store(
            Orchestrator(),
            market_date=date(2026, 4, 23),
            request_run_id='single-dispatch-exception',
            specs=[SimpleNamespace(role='eod-summarizer', agent_name='eod-summarizer')],
            payloads={'eod-summarizer': {'trade_date': '2026-04-23'}},
            symbol='__MARKET__',
        )

        self.assertEqual(items, [])
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]['error'], 'gateway unavailable')
        self.assertTrue(failures[0]['retryable'])
        self.assertEqual(len(cleanup_calls), 1)
        self.assertEqual(cleanup_calls[0]['session_key'], dispatched_payloads[0]['session_key'])
        self.assertEqual(failures[0]['diagnostics']['session_cleanup']['status'], 'deleted')


class RepairJsonTailTests(unittest.TestCase):
    def test_response_storage_payload_preserves_prompt_model_and_contract_provenance(self):
        spec = SimpleNamespace(role='signal-decider', agent_name='signal-decider')
        observed_model_identity = build_observed_model_identity(
            role='signal-decider',
            expected_model='openai/gpt-5.6-sol',
            actual_provider='openai',
            actual_model='gpt-5.6-sol',
        )
        model_identity = {
            **observed_model_identity,
            'status': 'verified',
            'cohort': 'signal-decider|openai/gpt-5.6-sol',
        }
        payload = build_response_storage_payload(
            spec=spec,
            batch_result={
                'response': {'model': 'openclaw/signal-decider'},
                'request_provenance': {
                    'prompt_sha256': 'sha256:prompt-hash',
                    'prompt_chars': 12345,
                    'contract_version': 'agent_role_contract_v1',
                    'contract_sha256': 'sha256:contract-hash',
                    'role': 'signal-decider',
                    'model_identity': observed_model_identity,
                    'runtime_contract': {
                        'role': 'signal-decider',
                        'version': 'v1',
                        'sha256': 'sha256:runtime-contract-hash',
                        'status': 'canonical_contract_available',
                    },
                },
            },
            normalized_feedback={
                'role': 'signal-decider',
                'risk_flags': [],
                'model_identity': model_identity,
            },
            collector_mode='test',
        )

        self.assertEqual(
            payload['decision_provenance'],
            {
                'model': 'openclaw/signal-decider',
                'model_identity': model_identity,
                'prompt_sha256': 'sha256:prompt-hash',
                'prompt_chars': 12345,
                'contract_version': 'agent_role_contract_v1',
                'contract_sha256': 'sha256:contract-hash',
                'role': 'signal-decider',
                'runtime_contract': {
                    'role': 'signal-decider',
                    'version': 'v1',
                    'sha256': 'sha256:runtime-contract-hash',
                    'status': 'canonical_contract_available',
                },
            },
        )

    def test_build_signal_decider_payload_promotes_pre_decider_enrichment_context(self):
        pre_decider_context = {
            'schema_version': 'pre_decider_enrichment_v1',
            'status': 'available',
            'quote_orderbook_snapshot': {'status': 'available', 'current_price': 71550},
            'trading_value': {'status': 'available', 'source': 'raw', 'trading_value_krw': 9870000000},
            'flow_short_sector_context': {'status': 'available'},
        }
        decision_input_quality_context = {
            'schema_version': 'decision_input_quality_v1',
            'final_judgment': {
                'decision_band': 'high_conviction',
                'hard_blocks': [],
            },
            'execution_feasibility': {
                'liquidity_tier': 'high',
                'max_safe_notional_krw': 49350000,
            },
        }
        macro_context = {
            'schema_version': 'macro_context_v1',
            'status': 'available',
            'macro_regime_summary': {
                'growth_label': 'growth_expansion',
                'inflation_label': 'inflation_stable',
                'freshness_label': 'macro_context_fresh',
                'llm_generated': False,
            },
            'usage_policy': {
                'contextual_input_only': True,
                'direct_buy_sell_signal': False,
                'llm_generated': False,
            },
        }
        market_regime_context = {
            'schema_version': 'market_regime_context_v1',
            'posture': 'caution',
            'reason_code': 'weak_market_breadth',
            'recommended_exec_action': 'downsize',
            'recommended_sizing_multiplier': 0.5,
        }

        payload = build_signal_decider_payload(
            item={
                'symbol': '005930',
                'market_date': '2026-04-14',
                'pre_decider_enrichment_context': pre_decider_context,
                'market_regime_context': market_regime_context,
                'decision_input_quality_context': decision_input_quality_context,
                'macro_context': macro_context,
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
        self.assertEqual(payload['market_regime_context'], market_regime_context)
        self.assertEqual(payload['decision_input_quality_context'], decision_input_quality_context)
        self.assertEqual(payload['macro_context'], macro_context)
        self.assertNotIn('pre_decider_enrichment_context', payload['symbol_day_context'])
        self.assertNotIn('market_regime_context', payload['symbol_day_context'])
        self.assertNotIn('decision_input_quality_context', payload['symbol_day_context'])
        self.assertNotIn('macro_context', payload['symbol_day_context'])
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'pre_decider_enrichment_context_authoritative'
            ]
        )

        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'market_regime_context_authoritative'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'account_for_market_regime_when_choosing_buy_wait_reject'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'macro_context_contextual_input_only'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'macro_context_not_a_direct_buy_sell_signal'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'macro_context_regime_summary_is_deterministic_no_llm'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'decision_input_quality_context_authoritative'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'performance_criteria_requires_model_guidance_eligibility'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'use_eligible_replay_evidence_to_tighten_buy_thresholds'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'manual_review_criteria_never_authorize_buy'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'deterministic_gate_requires_explicit_eligibility'
            ]
        )
        self.assertFalse(
            payload['decision_support_constraints']['missing_context_policy'][
                'relax_wait_reject_threshold_for_missed_opportunity_clusters'
            ]
        )
        self.assertTrue(
            payload['decision_support_constraints']['missing_context_policy'][
                'do_not_limit_decider_improvement_to_reason_names_or_text_detail'
            ]
        )

    def test_build_signal_decider_payload_promotes_company_market_to_canonical_context(self):
        payload = build_signal_decider_payload(
            item={
                'symbol': '005930',
                'market_date': '2026-04-14',
                'company': {'market': 'KOSPI'},
            },
            market_date=date(2026, 4, 14),
            request_run_id='request-run-market-contract',
            market_context={},
            upstream_summaries={},
            upstream_traceability={},
            signal_decider_agent_name='signal-decider',
            signal_decider_policy={},
        )

        self.assertEqual(payload['symbol_day_context']['market'], 'KOSPI')

    def test_signal_decider_performance_criteria_compacts_recent_calibration_report(self):
        service = AgentFeedbackOrchestrator()
        report = {
            'request': {
                'market_date_from': '2026-06-13',
                'market_date_to': '2026-07-13',
                'policy_version': 'signal_decider_policy_v1',
                'horizon_days': 5,
            },
            'summary': {
                'status': 'ready',
                'mature_computed_count': 12,
                'ready_for_operator_review': True,
                'decision_action_counts': {'buy': 5, 'wait': 7},
                'grade_label_counts': {'bad_buy': 4, 'missed_opportunity_after_wait': 3},
                'positive_outcome_rate': 0.416667,
                'negative_outcome_rate': 0.583333,
                'coverage_gap_count': 0,
            },
            'group_summaries': {
                'reward_risk_buckets': {
                    'items': [
                        {
                            'bucket_type': 'reward_risk_buckets',
                            'bucket_key': 'reward_risk',
                            'bucket_value': 'reward_risk_lt_1_0',
                            'sample_count': 4,
                            'negative_rate': 0.75,
                            'bad_buy_rate': 0.75,
                            'avg_close_return_pct': -2.1,
                            'avg_max_drawdown_return_pct': -4.8,
                        }
                    ]
                }
            },
            'rule_candidates': {
                'items': [
                    {
                        'candidate_id': 'rule-1',
                        'candidate_type': 'buy_veto',
                        'predicate': {'field': 'reward_risk', 'operator': 'eq', 'value': 'reward_risk_lt_1_0'},
                        'reason': '3/4 matching buys graded bad.',
                        'support_sample_count': 3,
                        'evaluated_sample_count': 4,
                        'negative_rate': 0.75,
                        'activation_status': 'needs_replay',
                        'manual_review_only': True,
                        'active_policy': False,
                        'deterministic_gate_eligible': False,
                    }
                ]
            },
            'calibration_hints': {
                'scoring_guidance': [{'kind': 'scoring_guidance', 'title': 'Negative cluster'}],
            },
        }

        with patch.object(orchestrator_module.signal_decider_calibration_service, 'build_report', return_value=report):
            criteria = service._build_signal_decider_performance_criteria(
                policy_context={'current_version': 'signal_decider_policy_v1'},
                market_date=date(2026, 7, 14),
            )

        self.assertEqual(criteria['status'], 'ready')
        self.assertEqual(criteria['sample_count'], 12)
        self.assertEqual(criteria['overall']['negative_outcome_rate'], 0.583333)
        self.assertEqual(criteria['performance_buckets'][0]['bucket_value'], 'reward_risk_lt_1_0')
        self.assertEqual(criteria['performance_buckets'][0]['bad_buy_rate'], 0.75)
        self.assertEqual(criteria['rule_candidates'][0]['candidate_id'], 'rule-1')
        self.assertTrue(criteria['rule_candidates'][0]['manual_review_only'])
        self.assertFalse(criteria['rule_candidates'][0]['active_policy'])
        self.assertFalse(criteria['rule_candidates'][0]['deterministic_gate_eligible'])
        self.assertTrue(criteria['usage_policy']['model_guidance_eligible'])
        self.assertTrue(criteria['usage_policy']['deterministic_gate_eligible'])
        self.assertFalse(criteria['usage_policy']['use_for_buy_wait_reject_thresholds'])
        self.assertTrue(criteria['usage_policy']['tighten_buy_when_matching_bucket_has_bad_buy_or_negative_return'])
        self.assertFalse(criteria['usage_policy']['relax_wait_or_reject_when_matching_bucket_has_missed_opportunity'])
        self.assertFalse(criteria['usage_policy']['allow_buy_authorization'])
        self.assertTrue(criteria['usage_policy']['reward_risk_and_outcome_path_override_name_only_reasoning'])
        self.assertFalse(criteria['usage_policy']['auto_apply_policy_changes'])

    def test_signal_decider_rule_candidate_compaction_normalizes_type_and_activation_safely(self):
        compacted = AgentFeedbackOrchestrator._compact_signal_decider_rule_candidates(
            {
                'items': [
                    {
                        'candidate_id': 'approved-legacy-veto',
                        'candidate_type': 'veto',
                        'predicate': {
                            'schema_version': 'signal_decider_rule_predicate_v1',
                            'all': [
                                {
                                    'field': 'decision.action',
                                    'operator': 'eq',
                                    'value': 'buy',
                                }
                            ],
                        },
                        'activation_status': 'active',
                        'manual_review_only': False,
                        'active_policy': True,
                        'deterministic_gate_eligible': True,
                    },
                    {
                        'candidate_id': 'unreviewed-legacy-downsize',
                        'candidate_type': 'downsize',
                        'predicate': {
                            'schema_version': 'signal_decider_rule_predicate_v1',
                            'all': [
                                {
                                    'field': 'decision.action',
                                    'operator': 'eq',
                                    'value': 'buy',
                                }
                            ],
                        },
                        'activation_status': 'needs_replay',
                        'manual_review_only': True,
                        'active_policy': True,
                        'deterministic_gate_eligible': True,
                    },
                    {
                        'candidate_id': 'active-with-noncanonical-predicate',
                        'candidate_type': 'buy_veto',
                        'predicate': {
                            'schema_version': 'signal_decider_rule_predicate_v1',
                            'operator': 'all',
                            'conditions': [
                                {
                                    'field': 'decision.action',
                                    'operator': 'eq',
                                    'value': 'buy',
                                }
                            ],
                        },
                        'activation_status': 'active',
                        'manual_review_only': False,
                        'active_policy': True,
                        'deterministic_gate_eligible': True,
                    },
                ]
            }
        )

        self.assertEqual(compacted[0]['candidate_type'], 'buy_veto')
        self.assertEqual(compacted[0]['activation_status'], 'active')
        self.assertFalse(compacted[0]['manual_review_only'])
        self.assertTrue(compacted[0]['active_policy'])
        self.assertTrue(compacted[0]['deterministic_gate_eligible'])
        self.assertEqual(compacted[1]['candidate_type'], 'tighten_buy_gate')
        self.assertEqual(compacted[1]['activation_status'], 'needs_replay')
        self.assertTrue(compacted[1]['manual_review_only'])
        self.assertFalse(compacted[1]['active_policy'])
        self.assertFalse(compacted[1]['deterministic_gate_eligible'])
        self.assertFalse(compacted[2]['active_policy'])
        self.assertFalse(compacted[2]['deterministic_gate_eligible'])

    def test_approved_calibration_report_candidate_survives_compaction_and_blocks_matching_factor(self):
        predicate = (
            orchestrator_module.signal_decider_calibration_service._rule_candidate_predicate(
                tokens=[
                    {
                        'key': 'factor.momentum.bucket',
                        'value': 'available:high',
                    }
                ],
                action_condition={
                    'field': 'decision.action',
                    'operator': 'eq',
                    'value': 'buy',
                },
            )
        )
        report = {
            'request': {
                'market_date_from': '2026-06-13',
                'market_date_to': '2026-07-13',
                'policy_version': 'signal_decider_policy_v1',
                'horizon_days': 5,
            },
            'summary': {
                'status': 'ready',
                'mature_computed_count': 12,
                'ready_for_operator_review': True,
                'decision_action_counts': {'buy': 12},
                'grade_label_counts': {'bad_buy': 9},
                'positive_outcome_rate': 0.25,
                'negative_outcome_rate': 0.75,
                'coverage_gap_count': 0,
            },
            'group_summaries': {},
            'rule_candidates': {
                'items': [
                    {
                        'candidate_id': 'approved-momentum-veto',
                        'candidate_type': 'buy_veto',
                        'predicate': predicate,
                        'reason': 'Approved after replay and operator review.',
                        'support_sample_count': 9,
                        'evaluated_sample_count': 12,
                        'negative_rate': 0.75,
                        'activation_status': 'active',
                        'manual_review_only': False,
                        'active_policy': True,
                        'deterministic_gate_eligible': True,
                    }
                ]
            },
            'calibration_hints': {},
        }

        with patch.object(
            orchestrator_module.signal_decider_calibration_service,
            'build_report',
            return_value=report,
        ):
            criteria = AgentFeedbackOrchestrator()._build_signal_decider_performance_criteria(
                policy_context={'current_version': 'signal_decider_policy_v1'},
                market_date=date(2026, 7, 14),
            )

        result = evaluate_next_open_performance_gate(
            request_payload={
                'scores': {
                    'momentum': {'status': 'available', 'score': 0.88},
                },
                'factor_summary': {
                    'factor_scores': {
                        'momentum': {'status': 'available', 'score': 0.88},
                    },
                },
                'signal_decider_policy': {
                    'performance_criteria': criteria,
                },
            },
            decision_action='buy',
            confidence_score_10=8,
            expected_return_pct=3,
            reference_price=50_000,
            target_price=52_000,
            stop_loss_price=49_000,
            expected_slippage_bps=0,
            expected_slippage_basis='quote_spread_bps',
            base_min_confidence_score_10=6,
            base_min_expected_return_pct=0,
            base_min_reward_risk_ratio='1.30',
        )

        self.assertEqual(predicate['schema_version'], 'signal_decider_rule_predicate_v1')
        self.assertIn('all', predicate)
        self.assertEqual(criteria['rule_candidates'][0]['candidate_type'], 'buy_veto')
        self.assertTrue(criteria['rule_candidates'][0]['deterministic_gate_eligible'])
        self.assertEqual(
            [item['candidate_id'] for item in result['matched_rule_candidates']],
            ['approved-momentum-veto'],
        )
        self.assertIn(
            'performance_rule_candidate_blocked',
            {reason['code'] for reason in result['blocking_reasons']},
        )

    def test_signal_decider_performance_criteria_are_ineligible_when_replay_sample_is_insufficient(self):
        service = AgentFeedbackOrchestrator()
        report = {
            'request': {'policy_version': 'signal_decider_policy_v1', 'horizon_days': 5},
            'summary': {
                'status': 'insufficient_sample',
                'mature_computed_count': 2,
                'positive_outcome_rate': 0.5,
                'negative_outcome_rate': 0.5,
            },
            'group_summaries': {},
            'rule_candidates': {'items': []},
            'calibration_hints': {},
        }

        with patch.object(orchestrator_module.signal_decider_calibration_service, 'build_report', return_value=report):
            criteria = service._build_signal_decider_performance_criteria(
                policy_context={'current_version': 'signal_decider_policy_v1'},
                market_date=date(2026, 7, 14),
            )

        self.assertFalse(criteria['usage_policy']['model_guidance_eligible'])
        self.assertFalse(criteria['usage_policy']['deterministic_gate_eligible'])
        self.assertFalse(criteria['usage_policy']['allow_buy_authorization'])

    def test_signal_decider_performance_criteria_are_ineligible_without_operator_review_readiness(self):
        service = AgentFeedbackOrchestrator()
        report = {
            'request': {'policy_version': 'signal_decider_policy_v1', 'horizon_days': 5},
            'summary': {
                'status': 'ready',
                'mature_computed_count': 12,
                'positive_outcome_rate': 0.5,
                'negative_outcome_rate': 0.5,
            },
            'group_summaries': {},
            'rule_candidates': {'items': []},
            'calibration_hints': {},
        }

        with patch.object(orchestrator_module.signal_decider_calibration_service, 'build_report', return_value=report):
            criteria = service._build_signal_decider_performance_criteria(
                policy_context={'current_version': 'signal_decider_policy_v1'},
                market_date=date(2026, 7, 14),
            )

        self.assertFalse(criteria['usage_policy']['model_guidance_eligible'])
        self.assertFalse(criteria['usage_policy']['deterministic_gate_eligible'])

    def test_build_signal_decider_payload_includes_signal_envelopes_without_replacing_raw_blocks(self):
        upstream_summaries = {
            'technical-summarizer': {
                'preferred_action': 'buy',
                'confidence_score_10': 8,
                'summary': 'Constructive breakout.',
                'supporting_evidence': ['Breakout held.'],
            }
        }
        upstream_traceability = {
            'technical-summarizer': {
                'feedback_id': 7,
                'run_id': 'tech-run-7',
                'source': 'persisted_feedback',
            }
        }

        payload = build_signal_decider_payload(
            item={
                'symbol': '005930',
                'market_date': '2026-04-14',
                'scores': {},
                'factor_summary': {
                    'requested_market_date': '2026-04-14',
                    'factor_scores': {
                        'momentum': {
                            'status': 'available',
                            'score': 0.91,
                            'components_used': 3,
                            'components_possible': 3,
                        }
                    },
                },
                'signal_envelopes': [
                    {
                        'schema_version': 'signal_envelope_v1',
                        'source_name': 'news:external',
                        'source_type': 'news',
                        'symbol': '005930',
                        'market_date': '2026-04-14',
                        'horizon': '5d',
                        'value': -0.4,
                        'confidence': 0.6,
                        'evidence_count': 1,
                        'risk_flags': ['material_news'],
                        'evidence_refs': [],
                        'summary': 'Material risk headline.',
                        'raw_ref': {'type': 'symbol_day_context', 'path': 'recent_external_news'},
                    }
                ],
            },
            market_date=date(2026, 4, 14),
            request_run_id='request-run-test',
            market_context={},
            upstream_summaries=upstream_summaries,
            upstream_traceability=upstream_traceability,
            signal_decider_agent_name='signal-decider',
        )

        by_source = {item['source_name']: item for item in payload['signal_envelopes']}
        self.assertIn('news:external', by_source)
        self.assertIn('technical-summarizer', by_source)
        self.assertIn('factor:momentum', by_source)
        self.assertNotIn('signal_envelopes', payload['symbol_day_context'])
        self.assertEqual(
            payload['upstream_summaries']['technical-summarizer']['supporting_evidence'],
            ['Breakout held.'],
        )
        self.assertEqual(payload['factor_summary']['factor_scores']['momentum']['score'], 0.91)
        self.assertEqual(by_source['technical-summarizer']['raw_ref']['feedback_id'], 7)
        self.assertEqual(
            payload['decision_support_constraints']['missing_context_policy']['signal_envelope_schema_version'],
            'signal_envelope_v1',
        )

    def test_build_signal_decider_payload_preserves_degraded_macro_context_statuses(self):
        for status in ('partial', 'unavailable', 'stale'):
            with self.subTest(status=status):
                macro_context = {
                    'schema_version': 'macro_context_v1',
                    'status': status,
                    'freshness': {
                        'status': status,
                        'knowledge_cutoff_applied': True,
                        'knowledge_cutoff_at': '2025-01-02T00:00:00+00:00',
                    },
                    'series': [
                        {
                            'source': 'world_bank',
                            'series_id': 'NY.GDP.MKTP.KD.ZG',
                            'status': 'missing' if status in {'partial', 'unavailable'} else 'stale',
                            'latest_value': None,
                        }
                    ],
                    'macro_regime_summary': {
                        'freshness_label': f'macro_context_{status}',
                        'llm_generated': False,
                    },
                    'usage_policy': {
                        'contextual_input_only': True,
                        'direct_buy_sell_signal': False,
                        'llm_generated': False,
                    },
                }

                payload = build_signal_decider_payload(
                    item={
                        'symbol': '005930',
                        'market_date': '2025-01-01',
                        'macro_context': macro_context,
                        'scores': {},
                        'factor_summary': {},
                        'liquidity_context': {},
                        'pre_decider_enrichment_context': {},
                        'decider_data_quality_context': {},
                    },
                    market_date=date(2025, 1, 1),
                    request_run_id='request-run-test',
                    market_context={},
                    upstream_summaries={},
                    upstream_traceability={},
                    signal_decider_agent_name='short-term-decider',
                )

                self.assertEqual(payload['macro_context']['status'], status)
                self.assertEqual(payload['macro_context']['freshness']['status'], status)
                self.assertTrue(payload['macro_context']['freshness']['knowledge_cutoff_applied'])
                self.assertEqual(payload['macro_context']['series'][0]['status'], macro_context['series'][0]['status'])

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

    def test_batch_correlation_rejects_coerced_noncanonical_response_shapes(self):
        malformed_responses = (
            {
                'batch': {
                    'items': [
                        {
                            'symbol': '005930',
                            'feedback': {'role': 'eod-summarizer'},
                        }
                    ]
                }
            },
            {
                'items': [
                    {
                        'symbol': '005930',
                        'role': 'eod-summarizer',
                        'summary': '중첩 feedback 객체가 누락됐습니다.',
                    }
                ]
            },
        )

        for malformed in malformed_responses:
            with self.subTest(shape=next(iter(malformed))):
                coerced = coerce_decoded_batch_response(malformed)
                _, metadata = correlate_decoded_batch_response(
                    decoded=coerced,
                    expected_symbols=['005930'],
                    session_reuse_mode='fresh_per_batch',
                )

                self.assertEqual(metadata['attribution_status'], 'mismatch')
                self.assertEqual(metadata['failure_class'], 'batch_response_structure_mismatch')
                self.assertTrue(metadata['structure_mismatch_flags'])

    def test_batch_correlation_rejects_top_level_feedback_fallback_shape(self):
        _, metadata = correlate_decoded_batch_response(
            decoded={
                '_coerced_from_top_level_object': True,
                'items': [
                    {
                        'symbol': '005930',
                        'feedback': {'role': 'eod-summarizer'},
                    }
                ],
            },
            expected_symbols=['005930'],
            session_reuse_mode='fresh_per_batch',
        )

        self.assertEqual(metadata['attribution_status'], 'mismatch')
        self.assertEqual(metadata['failure_class'], 'batch_response_structure_mismatch')
        self.assertEqual(metadata['structure_mismatch_flags'], ['top_level_feedback_object'])

    def test_batch_correlation_requires_exact_raw_krx_and_us_item_symbols(self):
        cases = (
            ('005930', '005930 '),
            ('AAPL', 'aapl'),
        )

        for expected_symbol, returned_symbol in cases:
            with self.subTest(expected_symbol=expected_symbol, returned_symbol=returned_symbol):
                _, metadata = correlate_decoded_batch_response(
                    decoded={
                        'items': [
                            {
                                'symbol': returned_symbol,
                                'feedback': {'symbol': expected_symbol},
                            }
                        ]
                    },
                    expected_symbols=[expected_symbol],
                    session_reuse_mode='fresh_per_batch',
                )

                self.assertEqual(metadata['attribution_status'], 'mismatch')
                self.assertEqual(metadata['failure_class'], 'batch_response_attribution_mismatch')
                self.assertEqual(
                    metadata['item_symbol_mismatches'],
                    [
                        {
                            'item_index': 0,
                            'expected_symbol': expected_symbol,
                            'response_symbol': returned_symbol,
                        }
                    ],
                )

    def test_decode_candidate_json_rejects_actionable_payload_with_tail_corruption(self):
        valid = json.dumps(
            {
                'items': [
                    {
                        'symbol': '005930',
                        'feedback': _complete_actionable_signal_feedback(),
                    }
                ]
            }
        )
        malformed_candidates = {
            'extra_closing_brace': f'{valid}}}',
            'missing_closing_brace': valid[:-1],
        }

        client = OpenClawAgentClient()
        for corruption, malformed in malformed_candidates.items():
            with self.subTest(corruption=corruption):
                decoded, failure = client._decode_candidate_json(malformed)
                self.assertIsNone(decoded)
                self.assertEqual(failure, 'json_decode_error')

    def test_decode_candidate_json_rejects_top_level_array(self):
        malformed = json.dumps(
            [
                {
                    'symbol': '005930',
                    'feedback': _complete_actionable_signal_feedback(),
                }
            ]
        )

        decoded, failure = OpenClawAgentClient._decode_candidate_json(malformed)

        self.assertIsNone(decoded)
        self.assertEqual(failure, 'json_shape_not_object')

    def test_decode_candidate_json_rejects_duplicate_keys_at_any_depth(self):
        feedback_json = json.dumps(_complete_actionable_signal_feedback())
        duplicate_action_feedback = '{"preferred_action":"wait",' + feedback_json[1:]
        canonical_batch = json.dumps(
            {
                'items': [
                    {
                        'symbol': '005930',
                        'feedback': _complete_actionable_signal_feedback(),
                    }
                ]
            }
        )
        duplicate_traceability_symbol = canonical_batch.replace(
            '"traceability": {"symbol": "005930"}',
            '"traceability": {"symbol": "000660", "symbol": "005930"}',
        )
        self.assertNotEqual(duplicate_traceability_symbol, canonical_batch)
        candidates = {
            'duplicate_action': (
                '{"items":[{"symbol":"005930","feedback":'
                f'{duplicate_action_feedback}'
                '}]}'
            ),
            'duplicate_nested_traceability_symbol': duplicate_traceability_symbol,
        }

        for duplicate_kind, candidate in candidates.items():
            with self.subTest(duplicate_kind=duplicate_kind):
                decoded, failure = OpenClawAgentClient._decode_candidate_json(candidate)
                self.assertIsNone(decoded)
                self.assertEqual(failure, 'json_duplicate_key')

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
                'decision_input_quality_context': {
                    'schema_version': 'decision_input_quality_v1',
                    'execution_feasibility': {'liquidity_tier': 'medium'},
                    'final_judgment': {'decision_band': 'small_probe', 'hard_blocks': []},
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
        self.assertEqual(
            trimmed['decision_input_quality_context']['final_judgment']['decision_band'],
            'small_probe',
        )

    def test_trim_batch_item_for_prompt_preserves_us_screening_context(self):
        trimmed = trim_batch_item_for_prompt(
            {
                'symbol': 'NVDA',
                'market_date': '2026-08-19',
                'symbol_day_context': {
                    'symbol': 'NVDA',
                    'market': 'US',
                    'exchange': 'NASDAQ',
                    'currency': 'USD',
                    'calendar_timezone': 'America/New_York',
                    'price_screen': {
                        'latest_price': '217.56',
                        'momentum_6m': '0.1587',
                        'median_dollar_volume_20d': '24686407629',
                    },
                    'fundamental_screen': {
                        'ttm_revenue': '253491000000',
                        'ttm_operating_cash_flow': '125648000000',
                    },
                    'screening_decision': {'selected': True},
                    'context_provenance': {
                        'prices': 'kis_overseas_dailyprice_adjusted',
                        'fundamentals': 'sec_edgar_companyfacts+submissions',
                        'screening_rank': 1,
                    },
                },
            }
        )

        context = trimmed['symbol_day_context']
        self.assertEqual(context['market'], 'US')
        self.assertEqual(context['exchange'], 'NASDAQ')
        self.assertEqual(context['currency'], 'USD')
        self.assertEqual(context['price_screen']['latest_price'], '217.56')
        self.assertEqual(context['fundamental_screen']['ttm_revenue'], '253491000000')
        self.assertTrue(context['screening_decision']['selected'])
        self.assertEqual(context['context_provenance']['screening_rank'], 1)


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


class MutatingDeciderInputReadService:
    def __init__(self, *, stable_calls: int = 1):
        self.calls = 0
        self.stable_calls = stable_calls
        self.delegate = DeciderReadyAgentContextReadService()

    def get_symbol_day_context(self, **kwargs):
        self.calls += 1
        payload = self.delegate.get_symbol_day_context(**kwargs)
        generation = (
            'generation-1'
            if self.calls <= self.stable_calls
            else 'generation-2'
        )
        for item in payload['items']:
            item['technical_snapshot'] = {
                **(item.get('technical_snapshot') or {}),
                'source_generation': generation,
            }
        return payload


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
        return _attach_response_correlation({
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
            'model_identity': build_observed_model_identity(
                role=spec.role,
                expected_model='openai/gpt-5.6-sol',
                actual_provider='openai',
                actual_model='gpt-5.6-sol',
            ),
            'request_provenance': {
                'runtime_contract': {'version': 'v1', 'verified': True, 'status': 'verified'}
            },
        }, expected_symbols=[item['symbol'] for item in batch_items])


class InnerIdentityMismatchBatchClient(RecordingBatchClient):
    def dispatch_batch(self, **kwargs):
        result = super().dispatch_batch(**kwargs)
        feedback = result['decoded']['items'][0]['feedback']
        feedback['role'] = kwargs['spec'].role
        feedback['symbol'] = '000660'
        feedback['market_date'] = '2026-04-13'
        return result


class DuplicateAttributionBatchClient(RecordingBatchClient):
    def dispatch_batch(self, **kwargs):
        result = super().dispatch_batch(**kwargs)
        first_item = result['decoded']['items'][0]
        result['decoded']['items'].append(
            {
                'symbol': first_item['symbol'],
                'feedback': dict(first_item['feedback']),
            }
        )
        _, response_correlation = correlate_decoded_batch_response(
            decoded=result['decoded'],
            expected_symbols=[item['symbol'] for item in kwargs['batch_items']],
            session_reuse_mode='fresh_per_batch',
        )
        result['response_correlation'] = response_correlation
        return result


class MissingFeedbackBatchClient(RecordingBatchClient):
    def dispatch_batch(self, **kwargs):
        result = super().dispatch_batch(**kwargs)
        result['decoded']['items'][0]['feedback'] = None
        return result


class MissingCorrelationBatchClient(RecordingBatchClient):
    def dispatch_batch(self, **kwargs):
        result = super().dispatch_batch(**kwargs)
        result.pop('response_correlation', None)
        return result


class RaisingBatchClient(RecordingBatchClient):
    def dispatch_batch(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
        raise RuntimeError('dispatch failed after session allocation')


class PreflightFailingClient:
    def __init__(self):
        self.preflight_calls = 0
        self.calls = []

    def preflight_dispatch_auth(self):
        self.preflight_calls += 1
        return {
            'ok': False,
            'status': 'failed',
            'failure_kind': 'openclaw_auth_missing',
            'failure_status': 'auth/missing',
            'failure_category': 'auth',
            'retryable': False,
            'error': 'OPENCLAW_GATEWAY_TOKEN is not configured',
            'token_configured': False,
        }

    def dispatch_batch(self, **kwargs):
        self.calls.append(kwargs)
        raise AssertionError('dispatch_batch should not run after auth preflight failure')


class HangingBatchClient:
    batch_http_timeout_seconds = 1.0

    def __init__(self):
        self.calls = []
        self._lock = threading.Lock()

    def dispatch_batch(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
        time.sleep(2)
        return {'decoded': {'items': []}}


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
                    'traceability': {'symbol': item['symbol']},
                    'tactical_action': 'watch_for_trigger',
                    'preferred_action': 'watch_for_trigger',
                    'wait_reason': {
                        'code': 'unsuitable_execution_timing',
                        'summary': '돌파와 거래량 확인 전입니다.',
                    },
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
                    'decision_delta': {},
                    'summary': '단기 탄력은 있으나 돌파 확인이 필요합니다.',
                    'confidence': 0.6,
                    'risk_flags': [],
                    'details': {},
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
                    'traceability': {'symbol': item['symbol']},
                    'preferred_action': 'buy',
                    'next_day_open_action': 'buy',
                    'wait_reason': None,
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
                    'decision_delta': {},
                    'summary': 'Specialists align on a tactical next-day long setup.',
                    'confidence': 0.7,
                    'risk_flags': [],
                    'details': {},
                    'improvement_notes': ['Add premarket gap context.'],
                }
                for item in batch_items
            }
        return _attach_response_correlation({
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
            'model_identity': build_observed_model_identity(
                role=spec.role,
                expected_model='openai/gpt-5.6-sol',
                actual_provider='openai',
                actual_model='gpt-5.6-sol',
            ),
            'request_provenance': {
                'runtime_contract': {'version': 'v1', 'verified': True, 'status': 'verified'}
            },
        }, expected_symbols=[item['symbol'] for item in batch_items])


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


class InvalidJSONHTTPResponse:
    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text

    def json(self):
        raise json.JSONDecodeError('Expecting value', self.text, 0)


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


class RaisingHTTPClient:
    def __init__(self, *, exc: Exception, calls: list[dict], **kwargs):
        self.exc = exc
        self.calls = calls
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url, json):
        self.calls.append({'url': url, 'json': json, 'kwargs': self.kwargs})
        raise self.exc


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


class RaisingSessionCleanupService(FakeSessionCleanupService):
    def delete_now(self, *, target):
        self.targets.append(target)
        raise RuntimeError('cleanup audit store unavailable')


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
    return SimpleNamespace(
        should_dispatch=should_dispatch,
        latest_feedback_id=41,
        latest_feedback_received_at='2026-04-14T05:00:00+00:00',
        latest_feedback={
            'id': 41,
            'run_id': 'reusable-feedback-source',
            'received_at': '2026-04-14T05:00:00+00:00',
        },
    )


def _patched_eligibility_split(*allowed_symbols: str):
    allowed = set(allowed_symbols)

    def split(*, market_date, requested_symbols):
        eligible = [symbol for symbol in requested_symbols if symbol in allowed]
        skipped = [symbol for symbol in requested_symbols if symbol not in allowed]
        records_by_symbol = {
            symbol: {
                'symbol': symbol,
                'market_date': market_date.isoformat(),
                'rule_version': 'strategy_aware_prefilter_scoring_v1',
                'strategy_scope': 'short_term_momentum_breakout',
                'candidate_path': 'short_term_momentum_breakout',
                'eligible': symbol in allowed,
                'hard_blocked': symbol not in allowed,
                'status': 'dispatched' if symbol in allowed else 'hard_blocked',
                'hard_block_reasons': [] if symbol in allowed else [{'code': 'test_hard_block'}],
                'soft_penalties': [],
                'filter_signals': {},
                'candidate_rank': list(requested_symbols).index(symbol) + 1,
                'candidate_cap': {
                    'pre_cap_eligible_count': len(requested_symbols),
                    'post_cap_dispatched_count': len(eligible),
                    'applied': False,
                },
            }
            for symbol in requested_symbols
        }
        return eligible, skipped, {
            'rule_version': 'strategy_aware_prefilter_scoring_v1',
            'strategy_scope': 'short_term_momentum_breakout',
            'evaluated_symbol_count': len(requested_symbols),
            'dispatched_symbol_count': len(eligible),
            'hard_blocked_symbol_count': len(skipped),
            'soft_penalized_dispatched_symbol_count': 0,
            'hard_blocked_symbols': skipped,
            'soft_penalized_symbols': [],
            'hard_block_reasons_by_symbol': {
                symbol: [{'code': 'test_hard_block'}]
                for symbol in skipped
            },
            'soft_penalties_by_symbol': {},
            'records_by_symbol': records_by_symbol,
        }

    return split


@contextmanager
def _patch_feedback_eligibility(*symbols: str):
    split = _patched_eligibility_split(*symbols)

    def load_records(*, market_date, requested_symbols=None, **_):
        scope = list(requested_symbols) if requested_symbols is not None else list(symbols)
        return list(
            split(
                market_date=market_date,
                requested_symbols=scope,
            )[2]['records_by_symbol'].values()
        )

    with patch.object(
        orchestrator_module,
        '_split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics',
        side_effect=split,
    ):
        with patch.object(
            orchestrator_module,
            'load_agent_feedback_eligibility_records',
            side_effect=load_records,
        ):
            with patch.object(
                orchestrator_module,
                '_split_requested_symbols_by_agent_feedback_eligibility',
                side_effect=lambda market_date, requested_symbols: (
                    [symbol for symbol in requested_symbols if symbol in set(symbols)],
                    [symbol for symbol in requested_symbols if symbol not in set(symbols)],
                ),
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
            openclaw_state_dir='/nonexistent/openclaw-test-state',
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()

        self.assertEqual(
            client.timeout_seconds,
            orchestrator_module.DEFAULT_OPENCLAW_AGENT_TIMEOUT_SECONDS,
        )
        self.assertEqual(client.http_timeout_seconds, client.timeout_seconds)

    def test_single_dispatch_persists_pre_and_post_runtime_contract_drift_proof(self):
        http_calls: list[dict] = []
        response_payload = {
            'id': 'response-1',
            'status': 'completed',
            'output': [
                {
                    'type': 'message',
                    'content': [
                        {
                            'type': 'output_text',
                            'text': json.dumps(_complete_actionable_signal_feedback()),
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
            openclaw_state_dir='/nonexistent/openclaw-test-state',
        )
        with patch.object(orchestrator_module, 'settings', fake_settings), patch(
            'app.agent_context.orchestrator.build_runtime_contract_provenance',
            side_effect=[_runtime_contract_proof('before'), _runtime_contract_proof('after')],
        ), patch(
            'app.agent_context.orchestrator.httpx.Client',
            side_effect=lambda **kwargs: FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            ),
        ):
            client = OpenClawAgentClient()
            with patch.object(
                client,
                '_observe_post_dispatch_model_identity',
                return_value=build_observed_model_identity(
                    role='signal-decider',
                    expected_model='openai/gpt-5.6-sol',
                    actual_provider='openai',
                    actual_model='gpt-5.6-sol',
                ),
            ):
                result = client.dispatch(
                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                    payload={
                        'market_date': '2026-04-23',
                        'symbol': '005930',
                        'session_key': 'feedback/signal-decider/drift-test',
                    },
                )

        proof = result['request_provenance']['runtime_contract']
        self.assertFalse(proof['verified'])
        self.assertEqual(proof['status'], 'live_contract_changed_during_dispatch')
        self.assertEqual(proof['pre_dispatch']['live_sha256'], 'sha256:before')
        self.assertEqual(proof['post_dispatch']['live_sha256'], 'sha256:after')
        self.assertFalse(result['normalized_feedback']['response_contract_valid'])
        self.assertEqual(result['normalized_feedback']['next_day_open_action'], 'reject')

    def test_batch_dispatch_persists_pre_and_post_runtime_contract_drift_proof(self):
        http_calls: list[dict] = []
        response_payload = {
            'id': 'response-1',
            'status': 'completed',
            'output': [
                {
                    'type': 'message',
                    'content': [
                        {
                            'type': 'output_text',
                            'text': json.dumps(
                                {'items': [{'symbol': '005930', 'feedback': _complete_actionable_signal_feedback()}]}
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
            openclaw_state_dir='/nonexistent/openclaw-test-state',
        )
        with patch.object(orchestrator_module, 'settings', fake_settings), patch(
            'app.agent_context.orchestrator.build_runtime_contract_provenance',
            side_effect=[_runtime_contract_proof('before'), _runtime_contract_proof('after')],
        ), patch(
            'app.agent_context.orchestrator.httpx.Client',
            side_effect=lambda **kwargs: FakeHTTPClient(
                response=FakeHTTPResponse(200, response_payload),
                calls=http_calls,
                **kwargs,
            ),
        ):
            client = OpenClawAgentClient()
            with patch.object(
                client,
                '_observe_post_dispatch_model_identity',
                return_value=build_observed_model_identity(
                    role='signal-decider',
                    expected_model='openai/gpt-5.6-sol',
                    actual_provider='openai',
                    actual_model='gpt-5.6-sol',
                ),
            ):
                result = client.dispatch_batch(
                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                    market_date=date(2026, 4, 23),
                    batch_items=[
                        {
                            'market_date': '2026-04-23',
                            'symbol': '005930',
                            'session_key': 'feedback/signal-decider/drift-test',
                        }
                    ],
                )

        proof = result['request_provenance']['runtime_contract']
        self.assertFalse(proof['verified'])
        self.assertEqual(proof['status'], 'live_contract_changed_during_dispatch')
        self.assertEqual(proof['pre_dispatch']['live_sha256'], 'sha256:before')
        self.assertEqual(proof['post_dispatch']['live_sha256'], 'sha256:after')

    def test_retry_stops_when_runtime_contract_drifts_after_failed_response(self):
        http_calls: list[dict] = []
        responses = [
            FakeHTTPResponse(500, {'status': 'failed'}),
            FakeHTTPResponse(200, {'status': 'completed', 'output': []}),
        ]
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=1,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
            openclaw_state_dir='/nonexistent/openclaw-test-state',
        )
        with patch.object(orchestrator_module, 'settings', fake_settings), patch(
            'app.agent_context.orchestrator.build_runtime_contract_provenance',
            side_effect=[_runtime_contract_proof('before'), _runtime_contract_proof('after')],
        ), patch(
            'app.agent_context.orchestrator.httpx.Client',
            side_effect=lambda **kwargs: FakeHTTPClient(
                response=responses.pop(0),
                calls=http_calls,
                **kwargs,
            ),
        ):
            client = OpenClawAgentClient()
            with self.assertRaises(orchestrator_module.AgentDispatchTerminalError):
                client.dispatch(
                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                    payload={
                        'market_date': '2026-04-23',
                        'symbol': '005930',
                        'session_key': 'feedback/signal-decider/retry-drift-test',
                    },
                )

        self.assertEqual(len(http_calls), 1)

    def test_single_dispatch_retry_anchors_model_identity_to_initial_attempt(self):
        initial_started_at = datetime(2026, 9, 11, 10, 0, tzinfo=timezone.utc)
        retry_started_at = initial_started_at + timedelta(seconds=10)
        observed_at = retry_started_at + timedelta(seconds=1)
        completed_at = observed_at + timedelta(seconds=1)
        clock_values = iter(
            [initial_started_at, retry_started_at, observed_at, completed_at]
        )

        class SequenceDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = next(clock_values)
                return value if tz is not None else value.replace(tzinfo=None)

        http_calls: list[dict] = []
        session_key = 'feedback/signal-decider/lost-response-retry'
        response_payload = {
            'id': 'response-after-retry',
            'session_id': 'session-completed-before-retry',
            'status': 'completed',
            'output': [
                {
                    'type': 'message',
                    'content': [
                        {
                            'type': 'output_text',
                            'text': json.dumps(_complete_actionable_signal_feedback()),
                        }
                    ],
                }
            ],
        }
        responses: list[Exception | FakeHTTPResponse] = [
            httpx.ReadTimeout('response lost after OpenClaw completion'),
            FakeHTTPResponse(200, response_payload),
        ]
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=300,
            openclaw_agent_max_retries=1,
            openclaw_agent_retry_backoff_seconds=1,
            openclaw_agent_max_prompt_chars=0,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            state_dir = Path(tmp_dir).resolve()
            fake_settings.openclaw_state_dir = str(state_dir)
            registry_path = (
                state_dir
                / 'agents'
                / 'signal-decider'
                / 'sessions'
                / 'sessions.json'
            )

            def complete_session_before_retry(_delay):
                started_ms = int(initial_started_at.timestamp() * 1000)
                registry_path.parent.mkdir(parents=True)
                registry_path.write_text(
                    json.dumps(
                        {
                            session_key: {
                                'sessionId': 'session-completed-before-retry',
                                'status': 'completed',
                                'startedAt': started_ms,
                                'endedAt': started_ms + 1_000,
                                'updatedAt': started_ms + 1_000,
                                'modelProvider': 'openai',
                                'model': 'gpt-5.6-sol',
                            }
                        }
                    ),
                    encoding='utf-8',
                )

            def fake_client_factory(*args, **kwargs):
                outcome = responses.pop(0)
                if isinstance(outcome, Exception):
                    return RaisingHTTPClient(
                        exc=outcome,
                        calls=http_calls,
                        **kwargs,
                    )
                return FakeHTTPClient(
                    response=outcome,
                    calls=http_calls,
                    **kwargs,
                )

            with patch.object(orchestrator_module, 'settings', fake_settings), patch.object(
                orchestrator_module,
                'datetime',
                SequenceDateTime,
            ), patch.object(
                orchestrator_module.time,
                'sleep',
                side_effect=complete_session_before_retry,
            ), patch(
                'app.agent_context.orchestrator.build_runtime_contract_provenance',
                return_value=_runtime_contract_proof('stable'),
            ), patch(
                'app.agent_context.orchestrator.httpx.Client',
                side_effect=fake_client_factory,
            ):
                result = OpenClawAgentClient().dispatch(
                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                    payload={
                        'market_date': '2026-04-23',
                        'symbol': '005930',
                        'session_key': session_key,
                    },
                )

        self.assertEqual(result['attempt'], 2)
        self.assertEqual(len(http_calls), 2)
        self.assertEqual(
            [call['kwargs']['headers']['x-openclaw-session-key'] for call in http_calls],
            [session_key, session_key],
        )
        self.assertEqual(
            result['model_identity']['actual_model_ref'],
            'openai/gpt-5.6-sol',
            result['model_identity'],
        )
        self.assertTrue(result['model_identity']['verified'])
        self.assertEqual(result['normalized_feedback']['next_day_open_action'], 'buy')

    def test_batch_dispatch_retry_anchors_model_identity_to_initial_attempt(self):
        initial_started_at = datetime(2026, 9, 11, 10, 0, tzinfo=timezone.utc)
        retry_started_at = initial_started_at + timedelta(seconds=10)
        observed_at = retry_started_at + timedelta(seconds=1)
        completed_at = observed_at + timedelta(seconds=1)
        clock_values = iter(
            [initial_started_at, retry_started_at, observed_at, completed_at]
        )

        class SequenceDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = next(clock_values)
                return value if tz is not None else value.replace(tzinfo=None)

        http_calls: list[dict] = []
        session_key = 'feedback/signal-decider/lost-batch-response-retry'
        response_payload = {
            'id': 'batch-response-after-retry',
            'metadata': {'session_id': 'batch-session-completed-before-retry'},
            'status': 'completed',
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
                                            'feedback': _complete_actionable_signal_feedback(),
                                        }
                                    ]
                                }
                            ),
                        }
                    ],
                }
            ],
        }
        responses: list[Exception | FakeHTTPResponse] = [
            httpx.ReadTimeout('batch response lost after OpenClaw completion'),
            FakeHTTPResponse(200, response_payload),
        ]
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=300,
            openclaw_agent_max_retries=1,
            openclaw_agent_retry_backoff_seconds=1,
            openclaw_agent_max_prompt_chars=0,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            state_dir = Path(tmp_dir).resolve()
            fake_settings.openclaw_state_dir = str(state_dir)
            registry_path = (
                state_dir
                / 'agents'
                / 'signal-decider'
                / 'sessions'
                / 'sessions.json'
            )

            def complete_session_before_retry(_delay):
                started_ms = int(initial_started_at.timestamp() * 1000)
                registry_path.parent.mkdir(parents=True)
                registry_path.write_text(
                    json.dumps(
                        {
                            session_key: {
                                'sessionId': 'batch-session-completed-before-retry',
                                'status': 'completed',
                                'startedAt': started_ms,
                                'endedAt': started_ms + 1_000,
                                'updatedAt': started_ms + 1_000,
                                'modelProvider': 'openai',
                                'model': 'gpt-5.6-sol',
                            }
                        }
                    ),
                    encoding='utf-8',
                )

            def fake_client_factory(*args, **kwargs):
                outcome = responses.pop(0)
                if isinstance(outcome, Exception):
                    return RaisingHTTPClient(
                        exc=outcome,
                        calls=http_calls,
                        **kwargs,
                    )
                return FakeHTTPClient(
                    response=outcome,
                    calls=http_calls,
                    **kwargs,
                )

            with patch.object(orchestrator_module, 'settings', fake_settings), patch.object(
                orchestrator_module,
                'datetime',
                SequenceDateTime,
            ), patch.object(
                orchestrator_module.time,
                'sleep',
                side_effect=complete_session_before_retry,
            ), patch(
                'app.agent_context.orchestrator.build_runtime_contract_provenance',
                return_value=_runtime_contract_proof('stable'),
            ), patch(
                'app.agent_context.orchestrator.httpx.Client',
                side_effect=fake_client_factory,
            ):
                result = OpenClawAgentClient().dispatch_batch(
                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                    market_date=date(2026, 4, 23),
                    batch_items=[
                        {
                            'market_date': '2026-04-23',
                            'symbol': '005930',
                            'session_key': session_key,
                        }
                    ],
                )

        self.assertEqual(result['attempt'], 2)
        self.assertEqual(len(http_calls), 2)
        self.assertEqual(
            [call['kwargs']['headers']['x-openclaw-session-key'] for call in http_calls],
            [session_key, session_key],
        )
        self.assertEqual(
            result['model_identity']['actual_model_ref'],
            'openai/gpt-5.6-sol',
            result['model_identity'],
        )
        self.assertTrue(result['model_identity']['verified'])

    def test_openclaw_auth_preflight_maps_gateway_auth_rejection(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )

        for status_code in (401, 403):
            with self.subTest(status_code=status_code):
                http_calls: list[dict] = []

                def fake_client_factory(*args, **kwargs):
                    return FakeHTTPClient(
                        response=FakeHTTPResponse(status_code, {'error': 'unauthorized'}),
                        calls=http_calls,
                        **kwargs,
                    )

                with patch.object(orchestrator_module, 'settings', fake_settings):
                    with patch('app.agent_context.orchestrator.httpx.Client', side_effect=fake_client_factory):
                        client = OpenClawAgentClient()
                        result = client.preflight_dispatch_auth()

                self.assertFalse(result['ok'])
                self.assertEqual(result['status'], 'failed')
                self.assertEqual(result['failure_kind'], 'openclaw_auth_rejected')
                self.assertEqual(result['failure_status'], 'auth/rejected')
                self.assertFalse(result['retryable'])
                self.assertTrue(result['token_configured'])
                self.assertEqual(result['response_status_code'], status_code)
                self.assertEqual(http_calls[0]['url'], 'https://openclaw.example/tools/invoke')
                self.assertEqual(http_calls[0]['json']['tool'], 'sessions_history')

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
        self.assertIn('usage_policy.model_guidance_eligible=true', message)
        self.assertIn('manual_review_only', message)
        self.assertIn('must never relax wait/reject into buy or authorize a buy', message)
        self.assertNotIn('tighten or relax the actual action threshold', message)

    def test_short_term_and_fundamental_prompts_use_canonical_horizon_and_market_specific_sources(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=0,
        )
        short_term_spec = next(
            spec for spec in orchestrator_module.DISPATCH_SPECS if spec.role == 'short-term-decider'
        )
        fundamental_spec = next(
            spec for spec in orchestrator_module.DISPATCH_SPECS if spec.role == 'fundamental-summarizer'
        )
        us_item = {
            'symbol': 'AAPL',
            'market_date': '2026-08-19',
            'symbol_day_context': {
                'market': 'US',
                'fundamentals': {'source': 'sec_edgar_companyfacts'},
            },
        }

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()
            short_term_message = client._build_batch_message(
                spec=short_term_spec,
                market_date=orchestrator_module.date.fromisoformat('2026-08-19'),
                batch_items=[us_item],
                request_marker='batch-request::short-horizon',
            )
            fundamental_message = client._build_batch_message(
                spec=fundamental_spec,
                market_date=orchestrator_module.date.fromisoformat('2026-08-19'),
                batch_items=[us_item],
                request_marker='batch-request::fundamental-source',
            )

        self.assertIn('1-3 trading day horizon', short_term_message)
        self.assertIn('"expected_holding_period": "1-3 trading days"', short_term_message)
        self.assertIn('"preferred_action": "watch_for_trigger"', short_term_message)
        self.assertNotIn('1-2 trading day', short_term_message)
        self.assertIn('DART', fundamental_message)
        self.assertIn('SEC', fundamental_message)
        self.assertIn('market and source metadata', fundamental_message)

    def test_eod_prompt_limits_a_bounded_batch_to_per_symbol_readiness_and_anomalies(self):
        eod_spec = next(
            spec for spec in orchestrator_module.DISPATCH_SPECS if spec.role == 'eod-summarizer'
        )

        self.assertIn('per-symbol', eod_spec.instruction)
        self.assertIn('completeness', eod_spec.instruction)
        self.assertIn('freshness', eod_spec.instruction)
        self.assertIn('anomalies', eod_spec.instruction)
        self.assertIn('Do not infer market-wide breadth, leaders, or laggards', eod_spec.instruction)
        self.assertIn('bounded dispatch batch', eod_spec.instruction)

    def test_signal_decider_prompt_budget_never_silently_drops_assigned_symbols(self):
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
            with self.assertRaisesRegex(
                orchestrator_module.AgentDispatchTerminalError,
                'without dropping assigned symbols',
            ):
                client._build_batch_message(
                    spec=orchestrator_module.DISPATCH_SPECS[-1],
                    market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                    batch_items=[
                        {'symbol': '005930', 'market_date': '2026-04-14', 'large_context': 'x' * 1000},
                        {'symbol': '000660', 'market_date': '2026-04-14', 'large_context': 'y' * 1000},
                    ],
                    request_marker='batch-request::trim-fallback',
                )

    def test_signal_decider_prompt_compression_preserves_every_assigned_symbol_and_count(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=60000,
        )
        huge_items = [
            {
                'symbol': symbol,
                'market_date': '2026-04-14',
                'context_provenance': {'raw': marker * 80000},
                'upstream_summaries': {'technical-summarizer': {'raw_payload': marker * 80000}},
            }
            for symbol, marker in [('005930', 'x'), ('000660', 'y')]
        ]

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()
            message = client._build_batch_message(
                spec=orchestrator_module.DISPATCH_SPECS[-1],
                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                batch_items=huge_items,
                request_marker='batch-request::compress-all',
            )

        self.assertLess(len(message), fake_settings.openclaw_agent_max_prompt_chars)
        self.assertIn('Request marker: batch-request::compress-all', message)
        self.assertIn('Assigned symbols (exact authoritative strings): ["005930", "000660"]', message)
        self.assertIn('output exactly 2 item(s)', message)
        self.assertIn('"symbol": "005930"', message)
        self.assertIn('"symbol": "000660"', message)
        self.assertGreaterEqual(message.count('compressed_payload'), 2)
        self.assertIn('usage_policy.model_guidance_eligible=true', message)
        self.assertIn('must never relax wait/reject into buy or authorize a buy', message)

    def test_compressed_batch_prompt_builds_and_includes_contract_suffix_once(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=60000,
        )
        signal_decider_spec = orchestrator_module.DISPATCH_SPECS[-1]
        original_suffix_builder = orchestrator_module.build_contract_prompt_suffix

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(
                orchestrator_module,
                'build_contract_prompt_suffix',
                wraps=original_suffix_builder,
            ) as suffix_builder:
                client = OpenClawAgentClient()
                message = client._build_batch_message(
                    spec=signal_decider_spec,
                    market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                    batch_items=[
                        {
                            'symbol': '005930',
                            'market_date': '2026-04-14',
                            'context_provenance': {'raw': 'x' * 80000},
                            'upstream_summaries': {
                                'technical-summarizer': {'raw_payload': 'y' * 80000},
                            },
                        }
                    ],
                    request_marker='batch-request::single-contract-suffix',
                )

        contract_suffix = original_suffix_builder(role=signal_decider_spec.role)
        self.assertEqual(suffix_builder.call_count, 1)
        self.assertEqual(message.count(contract_suffix), 1)

    def test_signal_decider_prompt_trim_final_fallback_compresses_large_one_symbol_payload(self):
        fake_settings = SimpleNamespace(
            openclaw_base_url='https://openclaw.example',
            openclaw_gateway_token='gateway-token',
            openclaw_agent_timeout_seconds=30,
            openclaw_agent_max_retries=0,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_prompt_chars=60000,
        )

        huge_item = {
            'symbol': '005930',
            'market_date': '2026-04-14',
            'context_provenance': {'raw': 'x' * 80000},
            'upstream_summaries': {
                'technical-summarizer': {
                    'summary': '상승 추세 확인. ' * 1000,
                    'raw_payload': 'y' * 80000,
                    'risk_flags': ['flag'] * 50,
                }
            },
            'decision_input_quality_context': {
                'final_judgment': {'decision_band': 'high_conviction', 'hard_blocks': []},
                'raw_rows': ['z' * 1000] * 100,
            },
        }

        with patch.object(orchestrator_module, 'settings', fake_settings):
            client = OpenClawAgentClient()
            message = client._build_batch_message(
                spec=orchestrator_module.DISPATCH_SPECS[-1],
                market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                batch_items=[huge_item],
                request_marker='batch-request::large-one',
            )

        self.assertLess(len(message), fake_settings.openclaw_agent_max_prompt_chars)
        self.assertIn('Assigned symbols (exact authoritative strings): ["005930"]', message)
        self.assertIn('"symbol": "005930"', message)
        self.assertIn('compressed_payload', message)
        self.assertIn('hash', message)
        self.assertIn('context_provenance', message)
        self.assertIn('raw_payload', message)

    def test_compressed_batch_item_for_prompt_keeps_bulky_raw_fields_as_summaries(self):
        compressed = compressed_batch_item_for_prompt(
            {
                'symbol': '005930',
                'market_date': '2026-04-14',
                'raw_payload': 'x' * 5000,
                'upstream_summaries': {
                    'signal-decider': {
                        'summary': '요약' * 1000,
                        'response_payload': {'raw': 'y' * 5000},
                    }
                },
            }
        )

        encoded = json.dumps(compressed, ensure_ascii=False)
        self.assertIn('005930', encoded)
        self.assertIn('compressed_payload', encoded)
        self.assertIn('text_evidence_summary_v1', encoded)
        self.assertIn('raw_payload', encoded)
        self.assertIn('response_payload', encoded)

    def test_compressed_batch_item_preserves_exact_dispatched_performance_policy(self):
        dispatched_policy = {
            'current_version': 'signal_decider_policy_v1',
            'activation_mode': 'manual_only',
            'performance_criteria': {
                'schema_version': 'signal_decider_performance_criteria_v1',
                'status': 'ready',
                'sample_count': 12,
                'performance_buckets': [
                    {
                        'bucket_key': 'decision.confidence_bucket',
                        'bucket_value': f'confidence_{index}',
                        'sample_count': 3,
                        'negative_rate': 0.7,
                    }
                    for index in range(6)
                ],
                'rule_candidates': [],
                'usage_policy': {
                    'model_guidance_eligible': True,
                    'deterministic_gate_eligible': True,
                    'manual_review_only': True,
                    'allow_buy_authorization': False,
                },
            },
        }

        compressed = compressed_batch_item_for_prompt(
            {
                'symbol': '005930',
                'market_date': '2026-04-14',
                'signal_decider_policy': deepcopy(dispatched_policy),
                'context_provenance': {'raw': 'x' * 80_000},
            }
        )

        self.assertEqual(compressed['signal_decider_policy'], dispatched_policy)

    def test_reuse_existing_skips_reusable_symbol_role_pairs(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        runtime_store = RuntimeFeedbackStateStore()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
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
            with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
                with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                    with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                                with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                        with _patch_feedback_eligibility('005930', '000660'):
                                            result = service.dispatch_for_symbols(
                                                market_date='2026-04-14',
                                                symbols=['005930', '000660'],
                                                roles=['eod-summarizer'],
                                                reuse_existing=True,
                                                parent_run_id='feedback-parent-reuse-test',
                                                knowledge_cutoff_at=(
                                                    '2026-04-14T08:15:00+00:00'
                                                ),
                                            )

        dispatched_symbols = [
            item['symbol']
            for call in client.calls
            for item in call['batch_items']
        ]
        self.assertEqual(read_service.calls[0]['symbols'], ['000660'])
        self.assertEqual(
            read_service.calls[0]['knowledge_cutoff_at'],
            '2026-04-14T08:15:00+00:00',
        )
        self.assertEqual(dispatched_symbols, ['000660'])
        self.assertEqual([item['symbol'] for item in feedback_service.bulk_items], ['000660'])
        self.assertEqual(failure_service.recorded, [])
        self.assertEqual(run_state_service.finished[0]['status'], 'completed')
        self.assertEqual(
            run_state_service.started[0]['request_payload']['parent_run_id'],
            'feedback-parent-reuse-test',
        )
        self.assertEqual(
            run_state_service.finished[0]['summary_payload']['request']['parent_run_id'],
            'feedback-parent-reuse-test',
        )
        self.assertEqual(
            run_state_service.finished[0]['summary_payload']['reused_symbols_by_role'],
            {'eod-summarizer': ['005930']},
        )
        self.assertEqual(result['request']['parent_run_id'], 'feedback-parent-reuse-test')
        self.assertEqual([target.symbol for target in cleanup_service.targets], ['000660'])

    def test_reuse_existing_cached_only_skips_openclaw_auth_preflight(self):
        service = AgentFeedbackOrchestrator()
        client = PreflightFailingClient()
        service.client = client
        runtime_store = RuntimeFeedbackStateStore()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        read_service = RecordingAgentContextReadService()
        cleanup_service = FakeSessionCleanupService()
        events = []

        def build_latest_decisions(**kwargs):
            events.append('reuse')
            return {
                '005930': {'eod-summarizer': _reuse_decision(False)},
                '000660': {'eod-summarizer': _reuse_decision(False)},
            }

        reuse_service = SimpleNamespace(build_latest_decisions=build_latest_decisions)
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                        with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                                with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                        with _patch_feedback_eligibility('005930', '000660'):
                                            result = service.dispatch_for_symbols(
                                                market_date='2026-04-14',
                                                symbols=['005930', '000660'],
                                                roles=['eod-summarizer'],
                                                reuse_existing=True,
                                                request_run_id='cached-only-reuse-test-run',
                                            )

        self.assertEqual(events, ['reuse'])
        self.assertEqual(client.preflight_calls, 0)
        self.assertEqual(client.calls, [])
        self.assertEqual(read_service.calls, [])
        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(failure_service.recorded, [])
        self.assertEqual(result['summary']['status'], 'completed')
        self.assertEqual(result['summary']['completed'], 2)
        self.assertEqual(result['summary']['computed_count'], 0)
        self.assertEqual(result['summary']['reused_count'], 2)
        self.assertEqual(result['summary']['failed'], 0)
        self.assertEqual(run_state_service.finished[0]['status'], 'completed')
        self.assertEqual(run_state_service.finished[0]['completed_count'], 2)

    def test_reuse_existing_live_symbol_runs_openclaw_auth_preflight_before_dispatch(self):
        service = AgentFeedbackOrchestrator()
        events = []

        class EventPreflightFailingClient(PreflightFailingClient):
            def preflight_dispatch_auth(self):
                events.append('preflight')
                return super().preflight_dispatch_auth()

        client = EventPreflightFailingClient()
        service.client = client
        runtime_store = RuntimeFeedbackStateStore()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        read_service = RecordingAgentContextReadService()
        cleanup_service = FakeSessionCleanupService()

        def build_latest_decisions(**kwargs):
            events.append('reuse')
            return {
                '005930': {'eod-summarizer': _reuse_decision(False)},
                '000660': {'eod-summarizer': _reuse_decision(True)},
            }

        reuse_service = SimpleNamespace(build_latest_decisions=build_latest_decisions)
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                        with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                                with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                        with _patch_feedback_eligibility('005930', '000660'):
                                            result = service.dispatch_for_symbols(
                                                market_date='2026-04-14',
                                                symbols=['005930', '000660'],
                                                roles=['eod-summarizer'],
                                                reuse_existing=True,
                                                request_run_id='mixed-reuse-auth-preflight-test-run',
                                            )

        self.assertEqual(events, ['reuse', 'preflight'])
        self.assertEqual(client.preflight_calls, 1)
        self.assertEqual(client.calls, [])
        self.assertEqual(read_service.calls, [])
        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['computed_count'], 0)
        self.assertEqual(result['summary']['reused_count'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertTrue(result['summary']['run_level_failure'])
        self.assertEqual(result['summary']['failure_kind'], 'openclaw_auth_missing')
        self.assertEqual(len(failure_service.recorded), 1)
        recorded = failure_service.recorded[0]
        self.assertEqual(recorded['symbol'], orchestrator_module.RUN_LEVEL_FAILURE_SYMBOL)
        self.assertEqual(recorded['agent_name'], orchestrator_module.OPENCLAW_GATEWAY_AGENT_NAME)
        self.assertEqual(recorded['failure_kind'], 'openclaw_auth_missing')
        self.assertEqual(recorded['context']['origin'], 'openclaw_dispatch_auth_preflight')
        self.assertEqual(recorded['context']['affected_symbols'], ['000660'])
        self.assertEqual(run_state_service.finished[0]['status'], 'failed')
        self.assertEqual(run_state_service.finished[0]['completed_count'], 0)

    def test_dispatch_finalization_counts_reused_successes_with_missing_terminal_failures(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        symbols = [f'{index:06d}' for index in range(1, 21)]
        reused_symbols = set(symbols[:15])
        missing_symbols = symbols[15:]
        reuse_service = SimpleNamespace(
            build_latest_decisions=lambda **kwargs: {
                symbol: {
                    'eod-summarizer': _reuse_decision(symbol not in reused_symbols),
                }
                for symbol in symbols
            }
        )
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(service, '_prepare_role_dispatch_items', return_value=[]):
            with patch.object(orchestrator_module, 'settings', fake_settings):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                                with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                        with _patch_feedback_eligibility(*symbols):
                                            result = service.dispatch_for_symbols(
                                                market_date='2026-04-14',
                                                symbols=symbols,
                                                roles=['eod-summarizer'],
                                                reuse_existing=True,
                                            )

        self.assertEqual(client.calls, [])
        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(result['summary']['status'], 'partial')
        self.assertEqual(result['summary']['completed'], 15)
        self.assertEqual(result['summary']['reused_count'], 15)
        self.assertEqual(result['summary']['computed_count'], 0)
        self.assertEqual(result['summary']['failed'], 5)
        self.assertEqual(run_state_service.finished[0]['status'], 'partial')
        self.assertEqual(run_state_service.finished[0]['completed_count'], 15)
        self.assertEqual(len(failure_service.recorded), 5)
        self.assertEqual(
            {failure['symbol'] for failure in failure_service.recorded},
            set(missing_symbols),
        )
        self.assertTrue(
            all(failure['failure_kind'] == 'dispatch_incomplete' for failure in failure_service.recorded)
        )

    def test_dispatch_for_symbols_persists_run_level_openclaw_auth_failure(self):
        service = AgentFeedbackOrchestrator()
        client = PreflightFailingClient()
        service.client = client
        runtime_store = RuntimeFeedbackStateStore()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        run_state_service = RecordingRunStateService()
        read_service = RecordingAgentContextReadService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'runtime_feedback_state_store', runtime_store):
                with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                    with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                            with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                                    with _patch_feedback_eligibility('005930', '000660'):
                                        result = service.dispatch_for_symbols(
                                            market_date='2026-04-14',
                                            symbols=['005930', '000660'],
                                            roles=['eod-summarizer'],
                                            reuse_existing=False,
                                            request_run_id='auth-preflight-test-run',
                                        )

        self.assertEqual(client.preflight_calls, 1)
        self.assertEqual(client.calls, [])
        self.assertEqual(read_service.calls, [])
        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['computed_count'], 0)
        self.assertEqual(result['summary']['reused_count'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertTrue(result['summary']['run_level_failure'])
        self.assertEqual(result['summary']['failure_kind'], 'openclaw_auth_missing')
        self.assertEqual(len(failure_service.recorded), 1)
        recorded = failure_service.recorded[0]
        self.assertEqual(recorded['symbol'], orchestrator_module.RUN_LEVEL_FAILURE_SYMBOL)
        self.assertEqual(recorded['agent_name'], orchestrator_module.OPENCLAW_GATEWAY_AGENT_NAME)
        self.assertEqual(recorded['failure_kind'], 'openclaw_auth_missing')
        self.assertFalse(recorded['retryable'])
        self.assertEqual(recorded['context']['origin'], 'openclaw_dispatch_auth_preflight')
        self.assertEqual(recorded['context']['affected_symbols'], ['005930', '000660'])
        self.assertEqual(run_state_service.finished[0]['status'], 'failed')
        self.assertEqual(run_state_service.finished[0]['completed_count'], 0)
        self.assertEqual(run_state_service.finished[0]['failed_count'], 1)
        self.assertNotIn(
            'dispatch_incomplete',
            {failure.get('failure_kind') for failure in result['failures']},
        )

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
                parent_run_id='feedback-parent-prerequisite-gate',
            )

        self.assertTrue(result['summary']['gated'])
        self.assertEqual(result['summary']['status'], 'blocked')
        self.assertEqual(result['items'], [])
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(
            result['request']['parent_run_id'],
            'feedback-parent-prerequisite-gate',
        )

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
        session_keys = [call['batch_items'][0]['session_key'] for call in client.calls]
        self.assertEqual(len(session_keys), len(set(session_keys)))
        for call in client.calls:
            batch_item = call['batch_items'][0]
            self.assertEqual(batch_item['session_reuse_mode'], 'fresh_per_batch')
            self.assertIn(result['request']['request_run_id'], batch_item['session_key'])
            self.assertRegex(batch_item['session_key'], r'/[0-9a-f]{10}$')
        self.assertEqual(
            {
                batch_index
                for batch_index in range(1, 4)
                if any(f'/batch-{batch_index:03d}/' in key for key in session_keys)
            },
            {1, 2, 3},
        )
        self.assertEqual(result['request']['per_role_parallelism'], 3)
        self.assertEqual(result['request']['total_parallel_subtasks'], 3)
        self.assertEqual(result['summary']['completed'], 6)
        self.assertEqual(len(cleanup_service.targets), 3)
        self.assertEqual(
            {target.session_key for target in cleanup_service.targets},
            set(session_keys),
        )

    def test_subagent_dispatch_rejects_inner_identity_mismatch_before_persistence(self):
        service = AgentFeedbackOrchestrator()
        service.client = InnerIdentityMismatchBatchClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    with _patch_feedback_eligibility('005930'):
                        result = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=['005930'],
                            roles=['eod-summarizer'],
                            per_role_parallelism=1,
                        )

        self.assertEqual(feedback_service.submitted, [])
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertEqual(
            result['failures'][0]['error'],
            'response_identity_mismatch:symbol_mismatch,market_date_mismatch',
        )

    def test_subagent_dispatch_rejects_batch_attribution_mismatch_before_persistence(self):
        service = AgentFeedbackOrchestrator()
        service.client = DuplicateAttributionBatchClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    with _patch_feedback_eligibility('005930'):
                        result = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=['005930'],
                            roles=['eod-summarizer'],
                            per_role_parallelism=1,
                        )

        self.assertEqual(feedback_service.submitted, [])
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertEqual(result['failures'][0]['error'], 'batch_response_attribution_mismatch')

    def test_subagent_dispatch_rejects_whole_batch_when_one_feedback_object_is_missing(self):
        service = AgentFeedbackOrchestrator()
        service.client = MissingFeedbackBatchClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    with _patch_feedback_eligibility('005930', '000660'):
                        result = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=['005930', '000660'],
                            roles=['eod-summarizer'],
                            per_role_parallelism=1,
                        )

        self.assertEqual(feedback_service.submitted, [])
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 2)
        self.assertEqual(
            {failure['error'] for failure in result['failures']},
            {'response_contract_invalid:feedback_missing'},
        )
        self.assertEqual(
            {failure['failure_kind'] for failure in result['failures']},
            {'response_contract_invalid:feedback_missing'},
        )
        self.assertEqual(len(cleanup_service.targets), 1)

    def test_subagent_dispatch_failure_cleans_allocated_batch_session_once(self):
        service = AgentFeedbackOrchestrator()
        client = RaisingBatchClient()
        service.client = client
        cleanup_service = FakeSessionCleanupService()

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                with _patch_feedback_eligibility('005930'):
                    result = service.dispatch_for_symbols_via_subagents(
                        market_date='2026-04-14',
                        symbols=['005930'],
                        roles=['eod-summarizer'],
                        per_role_parallelism=1,
                    )

        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertEqual(len(cleanup_service.targets), 1)
        self.assertEqual(
            cleanup_service.targets[0].session_key,
            client.calls[0]['batch_items'][0]['session_key'],
        )

    def test_subagent_cleanup_failure_is_audited_without_overwriting_dispatch_result(self):
        service = AgentFeedbackOrchestrator()
        service.client = RecordingBatchClient()
        feedback_service = FakeFeedbackService()
        cleanup_service = RaisingSessionCleanupService()

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    with _patch_feedback_eligibility('005930'):
                        result = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=['005930'],
                            roles=['eod-summarizer'],
                            per_role_parallelism=1,
                        )

        self.assertEqual(result['summary']['status'], 'completed')
        self.assertEqual(result['summary']['completed'], 1)
        self.assertEqual(result['summary']['failed'], 0)
        self.assertEqual(len(cleanup_service.targets), 1)
        self.assertEqual(result['session_cleanup'][0]['status'], 'deferred')
        self.assertEqual(
            result['session_cleanup'][0]['reason'],
            'fresh_batch_session_cleanup_failed',
        )
        self.assertEqual(
            result['session_cleanup'][0]['error'],
            'cleanup audit store unavailable',
        )

    def test_same_date_subagent_dispatches_never_reuse_batch_session_keys(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    with _patch_feedback_eligibility('005930'):
                        first = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=['005930'],
                            roles=['eod-summarizer'],
                            per_role_parallelism=1,
                        )
                        second = service.dispatch_for_symbols_via_subagents(
                            market_date='2026-04-14',
                            symbols=['005930'],
                            roles=['eod-summarizer'],
                            per_role_parallelism=1,
                        )

        first_key = client.calls[0]['batch_items'][0]['session_key']
        second_key = client.calls[1]['batch_items'][0]['session_key']
        self.assertNotEqual(first_key, second_key)
        self.assertIn(first['request']['request_run_id'], first_key)
        self.assertIn(second['request']['request_run_id'], second_key)
        self.assertIn('/batch-001/', first_key)
        self.assertIn('/batch-001/', second_key)
        self.assertEqual(client.calls[0]['batch_items'][0]['session_reuse_mode'], 'fresh_per_batch')
        self.assertEqual(client.calls[1]['batch_items'][0]['session_reuse_mode'], 'fresh_per_batch')

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

    def test_dispatch_for_symbols_skips_hard_blocked_prefilter_symbols(self):
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

    def test_dispatch_for_symbols_reports_soft_penalized_symbols_without_skipping_them(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        read_service = RecordingAgentContextReadService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=20)
        soft_record = {
            'symbol': '066570',
            'market_date': '2026-05-29',
            'rule_version': 'strategy_aware_prefilter_scoring_v1',
            'strategy_scope': 'short_term_momentum_breakout',
            'candidate_path': 'short_term_momentum_breakout',
            'eligible': True,
            'hard_blocked': False,
            'status': 'soft_penalized_dispatched',
            'hard_block_reasons': [],
            'soft_penalties': [{'code': 'not_low_pbr', 'source': 'piotroski_filter'}],
            'filter_signals': {'piotroski_filter': {'is_low_pbr': False}},
        }
        hard_record = {
            'symbol': '005930',
            'market_date': '2026-05-29',
            'rule_version': 'strategy_aware_prefilter_scoring_v1',
            'strategy_scope': 'short_term_momentum_breakout',
            'candidate_path': 'short_term_momentum_breakout',
            'eligible': False,
            'hard_blocked': True,
            'status': 'hard_blocked',
            'hard_block_reasons': [{'code': 'market_risk_trade_halted'}],
            'soft_penalties': [],
            'filter_signals': {'market_risk_filter': {'passed_filter': False}},
        }
        diagnostics = {
            'rule_version': 'strategy_aware_prefilter_scoring_v1',
            'strategy_scope': 'short_term_momentum_breakout',
            'evaluated_symbol_count': 2,
            'dispatched_symbol_count': 1,
            'hard_blocked_symbol_count': 1,
            'soft_penalized_dispatched_symbol_count': 1,
            'hard_blocked_symbols': ['005930'],
            'soft_penalized_symbols': ['066570'],
            'hard_block_reasons_by_symbol': {'005930': hard_record['hard_block_reasons']},
            'soft_penalties_by_symbol': {'066570': soft_record['soft_penalties']},
            'records_by_symbol': {'066570': soft_record, '005930': hard_record},
        }

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with patch.object(
                            orchestrator_module,
                            '_split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics',
                            return_value=(['066570'], ['005930'], diagnostics),
                        ):
                            result = service.dispatch_for_symbols(
                                market_date='2026-05-29',
                                symbols=['066570', '005930'],
                                roles=['eod-summarizer'],
                                reuse_existing=False,
                            )

        self.assertEqual(result['request']['symbols'], ['066570'])
        self.assertEqual(result['request']['skipped_symbols'], ['005930'])
        self.assertEqual(result['request']['soft_penalized_symbols'], ['066570'])
        self.assertEqual(result['request']['eligibility_diagnostics']['hard_blocked_symbols'], ['005930'])
        self.assertEqual(result['request']['eligibility_diagnostics']['soft_penalized_symbols'], ['066570'])
        self.assertNotIn('records_by_symbol', result['request']['eligibility_diagnostics'])
        dispatched_item = client.calls[0]['batch_items'][0]
        self.assertEqual(dispatched_item['symbol'], '066570')
        self.assertEqual(dispatched_item['agent_feedback_eligibility']['status'], 'soft_penalized_dispatched')
        self.assertEqual(
            dispatched_item['agent_feedback_eligibility']['soft_penalties'][0]['code'],
            'not_low_pbr',
        )

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

    def test_dispatch_for_symbols_allows_high_market_risk_coverage_with_low_dispatch_count(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        read_service = RecordingAgentContextReadService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=10, openclaw_agent_parallel_subtasks=1)
        requested_symbols = [f'{index:06d}' for index in range(1, 11)]
        dispatch_symbols = requested_symbols[:4]
        unrelated_rejected_symbols = requested_symbols[4:9]
        missing_market_risk_symbols = requested_symbols[9:]
        skipped_symbols = [*unrelated_rejected_symbols, *missing_market_risk_symbols]
        records_by_symbol = {
            symbol: {
                'symbol': symbol,
                'eligible': True,
                'hard_blocked': False,
                'cap_excluded': False,
                'hard_block_reasons': [],
                'soft_penalties': [],
            }
            for symbol in dispatch_symbols
        }
        records_by_symbol.update(
            {
                symbol: {
                    'symbol': symbol,
                    'eligible': False,
                    'hard_blocked': True,
                    'cap_excluded': False,
                    'hard_block_reasons': [{'code': 'test_hard_block'}],
                    'soft_penalties': [],
                }
                for symbol in unrelated_rejected_symbols
            }
        )
        records_by_symbol.update(
            {
                symbol: {
                    'symbol': symbol,
                    'eligible': False,
                    'hard_blocked': True,
                    'cap_excluded': False,
                    'hard_block_reasons': [
                        {
                            'code': 'missing_market_risk_filter',
                            'source': 'market_risk_filter',
                        }
                    ],
                    'soft_penalties': [],
                }
                for symbol in missing_market_risk_symbols
            }
        )
        diagnostics = {
            'rule_version': 'agent_feedback_short_term_momentum_v1',
            'requested_symbol_count': len(requested_symbols),
            'evaluated_symbol_count': len(requested_symbols),
            'dispatched_symbol_count': len(dispatch_symbols),
            'hard_blocked_symbol_count': len(skipped_symbols),
            'hard_blocked_symbols': skipped_symbols,
            'records_by_symbol': records_by_symbol,
        }

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        with patch.object(
                            orchestrator_module,
                            '_split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics',
                            return_value=(dispatch_symbols, skipped_symbols, diagnostics),
                        ):
                            result = service.dispatch_for_symbols(
                                market_date='2026-08-14',
                                symbols=requested_symbols,
                                roles=['eod-summarizer'],
                                reuse_existing=False,
                            )

        self.assertEqual(result['summary']['status'], 'completed')
        self.assertNotIn('gate_reason', result['summary'])
        self.assertEqual(result['summary']['completed'], 4)
        self.assertEqual(result['summary']['skipped'], 6)
        self.assertEqual(read_service.calls[0]['symbols'], dispatch_symbols)
        self.assertEqual(
            [item['symbol'] for call in client.calls for item in call['batch_items']],
            dispatch_symbols,
        )
        self.assertEqual(result['request']['symbols'], dispatch_symbols)
        self.assertEqual(result['request']['skipped_symbols'], skipped_symbols)
        self.assertNotIn('eligibility_scope_gate', result.get('prerequisites') or {})

    def test_dispatch_for_symbols_blocks_requested_scope_when_market_risk_coverage_collapses(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        requested_symbols = [f'{index:06d}' for index in range(1, 81)]
        dispatch_symbols = [requested_symbols[0]]
        skipped_symbols = requested_symbols[1:]
        records_by_symbol = {
            requested_symbols[0]: {
                'symbol': requested_symbols[0],
                'eligible': True,
                'hard_blocked': False,
                'cap_excluded': False,
                'hard_block_reasons': [],
                'soft_penalties': [],
            },
            **{
                symbol: {
                    'symbol': symbol,
                    'eligible': False,
                    'hard_blocked': True,
                    'cap_excluded': False,
                    'hard_block_reasons': [
                        {
                            'code': 'missing_market_risk_filter',
                            'source': 'market_risk_filter',
                        }
                    ],
                    'soft_penalties': [],
                }
                for symbol in skipped_symbols
            },
        }
        diagnostics = {
            'rule_version': 'agent_feedback_short_term_momentum_v1',
            'requested_symbol_count': len(requested_symbols),
            'evaluated_symbol_count': len(requested_symbols),
            'dispatched_symbol_count': len(dispatch_symbols),
            'hard_blocked_symbol_count': len(skipped_symbols),
            'hard_blocked_symbols': skipped_symbols,
            'records_by_symbol': records_by_symbol,
        }

        with patch.object(
            orchestrator_module,
            '_split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics',
            return_value=(dispatch_symbols, skipped_symbols, diagnostics),
        ):
            result = service.dispatch_for_symbols(
                market_date='2026-08-14',
                symbols=requested_symbols,
                roles=['signal-decider'],
                reuse_existing=False,
                parent_run_id='feedback-parent-eligibility-gate',
            )

        self.assertEqual(result['summary']['status'], 'blocked')
        self.assertTrue(result['summary']['gated'])
        self.assertEqual(
            result['summary']['gate_reason'],
            'requested_scope_missing_market_risk_filter_coverage',
        )
        self.assertEqual(result['prerequisites']['blocked_stage_ids'], ['market_risk_filter'])
        self.assertEqual(
            result['prerequisites']['eligibility_scope_gate']['dispatch_symbol_count'],
            1,
        )
        self.assertEqual(
            result['request']['parent_run_id'],
            'feedback-parent-eligibility-gate',
        )
        self.assertEqual(client.calls, [])

    def test_default_symbol_dispatch_builds_signal_decider_payload_from_full_upstream_summaries(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=2)
        reuse_calls = []
        def build_latest_decisions(**kwargs):
            reuse_calls.append(kwargs)
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
                                        force_recompute=True,
                                    )

        self.assertEqual(
            result['request']['roles'],
            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer', 'short-term-decider', 'signal-decider'],
        )
        upstream_reuse_call = next(
            call
            for call in reuse_calls
            if set(call['roles']) == set(orchestrator_module.UPSTREAM_SUMMARY_ROLES)
        )
        self.assertFalse(upstream_reuse_call['force_recompute'])
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
        self.assertTrue(
            decider_payload['decision_support_constraints']['external_news_contract'][
                'price_forecast_from_news_alone_prohibited'
            ]
        )
        self.assertTrue(
            decider_payload['decision_support_constraints']['external_news_contract'][
                'headline_only_or_paywalled_not_full_text'
            ]
        )
        self.assertTrue(
            decider_payload['decision_support_constraints']['external_news_contract'][
                'bounded_noise_filter_required'
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
        self.assertIn('/batch-001/', eod_call['batch_items'][0]['session_key'])
        decider_call = next(call for call in client.calls if call['spec'].role == 'signal-decider')
        self.assertEqual(decider_call['batch_items'][0]['session_reuse_mode'], 'fresh_per_batch')
        self.assertTrue(decider_call['batch_items'][0]['session_key'].startswith('agent:signal-decider:feedback-fresh/signal-decider/'))
        self.assertIn(result['request']['request_run_id'], decider_call['batch_items'][0]['session_key'])
        self.assertIn('/batch-001/', decider_call['batch_items'][0]['session_key'])
        self.assertEqual(market_context_builder.calls, ['2026-04-14'] * 5)
        self.assertEqual(result['summary']['completed'], 5)

    def test_auto_scoped_symbol_dispatch_applies_smaller_decider_role_caps(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        symbols = ['005930', '000660', '035420', '051910']
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=10, openclaw_agent_parallel_subtasks=1)
        reuse_service = SimpleNamespace(build_latest_decisions=lambda **kwargs: {})
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
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
                                with patch(
                                    'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_eod_summarizer',
                                    4,
                                ):
                                    with patch(
                                        'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_technical_summarizer',
                                        4,
                                    ):
                                        with patch(
                                            'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_fundamental_summarizer',
                                            4,
                                        ):
                                            with patch(
                                                'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_short_term_decider',
                                                1,
                                            ):
                                                with patch(
                                                    'app.agent_context.eligibility.settings.openclaw_feedback_role_cap_signal_decider',
                                                    1,
                                                ):
                                                    with _patch_feedback_eligibility(*symbols):
                                                        result = service.dispatch_for_symbols(
                                                            market_date='2026-04-14',
                                                            symbols=symbols,
                                                            reuse_existing=False,
                                                            auto_scope=True,
                                                        )

        symbols_by_role = {
            call['spec'].role: [item['symbol'] for item in call['batch_items']]
            for call in client.calls
        }

        self.assertEqual(symbols_by_role['eod-summarizer'], symbols)
        self.assertEqual(symbols_by_role['technical-summarizer'], symbols)
        self.assertEqual(symbols_by_role['fundamental-summarizer'], symbols)
        self.assertEqual(len(symbols_by_role['short-term-decider']), 1)
        self.assertEqual(len(symbols_by_role['signal-decider']), 1)
        self.assertEqual(result['request']['broad_symbols'], symbols)
        self.assertEqual(result['request']['role_symbol_counts']['eod-summarizer'], 4)
        self.assertEqual(result['request']['role_symbol_counts']['short-term-decider'], 1)
        self.assertEqual(result['request']['role_symbol_counts']['signal-decider'], 1)
        self.assertEqual(
            result['request']['funnel_diagnostics']['role_symbol_counts']['signal-decider'],
            1,
        )
        self.assertTrue(
            result['request']['funnel_diagnostics']['role_diagnostics']['signal-decider']['role_cap']['applied']
        )
        self.assertEqual(result['summary']['completed'], 14)

    def test_chunked_decider_revalidation_preserves_original_source_and_eligibility_scope(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        symbols = ['005930', '000660']
        roles = [
            'eod-summarizer',
            'technical-summarizer',
            'fundamental-summarizer',
            'short-term-decider',
        ]
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
        )
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
                'missing_fields': [],
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(
                orchestrator_module,
                'agent_context_read_service',
                DeciderReadyAgentContextReadService(),
            ):
                with patch.object(
                    orchestrator_module,
                    'agent_context_reuse_service',
                    SimpleNamespace(build_latest_decisions=lambda **kwargs: {}),
                ):
                    with patch.object(
                        orchestrator_module,
                        'agent_context_feedback_service',
                        feedback_service,
                    ):
                        with patch.object(
                            orchestrator_module,
                            'agent_feedback_session_cleanup_service',
                            FakeSessionCleanupService(),
                        ):
                            with patch.object(
                                orchestrator_module,
                                'signal_decider_market_context_builder',
                                market_context_builder,
                            ):
                                with _patch_feedback_eligibility(*symbols):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=symbols,
                                        roles=roles,
                                        reuse_existing=False,
                                    )

        decider_calls = [
            call
            for call in client.calls
            if call['spec'].role == 'short-term-decider'
        ]
        self.assertEqual(result['summary']['status'], 'completed')
        self.assertEqual(result['summary']['completed'], 8)
        self.assertEqual(result['failures'], [])
        self.assertEqual(
            [[item['symbol'] for item in call['batch_items']] for call in decider_calls],
            [['005930'], ['000660']],
        )

    def test_precomputed_auto_scope_revalidation_preserves_missing_eligibility_context(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        symbols = ['005930', '000660']
        roles = [
            'eod-summarizer',
            'technical-summarizer',
            'fundamental-summarizer',
            'short-term-decider',
        ]
        role_symbol_scopes = {
            role: list(symbols)
            for role in roles[:-1]
        }
        role_symbol_scopes['short-term-decider'] = ['005930']
        funnel_diagnostics = {
            'feedback_scope_mode': 'broad_feedback_universe',
            'broad_symbols': list(symbols),
            'role_symbols': role_symbol_scopes,
        }
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
        )
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
                'missing_fields': [],
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(
                orchestrator_module,
                'agent_context_read_service',
                DeciderReadyAgentContextReadService(),
            ):
                with patch.object(
                    orchestrator_module,
                    'agent_context_reuse_service',
                    SimpleNamespace(build_latest_decisions=lambda **kwargs: {}),
                ):
                    with patch.object(
                        orchestrator_module,
                        'agent_context_feedback_service',
                        feedback_service,
                    ):
                        with patch.object(
                            orchestrator_module,
                            'agent_feedback_session_cleanup_service',
                            FakeSessionCleanupService(),
                        ):
                            with patch.object(
                                orchestrator_module,
                                'signal_decider_market_context_builder',
                                market_context_builder,
                            ):
                                with _patch_feedback_eligibility(*symbols):
                                    result = service.dispatch_for_symbols(
                                        market_date='2026-04-14',
                                        symbols=symbols,
                                        roles=roles,
                                        reuse_existing=False,
                                        auto_scope=True,
                                        role_symbol_scopes=role_symbol_scopes,
                                        funnel_diagnostics=funnel_diagnostics,
                                    )

        decider_payload = next(
            payload
            for payload in client.decider_payloads
            if payload['role'] == 'short-term-decider'
        )
        self.assertEqual(result['summary']['status'], 'completed')
        self.assertEqual(result['summary']['completed'], 7)
        self.assertEqual(result['failures'], [])
        self.assertNotIn(
            'agent_feedback_eligibility',
            decider_payload['symbol_day_context'],
        )

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
        self.assertEqual(market_context_builder.calls, ['2026-04-14'] * 3)
        self.assertEqual(result['summary']['completed'], 4)

    def test_decider_response_is_rejected_when_source_context_changes_during_dispatch(self):
        service = AgentFeedbackOrchestrator()
        client = SignalDeciderBatchClient()
        service.client = client
        read_service = MutatingDeciderInputReadService()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
        )
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
                'missing_fields': [],
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(
                    orchestrator_module,
                    'agent_context_reuse_service',
                    SimpleNamespace(build_latest_decisions=lambda **kwargs: {}),
                ):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(
                            orchestrator_module,
                            'agent_context_feedback_failure_service',
                            failure_service,
                        ):
                            with patch.object(
                                orchestrator_module,
                                'agent_feedback_session_cleanup_service',
                                cleanup_service,
                            ):
                                with patch.object(
                                    orchestrator_module,
                                    'signal_decider_market_context_builder',
                                    market_context_builder,
                                ):
                                    with _patch_feedback_eligibility('005930'):
                                        result = service.dispatch_for_symbols(
                                            market_date='2026-04-14',
                                            symbols=['005930'],
                                            reuse_existing=False,
                                        )

        persisted_roles = {
            item['agent_name']
            for call in feedback_service.bulk_calls
            for item in call
        }
        self.assertEqual(
            persisted_roles,
            {
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
            },
        )
        self.assertEqual(
            {failure['agent_name'] for failure in failure_service.recorded},
            {'short-term-decider', 'signal-decider'},
        )
        self.assertEqual(
            {failure['failure_kind'] for failure in failure_service.recorded},
            {'dependency_upstream_changed_during_dispatch'},
        )
        self.assertEqual(result['summary']['status'], 'partial')
        self.assertEqual(result['summary']['completed'], 3)
        self.assertEqual(result['summary']['failed'], 2)
        self.assertGreaterEqual(read_service.calls, 3)

    def test_decider_response_is_rejected_when_eligibility_context_changes(self):
        service = AgentFeedbackOrchestrator()
        service.client = SignalDeciderBatchClient()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
        )
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
                'missing_fields': [],
            }
        )
        eligibility_calls = 0

        def changing_eligibility(*, market_date, requested_symbols):
            nonlocal eligibility_calls
            eligibility_calls += 1
            penalty_code = (
                'initial_penalty'
                if eligibility_calls == 1
                else 'updated_penalty'
            )
            records = {
                symbol: {
                    'symbol': symbol,
                    'market_date': market_date.isoformat(),
                    'rule_version': 'strategy_aware_prefilter_scoring_v1',
                    'strategy_scope': 'short_term_momentum_breakout',
                    'candidate_path': 'short_term_momentum_breakout',
                    'eligible': True,
                    'hard_blocked': False,
                    'status': 'soft_penalized_dispatched',
                    'hard_block_reasons': [],
                    'soft_penalties': [{'code': penalty_code}],
                    'filter_signals': {'rank': eligibility_calls},
                }
                for symbol in requested_symbols
            }
            return list(requested_symbols), [], {
                'records_by_symbol': records,
                'soft_penalized_symbols': list(requested_symbols),
                'hard_blocked_symbols': [],
            }

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(
                orchestrator_module,
                'agent_context_read_service',
                DeciderReadyAgentContextReadService(),
            ):
                with patch.object(
                    orchestrator_module,
                    'agent_context_reuse_service',
                    SimpleNamespace(build_latest_decisions=lambda **kwargs: {}),
                ):
                    with patch.object(
                        orchestrator_module,
                        'agent_context_feedback_service',
                        feedback_service,
                    ):
                        with patch.object(
                            orchestrator_module,
                            'agent_context_feedback_failure_service',
                            failure_service,
                        ):
                            with patch.object(
                                orchestrator_module,
                                'agent_feedback_session_cleanup_service',
                                cleanup_service,
                            ):
                                with patch.object(
                                    orchestrator_module,
                                    'signal_decider_market_context_builder',
                                    market_context_builder,
                                ):
                                    with patch.object(
                                        orchestrator_module,
                                        '_split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics',
                                        side_effect=changing_eligibility,
                                    ):
                                        result = service.dispatch_for_symbols(
                                            market_date='2026-04-14',
                                            symbols=['005930'],
                                            reuse_existing=False,
                                        )

        persisted_roles = {
            item['agent_name']
            for call in feedback_service.bulk_calls
            for item in call
        }
        self.assertEqual(
            persisted_roles,
            {
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
            },
        )
        self.assertEqual(
            {failure['agent_name'] for failure in failure_service.recorded},
            {'short-term-decider', 'signal-decider'},
        )
        revalidation_codes = {
            reason['code']
            for failure in failure_service.recorded
            for reason in failure['context'][
                'input_generation_revalidation'
            ]['reasons']
        }
        self.assertIn('source_context_changed', revalidation_codes)
        self.assertEqual(result['summary']['status'], 'partial')
        self.assertEqual(result['summary']['completed'], 3)
        self.assertEqual(result['summary']['failed'], 2)

    def test_finalization_invalidates_decider_success_when_source_changes_after_acceptance(self):
        service = AgentFeedbackOrchestrator()
        service.client = SignalDeciderBatchClient()
        read_service = MutatingDeciderInputReadService(stable_calls=3)
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
        )
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
                'missing_fields': [],
            }
        )

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(
                    orchestrator_module,
                    'agent_context_reuse_service',
                    SimpleNamespace(build_latest_decisions=lambda **kwargs: {}),
                ):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(
                            orchestrator_module,
                            'agent_context_feedback_failure_service',
                            failure_service,
                        ):
                            with patch.object(
                                orchestrator_module,
                                'agent_feedback_session_cleanup_service',
                                cleanup_service,
                            ):
                                with patch.object(
                                    orchestrator_module,
                                    'signal_decider_market_context_builder',
                                    market_context_builder,
                                ):
                                    with _patch_feedback_eligibility('005930'):
                                        result = service.dispatch_for_symbols(
                                            market_date='2026-04-14',
                                            symbols=['005930'],
                                            reuse_existing=False,
                                        )

        persisted_roles = {
            item['agent_name']
            for call in feedback_service.bulk_calls
            for item in call
        }
        self.assertIn('short-term-decider', persisted_roles)
        self.assertIn('signal-decider', persisted_roles)
        self.assertEqual(
            {failure['failure_kind'] for failure in failure_service.recorded},
            {'dependency_upstream_changed_during_dispatch'},
        )
        self.assertEqual(result['summary']['status'], 'partial')
        self.assertEqual(result['summary']['completed'], 3)
        self.assertEqual(result['summary']['failed'], 2)
        self.assertGreaterEqual(read_service.calls, 4)

    def test_finalization_failure_write_outage_keeps_invalidated_scope_in_run_summary(self):
        service = AgentFeedbackOrchestrator()
        service.client = SignalDeciderBatchClient()
        read_service = MutatingDeciderInputReadService(stable_calls=3)
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
        )
        market_context_builder = FakeMarketContextBuilder(
            {
                'status': 'ready',
                'market_date': '2026-04-14',
                'summary': {'market_bias': 'risk_on'},
                'flows': {},
                'missing_fields': [],
            }
        )

        class FailingFailureService:
            @staticmethod
            def record_failure(**_kwargs):
                raise RuntimeError('failure table unavailable')

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(
                    orchestrator_module,
                    'agent_context_reuse_service',
                    SimpleNamespace(build_latest_decisions=lambda **kwargs: {}),
                ):
                    with patch.object(
                        orchestrator_module,
                        'agent_context_feedback_service',
                        feedback_service,
                    ):
                        with patch.object(
                            orchestrator_module,
                            'agent_context_feedback_failure_service',
                            FailingFailureService(),
                        ):
                            with patch.object(
                                orchestrator_module,
                                'agent_context_feedback_run_state_service',
                                run_state_service,
                            ):
                                with patch.object(
                                    orchestrator_module,
                                    'agent_feedback_session_cleanup_service',
                                    cleanup_service,
                                ):
                                    with patch.object(
                                        orchestrator_module,
                                        'signal_decider_market_context_builder',
                                        market_context_builder,
                                    ):
                                        with _patch_feedback_eligibility('005930'):
                                            with self.assertRaisesRegex(
                                                RuntimeError,
                                                'failure table unavailable',
                                            ):
                                                service.dispatch_for_symbols(
                                                    market_date='2026-04-14',
                                                    symbols=['005930'],
                                                    reuse_existing=False,
                                                )

        persisted_roles = {
            item['agent_name']
            for call in feedback_service.bulk_calls
            for item in call
        }
        self.assertIn('short-term-decider', persisted_roles)
        self.assertIn('signal-decider', persisted_roles)
        finished = run_state_service.finished[-1]
        self.assertEqual(finished['status'], 'partial')
        self.assertEqual(finished['failed_count'], 2)
        self.assertEqual(
            finished['summary_payload']['failed_symbols_by_role'],
            {
                'short-term-decider': ['005930'],
                'signal-decider': ['005930'],
            },
        )

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

    def test_build_upstream_feedback_bundle_blocks_same_day_summary_pending_redispatch(self):
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

        self.assertEqual(
            set(upstream_summaries.keys()),
            {'eod-summarizer', 'fundamental-summarizer'},
        )
        self.assertEqual(missing_roles, ['technical-summarizer'])
        self.assertEqual(upstream_traceability['eod-summarizer']['source'], 'current_run_buffer')
        self.assertNotIn('technical-summarizer', upstream_traceability)

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
                'failure_status': 'dependency/missing-upstream',
                'failure_kind': 'dependency_missing_upstream',
                'dependency_policy': 'record_failure_and_continue',
                'missing_roles': ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'],
            },
        )
        self.assertEqual(failure_service.recorded[0]['failure_kind'], 'dependency_missing_upstream')
        self.assertIn(
            'missing_decider_upstream_summaries',
            failure_service.recorded[0]['summary']['error'],
        )

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
        self.assertNotIn('x-openclaw-model', http_calls[0]['kwargs']['headers'])
        self.assertEqual(result['request_provenance']['model'], 'openclaw/eod-summarizer')
        self.assertEqual(result['request_provenance']['contract_version'], 'agent_role_contract_v1')
        self.assertEqual(
            result['request_provenance']['prompt_sha256'],
            f"sha256:{hashlib.sha256(http_calls[0]['json']['input'].encode('utf-8')).hexdigest()}",
        )
        self.assertEqual(result['request_provenance']['prompt_chars'], len(http_calls[0]['json']['input']))
        self.assertIn('dispatch_batch_request', [entry['stage'] for entry in captured_logs])
        self.assertIn('dispatch_batch_response', [entry['stage'] for entry in captured_logs])
        mismatch_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_symbol_mismatch')
        self.assertEqual(mismatch_log['dispatch_id'], dispatch_id)
        self.assertEqual(mismatch_log['unexpected_symbols'], ['999999'])
        self.assertEqual(mismatch_log['missing_symbols'], [])
        self.assertEqual(mismatch_log['failure_class'], 'batch_response_attribution_mismatch')
        self.assertEqual(mismatch_log['session_reuse_mode'], 'slot_reused')

    def test_signal_decider_one_symbol_batch_rejects_missing_symbol_without_fallback_attribution(self):
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
            openclaw_state_dir='/nonexistent/openclaw-model-identity-test',
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
        self.assertEqual(
            http_calls[0]['kwargs']['headers']['x-openclaw-model'],
            'openai/gpt-5.6-sol',
        )
        self.assertEqual(
            result['request_provenance']['requested_model_identity']['requested_model'],
            'openai/gpt-5.6-sol',
        )
        self.assertIsNone(result['model_identity']['actual_model_ref'])
        self.assertFalse(result['model_identity']['verified'])
        self.assertNotIn('symbol', decoded_item)
        self.assertEqual(result['response_correlation']['attribution_status'], 'mismatch')
        self.assertEqual(result['response_correlation']['failure_class'], 'batch_response_attribution_mismatch')
        self.assertEqual(result['response_correlation']['missing_symbols'], ['005930'])
        self.assertEqual(result['response_correlation']['fallback_attributions'], [])
        self.assertNotIn('fallback_symbol_attribution', decoded_item['feedback']['risk_flags'])
        self.assertIn('batch_response_symbol_mismatch', [entry['stage'] for entry in captured_logs])
        self.assertNotIn('batch_response_symbol_attribution_fallback', [entry['stage'] for entry in captured_logs])

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

    def test_batch_correlation_rejects_duplicate_symbols_and_item_count_mismatch(self):
        decoded, metadata = correlate_decoded_batch_response(
            decoded={
                'items': [
                    {'symbol': '005930', 'feedback': {'symbol': '005930'}},
                    {'symbol': '005930', 'feedback': {'symbol': '005930'}},
                ]
            },
            expected_symbols=['005930'],
            session_reuse_mode='fresh_per_batch',
        )

        self.assertEqual(len(decoded['items']), 2)
        self.assertEqual(metadata['attribution_status'], 'mismatch')
        self.assertEqual(metadata['failure_class'], 'batch_response_attribution_mismatch')
        self.assertEqual(metadata['duplicate_symbols'], ['005930'])
        self.assertEqual(metadata['expected_item_count'], 1)
        self.assertEqual(metadata['response_item_count'], 2)
        self.assertFalse(metadata['item_count_matches'])

    def test_openclaw_batch_dispatch_converts_invalid_json_to_structured_fallback(self):
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

        parse_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_parse_failed')
        self.assertEqual(parse_log['failure_class'], 'invalid_json')
        self.assertEqual(parse_log['failure_mode'], 'session_history_assistant_non_json_after_marker')
        self.assertEqual(parse_log['json_failure_class'], 'json_object_not_found')
        self.assertEqual(
            parse_log['response_correlation']['response_recovery']['history']['failure_mode'],
            'session_history_assistant_non_json_after_marker',
        )
        fallback_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_structured_fallback')
        self.assertEqual(fallback_log['failure_class'], 'invalid_json')
        self.assertTrue(result['ok'])
        self.assertEqual(result['decoded']['items'][0]['symbol'], '005930')
        feedback = result['decoded']['items'][0]['feedback']
        self.assertEqual(feedback['preferred_action'], 'wait')
        self.assertEqual(feedback['next_day_open_action'], 'wait')
        self.assertEqual(feedback['wait_reason']['code'], 'core_execution_evidence_gap')
        self.assertIn('agent_json_contract_violation', feedback['risk_flags'])
        normalized_fallback = orchestrator_module.normalize_agent_response(
            role='signal-decider',
            agent_name='signal-decider',
            symbol='005930',
            market_date='2026-04-14',
            output_text=json.dumps(feedback, ensure_ascii=False),
            raw_response=feedback,
        )
        self.assertFalse(normalized_fallback['response_contract_valid'])
        self.assertEqual(normalized_fallback['next_day_open_action'], 'reject')
        self.assertIn(
            'core_generated_fallback',
            normalized_fallback['response_schema_violation_flags'],
        )
        self.assertEqual(result['response_correlation']['decoded_source'], 'core_structured_fallback')
        self.assertEqual(result['response_correlation']['missing_symbols'], [])
        self.assertNotIn('batch_response_symbol_mismatch', [entry['stage'] for entry in captured_logs])

    def test_openclaw_batch_dispatch_classifies_read_timeout(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:eod-summarizer:slot-1:batch-1'
        http_calls = []
        captured_logs = []
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
            return RaisingHTTPClient(
                exc=httpx.ReadTimeout('read timed out'),
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
                    with self.assertRaises(orchestrator_module.AgentDispatchRetryableError) as raised:
                        client.dispatch_batch(
                            spec=orchestrator_module.DISPATCH_SPECS[0],
                            market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                            batch_items=[
                                {
                                    'symbol': '005930',
                                    'market_date': '2026-04-14',
                                    'request_run_id': 'symbol-run-2026-04-14-abc123',
                                    'dispatch_id': dispatch_id,
                                    'session_slot': 1,
                                    'session_key': 'agent:eod-summarizer:feedback/eod-summarizer/slot-01',
                                    'session_reuse_mode': 'fresh_per_batch',
                                }
                            ],
                        )

        details = raised.exception.details
        self.assertEqual(details['failure_kind'], 'openclaw_transport_timeout')
        self.assertEqual(details['failure_status'], 'transport/timeout')
        self.assertEqual(details['failure_category'], 'transport')
        self.assertEqual(details['exception_type'], 'ReadTimeout')
        error_log = next(entry for entry in captured_logs if entry['stage'] == 'dispatch_batch_response_error')
        self.assertEqual(error_log['failure_kind'], 'openclaw_transport_timeout')
        self.assertEqual(error_log['failure_mode'], 'openclaw_transport_timeout')

    def test_openclaw_batch_dispatch_classifies_http_408_as_timeout(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:eod-summarizer:slot-1:batch-1'
        http_calls = []
        captured_logs = []
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
                response=FakeHTTPResponse(408, {}),
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
                    with self.assertRaises(orchestrator_module.AgentDispatchRetryableError) as raised:
                        client.dispatch_batch(
                            spec=orchestrator_module.DISPATCH_SPECS[0],
                            market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                            batch_items=[
                                {
                                    'symbol': '005930',
                                    'market_date': '2026-04-14',
                                    'request_run_id': 'symbol-run-2026-04-14-abc123',
                                    'dispatch_id': dispatch_id,
                                    'session_slot': 1,
                                    'session_key': 'agent:eod-summarizer:feedback/eod-summarizer/slot-01',
                                    'session_reuse_mode': 'fresh_per_batch',
                                }
                            ],
                        )

        details = raised.exception.details
        self.assertEqual(details['failure_kind'], 'openclaw_transport_timeout')
        self.assertEqual(details['failure_status'], 'transport/timeout')
        self.assertEqual(details['failure_category'], 'transport')
        self.assertEqual(details['response_status_code'], 408)
        error_log = next(entry for entry in captured_logs if entry['stage'] == 'dispatch_batch_response_error')
        self.assertEqual(error_log['failure_kind'], 'openclaw_transport_timeout')
        self.assertEqual(error_log['failure_mode'], 'openclaw_transport_timeout')

    def test_openclaw_batch_dispatch_classifies_non_json_http_response(self):
        dispatch_id = 'symbol-run-2026-04-14-abc123:eod-summarizer:slot-1:batch-1'
        http_calls = []
        captured_logs = []
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
                response=InvalidJSONHTTPResponse(200, '<html>gateway</html>'),
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
                    with self.assertRaises(orchestrator_module.AgentDispatchRetryableError) as raised:
                        client.dispatch_batch(
                            spec=orchestrator_module.DISPATCH_SPECS[0],
                            market_date=orchestrator_module.date.fromisoformat('2026-04-14'),
                            batch_items=[
                                {
                                    'symbol': '005930',
                                    'market_date': '2026-04-14',
                                    'request_run_id': 'symbol-run-2026-04-14-abc123',
                                    'dispatch_id': dispatch_id,
                                    'session_slot': 1,
                                    'session_key': 'agent:eod-summarizer:feedback/eod-summarizer/slot-01',
                                    'session_reuse_mode': 'fresh_per_batch',
                                }
                            ],
                        )

        details = raised.exception.details
        self.assertEqual(details['failure_kind'], 'openclaw_response_invalid_json')
        self.assertEqual(details['failure_status'], 'response/invalid-json')
        self.assertEqual(details['failure_category'], 'response')
        self.assertEqual(details['response_status_code'], 200)
        self.assertEqual(details['response_text_excerpt'], '<html>gateway</html>')
        error_log = next(entry for entry in captured_logs if entry['stage'] == 'dispatch_batch_response_error')
        self.assertEqual(error_log['failure_kind'], 'openclaw_response_invalid_json')
        self.assertEqual(error_log['failure_mode'], 'openclaw_response_invalid_json')

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

    def test_openclaw_batch_dispatch_uses_fallback_when_only_stale_trajectory_artifacts_exist(self):
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

        parse_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_parse_failed')
        self.assertEqual(parse_log['failure_mode'], 'trajectory_response_id_mismatch')
        self.assertEqual(parse_log['failure_class'], 'trajectory_response_id_mismatch')
        self.assertEqual(
            parse_log['response_correlation']['response_recovery']['trajectory']['failure_mode'],
            'trajectory_response_id_mismatch',
        )
        self.assertFalse(parse_log['response_correlation']['response_recovery']['trajectory']['matched_response_id'])
        self.assertEqual(
            parse_log['response_correlation']['response_recovery']['trajectory']['candidate_run_ids'],
            ['stale-run-id'],
        )
        fallback_log = next(entry for entry in captured_logs if entry['stage'] == 'batch_response_structured_fallback')
        self.assertEqual(fallback_log['failure_mode'], 'trajectory_response_id_mismatch')
        self.assertTrue(result['ok'])
        self.assertEqual(result['decoded']['items'][0]['symbol'], '005930')
        self.assertEqual(result['decoded']['items'][0]['feedback']['preferred_action'], 'wait')
        self.assertIn('agent_json_contract_violation', result['decoded']['items'][0]['feedback']['risk_flags'])
        self.assertEqual(result['response_correlation']['decoded_source'], 'core_structured_fallback')

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

    def test_dispatch_for_symbols_rejects_inner_identity_mismatch_without_persisting_feedback(self):
        service = AgentFeedbackOrchestrator()
        client = InnerIdentityMismatchBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        captured_logs = []
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
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

        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(len(failure_service.recorded), 1)
        self.assertEqual(failure_service.recorded[0]['failure_kind'], 'response_identity_mismatch')
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertNotIn('normalized_response_accepted', [entry['stage'] for entry in captured_logs])
        rejected_log = next(entry for entry in captured_logs if entry['stage'] == 'normalized_response_rejected')
        self.assertEqual(rejected_log['failure_class'], 'response_identity_mismatch')
        self.assertEqual(
            rejected_log['mismatch_flags'],
            ['symbol_mismatch', 'market_date_mismatch'],
        )

    def test_dispatch_for_symbols_rejects_duplicate_batch_attribution_without_persisting_feedback(self):
        service = AgentFeedbackOrchestrator()
        service.client = DuplicateAttributionBatchClient()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        captured_logs = []
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
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

        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(len(failure_service.recorded), 1)
        self.assertEqual(failure_service.recorded[0]['failure_kind'], 'batch_response_attribution_mismatch')
        self.assertEqual(result['summary']['status'], 'failed')
        rejected_log = next(entry for entry in captured_logs if entry['stage'] == 'normalized_response_rejected')
        self.assertEqual(rejected_log['failure_class'], 'batch_response_attribution_mismatch')
        self.assertEqual(rejected_log['response_correlation']['duplicate_symbols'], ['005930'])
        self.assertFalse(rejected_log['response_correlation']['item_count_matches'])

    def test_dispatch_for_symbols_rejects_missing_response_correlation_before_persistence(self):
        service = AgentFeedbackOrchestrator()
        service.client = MissingCorrelationBatchClient()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
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

        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 1)
        self.assertEqual(
            failure_service.recorded[0]['failure_kind'],
            'batch_response_correlation_missing',
        )
        self.assertEqual(len(cleanup_service.targets), 1)

    def test_dispatch_for_symbols_rejects_whole_batch_when_one_feedback_is_missing(self):
        service = AgentFeedbackOrchestrator()
        service.client = MissingFeedbackBatchClient()
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=2, openclaw_agent_parallel_subtasks=1)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            with _patch_feedback_eligibility('005930', '000660'):
                                result = service.dispatch_for_symbols(
                                    market_date='2026-04-14',
                                    symbols=['005930', '000660'],
                                    roles=['eod-summarizer'],
                                    reuse_existing=False,
                                )

        self.assertEqual(feedback_service.bulk_calls, [])
        self.assertEqual(result['summary']['completed'], 0)
        self.assertEqual(result['summary']['failed'], 2)
        self.assertEqual(len(failure_service.recorded), 2)
        self.assertEqual(
            {failure['failure_kind'] for failure in failure_service.recorded},
            {'response_contract_invalid:feedback_missing'},
        )
        self.assertEqual(len(cleanup_service.targets), 1)

    def test_dispatch_for_symbols_times_out_hung_batch_and_finishes(self):
        service = AgentFeedbackOrchestrator()
        client = HangingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        failure_service = RecordingFailureService()
        cleanup_service = FakeSessionCleanupService()
        run_state_service = RecordingRunStateService()
        captured_logs = []
        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
            openclaw_agent_subtask_poll_seconds=0.01,
        )

        started_at = time.monotonic()
        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_feedback_run_state_service', run_state_service):
                with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_context_feedback_failure_service', failure_service):
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

        self.assertLess(time.monotonic() - started_at, 5)
        self.assertEqual(result['summary']['status'], 'failed')
        self.assertEqual(result['summary']['failed'], 1)
        self.assertEqual(len(failure_service.recorded), 1)
        self.assertEqual(failure_service.recorded[0]['failure_kind'], 'openclaw_transport_timeout')
        final_log = next(entry for entry in captured_logs if entry['stage'] == 'run_finished')
        self.assertEqual(final_log['terminal_status'], 'failed')
        self.assertEqual(run_state_service.finished[-1]['status'], 'failed')

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
