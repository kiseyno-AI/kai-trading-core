from datetime import date
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_agent_context
from app.agent_context.runtime_state import RuntimeFeedbackStateStore


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

    def run_retry_cycles(self, *, config):
        self.last_config = config
        return {
            'market_date': config.market_date.isoformat(),
            'requested_symbols': list(config.symbols) if config.symbols is not None else None,
            'roles': list(config.roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'reuse_existing': config.reuse_existing,
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


class FakeSignalDeciderLoopService:
    def __init__(self):
        self.decision_args = None
        self.detail_args = None
        self.diagnostics_args = None
        self.aggregate_diagnostics_args = None
        self.compute_args = None
        self.evaluation_args = None
        self.candidate_generate_args = None
        self.candidate_list_args = None
        self.policy_list_args = None
        self.policy_detail_args = None
        self.policy_activate_args = None

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
                    'evaluation_summary': {'evaluated': True},
                }
            ],
        }

    def get_decision_detail(self, **kwargs):
        self.detail_args = kwargs
        return {
            'request': {'snapshot_id': kwargs['snapshot_id']},
            'summary': {'evaluated_count': 1},
            'item': {'snapshot_id': kwargs['snapshot_id'], 'evaluation_summary': {'evaluated': True}},
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
            'summary': {'count': 1},
            'items': [{'feedback_id': 11, 'symbol': kwargs['symbol'] or '005930', 'evaluations': [{'horizon_days': 5}]}],
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


class AgentContextRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_agent_context.router)

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
                ],
            )
        finally:
            routes_agent_context.agent_context_read_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.last_args['fs_div'], 'CFS')
        self.assertEqual(fake_service.last_args['disclosures_limit'], 3)
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
                    'max_cycles': 2,
                    'outer_batch_size': 10,
                    'reuse_existing': False,
                },
            )
        finally:
            routes_agent_context.agent_feedback_retry_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_config.market_date.isoformat(), '2026-04-14')
        self.assertEqual(fake_service.last_config.symbols, ('005930',))
        self.assertEqual(fake_service.last_config.roles, ('technical-summarizer',))
        self.assertEqual(fake_service.last_config.max_cycles, 2)
        self.assertEqual(fake_service.last_config.outer_batch_size, 10)
        self.assertFalse(fake_service.last_config.reuse_existing)
        self.assertEqual(response.json()['requested_symbols'], ['005930'])
        summary = response.json()['progress']['summary']
        self.assertEqual(summary['first_feedback_at'], '2026-04-14T01:00:00+00:00')
        self.assertEqual(summary['completed_feedback_per_minute'], 0.6)
        self.assertEqual(summary['estimated_completion_at'], '2026-04-14T01:05:00+00:00')

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
            'requested_symbols_intersected_with_active_pre_filters',
        )
        self.assertEqual(
            fake_run_state_service.started[0]['request_payload']['eligibility_filters'],
            ['market_risk_filter', 'piotroski_filter'],
        )
        self.assertEqual(
            payload['request']['scope'],
            'requested_symbols_intersected_with_active_pre_filters',
        )
        self.assertEqual(fake_orchestrator.calls[0]['request_run_id'], request_run_id)
        self.assertEqual(fake_orchestrator.calls[0]['symbols'], ['005930', '000660'])
        self.assertEqual(fake_orchestrator.calls[0]['roles'], ['eod-summarizer', 'technical-summarizer'])

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
        original_service = routes_agent_context.signal_decider_loop_service
        routes_agent_context.signal_decider_loop_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/agent-context/signal-decider/decision-policies/signal_decider_policy_v2/activate',
                json={
                    'activated_by': 'human.reviewer',
                    'activation_note': 'Approved after diagnostics review.',
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
            },
        )
        self.assertEqual(response.json()['item']['status'], 'active')
