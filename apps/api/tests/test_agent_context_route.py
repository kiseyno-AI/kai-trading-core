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
                    'symbols': None,
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
        self.assertEqual(fake_service.last_config.roles, ('technical-summarizer',))
        self.assertEqual(fake_service.last_config.max_cycles, 2)
        self.assertEqual(fake_service.last_config.outer_batch_size, 10)
        self.assertFalse(fake_service.last_config.reuse_existing)
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
