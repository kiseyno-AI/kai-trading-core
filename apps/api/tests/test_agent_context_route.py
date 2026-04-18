import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_agent_context


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
