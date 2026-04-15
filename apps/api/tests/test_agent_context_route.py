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
