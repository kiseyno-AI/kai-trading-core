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
