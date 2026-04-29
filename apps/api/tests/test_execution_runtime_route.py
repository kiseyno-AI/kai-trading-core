import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_execution_runtime


class FakeExecRuntimeService:
    def __init__(self) -> None:
        self.status_calls: list[dict] = []
        self.monitored_calls: list[dict] = []
        self.board_calls: list[dict] = []
        self.detail_calls: list[dict] = []

    async def dashboard_runtime_status(self, **kwargs):
        self.status_calls.append(kwargs)
        return {
            'status': 'ready',
            'mode': 'mock',
            'exec_connected': True,
            'automation_enabled': True,
            'paused': False,
            'kill_switch_enabled': False,
            'monitoring_alive': True,
            'last_heartbeat_at': '2026-04-30T00:05:00+00:00',
            'open_order_count': 1,
            'active_execution_job_count': 1,
            'issues': [],
        }

    async def list_runtime_monitored_symbols(self, **kwargs):
        self.monitored_calls.append(kwargs)
        return {
            'market_date': '2026-04-30',
            'runtime': {'status': 'ready'},
            'summary': {'items_total': 1},
            'items': [{'symbol': '005930', 'monitoring_state': 'watching_target_stop'}],
        }

    async def get_runtime_daily_board(self, **kwargs):
        self.board_calls.append(kwargs)
        return {
            'market_date': '2026-04-30',
            'runtime': {'status': 'ready'},
            'summary': {'items_total': 1, 'filled': 1},
            'items': [{'symbol': '005930', 'status': 'filled'}],
        }

    async def get_runtime_symbol_detail(self, **kwargs):
        self.detail_calls.append(kwargs)
        return {
            'market_date': '2026-04-30',
            'symbol': kwargs['symbol'],
            'runtime': {'status': 'ready'},
            'summary': {'items_total': 1},
            'items': [{'symbol': kwargs['symbol'], 'status': 'filled'}],
        }


class ExecutionRuntimeRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_execution_runtime.router)
        self.service = FakeExecRuntimeService()
        self.original_service = routes_execution_runtime.signal_decider_exec_integration_service
        routes_execution_runtime.signal_decider_exec_integration_service = self.service

    def tearDown(self) -> None:
        routes_execution_runtime.signal_decider_exec_integration_service = self.original_service

    def test_execution_runtime_routes_use_exec_integration_service(self) -> None:
        client = TestClient(self.app)

        status_response = client.get('/execution/runtime/status')
        monitored_response = client.get(
            '/execution/runtime/monitored-symbols',
            params={'market_date': '2026-04-30', 'refresh': 'false'},
        )
        board_response = client.get(
            '/execution/runtime/daily-board',
            params={'market_date': '2026-04-30'},
        )
        detail_response = client.get(
            '/execution/runtime/symbols/005930',
            params={'market_date': '2026-04-30', 'refresh': 'true'},
        )

        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_response.json()['status'], 'ready')
        self.assertEqual(monitored_response.status_code, 200)
        self.assertEqual(monitored_response.json()['items'][0]['symbol'], '005930')
        self.assertEqual(board_response.status_code, 200)
        self.assertEqual(board_response.json()['summary']['filled'], 1)
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(detail_response.json()['symbol'], '005930')

        self.assertTrue(self.service.status_calls[0]['refresh'])
        self.assertEqual(self.service.monitored_calls[0]['market_date'].isoformat(), '2026-04-30')
        self.assertFalse(self.service.monitored_calls[0]['refresh'])
        self.assertEqual(self.service.board_calls[0]['market_date'].isoformat(), '2026-04-30')
        self.assertEqual(self.service.detail_calls[0]['symbol'], '005930')
        self.assertTrue(self.service.detail_calls[0]['refresh'])


if __name__ == '__main__':
    unittest.main()
