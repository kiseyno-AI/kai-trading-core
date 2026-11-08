"""HTTP run-if-due routes must not lift server-side dry-run / enable settings."""

from __future__ import annotations

import unittest
from datetime import date
from typing import Any
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_trading
from app.api.routes_trading import _bounded_run_request
from app.core.settings import settings


class _NextOpenService:
    def __init__(self) -> None:
        self.run_calls: list[dict[str, Any]] = []

    async def _resolve_target_trade_date_async(self, target: date | None) -> date:
        return target or date(2026, 4, 30)

    async def run_market_open_execution_if_due(self, **kwargs: Any) -> dict[str, Any]:
        self.run_calls.append(kwargs)
        return {'summary': {'submitted_count': 0}}


class _IntradayService:
    def __init__(self) -> None:
        self.run_calls: list[dict[str, Any]] = []

    async def run_intraday_auto_sell_if_due(self, **kwargs: Any) -> dict[str, Any]:
        self.run_calls.append(kwargs)
        return {'summary': {'submitted_count': 0}}


class _ExecService:
    def should_delegate_execution(self) -> bool:
        return False

    def should_manage_delegated_authority(self, **_kwargs: Any) -> bool:
        return False


class BoundedRunRequestTests(unittest.TestCase):
    def _bound(self, **overrides: Any) -> tuple[bool | None, bool]:
        params: dict[str, Any] = {
            'route': 'test',
            'requested_dry_run': None,
            'requested_force': False,
            'server_dry_run': True,
            'server_enabled': False,
        }
        params.update(overrides)
        return _bounded_run_request(**params)

    def test_server_dry_run_cannot_be_lifted_by_the_request(self) -> None:
        self.assertEqual(self._bound(requested_dry_run=False, server_dry_run=True), (True, False))

    def test_request_may_still_ask_for_a_dry_run(self) -> None:
        self.assertEqual(
            self._bound(requested_dry_run=True, server_dry_run=False, server_enabled=True),
            (True, False),
        )

    def test_unset_dry_run_is_left_to_the_server_setting(self) -> None:
        self.assertEqual(self._bound(requested_dry_run=None, server_dry_run=True)[0], None)
        self.assertEqual(
            self._bound(requested_dry_run=None, server_dry_run=False, server_enabled=True)[0],
            None,
        )

    def test_force_cannot_bypass_a_disabled_server_for_a_live_run(self) -> None:
        self.assertEqual(
            self._bound(
                requested_dry_run=False,
                requested_force=True,
                server_dry_run=False,
                server_enabled=False,
            ),
            (False, False),
        )
        # An unset dry_run on a live-configured but disabled server is also a live run.
        self.assertEqual(
            self._bound(
                requested_dry_run=None,
                requested_force=True,
                server_dry_run=False,
                server_enabled=False,
            ),
            (None, False),
        )

    def test_force_is_preserved_when_the_run_is_a_dry_run_or_the_server_is_enabled(self) -> None:
        self.assertEqual(
            self._bound(requested_force=True, server_dry_run=True, server_enabled=False),
            (None, True),
        )
        self.assertEqual(
            self._bound(
                requested_dry_run=False,
                requested_force=True,
                server_dry_run=False,
                server_enabled=True,
            ),
            (False, True),
        )


class RunIfDueRouteBoundsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_trading.router)
        self.next_open = _NextOpenService()
        self.intraday = _IntradayService()
        self._patches = [
            patch.object(routes_trading, 'signal_decider_next_open_execution_service', self.next_open),
            patch.object(routes_trading, 'signal_decider_intraday_auto_sell_service', self.intraday),
            patch.object(routes_trading, 'signal_decider_exec_integration_service', _ExecService()),
        ]
        for item in self._patches:
            item.start()
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        for item in self._patches:
            item.stop()

    def test_next_open_route_keeps_a_dry_run_server_in_dry_run(self) -> None:
        with patch.object(settings, 'signal_decider_next_open_dry_run', True), patch.object(
            settings, 'signal_decider_next_open_enabled', True
        ):
            response = self.client.post(
                '/trading/orders/next-open-execution/run-if-due',
                json={'dry_run': False, 'force': True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIs(self.next_open.run_calls[0]['dry_run'], True)
        self.assertIs(self.next_open.run_calls[0]['force'], True)

    def test_next_open_route_force_cannot_bypass_a_disabled_live_server(self) -> None:
        with patch.object(settings, 'signal_decider_next_open_dry_run', False), patch.object(
            settings, 'signal_decider_next_open_enabled', False
        ):
            response = self.client.post(
                '/trading/orders/next-open-execution/run-if-due',
                json={'dry_run': False, 'force': True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIs(self.next_open.run_calls[0]['dry_run'], False)
        self.assertIs(self.next_open.run_calls[0]['force'], False)

    def test_intraday_route_keeps_a_dry_run_server_in_dry_run(self) -> None:
        with patch.object(settings, 'signal_decider_intraday_auto_sell_dry_run', True), patch.object(
            settings, 'signal_decider_intraday_auto_sell_enabled', False
        ):
            response = self.client.post(
                '/trading/orders/intraday-auto-sell/run-if-due',
                json={'dry_run': False, 'force': True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIs(self.intraday.run_calls[0]['dry_run'], True)
        self.assertIs(self.intraday.run_calls[0]['force'], True)

    def test_intraday_route_force_cannot_bypass_the_enabled_switch_for_a_live_run(self) -> None:
        with patch.object(settings, 'signal_decider_intraday_auto_sell_dry_run', False), patch.object(
            settings, 'signal_decider_intraday_auto_sell_enabled', False
        ):
            response = self.client.post(
                '/trading/orders/intraday-auto-sell/run-if-due',
                json={'dry_run': False, 'force': True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIs(self.intraday.run_calls[0]['force'], False)

    def test_operator_override_still_works_when_the_server_is_configured_live(self) -> None:
        with patch.object(settings, 'signal_decider_intraday_auto_sell_dry_run', False), patch.object(
            settings, 'signal_decider_intraday_auto_sell_enabled', True
        ):
            response = self.client.post(
                '/trading/orders/intraday-auto-sell/run-if-due',
                json={'dry_run': False, 'force': True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIs(self.intraday.run_calls[0]['dry_run'], False)
        self.assertIs(self.intraday.run_calls[0]['force'], True)


if __name__ == '__main__':
    unittest.main()
