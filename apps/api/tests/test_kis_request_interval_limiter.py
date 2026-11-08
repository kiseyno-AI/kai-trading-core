from __future__ import annotations

import unittest
from unittest.mock import patch

from app.brokers.kis import client as kis_client_module
from app.brokers.kis.client import KISClient
from app.core.settings import Settings, settings


class KISRequestIntervalLimiterTests(unittest.IsolatedAsyncioTestCase):
    def test_limiter_is_off_by_default(self) -> None:
        self.assertEqual(Settings.model_fields['kis_min_request_interval_seconds'].default, 0.0)
        client = KISClient(use_mock=True)
        with patch.object(settings, 'kis_min_request_interval_seconds', 0.0):
            self.assertEqual([client._reserve_request_slot_delay() for _ in range(3)], [0.0, 0.0, 0.0])

    def test_slots_are_spaced_by_the_configured_interval(self) -> None:
        client = KISClient(use_mock=True)
        with patch.object(settings, 'kis_min_request_interval_seconds', 0.5), patch.object(
            kis_client_module.time, 'monotonic', side_effect=[100.0, 100.0, 100.0, 101.9]
        ):
            delays = [client._reserve_request_slot_delay() for _ in range(4)]

        # Three back-to-back callers queue at 0.0 / 0.5 / 1.0s; a caller that
        # arrives after the queue drained (101.9 > 101.5) waits 0.
        self.assertEqual(delays, [0.0, 0.5, 1.0, 0.0])

    async def test_wait_sleeps_only_when_a_delay_is_reserved(self) -> None:
        client = KISClient(use_mock=True)
        slept: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        with patch.object(settings, 'kis_min_request_interval_seconds', 0.25), patch.object(
            kis_client_module.asyncio, 'sleep', fake_sleep
        ), patch.object(kis_client_module.time, 'monotonic', side_effect=[10.0, 10.0]):
            await client._wait_for_request_slot()
            await client._wait_for_request_slot()

        self.assertEqual(slept, [0.25])


if __name__ == '__main__':
    unittest.main()
