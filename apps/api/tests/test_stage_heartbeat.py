from __future__ import annotations

import threading
from unittest import TestCase

from app.eod.stage_heartbeat import PeriodicStageHeartbeat


class PeriodicStageHeartbeatTests(TestCase):
    def test_periodic_heartbeat_runs_and_stops_its_thread_cleanly(self) -> None:
        called = threading.Event()

        def heartbeat() -> bool:
            called.set()
            return True

        pulse = PeriodicStageHeartbeat(
            heartbeat,
            interval_seconds=0.01,
            thread_name='test-stage-heartbeat',
        )

        with pulse:
            self.assertTrue(called.wait(timeout=0.5))
            self.assertTrue(pulse.is_running)

        self.assertFalse(pulse.is_running)
        self.assertFalse(pulse.ownership_lost)
        self.assertFalse(
            any(
                thread.name == 'test-stage-heartbeat' and thread.is_alive()
                for thread in threading.enumerate()
            )
        )

    def test_false_heartbeat_result_records_ownership_loss(self) -> None:
        called = threading.Event()

        def heartbeat() -> bool:
            called.set()
            return False

        pulse = PeriodicStageHeartbeat(
            heartbeat,
            interval_seconds=0.01,
            thread_name='test-stage-ownership-loss',
        )

        with pulse:
            self.assertTrue(called.wait(timeout=0.5))

        self.assertTrue(pulse.ownership_lost)
        self.assertFalse(pulse.is_running)

    def test_transient_heartbeat_error_does_not_claim_ownership_loss(self) -> None:
        recovered = threading.Event()
        attempts = 0

        def heartbeat() -> bool:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError('temporary heartbeat write timeout')
            recovered.set()
            return True

        pulse = PeriodicStageHeartbeat(
            heartbeat,
            interval_seconds=0.01,
            thread_name='test-stage-heartbeat-retry',
        )

        with pulse:
            self.assertTrue(recovered.wait(timeout=0.5))

        self.assertGreaterEqual(attempts, 2)
        self.assertFalse(pulse.ownership_lost)
