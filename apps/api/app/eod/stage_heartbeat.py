from __future__ import annotations

import logging
import threading
from types import TracebackType
from typing import Callable


logger = logging.getLogger(__name__)


class PeriodicStageHeartbeat:
    """Run a bounded ownership heartbeat while synchronous stage work blocks."""

    def __init__(
        self,
        heartbeat: Callable[[], bool],
        *,
        interval_seconds: float,
        thread_name: str,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError('interval_seconds must be positive')
        self._heartbeat = heartbeat
        self._interval_seconds = float(interval_seconds)
        self._thread_name = thread_name
        self._stop_event = threading.Event()
        self._ownership_lost_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def ownership_lost(self) -> bool:
        return self._ownership_lost_event.is_set()

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def __enter__(self) -> PeriodicStageHeartbeat:
        if self._thread is not None:
            raise RuntimeError('stage heartbeat cannot be started twice')
        self._thread = threading.Thread(
            target=self._run,
            name=self._thread_name,
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join()

    def check_now(self) -> bool:
        """Confirm ownership once without treating a transient write error as loss."""

        if self.ownership_lost:
            return False
        try:
            still_owned = self._heartbeat()
        except Exception as exc:  # pragma: no cover - log assertion is not useful
            logger.warning(
                'Stage heartbeat failed transiently; retaining ownership until '
                'the state store explicitly rejects it: %s',
                exc,
            )
            return True
        if still_owned is False:
            self._ownership_lost_event.set()
            return False
        return True

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            if not self.check_now():
                return
