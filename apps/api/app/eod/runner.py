from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from app.eod.master import refresh_master_if_due
from app.eod.service import EOD_SERVICE
from app.market.clock import get_market_state

logger = logging.getLogger(__name__)


class EODBackgroundRunner:
    def __init__(self, interval_seconds: int = 60) -> None:
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name='eod-background-runner')

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                market = get_market_state()
                if market.state == 'PREOPEN':
                    refresh_master_if_due()
                elif market.state == 'CLOSED':
                    await EOD_SERVICE.capture_if_due(trigger='background')
            except Exception as exc:
                logger.exception('Background runner loop failed: %s', exc)
            await asyncio.sleep(self.interval_seconds)


background_runner = EODBackgroundRunner()
