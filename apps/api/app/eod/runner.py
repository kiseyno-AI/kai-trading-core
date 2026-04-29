from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from app.core.settings import settings
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.master import refresh_master_if_due
from app.market.clock import get_market_state
from app.orders.intraday_auto_sell import signal_decider_intraday_auto_sell_service
from app.orders.next_open_execution import signal_decider_next_open_execution_service
from app.orders.reconciliation import trading_order_reconciliation_service

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
        await auto_pykrx_eod_scheduler.stop()
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
                    signal_decider_next_open_execution_service.refresh_plan_if_due()
                elif market.state == 'OPEN':
                    await signal_decider_next_open_execution_service.run_market_open_execution_if_due(
                        trigger='market_open_scheduler'
                    )
                    await signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due(
                        trigger='market_open_scheduler'
                    )
                elif market.state == 'CLOSED':
                    await auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due()
                await trading_order_reconciliation_service.run_periodic_if_due()
            except Exception as exc:
                logger.exception('Background runner loop failed: %s', exc)
            await asyncio.sleep(self.interval_seconds)


background_runner = EODBackgroundRunner(interval_seconds=settings.eod_auto_pykrx_poll_seconds)
