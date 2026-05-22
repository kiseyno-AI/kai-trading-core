from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import datetime, time

from app.core.settings import settings
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.master import refresh_master_if_due
from app.market.clock import get_market_state
from app.market.snapshot import market_snapshot_service
from app.orders.exec_integration import signal_decider_exec_integration_service
from app.orders.intraday_auto_sell import signal_decider_intraday_auto_sell_service
from app.orders.next_open_execution import signal_decider_next_open_execution_service
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.reconciliation import trading_order_reconciliation_service

logger = logging.getLogger(__name__)
PREOPEN_EXEC_DISPATCH_TIME_KST = time(8, 0)
BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS = 45


def is_preopen_exec_dispatch_due(market: object) -> bool:
    if getattr(market, 'state', None) != 'PREOPEN' or not bool(getattr(market, 'is_trading_day', False)):
        return False
    server_time = getattr(market, 'server_time', None)
    if not server_time:
        return False
    try:
        parsed = datetime.fromisoformat(str(server_time))
    except ValueError:
        return False
    return parsed.time() >= PREOPEN_EXEC_DISPATCH_TIME_KST


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

    async def _run_blocking_step(self, name: str, func) -> None:
        try:
            await asyncio.wait_for(asyncio.to_thread(func), timeout=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(
                'Background runner blocking step timed out after %ss: %s',
                BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
                name,
            )

    async def _run_async_step(self, name: str, awaitable) -> None:
        try:
            await asyncio.wait_for(awaitable, timeout=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(
                'Background runner async step timed out after %ss: %s',
                BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
                name,
            )

    async def _run_loop(self) -> None:
        while self._running:
            try:
                market = get_market_state()
                if market.state == 'PREOPEN':
                    await self._run_blocking_step('refresh_master_if_due', refresh_master_if_due)
                    await self._run_blocking_step(
                        'signal_decider_next_open_execution_refresh_plan_if_due',
                        signal_decider_next_open_execution_service.refresh_plan_if_due,
                    )
                    if (
                        signal_decider_exec_integration_service.should_delegate_execution()
                        and is_preopen_exec_dispatch_due(market)
                    ):
                        await self._run_async_step(
                            'signal_decider_exec_dispatch_next_open_plan',
                            signal_decider_exec_integration_service.dispatch_next_open_plan(
                                target_trade_date=market.server_time[:10],
                                refresh_plan=False,
                                trigger='preopen_exec_dispatch',
                            ),
                        )
                elif market.state == 'OPEN':
                    if signal_decider_exec_integration_service.should_delegate_execution():
                        await self._run_async_step(
                            'signal_decider_exec_readback_refresh',
                            signal_decider_exec_integration_service.list_signal_decider_exec_overview(
                                target_trade_date=market.server_time[:10],
                                refresh=True,
                                trigger='market_open_scheduler',
                            ),
                        )
                    else:
                        await self._run_async_step(
                            'signal_decider_next_open_execution_run_if_due',
                            signal_decider_next_open_execution_service.run_market_open_execution_if_due(
                                trigger='market_open_scheduler'
                            ),
                        )
                        await self._run_async_step(
                            'signal_decider_intraday_auto_sell_run_if_due',
                            signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due(
                                trigger='market_open_scheduler'
                            ),
                        )
                elif market.state == 'CLOSED':
                    await self._run_async_step(
                        'auto_pykrx_schedule_daily_pipeline_if_due',
                        auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due(),
                    )
                await self._run_async_step(
                    'trading_order_reconciliation_periodic',
                    trading_order_reconciliation_service.run_periodic_if_due(),
                )
                await self._run_async_step(
                    'market_snapshot_refresh_if_due',
                    market_snapshot_service.refresh_market_snapshot_if_due(
                        trigger=f'background_runner:{market.state.lower()}'
                    ),
                )
                await self._run_async_step(
                    'portfolio_summary_refresh_if_due',
                    portfolio_summary_service.refresh_portfolio_summary_if_due(
                        trigger=f'background_runner:{market.state.lower()}'
                    ),
                )
            except Exception as exc:
                logger.exception('Background runner loop failed: %s', exc)
            await asyncio.sleep(self.interval_seconds)


background_runner = EODBackgroundRunner(interval_seconds=settings.eod_auto_pykrx_poll_seconds)
