from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from app.core.settings import settings
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.master import refresh_master_if_due
from app.market.clock import async_get_market_state
from app.market.snapshot import market_snapshot_service
from app.orders.exec_integration import signal_decider_exec_integration_service
from app.orders.intraday_auto_sell import signal_decider_intraday_auto_sell_service
from app.orders.next_open_execution import signal_decider_next_open_execution_service
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.reconciliation import trading_order_reconciliation_service

logger = logging.getLogger(__name__)
PREOPEN_EXEC_DISPATCH_TIME_KST = time(8, 0)
BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS = 45
NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS = 120
BACKGROUND_RUNNER_TIMEOUT_COOLDOWN_SECONDS = 600
MARKET_SNAPSHOT_OPEN_DRIVE_START_KST = time(9, 0)
MARKET_SNAPSHOT_OPEN_DRIVE_END_EXCLUSIVE_KST = time(14, 31)
NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY = 'signal_decider_next_open_execution_refresh_plan_if_due'
OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY = 'signal_decider_exec_dispatch_open_plan'
BUY_AUTHORITY_REVOCATION_CONSECUTIVE_FAILURE_THRESHOLD = 3
SEOUL_TZ = ZoneInfo('Asia/Seoul')


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
        self._step_timeout_cooldowns: dict[str, datetime] = {}
        self._blocking_step_tasks: dict[str, asyncio.Task] = {}
        self._async_step_tasks: dict[str, asyncio.Task] = {}
        self._last_open_drive_market_snapshot_refresh_at: datetime | None = None
        self._buy_authority_critical_failures: dict[str, int] = {}
        self._started_at: datetime | None = None
        self._stopped_at: datetime | None = None
        self._last_tick_started_at: datetime | None = None
        self._last_tick_completed_at: datetime | None = None
        self._last_tick_succeeded_at: datetime | None = None
        self._last_tick_error: dict[str, str] | None = None
        self._tick_count = 0

    def snapshot(self, *, now: datetime | None = None) -> dict:
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None or current.utcoffset() is None:
            raise ValueError('runner snapshot timestamp must be timezone-aware')
        heartbeat_at = (
            self._last_tick_completed_at
            or self._last_tick_started_at
            or self._started_at
        )
        heartbeat_age_seconds = (
            max(int((current - heartbeat_at).total_seconds()), 0)
            if heartbeat_at is not None
            else None
        )
        stale_after_seconds = max(int(self.interval_seconds) * 3, 180)
        is_stale = bool(
            self._running
            and heartbeat_age_seconds is not None
            and heartbeat_age_seconds > stale_after_seconds
        )
        enabled = bool(
            settings.background_runner_enabled
            or settings.eod_auto_pykrx_enabled
        )
        if not enabled:
            status = 'disabled'
        elif is_stale:
            status = 'stale'
        elif self._running:
            status = 'degraded' if self._last_tick_error else 'running'
        else:
            status = 'stopped'
        return {
            'enabled': enabled,
            'runtime_background_enabled': settings.background_runner_enabled,
            'daily_pipeline_enabled': settings.eod_auto_pykrx_enabled,
            'status': status,
            'running': self._running,
            'task_alive': bool(self._task is not None and not self._task.done()),
            'interval_seconds': self.interval_seconds,
            'started_at': self._started_at.isoformat() if self._started_at else None,
            'stopped_at': self._stopped_at.isoformat() if self._stopped_at else None,
            'heartbeat_at': heartbeat_at.isoformat() if heartbeat_at else None,
            'last_tick': {
                'count': self._tick_count,
                'started_at': (
                    self._last_tick_started_at.isoformat()
                    if self._last_tick_started_at
                    else None
                ),
                'completed_at': (
                    self._last_tick_completed_at.isoformat()
                    if self._last_tick_completed_at
                    else None
                ),
                'succeeded_at': (
                    self._last_tick_succeeded_at.isoformat()
                    if self._last_tick_succeeded_at
                    else None
                ),
                'error': self._last_tick_error,
            },
            'staleness': {
                'age_seconds': heartbeat_age_seconds,
                'stale_after_seconds': stale_after_seconds,
                'is_stale': is_stale,
            },
            'in_flight': {
                'blocking_steps': sorted(
                    name
                    for name, task in self._blocking_step_tasks.items()
                    if not task.done()
                ),
                'async_steps': sorted(
                    name
                    for name, task in self._async_step_tasks.items()
                    if not task.done()
                ),
            },
            'timeout_cooldowns': {
                name: expires_at.isoformat()
                for name, expires_at in sorted(self._step_timeout_cooldowns.items())
                if expires_at > current
            },
        }

    @staticmethod
    def _open_drive_market_time(market: object) -> datetime | None:
        if (
            str(getattr(market, 'market', '') or '').strip().upper() != 'KRX'
            or str(getattr(market, 'state', '') or '').strip().upper() != 'OPEN'
            or not bool(getattr(market, 'is_trading_day', False))
        ):
            return None
        server_time = getattr(market, 'server_time', None)
        if not server_time:
            return None
        try:
            parsed = datetime.fromisoformat(str(server_time).replace('Z', '+00:00'))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        current = parsed.astimezone(SEOUL_TZ)
        local_time = current.time().replace(tzinfo=None)
        if not (
            MARKET_SNAPSHOT_OPEN_DRIVE_START_KST
            <= local_time
            < MARKET_SNAPSHOT_OPEN_DRIVE_END_EXCLUSIVE_KST
        ):
            return None
        return current

    def _next_poll_interval_seconds(self, market: object | None) -> int:
        if self._open_drive_market_time(market) is None:
            return self.interval_seconds
        return min(
            self.interval_seconds,
            settings.market_snapshot_open_drive_refresh_interval_seconds,
        )

    def _claim_market_snapshot_open_drive_refresh(self, market: object) -> bool:
        current = self._open_drive_market_time(market)
        if current is None:
            return False

        interval_seconds = settings.market_snapshot_open_drive_refresh_interval_seconds
        window_start = current.replace(hour=9, minute=0, second=0, microsecond=0)
        elapsed_seconds = int((current - window_start).total_seconds())
        bucket_started_at = window_start + timedelta(
            seconds=(elapsed_seconds // interval_seconds) * interval_seconds
        )
        previous_bucket = self._last_open_drive_market_snapshot_refresh_at
        if (
            previous_bucket is not None
            and previous_bucket.date() == current.date()
            and bucket_started_at <= previous_bucket
        ):
            return False
        self._last_open_drive_market_snapshot_refresh_at = bucket_started_at
        return True

    def _step_in_timeout_cooldown(self, name: str) -> bool:
        cooldown_until = self._step_timeout_cooldowns.get(name)
        if cooldown_until is None:
            return False
        now = datetime.now(timezone.utc)
        if cooldown_until <= now:
            self._step_timeout_cooldowns.pop(name, None)
            return False
        logger.info(
            'Background runner skipping step during timeout cooldown: %s until %s',
            name,
            cooldown_until.isoformat(),
        )
        return True

    def _mark_step_timed_out(self, name: str) -> None:
        self._step_timeout_cooldowns[name] = datetime.now(timezone.utc) + timedelta(
            seconds=BACKGROUND_RUNNER_TIMEOUT_COOLDOWN_SECONDS
        )

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._started_at = datetime.now(timezone.utc)
        self._stopped_at = None
        self._task = asyncio.create_task(self._run_loop(), name='eod-background-runner')

    async def stop(self) -> None:
        self._running = False
        self._stopped_at = datetime.now(timezone.utc)
        await auto_pykrx_eod_scheduler.stop()
        # Cancelling an asyncio.to_thread wrapper cannot stop its worker thread.
        # Keep the task registered until its callback observes real completion so
        # a same-process restart cannot launch a duplicate critical refresh.
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run_blocking_step(
        self,
        name: str,
        func,
        *,
        fail_closed: bool = False,
        timeout_seconds: float | None = None,
    ) -> None:
        step_timeout_seconds = (
            BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS
            if timeout_seconds is None
            else timeout_seconds
        )
        running_task = self._blocking_step_tasks.get(name)
        if running_task is not None and not running_task.done():
            logger.info('Background runner blocking step already in flight: %s', name)
            if fail_closed:
                raise RuntimeError(f'blocking step still in flight: {name}')
            return
        if running_task is not None:
            self._blocking_step_tasks.pop(name, None)
        if self._step_in_timeout_cooldown(name):
            if fail_closed:
                raise RuntimeError(f'blocking step is in timeout cooldown: {name}')
            return
        task = asyncio.create_task(asyncio.to_thread(func), name=f'eod-background-blocking:{name}')
        self._blocking_step_tasks[name] = task
        task.add_done_callback(lambda completed, step_name=name: self._finalize_blocking_step(step_name, completed))
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=step_timeout_seconds,
            )
        except asyncio.TimeoutError:
            self._mark_step_timed_out(name)
            logger.warning(
                'Background runner blocking step still running after %ss: %s; keeping single-flight lock and cooling down for %ss',
                step_timeout_seconds,
                name,
                BACKGROUND_RUNNER_TIMEOUT_COOLDOWN_SECONDS,
            )
            if fail_closed:
                raise RuntimeError(f'blocking step timed out: {name}')

    def _finalize_blocking_step(self, name: str, task: asyncio.Task) -> None:
        if self._blocking_step_tasks.get(name) is task:
            self._blocking_step_tasks.pop(name, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning('Background runner blocking step failed: %s: %s', name, exc)
        else:
            self._step_timeout_cooldowns.pop(name, None)

    async def _run_async_step(
        self,
        name: str,
        awaitable,
        *,
        fail_closed: bool = False,
    ) -> bool:
        running_task = self._async_step_tasks.get(name)
        if running_task is not None and not running_task.done():
            with suppress(Exception):
                awaitable.close()
            logger.info('Background runner async step already in flight: %s', name)
            if fail_closed:
                raise RuntimeError(f'async step still in flight: {name}')
            return False
        if running_task is not None:
            self._finalize_async_step(name, running_task)
        if self._step_in_timeout_cooldown(name):
            with suppress(Exception):
                awaitable.close()
            if fail_closed:
                raise RuntimeError(f'async step is in timeout cooldown: {name}')
            return False
        task = asyncio.create_task(awaitable, name=f'eod-background-async:{name}')
        self._async_step_tasks[name] = task
        task.add_done_callback(
            lambda completed, step_name=name: self._finalize_async_step(
                step_name,
                completed,
            )
        )
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            self._mark_step_timed_out(name)
            if task.done():
                self._finalize_async_step(name, task)
            logger.warning(
                'Background runner async step still running after %ss: %s; keeping single-flight lock and cooling down for %ss',
                BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
                name,
                BACKGROUND_RUNNER_TIMEOUT_COOLDOWN_SECONDS,
            )
            if fail_closed:
                raise RuntimeError(f'async step timed out: {name}')
            return False
        return True

    def _finalize_async_step(self, name: str, task: asyncio.Task) -> None:
        if self._async_step_tasks.get(name) is task:
            self._async_step_tasks.pop(name, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning('Background runner async step failed: %s: %s', name, exc)
        else:
            self._step_timeout_cooldowns.pop(name, None)

    def _schedule_exec_readback_refresh(self, *, target_trade_date: str, trigger: str) -> None:
        result = signal_decider_exec_integration_service.schedule_signal_decider_exec_readback_refresh(
            target_trade_date=target_trade_date,
            trigger=trigger,
            timeout_seconds=BACKGROUND_RUNNER_STEP_TIMEOUT_SECONDS,
        )
        refresh = result.get('refresh') if isinstance(result, dict) else {}
        request = result.get('request') if isinstance(result, dict) else {}
        logger.info(
            'Background runner exec readback refresh status',
            extra={
                'exec_readback_refresh': {
                    'target_trade_date': (request or {}).get('target_trade_date') or target_trade_date,
                    'status': (refresh or {}).get('status'),
                    'in_flight': (refresh or {}).get('in_flight'),
                    'debounced': (refresh or {}).get('debounced'),
                    'timeout_seconds': (refresh or {}).get('timeout_seconds'),
                    'last_trigger': (refresh or {}).get('last_trigger'),
                    'last_attempted_at': (refresh or {}).get('last_attempted_at'),
                    'last_succeeded_at': (refresh or {}).get('last_succeeded_at'),
                    'last_error': (refresh or {}).get('last_error'),
                }
            },
        )

    async def _run_buy_authority_critical_step(
        self,
        *,
        name: str,
        market: object,
        awaitable,
    ) -> None:
        try:
            await awaitable
        except Exception:
            failure_count = self._buy_authority_critical_failures.get(name, 0) + 1
            self._buy_authority_critical_failures[name] = failure_count
            if failure_count < BUY_AUTHORITY_REVOCATION_CONSECUTIVE_FAILURE_THRESHOLD:
                logger.warning(
                    'Deferring delegated BUY authority revocation after transient '
                    'critical-step failure: %s (%s/%s)',
                    name,
                    failure_count,
                    BUY_AUTHORITY_REVOCATION_CONSECUTIVE_FAILURE_THRESHOLD,
                )
                raise
            trigger = f'background_runner:{name}_failed'
            try:
                await signal_decider_exec_integration_service.revoke_delegated_buys_for_unavailable_market_evidence(
                    target_trade_date=str(getattr(market, 'server_time', ''))[:10],
                    trigger=trigger,
                )
            except Exception as revocation_exc:
                logger.exception(
                    'Critical BUY authority revocation failed after %s: %s',
                    name,
                    revocation_exc,
                )
            raise
        else:
            self._buy_authority_critical_failures.pop(name, None)

    async def _run_loop(self) -> None:
        live_market_snapshot_loop = asyncio.get_running_loop()
        while self._running:
            self._tick_count += 1
            self._last_tick_started_at = datetime.now(timezone.utc)
            self._last_tick_error = None
            tick_failed = False
            market: object | None = None
            try:
                market = await async_get_market_state()
                if auto_pykrx_eod_scheduler.state.active_task:
                    await asyncio.sleep(self.interval_seconds)
                    continue
                runtime_background_enabled = settings.background_runner_enabled
                open_snapshot_refreshed = False
                if market.state in {'PREOPEN', 'CLOSED', 'WEEKEND', 'HOLIDAY'}:
                    await self._run_async_step(
                        'auto_pykrx_schedule_daily_pipeline_if_due',
                        auto_pykrx_eod_scheduler.schedule_daily_pipeline_if_due(
                            **(
                                {'expected_market_state': 'PREOPEN'}
                                if market.state == 'PREOPEN'
                                else {}
                            )
                        ),
                        fail_closed=True,
                    )
                    if auto_pykrx_eod_scheduler.state.active_task:
                        await asyncio.sleep(self.interval_seconds)
                        continue
                if runtime_background_enabled and market.state == 'OPEN':
                    await self._run_buy_authority_critical_step(
                        name='refresh_master_if_due',
                        market=market,
                        awaitable=self._run_blocking_step(
                            'refresh_master_if_due',
                            refresh_master_if_due,
                            fail_closed=True,
                        ),
                    )
                    force_index_refresh = self._claim_market_snapshot_open_drive_refresh(market)
                    await self._run_buy_authority_critical_step(
                        name='market_snapshot_refresh_if_due',
                        market=market,
                        awaitable=self._run_async_step(
                            'market_snapshot_refresh_if_due',
                            market_snapshot_service.refresh_market_snapshot_if_due(
                                trigger='background_runner:open',
                                force_index=force_index_refresh,
                            ),
                            fail_closed=True,
                        ),
                    )
                    open_snapshot_refreshed = True
                if runtime_background_enabled and market.state == 'PREOPEN':
                    await self._run_buy_authority_critical_step(
                        name='refresh_master_if_due',
                        market=market,
                        awaitable=self._run_blocking_step(
                            'refresh_master_if_due',
                            refresh_master_if_due,
                            fail_closed=True,
                        ),
                    )
                    await self._run_buy_authority_critical_step(
                        name='signal_decider_next_open_execution_refresh_plan_if_due',
                        market=market,
                        awaitable=self._run_blocking_step(
                            NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY,
                            lambda: signal_decider_next_open_execution_service.refresh_plan_if_due(
                                _live_market_snapshot_loop=live_market_snapshot_loop,
                            ),
                            fail_closed=True,
                            timeout_seconds=NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS,
                        ),
                    )
                    delegate_execution = (
                        signal_decider_exec_integration_service.should_delegate_execution()
                    )
                    reconcile_delegated_authority = (
                        not delegate_execution
                        and signal_decider_exec_integration_service.should_manage_delegated_authority(
                            target_trade_date=market.server_time[:10],
                        )
                    )
                    if (
                        (delegate_execution and is_preopen_exec_dispatch_due(market))
                        or reconcile_delegated_authority
                    ):
                        await self._run_async_step(
                            'signal_decider_exec_dispatch_next_open_plan',
                            signal_decider_exec_integration_service.dispatch_next_open_plan(
                                target_trade_date=market.server_time[:10],
                                refresh_plan=False,
                                trigger='preopen_exec_dispatch',
                            ),
                        )
                elif runtime_background_enabled and market.state == 'OPEN':
                    if signal_decider_exec_integration_service.should_delegate_execution():
                        open_dispatch_task = self._async_step_tasks.get(
                            OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY
                        )
                        if open_dispatch_task is not None and not open_dispatch_task.done():
                            logger.info(
                                'Background runner skipping OPEN exec cycle while prior '
                                'dispatch is still in flight'
                            )
                        else:
                            await self._run_buy_authority_critical_step(
                                name='signal_decider_open_execution_refresh_plan_if_due',
                                market=market,
                                awaitable=self._run_blocking_step(
                                    NEXT_OPEN_PLAN_REFRESH_SINGLE_FLIGHT_KEY,
                                    lambda: signal_decider_next_open_execution_service.refresh_plan_if_due(
                                        target_trade_date=market.server_time[:10],
                                        _live_market_snapshot_loop=live_market_snapshot_loop,
                                    ),
                                    fail_closed=True,
                                    timeout_seconds=NEXT_OPEN_PLAN_REFRESH_TIMEOUT_SECONDS,
                                ),
                            )
                            dispatch_completed = await self._run_async_step(
                                OPEN_EXEC_DISPATCH_SINGLE_FLIGHT_KEY,
                                signal_decider_exec_integration_service.dispatch_next_open_plan(
                                    target_trade_date=market.server_time[:10],
                                    refresh_plan=False,
                                    trigger='market_open_exec_dispatch',
                                ),
                            )
                            if dispatch_completed:
                                self._schedule_exec_readback_refresh(
                                    target_trade_date=market.server_time[:10],
                                    trigger='market_open_scheduler',
                                )
                    elif signal_decider_exec_integration_service.should_manage_delegated_authority(
                        target_trade_date=market.server_time[:10],
                    ):
                        await self._run_async_step(
                            'signal_decider_exec_reconcile_disabled_authority',
                            signal_decider_exec_integration_service.dispatch_next_open_plan(
                                target_trade_date=market.server_time[:10],
                                refresh_plan=False,
                                trigger='market_open_exec_control_reconciliation',
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
                if runtime_background_enabled:
                    await self._run_async_step(
                        'trading_order_reconciliation_periodic',
                        trading_order_reconciliation_service.run_periodic_if_due(),
                    )
                    if not open_snapshot_refreshed:
                        await self._run_async_step(
                            'market_snapshot_refresh_if_due',
                            market_snapshot_service.refresh_market_snapshot_if_due(
                                trigger=f'background_runner:{market.state.lower()}',
                                force_index=False,
                            ),
                        )
                    await self._run_async_step(
                        'portfolio_summary_refresh_if_due',
                        portfolio_summary_service.refresh_portfolio_summary_if_due(
                            trigger=f'background_runner:{market.state.lower()}'
                        ),
                    )
            except Exception as exc:
                tick_failed = True
                self._last_tick_error = {
                    'type': type(exc).__name__,
                    'message': str(exc)[:512],
                }
                logger.exception('Background runner loop failed: %s', exc)
            finally:
                self._last_tick_completed_at = datetime.now(timezone.utc)
                if not tick_failed and self._running:
                    self._last_tick_succeeded_at = self._last_tick_completed_at
            await asyncio.sleep(self._next_poll_interval_seconds(market))


background_runner = EODBackgroundRunner(interval_seconds=settings.eod_auto_pykrx_poll_seconds)
