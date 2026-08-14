from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg

from app.core.settings import settings
from app.db import DATABASE_URL

logger = logging.getLogger(__name__)

_OBSERVED_READ_QUERY_PATTERNS = (
    'from dart_fundamentals_snapshots',
    'from daily_signal_features',
    'from daily_trending_ranks',
    'from daily_technical_snapshots',
    'from daily_momentum_factor_results',
    'from daily_value_factor_results',
    'from daily_low_risk_factor_results',
    'from daily_beta_factor_results',
    'from daily_advanced_value_factor_results',
    'from daily_unified_value_factor_results',
    'from daily_quality_factor_results',
    'from daily_composite_factor_results',
    'from daily_market_risk_filter_results',
    'from daily_piotroski_filter_results',
)


def _psycopg_url() -> str:
    return DATABASE_URL.replace('postgresql+psycopg://', 'postgresql://', 1)


def _is_observed_read_query(query: str | None) -> bool:
    normalized = str(query or '').strip().lower()
    if not normalized.startswith('select '):
        return False
    return any(pattern in normalized for pattern in _OBSERVED_READ_QUERY_PATTERNS)


@dataclass
class PoolWatchdogSweep:
    active_count: int
    slow_read_count: int
    slow_read_pids: list[int]
    cancelled_pids: list[int]


class DatabasePoolWatchdog:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if not settings.db_pool_watchdog_enabled:
            return
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name='db-pool-watchdog')

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
                sweep = await asyncio.to_thread(self.sweep_once)
                if sweep.slow_read_count:
                    logger.warning(
                        'DB pool watchdog observed stale read queries',
                        extra={
                            'db_pool_watchdog': {
                                'active_count': sweep.active_count,
                                'slow_read_count': sweep.slow_read_count,
                                'slow_read_pids': sweep.slow_read_pids,
                                'cancelled_pids': sweep.cancelled_pids,
                            }
                        },
                    )
            except Exception as exc:
                logger.warning('DB pool watchdog sweep failed: %s', exc)
            await asyncio.sleep(max(1.0, float(settings.db_pool_watchdog_interval_seconds)))

    def sweep_once(self) -> PoolWatchdogSweep:
        observe_after_seconds = max(1, int(settings.db_pool_watchdog_cancel_after_seconds))
        active_threshold = max(1, int(settings.db_pool_watchdog_active_connection_threshold))
        observe_limit = max(1, int(settings.db_pool_watchdog_cancel_limit))

        with psycopg.connect(
            _psycopg_url(),
            autocommit=True,
            connect_timeout=max(1, int(settings.db_connect_timeout_seconds)),
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select count(*)
                    from pg_stat_activity
                    where datname = current_database()
                      and pid <> pg_backend_pid()
                      and state = 'active'
                    """
                )
                active_count = int(cur.fetchone()[0] or 0)
                if active_count < active_threshold:
                    return PoolWatchdogSweep(
                        active_count=active_count,
                        slow_read_count=0,
                        slow_read_pids=[],
                        cancelled_pids=[],
                    )

                cur.execute(
                    """
                    select pid, query
                    from pg_stat_activity
                    where datname = current_database()
                      and pid <> pg_backend_pid()
                      and state = 'active'
                      and now() - query_start > (%s || ' seconds')::interval
                    order by query_start asc
                    limit %s
                    """,
                    (observe_after_seconds, observe_limit * 3),
                )
                slow_read_pids = [
                    int(row[0])
                    for row in cur.fetchall()
                    if _is_observed_read_query(row[1])
                ][:observe_limit]
                cancelled_pids: list[int] = []
                if settings.db_pool_watchdog_cancel_enabled:
                    for pid in slow_read_pids:
                        cur.execute('select pg_cancel_backend(%s)', (pid,))
                        if bool(cur.fetchone()[0]):
                            cancelled_pids.append(pid)

        return PoolWatchdogSweep(
            active_count=active_count,
            slow_read_count=len(slow_read_pids),
            slow_read_pids=slow_read_pids,
            cancelled_pids=cancelled_pids,
        )


db_pool_watchdog = DatabasePoolWatchdog()
