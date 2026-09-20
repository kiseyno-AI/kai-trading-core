from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from app.core.settings import settings
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.market.clock import KST, async_get_market_state
from app.market.session import async_previous_krx_trading_date
from app.repositories.eod_run_repository import (
    load_oldest_outstanding_auto_eod_recovery_target,
)

logger = logging.getLogger(__name__)

_INCOMPLETE_UNIVERSE_ERROR_PREFIX = (
    'PyKRX universe snapshot is incomplete for requested markets:'
)
_INCOMPLETE_UNIVERSE_RETRY_BASE_MINUTES = 30
_INCOMPLETE_UNIVERSE_RETRY_MAX_MINUTES = 240
_RECOVERY_WINDOW_SESSION_COUNT = 5


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scheduled_pipeline_result_status(result: dict) -> str:
    raw_status = str(result.get('status') or '').strip().lower()
    normalized_status = {
        'completed_with_errors': 'partial',
        'warning': 'partial',
        'recovered': 'completed',
        'ok': 'completed',
    }.get(raw_status, raw_status)
    if normalized_status in {
        'completed',
        'partial',
        'failed',
        'blocked',
        'waiting',
        'interrupted',
        'abandoned',
    }:
        return normalized_status
    existing_run = result.get('existing_run')
    if (
        result.get('reason') == 'already_completed'
        and isinstance(existing_run, dict)
        and str(existing_run.get('status') or '').strip().lower()
        == 'completed'
    ):
        return 'completed'
    return 'completed' if result.get('triggered') else 'skipped'


def _parse_markets(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_MARKETS
    markets = tuple(item.strip().upper() for item in raw.split(',') if item.strip())
    return markets or DEFAULT_MARKETS


async def _oldest_recovery_window_date(latest_session_date: str) -> str:
    oldest_session_date = date.fromisoformat(latest_session_date)
    for _ in range(_RECOVERY_WINDOW_SESSION_COUNT - 1):
        previous_session_date = await async_previous_krx_trading_date(
            oldest_session_date,
            allow_fallback=True,
        )
        if previous_session_date is None:
            break
        oldest_session_date = previous_session_date
    return oldest_session_date.isoformat()


@dataclass
class AutoPykrxEODState:
    enabled: bool = settings.eod_auto_pykrx_enabled
    active_task: bool = False
    last_market_state: str | None = None
    last_market_date: str | None = None
    last_checked_at: str | None = None
    last_started_at: str | None = None
    last_completed_at: str | None = None
    last_status: str = 'idle'
    last_reason: str | None = None
    last_result: dict | None = None
    preflight_failure_market_date: str | None = None
    preflight_failure_markets: tuple[str, ...] = ()
    preflight_failure_count: int = 0
    preflight_retry_not_before: datetime | None = None

    def snapshot(self) -> dict:
        return {
            'enabled': self.enabled,
            'active_task': self.active_task,
            'last_market_state': self.last_market_state,
            'last_market_date': self.last_market_date,
            'last_checked_at': self.last_checked_at,
            'last_started_at': self.last_started_at,
            'last_completed_at': self.last_completed_at,
            'last_status': self.last_status,
            'last_reason': self.last_reason,
            'last_result': self.last_result,
            'preflight_failure_market_date': self.preflight_failure_market_date,
            'preflight_failure_markets': list(self.preflight_failure_markets),
            'preflight_failure_count': self.preflight_failure_count,
            'preflight_retry_not_before': (
                self.preflight_retry_not_before.isoformat()
                if self.preflight_retry_not_before is not None
                else None
            ),
        }


class AutoPykrxEODScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._state = AutoPykrxEODState()

    @property
    def state(self) -> AutoPykrxEODState:
        return self._state

    def snapshot(self) -> dict:
        self._state.enabled = settings.eod_auto_pykrx_enabled
        return self._state.snapshot()

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._state.active_task = False

    async def schedule_daily_pipeline_if_due(
        self,
        *,
        expected_market_state: str | None = None,
    ) -> None:
        self._state.enabled = settings.eod_auto_pykrx_enabled
        market = await async_get_market_state()
        self._state.last_checked_at = _utcnow()
        self._state.last_market_state = market.state
        current_market_date = market.server_time[:10]
        self._state.last_market_date = current_market_date

        if expected_market_state is not None and market.state != expected_market_state:
            self._state.last_status = 'idle'
            self._state.last_reason = (
                f'market_state_changed:{expected_market_state}->{market.state}'
            )
            return

        if not settings.eod_auto_pykrx_enabled:
            self._state.last_status = 'disabled'
            self._state.last_reason = 'disabled_by_config'
            return

        if self._task and not self._task.done():
            self._state.active_task = True
            self._state.last_status = 'running'
            return

        if self._task and self._task.done():
            try:
                self._task.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.exception('Automatic pykrx EOD task finished with error: %s', exc)
            self._task = None
            self._state.active_task = False

        try:
            recovery_window_latest_date = (
                current_market_date
                if market.is_trading_day
                else market.latest_valid_trading_date or current_market_date
            )
            recovery_window_start_date = await _oldest_recovery_window_date(
                recovery_window_latest_date
            )
            recovery_target = await asyncio.to_thread(
                load_oldest_outstanding_auto_eod_recovery_target,
                on_or_after=recovery_window_start_date,
                on_or_before=current_market_date,
            )
        except Exception as exc:
            logger.exception('Could not inspect automatic EOD recovery state: %s', exc)
            self._state.last_status = 'blocked'
            self._state.last_reason = 'recovery_state_unavailable'
            return

        recovery_market_date = (
            recovery_target.trade_date
            if recovery_target is not None
            else None
        )
        if recovery_market_date is not None:
            target_market_date = recovery_market_date
        elif market.state == 'CLOSED':
            target_market_date = current_market_date
        elif market.state in {'WEEKEND', 'HOLIDAY'}:
            target_market_date = market.latest_valid_trading_date
        else:
            target_market_date = None
        self._state.last_market_date = target_market_date or current_market_date

        configured_markets = _parse_markets(settings.eod_auto_pykrx_markets)
        same_preflight_failure = (
            self._state.preflight_failure_market_date == target_market_date
            and self._state.preflight_failure_markets == configured_markets
        )
        if (
            same_preflight_failure
            and self._state.preflight_retry_not_before is not None
            and datetime.now(timezone.utc) < self._state.preflight_retry_not_before
        ):
            self._state.last_status = 'blocked'
            self._state.last_reason = 'incomplete_universe_preflight_backoff'
            return
        if self._state.preflight_failure_market_date is not None and not same_preflight_failure:
            self._clear_preflight_backoff()

        if recovery_market_date is None and market.state not in {'CLOSED', 'WEEKEND', 'HOLIDAY'}:
            self._state.last_status = 'idle'
            self._state.last_reason = f'market_not_closed:{market.state}'
            return

        if target_market_date is None:
            self._state.last_status = 'idle'
            self._state.last_reason = 'missing_latest_valid_trading_date'
            return

        if recovery_market_date is None and market.state == 'CLOSED' and not market.session_close_at:
            self._state.last_status = 'idle'
            self._state.last_reason = 'missing_session_close'
            return

        if recovery_market_date is None and market.state == 'CLOSED':
            close_at = datetime.fromisoformat(market.session_close_at)
            eligible_at = close_at + timedelta(
                minutes=settings.eod_auto_pykrx_after_close_minutes
            )
            if datetime.now(KST) < eligible_at:
                self._state.last_status = 'idle'
                self._state.last_reason = f'waiting_until:{eligible_at.isoformat()}'
                return

        if expected_market_state is not None:
            latest_market = await async_get_market_state()
            self._state.last_market_state = latest_market.state
            self._state.last_checked_at = _utcnow()
            if latest_market.state != expected_market_state:
                self._state.last_status = 'idle'
                self._state.last_reason = (
                    f'market_state_changed:{expected_market_state}'
                    f'->{latest_market.state}'
                )
                return

        self._state.active_task = True
        self._state.last_started_at = _utcnow()
        self._state.last_status = 'running'
        self._state.last_reason = (
            'scheduled_recovery' if recovery_market_date is not None else 'scheduled'
        )
        self._task = asyncio.create_task(
            self._run_scheduled_daily_pipeline(
                target_market_date,
                markets=configured_markets,
                **(
                    {'point_in_time_recovery': True}
                    if recovery_market_date is not None
                    and date.fromisoformat(target_market_date)
                    < date.fromisoformat(current_market_date)
                    else {}
                ),
                **(
                    {'recovery_eod_run_id': recovery_target.eod_run_id}
                    if recovery_target is not None
                    else {}
                ),
            ),
            name='auto-pykrx-eod',
        )

    async def _run_scheduled_daily_pipeline(
        self,
        market_date: str,
        *,
        markets: tuple[str, ...] | None = None,
        point_in_time_recovery: bool = False,
        recovery_eod_run_id: int | None = None,
    ) -> None:
        resolved_markets = markets or _parse_markets(settings.eod_auto_pykrx_markets)
        try:
            run_kwargs = {
                'market_date': market_date,
                'markets': resolved_markets,
                'trigger': 'auto_close_scheduler',
                'refresh_universe': settings.eod_auto_pykrx_refresh_universe,
            }
            if point_in_time_recovery:
                run_kwargs['point_in_time_recovery'] = True
            if recovery_eod_run_id is not None:
                run_kwargs['recovery_eod_run_id'] = recovery_eod_run_id
            result = await asyncio.to_thread(
                PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due,
                **run_kwargs,
            )
            self._clear_preflight_backoff()
            self._state.last_result = result
            self._state.last_status = _scheduled_pipeline_result_status(result)
            self._state.last_reason = result.get('reason') or result.get('status')
        except Exception as exc:
            logger.exception('Automatic pykrx EOD scheduler failed for %s: %s', market_date, exc)
            error_message = str(exc)
            if error_message.startswith(_INCOMPLETE_UNIVERSE_ERROR_PREFIX):
                same_failure_scope = (
                    self._state.preflight_failure_market_date == market_date
                    and self._state.preflight_failure_markets == resolved_markets
                )
                failure_count = self._state.preflight_failure_count + 1 if same_failure_scope else 1
                retry_minutes = min(
                    _INCOMPLETE_UNIVERSE_RETRY_BASE_MINUTES
                    * (2 ** min(max(failure_count - 1, 0), 3)),
                    _INCOMPLETE_UNIVERSE_RETRY_MAX_MINUTES,
                )
                self._state.preflight_failure_market_date = market_date
                self._state.preflight_failure_markets = resolved_markets
                self._state.preflight_failure_count = failure_count
                self._state.preflight_retry_not_before = (
                    datetime.now(timezone.utc) + timedelta(minutes=retry_minutes)
                )
                self._state.last_result = {
                    'status': 'blocked',
                    'reason': 'incomplete_universe_preflight_backoff',
                    'error': error_message,
                    'markets': list(resolved_markets),
                    'failure_count': failure_count,
                    'retry_not_before': self._state.preflight_retry_not_before.isoformat(),
                }
                self._state.last_status = 'blocked'
                self._state.last_reason = 'incomplete_universe_preflight_backoff'
            else:
                self._state.last_result = None
                self._state.last_status = 'failed'
                self._state.last_reason = error_message
        finally:
            self._state.active_task = False
            self._state.last_completed_at = _utcnow()

    def _clear_preflight_backoff(self) -> None:
        self._state.preflight_failure_market_date = None
        self._state.preflight_failure_markets = ()
        self._state.preflight_failure_count = 0
        self._state.preflight_retry_not_before = None


auto_pykrx_eod_scheduler = AutoPykrxEODScheduler()
