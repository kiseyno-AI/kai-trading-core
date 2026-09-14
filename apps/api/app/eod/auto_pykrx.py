from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.core.settings import settings
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.market.clock import KST, async_get_market_state

logger = logging.getLogger(__name__)

_INCOMPLETE_UNIVERSE_ERROR_PREFIX = (
    'PyKRX universe snapshot is incomplete for requested markets:'
)
_INCOMPLETE_UNIVERSE_RETRY_BASE_MINUTES = 30
_INCOMPLETE_UNIVERSE_RETRY_MAX_MINUTES = 240


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
    return 'completed' if result.get('triggered') else 'skipped'


def _parse_markets(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_MARKETS
    markets = tuple(item.strip().upper() for item in raw.split(',') if item.strip())
    return markets or DEFAULT_MARKETS


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

    async def schedule_daily_pipeline_if_due(self) -> None:
        self._state.enabled = settings.eod_auto_pykrx_enabled
        market = await async_get_market_state()
        self._state.last_checked_at = _utcnow()
        self._state.last_market_state = market.state
        self._state.last_market_date = market.server_time[:10]

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

        configured_markets = _parse_markets(settings.eod_auto_pykrx_markets)
        same_preflight_failure = (
            self._state.preflight_failure_market_date == market.server_time[:10]
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

        if market.state != 'CLOSED':
            self._state.last_status = 'idle'
            self._state.last_reason = f'market_not_closed:{market.state}'
            return

        if not market.session_close_at:
            self._state.last_status = 'idle'
            self._state.last_reason = 'missing_session_close'
            return

        close_at = datetime.fromisoformat(market.session_close_at)
        eligible_at = close_at + timedelta(minutes=settings.eod_auto_pykrx_after_close_minutes)
        if datetime.now(KST) < eligible_at:
            self._state.last_status = 'idle'
            self._state.last_reason = f'waiting_until:{eligible_at.isoformat()}'
            return

        self._state.active_task = True
        self._state.last_started_at = _utcnow()
        self._state.last_status = 'running'
        self._state.last_reason = 'scheduled'
        self._task = asyncio.create_task(
            self._run_scheduled_daily_pipeline(
                market.server_time[:10],
                markets=configured_markets,
            ),
            name='auto-pykrx-eod',
        )

    async def _run_scheduled_daily_pipeline(
        self,
        market_date: str,
        *,
        markets: tuple[str, ...] | None = None,
    ) -> None:
        resolved_markets = markets or _parse_markets(settings.eod_auto_pykrx_markets)
        try:
            result = await asyncio.to_thread(
                PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due,
                market_date=market_date,
                markets=resolved_markets,
                trigger='auto_close_scheduler',
                refresh_universe=settings.eod_auto_pykrx_refresh_universe,
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
