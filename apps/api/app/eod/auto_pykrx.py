from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.core.settings import settings
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.market.clock import KST, get_market_state

logger = logging.getLogger(__name__)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


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

    async def maybe_schedule(self) -> None:
        self._state.enabled = settings.eod_auto_pykrx_enabled
        market = get_market_state()
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
        self._task = asyncio.create_task(self._run_for_market_date(market.server_time[:10]), name='auto-pykrx-eod')

    async def _run_for_market_date(self, market_date: str) -> None:
        try:
            result = await asyncio.to_thread(
                PYKRX_BACKFILL_SERVICE.run_daily_collection_if_due,
                market_date=market_date,
                markets=_parse_markets(settings.eod_auto_pykrx_markets),
                trigger='auto_close_scheduler',
                refresh_universe=settings.eod_auto_pykrx_refresh_universe,
            )
            self._state.last_result = result
            self._state.last_status = 'completed' if result.get('triggered') else 'skipped'
            self._state.last_reason = result.get('reason') or result.get('status')
        except Exception as exc:
            logger.exception('Automatic pykrx EOD scheduler failed for %s: %s', market_date, exc)
            self._state.last_result = None
            self._state.last_status = 'failed'
            self._state.last_reason = str(exc)
        finally:
            self._state.active_task = False
            self._state.last_completed_at = _utcnow()


auto_pykrx_eod_scheduler = AutoPykrxEODScheduler()
