from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import suppress
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import exchange_calendars as exchange_calendars
from fastapi.encoders import jsonable_encoder

from app.core.settings import settings
from app.repositories.us_equity_repository import (
    claim_us_equity_pipeline_run,
    touch_us_equity_pipeline_run,
    update_us_equity_pipeline_run,
)
from app.us_equities.agent_analysis_service import us_agent_analysis_service
from app.us_equities.execution_plan_service import us_execution_plan_service
from app.us_equities.fundamental_collection_service import us_fundamental_collection_service
from app.us_equities.price_collection_service import us_price_collection_service
from app.us_equities.universe_service import us_universe_refresh_service


logger = logging.getLogger(__name__)
US_EASTERN = ZoneInfo('America/New_York')
US_EQUITY_PIPELINE_LEASE_HEARTBEAT_SECONDS = 60.0
_STAGE_CHECKPOINT_FIELDS = (
    'status',
    'market_date',
    'run_id',
    'persist',
    'dry_run',
    'completed',
    'failed',
    'accepted_candidate_count',
    'rejected_candidate_count',
    'counts',
)


def _scheduled_time(value: str) -> time:
    try:
        hour_text, minute_text = value.strip().split(':', 1)
        return time(hour=int(hour_text), minute=int(minute_text))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError('US_EQUITY_AUTO_PIPELINE_LOCAL_TIME must use HH:MM') from exc


def _is_us_session(value: date) -> bool:
    return bool(exchange_calendars.get_calendar('XNYS').is_session(value))


def _stage_checkpoints(stages: dict[str, dict]) -> dict[str, dict]:
    checkpoints: dict[str, dict] = {}
    for name, result in stages.items():
        if not isinstance(result, dict):
            checkpoints[name] = {'status': 'completed'}
            continue
        checkpoint = {
            key: result[key]
            for key in _STAGE_CHECKPOINT_FIELDS
            if key in result
        }
        checkpoints[name] = jsonable_encoder(checkpoint or {'status': 'completed'})
    return checkpoints


def is_us_pipeline_due(
    *,
    now: datetime,
    last_completed_market_date: date | None,
    scheduled_local_time: str,
) -> bool:
    local_now = now.astimezone(US_EASTERN)
    market_date = local_now.date()
    return (
        _is_us_session(market_date)
        and local_now.time().replace(tzinfo=None) >= _scheduled_time(scheduled_local_time)
        and last_completed_market_date != market_date
    )


class USEquityPipelineScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._active_run: asyncio.Task | None = None
        self._running = False
        self._last_completed_market_date: date | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name='us-equity-pipeline-scheduler')

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._active_run and not self._active_run.done():
            self._active_run.cancel()
            await asyncio.gather(self._active_run, return_exceptions=True)
        self._active_run = None

    async def run_if_due(self, *, now: datetime | None = None, force: bool = False) -> dict:
        observed_at = now or datetime.now(US_EASTERN)
        market_date = observed_at.astimezone(US_EASTERN).date()
        if not force and not is_us_pipeline_due(
            now=observed_at,
            last_completed_market_date=self._last_completed_market_date,
            scheduled_local_time=settings.us_equity_auto_pipeline_local_time,
        ):
            return {'status': 'not_due', 'market_date': market_date.isoformat()}
        if self._active_run and not self._active_run.done():
            return {'status': 'already_running', 'market_date': market_date.isoformat()}
        run_id = f'us-pipeline-{market_date.isoformat()}-{uuid.uuid4().hex[:10]}'
        claim = await asyncio.to_thread(
            claim_us_equity_pipeline_run,
            market_date=market_date,
            run_id=run_id,
            stale_after_minutes=settings.us_equity_auto_pipeline_stale_after_minutes,
            force=force,
        )
        if not claim.get('claimed'):
            return {
                'status': claim.get('status', 'not_claimed'),
                'market_date': market_date.isoformat(),
                'run_id': claim.get('run_id'),
            }
        self._active_run = asyncio.create_task(
            self._run_pipeline(market_date, run_id=run_id),
            name=f'us-equity-pipeline:{market_date.isoformat()}',
        )
        result = await self._active_run
        if result.get('status') == 'completed':
            self._last_completed_market_date = market_date
        return result

    async def _run_pipeline(self, market_date: date, *, run_id: str) -> dict:
        stages: dict[str, dict] = {}
        lease_task = asyncio.create_task(
            self._maintain_lease(market_date=market_date, run_id=run_id),
            name=f'us-equity-pipeline-lease:{market_date.isoformat()}',
        )
        try:
            stages['universe'] = await us_universe_refresh_service.refresh(persist=True, preview_limit=0)
            await self._heartbeat(market_date=market_date, run_id=run_id, stages=stages)
            stages['prices'] = await us_price_collection_service.collect_and_screen(
                market_date=market_date,
                persist=True,
            )
            await self._heartbeat(market_date=market_date, run_id=run_id, stages=stages)
            stages['fundamentals'] = await us_fundamental_collection_service.collect_and_screen(
                market_date=market_date,
                persist=True,
            )
            await self._heartbeat(market_date=market_date, run_id=run_id, stages=stages)
            stages['agents'] = await asyncio.to_thread(
                us_agent_analysis_service.dispatch,
                market_date=market_date,
                persist=True,
                batch_size=5,
            )
            if stages['agents'].get('status') != 'completed' or int(stages['agents'].get('failed') or 0) != 0:
                raise RuntimeError('US agent analysis did not complete without failures')
            await self._heartbeat(market_date=market_date, run_id=run_id, stages=stages)
            stages['execution'] = await us_execution_plan_service.dispatch(
                source_market_date=market_date,
                dry_run=settings.us_equity_auto_pipeline_exec_dry_run,
                agent_run_id=str(stages['agents']['run_id']),
            )
            completed = await asyncio.to_thread(
                update_us_equity_pipeline_run,
                market_date=market_date,
                run_id=run_id,
                status='completed',
                stages=_stage_checkpoints(stages),
            )
            if not completed:
                raise RuntimeError('Lost durable US-equity pipeline run ownership before completion')
            return {
                'status': 'completed',
                'market_date': market_date.isoformat(),
                'run_id': run_id,
                'stages': stages,
            }
        except asyncio.CancelledError:
            await asyncio.to_thread(
                update_us_equity_pipeline_run,
                market_date=market_date,
                run_id=run_id,
                status='failed',
                stages=_stage_checkpoints(stages),
                error='CancelledError: scheduler stopped during pipeline run',
            )
            raise
        except Exception as exc:
            logger.exception('US equity automatic pipeline failed for %s', market_date)
            await asyncio.to_thread(
                update_us_equity_pipeline_run,
                market_date=market_date,
                run_id=run_id,
                status='failed',
                stages=_stage_checkpoints(stages),
                error=f'{type(exc).__name__}: {exc}',
            )
            return {
                'status': 'failed',
                'market_date': market_date.isoformat(),
                'run_id': run_id,
                'stages': stages,
                'error': f'{type(exc).__name__}: {exc}',
            }
        finally:
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await lease_task

    @staticmethod
    async def _maintain_lease(*, market_date: date, run_id: str) -> None:
        while True:
            await asyncio.sleep(US_EQUITY_PIPELINE_LEASE_HEARTBEAT_SECONDS)
            try:
                updated = await asyncio.to_thread(
                    touch_us_equity_pipeline_run,
                    market_date=market_date,
                    run_id=run_id,
                )
            except Exception as exc:
                logger.warning('US-equity pipeline lease heartbeat failed for %s: %s', market_date, exc)
                continue
            if not updated:
                logger.error('Lost durable US-equity pipeline lease for %s (%s)', market_date, run_id)
                return

    @staticmethod
    async def _heartbeat(*, market_date: date, run_id: str, stages: dict[str, dict]) -> None:
        updated = await asyncio.to_thread(
            update_us_equity_pipeline_run,
            market_date=market_date,
            run_id=run_id,
            status='running',
            stages=_stage_checkpoints(stages),
        )
        if not updated:
            raise RuntimeError('Lost durable US-equity pipeline run ownership')

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.run_if_due()
            except Exception as exc:
                logger.warning('US equity scheduler tick failed: %s', exc)
            await asyncio.sleep(settings.us_equity_auto_pipeline_interval_seconds)


us_equity_pipeline_scheduler = USEquityPipelineScheduler()
