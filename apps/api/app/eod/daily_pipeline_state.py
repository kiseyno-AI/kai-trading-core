from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import or_, select, text

from app.core.settings import settings
from app.db import session_scope
from app.db_models import DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun


ACTIVE_DAILY_PIPELINE_STATUSES = {'running'}
TERMINAL_DAILY_PIPELINE_STATUSES = {
    'completed',
    'partial',
    'failed',
    'interrupted',
    'abandoned',
    'blocked',
    'waiting',
    'skipped',
}
SLOW_DAILY_PIPELINE_STAGE_THRESHOLD_MS = 5 * 60 * 1000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _as_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    normalized = str(value or '').strip()
    if not normalized:
        raise ValueError('market_date is required for daily pipeline run state')
    return date.fromisoformat(normalized[:10])


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _build_counts(
    *,
    processed: Any = None,
    total: Any = None,
    succeeded: Any = None,
    failed: Any = None,
    remaining: Any = None,
) -> dict[str, int]:
    total_value = _as_int(total)
    succeeded_value = _as_int(succeeded)
    failed_value = _as_int(failed)
    processed_value = _as_int(processed, succeeded_value + failed_value)
    if processed is None:
        processed_value = succeeded_value + failed_value
    remaining_value = _as_int(remaining, max(total_value - processed_value, 0) if total_value else 0)
    return {
        'processed': processed_value,
        'total': total_value,
        'succeeded': succeeded_value,
        'failed': failed_value,
        'remaining': remaining_value,
    }


def _build_stage_duration_details(
    *,
    started_at: datetime | None,
    completed_at: datetime | None,
) -> dict[str, Any]:
    if started_at is None or completed_at is None:
        return {}
    elapsed_ms = max(0, int(round((completed_at - started_at).total_seconds() * 1000)))
    return {
        'duration_ms': elapsed_ms,
        'duration_seconds': round(elapsed_ms / 1000, 3),
        'slow_stage': elapsed_ms >= SLOW_DAILY_PIPELINE_STAGE_THRESHOLD_MS,
        'slow_stage_threshold_ms': SLOW_DAILY_PIPELINE_STAGE_THRESHOLD_MS,
    }


class DailyPipelineRunStateService:
    @staticmethod
    def _serialize_run(row: DailyPipelineRun, stage_rows: Sequence[DailyPipelineStageRun] | None = None) -> dict[str, Any]:
        payload = {
            'id': row.id,
            'market_date': _iso(row.market_date),
            'trigger': row.trigger,
            'status': row.status,
            'current_stage': row.current_stage,
            'started_at': _iso(row.started_at),
            'updated_at': _iso(row.updated_at),
            'completed_at': _iso(row.completed_at),
            'error_message': row.error_message,
            'run_metadata': row.run_metadata or {},
        }
        if stage_rows is not None:
            payload['stages'] = {
                stage.stage_id: DailyPipelineRunStateService._serialize_stage(stage)
                for stage in stage_rows
            }
        return payload

    @staticmethod
    def _serialize_stage(row: DailyPipelineStageRun) -> dict[str, Any]:
        return {
            'id': row.id,
            'pipeline_run_id': row.pipeline_run_id,
            'market_date': _iso(row.market_date),
            'stage_id': row.stage_id,
            'status': row.status,
            'processed': row.processed,
            'total': row.total,
            'succeeded': row.succeeded,
            'failed': row.failed,
            'remaining': row.remaining,
            'current_step': row.current_step,
            'current_activity': row.current_activity,
            'current_symbol': row.current_symbol,
            'current_period': row.current_period,
            'started_at': _iso(row.started_at),
            'updated_at': _iso(row.updated_at),
            'completed_at': _iso(row.completed_at),
            'error_message': row.error_message,
            'source_run_ids': row.source_run_ids or {},
            'details': row.details or {},
        }

    def start_run(
        self,
        *,
        market_date: str | date,
        trigger: str,
        requested_stages: Sequence[str],
        resolved_stages: Sequence[str],
        run_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _utcnow()
        normalized_market_date = _normalize_market_date(market_date)
        metadata = {
            'requested_stages': list(requested_stages),
            'resolved_stages': list(resolved_stages),
            **dict(run_metadata or {}),
        }
        with session_scope() as session:
            # Serialize admission for every entry point. HTTP clients can time
            # out while the worker thread continues, so limiting this guard to
            # monitor-triggered runs still permits duplicate manual/API runs.
            session.execute(
                text('SELECT pg_advisory_xact_lock(:source_key, :trade_key)'),
                {
                    'source_key': 20_001,
                    'trade_key': int(normalized_market_date.strftime('%Y%m%d')),
                },
            )
            active_rows = list(
                session.execute(
                    select(DailyPipelineRun)
                    .where(
                        DailyPipelineRun.market_date == normalized_market_date,
                        DailyPipelineRun.status.in_(ACTIVE_DAILY_PIPELINE_STATUSES),
                    )
                    .order_by(DailyPipelineRun.started_at.desc(), DailyPipelineRun.id.desc())
                    .with_for_update()
                ).scalars()
            )
            stale_after_minutes = max(1, int(settings.eod_auto_pykrx_stale_run_minutes or 30))
            stale_cutoff = now - timedelta(minutes=stale_after_minutes)
            current_active_rows: list[DailyPipelineRun] = []
            for active_row in active_rows:
                last_activity_at = _as_utc_datetime(active_row.updated_at or active_row.started_at)
                if last_activity_at is None or last_activity_at > stale_cutoff:
                    current_active_rows.append(active_row)
                    continue

                reason = (
                    f'Marked abandoned after {stale_after_minutes} minutes without daily pipeline activity.'
                )
                stale_metadata = dict(active_row.run_metadata or {})
                stale_metadata.update(
                    {
                        'abandoned_reason': 'stale_activity_timeout',
                        'abandoned_at': now.isoformat(),
                        'last_activity_at': last_activity_at.isoformat(),
                        'stale_after_minutes': stale_after_minutes,
                        'resumable': True,
                    }
                )
                active_row.status = 'abandoned'
                active_row.error_message = reason
                active_row.completed_at = now
                active_row.updated_at = now
                active_row.run_metadata = stale_metadata

                running_stage_rows = list(
                    session.execute(
                        select(DailyPipelineStageRun)
                        .where(
                            DailyPipelineStageRun.pipeline_run_id == active_row.id,
                            DailyPipelineStageRun.status.in_(ACTIVE_DAILY_PIPELINE_STATUSES),
                        )
                        .with_for_update()
                    ).scalars()
                )
                for stage_row in running_stage_rows:
                    details = dict(stage_row.details or {})
                    details.update(
                        {
                            'abandoned_reason': 'stale_activity_timeout',
                            'abandoned_at': now.isoformat(),
                            'last_activity_at': last_activity_at.isoformat(),
                            'stale_after_minutes': stale_after_minutes,
                            'resumable': True,
                        }
                    )
                    stage_row.status = 'abandoned'
                    stage_row.error_message = reason
                    stage_row.current_step = 'stale_run_abandoned'
                    stage_row.current_activity = reason
                    stage_row.completed_at = now
                    stage_row.updated_at = now
                    stage_row.details = details

            requested_stage_set = set(metadata['resolved_stages'])
            existing_active = next(
                (
                    row
                    for row in current_active_rows
                    if requested_stage_set.intersection(
                        set((row.run_metadata or {}).get('resolved_stages') or [])
                    )
                ),
                None,
            )
            if existing_active is not None:
                stage_rows = list(
                    session.execute(
                        select(DailyPipelineStageRun)
                        .where(DailyPipelineStageRun.pipeline_run_id == existing_active.id)
                        .order_by(DailyPipelineStageRun.id.asc())
                    ).scalars()
                )
                payload = self._serialize_run(existing_active, stage_rows)
                payload['deduplicated'] = True
                payload['deduplication_reason'] = 'overlapping_run_already_active'
                return payload

            if str(trigger or '').strip() == 'monitor_stale_continuation':
                existing = session.execute(
                    select(DailyPipelineRun)
                    .where(
                        DailyPipelineRun.market_date == normalized_market_date,
                        DailyPipelineRun.trigger == 'monitor_stale_continuation',
                        DailyPipelineRun.status.in_((*ACTIVE_DAILY_PIPELINE_STATUSES, 'completed')),
                    )
                    .order_by(DailyPipelineRun.started_at.desc(), DailyPipelineRun.id.desc())
                    .limit(1)
                ).scalar_one_or_none()
                if existing is not None:
                    stage_rows = list(
                        session.execute(
                            select(DailyPipelineStageRun)
                            .where(DailyPipelineStageRun.pipeline_run_id == existing.id)
                            .order_by(DailyPipelineStageRun.id.asc())
                        ).scalars()
                    )
                    payload = self._serialize_run(existing, stage_rows)
                    payload['deduplicated'] = True
                    payload['deduplication_reason'] = (
                        'recovery_already_completed'
                        if existing.status == 'completed'
                        else 'recovery_already_active'
                    )
                    return payload
            row = DailyPipelineRun(
                market_date=normalized_market_date,
                trigger=str(trigger or 'manual'),
                status='running',
                current_stage=None,
                started_at=now,
                completed_at=None,
                error_message=None,
                run_metadata=metadata,
            )
            session.add(row)
            session.flush()
            return self._serialize_run(row, [])

    def start_stage(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str | date,
        current_step: str | None = None,
        current_activity: str | None = None,
        processed: Any = 0,
        total: Any = 0,
        succeeded: Any = 0,
        failed: Any = 0,
        current_symbol: str | None = None,
        current_period: str | None = None,
        source_run_ids: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.update_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            status='running',
            current_step=current_step,
            current_activity=current_activity,
            processed=processed,
            total=total,
            succeeded=succeeded,
            failed=failed,
            current_symbol=current_symbol,
            current_period=current_period,
            source_run_ids=source_run_ids,
            details=details,
            mark_started=True,
        )

    def update_stage(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str | date,
        status: str = 'running',
        current_step: str | None = None,
        current_activity: str | None = None,
        processed: Any = None,
        total: Any = None,
        succeeded: Any = None,
        failed: Any = None,
        remaining: Any = None,
        current_symbol: str | None = None,
        current_period: str | None = None,
        error_message: str | None = None,
        source_run_ids: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
        completed_at: datetime | None = None,
        mark_started: bool = False,
        recompute_remaining: bool = False,
    ) -> dict[str, Any]:
        now = _utcnow()
        normalized_market_date = _normalize_market_date(market_date)
        normalized_status = str(status or 'running')
        terminal = normalized_status in TERMINAL_DAILY_PIPELINE_STATUSES
        with session_scope() as session:
            run_row = session.execute(
                select(DailyPipelineRun)
                .where(DailyPipelineRun.id == int(pipeline_run_id))
                .with_for_update()
            ).scalar_one_or_none()
            if run_row is None:
                raise ValueError(f'daily pipeline run not found: {pipeline_run_id}')
            if run_row.status not in ACTIVE_DAILY_PIPELINE_STATUSES:
                raise ValueError(
                    f'daily pipeline run is not active: {pipeline_run_id} ({run_row.status})'
                )
            row = session.execute(
                select(DailyPipelineStageRun).where(
                    DailyPipelineStageRun.pipeline_run_id == int(pipeline_run_id),
                    DailyPipelineStageRun.stage_id == stage_id,
                )
            ).scalar_one_or_none()
            if row is None:
                row = DailyPipelineStageRun(
                    pipeline_run_id=int(pipeline_run_id),
                    market_date=normalized_market_date,
                    stage_id=stage_id,
                    started_at=now,
                )
                session.add(row)
            elif mark_started and row.started_at is None:
                row.started_at = now
            resolved_remaining = remaining
            if remaining is None and not recompute_remaining and not terminal:
                resolved_remaining = row.remaining
            counts = _build_counts(
                processed=row.processed if processed is None else processed,
                total=row.total if total is None else total,
                succeeded=row.succeeded if succeeded is None else succeeded,
                failed=row.failed if failed is None else failed,
                remaining=resolved_remaining,
            )

            row.status = normalized_status
            row.processed = counts['processed']
            row.total = counts['total']
            row.succeeded = counts['succeeded']
            row.failed = counts['failed']
            row.remaining = counts['remaining']
            row.current_step = current_step
            row.current_activity = current_activity
            row.current_symbol = current_symbol
            row.current_period = current_period
            row.error_message = error_message
            if source_run_ids is not None:
                row.source_run_ids = dict(source_run_ids)
            row.completed_at = completed_at or (now if terminal else None)
            if details is not None:
                row.details = dict(details)
            if terminal:
                merged_details = dict(row.details or {})
                merged_details.update(
                    _build_stage_duration_details(
                        started_at=row.started_at,
                        completed_at=row.completed_at,
                    )
                )
                row.details = merged_details
            row.updated_at = now

            run_row.status = 'running' if normalized_status == 'running' else run_row.status
            run_row.current_stage = stage_id
            run_row.updated_at = now
            session.flush()
            return self._serialize_stage(row)

    def finish_stage(
        self,
        *,
        pipeline_run_id: int,
        stage_id: str,
        market_date: str | date,
        status: str,
        processed: Any = None,
        total: Any = None,
        succeeded: Any = None,
        failed: Any = None,
        current_step: str | None = None,
        current_activity: str | None = None,
        error_message: str | None = None,
        source_run_ids: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.update_stage(
            pipeline_run_id=pipeline_run_id,
            stage_id=stage_id,
            market_date=market_date,
            status=status,
            processed=processed,
            total=total,
            succeeded=succeeded,
            failed=failed,
            remaining=None,
            current_step=current_step,
            current_activity=current_activity,
            error_message=error_message,
            source_run_ids=source_run_ids,
            details=details,
            completed_at=_utcnow(),
            recompute_remaining=True,
        )

    def finish_run(
        self,
        *,
        pipeline_run_id: int,
        status: str,
        error_message: str | None = None,
        run_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _utcnow()
        with session_scope() as session:
            row = session.execute(
                select(DailyPipelineRun)
                .where(DailyPipelineRun.id == int(pipeline_run_id))
                .with_for_update()
            ).scalar_one_or_none()
            if row is None:
                raise ValueError(f'daily pipeline run not found: {pipeline_run_id}')
            if row.status not in ACTIVE_DAILY_PIPELINE_STATUSES:
                raise ValueError(
                    f'daily pipeline run is not active: {pipeline_run_id} ({row.status})'
                )
            metadata = dict(row.run_metadata or {})
            if run_metadata:
                metadata.update(dict(run_metadata))
            row.status = str(status or 'completed')
            row.error_message = error_message
            row.completed_at = now
            row.updated_at = now
            row.run_metadata = metadata
            stage_rows = list(
                session.execute(
                    select(DailyPipelineStageRun)
                    .where(DailyPipelineStageRun.pipeline_run_id == int(pipeline_run_id))
                    .order_by(DailyPipelineStageRun.id.asc())
                ).scalars()
            )
            session.flush()
            return self._serialize_run(row, stage_rows)

    def get_latest_run(
        self,
        *,
        market_date: str | date | None,
    ) -> dict[str, Any] | None:
        with session_scope() as session:
            statement = select(DailyPipelineRun)
            if market_date is not None:
                statement = statement.where(
                    DailyPipelineRun.market_date == _normalize_market_date(market_date)
                )
            row = session.execute(
                statement
                .order_by(DailyPipelineRun.started_at.desc(), DailyPipelineRun.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            stage_rows = list(
                session.execute(
                    select(DailyPipelineStageRun)
                    .where(DailyPipelineStageRun.pipeline_run_id == row.id)
                    .order_by(DailyPipelineStageRun.id.asc())
                ).scalars()
            )
            return self._serialize_run(row, stage_rows)

    def get_latest_run_for_context(
        self,
        *,
        market_date: str | date,
        triggers: Sequence[str] = (),
        entrypoints: Sequence[str] = (),
        search_limit: int = 100,
    ) -> dict[str, Any] | None:
        """Return the newest run produced by one of the requested entry points.

        A market date can also have manual or operations runs.  Those must not
        hide a failed automatic EOD continuation, so context matching happens
        before the associated stage rows are loaded.
        """

        normalized_market_date = _normalize_market_date(market_date)
        normalized_triggers = {str(item).strip() for item in triggers if str(item).strip()}
        normalized_entrypoints = {
            str(item).strip() for item in entrypoints if str(item).strip()
        }
        if not normalized_triggers and not normalized_entrypoints:
            raise ValueError('At least one trigger or entrypoint is required.')

        with session_scope() as session:
            context_filters = []
            if normalized_triggers:
                context_filters.append(
                    DailyPipelineRun.trigger.in_(sorted(normalized_triggers))
                )
            if normalized_entrypoints:
                context_filters.append(
                    DailyPipelineRun.run_metadata['entrypoint'].as_string().in_(
                        sorted(normalized_entrypoints)
                    )
                )
            rows = list(
                session.execute(
                    select(DailyPipelineRun)
                    .where(DailyPipelineRun.market_date == normalized_market_date)
                    .where(or_(*context_filters))
                    .order_by(DailyPipelineRun.started_at.desc(), DailyPipelineRun.id.desc())
                    .limit(max(1, int(search_limit)))
                ).scalars()
            )
            row = next(
                (
                    item
                    for item in rows
                    if item.trigger in normalized_triggers
                    or str((item.run_metadata or {}).get('entrypoint') or '').strip()
                    in normalized_entrypoints
                ),
                None,
            )
            if row is None:
                return None
            stage_rows = list(
                session.execute(
                    select(DailyPipelineStageRun)
                    .where(DailyPipelineStageRun.pipeline_run_id == row.id)
                    .order_by(DailyPipelineStageRun.id.asc())
                ).scalars()
            )
            return self._serialize_run(row, stage_rows)

    def count_runs(
        self,
        *,
        market_date: str | date,
        trigger: str,
    ) -> int:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_trigger = str(trigger or '').strip()
        if not normalized_trigger:
            raise ValueError('trigger is required when counting daily pipeline runs')
        with session_scope() as session:
            run_ids = session.execute(
                select(DailyPipelineRun.id).where(
                    DailyPipelineRun.market_date == normalized_market_date,
                    DailyPipelineRun.trigger == normalized_trigger,
                )
            ).scalars()
            return sum(1 for _ in run_ids)

    def mark_active_runs_interrupted(
        self,
        *,
        reason: str,
    ) -> dict[str, Any]:
        now = _utcnow()
        interrupted_run_ids: list[int] = []
        linked_eod_run_ids: set[int] = set()
        with session_scope() as session:
            run_rows = list(
                session.execute(
                    select(DailyPipelineRun).where(DailyPipelineRun.status.in_(ACTIVE_DAILY_PIPELINE_STATUSES))
                ).scalars()
            )
            for run_row in run_rows:
                interrupted_run_ids.append(int(run_row.id))
                metadata = dict(run_row.run_metadata or {})
                metadata['interrupted_reason'] = reason
                metadata['interrupted_at'] = now.isoformat()
                metadata['resumable'] = True
                run_row.status = 'interrupted'
                run_row.error_message = reason
                run_row.completed_at = now
                run_row.updated_at = now
                run_row.run_metadata = metadata

                stage_rows = list(
                    session.execute(
                        select(DailyPipelineStageRun).where(
                            DailyPipelineStageRun.pipeline_run_id == run_row.id,
                            DailyPipelineStageRun.status.in_(ACTIVE_DAILY_PIPELINE_STATUSES),
                        )
                    ).scalars()
                )
                for stage_row in stage_rows:
                    details = dict(stage_row.details or {})
                    details['interrupted_reason'] = reason
                    details['resumable'] = True
                    stage_row.status = 'interrupted'
                    stage_row.remaining = max(
                        _as_int(stage_row.total) - _as_int(stage_row.processed),
                        0,
                    )
                    stage_row.error_message = reason
                    stage_row.current_step = 'core_restart_interrupted'
                    stage_row.current_activity = reason
                    stage_row.completed_at = now
                    stage_row.updated_at = now
                    stage_row.details = details
                    source_run_ids = (
                        stage_row.source_run_ids
                        if isinstance(stage_row.source_run_ids, dict)
                        else {}
                    )
                    eod_run_id = _as_int(source_run_ids.get('eod_run_id'))
                    if eod_run_id > 0:
                        linked_eod_run_ids.add(eod_run_id)

            # The scheduler starts only after this transaction. Closing the
            # linked provider run now lets its normal scoped retry run on the
            # first poll instead of waiting for the stale-running threshold.
            interrupted_eod_run_ids: list[int] = []
            for eod_run_id in sorted(linked_eod_run_ids):
                eod_run = session.get(EODCollectionRun, eod_run_id)
                if eod_run is None or eod_run.status != 'running':
                    continue

                interrupted_eod_run_ids.append(eod_run_id)
                metadata = dict(eod_run.run_metadata or {})
                stage_progress = dict(metadata.get('stage_progress') or {})
                for stage_id, raw_progress in list(stage_progress.items()):
                    if not isinstance(raw_progress, dict):
                        continue
                    progress = dict(raw_progress)
                    if str(progress.get('status') or '').strip().lower() != 'running':
                        continue
                    progress.update(
                        {
                            'status': 'failed',
                            'current_step': 'core_restart_interrupted',
                            'current_activity': reason,
                            'completed_at': now.isoformat(),
                            'updated_at': now.isoformat(),
                        }
                    )
                    stage_progress[stage_id] = progress

                metadata.update(
                    {
                        'stage_progress': stage_progress,
                        'current_step': 'core_restart_interrupted',
                        'current_activity': reason,
                        'last_progress_at': now.isoformat(),
                        'interrupted_reason': reason,
                        'interrupted_at': now.isoformat(),
                        'resumable': True,
                    }
                )
                if isinstance(metadata.get('failed_symbols'), list):
                    metadata['failed_symbols_count'] = len(
                        metadata['failed_symbols']
                    )
                if isinstance(metadata.get('unattempted_symbols'), list):
                    metadata['unattempted_symbols_count'] = len(
                        metadata['unattempted_symbols']
                    )

                eod_run.status = 'failed'
                eod_run.symbols_succeeded = max(
                    _as_int(eod_run.symbols_succeeded),
                    0,
                )
                eod_run.symbols_failed = max(
                    _as_int(eod_run.symbols_failed),
                    max(
                        _as_int(eod_run.symbols_total)
                        - eod_run.symbols_succeeded,
                        0,
                    ),
                )
                eod_run.error_message = reason
                eod_run.completed_at = now
                eod_run.run_metadata = metadata
        return {
            'interrupted_runs': len(interrupted_run_ids),
            'run_ids': interrupted_run_ids,
            'interrupted_eod_runs': len(interrupted_eod_run_ids),
            'interrupted_eod_run_ids': interrupted_eod_run_ids,
            'reason': reason,
        }


daily_pipeline_run_state_service = DailyPipelineRunStateService()
