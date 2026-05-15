from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import select

from app.db import session_scope
from app.db_models import DailyPipelineRun, DailyPipelineStageRun


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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


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
    ) -> dict[str, Any]:
        now = _utcnow()
        normalized_market_date = _normalize_market_date(market_date)
        normalized_status = str(status or 'running')
        terminal = normalized_status in TERMINAL_DAILY_PIPELINE_STATUSES
        with session_scope() as session:
            run_row = session.get(DailyPipelineRun, int(pipeline_run_id))
            if run_row is None:
                raise ValueError(f'daily pipeline run not found: {pipeline_run_id}')
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
            counts = _build_counts(
                processed=row.processed if processed is None else processed,
                total=row.total if total is None else total,
                succeeded=row.succeeded if succeeded is None else succeeded,
                failed=row.failed if failed is None else failed,
                remaining=row.remaining if remaining is None else remaining,
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
            if details is not None:
                row.details = dict(details)
            row.completed_at = completed_at or (now if terminal else None)
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
            row = session.get(DailyPipelineRun, int(pipeline_run_id))
            if row is None:
                raise ValueError(f'daily pipeline run not found: {pipeline_run_id}')
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
        market_date: str | date,
    ) -> dict[str, Any] | None:
        normalized_market_date = _normalize_market_date(market_date)
        with session_scope() as session:
            row = session.execute(
                select(DailyPipelineRun)
                .where(DailyPipelineRun.market_date == normalized_market_date)
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

    def mark_active_runs_interrupted(
        self,
        *,
        reason: str,
    ) -> dict[str, Any]:
        now = _utcnow()
        interrupted_run_ids: list[int] = []
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
                    stage_row.error_message = reason
                    stage_row.current_step = 'core_restart_interrupted'
                    stage_row.current_activity = reason
                    stage_row.completed_at = now
                    stage_row.updated_at = now
                    stage_row.details = details
        return {
            'interrupted_runs': len(interrupted_run_ids),
            'run_ids': interrupted_run_ids,
            'reason': reason,
        }


daily_pipeline_run_state_service = DailyPipelineRunStateService()
