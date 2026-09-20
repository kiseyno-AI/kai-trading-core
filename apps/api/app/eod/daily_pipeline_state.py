from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import or_, select, text

from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    AgentContextFeedbackRun,
    DailyPipelineRun,
    DailyPipelineStageRun,
    DailyPriceBar,
    EODCollectionRun,
    Symbol,
)
from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_prefix_outcome_is_satisfied,
    terminal_continuation_marker_is_valid,
)
from app.repositories.eod_run_repository import (
    eod_collection_advisory_lock_keys,
)


ACTIVE_DAILY_PIPELINE_STATUSES = {'running'}
DAILY_PIPELINE_STATE_WRITE_TIMEOUT_MS = 15_000
STARTUP_RECOVERABLE_EOD_STATUSES = {
    'running',
    'completed',
    'completed_with_errors',
}
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
TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT = 6


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


def _parse_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc_datetime(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(
            value.strip().replace('Z', '+00:00')
        )
    except ValueError:
        return None
    return _as_utc_datetime(parsed)


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


def _terminal_continuation_execution_context_error(
    metadata: Mapping[str, Any],
    *,
    reused_eod_collection_run_id: int,
) -> str | None:
    """Return the first unsafe persisted input instead of normalizing it."""

    if (
        str(metadata.get('entrypoint') or '').strip()
        != 'daily_pipeline_stage_service.run'
    ):
        return 'entrypoint'
    if (
        _as_int(metadata.get('reused_eod_collection_run_id'))
        != reused_eod_collection_run_id
    ):
        return 'reused_eod_collection_run_id'

    raw_symbols = metadata.get('symbols')
    if (
        not isinstance(raw_symbols, list)
        or not raw_symbols
        or not all(
            isinstance(symbol, str) and symbol.strip()
            for symbol in raw_symbols
        )
    ):
        return 'symbols'
    raw_markets = metadata.get('markets')
    if (
        not isinstance(raw_markets, list)
        or not raw_markets
        or not all(
            isinstance(market, str) and market.strip()
            for market in raw_markets
        )
    ):
        return 'markets'
    if str(metadata.get('fs_div') or '').strip().upper() not in {
        'CFS',
        'OFS',
    }:
        return 'fs_div'

    for field in (
        'force',
        'bypass_prerequisite_gate',
        'reuse_existing_feedback',
        'point_in_time_recovery',
    ):
        if not isinstance(metadata.get(field), bool):
            return field

    integer_constraints = {
        'symbol_limit': (1, True),
        'symbol_offset': (0, False),
        'capture_limit': (1, False),
        'retry_cycles': (1, True),
        'feedback_max_cycles': (1, False),
        'feedback_outer_batch_size': (1, False),
    }
    for field, (minimum, optional) in integer_constraints.items():
        raw_value = metadata.get(field)
        if raw_value is None:
            if optional or field not in metadata:
                continue
            return field
        if isinstance(raw_value, bool):
            return field
        try:
            parsed_value = int(raw_value)
        except (TypeError, ValueError):
            return field
        if parsed_value < minimum:
            return field
    return None


def _resolve_terminal_summary_knowledge_cutoff(
    *,
    market_date: date,
    source_run_id: str,
    requested_symbols: Sequence[str],
    source_run_session: Any,
) -> dict[str, Any]:
    """Lock and resolve summary proof before spending the one-shot claim."""

    from app.agent_context.retry_service import (
        resolve_latest_completed_summary_knowledge_cutoff,
    )
    from app.agent_context.run_modes import (
        FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    )

    return resolve_latest_completed_summary_knowledge_cutoff(
        market_date=market_date,
        feedback_scope_mode=(
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
        ),
        source_run_id=source_run_id,
        requested_symbols=requested_symbols,
        source_run_session=source_run_session,
    )


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


def _stage_row_outcome(stage_row: object) -> dict[str, Any]:
    stage_id = str(getattr(stage_row, 'stage_id', '') or '').strip()
    return {
        'stage': stage_id,
        'stage_id': stage_id,
        'status': str(getattr(stage_row, 'status', '') or '').strip().lower(),
        'processed': _as_int(getattr(stage_row, 'processed', 0)),
        'total': _as_int(getattr(stage_row, 'total', 0)),
        'succeeded': _as_int(getattr(stage_row, 'succeeded', 0)),
        'failed': _as_int(getattr(stage_row, 'failed', 0)),
        'remaining': _as_int(getattr(stage_row, 'remaining', 0)),
        'completed_at': getattr(stage_row, 'completed_at', None),
        'error_message': getattr(stage_row, 'error_message', None),
        'details': (
            getattr(stage_row, 'details', {})
            if isinstance(getattr(stage_row, 'details', {}), Mapping)
            else {}
        ),
    }


def _expired_decision_skip_evidence_is_valid(
    *,
    stage_row: object,
    stage_id: str,
    market_date: date,
) -> bool:
    """Validate the exact no-child result emitted after an action window closes."""

    details = getattr(stage_row, 'details', {})
    result_summary = (
        details.get('result_summary')
        if isinstance(details, Mapping)
        and isinstance(details.get('result_summary'), Mapping)
        else {}
    )
    required_result = {
        'triggered': False,
        'status': 'skipped',
        'reason': 'next_open_window_expired',
        'market_date': market_date.isoformat(),
        'stage': stage_id,
    }
    allowed_result_keys = {
        *required_result,
        'target_trade_date',
        'current_market_date',
        'market_state',
        'completed_after_action_window',
    }
    completed_at = _as_utc_datetime(
        getattr(stage_row, 'completed_at', None)
    )
    if not (
        str(getattr(stage_row, 'status', '') or '').strip().lower()
        == 'skipped'
        and completed_at is not None
        and not str(getattr(stage_row, 'error_message', '') or '').strip()
        and str(getattr(stage_row, 'current_step', '') or '').strip()
        == f'{stage_id}_skipped'
        and all(
            _as_int(getattr(stage_row, field, 0)) == 0
            for field in (
                'processed',
                'total',
                'succeeded',
                'failed',
                'remaining',
            )
        )
        and isinstance(getattr(stage_row, 'source_run_ids', {}), Mapping)
        and not getattr(stage_row, 'source_run_ids', {})
        and set(result_summary).issubset(allowed_result_keys)
        and all(
            result_summary.get(key) == value
            for key, value in required_result.items()
        )
        and (
            result_summary.get('completed_after_action_window') is None
            or result_summary.get('completed_after_action_window') is True
        )
    ):
        return False

    try:
        target_trade_date = date.fromisoformat(
            str(result_summary.get('target_trade_date') or '')
        )
        current_market_date = date.fromisoformat(
            str(result_summary.get('current_market_date') or '')
        )
    except ValueError:
        return False
    market_state = str(
        result_summary.get('market_state') or ''
    ).strip().upper()
    if not (
        target_trade_date > market_date
        and market_state
        in {'PREOPEN', 'OPEN', 'CLOSED', 'WEEKEND', 'HOLIDAY'}
        and current_market_date
        == completed_at.astimezone(
            timezone(timedelta(hours=9))
        ).date()
        and (
            target_trade_date < current_market_date
            if market_state == 'PREOPEN'
            else target_trade_date <= current_market_date
        )
    ):
        return False
    return True


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
        with session_scope(
            statement_timeout_ms=DAILY_PIPELINE_STATE_WRITE_TIMEOUT_MS
        ) as session:
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
        with session_scope(
            statement_timeout_ms=DAILY_PIPELINE_STATE_WRITE_TIMEOUT_MS
        ) as session:
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
        reused_eod_collection_run_id: int | None = None,
    ) -> int:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_trigger = str(trigger or '').strip()
        if not normalized_trigger:
            raise ValueError('trigger is required when counting daily pipeline runs')
        filters = [
            DailyPipelineRun.market_date == normalized_market_date,
            DailyPipelineRun.trigger == normalized_trigger,
        ]
        if reused_eod_collection_run_id is not None:
            normalized_eod_run_id = int(reused_eod_collection_run_id)
            if normalized_eod_run_id <= 0:
                raise ValueError(
                    'reused_eod_collection_run_id must be greater than zero'
                )
            # Recovery budgets belong to one exact provider-run lineage.
            # Rows without this explicit link are intentionally excluded;
            # inferring legacy ownership by date could exhaust a newer run.
            filters.append(
                DailyPipelineRun.run_metadata[
                    'reused_eod_collection_run_id'
                ].as_integer()
                == normalized_eod_run_id
            )
        with session_scope() as session:
            run_ids = session.execute(
                select(DailyPipelineRun.id).where(*filters)
            ).scalars()
            return sum(1 for _ in run_ids)

    def claim_terminal_continuation(
        self,
        *,
        pipeline_run_id: int,
        market_date: str | date,
        reused_eod_collection_run_id: int,
        resume_from_stage: str = 'agent_feedback_summaries',
        reason: str = 'summary_knowledge_cutoff_repair',
    ) -> dict[str, Any]:
        """Atomically claim the one safe terminal continuation shape.

        This is deliberately narrower than a general-purpose retry. It only
        reopens an automatic EOD follow-up whose summaries completed before a
        decider-only missing-upstream failure, preserving the prior terminal
        evidence before resetting the summary/decision suffix in place.
        """

        normalized_pipeline_run_id = int(pipeline_run_id)
        normalized_eod_run_id = int(reused_eod_collection_run_id)
        if normalized_pipeline_run_id <= 0:
            raise ValueError('pipeline_run_id must be greater than zero')
        if normalized_eod_run_id <= 0:
            raise ValueError(
                'reused_eod_collection_run_id must be greater than zero'
            )
        normalized_market_date = _normalize_market_date(market_date)
        normalized_resume_stage = str(resume_from_stage or '').strip()
        normalized_reason = str(reason or '').strip()

        def reject(rejection_reason: str, **extra: Any) -> dict[str, Any]:
            return {
                'claimed': False,
                'pipeline_run_id': normalized_pipeline_run_id,
                'reason': rejection_reason,
                **extra,
            }

        if normalized_resume_stage != 'agent_feedback_summaries':
            return reject('unsupported_terminal_continuation_stage')
        if normalized_reason != 'summary_knowledge_cutoff_repair':
            return reject('unsupported_terminal_continuation_reason')

        continuation_suffix = (
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        )
        now = _utcnow()
        with session_scope(
            statement_timeout_ms=DAILY_PIPELINE_STATE_WRITE_TIMEOUT_MS
        ) as session:
            # Use the same market-date admission fence as start_run so a new
            # run cannot race this in-place terminal continuation claim.
            session.execute(
                text('SELECT pg_advisory_xact_lock(:source_key, :trade_key)'),
                {
                    'source_key': 20_001,
                    'trade_key': int(
                        normalized_market_date.strftime('%Y%m%d')
                    ),
                },
            )
            run_row = session.execute(
                select(DailyPipelineRun)
                .where(DailyPipelineRun.id == normalized_pipeline_run_id)
                .with_for_update()
            ).scalar_one_or_none()
            if run_row is None:
                return reject('pipeline_run_not_found')

            current_status = str(run_row.status or '').strip().lower()
            if run_row.market_date != normalized_market_date:
                return reject(
                    'market_date_mismatch',
                    status=current_status,
                )
            if str(run_row.trigger or '').strip() != 'auto_eod_followup_resume':
                return reject(
                    'trigger_mismatch',
                    status=current_status,
                )
            metadata = (
                dict(run_row.run_metadata)
                if isinstance(run_row.run_metadata, Mapping)
                else {}
            )
            if (
                current_status in {'running', 'interrupted'}
                and _as_int(metadata.get('terminal_continuation_count')) == 1
            ):
                return self._resume_claimed_terminal_continuation_locked(
                    session=session,
                    run_row=run_row,
                    metadata=metadata,
                    now=now,
                    normalized_pipeline_run_id=(
                        normalized_pipeline_run_id
                    ),
                    normalized_market_date=normalized_market_date,
                    normalized_eod_run_id=normalized_eod_run_id,
                    normalized_resume_stage=normalized_resume_stage,
                    normalized_reason=normalized_reason,
                    continuation_suffix=continuation_suffix,
                )
            if current_status != 'blocked':
                return reject(
                    'run_not_eligible_terminal_block',
                    status=current_status,
                )
            if (
                str(run_row.current_stage or '').strip()
                != 'signal_decider_evaluations'
            ):
                return reject(
                    'terminal_stage_mismatch',
                    status=current_status,
                )

            continuation_marker_keys = {
                'terminal_continuation_count',
                'terminal_continuation_version',
                'terminal_continuation_claimed_at',
                'terminal_continuation_reason',
                'terminal_continuation_evidence',
            }
            if continuation_marker_keys.intersection(metadata):
                return reject(
                    'terminal_continuation_already_claimed',
                    status=current_status,
                )
            execution_context_error = (
                _terminal_continuation_execution_context_error(
                    metadata,
                    reused_eod_collection_run_id=normalized_eod_run_id,
                )
            )
            if execution_context_error == 'entrypoint':
                return reject('entrypoint_mismatch', status=current_status)
            if execution_context_error == 'reused_eod_collection_run_id':
                return reject('eod_lineage_mismatch', status=current_status)
            if execution_context_error is not None:
                return reject(
                    'terminal_continuation_execution_context_invalid',
                    status=current_status,
                    field=execution_context_error,
                )

            raw_symbols = metadata['symbols']

            eod_run = session.get(EODCollectionRun, normalized_eod_run_id)
            if eod_run is None:
                return reject(
                    'eod_lineage_not_found',
                    status=current_status,
                )
            if (
                str(eod_run.market or '').strip().upper() != 'KRX'
                or str(eod_run.source or '').strip().lower() != 'pykrx'
                or eod_run.trade_date != normalized_market_date
            ):
                return reject(
                    'eod_lineage_context_mismatch',
                    status=current_status,
                )

            lineage_rows = list(
                session.execute(
                    select(DailyPipelineRun)
                    .where(
                        DailyPipelineRun.market_date
                        == normalized_market_date,
                        DailyPipelineRun.trigger
                        == 'auto_eod_followup_resume',
                        DailyPipelineRun.run_metadata[
                            'reused_eod_collection_run_id'
                        ].as_integer()
                        == normalized_eod_run_id,
                    )
                    .order_by(
                        DailyPipelineRun.started_at.asc(),
                        DailyPipelineRun.id.asc(),
                    )
                    .with_for_update()
                ).scalars()
            )
            lineage_run_ids = [int(item.id) for item in lineage_rows]
            lineage_run_count = len(lineage_run_ids)
            if (
                lineage_run_count
                != TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
            ):
                return reject(
                    'terminal_continuation_lineage_count_mismatch',
                    status=current_status,
                    expected_run_count=(
                        TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
                    ),
                    observed_run_count=lineage_run_count,
                )
            target_positions = [
                index + 1
                for index, run_id in enumerate(lineage_run_ids)
                if run_id == normalized_pipeline_run_id
            ]
            if (
                target_positions
                != [TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT]
                or lineage_run_ids[-1] != normalized_pipeline_run_id
            ):
                return reject(
                    'terminal_continuation_target_not_sixth_latest',
                    status=current_status,
                    target_ordinal=(
                        target_positions[0] if len(target_positions) == 1 else None
                    ),
                    latest_pipeline_run_id=lineage_run_ids[-1],
                )

            lineage_evidence: dict[str, Any] = {
                'reused_eod_collection_run_id': normalized_eod_run_id,
                'expected_run_count': (
                    TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
                ),
                'observed_run_count': lineage_run_count,
                'ordered_pipeline_run_ids': lineage_run_ids,
                'target_ordinal': target_positions[0],
                'latest_pipeline_run_id': lineage_run_ids[-1],
            }
            raw_repair_checkpoint_run_id = metadata.get(
                'repair_checkpoint_pipeline_run_id'
            )
            if raw_repair_checkpoint_run_id is not None:
                repair_checkpoint_run_id = _as_int(
                    raw_repair_checkpoint_run_id
                )
                if repair_checkpoint_run_id <= 0:
                    return reject(
                        'repair_checkpoint_provenance_invalid',
                        status=current_status,
                    )
                lineage_evidence['repair_checkpoint_pipeline_run_id'] = (
                    repair_checkpoint_run_id
                )

            active_rows = list(
                session.execute(
                    select(DailyPipelineRun)
                    .where(
                        DailyPipelineRun.market_date
                        == normalized_market_date,
                        DailyPipelineRun.status.in_(
                            ACTIVE_DAILY_PIPELINE_STATUSES
                        ),
                        DailyPipelineRun.id != normalized_pipeline_run_id,
                    )
                    .order_by(
                        DailyPipelineRun.started_at.desc(),
                        DailyPipelineRun.id.desc(),
                    )
                    .with_for_update()
                ).scalars()
            )
            if active_rows:
                return reject(
                    'overlapping_run_already_active',
                    status=current_status,
                    active_pipeline_run_ids=[
                        int(active_row.id) for active_row in active_rows
                    ],
                )

            raw_resolved_stages = metadata.get('resolved_stages')
            if (
                not isinstance(raw_resolved_stages, list)
                or any(
                    not isinstance(stage_id, str) or not stage_id.strip()
                    for stage_id in raw_resolved_stages
                )
            ):
                return reject(
                    'resolved_stages_unavailable',
                    status=current_status,
                )
            resolved_stages = [
                str(stage_id).strip() for stage_id in raw_resolved_stages
            ]
            if (
                len(resolved_stages) != len(set(resolved_stages))
                or tuple(resolved_stages[-len(continuation_suffix) :])
                != continuation_suffix
            ):
                return reject(
                    'terminal_continuation_suffix_mismatch',
                    status=current_status,
                )

            stage_rows = list(
                session.execute(
                    select(DailyPipelineStageRun)
                    .where(
                        DailyPipelineStageRun.pipeline_run_id
                        == normalized_pipeline_run_id
                    )
                    .order_by(DailyPipelineStageRun.id.asc())
                    .with_for_update()
                ).scalars()
            )
            stage_rows_by_id: dict[str, DailyPipelineStageRun] = {}
            for stage_row in stage_rows:
                stage_id = str(stage_row.stage_id or '').strip()
                if stage_id in stage_rows_by_id:
                    return reject(
                        'duplicate_stage_rows',
                        status=current_status,
                        duplicate_stage_id=stage_id,
                    )
                stage_rows_by_id[stage_id] = stage_row
            if set(stage_rows_by_id) != set(resolved_stages):
                return reject(
                    'resolved_stage_rows_mismatch',
                    status=current_status,
                    missing_stage_ids=[
                        stage_id
                        for stage_id in resolved_stages
                        if stage_id not in stage_rows_by_id
                    ],
                    unexpected_stage_ids=sorted(
                        set(stage_rows_by_id).difference(resolved_stages)
                    ),
                )

            prior_stage_statuses = metadata.get('stage_statuses')
            if not isinstance(prior_stage_statuses, Mapping) or any(
                str(prior_stage_statuses.get(stage_id) or '')
                .strip()
                .lower()
                != str(stage_rows_by_id[stage_id].status or '')
                .strip()
                .lower()
                for stage_id in resolved_stages
            ):
                return reject(
                    'stage_status_evidence_mismatch',
                    status=current_status,
                )

            resume_index = resolved_stages.index(normalized_resume_stage)
            incomplete_prefix_stages = [
                stage_id
                for stage_id in resolved_stages[:resume_index]
                if not daily_pipeline_prefix_outcome_is_satisfied(
                    _stage_row_outcome(stage_rows_by_id[stage_id])
                )
            ]
            if incomplete_prefix_stages:
                return reject(
                    'prefix_stages_not_satisfied',
                    status=current_status,
                    incomplete_stage_ids=incomplete_prefix_stages,
                )

            summary_stage = stage_rows_by_id['agent_feedback_summaries']
            if (
                str(summary_stage.status or '').strip().lower()
                != 'completed'
                or not daily_pipeline_outcome_is_satisfied(
                    _stage_row_outcome(summary_stage)
                )
            ):
                return reject(
                    'summary_stage_not_completed',
                    status=current_status,
                )
            summary_source_run_ids = (
                summary_stage.source_run_ids
                if isinstance(summary_stage.source_run_ids, Mapping)
                else {}
            )
            summary_source_ids = {
                str(summary_source_run_ids.get(key) or '').strip()
                for key in ('agent_feedback_run_id', 'run_id')
                if str(summary_source_run_ids.get(key) or '').strip()
            }
            if len(summary_source_ids) != 1:
                return reject(
                    'summary_source_run_id_unavailable',
                    status=current_status,
                )

            decider_stage = stage_rows_by_id['signal_decider']
            decider_details = (
                decider_stage.details
                if isinstance(decider_stage.details, Mapping)
                else {}
            )
            decider_result = (
                decider_details.get('result_summary')
                if isinstance(
                    decider_details.get('result_summary'), Mapping
                )
                else {}
            )
            decider_progress = (
                decider_result.get('progress')
                if isinstance(decider_result.get('progress'), Mapping)
                else {}
            )
            decider_summary = (
                decider_progress.get('summary')
                if isinstance(decider_progress.get('summary'), Mapping)
                else {}
            )
            failure_groups = decider_summary.get('failure_groups')
            missing_upstream_group = (
                failure_groups.get('dependency/missing-upstream')
                if isinstance(failure_groups, Mapping)
                and len(failure_groups) == 1
                and isinstance(
                    failure_groups.get('dependency/missing-upstream'),
                    Mapping,
                )
                else {}
            )
            failure_count = _as_int(missing_upstream_group.get('count'))
            retryable_count = _as_int(
                missing_upstream_group.get('retryable_count')
            )
            failure_kinds = missing_upstream_group.get('failure_kinds')
            decider_failure_is_safe = bool(
                str(decider_stage.status or '').strip().lower() == 'failed'
                and str(decider_result.get('status') or '')
                .strip()
                .lower()
                == 'failed'
                and isinstance(failure_groups, Mapping)
                and len(failure_groups) == 1
                and str(
                    missing_upstream_group.get('failure_status') or ''
                )
                .strip()
                .lower()
                == 'dependency/missing-upstream'
                and str(
                    missing_upstream_group.get('failure_category') or ''
                )
                .strip()
                .lower()
                == 'dependency'
                and failure_count > 0
                and retryable_count == failure_count
                and _as_int(decider_summary.get('failed_items'))
                == failure_count
                and isinstance(failure_kinds, list)
                and failure_kinds == ['dependency_missing_upstream']
            )
            if not decider_failure_is_safe:
                return reject(
                    'signal_decider_failure_not_retryable_missing_upstream_only',
                    status=current_status,
                )

            next_open_stage = stage_rows_by_id[
                'next_open_execution_plan'
            ]
            next_open_details = (
                next_open_stage.details
                if isinstance(next_open_stage.details, Mapping)
                else {}
            )
            next_open_result = (
                next_open_details.get('result_summary')
                if isinstance(
                    next_open_details.get('result_summary'), Mapping
                )
                else {}
            )
            if not (
                str(next_open_stage.status or '').strip().lower()
                == 'blocked'
                and str(next_open_result.get('status') or '')
                .strip()
                .lower()
                == 'blocked'
                and next_open_result.get('triggered') is False
                and str(next_open_result.get('reason') or '').strip()
                == 'signal_decider_stage_incomplete'
                and str(next_open_result.get('upstream_status') or '')
                .strip()
                .lower()
                == 'failed'
            ):
                return reject(
                    'next_open_block_reason_mismatch',
                    status=current_status,
                )

            evaluation_stage = stage_rows_by_id[
                'signal_decider_evaluations'
            ]
            evaluation_details = (
                evaluation_stage.details
                if isinstance(evaluation_stage.details, Mapping)
                else {}
            )
            evaluation_result = (
                evaluation_details.get('result_summary')
                if isinstance(
                    evaluation_details.get('result_summary'), Mapping
                )
                else {}
            )
            if not (
                str(evaluation_stage.status or '').strip().lower()
                == 'blocked'
                and str(evaluation_result.get('status') or '')
                .strip()
                .lower()
                == 'blocked'
                and evaluation_result.get('triggered') is False
                and str(evaluation_result.get('reason') or '').strip()
                == 'declared_dependencies_incomplete'
                and evaluation_result.get('incomplete_dependencies')
                == ['signal_decider']
            ):
                return reject(
                    'evaluation_block_reason_mismatch',
                    status=current_status,
                )

            summary_source_run_id = next(iter(summary_source_ids))
            cutoff_resolution = (
                _resolve_terminal_summary_knowledge_cutoff(
                    market_date=normalized_market_date,
                    source_run_id=summary_source_run_id,
                    requested_symbols=raw_symbols,
                    source_run_session=session,
                )
            )
            compact_cutoff_resolution = {
                'ready': bool(cutoff_resolution.get('ready')),
                'reason': cutoff_resolution.get('reason'),
                'knowledge_cutoff_at': cutoff_resolution.get(
                    'knowledge_cutoff_at'
                ),
                'source_run_id': cutoff_resolution.get('source_run_id'),
                'feedback_scope_mode': cutoff_resolution.get(
                    'feedback_scope_mode'
                ),
            }
            if not compact_cutoff_resolution['ready']:
                return reject(
                    'summary_knowledge_cutoff_unavailable',
                    status=current_status,
                    knowledge_cutoff_resolution={
                        key: value
                        for key, value in (
                            compact_cutoff_resolution.items()
                        )
                        if key != 'knowledge_cutoff_at'
                    },
                )
            if (
                compact_cutoff_resolution['source_run_id']
                != summary_source_run_id
                or compact_cutoff_resolution['knowledge_cutoff_at'] is None
            ):
                return reject(
                    'summary_knowledge_cutoff_proof_mismatch',
                    status=current_status,
                )
            summary_proof = {
                **compact_cutoff_resolution,
                'knowledge_cutoff_at': _iso(
                    compact_cutoff_resolution['knowledge_cutoff_at']
                ),
                'requested_symbols': list(raw_symbols),
                'requested_symbol_count': len(raw_symbols),
            }

            suffix_stage_evidence = {
                stage_id: self._serialize_stage(
                    stage_rows_by_id[stage_id]
                )
                for stage_id in continuation_suffix
            }
            prior_resume_from_stage = metadata.get('resume_from_stage')
            metadata.update(
                {
                    'resume_from_stage': normalized_resume_stage,
                    'terminal_continuation_count': 1,
                    'terminal_continuation_version': 1,
                    'terminal_continuation_claimed_at': now.isoformat(),
                    'terminal_continuation_reason': normalized_reason,
                    'terminal_continuation_evidence': {
                        'parent': {
                            'status': current_status,
                            'current_stage': run_row.current_stage,
                            'started_at': _iso(run_row.started_at),
                            'updated_at': _iso(run_row.updated_at),
                            'completed_at': _iso(run_row.completed_at),
                            'error_message': run_row.error_message,
                            'resume_from_stage': prior_resume_from_stage,
                            'stage_statuses': dict(
                                prior_stage_statuses
                            ),
                        },
                        'suffix_stages': suffix_stage_evidence,
                        'lineage': lineage_evidence,
                        'summary_proof': summary_proof,
                    },
                }
            )
            resumed_stage_statuses = dict(prior_stage_statuses)
            resumed_stage_statuses.update(
                {stage_id: 'pending' for stage_id in continuation_suffix}
            )
            metadata['stage_statuses'] = resumed_stage_statuses

            for stage_id in continuation_suffix:
                stage_row = stage_rows_by_id[stage_id]
                stage_row.status = 'pending'
                stage_row.processed = 0
                stage_row.total = 0
                stage_row.succeeded = 0
                stage_row.failed = 0
                stage_row.remaining = 0
                stage_row.current_step = 'terminal_continuation_pending'
                stage_row.current_activity = (
                    'Pending terminal continuation from '
                    f'{normalized_resume_stage}'
                )
                stage_row.current_symbol = None
                stage_row.current_period = None
                stage_row.started_at = now
                stage_row.updated_at = now
                stage_row.completed_at = None
                stage_row.error_message = None
                stage_row.source_run_ids = {}
                stage_row.details = {}

            run_row.status = 'running'
            run_row.current_stage = normalized_resume_stage
            run_row.updated_at = now
            run_row.completed_at = None
            run_row.error_message = None
            run_row.run_metadata = metadata
            session.flush()

            pipeline_run = self._serialize_run(
                run_row,
                [stage_rows_by_id[stage_id] for stage_id in resolved_stages],
            )
            return {
                'claimed': True,
                'reason': normalized_reason,
                'pipeline_run_id': normalized_pipeline_run_id,
                'pipeline_run': pipeline_run,
                'knowledge_cutoff_resolution': (
                    compact_cutoff_resolution
                ),
            }

    def _resume_claimed_terminal_continuation_locked(
        self,
        *,
        session: Any,
        run_row: DailyPipelineRun,
        metadata: dict[str, Any],
        now: datetime,
        normalized_pipeline_run_id: int,
        normalized_market_date: date,
        normalized_eod_run_id: int,
        normalized_resume_stage: str,
        normalized_reason: str,
        continuation_suffix: tuple[str, ...],
    ) -> dict[str, Any]:
        """Reclaim only an abandoned, untouched one-shot continuation."""

        current_status = str(run_row.status or '').strip().lower()

        def reject(rejection_reason: str, **extra: Any) -> dict[str, Any]:
            return {
                'claimed': False,
                'pipeline_run_id': normalized_pipeline_run_id,
                'reason': rejection_reason,
                'status': current_status,
                **extra,
            }

        continuation_evidence = metadata.get(
            'terminal_continuation_evidence'
        )
        if not (
            _as_int(metadata.get('terminal_continuation_count')) == 1
            and _as_int(metadata.get('terminal_continuation_version')) == 1
            and str(
                metadata.get('terminal_continuation_reason') or ''
            ).strip()
            == normalized_reason
            and str(metadata.get('resume_from_stage') or '').strip()
            == normalized_resume_stage
            and _parse_utc_datetime(
                metadata.get('terminal_continuation_claimed_at')
            )
            is not None
            and isinstance(continuation_evidence, Mapping)
        ):
            return reject('terminal_continuation_claim_evidence_invalid')

        stale_after_minutes = max(
            1,
            int(settings.eod_auto_pykrx_stale_run_minutes or 30),
        )
        stale_cutoff = now - timedelta(minutes=stale_after_minutes)
        if current_status == 'running':
            last_activity_at = _as_utc_datetime(
                run_row.updated_at or run_row.started_at
            )
            if (
                last_activity_at is None
                or last_activity_at > stale_cutoff
            ):
                return reject(
                    'terminal_continuation_still_active',
                    last_activity_at=_iso(last_activity_at),
                    stale_after_minutes=stale_after_minutes,
                )
        elif current_status == 'interrupted':
            if not (
                metadata.get('resumable') is True
                and str(metadata.get('interrupted_reason') or '').strip()
                and _parse_utc_datetime(metadata.get('interrupted_at'))
                is not None
            ):
                return reject(
                    'terminal_continuation_interruption_evidence_invalid'
                )
        else:
            return reject('run_not_eligible_terminal_continuation_resume')

        execution_context_error = (
            _terminal_continuation_execution_context_error(
                metadata,
                reused_eod_collection_run_id=normalized_eod_run_id,
            )
        )
        if execution_context_error is not None:
            return reject(
                'terminal_continuation_execution_context_invalid',
                field=execution_context_error,
            )
        raw_symbols = metadata['symbols']

        eod_run = session.get(EODCollectionRun, normalized_eod_run_id)
        if not (
            eod_run is not None
            and str(eod_run.market or '').strip().upper() == 'KRX'
            and str(eod_run.source or '').strip().lower() == 'pykrx'
            and eod_run.trade_date == normalized_market_date
        ):
            return reject('eod_lineage_context_mismatch')

        lineage_rows = list(
            session.execute(
                select(DailyPipelineRun)
                .where(
                    DailyPipelineRun.market_date
                    == normalized_market_date,
                    DailyPipelineRun.trigger
                    == 'auto_eod_followup_resume',
                    DailyPipelineRun.run_metadata[
                        'reused_eod_collection_run_id'
                    ].as_integer()
                    == normalized_eod_run_id,
                )
                .order_by(
                    DailyPipelineRun.started_at.asc(),
                    DailyPipelineRun.id.asc(),
                )
                .with_for_update()
            ).scalars()
        )
        lineage_run_ids = [int(item.id) for item in lineage_rows]
        if (
            len(lineage_run_ids)
            != TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
            or lineage_run_ids[-1] != normalized_pipeline_run_id
        ):
            return reject(
                'terminal_continuation_lineage_changed',
                observed_pipeline_run_ids=lineage_run_ids,
            )

        lineage_evidence = continuation_evidence.get('lineage')
        if not isinstance(lineage_evidence, Mapping) or not (
            _as_int(lineage_evidence.get('reused_eod_collection_run_id'))
            == normalized_eod_run_id
            and _as_int(lineage_evidence.get('expected_run_count'))
            == TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
            and _as_int(lineage_evidence.get('observed_run_count'))
            == TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
            and lineage_evidence.get('ordered_pipeline_run_ids')
            == lineage_run_ids
            and _as_int(lineage_evidence.get('target_ordinal'))
            == TERMINAL_CONTINUATION_EXPECTED_LINEAGE_RUN_COUNT
            and _as_int(lineage_evidence.get('latest_pipeline_run_id'))
            == normalized_pipeline_run_id
        ):
            return reject('terminal_continuation_lineage_evidence_invalid')

        active_rows = list(
            session.execute(
                select(DailyPipelineRun)
                .where(
                    DailyPipelineRun.market_date
                    == normalized_market_date,
                    DailyPipelineRun.status.in_(
                        ACTIVE_DAILY_PIPELINE_STATUSES
                    ),
                    DailyPipelineRun.id != normalized_pipeline_run_id,
                )
                .order_by(
                    DailyPipelineRun.started_at.desc(),
                    DailyPipelineRun.id.desc(),
                )
                .with_for_update()
            ).scalars()
        )
        if active_rows:
            return reject(
                'overlapping_run_already_active',
                active_pipeline_run_ids=[
                    int(active_row.id) for active_row in active_rows
                ],
            )

        raw_resolved_stages = metadata.get('resolved_stages')
        if not isinstance(raw_resolved_stages, list):
            return reject('resolved_stages_unavailable')
        resolved_stages = [
            str(stage_id or '').strip()
            for stage_id in raw_resolved_stages
            if str(stage_id or '').strip()
        ]
        if (
            len(resolved_stages) != len(raw_resolved_stages)
            or len(resolved_stages) != len(set(resolved_stages))
            or tuple(resolved_stages[-len(continuation_suffix) :])
            != continuation_suffix
        ):
            return reject('terminal_continuation_suffix_mismatch')

        stage_rows = list(
            session.execute(
                select(DailyPipelineStageRun)
                .where(
                    DailyPipelineStageRun.pipeline_run_id
                    == normalized_pipeline_run_id
                )
                .order_by(DailyPipelineStageRun.id.asc())
                .with_for_update()
            ).scalars()
        )
        stage_rows_by_id = {
            str(stage_row.stage_id or '').strip(): stage_row
            for stage_row in stage_rows
        }
        if (
            len(stage_rows_by_id) != len(stage_rows)
            or set(stage_rows_by_id) != set(resolved_stages)
        ):
            return reject('resolved_stage_rows_mismatch')

        stage_statuses = metadata.get('stage_statuses')
        if not isinstance(stage_statuses, Mapping):
            return reject('stage_status_evidence_mismatch')

        resume_index = resolved_stages.index(normalized_resume_stage)
        if any(
            not daily_pipeline_prefix_outcome_is_satisfied(
                _stage_row_outcome(stage_rows_by_id[stage_id])
            )
            for stage_id in resolved_stages[:resume_index]
        ):
            return reject('prefix_stages_not_satisfied')

        def untouched_claim_stage(stage_row: DailyPipelineStageRun) -> bool:
            return bool(
                str(stage_row.status or '').strip().lower() == 'pending'
                and _as_int(stage_row.processed) == 0
                and _as_int(stage_row.total) == 0
                and _as_int(stage_row.succeeded) == 0
                and _as_int(stage_row.failed) == 0
                and _as_int(stage_row.remaining) == 0
                and str(stage_row.current_step or '').strip()
                == 'terminal_continuation_pending'
                and stage_row.completed_at is None
                and not str(stage_row.error_message or '').strip()
                and isinstance(stage_row.source_run_ids, Mapping)
                and not stage_row.source_run_ids
                and isinstance(stage_row.details, Mapping)
                and not stage_row.details
            )

        if any(
            str(stage_statuses.get(stage_id) or '').strip().lower()
            != str(stage_rows_by_id[stage_id].status or '').strip().lower()
            for stage_id in resolved_stages[:resume_index]
        ):
            return reject('stage_status_evidence_mismatch')

        satisfied_prefix: list[str] = []
        for stage_id in continuation_suffix:
            stage_row = stage_rows_by_id[stage_id]
            if not (
                daily_pipeline_outcome_is_satisfied(
                    _stage_row_outcome(stage_row)
                )
                and stage_row.completed_at is not None
                and not str(stage_row.error_message or '').strip()
            ):
                break
            satisfied_prefix.append(stage_id)

        if len(satisfied_prefix) == len(continuation_suffix):
            return reject('terminal_continuation_suffix_already_satisfied')

        first_unfinished_index = len(satisfied_prefix)
        first_unfinished_stage = continuation_suffix[
            first_unfinished_index
        ]
        first_unfinished_row = stage_rows_by_id[first_unfinished_stage]
        first_unfinished_status = str(
            first_unfinished_row.status or ''
        ).strip().lower()
        current_stage = str(run_row.current_stage or '').strip()

        metadata_prefix_size = 0
        metadata_pending_seen = False
        for stage_id in continuation_suffix:
            persisted_status = str(
                stage_statuses.get(stage_id) or ''
            ).strip().lower()
            if persisted_status == 'pending':
                metadata_pending_seen = True
                continue
            if metadata_pending_seen:
                return reject('stage_status_evidence_mismatch')
            live_status = str(
                stage_rows_by_id[stage_id].status or ''
            ).strip().lower()
            if (
                not persisted_status
                or persisted_status != live_status
                or not daily_pipeline_outcome_is_satisfied(
                    _stage_row_outcome(stage_rows_by_id[stage_id])
                )
            ):
                return reject('stage_status_evidence_mismatch')
            metadata_prefix_size += 1
        if metadata_prefix_size > len(satisfied_prefix):
            return reject('stage_status_evidence_mismatch')

        feedback_stage_activity_at: datetime | None = None
        if first_unfinished_status == 'pending':
            expected_current_stages = {first_unfinished_stage}
            if satisfied_prefix:
                expected_current_stages.add(satisfied_prefix[-1])
            first_unfinished_is_valid = bool(
                untouched_claim_stage(first_unfinished_row)
                and current_stage in expected_current_stages
            )
        elif first_unfinished_status == 'running':
            first_unfinished_is_valid = bool(
                current_status == 'running'
                and current_stage == first_unfinished_stage
                and first_unfinished_row.completed_at is None
                and not str(
                    first_unfinished_row.error_message or ''
                ).strip()
            )
            if (
                first_unfinished_is_valid
                and first_unfinished_stage
                in {'agent_feedback_summaries', 'signal_decider'}
            ):
                feedback_stage_activity_at = _as_utc_datetime(
                    first_unfinished_row.updated_at
                    or first_unfinished_row.started_at
                )
        elif first_unfinished_status == 'interrupted':
            interrupted_details = (
                first_unfinished_row.details
                if isinstance(first_unfinished_row.details, Mapping)
                else {}
            )
            first_unfinished_is_valid = bool(
                current_status == 'interrupted'
                and current_stage == first_unfinished_stage
                and str(first_unfinished_row.current_step or '').strip()
                == 'core_restart_interrupted'
                and first_unfinished_row.completed_at is not None
                and str(
                    first_unfinished_row.error_message or ''
                ).strip()
                and interrupted_details.get('resumable') is True
                and str(
                    interrupted_details.get('interrupted_reason') or ''
                ).strip()
            )
            if (
                first_unfinished_is_valid
                and first_unfinished_stage
                in {'agent_feedback_summaries', 'signal_decider'}
            ):
                feedback_stage_activity_at = _as_utc_datetime(
                    first_unfinished_row.updated_at
                    or first_unfinished_row.started_at
                )
        else:
            first_unfinished_is_valid = False

        untouched_tail = all(
            untouched_claim_stage(stage_rows_by_id[stage_id])
            for stage_id in continuation_suffix[
                first_unfinished_index + 1 :
            ]
        )
        if not first_unfinished_is_valid or not untouched_tail:
            rejection_reason = (
                'terminal_continuation_suffix_already_started'
                if first_unfinished_status == 'pending'
                and not untouched_claim_stage(first_unfinished_row)
                else 'terminal_continuation_stage_progress_invalid'
            )
            return reject(
                rejection_reason,
                first_unfinished_stage=first_unfinished_stage,
                first_unfinished_status=first_unfinished_status,
            )
        if (
            first_unfinished_status in {'running', 'interrupted'}
            and first_unfinished_stage
            in {'agent_feedback_summaries', 'signal_decider'}
            and (
                first_unfinished_status == 'running'
                or feedback_stage_activity_at is None
                or feedback_stage_activity_at > stale_cutoff
            )
        ):
            return reject(
                'terminal_continuation_feedback_stage_still_active',
                first_unfinished_stage=first_unfinished_stage,
                last_activity_at=_iso(feedback_stage_activity_at),
                stale_after_minutes=stale_after_minutes,
            )

        parent_evidence = continuation_evidence.get('parent')
        suffix_evidence = continuation_evidence.get('suffix_stages')
        summary_proof = continuation_evidence.get('summary_proof')
        if not (
            isinstance(parent_evidence, Mapping)
            and str(parent_evidence.get('status') or '').strip().lower()
            == 'blocked'
            and str(parent_evidence.get('current_stage') or '').strip()
            == 'signal_decider_evaluations'
            and isinstance(suffix_evidence, Mapping)
            and set(suffix_evidence) == set(continuation_suffix)
            and isinstance(summary_proof, Mapping)
            and summary_proof.get('ready') is True
        ):
            return reject('terminal_continuation_evidence_invalid')

        archived_summary = suffix_evidence['agent_feedback_summaries']
        archived_decider = suffix_evidence['signal_decider']
        archived_plan = suffix_evidence['next_open_execution_plan']
        archived_evaluations = suffix_evidence[
            'signal_decider_evaluations'
        ]
        archived_summary_sources = (
            archived_summary.get('source_run_ids')
            if isinstance(archived_summary, Mapping)
            and isinstance(archived_summary.get('source_run_ids'), Mapping)
            else {}
        )
        archived_summary_source_ids = {
            str(archived_summary_sources.get(key) or '').strip()
            for key in ('agent_feedback_run_id', 'run_id')
            if str(archived_summary_sources.get(key) or '').strip()
        }
        archived_decider_result = (
            archived_decider.get('details', {}).get('result_summary')
            if isinstance(archived_decider, Mapping)
            and isinstance(archived_decider.get('details'), Mapping)
            and isinstance(
                archived_decider.get('details', {}).get('result_summary'),
                Mapping,
            )
            else {}
        )
        archived_decider_progress = (
            archived_decider_result.get('progress')
            if isinstance(archived_decider_result.get('progress'), Mapping)
            else {}
        )
        archived_decider_summary = (
            archived_decider_progress.get('summary')
            if isinstance(archived_decider_progress.get('summary'), Mapping)
            else {}
        )
        archived_failure_groups = archived_decider_summary.get(
            'failure_groups'
        )
        archived_missing_upstream = (
            archived_failure_groups.get('dependency/missing-upstream')
            if isinstance(archived_failure_groups, Mapping)
            and len(archived_failure_groups) == 1
            and isinstance(
                archived_failure_groups.get('dependency/missing-upstream'),
                Mapping,
            )
            else {}
        )
        archived_failure_count = _as_int(
            archived_missing_upstream.get('count')
        )
        archived_plan_result = (
            archived_plan.get('details', {}).get('result_summary')
            if isinstance(archived_plan, Mapping)
            and isinstance(archived_plan.get('details'), Mapping)
            and isinstance(
                archived_plan.get('details', {}).get('result_summary'),
                Mapping,
            )
            else {}
        )
        archived_evaluation_result = (
            archived_evaluations.get('details', {}).get('result_summary')
            if isinstance(archived_evaluations, Mapping)
            and isinstance(archived_evaluations.get('details'), Mapping)
            and isinstance(
                archived_evaluations.get('details', {}).get(
                    'result_summary'
                ),
                Mapping,
            )
            else {}
        )
        if not (
            isinstance(archived_summary, Mapping)
            and str(archived_summary.get('status') or '')
            .strip()
            .lower()
            == 'completed'
            and daily_pipeline_outcome_is_satisfied(
                {
                    **dict(archived_summary),
                    'stage': 'agent_feedback_summaries',
                }
            )
            and isinstance(archived_decider, Mapping)
            and str(archived_decider.get('status') or '')
            .strip()
            .lower()
            == 'failed'
            and str(archived_decider_result.get('status') or '')
            .strip()
            .lower()
            == 'failed'
            and isinstance(archived_failure_groups, Mapping)
            and len(archived_failure_groups) == 1
            and str(
                archived_missing_upstream.get('failure_status') or ''
            ).strip().lower()
            == 'dependency/missing-upstream'
            and str(
                archived_missing_upstream.get('failure_category') or ''
            ).strip().lower()
            == 'dependency'
            and archived_failure_count > 0
            and _as_int(
                archived_missing_upstream.get('retryable_count')
            )
            == archived_failure_count
            and _as_int(archived_decider_summary.get('failed_items'))
            == archived_failure_count
            and archived_missing_upstream.get('failure_kinds')
            == ['dependency_missing_upstream']
            and isinstance(archived_plan, Mapping)
            and str(archived_plan.get('status') or '')
            .strip()
            .lower()
            == 'blocked'
            and str(archived_plan_result.get('status') or '')
            .strip()
            .lower()
            == 'blocked'
            and archived_plan_result.get('triggered') is False
            and str(archived_plan_result.get('reason') or '').strip()
            == 'signal_decider_stage_incomplete'
            and str(
                archived_plan_result.get('upstream_status') or ''
            ).strip().lower()
            == 'failed'
            and isinstance(archived_evaluations, Mapping)
            and str(archived_evaluations.get('status') or '')
            .strip()
            .lower()
            == 'blocked'
            and str(archived_evaluation_result.get('status') or '')
            .strip()
            .lower()
            == 'blocked'
            and archived_evaluation_result.get('triggered') is False
            and str(
                archived_evaluation_result.get('reason') or ''
            ).strip()
            == 'declared_dependencies_incomplete'
            and archived_evaluation_result.get('incomplete_dependencies')
            == ['signal_decider']
        ):
            return reject('terminal_continuation_archived_suffix_invalid')

        summary_source_run_id = str(
            summary_proof.get('source_run_id') or ''
        ).strip()
        persisted_cutoff = _parse_utc_datetime(
            summary_proof.get('knowledge_cutoff_at')
        )
        if not (
            summary_source_run_id
            and persisted_cutoff is not None
            and archived_summary_source_ids == {summary_source_run_id}
        ):
            return reject('summary_knowledge_cutoff_proof_mismatch')

        cutoff_resolution = _resolve_terminal_summary_knowledge_cutoff(
            market_date=normalized_market_date,
            source_run_id=summary_source_run_id,
            requested_symbols=raw_symbols,
            source_run_session=session,
        )
        resolved_cutoff = _parse_utc_datetime(
            cutoff_resolution.get('knowledge_cutoff_at')
        )
        compact_cutoff_resolution = {
            'ready': bool(cutoff_resolution.get('ready')),
            'reason': cutoff_resolution.get('reason'),
            'knowledge_cutoff_at': resolved_cutoff,
            'source_run_id': cutoff_resolution.get('source_run_id'),
            'feedback_scope_mode': cutoff_resolution.get(
                'feedback_scope_mode'
            ),
        }
        if not (
            compact_cutoff_resolution['ready']
            and compact_cutoff_resolution['source_run_id']
            == summary_source_run_id
            and resolved_cutoff == persisted_cutoff
            and compact_cutoff_resolution['feedback_scope_mode']
            == summary_proof.get('feedback_scope_mode')
            and isinstance(
                summary_proof.get('requested_symbol_count'), int
            )
            and not isinstance(
                summary_proof.get('requested_symbol_count'), bool
            )
            and summary_proof.get('requested_symbol_count')
            == len(raw_symbols)
            and summary_proof.get('requested_symbols') == raw_symbols
        ):
            return reject('summary_knowledge_cutoff_proof_mismatch')

        feedback_stage_expectations = {
            'agent_feedback_summaries': (
                list(SUMMARIZER_AGENT_NAMES),
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            ),
            'signal_decider': (
                list(DECISION_SUPPORT_AGENT_NAMES),
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            ),
        }
        for stage_id in satisfied_prefix:
            stage_row = stage_rows_by_id[stage_id]
            stage_status = str(stage_row.status or '').strip().lower()
            if stage_status == 'skipped':
                if not _expired_decision_skip_evidence_is_valid(
                    stage_row=stage_row,
                    stage_id=stage_id,
                    market_date=normalized_market_date,
                ):
                    return reject(
                        'terminal_continuation_feedback_prefix_evidence_invalid',
                        stage_id=stage_id,
                        field='safe_skip_context',
                    )
                continue
            expectation = feedback_stage_expectations.get(stage_id)
            if expectation is None:
                continue
            if stage_status != 'completed':
                return reject(
                    'terminal_continuation_feedback_prefix_evidence_invalid',
                    stage_id=stage_id,
                    field='status',
                )
            source_run_ids = (
                stage_row.source_run_ids
                if isinstance(stage_row.source_run_ids, Mapping)
                else {}
            )
            child_source_ids = {
                str(source_run_ids.get(key) or '').strip()
                for key in ('agent_feedback_run_id', 'run_id')
                if str(source_run_ids.get(key) or '').strip()
            }
            if len(child_source_ids) != 1:
                return reject(
                    'terminal_continuation_feedback_prefix_evidence_invalid',
                    stage_id=stage_id,
                    field='source_run_id',
                )
            child_run_id = next(iter(child_source_ids))
            child_run = session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.run_id == child_run_id)
                .with_for_update()
            ).scalar_one_or_none()
            request_payload = (
                child_run.request_payload
                if child_run is not None
                and isinstance(child_run.request_payload, Mapping)
                else {}
            )
            expected_roles, expected_scope_mode = expectation
            expected_admission_scope = {
                role: list(raw_symbols) for role in expected_roles
            }
            role_symbol_scopes = request_payload.get('role_symbol_scopes')
            role_symbol_counts = request_payload.get('role_symbol_counts')
            requested_symbol_set = set(raw_symbols)
            role_scope_evidence_valid = bool(
                isinstance(role_symbol_scopes, Mapping)
                and isinstance(role_symbol_counts, Mapping)
                and set(role_symbol_scopes) == set(expected_roles)
                and set(role_symbol_counts) == set(expected_roles)
                and all(
                    isinstance(role_symbol_scopes.get(role), list)
                    and len(role_symbol_scopes[role])
                    == len(set(role_symbol_scopes[role]))
                    and all(
                        isinstance(symbol, str)
                        and symbol in requested_symbol_set
                        for symbol in role_symbol_scopes[role]
                    )
                    and isinstance(role_symbol_counts.get(role), int)
                    and not isinstance(role_symbol_counts[role], bool)
                    and role_symbol_counts[role]
                    == len(role_symbol_scopes[role])
                    for role in expected_roles
                )
            )
            scoped_symbol_total = (
                sum(role_symbol_counts.values())
                if role_scope_evidence_valid
                else -1
            )
            expected_force_recompute = bool(
                metadata['force']
                and not metadata['reuse_existing_feedback']
            )
            if not (
                child_run is not None
                and str(child_run.run_id or '').strip() == child_run_id
                and child_run.market_date == normalized_market_date
                and str(child_run.status or '').strip().lower()
                == 'completed'
                and child_run.completed_at is not None
                and _as_int(child_run.failed_count) == 0
                and role_scope_evidence_valid
                and _as_int(child_run.symbols_total) == scoped_symbol_total
                and _as_int(child_run.completed_count)
                == scoped_symbol_total
                and request_payload.get('market_date')
                == normalized_market_date.isoformat()
                and request_payload.get('request_run_id') == child_run_id
                and request_payload.get('mode')
                == DAILY_RETRY_CYCLE_MODE
                and request_payload.get('roles') == expected_roles
                and request_payload.get('requested_symbols')
                == raw_symbols
                and request_payload.get('feedback_scope_mode')
                == expected_scope_mode
                and request_payload.get('admission_scope_by_role')
                == expected_admission_scope
                and str(request_payload.get('fs_div') or '')
                .strip()
                .upper()
                == str(metadata['fs_div']).strip().upper()
                and request_payload.get('reuse_existing')
                is metadata['reuse_existing_feedback']
                and request_payload.get('force_recompute')
                is expected_force_recompute
                and _parse_utc_datetime(
                    request_payload.get('knowledge_cutoff_at')
                )
                == persisted_cutoff
                and _parse_utc_datetime(
                    request_payload.get('admission_knowledge_cutoff_at')
                )
                == persisted_cutoff
                and _parse_utc_datetime(
                    request_payload.get('requested_knowledge_cutoff_at')
                )
                == persisted_cutoff
            ):
                return reject(
                    'terminal_continuation_feedback_prefix_evidence_invalid',
                    stage_id=stage_id,
                    field='child_run_context',
                )

        resume_count = _as_int(
            metadata.get('terminal_continuation_resume_count')
        ) + 1
        persisted_stage_outcomes = {
            stage_id: _stage_row_outcome(stage_rows_by_id[stage_id])
            for stage_id in satisfied_prefix
        }
        resumed_stage_statuses = dict(stage_statuses)
        resumed_stage_statuses.update(
            {
                stage_id: str(
                    stage_rows_by_id[stage_id].status or ''
                ).strip().lower()
                for stage_id in satisfied_prefix
            }
        )
        resumed_stage_statuses.update(
            {
                stage_id: 'pending'
                for stage_id in continuation_suffix[
                    first_unfinished_index:
                ]
            }
        )
        metadata.update(
            {
                'terminal_continuation_resume_count': resume_count,
                'terminal_continuation_resumed_at': now.isoformat(),
                'terminal_continuation_resumed_from_status': (
                    current_status
                ),
                'terminal_continuation_resume_stage': (
                    first_unfinished_stage
                ),
                'terminal_continuation_completed_prefix': list(
                    satisfied_prefix
                ),
                'stage_statuses': resumed_stage_statuses,
            }
        )
        for stage_id in continuation_suffix[first_unfinished_index:]:
            stage_row = stage_rows_by_id[stage_id]
            stage_row.status = 'pending'
            stage_row.processed = 0
            stage_row.total = 0
            stage_row.succeeded = 0
            stage_row.failed = 0
            stage_row.remaining = 0
            stage_row.current_step = 'terminal_continuation_pending'
            stage_row.current_activity = (
                'Pending reclaimed terminal continuation from '
                f'{first_unfinished_stage}'
            )
            stage_row.current_symbol = None
            stage_row.current_period = None
            stage_row.started_at = now
            stage_row.updated_at = now
            stage_row.completed_at = None
            stage_row.error_message = None
            stage_row.source_run_ids = {}
            stage_row.details = {}

        run_row.status = 'running'
        run_row.current_stage = first_unfinished_stage
        run_row.updated_at = now
        run_row.completed_at = None
        run_row.error_message = None
        run_row.run_metadata = metadata
        session.flush()

        pipeline_run = self._serialize_run(
            run_row,
            [stage_rows_by_id[stage_id] for stage_id in resolved_stages],
        )
        return {
            'claimed': True,
            'resumed': True,
            'reason': 'terminal_continuation_reclaimed',
            'pipeline_run_id': normalized_pipeline_run_id,
            'pipeline_run': pipeline_run,
            'resume_from_stage': first_unfinished_stage,
            'persisted_stage_outcomes': persisted_stage_outcomes,
            'knowledge_cutoff_resolution': compact_cutoff_resolution,
        }

    def converge_terminal_stages_run(
        self,
        *,
        pipeline_run_id: int,
        allow_satisfied_terminal: bool = False,
    ) -> dict[str, Any]:
        """Finalize an orphaned parent from complete persisted stage evidence.

        The caller must own the provider admission fence before asking a
        still-running parent to converge. This closes the checkpoint window
        where every stage commit succeeded but the final parent commit did
        not. Automatic follow-up recovery may explicitly opt in to sealing an
        already-terminal satisfied parent as non-resumable while preserving
        its status and diagnostics.
        """
        normalized_pipeline_run_id = int(pipeline_run_id)
        now = _utcnow()
        with session_scope() as session:
            run_row = session.execute(
                select(DailyPipelineRun)
                .where(DailyPipelineRun.id == normalized_pipeline_run_id)
                .with_for_update()
            ).scalar_one_or_none()
            if run_row is None:
                return {
                    'updated': False,
                    'pipeline_run_id': normalized_pipeline_run_id,
                    'status': None,
                    'reason': 'run_not_found',
                }

            current_status = str(run_row.status or '').strip().lower()
            converging_interrupted_parent = current_status in {
                'interrupted',
                'running',
            }
            sealing_satisfied_terminal = bool(
                allow_satisfied_terminal
                and current_status in {'completed', 'partial', 'skipped'}
            )
            if not (
                converging_interrupted_parent
                or sealing_satisfied_terminal
            ):
                return {
                    'updated': False,
                    'pipeline_run_id': normalized_pipeline_run_id,
                    'status': current_status,
                    'reason': 'run_not_convergeable',
                }

            metadata = (
                dict(run_row.run_metadata)
                if isinstance(run_row.run_metadata, Mapping)
                else {}
            )
            raw_resolved_stages = metadata.get('resolved_stages')
            if (
                not isinstance(raw_resolved_stages, list)
                or not raw_resolved_stages
                or any(
                    not isinstance(stage_id, str) or not stage_id.strip()
                    for stage_id in raw_resolved_stages
                )
            ):
                return {
                    'updated': False,
                    'pipeline_run_id': normalized_pipeline_run_id,
                    'status': current_status,
                    'reason': 'resolved_stages_unavailable',
                }
            resolved_stages = list(
                dict.fromkeys(
                    str(stage_id).strip()
                    for stage_id in raw_resolved_stages
                )
            )

            stage_rows = list(
                session.execute(
                    select(DailyPipelineStageRun)
                    .where(
                        DailyPipelineStageRun.pipeline_run_id
                        == normalized_pipeline_run_id,
                        DailyPipelineStageRun.stage_id.in_(resolved_stages),
                    )
                    .with_for_update()
                ).scalars()
            )
            persisted_stage_outcomes = {
                str(stage_row.stage_id).strip(): _stage_row_outcome(stage_row)
                for stage_row in stage_rows
            }
            missing_stages = [
                stage_id
                for stage_id in resolved_stages
                if stage_id not in persisted_stage_outcomes
            ]
            if missing_stages:
                return {
                    'updated': False,
                    'pipeline_run_id': normalized_pipeline_run_id,
                    'status': current_status,
                    'reason': 'resolved_stage_rows_missing',
                    'missing_stages': missing_stages,
                }

            stage_statuses = {
                stage_id: str(persisted_stage_outcomes[stage_id]['status'])
                for stage_id in resolved_stages
            }
            outcome_is_satisfied = (
                daily_pipeline_prefix_outcome_is_satisfied
                if terminal_continuation_marker_is_valid(metadata)
                else daily_pipeline_outcome_is_satisfied
            )
            incomplete_stages = {
                stage_id: status
                for stage_id, status in stage_statuses.items()
                if not outcome_is_satisfied(
                    persisted_stage_outcomes[stage_id]
                )
            }
            if incomplete_stages:
                return {
                    'updated': False,
                    'pipeline_run_id': normalized_pipeline_run_id,
                    'status': current_status,
                    'reason': 'resolved_stages_incomplete',
                    'incomplete_stages': incomplete_stages,
                }

            if any(
                status == 'partial'
                for status in stage_statuses.values()
            ):
                terminal_status = 'partial'
            elif all(
                status == 'skipped'
                for status in stage_statuses.values()
            ):
                terminal_status = 'skipped'
            else:
                terminal_status = 'completed'
            if sealing_satisfied_terminal:
                if terminal_status != current_status:
                    return {
                        'updated': False,
                        'pipeline_run_id': normalized_pipeline_run_id,
                        'status': current_status,
                        'reason': 'terminal_status_mismatch',
                        'expected_status': terminal_status,
                    }
                if metadata.get('resumable') is False:
                    return {
                        'updated': False,
                        'pipeline_run_id': normalized_pipeline_run_id,
                        'status': current_status,
                        'reason': 'already_non_resumable',
                        'resolved_stages': resolved_stages,
                        'stage_statuses': stage_statuses,
                    }
                merged_stage_statuses = (
                    dict(metadata.get('stage_statuses') or {})
                    if isinstance(metadata.get('stage_statuses'), Mapping)
                    else {}
                )
                merged_stage_statuses.update(stage_statuses)
                metadata.update(
                    {
                        'resumable': False,
                        'terminal_satisfaction_confirmed_at': (
                            now.isoformat()
                        ),
                        'stage_statuses': merged_stage_statuses,
                    }
                )
                run_row.updated_at = now
                run_row.run_metadata = metadata
                return {
                    'updated': True,
                    'pipeline_run_id': normalized_pipeline_run_id,
                    'status': current_status,
                    'resolved_stages': resolved_stages,
                    'stage_statuses': stage_statuses,
                }

            metadata.update(
                {
                    'resumable': False,
                    'converged_at': now.isoformat(),
                    'converged_from_status': current_status,
                    'stage_statuses': stage_statuses,
                }
            )
            run_row.status = terminal_status
            run_row.error_message = None
            run_row.completed_at = now
            run_row.updated_at = now
            run_row.run_metadata = metadata

            return {
                'updated': True,
                'pipeline_run_id': normalized_pipeline_run_id,
                'status': terminal_status,
                'resolved_stages': resolved_stages,
                'stage_statuses': stage_statuses,
            }

    def link_eod_run(
        self,
        *,
        pipeline_run_id: int,
        eod_run_id: int,
    ) -> dict[str, int]:
        now = _utcnow()
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
                    f'daily pipeline run is not active: '
                    f'{pipeline_run_id} ({run_row.status})'
                )
            eod_run = session.get(EODCollectionRun, int(eod_run_id))
            if eod_run is None:
                raise ValueError(f'EOD collection run not found: {eod_run_id}')
            if eod_run.trade_date != run_row.market_date:
                raise ValueError(
                    'daily pipeline and EOD collection market dates do not match'
                )
            if (
                eod_run.market != 'KRX'
                or eod_run.source != 'pykrx'
                or eod_run.trigger != run_row.trigger
            ):
                raise ValueError(
                    'daily pipeline and EOD collection contexts do not match'
                )

            metadata = dict(run_row.run_metadata or {})
            metadata['eod_collection_run_id'] = int(eod_run_id)
            run_row.run_metadata = metadata
            run_row.updated_at = now
        return {
            'pipeline_run_id': int(pipeline_run_id),
            'eod_run_id': int(eod_run_id),
        }

    def mark_active_runs_interrupted(
        self,
        *,
        reason: str,
    ) -> dict[str, Any]:
        now = _utcnow()
        interrupted_run_ids: list[int] = []
        interrupted_eod_run_ids: list[int] = []
        augmented_eod_run_ids: list[int] = []
        provider_lock_owned_run_ids: list[int] = []
        ambiguous_eod_link_run_ids: list[int] = []
        with session_scope() as session:
            run_rows = list(
                session.execute(
                    select(DailyPipelineRun).where(
                        DailyPipelineRun.status.in_(
                            ACTIVE_DAILY_PIPELINE_STATUSES
                        )
                    )
                ).scalars()
            )
            for run_row in run_rows:
                expected_market_date = _normalize_market_date(run_row.market_date)
                stage_rows = list(
                    session.execute(
                        select(DailyPipelineStageRun).where(
                            DailyPipelineStageRun.pipeline_run_id == run_row.id
                        )
                    ).scalars()
                )
                run_metadata = (
                    run_row.run_metadata
                    if isinstance(run_row.run_metadata, dict)
                    else {}
                )
                has_eod_reference = _as_int(
                    run_metadata.get('eod_collection_run_id')
                ) > 0 or any(
                    _as_int(
                        (stage_row.source_run_ids or {}).get('eod_run_id')
                        if isinstance(stage_row.source_run_ids, dict)
                        else None
                    )
                    > 0
                    for stage_row in stage_rows
                )
                is_auto_pykrx_parent = bool(
                    run_metadata.get('mode') == 'daily_auto'
                    or run_metadata.get('entrypoint')
                    == 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
                    or run_row.trigger == 'auto_eod_followup_resume'
                )
                if has_eod_reference or is_auto_pykrx_parent:
                    source_key, trade_key = eod_collection_advisory_lock_keys(
                        market='KRX',
                        trade_date=expected_market_date,
                        source='pykrx',
                    )
                    lock_acquired = bool(
                        session.execute(
                            text(
                                'SELECT pg_try_advisory_xact_lock('
                                ':source_key, :trade_key)'
                            ),
                            {
                                'source_key': source_key,
                                'trade_key': trade_key,
                            },
                        ).scalar()
                    )
                    if not lock_acquired:
                        provider_lock_owned_run_ids.append(int(run_row.id))
                        continue
                    session.refresh(run_row, with_for_update=True)
                    for stage_row in stage_rows:
                        session.refresh(stage_row, with_for_update=True)
                    if run_row.status not in ACTIVE_DAILY_PIPELINE_STATUSES:
                        continue

                linked_eod_ids: list[int] = []
                metadata_eod_run_id = _as_int(
                    (run_row.run_metadata or {}).get('eod_collection_run_id')
                    if isinstance(run_row.run_metadata, dict)
                    else None
                )
                if metadata_eod_run_id > 0:
                    linked_eod_ids.append(metadata_eod_run_id)
                for stage_row in stage_rows:
                    source_run_ids = (
                        stage_row.source_run_ids
                        if isinstance(stage_row.source_run_ids, dict)
                        else {}
                    )
                    eod_run_id = _as_int(source_run_ids.get('eod_run_id'))
                    if eod_run_id > 0 and eod_run_id not in linked_eod_ids:
                        linked_eod_ids.append(eod_run_id)

                linked_eod_runs: list[EODCollectionRun] = []
                for eod_run_id in linked_eod_ids:
                    eod_run = session.get(EODCollectionRun, eod_run_id)
                    if eod_run is not None:
                        session.refresh(eod_run, with_for_update=True)
                    if (
                        eod_run is not None
                        and eod_run.status in STARTUP_RECOVERABLE_EOD_STATUSES
                        and eod_run.market == 'KRX'
                        and eod_run.source == 'pykrx'
                        and eod_run.trade_date == expected_market_date
                        and eod_run.trigger == run_row.trigger
                    ):
                        linked_eod_runs.append(eod_run)

                # Older runs may have crashed after creating the provider row
                # but before their first stage checkpoint persisted its ID.
                if not linked_eod_ids and is_auto_pykrx_parent:
                    fallback_eod_runs = list(
                        session.execute(
                            select(EODCollectionRun)
                            .where(
                                EODCollectionRun.market == 'KRX',
                                EODCollectionRun.source == 'pykrx',
                                EODCollectionRun.trade_date
                                == expected_market_date,
                                EODCollectionRun.trigger == run_row.trigger,
                                EODCollectionRun.status.in_(
                                    STARTUP_RECOVERABLE_EOD_STATUSES
                                ),
                                EODCollectionRun.run_metadata[
                                    'daily_pipeline_run_id'
                                ].as_integer()
                                == int(run_row.id),
                            )
                            .order_by(
                                EODCollectionRun.started_at.desc(),
                                EODCollectionRun.id.desc(),
                            )
                            .limit(2)
                        ).scalars()
                    )
                    if not fallback_eod_runs:
                        fallback_eod_runs = list(
                            session.execute(
                                select(EODCollectionRun)
                                .where(
                                    EODCollectionRun.market == 'KRX',
                                    EODCollectionRun.source == 'pykrx',
                                    EODCollectionRun.trade_date
                                    == expected_market_date,
                                    EODCollectionRun.trigger == run_row.trigger,
                                    EODCollectionRun.status == 'running',
                                )
                                .order_by(
                                    EODCollectionRun.started_at.desc(),
                                    EODCollectionRun.id.desc(),
                                )
                                .limit(2)
                            ).scalars()
                    )
                    if len(fallback_eod_runs) > 1:
                        ambiguous_eod_link_run_ids.append(int(run_row.id))
                        linked_eod_runs = []
                    else:
                        for eod_run in fallback_eod_runs:
                            session.refresh(eod_run, with_for_update=True)
                        linked_eod_runs = [
                            eod_run
                            for eod_run in fallback_eod_runs
                            if eod_run.status in STARTUP_RECOVERABLE_EOD_STATUSES
                            and eod_run.market == 'KRX'
                            and eod_run.source == 'pykrx'
                            and eod_run.trade_date == expected_market_date
                            and eod_run.trigger == run_row.trigger
                        ]

                if len({int(row.id) for row in linked_eod_runs}) > 1:
                    ambiguous_eod_link_run_ids.append(int(run_row.id))
                    linked_eod_runs = []

                resolved_stage_ids = {
                    str(stage_id).strip()
                    for stage_id in (
                        (run_row.run_metadata or {}).get('resolved_stages') or []
                    )
                    if str(stage_id).strip()
                }
                if not resolved_stage_ids:
                    resolved_stage_ids = {
                        str(stage_row.stage_id).strip()
                        for stage_row in stage_rows
                        if str(getattr(stage_row, 'stage_id', '')).strip()
                    }
                stage_outcomes = {
                    str(stage_row.stage_id).strip(): _stage_row_outcome(
                        stage_row
                    )
                    for stage_row in stage_rows
                    if str(getattr(stage_row, 'stage_id', '')).strip()
                }
                has_incomplete_downstream = any(
                    stage_id != 'price_collection'
                    and not daily_pipeline_outcome_is_satisfied(
                        stage_outcomes.get(stage_id)
                    )
                    for stage_id in resolved_stage_ids
                )

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

                for stage_row in stage_rows:
                    if stage_row.status not in ACTIVE_DAILY_PIPELINE_STATUSES:
                        continue
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
                if not linked_eod_runs:
                    continue
                eod_run = linked_eod_runs[0]
                eod_run_id = int(eod_run.id)
                metadata = dict(eod_run.run_metadata or {})
                symbols_total = max(_as_int(eod_run.symbols_total), 0)
                persisted_symbols = [
                    str(symbol).strip()
                    for symbol in session.execute(
                        select(Symbol.symbol)
                        .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                        .where(
                            DailyPriceBar.eod_run_id == eod_run_id,
                            DailyPriceBar.trade_date == expected_market_date,
                        )
                        .distinct()
                        .order_by(Symbol.symbol.asc())
                    ).scalars()
                    if str(symbol).strip()
                ]
                persisted_symbol_set = set(persisted_symbols)
                requested_symbols = list(
                    dict.fromkeys(
                        str(symbol).strip()
                        for symbol in (
                            metadata.get('requested_symbols') or []
                        )
                        if str(symbol).strip()
                    )
                )
                requested_symbol_set = set(requested_symbols)
                has_exact_requested_scope = bool(
                    requested_symbols
                    and len(requested_symbol_set) == symbols_total
                )
                unexpected_persisted_symbols = (
                    sorted(persisted_symbol_set - requested_symbol_set)
                    if has_exact_requested_scope
                    else []
                )
                metadata['unexpected_persisted_symbols'] = (
                    unexpected_persisted_symbols
                )
                metadata['unexpected_persisted_symbols_count'] = len(
                    unexpected_persisted_symbols
                )
                inherited_downstream_symbols = list(
                    dict.fromkeys(
                        str(symbol).strip()
                        for symbol in (
                            metadata.get(
                                'inherited_downstream_retry_symbols'
                            )
                            or []
                        )
                        if str(symbol).strip()
                    )
                )
                inherited_day_symbol_set: set[str] = set()
                if inherited_downstream_symbols:
                    inherited_day_symbol_set = {
                        str(symbol).strip()
                        for symbol in session.execute(
                            select(Symbol.symbol)
                            .join(
                                DailyPriceBar,
                                DailyPriceBar.symbol_id == Symbol.id,
                            )
                            .where(
                                DailyPriceBar.trade_date
                                == expected_market_date,
                                Symbol.symbol.in_(
                                    inherited_downstream_symbols
                                ),
                            )
                            .distinct()
                            .order_by(Symbol.symbol.asc())
                        ).scalars()
                        if str(symbol).strip()
                    }
                reconciled_inherited_downstream_symbols = [
                    symbol
                    for symbol in inherited_downstream_symbols
                    if symbol in inherited_day_symbol_set
                ]
                inherited_day_symbol_set = set(
                    reconciled_inherited_downstream_symbols
                )
                metadata['inherited_downstream_retry_symbols'] = (
                    reconciled_inherited_downstream_symbols
                )
                metadata['inherited_downstream_retry_symbols_count'] = len(
                    reconciled_inherited_downstream_symbols
                )
                reconciled_failed_symbols = [
                    str(symbol).strip()
                    for symbol in (metadata.get('failed_symbols') or [])
                    if str(symbol).strip()
                    and str(symbol).strip() not in persisted_symbol_set
                ]
                reconciled_unattempted_symbols = [
                    str(symbol).strip()
                    for symbol in (metadata.get('unattempted_symbols') or [])
                    if str(symbol).strip()
                    and str(symbol).strip() not in persisted_symbol_set
                ]
                known_retry_symbols = set(
                    [
                        *reconciled_failed_symbols,
                        *reconciled_unattempted_symbols,
                    ]
                )
                for symbol in requested_symbols:
                    if (
                        not symbol
                        or symbol in persisted_symbol_set
                        or symbol in known_retry_symbols
                    ):
                        continue
                    reconciled_unattempted_symbols.append(symbol)
                    known_retry_symbols.add(symbol)
                metadata['failed_symbols'] = reconciled_failed_symbols
                metadata['unattempted_symbols'] = (
                    reconciled_unattempted_symbols
                )
                price_retry_symbols = known_retry_symbols
                exact_symbols_succeeded = len(
                    requested_symbol_set & persisted_symbol_set
                )
                reconciled_symbols_succeeded = (
                    exact_symbols_succeeded
                    if has_exact_requested_scope
                    else min(
                        max(
                            _as_int(eod_run.symbols_succeeded),
                            len(persisted_symbols),
                        ),
                        symbols_total,
                    )
                )
                reconciled_symbols_failed = max(
                    symbols_total - reconciled_symbols_succeeded,
                    0,
                )
                requested_success_symbols = [
                    symbol
                    for symbol in requested_symbols
                    if symbol not in price_retry_symbols
                    and symbol in persisted_symbol_set
                ]
                existing_downstream_retry_symbols = list(
                    dict.fromkeys(
                        str(symbol).strip()
                        for symbol in (
                            metadata.get('downstream_retry_symbols') or []
                        )
                        if str(symbol).strip()
                    )
                )
                downstream_retry_symbols: list[str] = []
                seen_downstream_symbols: set[str] = set()
                for raw_symbol in [
                    *requested_success_symbols,
                    *existing_downstream_retry_symbols,
                    *persisted_symbols,
                ]:
                    symbol = str(raw_symbol).strip()
                    if (
                        not symbol
                        or symbol in price_retry_symbols
                        or (
                            symbol not in persisted_symbol_set
                            and symbol not in inherited_day_symbol_set
                        )
                        or (
                            has_exact_requested_scope
                            and symbol not in requested_symbol_set
                            and symbol not in inherited_day_symbol_set
                        )
                        or symbol in seen_downstream_symbols
                    ):
                        continue
                    seen_downstream_symbols.add(symbol)
                    downstream_retry_symbols.append(symbol)
                if not has_incomplete_downstream:
                    existing_downstream_retry_symbol_set = set(
                        existing_downstream_retry_symbols
                    )
                    downstream_retry_symbols = [
                        symbol
                        for symbol in downstream_retry_symbols
                        if symbol in existing_downstream_retry_symbol_set
                    ]

                stage_progress = dict(metadata.get('stage_progress') or {})
                for stage_id, raw_progress in list(stage_progress.items()):
                    if not isinstance(raw_progress, dict):
                        continue
                    progress = dict(raw_progress)
                    if (
                        str(progress.get('status') or '').strip().lower()
                        != 'running'
                    ):
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
                        'interrupted_reason': reason,
                        'interrupted_at': now.isoformat(),
                        'resumable': True,
                        'downstream_retry_symbols': downstream_retry_symbols,
                        'downstream_retry_symbols_count': len(
                            downstream_retry_symbols
                        ),
                        'collection_coverage_checked_at': now.isoformat(),
                        'collection_persisted_symbols_count': len(
                            persisted_symbols
                        ),
                        'collection_expected_symbols_count': symbols_total,
                        'collection_coverage_inconsistent': bool(
                            has_exact_requested_scope
                            and (
                                reconciled_symbols_failed
                                or unexpected_persisted_symbols
                            )
                        ),
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

                if eod_run.status == 'running':
                    interrupted_eod_run_ids.append(eod_run_id)
                    metadata.update(
                        {
                            'current_step': 'core_restart_interrupted',
                            'current_activity': reason,
                            'last_progress_at': now.isoformat(),
                        }
                    )
                    eod_run.symbols_succeeded = reconciled_symbols_succeeded
                    eod_run.symbols_failed = reconciled_symbols_failed
                    prices_complete = bool(
                        symbols_total > 0
                        and eod_run.symbols_succeeded == symbols_total
                        and not price_retry_symbols
                    )
                    eod_run.status = (
                        'completed_with_errors' if prices_complete else 'failed'
                    )
                    eod_run.error_message = reason
                    eod_run.completed_at = now
                else:
                    augmented_eod_run_ids.append(eod_run_id)
                    if has_exact_requested_scope:
                        eod_run.symbols_succeeded = (
                            reconciled_symbols_succeeded
                        )
                        eod_run.symbols_failed = reconciled_symbols_failed
                        if reconciled_symbols_failed:
                            eod_run.status = 'completed_with_errors'
                            if not eod_run.error_message:
                                eod_run.error_message = (
                                    'Persisted price coverage is incomplete '
                                    'for the admitted symbol scope after '
                                    'restart reconciliation.'
                                )
                    metadata.update(
                        {
                            'followup_interrupted_reason': reason,
                            'followup_interrupted_at': now.isoformat(),
                            'followup_recovery_required': bool(
                                downstream_retry_symbols
                            ),
                        }
                    )
                eod_run.run_metadata = metadata
        return {
            'interrupted_runs': len(interrupted_run_ids),
            'run_ids': interrupted_run_ids,
            'interrupted_eod_runs': len(interrupted_eod_run_ids),
            'interrupted_eod_run_ids': interrupted_eod_run_ids,
            'augmented_eod_run_ids': augmented_eod_run_ids,
            'provider_lock_owned_run_ids': provider_lock_owned_run_ids,
            'ambiguous_eod_link_run_ids': ambiguous_eod_link_run_ids,
            'reason': reason,
        }


daily_pipeline_run_state_service = DailyPipelineRunStateService()
