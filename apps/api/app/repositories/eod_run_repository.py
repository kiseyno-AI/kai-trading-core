from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import func, select, text

from app.db import engine, session_scope
from app.db_models import (
    DailyActionSummary,
    DailyPipelineRun,
    DailyPipelineStageRun,
    DailyPriceBar,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    EODCollectionRun,
    Symbol,
)
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied


logger = logging.getLogger(__name__)

_EOD_COLLECTION_LOCK_SOURCE_KEYS = {
    ('KRX', 'pykrx'): 10_001,
    ('KRX', 'kis'): 10_002,
}
_AUTO_DAILY_PIPELINE_ENTRYPOINT = (
    'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
)
_AUTO_DAILY_LEGACY_EOD_TRIGGERS = {
    'auto_close',
    'auto_close_scheduler',
}
_AUTO_DAILY_RECOVERY_TRIGGER = 'auto_eod_followup_resume'
_MAX_AUTO_PRICE_RECOVERY_RUNS_PER_TRADE_DATE = 5
# Mirrors the service admission limit without introducing a repository/service cycle.
_MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD = 5
_CONVERGENCE_ELIGIBLE_RECOVERY_STATUSES = {
    'completed',
    'interrupted',
    'partial',
    'running',
    'skipped',
}
_RECOVERABLE_DAILY_PIPELINE_STATUSES = {
    'running',
    'partial',
    'failed',
    'interrupted',
    'abandoned',
    'blocked',
    'waiting',
}


@dataclass
class _EODRecoveryCandidate:
    id: int
    trade_date: date
    trigger: str
    status: str
    symbols_total: int
    symbols_succeeded: int
    symbols_failed: int
    started_at: datetime
    run_metadata: dict[str, Any]


@dataclass
class _PipelineRecoveryCandidate:
    id: int
    market_date: date
    trigger: str
    status: str
    current_stage: str | None
    started_at: datetime
    run_metadata: dict[str, Any]


def _parse_trade_date(value: str) -> date:
    return date.fromisoformat(value)


def _normalize_trade_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return _parse_trade_date(value)


def eod_collection_advisory_lock_keys(
    *,
    market: str,
    trade_date: str | date,
    source: str,
) -> tuple[int, int]:
    normalized_trade_date = _normalize_trade_date(trade_date)
    source_key = _EOD_COLLECTION_LOCK_SOURCE_KEYS.get(
        (market, source),
        19_999,
    )
    return source_key, int(normalized_trade_date.strftime('%Y%m%d'))


def _serialize_eod_collection_run(row: EODCollectionRun) -> dict[str, Any]:
    return {
        'id': row.id,
        'market': row.market,
        'trade_date': row.trade_date.isoformat(),
        'source': row.source,
        'trigger': row.trigger,
        'status': row.status,
        'symbols_total': row.symbols_total,
        'symbols_succeeded': row.symbols_succeeded,
        'symbols_failed': row.symbols_failed,
        'error_message': row.error_message,
        'started_at': row.started_at.isoformat() if row.started_at else None,
        'completed_at': row.completed_at.isoformat() if row.completed_at else None,
        'run_metadata': row.run_metadata,
    }


def create_eod_collection_run(
    *,
    market: str,
    trade_date: str,
    symbols_total: int,
    trigger: str,
    source: str = 'kis',
    run_metadata: dict | None = None,
) -> int:
    with session_scope() as session:
        run = EODCollectionRun(
            market=market,
            trade_date=_parse_trade_date(trade_date),
            source=source,
            trigger=trigger,
            status='running',
            symbols_total=symbols_total,
            run_metadata=run_metadata,
        )
        session.add(run)
        session.flush()
        return run.id


def load_latest_eod_collection_run(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    statuses: Sequence[str] | None = None,
) -> dict | None:
    rows = list_eod_collection_runs(
        market=market,
        trade_date=trade_date,
        source=source,
        statuses=statuses,
        limit=1,
    )
    if not rows:
        return None
    return rows[0]


def list_eod_collection_runs(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    statuses: Sequence[str] | None = None,
    excluded_run_metadata_modes: Sequence[str] | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    with session_scope() as session:
        stmt = select(EODCollectionRun).where(
            EODCollectionRun.market == market,
            EODCollectionRun.trade_date == _parse_trade_date(trade_date),
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if statuses:
            stmt = stmt.where(EODCollectionRun.status.in_(statuses))
        if excluded_run_metadata_modes:
            metadata_mode = func.coalesce(
                EODCollectionRun.run_metadata['mode'].as_string(),
                '',
            )
            stmt = stmt.where(metadata_mode.not_in(excluded_run_metadata_modes))
        rows = session.execute(
            stmt.order_by(EODCollectionRun.started_at.desc(), EODCollectionRun.id.desc()).limit(limit)
        ).scalars()
        return [_serialize_eod_collection_run(row) for row in rows]


def load_most_complete_eod_collection_run(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    statuses: Sequence[str] | None = None,
    min_symbols_succeeded: int = 1,
) -> dict[str, Any] | None:
    with session_scope() as session:
        stmt = (
            select(EODCollectionRun)
            .where(EODCollectionRun.market == market)
            .where(EODCollectionRun.trade_date == _parse_trade_date(trade_date))
            .where(EODCollectionRun.symbols_succeeded >= min_symbols_succeeded)
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if statuses:
            stmt = stmt.where(EODCollectionRun.status.in_(statuses))
        row = session.execute(
            stmt.order_by(
                EODCollectionRun.symbols_succeeded.desc(),
                EODCollectionRun.symbols_total.desc(),
                EODCollectionRun.started_at.desc(),
                EODCollectionRun.id.desc(),
            ).limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None
        return _serialize_eod_collection_run(row)


def _metadata_list(metadata: dict[str, Any], key: str) -> list[Any]:
    value = metadata.get(key)
    return list(value) if isinstance(value, (list, tuple, set)) else []


def _metadata_positive_int(metadata: dict[str, Any], key: str) -> bool:
    try:
        return int(metadata.get(key) or 0) > 0
    except (TypeError, ValueError):
        return False


def _is_automatic_daily_eod_run(row: EODCollectionRun) -> bool:
    trigger = str(row.trigger or '').strip()
    metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
    mode = str(metadata.get('mode') or '').strip()
    if mode:
        return trigger.startswith('auto_') and mode == 'daily_auto'
    return trigger in _AUTO_DAILY_LEGACY_EOD_TRIGGERS


def _failed_eod_has_exact_complete_price_scope(row: EODCollectionRun) -> bool:
    if str(row.status or '').strip().lower() != 'failed':
        return False
    metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
    requested_symbols = {
        str(symbol).strip()
        for symbol in _metadata_list(metadata, 'requested_symbols')
        if str(symbol).strip()
    }
    try:
        symbols_total = max(int(row.symbols_total or 0), 0)
        symbols_succeeded = max(int(row.symbols_succeeded or 0), 0)
        symbols_failed = max(int(row.symbols_failed or 0), 0)
        persisted_count = int(metadata.get('collection_persisted_symbols_count'))
        expected_count = int(metadata.get('collection_expected_symbols_count'))
    except (TypeError, ValueError):
        return False
    return bool(
        symbols_total > 0
        and len(requested_symbols) == symbols_total
        and symbols_succeeded == symbols_total
        and symbols_failed == 0
        and persisted_count == symbols_total
        and expected_count == symbols_total
        and metadata.get('collection_coverage_checked_at')
        and metadata.get('collection_coverage_inconsistent') is False
        and not _metadata_list(metadata, 'unexpected_persisted_symbols')
        and not _metadata_list(metadata, 'failed_symbols')
        and not _metadata_list(metadata, 'unattempted_symbols')
    )


def _eod_has_price_recovery_scope(row: EODCollectionRun) -> bool:
    status = str(row.status or '').strip().lower()
    metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
    if status == 'running':
        return True
    if status == 'failed':
        return not _failed_eod_has_exact_complete_price_scope(row)
    if status != 'completed_with_errors':
        return False
    return bool(
        int(row.symbols_failed or 0) > 0
        or _metadata_list(metadata, 'failed_symbols')
        or _metadata_list(metadata, 'unattempted_symbols')
        or _metadata_positive_int(metadata, 'failed_symbols_count')
        or _metadata_positive_int(metadata, 'unattempted_symbols_count')
    )


def _eod_has_followup_recovery_scope(row: EODCollectionRun) -> bool:
    metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
    return bool(
        _metadata_list(metadata, 'downstream_retry_symbols')
        or _metadata_positive_int(metadata, 'downstream_retry_symbols_count')
        or metadata.get('followup_recovery_required') is True
    )


def _is_automatic_daily_pipeline_run(row: DailyPipelineRun) -> bool:
    trigger = str(row.trigger or '').strip()
    if not trigger.startswith('auto_'):
        return False
    metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
    return bool(
        trigger == _AUTO_DAILY_RECOVERY_TRIGGER
        or str(metadata.get('entrypoint') or '').strip()
        == _AUTO_DAILY_PIPELINE_ENTRYPOINT
    )


def _pipeline_run_links_to_eod(
    pipeline_run: DailyPipelineRun,
    eod_run: EODCollectionRun,
) -> bool:
    pipeline_metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
        else {}
    )
    explicit_eod_ids: list[int] = []
    for key in ('eod_collection_run_id', 'reused_eod_collection_run_id'):
        raw_value = pipeline_metadata.get(key)
        if raw_value is None:
            continue
        try:
            explicit_eod_ids.append(int(raw_value))
        except (TypeError, ValueError):
            return False
    if explicit_eod_ids:
        return set(explicit_eod_ids) == {int(eod_run.id)}

    eod_metadata = eod_run.run_metadata if isinstance(eod_run.run_metadata, dict) else {}
    try:
        return int(eod_metadata.get('daily_pipeline_run_id')) == int(
            pipeline_run.id
        )
    except (TypeError, ValueError):
        return False


def _pipeline_has_non_price_metadata_checkpoint(
    pipeline_run: DailyPipelineRun,
) -> bool:
    current_stage = str(pipeline_run.current_stage or '').strip()
    if current_stage and current_stage != 'price_collection':
        return True

    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
        else {}
    )
    for key in ('stage_statuses', 'stage_progress'):
        checkpoints = metadata.get(key)
        if not isinstance(checkpoints, dict):
            continue
        if any(
            str(stage_id or '').strip()
            and str(stage_id or '').strip() != 'price_collection'
            for stage_id in checkpoints
        ):
            return True
    return False


def _eod_recovery_candidate_statement(
    *,
    lower_bound: date,
    cutoff: date,
):
    normalized_trigger = func.trim(
        func.coalesce(EODCollectionRun.trigger, '')
    )
    return (
        select(
            EODCollectionRun.id.label('id'),
            EODCollectionRun.trade_date.label('trade_date'),
            EODCollectionRun.trigger.label('trigger'),
            EODCollectionRun.status.label('status'),
            EODCollectionRun.symbols_total.label('symbols_total'),
            EODCollectionRun.symbols_succeeded.label('symbols_succeeded'),
            EODCollectionRun.symbols_failed.label('symbols_failed'),
            EODCollectionRun.started_at.label('started_at'),
        )
        .where(
            EODCollectionRun.market == 'KRX',
            EODCollectionRun.source == 'pykrx',
            normalized_trigger.like(r'auto\_%', escape='\\'),
            EODCollectionRun.trade_date >= lower_bound,
            EODCollectionRun.trade_date <= cutoff,
        )
        .order_by(
            EODCollectionRun.trade_date.asc(),
            EODCollectionRun.started_at.desc(),
            EODCollectionRun.id.desc(),
        )
    )


def _build_eod_recovery_candidate(row: Any) -> _EODRecoveryCandidate:
    return _EODRecoveryCandidate(
        id=int(row.id),
        trade_date=row.trade_date,
        trigger=str(row.trigger or ''),
        status=str(row.status or ''),
        symbols_total=int(row.symbols_total or 0),
        symbols_succeeded=int(row.symbols_succeeded or 0),
        symbols_failed=int(row.symbols_failed or 0),
        started_at=row.started_at,
        run_metadata={},
    )


def _load_eod_recovery_modes(
    session: Any,
    *,
    candidates: Sequence[_EODRecoveryCandidate],
) -> None:
    candidate_by_id = {int(row.id): row for row in candidates}
    if not candidate_by_id:
        return
    metadata = EODCollectionRun.run_metadata
    rows = session.execute(
        select(
            EODCollectionRun.id,
            metadata['mode'].as_string().label('metadata_mode'),
        ).where(EODCollectionRun.id.in_(tuple(candidate_by_id)))
    )
    for row in rows:
        candidate_by_id[int(row.id)].run_metadata['mode'] = row.metadata_mode


def _load_eod_recovery_metadata(
    session: Any,
    *,
    candidates: Sequence[_EODRecoveryCandidate],
) -> None:
    candidate_by_id = {int(row.id): row for row in candidates}
    if not candidate_by_id:
        return
    metadata = EODCollectionRun.run_metadata
    rows = session.execute(
        select(
            EODCollectionRun.id,
            metadata['failed_symbols'].label('failed_symbols'),
            metadata['unattempted_symbols'].label('unattempted_symbols'),
            metadata['failed_symbols_count'].as_integer().label(
                'failed_symbols_count'
            ),
            metadata['unattempted_symbols_count'].as_integer().label(
                'unattempted_symbols_count'
            ),
            metadata['downstream_retry_symbols'].label(
                'downstream_retry_symbols'
            ),
            metadata['downstream_retry_symbols_count'].as_integer().label(
                'downstream_retry_symbols_count'
            ),
            metadata['followup_recovery_required'].as_boolean().label(
                'followup_recovery_required'
            ),
            metadata['daily_pipeline_run_id'].as_integer().label(
                'daily_pipeline_run_id'
            ),
        ).where(EODCollectionRun.id.in_(tuple(candidate_by_id)))
    )
    for row in rows:
        candidate_by_id[int(row.id)].run_metadata.update(
            {
                'failed_symbols': row.failed_symbols,
                'unattempted_symbols': row.unattempted_symbols,
                'failed_symbols_count': row.failed_symbols_count,
                'unattempted_symbols_count': row.unattempted_symbols_count,
                'downstream_retry_symbols': row.downstream_retry_symbols,
                'downstream_retry_symbols_count': (
                    row.downstream_retry_symbols_count
                ),
                'followup_recovery_required': (
                    row.followup_recovery_required
                ),
                'daily_pipeline_run_id': row.daily_pipeline_run_id,
            }
        )


def _failed_eod_needs_exact_complete_scope_check(
    row: _EODRecoveryCandidate,
) -> bool:
    return bool(
        str(row.status or '').strip().lower() == 'failed'
        and int(row.symbols_total or 0) > 0
        and int(row.symbols_succeeded or 0) == int(row.symbols_total or 0)
        and int(row.symbols_failed or 0) == 0
    )


def _load_failed_eod_exact_scope_metadata(
    session: Any,
    *,
    candidates: Sequence[_EODRecoveryCandidate],
) -> None:
    candidate_by_id = {int(row.id): row for row in candidates}
    if not candidate_by_id:
        return
    metadata = EODCollectionRun.run_metadata
    rows = session.execute(
        select(
            EODCollectionRun.id,
            metadata['requested_symbols'].label('requested_symbols'),
            metadata['collection_persisted_symbols_count'].as_integer().label(
                'collection_persisted_symbols_count'
            ),
            metadata['collection_expected_symbols_count'].as_integer().label(
                'collection_expected_symbols_count'
            ),
            metadata['collection_coverage_checked_at'].as_string().label(
                'collection_coverage_checked_at'
            ),
            metadata['collection_coverage_inconsistent'].as_boolean().label(
                'collection_coverage_inconsistent'
            ),
            metadata['unexpected_persisted_symbols'].label(
                'unexpected_persisted_symbols'
            ),
        ).where(EODCollectionRun.id.in_(tuple(candidate_by_id)))
    )
    for row in rows:
        candidate = candidate_by_id[int(row.id)]
        candidate.run_metadata.update(
            {
                'requested_symbols': row.requested_symbols,
                'collection_persisted_symbols_count': (
                    row.collection_persisted_symbols_count
                ),
                'collection_expected_symbols_count': (
                    row.collection_expected_symbols_count
                ),
                'collection_coverage_checked_at': (
                    row.collection_coverage_checked_at
                ),
                'collection_coverage_inconsistent': (
                    row.collection_coverage_inconsistent
                ),
                'unexpected_persisted_symbols': (
                    row.unexpected_persisted_symbols
                ),
            }
        )


def _load_pipeline_recovery_candidates(
    session: Any,
    *,
    market_dates: Sequence[date],
) -> list[_PipelineRecoveryCandidate]:
    if not market_dates:
        return []
    metadata = DailyPipelineRun.run_metadata
    rows = session.execute(
        select(
            DailyPipelineRun.id,
            DailyPipelineRun.market_date,
            DailyPipelineRun.trigger,
            DailyPipelineRun.status,
            DailyPipelineRun.current_stage,
            DailyPipelineRun.started_at,
            metadata['entrypoint'].as_string().label('entrypoint'),
            metadata['eod_collection_run_id'].as_integer().label(
                'eod_collection_run_id'
            ),
            metadata['reused_eod_collection_run_id'].as_integer().label(
                'reused_eod_collection_run_id'
            ),
            metadata['resumable'].as_boolean().label('resumable'),
            metadata['resolved_stages'].label('resolved_stages'),
        )
        .where(DailyPipelineRun.market_date.in_(tuple(market_dates)))
        .order_by(
            DailyPipelineRun.market_date.asc(),
            DailyPipelineRun.started_at.desc(),
            DailyPipelineRun.id.desc(),
        )
    )
    return [
        _PipelineRecoveryCandidate(
            id=int(row.id),
            market_date=row.market_date,
            trigger=str(row.trigger or ''),
            status=str(row.status or ''),
            current_stage=row.current_stage,
            started_at=row.started_at,
            run_metadata={
                'entrypoint': row.entrypoint,
                'eod_collection_run_id': row.eod_collection_run_id,
                'reused_eod_collection_run_id': (
                    row.reused_eod_collection_run_id
                ),
                'resumable': row.resumable,
                'resolved_stages': row.resolved_stages,
            },
        )
        for row in rows
    ]


def _load_pipeline_checkpoint_metadata(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
) -> None:
    metadata = DailyPipelineRun.run_metadata
    row = session.execute(
        select(
            metadata['stage_statuses'].label('stage_statuses'),
            metadata['stage_progress'].label('stage_progress'),
        ).where(DailyPipelineRun.id == int(pipeline_run.id))
    ).one()
    pipeline_run.run_metadata.update(
        {
            'stage_statuses': row.stage_statuses,
            'stage_progress': row.stage_progress,
        }
    )


def _count_auto_followup_recovery_attempts(
    session: Any,
    *,
    eod_run: _EODRecoveryCandidate,
) -> int:
    reused_eod_run_id = DailyPipelineRun.run_metadata[
        'reused_eod_collection_run_id'
    ].as_integer()
    return int(
        session.execute(
            select(func.count(DailyPipelineRun.id)).where(
                DailyPipelineRun.market_date == eod_run.trade_date,
                DailyPipelineRun.trigger == _AUTO_DAILY_RECOVERY_TRIGGER,
                reused_eod_run_id == int(eod_run.id),
            )
        ).scalar_one()
    )


def _latest_exact_followup_recovery(
    *,
    candidates: Sequence[_PipelineRecoveryCandidate],
    eod_run: _EODRecoveryCandidate,
) -> _PipelineRecoveryCandidate | None:
    for candidate in candidates:
        if candidate.trigger != _AUTO_DAILY_RECOVERY_TRIGGER:
            continue
        metadata = (
            candidate.run_metadata
            if isinstance(candidate.run_metadata, dict)
            else {}
        )
        try:
            if int(metadata.get('reused_eod_collection_run_id')) == int(
                eod_run.id
            ) and _pipeline_run_links_to_eod(candidate, eod_run):
                return candidate
        except (TypeError, ValueError):
            continue
    return None


def _pipeline_stage_outcome(stage_row: DailyPipelineStageRun) -> dict[str, Any]:
    stage_id = str(stage_row.stage_id or '').strip()
    return {
        'stage': stage_id,
        'stage_id': stage_id,
        'status': str(stage_row.status or '').strip().lower(),
        'processed': int(stage_row.processed or 0),
        'total': int(stage_row.total or 0),
        'succeeded': int(stage_row.succeeded or 0),
        'failed': int(stage_row.failed or 0),
        'error_message': stage_row.error_message,
        'details': (
            stage_row.details if isinstance(stage_row.details, dict) else {}
        ),
    }


def _followup_recovery_is_satisfied_for_convergence(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
) -> bool:
    if (
        str(pipeline_run.status or '').strip().lower()
        not in _CONVERGENCE_ELIGIBLE_RECOVERY_STATUSES
    ):
        return False
    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
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
        return False
    resolved_stages = tuple(
        dict.fromkeys(stage_id.strip() for stage_id in raw_resolved_stages)
    )
    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == int(pipeline_run.id),
                DailyPipelineStageRun.market_date == pipeline_run.market_date,
                DailyPipelineStageRun.stage_id.in_(resolved_stages),
            )
        ).scalars()
    )
    stage_rows_by_id = {
        str(stage_row.stage_id or '').strip(): stage_row
        for stage_row in stage_rows
    }
    if any(stage_id not in stage_rows_by_id for stage_id in resolved_stages):
        return False
    return all(
        daily_pipeline_outcome_is_satisfied(
            _pipeline_stage_outcome(stage_rows_by_id[stage_id])
        )
        for stage_id in resolved_stages
    )


def load_oldest_outstanding_auto_eod_recovery_date(
    *,
    on_or_after: str | date,
    on_or_before: str | date,
) -> str | None:
    """Return one oldest recent automatic EOD date with durable retry evidence.

    Only the newest automatic-daily KRX/PyKRX EOD row for each date is
    considered, so unrelated manual or historical work cannot hide a recovery
    candidate. Pipeline state and retry budgets are accepted only when their
    persisted EOD lineage matches exactly. Both date bounds are inclusive.
    """

    lower_bound = _normalize_trade_date(on_or_after)
    cutoff = _normalize_trade_date(on_or_before)
    with session_scope() as session:
        candidate_rows = [
            _build_eod_recovery_candidate(row)
            for row in session.execute(
                _eod_recovery_candidate_statement(
                    lower_bound=lower_bound,
                    cutoff=cutoff,
                )
            )
        ]
        _load_eod_recovery_modes(session, candidates=candidate_rows)
        possible_automatic_rows: list[_EODRecoveryCandidate] = []
        selected_trade_dates: set[date] = set()
        for row in candidate_rows:
            if row.trade_date in selected_trade_dates:
                continue
            if not _is_automatic_daily_eod_run(row):
                continue
            possible_automatic_rows.append(row)
            selected_trade_dates.add(row.trade_date)
        if not possible_automatic_rows:
            return None

        auto_attempt_counts = {
            trade_date: int(attempt_count)
            for trade_date, attempt_count in session.execute(
                select(
                    EODCollectionRun.trade_date,
                    func.count(EODCollectionRun.id),
                )
                .where(
                    EODCollectionRun.market == 'KRX',
                    EODCollectionRun.source == 'pykrx',
                    EODCollectionRun.trade_date.in_(
                        tuple(
                            row.trade_date
                            for row in possible_automatic_rows
                        )
                    ),
                    EODCollectionRun.trigger.like(
                        r'auto\_%',
                        escape='\\',
                    ),
                )
                .group_by(EODCollectionRun.trade_date)
            ).all()
        }

        for eod_row in possible_automatic_rows:
            _load_eod_recovery_metadata(
                session,
                candidates=[eod_row],
            )
            if _failed_eod_needs_exact_complete_scope_check(eod_row):
                _load_failed_eod_exact_scope_metadata(
                    session,
                    candidates=[eod_row],
                )
            eod_has_price_recovery_scope = _eod_has_price_recovery_scope(
                eod_row
            )
            if (
                eod_has_price_recovery_scope
                and auto_attempt_counts.get(eod_row.trade_date, 0)
                < _MAX_AUTO_PRICE_RECOVERY_RUNS_PER_TRADE_DATE
            ):
                return eod_row.trade_date.isoformat()
            if _eod_has_followup_recovery_scope(eod_row):
                followup_attempts = _count_auto_followup_recovery_attempts(
                    session,
                    eod_run=eod_row,
                )
                if followup_attempts < _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD:
                    return eod_row.trade_date.isoformat()
                pipeline_candidates = _load_pipeline_recovery_candidates(
                    session,
                    market_dates=[eod_row.trade_date],
                )
                latest_recovery = _latest_exact_followup_recovery(
                    candidates=pipeline_candidates,
                    eod_run=eod_row,
                )
                if (
                    latest_recovery is not None
                    and _followup_recovery_is_satisfied_for_convergence(
                        session,
                        pipeline_run=latest_recovery,
                    )
                ):
                    return eod_row.trade_date.isoformat()
                continue

            linked_pipeline = next(
                (
                    pipeline_row
                    for pipeline_row in _load_pipeline_recovery_candidates(
                        session,
                        market_dates=[eod_row.trade_date],
                    )
                    if _is_automatic_daily_pipeline_run(pipeline_row)
                    and _pipeline_run_links_to_eod(
                        pipeline_row,
                        eod_row,
                    )
                ),
                None,
            )
            pipeline_is_incomplete = bool(
                linked_pipeline is not None
                and str(linked_pipeline.status or '').strip().lower()
                in _RECOVERABLE_DAILY_PIPELINE_STATUSES
                and (
                    not isinstance(linked_pipeline.run_metadata, dict)
                    or linked_pipeline.run_metadata.get('resumable') is not False
                )
            )
            if pipeline_is_incomplete and eod_has_price_recovery_scope:
                has_non_price_stage_row = bool(
                    session.execute(
                        select(DailyPipelineStageRun.id)
                        .where(
                            DailyPipelineStageRun.pipeline_run_id
                            == int(linked_pipeline.id),
                            DailyPipelineStageRun.market_date
                            == linked_pipeline.market_date,
                            DailyPipelineStageRun.stage_id
                            != 'price_collection',
                        )
                        .limit(1)
                    ).scalar_one_or_none()
                )
                if (
                    not has_non_price_stage_row
                    and not _pipeline_has_non_price_metadata_checkpoint(
                        linked_pipeline
                    )
                ):
                    _load_pipeline_checkpoint_metadata(
                        session,
                        pipeline_run=linked_pipeline,
                    )
                    pipeline_is_incomplete = (
                        _pipeline_has_non_price_metadata_checkpoint(
                            linked_pipeline
                        )
                    )
            if (
                pipeline_is_incomplete
                and _count_auto_followup_recovery_attempts(
                    session,
                    eod_run=eod_row,
                )
                < _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
            ):
                return eod_row.trade_date.isoformat()
    return None


def count_eod_collection_runs(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    trigger_prefix: str | None = None,
) -> int:
    with session_scope() as session:
        stmt = select(func.count(EODCollectionRun.id)).where(
            EODCollectionRun.market == market,
            EODCollectionRun.trade_date == _parse_trade_date(trade_date),
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if trigger_prefix:
            escaped_prefix = trigger_prefix.replace('\\', '\\\\')
            escaped_prefix = escaped_prefix.replace('%', '\\%').replace('_', '\\_')
            stmt = stmt.where(EODCollectionRun.trigger.like(f'{escaped_prefix}%', escape='\\'))
        return int(session.execute(stmt).scalar_one())


def complete_eod_collection_run(
    run_id: int,
    *,
    status: str,
    symbols_succeeded: int,
    symbols_failed: int,
    error_message: str | None = None,
    run_metadata: dict | None = None,
) -> None:
    with session_scope() as session:
        run = session.get(EODCollectionRun, run_id)
        if run is None:
            return
        run.status = status
        run.symbols_succeeded = symbols_succeeded
        run.symbols_failed = symbols_failed
        run.error_message = error_message
        run.completed_at = datetime.now(timezone.utc)
        if run_metadata is not None:
            run.run_metadata = run_metadata


def update_eod_collection_run_progress(
    run_id: int,
    *,
    symbols_succeeded: int,
    symbols_failed: int,
    run_metadata: dict | None = None,
) -> None:
    with session_scope() as session:
        run = session.get(EODCollectionRun, run_id)
        if run is None:
            return
        run.symbols_succeeded = symbols_succeeded
        run.symbols_failed = symbols_failed
        if run_metadata is not None:
            run.run_metadata = run_metadata


def count_eod_run_persisted_symbols(
    run_id: int,
    *,
    trade_date: str | date | None = None,
) -> int:
    with session_scope() as session:
        statement = (
            select(func.count(func.distinct(DailyPriceBar.symbol_id)))
            .select_from(DailyPriceBar)
            .where(DailyPriceBar.eod_run_id == run_id)
        )
        if trade_date is not None:
            statement = statement.where(
                DailyPriceBar.trade_date == _normalize_trade_date(trade_date)
            )
        return int(
            session.execute(statement).scalar_one()
        )


def load_eod_run_persisted_symbols(
    run_id: int,
    *,
    trade_date: str | date | None = None,
) -> list[str]:
    with session_scope() as session:
        statement = (
            select(Symbol.symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .where(DailyPriceBar.eod_run_id == run_id)
            .group_by(Symbol.symbol)
            .order_by(Symbol.symbol.asc())
        )
        if trade_date is not None:
            statement = statement.where(
                DailyPriceBar.trade_date == _normalize_trade_date(trade_date)
            )
        rows = session.execute(statement).all()
        return [row[0] for row in rows]


def load_eod_run_stage_counts(run_id: int) -> dict[str, dict]:
    with session_scope() as session:
        price_persisted, price_updated_at = session.execute(
            select(
                func.count(DailyPriceBar.id),
                func.max(DailyPriceBar.captured_at),
            )
            .select_from(DailyPriceBar)
            .where(DailyPriceBar.eod_run_id == run_id)
        ).one()
        technical_persisted, technical_updated_at = session.execute(
            select(
                func.count(DailyTechnicalSnapshot.id),
                func.max(DailyTechnicalSnapshot.captured_at),
            )
            .select_from(DailyTechnicalSnapshot)
            .where(DailyTechnicalSnapshot.eod_run_id == run_id)
        ).one()
        action_persisted, action_updated_at = session.execute(
            select(
                func.count(DailyActionSummary.id),
                func.max(DailyActionSummary.captured_at),
            )
            .select_from(DailyActionSummary)
            .where(DailyActionSummary.eod_run_id == run_id)
        ).one()
        trending_rows, trending_updated_at = session.execute(
            select(
                func.count(DailyTrendingRank.id),
                func.max(DailyTrendingRank.captured_at),
            )
            .select_from(DailyTrendingRank)
            .where(DailyTrendingRank.eod_run_id == run_id)
        ).one()
        trending_periods = session.execute(
            select(func.count())
            .select_from(
                select(DailyTrendingRank.period)
                .where(DailyTrendingRank.eod_run_id == run_id)
                .group_by(DailyTrendingRank.period)
                .subquery()
            )
        ).scalar_one()

    return {
        'price_collection': {
            'persisted': int(price_persisted or 0),
            'updated_at': price_updated_at.isoformat() if price_updated_at else None,
        },
        'technical_snapshots': {
            'persisted': int(technical_persisted or 0),
            'updated_at': technical_updated_at.isoformat() if technical_updated_at else None,
        },
        'action_summaries': {
            'persisted': int(action_persisted or 0),
            'updated_at': action_updated_at.isoformat() if action_updated_at else None,
        },
        'trending_ranks': {
            'persisted': int(trending_rows or 0),
            'periods_persisted': int(trending_periods or 0),
            'updated_at': trending_updated_at.isoformat() if trending_updated_at else None,
        },
    }


@contextmanager
def advisory_eod_collection_lock(*, market: str, trade_date: str, source: str):
    source_key, trade_key = eod_collection_advisory_lock_keys(
        market=market,
        trade_date=trade_date,
        source=source,
    )

    # PostgreSQL session-level advisory locks survive transaction boundaries.
    # Run the lock statements in AUTOCOMMIT so this dedicated connection does
    # not sit idle in a transaction while the (potentially multi-hour) EOD job
    # is doing work on other connections. Otherwise PostgreSQL can terminate
    # it via idle_in_transaction_session_timeout before the unlock statement.
    with engine.connect() as raw_connection:
        connection = raw_connection.execution_options(isolation_level='AUTOCOMMIT')
        acquired = bool(
            connection.execute(
                text('SELECT pg_try_advisory_lock(:source_key, :trade_key)'),
                {
                    'source_key': source_key,
                    'trade_key': trade_key,
                },
            ).scalar()
        )
        try:
            yield acquired
        finally:
            if acquired:
                try:
                    connection.execute(
                        text('SELECT pg_advisory_unlock(:source_key, :trade_key)'),
                        {
                            'source_key': source_key,
                            'trade_key': trade_key,
                        },
                    )
                except Exception:
                    # Closing the dedicated PostgreSQL session releases any
                    # remaining session-level advisory lock. Do not replace a
                    # pipeline result/error with a cleanup-only failure.
                    logger.warning(
                        'Failed to explicitly release EOD advisory lock; '
                        'closing the dedicated connection will release it.',
                        exc_info=True,
                    )


def load_recent_daily_price_trade_dates(
    *,
    end_date: str | date,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []

    normalized_end_date = _normalize_trade_date(end_date)
    with session_scope() as session:
        rows = session.execute(
            select(DailyPriceBar.trade_date)
            .where(DailyPriceBar.trade_date <= normalized_end_date)
            .group_by(DailyPriceBar.trade_date)
            .order_by(DailyPriceBar.trade_date.desc())
            .limit(limit)
        ).all()

    return [row.trade_date.isoformat() for row in reversed(rows)]


def count_daily_price_bars_for_date(trade_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count())
                .select_from(DailyPriceBar)
                .where(DailyPriceBar.trade_date == _normalize_trade_date(trade_date))
            ).scalar_one()
        )


def load_latest_available_daily_price_trade_date() -> str | None:
    with session_scope() as session:
        value = session.execute(select(func.max(DailyPriceBar.trade_date))).scalar_one()
        return value.isoformat() if value else None


def load_latest_available_daily_technical_snapshot_date() -> str | None:
    with session_scope() as session:
        value = session.execute(select(func.max(DailyTechnicalSnapshot.market_date))).scalar_one()
        return value.isoformat() if value else None


def load_latest_available_daily_action_summary_date() -> str | None:
    with session_scope() as session:
        value = session.execute(select(func.max(DailyActionSummary.market_date))).scalar_one()
        return value.isoformat() if value else None


def count_live_symbols() -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(Symbol.id))
                .select_from(Symbol)
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )


def count_daily_price_bars_for_live_symbols(trade_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyPriceBar.symbol_id)))
                .select_from(DailyPriceBar)
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(DailyPriceBar.trade_date == _normalize_trade_date(trade_date))
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )


def count_daily_technical_snapshots_for_live_symbols(market_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyTechnicalSnapshot.symbol_id)))
                .select_from(DailyTechnicalSnapshot)
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .where(DailyTechnicalSnapshot.market_date == _normalize_trade_date(market_date))
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )


def count_daily_action_summaries_for_live_symbols(market_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyActionSummary.symbol_id)))
                .select_from(DailyActionSummary)
                .join(Symbol, Symbol.id == DailyActionSummary.symbol_id)
                .where(DailyActionSummary.market_date == _normalize_trade_date(market_date))
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )
