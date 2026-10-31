from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import and_, func, or_, select, text

from app.agent_context.lease_loss_evidence import (
    terminal_feedback_completed_run_evidence_error,
    terminal_feedback_lease_loss_evidence_error,
)
from app.db import engine, session_scope
from app.db_models import (
    AgentContextFeedbackRun,
    DailyActionSummary,
    DailyPipelineRun,
    DailyPipelineStageRun,
    DailyPriceBar,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    EODCollectionRun,
    Symbol,
)
from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_prefix_outcome_is_satisfied,
    terminal_continuation_expired_decision_skip_evidence_is_valid,
    terminal_continuation_feedback_lease_loss_run_id,
    terminal_continuation_lease_loss_resume_lineage_is_valid,
    terminal_continuation_marker_is_valid,
    terminal_continuation_resume_is_unconsumed,
)
from app.eod.run_stage_support import (
    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER,
    SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT,
    SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY,
    is_canonical_same_day_signal_rearm_proof,
    is_exact_auto_pitr_intraday_watch_recovery_request,
    is_recoverable_same_day_intraday_watch_expiration,
    pending_same_day_intraday_watch_rearm_marker,
    validated_nonblocking_partial_progression_marker,
)
from app.eod.summary_cutoff_contract import (
    is_resumable_summary_cutoff_contract_repair,
    is_summary_cutoff_contract_repair_candidate,
    summary_cutoff_proof_from_stage,
)


logger = logging.getLogger(__name__)


KIS_FINAL_BAR_REPAIR_SOURCE = 'kis_final_bar_repair'


def daily_price_bar_has_current_eod_run_provenance(
    run_id: int,
    *,
    original_source: str | None = None,
):
    """Match direct or indexed repair-origin ownership by an EOD run."""

    direct_conditions = [DailyPriceBar.eod_run_id == run_id]
    repaired_conditions = [
        DailyPriceBar.source == KIS_FINAL_BAR_REPAIR_SOURCE,
        DailyPriceBar.repair_origin_eod_run_id == run_id,
    ]
    if original_source is not None:
        direct_conditions.append(DailyPriceBar.source == original_source)
        repaired_conditions.append(
            DailyPriceBar.repair_origin_source == original_source
        )

    return or_(
        and_(*direct_conditions),
        and_(*repaired_conditions),
    )

_EOD_COLLECTION_LOCK_SOURCE_KEYS = {
    ('KRX', 'pykrx'): 10_001,
    ('KRX', 'kis'): 10_002,
    ('KRX', 'daily_price_bars'): 10_003,
    ('KRX', 'dart_share_issuance'): 10_004,
}
_AUTO_DAILY_PIPELINE_ENTRYPOINT = (
    'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
)
_AUTO_DAILY_LEGACY_EOD_TRIGGERS = {
    'auto_close',
    'auto_close_scheduler',
}
_AUTO_DAILY_RECOVERY_TRIGGER = 'auto_eod_followup_resume'
_PYKRX_REPAIR_CHECKPOINT_TRIGGERS = {
    'internal_ops_cli',
    'manual_api',
}
_REQUIRED_PYKRX_REPAIR_CATEGORIES = {
    'sector_classification',
    'investor_flow_symbol',
    'short_selling',
    'valuation_snapshot',
}
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


@dataclass(frozen=True)
class AutoEODRecoveryTarget:
    trade_date: str
    eod_run_id: int


@dataclass
class _PipelineRecoveryCandidate:
    id: int
    market_date: date
    trigger: str
    status: str
    current_stage: str | None
    started_at: datetime
    activity_at: datetime
    completed_at: datetime | None
    error_message: str | None
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


def _invalidate_advisory_lock_connection(connection: Any) -> None:
    """Discard a pooled session whose advisory-lock cleanup failed."""

    try:
        connection.invalidate()
    except Exception:
        logger.warning(
            'Failed to invalidate the EOD advisory-lock connection after '
            'unlock cleanup failed.',
            exc_info=True,
        )


@contextmanager
def advisory_eod_collection_locks(
    *,
    market: str,
    trade_dates: Sequence[str | date],
    source: str,
):
    """Hold a sorted set of EOD date locks on one dedicated connection."""

    normalized_dates = tuple(
        sorted({_normalize_trade_date(item) for item in trade_dates})
    )
    if not normalized_dates:
        raise ValueError('at least one EOD advisory-lock date is required')
    source_key = _EOD_COLLECTION_LOCK_SOURCE_KEYS.get(
        (market, source),
        19_999,
    )
    acquired_trade_keys: list[int] = []
    blocked_date: date | None = None
    with engine.connect() as raw_connection:
        connection = raw_connection.execution_options(isolation_level='AUTOCOMMIT')
        try:
            for trade_date in normalized_dates:
                trade_key = int(trade_date.strftime('%Y%m%d'))
                acquired = bool(
                    connection.execute(
                        text(
                            'SELECT pg_try_advisory_lock('
                            ':source_key, :trade_key)'
                        ),
                        {
                            'source_key': source_key,
                            'trade_key': trade_key,
                        },
                    ).scalar()
                )
                if not acquired:
                    blocked_date = trade_date
                    break
                acquired_trade_keys.append(trade_key)
            yield (
                blocked_date is None,
                blocked_date.isoformat() if blocked_date is not None else None,
            )
        finally:
            for trade_key in reversed(acquired_trade_keys):
                try:
                    connection.execute(
                        text(
                            'SELECT pg_advisory_unlock('
                            ':source_key, :trade_key)'
                        ),
                        {
                            'source_key': source_key,
                            'trade_key': trade_key,
                        },
                    )
                except Exception:
                    # SQLAlchemy close normally returns this session to the
                    # pool, where session-level locks would remain held.
                    # Invalidating it closes the PostgreSQL session and
                    # releases this lock plus any remaining locks in the set.
                    _invalidate_advisory_lock_connection(connection)
                    logger.warning(
                        'Failed to explicitly release EOD advisory locks; '
                        'discarded the dedicated connection.',
                        exc_info=True,
                    )
                    break


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
    statement_timeout_ms: int | None = None,
) -> dict | None:
    rows = list_eod_collection_runs(
        market=market,
        trade_date=trade_date,
        source=source,
        statuses=statuses,
        limit=1,
        statement_timeout_ms=statement_timeout_ms,
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
    statement_timeout_ms: int | None = None,
) -> list[dict[str, Any]]:
    session_options = (
        {'statement_timeout_ms': statement_timeout_ms}
        if statement_timeout_ms is not None
        else {}
    )
    with session_scope(**session_options) as session:
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


def load_exact_automatic_daily_eod_collection_run(
    *,
    eod_run_id: int,
    trade_date: str | date,
) -> dict[str, Any] | None:
    """Reload one still-current automatic daily EOD lineage exactly."""

    normalized_trade_date = _normalize_trade_date(trade_date)
    normalized_run_id = int(eod_run_id)
    with session_scope() as session:
        candidates = [
            _build_eod_recovery_candidate(row)
            for row in session.execute(
                _eod_recovery_candidate_statement(
                    lower_bound=normalized_trade_date,
                    cutoff=normalized_trade_date,
                )
            )
        ]
        _load_eod_recovery_modes(session, candidates=candidates)
        latest_automatic = next(
            (row for row in candidates if _is_automatic_daily_eod_run(row)),
            None,
        )
        if (
            latest_automatic is None
            or latest_automatic.id != normalized_run_id
        ):
            return None

        row = session.execute(
            select(EODCollectionRun).where(
                EODCollectionRun.id == normalized_run_id,
                EODCollectionRun.market == 'KRX',
                EODCollectionRun.source == 'pykrx',
                EODCollectionRun.trade_date == normalized_trade_date,
            )
        ).scalar_one_or_none()
        if row is None or not _is_automatic_daily_eod_run(row):
            return None
        return _serialize_eod_collection_run(row)


def load_automatic_daily_eod_collection_run_by_id(
    *,
    eod_run_id: int,
    trade_date: str | date,
) -> dict[str, Any] | None:
    """Load one automatic daily EOD ancestor without requiring it to be latest."""

    normalized_trade_date = _normalize_trade_date(trade_date)
    normalized_run_id = int(eod_run_id)
    with session_scope() as session:
        row = session.execute(
            select(EODCollectionRun).where(
                EODCollectionRun.id == normalized_run_id,
                EODCollectionRun.market == 'KRX',
                EODCollectionRun.source == 'pykrx',
                EODCollectionRun.trade_date == normalized_trade_date,
            )
        ).scalar_one_or_none()
        if row is None or not _is_automatic_daily_eod_run(row):
            return None
        return _serialize_eod_collection_run(row)


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
            metadata['markets'].label('markets'),
            metadata['requested_symbols'].label('requested_symbols'),
            metadata['stage_progress']['agent_feedback_summaries'].label(
                'feedback_summary_stage'
            ),
            metadata['same_day_intraday_watch_rearm'].label(
                'same_day_intraday_watch_rearm'
            ),
            metadata['nonblocking_partial_progression'].label(
                'nonblocking_partial_progression'
            ),
            metadata['nonblocking_feedback_retry_debt'].label(
                'nonblocking_feedback_retry_debt'
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
                'markets': row.markets,
                'requested_symbols': row.requested_symbols,
                'feedback_summary_stage': row.feedback_summary_stage,
                'same_day_intraday_watch_rearm': (
                    row.same_day_intraday_watch_rearm
                ),
                'nonblocking_partial_progression': (
                    row.nonblocking_partial_progression
                ),
                'nonblocking_feedback_retry_debt': (
                    row.nonblocking_feedback_retry_debt
                ),
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
            DailyPipelineRun.completed_at,
            DailyPipelineRun.error_message,
            func.coalesce(
                DailyPipelineRun.completed_at,
                DailyPipelineRun.updated_at,
                DailyPipelineRun.started_at,
            ).label('activity_at'),
            metadata['entrypoint'].as_string().label('entrypoint'),
            metadata['eod_collection_run_id'].as_integer().label(
                'eod_collection_run_id'
            ),
            metadata['reused_eod_collection_run_id'].as_integer().label(
                'reused_eod_collection_run_id'
            ),
            metadata['resumable'].as_boolean().label('resumable'),
            metadata['resolved_stages'].label('resolved_stages'),
            metadata['terminal_continuation_count']
            .as_integer()
            .label('terminal_continuation_count'),
            metadata['terminal_continuation_version']
            .as_integer()
            .label('terminal_continuation_version'),
            metadata['terminal_continuation_reason']
            .as_string()
            .label('terminal_continuation_reason'),
            metadata['terminal_continuation_claimed_at']
            .as_string()
            .label('terminal_continuation_claimed_at'),
            metadata['terminal_continuation_evidence'].label(
                'terminal_continuation_evidence'
            ),
            metadata['terminal_continuation_resume_count']
            .label('terminal_continuation_resume_count'),
            metadata['terminal_continuation_resumed_from_status']
            .as_string()
            .label('terminal_continuation_resumed_from_status'),
            metadata['terminal_continuation_resume_stage']
            .as_string()
            .label('terminal_continuation_resume_stage'),
            metadata['terminal_continuation_completed_prefix'].label(
                'terminal_continuation_completed_prefix'
            ),
            metadata['interrupted_reason']
            .as_string()
            .label('interrupted_reason'),
            metadata['interrupted_at']
            .as_string()
            .label('interrupted_at'),
            metadata['stage_statuses'].label('stage_statuses'),
            metadata['symbols'].label('symbols'),
            metadata['fs_div'].as_string().label('fs_div'),
            metadata['reuse_existing_feedback']
            .as_boolean()
            .label('reuse_existing_feedback'),
            metadata['force'].as_boolean().label('force'),
            metadata['point_in_time_recovery']
            .as_boolean()
            .label('point_in_time_recovery'),
            metadata['allow_expired_analysis_replay']
            .as_boolean()
            .label('allow_expired_analysis_replay'),
            metadata['same_day_intraday_watch_continuation_admitted']
            .as_boolean()
            .label('same_day_intraday_watch_continuation_admitted'),
            metadata['terminal_continuation_feedback_lease_loss_run_id']
            .as_string()
            .label('terminal_continuation_feedback_lease_loss_run_id'),
            metadata['resume_from_stage']
            .as_string()
            .label('resume_from_stage'),
            metadata['feedback_knowledge_cutoff'].label(
                'feedback_knowledge_cutoff'
            ),
            metadata['synthetic_recovery_parent']
            .as_boolean()
            .label('synthetic_recovery_parent'),
            metadata['summary_cutoff_contract_repair_version']
            .label('summary_cutoff_contract_repair_version'),
            metadata['summary_cutoff_contract_repair_proof'].label(
                'summary_cutoff_contract_repair_proof'
            ),
            metadata['summary_cutoff_contract_repair_resume_count']
            .label('summary_cutoff_contract_repair_resume_count'),
            metadata['nonblocking_partial_progression'].label(
                'nonblocking_partial_progression'
            ),
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
            activity_at=row.activity_at,
            completed_at=row.completed_at,
            error_message=row.error_message,
            run_metadata={
                'entrypoint': row.entrypoint,
                'eod_collection_run_id': row.eod_collection_run_id,
                'reused_eod_collection_run_id': (
                    row.reused_eod_collection_run_id
                ),
                'resumable': row.resumable,
                'resolved_stages': row.resolved_stages,
                'terminal_continuation_count': (
                    row.terminal_continuation_count
                ),
                'terminal_continuation_version': (
                    row.terminal_continuation_version
                ),
                'terminal_continuation_reason': (
                    row.terminal_continuation_reason
                ),
                'terminal_continuation_claimed_at': (
                    row.terminal_continuation_claimed_at
                ),
                'terminal_continuation_evidence': (
                    row.terminal_continuation_evidence
                ),
                'terminal_continuation_resume_count': (
                    row.terminal_continuation_resume_count
                ),
                'terminal_continuation_resumed_from_status': (
                    row.terminal_continuation_resumed_from_status
                ),
                'terminal_continuation_resume_stage': (
                    row.terminal_continuation_resume_stage
                ),
                'terminal_continuation_completed_prefix': (
                    row.terminal_continuation_completed_prefix
                ),
                'interrupted_reason': row.interrupted_reason,
                'interrupted_at': row.interrupted_at,
                'stage_statuses': row.stage_statuses,
                'symbols': row.symbols,
                'fs_div': row.fs_div,
                'reuse_existing_feedback': row.reuse_existing_feedback,
                'force': row.force,
                'point_in_time_recovery': row.point_in_time_recovery,
                'allow_expired_analysis_replay': (
                    row.allow_expired_analysis_replay
                ),
                'same_day_intraday_watch_continuation_admitted': (
                    row.same_day_intraday_watch_continuation_admitted
                ),
                'terminal_continuation_feedback_lease_loss_run_id': (
                    row.terminal_continuation_feedback_lease_loss_run_id
                ),
                'resume_from_stage': row.resume_from_stage,
                'feedback_knowledge_cutoff': (
                    row.feedback_knowledge_cutoff
                ),
                'synthetic_recovery_parent': (
                    row.synthetic_recovery_parent
                ),
                'summary_cutoff_contract_repair_version': (
                    row.summary_cutoff_contract_repair_version
                ),
                'summary_cutoff_contract_repair_proof': (
                    row.summary_cutoff_contract_repair_proof
                ),
                'summary_cutoff_contract_repair_resume_count': (
                    row.summary_cutoff_contract_repair_resume_count
                ),
                'nonblocking_partial_progression': (
                    row.nonblocking_partial_progression
                ),
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
        'remaining': int(stage_row.remaining or 0),
        'current_step': stage_row.current_step,
        'completed_at': stage_row.completed_at,
        'error_message': stage_row.error_message,
        'source_run_ids': (
            stage_row.source_run_ids
            if isinstance(stage_row.source_run_ids, dict)
            else {}
        ),
        'details': (
            stage_row.details if isinstance(stage_row.details, dict) else {}
        ),
    }


def _validated_nonblocking_partial_recovery(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    eod_run: _EODRecoveryCandidate,
    require_eod_reconciled: bool,
) -> dict[str, Any] | None:
    """Rebuild and validate a partial-success recovery from durable rows."""

    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
        else {}
    )
    raw_resolved_stages = metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return None
    resolved_stages = [
        str(stage_id or '').strip() for stage_id in raw_resolved_stages
    ]
    if not (
        resolved_stages
        and all(resolved_stages)
        and len(resolved_stages) == len(set(resolved_stages))
    ):
        return None
    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id
                == int(pipeline_run.id),
                DailyPipelineStageRun.market_date
                == pipeline_run.market_date,
                DailyPipelineStageRun.stage_id.in_(resolved_stages),
            )
        ).scalars()
    )
    stages = {
        str(stage_row.stage_id or '').strip(): _pipeline_stage_outcome(
            stage_row
        )
        for stage_row in stage_rows
    }
    if set(stages) != set(resolved_stages):
        return None
    recovery_result = {
        'status': pipeline_run.status,
        'pipeline_run_id': int(pipeline_run.id),
        'pipeline_run': {
            'id': int(pipeline_run.id),
            'market_date': pipeline_run.market_date.isoformat(),
            'trigger': pipeline_run.trigger,
            'status': pipeline_run.status,
            'current_stage': pipeline_run.current_stage,
            'completed_at': pipeline_run.completed_at,
            'error_message': pipeline_run.error_message,
            'run_metadata': metadata,
        },
        'market_date': pipeline_run.market_date.isoformat(),
        'resolved_stages': resolved_stages,
        'stages': stages,
    }
    marker = validated_nonblocking_partial_progression_marker(
        recovery_result,
        expected_eod_collection_run_id=int(eod_run.id),
    )
    if marker is None:
        return None
    if not require_eod_reconciled:
        return marker

    eod_metadata = (
        eod_run.run_metadata
        if isinstance(eod_run.run_metadata, dict)
        else {}
    )
    if not (
        eod_metadata.get('nonblocking_partial_progression') == marker
        and eod_metadata.get('nonblocking_feedback_retry_debt')
        == marker.get('retry_debt')
        and not _eod_has_followup_recovery_scope(eod_run)
        and not _eod_has_price_recovery_scope(eod_run)
    ):
        return None
    return marker


def _followup_recovery_is_recoverable_same_day_expiration(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    current_market_date: date,
) -> bool:
    """Keep one exact OPEN PITR false-skip from consuming recovery debt."""

    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
        else {}
    )
    raw_resolved_stages = metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return False
    resolved_stages = tuple(
        str(stage_id or '').strip()
        for stage_id in raw_resolved_stages
        if str(stage_id or '').strip()
    )
    if len(resolved_stages) != len(raw_resolved_stages):
        return False
    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id
                == int(pipeline_run.id),
                DailyPipelineStageRun.market_date
                == pipeline_run.market_date,
                DailyPipelineStageRun.stage_id.in_(resolved_stages),
            )
        ).scalars()
    )
    stages = {
        str(stage_row.stage_id or '').strip(): _pipeline_stage_outcome(
            stage_row
        )
        for stage_row in stage_rows
    }
    if set(stages) != set(resolved_stages):
        return False
    return is_recoverable_same_day_intraday_watch_expiration(
        {
            'id': pipeline_run.id,
            'market_date': pipeline_run.market_date,
            'trigger': pipeline_run.trigger,
            'status': pipeline_run.status,
            'current_stage': pipeline_run.current_stage,
            'started_at': pipeline_run.started_at,
            'updated_at': pipeline_run.activity_at,
            'completed_at': pipeline_run.completed_at,
            'error_message': pipeline_run.error_message,
            'run_metadata': metadata,
            'stages': stages,
        },
        current_market_date=current_market_date,
    )


def _followup_candidate_is_exact_intraday_watch_recovery(
    pipeline_run: _PipelineRecoveryCandidate,
    *,
    eod_run_id: int,
) -> bool:
    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, Mapping)
        else {}
    )
    resolved_stages = metadata.get('resolved_stages')
    return bool(
        metadata.get('entrypoint') == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and isinstance(resolved_stages, (list, tuple))
        and is_exact_auto_pitr_intraday_watch_recovery_request(
            trigger=pipeline_run.trigger,
            point_in_time_recovery=metadata.get('point_in_time_recovery'),
            reused_eod_collection_run_id=metadata.get(
                'reused_eod_collection_run_id'
            ),
            force=metadata.get('force'),
            reuse_existing_feedback=metadata.get('reuse_existing_feedback'),
            resume_from_stage=metadata.get('resume_from_stage'),
            resolved_stages=resolved_stages,
        )
        and metadata.get('reused_eod_collection_run_id') == eod_run_id
    )


def _followup_recovery_is_canonical_signal_rearm_proof(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    eod_run_id: int,
) -> bool:
    """Verify the immutable signal retry evidence named by a rearm marker."""

    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id
                == int(pipeline_run.id),
                DailyPipelineStageRun.market_date
                == pipeline_run.market_date,
                DailyPipelineStageRun.stage_id.in_(
                    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
                ),
            )
        ).scalars()
    )
    stages = {
        str(stage_row.stage_id or '').strip(): _pipeline_stage_outcome(
            stage_row
        )
        for stage_row in stage_rows
    }
    return is_canonical_same_day_signal_rearm_proof(
        {
            'id': int(pipeline_run.id),
            'market_date': pipeline_run.market_date,
            'trigger': pipeline_run.trigger,
            'status': pipeline_run.status,
            'current_stage': pipeline_run.current_stage,
            'run_metadata': pipeline_run.run_metadata,
            'stages': stages,
        },
        expected_eod_collection_run_id=eod_run_id,
    )


def _followup_recovery_has_valid_same_day_rearm_marker(
    session: Any,
    *,
    eod_run: _EODRecoveryCandidate,
    pipeline_candidates: Sequence[_PipelineRecoveryCandidate],
    latest_recovery: _PipelineRecoveryCandidate | None,
    current_market_date: date,
    followup_attempts: int,
    allow_stale_target_for_terminal_block: bool = False,
) -> bool:
    """Validate the narrow marker that extends one false-skip lineage.

    This is deliberately separate from the generic follow-up retry budget. The
    marker can surface only the exact same-day OPEN false-skip it names, and it
    cannot authorize work after its bounded eighth recovery observation.
    """

    within_rearm_attempt_budget = bool(
        _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD < followup_attempts
        <= SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
    )
    stale_pre_run_crash = bool(
        allow_stale_target_for_terminal_block
        and followup_attempts == _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
    )
    if not (
        (within_rearm_attempt_budget or stale_pre_run_crash)
        and latest_recovery is not None
        and str(latest_recovery.status or '').strip().lower()
        in (
            _RECOVERABLE_DAILY_PIPELINE_STATUSES
            | _CONVERGENCE_ELIGIBLE_RECOVERY_STATUSES
        )
        and _followup_candidate_is_exact_intraday_watch_recovery(
            latest_recovery,
            eod_run_id=eod_run.id,
        )
    ):
        return False

    marker = pending_same_day_intraday_watch_rearm_marker(
        eod_run.run_metadata.get(
            SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
        ),
        target_trade_date=(
            None
            if allow_stale_target_for_terminal_block
            else current_market_date
        ),
    )
    if marker is None:
        return False
    marker_target_trade_date = date.fromisoformat(
        str(marker['target_trade_date'])
    )
    if allow_stale_target_for_terminal_block and not (
        marker_target_trade_date < current_market_date
    ):
        return False
    marker_started_at = _normalized_datetime(marker.get('started_at'))
    if marker_started_at is None:
        return False
    source_false_skip_run_id = marker.get('source_false_skip_run_id')
    proof_pipeline_run_id = marker.get('proof_pipeline_run_id')

    candidates_by_id = {
        candidate.id: candidate for candidate in pipeline_candidates
    }
    source_false_skip = candidates_by_id.get(source_false_skip_run_id)
    proof_pipeline_run = candidates_by_id.get(proof_pipeline_run_id)
    if source_false_skip is None or proof_pipeline_run is None:
        return False

    def _reuses_exact_eod(candidate: _PipelineRecoveryCandidate) -> bool:
        metadata = (
            candidate.run_metadata
            if isinstance(candidate.run_metadata, Mapping)
            else {}
        )
        try:
            return int(metadata.get('reused_eod_collection_run_id')) == int(
                eod_run.id
            )
        except (TypeError, ValueError):
            return False

    if not (
        source_false_skip.market_date == eod_run.trade_date
        and proof_pipeline_run.market_date == eod_run.trade_date
        and source_false_skip.trigger == _AUTO_DAILY_RECOVERY_TRIGGER
        and proof_pipeline_run.trigger == _AUTO_DAILY_RECOVERY_TRIGGER
        and _reuses_exact_eod(source_false_skip)
        and _reuses_exact_eod(proof_pipeline_run)
        and proof_pipeline_run.started_at <= source_false_skip.started_at
        and (
            latest_recovery.id > source_false_skip.id
            or (
                stale_pre_run_crash
                and latest_recovery.id == source_false_skip.id
            )
        )
        and _reuses_exact_eod(latest_recovery)
    ):
        return False

    source_completed_at = _normalized_datetime(source_false_skip.completed_at)
    if source_completed_at is None or marker_started_at < source_completed_at:
        return False
    post_source_attempts = sorted(
        (
            candidate
            for candidate in pipeline_candidates
            if candidate.id > source_false_skip.id
            and candidate.market_date == eod_run.trade_date
            and candidate.trigger == _AUTO_DAILY_RECOVERY_TRIGGER
            and _reuses_exact_eod(candidate)
        ),
        key=lambda candidate: (
            _normalized_datetime(candidate.started_at)
            or datetime.min.replace(tzinfo=timezone.utc),
            candidate.id,
        ),
    )
    earliest_attempt_at = (
        _normalized_datetime(post_source_attempts[0].started_at)
        if post_source_attempts
        else None
    )
    marker_precedes_recovery_attempt = bool(
        (
            earliest_attempt_at is not None
            and marker_started_at <= earliest_attempt_at
        )
        or (stale_pre_run_crash and not post_source_attempts)
    )
    return bool(
        marker_precedes_recovery_attempt
        and _followup_recovery_is_recoverable_same_day_expiration(
            session,
            pipeline_run=source_false_skip,
            current_market_date=marker_target_trade_date,
        )
        and _followup_recovery_is_canonical_signal_rearm_proof(
            session,
            pipeline_run=proof_pipeline_run,
            eod_run_id=eod_run.id,
        )
    )


def _terminal_continuation_lease_loss_child_is_valid(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    child_run_id: str,
    completed_summary_source_run_id: str | None = None,
) -> bool:
    metadata = pipeline_run.run_metadata
    evidence = metadata.get('terminal_continuation_evidence')
    summary_proof = (
        evidence.get('summary_proof')
        if isinstance(evidence, Mapping)
        and isinstance(evidence.get('summary_proof'), Mapping)
        else {}
    )
    knowledge_cutoff_at = _normalized_datetime(
        summary_proof.get('knowledge_cutoff_at')
    )
    completed_prefix_summary_proof = (
        evidence.get('completed_prefix_summary_proof')
        if isinstance(evidence, Mapping)
        and isinstance(
            evidence.get('completed_prefix_summary_proof'),
            Mapping,
        )
        else None
    )
    if knowledge_cutoff_at is None:
        return False
    child_run = session.execute(
        select(AgentContextFeedbackRun).where(
            AgentContextFeedbackRun.run_id == child_run_id
        )
    ).scalar_one_or_none()
    return terminal_feedback_lease_loss_evidence_error(
        child_run=child_run,
        child_run_id=child_run_id,
        market_date=pipeline_run.market_date,
        metadata=metadata,
        knowledge_cutoff_at=knowledge_cutoff_at,
        completed_summary_proof=completed_prefix_summary_proof,
        expected_summary_source_run_id=(
            completed_summary_source_run_id
        ),
    ) is None


def _terminal_continuation_satisfied_prefix_evidence_is_valid(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    satisfied_prefix: Sequence[str],
    stage_rows_by_id: Mapping[str, DailyPipelineStageRun],
) -> bool:
    metadata = pipeline_run.run_metadata
    evidence = metadata.get('terminal_continuation_evidence')
    summary_proof = (
        evidence.get('summary_proof')
        if isinstance(evidence, Mapping)
        and isinstance(evidence.get('summary_proof'), Mapping)
        else {}
    )
    knowledge_cutoff_at = _normalized_datetime(
        summary_proof.get('knowledge_cutoff_at')
    )
    feedback_stage_ids = {
        'agent_feedback_summaries',
        'signal_decider',
    }
    completed_prefix_summary_proof = (
        evidence.get('completed_prefix_summary_proof')
        if isinstance(evidence, Mapping)
        and isinstance(
            evidence.get('completed_prefix_summary_proof'),
            Mapping,
        )
        else None
    )
    completed_summary_source_run_id: str | None = None
    for stage_id in satisfied_prefix:
        stage_row = stage_rows_by_id[stage_id]
        stage_status = str(stage_row.status or '').strip().lower()
        if stage_status == 'skipped':
            if not terminal_continuation_expired_decision_skip_evidence_is_valid(
                stage_row=stage_row,
                stage_id=stage_id,
                market_date=pipeline_run.market_date,
            ):
                return False
            continue
        if stage_id not in feedback_stage_ids:
            continue
        if stage_status != 'completed' or knowledge_cutoff_at is None:
            return False
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
            return False
        child_run_id = next(iter(child_source_ids))
        child_run = session.execute(
            select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == child_run_id
            )
        ).scalar_one_or_none()
        if (
            terminal_feedback_completed_run_evidence_error(
                child_run=child_run,
                child_run_id=child_run_id,
                stage_id=stage_id,
                market_date=pipeline_run.market_date,
                metadata=metadata,
                knowledge_cutoff_at=knowledge_cutoff_at,
                completed_summary_proof=(
                    completed_prefix_summary_proof
                    if stage_id == 'signal_decider'
                    else None
                ),
                expected_summary_source_run_id=(
                    completed_summary_source_run_id
                    if stage_id == 'signal_decider'
                    else None
                ),
            )
            is not None
        ):
            return False
        if stage_id == 'agent_feedback_summaries':
            completed_summary_source_run_id = child_run_id
    return True


def _followup_recovery_is_reclaimable_feedback_lease_loss(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
) -> bool:
    """Surface only the exact failed sixth continuation the runtime can reclaim."""

    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
        else {}
    )
    if not terminal_continuation_marker_is_valid(metadata):
        return False
    if not terminal_continuation_resume_is_unconsumed(metadata):
        return False
    evidence = metadata.get('terminal_continuation_evidence')
    lineage = (
        evidence.get('lineage')
        if isinstance(evidence, Mapping)
        and isinstance(evidence.get('lineage'), Mapping)
        else {}
    )
    ordered_run_ids = lineage.get('ordered_pipeline_run_ids')
    expected_run_count = _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD + 1
    if not (
        lineage.get('expected_run_count') == expected_run_count
        and lineage.get('observed_run_count') == expected_run_count
        and lineage.get('target_ordinal') == expected_run_count
        and lineage.get('latest_pipeline_run_id') == pipeline_run.id
        and lineage.get('reused_eod_collection_run_id')
        == metadata.get('reused_eod_collection_run_id')
        and isinstance(ordered_run_ids, list)
        and len(ordered_run_ids) == expected_run_count
        and ordered_run_ids[-1] == pipeline_run.id
    ):
        return False

    raw_resolved_stages = metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return False
    resolved_stages = [
        str(stage_id or '').strip()
        for stage_id in raw_resolved_stages
        if str(stage_id or '').strip()
    ]
    continuation_suffix = (
        'agent_feedback_summaries',
        'signal_decider',
        'next_open_execution_plan',
        'signal_decider_evaluations',
    )
    if (
        len(resolved_stages) != len(raw_resolved_stages)
        or len(resolved_stages) != len(set(resolved_stages))
        or tuple(resolved_stages[-len(continuation_suffix) :])
        != continuation_suffix
    ):
        return False

    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == pipeline_run.id,
                DailyPipelineStageRun.market_date == pipeline_run.market_date,
                DailyPipelineStageRun.stage_id.in_(tuple(resolved_stages)),
            )
        ).scalars()
    )
    stage_rows_by_id = {
        str(stage_row.stage_id or '').strip(): stage_row
        for stage_row in stage_rows
    }
    if set(stage_rows_by_id) != set(resolved_stages):
        return False
    suffix_start_index = len(resolved_stages) - len(continuation_suffix)
    if any(
        not daily_pipeline_prefix_outcome_is_satisfied(
            _pipeline_stage_outcome(stage_rows_by_id[stage_id])
        )
        for stage_id in resolved_stages[:suffix_start_index]
    ):
        return False

    summary = stage_rows_by_id['agent_feedback_summaries']
    if not (
        daily_pipeline_outcome_is_satisfied(_pipeline_stage_outcome(summary))
        and summary.completed_at is not None
        and not str(summary.error_message or '').strip()
    ):
        return False
    summary_source_run_ids = (
        summary.source_run_ids
        if isinstance(summary.source_run_ids, Mapping)
        else {}
    )
    completed_summary_source_ids = {
        str(summary_source_run_ids.get(key) or '').strip()
        for key in ('agent_feedback_run_id', 'run_id')
        if str(summary_source_run_ids.get(key) or '').strip()
    }
    if len(completed_summary_source_ids) != 1:
        return False
    completed_summary_source_run_id = next(
        iter(completed_summary_source_ids)
    )
    decider = stage_rows_by_id['signal_decider']
    child_run_id = terminal_continuation_feedback_lease_loss_run_id(
        pipeline_run={
            'status': pipeline_run.status,
            'current_stage': pipeline_run.current_stage,
            'market_date': pipeline_run.market_date,
            'completed_at': pipeline_run.completed_at,
            'error_message': pipeline_run.error_message,
            'run_metadata': metadata,
        },
        stage=_pipeline_stage_outcome(decider),
    )
    if not child_run_id or not _terminal_continuation_lease_loss_child_is_valid(
        session,
        pipeline_run=pipeline_run,
        child_run_id=child_run_id,
        completed_summary_source_run_id=completed_summary_source_run_id,
    ):
        return False

    return all(
        str(stage.status or '').strip().lower() == 'pending'
        and str(stage.current_step or '').strip()
        == 'terminal_continuation_pending'
        and int(stage.processed or 0) == 0
        and int(stage.total or 0) == 0
        and int(stage.succeeded or 0) == 0
        and int(stage.failed or 0) == 0
        and int(stage.remaining or 0) == 0
        and stage.completed_at is None
        and not str(stage.error_message or '').strip()
        and not (stage.source_run_ids or {})
        and not (stage.details or {})
        for stage in (
            stage_rows_by_id['next_open_execution_plan'],
            stage_rows_by_id['signal_decider_evaluations'],
        )
    )


def _followup_recovery_is_resumable_terminal_continuation(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
) -> bool:
    """Surface a previously reclaimed continuation after restart/stall."""

    metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, dict)
        else {}
    )
    status = str(pipeline_run.status or '').strip().lower()
    if not (
        status == 'interrupted'
        and terminal_continuation_marker_is_valid(metadata)
        and terminal_continuation_lease_loss_resume_lineage_is_valid(
            metadata
        )
    ):
        return False

    interrupted_reason = str(
        metadata.get('interrupted_reason') or ''
    ).strip()
    interrupted_at = _normalized_datetime(metadata.get('interrupted_at'))
    completed_at = _normalized_datetime(pipeline_run.completed_at)
    if not (
        metadata.get('resumable') is True
        and interrupted_reason
        and interrupted_at is not None
        and interrupted_at == completed_at
        and interrupted_reason
        == str(pipeline_run.error_message or '').strip()
    ):
        return False

    lease_loss_child_run_id = str(
        metadata.get(
            'terminal_continuation_feedback_lease_loss_run_id'
        )
        or ''
    ).strip()
    if not (
        lease_loss_child_run_id
        and _terminal_continuation_lease_loss_child_is_valid(
            session,
            pipeline_run=pipeline_run,
            child_run_id=lease_loss_child_run_id,
        )
    ):
        return False

    evidence = metadata.get('terminal_continuation_evidence')
    lineage = (
        evidence.get('lineage')
        if isinstance(evidence, Mapping)
        and isinstance(evidence.get('lineage'), Mapping)
        else {}
    )
    ordered_run_ids = lineage.get('ordered_pipeline_run_ids')
    expected_run_count = _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD + 1
    if not (
        lineage.get('expected_run_count') == expected_run_count
        and lineage.get('observed_run_count') == expected_run_count
        and lineage.get('target_ordinal') == expected_run_count
        and lineage.get('latest_pipeline_run_id') == pipeline_run.id
        and lineage.get('reused_eod_collection_run_id')
        == metadata.get('reused_eod_collection_run_id')
        and isinstance(ordered_run_ids, list)
        and len(ordered_run_ids) == expected_run_count
        and ordered_run_ids[-1] == pipeline_run.id
    ):
        return False

    raw_resolved_stages = metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return False
    resolved_stages = [
        str(stage_id or '').strip()
        for stage_id in raw_resolved_stages
        if str(stage_id or '').strip()
    ]
    continuation_suffix = (
        'agent_feedback_summaries',
        'signal_decider',
        'next_open_execution_plan',
        'signal_decider_evaluations',
    )
    if (
        len(resolved_stages) != len(raw_resolved_stages)
        or len(resolved_stages) != len(set(resolved_stages))
        or tuple(resolved_stages[-len(continuation_suffix) :])
        != continuation_suffix
    ):
        return False

    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id == pipeline_run.id,
                DailyPipelineStageRun.market_date == pipeline_run.market_date,
                DailyPipelineStageRun.stage_id.in_(tuple(resolved_stages)),
            )
        ).scalars()
    )
    stage_rows_by_id = {
        str(stage_row.stage_id or '').strip(): stage_row
        for stage_row in stage_rows
    }
    if set(stage_rows_by_id) != set(resolved_stages):
        return False
    suffix_start_index = len(resolved_stages) - len(continuation_suffix)
    if any(
        not daily_pipeline_prefix_outcome_is_satisfied(
            _pipeline_stage_outcome(stage_rows_by_id[stage_id])
        )
        for stage_id in resolved_stages[:suffix_start_index]
    ):
        return False

    satisfied_prefix: list[str] = []
    for stage_id in continuation_suffix:
        stage = stage_rows_by_id[stage_id]
        if not (
            daily_pipeline_outcome_is_satisfied(
                _pipeline_stage_outcome(stage)
            )
            and stage.completed_at is not None
            and not str(stage.error_message or '').strip()
        ):
            break
        satisfied_prefix.append(stage_id)
    if len(satisfied_prefix) == len(continuation_suffix):
        return False

    recorded_prefix = metadata.get('terminal_continuation_completed_prefix')
    resumed_stage = str(
        metadata.get('terminal_continuation_resume_stage') or ''
    ).strip()
    if not (
        isinstance(recorded_prefix, list)
        and recorded_prefix
        and recorded_prefix == list(
            continuation_suffix[: len(recorded_prefix)]
        )
        and len(recorded_prefix) <= len(satisfied_prefix)
        and len(recorded_prefix) < len(continuation_suffix)
        and resumed_stage == continuation_suffix[len(recorded_prefix)]
    ):
        return False

    stage_statuses = metadata.get('stage_statuses')
    if not isinstance(stage_statuses, Mapping):
        return False
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
            return False
        live_status = str(
            stage_rows_by_id[stage_id].status or ''
        ).strip().lower()
        if (
            not persisted_status
            or persisted_status != live_status
            or not daily_pipeline_outcome_is_satisfied(
                _pipeline_stage_outcome(stage_rows_by_id[stage_id])
            )
        ):
            return False
        metadata_prefix_size += 1
    if metadata_prefix_size > len(satisfied_prefix):
        return False
    if not _terminal_continuation_satisfied_prefix_evidence_is_valid(
        session,
        pipeline_run=pipeline_run,
        satisfied_prefix=satisfied_prefix,
        stage_rows_by_id=stage_rows_by_id,
    ):
        return False

    def untouched(stage: DailyPipelineStageRun) -> bool:
        return bool(
            str(stage.status or '').strip().lower() == 'pending'
            and str(stage.current_step or '').strip()
            == 'terminal_continuation_pending'
            and int(stage.processed or 0) == 0
            and int(stage.total or 0) == 0
            and int(stage.succeeded or 0) == 0
            and int(stage.failed or 0) == 0
            and int(stage.remaining or 0) == 0
            and stage.completed_at is None
            and not str(stage.error_message or '').strip()
            and not (stage.source_run_ids or {})
            and not (stage.details or {})
        )

    first_unfinished_index = len(satisfied_prefix)
    first_unfinished_stage = continuation_suffix[first_unfinished_index]
    first_unfinished = stage_rows_by_id[first_unfinished_stage]
    first_status = str(first_unfinished.status or '').strip().lower()
    current_stage = str(pipeline_run.current_stage or '').strip()
    if first_status == 'pending':
        expected_current_stages = {first_unfinished_stage}
        if satisfied_prefix:
            expected_current_stages.add(satisfied_prefix[-1])
        first_unfinished_is_valid = bool(
            untouched(first_unfinished)
            and current_stage in expected_current_stages
        )
    elif first_status == 'running':
        first_unfinished_is_valid = bool(
            status == 'running'
            and current_stage == first_unfinished_stage
            and first_unfinished.completed_at is None
            and not str(first_unfinished.error_message or '').strip()
        )
    elif first_status == 'interrupted':
        details = (
            first_unfinished.details
            if isinstance(first_unfinished.details, Mapping)
            else {}
        )
        first_unfinished_is_valid = bool(
            status == 'interrupted'
            and current_stage == first_unfinished_stage
            and str(first_unfinished.current_step or '').strip()
            == 'core_restart_interrupted'
            and first_unfinished.completed_at is not None
            and str(first_unfinished.error_message or '').strip()
            and details.get('resumable') is True
            and str(details.get('interrupted_reason') or '').strip()
            == interrupted_reason
            and str(first_unfinished.error_message or '').strip()
            == interrupted_reason
        )
    else:
        first_unfinished_is_valid = False
    return bool(
        first_unfinished_is_valid
        and all(
            untouched(stage_rows_by_id[stage_id])
            for stage_id in continuation_suffix[
                first_unfinished_index + 1 :
            ]
        )
    )


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
    outcome_is_satisfied = (
        daily_pipeline_prefix_outcome_is_satisfied
        if terminal_continuation_marker_is_valid(metadata)
        else daily_pipeline_outcome_is_satisfied
    )
    return all(
        outcome_is_satisfied(
            _pipeline_stage_outcome(stage_rows_by_id[stage_id])
        )
        for stage_id in resolved_stages
    )


def _followup_recovery_is_legacy_cutoff_contract_repair(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    eod_run: _EODRecoveryCandidate,
    has_price_recovery_scope: bool,
) -> bool:
    summary_proof = summary_cutoff_proof_from_stage(
        eod_run.run_metadata.get('feedback_summary_stage')
    )
    if summary_proof is None:
        return False
    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id
                == int(pipeline_run.id),
                DailyPipelineStageRun.market_date
                == pipeline_run.market_date,
            )
        ).scalars()
    )
    stages = {
        str(stage_row.stage_id or '').strip(): _pipeline_stage_outcome(
            stage_row
        )
        for stage_row in stage_rows
        if str(stage_row.stage_id or '').strip()
    }
    return is_summary_cutoff_contract_repair_candidate(
        pipeline_run={
            'id': pipeline_run.id,
            'trigger': pipeline_run.trigger,
            'status': pipeline_run.status,
            'current_stage': pipeline_run.current_stage,
            'run_metadata': pipeline_run.run_metadata,
            'stages': stages,
        },
        summary_cutoff_proof=summary_proof,
        price_retry_symbols=(
            ('price_retry_scope',) if has_price_recovery_scope else ()
        ),
    )


def _followup_recovery_is_resumable_cutoff_contract_repair(
    session: Any,
    *,
    pipeline_run: _PipelineRecoveryCandidate,
    eod_run: _EODRecoveryCandidate,
) -> bool:
    current_summary_proof = summary_cutoff_proof_from_stage(
        eod_run.run_metadata.get('feedback_summary_stage')
    )
    persisted_repair_proof = pipeline_run.run_metadata.get(
        'summary_cutoff_contract_repair_proof'
    )
    if not (
        current_summary_proof is not None
        and isinstance(persisted_repair_proof, Mapping)
        and dict(persisted_repair_proof) == current_summary_proof
    ):
        return False
    stage_rows = list(
        session.execute(
            select(DailyPipelineStageRun).where(
                DailyPipelineStageRun.pipeline_run_id
                == int(pipeline_run.id),
                DailyPipelineStageRun.market_date
                == pipeline_run.market_date,
            )
        ).scalars()
    )
    stages = {
        str(stage_row.stage_id or '').strip(): _pipeline_stage_outcome(
            stage_row
        )
        for stage_row in stage_rows
        if str(stage_row.stage_id or '').strip()
    }
    return is_resumable_summary_cutoff_contract_repair(
        {
            'id': pipeline_run.id,
            'trigger': pipeline_run.trigger,
            'status': pipeline_run.status,
            'current_stage': pipeline_run.current_stage,
            'run_metadata': pipeline_run.run_metadata,
            'stages': stages,
        }
    )


def _normalized_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    parsed = value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    if not isinstance(parsed, datetime):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_market_scope(markets: Sequence[str]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(market).strip().upper()
                for market in markets
                if str(market).strip()
            }
        )
    )


def _pykrx_enrichment_repair_checkpoint_payload(
    *,
    pipeline_run: DailyPipelineRun,
    stage_run: DailyPipelineStageRun,
    trade_date: date,
    markets: Sequence[str],
    completed_after: datetime | None,
) -> dict[str, Any] | None:
    if (
        str(pipeline_run.trigger or '').strip()
        not in _PYKRX_REPAIR_CHECKPOINT_TRIGGERS
        or str(pipeline_run.status or '').strip().lower() != 'completed'
        or str(stage_run.status or '').strip().lower() != 'completed'
        or pipeline_run.error_message is not None
        or stage_run.error_message is not None
        or any(
            int(value or 0) < 0
            for value in (
                stage_run.processed,
                stage_run.total,
                stage_run.succeeded,
                stage_run.failed,
                stage_run.remaining,
            )
        )
        or int(stage_run.total or 0) <= 0
        or int(stage_run.processed or 0) != int(stage_run.total or 0)
        or int(stage_run.succeeded or 0) + int(stage_run.failed or 0)
        != int(stage_run.total or 0)
        or int(stage_run.remaining or 0) != 0
    ):
        return None

    run_metadata = (
        pipeline_run.run_metadata
        if isinstance(pipeline_run.run_metadata, Mapping)
        else {}
    )
    resolved_stages = run_metadata.get('resolved_stages')
    if resolved_stages != ['pykrx_enrichment']:
        return None

    pipeline_completed_at = _normalized_datetime(pipeline_run.completed_at)
    stage_completed_at = _normalized_datetime(stage_run.completed_at)
    if (
        pipeline_completed_at is None
        or stage_completed_at is None
        or completed_after is None
        or pipeline_completed_at <= completed_after
        or stage_completed_at <= completed_after
        or stage_completed_at > pipeline_completed_at
    ):
        return None

    details = (
        stage_run.details
        if isinstance(stage_run.details, Mapping)
        else {}
    )
    result_summary = (
        details.get('result_summary')
        if isinstance(details.get('result_summary'), Mapping)
        else {}
    )
    downstream_preflight = (
        result_summary.get('downstream_preflight')
        if isinstance(
            result_summary.get('downstream_preflight'),
            Mapping,
        )
        else {}
    )
    raw_required_categories = downstream_preflight.get(
        'required_categories'
    )
    if not isinstance(raw_required_categories, list):
        return None
    required_categories = {
        str(category).strip()
        for category in raw_required_categories
        if str(category).strip()
    }
    category_row_counts = (
        downstream_preflight.get('category_row_counts')
        if isinstance(
            downstream_preflight.get('category_row_counts'),
            Mapping,
        )
        else {}
    )
    expected_markets = _normalize_market_scope(markets)
    result_markets = _normalize_market_scope(
        result_summary.get('markets')
        if isinstance(result_summary.get('markets'), list)
        else ()
    )
    metadata_markets = _normalize_market_scope(
        run_metadata.get('markets')
        if isinstance(run_metadata.get('markets'), list)
        else ()
    )
    if not expected_markets or not (
        result_markets == metadata_markets == expected_markets
    ):
        return None
    if (
        str(result_summary.get('market_date') or '').strip()
        != trade_date.isoformat()
        or str(result_summary.get('status') or '').strip().lower()
        != 'completed'
        or result_summary.get('point_in_time_recovery') is not True
        or str(result_summary.get('coverage_scope') or '').strip().lower()
        != 'full_market'
        or 'symbols_requested' not in result_summary
        or result_summary.get('symbols_requested') is not None
        or downstream_preflight.get('allowed') is not True
        or str(downstream_preflight.get('status') or '').strip().lower()
        != 'ready'
        or str(downstream_preflight.get('market_date') or '').strip()
        != trade_date.isoformat()
        or required_categories != _REQUIRED_PYKRX_REPAIR_CATEGORIES
        or any(
            not _metadata_positive_int(
                dict(category_row_counts),
                category,
            )
            for category in _REQUIRED_PYKRX_REPAIR_CATEGORIES
        )
        or downstream_preflight.get('failed_categories') != []
        or downstream_preflight.get('blocked_categories') != []
        or downstream_preflight.get('waiting_categories') != []
        or downstream_preflight.get('zero_coverage_categories') != []
    ):
        return None

    return {
        'pipeline_run_id': int(pipeline_run.id),
        'stage_run_id': int(stage_run.id),
        'market_date': trade_date.isoformat(),
        'completed_at': stage_completed_at.isoformat(),
        'markets': list(expected_markets),
        'preflight_status': 'ready',
    }


def _load_latest_pykrx_enrichment_repair_checkpoint(
    session: Any,
    *,
    trade_date: date,
    markets: Sequence[str],
    completed_after: str | datetime | None,
) -> dict[str, Any] | None:
    normalized_completed_after = _normalized_datetime(completed_after)
    if normalized_completed_after is None:
        return None
    rows = session.execute(
        select(DailyPipelineRun, DailyPipelineStageRun)
        .join(
            DailyPipelineStageRun,
            DailyPipelineStageRun.pipeline_run_id == DailyPipelineRun.id,
        )
        .where(
            DailyPipelineRun.market_date == trade_date,
            DailyPipelineRun.trigger.in_(
                sorted(_PYKRX_REPAIR_CHECKPOINT_TRIGGERS)
            ),
            DailyPipelineRun.status == 'completed',
            DailyPipelineStageRun.market_date == trade_date,
            DailyPipelineStageRun.stage_id == 'pykrx_enrichment',
            DailyPipelineStageRun.status == 'completed',
            DailyPipelineRun.completed_at > normalized_completed_after,
            DailyPipelineStageRun.completed_at > normalized_completed_after,
        )
        .order_by(
            DailyPipelineRun.completed_at.desc(),
            DailyPipelineRun.id.desc(),
        )
    ).all()
    for pipeline_run, stage_run in rows:
        payload = _pykrx_enrichment_repair_checkpoint_payload(
            pipeline_run=pipeline_run,
            stage_run=stage_run,
            trade_date=trade_date,
            markets=markets,
            completed_after=normalized_completed_after,
        )
        if payload is not None:
            return payload
    return None


def load_latest_pykrx_enrichment_repair_checkpoint(
    *,
    trade_date: str | date,
    markets: Sequence[str],
    completed_after: str | datetime | None,
) -> dict[str, Any] | None:
    """Load one fresh full-market enrichment repair witness.

    The witness only re-arms one exact automatic follow-up attempt. It is not
    collection-lineage completion evidence and never clears price retry debt.
    """

    normalized_trade_date = _normalize_trade_date(trade_date)
    with session_scope() as session:
        return _load_latest_pykrx_enrichment_repair_checkpoint(
            session,
            trade_date=normalized_trade_date,
            markets=markets,
            completed_after=completed_after,
        )


def load_oldest_outstanding_auto_eod_recovery_target(
    *,
    on_or_after: str | date,
    on_or_before: str | date,
) -> AutoEODRecoveryTarget | None:
    """Return the oldest recent automatic EOD lineage with retry evidence.

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
            # Retry caps prevent new collection attempts; they must not hide a
            # running lineage that still needs lock-protected reconciliation.
            if str(eod_row.status or '').strip().lower() == 'running':
                return AutoEODRecoveryTarget(
                    trade_date=eod_row.trade_date.isoformat(),
                    eod_run_id=eod_row.id,
                )
            eod_has_price_recovery_scope = _eod_has_price_recovery_scope(
                eod_row
            )
            auto_price_attempts = auto_attempt_counts.get(
                eod_row.trade_date, 0
            )
            price_recovery_exhausted = bool(
                eod_has_price_recovery_scope
                and auto_price_attempts
                >= _MAX_AUTO_PRICE_RECOVERY_RUNS_PER_TRADE_DATE
            )
            if (
                eod_has_price_recovery_scope
                and auto_price_attempts
                < _MAX_AUTO_PRICE_RECOVERY_RUNS_PER_TRADE_DATE
            ):
                return AutoEODRecoveryTarget(
                    trade_date=eod_row.trade_date.isoformat(),
                    eod_run_id=eod_row.id,
                )
            pipeline_candidates = _load_pipeline_recovery_candidates(
                session,
                market_dates=[eod_row.trade_date],
            )
            latest_recovery = _latest_exact_followup_recovery(
                candidates=pipeline_candidates,
                eod_run=eod_row,
            )
            followup_attempts = _count_auto_followup_recovery_attempts(
                session,
                eod_run=eod_row,
            )
            if (
                latest_recovery is not None
                and followup_attempts
                <= _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
                and _followup_recovery_is_recoverable_same_day_expiration(
                    session,
                    pipeline_run=latest_recovery,
                    current_market_date=cutoff,
                )
            ):
                return AutoEODRecoveryTarget(
                    trade_date=eod_row.trade_date.isoformat(),
                    eod_run_id=eod_row.id,
                )
            if _followup_recovery_has_valid_same_day_rearm_marker(
                session,
                eod_run=eod_row,
                pipeline_candidates=pipeline_candidates,
                latest_recovery=latest_recovery,
                current_market_date=cutoff,
                followup_attempts=followup_attempts,
            ):
                return AutoEODRecoveryTarget(
                    trade_date=eod_row.trade_date.isoformat(),
                    eod_run_id=eod_row.id,
                )
            if _followup_recovery_has_valid_same_day_rearm_marker(
                session,
                eod_run=eod_row,
                pipeline_candidates=pipeline_candidates,
                latest_recovery=latest_recovery,
                current_market_date=cutoff,
                followup_attempts=followup_attempts,
                allow_stale_target_for_terminal_block=True,
            ):
                return AutoEODRecoveryTarget(
                    trade_date=eod_row.trade_date.isoformat(),
                    eod_run_id=eod_row.id,
                )
            if _eod_has_followup_recovery_scope(eod_row):
                if (
                    latest_recovery is not None
                    and _validated_nonblocking_partial_recovery(
                        session,
                        pipeline_run=latest_recovery,
                        eod_run=eod_row,
                        require_eod_reconciled=False,
                    )
                    is not None
                ):
                    # The suffix finished, but Core may have restarted before
                    # copying the proof to its EOD ancestor. Surface exactly
                    # one lineage for idempotent reconciliation regardless of
                    # the ordinary retry-attempt cap.
                    return AutoEODRecoveryTarget(
                        trade_date=eod_row.trade_date.isoformat(),
                        eod_run_id=eod_row.id,
                    )
                if followup_attempts < _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD:
                    return AutoEODRecoveryTarget(
                        trade_date=eod_row.trade_date.isoformat(),
                        eod_run_id=eod_row.id,
                    )
                if (
                    latest_recovery is not None
                    and _followup_recovery_is_satisfied_for_convergence(
                        session,
                        pipeline_run=latest_recovery,
                    )
                ):
                    return AutoEODRecoveryTarget(
                        trade_date=eod_row.trade_date.isoformat(),
                        eod_run_id=eod_row.id,
                    )
                if (
                    latest_recovery is not None
                    and followup_attempts
                    == _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
                    and _followup_recovery_is_legacy_cutoff_contract_repair(
                        session,
                        pipeline_run=latest_recovery,
                        eod_run=eod_row,
                        has_price_recovery_scope=(
                            eod_has_price_recovery_scope
                        ),
                    )
                ):
                    return AutoEODRecoveryTarget(
                        trade_date=eod_row.trade_date.isoformat(),
                        eod_run_id=eod_row.id,
                    )
                if (
                    latest_recovery is not None
                    and followup_attempts
                    == _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD + 1
                    and (
                        _followup_recovery_is_reclaimable_feedback_lease_loss(
                            session,
                            pipeline_run=latest_recovery,
                        )
                        or _followup_recovery_is_resumable_terminal_continuation(
                            session,
                            pipeline_run=latest_recovery,
                        )
                        or _followup_recovery_is_resumable_cutoff_contract_repair(
                            session,
                            pipeline_run=latest_recovery,
                            eod_run=eod_row,
                        )
                    )
                ):
                    return AutoEODRecoveryTarget(
                        trade_date=eod_row.trade_date.isoformat(),
                        eod_run_id=eod_row.id,
                    )
                repair_markets = eod_row.run_metadata.get('markets')
                if (
                    latest_recovery is not None
                    and followup_attempts
                    == _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
                    and isinstance(repair_markets, list)
                    and _load_latest_pykrx_enrichment_repair_checkpoint(
                        session,
                        trade_date=eod_row.trade_date,
                        markets=repair_markets,
                        completed_after=latest_recovery.activity_at,
                    )
                    is not None
                ):
                    return AutoEODRecoveryTarget(
                        trade_date=eod_row.trade_date.isoformat(),
                        eod_run_id=eod_row.id,
                    )
                continue

            # The runtime cannot resume a primary pipeline from generic stage
            # rows once its only durable EOD authority is price retry scope and
            # that budget is exhausted. Treating those rows as fresh recovery
            # authority would repeatedly select this date while the runtime
            # returns ``auto_retry_limit_reached``, starving newer dates.
            if price_recovery_exhausted:
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
            pipeline_status = (
                str(linked_pipeline.status or '').strip().lower()
                if linked_pipeline is not None
                else ''
            )
            partial_pipeline_reconciled = bool(
                linked_pipeline is not None
                and pipeline_status == 'partial'
                and isinstance(linked_pipeline.run_metadata, dict)
                and linked_pipeline.run_metadata.get('resumable') is False
                and _validated_nonblocking_partial_recovery(
                    session,
                    pipeline_run=linked_pipeline,
                    eod_run=eod_row,
                    require_eod_reconciled=True,
                )
                is not None
            )
            pipeline_is_incomplete = bool(
                linked_pipeline is not None
                and pipeline_status in _RECOVERABLE_DAILY_PIPELINE_STATUSES
                and (
                    (
                        pipeline_status == 'partial'
                        and not partial_pipeline_reconciled
                    )
                    or (
                        pipeline_status != 'partial'
                        and (
                            not isinstance(
                                linked_pipeline.run_metadata,
                                dict,
                            )
                            or linked_pipeline.run_metadata.get(
                                'resumable'
                            )
                            is not False
                        )
                    )
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
            followup_attempts = (
                _count_auto_followup_recovery_attempts(
                    session,
                    eod_run=eod_row,
                )
                if pipeline_is_incomplete
                else 0
            )
            legacy_cutoff_repair = bool(
                pipeline_is_incomplete
                and linked_pipeline is not None
                and followup_attempts
                == _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
                and _followup_recovery_is_legacy_cutoff_contract_repair(
                    session,
                    pipeline_run=linked_pipeline,
                    eod_run=eod_row,
                    has_price_recovery_scope=eod_has_price_recovery_scope,
                )
            )
            resumable_cutoff_repair = bool(
                pipeline_is_incomplete
                and linked_pipeline is not None
                and followup_attempts
                == _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD + 1
                and _followup_recovery_is_resumable_cutoff_contract_repair(
                    session,
                    pipeline_run=linked_pipeline,
                    eod_run=eod_row,
                )
            )
            if pipeline_is_incomplete and (
                followup_attempts
                < _MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_EOD
                or legacy_cutoff_repair
                or resumable_cutoff_repair
            ):
                return AutoEODRecoveryTarget(
                    trade_date=eod_row.trade_date.isoformat(),
                    eod_run_id=eod_row.id,
                )
    return None


def load_oldest_outstanding_auto_eod_recovery_date(
    *,
    on_or_after: str | date,
    on_or_before: str | date,
) -> str | None:
    """Backward-compatible date-only view of the automatic recovery target."""

    target = load_oldest_outstanding_auto_eod_recovery_target(
        on_or_after=on_or_after,
        on_or_before=on_or_before,
    )
    return target.trade_date if target is not None else None


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
            .where(daily_price_bar_has_current_eod_run_provenance(run_id))
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
    statement_timeout_ms: int | None = None,
) -> list[str]:
    session_options = (
        {'statement_timeout_ms': statement_timeout_ms}
        if statement_timeout_ms is not None
        else {}
    )
    with session_scope(**session_options) as session:
        statement = (
            select(Symbol.symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .where(daily_price_bar_has_current_eod_run_provenance(run_id))
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
            .where(daily_price_bar_has_current_eod_run_provenance(run_id))
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
                    # Connection.close() normally returns the PostgreSQL
                    # session to SQLAlchemy's pool, so it does not release a
                    # session-level lock after a failed unlock. Invalidate the
                    # connection without replacing a pipeline result/error.
                    _invalidate_advisory_lock_connection(connection)
                    logger.warning(
                        'Failed to explicitly release EOD advisory lock; '
                        'discarded the dedicated connection.',
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
