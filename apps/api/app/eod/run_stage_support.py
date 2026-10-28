from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.eod.summary_cutoff_contract import (
    normalize_symbol_codes,
    stage_result_summary,
)


@dataclass(frozen=True)
class StageCounts:
    total: int
    processed: int
    succeeded: int
    failed: int
    remaining: int


@dataclass(frozen=True)
class FundamentalsRefreshMetrics:
    total: int
    processed: int
    succeeded: int
    failed: int
    remaining: int
    fresh: int
    refreshed: int
    events_detected: int
    events_captured: int
    events_failed: int


def build_next_open_window_expired_result(
    *,
    market_date: str,
    market: object,
    allow_same_day_intraday_watch_continuation: bool = False,
) -> dict[str, Any] | None:
    """Return an auditable no-op when a recovered plan's window has passed."""

    market_state = str(getattr(market, 'state', '') or '').strip().upper()

    server_time = str(getattr(market, 'server_time', '') or '')
    try:
        current_market_date = date.fromisoformat(server_time[:10])
        source_market_date = date.fromisoformat(market_date)
    except ValueError:
        return None

    from app.orders.next_open_execution import normalize_next_open_target_trade_date

    candidate = source_market_date + timedelta(days=1)
    target_trade_date = normalize_next_open_target_trade_date(
        candidate,
        default=candidate,
        allow_fallback=True,
    )
    target_is_still_actionable = bool(
        (
            market_state == 'PREOPEN'
            and target_trade_date >= current_market_date
        )
        or target_trade_date > current_market_date
        or (
            allow_same_day_intraday_watch_continuation
            and market_state == 'OPEN'
            and target_trade_date == current_market_date
        )
    )
    if target_is_still_actionable:
        return None
    return {
        'triggered': False,
        'status': 'skipped',
        'reason': 'next_open_window_expired',
        'market_date': source_market_date.isoformat(),
        'target_trade_date': target_trade_date.isoformat(),
        'current_market_date': current_market_date.isoformat(),
        'market_state': market_state,
    }


EXPIRED_DECISION_REPLAY_STAGE_IDS: frozenset[str] = frozenset(
    {
        'agent_feedback_summaries',
        'signal_decider',
        'signal_decider_evaluations',
    }
)

AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER: tuple[str, ...] = (
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY = (
    'same_day_intraday_watch_rearm'
)
SAME_DAY_INTRADAY_WATCH_REARM_VERSION = 1
SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT = 8


def pending_same_day_intraday_watch_rearm_marker(
    marker: object,
    *,
    target_trade_date: date | str | None = None,
) -> dict[str, Any] | None:
    """Return a normalized pending marker only when its identity is complete."""

    if not isinstance(marker, Mapping):
        return None
    source_run_id = marker.get('source_false_skip_run_id')
    proof_run_id = marker.get('proof_pipeline_run_id')
    attempt_limit = marker.get('attempt_limit')
    raw_target_trade_date = marker.get('target_trade_date')
    raw_started_at = marker.get('started_at')
    try:
        parsed_target_trade_date = date.fromisoformat(
            str(raw_target_trade_date or '')
        )
        parsed_started_at = datetime.fromisoformat(str(raw_started_at or ''))
    except (TypeError, ValueError):
        return None
    if parsed_started_at.tzinfo is None or parsed_started_at.utcoffset() is None:
        return None
    if target_trade_date is not None:
        try:
            expected_target_trade_date = date.fromisoformat(
                target_trade_date.isoformat()
                if isinstance(target_trade_date, date)
                else str(target_trade_date)
            )
        except (TypeError, ValueError):
            return None
        if parsed_target_trade_date != expected_target_trade_date:
            return None
    if not (
        type(marker.get('version')) is int
        and marker.get('version') == SAME_DAY_INTRADAY_WATCH_REARM_VERSION
        and str(marker.get('status') or '').strip() == 'pending'
        and type(attempt_limit) is int
        and attempt_limit == SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
        and type(source_run_id) is int
        and type(proof_run_id) is int
        and source_run_id > 0
        and proof_run_id > 0
        and proof_run_id < source_run_id
    ):
        return None
    return dict(marker)


def outcome_contains_reason(
    outcome: object,
    *,
    reason: str,
) -> bool:
    """Find a semantic reason in a nested pipeline result without looping."""

    expected_reason = str(reason or '').strip().lower()
    if not expected_reason:
        return False
    pending: list[object] = [outcome]
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        if id(value) in seen:
            continue
        seen.add(id(value))
        if isinstance(value, Mapping):
            if str(value.get('reason') or '').strip().lower() == expected_reason:
                return True
            pending.extend(value.values())
        elif isinstance(value, (list, tuple)):
            pending.extend(value)
    return False


def is_completed_auto_pitr_intraday_watch_recovery(
    recovery_result: Mapping[str, Any] | None,
    *,
    expected_eod_collection_run_id: int,
) -> bool:
    """Verify terminal evidence before a pending same-day marker is cleared."""

    if not isinstance(recovery_result, Mapping):
        return False
    if outcome_contains_reason(
        recovery_result,
        reason='next_open_window_expired',
    ):
        return False
    pipeline_run = recovery_result.get('pipeline_run')
    stages = recovery_result.get('stages')
    resolved_stages = recovery_result.get('resolved_stages')
    pipeline_run_id = recovery_result.get('pipeline_run_id')
    if not (
        type(pipeline_run_id) is int
        and pipeline_run_id > 0
        and isinstance(pipeline_run, Mapping)
        and pipeline_run.get('id') == pipeline_run_id
        and isinstance(stages, Mapping)
        and isinstance(resolved_stages, (list, tuple))
        and tuple(resolved_stages)
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        and set(stages) == set(AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER)
    ):
        return False
    metadata = get_run_metadata(pipeline_run)
    if not (
        metadata.get('entrypoint') == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get('same_day_intraday_watch_continuation_admitted')
        is True
        and tuple(metadata.get('resolved_stages') or ())
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        and pipeline_run.get('current_stage')
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
        and str(pipeline_run.get('status') or '').strip().lower()
        == str(recovery_result.get('status') or '').strip().lower()
        and is_exact_auto_pitr_intraday_watch_recovery_request(
            trigger=str(pipeline_run.get('trigger') or ''),
            point_in_time_recovery=metadata.get('point_in_time_recovery'),
            reused_eod_collection_run_id=metadata.get(
                'reused_eod_collection_run_id'
            ),
            force=metadata.get('force'),
            reuse_existing_feedback=metadata.get('reuse_existing_feedback'),
            resume_from_stage=metadata.get('resume_from_stage'),
            resolved_stages=resolved_stages,
        )
        and metadata.get('reused_eod_collection_run_id')
        == expected_eod_collection_run_id
    ):
        return False

    from app.eod.daily_pipeline_outcomes import (
        daily_pipeline_outcome_is_satisfied,
    )

    return bool(
        daily_pipeline_outcome_is_satisfied(recovery_result)
        and all(
            daily_pipeline_outcome_is_satisfied(stages[stage_id])
            for stage_id in AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        )
    )


def is_exact_auto_pitr_intraday_watch_recovery_request(
    *,
    trigger: str,
    point_in_time_recovery: bool,
    reused_eod_collection_run_id: object,
    force: bool,
    reuse_existing_feedback: bool,
    resume_from_stage: str | None,
    resolved_stages: Sequence[str],
) -> bool:
    """Recognize only the lineage-bound automatic intraday-watch suffix."""

    exact_stages = bool(
        isinstance(resolved_stages, (list, tuple))
        and tuple(resolved_stages)
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
    )
    return bool(
        trigger == 'auto_eod_followup_resume'
        and point_in_time_recovery is True
        and isinstance(reused_eod_collection_run_id, int)
        and not isinstance(reused_eod_collection_run_id, bool)
        and reused_eod_collection_run_id > 0
        and force is False
        and reuse_existing_feedback is True
        and resume_from_stage
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[0]
        and exact_stages
    )


def is_canonical_same_day_signal_rearm_proof(
    pipeline_run: Mapping[str, Any] | None,
    *,
    expected_eod_collection_run_id: int | None = None,
) -> bool:
    """Validate the one immutable signal proof a same-day marker may name."""

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = get_run_metadata(pipeline_run)
    stages = pipeline_run.get('stages')
    resolved_stages = metadata.get('resolved_stages')
    if not (
        isinstance(stages, Mapping)
        and set(stages)
        == set(AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER)
        and metadata.get('entrypoint')
        == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get(
            'same_day_intraday_watch_continuation_admitted'
        ) is True
        and isinstance(resolved_stages, (list, tuple))
        and is_exact_auto_pitr_intraday_watch_recovery_request(
            trigger=str(pipeline_run.get('trigger') or ''),
            point_in_time_recovery=metadata.get(
                'point_in_time_recovery'
            ),
            reused_eod_collection_run_id=metadata.get(
                'reused_eod_collection_run_id'
            ),
            force=metadata.get('force'),
            reuse_existing_feedback=metadata.get(
                'reuse_existing_feedback'
            ),
            resume_from_stage=metadata.get('resume_from_stage'),
            resolved_stages=resolved_stages,
        )
        and (
            expected_eod_collection_run_id is None
            or metadata.get('reused_eod_collection_run_id')
            == expected_eod_collection_run_id
        )
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'blocked'
        and pipeline_run.get('current_stage')
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
    ):
        return False

    cutoff = metadata.get('feedback_knowledge_cutoff')
    raw_cutoff_at = (
        cutoff.get('knowledge_cutoff_at')
        if isinstance(cutoff, Mapping)
        else None
    )
    try:
        cutoff_at = (
            raw_cutoff_at
            if isinstance(raw_cutoff_at, datetime)
            else datetime.fromisoformat(str(raw_cutoff_at or ''))
        )
    except (TypeError, ValueError):
        return False
    requested_symbols = (
        cutoff.get('requested_symbols')
        if isinstance(cutoff, Mapping)
        else None
    )
    if not (
        isinstance(cutoff, Mapping)
        and cutoff.get('ready') is True
        and cutoff_at.tzinfo is not None
        and cutoff_at.utcoffset() is not None
        and str(cutoff.get('source_run_id') or '').strip()
        and str(cutoff.get('feedback_scope_mode') or '').strip()
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and str(metadata.get('fs_div') or '').strip().upper()
        in {'CFS', 'OFS'}
        and isinstance(requested_symbols, list)
        and bool(requested_symbols)
        and normalize_symbol_codes(requested_symbols) == requested_symbols
    ):
        return False

    signal_stage = stages.get('signal_decider')
    plan_stage = stages.get('next_open_execution_plan')
    evaluation_stage = stages.get('signal_decider_evaluations')
    if not all(
        isinstance(stage, Mapping)
        for stage in (signal_stage, plan_stage, evaluation_stage)
    ):
        return False
    result_summary = stage_result_summary(signal_stage)
    progress = result_summary.get('progress')
    prerequisites = result_summary.get('prerequisites')
    if not isinstance(prerequisites, Mapping) and isinstance(
        progress,
        Mapping,
    ):
        prerequisites = progress.get('prerequisites')
    summary = (
        progress.get('summary')
        if isinstance(progress, Mapping)
        else None
    )
    role_progress = (
        summary.get('role_progress')
        if isinstance(summary, Mapping)
        else None
    )
    failure_groups = (
        summary.get('failure_groups')
        if isinstance(summary, Mapping)
        else None
    )
    source_run_ids = signal_stage.get('source_run_ids')
    result_run_id = str(result_summary.get('run_id') or '').strip()
    source_ids = {
        str(value or '').strip()
        for value in (
            source_run_ids.values()
            if isinstance(source_run_ids, Mapping)
            else ()
        )
        if str(value or '').strip()
    }
    plan_summary = stage_result_summary(plan_stage)
    evaluation_summary = stage_result_summary(evaluation_stage)
    try:
        total = int(signal_stage.get('total') or 0)
        processed = int(signal_stage.get('processed') or 0)
        succeeded = int(signal_stage.get('succeeded') or 0)
        failed = int(signal_stage.get('failed') or 0)
        remaining = int(signal_stage.get('remaining') or 0)
    except (TypeError, ValueError):
        return False
    if not (
        str(signal_stage.get('status') or '').strip().lower()
        == 'partial'
        and total > 0
        and processed == total
        and failed > 0
        and remaining == 0
        and succeeded + failed == processed
        and result_run_id
        and result_run_id in source_ids
        and len(source_ids) == 1
        and isinstance(source_run_ids, Mapping)
        and set(source_run_ids).issubset(
            {'run_id', 'agent_feedback_run_id'}
        )
        and 'openclaw_dispatch_circuit_breaker' not in result_summary
        and str(plan_stage.get('status') or '').strip().lower()
        == 'blocked'
        and str(evaluation_stage.get('status') or '').strip().lower()
        == 'blocked'
        and str(plan_summary.get('reason') or '').strip()
        == 'signal_decider_stage_incomplete'
        and str(evaluation_summary.get('reason') or '').strip()
        == 'declared_dependencies_incomplete'
        and evaluation_summary.get('incomplete_dependencies')
        == ['signal_decider']
        and isinstance(prerequisites, Mapping)
        and prerequisites.get('ready') is True
        and isinstance(summary, Mapping)
        and str(summary.get('run_status') or '').strip().lower()
        in {'partial', 'failed'}
        and isinstance(role_progress, Mapping)
        and bool(role_progress)
        and set(role_progress).issubset(DECISION_SUPPORT_AGENT_NAMES)
        and all(
            isinstance(role, Mapping)
            for role in role_progress.values()
        )
        and isinstance(failure_groups, Mapping)
        and bool(failure_groups)
    ):
        return False
    try:
        failed_items = int(summary.get('failed_items') or 0)
        retry_pending_items = sum(
            int(role.get('retry_pending_symbols') or 0)
            for role in role_progress.values()
        )
        in_progress_items = sum(
            int(role.get('in_progress_symbols') or 0)
            for role in role_progress.values()
        )
        pending_items = sum(
            int(role.get('pending_symbols') or 0)
            for role in role_progress.values()
        )
        retryable_failure_groups = all(
            isinstance(group, Mapping)
            and int(group.get('count') or 0) > 0
            and int(group.get('retryable_count') or 0)
            == int(group.get('count') or 0)
            for group in failure_groups.values()
        )
    except (TypeError, ValueError):
        return False
    return bool(
        failed_items > 0
        and retry_pending_items == failed_items
        and in_progress_items == 0
        and pending_items == 0
        and retryable_failure_groups
    )
def is_recoverable_same_day_intraday_watch_expiration(
    pipeline_run: Mapping[str, Any] | None,
    *,
    current_market_date: date | str,
) -> bool:
    """Keep an exact same-day OPEN false-skip recoverable as durable debt."""

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = get_run_metadata(pipeline_run)
    if not (
        pipeline_run.get('status') == 'skipped'
        and pipeline_run.get('current_stage')
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
        and str(pipeline_run.get('completed_at') or '').strip()
        and not str(pipeline_run.get('error_message') or '').strip()
        and metadata.get('entrypoint')
        == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get(
            'same_day_intraday_watch_continuation_admitted'
        )
        is False
    ):
        return False
    resolved_stages = metadata.get('resolved_stages')
    if not isinstance(resolved_stages, (list, tuple)):
        return False
    if not is_exact_auto_pitr_intraday_watch_recovery_request(
        trigger=str(pipeline_run.get('trigger') or ''),
        point_in_time_recovery=metadata.get('point_in_time_recovery'),
        reused_eod_collection_run_id=metadata.get(
            'reused_eod_collection_run_id'
        ),
        force=metadata.get('force'),
        reuse_existing_feedback=metadata.get('reuse_existing_feedback'),
        resume_from_stage=metadata.get('resume_from_stage'),
        resolved_stages=resolved_stages,
    ):
        return False

    try:
        normalized_current_market_date = date.fromisoformat(
            current_market_date.isoformat()[:10]
            if isinstance(current_market_date, date)
            else str(current_market_date)
        )
        source_market_date = date.fromisoformat(
            pipeline_run['market_date'].isoformat()[:10]
            if isinstance(pipeline_run.get('market_date'), date)
            else str(pipeline_run.get('market_date') or '')
        )
    except (KeyError, TypeError, ValueError):
        return False
    if source_market_date >= normalized_current_market_date:
        return False

    raw_stages = pipeline_run.get('stages')
    if not isinstance(raw_stages, Mapping) or set(raw_stages) != set(
        AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
    ):
        return False

    for stage_id in AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER:
        stage_row = raw_stages.get(stage_id)
        if not isinstance(stage_row, Mapping):
            return False
        if stage_row.get('status') != 'skipped':
            return False
        if any(
            stage_row.get(field) != 0
            for field in (
                'processed',
                'total',
                'succeeded',
                'failed',
                'remaining',
            )
        ):
            return False
        source_run_ids = stage_row.get('source_run_ids')
        if not isinstance(source_run_ids, Mapping) or source_run_ids:
            return False
        details = stage_row.get('details')
        result_summary = (
            details.get('result_summary')
            if isinstance(details, Mapping)
            else None
        )
        if not isinstance(result_summary, Mapping):
            return False
        if not (
            result_summary.get('triggered') is False
            and result_summary.get('status') == 'skipped'
            and result_summary.get('reason') == 'next_open_window_expired'
            and result_summary.get('market_state') == 'OPEN'
            and result_summary.get('stage') == stage_id
        ):
            return False
        try:
            result_market_date = date.fromisoformat(
                str(result_summary.get('market_date') or '')
            )
            result_target_trade_date = date.fromisoformat(
                str(result_summary.get('target_trade_date') or '')
            )
            result_current_market_date = date.fromisoformat(
                str(result_summary.get('current_market_date') or '')
            )
        except ValueError:
            return False
        if not (
            result_market_date == source_market_date
            and result_target_trade_date == normalized_current_market_date
            and result_current_market_date == normalized_current_market_date
        ):
            return False

    return True


def build_expired_decision_replay_result(
    *,
    stage_id: str,
    market_date: str,
    market: object,
    allow_same_day_intraday_watch_continuation: bool = False,
) -> dict[str, Any] | None:
    """Fence decision replay once its next-open action window has passed."""

    if stage_id not in EXPIRED_DECISION_REPLAY_STAGE_IDS:
        return None
    expired_result = build_next_open_window_expired_result(
        market_date=market_date,
        market=market,
        allow_same_day_intraday_watch_continuation=(
            allow_same_day_intraday_watch_continuation
        ),
    )
    if expired_result is None:
        return None
    return {
        **expired_result,
        'stage': stage_id,
    }


def admits_same_day_intraday_watch_continuation(
    *,
    market_date: str,
    market: object,
    allow_open_recovery_start: bool = False,
) -> bool:
    """Admit same-day PREOPEN, plus an explicitly verified OPEN recovery."""

    market_state = str(getattr(market, 'state', '') or '').strip().upper()
    if market_state != 'PREOPEN' and not (
        allow_open_recovery_start and market_state == 'OPEN'
    ):
        return False
    server_time = str(getattr(market, 'server_time', '') or '')
    try:
        current_market_date = date.fromisoformat(server_time[:10])
        source_market_date = date.fromisoformat(market_date)
    except ValueError:
        return False

    from app.orders.next_open_execution import normalize_next_open_target_trade_date

    candidate = source_market_date + timedelta(days=1)
    target_trade_date = normalize_next_open_target_trade_date(
        candidate,
        default=candidate,
        allow_fallback=True,
    )
    return target_trade_date == current_market_date


def get_run_metadata(run: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(run, Mapping):
        return {}
    metadata = run.get('run_metadata')
    return metadata if isinstance(metadata, dict) else {}


def normalize_run_status(value: str | None) -> str | None:
    if not value:
        return None
    if value == 'completed_with_errors':
        return 'partial'
    if value == 'skipped':
        return 'pending'
    return value


def resolve_run_updated_at(run: Mapping[str, Any] | None) -> Any | None:
    if not isinstance(run, Mapping):
        return None
    metadata = get_run_metadata(run)
    return metadata.get('last_progress_at') or run.get('completed_at') or run.get('started_at')


def select_preferred_eod_run(rows: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return next(
        (
            row
            for row in rows
            if get_run_metadata(row).get('mode') != 'historical_backfill'
        ),
        rows[0],
    )


def build_stage_counts(
    *,
    total: Any,
    succeeded: Any,
    failed: Any,
    processed: Any | None = None,
) -> StageCounts:
    total_value = int(total or 0)
    succeeded_value = int(succeeded or 0)
    failed_value = int(failed or 0)
    processed_value = int(processed or 0) if processed is not None else succeeded_value + failed_value
    return StageCounts(
        total=total_value,
        processed=processed_value,
        succeeded=succeeded_value,
        failed=failed_value,
        remaining=max(total_value - processed_value, 0) if total_value else 0,
    )


def extract_next_open_plan_stage_counts(
    result: Mapping[str, Any],
) -> StageCounts:
    """Count every plan-row outcome without treating a safe block as failure."""

    def _as_nonnegative_int(value: Any) -> int:
        try:
            return max(int(value or 0), 0)
        except (TypeError, ValueError):
            return 0

    summary = result.get('summary')
    if not isinstance(summary, Mapping):
        summary = {}
    outcome_count = sum(
        _as_nonnegative_int(summary.get(key))
        for key in (
            'eligible_count',
            'monitor_candidate_count',
            'blocked_count',
        )
    )
    reported_total = _as_nonnegative_int(summary.get('items_total'))
    total = max(reported_total, outcome_count)
    return build_stage_counts(
        total=total,
        succeeded=outcome_count,
        failed=max(total - outcome_count, 0),
    )


def extract_run_stage_counts(
    run: Mapping[str, Any],
    *,
    total_key: str = 'symbols_total',
    succeeded_key: str = 'symbols_succeeded',
    failed_key: str = 'symbols_failed',
    processed_key: str | None = None,
) -> StageCounts:
    processed = run.get(processed_key) if processed_key is not None else None
    return build_stage_counts(
        total=run.get(total_key),
        succeeded=run.get(succeeded_key),
        failed=run.get(failed_key),
        processed=processed,
    )


def extract_fundamentals_refresh_metrics(run: Mapping[str, Any]) -> FundamentalsRefreshMetrics:
    metadata = get_run_metadata(run)
    fresh = int(run.get('symbols_fresh') or 0)
    refreshed = int(run.get('symbols_refreshed') or 0)
    counts = build_stage_counts(
        total=metadata.get('symbols_selected') or run.get('symbols_total'),
        processed=run.get('symbols_checked'),
        succeeded=fresh + refreshed,
        failed=run.get('symbols_failed'),
    )
    return FundamentalsRefreshMetrics(
        total=counts.total,
        processed=counts.processed,
        succeeded=counts.succeeded,
        failed=counts.failed,
        remaining=counts.remaining,
        fresh=fresh,
        refreshed=refreshed,
        events_detected=int(run.get('events_detected') or 0),
        events_captured=int(run.get('events_captured') or 0),
        events_failed=int(run.get('events_failed') or 0),
    )
