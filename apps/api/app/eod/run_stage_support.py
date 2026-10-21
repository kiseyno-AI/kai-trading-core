from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping, Sequence


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
) -> bool:
    """Admit only a recovery already inside its same-day PREOPEN window."""

    if str(getattr(market, 'state', '') or '').strip().upper() != 'PREOPEN':
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
