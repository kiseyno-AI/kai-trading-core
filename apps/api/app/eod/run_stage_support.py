from __future__ import annotations

from dataclasses import dataclass
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
