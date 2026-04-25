from __future__ import annotations

from typing import Any

from app.market_data_store import DAY_BASED_TRENDING_PERIODS

PYKRX_STAGE_ORDER = (
    'price_collection',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
)
STAGE_LABELS = {
    'price_collection': 'pykrx daily price collection',
    'technical_snapshots': 'technical snapshot compute',
    'action_summaries': 'action summary compute',
    'trending_ranks': 'stored trending rank rebuild',
    'fundamentals_daily_refresh': 'DART fundamentals daily refresh',
}
STAGE_PROGRESS_RESERVED_KEYS = {
    'started_at',
    'updated_at',
    'completed_at',
    'status',
    'current_step',
    'current_activity',
    'processed',
    'total',
    'succeeded',
    'failed',
    'current_symbol',
    'current_period',
}


def _normalize_status(value: str | None) -> str | None:
    if not value:
        return None
    if value == 'completed_with_errors':
        return 'partial'
    if value == 'skipped':
        return 'pending'
    return value


def _stage_index(stage: str | None) -> int | None:
    if stage is None:
        return None
    try:
        return PYKRX_STAGE_ORDER.index(stage)
    except ValueError:
        return None


def _default_activity(label: str, status: str) -> str:
    if status == 'pending':
        return f'Pending {label}'
    if status == 'running':
        return f'Running {label}'
    if status == 'completed':
        return f'Completed {label}'
    if status == 'partial':
        return f'Completed {label} with errors'
    if status == 'failed':
        return f'Failed {label}'
    return label


def _progress_extras(progress: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in progress.items()
        if key not in STAGE_PROGRESS_RESERVED_KEYS and value is not None
    }


def _infer_pykrx_stage(
    *,
    run_status: str | None,
    run_metadata: dict[str, Any],
    stage_counts: dict[str, dict[str, Any]],
) -> str | None:
    stage = run_metadata.get('stage')
    if stage in PYKRX_STAGE_ORDER:
        return stage
    stage_progress = run_metadata.get('stage_progress') if isinstance(run_metadata.get('stage_progress'), dict) else {}
    for candidate in reversed(PYKRX_STAGE_ORDER):
        if stage_progress.get(candidate):
            return candidate
    if int(stage_counts.get('trending_ranks', {}).get('periods_persisted') or 0) > 0:
        return 'trending_ranks'
    if int(stage_counts.get('action_summaries', {}).get('persisted') or 0) > 0:
        return 'action_summaries'
    if int(stage_counts.get('technical_snapshots', {}).get('persisted') or 0) > 0:
        return 'technical_snapshots'
    if run_status:
        return 'price_collection'
    return None


def _build_fundamentals_activity(metadata: dict[str, Any], status: str) -> str:
    stage = str(metadata.get('stage') or '').strip()
    if not stage:
        return _default_activity(STAGE_LABELS['fundamentals_daily_refresh'], status)

    if stage == 'syncing_disclosures':
        group = metadata.get('current_sync_group') or {}
        corporation_count = group.get('corporation_count')
        check_start_date = group.get('check_start_date')
        if corporation_count and check_start_date:
            return f'Syncing DART disclosures for {corporation_count} corporations from {check_start_date}'
        return 'Syncing DART disclosures'

    if stage == 'capturing_fundamentals':
        capture_event_count = metadata.get('capture_event_count') or metadata.get('candidate_event_count')
        retry_cycle = metadata.get('retry_cycle')
        retry_cycles_requested = metadata.get('retry_cycles_requested')
        batch_index = metadata.get('batch_index')
        batch_count = metadata.get('batch_count')
        parts: list[str] = []
        if capture_event_count:
            parts.append(f'{capture_event_count} events')
        if retry_cycle and retry_cycles_requested:
            parts.append(f'retry cycle {retry_cycle}/{retry_cycles_requested}')
        if batch_index and batch_count:
            parts.append(f'batch {batch_index}/{batch_count}')
        if parts:
            return 'Capturing DART fundamentals for ' + ', '.join(parts)
        return 'Capturing DART fundamentals'

    if stage == 'loading_capture_candidates':
        return 'Loading DART capture candidates'
    if stage == 'capture_candidates_loaded':
        pending_event_count = metadata.get('pending_event_count')
        if pending_event_count is not None:
            return f'Loaded {pending_event_count} DART capture candidates'
        return 'Loaded DART capture candidates'
    if stage == 'sync_groups_prepared':
        sync_group_count = metadata.get('sync_group_count')
        if sync_group_count is not None:
            return f'Prepared {sync_group_count} DART sync groups'
        return 'Prepared DART sync groups'
    if stage == 'sync_groups_finished':
        return 'Finished DART disclosure sync groups'

    return stage.replace('_', ' ')
