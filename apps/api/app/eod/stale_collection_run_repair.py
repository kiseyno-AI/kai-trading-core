"""Exact-scope repair for the reviewed stale EOD collection-run incident.

This module is intentionally a one-time, persistence-only operation.  It does
not bootstrap the application, run migrations, call providers, or rebuild any
derived data.  Dry-run is the default; apply is admitted only when the frozen
manifest, parent corroboration, global running set, child references, and
provider advisory fences all still match the reviewed incident.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Mapping, Sequence

from sqlalchemy import func, select

from app.db import session_scope
from app.db_models import (
    DailyActionSummary,
    DailyMarketRiskFilterResult,
    DailyMarketRiskSnapshot,
    DailyPipelineRun,
    DailyPriceBar,
    DailyPriceContext,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    EODCollectionRun,
)
from app.repositories.eod_run_repository import advisory_eod_collection_locks


INCIDENT_ID = 'stale_eod_collection_runs_139_360_361_362_20261031'
MANIFEST_SCHEMA_VERSION = 'stale_eod_collection_run_repair_manifest_v1'
MARKER_SCHEMA_VERSION = 'stale_eod_collection_run_repair_marker_v1'
REPAIR_MARKER_KEY = 'stale_eod_collection_run_repair'
TARGET_STATUS = 'interrupted'
REPAIR_ERROR_MESSAGE = (
    'Interrupted by exact manual stale-run repair '
    f'{INCIDENT_ID} after advisory-lock and row revalidation.'
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(',', ':'),
        sort_keys=True,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


TARGET_ROW_STATE_FIELDS = (
    'id',
    'market',
    'trade_date',
    'source',
    'trigger',
    'status',
    'symbols_total',
    'symbols_succeeded',
    'symbols_failed',
    'error_message',
    'started_at',
    'completed_at',
    'run_metadata',
)
PARENT_ROW_STATE_FIELDS = (
    'id',
    'market_date',
    'trigger',
    'status',
    'current_stage',
    'started_at',
    'completed_at',
    'error_message',
    'run_metadata',
    'created_at',
    'updated_at',
)


def _normalize_target_row(row: EODCollectionRun) -> dict[str, Any]:
    """Return the stable, explicit persisted state used by the audit."""
    return {
        'id': int(row.id),
        'market': row.market,
        'trade_date': row.trade_date.isoformat(),
        'source': row.source,
        'trigger': row.trigger,
        'status': row.status,
        'symbols_total': int(row.symbols_total),
        'symbols_succeeded': int(row.symbols_succeeded),
        'symbols_failed': int(row.symbols_failed),
        'error_message': row.error_message,
        'started_at': _utc_iso(row.started_at),
        'completed_at': _utc_iso(row.completed_at),
        'run_metadata': deepcopy(row.run_metadata),
    }


def _normalize_parent_row(row: DailyPipelineRun) -> dict[str, Any]:
    """Return every persisted parent column without ORM-private state."""
    return {
        'id': int(row.id),
        'market_date': row.market_date.isoformat(),
        'trigger': row.trigger,
        'status': row.status,
        'current_stage': row.current_stage,
        'started_at': _utc_iso(row.started_at),
        'completed_at': _utc_iso(row.completed_at),
        'error_message': row.error_message,
        'run_metadata': deepcopy(row.run_metadata),
        'created_at': _utc_iso(row.created_at),
        'updated_at': _utc_iso(row.updated_at),
    }


def _expected_state(
    expected: Mapping[str, Any],
    fields: Sequence[str],
) -> dict[str, Any]:
    return {field: deepcopy(expected.get(field)) for field in fields}


def _state_mismatches(actual: Any, expected: Any, *, path: str = '') -> list[str]:
    if isinstance(actual, dict) and isinstance(expected, dict):
        mismatches: list[str] = []
        for key in sorted(set(actual) | set(expected)):
            child_path = f'{path}.{key}' if path else str(key)
            if key not in actual or key not in expected:
                mismatches.append(child_path)
                continue
            mismatches.extend(
                _state_mismatches(actual[key], expected[key], path=child_path)
            )
        return mismatches
    if isinstance(actual, list) and isinstance(expected, list):
        if len(actual) != len(expected):
            return [path]
        mismatches = []
        for index, (actual_item, expected_item) in enumerate(
            zip(actual, expected, strict=True)
        ):
            mismatches.extend(
                _state_mismatches(
                    actual_item,
                    expected_item,
                    path=f'{path}[{index}]',
                )
            )
        return mismatches
    return [] if actual == expected else [path]


CHILD_REFERENCE_FIELDS = (
    ('daily_price_bars.eod_run_id', DailyPriceBar.eod_run_id),
    (
        'daily_price_bars.repair_origin_eod_run_id',
        DailyPriceBar.repair_origin_eod_run_id,
    ),
    ('daily_price_contexts.eod_run_id', DailyPriceContext.eod_run_id),
    (
        'daily_technical_snapshots.eod_run_id',
        DailyTechnicalSnapshot.eod_run_id,
    ),
    ('daily_action_summaries.eod_run_id', DailyActionSummary.eod_run_id),
    ('daily_trending_ranks.eod_run_id', DailyTrendingRank.eod_run_id),
    (
        'daily_market_risk_snapshots.eod_run_id',
        DailyMarketRiskSnapshot.eod_run_id,
    ),
    (
        'daily_market_risk_filter_results.eod_run_id',
        DailyMarketRiskFilterResult.eod_run_id,
    ),
)


def _empty_child_reference_counts(*, direct_price_rows: int) -> dict[str, int]:
    return {
        label: direct_price_rows if label == 'daily_price_bars.eod_run_id' else 0
        for label, _column in CHILD_REFERENCE_FIELDS
    }


_ALL_DAILY_PIPELINE_STAGES = [
    'master_refresh',
    'pykrx_universe_refresh',
    'price_collection',
    'pykrx_enrichment',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
    'fundamentals_daily_refresh',
    'piotroski_filter',
    'momentum_factors',
    'value_factors',
    'low_risk_factors',
    'beta_factors',
    'advanced_value_factors',
    'unified_value_factors',
    'quality_factors',
    'composite_factors',
    'external_news_collect',
    'external_news_enrich',
    'macro_context_refresh',
]


def _frozen_parent_metadata(*, all_stages: bool) -> dict[str, Any]:
    stages = (
        deepcopy(_ALL_DAILY_PIPELINE_STAGES)
        if all_stages
        else ['price_collection']
    )
    return {
        'capture_limit': 500,
        'entrypoint': 'daily_pipeline_stage_service.run',
        'force': True,
        'fs_div': 'CFS',
        'interrupted_at': '2026-07-13T23:51:53.505526+00:00',
        'interrupted_reason': (
            'Core process started while previous daily pipeline run was '
            'still marked running.'
        ),
        'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
        'requested_stages': deepcopy(stages),
        'resolved_stages': deepcopy(stages),
        'resumable': True,
        'resume_from_stage': None,
        'symbol_limit': None,
        'symbol_offset': 0,
        'symbols': None,
    }


FROZEN_REPAIR_MANIFEST: dict[str, Any] = {
    'schema_version': MANIFEST_SCHEMA_VERSION,
    'incident_id': INCIDENT_ID,
    'target_status': TARGET_STATUS,
    'expected_global_running_ids': [139, 360, 361, 362],
    'rows': [
        {
            'id': 139,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 3581,
            'symbols_succeeded': 1535,
            'symbols_failed': 1,
            'error_message': None,
            'started_at': '2026-04-21T06:40:21.557847+00:00',
            'completed_at': None,
            'run_metadata': {
                'latest_available_trade_date': '2026-04-21',
                'limited_run': False,
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'mode': 'daily_auto',
                'requested_end_date': '20260421',
                'requested_start_date': '20260421',
                'symbol': None,
                'universe_refreshed': True,
                'universe_snapshot': {
                    'deactivate_missing': True,
                    'latest_trade_date': '2026-04-21',
                    'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                    'persisted_count': 3581,
                    'source': 'pykrx',
                    'symbol_count': 3581,
                    'universe_source': (
                        'pykrx_market_ohlcv_with_master_fallback'
                    ),
                },
            },
            'child_reference_counts': _empty_child_reference_counts(
                direct_price_rows=0
            ),
            'expected_row_sha256': (
                '941cbfd01167a5341ac12a2f27066b4cf1ac41553bc09d7cc173439b64c6bb98'
            ),
        },
        {
            'id': 360,
            'market': 'KRX',
            'trade_date': '2026-07-09',
            'source': 'pykrx',
            'trigger': 'internal_ops_cli',
            'status': 'running',
            'symbols_total': 2797,
            'symbols_succeeded': 1756,
            'symbols_failed': 1,
            'error_message': None,
            'started_at': '2026-07-13T16:51:48.874254+00:00',
            'completed_at': None,
            'run_metadata': {
                'current_activity': (
                    'Collecting pykrx daily prices for 321820'
                ),
                'current_step': 'fetch_symbol_ohlcv',
                'current_symbol': '321820',
                'last_progress_at': '2026-07-13T16:58:31.071855+00:00',
                'latest_available_trade_date': '2026-07-09',
                'limited_run': False,
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'mode': 'daily_stage_run',
                'requested_end_date': '20260709',
                'requested_start_date': '20260709',
                'stage': 'price_collection',
                'stage_progress': {
                    'price_collection': {
                        'bars_upserted': 1756,
                        'current_activity': (
                            'Collecting pykrx daily prices for 321820'
                        ),
                        'current_step': 'fetch_symbol_ohlcv',
                        'current_symbol': '321820',
                        'failed': 1,
                        'max_non_blocking_missing_ratio': 0.1,
                        'minor_missing_gap_treated_as_success': True,
                        'missing_ratio': 0.000358,
                        'processed': 1757,
                        'started_at': (
                            '2026-07-13T16:51:48.871636+00:00'
                        ),
                        'status': 'running',
                        'succeeded': 1756,
                        'total': 2797,
                        'updated_at': (
                            '2026-07-13T16:58:31.071855+00:00'
                        ),
                    }
                },
                'symbol': None,
                'universe_refreshed': True,
                'universe_snapshot': {
                    'deactivate_missing': True,
                    'latest_trade_date': '2026-07-13',
                    'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                    'persisted_count': 2797,
                    'source': 'pykrx',
                    'symbol_count': 2797,
                    'universe_source': 'pykrx_market_ohlcv',
                },
            },
            'child_reference_counts': _empty_child_reference_counts(
                direct_price_rows=567
            ),
            'expected_row_sha256': (
                '3e60952e3240e5847edc4e188df9d51428a47c5acf6d3da5b32e7a3780066983'
            ),
        },
        {
            'id': 361,
            'market': 'KRX',
            'trade_date': '2026-07-09',
            'source': 'pykrx',
            'trigger': 'internal_ops_cli',
            'status': 'running',
            'symbols_total': 3702,
            'symbols_succeeded': 2229,
            'symbols_failed': 28,
            'error_message': None,
            'started_at': '2026-07-13T17:01:46.755699+00:00',
            'completed_at': None,
            'run_metadata': {
                'current_activity': (
                    'Collecting pykrx daily prices for 008870'
                ),
                'current_step': 'fetch_symbol_ohlcv',
                'current_symbol': '008870',
                'last_progress_at': '2026-07-13T17:11:47.975108+00:00',
                'latest_available_trade_date': '2026-07-09',
                'limited_run': False,
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'mode': 'daily_stage_run',
                'requested_end_date': '20260709',
                'requested_start_date': '20260709',
                'stage': 'price_collection',
                'stage_progress': {
                    'price_collection': {
                        'bars_upserted': 2229,
                        'current_activity': (
                            'Collecting pykrx daily prices for 008870'
                        ),
                        'current_step': 'fetch_symbol_ohlcv',
                        'current_symbol': '008870',
                        'failed': 28,
                        'max_non_blocking_missing_ratio': 0.1,
                        'minor_missing_gap_treated_as_success': True,
                        'missing_ratio': 0.007563,
                        'processed': 2257,
                        'started_at': (
                            '2026-07-13T17:01:46.754806+00:00'
                        ),
                        'status': 'running',
                        'succeeded': 2229,
                        'total': 3702,
                        'updated_at': (
                            '2026-07-13T17:11:47.975108+00:00'
                        ),
                    }
                },
                'symbol': None,
                'universe_refreshed': False,
                'universe_snapshot': None,
            },
            'child_reference_counts': _empty_child_reference_counts(
                direct_price_rows=2227
            ),
            'expected_row_sha256': (
                '749365c04f7c093d5c712073b7fb816fe173e7b9f01d0020e73e5402b4a5e780'
            ),
        },
        {
            'id': 362,
            'market': 'KRX',
            'trade_date': '2026-07-10',
            'source': 'pykrx',
            'trigger': 'internal_ops_cli',
            'status': 'running',
            'symbols_total': 3702,
            'symbols_succeeded': 2546,
            'symbols_failed': 28,
            'error_message': None,
            'started_at': '2026-07-13T18:17:10.173208+00:00',
            'completed_at': None,
            'run_metadata': {
                'current_activity': (
                    'Collecting pykrx daily prices for 079980'
                ),
                'current_step': 'fetch_symbol_ohlcv',
                'current_symbol': '079980',
                'last_progress_at': '2026-07-13T18:29:15.317925+00:00',
                'latest_available_trade_date': '2026-07-10',
                'limited_run': False,
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'mode': 'daily_stage_run',
                'requested_end_date': '20260710',
                'requested_start_date': '20260710',
                'stage': 'price_collection',
                'stage_progress': {
                    'price_collection': {
                        'bars_upserted': 2546,
                        'current_activity': (
                            'Collecting pykrx daily prices for 079980'
                        ),
                        'current_step': 'fetch_symbol_ohlcv',
                        'current_symbol': '079980',
                        'failed': 28,
                        'max_non_blocking_missing_ratio': 0.1,
                        'minor_missing_gap_treated_as_success': True,
                        'missing_ratio': 0.007563,
                        'processed': 2574,
                        'started_at': (
                            '2026-07-13T18:17:10.170500+00:00'
                        ),
                        'status': 'running',
                        'succeeded': 2546,
                        'total': 3702,
                        'updated_at': (
                            '2026-07-13T18:29:15.317925+00:00'
                        ),
                    }
                },
                'symbol': None,
                'universe_refreshed': False,
                'universe_snapshot': None,
            },
            'child_reference_counts': _empty_child_reference_counts(
                direct_price_rows=2544
            ),
            'expected_row_sha256': (
                '044a758bd1d650696f86809b4127b78178298d9f856274dab7ef307a5dd03505'
            ),
        },
    ],
    'contextual_parent_gates': [
        {
            'id': 87,
            'market_date': '2026-07-09',
            'trigger': 'internal_ops_cli',
            'status': 'interrupted',
            'current_stage': None,
            'started_at': '2026-07-13T16:51:24.269454+00:00',
            'completed_at': '2026-07-13T23:51:53.505526+00:00',
            'error_message': (
                'Core process started while previous daily pipeline run was '
                'still marked running.'
            ),
            'run_metadata': _frozen_parent_metadata(all_stages=True),
            'created_at': '2026-07-13T16:51:24.272514+00:00',
            'updated_at': '2026-07-13T23:51:53.505526+00:00',
            'expected_row_sha256': (
                '3e9c7ad2796df7421933cd1e1597c9127484096d1db2003745e24e43ac08bdb9'
            ),
            'corroborates_eod_run_id': 360,
            'authoritative_link': False,
        },
        {
            'id': 88,
            'market_date': '2026-07-09',
            'trigger': 'internal_ops_cli',
            'status': 'interrupted',
            'current_stage': None,
            'started_at': '2026-07-13T17:01:46.621423+00:00',
            'completed_at': '2026-07-13T23:51:53.505526+00:00',
            'error_message': (
                'Core process started while previous daily pipeline run was '
                'still marked running.'
            ),
            'run_metadata': _frozen_parent_metadata(all_stages=False),
            'created_at': '2026-07-13T17:01:46.622469+00:00',
            'updated_at': '2026-07-13T23:51:53.505526+00:00',
            'expected_row_sha256': (
                'b8cd737a523f2164540d6ec93ba141b98523e4bce391627d97939a15e44499da'
            ),
            'corroborates_eod_run_id': 361,
            'authoritative_link': False,
        },
        {
            'id': 90,
            'market_date': '2026-07-10',
            'trigger': 'internal_ops_cli',
            'status': 'interrupted',
            'current_stage': None,
            'started_at': '2026-07-13T18:17:08.928838+00:00',
            'completed_at': '2026-07-13T23:51:53.505526+00:00',
            'error_message': (
                'Core process started while previous daily pipeline run was '
                'still marked running.'
            ),
            'run_metadata': _frozen_parent_metadata(all_stages=False),
            'created_at': '2026-07-13T18:17:08.933610+00:00',
            'updated_at': '2026-07-13T23:51:53.505526+00:00',
            'expected_row_sha256': (
                '61f7eb5752c3788c27d5ad05cb8fde041f71dae1e9c814682a1a8098b3ec1a7f'
            ),
            'corroborates_eod_run_id': 362,
            'authoritative_link': False,
        },
    ],
    'mutation_contract': {
        'preserve_counters': True,
        'preserve_child_rows': True,
        'interrupt_running_stage_progress': True,
        'stage_progress_current_step': 'stale_run_interrupted',
        'provider_calls': False,
        'runtime_bootstrap': False,
        'migrations': False,
    },
}
FROZEN_REPAIR_MANIFEST_SHA256 = (
    '61610cc75a8b84190328b8d55c739e61d904396e0a4cdaca5aa992f729cc84a9'
)
if _sha256(FROZEN_REPAIR_MANIFEST) != FROZEN_REPAIR_MANIFEST_SHA256:
    raise RuntimeError('frozen stale EOD collection-run manifest changed')


def _load_child_reference_counts(
    session: Any,
    *,
    run_ids: Sequence[int],
) -> dict[int, dict[str, int]]:
    normalized_ids = tuple(sorted({int(value) for value in run_ids}))
    counts = {
        run_id: {label: 0 for label, _column in CHILD_REFERENCE_FIELDS}
        for run_id in normalized_ids
    }
    for label, column in CHILD_REFERENCE_FIELDS:
        for run_id, count in session.execute(
            select(column, func.count())
            .where(column.in_(normalized_ids))
            .group_by(column)
        ).all():
            if run_id is not None and int(run_id) in counts:
                counts[int(run_id)][label] = int(count or 0)
    return counts


def _lock_child_reference_rows(
    session: Any,
    *,
    run_ids: Sequence[int],
) -> None:
    normalized_ids = tuple(sorted({int(value) for value in run_ids}))
    for _label, column in CHILD_REFERENCE_FIELDS:
        session.execute(
            select(column)
            .where(column.in_(normalized_ids))
            .order_by(column.asc())
            .with_for_update()
        ).all()


def _field_mismatches(
    row: EODCollectionRun,
    expected: Mapping[str, Any],
    *,
    child_reference_counts: Mapping[str, int],
) -> list[str]:
    mismatches = _state_mismatches(
        _normalize_target_row(row),
        _expected_state(expected, TARGET_ROW_STATE_FIELDS),
    )
    if dict(child_reference_counts) != dict(
        expected.get('child_reference_counts') or {}
    ):
        mismatches.append('child_reference_counts')
    return mismatches


def _parent_mismatches(
    row: DailyPipelineRun,
    expected: Mapping[str, Any],
) -> list[str]:
    return _state_mismatches(
        _normalize_parent_row(row),
        _expected_state(expected, PARENT_ROW_STATE_FIELDS),
    )


class StaleEODCollectionRunRepair:
    def __init__(self, manifest: Mapping[str, Any] | None = None) -> None:
        self.manifest = deepcopy(
            dict(manifest or FROZEN_REPAIR_MANIFEST)
        )
        self.manifest_sha256 = _sha256(self.manifest)

    def _repaired_metadata(
        self,
        expected: Mapping[str, Any],
        *,
        applied_at: datetime,
    ) -> dict[str, Any]:
        metadata = deepcopy(expected.get('run_metadata'))
        if not isinstance(metadata, dict):
            metadata = {}
        interrupted_stage_ids: list[str] = []
        stage_progress = metadata.get('stage_progress')
        if isinstance(stage_progress, dict):
            stage_progress = deepcopy(stage_progress)
            for stage_id, raw_progress in stage_progress.items():
                if not isinstance(raw_progress, dict):
                    continue
                progress = dict(raw_progress)
                if str(progress.get('status') or '').strip().lower() != 'running':
                    continue
                interrupted_stage_ids.append(str(stage_id))
                progress.update(
                    {
                        'status': TARGET_STATUS,
                        'current_step': 'stale_run_interrupted',
                        'current_activity': REPAIR_ERROR_MESSAGE,
                        'completed_at': applied_at.isoformat(),
                        'updated_at': applied_at.isoformat(),
                    }
                )
                stage_progress[stage_id] = progress
            metadata['stage_progress'] = stage_progress

        metadata.update(
            {
                REPAIR_MARKER_KEY: {
                    'schema_version': MARKER_SCHEMA_VERSION,
                    'incident_id': self.manifest['incident_id'],
                    'manifest_sha256': self.manifest_sha256,
                    'run_id': int(expected['id']),
                    'original_status': str(expected['status']),
                    'target_status': str(self.manifest['target_status']),
                    'original_started_at': str(expected['started_at']),
                    'original_last_progress_at': metadata.get(
                        'last_progress_at'
                    ),
                    'original_symbols_total': int(expected['symbols_total']),
                    'original_symbols_succeeded': int(
                        expected['symbols_succeeded']
                    ),
                    'original_symbols_failed': int(expected['symbols_failed']),
                    'original_child_reference_counts': dict(
                        expected['child_reference_counts']
                    ),
                    'original_row_sha256': str(
                        expected['expected_row_sha256']
                    ),
                    'interrupted_stage_ids': sorted(interrupted_stage_ids),
                    'applied_at': applied_at.isoformat(),
                },
                'interrupted_reason': (
                    'reviewed_stale_eod_collection_run_exact_scope_repair'
                ),
                'interrupted_at': applied_at.isoformat(),
                'resumable': True,
                'current_step': 'stale_run_interrupted',
                'current_activity': REPAIR_ERROR_MESSAGE,
                'last_progress_at': applied_at.isoformat(),
            }
        )
        return metadata

    def _marker_valid(
        self,
        row: EODCollectionRun,
        expected: Mapping[str, Any],
        *,
        child_reference_counts: Mapping[str, int],
    ) -> bool:
        metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
        marker = metadata.get(REPAIR_MARKER_KEY)
        if not isinstance(marker, dict):
            return False
        try:
            applied_at = datetime.fromisoformat(
                str(marker['applied_at']).replace('Z', '+00:00')
            )
        except (KeyError, TypeError, ValueError):
            return False
        if (
            applied_at.tzinfo is None
            or _utc_iso(applied_at) != str(marker['applied_at'])
        ):
            return False
        if dict(child_reference_counts) != dict(
            expected.get('child_reference_counts') or {}
        ):
            return False
        expected_state = _expected_state(expected, TARGET_ROW_STATE_FIELDS)
        expected_state.update(
            {
                'status': self.manifest['target_status'],
                'error_message': REPAIR_ERROR_MESSAGE,
                'completed_at': _utc_iso(applied_at),
                'run_metadata': self._repaired_metadata(
                    expected,
                    applied_at=applied_at,
                ),
            }
        )
        return _normalize_target_row(row) == expected_state

    def _apply_row(
        self,
        row: EODCollectionRun,
        expected: Mapping[str, Any],
        *,
        applied_at: datetime,
    ) -> None:
        row.status = TARGET_STATUS
        row.error_message = REPAIR_ERROR_MESSAGE
        row.completed_at = applied_at
        row.run_metadata = self._repaired_metadata(
            expected,
            applied_at=applied_at,
        )

    def repair(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
    ) -> dict[str, Any]:
        expected_hash = str(expected_manifest_sha256 or '').strip().lower()
        base_result = {
            'incident_id': self.manifest['incident_id'],
            'manifest_sha256': self.manifest_sha256,
            'dry_run': not apply,
            'applied': False,
            'target_status': self.manifest['target_status'],
            'expected_run_ids': [
                int(item['id']) for item in self.manifest['rows']
            ],
        }
        if apply and not expected_hash:
            return {
                **base_result,
                'status': 'blocked',
                'reason': 'expected_manifest_sha256_required',
            }
        if apply and expected_hash != self.manifest_sha256:
            return {
                **base_result,
                'status': 'blocked',
                'reason': 'expected_manifest_sha256_mismatch',
            }

        expected_rows = {
            int(item['id']): item for item in self.manifest['rows']
        }
        expected_parent_rows = {
            int(item['id']): item
            for item in self.manifest['contextual_parent_gates']
        }
        trade_dates = tuple(
            sorted({str(item['trade_date']) for item in expected_rows.values()})
        )
        markets = {str(item['market']) for item in expected_rows.values()}
        if markets != {'KRX'}:
            return {
                **base_result,
                'status': 'blocked',
                'reason': 'invalid_frozen_market_scope',
            }

        with advisory_eod_collection_locks(
            market='KRX',
            trade_dates=trade_dates,
            source='daily_price_bars',
        ) as (locks_acquired, blocked_trade_date):
            if not locks_acquired:
                return {
                    **base_result,
                    'status': 'blocked',
                    'reason': 'active_daily_price_bar_advisory_lock',
                    'blocked_trade_date': blocked_trade_date,
                }

            with session_scope(
                isolated_connection=True,
                statement_timeout_ms=15_000,
                lock_timeout_ms=2_000,
            ) as session:
                rows = list(
                    session.execute(
                        select(EODCollectionRun)
                        .where(EODCollectionRun.id.in_(tuple(expected_rows)))
                        .order_by(EODCollectionRun.id.asc())
                        .with_for_update()
                    ).scalars()
                )
                parent_rows = list(
                    session.execute(
                        select(DailyPipelineRun)
                        .where(
                            DailyPipelineRun.id.in_(
                                tuple(expected_parent_rows)
                            )
                        )
                        .order_by(DailyPipelineRun.id.asc())
                        .with_for_update()
                    ).scalars()
                )
                running_ids = [
                    int(value)
                    for value in session.execute(
                        select(EODCollectionRun.id)
                        .where(EODCollectionRun.status == 'running')
                        .order_by(EODCollectionRun.id.asc())
                        .with_for_update()
                    ).scalars()
                ]
                _lock_child_reference_rows(
                    session,
                    run_ids=tuple(expected_rows),
                )
                child_counts = _load_child_reference_counts(
                    session,
                    run_ids=tuple(expected_rows),
                )

                actual_row_sha256 = {
                    int(row.id): _sha256(_normalize_target_row(row))
                    for row in rows
                }
                actual_parent_row_sha256 = {
                    int(row.id): _sha256(_normalize_parent_row(row))
                    for row in parent_rows
                }
                expected_row_sha256 = {
                    row_id: str(expected['expected_row_sha256'])
                    for row_id, expected in expected_rows.items()
                }
                expected_parent_row_sha256 = {
                    row_id: str(expected['expected_row_sha256'])
                    for row_id, expected in expected_parent_rows.items()
                }
                expected_row_hashes_valid = all(
                    expected_row_sha256[row_id]
                    == _sha256(
                        _expected_state(expected, TARGET_ROW_STATE_FIELDS)
                    )
                    for row_id, expected in expected_rows.items()
                )
                expected_parent_row_hashes_valid = all(
                    expected_parent_row_sha256[row_id]
                    == _sha256(
                        _expected_state(expected, PARENT_ROW_STATE_FIELDS)
                    )
                    for row_id, expected in expected_parent_rows.items()
                )
                row_hashes_match = bool(
                    expected_row_hashes_valid
                    and actual_row_sha256 == expected_row_sha256
                )
                parent_row_hashes_match = bool(
                    expected_parent_row_hashes_valid
                    and actual_parent_row_sha256
                    == expected_parent_row_sha256
                )

                actual_ids = [int(row.id) for row in rows]
                actual_parent_ids = [int(row.id) for row in parent_rows]
                expected_ids = sorted(expected_rows)
                expected_parent_ids = sorted(expected_parent_rows)
                parent_mismatches = {
                    int(row.id): _parent_mismatches(
                        row,
                        expected_parent_rows[int(row.id)],
                    )
                    for row in parent_rows
                }
                parent_mismatches = {
                    row_id: fields
                    for row_id, fields in parent_mismatches.items()
                    if fields
                }
                parent_gate_ready = bool(
                    actual_parent_ids == expected_parent_ids
                    and not parent_mismatches
                    and parent_row_hashes_match
                )

                marker_states = []
                for row in rows:
                    metadata = (
                        row.run_metadata
                        if isinstance(row.run_metadata, dict)
                        else {}
                    )
                    marker_states.append(
                        isinstance(metadata.get(REPAIR_MARKER_KEY), dict)
                    )
                if any(marker_states) and not all(marker_states):
                    return {
                        **base_result,
                        'status': 'blocked',
                        'reason': 'partial_repair_marker_state',
                        'actual_run_ids': actual_ids,
                        'actual_parent_ids': actual_parent_ids,
                        'actual_row_sha256': actual_row_sha256,
                        'actual_parent_row_sha256': actual_parent_row_sha256,
                    }

                if rows and all(marker_states):
                    replay_valid = bool(
                        actual_ids == expected_ids
                        and expected_row_hashes_valid
                        and parent_gate_ready
                        and not running_ids
                        and all(
                            self._marker_valid(
                                row,
                                expected_rows[int(row.id)],
                                child_reference_counts=child_counts[int(row.id)],
                            )
                            for row in rows
                        )
                    )
                    if replay_valid:
                        return {
                            **base_result,
                            'status': 'already_applied',
                            'actual_run_ids': actual_ids,
                            'actual_parent_ids': actual_parent_ids,
                            'repaired_run_ids': actual_ids,
                            'expected_row_sha256': expected_row_sha256,
                            'actual_parent_row_sha256': (
                                actual_parent_row_sha256
                            ),
                        }
                    return {
                        **base_result,
                        'status': 'blocked',
                        'reason': 'invalid_repair_marker_state',
                        'actual_run_ids': actual_ids,
                        'actual_parent_ids': actual_parent_ids,
                    }

                row_mismatches = {
                    int(row.id): _field_mismatches(
                        row,
                        expected_rows[int(row.id)],
                        child_reference_counts=child_counts[int(row.id)],
                    )
                    for row in rows
                }
                row_mismatches = {
                    row_id: fields
                    for row_id, fields in row_mismatches.items()
                    if fields
                }
                expected_running_ids = sorted(
                    int(value)
                    for value in self.manifest['expected_global_running_ids']
                )
                invariants = {
                    'run_ids_match': actual_ids == expected_ids,
                    'row_fields_match': not row_mismatches,
                    'expected_row_hashes_valid': expected_row_hashes_valid,
                    'row_hashes_match': row_hashes_match,
                    'parent_ids_match': actual_parent_ids
                    == expected_parent_ids,
                    'parent_fields_match': not parent_mismatches,
                    'expected_parent_row_hashes_valid': (
                        expected_parent_row_hashes_valid
                    ),
                    'parent_row_hashes_match': parent_row_hashes_match,
                    'global_running_ids_match': running_ids
                    == expected_running_ids,
                    'child_reference_counts_match': all(
                        child_counts[run_id]
                        == expected_rows[run_id]['child_reference_counts']
                        for run_id in expected_ids
                    ),
                }
                if not all(invariants.values()):
                    return {
                        **base_result,
                        'status': 'blocked',
                        'reason': 'stale_eod_collection_run_manifest_mismatch',
                        'invariants': invariants,
                        'actual_run_ids': actual_ids,
                        'actual_parent_ids': actual_parent_ids,
                        'actual_global_running_ids': running_ids,
                        'row_mismatches': row_mismatches,
                        'parent_mismatches': parent_mismatches,
                        'expected_row_sha256': expected_row_sha256,
                        'actual_row_sha256': actual_row_sha256,
                        'expected_parent_row_sha256': (
                            expected_parent_row_sha256
                        ),
                        'actual_parent_row_sha256': actual_parent_row_sha256,
                        'child_reference_counts': child_counts,
                    }

                if not apply:
                    return {
                        **base_result,
                        'status': 'ready',
                        'invariants': invariants,
                        'actual_run_ids': actual_ids,
                        'actual_parent_ids': actual_parent_ids,
                        'actual_global_running_ids': running_ids,
                        'expected_row_sha256': expected_row_sha256,
                        'actual_row_sha256': actual_row_sha256,
                        'expected_parent_row_sha256': (
                            expected_parent_row_sha256
                        ),
                        'actual_parent_row_sha256': actual_parent_row_sha256,
                        'child_reference_counts': child_counts,
                        'manifest': deepcopy(self.manifest),
                    }

                applied_at = datetime.now(timezone.utc)
                counters_before = {
                    int(row.id): (
                        int(row.symbols_total),
                        int(row.symbols_succeeded),
                        int(row.symbols_failed),
                    )
                    for row in rows
                }
                for row in rows:
                    self._apply_row(
                        row,
                        expected_rows[int(row.id)],
                        applied_at=applied_at,
                    )
                session.flush()

                remaining_running_ids = [
                    int(value)
                    for value in session.execute(
                        select(EODCollectionRun.id)
                        .where(EODCollectionRun.status == 'running')
                        .order_by(EODCollectionRun.id.asc())
                    ).scalars()
                ]
                child_counts_after = _load_child_reference_counts(
                    session,
                    run_ids=tuple(expected_rows),
                )
                counters_after = {
                    int(row.id): (
                        int(row.symbols_total),
                        int(row.symbols_succeeded),
                        int(row.symbols_failed),
                    )
                    for row in rows
                }
                postconditions = {
                    'no_running_eod_collection_runs': not remaining_running_ids,
                    'target_status_applied': all(
                        row.status == TARGET_STATUS for row in rows
                    ),
                    'completed_at_present': all(
                        row.completed_at is not None for row in rows
                    ),
                    'counters_preserved': counters_after == counters_before,
                    'child_references_preserved': child_counts_after
                    == child_counts,
                    'repair_markers_valid': all(
                        self._marker_valid(
                            row,
                            expected_rows[int(row.id)],
                            child_reference_counts=child_counts_after[int(row.id)],
                        )
                        for row in rows
                    ),
                }
                if not all(postconditions.values()):
                    session.rollback()
                    return {
                        **base_result,
                        'status': 'blocked',
                        'reason': 'stale_eod_collection_run_postcondition_failed',
                        'postconditions': postconditions,
                        'remaining_running_ids': remaining_running_ids,
                    }
                return {
                    **base_result,
                    'status': 'completed',
                    'dry_run': False,
                    'applied': True,
                    'repaired_run_ids': actual_ids,
                    'postconditions': postconditions,
                    'completed_at': applied_at.isoformat(),
                }


stale_eod_collection_run_repair = StaleEODCollectionRunRepair()
