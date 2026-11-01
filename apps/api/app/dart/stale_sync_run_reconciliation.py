"""Exact reconciliation for six reviewed historical DART sync runs.

The operation is persistence-only and deliberately incident-scoped.  It does
not bootstrap the application, call DART, replay collection, run migrations,
or touch execution state.  Dry-run is the default.  Apply is admitted only
when the reviewed manifest hash, exact row state, stale cutoff, global running
set, and disclosure-event foreign-key counts still match.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Mapping, Sequence

from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from app.db import session_scope
from app.db_models import (
    DartDisclosureEvent,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
)


INCIDENT_ID = 'historical_stale_dart_sync_runs_3_327_1147_1148_1149_2038'
MANIFEST_SCHEMA_VERSION = (
    'dart_disclosure_stale_sync_reconciliation_manifest_v1'
)
MARKER_SCHEMA_VERSION = (
    'dart_disclosure_stale_sync_reconciliation_marker_v1'
)
RECONCILIATION_MARKER_KEY = 'stale_sync_run_reconciliation'
TARGET_STATUS = 'interrupted'
RECONCILIATION_REASON = (
    'reviewed_historical_dart_sync_run_exact_scope_reconciliation'
)
RECONCILIATION_ERROR_MESSAGE = (
    'Interrupted by exact reviewed historical DART sync-run reconciliation; '
    'no provider work was replayed.'
)

# Run 5594 was the new targeted repair run observed while these historical
# rows were reviewed.  It completed normally and is never part of this repair.
NEVER_TARGET_RUN_IDS = (5594,)


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


def _parse_datetime(value: datetime | str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    return _parse_datetime(value).isoformat()


def _normalize_persisted_row(row: DartDisclosureSyncRun) -> dict[str, Any]:
    return {
        'id': int(row.id),
        'trigger': row.trigger,
        'status': row.status,
        'window_start': row.window_start.isoformat(),
        'window_end': row.window_end.isoformat(),
        'page_size': int(row.page_size),
        'pages_fetched': int(row.pages_fetched),
        'events_seen': int(row.events_seen),
        'events_inserted': int(row.events_inserted),
        'events_updated': int(row.events_updated),
        'financial_report_events': int(row.financial_report_events),
        'error_message': row.error_message,
        'run_metadata': deepcopy(row.run_metadata),
        'started_at': _utc_iso(row.started_at),
        'completed_at': _utc_iso(row.completed_at),
    }


def _normalize_expected_persisted(
    persisted: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = deepcopy(dict(persisted))
    normalized['started_at'] = _utc_iso(normalized.get('started_at'))
    normalized['completed_at'] = _utc_iso(normalized.get('completed_at'))
    return normalized


def _state_mismatches(
    actual: Any,
    expected: Any,
    *,
    path: str = '',
) -> list[str]:
    if isinstance(actual, dict) and isinstance(expected, dict):
        mismatches: list[str] = []
        for key in sorted(set(actual) | set(expected)):
            child_path = f'{path}.{key}' if path else str(key)
            if key not in actual or key not in expected:
                mismatches.append(child_path)
                continue
            mismatches.extend(
                _state_mismatches(
                    actual[key],
                    expected[key],
                    path=child_path,
                )
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


FROZEN_RECONCILIATION_MANIFEST: dict[str, Any] = {
    'schema_version': MANIFEST_SCHEMA_VERSION,
    'target_run_ids': [3, 327, 1147, 1148, 1149, 2038],
    'expected_status': 'running',
    'expected_completed_at': None,
    'stale_started_at_min': '2026-04-07T19:40:07.870733+09:00',
    'stale_started_at_max_inclusive': (
        '2026-05-15T17:20:13.989031+09:00'
    ),
    'all_current_running_run_ids': [3, 327, 1147, 1148, 1149, 2038],
    'excluded_current_running_run_ids': [],
    'target_rows': [
        {
            'persisted': {
                'id': 3,
                'trigger': 'manual_api',
                'status': 'running',
                'window_start': '2026-03-09',
                'window_end': '2026-04-07',
                'page_size': 100,
                'pages_fetched': 0,
                'events_seen': 0,
                'events_inserted': 0,
                'events_updated': 0,
                'financial_report_events': 0,
                'error_message': None,
                'run_metadata': {'window_days': 30},
                'started_at': '2026-04-07T19:40:07.870733+09:00',
                'completed_at': None,
            },
            'event_references': {
                'first_seen_run_id_count': 2,
                'last_seen_run_id_count': 18,
                'both_first_and_last_count': 0,
            },
        },
        {
            'persisted': {
                'id': 327,
                'trigger': 'daily_incremental_refresh',
                'status': 'running',
                'window_start': '2015-01-01',
                'window_end': '2026-04-14',
                'page_size': 100,
                'pages_fetched': 0,
                'events_seen': 0,
                'events_inserted': 0,
                'events_updated': 0,
                'financial_report_events': 0,
                'error_message': None,
                'run_metadata': {
                    'requested_corporations': 424,
                    'scope': 'current_active_universe',
                },
                'started_at': '2026-04-14T11:21:56.375096+09:00',
                'completed_at': None,
            },
            'event_references': {
                'first_seen_run_id_count': 0,
                'last_seen_run_id_count': 0,
                'both_first_and_last_count': 0,
            },
        },
        {
            'persisted': {
                'id': 1147,
                'trigger': 'manual_active_universe_backfill',
                'status': 'running',
                'window_start': '2025-01-01',
                'window_end': '2026-04-22',
                'page_size': 100,
                'pages_fetched': 0,
                'events_seen': 0,
                'events_inserted': 0,
                'events_updated': 0,
                'financial_report_events': 0,
                'error_message': None,
                'run_metadata': {
                    'requested_corporations': 2617,
                    'scope': 'current_active_universe',
                },
                'started_at': '2026-04-22T09:12:41.861129+09:00',
                'completed_at': None,
            },
            'event_references': {
                'first_seen_run_id_count': 0,
                'last_seen_run_id_count': 0,
                'both_first_and_last_count': 0,
            },
        },
        {
            'persisted': {
                'id': 1148,
                'trigger': 'manual_active_universe_backfill',
                'status': 'running',
                'window_start': '2015-01-01',
                'window_end': '2026-04-21',
                'page_size': 100,
                'pages_fetched': 0,
                'events_seen': 0,
                'events_inserted': 0,
                'events_updated': 0,
                'financial_report_events': 0,
                'error_message': None,
                'run_metadata': {
                    'requested_corporations': 2617,
                    'scope': 'current_active_universe',
                },
                'started_at': '2026-04-22T12:52:43.163501+09:00',
                'completed_at': None,
            },
            'event_references': {
                'first_seen_run_id_count': 0,
                'last_seen_run_id_count': 64108,
                'both_first_and_last_count': 0,
            },
        },
        {
            'persisted': {
                'id': 1149,
                'trigger': 'manual_active_universe_backfill',
                'status': 'running',
                'window_start': '2015-01-01',
                'window_end': '2026-04-21',
                'page_size': 100,
                'pages_fetched': 0,
                'events_seen': 0,
                'events_inserted': 0,
                'events_updated': 0,
                'financial_report_events': 0,
                'error_message': None,
                'run_metadata': {
                    'requested_corporations': 1734,
                    'scope': 'current_active_universe',
                },
                'started_at': '2026-04-22T14:05:44.738872+09:00',
                'completed_at': None,
            },
            'event_references': {
                'first_seen_run_id_count': 0,
                'last_seen_run_id_count': 2528,
                'both_first_and_last_count': 0,
            },
        },
        {
            'persisted': {
                'id': 2038,
                'trigger': 'daily_incremental_refresh',
                'status': 'running',
                'window_start': '2026-03-19',
                'window_end': '2026-05-15',
                'page_size': 100,
                'pages_fetched': 0,
                'events_seen': 0,
                'events_inserted': 0,
                'events_updated': 0,
                'financial_report_events': 0,
                'error_message': None,
                'run_metadata': {
                    'requested_corporations': 486,
                    'scope': 'current_active_universe',
                },
                'started_at': '2026-05-15T17:20:13.989031+09:00',
                'completed_at': None,
            },
            'event_references': {
                'first_seen_run_id_count': 65,
                'last_seen_run_id_count': 0,
                'both_first_and_last_count': 0,
            },
        },
    ],
}
FROZEN_RECONCILIATION_MANIFEST_SHA256 = (
    '7ecc5cefaf5d776f98073f3343e6fe40a39c7534c6d467143da6e267c39bdf4d'
)
if (
    _sha256(FROZEN_RECONCILIATION_MANIFEST)
    != FROZEN_RECONCILIATION_MANIFEST_SHA256
):
    raise RuntimeError('frozen stale DART sync-run manifest changed')


def _load_event_reference_counts(
    session: Any,
    *,
    run_ids: Sequence[int],
) -> dict[int, dict[str, int]]:
    normalized_ids = tuple(sorted({int(run_id) for run_id in run_ids}))
    counts = {
        run_id: {
            'first_seen_run_id_count': 0,
            'last_seen_run_id_count': 0,
            'both_first_and_last_count': 0,
        }
        for run_id in normalized_ids
    }
    queries = (
        (
            'first_seen_run_id_count',
            DartDisclosureEvent.first_seen_run_id,
            DartDisclosureEvent.first_seen_run_id.in_(normalized_ids),
        ),
        (
            'last_seen_run_id_count',
            DartDisclosureEvent.last_seen_run_id,
            DartDisclosureEvent.last_seen_run_id.in_(normalized_ids),
        ),
        (
            'both_first_and_last_count',
            DartDisclosureEvent.first_seen_run_id,
            (
                DartDisclosureEvent.first_seen_run_id.in_(normalized_ids)
                & (
                    DartDisclosureEvent.first_seen_run_id
                    == DartDisclosureEvent.last_seen_run_id
                )
            ),
        ),
    )
    for label, group_column, predicate in queries:
        for run_id, count in session.execute(
            select(group_column, func.count())
            .where(predicate)
            .group_by(group_column)
        ).all():
            if run_id is not None and int(run_id) in counts:
                counts[int(run_id)][label] = int(count or 0)
    return counts


# Producers create/update their run row before mutating disclosure events.
# Keep every run table ahead of the event table to avoid taking the event
# fence while waiting on a producer that already owns a run-table lock.
_DART_MAINTENANCE_TABLES = (
    'dart_disclosure_sync_runs',
    'dart_fundamentals_capture_runs',
    'dart_fundamentals_refresh_runs',
    'dart_disclosure_events',
)


def _acquire_dart_maintenance_locks(session: Any) -> None:
    """Fence every DART writer while the exact apply proof is evaluated.

    PostgreSQL writers automatically take ``ROW EXCLUSIVE`` table locks for
    inserts, updates, and deletes.  ``SHARE ROW EXCLUSIVE`` therefore waits
    for already-active DART writers and prevents new ones until this
    transaction commits.  The session's bounded ``lock_timeout`` keeps the
    maintenance command fail-closed instead of waiting behind live work.

    SQLite is used only by isolated unit tests and has no equivalent table
    lock syntax.  Production is PostgreSQL; a real PostgreSQL two-session
    regression test verifies the lock compatibility contract.
    """
    if session.get_bind().dialect.name != 'postgresql':
        return
    session.execute(
        text(
            'LOCK TABLE '
            f'{", ".join(_DART_MAINTENANCE_TABLES)} '
            'IN SHARE ROW EXCLUSIVE MODE'
        )
    )


def _running_ids(session: Any, model: Any) -> list[int]:
    return [
        int(run_id)
        for run_id in session.execute(
            select(model.id)
            .where(model.status == 'running')
            .order_by(model.id.asc())
        ).scalars()
    ]


class StaleDartSyncRunReconciliation:
    def __init__(self, manifest: Mapping[str, Any] | None = None) -> None:
        self.manifest = deepcopy(
            dict(manifest or FROZEN_RECONCILIATION_MANIFEST)
        )
        self.manifest_sha256 = _sha256(self.manifest)

    @property
    def expected_rows(self) -> dict[int, dict[str, Any]]:
        return {
            int(item['persisted']['id']): item
            for item in self.manifest.get('target_rows') or []
        }

    def _manifest_errors(self) -> list[str]:
        errors: list[str] = []
        expected_rows = self.expected_rows
        if self.manifest.get('schema_version') != MANIFEST_SCHEMA_VERSION:
            errors.append('schema_version')
        target_ids = [
            int(run_id) for run_id in self.manifest.get('target_run_ids') or []
        ]
        row_ids = sorted(expected_rows)
        if target_ids != sorted(target_ids) or len(target_ids) != len(
            set(target_ids)
        ):
            errors.append('target_run_ids')
        if row_ids != target_ids:
            errors.append('target_rows')
        if set(target_ids) & set(NEVER_TARGET_RUN_IDS):
            errors.append('never_target_run_ids')
        if self.manifest.get('expected_status') != 'running':
            errors.append('expected_status')
        if self.manifest.get('expected_completed_at') is not None:
            errors.append('expected_completed_at')
        if [
            int(run_id)
            for run_id in self.manifest.get('all_current_running_run_ids') or []
        ] != target_ids:
            errors.append('all_current_running_run_ids')
        if self.manifest.get('excluded_current_running_run_ids') != []:
            errors.append('excluded_current_running_run_ids')

        started_at_values: list[datetime] = []
        for expected in expected_rows.values():
            persisted = expected.get('persisted') or {}
            if persisted.get('status') != 'running':
                errors.append(
                    f'target_rows.{persisted.get("id")}.status'
                )
            if persisted.get('completed_at') is not None:
                errors.append(
                    f'target_rows.{persisted.get("id")}.completed_at'
                )
            try:
                started_at_values.append(
                    _parse_datetime(str(persisted['started_at']))
                )
            except (KeyError, TypeError, ValueError):
                errors.append(
                    f'target_rows.{persisted.get("id")}.started_at'
                )
        try:
            expected_min = _parse_datetime(
                str(self.manifest['stale_started_at_min'])
            )
            expected_max = _parse_datetime(
                str(self.manifest['stale_started_at_max_inclusive'])
            )
        except (KeyError, TypeError, ValueError):
            errors.append('stale_started_at_cutoff')
        else:
            if expected_min > expected_max:
                errors.append('stale_started_at_cutoff_order')
            if started_at_values and (
                min(started_at_values) != expected_min
                or max(started_at_values) != expected_max
            ):
                errors.append('stale_started_at_cutoff_bounds')
        return sorted(set(errors))

    def _reconciled_metadata(
        self,
        expected: Mapping[str, Any],
        *,
        applied_at: datetime,
    ) -> dict[str, Any]:
        persisted = expected['persisted']
        metadata = deepcopy(persisted.get('run_metadata'))
        if not isinstance(metadata, dict):
            metadata = {}
        applied_iso = _utc_iso(applied_at)
        metadata.update(
            {
                RECONCILIATION_MARKER_KEY: {
                    'schema_version': MARKER_SCHEMA_VERSION,
                    'incident_id': INCIDENT_ID,
                    'manifest_sha256': self.manifest_sha256,
                    'run_id': int(persisted['id']),
                    'original_status': str(persisted['status']),
                    'target_status': TARGET_STATUS,
                    'original_completed_at': persisted['completed_at'],
                    'original_counters': {
                        key: int(persisted[key])
                        for key in (
                            'pages_fetched',
                            'events_seen',
                            'events_inserted',
                            'events_updated',
                            'financial_report_events',
                        )
                    },
                    'original_event_references': deepcopy(
                        expected['event_references']
                    ),
                    'original_row_sha256': _sha256(
                        _normalize_expected_persisted(persisted)
                    ),
                    'stale_started_at_max_inclusive': self.manifest[
                        'stale_started_at_max_inclusive'
                    ],
                    'reason': RECONCILIATION_REASON,
                    'applied_at': applied_iso,
                },
                'reconciliation_reason': RECONCILIATION_REASON,
                'reconciled_at': applied_iso,
                'interrupted_reason': RECONCILIATION_REASON,
                'interrupted_at': applied_iso,
            }
        )
        return metadata

    def _marker_valid(
        self,
        row: DartDisclosureSyncRun,
        expected: Mapping[str, Any],
        *,
        event_reference_counts: Mapping[str, int],
    ) -> bool:
        metadata = row.run_metadata if isinstance(row.run_metadata, dict) else {}
        marker = metadata.get(RECONCILIATION_MARKER_KEY)
        if not isinstance(marker, dict):
            return False
        try:
            applied_at = _parse_datetime(str(marker['applied_at']))
        except (KeyError, TypeError, ValueError):
            return False
        persisted = expected['persisted']
        expected_state = _normalize_expected_persisted(persisted)
        expected_state.update(
            {
                'status': TARGET_STATUS,
                'error_message': RECONCILIATION_ERROR_MESSAGE,
                'run_metadata': self._reconciled_metadata(
                    expected,
                    applied_at=applied_at,
                ),
                'completed_at': _utc_iso(applied_at),
            }
        )
        return bool(
            dict(event_reference_counts)
            == dict(expected['event_references'])
            and _normalize_persisted_row(row) == expected_state
        )

    def _cas_update_row(
        self,
        session: Any,
        expected: Mapping[str, Any],
        *,
        applied_at: datetime,
    ) -> int:
        persisted = expected['persisted']
        predicate = (
            (DartDisclosureSyncRun.id == int(persisted['id']))
            & (DartDisclosureSyncRun.trigger == persisted['trigger'])
            & (DartDisclosureSyncRun.status == persisted['status'])
            & (
                DartDisclosureSyncRun.window_start
                == date.fromisoformat(persisted['window_start'])
            )
            & (
                DartDisclosureSyncRun.window_end
                == date.fromisoformat(persisted['window_end'])
            )
            & (DartDisclosureSyncRun.page_size == int(persisted['page_size']))
            & (
                DartDisclosureSyncRun.pages_fetched
                == int(persisted['pages_fetched'])
            )
            & (DartDisclosureSyncRun.events_seen == int(persisted['events_seen']))
            & (
                DartDisclosureSyncRun.events_inserted
                == int(persisted['events_inserted'])
            )
            & (
                DartDisclosureSyncRun.events_updated
                == int(persisted['events_updated'])
            )
            & (
                DartDisclosureSyncRun.financial_report_events
                == int(persisted['financial_report_events'])
            )
            & (DartDisclosureSyncRun.started_at == _parse_datetime(persisted['started_at']))
            & DartDisclosureSyncRun.completed_at.is_(None)
        )
        if persisted.get('error_message') is None:
            predicate &= DartDisclosureSyncRun.error_message.is_(None)
        else:
            predicate &= (
                DartDisclosureSyncRun.error_message
                == str(persisted['error_message'])
            )
        result = session.execute(
            update(DartDisclosureSyncRun)
            .where(predicate)
            .values(
                status=TARGET_STATUS,
                error_message=RECONCILIATION_ERROR_MESSAGE,
                run_metadata=self._reconciled_metadata(
                    expected,
                    applied_at=applied_at,
                ),
                completed_at=applied_at,
            )
            .execution_options(synchronize_session=False)
        )
        return int(result.rowcount or 0)

    def reconcile(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
    ) -> dict[str, Any]:
        expected_hash = str(expected_manifest_sha256 or '').strip().lower()
        expected_rows = self.expected_rows
        expected_ids = sorted(expected_rows)
        base_result = {
            'incident_id': INCIDENT_ID,
            'manifest_sha256': self.manifest_sha256,
            'dry_run': not apply,
            'applied': False,
            'target_status': TARGET_STATUS,
            'target_run_ids': expected_ids,
            'never_target_run_ids': list(NEVER_TARGET_RUN_IDS),
            'stale_started_at_max_inclusive': self.manifest.get(
                'stale_started_at_max_inclusive'
            ),
        }
        manifest_errors = self._manifest_errors()
        if manifest_errors:
            return {
                **base_result,
                'status': 'blocked',
                'reason': 'invalid_frozen_manifest',
                'manifest_errors': manifest_errors,
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

        cutoff = _parse_datetime(
            self.manifest['stale_started_at_max_inclusive']
        )
        with session_scope(
            isolated_connection=True,
            statement_timeout_ms=15_000,
            lock_timeout_ms=2_000,
        ) as session:
            if apply:
                try:
                    _acquire_dart_maintenance_locks(session)
                except DBAPIError as exc:
                    # PostgreSQL aborts the transaction after a lock timeout.
                    # Roll it back before returning a controlled fail-closed
                    # result to the surrounding session context manager.
                    session.rollback()
                    return {
                        **base_result,
                        'status': 'blocked',
                        'reason': 'dart_maintenance_lock_unavailable',
                        'error_type': type(exc.orig).__name__,
                    }
            target_rows_statement = (
                select(DartDisclosureSyncRun)
                .where(DartDisclosureSyncRun.id.in_(tuple(expected_ids)))
                .order_by(DartDisclosureSyncRun.id.asc())
            )
            if apply:
                target_rows_statement = (
                    target_rows_statement.with_for_update()
                )
            rows = list(
                session.execute(target_rows_statement).scalars()
            )
            running_ids = _running_ids(session, DartDisclosureSyncRun)
            active_capture_run_ids = _running_ids(
                session,
                DartFundamentalsCaptureRun,
            )
            active_refresh_run_ids = _running_ids(
                session,
                DartFundamentalsRefreshRun,
            )
            stale_rows_statement = (
                select(DartDisclosureSyncRun.id)
                .where(
                    DartDisclosureSyncRun.status == 'running',
                    DartDisclosureSyncRun.started_at <= cutoff,
                )
                .order_by(DartDisclosureSyncRun.id.asc())
            )
            if apply:
                stale_rows_statement = stale_rows_statement.with_for_update()
            stale_running_ids = [
                int(run_id)
                for run_id in session.execute(stale_rows_statement).scalars()
            ]
            event_reference_counts = _load_event_reference_counts(
                session,
                run_ids=expected_ids,
            )
            actual_ids = [int(row.id) for row in rows]
            actual_row_sha256 = {
                int(row.id): _sha256(_normalize_persisted_row(row))
                for row in rows
            }
            expected_row_sha256 = {
                run_id: _sha256(
                    _normalize_expected_persisted(expected['persisted'])
                )
                for run_id, expected in expected_rows.items()
            }

            marker_states = [
                isinstance(
                    (row.run_metadata or {}).get(RECONCILIATION_MARKER_KEY),
                    dict,
                )
                for row in rows
            ]
            if any(marker_states) and not all(marker_states):
                return {
                    **base_result,
                    'status': 'blocked',
                    'reason': 'partial_reconciliation_marker_state',
                    'actual_run_ids': actual_ids,
                }
            if rows and all(marker_states):
                replay_valid = bool(
                    actual_ids == expected_ids
                    and not stale_running_ids
                    and not running_ids
                    and not active_capture_run_ids
                    and not active_refresh_run_ids
                    and all(
                        self._marker_valid(
                            row,
                            expected_rows[int(row.id)],
                            event_reference_counts=event_reference_counts[
                                int(row.id)
                            ],
                        )
                        for row in rows
                    )
                )
                if replay_valid:
                    return {
                        **base_result,
                        'status': 'already_applied',
                        'reconciled_run_ids': actual_ids,
                        'actual_running_run_ids': running_ids,
                        'active_fundamentals_capture_run_ids': (
                            active_capture_run_ids
                        ),
                        'active_fundamentals_refresh_run_ids': (
                            active_refresh_run_ids
                        ),
                    }
                return {
                    **base_result,
                    'status': 'blocked',
                    'reason': 'invalid_reconciliation_marker_state',
                    'actual_run_ids': actual_ids,
                    'actual_running_run_ids': running_ids,
                    'actual_stale_running_run_ids': stale_running_ids,
                    'active_fundamentals_capture_run_ids': (
                        active_capture_run_ids
                    ),
                    'active_fundamentals_refresh_run_ids': (
                        active_refresh_run_ids
                    ),
                }

            row_mismatches: dict[int, list[str]] = {}
            for row in rows:
                run_id = int(row.id)
                expected = expected_rows[run_id]
                mismatches = _state_mismatches(
                    _normalize_persisted_row(row),
                    _normalize_expected_persisted(expected['persisted']),
                )
                if event_reference_counts[run_id] != expected[
                    'event_references'
                ]:
                    mismatches.append('event_references')
                if mismatches:
                    row_mismatches[run_id] = mismatches

            target_started_at = [
                _parse_datetime(row.started_at) for row in rows
            ]
            cutoff_bounds_match = bool(
                target_started_at
                and min(target_started_at)
                == _parse_datetime(self.manifest['stale_started_at_min'])
                and max(target_started_at) == cutoff
                and all(value <= cutoff for value in target_started_at)
            )
            expected_running_ids = [
                int(run_id)
                for run_id in self.manifest['all_current_running_run_ids']
            ]
            expected_excluded_running_ids = [
                int(run_id)
                for run_id in self.manifest[
                    'excluded_current_running_run_ids'
                ]
            ]
            actual_excluded_running_ids = sorted(
                set(running_ids) - set(expected_ids)
            )
            invariants = {
                'manifest_hash_reviewed': self.manifest_sha256
                == FROZEN_RECONCILIATION_MANIFEST_SHA256
                if self.manifest.get('schema_version')
                == MANIFEST_SCHEMA_VERSION
                and expected_ids
                == FROZEN_RECONCILIATION_MANIFEST['target_run_ids']
                else True,
                'run_ids_match': actual_ids == expected_ids,
                'row_fields_match': not row_mismatches,
                'row_hashes_match': actual_row_sha256
                == expected_row_sha256,
                'global_running_ids_match': running_ids
                == expected_running_ids,
                'excluded_running_ids_match': actual_excluded_running_ids
                == expected_excluded_running_ids,
                'stale_running_ids_match': stale_running_ids
                == expected_ids,
                'stale_cutoff_bounds_match': cutoff_bounds_match,
                'no_active_fundamentals_capture_runs': (
                    not active_capture_run_ids
                ),
                'no_active_fundamentals_refresh_runs': (
                    not active_refresh_run_ids
                ),
                'event_reference_counts_match': all(
                    event_reference_counts[run_id]
                    == expected_rows[run_id]['event_references']
                    for run_id in expected_ids
                ),
                'never_target_ids_excluded': not (
                    set(expected_ids) & set(NEVER_TARGET_RUN_IDS)
                ),
            }
            if not all(invariants.values()):
                return {
                    **base_result,
                    'status': 'blocked',
                    'reason': 'stale_dart_sync_run_manifest_mismatch',
                    'invariants': invariants,
                    'actual_run_ids': actual_ids,
                    'actual_running_run_ids': running_ids,
                    'actual_stale_running_run_ids': stale_running_ids,
                    'actual_excluded_running_run_ids': (
                        actual_excluded_running_ids
                    ),
                    'active_fundamentals_capture_run_ids': (
                        active_capture_run_ids
                    ),
                    'active_fundamentals_refresh_run_ids': (
                        active_refresh_run_ids
                    ),
                    'row_mismatches': row_mismatches,
                    'expected_row_sha256': expected_row_sha256,
                    'actual_row_sha256': actual_row_sha256,
                    'event_reference_counts': event_reference_counts,
                }
            if not apply:
                return {
                    **base_result,
                    'status': 'ready',
                    'invariants': invariants,
                    'actual_run_ids': actual_ids,
                    'actual_running_run_ids': running_ids,
                    'actual_stale_running_run_ids': stale_running_ids,
                    'active_fundamentals_capture_run_ids': (
                        active_capture_run_ids
                    ),
                    'active_fundamentals_refresh_run_ids': (
                        active_refresh_run_ids
                    ),
                    'expected_row_sha256': expected_row_sha256,
                    'actual_row_sha256': actual_row_sha256,
                    'event_reference_counts': event_reference_counts,
                    'manifest': deepcopy(self.manifest),
                }

            applied_at = datetime.now(timezone.utc)
            counters_before = {
                int(row.id): (
                    int(row.pages_fetched),
                    int(row.events_seen),
                    int(row.events_inserted),
                    int(row.events_updated),
                    int(row.financial_report_events),
                )
                for row in rows
            }
            cas_rowcounts = {
                run_id: self._cas_update_row(
                    session,
                    expected_rows[run_id],
                    applied_at=applied_at,
                )
                for run_id in expected_ids
            }
            if any(rowcount != 1 for rowcount in cas_rowcounts.values()):
                session.rollback()
                return {
                    **base_result,
                    'status': 'blocked',
                    'reason': 'cas_update_conflict',
                    'cas_rowcounts': cas_rowcounts,
                }

            session.flush()
            session.expire_all()
            reconciled_rows = list(
                session.execute(
                    select(DartDisclosureSyncRun)
                    .where(DartDisclosureSyncRun.id.in_(tuple(expected_ids)))
                    .order_by(DartDisclosureSyncRun.id.asc())
                    .with_for_update()
                ).scalars()
            )
            event_reference_counts_after = _load_event_reference_counts(
                session,
                run_ids=expected_ids,
            )
            running_ids_after = _running_ids(
                session,
                DartDisclosureSyncRun,
            )
            active_capture_run_ids_after = _running_ids(
                session,
                DartFundamentalsCaptureRun,
            )
            active_refresh_run_ids_after = _running_ids(
                session,
                DartFundamentalsRefreshRun,
            )
            counters_after = {
                int(row.id): (
                    int(row.pages_fetched),
                    int(row.events_seen),
                    int(row.events_inserted),
                    int(row.events_updated),
                    int(row.financial_report_events),
                )
                for row in reconciled_rows
            }
            remaining_target_running_ids = [
                int(row.id)
                for row in reconciled_rows
                if row.status == 'running'
            ]
            postconditions = {
                'target_ids_not_running': not remaining_target_running_ids,
                'target_status_applied': all(
                    row.status == TARGET_STATUS for row in reconciled_rows
                ),
                'completed_at_present': all(
                    row.completed_at is not None for row in reconciled_rows
                ),
                'counters_preserved': counters_after == counters_before,
                'event_references_preserved': (
                    event_reference_counts_after == event_reference_counts
                ),
                'global_running_set_revalidated': not running_ids_after,
                'no_active_fundamentals_capture_runs': (
                    not active_capture_run_ids_after
                ),
                'no_active_fundamentals_refresh_runs': (
                    not active_refresh_run_ids_after
                ),
                'reconciliation_markers_valid': all(
                    self._marker_valid(
                        row,
                        expected_rows[int(row.id)],
                        event_reference_counts=event_reference_counts_after[
                            int(row.id)
                        ],
                    )
                    for row in reconciled_rows
                ),
            }
            if not all(postconditions.values()):
                session.rollback()
                return {
                    **base_result,
                    'status': 'blocked',
                    'reason': 'stale_dart_sync_run_postcondition_failed',
                    'postconditions': postconditions,
                    'remaining_target_running_ids': (
                        remaining_target_running_ids
                    ),
                    'actual_running_run_ids': running_ids_after,
                    'active_fundamentals_capture_run_ids': (
                        active_capture_run_ids_after
                    ),
                    'active_fundamentals_refresh_run_ids': (
                        active_refresh_run_ids_after
                    ),
                }
            return {
                **base_result,
                'status': 'completed',
                'dry_run': False,
                'applied': True,
                'reconciled_run_ids': expected_ids,
                'cas_rowcounts': cas_rowcounts,
                'postconditions': postconditions,
                'completed_at': _utc_iso(applied_at),
            }


stale_dart_sync_run_reconciliation = StaleDartSyncRunReconciliation()
