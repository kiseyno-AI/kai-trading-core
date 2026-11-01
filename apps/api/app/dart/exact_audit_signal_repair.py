from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from copy import deepcopy
from datetime import date
import json
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from app.core.settings import settings
from app.dart.signals_service import (
    AUDIT_SIGNAL_WRITER_LOCK_KEY,
    DartSignalsRepository,
    audit_signal_writer_lock_owner,
    classify_durable_audit_signal,
    dart_signals_service,
)
from app.db import session_scope
from app.db_models import (
    DailyPipelineRun,
    DartAuditSignal,
    DartDisclosureEvent,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
    EODCollectionRun,
)
from app.eod.derived_stage_repair import (
    GuardTransactionExitError,
    capture_guard_transaction_failure,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.internal.exact_repair_manifest import (
    canonical_json_sha256,
    checkpoint_file_lock,
    load_checkpoint,
    load_json_mapping,
    write_checkpoint,
)
from app.repositories.eod_run_repository import advisory_eod_collection_lock


MANIFEST_SCHEMA_VERSION = 'dart_audit_signal_exact_manifest_v1'
CHECKPOINT_SCHEMA_VERSION = 'dart_audit_signal_exact_checkpoint_v2'
REPLACEMENT_SOURCE_ERROR_CODE = '014'
MAX_BATCH_SIZE = 100
MAX_CONCURRENCY = 8
AUDIT_BATCH_IDLE_TRANSACTION_TIMEOUT_MIN_MS = 120_000
AUDIT_BATCH_IDLE_TRANSACTION_TIMEOUT_MAX_MS = 86_400_000
AUDIT_BATCH_IDLE_TRANSACTION_TIMEOUT_HEADROOM_MS = 60_000
AUDIT_REPAIR_DB_STATEMENT_TIMEOUT_MS = 5_000
AUDIT_BATCH_DB_OPERATIONS_PER_ENTRY = 3
_WRITER_RUN_TABLES = (
    'daily_pipeline_runs',
    'eod_collection_runs',
    'dart_disclosure_sync_runs',
    'dart_fundamentals_capture_runs',
    'dart_fundamentals_refresh_runs',
)
_RUN_MODELS = (
    DailyPipelineRun,
    EODCollectionRun,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
)
_TERMINAL_AUDIT_STATUSES = frozenset(
    {'captured', 'authoritative_absence'}
)
_AUDIT_CONTENT_FIELDS = (
    'opinion_code',
    'opinion_label',
    'has_audit_issue',
    'source_section',
    'evidence_text',
    'payload',
)


def _validate_stock_code(value: Any) -> str:
    stock_code = str(value or '').strip()
    if len(stock_code) != 6 or not stock_code.isdigit():
        raise ValueError('stock_code must contain exactly six digits')
    return stock_code


def _validate_rcept_no(value: Any) -> str:
    rcept_no = str(value or '').strip()
    if not rcept_no or len(rcept_no) > 32:
        raise ValueError('rcept_no must be a non-empty value up to 32 characters')
    return rcept_no


def _validate_event_id(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError('event_id must be a positive integer')
    return value


def _normalize_event_identity(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        'stock_code': _validate_stock_code(raw.get('stock_code')),
        'event_id': _validate_event_id(raw.get('event_id')),
        'rcept_no': _validate_rcept_no(raw.get('rcept_no')),
    }


def _normalize_audit_content(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        'opinion_code': raw.get('opinion_code'),
        'opinion_label': raw.get('opinion_label'),
        'has_audit_issue': raw.get('has_audit_issue'),
        'source_section': raw.get('source_section'),
        'evidence_text': raw.get('evidence_text'),
        'payload': raw.get('payload'),
    }


def _classify_durable_audit_content(
    audit_content: Mapping[str, Any],
    *,
    event_id: int,
    rcept_no: str,
    receipt_date: date | str,
) -> str | None:
    classification = classify_durable_audit_signal(
        audit_content,
        event_id=event_id,
        rcept_no=rcept_no,
        receipt_date=receipt_date,
    )
    if classification == 'authoritative_absence':
        return 'authoritative_absence'
    if classification == 'parsed_opinion':
        return 'captured'
    return None


def normalize_manifest(raw: Mapping[str, Any]) -> dict[str, Any]:
    if raw.get('schema_version') != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f'manifest schema_version must be {MANIFEST_SCHEMA_VERSION}')
    market_date = date.fromisoformat(str(raw.get('market_date') or ''))
    batch_size = raw.get('batch_size')
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= MAX_BATCH_SIZE
    ):
        raise ValueError(f'batch_size must be between 1 and {MAX_BATCH_SIZE}')
    raw_entries = raw.get('entries')
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValueError('manifest entries must be a non-empty list')
    entry_count = raw.get('entry_count')
    if (
        isinstance(entry_count, bool)
        or not isinstance(entry_count, int)
        or entry_count != len(raw_entries)
    ):
        raise ValueError('manifest entry_count does not match entries')

    entries: list[dict[str, Any]] = []
    event_ids: set[int] = set()
    rcept_nos: set[str] = set()
    replacement_target_event_ids: set[int] = set()
    replacement_target_rcept_nos: set[str] = set()
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, Mapping):
            raise ValueError('every manifest entry must be an object')
        entry = _normalize_event_identity(raw_entry)
        if entry['event_id'] in event_ids:
            raise ValueError('manifest contains duplicate event_id values')
        if entry['rcept_no'] in rcept_nos:
            raise ValueError('manifest contains duplicate rcept_no values')
        event_ids.add(entry['event_id'])
        rcept_nos.add(entry['rcept_no'])

        replacement_raw = raw_entry.get('replacement_for')
        if replacement_raw is not None:
            if not isinstance(replacement_raw, Mapping):
                raise ValueError('replacement_for must be an object')
            replacement = _normalize_event_identity(replacement_raw)
            if replacement['stock_code'] != entry['stock_code']:
                raise ValueError(
                    'replacement and original event must have the same stock_code'
                )
            if (
                replacement['event_id'] == entry['event_id']
                or replacement['rcept_no'] == entry['rcept_no']
            ):
                raise ValueError('replacement and original event must be distinct')
            if replacement_raw.get('source_error_code') != (
                REPLACEMENT_SOURCE_ERROR_CODE
            ):
                raise ValueError(
                    'replacement_for source_error_code must be the reviewed '
                    f'DART code {REPLACEMENT_SOURCE_ERROR_CODE}'
                )
            reason = str(replacement_raw.get('reason') or '').strip()
            if not reason:
                raise ValueError('replacement_for reason is required')
            if len(reason) > 200:
                raise ValueError(
                    'replacement_for reason must be at most 200 characters'
                )
            entry['replacement_for'] = {
                **replacement,
                'source_error_code': REPLACEMENT_SOURCE_ERROR_CODE,
                'reason': reason,
            }
            if replacement['event_id'] in replacement_target_event_ids:
                raise ValueError(
                    'manifest contains duplicate replacement target event_id '
                    'values'
                )
            if replacement['rcept_no'] in replacement_target_rcept_nos:
                raise ValueError(
                    'manifest contains duplicate replacement target rcept_no '
                    'values'
                )
            replacement_target_event_ids.add(replacement['event_id'])
            replacement_target_rcept_nos.add(replacement['rcept_no'])
        entries.append(entry)

    if replacement_target_event_ids.intersection(event_ids):
        raise ValueError(
            'replacement target event cannot also be a manifest source event'
        )
    if replacement_target_rcept_nos.intersection(rcept_nos):
        raise ValueError(
            'replacement target rcept_no cannot also be a manifest source '
            'rcept_no'
        )

    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'market_date': market_date.isoformat(),
        'entry_count': len(entries),
        'batch_size': batch_size,
        'entries': entries,
    }


def _load_event_identity(event_id: int) -> dict[str, Any] | None:
    with session_scope(
        statement_timeout_ms=AUDIT_REPAIR_DB_STATEMENT_TIMEOUT_MS
    ) as session:
        row = session.get(DartDisclosureEvent, event_id)
        if row is None:
            return None
        return {
            'event_id': int(row.id),
            'stock_code': str(row.stock_code or ''),
            'rcept_no': str(row.rcept_no),
            'report_nm': str(row.report_nm or ''),
            'receipt_date': (
                row.receipt_date.isoformat() if row.receipt_date else None
            ),
        }


def _load_audit_terminal_state(
    entry: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return only terminal state that is durably represented in storage.

    A missing ``DartAuditSignal`` row remains ambiguous.  A validated,
    versioned authoritative-absence payload is the only durable negative
    proof; its required boolean placeholder must never be interpreted as a
    clean parsed opinion.
    """

    with session_scope(
        statement_timeout_ms=AUDIT_REPAIR_DB_STATEMENT_TIMEOUT_MS
    ) as session:
        row = session.execute(
            select(
                DartAuditSignal.disclosure_event_id.label('event_id'),
                DartAuditSignal.stock_code,
                DartAuditSignal.rcept_no,
                DartAuditSignal.receipt_date,
                DartAuditSignal.opinion_code,
                DartAuditSignal.opinion_label,
                DartAuditSignal.has_audit_issue,
                DartAuditSignal.source_section,
                DartAuditSignal.evidence_text,
                DartAuditSignal.payload,
            ).where(DartAuditSignal.rcept_no == str(entry['rcept_no']))
        ).mappings().one_or_none()
        if row is None:
            return None
        stored_event_id = row['event_id']
        if (
            isinstance(stored_event_id, bool)
            or not isinstance(stored_event_id, int)
            or stored_event_id <= 0
        ):
            return None
        audit_content = _normalize_audit_content(row)
        status = _classify_durable_audit_content(
            audit_content,
            event_id=stored_event_id,
            rcept_no=str(row['rcept_no']),
            receipt_date=row['receipt_date'],
        )
        if status is None:
            return None
        return {
            'status': status,
            'event_id': row['event_id'],
            'stock_code': str(row['stock_code'] or ''),
            'rcept_no': str(row['rcept_no']),
            'audit_signal_evaluated': status == 'authoritative_absence',
            'receipt_date': (
                row['receipt_date'].isoformat()
                if row['receipt_date'] is not None
                else None
            ),
            'audit_content': audit_content,
            'audit_content_sha256': canonical_json_sha256(audit_content),
        }


def _load_current_selected_annual_candidate_scope(
    market_date: date,
    entries: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    event_ids = [int(entry['event_id']) for entry in entries]
    rows = DartSignalsRepository().list_selected_annual_audit_signal_candidates(
        market_date=market_date,
        event_ids=event_ids,
    )
    return [_normalize_event_identity(row) for row in rows]


def _load_current_missing_annual_candidate_scope(
    market_date: date,
    entries: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    event_ids = [int(entry['event_id']) for entry in entries]
    rows = (
        DartSignalsRepository()
        .list_missing_selected_annual_audit_signal_candidates(
            market_date=market_date,
            event_ids=event_ids,
        )
    )
    return [_normalize_event_identity(row) for row in rows]


def _active_writer_counts(session: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for model in _RUN_MODELS:
        count = session.execute(
            select(func.count()).select_from(model).where(model.status == 'running')
        ).scalar_one()
        counts[model.__tablename__] = int(count or 0)
    return counts


def _audit_event_timeout_seconds() -> float:
    return float(
        max(
            1,
            int(settings.dart_fundamentals_event_timeout_seconds),
        )
    )


def _audit_batch_idle_transaction_timeout_ms(
    *,
    batch_size: int,
    concurrency: int,
) -> int:
    """Budget the writer fence for every bounded external fetch wave."""

    normalized_batch_size = max(1, min(int(batch_size), MAX_BATCH_SIZE))
    normalized_concurrency = max(1, min(int(concurrency), MAX_CONCURRENCY))
    fetch_waves = (
        normalized_batch_size + normalized_concurrency - 1
    ) // normalized_concurrency
    configured_event_timeout_ms = int(
        _audit_event_timeout_seconds() * 1_000
    )
    bounded_db_operation_ms = (
        AUDIT_REPAIR_DB_STATEMENT_TIMEOUT_MS
        + int(settings.db_pool_timeout_seconds) * 1_000
        + int(settings.db_connect_timeout_seconds) * 1_000
    )
    validation_budget_ms = (
        normalized_batch_size
        * AUDIT_BATCH_DB_OPERATIONS_PER_ENTRY
        * bounded_db_operation_ms
    )
    calculated_timeout_ms = (
        fetch_waves * configured_event_timeout_ms
        + validation_budget_ms
        + AUDIT_BATCH_IDLE_TRANSACTION_TIMEOUT_HEADROOM_MS
    )
    if calculated_timeout_ms > AUDIT_BATCH_IDLE_TRANSACTION_TIMEOUT_MAX_MS:
        raise ValueError(
            'exact DART audit batch timeout budget exceeds the PostgreSQL '
            'idle transaction safety limit'
        )
    return max(
        AUDIT_BATCH_IDLE_TRANSACTION_TIMEOUT_MIN_MS,
        calculated_timeout_ms,
    )


@contextmanager
def exact_dart_audit_batch_guard(
    *,
    market_date: date,
    batch_size: int = MAX_BATCH_SIZE,
    concurrency: int = 1,
) -> Any:
    """Fence daily/EOD and DART writers for one bounded repair batch."""

    with advisory_eod_collection_lock(
        market='KRX',
        trade_date=market_date.isoformat(),
        source='daily_price_bars',
    ) as daily_lock_acquired:
        if not daily_lock_acquired:
            yield False, 'daily_eod_advisory_lock_unavailable'
            return
        with advisory_eod_collection_lock(
            market='KRX',
            trade_date=market_date.isoformat(),
            source='dart_share_issuance',
        ) as dart_lock_acquired:
            if not dart_lock_acquired:
                yield False, 'dart_advisory_lock_unavailable'
                return
            guard_yielded = False
            try:
                with session_scope(
                    statement_timeout_ms=AUDIT_REPAIR_DB_STATEMENT_TIMEOUT_MS,
                    lock_timeout_ms=1,
                    isolated_connection=True,
                ) as session:
                    if session.get_bind().dialect.name == 'postgresql':
                        try:
                            idle_timeout_ms = (
                                _audit_batch_idle_transaction_timeout_ms(
                                    batch_size=batch_size,
                                    concurrency=concurrency,
                                )
                            )
                        except ValueError:
                            guard_yielded = True
                            yield (
                                False,
                                'repair_timeout_budget_exceeds_postgres_limit',
                            )
                            return
                        session.execute(
                            text(
                                'SET LOCAL idle_in_transaction_session_timeout '
                                f'= {idle_timeout_ms}'
                            )
                        )
                        session.execute(
                            text(
                                'LOCK TABLE '
                                f'{", ".join(_WRITER_RUN_TABLES)} '
                                'IN SHARE ROW EXCLUSIVE MODE'
                            )
                        )
                        session.execute(
                            text(
                                'SELECT pg_advisory_xact_lock('
                                'hashtext(:audit_signal_writer_lock_key))'
                            ),
                            {
                                'audit_signal_writer_lock_key': (
                                    AUDIT_SIGNAL_WRITER_LOCK_KEY
                                )
                            },
                        )
                    active_counts = _active_writer_counts(session)
                    active = {
                        table: count
                        for table, count in active_counts.items()
                        if count > 0
                    }
                    if active:
                        guard_yielded = True
                        yield False, 'active_writer:' + ','.join(sorted(active))
                        return
                    guard_yielded = True
                    with audit_signal_writer_lock_owner():
                        yield True, None
            except SQLAlchemyError:
                if guard_yielded:
                    raise
                yield False, 'writer_table_lock_unavailable'


class ExactDartAuditSignalRepair:
    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        capture: Callable[..., Any] | None = None,
        event_loader: Callable[[int], Mapping[str, Any] | None] | None = None,
        terminal_state_loader: Callable[
            [Mapping[str, Any]], Mapping[str, Any] | None
        ]
        | None = None,
        candidate_scope_loader: Callable[
            [date, list[Mapping[str, Any]]], list[Mapping[str, Any]]
        ]
        | None = None,
        selected_candidate_scope_loader: Callable[
            [date, list[Mapping[str, Any]]], list[Mapping[str, Any]]
        ]
        | None = None,
        missing_candidate_scope_loader: Callable[
            [date, list[Mapping[str, Any]]], list[Mapping[str, Any]]
        ]
        | None = None,
        guard_factory: Callable[..., AbstractContextManager[Any]] | None = None,
        finality_validator: Callable[[date], None] | None = None,
    ) -> None:
        self.manifest = normalize_manifest(manifest)
        self.manifest_sha256 = canonical_json_sha256(self.manifest)
        self._uses_scoped_production_capture = capture is None
        self.capture = capture or dart_signals_service.capture_audit_signal_for_event
        self.event_loader = event_loader or _load_event_identity
        self._uses_storage_terminal_state_loader = terminal_state_loader is None
        self.terminal_state_loader = (
            terminal_state_loader or _load_audit_terminal_state
        )
        if candidate_scope_loader is not None:
            self.selected_candidate_scope_loader = candidate_scope_loader
            self.missing_candidate_scope_loader = candidate_scope_loader
        elif event_loader is None:
            self.selected_candidate_scope_loader = (
                _load_current_selected_annual_candidate_scope
            )
            self.missing_candidate_scope_loader = (
                _load_current_missing_annual_candidate_scope
            )
        else:
            # Injected event loaders are a test seam.  Production construction
            # always uses the storage-backed current-scope loader above.
            identity_loader = lambda _market_date, entries: [
                dict(entry) for entry in entries
            ]
            self.selected_candidate_scope_loader = identity_loader
            self.missing_candidate_scope_loader = identity_loader
        if selected_candidate_scope_loader is not None:
            self.selected_candidate_scope_loader = (
                selected_candidate_scope_loader
            )
        if missing_candidate_scope_loader is not None:
            self.missing_candidate_scope_loader = (
                missing_candidate_scope_loader
            )
        self.guard_factory = guard_factory or exact_dart_audit_batch_guard
        self.finality_validator = finality_validator or (
            lambda market_date: validate_finalized_krx_write_date(
                market_date,
                operation='exact DART audit-signal repair',
            )
        )

    @staticmethod
    def _candidate_scope_anchor(entry: Mapping[str, Any]) -> dict[str, Any]:
        replacement = entry.get('replacement_for')
        if isinstance(replacement, Mapping):
            return _normalize_event_identity(replacement)
        return _normalize_event_identity(entry)

    def _validate_current_candidate_scope(
        self,
        entries: list[dict[str, Any]],
        *,
        require_missing: bool,
    ) -> None:
        if not entries:
            return
        anchors = [self._candidate_scope_anchor(entry) for entry in entries]
        loader = (
            self.missing_candidate_scope_loader
            if require_missing
            else self.selected_candidate_scope_loader
        )
        observed_raw = loader(
            date.fromisoformat(self.manifest['market_date']),
            anchors,
        )
        observed = [
            _normalize_event_identity(item) for item in observed_raw
        ]
        expected_by_event_id = {
            int(item['event_id']): item for item in anchors
        }
        observed_by_event_id = {
            int(item['event_id']): item for item in observed
        }
        if observed_by_event_id != expected_by_event_id:
            missing_event_ids = sorted(
                set(expected_by_event_id) - set(observed_by_event_id)
            )
            unexpected_event_ids = sorted(
                set(observed_by_event_id) - set(expected_by_event_id)
            )
            mismatched_event_ids = sorted(
                event_id
                for event_id in (
                    set(expected_by_event_id) & set(observed_by_event_id)
                )
                if observed_by_event_id[event_id]
                != expected_by_event_id[event_id]
            )
            raise ValueError(
                'manifest candidate scope is no longer the selected annual '
                f'{"missing " if require_missing else ""}'
                'strategy-input scope: '
                f'missing_event_ids={missing_event_ids}, '
                f'unexpected_event_ids={unexpected_event_ids}, '
                f'mismatched_event_ids={mismatched_event_ids}'
            )

    def _validate_identity(self, identity: Mapping[str, Any]) -> dict[str, Any]:
        persisted = self.event_loader(int(identity['event_id']))
        if persisted is None:
            raise ValueError(
                f'DART event {identity["event_id"]} is missing from storage'
            )
        expected = {
            'event_id': int(identity['event_id']),
            'stock_code': str(identity['stock_code']),
            'rcept_no': str(identity['rcept_no']),
        }
        observed = {
            'event_id': int(persisted.get('event_id') or 0),
            'stock_code': str(persisted.get('stock_code') or ''),
            'rcept_no': str(persisted.get('rcept_no') or ''),
        }
        if observed != expected:
            raise ValueError(
                'DART event identity drift: '
                f'expected={expected}, observed={observed}'
            )
        raw_receipt_date = persisted.get('receipt_date')
        if raw_receipt_date is None:
            raise ValueError(
                f'DART event {identity["event_id"]} receipt_date is missing'
            )
        persisted_receipt_date = (
            raw_receipt_date
            if isinstance(raw_receipt_date, date)
            else date.fromisoformat(str(raw_receipt_date))
        )
        market_date = date.fromisoformat(self.manifest['market_date'])
        if persisted_receipt_date > market_date:
            raise ValueError(
                'DART event receipt_date exceeds manifest market_date: '
                f'event_id={identity["event_id"]}, '
                f'receipt_date={persisted_receipt_date.isoformat()}, '
                f'market_date={market_date.isoformat()}'
            )
        normalized = dict(persisted)
        normalized['receipt_date'] = persisted_receipt_date.isoformat()
        return normalized

    def _validate_entry(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        persisted = self._validate_identity(entry)
        lineage: dict[str, Any] = {
            'event_id': entry['event_id'],
            'rcept_no': entry['rcept_no'],
            'stock_code': entry['stock_code'],
            'persisted_receipt_date': persisted['receipt_date'],
        }
        replacement = entry.get('replacement_for')
        if isinstance(replacement, Mapping):
            original = self._validate_identity(replacement)
            lineage['replacement_for'] = {
                'event_id': replacement['event_id'],
                'rcept_no': replacement['rcept_no'],
                'stock_code': replacement['stock_code'],
                'source_error_code': replacement['source_error_code'],
                'reason': replacement['reason'],
                'persisted_report_nm': original.get('report_nm'),
                'persisted_receipt_date': original['receipt_date'],
            }
        lineage['persisted_report_nm'] = persisted.get('report_nm')
        return lineage

    async def _attempt_entry(
        self,
        entry: Mapping[str, Any],
        *,
        semaphore: asyncio.Semaphore,
    ) -> dict[str, Any]:
        try:
            lineage = self._validate_entry(entry)
        except Exception as exc:  # noqa: BLE001
            return {
                'event_id': entry.get('event_id'),
                'rcept_no': entry.get('rcept_no'),
                'stock_code': entry.get('stock_code'),
                'status': 'failed',
                'error_type': type(exc).__name__,
                'error_message': str(exc)[:500],
            }
        try:
            async with semaphore:
                event_timeout_seconds = _audit_event_timeout_seconds()
                capture_kwargs: dict[str, Any] = {
                    'rcept_no': str(entry['rcept_no'])
                }
                if self._uses_scoped_production_capture:
                    capture_kwargs.update(
                        {
                            'selected_candidate_identity': (
                                self._candidate_scope_anchor(entry)
                            ),
                            'selected_candidate_market_date': (
                                date.fromisoformat(
                                    self.manifest['market_date']
                                )
                            ),
                            'repair_manifest_sha256': (
                                self.manifest_sha256
                            ),
                        }
                    )
                    replacement = entry.get('replacement_for')
                    if isinstance(replacement, Mapping):
                        capture_kwargs.update(
                            {
                                'replacement_target_identity': dict(
                                    replacement
                                ),
                                'replacement_manifest_sha256': (
                                    self.manifest_sha256
                                ),
                            }
                        )
                result = await asyncio.wait_for(
                    self.capture(**capture_kwargs),
                    timeout=event_timeout_seconds,
                )
        except TimeoutError:
            return {
                **lineage,
                'status': 'failed',
                'error_type': 'TimeoutError',
                'error_message': (
                    'audit capture exceeded '
                    f'{event_timeout_seconds:g}s deadline'
                ),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                **lineage,
                'status': 'failed',
                'error_type': type(exc).__name__,
                'error_message': str(exc)[:500],
            }
        if not isinstance(result, Mapping):
            return {
                **lineage,
                'status': 'failed',
                'error_type': 'RuntimeError',
                'error_message': 'audit capture returned a non-object result',
            }
        audit_signal = result.get('audit_signal')
        if isinstance(audit_signal, Mapping):
            audit_content = _normalize_audit_content(audit_signal)
            persistence_identity = self._candidate_scope_anchor(entry)
            persistence_receipt_date = lineage['persisted_receipt_date']
            if 'replacement_for' in lineage:
                persistence_receipt_date = lineage['replacement_for'][
                    'persisted_receipt_date'
                ]
            audit_status = _classify_durable_audit_content(
                audit_content,
                event_id=persistence_identity['event_id'],
                rcept_no=str(persistence_identity['rcept_no']),
                receipt_date=persistence_receipt_date,
            )
            if audit_status is None:
                return {
                    **lineage,
                    'status': 'audit_evidence_missing',
                    'error_type': 'RuntimeError',
                    'error_message': (
                        'audit capture returned malformed durable evidence'
                    ),
                }
            return {
                **lineage,
                'status': audit_status,
                'opinion_code': audit_content['opinion_code'],
                'has_audit_issue': audit_content['has_audit_issue'],
                'audit_signal_evaluated': (
                    audit_status == 'authoritative_absence'
                ),
                'audit_content': audit_content,
                'audit_content_sha256': canonical_json_sha256(audit_content),
            }
        if result.get('audit_signal_evaluated') is True:
            return {
                **lineage,
                'status': 'audit_evidence_missing',
                'error_type': 'RuntimeError',
                'error_message': (
                    'authoritative absence was evaluated but no durable '
                    'audit proof was returned'
                ),
                'audit_diagnostics': result.get('audit_diagnostics'),
            }
        return {
            **lineage,
            'status': 'audit_evidence_missing',
            'audit_diagnostics': result.get('audit_diagnostics'),
        }

    @staticmethod
    def _completed_batch_checkpoint_is_valid(
        checkpoint_unit: Mapping[str, Any],
        batch: list[dict[str, Any]],
    ) -> bool:
        results = checkpoint_unit.get('results')
        if not isinstance(results, list) or len(results) != len(batch):
            return False
        for entry, result in zip(batch, results, strict=True):
            if (
                not isinstance(result, Mapping)
                or result.get('status') not in _TERMINAL_AUDIT_STATUSES
            ):
                return False
            if (
                result.get('status') == 'authoritative_absence'
                and result.get('audit_signal_evaluated') is not True
            ):
                return False
            if result.get('status') in _TERMINAL_AUDIT_STATUSES:
                audit_content = result.get('audit_content')
                if (
                    not isinstance(audit_content, Mapping)
                    or set(audit_content) != set(_AUDIT_CONTENT_FIELDS)
                    or result.get('audit_content_sha256')
                    != canonical_json_sha256(dict(audit_content))
                ):
                    return False
            if (
                result.get('event_id') != entry['event_id']
                or result.get('rcept_no') != entry['rcept_no']
                or result.get('stock_code') != entry['stock_code']
            ):
                return False
            expected_replacement = entry.get('replacement_for')
            observed_replacement = result.get('replacement_for')
            if isinstance(expected_replacement, Mapping):
                if not isinstance(observed_replacement, Mapping):
                    return False
                for field in (
                    'event_id',
                    'rcept_no',
                    'stock_code',
                    'source_error_code',
                    'reason',
                ):
                    if observed_replacement.get(field) != expected_replacement.get(field):
                        return False
            elif observed_replacement is not None:
                return False
        return True

    def _completed_batch_live_state_is_valid(
        self,
        checkpoint_unit: Mapping[str, Any],
        batch: list[dict[str, Any]],
    ) -> bool:
        results = checkpoint_unit['results']
        for entry, result in zip(batch, results, strict=True):
            try:
                self._validate_entry(entry)
            except ValueError:
                return False
            proof_entry = self._candidate_scope_anchor(entry)
            proof = self.terminal_state_loader(proof_entry)
            if not isinstance(proof, Mapping):
                return False
            if proof.get('status') != result.get('status'):
                return False
            if any(
                proof.get(field) != proof_entry[field]
                for field in ('event_id', 'rcept_no', 'stock_code')
            ):
                return False
            if (
                result.get('status') == 'authoritative_absence'
                and proof.get('audit_signal_evaluated') is not True
            ):
                return False
            if result.get('status') in _TERMINAL_AUDIT_STATUSES:
                expected_content = result.get('audit_content')
                observed_content = proof.get('audit_content')
                if (
                    not isinstance(expected_content, Mapping)
                    or not isinstance(observed_content, Mapping)
                    or dict(observed_content) != dict(expected_content)
                    or proof.get('audit_content_sha256')
                    != result.get('audit_content_sha256')
                ):
                    return False
                expected_receipt_date = result.get(
                    'persisted_receipt_date'
                )
                if isinstance(result.get('replacement_for'), Mapping):
                    expected_receipt_date = result['replacement_for'].get(
                        'persisted_receipt_date'
                    )
                if proof.get('receipt_date') != expected_receipt_date:
                    return False
        return True

    def _durable_result_for_entry(
        self,
        entry: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Reconcile a durable proof committed before its checkpoint.

        Audit rows commit independently from the repair checkpoint.  A crash
        in that narrow window must not make a resumed repair overwrite the
        proof or become permanently stranded behind the missing-only gate.
        """

        proof_entry = self._candidate_scope_anchor(entry)
        proof = self.terminal_state_loader(proof_entry)
        if not isinstance(proof, Mapping):
            return None
        if proof.get('status') not in _TERMINAL_AUDIT_STATUSES:
            return None
        if any(
            proof.get(field) != proof_entry[field]
            for field in ('event_id', 'rcept_no', 'stock_code')
        ):
            return None
        audit_content = proof.get('audit_content')
        if (
            not isinstance(audit_content, Mapping)
            or set(audit_content) != set(_AUDIT_CONTENT_FIELDS)
            or proof.get('audit_content_sha256')
            != canonical_json_sha256(dict(audit_content))
        ):
            return None
        lineage = self._validate_entry(entry)
        expected_receipt_date = lineage['persisted_receipt_date']
        payload = audit_content.get('payload')
        stored_exact_repair_lineage = (
            payload.get('exact_repair_lineage')
            if isinstance(payload, Mapping)
            else None
        )
        expected_exact_repair_lineage = {
            'manifest_sha256': self.manifest_sha256,
            'source': {
                'event_id': entry['event_id'],
                'rcept_no': entry['rcept_no'],
                'stock_code': entry['stock_code'],
                'receipt_date': lineage['persisted_receipt_date'],
            },
            'target': {
                'event_id': entry['event_id'],
                'rcept_no': entry['rcept_no'],
                'stock_code': entry['stock_code'],
                'receipt_date': lineage['persisted_receipt_date'],
            },
            'mode': 'direct',
        }
        if isinstance(lineage.get('replacement_for'), Mapping):
            expected_receipt_date = lineage['replacement_for'][
                'persisted_receipt_date'
            ]
            stored_replacement_lineage = (
                payload.get('replacement_lineage')
                if isinstance(payload, Mapping)
                else None
            )
            if stored_replacement_lineage is not None:
                expected_replacement_lineage = {
                    'manifest_sha256': self.manifest_sha256,
                    'source': {
                        'event_id': entry['event_id'],
                        'rcept_no': entry['rcept_no'],
                        'stock_code': entry['stock_code'],
                        'receipt_date': lineage[
                            'persisted_receipt_date'
                        ],
                    },
                    'target': {
                        'event_id': entry['replacement_for']['event_id'],
                        'rcept_no': entry['replacement_for']['rcept_no'],
                        'stock_code': entry['replacement_for']['stock_code'],
                        'receipt_date': expected_receipt_date,
                    },
                    'source_error_code': entry['replacement_for'][
                        'source_error_code'
                    ],
                    'reason': entry['replacement_for']['reason'],
                }
                if not isinstance(stored_replacement_lineage, Mapping) or any(
                    stored_replacement_lineage.get(field) != expected_value
                    for field, expected_value in (
                        expected_replacement_lineage.items()
                    )
                ):
                    raise ValueError(
                        'durable replacement proof has conflicting manifest '
                        'provenance'
                    )
            expected_exact_repair_lineage['target'] = {
                'event_id': entry['replacement_for']['event_id'],
                'rcept_no': entry['replacement_for']['rcept_no'],
                'stock_code': entry['replacement_for']['stock_code'],
                'receipt_date': expected_receipt_date,
            }
            expected_exact_repair_lineage['mode'] = (
                'replacement_projection'
            )
        exact_lineage_matches = (
            isinstance(stored_exact_repair_lineage, Mapping)
            and all(
                stored_exact_repair_lineage.get(field) == expected_value
                for field, expected_value in (
                    expected_exact_repair_lineage.items()
                )
            )
        )
        resolution = (
            'already_durable_matching_repair'
            if exact_lineage_matches
            else 'externally_resolved'
        )
        status = _classify_durable_audit_content(
            audit_content,
            event_id=proof_entry['event_id'],
            rcept_no=proof_entry['rcept_no'],
            receipt_date=expected_receipt_date,
        )
        if (
            status != proof.get('status')
            or proof.get('receipt_date') != expected_receipt_date
            or (
                status == 'authoritative_absence'
                and proof.get('audit_signal_evaluated') is not True
            )
        ):
            return None
        return {
            **lineage,
            'status': status,
            'opinion_code': audit_content['opinion_code'],
            'has_audit_issue': audit_content['has_audit_issue'],
            'audit_signal_evaluated': status == 'authoritative_absence',
            'audit_content': dict(audit_content),
            'audit_content_sha256': canonical_json_sha256(
                dict(audit_content)
            ),
            'resolution': resolution,
        }

    async def run(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
        checkpoint_path: str | Path | None = None,
        concurrency: int = 1,
        _checkpoint_lock_held: bool = False,
    ) -> dict[str, Any]:
        if not 1 <= concurrency <= MAX_CONCURRENCY:
            raise ValueError(f'concurrency must be between 1 and {MAX_CONCURRENCY}')
        base = {
            'schema_version': MANIFEST_SCHEMA_VERSION,
            'market_date': self.manifest['market_date'],
            'manifest_sha256': self.manifest_sha256,
            'entry_count': self.manifest['entry_count'],
            'batch_size': self.manifest['batch_size'],
            'mode': 'apply' if apply else 'dry_run',
        }
        if not apply:
            lineages = [
                self._validate_entry(entry)
                for entry in self.manifest['entries']
            ]
            self._validate_current_candidate_scope(
                list(self.manifest['entries']),
                require_missing=True,
            )
            return {
                **base,
                'status': 'dry_run',
                'applied': False,
                'validated_entry_count': len(lineages),
                'replacement_lineages': [
                    item for item in lineages if 'replacement_for' in item
                ],
            }

        expected_hash = str(expected_manifest_sha256 or '').strip().lower()
        if not expected_hash:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'expected_manifest_sha256_required',
            }
        if expected_hash != self.manifest_sha256:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'expected_manifest_sha256_mismatch',
            }
        if checkpoint_path is None:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_path_required',
            }
        if not _checkpoint_lock_held:
            with checkpoint_file_lock(checkpoint_path) as lock_acquired:
                if not lock_acquired:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'checkpoint_lock_unavailable',
                    }
                return await self.run(
                    apply=True,
                    expected_manifest_sha256=expected_manifest_sha256,
                    checkpoint_path=checkpoint_path,
                    concurrency=concurrency,
                    _checkpoint_lock_held=True,
                )
        try:
            self.finality_validator(
                date.fromisoformat(self.manifest['market_date'])
            )
        except (TypeError, ValueError) as exc:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'market_date_not_finalized',
                'error_message': str(exc),
            }
        try:
            checkpoint = load_checkpoint(
                checkpoint_path,
                schema_version=CHECKPOINT_SCHEMA_VERSION,
                manifest_sha256=self.manifest_sha256,
                item_count=self.manifest['entry_count'],
                unit_size=self.manifest['batch_size'],
            )
        except ValueError as exc:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_manifest_mismatch',
                'error_message': str(exc),
            }

        entries = self.manifest['entries']
        batch_size = self.manifest['batch_size']
        expected_batch_keys = {
            str(index)
            for index in range((len(entries) + batch_size - 1) // batch_size)
        }
        if set(checkpoint['units']) - expected_batch_keys:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_batch_set_mismatch',
            }
        completed_batches = 0
        skipped_batches = 0
        failed_batches = 0
        captured = 0
        authoritative_absence = 0
        evidence_missing = 0
        failed = 0
        externally_resolved = 0

        def progress(
            live_verified_results: Any = (),
        ) -> dict[str, Any]:
            live_verified = list(live_verified_results)
            live_captured = sum(
                item.get('status') == 'captured'
                for item in live_verified
                if isinstance(item, Mapping)
            )
            live_absence = sum(
                item.get('status') == 'authoritative_absence'
                for item in live_verified
                if isinstance(item, Mapping)
            )
            live_external = sum(
                item.get('resolution') == 'externally_resolved'
                for item in live_verified
                if isinstance(item, Mapping)
            )
            committed_entry_count = (
                captured
                + authoritative_absence
                + live_captured
                + live_absence
            )
            return {
                'completed_batches': completed_batches,
                'skipped_batches': skipped_batches,
                'failed_batches': failed_batches,
                'audit_signals_captured': captured + live_captured,
                'audit_signals_authoritatively_absent': (
                    authoritative_absence + live_absence
                ),
                'audit_evidence_missing': evidence_missing,
                'events_failed': failed,
                'committed_entry_count': committed_entry_count,
                'repair_committed_entry_count': (
                    committed_entry_count
                    - externally_resolved
                    - live_external
                ),
                'externally_resolved_entry_count': (
                    externally_resolved + live_external
                ),
                'partial_applied': (
                    committed_entry_count
                    - externally_resolved
                    - live_external
                )
                > 0,
                'fully_applied': False,
                'live_verified_committed_entry_count': (
                    live_captured + live_absence
                ),
            }

        def preserve_blocked_attempt(
            *,
            batch_key: str,
            batch_sha256: str,
            previous: Mapping[str, Any] | None,
            blocked_attempt: Mapping[str, Any],
        ) -> Exception | None:
            if isinstance(previous, Mapping):
                preserved = deepcopy(dict(previous))
                preserved['last_blocked_attempt'] = dict(blocked_attempt)
            else:
                preserved = {
                    'status': 'blocked',
                    'item_sha256': batch_sha256,
                    'last_blocked_attempt': dict(blocked_attempt),
                }
            checkpoint['units'][batch_key] = preserved
            try:
                write_checkpoint(checkpoint_path, checkpoint)
            except Exception as exc:  # noqa: BLE001
                # No repair mutation happened on a blocked path. Preserve the
                # original block reason and let the next run revalidate it.
                return exc
            return None

        def reconcile_durable_results(
            batch: Sequence[Mapping[str, Any]],
        ) -> list[dict[str, Any]]:
            reconciled: list[dict[str, Any]] = []
            for entry in batch:
                try:
                    durable_result = self._durable_result_for_entry(entry)
                except Exception:  # noqa: BLE001
                    durable_result = None
                if durable_result is not None:
                    reconciled.append(durable_result)
            return reconciled

        for start in range(0, len(entries), batch_size):
            batch_index = start // batch_size
            batch_key = str(batch_index)
            batch = entries[start : start + batch_size]
            batch_sha256 = canonical_json_sha256(batch)
            previous = checkpoint['units'].get(batch_key)
            if isinstance(previous, Mapping):
                previous_hash = previous.get('item_sha256')
                if previous_hash != batch_sha256:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'checkpoint_batch_hash_mismatch',
                        'batch_index': batch_index,
                        **progress(),
                    }
                if previous.get('status') == 'completed':
                    if not self._completed_batch_checkpoint_is_valid(
                        previous,
                        batch,
                    ):
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'checkpoint_completed_batch_invalid',
                            'batch_index': batch_index,
                            **progress(),
                        }

            guard_exit_state: dict[str, Any] = {}
            with suppress(
                GuardTransactionExitError
            ), capture_guard_transaction_failure(
                self.guard_factory,
                state=guard_exit_state,
                market_date=date.fromisoformat(self.manifest['market_date']),
                batch_size=len(batch),
                concurrency=concurrency,
            ) as guard:
                acquired, reason = guard
                if not acquired:
                    blocked_attempt = {
                        'reason': str(reason or 'active_writer'),
                    }
                    preserve_blocked_attempt(
                        batch_key=batch_key,
                        batch_sha256=batch_sha256,
                        previous=(
                            previous if isinstance(previous, Mapping) else None
                        ),
                        blocked_attempt=blocked_attempt,
                    )
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': str(reason or 'active_writer'),
                        'batch_index': batch_index,
                        **progress(),
                    }

                try:
                    # Every manifest anchor must still be the selected annual
                    # strategy input, regardless of whether durable audit
                    # proof already exists from an earlier attempt.
                    self._validate_current_candidate_scope(
                        batch,
                        require_missing=False,
                    )
                except Exception as exc:  # noqa: BLE001
                    blocked_attempt = {
                        'reason': 'manifest_candidate_scope_drift',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                    preserve_blocked_attempt(
                        batch_key=batch_key,
                        batch_sha256=batch_sha256,
                        previous=(
                            previous if isinstance(previous, Mapping) else None
                        ),
                        blocked_attempt=blocked_attempt,
                    )
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'manifest_candidate_scope_drift',
                        'batch_index': batch_index,
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                        **progress(),
                    }

                retained_results: dict[int, dict[str, Any]] = {}
                previous_results = (
                    previous.get('results')
                    if isinstance(previous, Mapping)
                    else None
                )
                if (
                    isinstance(previous_results, list)
                    and len(previous_results) == len(batch)
                ):
                    for item_index, (entry, result) in enumerate(
                        zip(batch, previous_results, strict=True)
                    ):
                        single_unit = {'results': [result]}
                        if not self._completed_batch_checkpoint_is_valid(
                            single_unit,
                            [entry],
                        ):
                            continue
                        try:
                            live_state_valid = (
                                self._completed_batch_live_state_is_valid(
                                    single_unit,
                                    [entry],
                                )
                            )
                        except Exception as exc:  # noqa: BLE001
                            return {
                                **base,
                                'status': 'blocked',
                                'applied': False,
                                'reason': (
                                    'checkpoint_live_revalidation_failed'
                                ),
                                'batch_index': batch_index,
                                'error_type': type(exc).__name__,
                                'error_message': str(exc)[:500],
                                **progress(retained_results.values()),
                            }
                        if live_state_valid:
                            retained_results[item_index] = dict(result)

                if self._uses_storage_terminal_state_loader:
                    for item_index, entry in enumerate(batch):
                        if item_index in retained_results:
                            continue
                        try:
                            durable_result = self._durable_result_for_entry(
                                entry
                            )
                        except ValueError as exc:
                            blocked_attempt = {
                                'reason': (
                                    'conflicting_durable_provenance'
                                ),
                                'error_type': type(exc).__name__,
                                'error_message': str(exc)[:500],
                            }
                            preserve_blocked_attempt(
                                batch_key=batch_key,
                                batch_sha256=batch_sha256,
                                previous=(
                                    previous
                                    if isinstance(previous, Mapping)
                                    else None
                                ),
                                blocked_attempt=blocked_attempt,
                            )
                            return {
                                **base,
                                'status': 'blocked',
                                'applied': False,
                                'reason': (
                                    'conflicting_durable_provenance'
                                ),
                                'batch_index': batch_index,
                                'error_type': type(exc).__name__,
                                'error_message': str(exc)[:500],
                                **progress(retained_results.values()),
                            }
                        if durable_result is not None:
                            retained_results[item_index] = durable_result

                retry_entries = [
                    entry
                    for item_index, entry in enumerate(batch)
                    if item_index not in retained_results
                ]
                try:
                    self._validate_current_candidate_scope(
                        retry_entries,
                        require_missing=True,
                    )
                except Exception as exc:  # noqa: BLE001
                    blocked_attempt = {
                        'reason': 'manifest_candidate_scope_drift',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                    preserve_blocked_attempt(
                        batch_key=batch_key,
                        batch_sha256=batch_sha256,
                        previous=(
                            previous if isinstance(previous, Mapping) else None
                        ),
                        blocked_attempt=blocked_attempt,
                    )
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'manifest_candidate_scope_drift',
                        'batch_index': batch_index,
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                        **progress(retained_results.values()),
                    }
                if not retry_entries:
                    skipped_batches += 1
                semaphore = asyncio.Semaphore(concurrency)
                retry_results = await asyncio.gather(
                    *(
                        self._attempt_entry(entry, semaphore=semaphore)
                        for entry in retry_entries
                    )
                )
                retry_results_iter = iter(retry_results)
                results = [
                    retained_results[item_index]
                    if item_index in retained_results
                    else next(retry_results_iter)
                    for item_index in range(len(batch))
                ]

            guard_exit_error = guard_exit_state.get('error')
            if isinstance(guard_exit_error, Exception):
                reconciled_results = reconcile_durable_results(batch)
                uncertain_attempt = {
                    'reason': 'guard_transaction_exit_failed',
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'reconciled_results': reconciled_results,
                }
                if isinstance(previous, Mapping):
                    uncertain_unit = deepcopy(dict(previous))
                    uncertain_unit['last_uncertain_attempt'] = (
                        uncertain_attempt
                    )
                else:
                    uncertain_unit = {
                        'status': 'uncertain',
                        'item_sha256': batch_sha256,
                        'last_uncertain_attempt': uncertain_attempt,
                    }
                checkpoint['units'][batch_key] = uncertain_unit
                checkpoint_write_error: Exception | None = None
                try:
                    write_checkpoint(checkpoint_path, checkpoint)
                except Exception as exc:  # noqa: BLE001
                    checkpoint_write_error = exc
                matching_repair_count = sum(
                    item.get('resolution')
                    == 'already_durable_matching_repair'
                    for item in reconciled_results
                )
                result = {
                    **base,
                    'status': (
                        'partial' if matching_repair_count else 'blocked'
                    ),
                    'applied': False,
                    'reason': 'guard_transaction_exit_failed',
                    'batch_index': batch_index,
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'uncertain_commit': True,
                    'retry_required': True,
                    'reconciled_proof_count': len(reconciled_results),
                    'checkpoint_persisted': checkpoint_write_error is None,
                    **progress(reconciled_results),
                }
                if checkpoint_write_error is not None:
                    result['checkpoint_error_type'] = type(
                        checkpoint_write_error
                    ).__name__
                    result['checkpoint_error_message'] = str(
                        checkpoint_write_error
                    )[:500]
                return result

            batch_failed = any(
                item['status'] not in _TERMINAL_AUDIT_STATUSES
                for item in results
            )
            checkpoint['units'][batch_key] = {
                'status': 'failed' if batch_failed else 'completed',
                'item_sha256': batch_sha256,
                'results': results,
            }
            try:
                write_checkpoint(checkpoint_path, checkpoint)
            except Exception as checkpoint_exc:  # noqa: BLE001
                reconciled_results = reconcile_durable_results(batch)
                matching_repair_count = sum(
                    item.get('resolution')
                    == 'already_durable_matching_repair'
                    for item in reconciled_results
                )
                return {
                    **base,
                    'status': (
                        'partial' if matching_repair_count else 'blocked'
                    ),
                    'applied': False,
                    'reason': 'checkpoint_write_failed_after_commit',
                    'batch_index': batch_index,
                    'error_type': type(checkpoint_exc).__name__,
                    'error_message': str(checkpoint_exc)[:500],
                    'uncertain_commit': True,
                    'checkpoint_persisted': False,
                    'reconciled_proof_count': len(reconciled_results),
                    'retry_required': True,
                    **progress(reconciled_results),
                }
            for item in results:
                if item.get('resolution') == 'externally_resolved':
                    externally_resolved += 1
                if item['status'] == 'captured':
                    captured += 1
                elif item['status'] == 'authoritative_absence':
                    authoritative_absence += 1
                elif item['status'] == 'audit_evidence_missing':
                    evidence_missing += 1
                else:
                    failed += 1
            if batch_failed:
                failed_batches += 1
            else:
                completed_batches += 1

        status = 'completed' if failed_batches == 0 else 'partial'
        return {
            **base,
            'status': status,
            'applied': (
                status == 'completed' and externally_resolved == 0
            ),
            **progress(),
            'fully_applied': (
                status == 'completed' and externally_resolved == 0
            ),
            'partial_applied': (
                (status != 'completed' or externally_resolved > 0)
                and (
                    captured
                    + authoritative_absence
                    - externally_resolved
                )
                > 0
            ),
            'retry_required': failed_batches > 0,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Run an exact, resumable DART audit-signal repair manifest.'
    )
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--checkpoint')
    parser.add_argument('--expected-manifest-sha256')
    parser.add_argument(
        '--concurrency',
        type=int,
        choices=range(1, MAX_CONCURRENCY + 1),
        default=1,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--dry-run', action='store_true')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.apply and args.checkpoint and (
        Path(args.manifest).resolve() == Path(args.checkpoint).resolve()
    ):
        raise ValueError('manifest and checkpoint paths must be different')
    manifest = load_json_mapping(args.manifest, label='manifest')
    repair = ExactDartAuditSignalRepair(deepcopy(manifest))
    result = asyncio.run(
        repair.run(
            apply=bool(args.apply),
            expected_manifest_sha256=args.expected_manifest_sha256,
            checkpoint_path=args.checkpoint,
            concurrency=args.concurrency,
        )
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['status'] in {'dry_run', 'completed'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
