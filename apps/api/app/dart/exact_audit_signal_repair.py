from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from datetime import date
import json
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from app.dart.signals_service import (
    AUDIT_SIGNAL_WRITER_LOCK_KEY,
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
from app.eod.finality import validate_finalized_krx_write_date
from app.internal.exact_repair_manifest import (
    canonical_json_sha256,
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
        entries.append(entry)

    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'market_date': market_date.isoformat(),
        'entry_count': len(entries),
        'batch_size': batch_size,
        'entries': entries,
    }


def _load_event_identity(event_id: int) -> dict[str, Any] | None:
    with session_scope(statement_timeout_ms=5_000) as session:
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

    with session_scope(statement_timeout_ms=5_000) as session:
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


def _active_writer_counts(session: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for model in _RUN_MODELS:
        count = session.execute(
            select(func.count()).select_from(model).where(model.status == 'running')
        ).scalar_one()
        counts[model.__tablename__] = int(count or 0)
    return counts


@contextmanager
def exact_dart_audit_batch_guard(
    *, market_date: date
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
                    statement_timeout_ms=5_000,
                    lock_timeout_ms=1,
                    isolated_connection=True,
                ) as session:
                    if session.get_bind().dialect.name == 'postgresql':
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
        guard_factory: Callable[..., AbstractContextManager[Any]] | None = None,
        finality_validator: Callable[[date], None] | None = None,
    ) -> None:
        self.manifest = normalize_manifest(manifest)
        self.manifest_sha256 = canonical_json_sha256(self.manifest)
        self.capture = capture or dart_signals_service.capture_audit_signal_for_event
        self.event_loader = event_loader or _load_event_identity
        self.terminal_state_loader = (
            terminal_state_loader or _load_audit_terminal_state
        )
        self.guard_factory = guard_factory or exact_dart_audit_batch_guard
        self.finality_validator = finality_validator or (
            lambda market_date: validate_finalized_krx_write_date(
                market_date,
                operation='exact DART audit-signal repair',
            )
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
                result = await self.capture(rcept_no=str(entry['rcept_no']))
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
            audit_status = _classify_durable_audit_content(
                audit_content,
                event_id=entry['event_id'],
                rcept_no=str(entry['rcept_no']),
                receipt_date=lineage['persisted_receipt_date'],
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
            proof = self.terminal_state_loader(entry)
            if not isinstance(proof, Mapping):
                return False
            if proof.get('status') != result.get('status'):
                return False
            if any(
                proof.get(field) != entry[field]
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
                if proof.get('receipt_date') != result.get(
                    'persisted_receipt_date'
                ):
                    return False
        return True

    async def run(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
        checkpoint_path: str | Path | None = None,
        concurrency: int = 1,
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
        for start in range(0, len(entries), batch_size):
            batch_index = start // batch_size
            batch_key = str(batch_index)
            batch = entries[start : start + batch_size]
            batch_sha256 = canonical_json_sha256(batch)
            previous = checkpoint['units'].get(batch_key)
            completed_checkpoint: Mapping[str, Any] | None = None
            if isinstance(previous, Mapping):
                previous_hash = previous.get('item_sha256')
                if previous_hash != batch_sha256:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'checkpoint_batch_hash_mismatch',
                        'batch_index': batch_index,
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
                        }
                    completed_checkpoint = previous

            with self.guard_factory(
                market_date=date.fromisoformat(self.manifest['market_date'])
            ) as guard:
                acquired, reason = guard
                if not acquired:
                    checkpoint['units'][batch_key] = {
                        'status': 'blocked',
                        'item_sha256': batch_sha256,
                        'reason': str(reason or 'active_writer'),
                    }
                    write_checkpoint(checkpoint_path, checkpoint)
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': str(reason or 'active_writer'),
                        'batch_index': batch_index,
                        'completed_batches': completed_batches,
                        'skipped_batches': skipped_batches,
                    }
                if completed_checkpoint is not None:
                    try:
                        live_state_valid = (
                            self._completed_batch_live_state_is_valid(
                                completed_checkpoint,
                                batch,
                            )
                        )
                    except Exception as exc:  # noqa: BLE001
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'checkpoint_live_revalidation_failed',
                            'batch_index': batch_index,
                            'error_type': type(exc).__name__,
                            'error_message': str(exc)[:500],
                        }
                    if live_state_valid:
                        for item in completed_checkpoint['results']:
                            if item['status'] == 'captured':
                                captured += 1
                            else:
                                authoritative_absence += 1
                        completed_batches += 1
                        skipped_batches += 1
                        continue
                semaphore = asyncio.Semaphore(concurrency)
                results = await asyncio.gather(
                    *(
                        self._attempt_entry(entry, semaphore=semaphore)
                        for entry in batch
                    )
                )

            batch_failed = any(
                item['status'] not in _TERMINAL_AUDIT_STATUSES
                for item in results
            )
            for item in results:
                if item['status'] == 'captured':
                    captured += 1
                elif item['status'] == 'authoritative_absence':
                    authoritative_absence += 1
                elif item['status'] == 'audit_evidence_missing':
                    evidence_missing += 1
                else:
                    failed += 1
            checkpoint['units'][batch_key] = {
                'status': 'failed' if batch_failed else 'completed',
                'item_sha256': batch_sha256,
                'results': results,
            }
            write_checkpoint(checkpoint_path, checkpoint)
            if batch_failed:
                failed_batches += 1
            else:
                completed_batches += 1

        status = 'completed' if failed_batches == 0 else 'partial'
        return {
            **base,
            'status': status,
            'applied': status == 'completed',
            'completed_batches': completed_batches,
            'skipped_batches': skipped_batches,
            'failed_batches': failed_batches,
            'audit_signals_captured': captured,
            'audit_signals_authoritatively_absent': authoritative_absence,
            'audit_evidence_missing': evidence_missing,
            'events_failed': failed,
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
