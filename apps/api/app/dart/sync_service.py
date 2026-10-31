from __future__ import annotations

import math
import re
import hashlib
import json
from contextlib import nullcontext
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import httpx
from sqlalchemy.exc import (
    DBAPIError,
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)

from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.dart.store import DartSyncRepository, dart_sync_repository
from app.eod.finality import validate_finalized_krx_write_date
from app.integrations.dart.client import DartAPIError, DartClient, dart_client
from app.repositories.eod_run_repository import advisory_eod_collection_lock

_ANNUAL_REPORT = 'annual_report'
_SEMIANNUAL_REPORT = 'semiannual_report'
_QUARTERLY_REPORT = 'quarterly_report'
_OTHER_REPORT = 'other'

_FINANCIAL_REPORT_PREFIXES = (
    ('사업보고서', _ANNUAL_REPORT),
    ('반기보고서', _SEMIANNUAL_REPORT),
    ('분기보고서', _QUARTERLY_REPORT),
)

_CONSECUTIVE_PROVIDER_FAILURE_LIMIT = 3
_SYSTEMIC_DART_STATUS_CODES = {
    '010',  # unregistered API key
    '011',  # disabled API key
    '012',  # disallowed IP
    '020',  # request limit
    '100',  # invalid request shape
    '800',  # provider system maintenance/error
    '900',  # provider-defined system error
}
_SYSTEMIC_DART_MESSAGE_MARKERS = (
    'api key',
    'api_key',
    'connection',
    'provider unavailable',
    'rate limit',
    'request limit',
    'service unavailable',
    'temporarily unavailable',
    'timed out',
    'timeout',
    'upstream unavailable',
)

_GLOBAL_DART_LIST_MAX_WINDOW_DAYS = 90

_CORPORATION_LOCAL_PERSISTENCE_SQLSTATES = {
    '22001',  # string data right truncation
    '22003',  # numeric value out of range
    '22007',  # invalid datetime format
    '22P02',  # invalid text representation
    '23505',  # unique violation
}


@dataclass
class _DisclosurePaginationGuard:
    """Prove a disclosure-list scan was complete before negative evidence."""

    expected_total_count: int | None = None
    expected_page_size: int | None = None
    expected_total_pages: int | None = None
    pages_seen: int = 0
    receipt_numbers: set[str] = field(default_factory=set)

    @staticmethod
    def _parse_nonnegative_int(payload: dict[str, Any], key: str) -> int:
        try:
            value = int(payload.get(key))
        except (TypeError, ValueError) as exc:
            raise ValueError(f'DART pagination {key} is not an integer') from exc
        if value < 0:
            raise ValueError(f'DART pagination {key} must be nonnegative')
        return value

    def accept_page(
        self,
        payload: dict[str, Any],
        *,
        requested_page_no: int,
    ) -> tuple[list[dict[str, Any]], int]:
        response_page_no = self._parse_nonnegative_int(payload, 'page_no')
        response_page_size = self._parse_nonnegative_int(payload, 'page_count')
        total_count = self._parse_nonnegative_int(payload, 'total_count')
        if response_page_no != requested_page_no:
            raise ValueError(
                'DART pagination page_no mismatch: '
                f'requested={requested_page_no}, response={response_page_no}'
            )

        if self.expected_total_count is None:
            if total_count > 0 and response_page_size <= 0:
                raise ValueError(
                    'DART pagination page_count must be positive when total_count is positive'
                )
            self.expected_total_count = total_count
            self.expected_page_size = response_page_size
            self.expected_total_pages = (
                max(math.ceil(total_count / response_page_size), 1)
                if total_count
                else 1
            )
        else:
            if total_count != self.expected_total_count:
                raise ValueError(
                    'DART pagination total_count changed during scan: '
                    f'expected={self.expected_total_count}, observed={total_count}'
                )
            if response_page_size != self.expected_page_size:
                raise ValueError(
                    'DART pagination page_count changed during scan: '
                    f'expected={self.expected_page_size}, observed={response_page_size}'
                )

        assert self.expected_total_count is not None
        assert self.expected_page_size is not None
        assert self.expected_total_pages is not None
        if requested_page_no > self.expected_total_pages:
            raise ValueError(
                'DART pagination returned a page beyond the expected range: '
                f'page={requested_page_no}, expected_pages={self.expected_total_pages}'
            )

        raw_items = payload.get('list')
        if not isinstance(raw_items, list):
            raise ValueError('DART pagination list payload must be an array')
        if self.expected_total_count == 0:
            expected_item_count = 0
        else:
            expected_item_count = min(
                self.expected_page_size,
                self.expected_total_count
                - ((requested_page_no - 1) * self.expected_page_size),
            )
        if len(raw_items) != expected_item_count:
            raise ValueError(
                'DART pagination item_count mismatch: '
                f'page={requested_page_no}, expected={expected_item_count}, '
                f'observed={len(raw_items)}'
            )

        normalized_items: list[dict[str, Any]] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                raise ValueError('DART pagination item must be an object')
            receipt_number = str(raw_item.get('rcept_no') or '').strip()
            if not receipt_number:
                raise ValueError('DART pagination item is missing rcept_no')
            if receipt_number in self.receipt_numbers:
                raise ValueError(
                    f'DART pagination duplicate rcept_no: {receipt_number}'
                )
            self.receipt_numbers.add(receipt_number)
            normalized_items.append(raw_item)

        self.pages_seen += 1
        return normalized_items, self.expected_total_pages

    def assert_complete(self) -> None:
        if (
            self.expected_total_count is None
            or self.expected_total_pages is None
        ):
            raise ValueError('DART pagination scan returned no page metadata')
        if self.pages_seen != self.expected_total_pages:
            raise ValueError(
                'DART pagination page coverage mismatch: '
                f'expected={self.expected_total_pages}, observed={self.pages_seen}'
            )
        if len(self.receipt_numbers) != self.expected_total_count:
            raise ValueError(
                'DART pagination unique receipt count mismatch: '
                f'expected={self.expected_total_count}, '
                f'observed={len(self.receipt_numbers)}'
            )


def _is_systemic_provider_failure(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError, httpx.TransportError)):
        return True
    if isinstance(exc, ValueError) and str(exc).startswith('DART pagination '):
        return True
    if not isinstance(exc, DartAPIError):
        return False

    message = str(exc).strip().lower()
    status_match = re.search(r'\[(\d{3})\]', message)
    if status_match:
        status = status_match.group(1)
        if status in _SYSTEMIC_DART_STATUS_CODES:
            return True
        if int(status) >= 400:
            return True
        return False
    if any(marker in message for marker in _SYSTEMIC_DART_MESSAGE_MARKERS):
        return True

    cause = exc.__cause__
    return isinstance(cause, Exception) and _is_systemic_provider_failure(cause)


def _persistence_sqlstate(exc: Exception) -> str:
    original = getattr(exc, 'orig', None)
    return str(
        getattr(original, 'sqlstate', None)
        or getattr(original, 'pgcode', None)
        or ''
    ).strip()


def _is_corporation_local_persistence_error(exc: Exception) -> bool:
    """Return whether splitting/retrying can isolate one malformed row."""
    if isinstance(exc, (IntegrityError, DataError)):
        return (
            not bool(getattr(exc, 'connection_invalidated', False))
            and _persistence_sqlstate(exc)
            in _CORPORATION_LOCAL_PERSISTENCE_SQLSTATES
        )
    return isinstance(exc, (KeyError, ValueError))


def _is_systemic_persistence_failure(exc: Exception) -> bool:
    """Return whether persistence is unavailable for the whole run."""
    if isinstance(exc, (OperationalError, InterfaceError)):
        return True
    if isinstance(exc, DBAPIError):
        if not _is_corporation_local_persistence_error(exc):
            return True
        if bool(getattr(exc, 'connection_invalidated', False)):
            return True
        sqlstate = _persistence_sqlstate(exc)
        if sqlstate == '57014':
            return True
        if 'statement timeout' in str(exc).lower():
            return True
    return False


def _normalize_report_name(report_name: str) -> str:
    normalized = report_name.strip()
    while normalized.startswith('[') and ']' in normalized:
        normalized = normalized.split(']', 1)[1].strip()
    return re.sub(r'\s+', '', normalized)


def classify_filing(report_name: str) -> dict[str, Any]:
    normalized = _normalize_report_name(report_name)
    for prefix, category in _FINANCIAL_REPORT_PREFIXES:
        if normalized.startswith(prefix):
            return {
                'filing_category': category,
                'is_financial_report': True,
                'normalized_report_name': normalized,
            }
    return {
        'filing_category': _OTHER_REPORT,
        'is_financial_report': False,
        'normalized_report_name': normalized,
    }


def _parse_receipt_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(f'{value[:4]}-{value[4:6]}-{value[6:8]}')


def _iter_global_disclosure_windows(
    *,
    start_date: date,
    end_date: date,
) -> list[tuple[date, date]]:
    if start_date > end_date:
        raise ValueError('global DART scan start_date must not exceed end_date')
    windows: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        window_end = min(
            cursor + timedelta(days=_GLOBAL_DART_LIST_MAX_WINDOW_DAYS - 1),
            end_date,
        )
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def _normalize_share_issuance_manifest(
    corporations: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    grouped: dict[str, dict[str, Any]] = {}
    requested_stock_codes: list[str] = []
    invalid_items: list[dict[str, Any]] = []
    for index, corporation in enumerate(corporations):
        corp_code = str(corporation.get('corp_code') or '').strip()
        raw_stock_codes = corporation.get('stock_codes')
        stock_values = (
            raw_stock_codes
            if isinstance(raw_stock_codes, list)
            else [corporation.get('stock_code')]
        )
        stock_codes: list[str] = []
        for value in stock_values:
            normalized = str(value or '').strip()
            if not normalized:
                continue
            normalized = normalized.zfill(6) if normalized.isdigit() else normalized
            if normalized not in stock_codes:
                stock_codes.append(normalized)
            if normalized not in requested_stock_codes:
                requested_stock_codes.append(normalized)
        if not corp_code or not stock_codes:
            invalid_items.append(
                {
                    'input_index': index,
                    'corp_code': corp_code or None,
                    'stock_codes': stock_codes,
                    'reason': (
                        'missing_corp_code'
                        if not corp_code
                        else 'missing_stock_code'
                    ),
                }
            )
            continue
        existing = grouped.get(corp_code)
        if existing is None:
            grouped[corp_code] = {
                'corp_code': corp_code,
                'stock_code': stock_codes[0],
                'stock_codes': list(stock_codes),
                'corp_name': str(
                    corporation.get('corp_name') or corp_code
                ).strip(),
            }
            continue
        for stock_code in stock_codes:
            if stock_code not in existing['stock_codes']:
                existing['stock_codes'].append(stock_code)
    manifest = sorted(
        grouped.values(),
        key=lambda item: (item['corp_code'], item['stock_code']),
    )
    corp_codes_by_stock: dict[str, set[str]] = {}
    for item in manifest:
        for stock_code in item['stock_codes']:
            corp_codes_by_stock.setdefault(stock_code, set()).add(
                item['corp_code']
            )
    conflicted_stock_codes = {
        stock_code
        for stock_code, corp_codes in corp_codes_by_stock.items()
        if len(corp_codes) > 1
    }
    if conflicted_stock_codes:
        for stock_code in sorted(conflicted_stock_codes):
            invalid_items.append(
                {
                    'input_index': None,
                    'corp_code': None,
                    'stock_codes': [stock_code],
                    'conflicting_corp_codes': sorted(
                        corp_codes_by_stock[stock_code]
                    ),
                    'reason': 'ambiguous_stock_to_corp_mapping',
                }
            )
        for item in manifest:
            item['stock_codes'] = [
                stock_code
                for stock_code in item['stock_codes']
                if stock_code not in conflicted_stock_codes
            ]
        manifest = [item for item in manifest if item['stock_codes']]
    for item in manifest:
        item['stock_codes'] = sorted(item['stock_codes'])
        item['stock_code'] = item['stock_codes'][0]
    return manifest, sorted(requested_stock_codes), invalid_items


def _persist_share_issuance_scan_evidence(**kwargs: Any) -> dict[str, Any]:
    from app.dart.signals_service import dart_signals_service

    return dart_signals_service.persist_share_issuance_scan_evidence(**kwargs)


def _persist_share_issuance_positive_events(**kwargs: Any) -> dict[str, Any]:
    from app.dart.signals_service import dart_signals_service

    return dart_signals_service.persist_share_issuance_positive_events(**kwargs)


class DartDisclosureSyncService:
    def __init__(
        self,
        *,
        client: DartClient | None = None,
        repository: DartSyncRepository | None = None,
        dilution_evidence_persister: Callable[..., dict[str, Any]] | None = None,
        dilution_positive_persister: Callable[..., dict[str, Any]] | None = None,
        global_scan_lock_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.client = client or dart_client
        self.repository = repository or dart_sync_repository
        self.dilution_evidence_persister = (
            dilution_evidence_persister
            or _persist_share_issuance_scan_evidence
        )
        self.dilution_positive_persister = (
            dilution_positive_persister
            or _persist_share_issuance_positive_events
        )
        self.global_scan_lock_factory = (
            global_scan_lock_factory
            or (lambda **_kwargs: nullcontext(True))
        )

    async def sync_global_share_issuance_evidence(
        self,
        *,
        corporations: list[dict[str, Any]],
        end_date: date,
        trigger: str = 'daily_issuance_global',
    ) -> dict[str, Any]:
        requested_stock_codes = sorted(
            {
                str(item.get('stock_code') or '').strip().zfill(6)
                for item in corporations
                if str(item.get('stock_code') or '').strip()
            }
        )
        with self.global_scan_lock_factory(
            market='KRX',
            trade_date=end_date.isoformat(),
            source='dart_share_issuance',
        ) as acquired:
            if not acquired:
                return {
                    'run_id': None,
                    'status': 'blocked',
                    'scan_status': 'not_started',
                    'reason': 'advisory_lock_unavailable',
                    'window_start': end_date.isoformat(),
                    'window_end': end_date.isoformat(),
                    'scan_completed_stock_codes': [],
                    'coverage_ready_stock_codes': [],
                    'decision_ready_positive_stock_codes': [],
                    'retry_debt_stock_codes': requested_stock_codes,
                }
            return await self._sync_global_share_issuance_evidence_unlocked(
                corporations=corporations,
                end_date=end_date,
                trigger=trigger[:32],
            )

    async def _sync_global_share_issuance_evidence_unlocked(
        self,
        *,
        corporations: list[dict[str, Any]],
        end_date: date,
        trigger: str = 'daily_issuance_global',
    ) -> dict[str, Any]:
        """Scan DART once for the active-corporation manifest.

        The global scan is the daily continuity path. Corporations without a
        complete 365-day baseline remain explicit retry debt for the bounded
        per-corporation backfill; they never expand the global window.
        """
        validate_finalized_krx_write_date(
            end_date,
            operation='DART global share issuance scan',
        )
        from app.dart.signals_service import (
            SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION,
            SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS,
            SHARE_ISSUANCE_RULE_VERSION,
            dart_signals_service,
        )

        (
            normalized_corporations,
            requested_stock_codes,
            invalid_manifest_items,
        ) = _normalize_share_issuance_manifest(corporations)

        if not normalized_corporations:
            manifest_hash = hashlib.sha256(b'[]').hexdigest()
            failure_reason = 'share issuance active-universe manifest is empty'
            run_id = self.repository.create_sync_run(
                window_start=end_date,
                window_end=end_date,
                trigger=trigger[:32],
                page_size=max(min(settings.dart_sync_page_count, 100), 1),
                run_metadata={
                    'scope': 'share_issuance_active_universe_global',
                    'manifest_version': 'dart_share_issuance_manifest_v2',
                    'manifest_hash': manifest_hash,
                    'manifest': [],
                    'requested_input_count': len(corporations),
                    'requested_stock_codes': requested_stock_codes,
                    'invalid_manifest_items': invalid_manifest_items,
                    'failure_reason': failure_reason,
                },
            )
            self.repository.complete_sync_run(
                run_id,
                status='failed',
                pages_fetched=0,
                events_seen=0,
                events_inserted=0,
                events_updated=0,
                financial_report_events=0,
                error_message=failure_reason,
                run_metadata={
                    'scope': 'share_issuance_active_universe_global',
                    'manifest_version': 'dart_share_issuance_manifest_v2',
                    'manifest_hash': manifest_hash,
                    'manifest': [],
                    'requested_input_count': len(corporations),
                    'requested_stock_codes': requested_stock_codes,
                    'invalid_manifest_items': invalid_manifest_items,
                    'failure_reason': failure_reason,
                },
            )
            return {
                'run_id': run_id,
                'status': 'failed',
                'scan_status': 'not_started',
                'window_start': end_date.isoformat(),
                'window_end': end_date.isoformat(),
                'manifest_hash': manifest_hash,
                'corporations_requested': 0,
                'corporations_processed': 0,
                'pages_fetched': 0,
                'events_seen': 0,
                'scan_completed_stock_codes': [],
                'coverage_ready_stock_codes': [],
                'retry_debt_stock_codes': requested_stock_codes,
                'invalid_manifest_items': invalid_manifest_items,
                'error': failure_reason,
            }

        plan = dart_signals_service.plan_global_share_issuance_scan(
            corporations=normalized_corporations,
            window_end=end_date,
        )
        start_date = plan['window_start']
        scan_windows = _iter_global_disclosure_windows(
            start_date=start_date,
            end_date=end_date,
        )
        page_size = max(min(settings.dart_sync_page_count, 100), 1)
        manifest = normalized_corporations
        manifest_hash = hashlib.sha256(
            json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest()
        stock_codes_by_corp = {
            item['corp_code']: list(item['stock_codes']) for item in manifest
        }
        manifest_corp_codes = set(stock_codes_by_corp)
        baseline_required_corp_codes = set(
            plan['baseline_required_corp_codes']
        )
        baseline_required_stock_codes = sorted(
            {
                stock_code
                for corp_code in baseline_required_corp_codes
                for stock_code in stock_codes_by_corp.get(corp_code, [])
            }
        )
        if not invalid_manifest_items:
            try:
                current_coverage_rows = (
                    dart_signals_service.repository.list_current_share_issuance_coverage(
                        corp_codes=sorted(manifest_corp_codes),
                        on_or_before=end_date,
                    )
                )
                latest_positive_dates = (
                    dart_signals_service.repository.list_latest_share_issuance_positive_event_dates(
                        corp_codes=sorted(manifest_corp_codes),
                        on_or_before=end_date,
                    )
                )
            except Exception:
                current_coverage_rows = []
                latest_positive_dates = {}
            current_complete_corp_codes: set[str] = set()
            for coverage_row in current_coverage_rows:
                corp_code = str(
                    coverage_row.get('corp_code') or ''
                ).strip()
                if corp_code not in manifest_corp_codes:
                    continue
                coverage = (
                    dart_signals_service._validated_share_issuance_coverage(
                        coverage_row,
                        corp_code=corp_code,
                    )
                )
                if coverage is None or coverage.get('status') != 'complete':
                    continue
                try:
                    coverage_start = date.fromisoformat(
                        str(coverage.get('window_start') or '')
                    )
                    coverage_end = date.fromisoformat(
                        str(coverage.get('window_end') or '')
                    )
                except ValueError:
                    continue
                required_coverage_start = end_date - timedelta(
                    days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS
                )
                if (
                    coverage_start <= required_coverage_start
                    and coverage_end == end_date
                ):
                    current_complete_corp_codes.add(corp_code)
            recent_positive_corp_codes = {
                corp_code
                for corp_code, positive_date in latest_positive_dates.items()
                if corp_code in manifest_corp_codes
                and isinstance(positive_date, date)
                and positive_date
                >= end_date
                - timedelta(days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS)
            }
            current_ready_corp_codes = (
                current_complete_corp_codes | recent_positive_corp_codes
            )
            list_runs = getattr(self.repository, 'list_sync_runs', None)
            try:
                prior_runs = (
                    list_runs(limit=50) if callable(list_runs) else []
                )
            except Exception:
                prior_runs = []
            for prior_run in prior_runs:
                prior_metadata = dict(
                    prior_run.get('run_metadata') or {}
                )
                if (
                    str(prior_run.get('status') or '').strip().lower()
                    not in {'completed', 'partial'}
                    or prior_metadata.get('scope')
                    != 'share_issuance_active_universe_global'
                    or prior_metadata.get('manifest_version')
                    != 'dart_share_issuance_manifest_v2'
                    or prior_metadata.get('authority_policy_version')
                    != SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
                    or prior_metadata.get('rule_version')
                    != SHARE_ISSUANCE_RULE_VERSION
                    or prior_metadata.get('manifest_hash') != manifest_hash
                    or str(prior_run.get('window_end') or '')
                    != end_date.isoformat()
                    or str(prior_metadata.get('scan_status') or '')
                    != 'completed'
                    or list(prior_metadata.get('failed_stock_codes') or [])
                    or list(
                        prior_metadata.get('unattempted_stock_codes') or []
                    )
                    or list(
                        prior_metadata.get('invalid_manifest_items') or []
                    )
                    or sorted(
                        prior_metadata.get('scan_completed_stock_codes')
                        or []
                    )
                    != requested_stock_codes
                    or sorted(
                        prior_metadata.get('requested_stock_codes') or []
                    )
                    != requested_stock_codes
                    or current_ready_corp_codes != manifest_corp_codes
                ):
                    continue
                return {
                    'run_id': prior_run.get('id'),
                    'status': 'completed',
                    'scan_status': 'completed',
                    'reason': 'exact_completed_global_scan_reused',
                    'reused': True,
                    'window_start': str(prior_run.get('window_start')),
                    'window_end': end_date.isoformat(),
                    'manifest_hash': manifest_hash,
                    'corporations_requested': len(manifest),
                    'corporations_processed': len(manifest),
                    'pages_fetched': int(
                        prior_run.get('pages_fetched') or 0
                    ),
                    'events_seen': int(prior_run.get('events_seen') or 0),
                    'scan_completed_stock_codes': list(
                        prior_metadata.get('scan_completed_stock_codes')
                        or requested_stock_codes
                    ),
                    'coverage_ready_stock_codes': sorted(
                        {
                            stock_code
                            for corp_code in current_complete_corp_codes
                            for stock_code in stock_codes_by_corp.get(
                                corp_code, []
                            )
                        }
                    ),
                    'decision_ready_positive_stock_codes': sorted(
                        {
                            stock_code
                            for corp_code in recent_positive_corp_codes
                            for stock_code in stock_codes_by_corp.get(
                                corp_code, []
                            )
                        }
                    ),
                    'baseline_required_stock_codes': [],
                    'failed_corp_codes': [],
                    'failed_corp_reasons': {},
                    'failed_stock_codes': [],
                    'retry_debt_stock_codes': [],
                    'invalid_manifest_items': [],
                    'evidence': prior_metadata.get('evidence') or {},
                }
        initial_metadata = {
            'scope': 'share_issuance_active_universe_global',
            'manifest_version': 'dart_share_issuance_manifest_v2',
            'authority_policy_version': (
                SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
            ),
            'rule_version': SHARE_ISSUANCE_RULE_VERSION,
            'manifest_hash': manifest_hash,
            'manifest': manifest,
            'requested_input_count': len(corporations),
            'requested_stock_codes': requested_stock_codes,
            'invalid_manifest_items': invalid_manifest_items,
            'requested_corporations': len(manifest),
            'baseline_required_stock_codes': baseline_required_stock_codes,
            'late_arrival_reconciliation': plan[
                'late_arrival_reconciliation'
            ],
            'scan_windows': [
                {
                    'window_start': slice_start.isoformat(),
                    'window_end': slice_end.isoformat(),
                }
                for slice_start, slice_end in scan_windows
            ],
            'scan_completed_stock_codes': [],
            'coverage_ready_stock_codes': [],
            'failed_stock_codes': [],
            'unattempted_stock_codes': sorted(
                {
                    stock_code
                    for item in manifest
                    for stock_code in item['stock_codes']
                }
            ),
            'next_manifest_cursor': 0,
        }
        run_id = self.repository.create_sync_run(
            window_start=start_date,
            window_end=end_date,
            trigger=trigger[:32],
            page_size=page_size,
            run_metadata=initial_metadata,
        )
        pages_fetched = 0
        events_seen = 0
        active_corp_codes = {
            item['corp_code'] for item in normalized_corporations
        }
        active_items: list[dict[str, Any]] = []
        decision_ready_positive_corp_codes: set[str] = set()
        failed_corp_reasons: dict[str, str] = {}
        seen_receipt_numbers: set[str] = set()
        try:
            corporation_by_corp = {
                item['corp_code']: item for item in normalized_corporations
            }
            completed_windows: list[dict[str, Any]] = []
            for slice_start, slice_end in scan_windows:
                pagination_guard = _DisclosurePaginationGuard()
                slice_pages = 0
                page_no = 1
                total_pages = 1
                while page_no <= total_pages:
                    payload = await self.client.fetch_disclosure_list(
                        bgn_de=slice_start.strftime('%Y%m%d'),
                        end_de=slice_end.strftime('%Y%m%d'),
                        page_no=page_no,
                        page_count=page_size,
                        last_reprt_at='N',
                    )
                    pages_fetched += 1
                    slice_pages += 1
                    raw_items, total_pages = pagination_guard.accept_page(
                        payload,
                        requested_page_no=page_no,
                    )
                    events_seen += len(raw_items)
                    active_page_items: dict[str, list[dict[str, Any]]] = {}
                    for raw_item in raw_items:
                        corp_code = str(raw_item.get('corp_code') or '').strip()
                        if not corp_code:
                            raise ValueError(
                                'global share issuance scan item is missing corp_code'
                            )
                        if corp_code not in active_corp_codes:
                            continue
                        try:
                            if not str(raw_item.get('report_nm') or '').strip():
                                raise ValueError(
                                    'global share issuance scan item is missing '
                                    'report_nm'
                                )
                            receipt_date = _parse_receipt_date(
                                raw_item.get('rcept_dt')
                            )
                            if (
                                receipt_date is None
                                or not slice_start <= receipt_date <= slice_end
                            ):
                                raise ValueError(
                                    'global share issuance scan item has an '
                                    'invalid receipt date'
                                )
                            receipt_number = str(
                                raw_item.get('rcept_no') or ''
                            ).strip()
                            if not receipt_number:
                                raise ValueError(
                                    'global share issuance scan item is missing '
                                    'rcept_no'
                                )
                        except ValueError as exc:
                            failed_corp_reasons.setdefault(corp_code, str(exc))
                            continue
                        if receipt_number in seen_receipt_numbers:
                            raise ValueError(
                                'global share issuance scan returned a duplicate '
                                f'rcept_no across date slices: {receipt_number}'
                            )
                        seen_receipt_numbers.add(receipt_number)
                        active_items.append(raw_item)
                        active_page_items.setdefault(corp_code, []).append(
                            raw_item
                        )
                    for corp_code, corp_items in active_page_items.items():
                        try:
                            positive_result = self.dilution_positive_persister(
                                corporation=corporation_by_corp[corp_code],
                                raw_items=corp_items,
                                window_start=slice_start,
                                window_end=slice_end,
                                sync_run_id=run_id,
                            )
                        except Exception as exc:
                            if not _is_corporation_local_persistence_error(exc):
                                raise
                            failed_corp_reasons.setdefault(
                                corp_code,
                                f'positive evidence persistence failed: {exc}',
                            )
                            continue
                        if int(positive_result.get('positive_event_count') or 0) > 0:
                            decision_ready_positive_corp_codes.add(corp_code)
                    page_no += 1
                pagination_guard.assert_complete()
                completed_windows.append(
                    {
                        'window_start': slice_start.isoformat(),
                        'window_end': slice_end.isoformat(),
                        'pages_fetched': slice_pages,
                    }
                )

            active_items_by_corp: dict[str, list[dict[str, Any]]] = {
                corp_code: [] for corp_code in active_corp_codes
            }
            for raw_item in active_items:
                active_items_by_corp[
                    str(raw_item.get('corp_code') or '').strip()
                ].append(raw_item)
            for corp_code, corporation in corporation_by_corp.items():
                if corp_code in failed_corp_reasons:
                    continue
                try:
                    self.dilution_positive_persister(
                        corporation=corporation,
                        raw_items=active_items_by_corp[corp_code],
                        window_start=start_date,
                        window_end=end_date,
                        sync_run_id=run_id,
                        pagination_completed=True,
                    )
                except Exception as exc:
                    if not _is_corporation_local_persistence_error(exc):
                        raise
                    failed_corp_reasons.setdefault(
                        corp_code,
                        f'positive evidence finalization failed: {exc}',
                    )

            evidence_corporations = [
                corporation_by_corp[corp_code]
                for corp_code in active_corp_codes
                if corp_code not in failed_corp_reasons
            ]
            evidence = (
                dart_signals_service.persist_global_share_issuance_scan_evidence(
                    corporations=evidence_corporations,
                    raw_items=[
                        item
                        for item in active_items
                        if str(item.get('corp_code') or '').strip()
                        not in failed_corp_reasons
                    ],
                    window_start=start_date,
                    window_end=end_date,
                    sync_run_id=run_id,
                    positive_events_already_persisted=True,
                )
            )
            for corp_code, reason in dict(
                evidence.get('failed_corp_reasons') or {}
            ).items():
                failed_corp_reasons.setdefault(str(corp_code), str(reason))
            evidence_failed_corp_codes = {
                str(corp_code)
                for corp_code in evidence.get('failed_corp_codes') or []
                if str(corp_code)
            }
            for corp_code in evidence_failed_corp_codes:
                failed_corp_reasons.setdefault(
                    corp_code,
                    'share issuance evidence persistence failed',
                )
            successful_corp_codes = (
                active_corp_codes - set(failed_corp_reasons)
            )
            scan_completed_stock_codes = sorted(
                {
                    stock_code
                    for corp_code in successful_corp_codes
                    for stock_code in stock_codes_by_corp.get(corp_code, [])
                }
            )
            ready_corp_codes = set(
                evidence.get('coverage_ready_corp_codes') or []
            )
            coverage_ready_stock_codes = sorted(
                {
                    stock_code
                    for corp_code in ready_corp_codes
                    for stock_code in stock_codes_by_corp.get(corp_code, [])
                }
            )
            decision_ready_positive_stock_codes = sorted(
                {
                    stock_code
                    for corp_code in decision_ready_positive_corp_codes
                    for stock_code in stock_codes_by_corp.get(corp_code, [])
                }
            )
            failed_stock_codes = sorted(
                {
                    stock_code
                    for corp_code in failed_corp_reasons
                    for stock_code in stock_codes_by_corp.get(corp_code, [])
                }
            )
            baseline_debt_stock_codes = sorted(
                set(scan_completed_stock_codes)
                - set(coverage_ready_stock_codes)
            )
            invalid_stock_codes = sorted(
                {
                    stock_code
                    for item in invalid_manifest_items
                    for stock_code in item.get('stock_codes') or []
                }
            )
            retry_debt_stock_codes = sorted(
                (
                    set(baseline_debt_stock_codes)
                    | set(invalid_stock_codes)
                    | set(failed_stock_codes)
                )
                - set(decision_ready_positive_stock_codes)
            )
            run_status = (
                'partial'
                if (
                    retry_debt_stock_codes
                    or invalid_manifest_items
                    or failed_corp_reasons
                )
                else 'completed'
            )
            final_metadata = {
                **initial_metadata,
                'scan_status': 'completed',
                'completed_windows': completed_windows,
                'scan_completed_stock_codes': scan_completed_stock_codes,
                'coverage_ready_stock_codes': coverage_ready_stock_codes,
                'decision_ready_positive_stock_codes': (
                    decision_ready_positive_stock_codes
                ),
                'baseline_debt_stock_codes': baseline_debt_stock_codes,
                'failed_corp_codes': sorted(failed_corp_reasons),
                'failed_corp_reasons': failed_corp_reasons,
                'failed_stock_codes': failed_stock_codes,
                'retry_debt_stock_codes': retry_debt_stock_codes,
                'unattempted_stock_codes': [],
                'next_manifest_cursor': len(manifest),
                'evidence': evidence,
            }
            self.repository.complete_sync_run(
                run_id,
                status=run_status,
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=0,
                events_updated=0,
                financial_report_events=0,
                run_metadata=final_metadata,
            )
            return {
                'run_id': run_id,
                'status': run_status,
                'scan_status': 'completed',
                'window_start': start_date.isoformat(),
                'window_end': end_date.isoformat(),
                'manifest_hash': manifest_hash,
                'corporations_requested': len(manifest),
                'corporations_processed': len(successful_corp_codes),
                'pages_fetched': pages_fetched,
                'events_seen': events_seen,
                'scan_windows': initial_metadata['scan_windows'],
                'scan_completed_stock_codes': scan_completed_stock_codes,
                'coverage_ready_stock_codes': coverage_ready_stock_codes,
                'decision_ready_positive_stock_codes': (
                    decision_ready_positive_stock_codes
                ),
                'baseline_required_stock_codes': baseline_debt_stock_codes,
                'failed_corp_codes': sorted(failed_corp_reasons),
                'failed_corp_reasons': failed_corp_reasons,
                'failed_stock_codes': failed_stock_codes,
                'retry_debt_stock_codes': retry_debt_stock_codes,
                'invalid_manifest_items': invalid_manifest_items,
                'evidence': evidence,
            }
        except Exception as exc:
            marker_error: str | None = None
            try:
                dart_signals_service.persist_global_share_issuance_incomplete_attempts(
                    corporations=normalized_corporations,
                    window_start=start_date,
                    window_end=end_date,
                    sync_run_id=run_id,
                    failure_reason=str(exc),
                )
            except Exception as marker_exc:
                marker_error = str(marker_exc)
            try:
                latest_positive_dates = (
                    dart_signals_service.repository.list_latest_share_issuance_positive_event_dates(
                        corp_codes=sorted(active_corp_codes),
                        on_or_before=end_date,
                    )
                )
                decision_ready_positive_corp_codes.update(
                    corp_code
                    for corp_code, positive_date in latest_positive_dates.items()
                    if isinstance(positive_date, date)
                    and positive_date
                    >= end_date
                    - timedelta(days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS)
                )
            except Exception:
                # The in-memory set still preserves positive events persisted
                # during this attempt.  A read failure must not hide the
                # original scan error or weaken retry debt.
                pass
            decision_ready_positive_stock_codes = sorted(
                {
                    stock_code
                    for corp_code in decision_ready_positive_corp_codes
                    for stock_code in stock_codes_by_corp.get(corp_code, [])
                }
            )
            failed_stock_codes = sorted(
                {
                    stock_code
                    for item in manifest
                    for stock_code in item['stock_codes']
                }
                | set(requested_stock_codes)
            )
            retry_debt_stock_codes = sorted(
                set(failed_stock_codes)
                - set(decision_ready_positive_stock_codes)
            )
            final_metadata = {
                **initial_metadata,
                'scan_status': 'failed',
                'failed_stock_codes': failed_stock_codes,
                'decision_ready_positive_stock_codes': (
                    decision_ready_positive_stock_codes
                ),
                'retry_debt_stock_codes': retry_debt_stock_codes,
                'unattempted_stock_codes': [],
                'next_manifest_cursor': len(manifest),
                'failure_reason': str(exc),
                'incomplete_marker_error': marker_error,
            }
            self.repository.complete_sync_run(
                run_id,
                status='failed',
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=0,
                events_updated=0,
                financial_report_events=0,
                error_message=str(exc),
                run_metadata=final_metadata,
            )
            return {
                'run_id': run_id,
                'status': 'failed',
                'scan_status': 'failed',
                'window_start': start_date.isoformat(),
                'window_end': end_date.isoformat(),
                'manifest_hash': manifest_hash,
                'corporations_requested': len(manifest),
                'corporations_processed': 0,
                'pages_fetched': pages_fetched,
                'events_seen': events_seen,
                'scan_windows': initial_metadata['scan_windows'],
                'scan_completed_stock_codes': [],
                'coverage_ready_stock_codes': [],
                'decision_ready_positive_stock_codes': (
                    decision_ready_positive_stock_codes
                ),
                'failed_stock_codes': failed_stock_codes,
                'retry_debt_stock_codes': retry_debt_stock_codes,
                'error': str(exc),
                'incomplete_marker_error': marker_error,
            }

    async def sync_recent_disclosures(
        self,
        *,
        days: int | None = None,
        trigger: str = 'manual_api',
    ) -> dict[str, Any]:
        window_days = max(days or settings.dart_sync_recent_days, 1)
        window_end = date.today()
        window_start = window_end - timedelta(days=window_days - 1)
        page_size = max(min(settings.dart_sync_page_count, 100), 1)
        log_fetch_event(
            service='dart',
            operation='sync_recent_disclosures',
            status='started',
            trigger=trigger[:32],
            window_days=window_days,
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            page_size=page_size,
        )
        run_id = self.repository.create_sync_run(
            window_start=window_start,
            window_end=window_end,
            trigger=trigger[:32],
            page_size=page_size,
            run_metadata={'window_days': window_days},
        )

        pages_fetched = 0
        events_seen = 0
        events_inserted = 0
        events_updated = 0
        financial_report_events = 0
        pagination_guard = _DisclosurePaginationGuard()

        try:
            page_no = 1
            total_pages = 1
            while page_no <= total_pages:
                payload = await self.client.fetch_disclosure_list(
                    bgn_de=window_start.strftime('%Y%m%d'),
                    end_de=window_end.strftime('%Y%m%d'),
                    page_no=page_no,
                    page_count=page_size,
                    last_reprt_at='N',
                )
                pages_fetched += 1
                raw_items, total_pages = pagination_guard.accept_page(
                    payload,
                    requested_page_no=page_no,
                )
                total_count = pagination_guard.expected_total_count or 0
                events_seen += len(raw_items)

                page_items = []
                for raw_item in raw_items:
                    rcept_no = (raw_item.get('rcept_no') or '').strip()
                    event_record = self._build_event_record(raw_item, run_id=run_id)
                    if event_record['is_financial_report']:
                        page_items.append(event_record)

                counts = self.repository.upsert_events(page_items, run_id=run_id)
                log_fetch_event(
                    service='dart',
                    operation='sync_disclosure_page',
                    status='completed',
                    run_id=run_id,
                    page_no=page_no,
                    page_size=page_size,
                    total_pages=total_pages,
                    payload_total_count=total_count,
                    raw_item_count=len(raw_items),
                    stored_item_count=len(page_items),
                    events_seen=events_seen,
                    events_inserted=counts['inserted'],
                    events_updated=counts['updated'],
                    financial_report_events=counts['financial_report_events'],
                )
                events_inserted += counts['inserted']
                events_updated += counts['updated']
                financial_report_events += counts['financial_report_events']
                page_no += 1

            pagination_guard.assert_complete()
            result = {
                'run_id': run_id,
                'status': 'completed',
                'window_start': window_start.isoformat(),
                'window_end': window_end.isoformat(),
                'page_size': page_size,
                'pages_fetched': pages_fetched,
                'events_seen': events_seen,
                'events_inserted': events_inserted,
                'events_updated': events_updated,
                'financial_report_events': financial_report_events,
            }
            log_fetch_event(
                service='dart',
                operation='sync_recent_disclosures',
                status='completed',
                **{key: value for key, value in result.items() if key != 'status'},
            )
            self.repository.complete_sync_run(
                run_id,
                status='completed',
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=events_inserted,
                events_updated=events_updated,
                financial_report_events=financial_report_events,
                run_metadata={'window_days': window_days},
            )
            return result
        except Exception as exc:
            log_fetch_event(
                service='dart',
                operation='sync_recent_disclosures',
                status='failed',
                run_id=run_id,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                page_size=page_size,
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=events_inserted,
                events_updated=events_updated,
                financial_report_events=financial_report_events,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            self.repository.complete_sync_run(
                run_id,
                status='failed',
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=events_inserted,
                events_updated=events_updated,
                financial_report_events=financial_report_events,
                error_message=str(exc),
                run_metadata={'window_days': window_days},
            )
            raise

    async def sync_disclosures_for_corporations(
        self,
        *,
        corporations: list[dict[str, str]],
        start_date: date,
        end_date: date,
        trigger: str = 'manual_active_universe_backfill',
        persist_negative_authority: bool = True,
    ) -> dict[str, Any]:
        requested_stock_codes = sorted(
            {
                str(item.get('stock_code') or '').strip().zfill(6)
                for item in corporations
                if str(item.get('stock_code') or '').strip()
            }
        )
        if not persist_negative_authority:
            return await self._sync_disclosures_for_corporations_unlocked(
                corporations=corporations,
                start_date=start_date,
                end_date=end_date,
                trigger=trigger,
                persist_negative_authority=False,
            )
        with self.global_scan_lock_factory(
            market='KRX',
            trade_date=end_date.isoformat(),
            source='dart_share_issuance',
        ) as acquired:
            if not acquired:
                return {
                    'run_id': None,
                    'status': 'blocked',
                    'reason': 'advisory_lock_unavailable',
                    'window_start': start_date.isoformat(),
                    'window_end': end_date.isoformat(),
                    'corporations_requested': len(corporations),
                    'corporations_attempted': 0,
                    'corporations_processed': 0,
                    'corporations_failed': 0,
                    'corporations_unattempted': len(corporations),
                    'successful_stock_codes': [],
                    'failed_stock_codes': [],
                    'unattempted_stock_codes': requested_stock_codes,
                    'incomplete_stock_codes': requested_stock_codes,
                    'persist_negative_authority': True,
                }
            return await self._sync_disclosures_for_corporations_unlocked(
                corporations=corporations,
                start_date=start_date,
                end_date=end_date,
                trigger=trigger,
                persist_negative_authority=True,
            )

    async def _sync_disclosures_for_corporations_unlocked(
        self,
        *,
        corporations: list[dict[str, str]],
        start_date: date,
        end_date: date,
        trigger: str = 'manual_active_universe_backfill',
        persist_negative_authority: bool = True,
    ) -> dict[str, Any]:
        validate_finalized_krx_write_date(
            end_date,
            operation='DART per-corporation disclosure scan',
        )
        requested_manifest = [
            {
                'corp_code': str(item.get('corp_code') or '').strip(),
                'stock_code': str(item.get('stock_code') or '').strip().zfill(6),
            }
            for item in corporations
        ]
        requested_manifest_hash = hashlib.sha256(
            json.dumps(
                requested_manifest,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest()
        page_size = max(min(settings.dart_sync_page_count, 100), 1)
        run_id = self.repository.create_sync_run(
            window_start=start_date,
            window_end=end_date,
            trigger=trigger[:32],
            page_size=page_size,
            run_metadata={
                'scope': 'current_active_universe',
                'requested_corporations': len(corporations),
                'manifest_version': 'dart_per_corp_scan_manifest_v1',
                'manifest_hash': requested_manifest_hash,
                'requested_manifest': requested_manifest,
                'persist_negative_authority': persist_negative_authority,
            },
        )

        pages_fetched = 0
        events_seen = 0
        events_inserted = 0
        events_updated = 0
        financial_report_events = 0
        dilution_positive_events = 0
        dilution_negative_coverage_rows = 0
        dilution_negative_segment_rows = 0
        dilution_incomplete_attempt_rows = 0
        dilution_evidence_rows_upserted = 0
        corporations_processed = 0
        corporations_failed = 0
        corporation_errors: list[dict[str, str]] = []
        incomplete_marker_errors: list[dict[str, str]] = []
        consecutive_provider_failures = 0
        circuit_breaker_tripped = False
        unattempted_corporations: list[dict[str, str]] = []

        try:
            for corporation_index, corporation in enumerate(corporations):
                pagination_guard = _DisclosurePaginationGuard()
                corporation_raw_items: list[dict[str, Any]] = []
                try:
                    page_no = 1
                    total_pages = 1
                    while page_no <= total_pages:
                        payload = await self.client.fetch_disclosure_list(
                            corp_code=corporation['corp_code'],
                            bgn_de=start_date.strftime('%Y%m%d'),
                            end_de=end_date.strftime('%Y%m%d'),
                            page_no=page_no,
                            page_count=page_size,
                            last_reprt_at='N',
                        )
                        pages_fetched += 1
                        raw_items, total_pages = pagination_guard.accept_page(
                            payload,
                            requested_page_no=page_no,
                        )
                        events_seen += len(raw_items)
                        corporation_raw_items.extend(raw_items)
                        positive_evidence = self.dilution_positive_persister(
                            corporation=corporation,
                            raw_items=raw_items,
                            window_start=start_date,
                            window_end=end_date,
                            sync_run_id=run_id,
                        )
                        dilution_positive_events += int(
                            positive_evidence.get('positive_event_count') or 0
                        )
                        dilution_evidence_rows_upserted += int(
                            positive_evidence.get('rows_upserted') or 0
                        )
                        page_items = []
                        for raw_item in raw_items:
                            event_record = self._build_event_record(raw_item, run_id=run_id)
                            if event_record['is_financial_report']:
                                page_items.append(event_record)

                        counts = self.repository.upsert_events(page_items, run_id=run_id)
                        events_inserted += counts['inserted']
                        events_updated += counts['updated']
                        financial_report_events += counts['financial_report_events']
                        page_no += 1

                    pagination_guard.assert_complete()
                    self.dilution_positive_persister(
                        corporation=corporation,
                        raw_items=corporation_raw_items,
                        window_start=start_date,
                        window_end=end_date,
                        sync_run_id=run_id,
                        pagination_completed=True,
                    )
                    if persist_negative_authority:
                        dilution_evidence = self.dilution_evidence_persister(
                            corporation=corporation,
                            raw_items=corporation_raw_items,
                            window_start=start_date,
                            window_end=end_date,
                            pagination_completed=True,
                            sync_run_id=run_id,
                            positive_events_already_persisted=True,
                        )
                        dilution_negative_coverage_rows += int(
                            dilution_evidence.get('negative_coverage_count') or 0
                        )
                        dilution_negative_segment_rows += int(
                            dilution_evidence.get('negative_segment_count') or 0
                        )
                        dilution_evidence_rows_upserted += int(
                            dilution_evidence.get('rows_upserted') or 0
                        )
                    corporations_processed += 1
                    consecutive_provider_failures = 0
                except Exception as exc:
                    systemic_persistence_failure = (
                        _is_systemic_persistence_failure(exc)
                    )
                    if persist_negative_authority and not systemic_persistence_failure:
                        try:
                            incomplete_evidence = self.dilution_evidence_persister(
                                corporation=corporation,
                                raw_items=[],
                                window_start=start_date,
                                window_end=end_date,
                                pagination_completed=False,
                                sync_run_id=run_id,
                                failure_reason=str(exc),
                            )
                            incomplete_rows = int(
                                incomplete_evidence.get('rows_upserted') or 0
                            )
                            dilution_incomplete_attempt_rows += incomplete_rows
                            dilution_evidence_rows_upserted += incomplete_rows
                        except Exception as marker_exc:
                            incomplete_marker_errors.append(
                                {
                                    'corp_code': corporation['corp_code'],
                                    'stock_code': corporation.get('stock_code', ''),
                                    'error': str(marker_exc),
                                }
                            )
                    corporations_failed += 1
                    corporation_errors.append(
                        {
                            'corp_code': corporation['corp_code'],
                            'stock_code': corporation.get('stock_code', ''),
                            'error': str(exc),
                        }
                    )
                    if systemic_persistence_failure:
                        circuit_breaker_tripped = True
                        unattempted_corporations = corporations[
                            corporation_index + 1 :
                        ]
                        break
                    if _is_systemic_provider_failure(exc):
                        consecutive_provider_failures += 1
                    else:
                        consecutive_provider_failures = 0
                    if consecutive_provider_failures >= _CONSECUTIVE_PROVIDER_FAILURE_LIMIT:
                        circuit_breaker_tripped = True
                        unattempted_corporations = corporations[corporation_index + 1 :]
                        break

            failed_stock_codes = [
                item['stock_code']
                for item in corporation_errors
                if item.get('stock_code')
            ]
            unattempted_stock_codes = [
                str(item.get('stock_code') or '').strip()
                for item in unattempted_corporations
                if str(item.get('stock_code') or '').strip()
            ]
            incomplete_stock_codes = list(
                dict.fromkeys([*failed_stock_codes, *unattempted_stock_codes])
            )
            corporations_attempted = corporations_processed + corporations_failed
            corporations_incomplete = corporations_failed + len(unattempted_corporations)
            successful_stock_codes = sorted(
                {
                    str(item.get('stock_code') or '').strip().zfill(6)
                    for item in corporations
                    if str(item.get('stock_code') or '').strip()
                }
                - set(incomplete_stock_codes)
            )
            run_metadata = {
                'scope': 'current_active_universe',
                'requested_corporations': len(corporations),
                'manifest_version': 'dart_per_corp_scan_manifest_v1',
                'manifest_hash': requested_manifest_hash,
                'requested_manifest': requested_manifest,
                'persist_negative_authority': persist_negative_authority,
                'corporations_attempted': corporations_attempted,
                'corporations_processed': corporations_processed,
                'corporations_failed': corporations_failed,
                'corporations_unattempted': len(unattempted_corporations),
                'dilution_positive_events': dilution_positive_events,
                'dilution_negative_coverage_rows': (
                    dilution_negative_coverage_rows
                ),
                'dilution_negative_segment_rows': (
                    dilution_negative_segment_rows
                ),
                'dilution_incomplete_attempt_rows': (
                    dilution_incomplete_attempt_rows
                ),
                'dilution_evidence_rows_upserted': (
                    dilution_evidence_rows_upserted
                ),
                'corporation_errors': corporation_errors[:25],
                'incomplete_marker_errors': incomplete_marker_errors,
                'successful_stock_codes': successful_stock_codes,
                'failed_stock_codes': failed_stock_codes,
                'unattempted_stock_codes': unattempted_stock_codes,
                'incomplete_stock_codes': incomplete_stock_codes,
                'circuit_breaker': {
                    'tripped': circuit_breaker_tripped,
                    'threshold': _CONSECUTIVE_PROVIDER_FAILURE_LIMIT,
                    'consecutive_provider_failures': consecutive_provider_failures,
                },
            }
            if corporations and corporations_processed == 0 and corporations_incomplete:
                status = 'failed'
            elif corporations_incomplete:
                status = 'partial'
            else:
                status = 'completed'
            result = {
                'run_id': run_id,
                'status': status,
                'window_start': start_date.isoformat(),
                'window_end': end_date.isoformat(),
                'page_size': page_size,
                'pages_fetched': pages_fetched,
                'events_seen': events_seen,
                'events_inserted': events_inserted,
                'events_updated': events_updated,
                'financial_report_events': financial_report_events,
                'dilution_positive_events': dilution_positive_events,
                'dilution_negative_coverage_rows': (
                    dilution_negative_coverage_rows
                ),
                'dilution_negative_segment_rows': (
                    dilution_negative_segment_rows
                ),
                'dilution_incomplete_attempt_rows': (
                    dilution_incomplete_attempt_rows
                ),
                'dilution_evidence_rows_upserted': (
                    dilution_evidence_rows_upserted
                ),
                'corporations_requested': len(corporations),
                'corporations_attempted': corporations_attempted,
                'corporations_processed': corporations_processed,
                'corporations_failed': corporations_failed,
                'corporations_unattempted': len(unattempted_corporations),
                'corporation_errors': corporation_errors[:25],
                'incomplete_marker_errors': incomplete_marker_errors,
                'successful_stock_codes': successful_stock_codes,
                'failed_stock_codes': failed_stock_codes,
                'unattempted_stock_codes': unattempted_stock_codes,
                'incomplete_stock_codes': incomplete_stock_codes,
                'circuit_breaker': run_metadata['circuit_breaker'],
            }
            self.repository.complete_sync_run(
                run_id,
                status=status,
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=events_inserted,
                events_updated=events_updated,
                financial_report_events=financial_report_events,
                run_metadata=run_metadata,
            )
            return result
        except Exception as exc:
            self.repository.complete_sync_run(
                run_id,
                status='failed',
                pages_fetched=pages_fetched,
                events_seen=events_seen,
                events_inserted=events_inserted,
                events_updated=events_updated,
                financial_report_events=financial_report_events,
                error_message=str(exc),
                run_metadata={
                    'scope': 'current_active_universe',
                    'requested_corporations': len(corporations),
                    'corporations_processed': corporations_processed,
                    'corporations_failed': corporations_failed,
                    'corporation_errors': corporation_errors[:25],
                },
            )
            raise

    def inspect_sync(self, *, limit: int = 10, events_limit: int = 25, financial_only: bool = False) -> dict[str, Any]:
        return {
            'latest_run': self.repository.get_latest_sync_run(),
            'runs': self.repository.list_sync_runs(limit=limit),
            'summary': self.repository.get_summary(),
            'events': self.repository.list_events(limit=events_limit, financial_only=financial_only),
        }

    def _build_event_record(self, raw_item: dict[str, Any], *, run_id: int) -> dict[str, Any]:
        classification = classify_filing(raw_item.get('report_nm', ''))
        stock_code = (raw_item.get('stock_code') or '').strip() or None
        return {
            'rcept_no': raw_item['rcept_no'].strip(),
            'corp_code': (raw_item.get('corp_code') or '').strip(),
            'corp_name': (raw_item.get('corp_name') or '').strip(),
            'stock_code': stock_code.zfill(6) if stock_code else None,
            'corp_cls': (raw_item.get('corp_cls') or '').strip() or None,
            'report_nm': (raw_item.get('report_nm') or '').strip(),
            'filing_category': classification['filing_category'],
            'is_financial_report': classification['is_financial_report'],
            'filer_name': (raw_item.get('flr_nm') or '').strip() or None,
            'receipt_date': _parse_receipt_date(raw_item.get('rcept_dt')),
            'remark': (raw_item.get('rm') or '').strip() or None,
            'payload': raw_item,
            'first_seen_run_id': run_id,
            'last_seen_run_id': run_id,
            'fundamentals_capture_status': 'pending' if classification['is_financial_report'] else 'not_applicable',
            'fundamentals_capture_attempts': 0,
            'fundamentals_last_attempt_at': None,
            'fundamentals_last_success_at': None,
            'fundamentals_last_error': None,
        }


dart_disclosure_sync_service = DartDisclosureSyncService(
    global_scan_lock_factory=advisory_eod_collection_lock,
)
