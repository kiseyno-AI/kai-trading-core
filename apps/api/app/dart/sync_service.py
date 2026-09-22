from __future__ import annotations

import math
import re
from datetime import date, timedelta
from typing import Any

import httpx

from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.dart.store import DartSyncRepository, dart_sync_repository
from app.integrations.dart.client import DartAPIError, DartClient, dart_client

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


def _is_systemic_provider_failure(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError, httpx.TransportError)):
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


class DartDisclosureSyncService:
    def __init__(
        self,
        *,
        client: DartClient | None = None,
        repository: DartSyncRepository | None = None,
    ) -> None:
        self.client = client or dart_client
        self.repository = repository or dart_sync_repository

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
            trigger=trigger,
            window_days=window_days,
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            page_size=page_size,
        )
        run_id = self.repository.create_sync_run(
            window_start=window_start,
            window_end=window_end,
            trigger=trigger,
            page_size=page_size,
            run_metadata={'window_days': window_days},
        )

        pages_fetched = 0
        events_seen = 0
        events_inserted = 0
        events_updated = 0
        financial_report_events = 0
        processed_rcept_nos: set[str] = set()

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
                total_count = int(payload.get('total_count', '0') or 0)
                total_pages = max(math.ceil(total_count / page_size), 1) if total_count else 1

                raw_items = payload.get('list', [])
                events_seen += len(raw_items)

                page_items = []
                for raw_item in raw_items:
                    rcept_no = (raw_item.get('rcept_no') or '').strip()
                    if not rcept_no or rcept_no in processed_rcept_nos:
                        continue
                    processed_rcept_nos.add(rcept_no)
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
    ) -> dict[str, Any]:
        page_size = max(min(settings.dart_sync_page_count, 100), 1)
        run_id = self.repository.create_sync_run(
            window_start=start_date,
            window_end=end_date,
            trigger=trigger,
            page_size=page_size,
            run_metadata={
                'scope': 'current_active_universe',
                'requested_corporations': len(corporations),
            },
        )

        pages_fetched = 0
        events_seen = 0
        events_inserted = 0
        events_updated = 0
        financial_report_events = 0
        corporations_processed = 0
        corporations_failed = 0
        corporation_errors: list[dict[str, str]] = []
        consecutive_provider_failures = 0
        circuit_breaker_tripped = False
        unattempted_corporations: list[dict[str, str]] = []

        try:
            for corporation_index, corporation in enumerate(corporations):
                processed_rcept_nos: set[str] = set()
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
                        total_count = int(payload.get('total_count', '0') or 0)
                        total_pages = max(math.ceil(total_count / page_size), 1) if total_count else 1

                        raw_items = payload.get('list', [])
                        events_seen += len(raw_items)
                        page_items = []
                        for raw_item in raw_items:
                            rcept_no = (raw_item.get('rcept_no') or '').strip()
                            if not rcept_no or rcept_no in processed_rcept_nos:
                                continue
                            processed_rcept_nos.add(rcept_no)
                            event_record = self._build_event_record(raw_item, run_id=run_id)
                            if event_record['is_financial_report']:
                                page_items.append(event_record)

                        counts = self.repository.upsert_events(page_items, run_id=run_id)
                        events_inserted += counts['inserted']
                        events_updated += counts['updated']
                        financial_report_events += counts['financial_report_events']
                        page_no += 1

                    corporations_processed += 1
                    consecutive_provider_failures = 0
                except Exception as exc:
                    corporations_failed += 1
                    corporation_errors.append(
                        {
                            'corp_code': corporation['corp_code'],
                            'stock_code': corporation.get('stock_code', ''),
                            'error': str(exc),
                        }
                    )
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
            run_metadata = {
                'scope': 'current_active_universe',
                'requested_corporations': len(corporations),
                'corporations_attempted': corporations_attempted,
                'corporations_processed': corporations_processed,
                'corporations_failed': corporations_failed,
                'corporations_unattempted': len(unattempted_corporations),
                'corporation_errors': corporation_errors[:25],
                'failed_stock_codes_sample': failed_stock_codes[:100],
                'unattempted_stock_codes_sample': unattempted_stock_codes[:100],
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
                'corporations_requested': len(corporations),
                'corporations_attempted': corporations_attempted,
                'corporations_processed': corporations_processed,
                'corporations_failed': corporations_failed,
                'corporations_unattempted': len(unattempted_corporations),
                'corporation_errors': corporation_errors[:25],
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


dart_disclosure_sync_service = DartDisclosureSyncService()
