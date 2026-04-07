from __future__ import annotations

import math
import re
from datetime import date, timedelta
from typing import Any

from app.core.settings import settings
from app.dart.store import DartSyncRepository, dart_sync_repository
from app.integrations.dart.client import DartClient, dart_client

_ANNUAL_REPORT = 'annual_report'
_SEMIANNUAL_REPORT = 'semiannual_report'
_QUARTERLY_REPORT = 'quarterly_report'
_OTHER_REPORT = 'other'

_FINANCIAL_REPORT_PREFIXES = (
    ('사업보고서', _ANNUAL_REPORT),
    ('반기보고서', _SEMIANNUAL_REPORT),
    ('분기보고서', _QUARTERLY_REPORT),
)


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

                page_items = []
                for raw_item in payload.get('list', []):
                    rcept_no = (raw_item.get('rcept_no') or '').strip()
                    if not rcept_no or rcept_no in processed_rcept_nos:
                        continue
                    processed_rcept_nos.add(rcept_no)
                    page_items.append(self._build_event_record(raw_item, run_id=run_id))

                counts = self.repository.upsert_events(page_items, run_id=run_id)
                events_seen += counts['seen']
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
        }


dart_disclosure_sync_service = DartDisclosureSyncService()
