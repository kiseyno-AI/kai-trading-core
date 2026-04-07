from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.db import session_scope
from app.db_models import DailyPriceBar
from app.dart.fundamentals_store import DartFundamentalsRepository, dart_fundamentals_repository
from app.dart.service import (
    FS_DIV_TO_STATEMENT_TYPE,
    REPORT_CODE_METADATA,
    build_fundamentals_dataset,
)
from app.integrations.dart.client import DartClient, dart_client
from app.market_data_store import load_symbols
from sqlalchemy import func, select

_REPORT_PERIOD_PATTERN = re.compile(r'\((\d{4})\.(\d{2})\)')

_FILING_CATEGORY_TO_REPORT_CODE = {
    'annual_report': '11011',
    'semiannual_report': '11012',
}

DART_FUNDAMENTALS_MIN_START_DATE = date(settings.dart_fundamentals_min_business_year, 1, 1)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def infer_fundamentals_request(
    event: dict[str, Any],
    *,
    fs_div: str,
    fiscal_year_end_month: int | str | None = None,
) -> dict[str, Any]:
    filing_category = event['filing_category']
    report_name = event['report_nm']
    period_match = _REPORT_PERIOD_PATTERN.search(report_name)
    if period_match is None:
        raise ValueError(f'Unable to infer report period from disclosure name: {report_name}')

    business_year = int(period_match.group(1))
    period_end_month = int(period_match.group(2))

    if filing_category == 'quarterly_report':
        report_code = _infer_quarterly_report_code(
            period_end_month=period_end_month,
            fiscal_year_end_month=fiscal_year_end_month,
        )
    else:
        report_code = _FILING_CATEGORY_TO_REPORT_CODE.get(filing_category)
        if report_code is None:
            raise ValueError(f'Unsupported filing category for fundamentals capture: {filing_category}')

    return {
        'business_year': business_year,
        'report_code': report_code,
        'period_end_month': period_end_month,
        'period_type': REPORT_CODE_METADATA[report_code]['period'],
        'statement_type': FS_DIV_TO_STATEMENT_TYPE[fs_div],
        'fs_div': fs_div,
    }


def _infer_quarterly_report_code(
    *,
    period_end_month: int,
    fiscal_year_end_month: int | str | None,
) -> str:
    normalized_fiscal_year_end_month = _normalize_month(fiscal_year_end_month)
    if normalized_fiscal_year_end_month is None:
        raise ValueError('Quarterly fundamentals capture requires a fiscal year-end month from DART company overview')

    first_quarter_end_month = _month_after(normalized_fiscal_year_end_month, 3)
    third_quarter_end_month = _month_after(normalized_fiscal_year_end_month, 9)

    if period_end_month == first_quarter_end_month:
        return '11013'
    if period_end_month == third_quarter_end_month:
        return '11014'

    raise ValueError(
        'Quarterly period month does not match the company fiscal calendar: '
        f'period_end_month={period_end_month:02d}, fiscal_year_end_month={normalized_fiscal_year_end_month:02d}'
    )


def _normalize_month(value: int | str | None) -> int | None:
    if value in (None, ''):
        return None
    month = int(value)
    if month < 1 or month > 12:
        raise ValueError(f'Invalid fiscal year-end month: {value}')
    return month


def _month_after(base_month: int, months_forward: int) -> int:
    return ((base_month - 1 + months_forward) % 12) + 1


def build_row_payloads(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payloads = []
    row_key_counts: dict[str, int] = {}
    for item in items:
        row_key = _build_row_key(item, row_key_counts=row_key_counts)
        payloads.append(
            {
                'row_key': row_key,
                'receipt_number': item.get('receipt_number'),
                'report_code': item.get('report_code'),
                'business_year': item.get('business_year'),
                'statement_type': item.get('statement_type'),
                'statement_name': item.get('statement_name'),
                'account_id': item.get('account_id'),
                'account_name': item.get('account_name'),
                'account_detail': item.get('account_detail'),
                'current_period_name': item.get('current_period_name'),
                'current_amount': item.get('current_amount'),
                'current_cumulative_amount': item.get('current_cumulative_amount'),
                'prior_period_name': item.get('prior_period_name'),
                'prior_amount': item.get('prior_amount'),
                'prior_quarter_name': item.get('prior_quarter_name'),
                'prior_quarter_amount': item.get('prior_quarter_amount'),
                'prior_cumulative_amount': item.get('prior_cumulative_amount'),
                'pre_prior_period_name': item.get('pre_prior_period_name'),
                'pre_prior_amount': item.get('pre_prior_amount'),
                'row_order': item.get('order'),
                'currency': item.get('currency'),
                'payload': item['raw'],
            }
        )
    return payloads


def _build_row_key(item: dict[str, Any], *, row_key_counts: dict[str, int]) -> str:
    identity = (
        item.get('statement_type') or '',
        item.get('statement_name') or '',
        item.get('account_id') or '',
        item.get('account_name') or '',
        item.get('account_detail') or '',
        item.get('current_period_name') or '',
        item.get('prior_period_name') or '',
        item.get('prior_quarter_name') or '',
        item.get('pre_prior_period_name') or '',
        str(item.get('order') or ''),
    )
    digest = hashlib.sha1('|'.join(identity).encode('utf-8')).hexdigest()
    occurrence = row_key_counts.get(digest, 0) + 1
    row_key_counts[digest] = occurrence
    if occurrence == 1:
        return digest
    return f'{digest}:{occurrence}'


def build_highlight_payloads(highlights: dict[str, Any]) -> list[dict[str, Any]]:
    payloads = []
    for metric_key, metric in highlights.items():
        if metric_key == 'ratios':
            for ratio_key, ratio_value in metric.items():
                payloads.append(
                    {
                        'metric_key': ratio_key,
                        'metric_type': 'ratio',
                        'statement_type': None,
                        'statement_name': None,
                        'account_id': None,
                        'account_name': None,
                        'current_amount': None,
                        'current_cumulative_amount': None,
                        'prior_amount': None,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': None,
                        'ratio_value': _parse_decimal(ratio_value),
                    }
                )
            continue

        payloads.append(
            {
                'metric_key': metric_key,
                'metric_type': 'account',
                'statement_type': metric.get('statement_type') if metric else None,
                'statement_name': metric.get('statement_name') if metric else None,
                'account_id': metric.get('account_id') if metric else None,
                'account_name': metric.get('account_name') if metric else None,
                'current_amount': metric.get('current_amount') if metric else None,
                'current_cumulative_amount': metric.get('current_cumulative_amount') if metric else None,
                'prior_amount': metric.get('prior_amount') if metric else None,
                'prior_cumulative_amount': metric.get('prior_cumulative_amount') if metric else None,
                'pre_prior_amount': metric.get('pre_prior_amount') if metric else None,
                'currency': metric.get('currency') if metric else None,
                'ratio_value': None,
            }
        )
    return payloads


def _parse_decimal(value: str | None) -> Decimal | None:
    if value in (None, ''):
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def _load_active_symbols() -> list[dict[str, Any]]:
    return [item for item in load_symbols(is_active=True) if (item.get('symbol') or '').strip().isdigit()]


def _load_daily_price_bar_bounds() -> tuple[date | None, date | None]:
    with session_scope() as session:
        return (
            session.execute(select(func.min(DailyPriceBar.trade_date))).scalar_one(),
            session.execute(select(func.max(DailyPriceBar.trade_date))).scalar_one(),
        )


class DartFundamentalsCaptureService:
    def __init__(
        self,
        *,
        client: DartClient | None = None,
        repository: DartFundamentalsRepository | None = None,
        active_symbol_loader: Callable[[], list[dict[str, Any]]] | None = None,
        daily_price_bar_bounds_loader: Callable[[], tuple[date | None, date | None]] | None = None,
    ) -> None:
        self.client = client or dart_client
        self.repository = repository or dart_fundamentals_repository
        self.active_symbol_loader = active_symbol_loader or _load_active_symbols
        self.daily_price_bar_bounds_loader = daily_price_bar_bounds_loader or _load_daily_price_bar_bounds
        self.event_timeout_seconds = settings.dart_fundamentals_event_timeout_seconds
        self.stale_run_minutes = settings.dart_fundamentals_stale_run_minutes

    async def capture_recent_fundamentals(
        self,
        *,
        limit: int | None = 20,
        force: bool = False,
        trigger: str = 'manual_api',
        fs_div: str | None = None,
        stock_codes: list[str] | None = None,
        receipt_date_from: date | None = None,
        receipt_date_to: date | None = None,
    ) -> dict[str, Any]:
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        run_metadata: dict[str, Any] = {'requested_limit': limit}
        if stock_codes:
            run_metadata['stock_code_count'] = len(stock_codes)
        if receipt_date_from:
            run_metadata['receipt_date_from'] = receipt_date_from.isoformat()
        if receipt_date_to:
            run_metadata['receipt_date_to'] = receipt_date_to.isoformat()
        log_fetch_event(
            service='dart',
            operation='capture_recent_fundamentals',
            status='started',
            limit=limit,
            force=force,
            trigger=trigger,
            fs_div=resolved_fs_div,
        )
        stale_run_ids = self.repository.fail_stale_capture_runs(
            stale_before=_utcnow() - timedelta(minutes=self.stale_run_minutes),
            error_message=(
                'Marked failed after exceeding the stale running threshold before a new capture run started'
            ),
        )
        if stale_run_ids:
            log_fetch_event(
                service='dart',
                operation='capture_recent_fundamentals',
                status='stale_runs_failed',
                stale_run_ids=stale_run_ids,
                stale_run_count=len(stale_run_ids),
                stale_run_minutes=self.stale_run_minutes,
            )
        run_id = self.repository.create_capture_run(
            trigger=trigger,
            limit_requested=limit,
            force=force,
            fs_div=resolved_fs_div,
            run_metadata=run_metadata,
        )
        events_seen = 0
        events_captured = 0
        events_failed = 0
        events_skipped = 0
        results: list[dict[str, Any]] = []

        try:
            events = self.repository.list_capture_candidates(
                limit=limit,
                force=force,
                stock_codes=stock_codes,
                receipt_date_from=receipt_date_from,
                receipt_date_to=receipt_date_to,
            )
            run_metadata_with_progress = {**run_metadata, 'candidate_count': len(events)}
            self.repository.update_capture_run_progress(
                run_id,
                events_seen=events_seen,
                events_captured=events_captured,
                events_failed=events_failed,
                events_skipped=events_skipped,
                run_metadata=run_metadata_with_progress,
            )
            for event in events:
                events_seen += 1
                run_metadata_with_progress = {
                    **run_metadata,
                    'candidate_count': len(events),
                    'current_event': {
                        'event_id': event['id'],
                        'rcept_no': event['rcept_no'],
                        'corp_code': event['corp_code'],
                        'stock_code': event['stock_code'],
                        'report_nm': event['report_nm'],
                    },
                    'last_progress_at': _utcnow().isoformat(),
                }
                self.repository.update_capture_run_progress(
                    run_id,
                    events_seen=events_seen,
                    events_captured=events_captured,
                    events_failed=events_failed,
                    events_skipped=events_skipped,
                    run_metadata=run_metadata_with_progress,
                )
                log_fetch_event(
                    service='dart',
                    operation='capture_fundamentals_event',
                    status='started',
                    run_id=run_id,
                    event_id=event['id'],
                    rcept_no=event['rcept_no'],
                    corp_code=event['corp_code'],
                    stock_code=event['stock_code'],
                    fs_div=resolved_fs_div,
                    timeout_seconds=self.event_timeout_seconds,
                )
                try:
                    request, dataset = await asyncio.wait_for(
                        self._capture_event(
                            event=event,
                            run_id=run_id,
                            fs_div=resolved_fs_div,
                        ),
                        timeout=self.event_timeout_seconds,
                    )
                    self.repository.mark_event_capture_success(event['id'])
                    events_captured += 1
                    run_metadata_with_progress = {
                        **run_metadata,
                        'candidate_count': len(events),
                        'last_event': {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'captured',
                        },
                        'last_progress_at': _utcnow().isoformat(),
                    }
                    self.repository.update_capture_run_progress(
                        run_id,
                        events_seen=events_seen,
                        events_captured=events_captured,
                        events_failed=events_failed,
                        events_skipped=events_skipped,
                        run_metadata=run_metadata_with_progress,
                    )
                    log_fetch_event(
                        service='dart',
                        operation='capture_fundamentals_event',
                        status='completed',
                        run_id=run_id,
                        event_id=event['id'],
                        rcept_no=event['rcept_no'],
                        corp_code=event['corp_code'],
                        stock_code=event['stock_code'],
                        business_year=request['business_year'],
                        report_code=request['report_code'],
                        fs_div=resolved_fs_div,
                        row_count=len(dataset['items']),
                    )
                    results.append(
                        {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'captured',
                            'business_year': request['business_year'],
                            'report_code': request['report_code'],
                            'fs_div': resolved_fs_div,
                            'row_count': len(dataset['items']),
                        }
                    )
                except asyncio.TimeoutError:
                    timeout_message = (
                        f'Capture timed out after {self.event_timeout_seconds}s '
                        f'for event_id={event["id"]} rcept_no={event["rcept_no"]}'
                    )
                    self.repository.mark_event_capture_failed(event['id'], error_message=timeout_message)
                    events_failed += 1
                    run_metadata_with_progress = {
                        **run_metadata,
                        'candidate_count': len(events),
                        'last_event': {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'failed',
                            'error': timeout_message,
                        },
                        'last_progress_at': _utcnow().isoformat(),
                    }
                    self.repository.update_capture_run_progress(
                        run_id,
                        events_seen=events_seen,
                        events_captured=events_captured,
                        events_failed=events_failed,
                        events_skipped=events_skipped,
                        run_metadata=run_metadata_with_progress,
                    )
                    log_fetch_event(
                        service='dart',
                        operation='capture_fundamentals_event',
                        status='failed',
                        run_id=run_id,
                        event_id=event['id'],
                        rcept_no=event['rcept_no'],
                        corp_code=event['corp_code'],
                        stock_code=event['stock_code'],
                        fs_div=resolved_fs_div,
                        error_type='TimeoutError',
                        error_message=timeout_message,
                    )
                    results.append(
                        {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'failed',
                            'error': timeout_message,
                        }
                    )
                except Exception as exc:
                    self.repository.mark_event_capture_failed(event['id'], error_message=str(exc))
                    events_failed += 1
                    run_metadata_with_progress = {
                        **run_metadata,
                        'candidate_count': len(events),
                        'last_event': {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'failed',
                            'error': str(exc),
                        },
                        'last_progress_at': _utcnow().isoformat(),
                    }
                    self.repository.update_capture_run_progress(
                        run_id,
                        events_seen=events_seen,
                        events_captured=events_captured,
                        events_failed=events_failed,
                        events_skipped=events_skipped,
                        run_metadata=run_metadata_with_progress,
                    )
                    log_fetch_event(
                        service='dart',
                        operation='capture_fundamentals_event',
                        status='failed',
                        run_id=run_id,
                        event_id=event['id'],
                        rcept_no=event['rcept_no'],
                        corp_code=event['corp_code'],
                        stock_code=event['stock_code'],
                        fs_div=resolved_fs_div,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    results.append(
                        {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'failed',
                            'error': str(exc),
                        }
                    )

            result = {
                'run_id': run_id,
                'status': 'completed',
                'fs_div': resolved_fs_div,
                'events_seen': events_seen,
                'events_captured': events_captured,
                'events_failed': events_failed,
                'events_skipped': events_skipped,
                'results': results,
            }
            log_fetch_event(
                service='dart',
                operation='capture_recent_fundamentals',
                status='completed',
                **{key: value for key, value in result.items() if key != 'status'},
            )
            self.repository.complete_capture_run(
                run_id,
                status='completed',
                events_seen=events_seen,
                events_captured=events_captured,
                events_failed=events_failed,
                events_skipped=events_skipped,
                run_metadata={
                    **run_metadata,
                    'candidate_count': len(events),
                    'last_progress_at': _utcnow().isoformat(),
                },
            )
            return result
        except Exception as exc:
            log_fetch_event(
                service='dart',
                operation='capture_recent_fundamentals',
                status='failed',
                run_id=run_id,
                fs_div=resolved_fs_div,
                events_seen=events_seen,
                events_captured=events_captured,
                events_failed=events_failed,
                events_skipped=events_skipped,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            self.repository.complete_capture_run(
                run_id,
                status='failed',
                events_seen=events_seen,
                events_captured=events_captured,
                events_failed=events_failed,
                events_skipped=events_skipped,
                error_message=str(exc),
                run_metadata={
                    **run_metadata,
                    'last_progress_at': _utcnow().isoformat(),
                },
            )
            raise

    async def _capture_event(
        self,
        *,
        event: dict[str, Any],
        run_id: int,
        fs_div: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        company_overview = await self.client.fetch_company_overview(event['corp_code'])
        request = infer_fundamentals_request(
            event,
            fs_div=fs_div,
            fiscal_year_end_month=company_overview.get('acc_mt'),
        )
        source_payload = await self.client.fetch_financial_statements(
            corp_code=event['corp_code'],
            bsns_year=str(request['business_year']),
            reprt_code=request['report_code'],
            fs_div=fs_div,
        )
        dataset = build_fundamentals_dataset(source_payload)
        self.repository.replace_snapshot_dataset(
            event_id=event['id'],
            capture_run_id=run_id,
            snapshot_payload={
                'rcept_no': event['rcept_no'],
                'corp_code': event['corp_code'],
                'stock_code': event['stock_code'],
                'corp_name': event['corp_name'],
                'business_year': request['business_year'],
                'report_code': request['report_code'],
                'filing_category': event['filing_category'],
                'fs_div': fs_div,
                'statement_type': request['statement_type'],
                'period_type': request['period_type'],
                'period_end_month': request['period_end_month'],
                'currency': dataset['currency'],
                'source_status': source_payload.get('status'),
                'source_message': source_payload.get('message'),
                'request_payload': {
                    'corp_code': event['corp_code'],
                    'business_year': request['business_year'],
                    'report_code': request['report_code'],
                    'fs_div': fs_div,
                    'rcept_no': event['rcept_no'],
                    'fiscal_year_end_month': company_overview.get('acc_mt'),
                },
                'response_payload': source_payload,
            },
            row_payloads=build_row_payloads(dataset['items']),
            highlight_payloads=build_highlight_payloads(dataset['highlights']),
        )
        if source_payload.get('status') != '000':
            raise ValueError(f"DART fundamentals returned [{source_payload.get('status')}]: {source_payload.get('message')}")
        return request, dataset

    async def backfill_current_universe_fundamentals(
        self,
        *,
        symbol_limit: int = 25,
        symbol_offset: int = 0,
        capture_limit: int | None = 250,
        force: bool = False,
        fs_div: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        active_symbols = self.active_symbol_loader()
        min_trade_date, max_trade_date = self.daily_price_bar_bounds_loader()
        resolved_end_date = end_date or max_trade_date or date.today()
        candidate_start_date = start_date or min_trade_date or DART_FUNDAMENTALS_MIN_START_DATE
        effective_start_date = max(candidate_start_date, DART_FUNDAMENTALS_MIN_START_DATE)
        if effective_start_date > resolved_end_date:
            raise ValueError(
                'effective_start_date must be on or before effective_end_date '
                f'({effective_start_date.isoformat()} > {resolved_end_date.isoformat()})'
            )
        selected_symbols = active_symbols[symbol_offset : symbol_offset + symbol_limit]
        corp_records = []
        symbol_lookup_errors: list[dict[str, str]] = []

        for symbol in selected_symbols:
            stock_code = (symbol.get('symbol') or '').strip().zfill(6)
            try:
                corp = await self.client.lookup_corp_code(stock_code=stock_code)
                corp_records.append(
                    {
                        'stock_code': stock_code,
                        'corp_code': corp['corp_code'],
                        'corp_name': corp['corp_name'],
                    }
                )
            except Exception as exc:
                symbol_lookup_errors.append({'stock_code': stock_code, 'error': str(exc)})

        from app.dart.sync_service import dart_disclosure_sync_service

        sync_result = await dart_disclosure_sync_service.sync_disclosures_for_corporations(
            corporations=corp_records,
            start_date=effective_start_date,
            end_date=resolved_end_date,
            trigger='manual_active_universe_backfill',
        )
        if corp_records:
            capture_result = await self.capture_recent_fundamentals(
                limit=capture_limit,
                force=force,
                trigger='manual_active_universe_backfill',
                fs_div=fs_div,
                stock_codes=[corp['stock_code'] for corp in corp_records],
                receipt_date_from=effective_start_date,
                receipt_date_to=resolved_end_date,
            )
        else:
            capture_result = {
                'run_id': None,
                'status': 'completed',
                'fs_div': (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper(),
                'events_seen': 0,
                'events_captured': 0,
                'events_failed': 0,
                'events_skipped': 0,
                'results': [],
            }
        return {
            'scope': 'current_active_universe',
            'active_symbols_total': len(active_symbols),
            'symbol_offset': symbol_offset,
            'symbol_limit': symbol_limit,
            'symbols_selected': len(selected_symbols),
            'symbols_resolved': len(corp_records),
            'symbol_lookup_errors': symbol_lookup_errors,
            'requested_start_date': candidate_start_date.isoformat() if candidate_start_date else None,
            'effective_start_date': effective_start_date.isoformat(),
            'effective_end_date': resolved_end_date.isoformat(),
            'daily_price_bar_min_trade_date': min_trade_date.isoformat() if min_trade_date else None,
            'daily_price_bar_max_trade_date': max_trade_date.isoformat() if max_trade_date else None,
            'dart_min_start_date': DART_FUNDAMENTALS_MIN_START_DATE.isoformat(),
            'sync': sync_result,
            'capture': capture_result,
        }

    def inspect_capture_status(
        self,
        *,
        runs_limit: int = 10,
        events_limit: int = 25,
        capture_status: str | None = None,
    ) -> dict[str, Any]:
        return {
            'latest_run': self.repository.get_latest_capture_run(),
            'runs': self.repository.list_capture_runs(limit=runs_limit),
            'summary': self.repository.get_capture_summary(),
            'events': self.repository.list_financial_events(limit=events_limit, capture_status=capture_status),
        }


dart_fundamentals_capture_service = DartFundamentalsCaptureService()
