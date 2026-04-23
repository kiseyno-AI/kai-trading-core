from __future__ import annotations

import asyncio
import json
import hashlib
import re
from collections import defaultdict
from pathlib import Path
from contextlib import suppress
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable

from app.core.audit_logging import log_fetch_event
from app.agent_context.retry_service import EXPECTED_ROLES, FeedbackRetryCycleConfig, agent_feedback_retry_service
from app.core.settings import settings
from app.db import session_scope
from app.db_models import DailyPriceBar
from app.dart.fundamentals_store import (
    CAPTURE_STATUS_MAPPING_FAILED,
    CAPTURE_STATUS_NO_DATA,
    RETRYABLE_CAPTURE_STATUSES,
    TERMINAL_CAPTURE_STATUSES,
    DartFundamentalsRepository,
    dart_fundamentals_repository,
)
from app.dart.service import (
    FS_DIV_TO_STATEMENT_TYPE,
    REPORT_CODE_METADATA,
    build_fundamentals_dataset,
)
from app.integrations.dart.client import DartClient, dart_client
from app.market_data_store import load_symbols
from app.signals.piotroski_filter import daily_piotroski_filter_service
from sqlalchemy import func, select

_REPORT_PERIOD_PATTERN = re.compile(r'\((\d{4})\.(\d{2})\)')
_DIRECT_BACKFILL_REPORT_PATTERN = re.compile(r'direct_backfill_(\d{4})_(\d{5})', re.IGNORECASE)

_FILING_CATEGORY_TO_REPORT_CODE = {
    'annual_report': '11011',
    'semiannual_report': '11012',
    'direct_backfill': '11011',
}

DART_FUNDAMENTALS_MIN_START_DATE = date(settings.dart_fundamentals_min_business_year, 1, 1)
DART_FUNDAMENTALS_REFRESH_HEARTBEAT_SECONDS = 30.0
DART_NO_DATA_STATUS = '013'
UNSUPPORTED_CODE_REASON = 'code_unsupported:dart_lookup'


class TerminalFundamentalsCaptureError(RuntimeError):
    capture_status = ''


class DartFundamentalsNoDataError(TerminalFundamentalsCaptureError):
    capture_status = CAPTURE_STATUS_NO_DATA


class DartFundamentalsMappingError(TerminalFundamentalsCaptureError):
    capture_status = CAPTURE_STATUS_MAPPING_FAILED


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _debug_refresh_log(stage: str, **payload: Any) -> None:
    line = {
        'scope': 'dart_fundamentals_refresh',
        'stage': stage,
        **payload,
    }
    debug_path = Path(__file__).resolve().parents[2] / 'var' / 'log' / 'dart-fundamentals-refresh.log'
    try:
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(line, ensure_ascii=False, default=str) + '\n')
    except OSError:
        pass
    try:
        log_fetch_event(
            source='dart_fundamentals_refresh',
            event=stage,
            metadata=payload,
        )
    except Exception:
        pass


async def _await_with_refresh_heartbeat(
    operation: Callable[[], Awaitable[Any]],
    *,
    heartbeat: Callable[[], None],
    interval_seconds: float | None = None,
) -> Any:
    heartbeat()
    resolved_interval_seconds = (
        DART_FUNDAMENTALS_REFRESH_HEARTBEAT_SECONDS
        if interval_seconds is None
        else interval_seconds
    )
    if resolved_interval_seconds <= 0:
        return await operation()

    async def heartbeat_loop() -> None:
        while True:
            await asyncio.sleep(resolved_interval_seconds)
            heartbeat()

    task = asyncio.create_task(heartbeat_loop())
    try:
        return await operation()
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def infer_fundamentals_request(
    event: dict[str, Any],
    *,
    fs_div: str,
    fiscal_year_end_month: int | str | None = None,
) -> dict[str, Any]:
    filing_category = event['filing_category']
    report_name = event['report_nm']

    direct_backfill_match = _DIRECT_BACKFILL_REPORT_PATTERN.search(report_name)
    if direct_backfill_match is not None:
        business_year = int(direct_backfill_match.group(1))
        report_code = direct_backfill_match.group(2)
        period_end_month = _period_end_month_for_report_code(
            report_code,
            fiscal_year_end_month=fiscal_year_end_month,
        )
        return {
            'business_year': business_year,
            'report_code': report_code,
            'period_end_month': period_end_month,
            'period_type': REPORT_CODE_METADATA[report_code]['period'],
            'statement_type': FS_DIV_TO_STATEMENT_TYPE[fs_div],
            'fs_div': fs_div,
        }

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


def _period_end_month_for_report_code(
    report_code: str,
    *,
    fiscal_year_end_month: int | str | None = None,
) -> int:
    normalized_fiscal_year_end_month = _normalize_month(fiscal_year_end_month)
    if normalized_fiscal_year_end_month is not None:
        quarter = REPORT_CODE_METADATA.get(report_code, {}).get('quarter')
        if quarter is None:
            raise ValueError(f'Unsupported report_code for period-end lookup: {report_code}')
        return _month_after(normalized_fiscal_year_end_month, quarter * 3)

    if report_code == '11011':
        return 12
    if report_code == '11012':
        return 6
    if report_code == '11013':
        return 3
    if report_code == '11014':
        return 9
    raise ValueError(f'Unsupported report_code for period-end lookup: {report_code}')


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


async def resolve_supported_dart_corp_record(
    client: DartClient,
    *,
    stock_code: str,
) -> tuple[dict[str, str] | None, dict[str, Any] | None]:
    normalized_stock_code = stock_code.strip().zfill(6)
    try:
        is_supported = await client.has_supported_stock_code(normalized_stock_code)
    except Exception:
        is_supported = True
    if not is_supported:
        return None, {
            'stock_code': normalized_stock_code,
            'error_type': 'DartLookupUnsupported',
            'error': f'unsupported stock code for DART lookup: {normalized_stock_code}',
            'terminal': True,
            'status': CAPTURE_STATUS_MAPPING_FAILED,
            'reason': UNSUPPORTED_CODE_REASON,
        }
    try:
        corp = await client.lookup_corp_code(stock_code=normalized_stock_code)
    except Exception as exc:
        error_type = type(exc).__name__
        error_message = str(exc)
        if error_type == 'DartLookupError' and 'Unknown stock_code:' in error_message:
            return None, {
                'stock_code': normalized_stock_code,
                'error_type': error_type,
                'error': error_message,
                'terminal': True,
                'status': CAPTURE_STATUS_MAPPING_FAILED,
                'reason': UNSUPPORTED_CODE_REASON,
            }
        return None, {
            'stock_code': normalized_stock_code,
            'error_type': error_type,
            'error': error_message,
        }
    return {
        'stock_code': normalized_stock_code,
        'corp_code': corp['corp_code'],
        'corp_name': corp['corp_name'],
    }, None


def _load_active_symbols() -> list[dict[str, Any]]:
    return [item for item in load_symbols(is_active=True) if (item.get('symbol') or '').strip().isdigit()]


def _load_daily_price_bar_bounds() -> tuple[date | None, date | None]:
    with session_scope() as session:
        return (
            session.execute(select(func.min(DailyPriceBar.trade_date))).scalar_one(),
            session.execute(select(func.max(DailyPriceBar.trade_date))).scalar_one(),
        )


def _normalize_refresh_date(value: date | str | None, *, default: date) -> date:
    if value is None:
        return default
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _parse_optional_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _date_after(value: date) -> date:
    return value + timedelta(days=1)


def _limited_sample(items: list[Any], *, limit: int = 50) -> list[Any]:
    return items[:limit]


def _terminal_symbol_state(status: str | None) -> str:
    if status == CAPTURE_STATUS_NO_DATA:
        return 'terminal_no_data'
    if status == CAPTURE_STATUS_MAPPING_FAILED:
        return 'terminal_mapping_failed'
    return 'terminal_resolved'


def _build_operator_buckets(
    *,
    lookup_errors: list[dict[str, str]],
    capture_results: list[dict[str, Any]],
    terminal_resolved_symbols: dict[str, str],
    remaining_retryable_events: int = 0,
) -> dict[str, Any]:
    retryable_capture_failures = [
        item for item in capture_results if item.get('status') == 'failed'
    ]
    terminal_results_by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in capture_results:
        status = item.get('status')
        if status in TERMINAL_CAPTURE_STATUSES:
            terminal_results_by_status[status].append(item)

    no_data_symbols = {
        item['stock_code']
        for item in terminal_results_by_status.get(CAPTURE_STATUS_NO_DATA, [])
        if item.get('stock_code')
    }
    mapping_symbols = {
        item['stock_code']
        for item in terminal_results_by_status.get(CAPTURE_STATUS_MAPPING_FAILED, [])
        if item.get('stock_code')
    }
    for stock_code, terminal_status in terminal_resolved_symbols.items():
        if terminal_status == CAPTURE_STATUS_NO_DATA:
            no_data_symbols.add(stock_code)
        elif terminal_status == CAPTURE_STATUS_MAPPING_FAILED:
            mapping_symbols.add(stock_code)

    return {
        'lookup_or_mapping_errors': {
            'count': len(lookup_errors),
            'items_sample': _limited_sample(lookup_errors),
            'symbols_sample': _limited_sample(sorted({item['stock_code'] for item in lookup_errors})),
        },
        'retryable_capture_failures': {
            'count': len(retryable_capture_failures),
            'items_sample': _limited_sample(retryable_capture_failures),
            'symbols_sample': _limited_sample(
                sorted({item['stock_code'] for item in retryable_capture_failures if item.get('stock_code')})
            ),
        },
        'terminal_no_data': {
            'event_count': len(terminal_results_by_status.get(CAPTURE_STATUS_NO_DATA, [])),
            'symbol_count': len(no_data_symbols),
            'items_sample': _limited_sample(terminal_results_by_status.get(CAPTURE_STATUS_NO_DATA, [])),
            'symbols_sample': _limited_sample(sorted(no_data_symbols)),
        },
        'terminal_mapping_failed': {
            'event_count': len(terminal_results_by_status.get(CAPTURE_STATUS_MAPPING_FAILED, [])),
            'symbol_count': len(mapping_symbols),
            'items_sample': _limited_sample(terminal_results_by_status.get(CAPTURE_STATUS_MAPPING_FAILED, [])),
            'symbols_sample': _limited_sample(sorted(mapping_symbols)),
        },
        'remaining_retryable_events': {
            'count': remaining_retryable_events,
        },
    }


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
        event_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        run_metadata: dict[str, Any] = {'requested_limit': limit}
        if stock_codes:
            run_metadata['stock_code_count'] = len(stock_codes)
        if receipt_date_from:
            run_metadata['receipt_date_from'] = receipt_date_from.isoformat()
        if receipt_date_to:
            run_metadata['receipt_date_to'] = receipt_date_to.isoformat()
        if event_ids is not None:
            run_metadata['event_id_count'] = len(event_ids)
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
                event_ids=event_ids,
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
                            'stock_code': event['stock_code'],
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
                            'stock_code': event['stock_code'],
                            'status': 'failed',
                            'error': timeout_message,
                        }
                    )
                except TerminalFundamentalsCaptureError as exc:
                    capture_status = exc.capture_status
                    self.repository.mark_event_capture_terminal(
                        event['id'],
                        capture_status=capture_status,
                        error_message=str(exc),
                    )
                    events_skipped += 1
                    run_metadata_with_progress = {
                        **run_metadata,
                        'candidate_count': len(events),
                        'last_event': {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': capture_status,
                            'terminal': True,
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
                        status=capture_status,
                        run_id=run_id,
                        event_id=event['id'],
                        rcept_no=event['rcept_no'],
                        corp_code=event['corp_code'],
                        stock_code=event['stock_code'],
                        fs_div=resolved_fs_div,
                        terminal=True,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    results.append(
                        {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'stock_code': event['stock_code'],
                            'status': capture_status,
                            'terminal': True,
                            'error': str(exc),
                        }
                    )
                except Exception as exc:
                    error_message = str(exc)
                    if f'DART fundamentals returned [{DART_NO_DATA_STATUS}]' in error_message:
                        capture_status = CAPTURE_STATUS_NO_DATA
                        self.repository.mark_event_capture_terminal(
                            event['id'],
                            capture_status=capture_status,
                            error_message=error_message,
                        )
                        events_skipped += 1
                        run_metadata_with_progress = {
                            **run_metadata,
                            'candidate_count': len(events),
                            'last_event': {
                                'event_id': event['id'],
                                'rcept_no': event['rcept_no'],
                                'status': capture_status,
                                'terminal': True,
                                'error': error_message,
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
                            status=capture_status,
                            run_id=run_id,
                            event_id=event['id'],
                            rcept_no=event['rcept_no'],
                            corp_code=event['corp_code'],
                            stock_code=event['stock_code'],
                            fs_div=resolved_fs_div,
                            terminal=True,
                            error_type=type(exc).__name__,
                            error_message=error_message,
                        )
                        results.append(
                            {
                                'event_id': event['id'],
                                'rcept_no': event['rcept_no'],
                                'stock_code': event['stock_code'],
                                'status': capture_status,
                                'terminal': True,
                                'error': error_message,
                            }
                        )
                        continue

                    self.repository.mark_event_capture_failed(event['id'], error_message=error_message)
                    events_failed += 1
                    run_metadata_with_progress = {
                        **run_metadata,
                        'candidate_count': len(events),
                        'last_event': {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'status': 'failed',
                            'error': error_message,
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
                        error_message=error_message,
                    )
                    results.append(
                        {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'stock_code': event['stock_code'],
                            'status': 'failed',
                            'error': error_message,
                        }
                    )

            operator_buckets = _build_operator_buckets(
                lookup_errors=[],
                capture_results=results,
                terminal_resolved_symbols={},
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
                'operator_buckets': operator_buckets,
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
                    'operator_buckets': operator_buckets,
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
        try:
            request = infer_fundamentals_request(
                event,
                fs_div=fs_div,
                fiscal_year_end_month=company_overview.get('acc_mt'),
            )
        except ValueError as exc:
            raise DartFundamentalsMappingError(str(exc)) from exc
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
        if source_payload.get('status') == DART_NO_DATA_STATUS:
            raise DartFundamentalsNoDataError(
                f"DART fundamentals returned [{source_payload.get('status')}]: {source_payload.get('message')}"
            )
        if source_payload.get('status') != '000':
            raise RuntimeError(
                f"DART fundamentals returned [{source_payload.get('status')}]: {source_payload.get('message')}"
            )
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
            corp_record, lookup_error = await resolve_supported_dart_corp_record(self.client, stock_code=stock_code)
            if corp_record is not None:
                corp_records.append(corp_record)
                continue
            if lookup_error is not None:
                symbol_lookup_errors.append(lookup_error)

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
        terminal_lookup_statuses = [
            item for item in symbol_lookup_errors if item.get('terminal') and item.get('status') == CAPTURE_STATUS_MAPPING_FAILED
        ]
        terminal_lookup_symbols = {
            item['stock_code']: CAPTURE_STATUS_MAPPING_FAILED
            for item in terminal_lookup_statuses
            if item.get('stock_code')
        }
        retryable_lookup_errors = [item for item in symbol_lookup_errors if not item.get('terminal')]
        return {
            'scope': 'current_active_universe',
            'active_symbols_total': len(active_symbols),
            'symbol_offset': symbol_offset,
            'symbol_limit': symbol_limit,
            'symbols_selected': len(selected_symbols),
            'symbols_resolved': len(corp_records),
            'symbol_lookup_error_count': len(retryable_lookup_errors),
            'symbol_lookup_errors': retryable_lookup_errors,
            'terminal_unsupported_symbol_count': len(terminal_lookup_statuses),
            'terminal_unsupported_symbols': terminal_lookup_statuses,
            'requested_start_date': candidate_start_date.isoformat() if candidate_start_date else None,
            'effective_start_date': effective_start_date.isoformat(),
            'effective_end_date': resolved_end_date.isoformat(),
            'daily_price_bar_min_trade_date': min_trade_date.isoformat() if min_trade_date else None,
            'daily_price_bar_max_trade_date': max_trade_date.isoformat() if max_trade_date else None,
            'dart_min_start_date': DART_FUNDAMENTALS_MIN_START_DATE.isoformat(),
            'sync': sync_result,
            'capture': capture_result,
            'operator_buckets': _build_operator_buckets(
                lookup_errors=retryable_lookup_errors,
                capture_results=capture_result.get('results', []),
                terminal_resolved_symbols=terminal_lookup_symbols,
            ),
        }

    async def run_daily_incremental_refresh(
        self,
        *,
        market_date: date | str | None = None,
        symbol_limit: int | None = None,
        symbol_offset: int = 0,
        capture_limit: int | None = 500,
        force: bool = False,
        fs_div: str | None = None,
        trigger: str = 'manual_daily_api',
        retry_cycles: int | None = None,
    ) -> dict[str, Any]:
        if symbol_offset < 0:
            raise ValueError('symbol_offset must be greater than or equal to 0')
        if symbol_limit is not None and symbol_limit < 1:
            raise ValueError('symbol_limit must be greater than or equal to 1')
        if capture_limit is not None and capture_limit < 1:
            raise ValueError('capture_limit must be greater than or equal to 1')
        resolved_retry_cycles = retry_cycles or settings.dart_fundamentals_daily_retry_cycles
        if resolved_retry_cycles < 1:
            raise ValueError('retry_cycles must be greater than or equal to 1')

        resolved_market_date = _normalize_refresh_date(market_date, default=date.today())
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        stale_refresh_run_ids = self.repository.fail_stale_refresh_runs(
            stale_before=_utcnow() - timedelta(minutes=self.stale_run_minutes),
            error_message=(
                'Marked failed after exceeding the stale running threshold before a new daily refresh started'
            ),
        )
        latest_run = self.repository.get_latest_refresh_run(
            market_date=resolved_market_date,
            fs_div=resolved_fs_div,
        )
        if latest_run and latest_run['status'] == 'running':
            return {
                'triggered': False,
                'reason': 'already_running',
                'market_date': resolved_market_date.isoformat(),
                'fs_div': resolved_fs_div,
                'existing_run': latest_run,
                'stale_refresh_run_ids_failed': stale_refresh_run_ids,
            }
        if latest_run and latest_run['status'] == 'completed' and not force:
            return {
                'triggered': False,
                'reason': 'already_completed',
                'market_date': resolved_market_date.isoformat(),
                'fs_div': resolved_fs_div,
                'existing_run': latest_run,
                'stale_refresh_run_ids_failed': stale_refresh_run_ids,
            }

        active_symbols = self.active_symbol_loader()
        upper_bound = None if symbol_limit is None else symbol_offset + symbol_limit
        selected_symbols = active_symbols[symbol_offset:upper_bound]
        selected_stock_codes = [
            (symbol.get('symbol') or '').strip().zfill(6)
            for symbol in selected_symbols
            if (symbol.get('symbol') or '').strip()
        ]
        if not selected_stock_codes:
            return {
                'scope': 'current_active_universe',
                'active_symbols_total': len(active_symbols),
                'symbol_offset': symbol_offset,
                'symbol_limit': symbol_limit,
                'symbols_selected': 0,
                'symbols_resolved': 0,
                'symbol_lookup_error_count': 0,
                'symbol_lookup_errors': [],
                'terminal_unsupported_symbol_count': 0,
                'terminal_unsupported_symbols': [],
                'requested_start_date': None,
                'effective_start_date': None,
                'effective_end_date': resolved_market_date.isoformat(),
                'daily_price_bar_min_trade_date': None,
                'daily_price_bar_max_trade_date': None,
                'dart_min_start_date': DART_FUNDAMENTALS_MIN_START_DATE.isoformat(),
                'sync': {
                    'status': 'completed',
                    'run_id': None,
                    'events_seen': 0,
                    'events_inserted': 0,
                    'events_updated': 0,
                    'financial_report_events': 0,
                    'corporations_requested': 0,
                    'corporations_processed': 0,
                    'corporations_failed': 0,
                    'corporation_errors': [],
                },
                'capture': {
                    'status': 'completed',
                    'run_id': None,
                    'fs_div': resolved_fs_div,
                    'events_seen': 0,
                    'events_captured': 0,
                    'events_failed': 0,
                    'events_skipped': 0,
                    'results': [],
                    'operator_buckets': {
                        'lookup_or_mapping_errors': {'count': 0, 'items_sample': [], 'symbols_sample': []},
                        'retryable_capture_failures': {'count': 0, 'items_sample': [], 'symbols_sample': []},
                        'terminal_no_data': {'event_count': 0, 'symbol_count': 0, 'items_sample': [], 'symbols_sample': []},
                        'terminal_mapping_failed': {'event_count': 0, 'symbol_count': 0, 'items_sample': [], 'symbols_sample': []},
                        'remaining_retryable_events': {'count': 0},
                    },
                },
                'operator_buckets': {
                    'lookup_or_mapping_errors': {'count': 0, 'items_sample': [], 'symbols_sample': []},
                    'retryable_capture_failures': {'count': 0, 'items_sample': [], 'symbols_sample': []},
                    'terminal_no_data': {'event_count': 0, 'symbol_count': 0, 'items_sample': [], 'symbols_sample': []},
                    'terminal_mapping_failed': {'event_count': 0, 'symbol_count': 0, 'items_sample': [], 'symbols_sample': []},
                    'remaining_retryable_events': {'count': 0},
                },
            }
        states = self.repository.list_symbol_fundamentals_states(
            stock_codes=selected_stock_codes,
            fs_div=resolved_fs_div,
        )
        run_metadata: dict[str, Any] = {
            'scope': 'daily_incremental_fundamentals_refresh',
            'market_date': resolved_market_date.isoformat(),
            'fs_div': resolved_fs_div,
            'active_symbols_total': len(active_symbols),
            'symbols_selected': len(selected_stock_codes),
            'symbol_offset': symbol_offset,
            'symbol_limit': symbol_limit,
            'capture_limit': capture_limit,
            'retry_cycles_requested': resolved_retry_cycles,
            'selected_symbols_sample': _limited_sample(selected_stock_codes),
            'stale_refresh_run_ids_failed': stale_refresh_run_ids,
            'last_progress_at': _utcnow().isoformat(),
        }
        run_id = self.repository.create_refresh_run(
            market_date=resolved_market_date,
            trigger=trigger,
            fs_div=resolved_fs_div,
            force=force,
            symbol_limit=symbol_limit,
            symbol_offset=symbol_offset,
            symbols_total=len(active_symbols),
            run_metadata=run_metadata,
        )

        symbols_checked = len(selected_stock_codes)
        symbols_missing = 0
        symbols_stale = 0
        symbols_fresh = 0
        symbols_refreshed = 0
        symbols_failed = 0
        events_detected = 0
        events_captured = 0
        events_failed = 0
        sync_results: list[dict[str, Any]] = []
        symbol_results: dict[str, dict[str, Any]] = {}
        lookup_errors: list[dict[str, str]] = []
        lookup_failed_symbols: set[str] = set()
        sync_groups: dict[date, list[dict[str, str]]] = {}
        check_start_by_symbol: dict[str, date] = {}
        latest_fundamentals_date_by_symbol: dict[str, date | None] = {}
        latest_resolved_date_by_symbol: dict[str, date | None] = {}
        terminal_resolved_symbols: dict[str, str] = {}

        refresh_progress_state: dict[str, Any] = {}

        def update_refresh_progress(
            *,
            stage: str,
            extra_metadata: dict[str, Any] | None = None,
            persist: bool = True,
        ) -> None:
            metadata = {
                **run_metadata,
                'stage': stage,
                'lookup_error_count': len(lookup_errors),
                'lookup_or_mapping_errors': _limited_sample(lookup_errors),
                'sync_run_count': len(sync_results),
                'last_progress_at': _utcnow().isoformat(),
            }
            if extra_metadata:
                metadata.update(extra_metadata)
            refresh_progress_state.clear()
            refresh_progress_state.update(metadata)
            if not persist:
                return
            self.repository.update_refresh_run_progress(
                run_id,
                symbols_checked=symbols_checked,
                symbols_missing=symbols_missing,
                symbols_stale=symbols_stale,
                symbols_fresh=symbols_fresh,
                symbols_refreshed=symbols_refreshed,
                symbols_failed=symbols_failed,
                events_detected=events_detected,
                events_captured=events_captured,
                events_failed=events_failed,
                run_metadata=dict(refresh_progress_state),
            )

        try:
            for stock_code in selected_stock_codes:
                state = states.get(stock_code) or {
                    'stock_code': stock_code,
                    'has_fundamentals': False,
                    'latest_fundamentals_receipt_date': None,
                    'latest_resolved_fundamentals_receipt_date': None,
                    'latest_snapshot': None,
                    'latest_terminal_fundamentals_event': None,
                }
                latest_fundamentals_date = _parse_optional_iso_date(
                    state.get('latest_fundamentals_receipt_date')
                )
                latest_resolved_date = _parse_optional_iso_date(
                    state.get('latest_resolved_fundamentals_receipt_date')
                ) or latest_fundamentals_date
                terminal_event = state.get('latest_terminal_fundamentals_event') or {}
                latest_terminal_status = terminal_event.get('fundamentals_capture_status')
                latest_terminal_date = _parse_optional_iso_date(terminal_event.get('receipt_date'))
                latest_fundamentals_date_by_symbol[stock_code] = latest_fundamentals_date
                latest_resolved_date_by_symbol[stock_code] = latest_resolved_date
                if (
                    latest_resolved_date is not None
                    and latest_terminal_date == latest_resolved_date
                    and latest_terminal_status in TERMINAL_CAPTURE_STATUSES
                ):
                    terminal_resolved_symbols[stock_code] = latest_terminal_status
                check_start_date = (
                    _date_after(latest_resolved_date)
                    if latest_resolved_date is not None
                    else DART_FUNDAMENTALS_MIN_START_DATE
                )
                symbol_result = {
                    'stock_code': stock_code,
                    'has_fundamentals': latest_fundamentals_date is not None,
                    'latest_fundamentals_receipt_date': (
                        latest_fundamentals_date.isoformat() if latest_fundamentals_date else None
                    ),
                    'latest_resolved_fundamentals_receipt_date': (
                        latest_resolved_date.isoformat() if latest_resolved_date else None
                    ),
                    'latest_terminal_capture_status': latest_terminal_status,
                    'check_start_date': check_start_date.isoformat(),
                    'state': 'checking',
                }
                symbol_results[stock_code] = symbol_result

                if latest_fundamentals_date is None:
                    symbols_missing += 1
                if check_start_date > resolved_market_date:
                    symbol_result['state'] = (
                        _terminal_symbol_state(latest_terminal_status)
                        if latest_fundamentals_date is None and latest_terminal_status in TERMINAL_CAPTURE_STATUSES
                        else 'fresh'
                    )
                    continue

                corp_record, lookup_error = await resolve_supported_dart_corp_record(
                    self.client,
                    stock_code=stock_code,
                )
                if lookup_error is not None:
                    error_message = lookup_error.get('error') or ''
                    if lookup_error.get('terminal') and lookup_error.get('status') == CAPTURE_STATUS_MAPPING_FAILED:
                        terminal_resolved_symbols[stock_code] = CAPTURE_STATUS_MAPPING_FAILED
                        symbol_result['state'] = _terminal_symbol_state(CAPTURE_STATUS_MAPPING_FAILED)
                        symbol_result['terminal'] = True
                        symbol_result['error'] = error_message
                        continue
                    lookup_errors.append(lookup_error)
                    lookup_failed_symbols.add(stock_code)
                    symbol_result['state'] = 'lookup_failed'
                    symbol_result['error'] = error_message
                    continue
                check_start_by_symbol[stock_code] = check_start_date
                sync_groups.setdefault(check_start_date, []).append(corp_record)

            symbols_failed = len(lookup_failed_symbols)
            update_refresh_progress(
                stage='sync_groups_prepared',
                extra_metadata={
                    'sync_group_count': len(sync_groups),
                },
            )
            _debug_refresh_log(
                'sync_groups_prepared',
                run_id=run_id,
                sync_group_count=len(sync_groups),
                selected_symbols=len(selected_stock_codes),
                lookup_failed_symbols=len(lookup_failed_symbols),
            )

            from app.dart.sync_service import dart_disclosure_sync_service

            for check_start_date, corporations in sorted(sync_groups.items(), key=lambda item: item[0]):
                sync_group_metadata = {
                    'current_sync_group': {
                        'check_start_date': check_start_date.isoformat(),
                        'corporation_count': len(corporations),
                        'stock_codes_sample': _limited_sample(
                            [corporation['stock_code'] for corporation in corporations]
                        ),
                    },
                }

                async def sync_group_operation() -> dict[str, Any]:
                    return await dart_disclosure_sync_service.sync_disclosures_for_corporations(
                        corporations=corporations,
                        start_date=check_start_date,
                        end_date=resolved_market_date,
                        trigger='daily_incremental_refresh',
                    )

                update_refresh_progress(
                    stage='syncing_disclosures',
                    extra_metadata=sync_group_metadata,
                )
                _debug_refresh_log(
                    'sync_group_start',
                    run_id=run_id,
                    check_start_date=check_start_date.isoformat(),
                    corporation_count=len(corporations),
                    stock_codes_sample=_limited_sample([corporation['stock_code'] for corporation in corporations]),
                )
                sync_result = await _await_with_refresh_heartbeat(
                    sync_group_operation,
                    heartbeat=lambda: update_refresh_progress(
                        stage='syncing_disclosures',
                        extra_metadata=sync_group_metadata,
                    ),
                )
                sync_results.append(
                    {
                        **sync_result,
                        'check_start_date': check_start_date.isoformat(),
                        'corporation_count': len(corporations),
                    }
                )
                update_refresh_progress(
                    stage='sync_group_completed',
                    extra_metadata={
                        **sync_group_metadata,
                        'sync_run_id': sync_result.get('run_id'),
                        'sync_status': sync_result.get('status'),
                    },
                )
                _debug_refresh_log(
                    'sync_group_completed',
                    run_id=run_id,
                    check_start_date=check_start_date.isoformat(),
                    sync_run_id=sync_result.get('run_id'),
                    sync_status=sync_result.get('status'),
                )

            update_refresh_progress(
                stage='sync_groups_finished',
                extra_metadata={
                    'sync_group_count': len(sync_groups),
                    'sync_result_count': len(sync_results),
                    'check_start_symbol_count': len(check_start_by_symbol),
                },
            )
            _debug_refresh_log(
                'sync_groups_finished',
                run_id=run_id,
                sync_group_count=len(sync_groups),
                sync_result_count=len(sync_results),
                check_start_symbol_count=len(check_start_by_symbol),
            )
            sync_stock_codes = list(check_start_by_symbol.keys())
            candidate_events: list[dict[str, Any]] = []
            if sync_stock_codes:
                update_refresh_progress(
                    stage='loading_capture_candidates',
                    extra_metadata={
                        'sync_stock_code_count': len(sync_stock_codes),
                        'candidate_receipt_date_from': min(check_start_by_symbol.values()).isoformat(),
                        'candidate_receipt_date_to': resolved_market_date.isoformat(),
                    },
                )
                _debug_refresh_log(
                    'loading_capture_candidates',
                    run_id=run_id,
                    sync_stock_code_count=len(sync_stock_codes),
                    candidate_receipt_date_from=min(check_start_by_symbol.values()).isoformat(),
                    candidate_receipt_date_to=resolved_market_date.isoformat(),
                )
                pending_events = self.repository.list_capture_candidates(
                    limit=None,
                    force=False,
                    stock_codes=sync_stock_codes,
                    receipt_date_from=min(check_start_by_symbol.values()),
                    receipt_date_to=resolved_market_date,
                    oldest_first=True,
                )
                update_refresh_progress(
                    stage='capture_candidates_loaded',
                    extra_metadata={
                        'pending_event_count': len(pending_events),
                        'pending_event_sample': _limited_sample(
                            [int(event['id']) for event in pending_events],
                            limit=50,
                        ),
                    },
                )
                _debug_refresh_log(
                    'capture_candidates_loaded',
                    run_id=run_id,
                    pending_event_count=len(pending_events),
                    pending_event_sample=_limited_sample([int(event['id']) for event in pending_events], limit=50),
                )
                seen_event_ids: set[int] = set()
                for event in pending_events:
                    event_id = int(event['id'])
                    if event_id in seen_event_ids:
                        continue
                    stock_code = event.get('stock_code')
                    if not stock_code or stock_code not in check_start_by_symbol:
                        continue
                    receipt_date = _parse_optional_iso_date(event.get('receipt_date'))
                    if receipt_date is None or receipt_date < check_start_by_symbol[stock_code]:
                        continue
                    candidate_events.append(event)
                    seen_event_ids.add(event_id)

            events_detected = len(candidate_events)
            candidate_event_ids = [int(event['id']) for event in candidate_events]
            event_ids_to_capture = (
                candidate_event_ids
                if capture_limit is None
                else candidate_event_ids[:capture_limit]
            )
            candidate_symbols = {event['stock_code'] for event in candidate_events if event.get('stock_code')}
            stale_symbols = {
                stock_code
                for stock_code in candidate_symbols
                if latest_fundamentals_date_by_symbol.get(stock_code) is not None
            }
            missing_candidate_symbols = candidate_symbols - stale_symbols
            symbols_stale = len(stale_symbols)

            for stock_code in stale_symbols:
                symbol_results[stock_code]['state'] = 'stale'
            for stock_code in missing_candidate_symbols:
                symbol_results[stock_code]['state'] = 'missing'
            for stock_code, symbol_result in symbol_results.items():
                if symbol_result['state'] != 'checking':
                    continue
                if latest_fundamentals_date_by_symbol.get(stock_code) is not None:
                    symbol_result['state'] = 'fresh'
                elif latest_resolved_date_by_symbol.get(stock_code) is not None:
                    symbol_result['state'] = 'terminal_no_newer_events'
                else:
                    symbol_result['state'] = 'missing_no_newer_events'

            capture_runs: list[dict[str, Any]] = []
            capture_result: dict[str, Any] = {
                'run_id': None,
                'status': 'skipped',
                'reason': 'no_stale_or_missing_events',
                'fs_div': resolved_fs_div,
                'events_seen': 0,
                'events_captured': 0,
                'events_failed': 0,
                'events_skipped': 0,
                'results': [],
            }
            remaining_candidate_events = events_detected
            retry_cycles_completed = 0

            over_limit_event_ids = candidate_event_ids[len(event_ids_to_capture):]

            if event_ids_to_capture:
                pending_event_ids = list(event_ids_to_capture)
                candidate_receipt_date_from = (
                    min(check_start_by_symbol.values()) if check_start_by_symbol else None
                )
                max_retry_cycles = max(1, resolved_retry_cycles)
                pending_batches: list[list[int]] = []
                if pending_event_ids:
                    pending_batches.append(list(pending_event_ids))
                while over_limit_event_ids:
                    next_batch = over_limit_event_ids[:capture_limit] if capture_limit is not None else list(over_limit_event_ids)
                    pending_batches.append(list(next_batch))
                    over_limit_event_ids = over_limit_event_ids[len(next_batch):]

                retryable_next_batches: list[list[int]] = []
                remaining_retryable_event_ids: list[int] = []
                for batch_index, batch_event_ids in enumerate(pending_batches, start=1):
                    pending_event_ids = list(batch_event_ids)
                    for cycle_index in range(max_retry_cycles):
                        if not pending_event_ids:
                            break
                        retry_cycles_completed = max(retry_cycles_completed, cycle_index + 1)
                        capture_metadata = {
                            'candidate_event_count': events_detected,
                            'capture_event_count': len(pending_event_ids),
                            'candidate_event_ids_sample': _limited_sample(candidate_event_ids, limit=100),
                            'retry_cycle': cycle_index + 1,
                            'retry_cycles_requested': max_retry_cycles,
                            'batch_index': batch_index,
                            'batch_count': len(pending_batches),
                        }

                        async def capture_operation() -> dict[str, Any]:
                            return await self.capture_recent_fundamentals(
                                limit=len(pending_event_ids),
                                force=False,
                                trigger='daily_incremental_refresh',
                                fs_div=resolved_fs_div,
                                stock_codes=sorted(candidate_symbols),
                                receipt_date_from=candidate_receipt_date_from,
                                receipt_date_to=resolved_market_date,
                                event_ids=list(pending_event_ids),
                            )

                        cycle_result = await _await_with_refresh_heartbeat(
                            capture_operation,
                            heartbeat=lambda: update_refresh_progress(
                                stage='capturing_fundamentals',
                                extra_metadata=capture_metadata,
                            ),
                        )
                        capture_runs.append(cycle_result)

                        pending_set = set(pending_event_ids)
                        retryable_next: list[int] = []
                        for item in cycle_result.get('results', []):
                            event_id = item.get('event_id')
                            if event_id not in pending_set:
                                continue
                            if item.get('status') in RETRYABLE_CAPTURE_STATUSES:
                                retryable_next.append(int(event_id))
                        pending_event_ids = retryable_next
                        remaining_candidate_events = sum(len(batch) for batch in retryable_next_batches) + len(pending_event_ids) + sum(
                            len(batch) for batch in pending_batches[batch_index:]
                        )
                        if not pending_event_ids:
                            break

                    if pending_event_ids:
                        retryable_next_batches.append(list(pending_event_ids))
                        remaining_retryable_event_ids.extend(pending_event_ids)

                pending_event_ids = list(remaining_retryable_event_ids)
                remaining_candidate_events = len(remaining_retryable_event_ids)

                if capture_runs:
                    final_results_by_event_id: dict[int, dict[str, Any]] = {}
                    ordered_final_results: list[dict[str, Any]] = []
                    for cycle_result in capture_runs:
                        for item in cycle_result.get('results', []):
                            event_id = item.get('event_id')
                            if event_id is None:
                                ordered_final_results.append(item)
                                continue
                            normalized_event_id = int(event_id)
                            if normalized_event_id not in final_results_by_event_id:
                                ordered_final_results.append(item)
                            final_results_by_event_id[normalized_event_id] = item

                    aggregated_results = []
                    seen_event_ids: set[int] = set()
                    for item in ordered_final_results:
                        event_id = item.get('event_id')
                        if event_id is None:
                            aggregated_results.append(item)
                            continue
                        normalized_event_id = int(event_id)
                        if normalized_event_id in seen_event_ids:
                            continue
                        aggregated_results.append(final_results_by_event_id[normalized_event_id])
                        seen_event_ids.add(normalized_event_id)

                    final_failed_event_ids = {
                        event_id
                        for event_id, item in final_results_by_event_id.items()
                        if item.get('status') == 'failed'
                    }
                    capture_result = {
                        'run_id': capture_runs[-1].get('run_id'),
                        'status': capture_runs[-1].get('status'),
                        'fs_div': resolved_fs_div,
                        'events_seen': sum(int(item.get('events_seen') or 0) for item in capture_runs),
                        'events_captured': sum(int(item.get('events_captured') or 0) for item in capture_runs),
                        'events_failed': len(final_failed_event_ids),
                        'events_skipped': sum(int(item.get('events_skipped') or 0) for item in capture_runs),
                        'results': aggregated_results,
                        'runs': capture_runs,
                        'retry_cycles_requested': max_retry_cycles,
                        'retry_cycles_completed': retry_cycles_completed,
                        'remaining_retryable_event_ids_sample': _limited_sample(pending_event_ids, limit=100),
                        'over_limit_event_ids_sample': _limited_sample(over_limit_event_ids, limit=100),
                    }
            events_captured = int(capture_result.get('events_captured') or 0)
            events_failed = int(capture_result.get('events_failed') or 0)
            refreshed_symbols = {
                item['stock_code']
                for item in capture_result.get('results', [])
                if item.get('status') == 'captured' and item.get('stock_code')
            }
            failed_symbols = {
                item['stock_code']
                for item in capture_result.get('results', [])
                if item.get('status') == 'failed' and item.get('stock_code')
            }
            terminal_symbols_by_status: dict[str, set[str]] = defaultdict(set)
            for item in capture_result.get('results', []):
                status_value = item.get('status')
                stock_code = item.get('stock_code')
                if status_value in TERMINAL_CAPTURE_STATUSES and stock_code:
                    terminal_symbols_by_status[status_value].add(stock_code)
                    if stock_code in symbol_results:
                        symbol_results[stock_code]['state'] = _terminal_symbol_state(status_value)
                        symbol_results[stock_code]['terminal'] = True
                        symbol_results[stock_code]['error'] = item.get('error')
            symbols_refreshed = len(refreshed_symbols)
            symbols_failed = len(lookup_failed_symbols | failed_symbols)
            symbols_fresh = len(
                [
                    stock_code
                    for stock_code, latest_date in latest_fundamentals_date_by_symbol.items()
                    if latest_date is not None
                    and stock_code not in stale_symbols
                    and stock_code not in lookup_failed_symbols
                ]
            )
            operator_buckets = _build_operator_buckets(
                lookup_errors=lookup_errors,
                capture_results=capture_result.get('results', []),
                terminal_resolved_symbols=terminal_resolved_symbols,
                remaining_retryable_events=remaining_candidate_events,
            )

            partial_reasons: list[str] = []
            if symbols_failed:
                partial_reasons.append('symbol_failures')
            if events_failed:
                partial_reasons.append('event_failures')
            if remaining_candidate_events:
                partial_reasons.append('remaining_retryable_events')

            status = 'partial' if partial_reasons else 'completed'

            piotroski_filter_followup = None
            piotroski_filter_followup_error = None
            if status in {'completed', 'partial'}:
                try:
                    piotroski_filter_followup = await asyncio.to_thread(
                        daily_piotroski_filter_service.rebuild_for_market_date,
                        market_date=resolved_market_date,
                    )
                    if int(piotroski_filter_followup.get('results_failed') or 0) > 0:
                        partial_reasons.append('piotroski_filter_persistence_incomplete')
                except Exception as exc:
                    piotroski_filter_followup_error = str(exc)
                    partial_reasons.append('piotroski_filter_followup_failed')
                status = 'partial' if partial_reasons else 'completed'

            metadata = {
                **run_metadata,
                'lookup_error_count': len(lookup_errors),
                'lookup_or_mapping_errors': _limited_sample(lookup_errors),
                'operator_buckets': operator_buckets,
                'sync_runs': sync_results,
                'capture_run_id': capture_result.get('run_id'),
                'capture_run_ids': [item.get('run_id') for item in capture_runs if item.get('run_id') is not None],
                'candidate_event_count': events_detected,
                'candidate_event_ids_sample': _limited_sample(candidate_event_ids, limit=100),
                'candidate_events_sample': _limited_sample(
                    [
                        {
                            'event_id': event['id'],
                            'rcept_no': event['rcept_no'],
                            'stock_code': event['stock_code'],
                            'receipt_date': event['receipt_date'],
                            'report_nm': event['report_nm'],
                            'fundamentals_capture_status': event['fundamentals_capture_status'],
                        }
                        for event in candidate_events
                    ],
                    limit=50,
                ),
                'stale_symbols_sample': _limited_sample(sorted(stale_symbols)),
                'missing_candidate_symbols_sample': _limited_sample(sorted(missing_candidate_symbols)),
                'refreshed_symbols_sample': _limited_sample(sorted(refreshed_symbols)),
                'failed_symbols_sample': _limited_sample(sorted(lookup_failed_symbols | failed_symbols)),
                'terminal_no_data_symbols_sample': _limited_sample(
                    sorted(terminal_symbols_by_status.get(CAPTURE_STATUS_NO_DATA, set()))
                ),
                'terminal_mapping_failed_symbols_sample': _limited_sample(
                    sorted(terminal_symbols_by_status.get(CAPTURE_STATUS_MAPPING_FAILED, set()))
                ),
                'symbol_results_sample': _limited_sample(
                    [symbol_results[stock_code] for stock_code in sorted(symbol_results)],
                    limit=100,
                ),
                'remaining_candidate_events': remaining_candidate_events,
                'over_limit_event_count': len(over_limit_event_ids),
                'over_limit_event_ids_sample': _limited_sample(over_limit_event_ids, limit=100),
                'retry_cycles_requested': resolved_retry_cycles,
                'retry_cycles_completed': retry_cycles_completed,
                'partial_reasons': partial_reasons,
                'piotroski_filter_followup': piotroski_filter_followup,
                'piotroski_filter_followup_error': piotroski_filter_followup_error,
                'terminal_outcomes': {
                    'no_data': len(terminal_symbols_by_status.get(CAPTURE_STATUS_NO_DATA, set())),
                    'mapping_failed': len(terminal_symbols_by_status.get(CAPTURE_STATUS_MAPPING_FAILED, set())),
                },
                'last_progress_at': _utcnow().isoformat(),
            }
            self.repository.complete_refresh_run(
                run_id,
                status=status,
                symbols_checked=symbols_checked,
                symbols_missing=symbols_missing,
                symbols_stale=symbols_stale,
                symbols_fresh=symbols_fresh,
                symbols_refreshed=symbols_refreshed,
                symbols_failed=symbols_failed,
                events_detected=events_detected,
                events_captured=events_captured,
                events_failed=events_failed,
                run_metadata=metadata,
            )
            feedback_followup = None
            feedback_followup_error = None
            should_trigger_feedback_followup = (
                settings.openclaw_feedback_trigger_after_fundamentals_refresh
                and status in {'completed', 'partial'}
                and piotroski_filter_followup is not None
                and not piotroski_filter_followup_error
                and int(piotroski_filter_followup.get('results_failed') or 0) == 0
            )
            if settings.openclaw_feedback_trigger_after_fundamentals_refresh and not should_trigger_feedback_followup:
                feedback_followup = {
                    'triggered': False,
                    'reason': 'piotroski_filter_followup_incomplete',
                    'market_date': resolved_market_date.isoformat(),
                }
            if should_trigger_feedback_followup:
                try:
                    feedback_followup = await asyncio.to_thread(
                        agent_feedback_retry_service.run_retry_cycles,
                        config=FeedbackRetryCycleConfig(
                            market_date=resolved_market_date,
                            roles=EXPECTED_ROLES,
                            max_cycles=settings.openclaw_feedback_retry_max_cycles,
                            outer_batch_size=settings.openclaw_feedback_retry_outer_batch_size,
                            reuse_existing=True,
                        ),
                    )
                except Exception as exc:
                    feedback_followup_error = str(exc)

                metadata = {
                    **metadata,
                    'feedback_followup': feedback_followup,
                    'feedback_followup_error': feedback_followup_error,
                    'last_progress_at': _utcnow().isoformat(),
                }
                self.repository.complete_refresh_run(
                    run_id,
                    status=status,
                    symbols_checked=symbols_checked,
                    symbols_missing=symbols_missing,
                    symbols_stale=symbols_stale,
                    symbols_fresh=symbols_fresh,
                    symbols_refreshed=symbols_refreshed,
                    symbols_failed=symbols_failed,
                    events_detected=events_detected,
                    events_captured=events_captured,
                    events_failed=events_failed,
                    run_metadata=metadata,
                )

            return {
                'triggered': True,
                'run_id': run_id,
                'status': status,
                'market_date': resolved_market_date.isoformat(),
                'fs_div': resolved_fs_div,
                'active_symbols_total': len(active_symbols),
                'symbol_offset': symbol_offset,
                'symbol_limit': symbol_limit,
                'symbols_selected': len(selected_stock_codes),
                'symbols_checked': symbols_checked,
                'symbols_missing': symbols_missing,
                'symbols_stale': symbols_stale,
                'symbols_fresh': symbols_fresh,
                'symbols_refreshed': symbols_refreshed,
                'symbols_failed': symbols_failed,
                'events_detected': events_detected,
                'events_captured': events_captured,
                'events_failed': events_failed,
                'partial_reasons': partial_reasons,
                'stale_refresh_run_ids_failed': stale_refresh_run_ids,
                'sync': {
                    'run_count': len(sync_results),
                    'runs': sync_results,
                    'financial_report_events': sum(
                        int(item.get('financial_report_events') or 0) for item in sync_results
                    ),
                },
                'capture': capture_result,
                'retry_cycles_requested': resolved_retry_cycles,
                'retry_cycles_completed': retry_cycles_completed,
                'remaining_retryable_events': remaining_candidate_events,
                'over_limit_event_count': len(over_limit_event_ids),
                'samples': {
                    'lookup_errors': _limited_sample(lookup_errors),
                    'stale_symbols': _limited_sample(sorted(stale_symbols)),
                    'missing_candidate_symbols': _limited_sample(sorted(missing_candidate_symbols)),
                    'refreshed_symbols': _limited_sample(sorted(refreshed_symbols)),
                    'failed_symbols': _limited_sample(sorted(lookup_failed_symbols | failed_symbols)),
                    'terminal_no_data_symbols': _limited_sample(
                        sorted(terminal_symbols_by_status.get(CAPTURE_STATUS_NO_DATA, set()))
                    ),
                    'terminal_mapping_failed_symbols': _limited_sample(
                        sorted(terminal_symbols_by_status.get(CAPTURE_STATUS_MAPPING_FAILED, set()))
                    ),
                },
                'operator_buckets': operator_buckets,
                'piotroski_filter': piotroski_filter_followup,
                'piotroski_filter_error': piotroski_filter_followup_error,
                'feedback_followup': feedback_followup,
                'feedback_followup_error': feedback_followup_error,
            }
        except Exception as exc:
            self.repository.complete_refresh_run(
                run_id,
                status='failed',
                symbols_checked=symbols_checked,
                symbols_missing=symbols_missing,
                symbols_stale=symbols_stale,
                symbols_fresh=symbols_fresh,
                symbols_refreshed=symbols_refreshed,
                symbols_failed=max(symbols_failed, len(lookup_failed_symbols)),
                events_detected=events_detected,
                events_captured=events_captured,
                events_failed=events_failed,
                error_message=str(exc),
                run_metadata={
                    **run_metadata,
                    'lookup_error_count': len(lookup_errors),
                    'lookup_or_mapping_errors': _limited_sample(lookup_errors),
                    'operator_buckets': _build_operator_buckets(
                        lookup_errors=lookup_errors,
                        capture_results=[],
                        terminal_resolved_symbols=terminal_resolved_symbols,
                    ),
                    'sync_runs': sync_results,
                    'symbol_results_sample': _limited_sample(
                        [symbol_results[stock_code] for stock_code in sorted(symbol_results)],
                        limit=100,
                    ),
                    'last_progress_at': _utcnow().isoformat(),
                },
            )
            raise

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

    def inspect_daily_refresh_status(
        self,
        *,
        runs_limit: int = 10,
        market_date: date | None = None,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        resolved_fs_div = fs_div.strip().upper() if fs_div else None
        return {
            'latest_run': self.repository.get_latest_refresh_run(
                market_date=market_date,
                fs_div=resolved_fs_div,
            ),
            'runs': self.repository.list_refresh_runs(
                limit=runs_limit,
                market_date=market_date,
                fs_div=resolved_fs_div,
            ),
        }


dart_fundamentals_capture_service = DartFundamentalsCaptureService()
