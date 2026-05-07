from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any

from app.core.audit_logging import log_fetch_event
from app.dart.fundamentals_store import DartFundamentalsRepository, dart_fundamentals_repository
from app.dart.service import (
    FS_DIV_TO_STATEMENT_TYPE,
    REPORT_CODE_METADATA,
    build_fundamental_context_from_highlights,
    classify_disclosure_event,
)


class DartFundamentalsHistoryService:
    def __init__(
        self,
        *,
        repository: DartFundamentalsRepository | None = None,
    ) -> None:
        self.repository = repository or dart_fundamentals_repository

    def get_historical_fundamentals(
        self,
        *,
        symbols: list[str],
        exact_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        resolved_start_date, resolved_end_date = _resolve_date_window(
            exact_date=exact_date,
            start_date=start_date,
            end_date=end_date,
        )
        normalized_fs_div = fs_div.strip().upper() if fs_div else None

        log_fetch_event(
            service='dart',
            operation='get_historical_fundamentals',
            status='started',
            symbol_count=len(normalized_symbols),
            exact_date=exact_date.isoformat() if exact_date else None,
            start_date=resolved_start_date.isoformat() if resolved_start_date else None,
            end_date=resolved_end_date.isoformat() if resolved_end_date else None,
            fs_div=normalized_fs_div,
        )
        stored_items = self.repository.list_stored_fundamentals(
            stock_codes=normalized_symbols,
            fs_div=normalized_fs_div,
        )

        companies: list[dict[str, Any]] = []
        companies_by_symbol: dict[str, dict[str, Any]] = {}
        matched_symbols: set[str] = set()
        total_entries = 0
        for item in stored_items:
            period_end_date = _build_period_end_date(
                business_year=item['business_year'],
                period_end_month=item.get('period_end_month'),
            )
            if not _matches_date_window(
                period_end_date=period_end_date,
                exact_date=exact_date,
                start_date=resolved_start_date,
                end_date=resolved_end_date,
            ):
                continue

            symbol = item.get('stock_code') or ''
            if symbol not in companies_by_symbol:
                company = {
                    'symbol': symbol,
                    'corp_code': item['corp_code'],
                    'corp_name': item['corp_name'],
                    'entries': [],
                }
                companies.append(company)
                companies_by_symbol[symbol] = company
            matched_symbols.add(symbol)
            companies_by_symbol[symbol]['entries'].append(_build_history_entry(item, period_end_date=period_end_date))
            total_entries += 1

        for company in companies:
            company['entries'].sort(
                key=lambda entry: (
                    entry['period']['period_end_date'] or '',
                    entry['filing']['receipt_date'] or '',
                    entry['filing']['rcept_no'],
                ),
                reverse=True,
            )

        missing_symbols = [symbol for symbol in normalized_symbols if symbol not in matched_symbols]
        result = {
            'request': {
                'symbols': normalized_symbols,
                'date': exact_date.isoformat() if exact_date else None,
                'start_date': resolved_start_date.isoformat() if resolved_start_date else None,
                'end_date': resolved_end_date.isoformat() if resolved_end_date else None,
                'fs_div': normalized_fs_div,
                'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(normalized_fs_div) if normalized_fs_div else None,
                'matched_on': 'period_end_date',
            },
            'summary': {
                'requested_symbol_count': len(normalized_symbols),
                'matched_symbol_count': len(matched_symbols),
                'missing_symbols': missing_symbols,
                'entries': total_entries,
            },
            'companies': companies,
        }
        log_fetch_event(
            service='dart',
            operation='get_historical_fundamentals',
            status='completed',
            symbol_count=len(normalized_symbols),
            matched_symbol_count=len(matched_symbols),
            entry_count=total_entries,
            fs_div=normalized_fs_div,
        )
        return result


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise ValueError('At least one symbol is required')
    return normalized


def _resolve_date_window(
    *,
    exact_date: date | None,
    start_date: date | None,
    end_date: date | None,
) -> tuple[date | None, date | None]:
    if exact_date is not None and (start_date is not None or end_date is not None):
        raise ValueError('date cannot be combined with start_date or end_date')
    if exact_date is None and start_date is None and end_date is None:
        raise ValueError('Provide date or at least one of start_date/end_date')
    if start_date and end_date and start_date > end_date:
        raise ValueError('start_date must be on or before end_date')
    if exact_date is not None:
        return exact_date, exact_date
    return start_date, end_date


def _matches_date_window(
    *,
    period_end_date: date | None,
    exact_date: date | None,
    start_date: date | None,
    end_date: date | None,
) -> bool:
    if period_end_date is None:
        return False
    if exact_date is not None:
        return period_end_date == exact_date
    if start_date is not None and period_end_date < start_date:
        return False
    if end_date is not None and period_end_date > end_date:
        return False
    return True


def _build_period_end_date(*, business_year: int, period_end_month: int | None) -> date | None:
    if period_end_month is None:
        return None
    last_day = monthrange(business_year, period_end_month)[1]
    return date(business_year, period_end_month, last_day)


def _build_history_entry(item: dict[str, Any], *, period_end_date: date | None) -> dict[str, Any]:
    report_meta = REPORT_CODE_METADATA.get(item['report_code'], {})
    statement_rows = item['statement_rows']
    highlights = _hydrate_highlights(item['highlights'])
    filing_metadata = {
        'rcept_no': item['rcept_no'],
        'receipt_date': item['disclosure'].get('receipt_date'),
        'captured_at': item.get('captured_at'),
        'report_name': item['disclosure'].get('report_nm'),
        'filing_category': item['filing_category'],
    }
    period_metadata = {
        'business_year': item['business_year'],
        'report_code': item['report_code'],
        'report_label': report_meta.get('label'),
        'period_type': item['period_type'],
        'period_scope': report_meta.get('scope'),
        'quarter': report_meta.get('quarter'),
        'period_end_month': item.get('period_end_month'),
        'period_end_date': period_end_date.isoformat() if period_end_date else None,
    }
    statement_metadata = {
        'fs_div': item['fs_div'],
        'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(item['fs_div']),
        'statement_type': item['statement_type'],
        'currency': item.get('currency'),
        'source_status': item.get('source_status'),
        'source_message': item.get('source_message'),
        'dataset': 'stored_dart_fundamentals',
        'source_provider': 'dart',
    }
    disclosure_flags = classify_disclosure_event(
        report_name=item['disclosure'].get('report_nm'),
        remark=item['disclosure'].get('remark'),
        filing_category=item['filing_category'],
        is_financial_report=True,
    )
    return {
        'filing': {
            'rcept_no': item['rcept_no'],
            'report_name': item['disclosure'].get('report_nm'),
            'filing_category': item['filing_category'],
            'receipt_date': item['disclosure'].get('receipt_date'),
            'captured_at': item.get('captured_at'),
            'is_correction': disclosure_flags['is_correction'],
            'disclosure_tags': disclosure_flags['disclosure_tags'],
        },
        'period': {
            'business_year': item['business_year'],
            'report_code': item['report_code'],
            'report_label': report_meta.get('label'),
            'period_type': item['period_type'],
            'period_scope': report_meta.get('scope'),
            'quarter': report_meta.get('quarter'),
            'period_end_month': item.get('period_end_month'),
            'period_end_date': period_end_date.isoformat() if period_end_date else None,
        },
        'statement': {
            'fs_div': item['fs_div'],
            'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(item['fs_div']),
            'statement_type': item['statement_type'],
            'currency': item.get('currency'),
            'source_status': item.get('source_status'),
            'source_message': item.get('source_message'),
            'row_count': len(statement_rows),
        },
        'highlights': highlights,
        'fundamental_context': build_fundamental_context_from_highlights(
            highlights,
            filing_metadata=filing_metadata,
            period_metadata=period_metadata,
            statement_metadata=statement_metadata,
        ),
        'statements': {
            'count': len(statement_rows),
            'items': statement_rows,
        },
    }


def _hydrate_highlights(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    ratios: dict[str, str | None] = {}
    for row in rows:
        metric_key = row['metric_key']
        if row['metric_type'] == 'ratio':
            ratios[metric_key] = row.get('ratio_value')
            continue
        result[metric_key] = {
            'statement_type': row.get('statement_type'),
            'statement_name': row.get('statement_name'),
            'account_id': row.get('account_id'),
            'account_name': row.get('account_name'),
            'current_amount': row.get('current_amount'),
            'current_cumulative_amount': row.get('current_cumulative_amount'),
            'prior_amount': row.get('prior_amount'),
            'prior_cumulative_amount': row.get('prior_cumulative_amount'),
            'pre_prior_amount': row.get('pre_prior_amount'),
            'currency': row.get('currency'),
        }
    if ratios:
        result['ratios'] = ratios
    return result


dart_fundamentals_history_service = DartFundamentalsHistoryService()
