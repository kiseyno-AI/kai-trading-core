from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.settings import settings
from app.dart.fundamentals_store import DartFundamentalsRepository, dart_fundamentals_repository
from app.dart.service import (
    FS_DIV_TO_STATEMENT_TYPE,
    REPORT_CODE_METADATA,
    build_fundamentals_dataset,
)
from app.integrations.dart.client import DartClient, dart_client

_REPORT_PERIOD_PATTERN = re.compile(r'\((\d{4})\.(\d{2})\)')

_FILING_CATEGORY_TO_REPORT_CODE = {
    'annual_report': '11011',
    'semiannual_report': '11012',
}

_QUARTER_END_MONTH_TO_REPORT_CODE = {
    3: '11013',
    9: '11014',
}


def infer_fundamentals_request(event: dict[str, Any], *, fs_div: str) -> dict[str, Any]:
    filing_category = event['filing_category']
    report_name = event['report_nm']
    period_match = _REPORT_PERIOD_PATTERN.search(report_name)
    if period_match is None:
        raise ValueError(f'Unable to infer report period from disclosure name: {report_name}')

    business_year = int(period_match.group(1))
    period_end_month = int(period_match.group(2))

    if filing_category == 'quarterly_report':
        report_code = _QUARTER_END_MONTH_TO_REPORT_CODE.get(period_end_month)
        if report_code is None:
            raise ValueError(f'Unsupported quarterly period month for fundamentals capture: {period_end_month:02d}')
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


def build_row_payloads(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payloads = []
    for item in items:
        row_key = '|'.join(
            [
                item.get('statement_type') or '',
                item.get('account_id') or '',
                item.get('account_name') or '',
                str(item.get('order') or ''),
            ]
        )
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


class DartFundamentalsCaptureService:
    def __init__(
        self,
        *,
        client: DartClient | None = None,
        repository: DartFundamentalsRepository | None = None,
    ) -> None:
        self.client = client or dart_client
        self.repository = repository or dart_fundamentals_repository

    async def capture_recent_fundamentals(
        self,
        *,
        limit: int = 20,
        force: bool = False,
        trigger: str = 'manual_api',
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        run_id = self.repository.create_capture_run(
            trigger=trigger,
            limit_requested=limit,
            force=force,
            fs_div=resolved_fs_div,
            run_metadata={'requested_limit': limit},
        )
        events_seen = 0
        events_captured = 0
        events_failed = 0
        events_skipped = 0
        results: list[dict[str, Any]] = []

        try:
            events = self.repository.list_capture_candidates(limit=limit, force=force)
            for event in events:
                events_seen += 1
                try:
                    request = infer_fundamentals_request(event, fs_div=resolved_fs_div)
                    source_payload = await self.client.fetch_financial_statements(
                        corp_code=event['corp_code'],
                        bsns_year=str(request['business_year']),
                        reprt_code=request['report_code'],
                        fs_div=resolved_fs_div,
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
                            'fs_div': resolved_fs_div,
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
                                'fs_div': resolved_fs_div,
                                'rcept_no': event['rcept_no'],
                            },
                            'response_payload': source_payload,
                        },
                        row_payloads=build_row_payloads(dataset['items']),
                        highlight_payloads=build_highlight_payloads(dataset['highlights']),
                    )
                    if source_payload.get('status') != '000':
                        raise ValueError(f"DART fundamentals returned [{source_payload.get('status')}]: {source_payload.get('message')}")
                    self.repository.mark_event_capture_success(event['id'])
                    events_captured += 1
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
                except Exception as exc:
                    self.repository.mark_event_capture_failed(event['id'], error_message=str(exc))
                    events_failed += 1
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
            self.repository.complete_capture_run(
                run_id,
                status='completed',
                events_seen=events_seen,
                events_captured=events_captured,
                events_failed=events_failed,
                events_skipped=events_skipped,
                run_metadata={'requested_limit': limit},
            )
            return result
        except Exception as exc:
            self.repository.complete_capture_run(
                run_id,
                status='failed',
                events_seen=events_seen,
                events_captured=events_captured,
                events_failed=events_failed,
                events_skipped=events_skipped,
                error_message=str(exc),
                run_metadata={'requested_limit': limit},
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


dart_fundamentals_capture_service = DartFundamentalsCaptureService()
