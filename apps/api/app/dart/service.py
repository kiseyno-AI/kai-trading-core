from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from app.integrations.dart.client import DartClient, dart_client

REPORT_CODE_METADATA = {
    '11011': {'period': 'annual', 'label': 'annual_report', 'scope': 'full_year', 'quarter': 4},
    '11013': {'period': 'q1', 'label': 'first_quarter_report', 'scope': 'year_to_date', 'quarter': 1},
    '11012': {'period': 'half', 'label': 'semiannual_report', 'scope': 'year_to_date', 'quarter': 2},
    '11014': {'period': 'q3', 'label': 'third_quarter_report', 'scope': 'year_to_date', 'quarter': 3},
}

PERIOD_TO_REPORT_CODE = {
    'annual': '11011',
    'year': '11011',
    'yearly': '11011',
    'fy': '11011',
    'q1': '11013',
    'quarter1': '11013',
    'first_quarter': '11013',
    'first-quarter': '11013',
    'half': '11012',
    'h1': '11012',
    'semiannual': '11012',
    'semi-annual': '11012',
    'half_year': '11012',
    'half-year': '11012',
    'q3': '11014',
    'quarter3': '11014',
    'third_quarter': '11014',
    'third-quarter': '11014',
}

STATEMENT_TYPE_TO_FS_DIV = {
    'consolidated': 'CFS',
    'separate': 'OFS',
}

HIGHLIGHT_ACCOUNT_MATCHERS = {
    'revenue': {
        'account_ids': {'ifrs-full_Revenue', 'ifrs_Revenue', 'ifrs-full_GrossRevenue'},
        'account_names': {'매출액', '영업수익'},
    },
    'operating_income': {
        'account_ids': {'dart_OperatingIncomeLoss'},
        'account_names': {'영업이익', '영업손실'},
    },
    'net_income': {
        'account_ids': {'ifrs-full_ProfitLoss', 'ifrs_ProfitLoss'},
        'account_names': {'당기순이익', '당기순손실', '분기순이익', '반기순이익'},
    },
    'total_assets': {
        'account_ids': {'ifrs-full_Assets', 'ifrs_Assets'},
        'account_names': {'자산총계'},
    },
    'total_liabilities': {
        'account_ids': {'ifrs-full_Liabilities', 'ifrs_Liabilities'},
        'account_names': {'부채총계'},
    },
    'total_equity': {
        'account_ids': {'ifrs-full_Equity', 'ifrs_Equity'},
        'account_names': {'자본총계'},
    },
}


class DartService:
    def __init__(self, client: DartClient | None = None) -> None:
        self.client = client or dart_client

    async def get_company_snapshot(
        self,
        *,
        corp_code: str | None = None,
        stock_code: str | None = None,
        corp_name: str | None = None,
        filing_days: int = 365,
        filings_limit: int = 10,
    ) -> dict[str, Any]:
        corp = await self.client.lookup_corp_code(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
        )

        today = date.today()
        filing_start = today - timedelta(days=max(filing_days, 1))
        company = await self.client.fetch_company_overview(corp['corp_code'])
        filings = await self.client.fetch_filings(
            corp_code=corp['corp_code'],
            bgn_de=filing_start.strftime('%Y%m%d'),
            end_de=today.strftime('%Y%m%d'),
            page_count=min(max(filings_limit, 1), 100),
        )

        return {
            'lookup': {
                'corp_code': corp['corp_code'],
                'corp_name': corp['corp_name'],
                'stock_code': corp['stock_code'],
                'modify_date': corp['modify_date'],
            },
            'company': {
                'corp_code': corp['corp_code'],
                'corp_name': company.get('corp_name', corp['corp_name']),
                'corp_name_eng': company.get('corp_name_eng'),
                'stock_code': company.get('stock_code', corp['stock_code']),
                'ceo_name': company.get('ceo_nm'),
                'corp_cls': company.get('corp_cls'),
                'business_registration_number': company.get('bizr_no'),
                'jurir_no': company.get('jurir_no'),
                'address': company.get('adres'),
                'homepage_url': company.get('hm_url'),
                'ir_url': company.get('ir_url'),
                'phone_number': company.get('phn_no'),
                'fax_number': company.get('fax_no'),
                'industry_code': company.get('induty_code'),
                'establishment_date': company.get('est_dt'),
                'fiscal_year_end_month': company.get('acc_mt'),
            },
            'recent_filings': {
                'total_count': filings.get('total_count'),
                'page_count': filings.get('page_count'),
                'page_no': filings.get('page_no'),
                'items': filings.get('list', []),
            },
        }

    async def get_company_fundamentals(
        self,
        *,
        corp_code: str | None = None,
        stock_code: str | None = None,
        corp_name: str | None = None,
        fiscal_year: int,
        period: str | None = 'annual',
        report_code: str | None = None,
        statement_type: str = 'consolidated',
    ) -> dict[str, Any]:
        resolved_report_code = self._resolve_report_code(period=period, report_code=report_code)

        normalized_statement_type = statement_type.strip().lower()
        fs_div = STATEMENT_TYPE_TO_FS_DIV.get(normalized_statement_type)
        if fs_div is None:
            raise ValueError('statement_type must be one of: consolidated, separate')

        corp = await self.client.lookup_corp_code(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
        )
        company = await self.client.fetch_company_overview(corp['corp_code'])
        statements = await self.client.fetch_financial_statements(
            corp_code=corp['corp_code'],
            bsns_year=str(fiscal_year),
            reprt_code=resolved_report_code,
            fs_div=fs_div,
        )

        normalized_rows = [_normalize_financial_statement_row(row) for row in statements.get('list', [])]
        highlights = _build_highlights(normalized_rows)
        report_meta = REPORT_CODE_METADATA[resolved_report_code]

        return {
            'lookup': {
                'corp_code': corp['corp_code'],
                'corp_name': corp['corp_name'],
                'stock_code': corp['stock_code'],
                'modify_date': corp['modify_date'],
            },
            'company': {
                'corp_code': corp['corp_code'],
                'corp_name': company.get('corp_name', corp['corp_name']),
                'corp_name_eng': company.get('corp_name_eng'),
                'stock_code': company.get('stock_code', corp['stock_code']),
                'corp_cls': company.get('corp_cls'),
            },
            'request': {
                'fiscal_year': str(fiscal_year),
                'period': report_meta['period'],
                'report_code': resolved_report_code,
                'report_label': report_meta['label'],
                'period_scope': report_meta['scope'],
                'quarter': report_meta['quarter'],
                'statement_type': normalized_statement_type,
                'fs_div': fs_div,
            },
            'highlights': highlights,
            'statements': {
                'count': len(normalized_rows),
                'items': normalized_rows,
            },
            'source': {
                'status': statements.get('status'),
                'message': statements.get('message'),
            },
        }

    @staticmethod
    def _resolve_report_code(*, period: str | None, report_code: str | None) -> str:
        normalized_period = (period or 'annual').strip().lower()
        inferred_report_code = PERIOD_TO_REPORT_CODE.get(normalized_period)
        if inferred_report_code is None:
            raise ValueError('period must map to one of: annual, q1, half, q3')

        if report_code is None:
            return inferred_report_code

        normalized_report_code = report_code.strip()
        if normalized_report_code not in REPORT_CODE_METADATA:
            raise ValueError('report_code must be one of: 11011, 11012, 11013, 11014')
        if normalized_report_code != inferred_report_code:
            raise ValueError('period and report_code refer to different filing windows')
        return normalized_report_code


def _normalize_financial_statement_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'account_id': row.get('account_id'),
        'account_name': row.get('account_nm'),
        'statement_section': row.get('sj_nm'),
        'statement_type': row.get('fs_nm'),
        'currency': row.get('currency'),
        'current_amount': _decimal_to_string(_parse_decimal(row.get('thstrm_amount'))),
        'prior_amount': _decimal_to_string(_parse_decimal(row.get('frmtrm_amount'))),
        'prior_prior_amount': _decimal_to_string(_parse_decimal(row.get('bfefrmtrm_amount'))),
        'current_label': row.get('thstrm_nm'),
        'prior_label': row.get('frmtrm_nm'),
        'prior_prior_label': row.get('bfefrmtrm_nm'),
        'raw': row,
    }


def _build_highlights(rows: list[dict[str, Any]]) -> dict[str, Any]:
    highlights: dict[str, Any] = {}
    matched_rows: dict[str, dict[str, Any] | None] = {}

    for key, matcher in HIGHLIGHT_ACCOUNT_MATCHERS.items():
        matched_row = _find_statement_row(
            rows,
            account_ids=matcher['account_ids'],
            account_names=matcher['account_names'],
        )
        matched_rows[key] = matched_row
        if matched_row is None:
            highlights[key] = None
            continue

        highlights[key] = {
            'account_id': matched_row.get('account_id'),
            'account_name': matched_row.get('account_name'),
            'currency': matched_row.get('currency'),
            'current_amount': matched_row.get('current_amount'),
            'prior_amount': matched_row.get('prior_amount'),
            'prior_prior_amount': matched_row.get('prior_prior_amount'),
        }

    revenue = _parse_decimal((matched_rows.get('revenue') or {}).get('current_amount'))
    operating_income = _parse_decimal((matched_rows.get('operating_income') or {}).get('current_amount'))
    net_income = _parse_decimal((matched_rows.get('net_income') or {}).get('current_amount'))
    liabilities = _parse_decimal((matched_rows.get('total_liabilities') or {}).get('current_amount'))
    equity = _parse_decimal((matched_rows.get('total_equity') or {}).get('current_amount'))

    highlights['ratios'] = {
        'operating_margin': _safe_ratio(operating_income, revenue),
        'net_margin': _safe_ratio(net_income, revenue),
        'debt_ratio': _safe_ratio(liabilities, equity),
    }
    return highlights


def _find_statement_row(
    rows: list[dict[str, Any]],
    *,
    account_ids: set[str],
    account_names: set[str],
) -> dict[str, Any] | None:
    for row in rows:
        if (row.get('account_id') or '').strip() in account_ids:
            return row
    for row in rows:
        if (row.get('account_name') or '').strip() in account_names:
            return row
    return None


def _safe_ratio(numerator: Decimal | None, denominator: Decimal | None) -> str | None:
    if numerator is None or denominator in (None, Decimal('0')):
        return None
    return _decimal_to_string((numerator / denominator).quantize(Decimal('0.0001')))


def _parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ''):
        return None
    normalized = str(value).replace(',', '').strip()
    if not normalized or normalized == '-':
        return None
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


dart_service = DartService()
