from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from app.core.settings import settings
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

FS_DIV_TO_STATEMENT_TYPE = {
    'CFS': 'consolidated',
    'OFS': 'separate',
}

STATEMENT_TYPE_TO_FS_DIV = {
    'consolidated': 'CFS',
    'separate': 'OFS',
}

HIGHLIGHT_ACCOUNT_MATCHERS = {
    'revenue': {
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'ifrs-full_Revenue',
            'ifrs_Revenue',
            'ifrs-full_GrossRevenue',
            'ifrs-full_RevenueFromContractsWithCustomers',
            'ifrs-full_RevenueFromInterest',
            'ifrs-full_InterestRevenueExpense',
            'ifrs-full_InsuranceRevenue',
        },
        'account_names': {
            '매출액',
            '영업수익',
            'Revenue',
            '이자수익',
            '순이자손익',
            '보험영업수익',
        },
        'account_name_contains': {
            '수익(매출액)',
        },
        'account_name_excludes': {
            '매출원가',
            '매출총이익',
            '기타수익',
            '금융수익',
            '수수료수익',
            '기타영업수익',
        },
    },
    'operating_income': {
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'dart_OperatingIncomeLoss',
            'ifrs-full_ProfitLossFromOperatingActivities',
            'ifrs_ProfitLossFromOperatingActivities',
        },
        'account_names': {'영업이익', '영업손실', '영업이익(손실)', '영업손익', 'Operating income'},
    },
    'net_income': {
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'ifrs-full_ProfitLoss',
            'ifrs_ProfitLoss',
            'ifrs-full_ProfitLossAttributableToOwnersOfParent',
        },
        'account_names': {'당기순이익', '당기순손실', '분기순이익', '반기순이익', 'Profit (loss)', '당기순이익(손실)'},
    },
    'total_assets': {
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Assets', 'ifrs_Assets'},
        'account_names': {'자산총계', 'Assets'},
    },
    'total_liabilities': {
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Liabilities', 'ifrs_Liabilities'},
        'account_names': {'부채총계', 'Liabilities'},
    },
    'total_equity': {
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Equity', 'ifrs_Equity'},
        'account_names': {'자본총계', 'Equity'},
    },
    'operating_cash_flow': {
        'statement_types': {'CF'},
        'account_ids': {
            'ifrs-full_CashFlowsFromUsedInOperatingActivities',
            'ifrs-full_NetCashFlowsFromUsedInOperatingActivities',
        },
        'account_names': {
            '영업활동으로 인한 현금흐름',
            '영업활동현금흐름',
            '영업활동순현금흐름',
            'Cash flows from operating activities',
        },
    },
}

_MATCH_NORMALIZATION_PATTERN = re.compile(r'[\s\-\_\(\)\[\]\{\}/\\,.:;]+')


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

    async def get_fundamentals(
        self,
        *,
        corp_code: str | None = None,
        stock_code: str | None = None,
        corp_name: str | None = None,
        business_year: int,
        period: str | None = None,
        report_code: str | None = None,
        statement_type: str | None = None,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        resolved_report_code = self._resolve_report_code(period=period, report_code=report_code)
        resolved_fs_div, normalized_statement_type = self._resolve_fs_div(
            statement_type=statement_type,
            fs_div=fs_div,
        )
        report_meta = REPORT_CODE_METADATA[resolved_report_code]

        corp = await self.client.lookup_corp_code(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
        )
        company = await self.client.fetch_company_overview(corp['corp_code'])
        statements = await self.client.fetch_single_company_financial_statements(
            corp_code=corp['corp_code'],
            bsns_year=str(business_year),
            reprt_code=resolved_report_code,
            fs_div=resolved_fs_div,
        )
        dataset = build_fundamentals_dataset(statements)

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
                'business_year': business_year,
                'fiscal_year': str(business_year),
                'period': report_meta['period'],
                'report_code': resolved_report_code,
                'report_label': report_meta['label'],
                'report_name': report_meta['label'],
                'period_scope': report_meta['scope'],
                'quarter': report_meta['quarter'],
                'statement_type': normalized_statement_type,
                'fs_div': resolved_fs_div,
                'fs_label': FS_DIV_TO_STATEMENT_TYPE[resolved_fs_div],
                'dataset': 'fnlttSinglAcntAll',
            },
            'highlights': dataset['highlights'],
            'statements': {
                'status': statements.get('status'),
                'message': statements.get('message'),
                'currency': dataset['currency'],
                'count': len(dataset['items']),
                'items': dataset['items'],
            },
            'source': {
                'status': statements.get('status'),
                'message': statements.get('message'),
            },
        }

    async def get_company_fundamentals(
        self,
        *,
        corp_code: str | None = None,
        stock_code: str | None = None,
        corp_name: str | None = None,
        fiscal_year: int,
        period: str | None = None,
        report_code: str | None = None,
        statement_type: str | None = None,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        return await self.get_fundamentals(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
            business_year=fiscal_year,
            period=period,
            report_code=report_code,
            statement_type=statement_type,
            fs_div=fs_div,
        )

    async def get_filing_document(self, *, rcept_no: str) -> dict[str, Any]:
        document = await self.client.download_document(rcept_no=rcept_no)
        return {
            'rcept_no': rcept_no,
            'artifact_type': 'original_document',
            'content': document['content'],
            'content_type': document['content_type'],
            'filename': document['filename'],
            'size_bytes': document['size_bytes'],
        }

    async def get_filing_xbrl(self, *, rcept_no: str, report_code: str | None = None) -> dict[str, Any]:
        resolved_report_code = self._resolve_report_code(period=None, report_code=report_code) if report_code else None
        document = await self.client.download_xbrl(rcept_no=rcept_no, reprt_code=resolved_report_code)
        return {
            'rcept_no': rcept_no,
            'artifact_type': 'xbrl',
            'report_code': resolved_report_code,
            'content': document['content'],
            'content_type': document['content_type'],
            'filename': document['filename'],
            'size_bytes': document['size_bytes'],
        }

    @staticmethod
    def _resolve_report_code(*, period: str | None, report_code: str | None) -> str:
        normalized_report_code = (report_code or '').strip()
        normalized_period = (period or '').strip().lower()

        if normalized_report_code:
            if normalized_report_code not in REPORT_CODE_METADATA:
                raise ValueError('report_code must be one of 11011, 11012, 11013, 11014')
            if not normalized_period:
                return normalized_report_code

            inferred_report_code = PERIOD_TO_REPORT_CODE.get(normalized_period)
            if inferred_report_code is None:
                raise ValueError('period must map to one of: annual, q1, half, q3')
            if normalized_report_code != inferred_report_code:
                raise ValueError('period and report_code refer to different filing windows')
            return normalized_report_code

        if normalized_period:
            inferred_report_code = PERIOD_TO_REPORT_CODE.get(normalized_period)
            if inferred_report_code is None:
                raise ValueError('period must map to one of: annual, q1, half, q3')
            return inferred_report_code

        default_report_code = settings.dart_fundamentals_default_report_code.strip()
        if default_report_code not in REPORT_CODE_METADATA:
            raise ValueError('report_code must be one of 11011, 11012, 11013, 11014')
        return default_report_code

    @staticmethod
    def _resolve_fs_div(*, statement_type: str | None, fs_div: str | None) -> tuple[str, str]:
        normalized_statement_type = (statement_type or '').strip().lower()
        normalized_fs_div = (fs_div or '').strip().upper()

        if normalized_statement_type:
            inferred_fs_div = STATEMENT_TYPE_TO_FS_DIV.get(normalized_statement_type)
            if inferred_fs_div is None:
                raise ValueError('statement_type must be one of: consolidated, separate')
            if normalized_fs_div and normalized_fs_div != inferred_fs_div:
                raise ValueError('statement_type and fs_div refer to different statement scopes')
            return inferred_fs_div, normalized_statement_type

        if normalized_fs_div:
            if normalized_fs_div not in FS_DIV_TO_STATEMENT_TYPE:
                raise ValueError('fs_div must be one of CFS or OFS')
            return normalized_fs_div, FS_DIV_TO_STATEMENT_TYPE[normalized_fs_div]

        default_fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()
        if default_fs_div not in FS_DIV_TO_STATEMENT_TYPE:
            raise ValueError('fs_div must be one of CFS or OFS')
        return default_fs_div, FS_DIV_TO_STATEMENT_TYPE[default_fs_div]


def build_fundamentals_dataset(statements: dict[str, Any]) -> dict[str, Any]:
    items = [_normalize_statement_item(item) for item in statements.get('list', [])]
    return {
        'items': items,
        'highlights': _build_highlights(items),
        'currency': next((item['currency'] for item in items if item.get('currency')), None),
    }


def _normalize_statement_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        'receipt_number': item.get('rcept_no'),
        'report_code': item.get('reprt_code'),
        'business_year': item.get('bsns_year'),
        'statement_type': item.get('sj_div'),
        'statement_name': item.get('sj_nm'),
        'account_id': item.get('account_id'),
        'account_name': item.get('account_nm'),
        'account_detail': item.get('account_detail'),
        'current_period_name': item.get('thstrm_nm'),
        'current_amount': _parse_int(item.get('thstrm_amount')),
        'current_cumulative_amount': _parse_int(item.get('thstrm_add_amount')),
        'prior_period_name': item.get('frmtrm_nm'),
        'prior_amount': _parse_int(item.get('frmtrm_amount')),
        'prior_quarter_name': item.get('frmtrm_q_nm'),
        'prior_quarter_amount': _parse_int(item.get('frmtrm_q_amount')),
        'prior_cumulative_amount': _parse_int(item.get('frmtrm_add_amount')),
        'pre_prior_period_name': item.get('bfefrmtrm_nm'),
        'pre_prior_amount': _parse_int(item.get('bfefrmtrm_amount')),
        'order': _parse_int(item.get('ord')),
        'currency': item.get('currency'),
        'raw': item,
    }


def _build_highlights(items: list[dict[str, Any]]) -> dict[str, Any]:
    highlights: dict[str, Any] = {}
    matched_items: dict[str, dict[str, Any] | None] = {}

    for key, matcher in HIGHLIGHT_ACCOUNT_MATCHERS.items():
        matched_item = _find_statement_item(
            items,
            statement_types=matcher.get('statement_types'),
            account_ids=matcher['account_ids'],
            account_names=matcher['account_names'],
            account_name_contains=matcher.get('account_name_contains'),
            account_name_excludes=matcher.get('account_name_excludes'),
        )
        matched_items[key] = matched_item
        if matched_item is None:
            highlights[key] = None
            continue

        highlights[key] = {
            'statement_type': matched_item.get('statement_type'),
            'statement_name': matched_item.get('statement_name'),
            'account_id': matched_item.get('account_id'),
            'account_name': matched_item.get('account_name'),
            'current_amount': matched_item.get('current_amount'),
            'current_cumulative_amount': matched_item.get('current_cumulative_amount'),
            'prior_amount': matched_item.get('prior_amount'),
            'prior_cumulative_amount': matched_item.get('prior_cumulative_amount'),
            'pre_prior_amount': matched_item.get('pre_prior_amount'),
            'currency': matched_item.get('currency'),
        }

    revenue = _to_decimal((matched_items.get('revenue') or {}).get('current_amount'))
    operating_income = _to_decimal((matched_items.get('operating_income') or {}).get('current_amount'))
    net_income = _to_decimal((matched_items.get('net_income') or {}).get('current_amount'))
    liabilities = _to_decimal((matched_items.get('total_liabilities') or {}).get('current_amount'))
    equity = _to_decimal((matched_items.get('total_equity') or {}).get('current_amount'))

    highlights['ratios'] = {
        'operating_margin': _safe_ratio(operating_income, revenue),
        'net_margin': _safe_ratio(net_income, revenue),
        'debt_ratio': _safe_ratio(liabilities, equity),
    }
    return highlights


def _find_statement_item(
    items: list[dict[str, Any]],
    *,
    statement_types: set[str] | None = None,
    account_ids: set[str],
    account_names: set[str],
    account_name_contains: set[str] | None = None,
    account_name_excludes: set[str] | None = None,
) -> dict[str, Any] | None:
    normalized_statement_types = {(value or '').strip().upper() for value in (statement_types or set()) if value}
    normalized_account_ids = {_normalize_match_text(value) for value in account_ids if value}
    normalized_account_names = {_normalize_match_text(value) for value in account_names if value}
    normalized_account_name_contains = {
        _normalize_match_text(value) for value in (account_name_contains or set()) if value
    }
    normalized_account_name_excludes = {
        _normalize_match_text(value) for value in (account_name_excludes or set()) if value
    }

    best_match: dict[str, Any] | None = None
    best_score: tuple[int, int, int, int, int] | None = None

    for item in items:
        score = _score_statement_item(
            item,
            statement_types=normalized_statement_types,
            account_ids=normalized_account_ids,
            account_names=normalized_account_names,
            account_name_contains=normalized_account_name_contains,
            account_name_excludes=normalized_account_name_excludes,
        )
        if score is None:
            continue
        if best_score is None or score > best_score:
            best_match = item
            best_score = score
    return best_match


def _score_statement_item(
    item: dict[str, Any],
    *,
    statement_types: set[str],
    account_ids: set[str],
    account_names: set[str],
    account_name_contains: set[str],
    account_name_excludes: set[str],
) -> tuple[int, int, int, int, int] | None:
    account_id = _normalize_match_text(item.get('account_id'))
    account_name = _normalize_match_text(item.get('account_name'))
    account_detail = _normalize_match_text(item.get('account_detail'))
    statement_type = (item.get('statement_type') or '').strip().upper()

    id_match = account_id in account_ids if account_id else False
    exact_name_match = account_name in account_names if account_name else False
    contains_name_match = bool(account_name) and any(token in account_name for token in account_name_contains)
    excluded_name_match = bool(account_name) and any(token in account_name for token in account_name_excludes)

    if not id_match and not exact_name_match and not contains_name_match:
        return None

    score = 0
    if statement_types:
        if statement_type in statement_types:
            score += 40
        else:
            score -= 40
    if id_match:
        score += 120
    if exact_name_match:
        score += 100
    if contains_name_match:
        score += 70
    if excluded_name_match:
        score -= 80
    if item.get('current_amount') is not None:
        score += 10
    if item.get('current_cumulative_amount') is not None:
        score += 5
    if account_detail in {'', '-', 'none'}:
        score += 8
    elif 'member' in account_detail or '구성요소' in account_detail:
        score -= 12

    return (
        score,
        1 if statement_type in statement_types else 0,
        1 if item.get('current_amount') is not None else 0,
        1 if account_detail in {'', '-', 'none'} else 0,
        -((item.get('order') or 10**9)),
    )


def _normalize_match_text(value: Any) -> str:
    if value in (None, ''):
        return ''
    normalized = _MATCH_NORMALIZATION_PATTERN.sub('', str(value)).strip().lower()
    return normalized


def _parse_int(value: Any) -> int | None:
    if value in (None, ''):
        return None
    if isinstance(value, int):
        return value

    normalized = str(value).replace(',', '').strip()
    if not normalized or normalized == '-':
        return None
    try:
        return int(normalized)
    except ValueError:
        return None


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ''):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _safe_ratio(numerator: Decimal | None, denominator: Decimal | None) -> str | None:
    if numerator is None or denominator in (None, Decimal('0')):
        return None
    return format((numerator / denominator).quantize(Decimal('0.0001')), 'f')


dart_service = DartService()
