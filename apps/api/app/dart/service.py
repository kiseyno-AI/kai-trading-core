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

CANONICAL_FACT_MATCHERS: dict[str, dict[str, Any]] = {
    'revenue': {
        'section': 'income_statement',
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
        'account_name_contains': {'수익(매출액)'},
        'account_name_excludes': {
            '매출원가',
            '매출총이익',
            '기타수익',
            '금융수익',
            '수수료수익',
            '기타영업수익',
        },
    },
    'gross_profit': {
        'section': 'income_statement',
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'ifrs-full_GrossProfit',
            'ifrs_GrossProfit',
        },
        'account_names': {
            '매출총이익',
            '매출총손실',
            'Gross profit',
        },
    },
    'operating_income': {
        'section': 'income_statement',
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'dart_OperatingIncomeLoss',
            'ifrs-full_ProfitLossFromOperatingActivities',
            'ifrs_ProfitLossFromOperatingActivities',
        },
        'account_names': {'영업이익', '영업손실', '영업이익(손실)', '영업손익', 'Operating income'},
    },
    'profit_before_tax': {
        'section': 'income_statement',
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'ifrs-full_ProfitLossBeforeTax',
            'ifrs_ProfitLossBeforeTax',
        },
        'account_names': {
            '법인세비용차감전순이익',
            '법인세비용차감전순손실',
            '법인세비용차감전순이익(손실)',
            '세전이익',
            '세전손익',
            'Profit before tax',
        },
    },
    'net_income': {
        'section': 'income_statement',
        'statement_types': {'IS', 'CIS'},
        'account_ids': {
            'ifrs-full_ProfitLoss',
            'ifrs_ProfitLoss',
            'ifrs-full_ProfitLossAttributableToOwnersOfParent',
        },
        'account_names': {
            '당기순이익',
            '당기순손실',
            '분기순이익',
            '반기순이익',
            'Profit (loss)',
            '당기순이익(손실)',
        },
    },
    'total_assets': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Assets', 'ifrs_Assets'},
        'account_names': {'자산총계', 'Assets'},
    },
    'current_assets': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_CurrentAssets', 'ifrs_CurrentAssets'},
        'account_names': {'유동자산', 'Current assets'},
    },
    'cash_and_cash_equivalents': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {
            'ifrs-full_CashAndCashEquivalents',
            'ifrs_CashAndCashEquivalents',
        },
        'account_names': {
            '현금및현금성자산',
            '현금및현금성자산(유동)',
            'Cash and cash equivalents',
        },
    },
    'inventories': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Inventories', 'ifrs_Inventories'},
        'account_names': {'재고자산', 'Inventories'},
    },
    'total_liabilities': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Liabilities', 'ifrs_Liabilities'},
        'account_names': {'부채총계', 'Liabilities'},
    },
    'current_liabilities': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_CurrentLiabilities', 'ifrs_CurrentLiabilities'},
        'account_names': {'유동부채', 'Current liabilities'},
    },
    'total_equity': {
        'section': 'balance_sheet',
        'statement_types': {'BS'},
        'account_ids': {'ifrs-full_Equity', 'ifrs_Equity'},
        'account_names': {'자본총계', 'Equity'},
    },
    'operating_cash_flow': {
        'section': 'cash_flow_statement',
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
    'investing_cash_flow': {
        'section': 'cash_flow_statement',
        'statement_types': {'CF'},
        'account_ids': {
            'ifrs-full_CashFlowsFromUsedInInvestingActivities',
            'ifrs-full_NetCashFlowsFromUsedInInvestingActivities',
        },
        'account_names': {
            '투자활동으로 인한 현금흐름',
            '투자활동현금흐름',
            '투자활동순현금흐름',
            'Cash flows from investing activities',
        },
    },
    'financing_cash_flow': {
        'section': 'cash_flow_statement',
        'statement_types': {'CF'},
        'account_ids': {
            'ifrs-full_CashFlowsFromUsedInFinancingActivities',
            'ifrs-full_NetCashFlowsFromUsedInFinancingActivities',
        },
        'account_names': {
            '재무활동으로 인한 현금흐름',
            '재무활동현금흐름',
            '재무활동순현금흐름',
            'Cash flows from financing activities',
        },
    },
}

CANONICAL_FACT_KEYS_BY_SECTION: dict[str, tuple[str, ...]] = {
    'balance_sheet': (
        'total_assets',
        'current_assets',
        'cash_and_cash_equivalents',
        'inventories',
        'total_liabilities',
        'current_liabilities',
        'total_equity',
    ),
    'income_statement': (
        'revenue',
        'gross_profit',
        'operating_income',
        'profit_before_tax',
        'net_income',
    ),
    'cash_flow_statement': (
        'operating_cash_flow',
        'investing_cash_flow',
        'financing_cash_flow',
    ),
}

DISCLOSURE_CORRECTION_REPORT_TOKENS = (
    '[기재정정]',
    '[첨부정정]',
    '[정정]',
    '기재정정',
    '첨부정정',
    '정정',
)
DISCLOSURE_CORRECTION_REMARK_TOKENS = {'정', '정정', '기재정정', '첨부정정'}
DISCLOSURE_OWNERSHIP_TOKENS = (
    '대량보유',
    '주식등의 대량보유',
    '최대주주',
    '주요주주',
    '임원ㆍ주요주주',
    '소유주식',
    '지분',
    '보유상황',
)
DISCLOSURE_LARGE_HOLDER_TOKENS = (
    '대량보유',
    '5%',
    '주요주주',
)
DISCLOSURE_AUDIT_TOKENS = (
    '감사보고서',
    '검토보고서',
)

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
        filing_metadata = {
            'rcept_no': next(
                (item.get('receipt_number') for item in dataset['items'] if item.get('receipt_number')),
                None,
            ),
            'receipt_date': None,
            'captured_at': None,
            'report_name': report_meta['label'],
            'filing_category': report_meta['label'],
        }
        period_metadata = {
            'business_year': business_year,
            'report_code': resolved_report_code,
            'report_label': report_meta['label'],
            'period_type': report_meta['period'],
            'period_scope': report_meta['scope'],
            'quarter': report_meta['quarter'],
            'period_end_month': None,
            'period_end_date': None,
        }
        statement_metadata = {
            'fs_div': resolved_fs_div,
            'fs_label': FS_DIV_TO_STATEMENT_TYPE[resolved_fs_div],
            'statement_type': normalized_statement_type,
            'currency': dataset['currency'],
            'source_status': statements.get('status'),
            'source_message': statements.get('message'),
            'dataset': 'fnlttSinglAcntAll',
            'source_provider': 'dart',
        }

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
            'fundamental_context': build_fundamental_context_from_highlights(
                dataset['highlights'],
                filing_metadata=filing_metadata,
                period_metadata=period_metadata,
                statement_metadata=statement_metadata,
            ),
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
    highlights = _build_highlights(items)
    return {
        'items': items,
        'highlights': highlights,
        'currency': next((item['currency'] for item in items if item.get('currency')), None),
    }


def build_fundamental_context_from_highlights(
    highlights: dict[str, Any],
    *,
    filing_metadata: dict[str, Any] | None = None,
    period_metadata: dict[str, Any] | None = None,
    statement_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_facts = _build_normalized_facts_from_highlights(highlights)
    return {
        'normalized_facts': normalized_facts,
        'analysis': _build_analysis_from_normalized_facts(normalized_facts),
        'coverage': _build_normalized_fact_coverage(normalized_facts),
        'source_provenance': {
            'provider': (statement_metadata or {}).get('source_provider', 'dart'),
            'dataset': (statement_metadata or {}).get('dataset', 'stored_dart_fundamentals'),
            'rcept_no': (filing_metadata or {}).get('rcept_no'),
            'receipt_date': (filing_metadata or {}).get('receipt_date'),
            'captured_at': (filing_metadata or {}).get('captured_at'),
            'report_name': (filing_metadata or {}).get('report_name'),
            'filing_category': (filing_metadata or {}).get('filing_category'),
            'business_year': (period_metadata or {}).get('business_year'),
            'report_code': (period_metadata or {}).get('report_code'),
            'report_label': (period_metadata or {}).get('report_label'),
            'period_type': (period_metadata or {}).get('period_type'),
            'period_scope': (period_metadata or {}).get('period_scope'),
            'quarter': (period_metadata or {}).get('quarter'),
            'period_end_month': (period_metadata or {}).get('period_end_month'),
            'period_end_date': (period_metadata or {}).get('period_end_date'),
            'fs_div': (statement_metadata or {}).get('fs_div'),
            'fs_label': (statement_metadata or {}).get('fs_label'),
            'statement_type': (statement_metadata or {}).get('statement_type'),
            'currency': (statement_metadata or {}).get('currency'),
            'source_status': (statement_metadata or {}).get('source_status'),
            'source_message': (statement_metadata or {}).get('source_message'),
        },
    }


def classify_disclosure_event(
    *,
    report_name: str | None,
    remark: str | None = None,
    filing_category: str | None = None,
    is_financial_report: bool | None = None,
) -> dict[str, Any]:
    normalized_report_name = (report_name or '').strip()
    normalized_remark = (remark or '').strip()
    report_name_lower = normalized_report_name.lower()
    remark_lower = normalized_remark.lower()
    is_correction = (
        any(token in normalized_report_name for token in DISCLOSURE_CORRECTION_REPORT_TOKENS)
        or normalized_remark in DISCLOSURE_CORRECTION_REMARK_TOKENS
        or any(token in remark_lower for token in ('정정', 'correction'))
    )
    is_ownership_related = any(token in normalized_report_name for token in DISCLOSURE_OWNERSHIP_TOKENS)
    is_large_holder_related = any(token in normalized_report_name for token in DISCLOSURE_LARGE_HOLDER_TOKENS)
    is_audit_related = any(token in normalized_report_name for token in DISCLOSURE_AUDIT_TOKENS)
    tags: list[str] = []
    if is_financial_report:
        tags.append('financial_report')
    if filing_category:
        tags.append(str(filing_category))
    if is_correction:
        tags.append('correction')
    if is_ownership_related:
        tags.append('ownership_related')
    if is_large_holder_related:
        tags.append('large_holder_related')
    if is_audit_related:
        tags.append('audit_related')
    if 'ownership_related' in tags and 'large_holder_related' in tags:
        tags.append('ownership_large_holder_change')
    return {
        'is_correction': is_correction,
        'is_ownership_related': is_ownership_related,
        'is_large_holder_related': is_large_holder_related,
        'is_audit_related': is_audit_related,
        'disclosure_tags': tags,
        'normalized_report_name': report_name_lower or None,
        'normalized_remark': remark_lower or None,
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
    matches = _collect_fact_matches(items)
    highlights = {
        key: _serialize_matched_item(matches.get(key))
        for key in CANONICAL_FACT_MATCHERS
    }

    revenue = _fact_current_decimal(matches.get('revenue'))
    gross_profit = _fact_current_decimal(matches.get('gross_profit'))
    operating_income = _fact_current_decimal(matches.get('operating_income'))
    net_income = _fact_current_decimal(matches.get('net_income'))
    total_assets = _fact_current_decimal(matches.get('total_assets'))
    liabilities = _fact_current_decimal(matches.get('total_liabilities'))
    equity = _fact_current_decimal(matches.get('total_equity'))
    current_assets = _fact_current_decimal(matches.get('current_assets'))
    current_liabilities = _fact_current_decimal(matches.get('current_liabilities'))
    cash = _fact_current_decimal(matches.get('cash_and_cash_equivalents'))
    operating_cash_flow = _fact_current_decimal(matches.get('operating_cash_flow'))

    highlights['ratios'] = {
        'gross_margin': _safe_ratio(gross_profit, revenue),
        'operating_margin': _safe_ratio(operating_income, revenue),
        'net_margin': _safe_ratio(net_income, revenue),
        'return_on_assets': _safe_ratio(net_income, total_assets),
        'return_on_equity': _safe_ratio(net_income, equity),
        'debt_ratio': _safe_ratio(liabilities, equity),
        'liabilities_to_assets': _safe_ratio(liabilities, total_assets),
        'current_ratio': _safe_ratio(current_assets, current_liabilities),
        'cash_ratio': _safe_ratio(cash, current_liabilities),
        'operating_cash_flow_to_net_income': _safe_ratio(operating_cash_flow, net_income),
        'operating_cash_flow_to_operating_income': _safe_ratio(operating_cash_flow, operating_income),
        'operating_cash_flow_margin': _safe_ratio(operating_cash_flow, revenue),
    }
    return highlights


def _collect_fact_matches(items: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    matches: dict[str, dict[str, Any] | None] = {}
    for key, matcher in CANONICAL_FACT_MATCHERS.items():
        matches[key] = _find_statement_item(
            items,
            statement_types=matcher.get('statement_types'),
            account_ids=matcher['account_ids'],
            account_names=matcher['account_names'],
            account_name_contains=matcher.get('account_name_contains'),
            account_name_excludes=matcher.get('account_name_excludes'),
        )
    return matches


def _serialize_matched_item(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if item is None:
        return None
    return {
        'statement_type': item.get('statement_type'),
        'statement_name': item.get('statement_name'),
        'account_id': item.get('account_id'),
        'account_name': item.get('account_name'),
        'current_amount': item.get('current_amount'),
        'current_cumulative_amount': item.get('current_cumulative_amount'),
        'prior_amount': item.get('prior_amount'),
        'prior_cumulative_amount': item.get('prior_cumulative_amount'),
        'pre_prior_amount': item.get('pre_prior_amount'),
        'currency': item.get('currency'),
    }


def _build_normalized_facts_from_highlights(highlights: dict[str, Any]) -> dict[str, dict[str, Any]]:
    blocks: dict[str, dict[str, Any]] = {}
    for section, fact_keys in CANONICAL_FACT_KEYS_BY_SECTION.items():
        blocks[section] = {
            fact_key: _build_fact_snapshot(
                fact_key,
                highlights.get(fact_key),
            )
            for fact_key in fact_keys
        }
    return blocks


def _build_fact_snapshot(fact_key: str, highlight: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(highlight, dict):
        return {
            'status': 'missing',
            'statement_types': sorted(CANONICAL_FACT_MATCHERS[fact_key].get('statement_types') or []),
            'account_id': None,
            'account_name': None,
            'current_value': None,
            'current_value_basis': None,
            'prior_value': None,
            'prior_value_basis': None,
            'current_amount': None,
            'current_cumulative_amount': None,
            'prior_amount': None,
            'prior_cumulative_amount': None,
            'pre_prior_amount': None,
            'currency': None,
        }

    current_value, current_basis = _resolve_highlight_value(highlight, current=True)
    prior_value, prior_basis = _resolve_highlight_value(highlight, current=False)
    return {
        'status': 'available',
        'statement_types': sorted(CANONICAL_FACT_MATCHERS[fact_key].get('statement_types') or []),
        'statement_type': highlight.get('statement_type'),
        'statement_name': highlight.get('statement_name'),
        'account_id': highlight.get('account_id'),
        'account_name': highlight.get('account_name'),
        'current_value': current_value,
        'current_value_basis': current_basis,
        'prior_value': prior_value,
        'prior_value_basis': prior_basis,
        'current_amount': highlight.get('current_amount'),
        'current_cumulative_amount': highlight.get('current_cumulative_amount'),
        'prior_amount': highlight.get('prior_amount'),
        'prior_cumulative_amount': highlight.get('prior_cumulative_amount'),
        'pre_prior_amount': highlight.get('pre_prior_amount'),
        'currency': highlight.get('currency'),
    }


def _resolve_highlight_value(highlight: dict[str, Any], *, current: bool) -> tuple[int | None, str | None]:
    statement_type = str(highlight.get('statement_type') or '').strip().upper()
    if current:
        cumulative_key = 'current_cumulative_amount'
        amount_key = 'current_amount'
    else:
        cumulative_key = 'prior_cumulative_amount'
        amount_key = 'prior_amount'

    if statement_type in {'IS', 'CIS', 'CF'} and highlight.get(cumulative_key) is not None:
        return highlight.get(cumulative_key), cumulative_key
    if highlight.get(amount_key) is not None:
        return highlight.get(amount_key), amount_key
    return highlight.get(cumulative_key), cumulative_key if highlight.get(cumulative_key) is not None else None


def _build_analysis_from_normalized_facts(normalized_facts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    profitability_metrics, profitability_missing = _build_metric_values(
        {
            'gross_margin': ('gross_profit', 'revenue'),
            'operating_margin': ('operating_income', 'revenue'),
            'net_margin': ('net_income', 'revenue'),
            'return_on_assets': ('net_income', 'total_assets'),
            'return_on_equity': ('net_income', 'total_equity'),
        },
        normalized_facts,
    )
    leverage_liquidity_metrics, leverage_liquidity_missing = _build_metric_values(
        {
            'debt_to_equity': ('total_liabilities', 'total_equity'),
            'liabilities_to_assets': ('total_liabilities', 'total_assets'),
            'current_ratio': ('current_assets', 'current_liabilities'),
            'cash_ratio': ('cash_and_cash_equivalents', 'current_liabilities'),
        },
        normalized_facts,
    )
    cash_flow_quality_metrics, cash_flow_quality_missing = _build_metric_values(
        {
            'operating_cash_flow_to_net_income': ('operating_cash_flow', 'net_income'),
            'operating_cash_flow_to_operating_income': ('operating_cash_flow', 'operating_income'),
            'operating_cash_flow_margin': ('operating_cash_flow', 'revenue'),
        },
        normalized_facts,
    )
    return {
        'profitability': _build_metric_section(
            metrics=profitability_metrics,
            missing_inputs=profitability_missing,
        ),
        'leverage_liquidity': _build_metric_section(
            metrics=leverage_liquidity_metrics,
            missing_inputs=leverage_liquidity_missing,
        ),
        'cash_flow_quality': _build_metric_section(
            metrics=cash_flow_quality_metrics,
            missing_inputs=cash_flow_quality_missing,
        ),
    }


def _build_metric_values(
    metric_pairs: dict[str, tuple[str, str]],
    normalized_facts: dict[str, dict[str, Any]],
) -> tuple[dict[str, str | None], list[str]]:
    metrics: dict[str, str | None] = {}
    missing_inputs: set[str] = set()
    for metric_name, (numerator_key, denominator_key) in metric_pairs.items():
        numerator = _normalized_fact_decimal(normalized_facts, numerator_key)
        denominator = _normalized_fact_decimal(normalized_facts, denominator_key)
        if numerator is None:
            missing_inputs.add(numerator_key)
        if denominator in (None, Decimal('0')):
            missing_inputs.add(denominator_key)
        metrics[metric_name] = _safe_ratio(numerator, denominator)
    return metrics, sorted(missing_inputs)


def _build_metric_section(
    *,
    metrics: dict[str, str | None],
    missing_inputs: list[str],
) -> dict[str, Any]:
    available_metric_count = sum(1 for value in metrics.values() if value is not None)
    if available_metric_count == 0:
        status = 'missing'
    elif available_metric_count == len(metrics) and not missing_inputs:
        status = 'available'
    else:
        status = 'partial'
    return {
        'status': status,
        'metrics': metrics,
        'available_metric_count': available_metric_count,
        'metric_count': len(metrics),
        'missing_inputs': missing_inputs,
    }


def _build_normalized_fact_coverage(normalized_facts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    missing_fact_keys: list[str] = []
    mapped_fact_count = 0
    for section, facts in normalized_facts.items():
        for fact_key, fact_snapshot in facts.items():
            if fact_snapshot.get('status') == 'available':
                mapped_fact_count += 1
                continue
            missing_fact_keys.append(f'{section}.{fact_key}')
    total_fact_count = sum(len(facts) for facts in normalized_facts.values())
    return {
        'mapped_fact_count': mapped_fact_count,
        'total_fact_count': total_fact_count,
        'missing_fact_count': total_fact_count - mapped_fact_count,
        'missing_fact_keys': missing_fact_keys,
    }


def _normalized_fact_decimal(
    normalized_facts: dict[str, dict[str, Any]],
    fact_key: str,
) -> Decimal | None:
    for facts in normalized_facts.values():
        fact_snapshot = facts.get(fact_key)
        if fact_snapshot is None:
            continue
        return _to_decimal(fact_snapshot.get('current_value'))
    return None


def _fact_current_decimal(item: dict[str, Any] | None) -> Decimal | None:
    if item is None:
        return None
    value, _value_basis = _resolve_highlight_value(item, current=True)
    return _to_decimal(value)


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
