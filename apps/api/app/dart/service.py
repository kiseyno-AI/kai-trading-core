from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.core.settings import settings
from app.integrations.dart.client import DartClient, dart_client

REPORT_CODE_LABELS = {
    '11011': 'annual_report',
    '11012': 'semiannual_report',
    '11013': 'first_quarter_report',
    '11014': 'third_quarter_report',
}

FS_DIV_LABELS = {
    'CFS': 'consolidated',
    'OFS': 'separate',
}

NORMALIZED_ACCOUNT_NAMES = {
    'total_assets': {'자산총계', 'Assets'},
    'total_liabilities': {'부채총계', 'Liabilities'},
    'total_equity': {'자본총계', 'Equity'},
    'revenue': {'매출액', '영업수익', 'Revenue'},
    'operating_income': {'영업이익', 'Operating income'},
    'net_income': {'당기순이익', '분기순이익', '반기순이익', 'Profit (loss)', '당기순이익(손실)'},
    'operating_cash_flow': {'영업활동으로 인한 현금흐름', '영업활동현금흐름', 'Cash flows from operating activities'},
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

    async def get_fundamentals(
        self,
        *,
        corp_code: str | None = None,
        stock_code: str | None = None,
        corp_name: str | None = None,
        business_year: int,
        report_code: str | None = None,
        fs_div: str | None = None,
    ) -> dict[str, Any]:
        corp = await self.client.lookup_corp_code(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
        )

        resolved_report_code = (report_code or settings.dart_fundamentals_default_report_code).strip()
        resolved_fs_div = (fs_div or settings.dart_fundamentals_default_fs_div).strip().upper()
        statements = await self.client.fetch_single_company_financial_statements(
            corp_code=corp['corp_code'],
            bsns_year=str(business_year),
            reprt_code=resolved_report_code,
            fs_div=resolved_fs_div,
        )
        items = [self._normalize_statement_item(item) for item in statements.get('list', [])]

        return {
            'lookup': {
                'corp_code': corp['corp_code'],
                'corp_name': corp['corp_name'],
                'stock_code': corp['stock_code'],
                'modify_date': corp['modify_date'],
            },
            'request': {
                'business_year': business_year,
                'report_code': resolved_report_code,
                'report_name': REPORT_CODE_LABELS.get(resolved_report_code, 'unknown'),
                'fs_div': resolved_fs_div,
                'fs_label': FS_DIV_LABELS.get(resolved_fs_div, 'unknown'),
                'dataset': 'fnlttSinglAcntAll',
            },
            'highlights': self._extract_highlights(items),
            'statements': {
                'status': statements.get('status'),
                'message': statements.get('message'),
                'currency': next((item['currency'] for item in items if item.get('currency')), None),
                'count': len(items),
                'items': items,
            },
        }

    def _extract_highlights(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        highlights: dict[str, Any] = {}
        for key, names in NORMALIZED_ACCOUNT_NAMES.items():
            for item in items:
                if item['account_name'] in names:
                    highlights[key] = {
                        'statement_type': item['statement_type'],
                        'statement_name': item['statement_name'],
                        'account_name': item['account_name'],
                        'current_amount': item['current_amount'],
                        'current_cumulative_amount': item['current_cumulative_amount'],
                        'prior_amount': item['prior_amount'],
                        'prior_cumulative_amount': item['prior_cumulative_amount'],
                        'currency': item['currency'],
                    }
                    break
        return highlights

    def _normalize_statement_item(self, item: dict[str, Any]) -> dict[str, Any]:
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
            'current_amount': self._parse_amount(item.get('thstrm_amount')),
            'current_cumulative_amount': self._parse_amount(item.get('thstrm_add_amount')),
            'prior_period_name': item.get('frmtrm_nm'),
            'prior_amount': self._parse_amount(item.get('frmtrm_amount')),
            'prior_quarter_name': item.get('frmtrm_q_nm'),
            'prior_quarter_amount': self._parse_amount(item.get('frmtrm_q_amount')),
            'prior_cumulative_amount': self._parse_amount(item.get('frmtrm_add_amount')),
            'pre_prior_period_name': item.get('bfefrmtrm_nm'),
            'pre_prior_amount': self._parse_amount(item.get('bfefrmtrm_amount')),
            'order': self._parse_amount(item.get('ord')),
            'currency': item.get('currency'),
        }

    @staticmethod
    def _parse_amount(value: Any) -> int | None:
        if value in (None, ''):
            return None
        if isinstance(value, int):
            return value

        normalized = str(value).replace(',', '').strip()
        if not normalized:
            return None
        try:
            return int(normalized)
        except ValueError:
            return None


dart_service = DartService()
