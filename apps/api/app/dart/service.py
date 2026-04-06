from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.integrations.dart.client import DartClient, dart_client


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


dart_service = DartService()
