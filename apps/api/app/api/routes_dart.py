from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.settings import settings
from app.dart.service import dart_service
from app.integrations.dart.client import DartAPIError, DartLookupError

router = APIRouter(prefix='/disclosures/dart', tags=['disclosures:dart'])

VALID_REPORT_CODES = {'11011', '11012', '11013', '11014'}
VALID_FS_DIVS = {'CFS', 'OFS'}


@router.get('/company-snapshot')
async def get_dart_company_snapshot(
    corp_code: Optional[str] = None,
    stock_code: Optional[str] = None,
    corp_name: Optional[str] = None,
    filing_days: int = Query(default=365, ge=1, le=3650),
    filings_limit: int = Query(default=10, ge=1, le=100),
) -> dict:
    if not any([corp_code, stock_code, corp_name]):
        raise HTTPException(status_code=400, detail='One of corp_code, stock_code, or corp_name is required')

    try:
        return await dart_service.get_company_snapshot(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
            filing_days=filing_days,
            filings_limit=filings_limit,
        )
    except DartLookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART snapshot failed: {exc}') from exc


@router.get('/fundamentals')
async def get_dart_fundamentals(
    business_year: int = Query(..., ge=2015, le=2999),
    corp_code: Optional[str] = None,
    stock_code: Optional[str] = None,
    corp_name: Optional[str] = None,
    report_code: str = Query(default=settings.dart_fundamentals_default_report_code),
    fs_div: str = Query(default=settings.dart_fundamentals_default_fs_div),
) -> dict:
    if not any([corp_code, stock_code, corp_name]):
        raise HTTPException(status_code=400, detail='One of corp_code, stock_code, or corp_name is required')

    normalized_report_code = report_code.strip()
    normalized_fs_div = fs_div.strip().upper()

    if normalized_report_code not in VALID_REPORT_CODES:
        raise HTTPException(
            status_code=400,
            detail='report_code must be one of 11011, 11012, 11013, 11014',
        )
    if normalized_fs_div not in VALID_FS_DIVS:
        raise HTTPException(status_code=400, detail='fs_div must be one of CFS or OFS')

    try:
        return await dart_service.get_fundamentals(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
            business_year=business_year,
            report_code=normalized_report_code,
            fs_div=normalized_fs_div,
        )
    except DartLookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART fundamentals failed: {exc}') from exc
