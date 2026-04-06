from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.dart.service import dart_service
from app.integrations.dart.client import DartAPIError, DartLookupError

router = APIRouter(prefix='/disclosures/dart', tags=['disclosures:dart'])


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
