from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.settings import settings
from app.dart.service import dart_service
from app.dart.sync_service import dart_disclosure_sync_service
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


@router.post('/sync/events')
async def run_dart_disclosure_event_sync(
    days: int = Query(default=settings.dart_sync_recent_days, ge=1, le=30),
) -> dict:
    try:
        return await dart_disclosure_sync_service.sync_recent_disclosures(days=days, trigger='manual_api')
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART disclosure sync failed: {exc}') from exc


@router.get('/sync/inspect')
async def inspect_dart_disclosure_event_sync(
    runs_limit: int = Query(default=10, ge=1, le=50),
    events_limit: int = Query(default=25, ge=1, le=100),
    financial_only: bool = False,
) -> dict:
    try:
        return dart_disclosure_sync_service.inspect_sync(
            limit=runs_limit,
            events_limit=events_limit,
            financial_only=financial_only,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART disclosure sync inspect failed: {exc}') from exc
