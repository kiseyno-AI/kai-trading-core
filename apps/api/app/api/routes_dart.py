from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.core.settings import settings
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.dart.service import dart_service
from app.dart.sync_service import dart_disclosure_sync_service
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


@router.get('/fundamentals')
async def get_dart_fundamentals(
    business_year: Optional[int] = Query(default=None, ge=2015, le=2999),
    fiscal_year: Optional[int] = Query(default=None, ge=2015, le=2100),
    corp_code: Optional[str] = None,
    stock_code: Optional[str] = None,
    corp_name: Optional[str] = None,
    period: Optional[str] = Query(default=None),
    report_code: Optional[str] = Query(default=None),
    statement_type: Optional[str] = Query(default=None),
    fs_div: Optional[str] = Query(default=None),
) -> dict:
    if not any([corp_code, stock_code, corp_name]):
        raise HTTPException(status_code=400, detail='One of corp_code, stock_code, or corp_name is required')

    resolved_business_year = _resolve_business_year(business_year=business_year, fiscal_year=fiscal_year)
    normalized_report_code = _normalize_report_code(report_code)
    normalized_fs_div = _normalize_fs_div(fs_div)

    try:
        return await dart_service.get_fundamentals(
            corp_code=corp_code,
            stock_code=stock_code,
            corp_name=corp_name,
            business_year=resolved_business_year,
            period=period,
            report_code=normalized_report_code,
            statement_type=statement_type,
            fs_div=normalized_fs_div,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DartLookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART fundamentals failed: {exc}') from exc


@router.post('/fundamentals/capture')
async def capture_dart_fundamentals(
    limit: int = Query(default=20, ge=1, le=200),
    force: bool = False,
    fs_div: Optional[str] = Query(default=None),
) -> dict:
    normalized_fs_div = _normalize_fs_div(fs_div) if fs_div is not None else settings.dart_fundamentals_default_fs_div.strip().upper()
    try:
        return await dart_fundamentals_capture_service.capture_recent_fundamentals(
            limit=limit,
            force=force,
            trigger='manual_api',
            fs_div=normalized_fs_div,
        )
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART fundamentals capture failed: {exc}') from exc


@router.get('/fundamentals/capture/inspect')
async def inspect_dart_fundamentals_capture(
    runs_limit: int = Query(default=10, ge=1, le=50),
    events_limit: int = Query(default=25, ge=1, le=100),
    capture_status: Optional[str] = Query(default=None),
) -> dict:
    try:
        return dart_fundamentals_capture_service.inspect_capture_status(
            runs_limit=runs_limit,
            events_limit=events_limit,
            capture_status=capture_status,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART fundamentals capture inspect failed: {exc}') from exc


@router.get('/filings/document')
async def download_dart_filing_document(
    rcept_no: str = Query(..., min_length=14, max_length=14),
) -> Response:
    try:
        artifact = await dart_service.get_filing_document(rcept_no=rcept_no)
        return _build_binary_response(artifact)
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART filing document download failed: {exc}') from exc


@router.get('/filings/xbrl')
async def download_dart_filing_xbrl(
    rcept_no: str = Query(..., min_length=14, max_length=14),
    report_code: Optional[str] = Query(default=None),
) -> Response:
    normalized_report_code = _normalize_report_code(report_code)

    try:
        artifact = await dart_service.get_filing_xbrl(
            rcept_no=rcept_no,
            report_code=normalized_report_code,
        )
        return _build_binary_response(artifact)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DartAPIError as exc:
        raise HTTPException(status_code=502, detail=f'DART request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'DART XBRL download failed: {exc}') from exc


def _resolve_business_year(*, business_year: Optional[int], fiscal_year: Optional[int]) -> int:
    if business_year is None and fiscal_year is None:
        raise HTTPException(status_code=400, detail='One of business_year or fiscal_year is required')
    if business_year is not None and fiscal_year is not None and business_year != fiscal_year:
        raise HTTPException(status_code=400, detail='business_year and fiscal_year must match when both are provided')
    if business_year is not None:
        return business_year
    assert fiscal_year is not None
    return fiscal_year


def _normalize_report_code(report_code: Optional[str]) -> Optional[str]:
    if report_code is None:
        return None

    normalized_report_code = report_code.strip() or settings.dart_fundamentals_default_report_code
    if normalized_report_code not in VALID_REPORT_CODES:
        raise HTTPException(
            status_code=400,
            detail='report_code must be one of 11011, 11012, 11013, 11014',
        )
    return normalized_report_code


def _normalize_fs_div(fs_div: Optional[str]) -> Optional[str]:
    if fs_div is None:
        return None

    normalized_fs_div = fs_div.strip().upper() or settings.dart_fundamentals_default_fs_div
    if normalized_fs_div not in VALID_FS_DIVS:
        raise HTTPException(status_code=400, detail='fs_div must be one of CFS or OFS')
    return normalized_fs_div


def _build_binary_response(artifact: dict) -> Response:
    return Response(
        content=artifact['content'],
        media_type=artifact['content_type'],
        headers={
            'Content-Disposition': f'attachment; filename="{artifact["filename"]}"',
            'Content-Length': str(artifact['size_bytes']),
            'X-DART-Artifact-Type': artifact['artifact_type'],
            'X-DART-Receipt-No': artifact['rcept_no'],
        },
    )
