from fastapi import APIRouter, HTTPException

from app.eod.service import EOD_SERVICE
from app.eod.state import job_state

router = APIRouter(prefix='/eod', tags=['eod'])


@router.get('/status')
async def get_eod_status() -> dict:
    return job_state.snapshot()


@router.post('/capture-if-due')
async def capture_eod_if_due() -> dict:
    try:
        return await EOD_SERVICE.capture_if_due()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'EOD capture failed: {exc}') from exc


@router.post('/capture')
async def capture_eod(market_date: str) -> dict:
    try:
        return await EOD_SERVICE.capture_market_date(market_date)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'EOD capture failed: {exc}') from exc
