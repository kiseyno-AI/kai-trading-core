import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.market_data_store import replace_tracked_symbols
from app.scanner.realtime_trending import KIS_REALTIME_TRENDING_SOURCES, realtime_trending_service
from app.scanner.state import scanner_state
from app.scanner.trending import trending_stocks_service

router = APIRouter(prefix='/scanner', tags=['scanner'])
logger = logging.getLogger(__name__)


class TrackedSymbolsUpdate(BaseModel):
    symbols: list[str]


@router.get('/tracked-symbols')
async def get_tracked_symbols() -> dict:
    return scanner_state.snapshot()


@router.get('/trending-stocks')
async def get_trending_stocks(
    market_date: date | None = Query(default=None),
    period: str = Query(default='5d'),
    direction: str = Query(default='both'),
    limit: int = Query(default=20, ge=1, le=100),
    markets: list[str] | None = Query(default=None),
) -> dict:
    try:
        return trending_stocks_service.get_trending_stocks(
            market_date=market_date,
            period=period,
            direction=direction,
            limit=limit,
            markets=markets,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Trending stocks read failed: {exc}') from exc


@router.get('/realtime-trending')
async def get_realtime_trending(
    limit: int = Query(20, ge=1, le=50),
    market_code: str = Query('J'),
    sources: str = Query(','.join(KIS_REALTIME_TRENDING_SOURCES)),
) -> dict:
    try:
        return await realtime_trending_service.get_realtime_trending(
            limit=limit,
            market_code=market_code,
            sources=sources,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/tracked-symbols')
async def update_tracked_symbols(payload: TrackedSymbolsUpdate) -> dict:
    snapshot = scanner_state.update(payload.symbols)
    try:
        replace_tracked_symbols(payload.symbols)
    except Exception as exc:
        logger.warning('Tracked symbol persistence failed: %s', exc)
    return snapshot
