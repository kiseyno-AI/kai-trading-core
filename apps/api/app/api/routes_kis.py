from fastapi import APIRouter, HTTPException

from app.brokers.kis.provider import kis_client
from app.market.snapshot import market_snapshot_service
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.kis_realtime_order_notices import kis_realtime_order_notice_service

router = APIRouter(prefix='/broker/kis', tags=['broker:kis'])


@router.get('/health')
async def kis_health() -> dict:
    payload = await kis_client.healthcheck()
    payload['realtime_order_notices'] = kis_realtime_order_notice_service.health_snapshot()
    payload['market_snapshot_runtime'] = market_snapshot_service.runtime_status()
    payload['portfolio_summary_runtime'] = portfolio_summary_service.runtime_status()
    return payload


@router.post('/token')
async def issue_kis_token(force_refresh: bool = False) -> dict:
    try:
        return await kis_client.issue_access_token(force_refresh=force_refresh)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'KIS token issue failed: {exc}') from exc


@router.get('/quote')
async def get_kis_quote(symbol: str, market_code: str = 'J') -> dict:
    try:
        return await kis_client.fetch_domestic_quote(symbol=symbol, market_code=market_code)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'KIS quote failed: {exc}') from exc
