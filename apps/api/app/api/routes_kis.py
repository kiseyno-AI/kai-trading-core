from fastapi import APIRouter, HTTPException

from app.brokers.kis.provider import kis_client

router = APIRouter(prefix='/broker/kis', tags=['broker:kis'])


@router.get('/health')
async def kis_health() -> dict:
    return await kis_client.healthcheck()


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
