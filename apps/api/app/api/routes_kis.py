from fastapi import APIRouter, HTTPException

from app.brokers.kis.client import KISClient

router = APIRouter(prefix='/broker/kis', tags=['broker:kis'])


@router.get('/health')
async def kis_health() -> dict:
    client = KISClient()
    return await client.healthcheck()


@router.post('/token')
async def issue_kis_token() -> dict:
    client = KISClient()
    try:
        return await client.issue_access_token()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'KIS token issue failed: {exc}') from exc


@router.get('/quote')
async def get_kis_quote(symbol: str, market_code: str = 'J') -> dict:
    client = KISClient()
    try:
        return await client.fetch_domestic_quote(symbol=symbol, market_code=market_code)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'KIS quote failed: {exc}') from exc
