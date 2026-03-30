from typing import Optional

from fastapi import APIRouter, HTTPException

from app.integrations.fx.client import KoreaEximClient

router = APIRouter(prefix='/fx/koreaexim', tags=['fx:koreaexim'])


@router.get('/rates')
async def get_exchange_rates(search_date: Optional[str] = None, data: str = 'AP01') -> dict:
    client = KoreaEximClient()
    try:
        return await client.fetch_exchange_rates(search_date=search_date, data=data)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Korea Exim fetch failed: {exc}') from exc
