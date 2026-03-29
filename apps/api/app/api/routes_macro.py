from fastapi import APIRouter, HTTPException

from app.integrations.bok.client import BOKClient

router = APIRouter(prefix='/macro/bok', tags=['macro:bok'])


@router.get('/series')
async def get_bok_series(series_id: str, start_period: str, end_period: str, cycle: str = 'D') -> dict:
    client = BOKClient()
    try:
        return await client.fetch_series(series_id=series_id, start_period=start_period, end_period=end_period, cycle=cycle)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'BOK series fetch failed: {exc}') from exc
