from fastapi import APIRouter

from app.market.clock import get_market_state

router = APIRouter(prefix='/market', tags=['market'])


@router.get('/status')
async def market_status() -> dict:
    return get_market_state().__dict__
