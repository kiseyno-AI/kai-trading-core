from fastapi import APIRouter
import logging

from app.market_data_store import persist_market_status_snapshot
from app.market.clock import get_market_state

router = APIRouter(prefix='/market', tags=['market'])
logger = logging.getLogger(__name__)


@router.get('/status')
async def market_status() -> dict:
    state = get_market_state()
    try:
        persist_market_status_snapshot(state, source='api')
    except Exception as exc:
        logger.warning('Market status persistence failed: %s', exc)
    return state.__dict__
