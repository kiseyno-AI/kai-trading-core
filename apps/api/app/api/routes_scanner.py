from fastapi import APIRouter
import logging
from pydantic import BaseModel

from app.market_data_store import replace_tracked_symbols
from app.scanner.state import scanner_state

router = APIRouter(prefix='/scanner', tags=['scanner'])
logger = logging.getLogger(__name__)


class TrackedSymbolsUpdate(BaseModel):
    symbols: list[str]


@router.get('/tracked-symbols')
async def get_tracked_symbols() -> dict:
    return scanner_state.snapshot()


@router.post('/tracked-symbols')
async def update_tracked_symbols(payload: TrackedSymbolsUpdate) -> dict:
    snapshot = scanner_state.update(payload.symbols)
    try:
        replace_tracked_symbols(payload.symbols)
    except Exception as exc:
        logger.warning('Tracked symbol persistence failed: %s', exc)
    return snapshot
