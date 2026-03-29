from fastapi import APIRouter
from pydantic import BaseModel

from app.scanner.state import scanner_state

router = APIRouter(prefix='/scanner', tags=['scanner'])


class TrackedSymbolsUpdate(BaseModel):
    symbols: list[str]


@router.get('/tracked-symbols')
async def get_tracked_symbols() -> dict:
    return scanner_state.snapshot()


@router.post('/tracked-symbols')
async def update_tracked_symbols(payload: TrackedSymbolsUpdate) -> dict:
    return scanner_state.update(payload.symbols)
