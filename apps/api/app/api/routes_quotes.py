from fastapi import APIRouter, HTTPException

from app.quotes.service import QuoteService

router = APIRouter(prefix='/quotes', tags=['quotes'])


@router.get('/tracked')
async def get_tracked_quotes() -> dict:
    service = QuoteService()
    return await service.get_tracked_quotes()


@router.get('/{symbol}')
async def get_quote(symbol: str) -> dict:
    service = QuoteService()
    try:
        return await service.get_quote(symbol)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Quote fetch failed: {exc}') from exc
