import asyncio
import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.core.settings import settings
from app.quotes.kis_realtime_prices import kis_realtime_price_service
from app.quotes.service import QuoteService

router = APIRouter(prefix='/quotes', tags=['quotes'])


@router.get('/tracked')
async def get_tracked_quotes() -> dict:
    service = QuoteService()
    return await service.get_tracked_quotes()


@router.get('/realtime/monitored')
async def get_realtime_monitored_prices(
    refresh_symbols: bool = Query(default=False),
) -> dict:
    if refresh_symbols:
        await kis_realtime_price_service.refresh_subscription_source_if_due(
            trigger='route_quotes_realtime_monitored',
            force=False,
        )
    return kis_realtime_price_service.monitored_snapshot()


@router.get('/realtime/monitored/stream')
async def stream_realtime_monitored_prices(request: Request) -> StreamingResponse:
    async def event_stream():
        last_payload = None
        interval_seconds = max(0.25, float(settings.kis_realtime_price_sse_interval_seconds))
        while not await request.is_disconnected():
            payload = kis_realtime_price_service.monitored_snapshot()
            encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'))
            if encoded != last_payload:
                yield f'event: prices\ndata: {encoded}\n\n'
                last_payload = encoded
            await asyncio.sleep(interval_seconds)

    return StreamingResponse(
        event_stream(),
        media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache'},
    )


@router.get('/{symbol}')
async def get_quote(symbol: str) -> dict:
    service = QuoteService()
    try:
        return await service.get_quote(symbol)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Quote fetch failed: {exc}') from exc
