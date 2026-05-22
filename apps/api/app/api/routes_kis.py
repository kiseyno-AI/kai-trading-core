import asyncio

from fastapi import APIRouter, HTTPException, Query

from app.brokers.kis.provider import kis_client
from app.market.kis_intraday_snapshots import kis_intraday_snapshot_service
from app.market.snapshot import market_snapshot_service
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.kis_realtime_order_notices import kis_realtime_order_notice_service
from app.quotes.kis_realtime_prices import kis_realtime_price_service

router = APIRouter(prefix='/broker/kis', tags=['broker:kis'])


@router.get('/health')
async def kis_health() -> dict:
    payload = await kis_client.healthcheck()
    payload['realtime_order_notices'] = kis_realtime_order_notice_service.health_snapshot()
    payload['realtime_prices'] = kis_realtime_price_service.health_snapshot()
    payload['market_snapshot_runtime'] = market_snapshot_service.runtime_status()
    payload['portfolio_summary_runtime'] = portfolio_summary_service.runtime_status()
    payload['intraday_snapshot_runtime'] = kis_intraday_snapshot_service.runtime_status()
    return payload


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


@router.post('/intraday-snapshots/collect')
async def collect_kis_intraday_snapshots(
    symbols: str | None = None,
    market_code: str = 'J',
    max_ranking_pages: int = 2,
    max_news_items: int = 5,
    include_news: bool = True,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await kis_intraday_snapshot_service.collect_snapshots(
            symbols=parsed_symbols,
            market_code=market_code,
            max_ranking_pages=max_ranking_pages,
            max_news_items=max_news_items,
            include_news=include_news,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'KIS intraday snapshot collection failed: {exc}') from exc


@router.get('/intraday-snapshots')
async def inspect_kis_intraday_snapshots(
    symbols: str | None = None,
    limit: int = Query(default=100, ge=1, le=5000),
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            kis_intraday_snapshot_service.inspect,
            symbols=parsed_symbols,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'KIS intraday snapshot read failed: {exc}') from exc
