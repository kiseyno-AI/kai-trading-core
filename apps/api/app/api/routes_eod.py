import asyncio

from fastapi import APIRouter, HTTPException

from app.core.settings import settings
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.master import refresh_master_if_due
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.master_state import master_refresh_state
from app.eod.service import EOD_SERVICE
from app.eod.state import job_state
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.market.clock import get_market_state

router = APIRouter(prefix='/eod', tags=['eod'])


@router.get('/status')
async def get_eod_status() -> dict:
    return {
        'job': job_state.snapshot(),
        'master': master_refresh_state.snapshot(),
        'pykrx_auto': auto_pykrx_eod_scheduler.snapshot(),
    }


@router.post('/master/refresh-if-due')
async def refresh_eod_master_if_due(force: bool = False) -> dict:
    try:
        symbols = refresh_master_if_due(force=force)
        return {
            'refreshed': True,
            'symbol_count': len(symbols),
            'master': master_refresh_state.snapshot(),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Master refresh failed: {exc}') from exc


@router.post('/capture-if-due')
async def capture_eod_if_due() -> dict:
    try:
        return await EOD_SERVICE.capture_if_due()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'EOD capture failed: {exc}') from exc


@router.post('/capture')
async def capture_eod(market_date: str) -> dict:
    try:
        return await EOD_SERVICE.capture_market_date(market_date, trigger='manual')
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'EOD capture failed: {exc}') from exc


@router.post('/pykrx/universe/refresh')
async def refresh_pykrx_universe(markets: str = ','.join(DEFAULT_MARKETS), limit: int | None = None) -> dict:
    try:
        parsed_markets = tuple(item.strip().upper() for item in markets.split(',') if item.strip())
        return await asyncio.to_thread(
            PYKRX_BACKFILL_SERVICE.refresh_recent_universe,
            markets=parsed_markets or DEFAULT_MARKETS,
            limit=limit,
            deactivate_missing=limit is None,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'pykrx universe refresh failed: {exc}') from exc


@router.post('/pykrx/backfill')
async def backfill_pykrx_eod(
    start_date: str = '19560101',
    end_date: str | None = None,
    markets: str = ','.join(DEFAULT_MARKETS),
    limit: int | None = None,
    symbol: str | None = None,
    refresh_universe: bool = True,
) -> dict:
    try:
        parsed_markets = tuple(item.strip().upper() for item in markets.split(',') if item.strip())
        return await asyncio.to_thread(
            PYKRX_BACKFILL_SERVICE.run_backfill,
            start_date=start_date,
            end_date=end_date,
            markets=parsed_markets or DEFAULT_MARKETS,
            trigger='manual_api',
            refresh_universe=refresh_universe,
            limit=limit,
            symbol=symbol,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'pykrx backfill failed: {exc}') from exc


@router.post('/pykrx/daily/run-if-due')
async def run_pykrx_daily_if_due(
    market_date: str | None = None,
    markets: str | None = None,
    force: bool = False,
    refresh_universe: bool | None = None,
) -> dict:
    try:
        parsed_markets = tuple(item.strip().upper() for item in (markets or settings.eod_auto_pykrx_markets).split(',') if item.strip())
        return await asyncio.to_thread(
            PYKRX_BACKFILL_SERVICE.run_daily_collection_if_due,
            market_date=market_date or get_market_state().server_time[:10],
            markets=parsed_markets or DEFAULT_MARKETS,
            trigger='manual_daily_api',
            refresh_universe=settings.eod_auto_pykrx_refresh_universe if refresh_universe is None else refresh_universe,
            force=force,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'pykrx daily EOD run failed: {exc}') from exc


@router.post('/technical-snapshots/compute')
async def compute_daily_technical_snapshots(
    market_date: str,
    symbols: str | None = None,
    source: str = 'pykrx',
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_technical_snapshot_service.compute_and_persist_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily technical snapshot compute failed: {exc}') from exc


@router.get('/technical-snapshots')
async def inspect_daily_technical_snapshots(
    market_date: str,
    symbols: str | None = None,
    limit: int = 100,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_technical_snapshot_service.inspect,
            market_date=market_date,
            symbols=parsed_symbols,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily technical snapshot inspect failed: {exc}') from exc
