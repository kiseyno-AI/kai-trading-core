import asyncio

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.settings import settings
from app.eod.action_summaries import daily_action_summary_service
from app.eod.auto_pykrx import auto_pykrx_eod_scheduler
from app.eod.daily_pipeline_stages import daily_pipeline_stage_service
from app.eod.freshness import eod_freshness_service
from app.eod.master import refresh_master_if_due
from app.eod.pipeline_status import eod_pipeline_status_service
from app.eod.pykrx_backfill import DEFAULT_MARKETS, PYKRX_BACKFILL_SERVICE
from app.eod.screening_results import screening_result_read_service
from app.eod.master_state import master_refresh_state
from app.eod.service import EOD_SERVICE
from app.eod.state import job_state
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.market.clock import get_market_state
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_FACTOR_SOURCE,
    daily_advanced_value_factor_service,
)
from app.signals.beta_factor import BETA_FACTOR_SOURCE, daily_beta_factor_service
from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE, daily_composite_factor_service
from app.signals.daily_signal_features import daily_signal_feature_service
from app.signals.low_risk_factor import LOW_RISK_SOURCE, daily_low_risk_factor_service
from app.signals.market_risk_filter import (
    MARKET_RISK_FILTER_SOURCE,
    daily_market_risk_filter_service,
)
from app.signals.market_risk_snapshot import (
    MARKET_RISK_SNAPSHOT_SOURCE,
    daily_market_risk_snapshot_service,
)
from app.signals.momentum_factor import daily_momentum_factor_service
from app.signals.piotroski_filter import daily_piotroski_filter_service
from app.signals.value_factor import VALUE_FACTOR_SOURCE, daily_value_factor_service

router = APIRouter(prefix='/eod', tags=['eod'])


class EODPipelineRunRequest(BaseModel):
    market_date: str | None = None
    stages: list[str] | None = None
    symbols: list[str] | None = None
    markets: list[str] | None = None
    force: bool = False
    refresh_universe: bool | None = None
    fs_div: str | None = None
    symbol_limit: int | None = Field(default=None, ge=1)
    symbol_offset: int = Field(default=0, ge=0)
    capture_limit: int = Field(default=500, ge=1)
    retry_cycles: int | None = Field(default=None, ge=1)
    feedback_max_cycles: int = Field(default=3, ge=1)
    feedback_outer_batch_size: int = Field(default=20, ge=1)
    reuse_existing_feedback: bool = True


@router.get('/status')
async def get_eod_status() -> dict:
    return {
        'job': job_state.snapshot(),
        'master': master_refresh_state.snapshot(),
        'pykrx_auto': auto_pykrx_eod_scheduler.snapshot(),
    }


@router.get('/freshness')
async def get_eod_freshness() -> dict:
    return eod_freshness_service.read()


@router.get('/screening-results')
async def get_eod_screening_results(
    family: str,
    market_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            screening_result_read_service.read,
            family=family,
            market_date=market_date,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'EOD screening result read failed: {exc}') from exc


@router.get('/pipeline/status')
async def get_eod_pipeline_status(
    market_date: str | None = None,
    fs_div: str | None = None,
) -> dict:
    try:
        return await asyncio.to_thread(
            eod_pipeline_status_service.read,
            market_date=market_date,
            fs_div=fs_div,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'EOD pipeline status read failed: {exc}') from exc


@router.get('/pipeline/stages')
async def get_eod_pipeline_stages() -> dict:
    try:
        return await asyncio.to_thread(daily_pipeline_stage_service.describe)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'EOD pipeline stage catalog read failed: {exc}') from exc


@router.post('/pipeline/run')
async def run_eod_pipeline(payload: EODPipelineRunRequest) -> dict:
    try:
        request_kwargs = (
            payload.model_dump(exclude_none=True)
            if hasattr(payload, 'model_dump')
            else payload.dict(exclude_none=True)
        )
        return await asyncio.to_thread(
            daily_pipeline_stage_service.run,
            **request_kwargs,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'EOD pipeline run failed: {exc}') from exc


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
            PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due,
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
            daily_technical_snapshot_service.rebuild_for_market_date,
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


@router.post('/action-summaries/compute')
async def compute_daily_action_summaries(
    market_date: str,
    symbols: str | None = None,
    source: str = 'pykrx',
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_action_summary_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily action summary compute failed: {exc}') from exc


@router.get('/action-summaries')
async def inspect_daily_action_summaries(
    market_date: str,
    symbols: str | None = None,
    limit: int = 100,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_action_summary_service.inspect,
            market_date=market_date,
            symbols=parsed_symbols,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily action summary inspect failed: {exc}') from exc


@router.post('/signal-features/collect')
async def collect_daily_signal_features(
    market_date: str,
    sources: str | None = None,
    max_pages: int = 10,
    overtime_volume_rank_sort_cls_code: str = '2',
    overtime_fluctuation_div_cls_code: str = '2',
) -> dict:
    try:
        parsed_sources = [item.strip() for item in sources.split(',') if item.strip()] if sources else None
        return await daily_signal_feature_service.refresh_for_market_date(
            market_date=market_date,
            sources=parsed_sources,
            max_pages=max_pages,
            overtime_volume_rank_sort_cls_code=overtime_volume_rank_sort_cls_code,
            overtime_fluctuation_div_cls_code=overtime_fluctuation_div_cls_code,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily signal feature collection failed: {exc}') from exc


@router.get('/signal-features')
async def inspect_daily_signal_features(
    market_date: str,
    symbols: str | None = None,
    limit: int = 100,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_signal_feature_service.inspect,
            market_date=market_date,
            symbols=parsed_symbols,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily signal feature inspect failed: {exc}') from exc


@router.post('/market-risk-filters/compute')
async def compute_daily_market_risk_filters(
    market_date: str,
    symbols: str | None = None,
    source: str = MARKET_RISK_FILTER_SOURCE,
    market_code: str = 'J',
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await daily_market_risk_filter_service.rebuild_for_market_date(
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
            market_code=market_code,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily market risk filter compute failed: {exc}') from exc


@router.post('/market-risk-snapshots/collect')
async def collect_daily_market_risk_snapshots(
    market_date: str,
    symbols: str | None = None,
    source: str = MARKET_RISK_SNAPSHOT_SOURCE,
    market_code: str = 'J',
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await daily_market_risk_snapshot_service.collect_for_market_date(
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
            market_code=market_code,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily market risk snapshot collection failed: {exc}') from exc


@router.get('/market-risk-snapshots')
async def inspect_daily_market_risk_snapshots(
    market_date: str | None = None,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_market_risk_snapshot_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily market risk snapshot read failed: {exc}') from exc


@router.get('/market-risk-filters')
async def inspect_daily_market_risk_filters(
    market_date: str | None = None,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_market_risk_filter_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily market risk filter read failed: {exc}') from exc


@router.post('/low-risk-factors/compute')
async def compute_daily_low_risk_factors(
    market_date: str,
    symbols: str | None = None,
    source: str = LOW_RISK_SOURCE,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_low_risk_factor_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily low-risk factor compute failed: {exc}') from exc


@router.post('/beta-factors/compute')
async def compute_daily_beta_factors(
    market_date: str,
    symbols: str | None = None,
    source: str = BETA_FACTOR_SOURCE,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_beta_factor_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily beta factor compute failed: {exc}') from exc


@router.get('/beta-factors')
async def inspect_daily_beta_factors(
    market_date: str,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_beta_factor_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily beta factor read failed: {exc}') from exc


@router.get('/low-risk-factors')
async def inspect_daily_low_risk_factors(
    market_date: str,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_low_risk_factor_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily low-risk factor read failed: {exc}') from exc


@router.post('/momentum-factors/compute')
async def compute_daily_momentum_factors(
    market_date: str,
    symbols: str | None = None,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_momentum_factor_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily momentum factor compute failed: {exc}') from exc


@router.get('/momentum-factors')
async def inspect_daily_momentum_factors(
    market_date: str,
    symbols: str | None = None,
    limit: int = 100,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_momentum_factor_service.inspect,
            market_date=market_date,
            symbols=parsed_symbols,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily momentum factor inspect failed: {exc}') from exc


@router.post('/value-factors/compute')
async def compute_daily_value_factors(
    market_date: str,
    symbols: str | None = None,
    source: str = VALUE_FACTOR_SOURCE,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_value_factor_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily value factor compute failed: {exc}') from exc


@router.get('/value-factors')
async def inspect_daily_value_factors(
    market_date: str,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_value_factor_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily value factor read failed: {exc}') from exc


@router.post('/advanced-value-factors/compute')
async def compute_daily_advanced_value_factors(
    market_date: str,
    symbols: str | None = None,
    source: str = ADVANCED_VALUE_FACTOR_SOURCE,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_advanced_value_factor_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily advanced value factor compute failed: {exc}') from exc


@router.get('/advanced-value-factors')
async def inspect_daily_advanced_value_factors(
    market_date: str,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_advanced_value_factor_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily advanced value factor read failed: {exc}') from exc


@router.post('/composite-factors/compute')
async def compute_daily_composite_factors(
    market_date: str,
    symbols: str | None = None,
    source: str = COMPOSITE_FACTOR_SOURCE,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            daily_composite_factor_service.rebuild_for_market_date,
            market_date=market_date,
            symbols=parsed_symbols,
            source=source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'Daily composite factor compute failed: {exc}') from exc


@router.get('/composite-factors')
async def inspect_daily_composite_factors(
    market_date: str,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_composite_factor_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily composite factor read failed: {exc}') from exc


@router.get('/piotroski-filter-results')
async def inspect_daily_piotroski_filter_results(
    market_date: str | None = None,
    symbol: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=20000),
) -> dict:
    try:
        return await asyncio.to_thread(
            daily_piotroski_filter_service.inspect,
            market_date=market_date,
            symbol=symbol,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Daily Piotroski filter result read failed: {exc}') from exc
