import asyncio
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.integrations.sec_edgar.client import SECEdgarConfigurationError
from app.repositories.symbol_repository import load_symbols
from app.repositories.us_equity_repository import load_us_screening_snapshots
from app.us_equities.fundamental_collection_service import us_fundamental_collection_service
from app.us_equities.agent_analysis_service import us_agent_analysis_service
from app.us_equities.execution_plan_service import us_execution_plan_service
from app.us_equities.price_collection_service import us_price_collection_service
from app.us_equities.universe_service import us_universe_refresh_service


router = APIRouter(prefix='/us-equities', tags=['us-equities'])


class USUniverseRefreshRequest(BaseModel):
    persist: bool = True
    preview_limit: int = Field(default=20, ge=0, le=200)


class USPriceCollectionRequest(BaseModel):
    market_date: date
    persist: bool = True
    prefilter_cap: int | None = Field(default=None, ge=1, le=600)


class USFundamentalCollectionRequest(BaseModel):
    market_date: date
    collection_run_id: int = Field(ge=1)
    persist: bool = True


class USAgentDispatchRequest(BaseModel):
    market_date: date
    collection_run_id: int = Field(ge=1)
    roles: list[str] | None = None
    persist: bool = True
    batch_size: int = Field(default=5, ge=1, le=20)


class USExecutionPlanRequest(BaseModel):
    source_market_date: date
    target_trade_date: date | None = None
    max_candidates: int | None = Field(default=None, ge=1, le=80)
    dry_run: bool = True
    agent_run_id: str = Field(min_length=1, max_length=128)


@router.post('/universe/refresh')
async def refresh_us_equity_universe(payload: USUniverseRefreshRequest) -> dict:
    try:
        return await us_universe_refresh_service.refresh(
            persist=payload.persist,
            preview_limit=payload.preview_limit,
        )
    except SECEdgarConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'US equity universe refresh failed: {exc}') from exc


@router.get('/universe')
async def read_us_equity_universe(limit: int = Query(default=100, ge=1, le=5000)) -> dict:
    try:
        items = await asyncio.to_thread(
            load_symbols,
            is_active=True,
            source='us_integrated_master',
            markets=['NASDAQ', 'NYSE', 'NYSE_AMERICAN'],
            limit=limit,
        )
        return {'count': len(items), 'items': items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'US equity universe read failed: {exc}') from exc


@router.post('/prices/collect-and-screen')
async def collect_and_screen_us_equity_prices(payload: USPriceCollectionRequest) -> dict:
    try:
        return await us_price_collection_service.collect_and_screen(
            market_date=payload.market_date,
            persist=payload.persist,
            prefilter_cap=payload.prefilter_cap,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'US equity price collection failed: {exc}') from exc


@router.get('/screening/{market_date}')
async def read_us_equity_screening(
    market_date: date,
    eligible_for_agent: bool | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=3000),
) -> dict:
    try:
        items = await asyncio.to_thread(
            load_us_screening_snapshots,
            market_date=market_date,
            eligible_for_agent=eligible_for_agent,
            limit=limit,
        )
        return {'market_date': market_date.isoformat(), 'count': len(items), 'items': items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'US equity screening read failed: {exc}') from exc


@router.post('/fundamentals/collect-and-screen')
async def collect_and_screen_us_equity_fundamentals(payload: USFundamentalCollectionRequest) -> dict:
    try:
        return await us_fundamental_collection_service.collect_and_screen(
            market_date=payload.market_date,
            collection_run_id=payload.collection_run_id,
            persist=payload.persist,
        )
    except SECEdgarConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'US equity fundamental collection failed: {exc}') from exc


@router.post('/agents/dispatch')
async def dispatch_us_equity_agents(payload: USAgentDispatchRequest) -> dict:
    try:
        return await asyncio.to_thread(
            us_agent_analysis_service.dispatch,
            market_date=payload.market_date,
            collection_run_id=payload.collection_run_id,
            roles=payload.roles,
            persist=payload.persist,
            batch_size=payload.batch_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'US equity agent dispatch failed: {exc}') from exc


@router.post('/execution-plans/dispatch')
async def dispatch_us_equity_execution_plan(payload: USExecutionPlanRequest) -> dict:
    try:
        return await us_execution_plan_service.dispatch(
            source_market_date=payload.source_market_date,
            target_trade_date=payload.target_trade_date,
            max_candidates=payload.max_candidates,
            dry_run=payload.dry_run,
            agent_run_id=payload.agent_run_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f'US equity execution-plan dispatch failed: {exc}') from exc
