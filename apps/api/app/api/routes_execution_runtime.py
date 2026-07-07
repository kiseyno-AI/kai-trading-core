from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.orders.exec_integration import signal_decider_exec_integration_service

router = APIRouter(prefix='/execution/runtime', tags=['execution:runtime'])


@router.get('/status')
async def get_execution_runtime_status(
    refresh: bool = Query(default=True),
) -> dict[str, Any]:
    try:
        return await signal_decider_exec_integration_service.dashboard_runtime_status(refresh=refresh)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Execution runtime status read failed: {exc}') from exc


@router.get('/monitored-symbols')
async def list_execution_runtime_monitored_symbols(
    market_date: date | None = Query(default=None),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await signal_decider_exec_integration_service.list_runtime_monitored_symbols(
            market_date=market_date,
            refresh=refresh,
            trigger='route_runtime_monitored_symbols',
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Execution runtime monitored-symbols read failed: {exc}') from exc


@router.get('/daily-board')
async def get_execution_runtime_daily_board(
    market_date: date | None = Query(default=None),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await signal_decider_exec_integration_service.get_runtime_daily_board(
            market_date=market_date,
            refresh=refresh,
            trigger='route_runtime_daily_board',
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Execution runtime daily board read failed: {exc}') from exc


@router.get('/symbols/{symbol}')
async def get_execution_runtime_symbol_detail(
    symbol: str,
    market_date: date | None = Query(default=None),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await signal_decider_exec_integration_service.get_runtime_symbol_detail(
            market_date=market_date,
            symbol=symbol,
            refresh=refresh,
            trigger='route_runtime_symbol_detail',
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Execution runtime symbol detail read failed: {exc}') from exc
