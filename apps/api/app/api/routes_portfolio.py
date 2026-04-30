from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.orders.portfolio_summary_service import portfolio_summary_service

router = APIRouter(prefix='/portfolio', tags=['portfolio'])


@router.get('/summary')
async def get_portfolio_summary(
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await portfolio_summary_service.get_portfolio_summary(refresh=refresh, trigger='route_read')
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Portfolio summary failed: {exc}') from exc
