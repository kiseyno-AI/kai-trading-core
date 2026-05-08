from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Query

from app.external_news.service import external_news_service

router = APIRouter(prefix='/external/news', tags=['external:news'])


@router.get('/health')
async def external_news_health() -> dict:
    return external_news_service.provider_status()


@router.post('/collect')
async def collect_external_news(symbols: str, display: int = Query(default=10, ge=1, le=100)) -> dict:
    try:
        return await external_news_service.collect_for_symbols(symbols=symbols, display=display)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get('')
async def list_external_news(symbols: str | None = None, limit: int = Query(default=100, ge=1, le=1000)) -> dict:
    try:
        return await asyncio.to_thread(external_news_service.list_news, symbols=symbols, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
