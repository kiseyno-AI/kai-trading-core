from __future__ import annotations

import asyncio
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from app.external_news.service import external_news_service
from app.market_news.catalog import MARKET_NEWS_TOPICS
from app.market_news.service import market_news_service

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


@router.get('/market/topics')
async def get_market_news_topics() -> dict:
    return {
        'topics': [
            {'key': topic.key, 'region': topic.region, 'query': topic.query, 'business_lines': list(topic.business_lines)}
            for topic in MARKET_NEWS_TOPICS
        ],
        'latest_first_seen_at': (
            latest.isoformat()
            if (latest := await asyncio.to_thread(market_news_service.latest_first_seen_at))
            else None
        ),
    }


@router.post('/market/collect')
async def collect_market_news(
    topics: str | None = None,
    pages: int = Query(default=1, ge=1, le=10),
) -> dict:
    """Operator collection; ``pages`` > 1 backfills older articles (up to 1,000 per topic)."""

    keys = [key.strip() for key in topics.split(',') if key.strip()] if topics else None
    try:
        return await market_news_service.collect(topic_keys=keys, pages=pages)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get('/market')
async def get_market_news(
    topics: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    as_of: datetime | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    """Stored articles only; never calls NAVER. ``as_of`` cuts at first observation."""

    keys = [key.strip() for key in topics.split(',') if key.strip()] if topics else None
    try:
        items = await asyncio.to_thread(
            market_news_service.list_items, topic_keys=keys, start=start, end=end, as_of=as_of, limit=limit
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {'count': len(items), 'items': items}
