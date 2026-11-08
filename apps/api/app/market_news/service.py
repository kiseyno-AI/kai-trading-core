"""Collect and read market / business-line news topics.

Collection only calls NAVER News Search with the credentials Core already
has; reads never call upstream. Every stored article keeps ``first_seen_at``
so historical reads can cut at what was known then.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.core.settings import settings
from app.db import session_scope
from app.db_models import MarketNewsItem
from app.external_news.provider import ExternalNewsProvider, NaverNewsSearchProvider
from app.market_news.catalog import MarketNewsTopic, resolve_topics

logger = logging.getLogger(__name__)
KST = ZoneInfo('Asia/Seoul')
MAX_PAGES = 10  # NAVER stops at start=1000


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode('utf-8')).hexdigest()


def _kst_day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=KST)
    return start, start + timedelta(days=1)


class MarketNewsService:
    def __init__(self, provider: ExternalNewsProvider | None = None) -> None:
        self.provider = provider or NaverNewsSearchProvider()
        self._lock = threading.Lock()
        self._last_collect_at: datetime | None = None

    # ---------------------------------------------------------------- collect
    async def collect(
        self,
        *,
        topic_keys: list[str] | None = None,
        pages: int = 1,
        display: int | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Fetch the newest articles per topic; ``pages`` > 1 backfills older ones."""

        topics = resolve_topics(topic_keys)
        status = self.provider.status()
        if not status.enabled:
            return {'status': 'skipped', 'reason': status.reason, 'topics': {}}
        observed_at = now or datetime.now(timezone.utc)
        per_page = max(1, min(int(display or settings.market_news_display), 100))
        pages = max(1, min(int(pages), MAX_PAGES))
        throttle = max(0.0, float(settings.external_news_collect_throttle_seconds or 0.0))
        results: dict[str, dict[str, Any]] = {}
        calls = 0
        for topic in topics:
            fetched = inserted = 0
            error = None
            for page in range(pages):
                if calls and throttle:
                    await asyncio.sleep(throttle)
                calls += 1
                try:
                    items = await self.provider.search_news(
                        query=topic.query,
                        display=per_page,
                        start=page * per_page + 1,
                    )
                except Exception as exc:  # noqa: BLE001 - one topic must not stop the rest
                    error = f'{type(exc).__name__}: {exc}'[:300]
                    break
                fetched += len(items)
                inserted += await asyncio.to_thread(self._store, topic, items, observed_at)
                if len(items) < per_page:
                    break
            results[topic.key] = {'fetched': fetched, 'inserted': inserted, **({'error': error} if error else {})}
        failed = sum(1 for item in results.values() if 'error' in item)
        with self._lock:
            self._last_collect_at = observed_at
        summary = {
            'status': 'completed' if not failed else ('failed' if failed == len(results) else 'partial'),
            'observed_at': observed_at.isoformat(),
            'requests': calls,
            'fetched': sum(item['fetched'] for item in results.values()),
            'inserted': sum(item['inserted'] for item in results.values()),
            'failed_topics': failed,
            'topics': results,
        }
        logger.info(
            'market_news_collect status=%s requests=%s inserted=%s failed_topics=%s',
            summary['status'], calls, summary['inserted'], failed,
        )
        return summary

    def _store(self, topic: MarketNewsTopic, items: list[Any], observed_at: datetime) -> int:
        rows = []
        for item in items:
            url = str(item.url or '').strip()
            if not url:
                continue
            rows.append(
                {
                    'topic_key': topic.key,
                    'region': topic.region,
                    'provider': item.provider,
                    'url': url,
                    'url_hash': _url_hash(url),
                    'title': item.title,
                    'description': item.description,
                    'published_at': item.published_at,
                    'first_seen_at': observed_at,
                    'payload': {'query': topic.query, 'source_url': item.source_url},
                }
            )
        if not rows:
            return 0
        statement = (
            insert(MarketNewsItem)
            .values(rows)
            .on_conflict_do_nothing(constraint='uq_market_news_items_topic_url')
            .returning(MarketNewsItem.id)
        )
        with session_scope() as session:
            return len(session.execute(statement).all())

    async def collect_if_due(self, *, now: datetime | None = None) -> dict[str, Any]:
        if not settings.market_news_enabled:
            return {'status': 'skipped', 'reason': 'MARKET_NEWS_ENABLED=false'}
        status = self.provider.status()
        if not status.enabled:
            return {'status': 'skipped', 'reason': status.reason}
        now = now or datetime.now(timezone.utc)
        interval = timedelta(minutes=max(1, int(settings.market_news_collect_interval_minutes)))
        with self._lock:
            last = self._last_collect_at
        if last is None:
            last = await asyncio.to_thread(self.latest_first_seen_at)
        if last is not None and now - last < interval:
            return {'status': 'skipped', 'reason': 'not_due', 'next_due_at': (last + interval).isoformat()}
        return await self.collect(now=now)

    # ------------------------------------------------------------------- read
    def latest_first_seen_at(self) -> datetime | None:
        with session_scope() as session:
            return session.execute(select(func.max(MarketNewsItem.first_seen_at))).scalar()

    def list_items(
        self,
        *,
        topic_keys: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        as_of: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Newest first. ``as_of`` keeps only articles Core had seen before it."""

        query = select(MarketNewsItem).order_by(
            MarketNewsItem.published_at.desc().nullslast(), MarketNewsItem.id.desc()
        )
        if topic_keys is not None:
            query = query.where(MarketNewsItem.topic_key.in_([topic.key for topic in resolve_topics(topic_keys)]))
        if start is not None:
            query = query.where(MarketNewsItem.published_at >= start)
        if end is not None:
            query = query.where(MarketNewsItem.published_at < end)
        if as_of is not None:
            query = query.where(MarketNewsItem.first_seen_at < as_of)
        with session_scope() as session:
            rows = session.execute(query.limit(max(1, min(int(limit), 500)))).scalars().all()
            return [
                {
                    'topic_key': row.topic_key,
                    'region': row.region,
                    'title': row.title,
                    'description': row.description,
                    'url': row.url,
                    'published_at': row.published_at.isoformat() if row.published_at else None,
                    'first_seen_at': row.first_seen_at.isoformat(),
                }
                for row in rows
            ]

    def daily_topic_counts(self, *, start_day: date, end_day: date) -> dict[str, dict[str, int]]:
        """Articles per KST publication day and topic, ``{'2026-11-06': {'line_hbm': 12}}``."""

        start, _ = _kst_day_bounds(start_day)
        _, end = _kst_day_bounds(end_day)
        day = func.date(func.timezone('Asia/Seoul', MarketNewsItem.published_at))
        query = (
            select(day, MarketNewsItem.topic_key, func.count())
            .where(MarketNewsItem.published_at >= start, MarketNewsItem.published_at < end)
            .group_by(day, MarketNewsItem.topic_key)
        )
        counts: dict[str, dict[str, int]] = defaultdict(dict)
        with session_scope() as session:
            for row_day, topic_key, count in session.execute(query):
                counts[row_day.isoformat()][topic_key] = int(count)
        return dict(counts)


market_news_service = MarketNewsService()
