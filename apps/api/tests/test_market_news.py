import asyncio
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import delete

from app.core.settings import settings
from app.db import engine, session_scope
from app.db_models import MarketNewsItem
from app.external_news.provider import ExternalNewsItem, ExternalNewsProviderStatus
from app.market_news.catalog import MARKET_NEWS_TOPICS, resolve_topics
from app.market_news.service import MarketNewsService


class FakeProvider:
    provider_name = 'naver_news_search'

    def __init__(self, articles_by_query=None, fail_queries=(), enabled=True):
        self.articles_by_query = articles_by_query or {}
        self.fail_queries = set(fail_queries)
        self.enabled = enabled
        self.calls = []

    def status(self):
        return ExternalNewsProviderStatus(
            provider=self.provider_name,
            enabled=self.enabled,
            status='available' if self.enabled else 'disabled',
            reason=None if self.enabled else 'naver_search_credentials_missing',
        )

    async def search_news(self, *, query, display=10, start=1):
        self.calls.append((query, display, start))
        if query in self.fail_queries:
            raise RuntimeError('Naver News Search request failed [429]')
        items = self.articles_by_query.get(query, [])
        return items[start - 1:start - 1 + display]


def _article(url, published):
    return ExternalNewsItem(
        provider='naver_news_search',
        provider_item_id=url,
        title=f'title {url}',
        description='desc',
        url=url,
        source_url=None,
        published_at=published,
        payload={},
    )


T0 = datetime(2026, 11, 6, 1, 0, tzinfo=timezone.utc)  # 10:00 KST


class MarketNewsServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        MarketNewsItem.__table__.create(bind=engine, checkfirst=True)

    def setUp(self):
        with session_scope() as session:
            session.execute(delete(MarketNewsItem))
        self.addCleanup(self._clean)
        patcher = patch.object(settings, 'external_news_collect_throttle_seconds', 0.0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _clean(self):
        with session_scope() as session:
            session.execute(delete(MarketNewsItem))

    def test_collect_stores_once_per_topic_and_url(self):
        provider = FakeProvider({'HBM 메모리': [_article('https://a/1', T0), _article('https://a/2', T0)]})
        service = MarketNewsService(provider)

        first = asyncio.run(service.collect(topic_keys=['line_hbm'], now=T0))
        again = asyncio.run(service.collect(topic_keys=['line_hbm'], now=T0 + timedelta(hours=1)))

        self.assertEqual((first['status'], first['inserted']), ('completed', 2))
        self.assertEqual(again['inserted'], 0)
        items = service.list_items(topic_keys=['line_hbm'])
        self.assertEqual(len(items), 2)
        # The first observation time is kept, not overwritten by the re-fetch.
        self.assertTrue(all(datetime.fromisoformat(item['first_seen_at']) == T0 for item in items))

    def test_one_failing_topic_does_not_stop_the_rest(self):
        provider = FakeProvider({'조선 수주': [_article('https://s/1', T0)]}, fail_queries={'방산 수출'})
        service = MarketNewsService(provider)

        result = asyncio.run(service.collect(topic_keys=['line_defense', 'line_shipbuilding'], now=T0))

        self.assertEqual(result['status'], 'partial')
        self.assertIn('429', result['topics']['line_defense']['error'])
        self.assertEqual(result['topics']['line_shipbuilding']['inserted'], 1)

    def test_backfill_pages_until_a_short_page(self):
        articles = [_article(f'https://b/{i}', T0 - timedelta(hours=i)) for i in range(120)]
        provider = FakeProvider({'2차전지 배터리': articles})
        service = MarketNewsService(provider)

        result = asyncio.run(service.collect(topic_keys=['line_battery'], pages=5, display=50, now=T0))

        self.assertEqual(result['inserted'], 120)
        self.assertEqual([call[2] for call in provider.calls], [1, 51, 101])

    def test_as_of_cuts_at_first_observation(self):
        provider = FakeProvider({'코스피': [_article('https://k/1', T0)]})
        service = MarketNewsService(provider)
        asyncio.run(service.collect(topic_keys=['kr_kospi'], now=T0))
        provider.articles_by_query['코스피'].append(_article('https://k/2', T0))
        asyncio.run(service.collect(topic_keys=['kr_kospi'], now=T0 + timedelta(hours=2)))

        before = service.list_items(topic_keys=['kr_kospi'], as_of=T0 + timedelta(hours=1))

        self.assertEqual([item['url'] for item in before], ['https://k/1'])

    def test_daily_counts_use_the_korean_publication_day(self):
        late_utc = datetime(2026, 11, 5, 16, 30, tzinfo=timezone.utc)  # 2026-11-06 01:30 KST
        provider = FakeProvider({'국제유가': [_article('https://o/1', late_utc), _article('https://o/2', T0)]})
        service = MarketNewsService(provider)
        asyncio.run(service.collect(topic_keys=['global_oil'], now=T0))

        counts = service.daily_topic_counts(start_day=date(2026, 11, 5), end_day=date(2026, 11, 6))

        self.assertEqual(counts, {'2026-11-06': {'global_oil': 2}})

    def test_collect_if_due_waits_for_the_interval_and_skips_without_credentials(self):
        provider = FakeProvider({})
        service = MarketNewsService(provider)
        with patch.object(settings, 'market_news_collect_interval_minutes', 60):
            asyncio.run(service.collect(topic_keys=['kr_kospi'], now=T0))
            self.assertEqual(asyncio.run(service.collect_if_due(now=T0 + timedelta(minutes=30)))['reason'], 'not_due')
        disabled = MarketNewsService(FakeProvider(enabled=False))
        self.assertEqual(asyncio.run(disabled.collect(now=T0))['status'], 'skipped')


class CatalogTests(unittest.TestCase):
    def test_topic_keys_are_unique_and_unknown_keys_fail(self):
        keys = [topic.key for topic in MARKET_NEWS_TOPICS]
        self.assertEqual(len(keys), len(set(keys)))
        with self.assertRaises(ValueError):
            resolve_topics(['no_such_topic'])


if __name__ == '__main__':
    unittest.main()
