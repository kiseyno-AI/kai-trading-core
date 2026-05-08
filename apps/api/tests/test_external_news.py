from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx

from app.core.settings import settings
from app.external_news.classifier import classify_external_news_item
from app.external_news.provider import NaverNewsSearchProvider
from app.external_news.service import ExternalNewsService
from app.external_news.provider import ExternalNewsItem
from app.agent_context.read_service import (
    _build_external_news_freshness_provenance,
    _build_recent_external_news_context,
)


class FakeExternalNewsProvider:
    provider_name = 'fake_news'

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.queries: list[str] = []

    def status(self):
        enabled = self.enabled
        reason = None if enabled else 'fake_credentials_missing'
        return SimpleNamespace(
            enabled=enabled,
            reason=reason,
            as_dict=lambda: {
                'provider': 'fake_news',
                'enabled': enabled,
                'status': 'available' if enabled else 'disabled',
                'reason': reason,
            },
        )

    async def search_news(self, *, query: str, display: int = 10, start: int = 1):
        self.queries.append(query)
        return [
            ExternalNewsItem(
                provider='fake_news',
                provider_item_id='item-1',
                title='삼성전자 공급 계약',
                description='테스트 설명',
                url='https://example.test/news/1',
                source_url='https://example.test/news/1',
                published_at=datetime(2026, 5, 8, 0, 0, tzinfo=timezone.utc),
                payload={'query': query},
            )
        ]


class ExternalNewsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_is_disabled_without_provider_credentials_before_symbol_lookup(self) -> None:
        provider = FakeExternalNewsProvider(enabled=False)
        service = ExternalNewsService(provider=provider)

        with patch('app.external_news.service.load_symbols', side_effect=AssertionError('should not load symbols')):
            result = await service.collect_for_symbols(symbols=['005930'])

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(result['reason'], 'fake_credentials_missing')
        self.assertEqual(result['summary']['requested_symbol_count'], 1)
        self.assertEqual(result['summary']['items_fetched'], 0)

    async def test_collect_builds_symbol_query_and_persists_normalized_items(self) -> None:
        provider = FakeExternalNewsProvider(enabled=True)
        service = ExternalNewsService(provider=provider)

        with (
            patch(
                'app.external_news.service.load_symbols',
                return_value=[{'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'}],
            ),
            patch('app.external_news.service.persist_external_news_items', return_value={'inserted': 1, 'updated': 0, 'skipped': 0}) as persist,
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
        ):
            result = await service.collect_for_symbols(symbols='005930', display=1)

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(provider.queries, ['삼성전자 005930'])
        self.assertEqual(result['summary']['items_fetched'], 1)
        self.assertEqual(result['summary']['items_inserted'], 1)
        persist.assert_called_once()

    async def test_collect_for_agent_feedback_candidates_skips_eligibility_when_provider_disabled(self) -> None:
        provider = FakeExternalNewsProvider(enabled=False)
        service = ExternalNewsService(provider=provider)

        with patch(
            'app.external_news.service.load_agent_feedback_eligible_symbols',
            side_effect=AssertionError('should not inspect eligibility when provider is disabled'),
        ):
            result = await service.collect_for_agent_feedback_candidates(
                market_date='2026-05-08',
                symbols=['005930'],
            )

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(result['reason'], 'fake_credentials_missing')
        self.assertEqual(result['summary']['eligible_symbol_count'], 0)

    async def test_collect_for_agent_feedback_candidates_uses_prefiltered_symbols(self) -> None:
        provider = FakeExternalNewsProvider(enabled=True)
        service = ExternalNewsService(provider=provider)

        with (
            patch('app.external_news.service.load_agent_feedback_eligible_symbols', return_value=['005930', '000660']) as eligible,
            patch(
                'app.external_news.service.load_symbols',
                return_value=[
                    {'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'},
                    {'symbol': '000660', 'name_ko': 'SK하이닉스', 'market': 'KOSPI'},
                ],
            ),
            patch('app.external_news.service.persist_external_news_items', return_value={'inserted': 1, 'updated': 0, 'skipped': 0}),
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
        ):
            result = await service.collect_for_agent_feedback_candidates(
                market_date='2026-05-08',
                symbols=['005930', '035420'],
                display=1,
            )

        eligible.assert_called_once()
        self.assertEqual(result['trigger'], 'agent_feedback_candidates')
        self.assertEqual(result['eligible_symbols'], ['005930', '000660'])
        self.assertEqual(provider.queries, ['삼성전자 005930', 'SK하이닉스 000660'])
        self.assertEqual(result['summary']['eligible_symbol_count'], 2)


class NaverNewsProviderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_client_id = settings.naver_search_client_id
        self.original_client_secret = settings.naver_search_client_secret
        settings.naver_search_client_id = 'test-client-id'
        settings.naver_search_client_secret = 'test-client-secret'

    def tearDown(self) -> None:
        settings.naver_search_client_id = self.original_client_id
        settings.naver_search_client_secret = self.original_client_secret

    async def test_naver_provider_strips_html_and_parses_pubdate(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.headers['X-Naver-Client-Id'], 'test-client-id')
            return httpx.Response(
                200,
                json={
                    'items': [
                        {
                            'title': '<b>삼성전자</b> 강세',
                            'description': 'AI &amp; 반도체 기대',
                            'originallink': 'https://news.example/1',
                            'link': 'https://search.naver.example/1',
                            'pubDate': 'Fri, 08 May 2026 09:00:00 +0900',
                        }
                    ]
                },
            )

        provider = NaverNewsSearchProvider(transport=httpx.MockTransport(handler))
        items = await provider.search_news(query='삼성전자 005930', display=1)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].title, '삼성전자 강세')
        self.assertEqual(items[0].description, 'AI & 반도체 기대')
        self.assertEqual(items[0].url, 'https://news.example/1')
        self.assertEqual(items[0].published_at.isoformat(), '2026-05-08T00:00:00+00:00')


class ExternalNewsClassifierTests(unittest.TestCase):
    def test_rule_based_classifier_tags_sentiment_and_materiality(self) -> None:
        classification = classify_external_news_item(
            title='삼성전자 대규모 반도체 공급 계약 체결, 실적 개선 기대',
            description='증권가는 영업이익 성장과 목표주가 상향을 전망했다.',
            payload={'raw': {'pubDate': 'Fri, 08 May 2026 09:00:00 +0900'}},
        )

        self.assertIn('contract', classification['event_tags'])
        self.assertIn('earnings', classification['event_tags'])
        self.assertEqual(classification['sentiment'], 'positive')
        self.assertGreaterEqual(classification['materiality_score'], 0.5)
        self.assertEqual(classification['classifier'], 'rule_based_external_news_v1')


class ExternalNewsContextTests(unittest.TestCase):
    def test_context_distinguishes_provider_unavailable_from_no_news(self) -> None:
        disabled = _build_recent_external_news_context(
            [],
            provider_status={
                'provider': 'naver_news_search',
                'enabled': False,
                'status': 'disabled',
                'reason': 'naver_search_credentials_missing',
            },
        )
        empty = _build_recent_external_news_context(
            [],
            provider_status={'provider': 'naver_news_search', 'enabled': True, 'status': 'available', 'reason': None},
        )

        self.assertEqual(disabled['status'], 'unavailable')
        self.assertEqual(disabled['reason'], 'naver_search_credentials_missing')
        self.assertEqual(empty['status'], 'empty')
        self.assertEqual(empty['reason'], 'no_recent_external_news_found')

        provenance = _build_external_news_freshness_provenance(
            context=disabled,
            requested_market_date=datetime(2026, 5, 8, tzinfo=timezone.utc).date(),
        )
        self.assertEqual(provenance['freshness_status'], 'missing')
        self.assertEqual(provenance['provenance_status'], 'external_provider_unavailable')

    def test_context_exposes_news_classification_fields(self) -> None:
        context = _build_recent_external_news_context(
            [
                {
                    'provider': 'fake_news',
                    'title': '삼성전자 공급 계약',
                    'description': '실적 개선 기대',
                    'url': 'https://example.test/news/1',
                    'published_at': '2026-05-08T00:00:00+00:00',
                    'collected_at': '2026-05-08T00:01:00+00:00',
                    'event_tags': ['contract', 'earnings'],
                    'sentiment': 'positive',
                    'materiality_score': 0.62,
                    'materiality_bucket': 'medium',
                }
            ],
            provider_status={'provider': 'fake_news', 'enabled': True, 'status': 'available', 'reason': None},
        )

        self.assertEqual(context['status'], 'available')
        self.assertEqual(context['items'][0]['event_tags'], ['contract', 'earnings'])
        self.assertEqual(context['items'][0]['sentiment'], 'positive')
        self.assertEqual(context['classification_summary']['event_tag_counts']['contract'], 1)
        self.assertEqual(context['classification_summary']['max_materiality_score'], 0.62)


if __name__ == '__main__':
    unittest.main()
