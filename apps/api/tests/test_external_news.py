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
from app.external_news.store import _dedupe_provider_news_items
from app.agent_context.read_service import (
    _build_decider_disclosures_news_quality_context,
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


def test_dedupe_provider_news_items_skips_duplicate_provider_ids_and_urls() -> None:
    published_at = datetime(2026, 5, 8, 0, 0, tzinfo=timezone.utc)
    items = [
        ExternalNewsItem(
            provider='fake_news',
            provider_item_id='item-1',
            title='first',
            description=None,
            url='https://example.test/news/1',
            source_url=None,
            published_at=published_at,
            payload={},
        ),
        ExternalNewsItem(
            provider='fake_news',
            provider_item_id='item-1',
            title='duplicate id',
            description=None,
            url='https://example.test/news/1-copy',
            source_url=None,
            published_at=published_at,
            payload={},
        ),
        ExternalNewsItem(
            provider='fake_news',
            provider_item_id='item-2',
            title='duplicate url',
            description=None,
            url='https://example.test/news/1',
            source_url=None,
            published_at=published_at,
            payload={},
        ),
        ExternalNewsItem(
            provider='fake_news',
            provider_item_id='item-3',
            title='second',
            description=None,
            url='https://example.test/news/2',
            source_url=None,
            published_at=published_at,
            payload={},
        ),
    ]

    deduped, skipped = _dedupe_provider_news_items(items)

    assert [item.title for item in deduped] == ['first', 'second']
    assert skipped == 2


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
        self.assertEqual(items[0].payload['access_status'], 'headline_only')
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
        self.assertEqual(classification['event_type'], 'earnings')
        self.assertEqual(classification['sentiment'], 'positive')
        self.assertEqual(classification['materiality'], 'material')
        self.assertIn('material event', classification['materiality_reason'])
        self.assertGreaterEqual(classification['materiality_score'], 0.5)
        self.assertEqual(classification['access_status'], 'headline_only')
        self.assertTrue(classification['price_forecast_prohibited'])
        self.assertEqual(classification['classifier'], 'rule_based_external_news_contract_v1')

    def test_rule_based_classifier_labels_noise_event(self) -> None:
        classification = classify_external_news_item(
            title='삼성전자 주가 흐름 두고 시장 관심',
            description='전문가들은 업종 전반의 투자심리를 점검했다.',
        )

        self.assertEqual(classification['event_type'], 'generic_commentary')
        self.assertEqual(classification['materiality'], 'noise')
        self.assertIn('background commentary', classification['materiality_reason'])
        self.assertEqual(classification['access_status'], 'headline_only')

    def test_rule_based_classifier_preserves_paywalled_headline_only_status(self) -> None:
        classification = classify_external_news_item(
            title='삼성전자 목표주가 상향 리포트',
            description='구독자 전용 리포트에서 실적 전망을 다뤘다.',
            payload={'access_status': 'paywalled'},
            url='https://research.example.test/report/1',
            source_url='https://search.example.test/report/1',
            published_at=datetime(2026, 5, 8, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(classification['access_status'], 'paywalled')
        self.assertFalse(classification['content_basis'] == 'full_text')
        self.assertEqual(classification['source_evidence']['url'], 'https://research.example.test/report/1')
        self.assertEqual(classification['source_evidence']['publication'], 'research.example.test')
        self.assertEqual(classification['source_evidence']['published_at'], '2026-05-08T00:00:00+00:00')
        self.assertIn('full article text was not read', classification['summary_policy'])

    def test_rule_based_classifier_handles_missing_source_evidence(self) -> None:
        classification = classify_external_news_item(
            title='삼성전자 실적 발표',
            description='매출과 영업이익이 공개됐다.',
            payload={},
        )

        self.assertEqual(classification['materiality'], 'material')
        self.assertIsNone(classification['source_evidence']['url'])
        self.assertIsNone(classification['source_evidence']['publication'])
        self.assertIsNone(classification['source_evidence']['published_at'])


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
                    'title': '삼성전자 주가 흐름 시장 관심',
                    'description': '업종 전반의 심리 점검 기사',
                    'url': 'https://example.test/news/noise',
                    'published_at': '2026-05-08T00:02:00+00:00',
                    'collected_at': '2026-05-08T00:03:00+00:00',
                    'event_tags': ['general_news'],
                    'event_type': 'generic_commentary',
                    'sentiment': 'neutral',
                    'materiality': 'noise',
                    'materiality_reason': 'No company-specific material event keywords were detected.',
                    'materiality_score': 0.1,
                    'materiality_bucket': 'low',
                    'access_status': 'headline_only',
                    'source_evidence': {
                        'url': 'https://example.test/news/noise',
                        'publication': 'example.test',
                        'published_at': '2026-05-08T00:02:00+00:00',
                    },
                },
                {
                    'provider': 'fake_news',
                    'title': '삼성전자 공급 계약',
                    'description': '실적 개선 기대',
                    'url': 'https://example.test/news/1',
                    'published_at': '2026-05-08T00:00:00+00:00',
                    'collected_at': '2026-05-08T00:01:00+00:00',
                    'event_tags': ['contract', 'earnings'],
                    'event_type': 'contract',
                    'sentiment': 'positive',
                    'materiality': 'material',
                    'materiality_reason': 'Detected company-specific material event type "contract".',
                    'materiality_score': 0.62,
                    'materiality_bucket': 'medium',
                    'access_status': 'full_text',
                    'source_evidence': {
                        'url': 'https://example.test/news/1',
                        'publication': 'example.test',
                        'published_at': '2026-05-08T00:00:00+00:00',
                    },
                },
                {
                    'provider': 'fake_news',
                    'title': '삼성전자 프리미엄 리포트',
                    'description': '구독자 전용 기사',
                    'url': None,
                    'published_at': None,
                    'collected_at': '2026-05-08T00:04:00+00:00',
                    'event_tags': ['analyst'],
                    'event_type': 'analyst_rating',
                    'sentiment': 'neutral',
                    'materiality': 'unknown',
                    'materiality_reason': 'No URL/publication date was available for source-backed classification.',
                    'materiality_score': 0.2,
                    'materiality_bucket': 'low',
                    'access_status': 'paywalled',
                    'source_evidence': {
                        'url': None,
                        'publication': None,
                        'published_at': None,
                    },
                }
            ],
            provider_status={'provider': 'fake_news', 'enabled': True, 'status': 'available', 'reason': None},
        )

        self.assertEqual(context['status'], 'available')
        self.assertTrue(context['contract']['price_forecast_prohibited'])
        self.assertEqual(context['items'][0]['materiality'], 'material')
        self.assertEqual(context['items'][0]['event_tags'], ['contract', 'earnings'])
        self.assertEqual(context['items'][0]['sentiment'], 'positive')
        self.assertEqual(context['items'][0]['source_evidence']['url'], 'https://example.test/news/1')
        self.assertEqual(context['material_items'][0]['title'], '삼성전자 공급 계약')
        self.assertEqual(context['unknown_items'][0]['access_status'], 'paywalled')
        self.assertEqual(context['unknown_items'][0]['source_evidence']['url'], None)
        self.assertEqual(context['noise_filter']['item_count'], 1)
        self.assertEqual(context['noise_filter']['items'][0]['materiality'], 'noise')
        self.assertEqual(context['classification_summary']['event_tag_counts']['contract'], 1)
        self.assertEqual(context['classification_summary']['materiality_counts']['material'], 1)
        self.assertEqual(context['classification_summary']['materiality_counts']['noise'], 1)
        self.assertEqual(context['classification_summary']['access_status_counts']['paywalled'], 1)
        self.assertEqual(context['classification_summary']['max_materiality_score'], 0.62)

    def test_freshness_provenance_uses_latest_news_timestamp_after_material_sorting(self) -> None:
        provider_status = {'provider': 'fake_news', 'enabled': True, 'status': 'available', 'reason': None}
        older_material_timestamp = '2026-05-08T00:00:00+00:00'
        newer_noise_timestamp = '2026-05-08T00:05:00+00:00'

        with patch.object(settings, 'external_news_context_limit', 1):
            context = _build_recent_external_news_context(
                [
                    {
                        'provider': 'fake_news',
                        'title': '삼성전자 공급 계약',
                        'description': '회사별 계약 기사',
                        'url': 'https://example.test/news/material',
                        'published_at': older_material_timestamp,
                        'collected_at': '2026-05-08T00:01:00+00:00',
                        'event_tags': ['contract'],
                        'event_type': 'contract',
                        'sentiment': 'positive',
                        'materiality': 'material',
                        'materiality_reason': 'Detected company-specific material event type "contract".',
                        'materiality_score': 0.9,
                        'materiality_bucket': 'high',
                        'access_status': 'full_text',
                    },
                    {
                        'provider': 'fake_news',
                        'title': '삼성전자 시장 관심',
                        'description': '업종 전반 관심 기사',
                        'url': 'https://example.test/news/noise',
                        'published_at': newer_noise_timestamp,
                        'collected_at': '2026-05-08T00:06:00+00:00',
                        'event_tags': ['general_news'],
                        'event_type': 'generic_commentary',
                        'sentiment': 'neutral',
                        'materiality': 'noise',
                        'materiality_reason': 'No company-specific material event keywords were detected.',
                        'materiality_score': 0.1,
                        'materiality_bucket': 'low',
                        'access_status': 'headline_only',
                    },
                ],
                provider_status=provider_status,
            )

        provenance = _build_external_news_freshness_provenance(
            context=context,
            requested_market_date=datetime(2026, 5, 8, tzinfo=timezone.utc).date(),
        )

        self.assertEqual(context['items'][0]['title'], '삼성전자 공급 계약')
        self.assertEqual(context['items'][0]['published_at'], older_material_timestamp)
        self.assertEqual(context['latest_published_at'], newer_noise_timestamp)
        self.assertEqual(provenance['latest_available_timestamp'], newer_noise_timestamp)

    def test_decider_quality_context_distinguishes_empty_disclosures_from_news_provider_error(self) -> None:
        block = _build_decider_disclosures_news_quality_context(
            recent_dart_events={
                'status': 'empty',
                'reason': 'no_recent_disclosures',
                'summary': {},
                'item_count': 0,
            },
            recent_disclosures_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_disclosures_up_to_request_date',
            },
            recent_external_news={
                'status': 'unavailable',
                'reason': 'naver_search_credentials_missing',
                'provider': {
                    'provider': 'naver_news_search',
                    'enabled': False,
                    'status': 'disabled',
                    'reason': 'naver_search_credentials_missing',
                },
            },
            recent_external_news_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'external_provider_unavailable',
                'reason': 'naver_search_credentials_missing',
            },
        )

        self.assertEqual(block['status'], 'provider_error')
        self.assertEqual(block['providers']['dart_recent_events']['status'], 'empty')
        self.assertEqual(block['providers']['external_news']['status'], 'provider_error')
        self.assertFalse(block['empty_news_is_neutral'])
        self.assertEqual(
            block['providers']['external_news']['provider_reason'],
            'naver_search_credentials_missing',
        )

    def test_decider_quality_context_treats_empty_news_as_auxiliary_neutral_context(self) -> None:
        block = _build_decider_disclosures_news_quality_context(
            recent_dart_events={
                'status': 'empty',
                'reason': 'no_recent_disclosures',
                'summary': {},
                'item_count': 0,
            },
            recent_disclosures_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_disclosures_up_to_request_date',
            },
            recent_external_news={
                'status': 'empty',
                'reason': 'no_recent_external_news_found',
                'provider': {
                    'provider': 'naver_news_search',
                    'enabled': True,
                    'status': 'available',
                    'reason': None,
                },
            },
            recent_external_news_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_external_news_found',
            },
        )

        self.assertEqual(block['status'], 'empty')
        self.assertTrue(block['empty_news_is_neutral'])
        self.assertEqual(
            block['empty_state_interpretation'],
            'No captured recent optional news context; neutral unless separate risk evidence exists.',
        )


if __name__ == '__main__':
    unittest.main()
