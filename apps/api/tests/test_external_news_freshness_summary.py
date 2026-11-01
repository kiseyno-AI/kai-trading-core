from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.core.settings import settings
from app.external_news.provider import ExternalNewsItem
from app.external_news.service import ExternalNewsService


def _news_item(item_id: str, published_at: datetime) -> ExternalNewsItem:
    return ExternalNewsItem(
        provider='freshness_test',
        provider_item_id=item_id,
        title=f'item-{item_id}',
        description=None,
        url=f'https://example.test/{item_id}',
        source_url=None,
        published_at=published_at,
        payload={},
    )


class _FreshnessProvider:
    provider_name = 'freshness_test'

    def __init__(self) -> None:
        self._results = iter(
            [
                [_news_item('recent', datetime(2026, 10, 29, tzinfo=timezone.utc))],
                [_news_item('stale', datetime(2026, 10, 1, tzinfo=timezone.utc))],
                [_news_item('future', datetime(2026, 10, 31, tzinfo=timezone.utc))],
                [],
            ]
        )

    @staticmethod
    def status() -> SimpleNamespace:
        return SimpleNamespace(
            enabled=True,
            reason=None,
            as_dict=lambda: {
                'provider': 'freshness_test',
                'enabled': True,
                'status': 'available',
                'reason': None,
            },
        )

    async def search_news(
        self,
        *,
        query: str,
        display: int = 10,
        start: int = 1,
    ) -> list[ExternalNewsItem]:
        del query, display, start
        return next(self._results)


class _DisabledProvider:
    provider_name = 'disabled_test'

    @staticmethod
    def status() -> SimpleNamespace:
        return SimpleNamespace(
            enabled=False,
            reason='credentials_missing',
            as_dict=lambda: {
                'provider': 'disabled_test',
                'enabled': False,
                'status': 'disabled',
                'reason': 'credentials_missing',
            },
        )


class ExternalNewsFreshnessSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_feedback_news_summary_keeps_explicit_zero_buckets(
        self,
    ) -> None:
        service = ExternalNewsService(provider=_DisabledProvider())

        result = await service.collect_for_agent_feedback_candidates(
            market_date='2026-10-30',
            symbols=['005930'],
        )

        self.assertEqual(result['summary']['freshness_lookback_days'], 14)
        self.assertEqual(
            result['summary']['freshness_cutoff_at'],
            '2026-10-15T15:00:00+00:00',
        )
        self.assertEqual(
            result['summary']['freshness_published_before'],
            '2026-10-30T15:00:00+00:00',
        )
        self.assertEqual(result['summary']['recent_news_symbol_count'], 0)
        self.assertEqual(result['summary']['stale_news_symbol_count'], 0)
        self.assertEqual(result['summary']['no_result_symbol_count'], 0)

    async def test_feedback_news_summary_separates_recent_stale_and_no_result_symbols(
        self,
    ) -> None:
        service = ExternalNewsService(provider=_FreshnessProvider())
        symbols = ['005930', '000660', '035420', '051910']
        symbol_rows = [
            {'symbol': symbol, 'name_ko': symbol, 'market': 'KOSPI'}
            for symbol in symbols
        ]

        with (
            patch(
                'app.external_news.service.load_agent_feedback_eligible_symbols',
                return_value=symbols,
            ),
            patch('app.external_news.service.load_symbols', return_value=symbol_rows),
            patch(
                'app.external_news.service.persist_external_news_items',
                return_value={'inserted': 0, 'updated': 0, 'skipped': 0},
            ),
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
            patch.object(settings, 'external_news_context_lookback_days', 14),
        ):
            result = await service.collect_for_agent_feedback_candidates(
                market_date='2026-10-30',
                symbols=symbols,
            )

        self.assertEqual(result['summary']['freshness_lookback_days'], 14)
        self.assertEqual(
            result['summary']['freshness_cutoff_at'],
            '2026-10-15T15:00:00+00:00',
        )
        self.assertEqual(
            result['summary']['freshness_published_before'],
            '2026-10-30T15:00:00+00:00',
        )
        self.assertEqual(result['summary']['recent_news_symbol_count'], 1)
        self.assertEqual(result['summary']['stale_news_symbol_count'], 2)
        self.assertEqual(result['summary']['no_result_symbol_count'], 1)
        self.assertEqual(result['summary']['symbols_with_news'], 1)
        self.assertEqual(result['summary']['symbols_with_any_news'], 3)
        self.assertEqual(
            [item['fetched_count'] for item in result['items']],
            [1, 1, 1, 0],
        )
