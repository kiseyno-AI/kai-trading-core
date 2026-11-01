from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sqlalchemy.exc import IntegrityError

from app.core.settings import settings
from app.external_news.service import ExternalNewsService


class _SequencedProvider:
    provider_name = 'sequenced_news'

    def __init__(self, outcomes: list[object]) -> None:
        self._outcomes = iter(outcomes)
        self.queries: list[str] = []

    @staticmethod
    def status() -> SimpleNamespace:
        return SimpleNamespace(
            enabled=True,
            reason=None,
            as_dict=lambda: {
                'provider': 'sequenced_news',
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
    ) -> list[object]:
        del display, start
        self.queries.append(query)
        outcome = next(self._outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return list(outcome)


def _symbol_rows(*symbols: str) -> list[dict[str, str]]:
    return [
        {
            'symbol': symbol,
            'name_ko': f'name-{symbol}',
            'market': 'KOSPI',
        }
        for symbol in symbols
    ]


class ExternalNewsResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_failure_isolated_and_later_symbol_still_collected(self) -> None:
        provider = _SequencedProvider([RuntimeError('temporary outage'), []])
        service = ExternalNewsService(provider=provider)

        with (
            patch(
                'app.external_news.service.load_symbols',
                return_value=_symbol_rows('005930', '000660'),
            ),
            patch(
                'app.external_news.service.persist_external_news_items',
                return_value={'inserted': 0, 'updated': 0, 'skipped': 0},
            ) as persist,
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
        ):
            result = await service.collect_for_symbols(
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['summary']['collected_symbol_count'], 1)
        self.assertEqual(result['summary']['failed_symbol_count'], 1)
        self.assertEqual(result['summary']['skipped_symbol_count'], 0)
        self.assertFalse(result['summary']['provider_circuit_open'])
        self.assertEqual([item['status'] for item in result['items']], ['failed', 'ok'])
        self.assertEqual(len(provider.queries), 2)
        persist.assert_called_once()

    async def test_repeated_provider_failures_open_circuit_and_skip_remaining_symbols(self) -> None:
        provider = _SequencedProvider(
            [RuntimeError('outage one'), RuntimeError('outage two')]
        )
        service = ExternalNewsService(provider=provider)
        symbols = ['005930', '000660', '035420', '051910']

        with (
            patch(
                'app.external_news.service.load_symbols',
                return_value=_symbol_rows(*symbols),
            ),
            patch(
                'app.external_news.service.persist_external_news_items',
                side_effect=AssertionError('failed provider results must not persist'),
            ),
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
        ):
            result = await service.collect_for_symbols(symbols=symbols)

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['summary']['collected_symbol_count'], 0)
        self.assertEqual(result['summary']['failed_symbol_count'], 2)
        self.assertEqual(result['summary']['skipped_symbol_count'], 2)
        self.assertTrue(result['summary']['provider_circuit_open'])
        self.assertEqual(len(provider.queries), 2)
        self.assertEqual(
            [item['reason'] for item in result['items'][2:]],
            ['provider_circuit_open', 'provider_circuit_open'],
        )

    async def test_persistence_failure_does_not_trip_provider_circuit(self) -> None:
        provider = _SequencedProvider([[], []])
        service = ExternalNewsService(provider=provider)

        with (
            patch(
                'app.external_news.service.load_symbols',
                return_value=_symbol_rows('005930', '000660'),
            ),
            patch(
                'app.external_news.service.persist_external_news_items',
                side_effect=[
                    RuntimeError('storage unavailable'),
                    {'inserted': 0, 'updated': 0, 'skipped': 0},
                ],
            ),
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
        ):
            result = await service.collect_for_symbols(
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['summary']['collected_symbol_count'], 1)
        self.assertEqual(result['summary']['failed_symbol_count'], 1)
        self.assertFalse(result['summary']['provider_circuit_open'])
        self.assertEqual(len(provider.queries), 2)

    async def test_persistence_integrity_failure_remains_fatal(self) -> None:
        provider = _SequencedProvider([[]])
        service = ExternalNewsService(provider=provider)
        integrity_error = IntegrityError(
            'insert into external_news_items',
            {},
            RuntimeError('constraint violation'),
        )

        with (
            patch(
                'app.external_news.service.load_symbols',
                return_value=_symbol_rows('005930'),
            ),
            patch(
                'app.external_news.service.persist_external_news_items',
                side_effect=integrity_error,
            ),
            patch.object(
                settings,
                'external_news_collect_throttle_seconds',
                0,
            ),
            self.assertRaises(IntegrityError),
        ):
            await service.collect_for_symbols(symbols=['005930'])

    async def test_feedback_collection_preserves_actual_collected_symbol_count(self) -> None:
        provider = _SequencedProvider([RuntimeError('temporary outage'), []])
        service = ExternalNewsService(provider=provider)

        with (
            patch(
                'app.external_news.service.load_agent_feedback_eligible_symbols',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.external_news.service.load_symbols',
                return_value=_symbol_rows('005930', '000660'),
            ),
            patch(
                'app.external_news.service.persist_external_news_items',
                return_value={'inserted': 0, 'updated': 0, 'skipped': 0},
            ),
            patch.object(settings, 'external_news_collect_throttle_seconds', 0),
        ):
            result = await service.collect_for_agent_feedback_candidates(
                market_date='2026-10-30',
                symbols=['005930', '000660'],
            )

        self.assertEqual(result['summary']['eligible_symbol_count'], 2)
        self.assertEqual(result['summary']['collected_symbol_count'], 1)
        self.assertEqual(result['summary']['failed_symbol_count'], 1)


if __name__ == '__main__':
    unittest.main()
