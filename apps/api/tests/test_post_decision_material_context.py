from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import AgentFeedbackOrchestrator


def test_post_decision_material_reasons_ignore_classified_noise() -> None:
    reasons = AgentFeedbackOrchestrator._post_decision_material_reasons(
        market_date=date(2026, 9, 11),
        news_items=[
            {'id': 1, 'materiality': 'noise'},
            {'id': 2, 'materiality': 'material'},
            {'id': 3, 'classification': {}},
        ],
        disclosures=[],
    )

    assert reasons == [
        {
            'code': 'post_decision_news_requires_redecision',
            'event_count': 2,
            'events': [
                {
                    'external_news_item_id': 2,
                    'provider': None,
                    'provider_item_id': None,
                    'materiality': 'material',
                    'published_at': None,
                    'collected_at': None,
                    'associated_at': None,
                    'classified_at': None,
                },
                {
                    'external_news_item_id': 3,
                    'provider': None,
                    'provider_item_id': None,
                    'materiality': 'unknown',
                    'published_at': None,
                    'collected_at': None,
                    'associated_at': None,
                    'classified_at': None,
                },
            ],
        }
    ]


def test_post_decision_disclosure_after_source_day_requires_redecision() -> None:
    reasons = AgentFeedbackOrchestrator._post_decision_material_reasons(
        market_date=date(2026, 9, 11),
        news_items=[],
        disclosures=[
            {'rcept_no': 'old', 'receipt_date': '2026-09-11'},
            {
                'rcept_no': 'new',
                'receipt_date': '2026-09-14',
                'filing_category': 'material_event',
                'is_financial_report': False,
            },
        ],
    )

    assert reasons == [
        {
            'code': 'post_decision_disclosure_requires_redecision',
            'event_count': 1,
            'events': [
                {
                    'rcept_no': 'new',
                    'receipt_date': '2026-09-14',
                    'filing_category': 'material_event',
                    'is_financial_report': False,
                }
            ],
        }
    ]


def test_post_decision_loader_uses_exact_observation_watermark() -> None:
    disclosure_repository = Mock()
    disclosure_repository.list_recent_events_by_stock_codes.return_value = {
        '005930': []
    }
    service = AgentFeedbackOrchestrator()
    cutoff = '2026-09-11T06:30:00+00:00'
    check_at = datetime(2026, 9, 13, 23, 0, tzinfo=timezone.utc)

    with (
        patch.object(
            orchestrator_module,
            'agent_context_read_service',
            SimpleNamespace(disclosure_repository=disclosure_repository),
        ),
        patch.object(
            orchestrator_module,
            'list_external_news_items',
            return_value=[
                {
                    'id': 77,
                    'provider': 'naver_news',
                    'materiality': 'material',
                    'collected_at': '2026-09-12T01:00:00+00:00',
                }
            ],
        ) as list_news,
    ):
        failures = service._load_post_decision_material_context_failures(
            market_date=date(2026, 9, 11),
            source_items=[
                {
                    'role': 'signal-decider',
                    'symbol': '005930',
                }
            ],
            source_knowledge_cutoff_at=cutoff,
            disclosures_limit=5,
            check_at=check_at,
        )

    assert failures[('signal-decider', '005930')]['reason'] == (
        'post_decision_material_context_changed'
    )
    assert list_news.call_args.kwargs['observed_after'] == datetime(
        2026, 9, 11, 6, 30, tzinfo=timezone.utc
    )
    assert list_news.call_args.kwargs['collected_before'] == check_at
    disclosure_repository.list_recent_events_by_stock_codes.assert_called_once_with(
        stock_codes=['005930'],
        receipt_date_to=date(2026, 9, 14),
        limit_per_symbol=5,
    )


def test_post_decision_lookup_failure_is_fail_closed() -> None:
    service = AgentFeedbackOrchestrator()
    with patch.object(
        orchestrator_module,
        'list_external_news_items',
        side_effect=RuntimeError('storage unavailable'),
    ):
        failures = service._load_post_decision_material_context_failures(
            market_date=date(2026, 9, 11),
            source_items=[
                {'role': 'short-term-decider', 'symbol': '000660'}
            ],
            source_knowledge_cutoff_at='2026-09-11T06:30:00+00:00',
            disclosures_limit=5,
            check_at=datetime(2026, 9, 13, 23, 0, tzinfo=timezone.utc),
        )

    assert failures[('short-term-decider', '000660')]['reason'] == (
        'post_decision_material_context_unavailable'
    )
    assert failures[('short-term-decider', '000660')]['reasons'][0][
        'code'
    ] == 'post_decision_news_lookup_failed'
