import asyncio
from datetime import date
from unittest.mock import patch

from app.core.settings import settings
from app.orders.execution_handoff_contract import (
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
)
from app.us_equities.execution_plan_service import USExecutionPlanService


def _row(symbol: str, *, action: str = 'buy', warnings=None, confidence=8.0, risk_flags=None) -> dict:
    return {
        'feedback_id': 101,
        'run_id': 'us-run-1',
        'symbol_id': 1,
        'symbol': symbol,
        'standard_code': symbol,
        'market': 'NASDAQ',
        'exchange_code': 'NASDAQ',
        'rank': 1,
        'composite_score': '0.9',
        # A later same-date screen may exist, but execution must use the price
        # persisted with the explicitly selected agent run.
        'metrics_payload': {'price': {'latest_price': '999.00'}},
        'context_payload': {'symbol_day_context': {'price_screen': {'latest_price': '50.00'}}},
        'decision_payload': {},
        'normalized_feedback_payload': {
            'role': 'signal-decider',
            'agent_name': 'signal-decider',
            'symbol': symbol,
            'market_date': '2026-08-19',
            'next_day_open_action': action,
            'preferred_action': action,
            'target_price': 60.0,
            'stop_loss_price': 45.0,
            'confidence_score_10': confidence,
            'data_quality_warnings': list(warnings or []),
            'risk_flags': list(risk_flags or []),
        },
    }


def test_builds_bounded_us_kis_entry_and_exit_plan() -> None:
    loader_calls = []
    service = USExecutionPlanService(
        feedback_loader=lambda **kwargs: loader_calls.append(kwargs) or [_row('AAPL')]
    )

    result = service.build(
        source_market_date=date(2026, 8, 19),
        target_trade_date=date(2026, 8, 20),
        agent_run_id='us-run-1',
    )

    assert result['status'] == 'preview_only'
    assert result['execution_supported'] is False
    assert result['execution_block_reason'] == 'exec_currency_aware_sizing_capability_unavailable'
    assert result['accepted_candidate_count'] == 1
    assert result['payload'] is None
    payload = result['preview_payload']
    assert payload['metadata'] == {
        'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
        'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
        'required_capability': EXECUTION_HANDOFF_CAPABILITY,
        'agent_name': 'signal-decider',
        'decision_agent_name': 'signal-decider',
        'trigger': 'us_equity_signal_decider',
        'requested_at': payload['requested_at'],
        'account_reference': None,
        'account_label': None,
        'source_market_date': '2026-08-19',
        'target_trade_date': '2026-08-20',
        'dispatch_batch_type': 'initial',
        'dispatch_batch_key': None,
    }
    assert payload['market_date'] == '2026-08-20'
    assert [item['intent'] for item in payload['items']] == [
        'next_open_buy',
        'target_sell',
        'stop_sell',
    ]
    entry = payload['items'][0]
    assert entry['asset_class'] == 'us_equity'
    assert entry['exchange'] == 'NASD'
    assert entry['currency'] == 'USD'
    assert entry['broker'] == 'kis'
    assert entry['limit_price'] == '50.25'
    assert entry['handoff_intent'] == 'next_open_buy'
    assert entry['raw_action'] == 'buy'
    assert entry['generic_action'] == 'buy'
    assert entry['quantity_policy'] == {
        'type': 'exec_agent_sized',
        'max_quantity': 1,
        'budget_usd': '100',
        'estimated_reference_price': '50.25',
        'pricing_note': (
            'Preview only: dispatch remains blocked until Exec enforces '
            'currency-aware USD sizing and account-risk ceilings.'
        ),
    }
    assert payload['strategy_run_id'] == 'us-run-1'
    assert payload['source'] == 'kai-trading-core'
    assert payload['requested_at'] == payload['metadata']['requested_at']
    assert loader_calls == [{'market_date': date(2026, 8, 19), 'limit': 80, 'run_id': 'us-run-1'}]


def test_rejects_wait_low_confidence_and_data_warning_before_exec_feedback() -> None:
    rows = [
        _row('WAIT', action='wait'),
        _row('LOW', confidence=6.0),
        _row('WARN', warnings=['stale_quote']),
    ]
    service = USExecutionPlanService(feedback_loader=lambda **kwargs: rows)

    result = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-1')

    assert result['status'] == 'empty'
    assert result['payload'] is None
    assert [item['reason'] for item in result['rejections']] == [
        'agent_action_not_buy',
        'confidence_below_threshold',
        'agent_data_quality_warning',
    ]


def test_rejects_structurally_mismatched_agent_feedback() -> None:
    row = _row('AAPL', risk_flags=['symbol_mismatch'])
    row['normalized_feedback_payload']['symbol'] = 'MSFT'
    service = USExecutionPlanService(feedback_loader=lambda **kwargs: [row])

    result = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-1')

    assert result['status'] == 'empty'
    assert result['payload'] is None
    assert result['rejections'] == [{'symbol': 'AAPL', 'reason': 'agent_structural_mismatch'}]


def test_execution_plan_identity_is_stable_per_run_and_distinct_across_reruns() -> None:
    service = USExecutionPlanService(feedback_loader=lambda **kwargs: [_row('AAPL')])

    first = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-1')
    retry = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-1')
    rerun = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-2')

    assert first['preview_payload']['plan_id'] == retry['preview_payload']['plan_id']
    assert first['preview_payload']['plan_id'] != rerun['preview_payload']['plan_id']
    assert len(first['preview_payload']['plan_id']) <= 128


def test_dispatch_defaults_to_preview_without_calling_exec() -> None:
    class FailingClient:
        async def post_execution_plan(self, payload):
            raise AssertionError('dry-run must not call exec')

    service = USExecutionPlanService(
        feedback_loader=lambda **kwargs: [_row('AAPL')],
        client_factory=FailingClient,
    )

    result = asyncio.run(
        service.dispatch(
            source_market_date=date(2026, 8, 19), dry_run=True, agent_run_id='us-run-1'
        )
    )

    assert result['dry_run'] is True
    assert result['status'] == 'preview_only'
    assert result['execution_supported'] is False


def test_non_preview_dispatch_fails_before_calling_exec_without_usd_sizing_capability() -> None:
    class FailingClient:
        def __init__(self):
            raise AssertionError('unsupported US dispatch must not construct an Exec client')

    service = USExecutionPlanService(
        feedback_loader=lambda **kwargs: [_row('AAPL')],
        client_factory=FailingClient,
    )

    with patch.object(settings, 'us_equity_execution_enabled', True):
        try:
            asyncio.run(
                service.dispatch(
                    source_market_date=date(2026, 8, 19),
                    dry_run=False,
                    agent_run_id='us-run-1',
                )
            )
        except ValueError as exc:
            assert str(exc) == 'exec_currency_aware_sizing_capability_unavailable'
        else:
            raise AssertionError('unsupported US sizing must fail closed before dispatch')


def test_future_supported_dispatch_must_validate_exact_exec_ack() -> None:
    class InvalidAckClient:
        async def post_execution_plan(self, payload):
            return {'status': 'accepted'}

    service = USExecutionPlanService(
        feedback_loader=lambda **kwargs: [],
        client_factory=InvalidAckClient,
    )
    service.build = lambda **kwargs: {
        'status': 'ready',
        'execution_supported': True,
        'execution_block_reason': None,
            'payload': {
                'plan_id': 'future-us-plan',
                'items': [{'core_item_key': 'future-us-entry'}],
                'metadata': {
                    'dispatch_material_fingerprint': 'a' * 64,
                },
            },
        }

    with patch.object(settings, 'us_equity_execution_enabled', True):
        try:
            asyncio.run(
                service.dispatch(
                    source_market_date=date(2026, 8, 19),
                    dry_run=False,
                    agent_run_id='us-run-1',
                )
            )
        except ValueError as exc:
            assert 'validation error' in str(exc).lower()
        else:
            raise AssertionError('an unbound Exec response must not count as dispatched')


def test_empty_dispatch_does_not_call_exec_or_emit_an_invalid_v4_plan() -> None:
    class FailingClient:
        async def post_execution_plan(self, payload):
            raise AssertionError('an empty candidate set must not call Exec')

    service = USExecutionPlanService(
        feedback_loader=lambda **kwargs: [_row('WAIT', action='wait')],
        client_factory=FailingClient,
    )

    result = asyncio.run(
        service.dispatch(
            source_market_date=date(2026, 8, 19),
            dry_run=False,
            agent_run_id='us-run-1',
        )
    )

    assert result['status'] == 'empty'
    assert result['payload'] is None
    assert result['dry_run'] is False


def test_non_preview_dispatch_requires_current_agent_run_id() -> None:
    service = USExecutionPlanService(feedback_loader=lambda **kwargs: [_row('AAPL')])

    try:
        asyncio.run(service.dispatch(source_market_date=date(2026, 8, 19), dry_run=False))
    except ValueError as exc:
        assert str(exc) == 'agent_run_id is required for US equity execution planning'
    else:
        raise AssertionError('non-preview dispatch must reject an unscoped agent run')


def test_default_target_trade_date_skips_us_exchange_holiday() -> None:
    service = USExecutionPlanService(feedback_loader=lambda **kwargs: [_row('AAPL')])

    result = service.build(source_market_date=date(2026, 7, 2), agent_run_id='us-run-1')

    assert result['target_trade_date'] == '2026-07-06'
