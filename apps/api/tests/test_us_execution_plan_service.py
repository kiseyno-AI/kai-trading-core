import asyncio
from datetime import date

from app.orders.execution_handoff_contract import EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION
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

    assert result['status'] == 'ready'
    assert result['accepted_candidate_count'] == 1
    payload = result['payload']
    assert payload['metadata']['schema_version'] == EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION
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
    assert entry['quantity_policy'] == {'kind': 'shares', 'quantity': 1}
    assert payload['strategy_run_id'] == 'us-run-1'
    assert payload['metadata']['agent_run_id'] == 'us-run-1'
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
    assert result['payload']['items'] == []
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
    assert result['rejections'] == [{'symbol': 'AAPL', 'reason': 'agent_structural_mismatch'}]


def test_execution_plan_identity_is_stable_per_run_and_distinct_across_reruns() -> None:
    service = USExecutionPlanService(feedback_loader=lambda **kwargs: [_row('AAPL')])

    first = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-1')
    retry = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-1')
    rerun = service.build(source_market_date=date(2026, 8, 19), agent_run_id='us-run-2')

    assert first['payload']['plan_id'] == retry['payload']['plan_id']
    assert first['payload']['plan_id'] != rerun['payload']['plan_id']
    assert len(first['payload']['plan_id']) <= 128


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
    assert result['status'] == 'ready'


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
