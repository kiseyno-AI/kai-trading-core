import pytest

from app.agent_context.decision_delta import (
    build_decision_delta,
    build_previous_decision_context,
)


@pytest.mark.parametrize('previous_wait_reason', [None, {}])
def test_repeated_wait_handles_prior_feedback_without_wait_reason(previous_wait_reason):
    previous = build_previous_decision_context(
        role='signal-decider',
        latest_feedback={
            'market_date': '2026-05-07',
            'symbol': '005930',
            'feedback_payload': {
                'next_day_open_action': 'wait',
                'wait_reason': previous_wait_reason,
            },
        },
    )
    assert previous['wait_reason'] is None

    result = build_decision_delta(
        role='signal-decider',
        payload={
            'next_day_open_action': 'wait',
            'wait_reason': {
                'code': 'weak_trend_relative_strength',
                'summary': 'Trend confirmation is required.',
            },
        },
        market_date='2026-05-08',
        symbol='005930',
        previous_decision_context=previous,
    )

    assert result['has_previous_decision'] is True
    assert result['repeat_wait'] is True
    assert result['previous_wait_reason'] is None
    assert result['persistence_trend'] == 'changed'


def test_repeated_wait_handles_missing_reasons_on_both_decisions():
    previous = build_previous_decision_context(
        role='signal-decider',
        latest_feedback={
            'feedback_payload': {'next_day_open_action': 'wait'},
        },
    )
    result = build_decision_delta(
        role='signal-decider',
        payload={'next_day_open_action': 'wait'},
        market_date='2026-05-08',
        symbol='005930',
        previous_decision_context=previous,
    )

    assert result['previous_wait_reason'] is None
    assert result['current_wait_reason'] is None
    assert result['persistence_trend'] == 'persistent'
