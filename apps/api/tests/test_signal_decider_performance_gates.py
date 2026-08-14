from decimal import Decimal

from app.orders.signal_decider_performance_gates import evaluate_next_open_performance_gate


def _evaluate(*, target_price: int | Decimal, stop_loss_price: int, action: str = 'buy') -> dict:
    return evaluate_next_open_performance_gate(
        request_payload={},
        decision_action=action,
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=target_price,
        stop_loss_price=stop_loss_price,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )


def test_next_open_buy_blocks_reward_risk_below_configured_floor() -> None:
    result = _evaluate(target_price=51_000, stop_loss_price=49_000)

    assert result['status'] == 'absent'
    assert result['feature_values']['decision.reward_risk_ratio'] == '1.0000'
    assert {reason['code'] for reason in result['blocking_reasons']} == {
        'reward_risk_ratio_below_threshold'
    }


def test_next_open_buy_accepts_reward_risk_at_configured_floor() -> None:
    result = _evaluate(target_price=51_300, stop_loss_price=49_000)

    assert result['feature_values']['decision.reward_risk_ratio'] == '1.3000'
    assert result['blocking_reasons'] == []


def test_next_open_buy_uses_unrounded_reward_risk_for_threshold_check() -> None:
    result = _evaluate(target_price=Decimal('51299.95'), stop_loss_price=49_000)

    assert result['feature_values']['decision.reward_risk_ratio'] == '1.3000'
    assert result['blocking_reasons'][0]['code'] == 'reward_risk_ratio_below_threshold'


def test_non_buy_decision_is_not_blocked_by_buy_reward_risk_floor() -> None:
    result = _evaluate(target_price=49_000, stop_loss_price=51_000, action='wait')

    assert result['feature_values']['decision.reward_risk_bucket'] == 'invalid_reward_risk'
    assert result['blocking_reasons'] == []
