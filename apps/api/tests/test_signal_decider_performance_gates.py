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


def test_next_open_ignores_performance_criteria_without_explicit_gate_eligibility() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={
            'signal_decider_policy': {
                'performance_criteria': {
                    'schema_version': 'signal_decider_performance_criteria_v1',
                    'status': 'ready',
                    'sample_count': 12,
                    'overall': {
                        'positive_outcome_rate': 0.2,
                        'negative_outcome_rate': 0.8,
                    },
                    'performance_buckets': [
                        {
                            'bucket_key': 'decision.reward_risk_bucket',
                            'bucket_value': 'reward_risk_1_3_to_2_0',
                            'sample_count': 8,
                            'bad_buy_rate': 0.8,
                            'negative_rate': 0.8,
                            'avg_close_return_pct': -1.7,
                        }
                    ],
                    'usage_policy': {
                        'manual_review_only': True,
                        'allow_buy_authorization': False,
                    },
                }
            }
        },
        decision_action='buy',
        confidence_score_10=6.5,
        expected_return_pct=1.0,
        reference_price=50_000,
        target_price=51_500,
        stop_loss_price=49_000,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )

    assert result['status'] == 'not_eligible'
    assert result['adjusted_min_confidence_score_10'] == '6'
    assert result['adjusted_min_expected_return_pct'] == '0'
    assert result['matched_negative_buckets'] == []
    assert result['matched_rule_candidates'] == []
    assert result['blocking_reasons'] == []


def test_next_open_does_not_activate_manual_review_rule_candidates() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={
            'signal_decider_policy': {
                'performance_criteria': {
                    'schema_version': 'signal_decider_performance_criteria_v1',
                    'status': 'ready',
                    'sample_count': 12,
                    'performance_buckets': [],
                    'rule_candidates': [
                        {
                            'candidate_id': 'candidate-buy-veto',
                            'candidate_type': 'buy_veto',
                            'activation_status': 'needs_replay',
                            'manual_review_only': True,
                            'active_policy': False,
                            'predicate': {
                                'field': 'decision.action',
                                'operator': 'eq',
                                'value': 'buy',
                            },
                        }
                    ],
                    'usage_policy': {
                        'deterministic_gate_eligible': True,
                        'manual_review_only': True,
                        'allow_buy_authorization': False,
                    },
                }
            }
        },
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=52_000,
        stop_loss_price=49_000,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )

    assert result['status'] == 'ready'
    assert result['matched_rule_candidates'] == []
    assert 'performance_rule_candidate_blocked' not in {
        reason['code'] for reason in result['blocking_reasons']
    }
