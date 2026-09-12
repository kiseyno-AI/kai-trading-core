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
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )


def test_next_open_buy_blocks_reward_risk_below_configured_floor() -> None:
    result = _evaluate(target_price=51_000, stop_loss_price=49_000)

    assert result['status'] == 'absent'
    assert result['feature_values']['decision.reward_risk_ratio'] == '1.0000'
    assert {reason['code'] for reason in result['blocking_reasons']} == {
        'slippage_adjusted_reward_risk_ratio_below_threshold'
    }


def test_next_open_buy_accepts_reward_risk_at_configured_floor() -> None:
    result = _evaluate(target_price=51_300, stop_loss_price=49_000)

    assert result['feature_values']['decision.reward_risk_ratio'] == '1.3000'
    assert result['blocking_reasons'] == []


def test_next_open_buy_gates_slippage_adjusted_reward_risk_like_exec() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=51_500,
        stop_loss_price=48_900,
        expected_slippage_bps=50,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=2,
        base_min_reward_risk_ratio='1.30',
    )

    economics = result['expected_return_economics']
    assert economics['derived_gross_return_pct'] == '3'
    assert economics['derived_slippage_adjusted_return_pct'] == '2.5'
    assert economics['estimated_slippage_cost_amount'] == '250'
    assert economics['gross_reward_risk_ratio'] == '1.3636'
    assert economics['slippage_adjusted_reward_risk_ratio'] == '0.9259'
    assert result['feature_values']['decision.slippage_adjusted_reward_risk_ratio'] == '0.9259'
    assert 'slippage_adjusted_reward_risk_ratio_below_threshold' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_core_direct_buy_gates_on_server_owned_total_round_trip_cost() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=51_500,
        stop_loss_price=48_900,
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        server_non_spread_round_trip_cost_bps=50,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=2,
        base_min_reward_risk_ratio='1.30',
    )

    economics = result['expected_return_economics']
    assert economics['basis'] == 'target_vs_reference_less_total_transaction_costs'
    assert economics['derived_slippage_adjusted_return_pct'] == '3'
    assert economics['slippage_adjusted_reward_risk_ratio'] == '1.3636'
    assert economics['server_non_spread_round_trip_cost_bps'] == '50'
    assert economics['estimated_total_transaction_cost_bps'] == '50'
    assert economics['estimated_total_transaction_cost_amount'] == '250'
    assert economics['derived_total_cost_adjusted_return_pct'] == '2.5'
    assert economics['total_cost_adjusted_reward_risk_ratio'] == '0.9259'
    assert 'total_cost_adjusted_reward_risk_ratio_below_threshold' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_exec_dispatch_prescreen_labels_only_proven_slippage_and_defers_full_cost() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=51_500,
        stop_loss_price=48_900,
        expected_slippage_bps=50,
        expected_slippage_basis='quote_spread_bps',
        server_non_spread_round_trip_cost_bps=None,
        defer_non_spread_round_trip_cost_to_exec=True,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=2,
        base_min_reward_risk_ratio='1.30',
    )

    economics = result['expected_return_economics']
    assert economics['basis'] == 'target_vs_reference_less_entry_slippage'
    assert economics['server_non_spread_round_trip_cost_bps'] is None
    assert (
        economics['server_non_spread_round_trip_cost_basis']
        == 'deferred_to_exec_final_full_cost_check'
    )
    assert economics['derived_slippage_adjusted_return_pct'] == '2.5'
    assert economics['slippage_adjusted_reward_risk_ratio'] == '0.9259'
    assert economics['estimated_total_transaction_cost_bps'] is None
    assert economics['derived_total_cost_adjusted_return_pct'] is None
    assert economics['total_cost_adjusted_reward_risk_ratio'] is None
    assert 'slippage_adjusted_reward_risk_ratio_below_threshold' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_zero_slippage_keeps_adjusted_and_gross_economics_equal() -> None:
    result = _evaluate(target_price=51_300, stop_loss_price=49_000)

    economics = result['expected_return_economics']
    assert economics['expected_slippage_bps'] == '0'
    assert economics['estimated_slippage_cost_amount'] == '0'
    assert economics['derived_slippage_adjusted_return_pct'] == economics['derived_gross_return_pct']
    assert economics['slippage_adjusted_reward_risk_ratio'] == economics['gross_reward_risk_ratio']


def test_dispatch_prescreen_allows_only_explicit_pending_live_cost_confirmation() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=5,
        reference_price=50_000,
        target_price=52_000,
        stop_loss_price=49_000,
        expected_slippage_bps=None,
        expected_slippage_basis='unavailable',
        allow_pending_live_cost_confirmation=True,
        server_non_spread_round_trip_cost_bps=None,
        defer_non_spread_round_trip_cost_to_exec=True,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=2,
        base_min_reward_risk_ratio='1.30',
    )

    economics = result['expected_return_economics']
    assert economics['status'] == 'pending_live_cost_confirmation'
    assert economics['basis'] == 'target_vs_reference_gross_pending_live_cost_confirmation'
    assert economics['effective_expected_return_pct'] == '4'
    assert economics['derived_slippage_adjusted_return_pct'] is None
    assert economics['slippage_adjusted_reward_risk_ratio'] is None
    assert result['blocking_reasons'] == []
    assert {warning['code'] for warning in result['warnings']} == {
        'reported_expected_return_capped_to_derived_gross',
        'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
    }


def test_pending_live_cost_confirmation_does_not_relax_malformed_or_present_cost() -> None:
    for slippage_bps, slippage_basis in (
        (None, 'quote_spread_bps'),
        (10, 'unavailable'),
        (float('nan'), 'unavailable'),
        (-1, 'unavailable'),
    ):
        result = evaluate_next_open_performance_gate(
            request_payload={},
            decision_action='buy',
            confidence_score_10=8,
            expected_return_pct=5,
            reference_price=50_000,
            target_price=52_000,
            stop_loss_price=49_000,
            expected_slippage_bps=slippage_bps,
            expected_slippage_basis=slippage_basis,
            allow_pending_live_cost_confirmation=True,
            server_non_spread_round_trip_cost_bps=None,
            defer_non_spread_round_trip_cost_to_exec=True,
            base_min_confidence_score_10=6,
            base_min_expected_return_pct=2,
        )

        assert result['expected_return_economics']['status'] == 'unproven'
        assert 'transaction_cost_evidence_missing' in {
            reason['code'] for reason in result['blocking_reasons']
        }


def test_pending_live_cost_confirmation_handles_nonpositive_gross_edge_without_none_comparison() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=50_000,
        stop_loss_price=49_000,
        expected_slippage_bps=None,
        expected_slippage_basis='unavailable',
        allow_pending_live_cost_confirmation=True,
        server_non_spread_round_trip_cost_bps=None,
        defer_non_spread_round_trip_cost_to_exec=True,
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
    )

    assert result['expected_return_economics']['status'] == 'pending_live_cost_confirmation'
    assert 'expected_return_net_edge_not_positive' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_fails_closed_when_stop_geometry_is_missing_or_invalid() -> None:
    for stop_loss_price in (None, 50_000, 51_000, float('nan')):
        result = evaluate_next_open_performance_gate(
            request_payload={},
            decision_action='buy',
            confidence_score_10=8,
            expected_return_pct=3,
            reference_price=50_000,
            target_price=52_000,
            stop_loss_price=stop_loss_price,
            expected_slippage_bps=0,
            expected_slippage_basis='quote_spread_bps',
            base_min_confidence_score_10=6,
            base_min_expected_return_pct=0,
        )

        assert 'reward_risk_structure_invalid' in {
            reason['code'] for reason in result['blocking_reasons']
        }


def test_next_open_buy_uses_unrounded_reward_risk_for_threshold_check() -> None:
    result = _evaluate(target_price=Decimal('51299.95'), stop_loss_price=49_000)

    assert result['feature_values']['decision.reward_risk_ratio'] == '1.3000'
    assert result['blocking_reasons'][0]['code'] == 'slippage_adjusted_reward_risk_ratio_below_threshold'


def test_next_open_buy_caps_inflated_reported_return_to_target_reference_upside() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=10,
        reference_price=50_000,
        target_price=51_500,
        stop_loss_price=49_000,
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=4,
        base_min_reward_risk_ratio='1.30',
    )

    economics = result['expected_return_economics']
    assert economics['reported_expected_return_pct'] == '10'
    assert economics['derived_gross_return_pct'] == '3'
    assert economics['effective_expected_return_pct'] == '3'
    assert {warning['code'] for warning in result['warnings']} == {
        'reported_expected_return_capped_to_derived_slippage_adjusted'
    }
    assert 'performance_adjusted_expected_return_below_threshold' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_fails_closed_when_reference_economics_are_missing() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=5,
        reference_price=None,
        target_price=51_500,
        stop_loss_price=49_000,
        expected_slippage_bps=10,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
    )

    assert result['expected_return_economics']['status'] == 'unproven'
    assert result['expected_return_economics']['invalid_fields'] == ['reference_price']
    assert 'expected_return_economics_unproven' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_rejects_missing_negative_and_nonfinite_reported_return() -> None:
    for reported_return in (None, -0.1, float('nan'), float('inf')):
        result = evaluate_next_open_performance_gate(
            request_payload={},
            decision_action='buy',
            confidence_score_10=8,
            expected_return_pct=reported_return,
            reference_price=50_000,
            target_price=52_000,
            stop_loss_price=49_000,
            expected_slippage_bps=10,
            expected_slippage_basis='quote_spread_bps',
            base_min_confidence_score_10=6,
            base_min_expected_return_pct=0,
        )

        assert result['expected_return_economics']['status'] == 'unproven'
        assert 'expected_return_pct' in result['expected_return_economics']['invalid_fields']
        assert 'expected_return_economics_unproven' in {
            reason['code'] for reason in result['blocking_reasons']
        }


def test_next_open_buy_rejects_target_that_has_no_upside_from_entry_reference() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=5,
        reference_price=50_000,
        target_price=49_900,
        stop_loss_price=49_000,
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
    )

    assert result['expected_return_economics']['derived_slippage_adjusted_return_pct'] == '-0.2'
    assert 'expected_return_net_edge_not_positive' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_rejects_edge_erased_by_available_slippage_evidence() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=0.4,
        reference_price=100,
        target_price=100.4,
        stop_loss_price=98,
        expected_slippage_bps=50,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
    )

    economics = result['expected_return_economics']
    assert economics['derived_gross_return_pct'] == '0.4'
    assert economics['expected_slippage_bps'] == '50'
    assert Decimal(economics['derived_slippage_adjusted_return_pct']) < 0
    assert 'expected_return_edge_erased_by_slippage' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_fails_closed_when_transaction_cost_evidence_is_unavailable() -> None:
    for slippage_bps, slippage_basis in (
        (None, 'unavailable'),
        (None, 'quote_spread_bps'),
        (0, None),
    ):
        result = evaluate_next_open_performance_gate(
            request_payload={},
            decision_action='buy',
            confidence_score_10=8,
            expected_return_pct=3,
            reference_price=50_000,
            target_price=52_000,
            stop_loss_price=49_000,
            expected_slippage_bps=slippage_bps,
            expected_slippage_basis=slippage_basis,
            base_min_confidence_score_10=6,
            base_min_expected_return_pct=0,
        )

        assert result['expected_return_economics']['status'] == 'unproven'
        assert 'transaction_cost_evidence_missing' in {
            reason['code'] for reason in result['blocking_reasons']
        }


def test_next_open_buy_rejects_unrecognized_or_noncanonical_slippage_basis() -> None:
    for slippage_basis in (
        'liquidity_tier_estimate',
        'model_estimate',
        'QUOTE_SPREAD_BPS',
        ' quote_spread_bps ',
        '   ',
    ):
        result = evaluate_next_open_performance_gate(
            request_payload={},
            decision_action='buy',
            confidence_score_10=8,
            expected_return_pct=3,
            reference_price=50_000,
            target_price=52_000,
            stop_loss_price=49_000,
            expected_slippage_bps=10,
            expected_slippage_basis=slippage_basis,
            base_min_confidence_score_10=6,
            base_min_expected_return_pct=0,
        )

        economics = result['expected_return_economics']
        assert economics['status'] == 'unproven'
        assert economics['expected_slippage_basis'] == 'unavailable'
        assert 'expected_slippage_basis' in economics['invalid_fields']
        assert 'transaction_cost_evidence_missing' in {
            reason['code'] for reason in result['blocking_reasons']
        }


def test_next_open_buy_rejects_extreme_finite_decimal_without_raising() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=100,
        reference_price=Decimal('1E+999'),
        target_price=Decimal('2E+999'),
        stop_loss_price=Decimal('5E+998'),
        expected_slippage_bps=10,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
    )

    assert result['expected_return_economics']['status'] == 'unproven'
    assert 'expected_return_numeric_range_invalid' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_buy_rejects_unbalanced_extreme_decimal_without_raising() -> None:
    result = evaluate_next_open_performance_gate(
        request_payload={},
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=100,
        reference_price=Decimal('1'),
        target_price=Decimal('1E+999999'),
        stop_loss_price=Decimal('0.5'),
        expected_slippage_bps=10,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
    )

    assert result['expected_return_economics']['status'] == 'unproven'
    assert 'expected_return_numeric_range_invalid' in {
        reason['code'] for reason in result['blocking_reasons']
    }


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
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
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
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )

    assert result['status'] == 'ready'
    assert result['matched_rule_candidates'] == []
    assert 'performance_rule_candidate_blocked' not in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_rejects_noncanonical_active_rule_candidate_contracts() -> None:
    canonical_condition = {
        'field': 'decision.action',
        'operator': 'eq',
        'value': 'buy',
    }
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
                            'candidate_id': 'legacy-veto',
                            'candidate_type': 'veto',
                            'activation_status': 'active',
                            'manual_review_only': False,
                            'active_policy': True,
                            'deterministic_gate_eligible': True,
                            'predicate': {
                                'schema_version': 'signal_decider_rule_predicate_v1',
                                'all': [canonical_condition],
                            },
                        },
                        {
                            'candidate_id': 'predicate-with-extra-root-key',
                            'candidate_type': 'buy_veto',
                            'activation_status': 'active',
                            'manual_review_only': False,
                            'active_policy': True,
                            'deterministic_gate_eligible': True,
                            'predicate': {
                                'schema_version': 'signal_decider_rule_predicate_v1',
                                'all': [canonical_condition],
                                'conditions': [canonical_condition],
                            },
                        },
                    ],
                    'usage_policy': {'deterministic_gate_eligible': True},
                }
            }
        },
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=52_000,
        stop_loss_price=49_000,
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )

    assert result['matched_rule_candidates'] == []
    assert 'performance_rule_candidate_blocked' not in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_matches_approved_rule_against_server_owned_factor_inputs() -> None:
    request_payload = {
        'symbol_day_context': {
            'action_summary': {'signal_score': 82},
            'price_context': {'close_location_ratio': 0.86},
        },
        'scores': {
            'unified_value': {'status': 'available', 'score': 0.72},
            'momentum': {'status': 'available', 'score': 0.88},
            'quality': {'status': 'available', 'score': 0.20},
        },
        'factor_summary': {
            'factor_scores': {
                'unified_value': {'status': 'available', 'score': 0.72},
                'momentum': {'status': 'available', 'score': 0.88},
                'quality': {'status': 'available', 'score': 0.20},
            },
            'signal_features': {
                'status': 'available',
                'row_kind': 'direct_plus_fallback',
            },
        },
        'liquidity_context': {
            'status': 'available',
            'ranking': {
                'status': 'available',
                'liquidity_percentile': 0.84,
                'passes_liquidity_floor': True,
            },
        },
        'signal_decider_policy': {
            'performance_criteria': {
                'schema_version': 'signal_decider_performance_criteria_v1',
                'status': 'ready',
                'sample_count': 12,
                'performance_buckets': [],
                'rule_candidates': [
                    {
                        'candidate_id': 'approved-factor-veto',
                        'candidate_type': 'buy_veto',
                        'activation_status': 'active',
                        'manual_review_only': False,
                        'active_policy': True,
                        'deterministic_gate_eligible': True,
                        'predicate': {
                            'schema_version': 'signal_decider_rule_predicate_v1',
                            'all': [
                                {'field': 'decision.action', 'operator': 'eq', 'value': 'buy'},
                                {
                                    'field': 'feature.action_summary.signal_score_bucket',
                                    'operator': 'eq',
                                    'value': 'high',
                                },
                                {'field': 'liquidity.bucket', 'operator': 'eq', 'value': 'high_liquidity'},
                                {
                                    'field': 'price_context.close_location_bucket',
                                    'operator': 'eq',
                                    'value': 'extended_high',
                                },
                                {
                                    'field': 'factor.unified_value.bucket',
                                    'operator': 'eq',
                                    'value': 'available:medium',
                                },
                                {
                                    'field': 'factor.momentum.bucket',
                                    'operator': 'eq',
                                    'value': 'available:high',
                                },
                                {
                                    'field': 'factor.quality.bucket',
                                    'operator': 'eq',
                                    'value': 'available:low',
                                },
                                {
                                    'field': 'factor.signal_features.bucket',
                                    'operator': 'eq',
                                    'value': 'direct_plus_fallback',
                                },
                            ],
                        },
                    }
                ],
                'usage_policy': {
                    'deterministic_gate_eligible': True,
                    'manual_review_only': True,
                    'allow_buy_authorization': False,
                },
            }
        },
    }

    result = evaluate_next_open_performance_gate(
        request_payload=request_payload,
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=52_000,
        stop_loss_price=49_000,
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )

    assert result['feature_values']['feature.action_summary.signal_score_bucket'] == 'high'
    assert result['feature_values']['liquidity.bucket'] == 'high_liquidity'
    assert result['feature_values']['price_context.close_location_bucket'] == 'extended_high'
    assert result['feature_values']['factor.unified_value.bucket'] == 'available:medium'
    assert result['feature_values']['factor.momentum.bucket'] == 'available:high'
    assert result['feature_values']['factor.quality.bucket'] == 'available:low'
    assert result['feature_values']['factor.signal_features.bucket'] == 'direct_plus_fallback'
    assert [item['candidate_id'] for item in result['matched_rule_candidates']] == [
        'approved-factor-veto'
    ]
    assert 'performance_rule_candidate_blocked' in {
        reason['code'] for reason in result['blocking_reasons']
    }


def test_next_open_does_not_use_model_only_factor_values_for_rule_matching() -> None:
    request_payload = {
        'scores': {'momentum': {'status': 'available', 'score': 0.20}},
        'factor_summary': {
            'factor_scores': {'momentum': {'status': 'available', 'score': 0.20}}
        },
        'signal_decider_trace': {
            'feature_snapshot': {
                'scores': {'momentum': {'status': 'available', 'score': 0.99}}
            }
        },
        'model_output': {
            'factor_summary': {
                'factor_scores': {'momentum': {'status': 'available', 'score': 0.99}}
            }
        },
        'signal_decider_policy': {
            'performance_criteria': {
                'schema_version': 'signal_decider_performance_criteria_v1',
                'status': 'ready',
                'sample_count': 12,
                'performance_buckets': [],
                'rule_candidates': [
                    {
                        'candidate_id': 'approved-factor-veto',
                        'candidate_type': 'buy_veto',
                        'activation_status': 'active',
                        'manual_review_only': False,
                        'active_policy': True,
                        'deterministic_gate_eligible': True,
                        'predicate': {
                            'schema_version': 'signal_decider_rule_predicate_v1',
                            'all': [
                                {'field': 'decision.action', 'operator': 'eq', 'value': 'buy'},
                                {
                                    'field': 'factor.momentum.bucket',
                                    'operator': 'eq',
                                    'value': 'available:high',
                                },
                            ],
                        },
                    }
                ],
                'usage_policy': {'deterministic_gate_eligible': True},
            }
        },
    }

    result = evaluate_next_open_performance_gate(
        request_payload=request_payload,
        decision_action='buy',
        confidence_score_10=8,
        expected_return_pct=3,
        reference_price=50_000,
        target_price=52_000,
        stop_loss_price=49_000,
        expected_slippage_bps=0,
        expected_slippage_basis='quote_spread_bps',
        base_min_confidence_score_10=6,
        base_min_expected_return_pct=0,
        base_min_reward_risk_ratio='1.30',
    )

    assert result['feature_values']['factor.momentum.bucket'] == 'available:low'
    assert result['matched_rule_candidates'] == []
    assert 'performance_rule_candidate_blocked' not in {
        reason['code'] for reason in result['blocking_reasons']
    }
