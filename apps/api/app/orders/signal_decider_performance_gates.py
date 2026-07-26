from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


BAD_BUY_RATE_BLOCK_THRESHOLD = Decimal('0.70')
NEGATIVE_RATE_BLOCK_THRESHOLD = Decimal('0.70')
NEGATIVE_RATE_TIGHTEN_THRESHOLD = Decimal('0.60')
MIN_PERFORMANCE_SAMPLE_COUNT = 3
CONFIDENCE_TIGHTEN_DELTA = Decimal('1.0')
EXPECTED_RETURN_TIGHTEN_DELTA = Decimal('1.0')
MIN_REWARD_RISK_WHEN_TIGHTENED = Decimal('1.30')
PROTECTIVE_TARGET_UPSIDE_MULTIPLIER = Decimal('0.80')
PROTECTIVE_STOP_DOWNSIDE_MULTIPLIER = Decimal('0.70')


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_int_price(value: Decimal | None) -> int | None:
    if value is None:
        return None
    return int(value.quantize(Decimal('1'), rounding=ROUND_HALF_UP))


def _maybe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _maybe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _rate_is_blocking(value: Any, *, threshold: Decimal) -> bool:
    parsed = _to_decimal(value)
    return parsed is not None and parsed >= threshold


def _confidence_bucket(score_10: Decimal | None) -> str | None:
    if score_10 is None:
        return None
    if score_10 >= Decimal('8.0'):
        return 'high'
    if score_10 >= Decimal('5.0'):
        return 'medium'
    return 'low'


def _reward_risk_bucket(
    *,
    reference_price: Decimal | None,
    target_price: Decimal | None,
    stop_loss_price: Decimal | None,
) -> str | None:
    if reference_price is None or target_price is None or stop_loss_price is None:
        return None
    upside = target_price - reference_price
    downside = reference_price - stop_loss_price
    if upside <= 0 or downside <= 0:
        return 'invalid_reward_risk'
    ratio = upside / downside
    if ratio < Decimal('1.0'):
        return 'reward_risk_lt_1_0'
    if ratio < Decimal('1.3'):
        return 'reward_risk_lt_1_3'
    if ratio < Decimal('2.0'):
        return 'reward_risk_1_3_to_2_0'
    return 'reward_risk_gte_2_0'


def _criteria_from_request_payload(request_payload: dict[str, Any]) -> dict[str, Any]:
    policy = _maybe_dict(request_payload.get('signal_decider_policy'))
    criteria = _maybe_dict(policy.get('performance_criteria'))
    return criteria if criteria.get('schema_version') == 'signal_decider_performance_criteria_v1' else {}


def _decision_feature_values(
    *,
    decision_action: str,
    confidence_score_10: Any,
    reference_price: Any,
    target_price: Any,
    stop_loss_price: Any,
) -> dict[str, str]:
    confidence = _to_decimal(confidence_score_10)
    reference = _to_decimal(reference_price)
    target = _to_decimal(target_price)
    stop = _to_decimal(stop_loss_price)
    values = {
        'decision.action': decision_action,
    }
    confidence_bucket = _confidence_bucket(confidence)
    if confidence_bucket:
        values['decision.confidence_bucket'] = confidence_bucket
    reward_risk = _reward_risk_bucket(
        reference_price=reference,
        target_price=target,
        stop_loss_price=stop,
    )
    if reward_risk:
        values['decision.reward_risk_bucket'] = reward_risk
    return values


def _predicate_matches(predicate: dict[str, Any], *, feature_values: dict[str, str]) -> bool:
    if not predicate:
        return False
    all_predicates = _maybe_list(predicate.get('all'))
    if all_predicates:
        return all(
            _predicate_matches(item, feature_values=feature_values)
            for item in all_predicates
            if isinstance(item, dict)
        )
    any_predicates = _maybe_list(predicate.get('any'))
    if any_predicates:
        return any(
            _predicate_matches(item, feature_values=feature_values)
            for item in any_predicates
            if isinstance(item, dict)
        )
    field = str(predicate.get('field') or predicate.get('key') or '').strip()
    if not field:
        return False
    actual = feature_values.get(field)
    operator = str(predicate.get('operator') or predicate.get('op') or 'eq').strip().lower()
    expected = predicate.get('value')
    if operator in {'eq', '=='}:
        return actual == str(expected)
    if operator == 'in':
        return actual in {str(item) for item in _maybe_list(expected)}
    if operator in {'ne', '!='}:
        return actual is not None and actual != str(expected)
    return False


def evaluate_next_open_performance_gate(
    *,
    request_payload: dict[str, Any],
    decision_action: str,
    confidence_score_10: Any,
    expected_return_pct: Any,
    reference_price: Any,
    target_price: Any,
    stop_loss_price: Any,
    base_min_confidence_score_10: Any,
    base_min_expected_return_pct: Any,
) -> dict[str, Any]:
    criteria = _criteria_from_request_payload(request_payload)
    base_min_confidence = _to_decimal(base_min_confidence_score_10) or Decimal('0')
    base_min_expected_return = _to_decimal(base_min_expected_return_pct) or Decimal('0')
    adjusted_min_confidence = base_min_confidence
    adjusted_min_expected_return = base_min_expected_return
    if not criteria:
        return {
            'status': 'absent',
            'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
            'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
            'blocking_reasons': [],
            'warnings': [],
            'matched_negative_buckets': [],
            'matched_rule_candidates': [],
        }

    feature_values = _decision_feature_values(
        decision_action=decision_action,
        confidence_score_10=confidence_score_10,
        reference_price=reference_price,
        target_price=target_price,
        stop_loss_price=stop_loss_price,
    )
    overall = _maybe_dict(criteria.get('overall'))
    if (
        _to_decimal(overall.get('negative_outcome_rate')) is not None
        and _to_decimal(overall.get('positive_outcome_rate')) is not None
        and _to_decimal(overall.get('negative_outcome_rate')) > _to_decimal(overall.get('positive_outcome_rate')) + Decimal('0.10')
    ):
        adjusted_min_confidence += CONFIDENCE_TIGHTEN_DELTA
        adjusted_min_expected_return += EXPECTED_RETURN_TIGHTEN_DELTA

    matched_negative_buckets: list[dict[str, Any]] = []
    for bucket in _maybe_list(criteria.get('performance_buckets')):
        if not isinstance(bucket, dict):
            continue
        sample_count = int(bucket.get('sample_count') or 0)
        bucket_key = str(bucket.get('bucket_key') or '')
        bucket_value = str(bucket.get('bucket_value') or '')
        if sample_count < MIN_PERFORMANCE_SAMPLE_COUNT or feature_values.get(bucket_key) != bucket_value:
            continue
        negative_match = (
            _rate_is_blocking(bucket.get('bad_buy_rate'), threshold=BAD_BUY_RATE_BLOCK_THRESHOLD)
            or _rate_is_blocking(bucket.get('negative_rate'), threshold=NEGATIVE_RATE_BLOCK_THRESHOLD)
            or (
                _rate_is_blocking(bucket.get('negative_rate'), threshold=NEGATIVE_RATE_TIGHTEN_THRESHOLD)
                and (_to_decimal(bucket.get('avg_close_return_pct')) or Decimal('0')) < Decimal('0')
            )
        )
        if negative_match:
            matched_negative_buckets.append(bucket)

    matched_rule_candidates: list[dict[str, Any]] = []
    for candidate in _maybe_list(criteria.get('rule_candidates')):
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get('activation_status') or '') not in {'needs_replay', 'generated', 'candidate', 'active'}:
            continue
        if _predicate_matches(_maybe_dict(candidate.get('predicate')), feature_values=feature_values):
            matched_rule_candidates.append(candidate)

    blocking_reasons: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if matched_negative_buckets or matched_rule_candidates:
        adjusted_min_confidence += CONFIDENCE_TIGHTEN_DELTA
        adjusted_min_expected_return += EXPECTED_RETURN_TIGHTEN_DELTA
        reward_risk = feature_values.get('decision.reward_risk_bucket')
        if reward_risk in {'invalid_reward_risk', 'reward_risk_lt_1_0', 'reward_risk_lt_1_3'}:
            blocking_reasons.append(
                {
                    'code': 'performance_reward_risk_gate_blocked',
                    'message': 'Recent replay performance marks this reward/risk bucket as too weak for next-open buy eligibility.',
                    'reward_risk_bucket': reward_risk,
                }
            )
        if any(str(item.get('candidate_type') or '') in {'buy_veto', 'tighten_buy_gate'} for item in matched_rule_candidates):
            blocking_reasons.append(
                {
                    'code': 'performance_rule_candidate_blocked',
                    'message': 'Recent calibration generated a matching buy-veto/tighten rule candidate.',
                }
            )
        if not blocking_reasons:
            warnings.append(
                {
                    'code': 'performance_thresholds_tightened',
                    'message': 'Recent replay performance matched weak buckets, so confidence and expected-return thresholds were tightened.',
                }
            )

    confidence = _to_decimal(confidence_score_10)
    expected_return = _to_decimal(expected_return_pct)
    if confidence is None or confidence < adjusted_min_confidence:
        blocking_reasons.append(
            {
                'code': 'performance_adjusted_confidence_below_threshold',
                'message': 'Confidence is below the performance-adjusted next-open buy threshold.',
                'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
            }
        )
    if expected_return is None or expected_return < adjusted_min_expected_return:
        blocking_reasons.append(
            {
                'code': 'performance_adjusted_expected_return_below_threshold',
                'message': 'Expected return is below the performance-adjusted next-open buy threshold.',
                'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
            }
        )

    return {
        'status': criteria.get('status') or 'ready',
        'sample_count': criteria.get('sample_count'),
        'feature_values': feature_values,
        'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
        'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
        'blocking_reasons': blocking_reasons,
        'warnings': warnings,
        'matched_negative_buckets': matched_negative_buckets[:4],
        'matched_rule_candidates': matched_rule_candidates[:4],
    }


def build_protective_exit_thresholds(
    *,
    request_payload: dict[str, Any],
    decision_action: str,
    confidence_score_10: Any,
    reference_price: Any,
    target_price: Any,
    stop_loss_price: Any,
) -> dict[str, Any]:
    criteria = _criteria_from_request_payload(request_payload)
    reference = _to_decimal(reference_price)
    target = _to_decimal(target_price)
    stop = _to_decimal(stop_loss_price)
    if not criteria or reference is None or target is None or stop is None or target <= reference or stop >= reference:
        return {
            'status': 'absent' if not criteria else 'not_applicable',
            'effective_target_price': _to_int_price(target),
            'effective_stop_loss_price': _to_int_price(stop),
            'matched_negative_buckets': [],
            'matched_rule_candidates': [],
            'protective_exit_applied': False,
        }
    gate = evaluate_next_open_performance_gate(
        request_payload=request_payload,
        decision_action=decision_action,
        confidence_score_10=confidence_score_10,
        expected_return_pct=None,
        reference_price=reference,
        target_price=target,
        stop_loss_price=stop,
        base_min_confidence_score_10=Decimal('0'),
        base_min_expected_return_pct=Decimal('-999'),
    )
    weak_match = bool(gate.get('matched_negative_buckets') or gate.get('matched_rule_candidates'))
    if not weak_match:
        return {
            'status': gate.get('status') or 'ready',
            'effective_target_price': _to_int_price(target),
            'effective_stop_loss_price': _to_int_price(stop),
            'matched_negative_buckets': [],
            'matched_rule_candidates': [],
            'protective_exit_applied': False,
        }
    effective_target = reference + ((target - reference) * PROTECTIVE_TARGET_UPSIDE_MULTIPLIER)
    effective_stop = reference - ((reference - stop) * PROTECTIVE_STOP_DOWNSIDE_MULTIPLIER)
    return {
        'status': gate.get('status') or 'ready',
        'effective_target_price': _to_int_price(effective_target),
        'effective_stop_loss_price': _to_int_price(effective_stop),
        'original_target_price': _to_int_price(target),
        'original_stop_loss_price': _to_int_price(stop),
        'reference_price': _to_int_price(reference),
        'matched_negative_buckets': gate.get('matched_negative_buckets') or [],
        'matched_rule_candidates': gate.get('matched_rule_candidates') or [],
        'protective_exit_applied': True,
    }
