from __future__ import annotations

from decimal import Decimal, DecimalException, InvalidOperation, ROUND_HALF_UP
from typing import Any


BAD_BUY_RATE_BLOCK_THRESHOLD = Decimal('0.70')
NEGATIVE_RATE_BLOCK_THRESHOLD = Decimal('0.70')
NEGATIVE_RATE_TIGHTEN_THRESHOLD = Decimal('0.60')
MIN_PERFORMANCE_SAMPLE_COUNT = 3
CONFIDENCE_TIGHTEN_DELTA = Decimal('1.0')
EXPECTED_RETURN_TIGHTEN_DELTA = Decimal('1.0')
DEFAULT_MIN_REWARD_RISK_RATIO = Decimal('1.30')
PROTECTIVE_TARGET_UPSIDE_MULTIPLIER = Decimal('0.80')
PROTECTIVE_STOP_DOWNSIDE_MULTIPLIER = Decimal('0.70')
PERCENT_PLACES = Decimal('0.000001')
RATIO_PLACES = Decimal('0.0001')
MAX_DECIMAL_TEXT_LENGTH = 64
ALLOWED_EXPECTED_SLIPPAGE_BASES = frozenset({'quote_spread_bps'})
RULE_CANDIDATE_PREDICATE_SCHEMA_VERSION = 'signal_decider_rule_predicate_v1'


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | None, *, places: Decimal | None = None) -> str | None:
    if value is None or not value.is_finite():
        return None
    try:
        normalized = value.quantize(places) if places is not None else value
    except DecimalException:
        return None
    if not normalized.is_finite():
        return None
    _, digits, exponent = normalized.as_tuple()
    if len(digits) + abs(exponent) + 2 > MAX_DECIMAL_TEXT_LENGTH:
        return None
    if normalized == 0:
        return '0'
    text = format(normalized, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text


def _unproven_expected_return_economics(
    *,
    evidence: dict[str, Any],
    invalid_fields: list[str],
    is_buy: bool,
    transaction_cost_evidence_missing: bool = False,
    numeric_range_invalid: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    blocking_reasons: list[dict[str, Any]] = []
    if is_buy:
        if transaction_cost_evidence_missing:
            blocking_reasons.append(
                {
                    'code': 'transaction_cost_evidence_missing',
                    'message': (
                        'Live buy economics require explicit finite non-negative slippage evidence '
                        'and its provenance.'
                    ),
                }
            )
        if numeric_range_invalid:
            blocking_reasons.append(
                {
                    'code': 'expected_return_numeric_range_invalid',
                    'message': 'Buy economics include a value outside the supported request numeric range.',
                    'invalid_fields': invalid_fields,
                }
            )
        blocking_reasons.append(
            {
                'code': 'expected_return_economics_unproven',
                'message': (
                    'Buy expected return requires positive reported return, reference and target prices, '
                    'and non-negative slippage evidence.'
                ),
                'invalid_fields': invalid_fields,
            }
        )
    return (
        {
            'status': 'unproven',
            **evidence,
            'estimated_slippage_cost_amount': None,
            'derived_gross_return_pct': None,
            'derived_slippage_adjusted_return_pct': None,
            'gross_reward_risk_ratio': None,
            'slippage_adjusted_reward_risk_ratio': None,
            'estimated_total_transaction_cost_bps': None,
            'estimated_total_transaction_cost_amount': None,
            'derived_total_cost_adjusted_return_pct': None,
            'total_cost_adjusted_reward_risk_ratio': None,
            'effective_expected_return_pct': None,
            'invalid_fields': invalid_fields,
        },
        blocking_reasons,
        [],
    )


def _expected_return_economics(
    *,
    decision_action: str,
    reported_expected_return_pct: Any,
    reference_price: Any,
    target_price: Any,
    stop_loss_price: Any,
    expected_slippage_bps: Any,
    expected_slippage_basis: Any,
    server_non_spread_round_trip_cost_bps: Any,
    defer_non_spread_round_trip_cost_to_exec: bool,
    allow_pending_live_cost_confirmation: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    reported = _to_decimal(reported_expected_return_pct)
    reference = _to_decimal(reference_price)
    target = _to_decimal(target_price)
    stop = _to_decimal(stop_loss_price)
    slippage_bps = _to_decimal(expected_slippage_bps)
    non_spread_round_trip_cost_bps = _to_decimal(server_non_spread_round_trip_cost_bps)
    slippage_basis = (
        expected_slippage_basis
        if isinstance(expected_slippage_basis, str)
        and expected_slippage_basis in ALLOWED_EXPECTED_SLIPPAGE_BASES
        else None
    )
    is_buy = decision_action.strip().lower() == 'buy'
    full_cost_deferred_to_exec = bool(defer_non_spread_round_trip_cost_to_exec)
    pending_live_cost_confirmation = (
        is_buy
        and allow_pending_live_cost_confirmation
        and full_cost_deferred_to_exec
        and expected_slippage_bps is None
        and expected_slippage_basis == 'unavailable'
        and server_non_spread_round_trip_cost_bps is None
    )

    reported_text = _decimal_text(reported)
    reference_text = _decimal_text(reference)
    target_text = _decimal_text(target)
    slippage_text = _decimal_text(slippage_bps)
    non_spread_round_trip_cost_text = _decimal_text(non_spread_round_trip_cost_bps)
    evidence = {
        'basis': (
            'target_vs_reference_gross_pending_live_cost_confirmation'
            if pending_live_cost_confirmation
            else 'target_vs_reference_less_total_transaction_costs'
            if not full_cost_deferred_to_exec
            and non_spread_round_trip_cost_bps is not None
            and non_spread_round_trip_cost_bps > 0
            else 'target_vs_reference_less_entry_slippage'
        ),
        'reported_expected_return_pct': reported_text,
        'reference_price': reference_text,
        'target_price': target_text,
        'expected_slippage_bps': slippage_text,
        'expected_slippage_basis': slippage_basis or 'unavailable',
        'server_non_spread_round_trip_cost_bps': (
            None if full_cost_deferred_to_exec else non_spread_round_trip_cost_text
        ),
        'server_non_spread_round_trip_cost_basis': (
            'deferred_to_exec_final_full_cost_check'
            if full_cost_deferred_to_exec
            else 'server_setting'
        ),
    }
    numeric_range_invalid_fields: list[str] = []
    for field_name, parsed, rendered in (
        ('expected_return_pct', reported, reported_text),
        ('reference_price', reference, reference_text),
        ('target_price', target, target_text),
        ('expected_slippage_bps', slippage_bps, slippage_text),
        (
            'server_non_spread_round_trip_cost_bps',
            non_spread_round_trip_cost_bps,
            non_spread_round_trip_cost_text,
        ),
    ):
        if parsed is not None and rendered is None:
            numeric_range_invalid_fields.append(field_name)

    invalid_fields: list[str] = []
    if reported is None or reported <= 0 or reported_text is None:
        invalid_fields.append('expected_return_pct')
    if reference is None or reference <= 0 or reference_text is None:
        invalid_fields.append('reference_price')
    if target is None or target <= 0 or target_text is None:
        invalid_fields.append('target_price')
    transaction_cost_evidence_missing = (
        slippage_bps is None
        or slippage_bps < 0
        or slippage_text is None
        or slippage_basis is None
    )
    if not pending_live_cost_confirmation and (
        slippage_bps is None or slippage_bps < 0 or slippage_text is None
    ):
        invalid_fields.append('expected_slippage_bps')
    if not pending_live_cost_confirmation and slippage_basis is None:
        invalid_fields.append('expected_slippage_basis')
    if not full_cost_deferred_to_exec and (
        non_spread_round_trip_cost_bps is None
        or non_spread_round_trip_cost_bps < 0
        or non_spread_round_trip_cost_text is None
    ):
        invalid_fields.append('server_non_spread_round_trip_cost_bps')
    if full_cost_deferred_to_exec and server_non_spread_round_trip_cost_bps is not None:
        invalid_fields.append('server_non_spread_round_trip_cost_bps')
    if invalid_fields:
        return _unproven_expected_return_economics(
            evidence=evidence,
            invalid_fields=invalid_fields,
            is_buy=is_buy,
            transaction_cost_evidence_missing=(
                transaction_cost_evidence_missing and not pending_live_cost_confirmation
            ),
            numeric_range_invalid=bool(numeric_range_invalid_fields),
        )

    try:
        derived_gross_return = ((target - reference) / reference) * Decimal('100')
        gross_reward_risk_ratio = _reward_risk_ratio(
            reference_price=reference,
            target_price=target,
            stop_loss_price=stop,
        )
        estimated_slippage_cost_amount = None
        derived_slippage_adjusted_return = None
        slippage_adjusted_reward_risk_ratio = None
        estimated_total_transaction_cost_bps = None
        estimated_total_transaction_cost_amount = None
        derived_total_cost_adjusted_return = None
        total_cost_adjusted_reward_risk_ratio = None
        if not pending_live_cost_confirmation:
            estimated_slippage_cost_amount = reference * (slippage_bps / Decimal('10000'))
            derived_slippage_adjusted_return = (
                (target - reference - estimated_slippage_cost_amount) / reference
            ) * Decimal('100')
            gross_reward = target - reference
            gross_risk = reference - stop if stop is not None else None
            if gross_reward > 0 and gross_risk is not None and gross_risk > 0:
                adjusted_reward = gross_reward - estimated_slippage_cost_amount
                adjusted_risk = gross_risk + estimated_slippage_cost_amount
                if adjusted_reward > 0 and adjusted_risk > 0:
                    slippage_adjusted_reward_risk_ratio = adjusted_reward / adjusted_risk
            if not full_cost_deferred_to_exec:
                estimated_total_transaction_cost_bps = (
                    slippage_bps + non_spread_round_trip_cost_bps
                )
                estimated_total_transaction_cost_amount = reference * (
                    estimated_total_transaction_cost_bps / Decimal('10000')
                )
                derived_total_cost_adjusted_return = (
                    (target - reference - estimated_total_transaction_cost_amount) / reference
                ) * Decimal('100')
                if gross_reward > 0 and gross_risk is not None and gross_risk > 0:
                    total_adjusted_reward = gross_reward - estimated_total_transaction_cost_amount
                    total_adjusted_risk = gross_risk + estimated_total_transaction_cost_amount
                    if total_adjusted_reward > 0 and total_adjusted_risk > 0:
                        total_cost_adjusted_reward_risk_ratio = (
                            total_adjusted_reward / total_adjusted_risk
                        )
    except DecimalException:
        return _unproven_expected_return_economics(
            evidence=evidence,
            invalid_fields=['expected_return_calculation'],
            is_buy=is_buy,
            numeric_range_invalid=True,
        )

    derived_return_for_gate = (
        derived_gross_return
        if pending_live_cost_confirmation
        else derived_slippage_adjusted_return
        if full_cost_deferred_to_exec
        else derived_total_cost_adjusted_return
    )
    effective_expected_return = min(reported, derived_return_for_gate)
    estimated_slippage_cost_text = _decimal_text(
        estimated_slippage_cost_amount,
        places=PERCENT_PLACES,
    )
    derived_gross_return_text = _decimal_text(derived_gross_return, places=PERCENT_PLACES)
    derived_slippage_adjusted_return_text = _decimal_text(
        derived_slippage_adjusted_return,
        places=PERCENT_PLACES,
    )
    gross_reward_risk_ratio_text = _decimal_text(
        gross_reward_risk_ratio,
        places=RATIO_PLACES,
    )
    slippage_adjusted_reward_risk_ratio_text = _decimal_text(
        slippage_adjusted_reward_risk_ratio,
        places=RATIO_PLACES,
    )
    estimated_total_transaction_cost_bps_text = _decimal_text(
        estimated_total_transaction_cost_bps,
        places=PERCENT_PLACES,
    )
    estimated_total_transaction_cost_amount_text = _decimal_text(
        estimated_total_transaction_cost_amount,
        places=PERCENT_PLACES,
    )
    derived_total_cost_adjusted_return_text = _decimal_text(
        derived_total_cost_adjusted_return,
        places=PERCENT_PLACES,
    )
    total_cost_adjusted_reward_risk_ratio_text = _decimal_text(
        total_cost_adjusted_reward_risk_ratio,
        places=RATIO_PLACES,
    )
    effective_expected_return_text = _decimal_text(
        effective_expected_return,
        places=PERCENT_PLACES,
    )
    range_invalid_outputs = [
        field_name
        for field_name, rendered in (
            ('derived_gross_return_pct', derived_gross_return_text),
            ('effective_expected_return_pct', effective_expected_return_text),
        )
        if rendered is None
    ]
    if not pending_live_cost_confirmation:
        range_invalid_outputs.extend(
            field_name
            for field_name, rendered in (
                ('estimated_slippage_cost_amount', estimated_slippage_cost_text),
                (
                    'derived_slippage_adjusted_return_pct',
                    derived_slippage_adjusted_return_text,
                ),
            )
            if rendered is None
        )
    if not pending_live_cost_confirmation and not full_cost_deferred_to_exec:
        range_invalid_outputs.extend(
            field_name
            for field_name, rendered in (
                (
                    'estimated_total_transaction_cost_bps',
                    estimated_total_transaction_cost_bps_text,
                ),
                (
                    'estimated_total_transaction_cost_amount',
                    estimated_total_transaction_cost_amount_text,
                ),
                (
                    'derived_total_cost_adjusted_return_pct',
                    derived_total_cost_adjusted_return_text,
                ),
            )
            if rendered is None
        )
    if gross_reward_risk_ratio is not None and gross_reward_risk_ratio_text is None:
        range_invalid_outputs.append('gross_reward_risk_ratio')
    if (
        slippage_adjusted_reward_risk_ratio is not None
        and slippage_adjusted_reward_risk_ratio_text is None
    ):
        range_invalid_outputs.append('slippage_adjusted_reward_risk_ratio')
    if (
        total_cost_adjusted_reward_risk_ratio is not None
        and total_cost_adjusted_reward_risk_ratio_text is None
    ):
        range_invalid_outputs.append('total_cost_adjusted_reward_risk_ratio')
    if range_invalid_outputs:
        return _unproven_expected_return_economics(
            evidence=evidence,
            invalid_fields=range_invalid_outputs,
            is_buy=is_buy,
            numeric_range_invalid=True,
        )

    blocking_reasons: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    edge_return = derived_return_for_gate
    applied_cost_bps = (
        None
        if pending_live_cost_confirmation
        else slippage_bps
        if full_cost_deferred_to_exec
        else estimated_total_transaction_cost_bps
    )
    if is_buy and edge_return <= 0:
        edge_erased_by_cost = bool(
            derived_gross_return > 0
            and applied_cost_bps is not None
            and applied_cost_bps > 0
        )
        edge_erased_by_non_spread_cost = bool(
            edge_erased_by_cost
            and not full_cost_deferred_to_exec
            and non_spread_round_trip_cost_bps > 0
        )
        blocking_reasons.append(
            {
                'code': (
                    'expected_return_edge_erased_by_transaction_costs'
                    if edge_erased_by_non_spread_cost
                    else 'expected_return_edge_erased_by_slippage'
                    if edge_erased_by_cost
                    else 'expected_return_net_edge_not_positive'
                ),
                'message': (
                    'Available transaction-cost evidence erases the target/reference upside.'
                    if edge_erased_by_cost
                    else 'Target upside does not remain positive from the entry reference.'
                ),
            }
        )
    if reported > edge_return:
        warnings.append(
            {
                'code': (
                    'reported_expected_return_capped_to_derived_gross'
                    if pending_live_cost_confirmation
                    else 'reported_expected_return_capped_to_derived_total_cost_adjusted'
                    if not full_cost_deferred_to_exec
                    and non_spread_round_trip_cost_bps > 0
                    else 'reported_expected_return_capped_to_derived_slippage_adjusted'
                ),
                'message': (
                    'Reported expected return exceeded target/reference prescreen upside '
                    'and was capped conservatively.'
                ),
            }
        )
    if pending_live_cost_confirmation:
        warnings.append(
            {
                'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
                'message': (
                    'Closed-market pre-screen used gross target/reference economics; exec must '
                    'confirm finite non-negative live transaction-cost evidence before any buy.'
                ),
            }
        )
    return (
        {
            'status': (
                'pending_live_cost_confirmation'
                if pending_live_cost_confirmation
                else 'ready'
                if edge_return > 0
                else 'no_positive_edge'
            ),
            **evidence,
            'estimated_slippage_cost_amount': estimated_slippage_cost_text,
            'derived_gross_return_pct': derived_gross_return_text,
            'derived_slippage_adjusted_return_pct': derived_slippage_adjusted_return_text,
            'gross_reward_risk_ratio': gross_reward_risk_ratio_text,
            'slippage_adjusted_reward_risk_ratio': slippage_adjusted_reward_risk_ratio_text,
            'estimated_total_transaction_cost_bps': estimated_total_transaction_cost_bps_text,
            'estimated_total_transaction_cost_amount': estimated_total_transaction_cost_amount_text,
            'derived_total_cost_adjusted_return_pct': derived_total_cost_adjusted_return_text,
            'total_cost_adjusted_reward_risk_ratio': total_cost_adjusted_reward_risk_ratio_text,
            'effective_expected_return_pct': effective_expected_return_text,
            '_effective_expected_return': effective_expected_return,
            '_gross_reward_risk_ratio': gross_reward_risk_ratio,
            '_slippage_adjusted_reward_risk_ratio': slippage_adjusted_reward_risk_ratio,
            '_total_cost_adjusted_reward_risk_ratio': total_cost_adjusted_reward_risk_ratio,
        },
        blocking_reasons,
        warnings,
    )


def _append_adjusted_threshold_blocks(
    *,
    decision_action: str,
    confidence_score_10: Any,
    effective_expected_return: Decimal | None,
    adjusted_min_confidence: Decimal,
    adjusted_min_expected_return: Decimal,
    blocking_reasons: list[dict[str, Any]],
) -> None:
    if decision_action.strip().lower() != 'buy':
        return
    confidence = _to_decimal(confidence_score_10)
    if confidence is None or confidence < adjusted_min_confidence:
        blocking_reasons.append(
            {
                'code': 'performance_adjusted_confidence_below_threshold',
                'message': 'Confidence is below the performance-adjusted next-open buy threshold.',
                'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
            }
        )
    if effective_expected_return is None or effective_expected_return < adjusted_min_expected_return:
        blocking_reasons.append(
            {
                'code': 'performance_adjusted_expected_return_below_threshold',
                'message': (
                    'Conservatively derived expected return is below the performance-adjusted '
                    'next-open buy threshold.'
                ),
                'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
            }
        )


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
    try:
        ratio = upside / downside
    except DecimalException:
        return 'invalid_reward_risk'
    if ratio < Decimal('1.0'):
        return 'reward_risk_lt_1_0'
    if ratio < Decimal('1.3'):
        return 'reward_risk_lt_1_3'
    if ratio < Decimal('2.0'):
        return 'reward_risk_1_3_to_2_0'
    return 'reward_risk_gte_2_0'


def _reward_risk_ratio(
    *,
    reference_price: Decimal | None,
    target_price: Decimal | None,
    stop_loss_price: Decimal | None,
) -> Decimal | None:
    if reference_price is None or target_price is None or stop_loss_price is None:
        return None
    upside = target_price - reference_price
    downside = reference_price - stop_loss_price
    if upside <= 0 or downside <= 0:
        return None
    try:
        return upside / downside
    except DecimalException:
        return None


def _criteria_from_request_payload(request_payload: dict[str, Any]) -> dict[str, Any]:
    policy = _maybe_dict(request_payload.get('signal_decider_policy'))
    criteria = _maybe_dict(policy.get('performance_criteria'))
    return criteria if criteria.get('schema_version') == 'signal_decider_performance_criteria_v1' else {}


def _normalized_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _factor_score_bucket(score: Decimal | None) -> str | None:
    if score is None:
        return None
    if score >= Decimal('0.80'):
        return 'high'
    if score >= Decimal('0.55'):
        return 'medium'
    return 'low'


def _factor_bucket_value(*, status: Any, score: Any) -> str | None:
    normalized_status = _normalized_text(status)
    score_bucket = _factor_score_bucket(_to_decimal(score))
    if normalized_status and score_bucket:
        return f'{normalized_status}:{score_bucket}'
    return score_bucket or normalized_status


def _server_owned_calibration_feature_values(request_payload: dict[str, Any]) -> dict[str, str]:
    values: dict[str, str] = {}
    symbol_day_context = _maybe_dict(request_payload.get('symbol_day_context'))
    action_summary = _maybe_dict(symbol_day_context.get('action_summary'))
    signal_score = _to_decimal(action_summary.get('signal_score'))
    if signal_score is not None:
        values['feature.action_summary.signal_score_bucket'] = (
            'high' if signal_score >= Decimal('70')
            else 'medium' if signal_score >= Decimal('40')
            else 'strong_negative' if signal_score <= Decimal('-70')
            else 'negative' if signal_score <= Decimal('-40')
            else 'low'
        )

    price_context = _maybe_dict(symbol_day_context.get('price_context'))
    close_location_ratio = _to_decimal(price_context.get('close_location_ratio'))
    if close_location_ratio is not None:
        values['price_context.close_location_bucket'] = (
            'extended_high' if close_location_ratio >= Decimal('0.75')
            else 'near_session_low' if close_location_ratio <= Decimal('0.25')
            else 'mid_range'
        )

    liquidity_context = _maybe_dict(request_payload.get('liquidity_context'))
    liquidity_ranking = _maybe_dict(liquidity_context.get('ranking'))
    liquidity_percentile = _to_decimal(liquidity_ranking.get('liquidity_percentile'))
    liquidity_status = _normalized_text(
        liquidity_context.get('status') or liquidity_ranking.get('status')
    )
    if liquidity_ranking.get('passes_liquidity_floor') is False:
        values['liquidity.bucket'] = 'fails_liquidity_floor'
    elif liquidity_percentile is not None:
        values['liquidity.bucket'] = (
            'high_liquidity' if liquidity_percentile >= Decimal('0.80')
            else 'medium_liquidity' if liquidity_percentile >= Decimal('0.50')
            else 'lower_liquidity'
        )
    elif liquidity_status:
        values['liquidity.bucket'] = f'{liquidity_status}_context'

    scores = _maybe_dict(request_payload.get('scores'))
    factor_summary = _maybe_dict(request_payload.get('factor_summary'))
    factor_scores = _maybe_dict(factor_summary.get('factor_scores'))
    for family in ('unified_value', 'momentum', 'quality'):
        score_payload = _maybe_dict(scores.get(family))
        factor_payload = _maybe_dict(factor_scores.get(family))
        raw_score = score_payload.get('score')
        if raw_score is None:
            raw_score = factor_payload.get('score')
        bucket_value = _factor_bucket_value(
            status=factor_payload.get('status') or score_payload.get('status'),
            score=raw_score,
        )
        if bucket_value:
            values[f'factor.{family}.bucket'] = bucket_value

    signal_features = _maybe_dict(factor_summary.get('signal_features'))
    signal_feature_bucket = _normalized_text(
        signal_features.get('row_kind') or signal_features.get('status')
    )
    if signal_feature_bucket:
        values['factor.signal_features.bucket'] = signal_feature_bucket
    return values


def _decision_feature_values(
    *,
    request_payload: dict[str, Any],
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
    values = _server_owned_calibration_feature_values(request_payload)
    values['decision.action'] = decision_action
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
    reward_risk_ratio = _reward_risk_ratio(
        reference_price=reference,
        target_price=target,
        stop_loss_price=stop,
    )
    if reward_risk_ratio is not None:
        try:
            normalized_reward_risk_ratio = reward_risk_ratio.quantize(RATIO_PLACES)
        except DecimalException:
            normalized_reward_risk_ratio = None
        if _decimal_text(normalized_reward_risk_ratio) is not None:
            values['decision.reward_risk_ratio'] = format(normalized_reward_risk_ratio, 'f')
    return values


def _predicate_matches(predicate: dict[str, Any], *, feature_values: dict[str, str]) -> bool:
    if not predicate:
        return False
    if 'all' in predicate:
        all_predicates = predicate.get('all')
        if (
            not isinstance(all_predicates, list)
            or not all_predicates
            or any(not isinstance(item, dict) for item in all_predicates)
        ):
            return False
        return all(
            _predicate_matches(item, feature_values=feature_values)
            for item in all_predicates
        )
    if 'any' in predicate:
        any_predicates = predicate.get('any')
        if (
            not isinstance(any_predicates, list)
            or not any_predicates
            or any(not isinstance(item, dict) for item in any_predicates)
        ):
            return False
        return any(
            _predicate_matches(item, feature_values=feature_values)
            for item in any_predicates
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


def _canonical_rule_candidate_predicate_matches(
    predicate: dict[str, Any],
    *,
    feature_values: dict[str, str],
) -> bool:
    if predicate.get('schema_version') != RULE_CANDIDATE_PREDICATE_SCHEMA_VERSION:
        return False
    if ('all' in predicate) == ('any' in predicate):
        return False
    predicate_branch = 'all' if 'all' in predicate else 'any'
    if set(predicate) != {'schema_version', predicate_branch}:
        return False
    return _predicate_matches(predicate, feature_values=feature_values)


def evaluate_next_open_performance_gate(
    *,
    request_payload: dict[str, Any],
    decision_action: str,
    confidence_score_10: Any,
    expected_return_pct: Any,
    reference_price: Any,
    target_price: Any,
    stop_loss_price: Any,
    expected_slippage_bps: Any = None,
    expected_slippage_basis: Any = None,
    allow_pending_live_cost_confirmation: bool = False,
    server_non_spread_round_trip_cost_bps: Any = 0,
    defer_non_spread_round_trip_cost_to_exec: bool = False,
    base_min_confidence_score_10: Any,
    base_min_expected_return_pct: Any,
    base_min_reward_risk_ratio: Any = DEFAULT_MIN_REWARD_RISK_RATIO,
) -> dict[str, Any]:
    criteria = _criteria_from_request_payload(request_payload)
    base_min_confidence = _to_decimal(base_min_confidence_score_10) or Decimal('0')
    base_min_expected_return = _to_decimal(base_min_expected_return_pct) or Decimal('0')
    min_reward_risk = _to_decimal(base_min_reward_risk_ratio)
    if min_reward_risk is None:
        min_reward_risk = DEFAULT_MIN_REWARD_RISK_RATIO
    adjusted_min_confidence = base_min_confidence
    adjusted_min_expected_return = base_min_expected_return
    feature_values = _decision_feature_values(
        request_payload=request_payload,
        decision_action=decision_action,
        confidence_score_10=confidence_score_10,
        reference_price=reference_price,
        target_price=target_price,
        stop_loss_price=stop_loss_price,
    )
    blocking_reasons: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    expected_return_economics, economics_blocks, economics_warnings = _expected_return_economics(
        decision_action=decision_action,
        reported_expected_return_pct=expected_return_pct,
        reference_price=reference_price,
        target_price=target_price,
        stop_loss_price=stop_loss_price,
        expected_slippage_bps=expected_slippage_bps,
        expected_slippage_basis=expected_slippage_basis,
        server_non_spread_round_trip_cost_bps=server_non_spread_round_trip_cost_bps,
        defer_non_spread_round_trip_cost_to_exec=defer_non_spread_round_trip_cost_to_exec,
        allow_pending_live_cost_confirmation=allow_pending_live_cost_confirmation,
    )
    effective_expected_return = expected_return_economics.pop('_effective_expected_return', None)
    economics_gross_reward_risk_ratio = expected_return_economics.pop(
        '_gross_reward_risk_ratio',
        None,
    )
    economics_slippage_adjusted_reward_risk_ratio = expected_return_economics.pop(
        '_slippage_adjusted_reward_risk_ratio',
        None,
    )
    economics_total_cost_adjusted_reward_risk_ratio = expected_return_economics.pop(
        '_total_cost_adjusted_reward_risk_ratio',
        None,
    )
    blocking_reasons.extend(economics_blocks)
    warnings.extend(economics_warnings)
    for key in (
        'reported_expected_return_pct',
        'derived_gross_return_pct',
        'derived_slippage_adjusted_return_pct',
        'gross_reward_risk_ratio',
        'slippage_adjusted_reward_risk_ratio',
        'estimated_total_transaction_cost_bps',
        'derived_total_cost_adjusted_return_pct',
        'total_cost_adjusted_reward_risk_ratio',
        'effective_expected_return_pct',
        'expected_slippage_bps',
        'expected_slippage_basis',
    ):
        value = expected_return_economics.get(key)
        if value is not None:
            feature_values[f'decision.{key}'] = value
    reward_risk_bucket = feature_values.get('decision.reward_risk_bucket')
    reward_risk_ratio = _reward_risk_ratio(
        reference_price=_to_decimal(reference_price),
        target_price=_to_decimal(target_price),
        stop_loss_price=_to_decimal(stop_loss_price),
    )
    reward_risk_ratio_for_gate = (
        economics_gross_reward_risk_ratio
        if expected_return_economics.get('status') == 'pending_live_cost_confirmation'
        else economics_slippage_adjusted_reward_risk_ratio
        if defer_non_spread_round_trip_cost_to_exec
        else economics_total_cost_adjusted_reward_risk_ratio
    )
    if decision_action.strip().lower() == 'buy':
        if reward_risk_ratio is None:
            blocking_reasons.append(
                {
                    'code': 'reward_risk_structure_invalid',
                    'message': 'Buy target and stop must provide positive upside and downside from the reference price.',
                    'reward_risk_bucket': reward_risk_bucket,
                }
            )
        elif reward_risk_ratio_for_gate is not None and reward_risk_ratio_for_gate < min_reward_risk:
            blocking_reasons.append(
                {
                    'code': (
                        'reward_risk_ratio_below_threshold'
                        if expected_return_economics.get('status') == 'pending_live_cost_confirmation'
                        else 'total_cost_adjusted_reward_risk_ratio_below_threshold'
                        if not defer_non_spread_round_trip_cost_to_exec
                        and _to_decimal(server_non_spread_round_trip_cost_bps)
                        not in {None, Decimal('0')}
                        else 'slippage_adjusted_reward_risk_ratio_below_threshold'
                    ),
                    'message': (
                        'Transaction-cost-adjusted expected upside is too small relative to adjusted loss '
                        'distance for next-open buy eligibility.'
                    ),
                    'reward_risk_ratio': format(
                        reward_risk_ratio_for_gate.quantize(RATIO_PLACES),
                        'f',
                    ),
                    'min_reward_risk_ratio': format(min_reward_risk, 'f'),
                }
            )
    if not criteria:
        _append_adjusted_threshold_blocks(
            decision_action=decision_action,
            confidence_score_10=confidence_score_10,
            effective_expected_return=effective_expected_return,
            adjusted_min_confidence=adjusted_min_confidence,
            adjusted_min_expected_return=adjusted_min_expected_return,
            blocking_reasons=blocking_reasons,
        )
        return {
            'status': 'absent',
            'feature_values': feature_values,
            'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
            'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
            'min_reward_risk_ratio': format(min_reward_risk, 'f'),
            'expected_return_economics': expected_return_economics,
            'blocking_reasons': blocking_reasons,
            'warnings': warnings,
            'matched_negative_buckets': [],
            'matched_rule_candidates': [],
        }
    usage_policy = _maybe_dict(criteria.get('usage_policy'))
    if usage_policy.get('deterministic_gate_eligible') is not True:
        _append_adjusted_threshold_blocks(
            decision_action=decision_action,
            confidence_score_10=confidence_score_10,
            effective_expected_return=effective_expected_return,
            adjusted_min_confidence=adjusted_min_confidence,
            adjusted_min_expected_return=adjusted_min_expected_return,
            blocking_reasons=blocking_reasons,
        )
        return {
            'status': 'not_eligible',
            'sample_count': criteria.get('sample_count'),
            'feature_values': feature_values,
            'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
            'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
            'min_reward_risk_ratio': format(min_reward_risk, 'f'),
            'expected_return_economics': expected_return_economics,
            'blocking_reasons': blocking_reasons,
            'warnings': warnings,
            'matched_negative_buckets': [],
            'matched_rule_candidates': [],
        }

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
        if (
            str(candidate.get('candidate_type') or '') not in {'buy_veto', 'tighten_buy_gate'}
            or str(candidate.get('activation_status') or '') != 'active'
            or candidate.get('active_policy') is not True
            or candidate.get('deterministic_gate_eligible') is not True
            or candidate.get('manual_review_only') is True
        ):
            continue
        if _canonical_rule_candidate_predicate_matches(
            _maybe_dict(candidate.get('predicate')),
            feature_values=feature_values,
        ):
            matched_rule_candidates.append(candidate)

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

    _append_adjusted_threshold_blocks(
        decision_action=decision_action,
        confidence_score_10=confidence_score_10,
        effective_expected_return=effective_expected_return,
        adjusted_min_confidence=adjusted_min_confidence,
        adjusted_min_expected_return=adjusted_min_expected_return,
        blocking_reasons=blocking_reasons,
    )

    return {
        'status': criteria.get('status') or 'ready',
        'sample_count': criteria.get('sample_count'),
        'feature_values': feature_values,
        'adjusted_min_confidence_score_10': str(adjusted_min_confidence),
        'adjusted_min_expected_return_pct': str(adjusted_min_expected_return),
        'min_reward_risk_ratio': format(min_reward_risk, 'f'),
        'expected_return_economics': expected_return_economics,
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
        base_min_reward_risk_ratio=Decimal('0'),
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
