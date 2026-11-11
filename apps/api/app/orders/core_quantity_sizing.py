"""Deterministic, account-wide KRX entry targets; this module never submits orders."""

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

from app.market.symbol_codes import is_canonical_krx_short_code


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _whole(value: Any) -> int | None:
    number = _decimal(value)
    return int(number) if number is not None and number >= 0 and number == number.to_integral_value() else None


def calculate_core_quantity_targets(
    candidates: Sequence[Mapping[str, Any]],
    *,
    account_equity_krw: Any,
    available_cash_krw: Any,
    symbol_exposure_krw: Mapping[str, Any] | None = None,
    reserved_buy_notional_by_symbol: Mapping[str, Any] | None = None,
    risk_fraction: Decimal = Decimal('0.015'),
    max_symbol_fraction: Decimal = Decimal('0.30'),
    round_trip_cost_bps: Decimal = Decimal('50'),
    max_symbol_notional_krw: Any = None,
    max_total_buy_notional_krw: Any = None,
) -> list[dict[str, Any]]:
    """Size cumulative entry quantities independently by role, sharing account limits.

    Cash MUST already be net of every pending-buy reservation. Symbol exposure is
    current held notional; reserved notional is separate and is not subtracted
    from cash again. Filled quantity is lifetime BUY fills for the entry family,
    not current holdings. A smaller target never requests a SELL.

    The default 1.5% trade-risk ceiling, 30% symbol ceiling and 50bps KRX cost
    match existing Exec guardrails. There is no minimum/fixed monetary allocation.
    The immutable chase limit is the worst entry used for cash and risk sizing.
    An optional total budget counts cumulative entry fills plus still-open BUY
    reservations, including entries that are now observation-only.
    """
    risk_fraction = _decimal(risk_fraction)
    max_symbol_fraction = _decimal(max_symbol_fraction)
    round_trip_cost_bps = _decimal(round_trip_cost_bps)
    absolute_symbol_cap = _decimal(max_symbol_notional_krw)
    total_buy_cap = _decimal(max_total_buy_notional_krw)
    if (
        risk_fraction is None or not 0 < risk_fraction <= Decimal('0.015')
        or max_symbol_fraction is None or not 0 < max_symbol_fraction <= 1
        or round_trip_cost_bps is None or round_trip_cost_bps < 0
        or max_symbol_notional_krw is not None and (absolute_symbol_cap is None or absolute_symbol_cap <= 0)
        or max_total_buy_notional_krw is not None and (total_buy_cap is None or total_buy_cap < 0)
    ):
        raise ValueError('Invalid Core quantity sizing limits')
    equity = _decimal(account_equity_krw)
    cash = _decimal(available_cash_krw)
    account_ready = equity is not None and equity > 0 and cash is not None and cash >= 0
    counts = Counter(str(row.get('core_plan_item_id')) for row in candidates)
    prepared = []
    weights = defaultdict(Decimal)
    own_reservations = defaultdict(Decimal)
    filled_notionals = defaultdict(Decimal)
    liquidity_limits = {}
    committed_notional = Decimal(0)
    total_commitment_known = True
    for row in candidates:
        reasons = []
        warnings = []
        filled = _whole(row.get('filled_quantity', 0))
        pending = _whole(row.get('pending_buy_quantity', 0))
        prices = [_decimal(row.get(key)) for key in (
            'trigger_price', 'chase_limit_price', 'target_price', 'stop_loss_price',
        )]
        trigger, entry, target, stop = prices
        confidence = _decimal(row.get('confidence_score_10'))
        if filled is None or pending is None or not row.get('core_plan_item_id') or counts[str(row.get('core_plan_item_id'))] != 1:
            total_commitment_known = False
        elif filled or pending:
            if entry is None or entry <= 0:
                total_commitment_known = False
            else:
                committed_notional += (filled + pending) * entry
        if filled is None or pending is None:
            reasons.append('invalid_execution_quantities')
        filled, pending = filled or 0, pending or 0
        if not row.get('core_plan_item_id') or counts[str(row.get('core_plan_item_id'))] != 1:
            reasons.append('invalid_or_duplicate_plan_item_id')
        if (
            row.get('decision_agent_name') not in {'signal-decider', 'short-term-decider'}
            or not is_canonical_krx_short_code(row.get('symbol'))
            or row.get('symbol') != str(row.get('symbol')).strip().upper()
        ):
            reasons.append('invalid_candidate_identity')
        if any(value is None or value <= 0 for value in prices) or not stop < trigger <= entry < target:
            reasons.append('invalid_price_contract')
        if confidence is None or not 0 <= confidence <= 10:
            reasons.append('invalid_confidence')
        elif confidence < 6:
            reasons.append('observation_only_confidence')
        if row.get('entry_closed') is True:
            reasons.append('entry_family_closed')
        if not account_ready:
            reasons.append('account_snapshot_unavailable')
        item = {'row': row, 'filled': filled, 'pending': pending, 'reasons': reasons, 'warnings': warnings}
        prepared.append(item)
        if reasons:
            continue
        net_return = (target / entry - 1) * 100
        if row.get('expected_return_pct') is not None:
            supplied_return = _decimal(row['expected_return_pct'])
            if supplied_return is None:
                warnings.append('invalid_expected_return_ignored')
            else:
                net_return = min(net_return, supplied_return)
        net_return -= round_trip_cost_bps / 100
        if net_return <= 0:
            reasons.append('nonpositive_net_expected_return')
            continue
        risk_distance = entry - stop
        if row.get('atr_pct_14') is not None:
            volatility = _decimal(row['atr_pct_14'])
            if volatility is None or volatility <= 0:
                warnings.append('invalid_volatility_ignored')
            else:
                risk_distance = max(risk_distance, entry * volatility / 100)
        liquidity = _decimal(row.get('max_safe_notional_krw'))
        if row.get('max_safe_notional_krw') is not None and (liquidity is None or liquidity < 0):
            warnings.append('invalid_liquidity_cap_ignored')
            liquidity = None
        weight = confidence / 10 * net_return / (risk_distance / entry * 100)
        item.update(entry=entry, net_return=net_return, risk_distance=risk_distance, weight=weight, liquidity=liquidity)
        weights[row['symbol']] += weight
        own_reservations[row['symbol']] += pending * entry
        filled_notionals[row['symbol']] += filled * entry
        if liquidity is not None:
            liquidity_limits[row['symbol']] = min(liquidity, liquidity_limits.get(row['symbol'], liquidity))

    total_weight = sum(weights.values(), Decimal(0))
    total_remaining = max(total_buy_cap - committed_notional, Decimal(0)) if total_buy_cap is not None else None
    result = []
    for item in prepared:
        row, reasons = item['row'], item['reasons']
        filled, pending = item['filled'], item['pending']
        quantity = filled
        audit = {
            'schema_version': 'core_quantity_sizing_v1',
            'risk_fraction': str(risk_fraction), 'max_symbol_fraction': str(max_symbol_fraction),
            'round_trip_cost_bps': str(round_trip_cost_bps),
            'max_symbol_notional_krw': str(absolute_symbol_cap) if absolute_symbol_cap is not None else None,
            'max_total_buy_notional_krw': str(total_buy_cap) if total_buy_cap is not None else None,
            'warnings': item['warnings'], 'reasons': reasons,
        }
        if total_buy_cap is not None and not total_commitment_known:
            reasons.append('total_buy_commitments_unproven')
        if not reasons:
            symbol = row['symbol']
            exposure = _decimal((symbol_exposure_krw or {}).get(symbol, 0))
            reserved = _decimal((reserved_buy_notional_by_symbol or {}).get(symbol, 0))
            if exposure is None or exposure < 0 or reserved is None or reserved < 0:
                reasons.append('invalid_symbol_exposure')
            else:
                entry, weight = item['entry'], item['weight']
                risk_budget = equity * risk_fraction
                unrelated_reserved = max(reserved - own_reservations[symbol], Decimal(0))
                symbol_cap = equity * max_symbol_fraction
                if absolute_symbol_cap is not None:
                    symbol_cap = min(symbol_cap, absolute_symbol_cap)
                symbol_remaining = max(symbol_cap - exposure - unrelated_reserved, Decimal(0))
                cash_share = cash * weight / total_weight
                symbol_share = symbol_remaining * weight / weights[symbol]
                existing_quantity = filled + pending
                limits = {
                    'cash_quantity': existing_quantity + int((cash_share / entry).to_integral_value(rounding=ROUND_DOWN)),
                    'symbol_quantity': filled + int((symbol_share / entry).to_integral_value(rounding=ROUND_DOWN)),
                    'risk_quantity': int((risk_budget / item['risk_distance']).to_integral_value(rounding=ROUND_DOWN)),
                }
                if total_remaining is not None:
                    limits['total_budget_quantity'] = existing_quantity + int((
                        total_remaining * weight / total_weight / entry
                    ).to_integral_value(rounding=ROUND_DOWN))
                if symbol in liquidity_limits:
                    liquidity_remaining = max(
                        liquidity_limits[symbol] - filled_notionals[symbol] - unrelated_reserved, Decimal(0),
                    )
                    limits['liquidity_quantity'] = filled + int((
                        liquidity_remaining * weight / weights[symbol] / entry
                    ).to_integral_value(rounding=ROUND_DOWN))
                quantity = max(filled, min(limits.values()))
                audit.update({
                    'sizing_price': str(entry), 'net_expected_return_pct': str(item['net_return']),
                    'risk_distance': str(item['risk_distance']), 'risk_budget_krw': str(risk_budget),
                    'allocation_weight': str(weight), 'cash_share_krw': str(cash_share),
                    'symbol_remaining_krw': str(symbol_remaining), 'quantity_limits': limits,
                    'held_quantity_preserved': filled > min(limits.values()),
                })
        remaining = max(quantity - filled, 0)
        result.append({
            'core_plan_item_id': row.get('core_plan_item_id'), 'symbol': row.get('symbol'),
            'decision_agent_name': row.get('decision_agent_name'),
            'status': 'observation_only' if reasons or remaining == 0 else 'ready',
            'target_quantity': quantity, 'remaining_buy_quantity': remaining,
            'additional_buy_quantity': max(remaining - pending, 0),
            'pending_buy_quantity_to_reduce': max(pending - remaining, 0),
            'audit': audit,
        })
    return result
