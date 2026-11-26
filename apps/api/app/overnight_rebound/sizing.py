"""Whole-share rule sizing after the original lanes' proven entitlement."""

from datetime import datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from typing import Literal

from pydantic import AwareDatetime, StrictBool, StrictInt

from app.overnight_rebound.execution_contract import (
    Nonnegative, Positive, RuleExecutionPolicy, RuleExecutionSetup, Text, _Frozen, _aware,
)


class RuleAccountEvidence(_Frozen):
    """Private evidence group, not an atomic broker snapshot.

    The adapter must prove the full original-lane entitlement, including
    potential orders, and every pending reservation across the shared account.
    The two KIS cash-only ceilings are conditional broker limits, not proof of
    settled/net cash. Unknown broker reservation coverage needs a proven upper
    bound in unproven_broker_pending_reserve_krw, or cannot produce a grant.
    Unknown values stay None and cannot produce a grant.
    """

    account_reference: Text
    provider: Literal['kis']
    runtime_mode: Literal['paper', 'live']
    symbol: Literal['069500', '091160']
    order_possible_price: Positive
    read_group_id: Text
    observed_at: AwareDatetime
    balance_observed_at: AwareDatetime
    order_possible_observed_at: AwareDatetime
    order_possible_requested_at: AwareDatetime
    broker_orders_observed_at: AwareDatetime
    baseline_observed_at: AwareDatetime
    account_equity_krw: Positive | None
    ord_psbl_cash: Nonnegative | None
    nrcvb_buy_amt: Nonnegative | None
    nrcvb_buy_qty: StrictInt | None
    unproven_broker_pending_reserve_krw: Nonnegative | None
    baseline_entitlement_krw: Nonnegative | None
    buying_power_krw: Nonnegative | None
    remaining_account_buy_notional_krw: Nonnegative | None
    remaining_account_risk_krw: Nonnegative | None
    symbol_exposure_krw: Nonnegative | None
    symbol_reserved_krw: Nonnegative | None
    strategy_committed_notional_krw: Nonnegative | None
    liquidity_committed_notional_krw: Nonnegative | None
    other_rule_committed_notional_krw: Nonnegative | None
    other_rule_committed_risk_krw: Nonnegative | None
    baseline_entitlements_proven: StrictBool
    reservations_complete: StrictBool
    exposure_complete: StrictBool
    original_signal_potential: StrictBool | None
    original_short_term_potential: StrictBool | None
    original_open_order: StrictBool | None
    original_holding: StrictBool | None
    ambiguous_exposure: StrictBool | None


class RuleSizingResult(_Frozen):
    target_quantity: StrictInt
    sizing_price: Positive
    cash_per_share: Positive
    risk_per_share: Positive
    residual_cash_krw: Nonnegative | None
    calculated_at: AwareDatetime
    valid_until: AwareDatetime
    reasons: tuple[str, ...]
    quantity_limits: tuple[tuple[str, int], ...]


def size_rule_entry(
    *, setup: RuleExecutionSetup, policy: RuleExecutionPolicy,
    account: RuleAccountEvidence | None, now: datetime,
) -> RuleSizingResult:
    """Grant an initial cumulative target, never a fixed monetary allocation.

    This does not resize filled families or release baseline reservations. A
    later execution/revision needs exact persisted outbox debt and Exec CAS.
    """
    now = _aware(now)
    if setup.policy_hash != policy.policy_hash:
        raise ValueError('rule sizing policy differs from the immutable setup')
    entry = setup.chase_limit_price
    cost = entry * (policy.round_trip_cost_bps + policy.slippage_bps) / 10000
    risk = max(entry - setup.stop_price, entry * setup.volatility_pct / 100 * policy.volatility_multiplier) + cost
    cash_per_share = entry + cost
    reasons = []
    remaining = None
    limits = {}
    if now >= setup.entry_expires_at or now.date() != setup.origin.target_trade_date:
        reasons.append('entry_grant_expired')
    if not timedelta(0) <= now - setup.evidence_observed_at <= timedelta(seconds=policy.max_evidence_age_seconds):
        reasons.append('market_evidence_stale_or_future')
    if account is None:
        reasons.append('account_evidence_unavailable')
    else:
        if account.account_reference != setup.account_reference:
            raise ValueError('rule account differs from the immutable setup')
        if account.symbol != setup.symbol or account.runtime_mode != setup.mode or account.order_possible_price != entry:
            raise ValueError('rule account evidence scope/price differs from setup')
        if account.order_possible_requested_at > account.order_possible_observed_at:
            raise ValueError('rule order-possible request must precede its receipt')
        if any(not timedelta(0) <= now - observed <= timedelta(seconds=policy.max_evidence_age_seconds) for observed in (
            account.observed_at, account.balance_observed_at, account.order_possible_observed_at,
            account.order_possible_requested_at, account.broker_orders_observed_at, account.baseline_observed_at,
        )):
            reasons.append('account_evidence_stale_or_future')
        if not account.baseline_entitlements_proven:
            reasons.append('baseline_entitlement_unproven')
        if not account.reservations_complete:
            reasons.append('account_reservations_unproven')
        if not account.exposure_complete:
            reasons.append('account_exposure_unproven')
        conflicts = (
            account.original_signal_potential, account.original_short_term_potential,
            account.original_open_order, account.original_holding, account.ambiguous_exposure,
        )
        if any(value is None for value in conflicts):
            reasons.append('original_lane_exposure_unknown')
        elif any(conflicts):
            reasons.append('original_lane_symbol_priority')
        money_fields = (
            'account_equity_krw', 'ord_psbl_cash', 'nrcvb_buy_amt', 'nrcvb_buy_qty',
            'unproven_broker_pending_reserve_krw',
            'baseline_entitlement_krw', 'buying_power_krw', 'remaining_account_buy_notional_krw',
            'remaining_account_risk_krw', 'symbol_exposure_krw', 'symbol_reserved_krw',
            'strategy_committed_notional_krw', 'liquidity_committed_notional_krw',
            'other_rule_committed_notional_krw', 'other_rule_committed_risk_krw',
        )
        if any(getattr(account, name) is None for name in money_fields):
            reasons.append('account_limits_incomplete')
        if not reasons:
            remaining = max(
                min(account.ord_psbl_cash, account.nrcvb_buy_amt) - account.baseline_entitlement_krw
                - account.unproven_broker_pending_reserve_krw - account.other_rule_committed_notional_krw, Decimal(0),
            )
            symbol_remaining = max(
                account.account_equity_krw * policy.max_symbol_fraction
                - account.symbol_exposure_krw - account.symbol_reserved_krw, Decimal(0),
            )
            strategy_remaining = max(
                policy.max_strategy_notional_krw - account.strategy_committed_notional_krw, Decimal(0),
            )
            liquidity_remaining = max(
                setup.daily_value_krw * policy.max_liquidity_fraction
                - account.liquidity_committed_notional_krw, Decimal(0),
            )
            risk_budget = min(
                max(account.account_equity_krw * policy.loss_fraction - account.other_rule_committed_risk_krw, Decimal(0)),
                max(account.account_equity_krw * Decimal('0.015') - account.other_rule_committed_risk_krw, Decimal(0)),
                account.remaining_account_risk_krw,
            )
            for label, amount in (
                ('residual_cash', remaining), ('buying_power', account.buying_power_krw),
                ('account_buy_limit', account.remaining_account_buy_notional_krw),
                ('trade_limit', policy.max_trade_notional_krw), ('symbol_limit', symbol_remaining),
                ('strategy_limit', strategy_remaining), ('liquidity_limit', liquidity_remaining),
            ):
                limits[label] = int((amount / cash_per_share).to_integral_value(rounding=ROUND_FLOOR))
            limits['loss_limit'] = int((risk_budget / risk).to_integral_value(rounding=ROUND_FLOOR))
            limits['broker_cash_only_quantity'] = max(account.nrcvb_buy_qty, 0)
            if min(limits.values()) == 0:
                reasons.append('insufficient_residual_capacity')
    return RuleSizingResult(
        target_quantity=0 if reasons else min(limits.values()), sizing_price=entry,
        cash_per_share=cash_per_share, risk_per_share=risk, residual_cash_krw=remaining,
        calculated_at=now, valid_until=setup.entry_expires_at,
        reasons=tuple(reasons), quantity_limits=tuple(limits.items()),
    )
