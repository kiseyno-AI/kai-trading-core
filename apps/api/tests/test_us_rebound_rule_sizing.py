from decimal import Decimal

import pytest

from app.overnight_rebound.sizing import RuleAccountEvidence, size_rule_entry
from tests.test_us_rebound_execution_contract import NOW, market, policy, setup


def account(**updates):
    return RuleAccountEvidence.model_validate({
        'account_reference': 'KIS:test-only', 'observed_at': NOW.isoformat(),
        'provider': 'kis', 'runtime_mode': 'paper', 'symbol': '069500',
        'order_possible_price': '10050', 'read_group_id': 'test-evidence-group',
        'balance_observed_at': NOW.isoformat(), 'order_possible_observed_at': NOW.isoformat(),
        'order_possible_requested_at': NOW.isoformat(),
        'broker_orders_observed_at': NOW.isoformat(), 'baseline_observed_at': NOW.isoformat(),
        'account_equity_krw': '1000000', 'ord_psbl_cash': '500000', 'nrcvb_buy_amt': '500000',
        'nrcvb_buy_qty': 40, 'unproven_broker_pending_reserve_krw': '100000', 'baseline_entitlement_krw': '200000',
        'buying_power_krw': '400000', 'remaining_account_buy_notional_krw': '400000',
        'remaining_account_risk_krw': '15000',
        'symbol_exposure_krw': '0', 'symbol_reserved_krw': '0',
        'strategy_committed_notional_krw': '0', 'liquidity_committed_notional_krw': '0',
        'other_rule_committed_notional_krw': '0', 'other_rule_committed_risk_krw': '0',
        'baseline_entitlements_proven': True, 'reservations_complete': True,
        'exposure_complete': True, 'original_signal_potential': False,
        'original_short_term_potential': False, 'original_open_order': False,
        'original_holding': False, 'ambiguous_exposure': False, **updates,
    })


def test_rule_spends_only_proven_residual_cash_in_whole_shares_including_costs():
    result = size_rule_entry(setup=setup().setup, policy=policy(), account=account(), now=NOW)

    # 500,000 broker ceiling - 100,000 unproven pending reserve - 200,000 baseline = 200,000.
    # Each share reserves 10,050 + 0.5% round-trip/slippage = 10,100.25.
    assert result.target_quantity == 19
    assert result.sizing_price == Decimal('10050')
    assert result.cash_per_share == Decimal('10100.25')
    assert result.risk_per_share == Decimal('300.25')
    assert result.reasons == ()


@pytest.mark.parametrize(('updates', 'reason'), [
    ({'original_signal_potential': True}, 'original_lane_symbol_priority'),
    ({'original_short_term_potential': True}, 'original_lane_symbol_priority'),
    ({'original_open_order': True}, 'original_lane_symbol_priority'),
    ({'original_holding': True}, 'original_lane_symbol_priority'),
    ({'ambiguous_exposure': True}, 'original_lane_symbol_priority'),
    ({'ambiguous_exposure': None}, 'original_lane_exposure_unknown'),
    ({'baseline_entitlements_proven': False}, 'baseline_entitlement_unproven'),
    ({'reservations_complete': False}, 'account_reservations_unproven'),
    ({'exposure_complete': False}, 'account_exposure_unproven'),
    ({'other_rule_committed_notional_krw': None}, 'account_limits_incomplete'),
    ({'account_equity_krw': None}, 'account_limits_incomplete'),
    ({'remaining_account_risk_krw': None}, 'account_limits_incomplete'),
    ({'order_possible_observed_at': NOW.replace(hour=7, minute=54), 'order_possible_requested_at': NOW.replace(hour=7, minute=54)}, 'account_evidence_stale_or_future'),
    ({'balance_observed_at': NOW.replace(minute=1)}, 'account_evidence_stale_or_future'),
])
def test_original_lanes_unknown_limits_and_each_stale_component_deny_the_rule_only(updates, reason):
    result = size_rule_entry(setup=setup().setup, policy=policy(), account=account(**updates), now=NOW)
    assert result.target_quantity == 0
    assert reason in result.reasons


@pytest.mark.parametrize(('updates', 'quantity'), [
    ({'nrcvb_buy_qty': 1}, 1),
    ({'buying_power_krw': '10100.25'}, 1),
    ({'remaining_account_buy_notional_krw': '20200.50'}, 2),
    ({'remaining_account_risk_krw': '600.50'}, 2),
    ({'symbol_exposure_krw': '189899.75'}, 1),
    ({'strategy_committed_notional_krw': '4989899.75'}, 1),
    ({'liquidity_committed_notional_krw': '9989899.75'}, 1),
    ({'other_rule_committed_notional_krw': '189899.75'}, 1),
    ({'nrcvb_buy_amt': '300000'}, 0),
])
def test_every_shared_ceiling_and_private_rule_commitment_limits_the_whole_share_grant(updates, quantity):
    result = size_rule_entry(setup=setup().setup, policy=policy(), account=account(**updates), now=NOW)
    assert result.target_quantity == quantity


def test_loss_limit_uses_stop_or_volatility_plus_all_costs_without_fake_confidence():
    explicit = policy(loss_fraction='0.001')
    prepared = setup(policy=explicit).setup
    result = size_rule_entry(setup=prepared, policy=explicit, account=account(), now=NOW)
    # 1,000,000 * 0.1% = 1,000 risk; 1,000 / 300.25 permits exactly 3 shares.
    assert result.target_quantity == 3
    volatile = setup(market=market(volatility_pct='4')).setup
    result = size_rule_entry(setup=volatile, policy=policy(), account=account(), now=NOW)
    assert result.risk_per_share == Decimal('452.25')


def test_account_order_possible_evidence_must_bind_the_exact_symbol_mode_and_price():
    with pytest.raises(ValueError, match='scope/price'):
        size_rule_entry(setup=setup().setup, policy=policy(), account=account(symbol='091160'), now=NOW)
    with pytest.raises(ValueError, match='scope/price'):
        size_rule_entry(setup=setup().setup, policy=policy(), account=account(runtime_mode='live'), now=NOW)
    with pytest.raises(ValueError, match='scope/price'):
        size_rule_entry(setup=setup().setup, policy=policy(), account=account(order_possible_price='10000'), now=NOW)

