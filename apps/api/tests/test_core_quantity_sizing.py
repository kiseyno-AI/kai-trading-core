from decimal import Decimal

import pytest

from app.orders.core_quantity_sizing import calculate_core_quantity_targets


def candidate(key, symbol, **changes):
    return {
        'core_plan_item_id': key, 'symbol': symbol, 'decision_agent_name': 'signal-decider',
        'trigger_price': '100', 'chase_limit_price': '100', 'target_price': '130',
        'stop_loss_price': '95', 'confidence_score_10': '10', **changes,
    }


def test_expected_reward_and_stop_risk_determine_different_whole_share_allocations():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930'), candidate(2, '000660', target_price='120', stop_loss_price='90')],
        account_equity_krw=Decimal('100000'), available_cash_krw=Decimal('10000'),
    )

    # Net reward/risk weights are 29.5/5 and 19.5/10; cash shares are 7515.92/2484.07.
    assert [row['target_quantity'] for row in result] == [75, 24]
    assert [row['additional_buy_quantity'] for row in result] == [75, 24]
    assert sum(row['target_quantity'] * 100 for row in result) == 9900
    assert result[0]['audit']['sizing_price'] == '100'
    assert result[0]['audit']['net_expected_return_pct'] == '29.5'


def test_decider_roles_remain_independent_and_share_one_symbol_exposure_ceiling():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930'), candidate(2, '005930', decision_agent_name='short-term-decider')],
        account_equity_krw='100000', available_cash_krw='100000',
    )
    assert [row['target_quantity'] for row in result] == [150, 150]
    assert [row['decision_agent_name'] for row in result] == ['signal-decider', 'short-term-decider']
    assert sum(row['additional_buy_quantity'] * 100 for row in result) == 30000


def test_symbol_alias_cannot_bypass_shared_exposure_limit():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930'), candidate(2, '005930 ', decision_agent_name='short-term-decider')],
        account_equity_krw='100000', available_cash_krw='100000',
    )
    assert [row['target_quantity'] for row in result] == [300, 0]
    assert result[1]['audit']['reasons'] == ['invalid_candidate_identity']


def test_reduced_target_preserves_fills_and_reduces_pending_buys_without_a_sell():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', stop_loss_price='98', filled_quantity=250, pending_buy_quantity=100)],
        account_equity_krw='100000', available_cash_krw='0',
        symbol_exposure_krw={'005930': '25000'},
        reserved_buy_notional_by_symbol={'005930': '10000'},
    )
    assert result[0]['target_quantity'] == 300
    assert result[0]['remaining_buy_quantity'] == 50
    assert result[0]['additional_buy_quantity'] == 0
    assert result[0]['pending_buy_quantity_to_reduce'] == 50


def test_liquidity_cap_is_shared_by_both_decider_lanes_for_one_symbol():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', max_safe_notional_krw='5000'),
         candidate(2, '005930', decision_agent_name='short-term-decider', max_safe_notional_krw='5000')],
        account_equity_krw='100000', available_cash_krw='100000',
    )
    assert [row['target_quantity'] for row in result] == [25, 25]


def test_zero_liquidity_ceiling_preserves_fills_without_another_buy():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', max_safe_notional_krw=0, filled_quantity=10, pending_buy_quantity=3),
         candidate(2, '000660')],
        account_equity_krw='100000', available_cash_krw='10000',
        symbol_exposure_krw={'005930': '1000'},
        reserved_buy_notional_by_symbol={'005930': '300'},
    )
    assert result[0]['target_quantity'] == 10
    assert result[0]['additional_buy_quantity'] == 0
    assert result[0]['pending_buy_quantity_to_reduce'] == 3
    assert result[1]['additional_buy_quantity'] > 0


def test_net_cash_is_not_reduced_twice_by_known_broker_reservations():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', filled_quantity=10, pending_buy_quantity=20)],
        account_equity_krw='100000', available_cash_krw='10000',
        symbol_exposure_krw={'005930': '1000'},
        reserved_buy_notional_by_symbol={'005930': '5000'},
    )
    assert result[0]['target_quantity'] == 130
    assert result[0]['additional_buy_quantity'] == 100


@pytest.mark.parametrize('equity,cash', [(None, '1000'), ('NaN', '1000'), ('10000', None), ('10000', '-1')])
def test_unknown_account_retains_fills_and_does_not_authorize_another_buy(equity, cash):
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', filled_quantity=20, pending_buy_quantity=3)],
        account_equity_krw=equity, available_cash_krw=cash,
    )
    assert result[0]['target_quantity'] == 20
    assert result[0]['remaining_buy_quantity'] == 0
    assert result[0]['additional_buy_quantity'] == 0
    assert 'account_snapshot_unavailable' in result[0]['audit']['reasons']


def test_missing_optional_data_does_not_block_and_valid_volatility_reduces_risk_quantity():
    base = calculate_core_quantity_targets(
        [candidate(1, '005930')], account_equity_krw='100000', available_cash_krw='100000',
    )[0]
    volatile = calculate_core_quantity_targets(
        [candidate(1, '005930', atr_pct_14='20')],
        account_equity_krw='100000', available_cash_krw='100000',
    )[0]
    assert base['target_quantity'] == 300
    assert volatile['target_quantity'] == 75
    invalid_optional = calculate_core_quantity_targets(
        [candidate(1, '005930', atr_pct_14='NaN', max_safe_notional_krw='bad')],
        account_equity_krw='100000', available_cash_krw='100000',
    )[0]
    assert invalid_optional['target_quantity'] == 300
    assert invalid_optional['audit']['warnings'] == ['invalid_volatility_ignored', 'invalid_liquidity_cap_ignored']


def test_unusable_candidate_does_not_block_other_role_and_low_score_remains_observation_only():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', target_price=None),
         candidate(2, '000660', confidence_score_10='5.9'),
         candidate(3, '035420', decision_agent_name='short-term-decider')],
        account_equity_krw='100000', available_cash_krw='350',
    )
    assert [row['target_quantity'] for row in result] == [0, 0, 3]
    assert result[1]['audit']['reasons'] == ['observation_only_confidence']


def test_supplied_return_cannot_inflate_geometry_and_inputs_are_not_mutated():
    source = candidate(1, '005930', expected_return_pct='999')
    before = dict(source)
    result = calculate_core_quantity_targets(
        [source], account_equity_krw='100000', available_cash_krw='100000',
    )
    assert source == before
    assert result[0]['audit']['net_expected_return_pct'] == '29.5'


def test_exposure_cap_never_sells_existing_fills_when_account_has_shrunk():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', filled_quantity=400, pending_buy_quantity=20)],
        account_equity_krw='100000', available_cash_krw='50000',
        symbol_exposure_krw={'005930': '40000'},
    )[0]
    assert result['target_quantity'] == 400
    assert result['additional_buy_quantity'] == 0
    assert result['pending_buy_quantity_to_reduce'] == 20
    assert result['audit']['held_quantity_preserved'] is True


def test_active_exec_absolute_symbol_cap_limits_large_wallet_without_a_fixed_minimum():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930')], account_equity_krw='100000000', available_cash_krw='100000000',
        max_symbol_notional_krw='3000000',
    )[0]
    assert result['target_quantity'] == 30000
    assert result['audit']['max_symbol_notional_krw'] == '3000000'


def test_total_budget_counts_lifetime_fills_and_pending_instead_of_regranting_it_each_refresh():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', filled_quantity=10000, pending_buy_quantity=5000),
         candidate(2, '000660', filled_quantity=20000, pending_buy_quantity=10000)],
        account_equity_krw='100000000', available_cash_krw='100000000',
        max_total_buy_notional_krw='9000000',
    )
    assert [row['target_quantity'] for row in result] == [37500, 52500]
    assert sum(row['target_quantity'] * 100 for row in result) == 9000000
    exhausted = calculate_core_quantity_targets(
        [candidate(1, '005930', filled_quantity=45000), candidate(2, '000660', filled_quantity=45000)],
        account_equity_krw='100000000', available_cash_krw='100000000',
        max_total_buy_notional_krw='9000000',
    )
    assert [row['target_quantity'] for row in exhausted] == [45000, 45000]
    assert sum(row['additional_buy_quantity'] for row in exhausted) == 0


def test_closed_entry_family_keeps_lifetime_fills_in_total_budget_and_never_reenters():
    result = calculate_core_quantity_targets(
        [candidate(1, '005930', entry_closed=True, filled_quantity=90000), candidate(2, '000660')],
        account_equity_krw='100000000', available_cash_krw='100000000',
        max_total_buy_notional_krw='9000000',
    )
    assert [row['target_quantity'] for row in result] == [90000, 0]
    assert sum(row['additional_buy_quantity'] for row in result) == 0
    assert 'entry_family_closed' in result[0]['audit']['reasons']
