from datetime import date, datetime
from decimal import Decimal

import pytest

from app.internal.exact_repair_manifest import canonical_json_sha256
from app.overnight_rebound.execution_contract import (
    RuleExecutionPolicy, RuleMarketEvidence, RuleSessionEvidence,
    build_rule_execution_setup,
)


NOW = datetime.fromisoformat('2026-11-27T08:00:00+09:00')


def policy(**updates):
    return RuleExecutionPolicy.model_validate({
        'execution_mode': 'paper', 'validation_status': 'paper_only',
        'validation_evidence_id': None, 'validation_evidence_hash': None,
        'entry_gap_pct': '0', 'chase_gap_pct': '0.5',
        'stop_loss_pct': '2', 'target_return_pct': '5',
        'round_trip_cost_bps': '30', 'slippage_bps': '20',
        'volatility_multiplier': '1', 'loss_fraction': '0.01',
        'max_symbol_fraction': '0.20', 'max_trade_notional_krw': '3000000',
        'max_strategy_notional_krw': '5000000', 'max_liquidity_fraction': '0.01',
        'max_evidence_age_seconds': 300,
        'entry_expires_at': '2026-11-27T09:10:00+09:00',
        'close_buffer_seconds': 600,
        **updates,
    })


def evaluation():
    body = {
        'schema_version': 'us_rebound_evaluation_v1', 'strategy_id': 'krx_us_rebound',
        'status': 'WATCH', 'rule_version': 'exploratory_v1',
        'validation_status': 'unvalidated', 'order_authority': False,
        'config_hash': 'a' * 64, 'feature_id': 'b' * 64,
        'input_manifest_id': 'c' * 64, 'target_trade_date': '2026-11-27',
        'source_environment': 'real', 'completed_at': NOW.isoformat(),
    }
    return {**body, 'evaluation_id': canonical_json_sha256(body)}


def market(**updates):
    return RuleMarketEvidence.model_validate({
        'symbol': '069500', 'prior_close_date': '2026-11-26',
        'prior_close': '10000', 'volatility_pct': '1.5', 'daily_value_krw': '1000000000',
        'observed_at': NOW.isoformat(), 'source': 'verified-krx-eod',
        'evidence_id': 'd' * 64, 'source_environment': 'real', 'status': 'verified',
        **updates,
    })


def session(**updates):
    return RuleSessionEvidence.model_validate({
        'target_trade_date': '2026-11-27', 'previous_trade_date': '2026-11-26',
        'session_open_at': '2026-11-27T09:00:00+09:00',
        'session_close_at': '2026-11-27T15:30:00+09:00',
        'calendar_status': 'authoritative', 'calendar_source': 'exchange_calendars.XKRX:test',
        'special_hours': False, **updates,
    })


def setup(**updates):
    return build_rule_execution_setup(**{
        'policy': policy(), 'evaluation': evaluation(), 'market': market(), 'session': session(),
        'account_reference': 'KIS:test-only', 'run_id': 'run-test', 'now': NOW, **updates,
    })


def test_explicit_paper_policy_builds_tick_aligned_etf_geometry_without_agent_provenance():
    result = setup()

    assert result.reasons == ()
    assert result.setup.trigger_price == Decimal('10000')
    assert result.setup.chase_limit_price == Decimal('10050')
    assert result.setup.stop_price == Decimal('9800')
    assert result.setup.target_price == Decimal('10500')
    assert result.setup.close_exit_at.isoformat() == '2026-11-27T15:20:00+09:00'
    assert result.setup.origin.strategy_id == 'krx_us_rebound'
    assert evaluation()['order_authority'] is False


def test_rule_plan_keeps_null_agent_fields_and_exact_linked_exit_family():
    from app.overnight_rebound.execution_contract import build_rule_execution_plan
    from app.overnight_rebound.sizing import size_rule_entry
    from tests.test_us_rebound_rule_sizing import account

    prepared = setup().setup
    grant = size_rule_entry(setup=prepared, policy=policy(), account=account(), now=NOW)
    plan = build_rule_execution_plan(
        setup=prepared, sizing=grant, authority_generation_fingerprint='e' * 64, requested_at=NOW,
    )

    assert plan['origin_type'] == 'rule'
    assert plan['strategy_run_id'] is None
    assert plan['metadata']['agent_name'] is None
    assert len(plan['items']) == 3
    buy, target, stop = plan['items']
    assert buy['quantity_policy']['target_quantity'] == 19
    assert target['quantity_policy']['linked_entry_item_key'] == buy['core_item_key']
    assert stop['quantity_policy']['linked_entry_item_key'] == buy['core_item_key']
    assert all(item['rule_origin'] == plan['rule_origin'] for item in plan['items'])
    assert all(item['agent_name'] is item['decision_agent_name'] is item['decision_snapshot_id'] is None
               for item in plan['items'])


@pytest.mark.parametrize(('updates', 'reason'), [
    ({'market': None}, 'market_evidence_unavailable'),
    ({'session': None}, 'calendar_evidence_unavailable'),
    ({'now': NOW.replace(hour=8, minute=40)}, 'new_setup_window_expired'),
    ({'market': market(observed_at=NOW.replace(minute=54, hour=7))}, 'market_evidence_stale_or_future'),
    ({'market': market(prior_close_date='2026-11-25')}, 'prior_krx_close_unproven'),
    ({'session': session(special_hours=True)}, 'special_session_unsupported'),
    ({'policy': policy(execution_mode='live')}, 'policy_not_validated_for_live'),
    ({'policy': policy(target_return_pct='0.6')}, 'nonpositive_net_edge'),
    ({'policy': policy(chase_gap_pct='6')}, 'invalid_price_geometry_or_exhausted_gap'),
])
def test_incomplete_unvalidated_or_exhausted_evidence_cannot_create_a_setup(updates, reason):
    result = setup(**updates)
    assert result.setup is None
    assert reason in result.reasons


def test_etf_ticks_change_at_2000_without_rounding_the_chase_up():
    prepared = setup(market=market(prior_close='1999')).setup
    assert (prepared.trigger_price, prepared.chase_limit_price, prepared.target_price, prepared.stop_price) == (
        Decimal('1999'), Decimal('2005'), Decimal('2095'), Decimal('1959'),
    )


def test_rule_only_price_digest_matches_independent_exec_literal():
    from app.overnight_rebound.execution_contract import rule_price_contract_digest

    assert rule_price_contract_digest(
        entry_limit_price=Decimal('10000'), entry_trigger_price=Decimal('10000'),
        target_price=Decimal('10500'), stop_price=Decimal('9800'),
        close_exit_at=datetime.fromisoformat('2026-11-27T15:20:00+09:00'),
    ) == '295d5aa22d7dcc2102e8224094d8e273e953e04e43b73180f2f289b08f21a94f'


def test_updated_evaluation_run_and_rule_version_keep_the_same_session_setup_claim():
    updated = evaluation()
    updated['rule_version'] = 'new-version'
    updated.pop('evaluation_id')
    updated['evaluation_id'] = canonical_json_sha256(updated)
    assert setup(evaluation=updated, run_id='new-run').setup.origin.setup_id == setup().setup.origin.setup_id


def test_live_policy_requires_explicit_validation_evidence_and_all_parameters():
    with pytest.raises(ValueError, match='explicit evidence'):
        policy(execution_mode='live', validation_status='validated_live')
    with pytest.raises(ValueError):
        RuleExecutionPolicy()
    with pytest.raises(ValueError):
        policy(loss_fraction='0.0151')


def test_future_evaluation_or_malformed_replay_identity_is_not_executable():
    future = evaluation()
    future['completed_at'] = NOW.replace(minute=1).isoformat()
    future.pop('evaluation_id')
    future['evaluation_id'] = canonical_json_sha256(future)
    with pytest.raises(ValueError, match='evaluation'):
        setup(evaluation=future)
    invalid = evaluation()
    invalid['config_hash'] = 'not-a-digest'
    invalid.pop('evaluation_id')
    invalid['evaluation_id'] = canonical_json_sha256(invalid)
    with pytest.raises(ValueError, match='evaluation'):
        setup(evaluation=invalid)


def test_actual_worked_wire_fixture_is_reproducible_for_exec_public_intake():
    import json
    from pathlib import Path
    from app.overnight_rebound.execution_contract import build_rule_execution_plan
    from app.overnight_rebound.sizing import size_rule_entry
    from tests.test_us_rebound_rule_sizing import account

    prepared = setup(account_reference='KIS:12345678-01').setup
    grant = size_rule_entry(setup=prepared, policy=policy(), account=account(account_reference='KIS:12345678-01'), now=NOW)
    actual = build_rule_execution_plan(setup=prepared, sizing=grant, authority_generation_fingerprint='e' * 64, requested_at=NOW)
    fixture = json.loads((Path(__file__).parent / 'fixtures' / 'us_rebound_rule_v4.json').read_text())
    assert actual == fixture
    assert actual['metadata']['dispatch_material_fingerprint'] == '3f1c25f505d2a83b939f146e7ff4c63726674127d53096a5ae7d59e26d0ba44a'


@pytest.mark.parametrize('mutation', ['snapshot', 'agent', 'deadline', 'missing_session', 'link', 'extra_field'])
def test_rule_wire_rejects_fake_agent_provenance_and_broken_protection(mutation):
    import json
    from pathlib import Path
    from app.overnight_rebound.execution_contract import RuleExecutionPlan

    payload = json.loads((Path(__file__).parent / 'fixtures' / 'us_rebound_rule_v4.json').read_text())
    if mutation == 'snapshot':
        payload['items'][0]['decision_snapshot_id'] = 'fabricated'
    elif mutation == 'agent':
        payload['metadata']['agent_name'] = 'signal-decider'
    elif mutation == 'deadline':
        payload['items'][0]['close_exit_at'] = '2026-11-27T09:00:00+09:00'
    elif mutation == 'missing_session':
        del payload['items'][0]['risk_context']['rule_session']
    elif mutation == 'link':
        payload['items'][1]['quantity_policy']['linked_entry_item_key'] = 'unrelated-entry'
    else:
        payload['items'][0]['confidence_score_10'] = '9'
    with pytest.raises(ValueError):
        RuleExecutionPlan.model_validate(payload)

