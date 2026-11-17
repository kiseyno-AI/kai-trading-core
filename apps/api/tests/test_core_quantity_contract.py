import copy

import pytest
from pydantic import ValidationError

from app.orders.execution_handoff_contract import CoreQuantityPolicy, ExecutionQuantityRefreshPayload


def policy():
    return {
        'type': 'core_sized', 'target_quantity': 7, 'revision': 1,
        'calculated_at': '2026-11-11T00:00:00+00:00',
        'valid_until': '2026-11-11T02:00:00+00:00',
        'sizing_inputs': {'price_contract_digest': 'a' * 64, 'net_expected_return_pct': '4.5'},
    }


def test_core_quantity_policy_preserves_integer_target_and_frozen_price_digest():
    result = CoreQuantityPolicy.model_validate(policy()).model_dump(mode='json')
    assert result['target_quantity'] == 7
    assert result['sizing_inputs']['price_contract_digest'] == 'a' * 64
    assert CoreQuantityPolicy.model_validate({**policy(), 'target_quantity': 0}).target_quantity == 0


@pytest.mark.parametrize('value', [True, -1, 1.5, '7'])
def test_core_quantity_policy_rejects_ambiguous_quantities(value):
    with pytest.raises(ValidationError):
        CoreQuantityPolicy.model_validate({**policy(), 'target_quantity': value})


def test_quantity_revision_is_exactly_bound_and_cannot_change_prices():
    initial = policy()
    item = {key: value for key, value in initial.items() if key != 'type'}
    item.update({
        'external_plan_id': 'core-plan', 'execution_plan_id': 'exec-plan',
        'core_item_key': 'entry-1', 'decision_snapshot_id': 'snapshot-1',
        'expected_revision': 1, 'revision': 2,
    })
    result = ExecutionQuantityRefreshPayload.model_validate({'items': [item]})
    assert result.items[0].target_quantity == 7
    invalid = copy.deepcopy(item)
    invalid['trigger_price'] = 100
    with pytest.raises(ValidationError):
        ExecutionQuantityRefreshPayload.model_validate({'items': [invalid]})
    with pytest.raises(ValidationError):
        ExecutionQuantityRefreshPayload.model_validate({'items': [{**item, 'revision': 1}]})


@pytest.mark.parametrize('overrides', [
    {'calculated_at': '2026-11-11T00:00:00'},
    {'valid_until': '2026-11-10T23:59:00+00:00'},
    {'sizing_inputs': {}},
])
def test_core_quantity_policy_requires_aware_time_and_contract_proof(overrides):
    with pytest.raises(ValidationError):
        CoreQuantityPolicy.model_validate({**policy(), **overrides})
