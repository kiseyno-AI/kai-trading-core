from __future__ import annotations

from copy import deepcopy
from decimal import Decimal

import pytest

from app.orders.execution_handoff_contract import (
    EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    execution_plan_dispatch_material_fingerprint,
    serialize_execution_plan_dispatch_payload,
    validate_execution_plan_dispatch_ack,
)


CANONICAL_FINGERPRINT_VECTOR = (
    '375c4f82a02c723364675d9999ae9d0bb9e41a46d7ae54654d070e5f45bb54b6'
)


def _dispatch_payload(*, budget: object = 100_000, risk_value: object = 1) -> dict:
    entry_key = 'core-plan-1:next_open_buy'

    def item(*, key: str, side: str, intent: str, trigger_price: object = None) -> dict:
        quantity_policy = (
            {
                'type': 'exec_agent_sized',
                'budget_krw': budget,
            }
            if side == 'buy'
            else {
                'type': 'position_exit_from_entry',
                'linked_entry_item_key': entry_key,
            }
        )
        return {
            'core_item_key': key,
            'source_plan_item_id': key,
            'agent_name': 'signal-decider',
            'decision_agent_name': 'signal-decider',
            'symbol': '005930',
            'asset_class': 'stock',
            'market': 'KRX',
            'exchange': 'KRX',
            'currency': 'KRW',
            'broker': 'kis',
            'provider': 'kis',
            'venue': 'kis',
            'session_policy': {
                'session': 'regular',
                'timezone': 'Asia/Seoul',
                'allow_extended_hours': False,
            },
            'side': side,
            'intent': intent,
            'quantity_policy': quantity_policy,
            'trigger_price': trigger_price,
            'risk_context': {'numeric_probe': risk_value},
            'decision_snapshot_id': '41',
        }

    requested_at = '2026-04-30T00:00:00+00:00'
    return {
        'plan_id': 'core-signal-decider-20260430-main',
        'source': 'kai-trading-core',
        'mode': 'mock',
        'market_date': '2026-04-30',
        'strategy_run_id': 'signal-decider:2026-04-30:main',
        'requested_at': requested_at,
        'items': [
            item(key=entry_key, side='buy', intent='next_open_buy'),
            item(
                key='core-plan-1:target_sell',
                side='sell',
                intent='target_sell',
                trigger_price=54_000,
            ),
            item(
                key='core-plan-1:stop_sell',
                side='sell',
                intent='stop_sell',
                trigger_price=47_000,
            ),
        ],
        'metadata': {
            'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'required_capability': EXECUTION_HANDOFF_CAPABILITY,
            'agent_name': 'signal-decider',
            'decision_agent_name': 'signal-decider',
            'trigger': 'test',
            'requested_at': requested_at,
            'account_reference': 'KIS:12345678-01',
            'account_label': 'Main',
            'source_market_date': '2026-04-29',
            'target_trade_date': '2026-04-30',
            'dispatch_batch_type': 'initial',
            'dispatch_batch_key': None,
        },
    }


def _ack(*, payload: dict, fingerprint: str) -> dict:
    item_acks = [
        {
            'core_item_key': item['core_item_key'],
            'status': 'accepted',
            'execution_plan_id': 'exec-plan-1',
            'execution_plan_item_id': f"exec-{index}",
        }
        for index, item in enumerate(payload['items'], start=1)
    ]
    return {
        'contract_ack': {
            'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'capability': EXECUTION_HANDOFF_CAPABILITY,
            'plan_id': payload['plan_id'],
            'status': 'accepted',
            'dispatch_material_fingerprint': fingerprint,
        },
        'item_acks': item_acks,
        'plan_id': payload['plan_id'],
        'execution_plan_id': 'exec-plan-1',
        'accepted_item_count': len(item_acks),
        'rejected_item_count': 0,
    }


def test_dispatch_material_fingerprint_matches_cross_repo_canonical_vector() -> None:
    payload = {
        'plan_id': 'canonical-vector',
        'requested_at': 'ignored-top',
        'metadata': {
            'trigger': 'ignored-trigger',
            'requested_at': 'ignored-metadata',
            'dispatch_material_fingerprint': 'f' * 64,
            'numeric': 1,
        },
        'nested': [
            True,
            1,
            1.0,
            Decimal('1.000'),
            '1',
            None,
            {'b': 2, 'a': '한글'},
        ],
    }

    assert (
        execution_plan_dispatch_material_fingerprint(payload)
        == CANONICAL_FINGERPRINT_VECTOR
    )


def test_dispatch_serializer_binds_canonical_material_fingerprint() -> None:
    integer = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=100_000, risk_value=1)
    )
    floating = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=100_000.0, risk_value=1.0)
    )
    decimal = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=Decimal('100000.000'), risk_value=Decimal('1.000'))
    )

    fingerprint = integer['metadata']['dispatch_material_fingerprint']
    assert len(fingerprint) == 64
    assert set(fingerprint) <= set('0123456789abcdef')
    assert floating['metadata']['dispatch_material_fingerprint'] == fingerprint
    assert decimal['metadata']['dispatch_material_fingerprint'] == fingerprint

    non_material = deepcopy(integer)
    non_material['requested_at'] = '2026-04-30T00:01:00+00:00'
    non_material['metadata']['requested_at'] = non_material['requested_at']
    non_material['metadata']['trigger'] = 'retry'
    assert (
        serialize_execution_plan_dispatch_payload(non_material)['metadata'][
            'dispatch_material_fingerprint'
        ]
        == fingerprint
    )

    changed = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=100_001, risk_value=1)
    )
    assert changed['metadata']['dispatch_material_fingerprint'] != fingerprint

    boolean = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=100_000, risk_value=True)
    )
    assert boolean['metadata']['dispatch_material_fingerprint'] != fingerprint


def test_dispatch_ack_rejects_old_material_with_same_plan_and_item_keys() -> None:
    old_dispatch = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=100_000)
    )
    current_dispatch = serialize_execution_plan_dispatch_payload(
        _dispatch_payload(budget=200_000)
    )
    old_fingerprint = old_dispatch['metadata']['dispatch_material_fingerprint']
    current_fingerprint = current_dispatch['metadata']['dispatch_material_fingerprint']
    stale_ack = _ack(payload=old_dispatch, fingerprint=old_fingerprint)
    submitted_keys = [item['core_item_key'] for item in current_dispatch['items']]

    with pytest.raises(ValueError, match='material fingerprint mismatch'):
        validate_execution_plan_dispatch_ack(
            stale_ack,
            plan_id=current_dispatch['plan_id'],
            submitted_core_item_keys=submitted_keys,
            expected_dispatch_material_fingerprint=current_fingerprint,
        )

    accepted = validate_execution_plan_dispatch_ack(
        stale_ack,
        plan_id=old_dispatch['plan_id'],
        submitted_core_item_keys=submitted_keys,
        expected_dispatch_material_fingerprint=old_fingerprint,
    )
    assert set(accepted) == set(submitted_keys)
