import json

from app.orders.exec_payload_slimming import (
    SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES,
    compact_exec_item_link,
    compact_exec_plan_item,
    compact_exec_readback,
    compact_exec_response_summary,
    json_size_bytes,
    slim_signal_decider_exec_submission_payload,
)


def test_compact_exec_readback_preserves_execution_plan_item_identity() -> None:
    compact = compact_exec_readback(
        {
            'executions': {
                'status': 'ok',
                'count': 1,
                'items': [
                    {
                        'id': 'execution-1',
                        'execution_plan_item_id': 'exec-plan-item-1',
                        'broker_order_id': 'broker-order-1',
                        'symbol': '005930',
                        'side': 'buy',
                        'quantity': '1',
                        'price': '50000',
                    }
                ],
            },
        }
    )

    assert compact is not None
    assert compact['executions']['items'][0]['execution_plan_item_id'] == (
        'exec-plan-item-1'
    )


def test_compact_exec_readback_preserves_broker_order_pk_linkage() -> None:
    compact = compact_exec_readback(
        {
            'orders': {
                'status': 'ok',
                'count': 1,
                'items': [
                    {
                        'broker_order_pk': 'internal-order-row-1',
                        'execution_plan_item_id': 'exec-plan-item-1',
                        'symbol': '005930',
                        'side': 'buy',
                    }
                ],
            },
            'executions': {
                'status': 'ok',
                'count': 1,
                'items': [
                    {
                        'execution_fill_id': 'execution-1',
                        'broker_order_pk': 'internal-order-row-1',
                        'symbol': '005930',
                        'side': 'buy',
                    }
                ],
            },
        }
    )

    assert compact is not None
    assert compact['orders']['items'][0]['broker_order_pk'] == (
        'internal-order-row-1'
    )
    assert compact['executions']['items'][0]['broker_order_pk'] == (
        'internal-order-row-1'
    )
    assert compact['executions']['items'][0]['execution_fill_id'] == 'execution-1'


def test_compact_exec_item_link_preserves_long_remote_identity_strings() -> None:
    long_core_item_key = 'core-item-' + ('c' * 300)
    long_exec_plan_id = 'exec-plan-' + ('p' * 300)
    long_exec_plan_item_id = 'exec-item-' + ('i' * 300)

    compact = compact_exec_item_link(
        {
            'core_item_key': long_core_item_key,
            'intent': 'next_open_buy',
            'side': 'buy',
            'exec_plan_id': long_exec_plan_id,
            'exec_plan_item_id': long_exec_plan_item_id,
            'remote_status': 'accepted',
        }
    )

    assert compact is not None
    assert compact['core_item_key'] == long_core_item_key
    assert compact['exec_plan_id'] == long_exec_plan_id
    assert compact['exec_plan_item_id'] == long_exec_plan_item_id


def test_compact_exec_plan_preserves_delegated_gate_and_slippage_contract() -> None:
    compact = compact_exec_plan_item(
        {
            'core_item_key': 'core-plan-81:intraday_watch_buy',
            'agent_name': 'short-term-decider',
            'decision_agent_name': 'short-term-decider',
            'symbol': '005930',
            'side': 'buy',
            'intent': 'intraday_watch_buy',
            'quantity_policy': {'type': 'exec_agent_sized'},
            'risk_context': {
                'classification': 'delegated_for_exec_agent_sizing',
                'delegated_gate_codes': [
                    'short_term_liquidity_floor_block_delegated_to_exec_watch',
                    'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
                ],
                'delegated_gate_confirmation_required': True,
                'delegated_gates': [
                    {
                        'code': 'short_term_liquidity_floor_block_delegated_to_exec_watch',
                        'observed_value': '2500000000',
                        'field_path': 'risk_context.validation.short_term_entry_quality.raw_trading_value_krw',
                        'comparator': 'gte',
                        'threshold': '3000000000',
                        'policy_version': 'core_delegated_gate_policy_v1',
                        'observed_at': '2026-04-29T06:00:00+00:00',
                        'recheck_source': 'exec_trigger_time_market_liquidity',
                        'expires_at': '2026-04-30T15:30:00+09:00',
                    },
                    {
                        'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
                        'observed_value': None,
                        'field_path': 'risk_context.execution_feasibility_context.expected_slippage_bps',
                        'comparator': 'finite_non_negative_with_basis',
                        'threshold': '0',
                        'policy_version': 'core_delegated_gate_policy_v1',
                        'observed_at': '2026-04-29T06:00:00+00:00',
                        'recheck_source': 'exec_trigger_time_quote_and_cost_model',
                        'expires_at': '2026-04-30T15:30:00+09:00',
                    },
                ],
                'execution_feasibility_context': {
                    'expected_slippage_bps': None,
                    'expected_slippage_basis': 'unavailable',
                    'expected_slippage_status': 'missing',
                    'expected_slippage_reason': 'quote_spread_missing',
                    'liquidity_tier': 'medium',
                },
            },
            'decision_snapshot_id': '145',
        }
    )

    risk_summary = compact['risk_context_summary']
    assert risk_summary['delegated_gate_codes'] == [
        'short_term_liquidity_floor_block_delegated_to_exec_watch',
        'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
    ]
    assert risk_summary['delegated_gate_confirmation_required'] is True
    assert risk_summary['delegated_gates'] == [
        {
            'code': 'short_term_liquidity_floor_block_delegated_to_exec_watch',
            'observed_value': '2500000000',
            'field_path': 'risk_context.validation.short_term_entry_quality.raw_trading_value_krw',
            'comparator': 'gte',
            'threshold': '3000000000',
            'policy_version': 'core_delegated_gate_policy_v1',
            'observed_at': '2026-04-29T06:00:00+00:00',
            'recheck_source': 'exec_trigger_time_market_liquidity',
            'expires_at': '2026-04-30T15:30:00+09:00',
        },
        {
            'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
            'observed_value': None,
            'field_path': 'risk_context.execution_feasibility_context.expected_slippage_bps',
            'comparator': 'finite_non_negative_with_basis',
            'threshold': '0',
            'policy_version': 'core_delegated_gate_policy_v1',
            'observed_at': '2026-04-29T06:00:00+00:00',
            'recheck_source': 'exec_trigger_time_quote_and_cost_model',
            'expires_at': '2026-04-30T15:30:00+09:00',
        },
    ]
    assert risk_summary['execution_feasibility_context'] == {
        'expected_slippage_bps': None,
        'expected_slippage_basis': 'unavailable',
        'expected_slippage_status': 'missing',
        'expected_slippage_reason': 'quote_spread_missing',
        'liquidity_tier': 'medium',
    }


def test_compact_exec_plan_normalizes_nonfinite_values_before_strict_json_serialization() -> None:
    compact = compact_exec_plan_item(
        {
            'core_item_key': 'core-plan-82:next_open_buy',
            'symbol': '005930',
            'side': 'buy',
            'intent': 'next_open_buy',
            'risk_context': {
                'execution_feasibility_context': {
                    'expected_slippage_bps': float('nan'),
                    'expected_slippage_basis': 'unavailable',
                },
                'delegated_gates': [
                    {
                        'code': 'short_term_liquidity_floor_block_delegated_to_exec_watch',
                        'observed_value': float('inf'),
                    }
                ],
            },
        }
    )

    json.dumps(compact, allow_nan=False)
    risk_summary = compact['risk_context_summary']
    self_cost = risk_summary['execution_feasibility_context']['expected_slippage_bps']
    assert self_cost is None
    assert risk_summary['delegated_gates'][0]['observed_value'] is None


def test_compact_v4_ack_summary_reads_item_acks() -> None:
    summary = compact_exec_response_summary(
        {
            'plan_id': 'core-plan-1',
            'execution_plan_id': 'exec-plan-1',
            'accepted_item_count': 2,
            'rejected_item_count': 0,
            'item_acks': [
                {
                    'core_item_key': 'entry',
                    'execution_plan_id': 'exec-plan-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'status': 'accepted',
                },
                {
                    'core_item_key': 'stop',
                    'execution_plan_id': 'exec-plan-1',
                    'execution_plan_item_id': 'exec-item-2',
                    'status': 'accepted',
                },
            ],
        },
        item_links=[
            {'core_item_key': 'entry'},
            {'core_item_key': 'stop'},
        ],
    )

    assert summary is not None
    assert summary['total_item_count'] == 2
    assert summary['row_item_count'] == 2
    assert [item['execution_plan_item_id'] for item in summary['row_items']] == [
        'exec-item-1',
        'exec-item-2',
    ]


def test_submission_slimming_preserves_pending_revocation_identity_and_histories() -> None:
    item_links = [
        {
            'core_item_key': f'core-plan-91:{intent}',
            'intent': intent,
            'side': 'buy' if intent == 'next_open_buy' else 'sell',
            'exec_plan_id': 'exec-plan-old',
            'exec_plan_item_id': f'exec-item-{index}',
            'remote_status': 'accepted',
        }
        for index, intent in enumerate(
            ('next_open_buy', 'target_sell', 'stop_sell'),
            start=1,
        )
    ]
    revocation = {
        'schema_version': 'core_execution_revocation_state_v1',
        'status': 'pending',
        'reason_code': 'current_live_market_risk_off',
        'requested_at': '2026-04-30T00:05:00+00:00',
        'prior_decision_snapshot_id': '41',
        'replacement_decision_snapshot_id': 'observation:risk-off:1',
        'replacement_observation_id': 'observation:risk-off:1',
        'replacement_action': 'buy',
        'replacement_actionable': False,
        'plan_id': 'core-plan-old',
        'item_links': item_links,
    }
    revision_history = [
        {
            'decision_snapshot_id': '41',
            'decision_agent_name': 'signal-decider',
            'symbol': '005930',
            'execution_status': 'submitted',
            'dispatch_evidence': {
                'plan_id': 'core-plan-old',
                'dispatch_status': 'accepted',
                'item_links': item_links,
            },
            'archived_at': '2026-04-30T00:05:00+00:00',
        }
    ]
    revocation_history = [
        {
            'acknowledged_at': '2026-04-30T00:04:00+00:00',
            'request': {
                'plan_id': 'core-plan-before-old',
                'replacement_decision_snapshot_id': '40',
                'items': [
                    {
                        'core_item_key': item['core_item_key'],
                        'execution_plan_item_id': item['exec_plan_item_id'],
                    }
                    for item in item_links
                ],
            },
            'response': {
                'plan_id': 'core-plan-before-old',
                'item_acks': [
                    {
                        'core_item_key': item['core_item_key'],
                        'execution_plan_item_id': item['exec_plan_item_id'],
                        'status': 'revoked',
                    }
                    for item in item_links
                ],
            },
        }
    ]
    result = slim_signal_decider_exec_submission_payload(
        {
            'mode': 'exec_dispatch',
            'exec_dispatch': {
                'plan_id': 'core-plan-old',
                'dispatch_status': 'accepted',
                'item_links': item_links,
                'last_request_payload': {'raw_context': 'x' * 70000},
            },
            'revision_history': revision_history,
            'revocation': revocation,
            'revocation_history': revocation_history,
        }
    )

    assert result.changed is True
    assert result.payload is not None
    assert result.payload['revocation'] == revocation
    assert result.payload['revision_history'] == revision_history
    assert result.payload['revocation_history'] == revocation_history
    assert json_size_bytes(result.payload) <= SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES


def test_submission_slimming_preserves_write_ahead_and_destination_binding() -> None:
    destination_binding = {
        'schema_version': 'core_exec_destination_binding_v1',
        'contract_version': 'kai-execution-handoff-v4',
        'base_url': 'https://exec-a.example.test:443/api',
        'runtime_mode': 'paper',
        'dispatch_source': 'kai-trading-core',
        'account_reference': 'KIS:98123456-01',
        'destination_fingerprint': 'd' * 64,
    }
    result = slim_signal_decider_exec_submission_payload(
        {
            'mode': 'exec_dispatch',
            'exec_dispatch': {
                'plan_id': 'core-signal-decider-20260430-main',
                'plan_fingerprint': 'a' * 64,
                'dispatch_revision_fingerprint': 'b' * 64,
                'dispatch_material_fingerprint': 'c' * 64,
                'dispatch_batch_key': 'batch-1',
                'dispatch_status': 'pending_remote_ack',
                'prior_execution_status': 'planned',
                'prior_eligibility_status': 'eligible',
                'destination_binding': destination_binding,
                'authority_generation_before': {
                    'status': 'valid',
                    'generation_fingerprint': 'e' * 64,
                },
                'item_links': [
                    {
                        'core_item_key': 'core-plan-91:next_open_buy',
                        'intent': 'next_open_buy',
                        'side': 'buy',
                        'symbol': '005930',
                    }
                ],
                'last_request_payload': {
                    'items': [{'raw_context': 'x' * 70000}],
                },
            },
        }
    )

    assert result.payload is not None
    write_ahead = result.payload['exec_dispatch']
    assert write_ahead['dispatch_status'] == 'pending_remote_ack'
    assert write_ahead['dispatch_material_fingerprint'] == 'c' * 64
    assert write_ahead['dispatch_revision_fingerprint'] == 'b' * 64
    assert write_ahead['destination_binding'] == destination_binding
    assert write_ahead['authority_generation_before'] == {
        'status': 'valid',
        'generation_fingerprint': 'e' * 64,
    }
    assert write_ahead['item_links'][0]['core_item_key'] == (
        'core-plan-91:next_open_buy'
    )
    assert 'bearer_token' not in json.dumps(result.payload)
    assert json_size_bytes(result.payload) <= SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES


def test_oversized_lifecycle_evidence_keeps_revocation_bindings_actionable() -> None:
    item_links = [
        {
            'core_item_key': f'core-plan-92:{intent}',
            'intent': intent,
            'side': 'buy' if intent == 'next_open_buy' else 'sell',
            'exec_plan_id': 'exec-plan-old',
            'exec_plan_item_id': f'exec-item-{index}',
            'remote_status': 'accepted',
        }
        for index, intent in enumerate(
            ('next_open_buy', 'target_sell', 'stop_sell'),
            start=1,
        )
    ]
    revocation = {
        'status': 'pending',
        'reason_code': 'latest_snapshot_replaced',
        'requested_at': '2026-04-30T00:05:00+00:00',
        'prior_decision_snapshot_id': '41',
        'replacement_decision_snapshot_id': '42',
        'replacement_action': 'buy',
        'replacement_actionable': True,
        'plan_id': 'core-plan-old',
        'item_links': item_links,
    }
    result = slim_signal_decider_exec_submission_payload(
        {
            'mode': 'exec_dispatch',
            'exec_dispatch': {
                'plan_id': 'core-plan-old',
                'dispatch_status': 'accepted',
                'item_links': item_links,
                'last_request_payload': {'raw_context': 'x' * 70000},
            },
            'revocation': revocation,
            'revision_history': [
                {
                    'decision_snapshot_id': '41',
                    'validation_payload': {'raw_context': 'y' * 70000},
                    'traceability_payload': {'raw_context': 'z' * 70000},
                    'dispatch_evidence': {
                        'plan_id': 'core-plan-old',
                        'item_links': item_links,
                    },
                }
            ],
            'revocation_history': [
                {
                    'acknowledged_at': '2026-04-29T23:59:00+00:00',
                    'request': {
                        'plan_id': 'prior-plan',
                        'items': [
                            {
                                'core_item_key': item['core_item_key'],
                                'execution_plan_item_id': item['exec_plan_item_id'],
                            }
                            for item in item_links
                        ],
                        'raw_context': 'q' * 70000,
                    },
                    'response': {
                        'plan_id': 'prior-plan',
                        'item_acks': [
                            {
                                'core_item_key': item['core_item_key'],
                                'execution_plan_item_id': item['exec_plan_item_id'],
                                'status': 'revoked',
                            }
                            for item in item_links
                        ],
                        'raw_context': 'r' * 70000,
                    },
                }
            ],
        }
    )

    assert result.payload is not None
    assert result.payload['revocation']['item_links'] == item_links
    assert (
        result.payload['revision_history'][0]['dispatch_evidence']['item_links']
        == item_links
    )
    assert result.payload['revocation_history'][0]['request']['items'] == [
        {
            'core_item_key': item['core_item_key'],
            'execution_plan_item_id': item['exec_plan_item_id'],
        }
        for item in item_links
    ]
    assert result.payload['revocation_history'][0]['response']['item_acks'] == [
        {
            'core_item_key': item['core_item_key'],
            'execution_plan_item_id': item['exec_plan_item_id'],
            'status': 'revoked',
        }
        for item in item_links
    ]
    assert json_size_bytes(result.payload) <= SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES
