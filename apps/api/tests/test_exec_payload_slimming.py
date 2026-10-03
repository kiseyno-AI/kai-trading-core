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


def test_incident_shaped_lifecycle_history_never_evicts_active_dispatch_proof() -> None:
    destination_binding = {
        'schema_version': 'core_exec_destination_binding_v1',
        'contract_version': 'execution_handoff_contract_v4',
        'base_url': 'https://exec.example.test',
        'runtime_mode': 'live',
        'dispatch_source': 'kai-trading-core',
        'account_reference': 'KIS:98123456-01',
        'destination_fingerprint': 'd' * 64,
    }
    item_links = [
        {
            'core_item_key': f'core-plan-17717:{intent}',
            'intent': intent,
            'side': 'buy' if intent == 'intraday_watch_buy' else 'sell',
            'symbol': '005930',
            'exec_plan_id': 'exec-plan-new',
            'exec_plan_item_id': f'exec-item-{index}',
            'remote_status': 'accepted',
        }
        for index, intent in enumerate(
            ('intraday_watch_buy', 'target_sell', 'stop_sell'),
            start=1,
        )
    ]
    contract_proof = {
        'schema_version': 'core_exec_dispatch_proof_v2',
        'execution_plan_id': 'exec-plan-new',
        'dispatch_material_fingerprint': 'c' * 64,
        'submitted_core_item_keys': [
            item['core_item_key'] for item in item_links
        ],
        'destination_binding': destination_binding,
        'core_activation': {
            'schema_version': 'core_execution_activation_binding_v1',
            'mode': 'core_confirmed',
            'authority_generation_fingerprint': 'e' * 64,
            'status': 'pending',
            'dispatch_material_fingerprint': 'c' * 64,
        },
    }
    oversized_history = [
        {
            'decision_snapshot_id': str(100 + generation),
            'execution_status': 'revoked',
            **{
                f'diagnostic_{index}': f'generation-{generation}-' + ('x' * 2_000)
                for index in range(30)
            },
        }
        for generation in range(5)
    ]
    result = slim_signal_decider_exec_submission_payload(
        {
            'mode': 'exec_dispatch',
            'latest': {'dispatch_status': 'pending_remote_ack'},
            'exec_dispatch': {
                'plan_id': 'exec-plan-new',
                'plan_fingerprint': 'a' * 64,
                'dispatch_revision_fingerprint': 'b' * 64,
                'dispatch_material_fingerprint': 'c' * 64,
                'dispatch_status': 'pending_remote_ack',
                'prior_execution_status': 'planned',
                'prior_eligibility_status': 'eligible',
                'destination_binding': destination_binding,
                'contract_proof': contract_proof,
                'item_links': item_links,
                'last_request_payload': {'raw_context': 'r' * 100_000},
            },
            'revision_history': oversized_history,
            'revocation': {
                'status': 'acknowledged',
                'reason_code': 'current_live_market_evidence_unavailable',
                'plan_id': 'exec-plan-old',
                'item_links': item_links,
                **{
                    f'diagnostic_{index}': 'v' * 2_000
                    for index in range(30)
                },
            },
            'revocation_history': oversized_history,
            'dispatch_reconciliation': {
                'status': 'manual_review_required',
                'reason_code': 'ambiguous_remote_acceptance_after_core_revision',
                **{
                    f'diagnostic_{index}': 'q' * 2_000
                    for index in range(30)
                },
            },
        }
    )

    assert result.payload is not None
    assert json_size_bytes(result.payload) <= SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES
    active_dispatch = result.payload['exec_dispatch']
    assert active_dispatch['plan_id'] == 'exec-plan-new'
    assert active_dispatch['dispatch_status'] == 'pending_remote_ack'
    assert active_dispatch['dispatch_revision_fingerprint'] == 'b' * 64
    assert active_dispatch['dispatch_material_fingerprint'] == 'c' * 64
    assert active_dispatch['destination_binding'] == destination_binding
    assert active_dispatch['contract_proof'] == contract_proof
    assert active_dispatch['item_links'] == item_links
    assert len(result.payload.get('revision_history') or []) < len(oversized_history)


def test_pending_revocation_and_recovery_evidence_precede_optional_diagnostics() -> None:
    destination_binding = {
        'schema_version': 'core_exec_destination_binding_v1',
        'contract_version': 'execution_handoff_contract_v4',
        'base_url': 'https://exec.example.test:443',
        'runtime_mode': 'live',
        'dispatch_source': 'kai-trading-core',
        'account_reference': 'KIS:98123456-01',
        'destination_fingerprint': 'd' * 64,
    }
    item_links = [
        {
            'core_item_key': f'core-plan-17717:{intent}',
            'intent': intent,
            'side': 'buy' if intent == 'intraday_watch_buy' else 'sell',
            'symbol': '005930',
            'exec_plan_id': 'exec-plan-active',
            'exec_plan_item_id': f'exec-item-{index}',
            'remote_status': 'accepted',
        }
        for index, intent in enumerate(
            ('intraday_watch_buy', 'target_sell', 'stop_sell'),
            start=1,
        )
    ]
    reconciliation = {
        'status': 'manual_review_required',
        'reason_code': 'ambiguous_remote_acceptance',
        'observed_at': '2026-10-02T00:10:00+00:00',
        'plan_id': 'exec-plan-active',
        'plan_fingerprint': 'a' * 64,
        'submitted_dispatch_revision_fingerprint': 'b' * 64,
        'current_dispatch_revision_fingerprint': 'b' * 64,
        'error': {
            'code': 'unavailable',
            'message': 'activation response was lost',
            'retryable': True,
        },
    }
    result = slim_signal_decider_exec_submission_payload(
        {
            'mode': 'exec_dispatch',
            'exec_dispatch': {
                'plan_id': 'exec-plan-active',
                'plan_fingerprint': 'a' * 64,
                'dispatch_revision_fingerprint': 'b' * 64,
                'dispatch_material_fingerprint': 'c' * 64,
                'dispatch_status': 'accepted',
                'prior_execution_status': 'planned',
                'prior_eligibility_status': 'monitor_candidate',
                'destination_binding': destination_binding,
                'contract_proof': {
                    'schema_version': 'core_exec_dispatch_proof_v2',
                    'execution_plan_id': 'exec-plan-active',
                    'dispatch_material_fingerprint': 'c' * 64,
                    'submitted_core_item_keys': [
                        item['core_item_key'] for item in item_links
                    ],
                },
                'item_links': item_links,
                # This diagnostic is deliberately large enough to crowd out
                # lifecycle state when optional fields are admitted first.
                'last_request_summary': {
                    f'diagnostic_{index}': 'x' * 465
                    for index in range(50)
                },
            },
            'revocation': {
                'schema_version': 'core_execution_revocation_state_v1',
                'status': 'pending',
                'reason_code': 'current_live_market_evidence_unavailable',
                'requested_at': '2026-10-02T00:11:00+00:00',
                'prior_decision_snapshot_id': '17717',
                'replacement_decision_snapshot_id': '17718',
                'replacement_observation_id': (
                    'observation:current_live_market_evidence_unavailable:test'
                ),
                'replacement_action': 'reject',
                'replacement_actionable': False,
                'plan_id': 'exec-plan-active',
                'destination_binding': destination_binding,
                'item_links': item_links,
            },
            'dispatch_reconciliation': reconciliation,
        }
    )

    assert result.payload is not None
    assert json_size_bytes(result.payload) <= SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES
    assert result.payload['revocation']['status'] == 'pending'
    assert result.payload['revocation']['destination_binding'] == destination_binding
    assert result.payload['revocation']['item_links'] == item_links
    assert result.payload['dispatch_reconciliation']['reason_code'] == (
        'ambiguous_remote_acceptance'
    )
    assert result.payload['dispatch_reconciliation']['error']['retryable'] is True


def test_pending_activation_exact_replay_evidence_is_non_evictable() -> None:
    item_links = [
        {
            'core_item_key': 'core-plan-17717:intraday_watch_buy',
            'intent': 'intraday_watch_buy',
            'side': 'buy',
            'symbol': '005930',
            'exec_plan_id': 'exec-plan-active',
            'exec_plan_item_id': 'exec-item-1',
            'remote_status': 'accepted',
        }
    ]
    requested_at = '2026-10-02T00:00:00+00:00'
    request_summary = {
        'schema_version': 'exec_dispatch_request_summary_v1',
        'plan_id': 'core-plan-active',
        'requested_at': requested_at,
        'total_item_count': 1,
        'row_item_count': 1,
        'row_items': [
            {
                'core_item_key': item_links[0]['core_item_key'],
                'decision_snapshot_id': '17717',
            }
        ],
        'metadata': {
            'dispatch_material_fingerprint': 'c' * 64,
        },
        'diagnostic_blob': 'r' * 14_000,
    }
    response_summary = {
        'schema_version': 'exec_dispatch_response_summary_v1',
        'execution_plan_id': 'exec-plan-active',
        'accepted_item_count': 1,
        'rejected_item_count': 0,
        'total_item_count': 1,
        'row_item_count': 1,
        'row_items': [
            {
                'core_item_key': item_links[0]['core_item_key'],
                'execution_plan_id': 'exec-plan-active',
                'execution_plan_item_id': 'exec-item-1',
            }
        ],
        'contract_ack': {
            'status': 'accepted',
            'dispatch_material_fingerprint': 'c' * 64,
        },
        'diagnostic_blob': 's' * 14_000,
    }
    result = slim_signal_decider_exec_submission_payload(
        {
            'mode': 'exec_dispatch',
            'exec_dispatch': {
                'plan_id': 'core-plan-active',
                'plan_fingerprint': 'a' * 64,
                'dispatch_revision_fingerprint': 'b' * 64,
                'dispatch_material_fingerprint': 'c' * 64,
                'dispatch_status': 'accepted',
                'destination_binding': {'base_url': 'https://exec.example.test'},
                'contract_proof': {
                    'schema_version': 'core_exec_dispatch_proof_v2',
                    'execution_plan_id': 'exec-plan-active',
                    'dispatch_material_fingerprint': 'c' * 64,
                    'submitted_core_item_keys': [item_links[0]['core_item_key']],
                    'core_activation': {
                        'schema_version': 'core_execution_activation_binding_v1',
                        'mode': 'core_confirmed',
                        'authority_generation_fingerprint': 'e' * 64,
                        'status': 'pending',
                        'dispatch_material_fingerprint': 'c' * 64,
                    },
                },
                'item_links': item_links,
                'last_request_summary': request_summary,
                'last_response_summary': response_summary,
            },
        }
    )

    assert result.payload is not None
    assert json_size_bytes(result.payload) <= SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES
    replay_request = result.payload['exec_dispatch']['last_request_summary']
    replay_response = result.payload['exec_dispatch']['last_response_summary']
    assert replay_request['plan_id'] == 'core-plan-active'
    assert replay_request['requested_at'] == requested_at
    assert replay_request['total_item_count'] == 1
    assert replay_request['row_item_count'] == 1
    assert replay_request['row_items'] == request_summary['row_items']
    assert replay_request['metadata'] == request_summary['metadata']
    assert replay_response['execution_plan_id'] == 'exec-plan-active'
    assert replay_response['accepted_item_count'] == 1
    assert replay_response['rejected_item_count'] == 0
    assert replay_response['total_item_count'] == 1
    assert replay_response['row_item_count'] == 1
    assert replay_response['row_items'] == response_summary['row_items']
    assert replay_response['contract_ack'] == response_summary['contract_ack']
