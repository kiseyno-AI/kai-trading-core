import asyncio
import copy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy import delete, event, select

from app.agent_context.decider_replay_service import DeciderReplayCostAssumptions, DeciderReplayRepository
from app.agent_context.model_identity import build_observed_model_identity
from app.core.settings import settings
from app.db import engine, session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    AgentContextFeedbackRun,
    BrokerOrder,
    CurrentMarketSnapshotCache,
    OrderRequest,
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    SignalDeciderPolicyVersion,
    Symbol,
)
from app.market.clock import MarketState
from app.market.session import (
    KisKrxTradingCalendarProvider,
    reset_krx_calendar_provider,
    set_krx_calendar_provider,
)
from app.orders.exec_integration import (
    CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS,
    EXEC_DECIDER_HANDOFF_MAX_BYTES,
    EXEC_ENTRY_RISK_CONTEXT_MAX_BYTES,
    EXEC_ENTRY_BUY_INTENTS,
    EXEC_EXIT_RISK_CONTEXT_MAX_BYTES,
    EXEC_INTENT_INTRADAY_WATCH_BUY,
    EXEC_INTENT_NEXT_OPEN_BUY,
    EXEC_INTENT_STOP_SELL,
    EXEC_INTENT_TARGET_SELL,
    ExecRuntimeMonitoredSymbolMembership,
    PortfolioHoldingSymbolMembership,
    SignalDeciderExecClientError,
    SignalDeciderExecIntegrationService,
    SignalDeciderExecPayloadContractError,
    _bind_core_confirmed_activation,
    _bounded_dispatch_object,
    _build_exec_destination_binding,
    _build_market_regime_sizing_policy,
    _collect_items,
    _delegated_gate_contract_error,
    _delegated_gate_codes,
    _delegated_gate_observed_at,
    _delegated_gates,
    _delta_dispatch_batch_key,
    _delta_dispatch_plan_id,
    _fingerprint_exec_plan,
    _json_safe,
    _row_readback_revision_binding,
    _row_dispatch_revision_fingerprint,
    _row_has_accepted_exec_dispatch,
)
from app.orders.execution_handoff_contract import (
    EXECUTION_PLAN_DISPATCH_MAX_BYTES,
    EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
    EXECUTION_REVOCATION_CAPABILITY,
    EXECUTION_REVOCATION_SCHEMA_VERSION,
    serialize_execution_plan_dispatch_payload,
    validate_execution_plan_dispatch_ack,
)
from app.orders.exec_payload_slimming import (
    SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES,
    cleanup_signal_decider_exec_submission_payloads,
    json_size_bytes,
    slim_signal_decider_exec_submission_payload,
)
from app.orders.next_open_execution import (
    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
    EXECUTION_STATUS_REVOCATION_PENDING,
    EXECUTION_STATUS_SUBMIT_FAILED,
    EXECUTION_STATUS_SUBMITTED,
    NextOpenAccountTarget,
    _PlanRefreshRevisionConflict,
    _load_signal_decider_policy_authority,
    _next_open_policy_fingerprint,
    _short_term_entry_quality_blocks,
    signal_decider_next_open_execution_service,
)
from app.quotes.kis_realtime_prices import (
    KISRealtimeMonitoredSymbolSource,
    KISRealtimePriceService,
)


def _runtime_monitored_membership_payload(
    symbols: list[str],
    *,
    market_date: str = '2026-04-30',
    generated_at: str = '2026-04-30T00:00:00+00:00',
    membership_complete: bool = True,
) -> dict:
    return {
        'schema_version': 'runtime_monitored_symbol_membership_v1',
        'market_date': market_date,
        'generated_at': generated_at,
        'membership_complete': membership_complete,
        'symbols': symbols,
        'membership_scope': {
            'market': 'KRX',
            'exchanges': ['KOSDAQ', 'KOSPI', 'KRX', 'NXT', 'SOR'],
        },
        'coverage': {
            'status': 'complete' if membership_complete else 'unavailable',
            'reason': None if membership_complete else 'test_coverage_unavailable',
            'provider': 'kis',
            'runtime_mode': 'mock',
            'execution_scope': {
                'provider': 'kis',
                'status': 'available' if membership_complete else 'unavailable',
                'source': 'configured_broker_account' if membership_complete else None,
                'reason': None if membership_complete else 'test_scope_unavailable',
                'exchanges': [
                    'AMEX',
                    'KOSDAQ',
                    'KOSPI',
                    'KRX',
                    'NASD',
                    'NXT',
                    'NYSE',
                    'SOR',
                ],
            },
        },
    }


class FakeExecClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        bearer_token: str | None = None,
    ) -> None:
        self.base_url = (
            settings.signal_decider_exec_base_url
            if base_url is None
            else base_url
        )
        self.bearer_token = (
            settings.signal_decider_exec_bearer_token
            if bearer_token is None
            else bearer_token
        )
        self.post_calls: list[dict] = []
        self.activation_calls: list[dict] = []
        self.revocation_calls: list[dict] = []
        self.dispatch_events: list[str] = []
        self.runtime_calls = 0
        self.runtime_membership_calls = 0
        self.plan_calls: list[dict] = []
        self.order_calls: list[dict] = []
        self.execution_calls: list[dict] = []
        self.position_calls: list[dict] = []
        self.runtime_response: dict = {'ready': True, 'mode': 'mock'}
        self.runtime_membership_response = _runtime_monitored_membership_payload([])
        self.plan_response: dict = {'items': []}
        self.order_response: dict = {'items': []}
        self.execution_response: dict = {'items': []}
        self.position_response: dict = {'items': []}
        self.execution_plan_id_override: str | None = None
        self.execution_plan_id_by_item_binding: dict[tuple[str, str], str] = {}
        self.agent_approved_quantities_by_agent: dict[str, str] = {}
        self.agent_approved_quantities_by_core_key: dict[str, str] = {}
        self.agent_sizing_payloads_by_agent: dict[str, dict] = {}
        self.agent_sizing_payloads_by_core_key: dict[str, dict] = {}

    async def get_runtime_status(self) -> dict:
        self.runtime_calls += 1
        return self.runtime_response

    async def get_runtime_monitored_symbol_membership(self) -> dict:
        self.runtime_membership_calls += 1
        return self.runtime_membership_response

    async def post_execution_plan(self, payload: dict) -> dict:
        self.dispatch_events.append('plan')
        self.post_calls.append(payload)
        execution_plan_id = self.execution_plan_id_override or payload['plan_id']
        items = []
        for index, item in enumerate(payload['items'], start=1):
            response_item = {
                'core_item_key': item['core_item_key'],
                'execution_plan_id': execution_plan_id,
                'execution_plan_item_id': f'exec-item-{index}',
                'status': 'accepted',
            }
            self.execution_plan_id_by_item_binding[
                (
                    response_item['core_item_key'],
                    response_item['execution_plan_item_id'],
                )
            ] = execution_plan_id
            if (
                item.get('intent') in EXEC_ENTRY_BUY_INTENTS
                and (item.get('quantity_policy') or {}).get('type') == 'exec_agent_sized'
            ):
                agent_sizing_payload = (
                    self.agent_sizing_payloads_by_core_key.get(item['core_item_key'])
                    or self.agent_sizing_payloads_by_agent.get(item.get('decision_agent_name'))
                )
                approved_quantity = (
                    self.agent_approved_quantities_by_core_key.get(item['core_item_key'])
                    or self.agent_approved_quantities_by_agent.get(item.get('decision_agent_name'))
                )
                if agent_sizing_payload is not None:
                    response_item['agent_sizing'] = agent_sizing_payload
                elif approved_quantity is not None:
                    response_item['agent_sizing'] = {
                        'approved': True,
                        'quantity': approved_quantity,
                    }
            items.append(response_item)
        return {
            'plan_id': payload['plan_id'],
            'execution_plan_id': execution_plan_id,
            'contract_ack': {
                'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
                'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                'capability': EXECUTION_HANDOFF_CAPABILITY,
                'plan_id': payload['plan_id'],
                'status': 'accepted',
                'dispatch_material_fingerprint': payload['metadata'][
                    'dispatch_material_fingerprint'
                ],
            },
            'accepted_item_count': len(items),
            'rejected_item_count': 0,
            'item_acks': items,
        }

    async def post_execution_plan_activation(self, payload: dict) -> dict:
        self.dispatch_events.append('activation')
        self.activation_calls.append(payload)
        return {
            'plan_id': payload['plan_id'],
            'execution_plan_id': payload['execution_plan_id'],
            'contract_ack': {
                'schema_version': 'execution_handoff_activation_v1',
                'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                'capability': 'core_confirmed_activation_v1',
                'plan_id': payload['plan_id'],
                'status': 'activated',
                'dispatch_material_fingerprint': payload[
                    'dispatch_material_fingerprint'
                ],
                'authority_generation_fingerprint': payload[
                    'authority_generation_fingerprint'
                ],
            },
            'item_acks': [
                {
                    **item,
                    'status': 'activated',
                    'execution_plan_id': payload['execution_plan_id'],
                }
                for item in payload['items']
            ],
            'activated_item_count': len(payload['items']),
            'idempotent_replay': False,
        }

    async def post_execution_plan_revocation(self, payload: dict) -> dict:
        self.dispatch_events.append('revocation')
        self.revocation_calls.append(payload)
        fallback_execution_plan_id = (
            payload['plan_id']
            if all(
                str(item.get('execution_plan_item_id') or '').startswith('exec-item-')
                for item in payload['items']
            )
            else 'exec-old-plan'
        )
        execution_plan_ids = {
            self.execution_plan_id_by_item_binding.get(
                (
                    str(item.get('core_item_key') or ''),
                    str(item.get('execution_plan_item_id') or ''),
                ),
                fallback_execution_plan_id,
            )
            for item in payload['items']
        }
        execution_plan_id = (
            next(iter(execution_plan_ids))
            if len(execution_plan_ids) == 1
            else 'exec-old-plan'
        )
        return {
            'plan_id': payload['plan_id'],
            'execution_plan_id': execution_plan_id,
            'contract_ack': {
                'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
                'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                'capability': EXECUTION_REVOCATION_CAPABILITY,
                'plan_id': payload['plan_id'],
                'status': 'revoked',
            },
            'item_acks': [
                {
                    'core_item_key': item['core_item_key'],
                    'status': 'revoked',
                    'execution_plan_id': execution_plan_id,
                    'execution_plan_item_id': item['execution_plan_item_id'],
                }
                for item in payload['items']
            ],
            'reason_code': payload['reason_code'],
            'revoked_item_count': len(payload['items']),
            'manual_review_item_count': 0,
            'preserved_item_count': 0,
        }

    async def list_execution_plans(self, *, external_plan_id: str | None = None) -> dict:
        self.plan_calls.append({'external_plan_id': external_plan_id})
        return self.plan_response

    async def list_orders(self, *, external_plan_id: str | None = None, symbols=None) -> dict:
        self.order_calls.append({'external_plan_id': external_plan_id, 'symbols': symbols})
        return self.order_response

    async def list_executions(self, *, external_plan_id: str | None = None, symbols=None) -> dict:
        self.execution_calls.append({'external_plan_id': external_plan_id, 'symbols': symbols})
        return self.execution_response

    async def list_positions(
        self,
        *,
        external_plan_id: str,
        execution_plan_id: str,
        symbols=None,
    ) -> dict:
        self.position_calls.append(
            {
                'external_plan_id': external_plan_id,
                'execution_plan_id': execution_plan_id,
                'symbols': symbols,
            }
        )
        payload = copy.deepcopy(self.position_response)
        items = _collect_items(payload)
        for item in items:
            item.setdefault('provider', 'kis')
            item.setdefault('account_scope', 'a' * 24)
            item.setdefault('exchange', 'KRX')
            item.setdefault(
                'position_scope_proof',
                {
                    'schema_version': 'execution_position_plan_scope_v1',
                    'plan_id': external_plan_id,
                    'execution_plan_id': execution_plan_id,
                    'provider': item['provider'],
                    'account_scope': item['account_scope'],
                    'exchange': item['exchange'],
                },
            )
        return {**payload, 'items': items}


def test_exec_payload_json_normalization_removes_nonfinite_values() -> None:
    normalized = _json_safe(
        {
            'decimal_nan': Decimal('NaN'),
            'decimal_infinity': Decimal('Infinity'),
            'float_nan': float('nan'),
            'float_infinity': float('inf'),
        }
    )

    assert normalized == {
        'decimal_nan': None,
        'decimal_infinity': None,
        'float_nan': None,
        'float_infinity': None,
    }
    json.dumps(normalized, allow_nan=False)


def test_bounded_dispatch_projection_never_drops_late_essential_keys() -> None:
    payload = {
        **{f'nonessential_{index}': f'value-{index}' for index in range(30)},
        'delegated_gate_codes': ['delegated_live_cost_gate'],
        'delegated_gate_confirmation_required': True,
        'decider_handoff_context_digest': 'd' * 64,
        'oversized_diagnostic': 'x' * 8_000,
    }

    projected = _bounded_dispatch_object(
        payload,
        max_bytes=2 * 1024,
        label='test_entry_risk_context',
        essential_keys=(
            'delegated_gate_codes',
            'delegated_gate_confirmation_required',
            'decider_handoff_context_digest',
        ),
    )

    assert projected['delegated_gate_codes'] == ['delegated_live_cost_gate']
    assert projected['delegated_gate_confirmation_required'] is True
    assert projected['decider_handoff_context_digest'] == 'd' * 64
    assert len(json.dumps(projected, separators=(',', ':')).encode('utf-8')) <= 2 * 1024


def test_top_level_exec_plan_list_preserves_current_item_statuses() -> None:
    statuses = (
        'watch_monitoring_active',
        'watch_sizing_pending',
        'watch_buy_triggered_blocked',
    )
    plans_result = {
        'status': 'ok',
        'payload': [
            {
                'execution_plan_id': 'exec-plan-1',
                'external_plan_id': 'core-plan-1',
                'status': 'accepted',
                'item_states': [
                    {
                        'core_item_key': f'core-item-{index}',
                        'execution_plan_item_id': f'exec-item-{index}',
                        'status': status,
                    }
                    for index, status in enumerate(statuses, start=1)
                ],
            }
        ],
    }

    items = SignalDeciderExecIntegrationService._collect_exec_plan_readback_items(
        plans_result=plans_result
    )

    assert [item['status'] for item in items] == list(statuses)
    assert {item['execution_plan_id'] for item in items} == {'exec-plan-1'}
    assert {item['external_plan_id'] for item in items} == {'core-plan-1'}


def test_exact_current_exec_state_replaces_stale_agent_rejection() -> None:
    statuses = (
        'watch_monitoring_active',
        'watch_sizing_pending',
        'watch_buy_triggered_blocked',
    )
    for status in statuses:
        merged_links, identity_mismatches = (
            SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
                item_links=[
                    {
                        'core_item_key': 'core-item-1',
                        'exec_plan_id': 'exec-plan-1',
                        'exec_plan_item_id': 'exec-item-1',
                        'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                        'remote_status': 'exec_agent_rejected',
                        'remote_status_source': 'exec_item_state',
                        'current_state': {
                            'core_item_key': 'core-item-1',
                            'execution_plan_id': 'exec-plan-1',
                            'execution_plan_item_id': 'exec-item-1',
                            'status': 'exec_agent_rejected',
                        },
                    }
                ],
                plans_result={
                    'status': 'ok',
                    'payload': [
                        {
                            'execution_plan_id': 'exec-plan-1',
                            'external_plan_id': 'core-plan-1',
                            'status': 'accepted',
                            'item_states': [
                                {
                                    'core_item_key': 'core-item-1',
                                    'execution_plan_item_id': 'exec-item-1',
                                    'status': status,
                                }
                            ],
                        }
                    ],
                },
            )
        )

        assert identity_mismatches == [], status
        assert merged_links[0]['remote_status'] == status
        assert merged_links[0]['current_state']['status'] == status


def test_partial_top_level_exec_state_cannot_replace_exact_bound_state() -> None:
    original_link = {
        'core_item_key': 'core-item-1',
        'exec_plan_id': 'exec-plan-1',
        'exec_plan_item_id': 'exec-item-1',
        'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
        'remote_status': 'exec_agent_rejected',
        'remote_status_source': 'exec_item_state',
        'current_state': {
            'core_item_key': 'core-item-1',
            'execution_plan_id': 'exec-plan-1',
            'execution_plan_item_id': 'exec-item-1',
            'status': 'exec_agent_rejected',
        },
    }

    merged_links, identity_mismatches = (
        SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
            item_links=[original_link],
            plans_result={
                'status': 'ok',
                'payload': [
                    {
                        'execution_plan_id': 'exec-plan-1',
                        'external_plan_id': 'core-plan-1',
                        'status': 'accepted',
                        'item_states': [
                            {
                                'core_item_key': 'core-item-1',
                                'status': 'watch_monitoring_active',
                            }
                        ],
                    }
                ],
            },
        )
    )

    assert identity_mismatches[0]['reason_code'] == (
        'exec_readback_identity_mismatch'
    )
    assert merged_links[0]['remote_status'] == 'exec_agent_rejected'
    assert merged_links[0]['current_state']['status'] == (
        'exec_agent_rejected'
    )


def test_canonical_exec_plan_id_mismatch_cannot_match_external_alias() -> None:
    rejected_link = {
        'core_item_key': 'core-item-1',
        'exec_plan_id': 'exec-plan-a',
        'exec_plan_item_id': 'exec-item-1',
        'remote_status': 'exec_agent_rejected',
        'remote_status_source': 'exec_item_state',
        'current_state': {
            'core_item_key': 'core-item-1',
            'execution_plan_id': 'exec-plan-a',
            'execution_plan_item_id': 'exec-item-1',
            'status': 'exec_agent_rejected',
            'terminal': True,
        },
    }

    merged_links, identity_mismatches = (
        SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
            item_links=[rejected_link],
            plans_result={
                'status': 'ok',
                'payload': [
                    {
                        'execution_plan_id': 'exec-plan-b',
                        'external_plan_id': 'exec-plan-a',
                        'status': 'running',
                        'item_states': [
                            {
                                'core_item_key': 'core-item-1',
                                'execution_plan_item_id': 'exec-item-1',
                                'status': 'watch_monitoring_active',
                            }
                        ],
                    }
                ],
            },
        )
    )

    assert identity_mismatches[0]['reason_code'] == (
        'exec_readback_identity_mismatch'
    )
    assert merged_links[0]['remote_status'] == 'exec_agent_rejected'
    assert merged_links[0]['current_state']['status'] == (
        'exec_agent_rejected'
    )


def test_parent_canonical_plan_id_rejects_conflicting_child_legacy_alias() -> None:
    rejected_link = {
        'core_item_key': 'core-item-1',
        'exec_plan_id': 'exec-plan-a',
        'exec_plan_item_id': 'exec-item-1',
        'remote_status': 'exec_agent_rejected',
        'remote_status_source': 'exec_item_state',
        'current_state': {
            'core_item_key': 'core-item-1',
            'execution_plan_id': 'exec-plan-a',
            'execution_plan_item_id': 'exec-item-1',
            'status': 'exec_agent_rejected',
            'terminal': True,
        },
    }

    merged_links, identity_mismatches = (
        SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
            item_links=[rejected_link],
            plans_result={
                'status': 'ok',
                'payload': [
                    {
                        'execution_plan_id': 'exec-plan-b',
                        'external_plan_id': 'exec-plan-a',
                        'status': 'running',
                        'item_states': [
                            {
                                'core_item_key': 'core-item-1',
                                'exec_plan_id': 'exec-plan-a',
                                'execution_plan_item_id': 'exec-item-1',
                                'status': 'watch_monitoring_active',
                            }
                        ],
                    }
                ],
            },
        )
    )

    assert identity_mismatches[0]['reason_code'] == (
        'exec_readback_identity_mismatch'
    )
    assert merged_links[0]['remote_status'] == 'exec_agent_rejected'
    assert merged_links[0]['current_state']['status'] == (
        'exec_agent_rejected'
    )


def test_top_level_exec_plan_list_uses_parent_state_when_items_are_omitted() -> None:
    merged_links, identity_mismatches = (
        SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
            item_links=[
                {
                    'core_item_key': 'core-item-1',
                    'exec_plan_id': 'exec-plan-1',
                    'exec_plan_item_id': 'exec-item-1',
                    'remote_status': 'accepted',
                    'remote_status_source': 'dispatch_ack',
                }
            ],
            plans_result={
                'status': 'ok',
                'payload': [
                    {
                        'execution_plan_id': 'exec-plan-1',
                        'status': 'running',
                    }
                ],
            },
        )
    )

    assert identity_mismatches == []
    assert merged_links[0]['remote_status'] == 'running'
    assert merged_links[0]['current_state'] == {
        'core_item_key': 'core-item-1',
        'execution_plan_id': 'exec-plan-1',
        'execution_plan_item_id': 'exec-item-1',
        'status': 'running',
        'terminal': False,
    }


def test_parent_plan_state_cannot_reopen_exact_agent_rejection() -> None:
    merged_links, identity_mismatches = (
        SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
            item_links=[
                {
                    'core_item_key': 'core-item-1',
                    'exec_plan_id': 'exec-plan-1',
                    'exec_plan_item_id': 'exec-item-1',
                    'remote_status': 'exec_agent_rejected',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'core_item_key': 'core-item-1',
                        'execution_plan_id': 'exec-plan-1',
                        'execution_plan_item_id': 'exec-item-1',
                        'status': 'exec_agent_rejected',
                        'terminal': True,
                    },
                }
            ],
            plans_result={
                'status': 'ok',
                'payload': [
                    {
                        'execution_plan_id': 'exec-plan-1',
                        'status': 'running',
                    }
                ],
            },
        )
    )

    assert identity_mismatches == []
    assert merged_links[0]['remote_status'] == 'exec_agent_rejected'
    assert merged_links[0]['current_state']['status'] == (
        'exec_agent_rejected'
    )


def test_parent_state_projected_through_item_ack_cannot_reopen_rejection() -> None:
    rejected_link = {
        'core_item_key': 'core-item-1',
        'exec_plan_id': 'exec-plan-1',
        'exec_plan_item_id': 'exec-item-1',
        'remote_status': 'exec_agent_rejected',
        'remote_status_source': 'exec_item_state',
        'current_state': {
            'core_item_key': 'core-item-1',
            'execution_plan_id': 'exec-plan-1',
            'execution_plan_item_id': 'exec-item-1',
            'status': 'exec_agent_rejected',
            'terminal': True,
        },
    }

    merged_links, identity_mismatches = (
        SignalDeciderExecIntegrationService._merge_exec_plan_readback_links(
            item_links=[rejected_link],
            plans_result={
                'status': 'ok',
                'payload': [
                    {
                        'execution_plan_id': 'exec-plan-1',
                        'status': 'running',
                        'item_acks': [
                            {
                                'core_item_key': 'core-item-1',
                                'execution_plan_id': 'exec-plan-1',
                                'execution_plan_item_id': 'exec-item-1',
                                'status': 'accepted',
                            }
                        ],
                    }
                ],
            },
        )
    )

    assert identity_mismatches == []
    assert merged_links[0]['remote_status'] == 'exec_agent_rejected'
    assert merged_links[0]['current_state']['status'] == (
        'exec_agent_rejected'
    )


def test_bounded_dispatch_projection_fails_closed_when_essentials_exceed_budget() -> None:
    essential_keys = tuple(f'identity_{index}' for index in range(8))
    payload = {key: 'x' * 2_000 for key in essential_keys}

    with unittest.TestCase().assertRaisesRegex(
        SignalDeciderExecPayloadContractError,
        'exceeds bounded projection budget',
    ):
        _bounded_dispatch_object(
            payload,
            max_bytes=2 * 1024,
            label='test_protective_exit_risk_context',
            essential_keys=essential_keys,
        )


def test_bounded_dispatch_projection_digests_nested_essential_contexts() -> None:
    def wide_context(prefix: str) -> dict[str, dict[str, str]]:
        return {
            f'{prefix}_section_{section_index}': {
                f'field_{field_index}': (
                    f'{prefix}-{section_index}-{field_index}-' + ('x' * 600)
                )
                for field_index in range(24)
            }
            for section_index in range(8)
        }

    payload = {
        'schema_version': 'exec_agent_decider_handoff_v1',
        'classification': 'delegated_for_exec_agent_sizing',
        'symbol': '005930',
        'target_trade_date': '2026-10-06',
        'source_market_date': '2026-10-02',
        'traceability': wide_context('traceability'),
        'decider_input_context': wide_context('input'),
        'market_regime_context': wide_context('regime'),
        'decision_input_quality_context': wide_context('quality'),
        'decider_output': wide_context('output'),
        'eod_pre_decider_enrichment_metadata': wide_context('enrichment'),
    }

    projected = _bounded_dispatch_object(
        payload,
        max_bytes=12 * 1024,
        label='exec_decider_handoff_context',
        essential_keys=tuple(payload),
        compact_nested_keys=(
            'traceability',
            'decider_input_context',
            'market_regime_context',
            'decision_input_quality_context',
            'decider_output',
            'eod_pre_decider_enrichment_metadata',
        ),
    )

    assert projected['schema_version'] == 'exec_agent_decider_handoff_v1'
    assert projected['classification'] == 'delegated_for_exec_agent_sizing'
    assert projected['symbol'] == '005930'
    assert projected['target_trade_date'] == '2026-10-06'
    assert projected['source_market_date'] == '2026-10-02'
    assert projected['dispatch_projection']['source_size_bytes'] > 12 * 1024
    assert len(json.dumps(projected, separators=(',', ':')).encode('utf-8')) <= 12 * 1024
    assert projected['decider_input_context']['input_section_0']['projection_digest']


def test_bounded_dispatch_projection_preserves_prebounded_essential_context() -> None:
    flow_context = {
        'investor_flow': {
            'symbol_flow': {
                'investor_groups': {
                    'foreign': {'net_value_krw': 1_250_000_000},
                },
            },
        },
        'short_selling': {
            'short_volume': {'short_volume_ratio_pct': 2.1},
        },
        'sector_peers': {
            'peers': [
                {
                    'symbol': f'{index:06d}',
                    'change_pct': 1.8,
                    'provider_evidence': 'e' * 240,
                }
                for index in range(16)
            ],
        },
    }
    payload = {
        'schema_version': 'entry_risk_v1',
        'flow_short_sector_context': flow_context,
        'advisory_context': {
            f'section_{index}': {'detail': 'x' * 1_000}
            for index in range(24)
        },
    }

    projected = _bounded_dispatch_object(
        payload,
        max_bytes=12 * 1024,
        label='entry_risk_context',
        essential_keys=tuple(payload),
        compact_nested_keys=('advisory_context',),
        preserve_bounded_keys=('flow_short_sector_context',),
    )

    assert projected['flow_short_sector_context'] == flow_context
    assert 'projection_digest' not in json.dumps(
        projected['flow_short_sector_context']
    )
    assert len(json.dumps(projected, separators=(',', ':')).encode('utf-8')) <= 12 * 1024


def test_v4_dispatch_contract_requires_exact_schema_and_capability() -> None:
    payload = {
        'plan_id': 'core-contract-test',
        'source': 'kai-trading-core',
        'mode': 'mock',
        'market_date': '2026-09-10',
        'strategy_run_id': 'run-1',
        'requested_at': '2026-09-09T12:00:00+00:00',
        'items': [
            {
                'core_item_key': 'core-plan-1:next_open_buy',
                'source_plan_item_id': 'core-plan-1:next_open_buy',
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
                'side': 'buy',
                'intent': 'next_open_buy',
                'quantity_policy': {'type': 'exec_agent_sized'},
                'limit_price': None,
                'trigger_price': None,
                'time_in_force': 'day',
                'risk_context': {},
                'decision_snapshot_id': '101',
            }
        ],
        'metadata': {
            'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'required_capability': EXECUTION_HANDOFF_CAPABILITY,
            'agent_name': 'signal-decider',
            'decision_agent_name': 'signal-decider',
            'trigger': 'test',
            'requested_at': '2026-09-09T12:00:00+00:00',
            'account_reference': 'KIS:00000000-00',
            'account_label': 'TEST',
            'source_market_date': '2026-09-09',
            'target_trade_date': '2026-09-10',
            'dispatch_batch_type': 'initial',
            'dispatch_batch_key': None,
        },
    }
    entry = payload['items'][0]
    for intent, trigger_price in (('target_sell', 54000), ('stop_sell', 47000)):
        payload['items'].append(
            {
                **entry,
                'core_item_key': f'core-plan-1:{intent}',
                'source_plan_item_id': f'core-plan-1:{intent}',
                'side': 'sell',
                'intent': intent,
                'quantity_policy': {
                    'type': 'position_exit_from_entry',
                    'linked_entry_item_key': entry['core_item_key'],
                },
                'trigger_price': trigger_price,
            }
        )

    serialized = serialize_execution_plan_dispatch_payload(payload)
    assert {
        **serialized,
        'metadata': {
            **serialized['metadata'],
            'dispatch_material_fingerprint': None,
        },
    } == {
        **payload,
        'metadata': {
            **payload['metadata'],
            'dispatch_material_fingerprint': None,
        },
    }
    assert len(serialized['metadata']['dispatch_material_fingerprint']) == 64
    assert set(serialized['metadata']['dispatch_material_fingerprint']) <= set(
        '0123456789abcdef'
    )
    unsupported_us = json.loads(json.dumps(payload))
    for item in unsupported_us['items']:
        item.update(
            {
                'symbol': 'AAPL',
                'asset_class': 'us_equity',
                'market': 'US',
                'exchange': 'NASD',
                'currency': 'USD',
                'session_policy': {
                    'session': 'regular',
                    'timezone': 'America/New_York',
                    'allow_extended_hours': False,
                    'whole_shares_only': True,
                },
            }
        )
    with unittest.TestCase().assertRaisesRegex(
        ValueError,
        'currently supports KIS/KRX/KRW only',
    ):
        serialize_execution_plan_dispatch_payload(unsupported_us)
    missing_protection = json.loads(json.dumps(payload))
    missing_protection['items'] = missing_protection['items'][:1]
    with unittest.TestCase().assertRaises(ValueError):
        serialize_execution_plan_dispatch_payload(missing_protection)
    for field, value in (
        ('schema_version', 'signal_decider_exec_plan_v3'),
        ('contract_version', 'execution_handoff_contract_v3'),
        ('required_capability', 'legacy'),
    ):
        malformed = json.loads(json.dumps(payload))
        malformed['metadata'][field] = value
        with unittest.TestCase().assertRaises(ValueError):
            serialize_execution_plan_dispatch_payload(malformed)


def test_v4_dispatch_ack_requires_exact_one_to_one_items_and_durable_ids() -> None:
    keys = ['core-plan-1:next_open_buy', 'core-plan-1:target_sell']
    dispatch_material_fingerprint = 'a' * 64
    valid = {
        'plan_id': 'plan-1',
        'execution_plan_id': 'exec-plan-1',
        'accepted_item_count': 2,
        'rejected_item_count': 0,
        'contract_ack': {
            'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'capability': EXECUTION_HANDOFF_CAPABILITY,
            'plan_id': 'plan-1',
            'status': 'accepted',
            'dispatch_material_fingerprint': dispatch_material_fingerprint,
        },
        'item_acks': [
            {
                'core_item_key': key,
                'status': 'accepted',
                'execution_plan_id': 'exec-plan-1',
                'execution_plan_item_id': f'exec-item-{index}',
            }
            for index, key in enumerate(keys, start=1)
        ],
    }
    assert set(
        validate_execution_plan_dispatch_ack(
            valid,
            plan_id='plan-1',
            submitted_core_item_keys=keys,
            expected_dispatch_material_fingerprint=dispatch_material_fingerprint,
        )
    ) == set(keys)

    malformed_variants = [
        {},
        {**valid, 'contract_ack': {**valid['contract_ack'], 'contract_version': 'v3'}},
        {**valid, 'item_acks': valid['item_acks'][:1]},
        {**valid, 'item_acks': [valid['item_acks'][0], valid['item_acks'][0]]},
        {
            **valid,
            'item_acks': [
                {**item, 'execution_plan_item_id': 'exec-item-duplicate'}
                for item in valid['item_acks']
            ],
        },
        {
            **valid,
            'item_acks': [
                {**item, 'execution_plan_item_id': None}
                for item in valid['item_acks']
            ],
        },
        {
            **valid,
            'contract_ack': {**valid['contract_ack'], 'status': 'partial'},
            'accepted_item_count': 1,
            'rejected_item_count': 1,
            'item_acks': [
                valid['item_acks'][0],
                {
                    **valid['item_acks'][1],
                    'status': 'rejected',
                    'execution_plan_id': None,
                    'execution_plan_item_id': None,
                    'reason_code': 'invalid_item',
                },
            ],
        },
        {key: value for key, value in valid.items() if key != 'accepted_item_count'},
    ]
    for malformed in malformed_variants:
        with unittest.TestCase().assertRaises(ValueError):
            validate_execution_plan_dispatch_ack(
                malformed,
                plan_id='plan-1',
                submitted_core_item_keys=keys,
                expected_dispatch_material_fingerprint=dispatch_material_fingerprint,
            )


def test_ambiguous_dispatch_strict_readback_recovers_equivalent_top_level_list_ack() -> None:
    plan_id = 'core-plan-1'
    execution_plan_id = 'exec-plan-1'
    core_item_key = 'core-plan-1:next_open_buy'
    dispatch_material_fingerprint = 'a' * 64
    destination_binding = _build_exec_destination_binding(
        base_url='http://127.0.0.1:8010',
        runtime_mode='paper',
        dispatch_source='core',
        account_reference='KIS:00000000-00',
    )
    exec_dispatch = {
        'plan_id': plan_id,
        'dispatch_material_fingerprint': dispatch_material_fingerprint,
        'destination_binding': destination_binding,
        'last_request_summary': {
            'metadata': {
                'dispatch_material_fingerprint': (
                    dispatch_material_fingerprint
                ),
            },
        },
        'item_links': [{'core_item_key': core_item_key}],
    }
    remote_plan = {
        'plan_id': plan_id,
        'external_plan_id': plan_id,
        'execution_plan_id': execution_plan_id,
        'accepted_item_count': 1,
        'rejected_item_count': 0,
        'contract_ack': {
            'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'capability': EXECUTION_HANDOFF_CAPABILITY,
            'plan_id': plan_id,
            'status': 'accepted',
            'dispatch_material_fingerprint': dispatch_material_fingerprint,
        },
        'item_acks': [
            {
                'core_item_key': core_item_key,
                'execution_plan_id': execution_plan_id,
                'execution_plan_item_id': 'exec-item-1',
                'status': 'accepted',
            }
        ],
    }

    wrapped = (
        SignalDeciderExecIntegrationService
        ._bind_ambiguous_dispatch_from_strict_readback_ack(
            exec_dispatch=exec_dispatch,
            plans_result={'payload': {'items': [remote_plan]}},
        )
    )
    top_level = (
        SignalDeciderExecIntegrationService
        ._bind_ambiguous_dispatch_from_strict_readback_ack(
            exec_dispatch=exec_dispatch,
            plans_result={'payload': [remote_plan]},
        )
    )

    assert wrapped is not None
    assert top_level == wrapped


def test_exec_payload_delegation_codes_are_allowlisted() -> None:
    validation = {
        'warnings': [
            {'code': 'short_term_liquidity_floor_block_delegated_to_exec_watch'},
            {'code': 'short_term_reward_risk_ratio_block_delegated_to_exec_watch'},
            {'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch'},
            {'code': 'short_term_buy_quality_score_block_delegated_to_exec_watch'},
        ]
    }

    assert _delegated_gate_codes(
        validation,
        decision_agent_name='short-term-decider',
    ) == [
        'short_term_liquidity_floor_block_delegated_to_exec_watch',
        'short_term_reward_risk_ratio_block_delegated_to_exec_watch',
    ]
    assert _delegated_gate_codes(
        validation,
        decision_agent_name='signal-decider',
    ) == [
        'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
    ]


def test_delegated_gates_reject_naive_future_and_expired_observation_windows() -> None:
    now = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
    row = SimpleNamespace(target_trade_date=date(2026, 4, 30))
    validation = {
        'short_term_entry_quality': {
            'raw_trading_value_krw': 2_500_000_000,
        }
    }
    code = 'short_term_liquidity_floor_block_delegated_to_exec_watch'

    assert _delegated_gate_observed_at(
        SimpleNamespace(feedback_received_at=datetime(2026, 4, 29, 15, 0))
    ) is None
    for observed_at, evaluated_at in (
        (now + timedelta(seconds=1), now),
        (now - timedelta(days=1), datetime(2026, 4, 30, 6, 30, tzinfo=timezone.utc)),
        (None, now),
    ):
        assert _delegated_gates(
            row=row,
            snapshot=SimpleNamespace(feedback_received_at=observed_at),
            validation=validation,
            codes=[code],
            now=evaluated_at,
        ) == []


def test_delegated_gate_contract_requires_exact_allowlisted_nine_field_binding() -> None:
    now = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
    code = 'short_term_liquidity_floor_block_delegated_to_exec_watch'
    gates = _delegated_gates(
        row=SimpleNamespace(target_trade_date=date(2026, 4, 30)),
        snapshot=SimpleNamespace(
            feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc)
        ),
        validation={
            'short_term_entry_quality': {
                'raw_trading_value_krw': 2_500_000_000,
            }
        },
        codes=[code],
        now=now,
    )

    assert _delegated_gate_contract_error(
        decision_agent_name='short-term-decider',
        codes=[code],
        confirmation_required=True,
        gates=gates,
        now=now,
    ) is None
    malformed_variants = (
        {'codes': [code], 'confirmation_required': True, 'gates': []},
        {'codes': [], 'confirmation_required': True, 'gates': gates},
        {
            'codes': [code],
            'confirmation_required': True,
            'gates': [{key: value for key, value in gates[0].items() if key != 'expires_at'}],
        },
        {
            'codes': [code],
            'confirmation_required': True,
            'gates': [{**gates[0], 'unexpected': 'authority'}],
        },
        {
            'codes': ['short_term_buy_quality_score_block_delegated_to_exec_watch'],
            'confirmation_required': True,
            'gates': [{**gates[0], 'code': 'short_term_buy_quality_score_block_delegated_to_exec_watch'}],
        },
    )
    for variant in malformed_variants:
        assert _delegated_gate_contract_error(
            decision_agent_name='short-term-decider',
            now=now,
            **variant,
        ) is not None


class FailingRuntimeExecClient(FakeExecClient):
    async def get_runtime_status(self) -> dict:
        self.runtime_calls += 1
        raise SignalDeciderExecClientError(
            'exec transport failure',
            code='unavailable',
            retryable=True,
        )


def _accepted_plan_readback_for_payload(payload: dict) -> dict:
    return {
        'items': [
            {
                'external_plan_id': payload['plan_id'],
                'plan_id': payload['plan_id'],
                'execution_plan_id': payload['plan_id'],
                'status': 'accepted',
                'accepted_item_count': len(payload['items']),
                'rejected_item_count': 0,
                'contract_ack': {
                    'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'capability': EXECUTION_HANDOFF_CAPABILITY,
                    'plan_id': payload['plan_id'],
                    'status': 'accepted',
                    'dispatch_material_fingerprint': payload['metadata'][
                        'dispatch_material_fingerprint'
                    ],
                },
                'item_acks': [
                    {
                        'core_item_key': item['core_item_key'],
                        'execution_plan_id': payload['plan_id'],
                        'execution_plan_item_id': f"exec-item-{index}",
                        'status': 'accepted',
                    }
                    for index, item in enumerate(payload['items'], start=1)
                ],
            }
        ]
    }


class TimeoutAfterAcceptExecClient(FakeExecClient):
    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        self.plan_response = _accepted_plan_readback_for_payload(payload)
        raise SignalDeciderExecClientError(
            'exec request timed out',
            code='unavailable',
            retryable=True,
        )


class DuplicateConflictAfterAcceptExecClient(FakeExecClient):
    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        self.plan_response = _accepted_plan_readback_for_payload(payload)
        raise SignalDeciderExecClientError(
            'exec plan already exists',
            code='failed',
            retryable=False,
            status_code=409,
        )


class TimeoutWithStaleSamePlanReadbackExecClient(FakeExecClient):
    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        stale_items = []
        for index, item in enumerate(payload['items'], start=1):
            stale_item = {
                'execution_plan_id': payload['plan_id'],
                'execution_plan_item_id': f"stale-exec-item-{index}",
                'symbol': item['symbol'],
                'intent': item['intent'],
                'status': 'accepted',
            }
            if index % 2 == 0:
                stale_item['core_item_key'] = f"stale-{item['core_item_key']}"
            stale_items.append(stale_item)
        self.plan_response = {
            'items': [
                {
                    'external_plan_id': payload['plan_id'],
                    'status': 'accepted',
                    'items': stale_items,
                }
            ]
        }
        raise SignalDeciderExecClientError(
            'exec request timed out',
            code='unavailable',
            retryable=True,
        )


class FakeCalendarClient:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls: list[dict] = []

    async def fetch_krx_holiday_calendar(self, **kwargs):
        self.calls.append(kwargs)
        return {'output': list(self.rows)}


class FailingCalendarClient:
    async def fetch_krx_holiday_calendar(self, **_kwargs):
        raise RuntimeError('kis calendar unavailable')


class MarketRegimeSizingPolicyTests(unittest.TestCase):
    def test_preserves_zero_quantity_multiplier(self) -> None:
        for recommended_sizing_multiplier in (0, 1):
            with self.subTest(recommended_sizing_multiplier=recommended_sizing_multiplier):
                policy = _build_market_regime_sizing_policy(
                    {
                        'market_regime': 'defensive',
                        'quantity_multiplier': 0,
                        'recommended_sizing_multiplier': recommended_sizing_multiplier,
                    }
                )

                self.assertEqual(policy['quantity_multiplier'], 0)
                self.assertEqual(policy['raw_quantity_multiplier'], 0)

    def test_preserves_zero_recommended_sizing_multiplier_when_quantity_missing(self) -> None:
        policy = _build_market_regime_sizing_policy(
            {
                'market_regime': 'defensive',
                'recommended_sizing_multiplier': 0,
            }
        )

        self.assertEqual(policy['quantity_multiplier'], 0)
        self.assertEqual(policy['raw_quantity_multiplier'], 0)

    def test_defaults_missing_or_none_multiplier_hints_to_one(self) -> None:
        for context in (
            {'market_regime': 'normal'},
            {
                'market_regime': 'normal',
                'quantity_multiplier': None,
                'recommended_sizing_multiplier': None,
            },
        ):
            with self.subTest(context=context):
                policy = _build_market_regime_sizing_policy(context)

                self.assertEqual(policy['quantity_multiplier'], 1)
                self.assertEqual(policy['raw_quantity_multiplier'], 1)

    def test_uncalibrated_aggressive_context_cannot_enable_live_upsize(self) -> None:
        policy = _build_market_regime_sizing_policy(
            {
                'schema_version': 'market_regime_context_v1',
                'market_regime': 'aggressive_open',
                'regime_confidence': 0.84,
                'regime_reasons': ['breadth_strong', 'early_continuation_confirmed'],
                'quantity_multiplier': 3.5,
                'entry_priority_boost': True,
                'live_upsize_status': 'shadow_only_uncalibrated',
                'valid_until': '2026-04-30T09:20:00+09:00',
                'sizing_window': {
                    'name': 'aggressive_open',
                    'timezone': 'Asia/Seoul',
                    'valid_from': '2026-04-30T09:00:00+09:00',
                    'valid_until': '2026-04-30T09:20:00+09:00',
                    'quantity_multiplier_cap': 2.0,
                    'entry_priority_boost': True,
                    'exec_enforced': True,
                },
            }
        )

        self.assertEqual(policy['market_regime'], 'aggressive_open')
        self.assertEqual(policy['quantity_multiplier'], 1)
        self.assertEqual(policy['raw_quantity_multiplier'], 3.5)
        self.assertEqual(policy['quantity_multiplier_cap'], 1)
        self.assertEqual(policy['valid_until'], '2026-04-30T09:20:00+09:00')
        self.assertEqual(policy['sizing_window']['valid_from'], '2026-04-30T09:00:00+09:00')
        self.assertEqual(policy['sizing_window']['quantity_multiplier_cap'], 1)
        self.assertFalse(policy['sizing_window']['entry_priority_boost'])
        self.assertFalse(policy['entry_priority_boost'])
        self.assertEqual(policy['live_upsize_status'], 'shadow_only_uncalibrated')
        self.assertTrue(policy['exec_must_enforce_window'])
        self.assertFalse(policy['core_bypasses_safety_gates'])
        self.assertEqual(
            set(policy['required_exec_gates']),
            {'kill_switch', 'account_drawdown_gate', 'symbol_caps', 'max_notional_caps', 'stop_loss_required'},
        )


class ExecIntegrationDashboardCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_payload_build_is_singleflight_per_cache_key(self) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 8, 5)
        refresh_state = {'requested': False, 'status': 'idle'}
        build_calls = 0

        async def build() -> dict:
            nonlocal build_calls
            build_calls += 1
            await asyncio.sleep(0.01)
            payload = {
                'request': {'market_date': market_date.isoformat(), 'refresh': False},
                'freshness': {'status': 'fresh', 'is_stale': False, 'is_partial': False},
                'refresh': refresh_state,
                'items': [{'symbol': '005930'}],
            }
            service._store_dashboard_cached_payload(
                endpoint='monitored_symbols',
                market_date=market_date,
                symbol=None,
                payload=payload,
            )
            return payload

        first, second = await asyncio.gather(
            service._build_dashboard_payload_singleflight(
                endpoint='monitored_symbols',
                market_date=market_date,
                symbol=None,
                refresh_state=refresh_state,
                build=build,
            ),
            service._build_dashboard_payload_singleflight(
                endpoint='monitored_symbols',
                market_date=market_date,
                symbol=None,
                refresh_state=refresh_state,
                build=build,
            ),
        )

        self.assertEqual(build_calls, 1)
        self.assertEqual(first['items'], [{'symbol': '005930'}])
        self.assertEqual(second['freshness']['served_from_cache'], True)

    async def test_default_dashboard_refresh_timeout_allows_slow_readback(self) -> None:
        client = FakeExecClient()
        timeout_setting = 'signal_decider_exec_dashboard_refresh_timeout_seconds'
        default_timeout = type(settings).model_fields[timeout_setting].default
        self.assertEqual(default_timeout, 30.0)
        with patch.object(settings, timeout_setting, default_timeout):
            service = SignalDeciderExecIntegrationService(client=client)
        market_date = date(2026, 10, 19)
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_cancelled = False

        async def slow_refresh_overview(**_kwargs):
            nonlocal refresh_cancelled
            refresh_started.set()
            try:
                await release_refresh.wait()
            except asyncio.CancelledError:
                refresh_cancelled = True
                raise
            return {
                'integration': {
                    'enabled': True,
                    'config_status': 'ready',
                    'issues': [],
                },
                'runtime': {'ready': True},
                'items': [],
            }

        with (
            patch.object(
                service,
                'integration_status',
                return_value={
                    'enabled': True,
                    'config_status': 'ready',
                    'issues': [],
                },
            ),
            patch.object(
                service,
                'list_signal_decider_exec_overview',
                new=AsyncMock(side_effect=slow_refresh_overview),
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                '_store_dashboard_caches_from_overview',
                new=AsyncMock(),
            ) as store_caches,
        ):
            try:
                scheduled = service._ensure_dashboard_background_refresh(
                    target_trade_date=market_date,
                    account_reference=None,
                    symbol=None,
                    refresh=True,
                    trigger='test',
                )

                self.assertEqual(scheduled['status'], 'scheduled')
                self.assertEqual(scheduled['timeout_seconds'], 30.0)
                await asyncio.wait_for(refresh_started.wait(), timeout=1.0)

                tasks = list(service._dashboard_refresh_tasks.values())
                self.assertEqual(len(tasks), 1)
                task = tasks[0]
                await asyncio.sleep(2.1)

                self.assertFalse(refresh_cancelled)
                self.assertFalse(task.done())

                release_refresh.set()
                await asyncio.wait_for(task, timeout=1.0)
                key = service._dashboard_refresh_key(
                    target_trade_date=market_date,
                    account_reference=None,
                    symbol=None,
                )
                refresh_state = service._dashboard_refresh_payload(
                    key,
                    requested=True,
                )

                self.assertEqual(refresh_state['status'], 'succeeded')
                self.assertEqual(refresh_state['timeout_seconds'], 30.0)
                self.assertIsNone(refresh_state['last_error'])
                store_caches.assert_awaited_once()
                self.assertEqual(client.post_calls, [])
            finally:
                release_refresh.set()
                tasks = list(service._dashboard_refresh_tasks.values())
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

    async def test_realtime_source_cold_start_returns_holdings_without_dashboard_build(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        source_version = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
        call_order: list[str] = []
        dashboard_started = asyncio.Event()
        release_dashboard = asyncio.Event()

        async def blocked_dashboard_refresh(**_kwargs):
            call_order.append('dashboard_refresh_started')
            dashboard_started.set()
            await release_dashboard.wait()
            raise RuntimeError('test dashboard refresh stopped')

        async def load_holding_membership(**_kwargs):
            call_order.append('portfolio_holdings_loaded')
            return PortfolioHoldingSymbolMembership(
                symbols=frozenset({'027710'}),
                membership_complete=True,
                source_version=source_version,
            )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=source_version,
            ),
            patch.object(
                service,
                'integration_status',
                return_value={
                    'enabled': True,
                    'config_status': 'ready',
                    'issues': [],
                },
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value=None,
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(side_effect=load_holding_membership),
            ) as load_holdings,
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                'list_signal_decider_exec_overview',
                new=AsyncMock(side_effect=blocked_dashboard_refresh),
            ),
        ):
            try:
                source = await asyncio.wait_for(
                    service.load_realtime_monitored_symbol_source(
                        market_date=market_date,
                        trigger='kis_realtime_price:background',
                    ),
                    timeout=1.0,
                )
                await asyncio.wait_for(
                    dashboard_started.wait(),
                    timeout=0.1,
                )
            finally:
                release_dashboard.set()
                tasks = list(service._dashboard_refresh_tasks.values())
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

        self.assertEqual(source.symbols, ('027710',))
        self.assertEqual(source.market_date, market_date.isoformat())
        self.assertEqual(source.source_version, source_version)
        self.assertFalse(source.membership_complete)
        self.assertEqual(source.protected_symbols, ('027710',))
        self.assertEqual(source.critical_protected_symbols, ('027710',))
        load_holdings.assert_awaited_once()
        self.assertEqual(
            call_order,
            ['portfolio_holdings_loaded', 'dashboard_refresh_started'],
        )

    async def test_realtime_source_merges_fresh_holdings_into_cached_membership(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        cached_version = datetime(
            2026,
            9,
            14,
            0,
            0,
            tzinfo=timezone.utc,
        )
        holding_version = cached_version + timedelta(seconds=5)
        cached_source = KISRealtimeMonitoredSymbolSource(
            symbols=('035420', '005930'),
            market_date=market_date.isoformat(),
            source_version=cached_version,
            membership_complete=True,
            protected_symbols=('035420',),
        )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=holding_version,
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value={'items': []},
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(
                    return_value=PortfolioHoldingSymbolMembership(
                        symbols=frozenset({'027710'}),
                        membership_complete=True,
                        source_version=holding_version,
                    )
                ),
            ) as load_holdings,
            patch.object(
                KISRealtimePriceService,
                '_runtime_monitored_source_from_payload',
                return_value=cached_source,
            ),
        ):
            source = await service.load_realtime_monitored_symbol_source(
                market_date=market_date,
                trigger='kis_realtime_price:background',
            )

        self.assertEqual(source.symbols, ('027710', '035420', '005930'))
        self.assertEqual(source.source_version, holding_version)
        self.assertFalse(source.membership_complete)
        self.assertEqual(source.protected_symbols, ('027710', '035420'))
        self.assertEqual(source.critical_protected_symbols, ('027710',))
        load_holdings.assert_awaited_once()

    async def test_realtime_source_does_not_promote_future_cached_version(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        holding_version = datetime(
            2026,
            9,
            14,
            0,
            0,
            tzinfo=timezone.utc,
        )
        future_cached_source = KISRealtimeMonitoredSymbolSource(
            symbols=('005930',),
            market_date=market_date.isoformat(),
            source_version=holding_version + timedelta(minutes=1),
            membership_complete=False,
            protected_symbols=('005930',),
        )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=holding_version,
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value={'items': []},
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(
                    return_value=PortfolioHoldingSymbolMembership(
                        symbols=frozenset({'027710'}),
                        membership_complete=True,
                        source_version=holding_version,
                    )
                ),
            ),
            patch.object(
                KISRealtimePriceService,
                '_runtime_monitored_source_from_payload',
                return_value=future_cached_source,
            ),
        ):
            source = await service.load_realtime_monitored_symbol_source(
                market_date=market_date,
                trigger='kis_realtime_price:background',
            )

        self.assertEqual(source.symbols, ('027710',))
        self.assertEqual(source.source_version, holding_version)
        self.assertEqual(source.protected_symbols, ('027710',))
        self.assertFalse(source.membership_complete)

    async def test_realtime_source_recovers_exact_cached_membership_authority(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        cached_version = datetime(
            2026,
            9,
            14,
            0,
            5,
            tzinfo=timezone.utc,
        )
        holding_version = cached_version - timedelta(seconds=5)
        cached_source = KISRealtimeMonitoredSymbolSource(
            symbols=('027710', '005930'),
            market_date=market_date.isoformat(),
            source_version=cached_version,
            membership_complete=True,
            protected_symbols=('027710',),
            critical_protected_symbols=('027710',),
        )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=cached_version,
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value={'items': []},
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(
                    return_value=PortfolioHoldingSymbolMembership(
                        symbols=frozenset({'027710'}),
                        membership_complete=True,
                        source_version=holding_version,
                    )
                ),
            ),
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(
                    return_value=ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({'005930'}),
                        membership_complete=True,
                        source_version=holding_version,
                    )
                ),
            ),
            patch.object(
                KISRealtimePriceService,
                '_runtime_monitored_source_from_payload',
                return_value=cached_source,
            ),
        ):
            source = await service.load_realtime_monitored_symbol_source(
                market_date=market_date,
                trigger='kis_realtime_price:background',
            )

        self.assertEqual(source.symbols, cached_source.symbols)
        self.assertEqual(source.source_version, cached_version)
        self.assertTrue(source.membership_complete)
        self.assertEqual(source.protected_symbols, ('027710', '005930'))
        self.assertEqual(
            source.critical_protected_symbols,
            cached_source.critical_protected_symbols,
        )

    async def test_realtime_source_keeps_newer_holding_removal_merge_only(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        cached_version = datetime(
            2026,
            9,
            14,
            0,
            0,
            tzinfo=timezone.utc,
        )
        holding_version = cached_version + timedelta(seconds=1)
        cached_source = KISRealtimeMonitoredSymbolSource(
            symbols=('027710',),
            market_date=market_date.isoformat(),
            source_version=cached_version,
            membership_complete=True,
            protected_symbols=('027710',),
            critical_protected_symbols=('027710',),
        )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=holding_version,
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value={'items': []},
            ),
            patch.object(
                KISRealtimePriceService,
                '_runtime_monitored_source_from_payload',
                return_value=cached_source,
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(
                    return_value=PortfolioHoldingSymbolMembership(
                        symbols=frozenset(),
                        membership_complete=True,
                        source_version=holding_version,
                    )
                ),
            ),
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ),
        ):
            source = await service.load_realtime_monitored_symbol_source(
                market_date=market_date,
                trigger='kis_realtime_price:background',
            )

        self.assertEqual(source.symbols, ('027710',))
        self.assertEqual(source.source_version, holding_version)
        self.assertFalse(source.membership_complete)

    async def test_realtime_source_ingests_slow_exec_membership_without_blocking_holdings(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        source_version = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
        exec_started = asyncio.Event()
        release_exec = asyncio.Event()

        async def load_exec_membership(**_kwargs):
            exec_started.set()
            await release_exec.wait()
            return ExecRuntimeMonitoredSymbolMembership(
                symbols=frozenset({'005930'}),
                membership_complete=True,
                source_version=source_version + timedelta(seconds=1),
            )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=source_version,
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value=None,
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(
                    return_value=PortfolioHoldingSymbolMembership(
                        symbols=frozenset({'027710'}),
                        membership_complete=True,
                        source_version=source_version,
                    )
                ),
            ),
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(side_effect=load_exec_membership),
            ) as load_exec,
        ):
            first = await asyncio.wait_for(
                service.load_realtime_monitored_symbol_source(
                    market_date=market_date,
                    trigger='kis_realtime_price:background',
                ),
                timeout=1.0,
            )
            await asyncio.wait_for(exec_started.wait(), timeout=0.1)
            second = await asyncio.wait_for(
                service.load_realtime_monitored_symbol_source(
                    market_date=market_date,
                    trigger='kis_realtime_price:background',
                ),
                timeout=1.0,
            )
            release_exec.set()
            await asyncio.sleep(0)
            third = await service.load_realtime_monitored_symbol_source(
                market_date=market_date,
                trigger='kis_realtime_price:background',
            )

        self.assertEqual(first.symbols, ('027710',))
        self.assertEqual(second.symbols, ('027710',))
        self.assertEqual(third.symbols, ('027710', '005930'))
        self.assertEqual(third.protected_symbols, ('027710', '005930'))
        self.assertEqual(third.critical_protected_symbols, ('027710',))
        self.assertFalse(third.membership_complete)
        self.assertEqual(load_exec.await_count, 1)

    async def test_realtime_source_bounds_cold_portfolio_wait_before_dashboard_refresh(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        source_version = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
        portfolio_started = asyncio.Event()
        release_portfolio = asyncio.Event()

        async def blocked_portfolio_membership(**_kwargs):
            portfolio_started.set()
            await release_portfolio.wait()
            return PortfolioHoldingSymbolMembership(
                symbols=frozenset({'027710'}),
                membership_complete=True,
                source_version=source_version,
            )

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=source_version,
            ),
            patch(
                'app.orders.exec_integration.'
                'REALTIME_MONITORED_PORTFOLIO_WAIT_SECONDS',
                0.01,
                create=True,
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value=None,
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(side_effect=blocked_portfolio_membership),
            ),
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ) as schedule_refresh,
        ):
            try:
                source = await asyncio.wait_for(
                    service.load_realtime_monitored_symbol_source(
                        market_date=market_date,
                        trigger='kis_realtime_price:background',
                    ),
                    timeout=0.2,
                )
            finally:
                release_portfolio.set()

        self.assertTrue(portfolio_started.is_set())
        self.assertEqual(source.symbols, ())
        self.assertFalse(source.membership_complete)
        schedule_refresh.assert_called_once()

    async def test_realtime_source_serves_cache_as_partial_while_portfolio_refresh_is_slow(
        self,
    ) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 9, 14)
        source_version = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
        portfolio_started = asyncio.Event()
        release_portfolio = asyncio.Event()
        cached_source = KISRealtimeMonitoredSymbolSource(
            symbols=('027710',),
            market_date=market_date.isoformat(),
            source_version=source_version,
            membership_complete=True,
            protected_symbols=('027710',),
            critical_protected_symbols=('027710',),
        )

        async def blocked_portfolio_membership(**_kwargs):
            portfolio_started.set()
            await release_portfolio.wait()
            return None

        with (
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=source_version,
            ),
            patch.object(
                service,
                '_load_dashboard_cached_payload',
                return_value={'items': []},
            ),
            patch.object(
                KISRealtimePriceService,
                '_runtime_monitored_source_from_payload',
                return_value=cached_source,
            ),
            patch.object(
                service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(side_effect=blocked_portfolio_membership),
            ),
            patch.object(
                service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                '_ensure_dashboard_background_refresh',
                return_value={
                    'requested': True,
                    'status': 'scheduled',
                    'in_flight': True,
                },
            ),
        ):
            try:
                source = await asyncio.wait_for(
                    service.load_realtime_monitored_symbol_source(
                        market_date=market_date,
                        trigger='kis_realtime_price:background',
                    ),
                    timeout=0.1,
                )
            finally:
                release_portfolio.set()
                tasks = list(
                    service._realtime_portfolio_membership_tasks.values()
                )
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

        self.assertTrue(portfolio_started.is_set())
        self.assertEqual(source.symbols, cached_source.symbols)
        self.assertEqual(source.source_version, source_version)
        self.assertFalse(source.membership_complete)

    async def test_historical_daily_board_ignores_unscoped_current_runtime_activity(self) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        overview = {
            'integration': {
                'mode': 'dispatch',
                'enabled': True,
                'delegate_execution': True,
                'runtime_mode': 'live',
                'issues': [],
            },
            'runtime': {
                'status': 'ok',
                'payload': {
                    'mode': 'live',
                    'ready': True,
                    'counts': {'open_orders': 1, 'active_execution_jobs': 0},
                },
                'error': None,
            },
            'items': [],
        }

        with (
            patch.object(
                service,
                '_load_dashboard_read_model_overview',
                new=AsyncMock(return_value=overview),
            ),
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=datetime(2026, 8, 29, 1, 0, tzinfo=timezone.utc),
            ),
        ):
            board = await service.get_runtime_daily_board(
                market_date=date(2026, 8, 28),
                refresh=False,
                trigger='test',
            )

        self.assertEqual(board['items'], [])
        self.assertEqual(board['activity_coverage']['status'], 'unavailable')
        self.assertEqual(
            board['activity_coverage']['status_reason'],
            'runtime_read_model_unavailable',
        )
        self.assertIsNone(board['activity_coverage']['exec_activity_detected'])
        self.assertEqual(board['freshness']['status'], 'unavailable')


class ExecRuntimeReadbackNormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        self.integration = {
            'mode': 'dispatch',
            'enabled': True,
            'delegate_execution': True,
            'runtime_mode': 'live',
            'issues': [],
        }

    @staticmethod
    def _current_exec_payload() -> dict:
        return {
            'mode': 'live',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'liveness': {
                'monitoring': {
                    'status': 'degraded',
                    'last_heartbeat_at': '2026-08-29T00:06:53+00:00',
                },
                'autonomous': {
                    'enabled': True,
                    'monitoring': {
                        'worker_name': 'monitoring_dispatch_loop',
                        'status': 'running',
                        'poll_interval_seconds': 5,
                        'last_heartbeat_at': '2026-08-29T00:06:53+00:00',
                        'last_success_at': '2026-08-29T00:06:53+00:00',
                    },
                }
            },
            'readiness': {'reasons': []},
        }

    def test_normalizes_current_exec_autonomous_monitoring_liveness(self) -> None:
        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': self._current_exec_payload(), 'error': None},
        )

        self.assertEqual(result['status'], 'ready')
        self.assertTrue(result['monitoring_alive'])
        self.assertEqual(result['last_heartbeat_at'], '2026-08-29T00:06:53+00:00')

    def test_exposes_active_monitoring_facts_from_exec_last_cycle(self) -> None:
        payload = self._current_exec_payload()
        payload['liveness']['autonomous']['monitoring']['last_cycle'] = {
            'triggered_at': '2026-08-29T00:06:52+00:00',
            'items': [
                {
                    'symbol': '027710',
                    'intent': 'target_sell',
                    'status': 'target_sell_monitoring',
                    'trigger_price': '1154.1479',
                    'observed_price': '1126',
                    'outcome': 'waiting',
                    'internal_note': 'must not cross the Core contract',
                },
                {
                    'symbol': '027710',
                    'intent': 'stop_sell',
                    'status': 'stop_sell_monitoring',
                    'trigger_price': '1097.8480',
                    'observed_price': '1126',
                    'outcome': 'waiting',
                },
            ],
        }

        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': payload, 'error': None},
        )

        self.assertTrue(result['active_monitoring_facts_complete'])
        self.assertEqual(
            result['active_monitoring_items'],
            [
                {
                    'symbol': '027710',
                    'intent': 'target_sell',
                    'status': 'target_sell_monitoring',
                    'trigger_price': '1154.1479',
                    'observed_price': '1126',
                    'outcome': 'waiting',
                    'observed_at': '2026-08-29T00:06:52+00:00',
                },
                {
                    'symbol': '027710',
                    'intent': 'stop_sell',
                    'status': 'stop_sell_monitoring',
                    'trigger_price': '1097.8480',
                    'observed_price': '1126',
                    'outcome': 'waiting',
                    'observed_at': '2026-08-29T00:06:52+00:00',
                },
            ],
        )

    def test_live_exec_monitoring_facts_override_stale_core_triggers(self) -> None:
        result = self.service._apply_live_exec_monitoring_truth(
            item={
                'symbol': '027710',
                'exec_monitoring_active': True,
                'target_price': 1260,
                'stop_loss_price': 1165,
                'trigger_price': 1200,
                'current_price': 1121,
                'price_as_of': '2026-08-28T00:00:00+00:00',
                'price_received_at': '2026-08-28T00:00:00+00:00',
                'price_source': 'core_historical',
                'price_status': 'available',
                'display_price_available': True,
                'monitoring_state': 'watching_fill',
                'monitoring_statuses': ['runtime_membership_active'],
                'last_event_at': '2026-08-28T00:00:00+00:00',
            },
            facts=[
                {
                    'symbol': '027710',
                    'intent': 'target_sell',
                    'status': 'target_sell_monitoring',
                    'trigger_price': '1154.1479',
                    'observed_price': '1126',
                    'outcome': 'waiting',
                    'observed_at': '2026-08-29T00:06:52+00:00',
                },
                {
                    'symbol': '027710',
                    'intent': 'stop_sell',
                    'status': 'stop_sell_monitoring',
                    'trigger_price': '1097.8480',
                    'observed_price': '1126',
                    'outcome': 'waiting',
                    'observed_at': '2026-08-29T00:06:52+00:00',
                },
            ],
            facts_complete=True,
        )

        self.assertEqual(result['target_price'], '1154.1479')
        self.assertEqual(result['stop_loss_price'], '1097.8480')
        self.assertIsNone(result['trigger_price'])
        self.assertEqual(result['current_price'], 1126)
        self.assertEqual(result['monitoring_state'], 'watching_target_stop')
        self.assertEqual(result['last_event_at'], '2026-08-29T00:06:52+00:00')
        self.assertEqual(result['price_source'], 'exec_runtime_monitoring')
        self.assertIn('target_sell_monitoring', result['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', result['monitoring_statuses'])

    def test_complete_exec_monitoring_facts_do_not_present_stale_triggers(self) -> None:
        result = self.service._apply_live_exec_monitoring_truth(
            item={
                'symbol': '027710',
                'exec_monitoring_active': True,
                'target_price': 1260,
                'stop_loss_price': 1165,
                'trigger_price': 1200,
                'monitoring_state': 'watching_target_stop',
                'monitoring_statuses': ['runtime_membership_active'],
                'last_event_at': '2026-08-28T00:00:00+00:00',
            },
            facts=[],
            facts_complete=True,
        )

        self.assertIsNone(result['target_price'])
        self.assertIsNone(result['stop_loss_price'])
        self.assertIsNone(result['trigger_price'])
        self.assertEqual(result['monitoring_state'], 'reconciling')
        self.assertIsNone(result['last_event_at'])
        self.assertIn(
            'runtime_monitoring_facts_unavailable',
            result['monitoring_statuses'],
        )

    def test_exposes_missing_pnl_coverage_reasons(self) -> None:
        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': self._current_exec_payload(), 'error': None},
        )

        self.assertIsNone(result['daily_pnl'])
        self.assertIsNone(result['weekly_pnl'])
        self.assertIsNone(result['monthly_pnl'])
        self.assertEqual(
            result['pnl_coverage'],
            {
                period: {
                    'status': 'unavailable',
                    'status_reason': 'not_provided_by_exec',
                    'source': 'exec_runtime_status',
                }
                for period in ('daily', 'weekly', 'monthly')
            },
        )

    def test_exposes_upstream_pnl_not_computable_reason(self) -> None:
        payload = self._current_exec_payload()
        payload['daily_pnl'] = {
            'daily_realized_pnl': None,
            'status_reason': 'fill_coverage_incomplete',
        }

        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': payload, 'error': None},
        )

        self.assertEqual(
            result['pnl_coverage']['daily'],
            {
                'status': 'unavailable',
                'status_reason': 'fill_coverage_incomplete',
                'source': 'exec_runtime_status',
            },
        )

    def test_unknown_exec_item_state_never_defaults_to_accepted(self) -> None:
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'remote_status': 'new_exec_state_not_yet_known_to_core',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'status': 'new_exec_state_not_yet_known_to_core',
                    },
                }
            ],
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )

        self.assertEqual(result['derived_status'], 'unknown')
        self.assertEqual(
            result['plan_item_status_counts'],
            {'new_exec_state_not_yet_known_to_core': 1},
        )

    def test_known_v4_nonterminal_states_have_explicit_semantics(self) -> None:
        cases = {
            'next_open_buy_pending_configuration': 'pending',
            'next_open_buy_scheduled': 'pending',
            'next_open_buy_retry_pending': 'pending',
            'agent_delay': 'pending',
            'next_open_buy_dispatching': 'working',
            'next_open_buy_retrying': 'working',
            'broker_order_submission_pending': 'working',
            'broker_order_submitted': 'working',
            'partially_filled': 'working',
            'watch_buy_triggered_pending_dispatch': 'working',
            'max_holding_close_sell_triggered_blocked': 'pending',
        }

        for status, expected in cases.items():
            with self.subTest(status=status):
                result = self.service._derive_remote_state(
                    row=SimpleNamespace(symbol='005930'),
                    item_links=[
                        {
                            'intent': 'next_open_buy',
                            'remote_status': status,
                            'remote_status_source': 'exec_item_state',
                            'current_state': {
                                'status': status,
                                'terminal': False,
                            },
                        }
                    ],
                    orders=[],
                    executions=[],
                    positions=[],
                    endpoint_statuses={
                        'plans': 'ok',
                        'orders': 'ok',
                        'executions': 'ok',
                        'positions': 'ok',
                    },
                )

                self.assertEqual(result['derived_status'], expected)

    def test_known_v4_terminal_states_have_explicit_semantics(self) -> None:
        cases = {
            'agent_rejected': 'failed',
            'broker_order_failed': 'failed',
            'broker_order_rejected': 'failed',
            'crypto_paper_failed': 'failed',
            'crypto_paper_rejected': 'failed',
            'manual_review_required': 'failed',
            'manual_review_resolved_no_exposure': 'failed',
            'next_open_buy_failed': 'failed',
            'unsupported_semantics': 'failed',
            'watch_buy_rejected': 'failed',
            'next_open_buy_expired': 'cancelled',
            'not_applicable': 'cancelled',
            'revoked': 'cancelled',
            'superseded': 'cancelled',
            'watch_cancelled': 'cancelled',
            'watch_expired': 'cancelled',
            'filled': 'unknown',
        }

        for status, expected in cases.items():
            with self.subTest(status=status):
                result = self.service._derive_remote_state(
                    row=SimpleNamespace(symbol='005930'),
                    item_links=[
                        {
                            'intent': 'next_open_buy',
                            'remote_status': status,
                            'remote_status_source': 'exec_item_state',
                            'current_state': {
                                'status': status,
                                'terminal': True,
                            },
                        }
                    ],
                    orders=[],
                    executions=[],
                    positions=[],
                    endpoint_statuses={
                        'plans': 'ok',
                        'orders': 'ok',
                        'executions': 'ok',
                        'positions': 'ok',
                    },
                )

                self.assertEqual(result['derived_status'], expected)

    def test_watch_expiry_does_not_clear_quarantine_on_partial_readback(self) -> None:
        row = SimpleNamespace(
            decision_agent_name='short-term-decider',
            planned_side='buy',
            eligibility_status='blocked',
            execution_status='submit_failed',
            validation_payload={
                'blocking_reasons': [
                    {
                        'code': 'exec_contract_quarantined',
                        'message': 'Prior exact contract quarantine.',
                    }
                ],
                'short_term': {
                    'tactical_action': 'watch_for_trigger',
                    'handoff_intent': 'intraday_watch_buy',
                },
            },
            traceability_payload={
                'raw_action': 'watch_for_trigger',
                'handoff_intent': 'intraday_watch_buy',
            },
        )
        item_links = [
            {
                'intent': 'intraday_watch_buy',
                'current_state': {
                    'status': 'watch_expired',
                    'terminal': True,
                },
                'remote_status': 'watch_expired',
                'remote_status_source': 'exec_item_state',
            }
        ]
        derived_state = {
            'derived_status': 'cancelled',
            'has_plan_attributed_fill_evidence': False,
            'matched_order_count': 0,
            'matched_execution_count': 0,
            'open_quantity': '0',
            'scoped_position_exposure_quantity': '0',
            'endpoint_statuses': {
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'error',
                'positions': 'ok',
            },
        }

        self.service._sync_exec_terminal_state(
            row=row,
            item_links=item_links,
            derived_state=derived_state,
            now=datetime(2026, 10, 14, tzinfo=timezone.utc),
            trigger='test_partial_readback',
        )

        self.assertEqual(row.execution_status, 'submit_failed')
        self.assertIn(
            'exec_contract_quarantined',
            {
                reason['code']
                for reason in row.validation_payload['blocking_reasons']
            },
        )

    def test_v4_item_states_are_current_truth_while_item_acks_remain_proof(self) -> None:
        link = {
            'core_item_key': 'core-item-1',
            'exec_plan_id': 'exec-plan-1',
            'exec_plan_item_id': 'exec-item-1',
            'ack_status': 'accepted',
            'remote_status': 'accepted',
            'remote_status_source': 'dispatch_ack',
            'response_item': {
                'core_item_key': 'core-item-1',
                'execution_plan_id': 'exec-plan-1',
                'execution_plan_item_id': 'exec-item-1',
                'status': 'accepted',
            },
        }
        merged, mismatches = self.service._merge_exec_plan_readback_links(
            item_links=[link],
            plans_result={
                'status': 'ok',
                'payload': {
                    'items': [
                        {
                            'external_plan_id': 'exec-plan-1',
                            'execution_plan_id': 'exec-plan-1',
                            'status': 'accepted',
                            'item_acks': [copy.deepcopy(link['response_item'])],
                            'item_states': [
                                {
                                    'core_item_key': 'core-item-1',
                                    'execution_plan_item_id': 'exec-item-1',
                                    'status': 'exec_agent_rejected',
                                    'updated_at': '2026-10-07T05:00:00+00:00',
                                    'terminal': True,
                                    'decision': {
                                        'action': 'reject',
                                        'decision': 'rejected',
                                        'reason_code': 'account_drawdown_gate',
                                        'reason_family': 'risk_gate',
                                        'rejection_disposition': 'terminal',
                                        'reason': 'Account drawdown gate blocked execution.',
                                    },
                                }
                            ],
                        }
                    ]
                },
            },
        )

        self.assertEqual(mismatches, [])
        self.assertEqual(merged[0]['response_item']['status'], 'accepted')
        self.assertEqual(merged[0]['ack_status'], 'accepted')
        self.assertEqual(merged[0]['remote_status'], 'exec_agent_rejected')
        self.assertEqual(merged[0]['remote_status_source'], 'exec_item_state')
        self.assertEqual(
            merged[0]['current_state']['decision']['reason_code'],
            'account_drawdown_gate',
        )

    def test_v4_deferred_item_state_remains_retryable_pending(self) -> None:
        link = {
            'core_item_key': 'candidate:next_open_buy',
            'exec_plan_id': 'exec-plan-uuid',
            'exec_plan_item_id': 'item-uuid',
            'ack_status': 'accepted',
            'remote_status': 'accepted',
            'remote_status_source': 'dispatch_ack',
            'response_item': {
                'core_item_key': 'candidate:next_open_buy',
                'execution_plan_id': 'exec-plan-uuid',
                'execution_plan_item_id': 'item-uuid',
                'status': 'accepted',
            },
        }
        merged, mismatches = self.service._merge_exec_plan_readback_links(
            item_links=[link],
            plans_result={
                'status': 'ok',
                'payload': {
                    'items': [
                        {
                            'execution_plan_id': 'exec-plan-uuid',
                            'external_plan_id': 'core-plan-id',
                            'plan_id': 'core-plan-id',
                            'status': 'running',
                            'item_acks': [copy.deepcopy(link['response_item'])],
                            'item_states': [
                                {
                                    'core_item_key': 'candidate:next_open_buy',
                                    'execution_plan_item_id': 'item-uuid',
                                    'status': 'watch_sizing_pending',
                                    'updated_at': '2026-10-08T00:00:00+00:00',
                                    'terminal': False,
                                    'decision': {
                                        'action': 'defer',
                                        'decision': 'deferred',
                                        'reason_code': 'reward_risk_below_floor',
                                        'reason_family': 'reward_risk',
                                        'rejection_disposition': 'defer',
                                        'reason': 'Reward/risk is below the current floor.',
                                        'retry_after': '2026-10-08T00:15:00+00:00',
                                    },
                                }
                            ],
                        }
                    ]
                },
            },
        )
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=merged,
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )

        self.assertEqual(mismatches, [])
        self.assertEqual(result['derived_status'], 'pending')
        self.assertEqual(
            merged[0]['current_state']['decision']['retry_after'],
            '2026-10-08T00:15:00+00:00',
        )

    def test_v4_new_plan_pending_item_family_is_pending_without_defer_decision(
        self,
    ) -> None:
        states = (
            ('candidate:next_open_buy', 'entry-item', 'watch_sizing_pending'),
            ('candidate:target_sell', 'target-item', 'target_sell_pending_config'),
            ('candidate:stop_sell', 'stop-item', 'stop_sell_pending_config'),
        )
        links = [
            {
                'core_item_key': core_item_key,
                'exec_plan_id': 'exec-plan-uuid',
                'exec_plan_item_id': item_id,
                'ack_status': 'accepted',
                'remote_status': 'accepted',
                'remote_status_source': 'dispatch_ack',
                'response_item': {
                    'core_item_key': core_item_key,
                    'execution_plan_id': 'exec-plan-uuid',
                    'execution_plan_item_id': item_id,
                    'status': 'accepted',
                },
            }
            for core_item_key, item_id, _ in states
        ]
        merged, mismatches = self.service._merge_exec_plan_readback_links(
            item_links=links,
            plans_result={
                'status': 'ok',
                'payload': {
                    'items': [
                        {
                            'execution_plan_id': 'exec-plan-uuid',
                            'external_plan_id': 'core-plan-id',
                            'plan_id': 'core-plan-id',
                            'status': 'running',
                            'item_acks': [
                                copy.deepcopy(link['response_item'])
                                for link in links
                            ],
                            'item_status_counts': {
                                'watch_sizing_pending': 1,
                                'target_sell_pending_config': 1,
                                'stop_sell_pending_config': 1,
                            },
                            'item_states': [
                                {
                                    'core_item_key': core_item_key,
                                    'execution_plan_item_id': item_id,
                                    'status': status,
                                    'updated_at': '2026-10-08T00:00:00+00:00',
                                    'terminal': False,
                                    'decision': None,
                                }
                                for core_item_key, item_id, status in states
                            ],
                        }
                    ]
                },
            },
        )
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=merged,
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )

        self.assertEqual(mismatches, [])
        self.assertEqual(result['derived_status'], 'pending')
        self.assertEqual(
            result['plan_item_status_counts'],
            {
                'stop_sell_pending_config': 1,
                'target_sell_pending_config': 1,
                'watch_sizing_pending': 1,
            },
        )

    def _acknowledged_revocation_readback(
        self,
        *,
        execution_status: str,
    ) -> tuple[SimpleNamespace, list[dict], dict]:
        bindings = (
            ('candidate:next_open_buy', 'next_open_buy', 'entry-item'),
            ('candidate:target_sell', 'target_sell', 'target-item'),
            ('candidate:stop_sell', 'stop_sell', 'stop-item'),
        )
        revocation_links = [
            {
                'core_item_key': core_item_key,
                'intent': intent,
                'side': 'buy' if intent == 'next_open_buy' else 'sell',
                'exec_plan_id': 'exec-plan-uuid',
                'exec_plan_item_id': item_id,
            }
            for core_item_key, intent, item_id in bindings
        ]
        request_payload = {
            'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'required_capability': EXECUTION_REVOCATION_CAPABILITY,
            'plan_id': 'core-plan-id',
            'requested_at': '2026-10-08T00:00:00+00:00',
            'reason_code': 'exec_contract_quarantined',
            'replacement_decision_snapshot_id': 'quarantine:core-plan-id',
            'replacement_action': 'reject',
            'items': [
                {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                    'decision_snapshot_id': '1234',
                }
                for link in revocation_links
            ],
        }
        response_payload = {
            'plan_id': 'core-plan-id',
            'execution_plan_id': 'exec-plan-uuid',
            'contract_ack': {
                'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
                'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                'capability': EXECUTION_REVOCATION_CAPABILITY,
                'plan_id': 'core-plan-id',
                'status': 'revoked',
            },
            'item_acks': [
                {
                    'core_item_key': link['core_item_key'],
                    'status': 'revoked',
                    'execution_plan_id': link['exec_plan_id'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                }
                for link in revocation_links
            ],
            'reason_code': 'exec_contract_quarantined',
            'revoked_item_count': len(revocation_links),
            'manual_review_item_count': 0,
            'preserved_item_count': 0,
        }
        item_links = [
            {
                **link,
                'remote_status': 'revoked',
                'remote_status_source': 'exec_item_state',
                'current_state': {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_id': link['exec_plan_id'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                    'status': 'revoked',
                    'terminal': True,
                },
            }
            for link in revocation_links
        ]
        row = SimpleNamespace(
            symbol='005930',
            eligibility_status='blocked',
            execution_status=execution_status,
            submission_payload={
                'revocation': {
                    'schema_version': 'core_execution_revocation_state_v1',
                    'status': 'acknowledged',
                    'ack_status': 'revoked',
                    'plan_id': 'core-plan-id',
                    'reason_code': 'exec_contract_quarantined',
                    'requested_at': '2026-10-08T00:00:00+00:00',
                    'acknowledged_at': '2026-10-08T00:01:00+00:00',
                    'prior_decision_snapshot_id': '1234',
                    'replacement_decision_snapshot_id': 'quarantine:core-plan-id',
                    'replacement_action': 'reject',
                    'replacement_actionable': False,
                    'item_links': copy.deepcopy(revocation_links),
                },
                'revocation_history': [
                    {
                        'acknowledged_at': '2026-10-08T00:01:00+00:00',
                        'request': request_payload,
                        'response': response_payload,
                    }
                ],
            },
            validation_payload={
                'blocking_reasons': [
                    {
                        'code': 'exec_contract_quarantined',
                        'message': 'The accepted Exec plan was quarantined.',
                    }
                ],
            },
            traceability_payload={
                'handoff_active': False,
                'exec_monitoring_active': False,
                'exec_agent_approved': False,
                'approval_state': 'rejected',
            },
        )
        derived_state = self.service._derive_remote_state(
            row=row,
            item_links=item_links,
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )
        return row, item_links, derived_state

    def test_acknowledged_revocation_remains_revoked_on_repeated_readback(
        self,
    ) -> None:
        row, item_links, derived_state = self._acknowledged_revocation_readback(
            execution_status='revoked',
        )
        row.submission_payload = {
            **row.submission_payload,
            'mode': 'exec_dispatch',
            'exec_dispatch': {
                'plan_id': 'core-plan-id',
                'dispatch_status': 'accepted',
                'item_links': copy.deepcopy(item_links),
                'last_request_summary': {
                    'diagnostic_blob': 'x' * 22_000,
                },
            },
        }

        for trigger in ('first_readback', 'second_readback'):
            slimmed = slim_signal_decider_exec_submission_payload(
                row.submission_payload
            )
            self.assertIsNotNone(slimmed.payload)
            row.submission_payload = slimmed.payload
            audit = self.service._sync_exec_terminal_state(
                row=row,
                item_links=item_links,
                derived_state=derived_state,
                now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                trigger=trigger,
            )

            self.assertIsNotNone(audit)
            self.assertEqual(audit['core_state_after']['execution_status'], 'revoked')
            self.assertTrue(audit['acknowledged_revocation_stable'])
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                [
                    reason['code']
                    for reason in row.validation_payload['blocking_reasons']
                ],
                ['exec_contract_quarantined'],
            )

    def test_acknowledged_revocation_self_heals_legacy_submit_failed_state(
        self,
    ) -> None:
        row, item_links, derived_state = self._acknowledged_revocation_readback(
            execution_status=EXECUTION_STATUS_SUBMIT_FAILED,
        )

        audit = self.service._sync_exec_terminal_state(
            row=row,
            item_links=item_links,
            derived_state=derived_state,
            now=datetime(2026, 10, 8, tzinfo=timezone.utc),
            trigger='legacy_submit_failed_readback',
        )

        self.assertIsNotNone(audit)
        self.assertEqual(
            audit['core_state_before']['execution_status'],
            EXECUTION_STATUS_SUBMIT_FAILED,
        )
        self.assertEqual(audit['core_state_after']['execution_status'], 'revoked')
        self.assertTrue(audit['acknowledged_revocation_stable'])
        self.assertEqual(row.execution_status, 'revoked')

    def test_revocation_readback_requires_exact_complete_acknowledged_evidence(
        self,
    ) -> None:
        mutations = {
            'unacknowledged': lambda row, _links, _state: row.submission_payload[
                'revocation'
            ].update(status='pending'),
            'non_revoked_ack': lambda row, _links, _state: row.submission_payload[
                'revocation'
            ].update(ack_status='manual_review_required'),
            'unknown_state_schema': lambda row, _links, _state: (
                row.submission_payload['revocation'].update(
                    schema_version='unknown_revocation_state_v1'
                )
            ),
            'replacement_action_mismatch': lambda row, _links, _state: (
                row.submission_payload['revocation'].update(
                    replacement_action='buy'
                )
            ),
            'replacement_still_actionable': lambda row, _links, _state: (
                row.submission_payload['revocation'].update(
                    replacement_actionable=True
                )
            ),
            'request_time_mismatch': lambda row, _links, _state: (
                row.submission_payload['revocation_history'][0][
                    'request'
                ].update(requested_at='2026-10-08T00:00:01+00:00')
            ),
            'request_snapshot_mismatch': lambda row, _links, _state: (
                row.submission_payload['revocation_history'][0]['request'][
                    'items'
                ][0].update(decision_snapshot_id='different-snapshot')
            ),
            'identity_mismatch': lambda row, _links, _state: row.submission_payload[
                'revocation'
            ]['item_links'][0].update(exec_plan_item_id='different-item'),
            'malformed_expected_link': lambda row, _links, _state: (
                row.submission_payload['revocation']['item_links'].append(None)
            ),
            'malformed_observed_link': lambda _row, links, _state: (
                links.append(None)
            ),
            'ack_history_mismatch': lambda row, _links, _state: row.submission_payload[
                'revocation_history'
            ][0]['response']['item_acks'][0].update(
                execution_plan_item_id='different-item'
            ),
            'unknown_reason': lambda row, _links, _state: (
                row.submission_payload['revocation'].update(
                    reason_code='unknown_reason'
                ),
                row.submission_payload['revocation_history'][0][
                    'request'
                ].update(reason_code='unknown_reason'),
                row.submission_payload['revocation_history'][0][
                    'response'
                ].update(reason_code='unknown_reason'),
            ),
            'malformed_latest_ack': lambda row, _links, _state: (
                row.submission_payload['revocation_history'].append(None)
            ),
            'remote_identity_mismatch': lambda _row, links, _state: links[0][
                'current_state'
            ].update(execution_plan_item_id='different-item'),
            'partial_remote_state': lambda _row, links, state: (
                links[-1]['current_state'].update(status='not_applicable'),
                state['plan_item_status_counts'].update(
                    {'revoked': 2, 'not_applicable': 1}
                ),
            ),
            'exposure_unknown': lambda _row, _links, state: state.update(
                scoped_position_exposure_quantity=None
            ),
        }

        for name, mutate in mutations.items():
            with self.subTest(name=name):
                row, item_links, derived_state = (
                    self._acknowledged_revocation_readback(
                        execution_status=EXECUTION_STATUS_SUBMIT_FAILED,
                    )
                )
                mutate(row, item_links, derived_state)

                audit = self.service._sync_exec_terminal_state(
                    row=row,
                    item_links=item_links,
                    derived_state=derived_state,
                    now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    trigger=f'incomplete_revocation:{name}',
                )

                self.assertIsNotNone(audit)
                self.assertNotIn('acknowledged_revocation_stable', audit)
                self.assertEqual(
                    row.execution_status,
                    EXECUTION_STATUS_SUBMIT_FAILED,
                )

    def test_revocation_readback_rejects_unknown_order_or_execution_evidence(
        self,
    ) -> None:
        cases = {
            'order_without_status': {
                'orders': [
                    {
                        'execution_plan_item_id': 'entry-item',
                        'symbol': '005930',
                        'side': 'buy',
                    }
                ],
                'executions': [],
            },
            'execution_without_quantity': {
                'orders': [],
                'executions': [
                    {
                        'execution_plan_item_id': 'entry-item',
                        'symbol': '005930',
                        'side': 'buy',
                        'quantity': 'unknown',
                    }
                ],
            },
        }

        for name, evidence in cases.items():
            with self.subTest(name=name):
                row, item_links, _ = self._acknowledged_revocation_readback(
                    execution_status=EXECUTION_STATUS_SUBMIT_FAILED,
                )
                derived_state = self.service._derive_remote_state(
                    row=row,
                    item_links=item_links,
                    orders=evidence['orders'],
                    executions=evidence['executions'],
                    positions=[],
                    endpoint_statuses={
                        'plans': 'ok',
                        'orders': 'ok',
                        'executions': 'ok',
                        'positions': 'ok',
                    },
                )

                audit = self.service._sync_exec_terminal_state(
                    row=row,
                    item_links=item_links,
                    derived_state=derived_state,
                    now=datetime(2026, 10, 8, tzinfo=timezone.utc),
                    trigger=f'unknown_evidence:{name}',
                )

                self.assertIsNotNone(audit)
                self.assertNotIn('acknowledged_revocation_stable', audit)
                self.assertEqual(
                    row.execution_status,
                    EXECUTION_STATUS_SUBMIT_FAILED,
                )

    def test_v4_completed_no_trade_family_uses_entry_reason_and_becomes_terminal(
        self,
    ) -> None:
        states = (
            (
                'candidate:next_open_buy',
                'next_open_buy',
                'entry-item',
                'next_open_buy_expired',
                {
                    'action': 'cancel',
                    'decision': 'expired',
                    'reason_code': 'missed_next_open_window',
                    'reason_family': 'execution_window',
                    'rejection_disposition': 'terminal',
                    'reason': 'The next-open entry window expired before submission.',
                },
            ),
            (
                'candidate:target_sell',
                'target_sell',
                'target-item',
                'not_applicable',
                None,
            ),
            (
                'candidate:stop_sell',
                'stop_sell',
                'stop-item',
                'not_applicable',
                None,
            ),
        )
        links = [
            {
                'core_item_key': core_item_key,
                'intent': intent,
                'exec_plan_id': 'exec-plan-uuid',
                'exec_plan_item_id': item_id,
                'ack_status': 'accepted',
                'remote_status': 'accepted',
                'remote_status_source': 'dispatch_ack',
                'response_item': {
                    'core_item_key': core_item_key,
                    'execution_plan_id': 'exec-plan-uuid',
                    'execution_plan_item_id': item_id,
                    'status': 'accepted',
                },
            }
            for core_item_key, intent, item_id, _, _ in states
        ]
        merged, mismatches = self.service._merge_exec_plan_readback_links(
            item_links=links,
            plans_result={
                'status': 'ok',
                'payload': {
                    'items': [
                        {
                            'execution_plan_id': 'exec-plan-uuid',
                            'external_plan_id': 'core-plan-id',
                            'status': 'completed_no_trade',
                            'item_status_counts': {
                                'next_open_buy_expired': 1,
                                'not_applicable': 2,
                            },
                            'item_states': [
                                {
                                    'core_item_key': core_item_key,
                                    'execution_plan_item_id': item_id,
                                    'status': status,
                                    'updated_at': '2026-10-08T00:00:00+00:00',
                                    'terminal': True,
                                    'decision': copy.deepcopy(decision),
                                }
                                for core_item_key, _, item_id, status, decision in states
                            ],
                        }
                    ]
                },
            },
        )
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=merged,
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )
        row = SimpleNamespace(
            eligibility_status='eligible',
            execution_status='delegated_for_exec_agent_sizing',
            validation_payload={'blocking_reasons': []},
            traceability_payload={
                'handoff_active': True,
                'exec_monitoring_active': True,
                'exec_agent_approved': True,
            },
        )
        audit = self.service._sync_exec_terminal_state(
            row=row,
            item_links=merged,
            derived_state=result,
            now=datetime(2026, 10, 8, tzinfo=timezone.utc),
            trigger='test_completed_no_trade',
        )

        self.assertEqual(mismatches, [])
        self.assertEqual(result['derived_status'], 'cancelled')
        self.assertEqual(result['terminal_decision']['reason_code'], 'missed_next_open_window')
        self.assertEqual(
            self.service._entry_link_active_statuses(item_links=merged),
            [],
        )
        self.assertIsNotNone(audit)
        self.assertEqual(audit['terminal_status'], 'next_open_buy_expired')
        self.assertEqual(audit['decision']['reason_code'], 'missed_next_open_window')
        self.assertEqual(row.eligibility_status, 'blocked')
        self.assertEqual(row.execution_status, EXECUTION_STATUS_SUBMIT_FAILED)
        self.assertFalse(row.traceability_payload['exec_monitoring_active'])
        self.assertFalse(row.traceability_payload['exec_agent_approved'])

    def test_v4_terminal_entry_rejection_outranks_cancelled_siblings(self) -> None:
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'intent': 'next_open_buy',
                    'remote_status': 'exec_agent_rejected',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'status': 'exec_agent_rejected',
                        'terminal': True,
                    },
                },
                {
                    'intent': 'target_sell',
                    'remote_status': 'not_applicable',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'status': 'not_applicable',
                        'terminal': True,
                    },
                },
                {
                    'intent': 'stop_sell',
                    'remote_status': 'not_applicable',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'status': 'not_applicable',
                        'terminal': True,
                    },
                },
            ],
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )

        self.assertEqual(result['derived_status'], 'failed')

    def test_v4_filled_entry_is_not_cancelled_by_non_applicable_siblings(
        self,
    ) -> None:
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'intent': 'next_open_buy',
                    'remote_status': 'filled',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {'status': 'filled', 'terminal': True},
                },
                {
                    'intent': 'target_sell',
                    'remote_status': 'not_applicable',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'status': 'not_applicable',
                        'terminal': True,
                    },
                },
                {
                    'intent': 'stop_sell',
                    'remote_status': 'not_applicable',
                    'remote_status_source': 'exec_item_state',
                    'current_state': {
                        'status': 'not_applicable',
                        'terminal': True,
                    },
                },
            ],
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )

        self.assertEqual(result['derived_status'], 'unknown')

    def test_v4_protective_exit_failure_is_selected_over_filled_entry(self) -> None:
        links = [
            {
                'intent': 'next_open_buy',
                'remote_status': 'filled',
                'remote_status_source': 'exec_item_state',
                'current_state': {'status': 'filled', 'terminal': True},
            },
            {
                'intent': 'target_sell',
                'remote_status': 'broker_order_failed',
                'remote_status_source': 'exec_item_state',
                'current_state': {
                    'status': 'broker_order_failed',
                    'terminal': True,
                    'decision': {
                        'reason_code': 'protective_exit_submission_failed',
                    },
                },
            },
        ]
        result = self.service._derive_remote_state(
            row=SimpleNamespace(symbol='005930'),
            item_links=links,
            orders=[],
            executions=[],
            positions=[],
            endpoint_statuses={
                'plans': 'ok',
                'orders': 'ok',
                'executions': 'ok',
                'positions': 'ok',
            },
        )
        row = SimpleNamespace(
            eligibility_status='eligible',
            execution_status='submitted',
            validation_payload={'blocking_reasons': []},
            traceability_payload={},
        )
        audit = self.service._sync_exec_terminal_state(
            row=row,
            item_links=links,
            derived_state=result,
            now=datetime(2026, 10, 8, tzinfo=timezone.utc),
            trigger='test_exit_failure',
        )

        self.assertEqual(result['derived_status'], 'failed')
        self.assertEqual(audit['terminal_status'], 'broker_order_failed')
        self.assertEqual(
            audit['decision']['reason_code'],
            'protective_exit_submission_failed',
        )

    def test_terminal_item_state_cannot_be_reopened_by_later_nonterminal_readback(
        self,
    ) -> None:
        link = {
            'core_item_key': 'candidate:next_open_buy',
            'exec_plan_id': 'exec-plan-uuid',
            'exec_plan_item_id': 'item-uuid',
            'ack_status': 'accepted',
            'remote_status': 'contract_quarantined',
            'remote_status_source': 'exec_item_state',
            'response_item': {
                'core_item_key': 'candidate:next_open_buy',
                'execution_plan_id': 'exec-plan-uuid',
                'execution_plan_item_id': 'item-uuid',
                'status': 'accepted',
            },
            'current_state': {
                'core_item_key': 'candidate:next_open_buy',
                'execution_plan_id': 'exec-plan-uuid',
                'execution_plan_item_id': 'item-uuid',
                'status': 'contract_quarantined',
                'terminal': True,
                'decision': {
                    'reason_code': 'execution_contract_identity_invalid',
                },
            },
        }

        merged, mismatches = self.service._merge_exec_plan_readback_links(
            item_links=[link],
            plans_result={
                'status': 'ok',
                'payload': {
                    'execution_plan_id': 'exec-plan-uuid',
                    'external_plan_id': 'core-plan-id',
                    'status': 'running',
                    'item_states': [
                        {
                            'core_item_key': 'candidate:next_open_buy',
                            'execution_plan_item_id': 'item-uuid',
                            'status': 'watch_sizing_pending',
                            'terminal': False,
                        }
                    ],
                },
            },
        )

        self.assertEqual(mismatches, [])
        self.assertEqual(merged[0]['remote_status'], 'contract_quarantined')
        self.assertEqual(
            merged[0]['current_state']['decision']['reason_code'],
            'execution_contract_identity_invalid',
        )

    def test_parent_manual_review_does_not_overwrite_live_protective_exit_states(
        self,
    ) -> None:
        states = (
            ('candidate:next_open_buy', 'entry-item', 'manual_review_required'),
            ('candidate:target_sell', 'target-item', 'target_sell_monitoring'),
            ('candidate:stop_sell', 'stop-item', 'stop_sell_monitoring'),
        )
        links = [
            {
                'core_item_key': core_item_key,
                'exec_plan_id': 'exec-plan-uuid',
                'exec_plan_item_id': item_id,
                'ack_status': 'accepted',
                'remote_status': 'accepted',
                'remote_status_source': 'dispatch_ack',
                'response_item': {
                    'core_item_key': core_item_key,
                    'execution_plan_id': 'exec-plan-uuid',
                    'execution_plan_item_id': item_id,
                    'status': 'accepted',
                },
            }
            for core_item_key, item_id, _ in states
        ]

        merged, mismatches = self.service._merge_exec_plan_readback_links(
            item_links=links,
            plans_result={
                'status': 'ok',
                'payload': {
                    'items': [
                        {
                            'execution_plan_id': 'exec-plan-uuid',
                            'external_plan_id': 'core-plan-id',
                            'status': 'manual_review_required',
                            'item_states': [
                                {
                                    'core_item_key': core_item_key,
                                    'execution_plan_item_id': item_id,
                                    'status': status,
                                    'terminal': status == 'manual_review_required',
                                }
                                for core_item_key, item_id, status in states
                            ],
                        }
                    ]
                },
            },
        )

        self.assertEqual(mismatches, [])
        self.assertEqual(
            [link['current_state']['status'] for link in merged],
            [
                'manual_review_required',
                'target_sell_monitoring',
                'stop_sell_monitoring',
            ],
        )

    def test_exec_runtime_activity_is_unknown_when_activity_summaries_are_omitted(self) -> None:
        runtime = {
            'status': 'ok',
            'payload': self._current_exec_payload(),
            'error': None,
        }

        result = self.service._exec_runtime_activity_detected(runtime=runtime)

        self.assertIsNone(result)

    def test_execution_identity_accepts_exact_broker_id_when_order_has_internal_id(self) -> None:
        executions, mismatches = self.service._match_remote_executions(
            row=SimpleNamespace(symbol='005930'),
            item_links=[{'exec_plan_item_id': 'exec-item-1'}],
            orders=[
                {
                    'id': 'internal-order-row-1',
                    'broker_order_id': 'broker-order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                }
            ],
            executions=[
                {
                    'id': 'execution-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'broker_order_id': 'broker-order-1',
                    'symbol': '005930',
                }
            ],
        )

        self.assertEqual([item['id'] for item in executions], ['execution-1'])
        self.assertEqual(mismatches, [])

    def test_execution_identity_accepts_exec_serializer_broker_order_pk_link(self) -> None:
        executions, mismatches = self.service._match_remote_executions(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'core_item_key': 'core-item-1',
                    'exec_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
            orders=[
                {
                    'broker_order_pk': 'internal-order-row-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'broker_order_id': 'broker-order-1',
                    'symbol': '005930',
                    'side': 'buy',
                    'source_identity': {
                        'source_plan_item_id': 'core-item-1',
                        'core_plan_item_id': 'core-item-1',
                    },
                }
            ],
            executions=[
                {
                    'execution_fill_id': 'execution-1',
                    'broker_order_pk': 'internal-order-row-1',
                    'symbol': '005930',
                    'side': 'buy',
                    'source_identity': {
                        'source_plan_item_id': 'core-item-1',
                        'core_plan_item_id': 'core-item-1',
                    },
                }
            ],
        )

        self.assertEqual(
            [item['execution_fill_id'] for item in executions],
            ['execution-1'],
        )
        self.assertEqual(mismatches, [])

    def test_execution_identity_rejects_conflicting_plan_item_aliases(self) -> None:
        executions, mismatches = self.service._match_remote_executions(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'exec_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
            orders=[
                {
                    'broker_order_pk': 'internal-order-row-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
            executions=[
                {
                    'execution_fill_id': 'execution-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'plan_item_id': 'foreign-exec-item',
                    'broker_order_pk': 'internal-order-row-1',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
        )

        self.assertEqual(executions, [])
        self.assertEqual(len(mismatches), 1)

    def test_execution_identity_rejects_order_ids_resolving_to_different_orders(
        self,
    ) -> None:
        executions, mismatches = self.service._match_remote_executions(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'exec_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                    'side': 'buy',
                },
                {
                    'exec_plan_item_id': 'exec-item-2',
                    'symbol': '005930',
                    'side': 'buy',
                },
            ],
            orders=[
                {
                    'broker_order_pk': 'internal-order-row-1',
                    'broker_order_id': 'broker-order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                    'side': 'buy',
                },
                {
                    'broker_order_pk': 'internal-order-row-2',
                    'broker_order_id': 'broker-order-2',
                    'execution_plan_item_id': 'exec-item-2',
                    'symbol': '005930',
                    'side': 'buy',
                },
            ],
            executions=[
                {
                    'execution_fill_id': 'execution-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'broker_order_id': 'broker-order-1',
                    'broker_order_pk': 'internal-order-row-2',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
        )

        self.assertEqual(executions, [])
        self.assertEqual(len(mismatches), 1)

    def test_order_identity_rejects_conflicting_plan_item_aliases(self) -> None:
        orders, mismatches = self.service._match_remote_orders(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'exec_plan_item_id': 'exec-item-1',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
            orders=[
                {
                    'broker_order_pk': 'internal-order-row-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'plan_item_id': 'foreign-exec-item',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
        )

        self.assertEqual(orders, [])
        self.assertEqual(len(mismatches), 1)

    def test_order_identity_rejects_conflicting_symbol_or_side(self) -> None:
        item_links = [
            {
                'exec_plan_item_id': 'exec-item-1',
                'symbol': '005930',
                'side': 'buy',
            }
        ]
        conflicting_orders = (
            {
                'id': 'order-wrong-symbol',
                'execution_plan_item_id': 'exec-item-1',
                'symbol': '000660',
                'side': 'buy',
            },
            {
                'id': 'order-wrong-side',
                'execution_plan_item_id': 'exec-item-1',
                'symbol': '005930',
                'side': 'sell',
            },
        )

        for order in conflicting_orders:
            with self.subTest(order_id=order['id']):
                orders, mismatches = self.service._match_remote_orders(
                    row=SimpleNamespace(symbol='005930'),
                    item_links=item_links,
                    orders=[order],
                )

                self.assertEqual(orders, [])
                self.assertEqual(len(mismatches), 1)

    def test_execution_identity_rejects_cross_linked_exact_item_and_order_ids(
        self,
    ) -> None:
        executions, mismatches = self.service._match_remote_executions(
            row=SimpleNamespace(symbol='005930'),
            item_links=[
                {
                    'exec_plan_item_id': 'exec-entry-item',
                    'symbol': '005930',
                    'side': 'buy',
                },
                {
                    'exec_plan_item_id': 'exec-target-item',
                    'symbol': '005930',
                    'side': 'sell',
                },
            ],
            orders=[
                {
                    'id': 'entry-order',
                    'execution_plan_item_id': 'exec-entry-item',
                    'symbol': '005930',
                    'side': 'buy',
                },
                {
                    'id': 'target-order',
                    'execution_plan_item_id': 'exec-target-item',
                    'symbol': '005930',
                    'side': 'sell',
                },
            ],
            executions=[
                {
                    'id': 'cross-linked-execution',
                    'execution_plan_item_id': 'exec-entry-item',
                    'broker_order_id': 'target-order',
                    'symbol': '005930',
                    'side': 'buy',
                }
            ],
        )

        self.assertEqual(executions, [])
        self.assertEqual(len(mismatches), 1)

    def test_daily_board_distinguishes_no_activity_from_unavailable_exec_activity(self) -> None:
        refresh_state = {
            'requested': False,
            'status': 'not_requested',
            'source': 'exec_readback',
        }
        active_payload = self._current_exec_payload()
        active_payload['symbols'] = {'filled_today': ['005930']}
        no_activity_payload = self._current_exec_payload()
        no_activity_payload['symbols'] = {
            'monitored': [],
            'scheduled_today': [],
            'filled_today': [],
            'failed_today': [],
            'positions': [],
        }
        no_activity_payload['counts'] = {'open_orders': 0, 'active_execution_jobs': 0}

        def build(payload: dict | None) -> dict:
            return self.service._build_runtime_daily_board_payload(
                market_date=date(2026, 8, 29),
                refresh=False,
                overview={
                    'integration': self.integration,
                    'runtime': {'status': 'ok', 'payload': payload, 'error': None},
                    'items': [],
                },
                refresh_state=refresh_state,
                served_from_cache=False,
            )

        with patch(
            'app.orders.exec_integration._utcnow',
            return_value=datetime(2026, 8, 29, 1, 0, tzinfo=timezone.utc),
        ):
            unavailable = build(active_payload)
            no_activity = build(no_activity_payload)
            unloaded = build(None)

        self.assertEqual(unavailable['items'], [])
        self.assertEqual(unavailable['activity_coverage']['status'], 'unavailable')
        self.assertEqual(unavailable['activity_coverage']['status_reason'], 'exec_activity_not_materialized')
        self.assertTrue(unavailable['activity_coverage']['exec_activity_detected'])
        self.assertEqual(unavailable['freshness']['status'], 'unavailable')
        self.assertEqual(no_activity['items'], [])
        self.assertEqual(no_activity['activity_coverage']['status'], 'no_activity')
        self.assertEqual(no_activity['activity_coverage']['status_reason'], 'no_exec_activity')
        self.assertFalse(no_activity['activity_coverage']['exec_activity_detected'])
        self.assertEqual(no_activity['freshness']['status'], 'fresh')
        self.assertEqual(unloaded['activity_coverage']['status'], 'unavailable')
        self.assertEqual(unloaded['activity_coverage']['status_reason'], 'runtime_read_model_unavailable')
        self.assertIsNone(unloaded['activity_coverage']['exec_activity_detected'])
        self.assertEqual(unloaded['freshness']['status'], 'unavailable')


class ExecIntegrationServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        AgentContextFeedback.__table__.create(bind=engine, checkfirst=True)
        AgentContextFeedbackFailure.__table__.create(bind=engine, checkfirst=True)
        AgentContextFeedbackRun.__table__.create(bind=engine, checkfirst=True)
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderDecisionSnapshot.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderExecutionPlanItem.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderPolicyVersion.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderActualExecutionOutcome.__table__.create(bind=engine, checkfirst=True)
        CurrentMarketSnapshotCache.__table__.create(bind=engine, checkfirst=True)
        Symbol.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.policy_version = f'signal_decider_policy_exec_{suffix.lower()}'
        self.account_reference = f'KIS:98{suffix[:6]}-01'
        self.symbol = f'T{suffix[:5]}'
        self.seeded_symbols = {self.symbol}
        self.main_client_order_id = f'SDNO-20260430-{self.symbol}'
        self.short_term_client_order_id = f'STDE-20260430-{self.symbol}'
        self.market_date = date(2026, 4, 29)
        self.target_trade_date = date(2026, 4, 30)
        self._previous_active_policy_states: list[
            tuple[str, datetime | None]
        ] = []
        self.original_settings = {
            'signal_decider_exec_integration_mode': settings.signal_decider_exec_integration_mode,
            'signal_decider_exec_base_url': settings.signal_decider_exec_base_url,
            'signal_decider_exec_bearer_token': settings.signal_decider_exec_bearer_token,
            'signal_decider_exec_dispatch_source': settings.signal_decider_exec_dispatch_source,
            'signal_decider_exec_runtime_mode': settings.signal_decider_exec_runtime_mode,
            'signal_decider_exec_request_timeout_seconds': settings.signal_decider_exec_request_timeout_seconds,
            'signal_decider_next_open_enabled': settings.signal_decider_next_open_enabled,
            'signal_decider_next_open_dry_run': settings.signal_decider_next_open_dry_run,
            'signal_decider_next_open_account_targets': settings.signal_decider_next_open_account_targets,
        }
        settings.signal_decider_exec_integration_mode = 'dispatch'
        settings.signal_decider_exec_base_url = 'http://exec.test'
        settings.signal_decider_exec_bearer_token = ''
        settings.signal_decider_exec_dispatch_source = 'kai-trading-core'
        settings.signal_decider_exec_runtime_mode = 'mock'
        settings.signal_decider_exec_request_timeout_seconds = 5.0
        settings.signal_decider_next_open_enabled = True
        settings.signal_decider_next_open_dry_run = False
        settings.signal_decider_next_open_account_targets = f'Main={self.account_reference}@1500000'
        self.client = FakeExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)
        self.utcnow_patcher = patch(
            'app.orders.exec_integration._utcnow',
            return_value=datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc),
        )
        self.utcnow_patcher.start()
        self.addCleanup(self.utcnow_patcher.stop)
        self.decision_quality_utcnow_patcher = patch(
            'app.agent_context.decision_input_quality._utcnow',
            return_value=datetime(2026, 4, 29, 0, 57, 10, tzinfo=timezone.utc),
        )
        self.decision_quality_utcnow_patcher.start()
        self.addCleanup(self.decision_quality_utcnow_patcher.stop)
        self.enrichment_preflight_patcher = patch(
            'app.orders.next_open_execution.inspect_pykrx_enrichment_preflight',
            side_effect=lambda **kwargs: {
                'market_date': str(kwargs.get('market_date') or self.market_date),
                'status': 'ready',
                'allowed': True,
                'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
                'failed_categories': [],
                'zero_coverage_categories': [],
                'category_row_counts': {
                    'sector_classification': 1,
                    'investor_flow_symbol': 1,
                    'short_selling': 1,
                },
            },
        )
        self.enrichment_preflight_mock = self.enrichment_preflight_patcher.start()
        self.addCleanup(self.enrichment_preflight_patcher.stop)

        def complete_agent_context(
            *,
            source_market_date,
            candidate_symbols=None,
            **_kwargs,
        ):
            return {
                'schema_version': 'agent_context_completeness_v1',
                'source_market_date': source_market_date.isoformat(),
                'candidate_symbols': sorted(set(candidate_symbols or [])),
                'candidate_set_basis': 'latest_signal_decider_snapshots',
                'status': 'complete',
                'cohort_status': 'complete',
                'has_incomplete_agent_context': False,
                'candidate_set_definitive': True,
                'authority_fingerprint': 'exec-integration-agent-context-authority',
                'latest_run': None,
                'failure_count': 0,
                'failure_groups': [],
            }

        self.agent_context_builder_patcher = patch(
            'app.orders.next_open_execution.SignalDeciderNextOpenExecutionService.'
            '_build_agent_context_completeness_context',
            side_effect=complete_agent_context,
        )
        self.agent_context_builder_patcher.start()
        self.addCleanup(self.agent_context_builder_patcher.stop)
        self.agent_context_from_state_patcher = patch(
            'app.orders.next_open_execution.SignalDeciderNextOpenExecutionService.'
            '_build_agent_context_completeness_from_state',
            side_effect=complete_agent_context,
        )
        self.agent_context_from_state_patcher.start()
        self.addCleanup(self.agent_context_from_state_patcher.stop)

        self.snapshot_generation_patcher = patch(
            'app.orders.next_open_execution.agent_feedback_orchestrator.'
            'revalidate_persisted_decider_snapshots',
            side_effect=self._valid_snapshot_generation_fixture,
        )
        self.snapshot_generation_mock = self.snapshot_generation_patcher.start()
        self.addCleanup(self.snapshot_generation_patcher.stop)
        self.calendar_client = FakeCalendarClient(
            [
                {'bass_dt': '20260429', 'opnd_yn': 'Y'},
                {'bass_dt': '20260430', 'opnd_yn': 'Y'},
            ]
        )
        set_krx_calendar_provider(KisKrxTradingCalendarProvider(client=self.calendar_client))
        self.addCleanup(reset_krx_calendar_provider)
        with session_scope() as session:
            active_policies = session.execute(
                select(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.status == 'active'
                )
            ).scalars().all()
            for active_policy in active_policies:
                self._previous_active_policy_states.append(
                    (active_policy.policy_version, active_policy.retired_at)
                )
                active_policy.status = 'retired'
                active_policy.retired_at = datetime.now(timezone.utc)
            session.flush()
            session.add(
                SignalDeciderPolicyVersion(
                    policy_version=self.policy_version,
                    status='active',
                    change_summary='Exec integration test active policy.',
                    rule_bundle_payload={
                        'schema_version': 'signal_decider_policy_bundle_v1'
                    },
                    metadata_payload={
                        'schema_version': 'signal_decider_policy_metadata_v1',
                        'activation_mode': 'manual_only',
                        'candidate_activation_required': True,
                    },
                    created_by='test',
                    activated_by='test',
                    activated_at=datetime(
                        2026, 4, 29, 0, 0, tzinfo=timezone.utc
                    ),
                )
            )
        self._seed_symbol()
        self.snapshot_id = self._seed_snapshot()
        self.plan_item_id = self._seed_plan_item()

    def tearDown(self) -> None:
        reset_krx_calendar_provider()
        for key, value in self.original_settings.items():
            setattr(settings, key, value)
        with session_scope() as session:
            session.execute(
                delete(SignalDeciderActualExecutionOutcome).where(
                    SignalDeciderActualExecutionOutcome.account_reference == self.account_reference
                )
            )
            session.execute(
                delete(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference
                )
            )
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.policy_version == self.policy_version
                )
            )
            session.execute(
                delete(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version
                    == self.policy_version
                )
            )
            session.flush()
            for policy_version, retired_at in self._previous_active_policy_states:
                previous_policy = session.execute(
                    select(SignalDeciderPolicyVersion).where(
                        SignalDeciderPolicyVersion.policy_version
                        == policy_version
                    )
                ).scalar_one_or_none()
                if previous_policy is not None:
                    previous_policy.status = 'active'
                    previous_policy.retired_at = retired_at
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.agent_name == 'short-term-decider',
                    SignalDeciderDecisionSnapshot.market_date == self.market_date,
                )
            )
            session.execute(
                delete(CurrentMarketSnapshotCache).where(
                    CurrentMarketSnapshotCache.cache_key == 'dashboard'
                )
            )
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.seeded_symbols)))

    def test_dispatch_revision_fingerprint_covers_server_owned_order_material(self) -> None:
        row = SimpleNamespace(
            id=71,
            execution_window='next_open',
            decision_snapshot_id=41,
            decision_agent_name='signal-decider',
            broker_name='kis',
            target_trade_date=date(2026, 4, 30),
            source_market_date=date(2026, 4, 29),
            account_reference='KIS:98123456-01',
            account_label='Main',
            symbol='005930',
            policy_version='policy-v1',
            planned_side='buy',
            planned_order_type='market',
            planned_time_in_force='day',
            allocation_budget_krw=Decimal('1500000'),
            estimated_reference_price=50000,
            planned_quantity=Decimal('30'),
            estimated_notional_krw=Decimal('1500000'),
            client_order_id='SDNO-20260430-005930',
            validation_payload={'preopen_policy_fingerprint': 'fingerprint-v1'},
            traceability_payload={
                'market': 'KRX',
                'target_price': 55000,
                'stop_loss_price': 47000,
            },
            submission_payload={'readback_only': 'ignored'},
        )
        baseline = _row_dispatch_revision_fingerprint(row)
        mutations = {
            'id': 72,
            'execution_window': 'intraday',
            'decision_snapshot_id': 42,
            'decision_agent_name': 'short-term-decider',
            'broker_name': 'other',
            'target_trade_date': date(2026, 5, 1),
            'source_market_date': date(2026, 4, 28),
            'account_reference': 'KIS:98999999-01',
            'account_label': 'Secondary',
            'symbol': '000660',
            'policy_version': 'policy-v2',
            'planned_side': 'sell',
            'planned_order_type': 'limit',
            'planned_time_in_force': 'ioc',
            'allocation_budget_krw': Decimal('900000'),
            'estimated_reference_price': 51000,
            'planned_quantity': Decimal('17'),
            'estimated_notional_krw': Decimal('867000'),
            'client_order_id': 'SDNO-20260430-000660',
            'validation_payload': {'preopen_policy_fingerprint': 'fingerprint-v2'},
            'traceability_payload': {
                'market': 'US',
                'target_price': 56000,
                'stop_loss_price': 46000,
            },
        }

        for field, value in mutations.items():
            with self.subTest(field=field):
                revised = copy.deepcopy(row)
                setattr(revised, field, value)
                self.assertNotEqual(
                    _row_dispatch_revision_fingerprint(revised),
                    baseline,
                )

        readback_only_revision = copy.deepcopy(row)
        readback_only_revision.submission_payload = {'readback_only': 'new-value'}
        self.assertEqual(
            _row_dispatch_revision_fingerprint(readback_only_revision),
            baseline,
        )

    async def test_exec_payload_builder_rejects_us_route_before_krw_sizing_is_serialized(self) -> None:
        route_markers = (
            {'market': 'US'},
            {'exchange': 'NASDAQ'},
            {'exchange': 'NASD'},
            {'currency': 'USD'},
            {'asset_class': 'us_equity'},
            {'session_policy': {'timezone': 'America/New_York'}},
        )
        route_keys = {'market', 'exchange', 'currency', 'asset_class', 'session_policy'}
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            base_traceability = {
                key: value
                for key, value in row.traceability_payload.items()
                if key not in route_keys
            }

        for marker in route_markers:
            with self.subTest(marker=marker):
                with session_scope() as session:
                    row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                    row.traceability_payload = {**base_traceability, **marker}

                rows = self.service._load_next_open_rows(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                )
                snapshots = self.service._load_snapshots(rows)

                with self.assertRaisesRegex(
                    SignalDeciderExecPayloadContractError,
                    'unsupported_us_equity_exec_agent_sizing',
                ):
                    self.service._build_exec_plan_payload(
                        rows=rows,
                        snapshots=snapshots,
                        trigger='us_route_safety_test',
                    )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='us_route_safety_test',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['failed_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'contract_error')
            self.assertEqual(
                row.submission_payload['contract_error']['reason_code'],
                'unsupported_us_equity_exec_agent_sizing',
            )

    async def test_unsupported_us_row_does_not_block_same_account_krx_dispatch(self) -> None:
        domestic_symbol = f'K{uuid4().hex[:5].upper()}'
        domestic_id = self._seed_signal_candidate(
            symbol=domestic_symbol,
            name_ko='Domestic candidate',
            decision_run_id='mixed-route-domestic-run',
            client_order_id=f'MIXED-KRX-{uuid4().hex[:8]}',
        )
        with session_scope() as session:
            us_row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            us_row.traceability_payload = {
                **us_row.traceability_payload,
                'currency': 'USD',
            }

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='mixed_route_safety_test',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(
            {item['symbol'] for item in self.client.post_calls[0]['items']},
            {domestic_symbol},
        )
        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        with session_scope() as session:
            us_row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            domestic_row = session.get(SignalDeciderExecutionPlanItem, domestic_id)
            self.assertEqual(us_row.execution_status, 'contract_error')
            self.assertEqual(
                domestic_row.execution_status,
                EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
            )

    def _seed_snapshot(self) -> int:
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=self.symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id='exec-run-1',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=54000,
                expected_return_pct=4.5,
                stop_loss_price=47000,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.5,
                conviction_score_10=6.1,
                thesis_summary='Dispatch to exec.',
                supporting_evidence_payload=['Breadth improved.'],
                decision_risks_payload=['Leadership remained narrow.'],
                invalidation_conditions_payload=['Close below support.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={'gate_checks': {'missing_upstream_roles': []}},
                input_feature_snapshot_payload={'technical': {'trend_state': 'uptrend'}},
                request_payload={
                    'signal_decider_policy': (
                        self._signal_decider_policy_request_fixture()
                    ),
                    'market_regime_context': self._market_regime_context(),
                    'symbol_day_context': {
                        'company': {'market': 'KOSPI'},
                    },
                },
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    def _signal_decider_policy_request_fixture(self) -> dict:
        return {
            'current_version': self.policy_version,
            'rule_bundle': {
                'schema_version': 'signal_decider_policy_bundle_v1'
            },
        }

    def _market_regime_context(
        self,
        *,
        posture: str = 'caution',
        market_regime: str | None = None,
        quantity_multiplier: float | None = None,
    ) -> dict:
        resolved_market_regime = market_regime or ('defensive' if posture == 'caution' else 'normal')
        resolved_multiplier = (
            quantity_multiplier
            if quantity_multiplier is not None
            else 0.5
            if posture == 'caution'
            else 1.0
        )
        sizing_window = (
            {
                'name': 'aggressive_open',
                'timezone': 'Asia/Seoul',
                'valid_from': f'{self.target_trade_date.isoformat()}T09:00:00+09:00',
                'valid_until': f'{self.target_trade_date.isoformat()}T09:20:00+09:00',
                'quantity_multiplier_cap': 2.0,
                'entry_priority_boost': True,
                'exec_enforced': True,
            }
            if resolved_market_regime == 'aggressive_open'
            else None
        )
        return {
            'schema_version': 'market_regime_context_v1',
            'market_date': self.market_date.isoformat(),
            'market': 'KOSPI',
            'posture': posture,
            'market_regime': resolved_market_regime,
            'risk_mode': resolved_market_regime,
            'regime_confidence': 0.81 if resolved_market_regime == 'aggressive_open' else 0.60,
            'regime_reasons': ['breadth_strong'] if resolved_market_regime == 'aggressive_open' else ['weak_market_breadth'],
            'reason_code': 'weak_market_breadth' if posture == 'caution' else 'market_regime_normal',
            'reason_summary': 'Market breadth is weak.',
            'source_status': {
                'overall': 'available',
                'market_breadth': 'available',
                'sector_context': 'available',
                'relative_strength_context': 'available',
            },
            'freshness': {'overall_status': 'fresh'},
            'market_breadth': {'up_ratio': 0.42, 'down_ratio': 0.52},
            'sector_context': {'sector_name': 'Semiconductors', 'weak_sector': False},
            'relative_strength_context': {'return_percentile': 0.72, 'volume_percentile': 0.68},
            'recommended_exec_action': 'downsize' if posture == 'caution' else 'allow',
            'recommended_sizing_multiplier': resolved_multiplier,
            'quantity_multiplier': resolved_multiplier,
            'entry_priority_boost': resolved_market_regime == 'aggressive_open',
            'valid_until': sizing_window['valid_until'] if sizing_window else None,
            'sizing_window': sizing_window,
            'exec_guardrails': {
                'core_bypasses_safety_gates': False,
                'exec_must_enforce_window': resolved_market_regime == 'aggressive_open',
                'must_respect': ['kill_switch', 'account_drawdown_gate', 'symbol_caps', 'max_notional_caps', 'stop_loss_required'],
            },
            'missing_fields': [],
        }

    @staticmethod
    def _valid_snapshot_generation_fixture(
        *,
        market_date,
        snapshots,
        full_source_revalidation_actionable_only=False,
        policy_context_as_of_date=None,
    ) -> dict:
        snapshot_ids = sorted(int(snapshot.id) for snapshot in snapshots)
        identity_by_id = {
            int(snapshot.id): {
                'symbol': str(snapshot.symbol).strip().upper(),
                'agent_name': str(snapshot.agent_name).strip(),
            }
            for snapshot in snapshots
        }
        return {
            'schema_version': 'persisted_decider_generation_validation_v1',
            'market_date': str(market_date),
            'valid_snapshot_ids': snapshot_ids,
            'invalid_snapshot_ids': [],
            'validations': [
                {
                    'snapshot_id': snapshot_id,
                    **identity_by_id[snapshot_id],
                    'status': 'valid',
                    'reason': 'test_generation_unchanged',
                }
                for snapshot_id in snapshot_ids
            ],
            'generation_fingerprint': (
                'test-snapshot-generation:'
                + ','.join(str(snapshot_id) for snapshot_id in snapshot_ids)
            ),
        }

    def _stored_agent_context_fixture(
        self,
        *,
        snapshots: list[tuple[int, str, str]],
        candidate_symbols: list[str],
    ) -> dict:
        base = {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': self.market_date.isoformat(),
            'candidate_symbols': sorted(set(candidate_symbols)),
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': 'complete',
            'cohort_status': 'complete',
            'has_incomplete_agent_context': False,
            'candidate_set_definitive': True,
            'authority_fingerprint': 'exec-integration-agent-context-authority',
            'latest_run': None,
            'failure_count': 0,
            'failure_groups': [],
        }
        generation = self._valid_snapshot_generation_fixture(
            market_date=self.market_date,
            snapshots=[
                SimpleNamespace(
                    id=snapshot_id,
                    agent_name=agent_name,
                    symbol=symbol,
                )
                for snapshot_id, agent_name, symbol in snapshots
            ],
        )
        return (
            signal_decider_next_open_execution_service
            ._attach_snapshot_generation_validation(
                agent_context_completeness=base,
                generation_validation=generation,
            )
        )

    def _signal_dispatch_validation(
        self,
        *,
        snapshot_id: int | None = None,
        symbol: str | None = None,
    ) -> dict:
        market_date = self.market_date.isoformat()
        observed_at = f'{market_date}T09:57:00+09:00'
        resolved_snapshot_id = snapshot_id or self.snapshot_id
        resolved_symbol = symbol or self.symbol
        with session_scope() as session:
            policy_authority = _load_signal_decider_policy_authority(
                session,
                policy_versions=[self.policy_version],
            )[self.policy_version]
        return {
            'execution_cost_evidence_policy_version': 'next_open_cost_evidence_v1',
            'preopen_policy_fingerprint': _next_open_policy_fingerprint(
                [
                    NextOpenAccountTarget(
                        label='Main',
                        account_reference=self.account_reference,
                        budget_krw=Decimal('1500000'),
                    )
                ]
            ),
            'agent_context_completeness': self._stored_agent_context_fixture(
                snapshots=[
                    (resolved_snapshot_id, 'signal-decider', resolved_symbol)
                ],
                candidate_symbols=[resolved_symbol],
            ),
            'policy_context': {
                'policy_version': self.policy_version,
                'status': 'active',
                'activation_mode': 'manual_only',
                'authority': policy_authority,
            },
            'blocking_reasons': [],
            'warnings': [],
            'pre_decider_enrichment_context': {
                'schema_version': 'pre_decider_enrichment_v1',
                'quote_orderbook_snapshot': {
                    'status': 'available',
                    'snapshot_type': 'current_quote_cache',
                    'market_date': market_date,
                    'as_of_market_date': market_date,
                    'quote_freshness_status': 'fresh',
                    'quote_freshness_timestamp': observed_at,
                    'current_price': 50000,
                    'bid_price': 49975,
                    'ask_price': 50000,
                    'spread_points': 25,
                    'spread_bps': 5,
                    'spread_source_context': 'quote_context',
                    'spread_component_source_context': {
                        field: 'quote_context'
                        for field in (
                            'current_price',
                            'bid_price',
                            'ask_price',
                            'spread_points',
                            'spread_bps',
                        )
                    },
                    'source': {'quote_context': 'kis'},
                    'freshness': {
                        'quote_context': {
                            'freshness_status': 'fresh',
                            'provenance_status': 'provider_sourced',
                            'as_of_market_date': market_date,
                            'as_of_timestamp': observed_at,
                            'source_provider': 'kis',
                            'source_tables': ['current_quote_cache', 'quote_snapshots'],
                        },
                    },
                },
            },
            'performance_gate': {
                'expected_return_economics': {
                    'status': 'ready',
                    'expected_slippage_bps': '5',
                    'expected_slippage_basis': 'quote_spread_bps',
                    'server_non_spread_round_trip_cost_basis': 'deferred_to_exec_final_full_cost_check',
                    'estimated_total_transaction_cost_bps': None,
                },
            },
        }

    def _closed_market_pre_decider_context(self) -> dict:
        return {
            'schema_version': 'pre_decider_enrichment_v1',
            'market_data_phase_contract': {
                'schema_version': 'market_data_phase_contract_v1',
                'recommendation_basis': 'latest_completed_eod',
                'realtime_quote_orderbook_required': False,
                'eod_reference_price_accepted': True,
                'not_applicable_realtime_is_expected': True,
            },
            'quote_orderbook_snapshot': {
                'status': 'stale_eod',
                'snapshot_type': 'eod_stale',
                'quote_freshness_status': 'stale',
                'as_of_market_date': self.market_date.isoformat(),
                'as_of_timestamp': f'{self.market_date.isoformat()}T15:30:00+09:00',
                'spread_bps': None,
            },
            'trading_value': {
                'status': 'available',
                'source': 'raw',
                'raw_trading_value_krw': 8_000_000_000,
                'trading_value_krw': 8_000_000_000,
            },
        }

    def _seed_short_term_snapshot(
        self,
        *,
        tactical_action: str = 'watch_for_trigger',
        trigger_price: int | None = 50500,
        chase_limit_price: int | None = 51500,
        target_price: int | None = 54000,
        stop_loss_price: int | None = 49200,
        request_payload: dict | None = None,
        input_feature_snapshot_payload: dict | None = None,
        response_payload_extra: dict | None = None,
        confidence_score_10: float = 7.3,
    ) -> int:
        generic_action = 'buy' if tactical_action in {'scalp_buy_candidate', 'watch_for_trigger'} else tactical_action
        if generic_action not in {'buy', 'wait', 'reject'}:
            generic_action = 'wait'
        resolved_request_payload = copy.deepcopy(request_payload or {})
        symbol_day_context = resolved_request_payload.setdefault(
            'symbol_day_context',
            {},
        )
        if isinstance(symbol_day_context, dict):
            company = symbol_day_context.setdefault('company', {})
            if (
                not symbol_day_context.get('market')
                and isinstance(company, dict)
                and not company.get('market')
            ):
                company['market'] = 'KOSPI'
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=self.symbol,
                agent_name='short-term-decider',
                agent_version='test-agent',
                decision_run_id='short-run-1',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version='short_term_decider_contract_v1',
                decision_action=generic_action,
                preferred_action=generic_action,
                next_day_open_action=generic_action,
                entry_price_basis='trigger_limit',
                target_price=target_price,
                expected_return_pct=None,
                stop_loss_price=stop_loss_price,
                suggested_holding_period='1-2 trading days',
                confidence_score_10=confidence_score_10,
                conviction_score_10=6.5,
                thesis_summary='Dispatch guarded short-term setup.',
                supporting_evidence_payload=['Opening setup stayed constructive.'],
                decision_risks_payload=['Gap risk remained elevated.'],
                invalidation_conditions_payload=['Trigger reclaim failed.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={
                    'schema_version': 'short_term_decider_trace_v1',
                    'gate_checks': {
                        'missing_upstream_roles': [],
                        'decision_support_only': True,
                        'allow_execution_logic': False,
                    },
                },
                input_feature_snapshot_payload=input_feature_snapshot_payload
                or {'meta': {'source_agent_name': 'short-term-decider'}},
                request_payload=resolved_request_payload,
                response_payload={
                    'role': 'short-term-decider',
                    'agent_name': 'short-term-decider',
                    'tactical_action': tactical_action,
                    'preferred_action': tactical_action,
                    'trigger_price': trigger_price,
                    'chase_limit_price': chase_limit_price,
                    'target_price': target_price,
                    'stop_loss_price': stop_loss_price,
                    'expected_holding_period': '1-2 trading days',
                    'confidence_score_10': confidence_score_10,
                    'conviction_score_10': 6.5,
                    **(response_payload_extra or {}),
                },
                response_summary_payload={
                    'model': 'openclaw/short-term-decider',
                    'decision_provenance': {
                        'model_identity': build_observed_model_identity(
                            role='short-term-decider',
                            expected_model='openai/gpt-6.1-sol',
                            actual_provider='openai',
                            actual_model='gpt-6.1-sol',
                        ),
                    },
                },
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_symbol(self, *, symbol: str | None = None, name_ko: str = 'Samsung Electronics') -> None:
        resolved_symbol = symbol or self.symbol
        self.seeded_symbols.add(resolved_symbol)
        with session_scope() as session:
            session.execute(delete(Symbol).where(Symbol.symbol == resolved_symbol))
            session.add(
                Symbol(
                    symbol=resolved_symbol,
                    standard_code=None,
                    name_ko=name_ko,
                    market='KOSPI',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                )
            )
            session.flush()

    def _seed_signal_candidate(
        self,
        *,
        symbol: str,
        name_ko: str,
        decision_run_id: str,
        client_order_id: str,
        request_payload: dict | None = None,
        input_feature_snapshot_payload: dict | None = None,
    ) -> int:
        self._seed_symbol(symbol=symbol, name_ko=name_ko)
        resolved_request_payload = copy.deepcopy(
            request_payload
            or {
                'signal_decider_policy': (
                    self._signal_decider_policy_request_fixture()
                )
            }
        )
        symbol_day_context = resolved_request_payload.setdefault(
            'symbol_day_context',
            {},
        )
        if isinstance(symbol_day_context, dict):
            company = symbol_day_context.setdefault('company', {})
            if (
                not symbol_day_context.get('market')
                and isinstance(company, dict)
                and not company.get('market')
            ):
                company['market'] = 'KOSPI'
        with session_scope() as session:
            snapshot = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id=decision_run_id,
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=130000,
                expected_return_pct=3.1,
                stop_loss_price=112000,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.1,
                conviction_score_10=6.0,
                thesis_summary='Second dispatch candidate.',
                supporting_evidence_payload=['Liquidity remained strong.'],
                decision_risks_payload=['Gap risk.'],
                invalidation_conditions_payload=['Lose support.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={'gate_checks': {'missing_upstream_roles': []}},
                input_feature_snapshot_payload=input_feature_snapshot_payload or {'technical': {'trend_state': 'uptrend'}},
                request_payload=resolved_request_payload,
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(snapshot)
            session.flush()
            plan_item = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name='signal-decider',
                decision_snapshot_id=snapshot.id,
                order_request_id=None,
                broker_order_row_id=None,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=symbol,
                policy_version=self.policy_version,
                planned_side='buy',
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw='1500000',
                estimated_reference_price=120000,
                planned_quantity='5',
                estimated_notional_krw='600000',
                eligibility_status='eligible',
                execution_status='planned',
                client_order_id=client_order_id,
                validation_payload=self._signal_dispatch_validation(
                    snapshot_id=int(snapshot.id),
                    symbol=symbol,
                ),
                traceability_payload={
                    'decision_snapshot_id': snapshot.id,
                    'decision_run_id': decision_run_id,
                    'policy_version': self.policy_version,
                    'target_trade_date': self.target_trade_date.isoformat(),
                    'source_market_date': self.market_date.isoformat(),
                    'account_reference': self.account_reference,
                    'account_label': 'Main',
                    'allocation_budget_krw': '1500000',
                    'target_price': 130000,
                    'stop_loss_price': 112000,
                    'suggested_holding_period': '3-10 trading days',
                    'signal_traceability': {'gate_checks': {'missing_upstream_roles': []}},
                },
                submission_payload=None,
                first_ready_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                submitted_at=None,
            )
            session.add(plan_item)
            session.flush()
            return plan_item.id

    def _seed_plan_item(self) -> int:
        with session_scope() as session:
            row = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name='signal-decider',
                decision_snapshot_id=self.snapshot_id,
                order_request_id=None,
                broker_order_row_id=None,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=self.symbol,
                policy_version=self.policy_version,
                planned_side='buy',
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw='1500000',
                estimated_reference_price=50000,
                planned_quantity='5',
                estimated_notional_krw='250000',
                eligibility_status='eligible',
                execution_status='planned',
                client_order_id=self.main_client_order_id,
                validation_payload=self._signal_dispatch_validation(),
                traceability_payload={
                    'decision_snapshot_id': self.snapshot_id,
                    'decision_run_id': 'exec-run-1',
                    'policy_version': self.policy_version,
                    'target_trade_date': self.target_trade_date.isoformat(),
                    'source_market_date': self.market_date.isoformat(),
                    'account_reference': self.account_reference,
                    'account_label': 'Main',
                    'allocation_budget_krw': '1500000',
                    'target_price': 54000,
                    'stop_loss_price': 47000,
                    'suggested_holding_period': '3-10 trading days',
                    'signal_traceability': {'gate_checks': {'missing_upstream_roles': []}},
                },
                submission_payload=None,
                first_ready_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                submitted_at=None,
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_short_term_plan_item(
        self,
        *,
        snapshot_id: int,
        trigger_price: int | None = 50500,
        chase_limit_price: int | None = 51500,
        target_price: int | None = 54000,
        stop_loss_price: int | None = 49200,
    ) -> int:
        with session_scope() as session:
            shared_agent_context = self._stored_agent_context_fixture(
                snapshots=[
                    (
                        self.snapshot_id,
                        'signal-decider',
                        self.symbol,
                    ),
                    (
                        snapshot_id,
                        'short-term-decider',
                        self.symbol,
                    ),
                ],
                candidate_symbols=[self.symbol],
            )
            signal_row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            if signal_row is not None:
                signal_validation = copy.deepcopy(
                    signal_row.validation_payload or {}
                )
                signal_validation['agent_context_completeness'] = copy.deepcopy(
                    shared_agent_context
                )
                signal_row.validation_payload = signal_validation
            row = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name='short-term-decider',
                decision_snapshot_id=snapshot_id,
                order_request_id=None,
                broker_order_row_id=None,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=self.symbol,
                policy_version='short_term_decider_contract_v1',
                planned_side='buy',
                planned_order_type='limit',
                planned_time_in_force='day',
                allocation_budget_krw='1500000',
                estimated_reference_price=51500,
                planned_quantity='29',
                estimated_notional_krw='1493500',
                eligibility_status='eligible',
                execution_status='planned',
                client_order_id=self.short_term_client_order_id,
                validation_payload={
                    'preopen_policy_fingerprint': _next_open_policy_fingerprint(
                        [
                            NextOpenAccountTarget(
                                label='Main',
                                account_reference=self.account_reference,
                                budget_krw=Decimal('1500000'),
                            )
                        ]
                    ),
                    'agent_context_completeness': shared_agent_context,
                    'blocking_reasons': [],
                    'warnings': [],
                    'decision_agent_name': 'short-term-decider',
                    'short_term': {
                        'raw_action': 'watch_for_trigger',
                        'tactical_action': 'watch_for_trigger',
                        'generic_action': 'buy',
                        'handoff_intent': 'intraday_watch_buy',
                        'entry_execution_mode': 'pending_trigger_limit',
                        'trigger_price': trigger_price,
                        'chase_limit_price': chase_limit_price,
                        'target_price': target_price,
                        'stop_loss_price': stop_loss_price,
                        'expected_holding_period': '1-2 trading days',
                    },
                },
                traceability_payload={
                    'agent_name': 'short-term-decider',
                    'decision_agent_name': 'short-term-decider',
                    'decision_snapshot_id': snapshot_id,
                    'source_feedback_id': None,
                    'decision_run_id': 'short-run-1',
                    'policy_version': 'short_term_decider_contract_v1',
                    'target_trade_date': self.target_trade_date.isoformat(),
                    'source_market_date': self.market_date.isoformat(),
                    'account_reference': self.account_reference,
                    'account_label': 'Main',
                    'allocation_budget_krw': '1500000',
                    'target_price': target_price,
                    'stop_loss_price': stop_loss_price,
                    'expected_holding_period': '1-2 trading days',
                    'raw_action': 'watch_for_trigger',
                    'tactical_action': 'watch_for_trigger',
                    'generic_action': 'buy',
                    'handoff_intent': 'intraday_watch_buy',
                    'handoff_active': target_price is not None and stop_loss_price is not None,
                    'exec_monitoring_active': target_price is not None and stop_loss_price is not None,
                    'approval_state': 'pending_exec_agent' if target_price is not None and stop_loss_price is not None else 'blocked',
                    'trigger_price': trigger_price,
                    'chase_limit_price': chase_limit_price,
                    'entry_execution_mode': 'pending_trigger_limit',
                    'decision_traceability': {'schema_version': 'short_term_decider_trace_v1'},
                },
                submission_payload=None,
                first_ready_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                submitted_at=None,
            )
            session.add(row)
            session.flush()
            return row.id

    def _mark_plan_item_blocked_with_readback(
        self,
        *,
        readback: dict,
        dispatch_status: str = 'accepted',
        execution_status: str = 'planned',
    ) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = dict(row.submission_payload or {})
            exec_dispatch = dict(submission.get('exec_dispatch') or {})
            exec_dispatch['dispatch_status'] = dispatch_status
            exec_dispatch['readback'] = readback
            submission['exec_dispatch'] = exec_dispatch
            row.submission_payload = submission
            row.eligibility_status = 'blocked'
            row.execution_status = execution_status
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

    async def _readback_stale_short_term_watch(
        self,
        *,
        exec_status: str = 'watch_monitoring_active',
        response_item_status: str | None = None,
        core_execution_status: str = 'planned',
        blocking_reason_code: str = 'manual_recovery_gate_block',
    ) -> tuple[dict, dict, int]:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        short_group = next(
            group
            for group in dispatch_result['dispatch']['groups']
            if group['decision_agent_name'] == 'short-term-decider'
        )
        short_plan_id = short_group['plan_id']
        short_post_call = next(
            payload
            for payload in self.client.post_calls
            if payload['metadata']['decision_agent_name'] == 'short-term-decider'
        )
        short_entry_item = next(
            item
            for item in short_post_call['items']
            if item.get('intent') == EXEC_INTENT_INTRADAY_WATCH_BUY
        )
        with session_scope() as session:
            persisted_short_row = session.get(
                SignalDeciderExecutionPlanItem,
                short_plan_item_id,
            )
            short_entry_link = next(
                link
                for link in persisted_short_row.submission_payload[
                    'exec_dispatch'
                ]['item_links']
                if link['intent'] == EXEC_INTENT_INTRADAY_WATCH_BUY
            )
        original_blocking_reason = {
            'code': blocking_reason_code,
            'message': 'Original core quality gate remained blocked before exec restore.',
        }
        original_warning = {
            'code': 'original_soft_gate_warning',
            'message': 'Original soft gate warning is preserved.',
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            row.eligibility_status = 'blocked'
            row.execution_status = core_execution_status
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [original_blocking_reason],
                'warnings': [original_warning],
            }
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'exec_monitoring_active': False,
                'approval_state': 'blocked',
            }
            session.flush()

        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'monitoring': {'alive': True},
        }
        self.client.plan_response = {
            'items': [
                {
                    'external_plan_id': short_plan_id,
                    'status': 'accepted',
                    'items': [
                        {
                            'core_item_key': short_entry_item['core_item_key'],
                            'execution_plan_id': short_entry_link['exec_plan_id'],
                            'execution_plan_item_id': short_entry_link[
                                'exec_plan_item_id'
                            ],
                            'symbol': self.symbol,
                            'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                            'status': response_item_status or exec_status,
                            'monitoring_status': exec_status,
                        }
                    ],
                }
            ]
        }
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )
        short_item = next(
            item
            for item in overview['items']
            if item['plan_item_id'] == short_plan_item_id
        )
        return overview, short_item, short_plan_item_id

    async def test_dispatch_next_open_plan_blocks_when_source_pykrx_enrichment_missing(self) -> None:
        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
            'failed_categories': ['investor_flow_symbol', 'short_selling'],
            'zero_coverage_categories': ['investor_flow_symbol', 'short_selling'],
            'category_row_counts': {
                'sector_classification': 1,
                'investor_flow_symbol': 0,
                'short_selling': 0,
            },
        }
        with patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=blocked_preflight):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test',
            )

        self.assertEqual(result['summary']['reason'], 'pykrx_enrichment_preflight_blocked')
        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['translated_item_count'], 0)
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(result['summary']['enrichment_preflight_status'], 'blocked')
        self.assertEqual(result['enrichment_preflight']['zero_coverage_categories'], ['investor_flow_symbol', 'short_selling'])
        self.assertEqual(self.client.post_calls, [])
        blocked_item = next(
            item for item in result['items'] if item['plan_item_id'] == self.plan_item_id
        )
        self.assertEqual(blocked_item['eligibility_status'], 'blocked')
        blocking_codes = [entry['code'] for entry in blocked_item['validation']['blocking_reasons']]
        self.assertIn('pykrx_enrichment_preflight_blocked', blocking_codes)

    async def test_dispatch_next_open_plan_continues_for_candidate_enriched_short_term_rows(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        with session_scope() as session:
            short_row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.decision_snapshot_id == short_snapshot_id,
                    SignalDeciderExecutionPlanItem.decision_agent_name == 'short-term-decider',
                )
            ).scalar_one()
            short_validation = dict(short_row.validation_payload or {})
            short_validation['candidate_pykrx_enrichment'] = {
                'market_date': self.market_date.isoformat(),
                'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
                'present_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
                'missing_categories': [],
                'allowed': True,
                'source': 'candidate_symbol_pykrx_enrichment',
            }
            short_validation['warnings'] = [
                {
                    'code': 'global_pykrx_enrichment_preflight_partial',
                    'message': 'Global PyKRX enrichment coverage is partial, but this candidate is complete.',
                }
            ]
            short_row.validation_payload = short_validation
            session.flush()
        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
            'failed_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
            'zero_coverage_categories': [],
            'category_row_counts': {
                'sector_classification': 2699,
                'investor_flow_symbol': 2672,
                'short_selling': 2806,
            },
        }

        with patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=blocked_preflight):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test',
            )

        self.assertTrue(result['summary']['triggered'])
        self.assertEqual(result['summary']['translated_item_count'], 3)
        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertIn('dispatch_note', result['enrichment_preflight'])
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.post_calls[0]['metadata']['decision_agent_name'], 'short-term-decider')
        groups_by_agent = {group['decision_agent_name']: group for group in result['dispatch']['groups']}
        self.assertEqual(groups_by_agent['signal-decider']['dispatch_status'], 'skipped')
        self.assertEqual(groups_by_agent['short-term-decider']['dispatch_status'], 'accepted')
        items_by_agent = {item['decision_agent_name']: item for item in result['items']}
        self.assertEqual(items_by_agent['signal-decider']['eligibility_status'], 'blocked')
        self.assertEqual(items_by_agent['short-term-decider']['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    async def test_short_term_entry_quality_blocks_scalp_when_signal_decider_waits(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            tactical_action='scalp_buy_candidate',
            request_payload={
                'symbol_day_context': {
                    'technical_snapshot': {
                        'trend_state': 'uptrend',
                        'breakout_20d': True,
                        'breakdown_20d': False,
                    },
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.82,
                            'volume_percentile': 0.76,
                        },
                    },
                },
                'liquidity_context': {
                    'turnover': {
                        'trading_value_source': 'raw',
                        'raw_trading_value_krw': 9_000_000_000,
                    },
                },
            },
            response_payload_extra={
                'thesis_summary': 'Immediate scalp setup with momentum expansion.',
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={
                    'action': 'wait',
                    'decision_snapshot_id': self.snapshot_id,
                    'policy_version': self.policy_version,
                },
                tactical_action='scalp_buy_candidate',
            )

        blocking_codes = {entry['code'] for entry in blocks}
        self.assertIn('signal_decider_wait_blocks_scalp_buy', blocking_codes)
        self.assertEqual(warnings, [])
        self.assertEqual(feature_snapshot['signal_decider_crosscheck']['action'], 'wait')

    async def test_short_term_entry_quality_warns_low_buy_quality_score(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload={
                'symbol_day_context': {
                    'technical_snapshot': {
                        'trend_state': 'uptrend',
                        'breakout_20d': True,
                        'breakdown_20d': False,
                    },
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.86,
                            'volume_percentile': 0.80,
                        },
                    },
                    'decision_input_quality_context': {
                        'final_judgment': {
                            'buy_quality_score': 59,
                            'decision_band': 'small_probe',
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 12_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertEqual(blocks, [])
        self.assertIn('short_term_buy_quality_score_warning', {entry['code'] for entry in warnings})
        self.assertEqual(Decimal(str(feature_snapshot['buy_quality_score'])), Decimal('59'))

    async def test_short_term_entry_quality_warns_missing_objective_selection_data(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertEqual(blocks, [])
        codes = {entry['code'] for entry in warnings}
        self.assertIn('short_term_relative_strength_unverified_warning', codes)
        self.assertIn('short_term_relative_volume_unverified_warning', codes)
        self.assertIn('short_term_liquidity_unverified_warning', codes)
        self.assertIsNone(feature_snapshot['return_percentile'])
        self.assertIsNone(feature_snapshot['volume_percentile'])

    async def test_short_term_entry_quality_warns_weak_relative_strength(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload={
                'symbol_day_context': {
                    'technical_snapshot': {
                        'trend_state': 'uptrend',
                        'breakout_20d': True,
                        'breakdown_20d': False,
                    },
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.55,
                            'volume_percentile': 0.88,
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 12_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertEqual(blocks, [])
        self.assertIn('short_term_weak_relative_strength_warning', {entry['code'] for entry in warnings})
        self.assertEqual(Decimal(str(feature_snapshot['return_percentile'])), Decimal('0.55'))

    async def test_short_term_entry_quality_blocks_very_weak_relative_strength(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload={
                'symbol_day_context': {
                    'technical_snapshot': {
                        'trend_state': 'uptrend',
                        'breakout_20d': True,
                        'breakdown_20d': False,
                    },
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.22,
                            'volume_percentile': 0.88,
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 12_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertIn('short_term_very_weak_relative_strength_block', {entry['code'] for entry in blocks})
        self.assertEqual(warnings, [])
        self.assertEqual(Decimal(str(feature_snapshot['return_percentile'])), Decimal('0.22'))

    async def test_short_term_entry_quality_blocks_poor_reward_risk(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            trigger_price=50500,
            chase_limit_price=50800,
            target_price=52000,
            stop_loss_price=49200,
            request_payload={
                'symbol_day_context': {
                    'technical_snapshot': {
                        'trend_state': 'uptrend',
                        'breakout_20d': True,
                        'breakdown_20d': False,
                    },
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.82,
                            'volume_percentile': 0.76,
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 15_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['거래량 동반 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertIn('short_term_reward_risk_ratio_block', {entry['code'] for entry in blocks})
        self.assertEqual(warnings, [])
        self.assertLess(Decimal(str(feature_snapshot['reward_risk_ratio'])), Decimal('1.80'))

    async def test_dispatch_next_open_plan_fails_closed_on_calendar_fallback(self) -> None:
        set_krx_calendar_provider(
            KisKrxTradingCalendarProvider(client=FailingCalendarClient())
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test',
        )

        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['reason'], 'calendar_unavailable')
        self.assertEqual(result['summary']['calendar_source'], 'exchange_calendars')
        self.assertEqual(self.client.post_calls, [])

    async def test_dispatch_next_open_plan_translates_and_persists_exec_linkage(self) -> None:
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['translated_item_count'], 3)
        self.assertTrue(result['summary']['delegated_to_exec'])
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(self.client.dispatch_events, ['plan', 'activation'])
        payload = self.client.post_calls[0]
        activation = self.client.activation_calls[0]
        activation_bindings = {
            item['risk_context']['_core_activation'][
                'authority_generation_fingerprint'
            ]
            for item in payload['items']
        }
        self.assertEqual(len(activation_bindings), 1)
        authority_generation_fingerprint = activation_bindings.pop()
        self.assertRegex(authority_generation_fingerprint, r'^[0-9a-f]{64}$')
        self.assertEqual(
            activation['authority_generation_fingerprint'],
            authority_generation_fingerprint,
        )
        self.assertEqual(
            activation['dispatch_material_fingerprint'],
            payload['metadata']['dispatch_material_fingerprint'],
        )
        self.assertEqual(
            {
                (
                    item['core_item_key'],
                    item['decision_snapshot_id'],
                )
                for item in activation['items']
            },
            {
                (
                    item['core_item_key'],
                    item['decision_snapshot_id'],
                )
                for item in payload['items']
            },
        )
        self.assertEqual(payload['market_date'], '2026-04-30')
        intents = [item['intent'] for item in payload['items']]
        self.assertEqual(
            intents,
            [EXEC_INTENT_NEXT_OPEN_BUY, EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL],
        )
        for item in payload['items']:
            self.assertEqual(
                {
                    key: item[key]
                    for key in ('asset_class', 'market', 'exchange', 'currency', 'broker', 'session_policy')
                },
                {
                    'asset_class': 'stock',
                    'market': 'KRX',
                    'exchange': 'KRX',
                    'currency': 'KRW',
                    'broker': 'kis',
                    'session_policy': {
                        'session': 'regular',
                        'timezone': 'Asia/Seoul',
                        'allow_extended_hours': False,
                    },
                },
            )
        entry_item = payload['items'][0]
        self.assertEqual(entry_item['quantity_policy']['type'], 'exec_agent_sized')
        self.assertNotIn('quantity', entry_item['quantity_policy'])
        self.assertEqual(Decimal(str(entry_item['quantity_policy']['budget_krw'])), Decimal('1500000'))
        self.assertIsNone(entry_item['quantity_policy']['estimated_notional_krw'])
        self.assertEqual(entry_item['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(
            entry_item['risk_context']['max_holding_close_policy'],
            {
                'enabled': False,
                'type': 'none',
                'intent': None,
                'max_holding_trading_sessions': None,
                'reason': 'general_signal_decider_uses_decider_holding_context_no_short_term_forced_close',
            },
        )
        self.assertNotIn('max_holding_close_sell', [item['intent'] for item in payload['items']])
        self.assertEqual(
            entry_item['risk_context']['decider_handoff_context']['classification'],
            EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
        )
        entry_key = entry_item['core_item_key']
        for exit_item in payload['items'][1:]:
            self.assertEqual(exit_item['quantity_policy']['type'], 'position_exit_from_entry')
            self.assertEqual(exit_item['quantity_policy']['linked_entry_item_key'], entry_key)
            self.assertNotIn('quantity', exit_item['quantity_policy'])

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertIn('plan_id', exec_dispatch)
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertEqual(len(exec_dispatch['item_links']), 3)

    async def test_dispatch_flushes_buy_to_wait_revocation_before_any_new_plan(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_buy_to_wait_revocation',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            exec_dispatch = submission['exec_dispatch']
            item_links = copy.deepcopy(exec_dispatch['item_links'])
            plan_id = exec_dispatch['plan_id']
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submitted_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                **submission,
                'revision_history': [{'decision_snapshot_id': str(self.snapshot_id)}],
                'revocation': {
                    'status': 'pending',
                    'reason_code': 'latest_snapshot_not_actionable',
                    'requested_at': '2026-04-30T00:00:00+00:00',
                    'prior_decision_snapshot_id': str(self.snapshot_id),
                    'replacement_decision_snapshot_id': str(self.snapshot_id + 1),
                    'replacement_action': 'wait',
                    'replacement_actionable': False,
                    'plan_id': plan_id,
                    'destination_binding': copy.deepcopy(
                        exec_dispatch['destination_binding']
                    ),
                    'item_links': item_links,
                },
            }

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test_revocation',
        )

        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.submission_payload['revocation']['status'], 'acknowledged')
            self.assertEqual(len(row.submission_payload['revocation_history']), 1)

    async def test_enrichment_block_preserves_and_flushes_live_market_revocation_to_exec(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_live_market_revocation',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            exec_dispatch = submission['exec_dispatch']
            item_links = copy.deepcopy(exec_dispatch['item_links'])
            plan_id = exec_dispatch['plan_id']
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submitted_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                **submission,
                'revision_history': [{'decision_snapshot_id': str(self.snapshot_id)}],
                'revocation': {
                    'status': 'pending',
                    'reason_code': 'current_live_market_risk_off',
                    'requested_at': '2026-04-30T00:00:00+00:00',
                    'prior_decision_snapshot_id': str(self.snapshot_id),
                    'replacement_decision_snapshot_id': 'observation:risk-off:1',
                    'replacement_action': 'buy',
                    'replacement_actionable': False,
                    'plan_id': plan_id,
                    'destination_binding': copy.deepcopy(
                        exec_dispatch['destination_binding']
                    ),
                    'item_links': item_links,
                },
            }

        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'required_categories': ['investor_flow_symbol'],
            'failed_categories': ['investor_flow_symbol'],
            'zero_coverage_categories': ['investor_flow_symbol'],
            'category_row_counts': {'investor_flow_symbol': 0},
        }
        with patch(
            'app.orders.next_open_execution.inspect_pykrx_enrichment_preflight',
            return_value=blocked_preflight,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=True,
                dry_run=False,
                force=False,
                trigger='test_live_market_risk_off_revocation',
            )

        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(
            self.client.revocation_calls[0]['reason_code'],
            'current_live_market_risk_off',
        )
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(
            result['summary']['reason'],
            'pykrx_enrichment_preflight_blocked',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.submission_payload['revocation']['status'], 'acknowledged')

    async def test_critical_market_evidence_failure_revokes_buy_and_preserves_protective_exits(
        self,
    ) -> None:
        dispatched = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='seed_accepted_exec_authority',
        )
        self.assertEqual(dispatched['summary']['dispatched_group_count'], 1)

        async def revoke_with_protective_exits(payload: dict) -> dict:
            self.client.revocation_calls.append(copy.deepcopy(payload))
            item_acks = [
                {
                    'core_item_key': item['core_item_key'],
                    'status': (
                        'revoked'
                        if item['core_item_key'].endswith(':next_open_buy')
                        else 'preserved_protective_exit'
                    ),
                    'execution_plan_id': payload['plan_id'],
                    'execution_plan_item_id': item['execution_plan_item_id'],
                }
                for item in payload['items']
            ]
            return {
                'plan_id': payload['plan_id'],
                'execution_plan_id': payload['plan_id'],
                'contract_ack': {
                    'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'capability': EXECUTION_REVOCATION_CAPABILITY,
                    'plan_id': payload['plan_id'],
                    'status': 'revoked',
                },
                'item_acks': item_acks,
                'reason_code': 'current_live_market_evidence_unavailable',
                'revoked_item_count': 1,
                'manual_review_item_count': 0,
                'preserved_item_count': 2,
            }

        with patch.object(
            self.client,
            'post_execution_plan_revocation',
            side_effect=revoke_with_protective_exits,
        ):
            result = await self.service.revoke_delegated_buys_for_unavailable_market_evidence(
                target_trade_date=self.target_trade_date,
                trigger='background_runner:market_snapshot_refresh_if_due_failed',
            )

        self.assertEqual(result['staged_count'], 1)
        self.assertEqual(result['revocations'][0]['status'], 'revoked')
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(
            self.client.revocation_calls[0]['reason_code'],
            'current_live_market_evidence_unavailable',
        )
        self.assertEqual(
            {
                item['core_item_key'].rsplit(':', 1)[-1]
                for item in self.client.revocation_calls[0]['items']
            },
            {'next_open_buy', 'target_sell', 'stop_sell'},
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            response_items = row.submission_payload['revocation_history'][-1]['response'][
                'item_acks'
            ]
            self.assertEqual(
                {
                    item['core_item_key'].rsplit(':', 1)[-1]: item['status']
                    for item in response_items
                },
                {
                    'next_open_buy': 'revoked',
                    'target_sell': 'preserved_protective_exit',
                    'stop_sell': 'preserved_protective_exit',
                },
            )

    async def test_revocation_ack_with_wrong_reason_keeps_pending_authority_blocked(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_wrong_reason_revocation',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            exec_dispatch = submission['exec_dispatch']
            item_links = copy.deepcopy(exec_dispatch['item_links'])
            plan_id = exec_dispatch['plan_id']
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submitted_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                **submission,
                'revocation': {
                    'status': 'pending',
                    'reason_code': 'current_live_market_risk_off',
                    'requested_at': '2026-04-30T00:00:00+00:00',
                    'prior_decision_snapshot_id': str(self.snapshot_id),
                    'replacement_decision_snapshot_id': 'observation:risk-off:reason',
                    'replacement_action': 'buy',
                    'replacement_actionable': False,
                    'plan_id': plan_id,
                    'destination_binding': copy.deepcopy(
                        exec_dispatch['destination_binding']
                    ),
                    'item_links': item_links,
                },
            }

        original_revoke = self.client.post_execution_plan_revocation

        async def respond_with_wrong_reason(payload: dict) -> dict:
            response = await original_revoke(payload)
            return {
                **response,
                'reason_code': 'latest_snapshot_replaced',
            }

        with patch.object(
            self.client,
            'post_execution_plan_revocation',
            side_effect=respond_with_wrong_reason,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_wrong_revocation_reason_ack',
            )

        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['revoked_group_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_pending')
            self.assertEqual(row.submission_payload['revocation']['status'], 'pending')
            self.assertNotIn('revocation_history', row.submission_payload)

    async def test_revocation_state_mismatch_persists_terminal_manual_review_ack(
        self,
    ) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_manual_review_revocation',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            exec_dispatch = submission['exec_dispatch']
            item_links = copy.deepcopy(exec_dispatch['item_links'])
            plan_id = exec_dispatch['plan_id']
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submitted_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                **submission,
                'revocation': {
                    'status': 'pending',
                    'reason_code': 'current_live_market_evidence_unavailable',
                    'requested_at': '2026-04-30T00:00:00+00:00',
                    'prior_decision_snapshot_id': str(self.snapshot_id),
                    'replacement_decision_snapshot_id': (
                        'observation:evidence-unavailable:manual-review'
                    ),
                    'replacement_action': 'reject',
                    'replacement_actionable': False,
                    'plan_id': plan_id,
                    'destination_binding': copy.deepcopy(
                        exec_dispatch['destination_binding']
                    ),
                    'item_links': item_links,
                },
            }

        original_revoke = self.client.post_execution_plan_revocation

        async def respond_manual_review(payload: dict) -> dict:
            accepted_response = await original_revoke(payload)
            manual_review_response = {
                **accepted_response,
                'contract_ack': {
                    **accepted_response['contract_ack'],
                    'status': 'manual_review_required',
                },
                'item_acks': [
                    {
                        **item,
                        'status': 'manual_review_required',
                    }
                    for item in accepted_response['item_acks']
                ],
                'reason_code': 'revocation_state_mismatch',
                'revoked_item_count': 0,
                'manual_review_item_count': len(accepted_response['item_acks']),
                'preserved_item_count': 0,
            }
            raise SignalDeciderExecClientError(
                'Exec requires manual review',
                code='conflict',
                retryable=False,
                status_code=409,
                payload=manual_review_response,
            )

        with patch.object(
            self.client,
            'post_execution_plan_revocation',
            side_effect=respond_manual_review,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_manual_review_revocation',
            )

        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['revoked_group_count'], 0)
        self.assertEqual(result['summary']['revocation_manual_review_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['revocation']['status'],
                'manual_review_required',
            )
            self.assertEqual(
                row.submission_payload['revocation_history'][-1]['response'][
                    'reason_code'
                ],
                'revocation_state_mismatch',
            )
            manual_review_submission = copy.deepcopy(row.submission_payload)
            signal_decider_next_open_execution_service.stage_exec_contract_quarantine_retirement(
                row=row,
                item_links=[row.submission_payload['exec_dispatch']['item_links'][0]],
                quarantine_observed_at='2026-04-30T00:01:00+00:00',
                trigger='repeated_exec_readback',
                now=datetime(2026, 4, 30, 0, 1, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(row.submission_payload, manual_review_submission)

    async def test_revocation_not_found_ack_enters_manual_review_and_blocks_replacement(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_not_found_revocation',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            exec_dispatch = submission['exec_dispatch']
            item_links = copy.deepcopy(exec_dispatch['item_links'])
            plan_id = exec_dispatch['plan_id']
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submitted_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                **submission,
                'revision_history': [{'decision_snapshot_id': str(self.snapshot_id)}],
                'revocation': {
                    'status': 'pending',
                    'reason_code': 'latest_snapshot_replaced',
                    'requested_at': '2026-04-30T00:00:00+00:00',
                    'prior_decision_snapshot_id': str(self.snapshot_id),
                    'replacement_decision_snapshot_id': str(self.snapshot_id + 1),
                    'replacement_action': 'buy',
                    'replacement_actionable': True,
                    'plan_id': plan_id,
                    'destination_binding': copy.deepcopy(
                        exec_dispatch['destination_binding']
                    ),
                    'item_links': item_links,
                },
            }

        async def respond_not_found(payload: dict) -> dict:
            self.client.revocation_calls.append(payload)
            not_found_response = {
                'plan_id': payload['plan_id'],
                'execution_plan_id': None,
                'contract_ack': {
                    'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'capability': EXECUTION_REVOCATION_CAPABILITY,
                    'plan_id': payload['plan_id'],
                    'status': 'not_found',
                },
                'item_acks': [
                    {
                        'core_item_key': item['core_item_key'],
                        'status': 'not_found',
                        'execution_plan_id': None,
                        'execution_plan_item_id': item['execution_plan_item_id'],
                    }
                    for item in payload['items']
                ],
                'reason_code': 'execution_plan_not_found',
                'revoked_item_count': 0,
                'manual_review_item_count': 0,
                'preserved_item_count': 0,
            }
            raise SignalDeciderExecClientError(
                'Execution plan was not found',
                code='not_found',
                retryable=False,
                status_code=404,
                payload=not_found_response,
            )

        with patch.object(
            self.client,
            'post_execution_plan_revocation',
            side_effect=respond_not_found,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_not_found_revocation',
            )

        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['revoked_group_count'], 0)
        self.assertEqual(result['summary']['revocation_manual_review_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(row.decision_snapshot_id, self.snapshot_id)
            self.assertIsNotNone(row.submitted_at)
            self.assertIn('exec_dispatch', row.submission_payload)
            self.assertEqual(
                row.submission_payload['revocation']['status'],
                'manual_review_required',
            )
            self.assertEqual(
                row.submission_payload['revocation']['ack_status'],
                'manual_review_required',
            )
            self.assertEqual(
                row.submission_payload['revocation_history'][-1]['response']['contract_ack']['status'],
                'not_found',
            )
            self.assertEqual(
                row.submission_payload['revocation_history'][-1]['response'][
                    'reason_code'
                ],
                'execution_plan_not_found',
            )

    async def test_revocation_transport_status_must_match_ack_outcome(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_revocation_transport_contract',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            item_links = copy.deepcopy(
                row.submission_payload['exec_dispatch']['item_links']
            )
            signal_decider_next_open_execution_service.stage_exec_contract_quarantine_retirement(
                row=row,
                item_links=item_links,
                quarantine_observed_at='2026-04-30T00:01:00+00:00',
                trigger='exec_contract_readback',
                now=datetime(2026, 4, 30, 0, 1, 1, tzinfo=timezone.utc),
            )

        original_revoke = self.client.post_execution_plan_revocation

        async def respond_success_with_manual_review(payload: dict) -> dict:
            response = await original_revoke(payload)
            return {
                **response,
                'contract_ack': {
                    **response['contract_ack'],
                    'status': 'manual_review_required',
                },
                'item_acks': [
                    {**item, 'status': 'manual_review_required'}
                    for item in response['item_acks']
                ],
                'reason_code': 'revocation_state_mismatch',
                'revoked_item_count': 0,
                'manual_review_item_count': len(response['item_acks']),
            }

        async def respond_error_with_revoked_ack(
            payload: dict,
            *,
            status_code: int,
        ) -> dict:
            response = await original_revoke(payload)
            raise SignalDeciderExecClientError(
                'Exec transport status conflicts with ACK outcome',
                code='conflict' if status_code == 409 else 'not_found',
                retryable=False,
                status_code=status_code,
                payload=response,
            )

        async def respond_404_with_revoked_ack(payload: dict) -> dict:
            return await respond_error_with_revoked_ack(payload, status_code=404)

        async def respond_409_with_revoked_ack(payload: dict) -> dict:
            return await respond_error_with_revoked_ack(payload, status_code=409)

        response_cases = (
            (
                'successful manual-review ACK',
                respond_success_with_manual_review,
                'successful execution revocation response must be revoked',
            ),
            (
                '404 revoked ACK',
                respond_404_with_revoked_ack,
                'HTTP 404 execution revocation response must be not_found',
            ),
            (
                '409 revoked ACK',
                respond_409_with_revoked_ack,
                'HTTP 409 execution revocation response must be',
            ),
        )
        for label, response_side_effect, expected_error in response_cases:
            with self.subTest(case=label), patch.object(
                self.client,
                'post_execution_plan_revocation',
                side_effect=response_side_effect,
            ):
                result = await self.service._flush_pending_exec_revocations(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    integration=self.service.integration_status(),
                    trigger='test_revocation_transport_contract',
                )

            self.assertEqual(result[0]['status'], 'contract_error')
            self.assertIn(expected_error, result[0]['error']['message'])
            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                self.assertEqual(
                    row.execution_status,
                    EXECUTION_STATUS_REVOCATION_PENDING,
                )
                self.assertEqual(
                    row.submission_payload['revocation']['status'],
                    'pending',
                )
                self.assertNotIn('revocation_history', row.submission_payload)

        self.assertEqual(len(self.client.revocation_calls), 3)
        self.assertEqual(
            self.client.revocation_calls,
            [self.client.revocation_calls[0]] * 3,
        )

    async def test_transient_revocation_exposure_scope_busy_keeps_exact_retry_pending(
        self,
    ) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            force=True,
            trigger='seed_quarantine_revocation_retry',
        )
        self.client.post_calls.clear()
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            item_links = copy.deepcopy(
                row.submission_payload['exec_dispatch']['item_links']
            )
            execution_plan_id = item_links[0]['exec_plan_id']
            signal_decider_next_open_execution_service.stage_exec_contract_quarantine_retirement(
                row=row,
                item_links=item_links,
                quarantine_observed_at='2026-04-30T00:01:00+00:00',
                trigger='exec_contract_readback',
                now=datetime(2026, 4, 30, 0, 1, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(
                row.execution_status,
                EXECUTION_STATUS_REVOCATION_PENDING,
            )

        async def respond_scope_busy(payload: dict) -> dict:
            self.client.revocation_calls.append(payload)
            blocked_response = {
                'plan_id': payload['plan_id'],
                'execution_plan_id': execution_plan_id,
                'contract_ack': {
                    'schema_version': EXECUTION_REVOCATION_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'capability': EXECUTION_REVOCATION_CAPABILITY,
                    'plan_id': payload['plan_id'],
                    'status': 'blocked',
                },
                'item_acks': [
                    {
                        'core_item_key': item['core_item_key'],
                        'status': 'blocked',
                        'execution_plan_id': execution_plan_id,
                        'execution_plan_item_id': item['execution_plan_item_id'],
                    }
                    for item in payload['items']
                ],
                'reason_code': 'revocation_exposure_scope_busy',
                'revoked_item_count': 0,
                'manual_review_item_count': 0,
                'preserved_item_count': 0,
            }
            raise SignalDeciderExecClientError(
                'Execution exposure scope is busy',
                code='conflict',
                retryable=True,
                status_code=409,
                payload=blocked_response,
            )

        with patch.object(
            self.client,
            'post_execution_plan_revocation',
            side_effect=respond_scope_busy,
        ):
            blocked = await self.service._flush_pending_exec_revocations(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                integration=self.service.integration_status(),
                trigger='test_revocation_exposure_scope_busy',
            )

        self.assertEqual(
            blocked,
            [
                {
                    'row_id': self.plan_item_id,
                    'status': 'blocked',
                    'reason_code': 'revocation_exposure_scope_busy',
                    'retryable': True,
                    'trigger': 'test_revocation_exposure_scope_busy',
                }
            ],
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(
                row.execution_status,
                EXECUTION_STATUS_REVOCATION_PENDING,
            )
            self.assertEqual(row.submission_payload['revocation']['status'], 'pending')
            self.assertNotIn('revocation_history', row.submission_payload)

        retried = await self.service._flush_pending_exec_revocations(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            integration=self.service.integration_status(),
            trigger='test_quarantine_revocation_retry',
        )

        self.assertEqual(retried[0]['status'], 'revoked')
        self.assertEqual(len(self.client.revocation_calls), 2)
        self.assertEqual(
            self.client.revocation_calls[0],
            self.client.revocation_calls[1],
        )
        self.assertEqual(
            self.client.revocation_calls[1]['reason_code'],
            'exec_contract_quarantined',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                row.submission_payload['revocation_history'][-1]['request'],
                self.client.revocation_calls[1],
            )

    async def test_dispatch_persistence_never_overwrites_newer_revocation_state(self) -> None:
        detached_rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        snapshots = self.service._load_snapshots(detached_rows)
        payload, row_links = self.service._build_exec_plan_payload(
            rows=detached_rows,
            snapshots=snapshots,
            trigger='race_test',
        )
        dispatch_revision_fingerprints = {
            int(row.id): _row_dispatch_revision_fingerprint(row)
            for row in detached_rows
        }
        response = await self.client.post_execution_plan(payload)
        pending_submission = {
            'marker': 'newer-revocation-must-win',
            'revocation': {
                'status': 'pending',
                'plan_id': 'already-handed-off-plan',
                'replacement_decision_snapshot_id': str(self.snapshot_id + 1),
            },
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submission_payload = pending_submission

        success = self.service._persist_dispatch_success(
            rows=detached_rows,
            submitted_revision_fingerprints=dispatch_revision_fingerprints,
            row_links=row_links,
            plan_id=payload['plan_id'],
            fingerprint='race-fingerprint',
            payload=payload,
            response_payload=response,
            trigger='race_test',
        )
        self.assertEqual(success['dispatch_status'], 'revocation_pending')
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_pending')
            self.assertEqual(row.submission_payload, pending_submission)

        self.service._persist_dispatch_failure(
            rows=detached_rows,
            submitted_revision_fingerprints=dispatch_revision_fingerprints,
            row_links=row_links,
            plan_id=payload['plan_id'],
            fingerprint='race-fingerprint',
            payload=payload,
            trigger='race_test',
            status='unavailable',
            error={'code': 'timeout', 'message': 'ambiguous remote result'},
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_pending')
            self.assertEqual(row.submission_payload, pending_submission)

    async def test_equal_authority_fingerprint_is_not_labeled_after_core_revision(
        self,
    ) -> None:
        detached_rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        snapshots = self.service._load_snapshots(detached_rows)
        payload, row_links = self.service._build_exec_plan_payload(
            rows=detached_rows,
            snapshots=snapshots,
            trigger='equal_revision_failure_test',
        )
        dispatch_revision_fingerprints = {
            int(row.id): _row_dispatch_revision_fingerprint(row)
            for row in detached_rows
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.execution_status = 'submitted'
            row.submission_payload = {'marker': 'status-only-transition'}

        self.service._persist_dispatch_failure(
            rows=detached_rows,
            submitted_revision_fingerprints=dispatch_revision_fingerprints,
            row_links=row_links,
            plan_id=payload['plan_id'],
            fingerprint=_fingerprint_exec_plan(payload),
            payload=payload,
            trigger='equal_revision_failure_test',
            status='unavailable',
            error={
                'code': 'unavailable',
                'message': 'ambiguous remote result',
                'retryable': True,
            },
            remote_acceptance_ambiguous=True,
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            reconciliation = row.submission_payload['dispatch_reconciliation']
            self.assertEqual(
                reconciliation['submitted_dispatch_revision_fingerprint'],
                reconciliation['current_dispatch_revision_fingerprint'],
            )
            self.assertEqual(
                reconciliation['reason_code'],
                'ambiguous_remote_acceptance',
            )

    async def test_dispatch_ack_revokes_remote_authority_after_material_row_revision(self) -> None:
        original_post_execution_plan = self.client.post_execution_plan

        async def mutate_row_before_ack(payload: dict) -> dict:
            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                row.allocation_budget_krw = Decimal('900000')
                row.validation_payload = {
                    **(row.validation_payload or {}),
                    'preopen_policy_fingerprint': 'new-policy-fingerprint',
                }
                row.traceability_payload = {
                    **(row.traceability_payload or {}),
                    'allocation_budget_krw': '900000',
                }
            return await original_post_execution_plan(payload)

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=mutate_row_before_ack,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_material_revision_race',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(result['dispatch']['groups'][0]['dispatch_status'], 'revocation_pending')
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.allocation_budget_krw, Decimal('900000'))
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'planned')
            self.assertNotIn('exec_dispatch', row.submission_payload)
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'policy_scope_changed',
            )

    async def test_dispatch_ack_revokes_remote_authority_when_policy_retires_during_post(
        self,
    ) -> None:
        original_post_execution_plan = self.client.post_execution_plan

        async def retire_policy_before_ack(payload: dict) -> dict:
            with session_scope() as session:
                policy = session.execute(
                    select(SignalDeciderPolicyVersion).where(
                        SignalDeciderPolicyVersion.policy_version
                        == self.policy_version
                    )
                ).scalar_one()
                policy.status = 'retired'
                policy.retired_at = datetime(
                    2026, 4, 30, 0, 0, 1, tzinfo=timezone.utc
                )
            return await original_post_execution_plan(payload)

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=retire_policy_before_ack,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_policy_retirement_race',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(
            result['dispatch']['groups'][0]['dispatch_status'],
            'revocation_pending',
        )
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'policy_scope_changed',
            )
            self.assertEqual(row.submission_payload['revocation']['status'], 'acknowledged')
            self.assertEqual(len(row.submission_payload['revocation_history']), 1)

    async def test_activation_fence_rejects_policy_change_after_plan_ack(
        self,
    ) -> None:
        original_activate = self.service._activate_core_confirmed_dispatch

        async def retire_policy_before_activation(**kwargs):
            with session_scope() as session:
                policy = session.execute(
                    select(SignalDeciderPolicyVersion).where(
                        SignalDeciderPolicyVersion.policy_version
                        == self.policy_version
                    )
                ).scalar_one()
                policy.status = 'retired'
                policy.retired_at = datetime(
                    2026, 4, 30, 0, 0, 2, tzinfo=timezone.utc
                )
            return await original_activate(**kwargs)

        with patch.object(
            self.service,
            '_activate_core_confirmed_dispatch',
            side_effect=retire_policy_before_activation,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_post_plan_ack_policy_retirement',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(
            result['dispatch']['groups'][0]['dispatch_status'],
            'revocation_pending',
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'blocked')

    async def test_predispatch_generation_fence_blocks_new_wait_without_post(self) -> None:
        with session_scope() as session:
            prior = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            values = {
                column.name: copy.deepcopy(getattr(prior, column.name))
                for column in SignalDeciderDecisionSnapshot.__table__.columns
                if column.name not in {'id', 'created_at', 'updated_at'}
            }
            values.update(
                {
                    'feedback_received_at': datetime(
                        2026,
                        4,
                        29,
                        6,
                        1,
                        tzinfo=timezone.utc,
                    ),
                    'decision_action': 'wait',
                    'preferred_action': 'wait',
                    'next_day_open_action': 'wait',
                    'response_payload': {'next_day_open_action': 'wait'},
                }
            )
            session.add(SignalDeciderDecisionSnapshot(**values))

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test_predispatch_new_wait_generation',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['dispatch_status'],
            'skipped',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(
                row.validation_payload['blocking_reasons'][0]['code'],
                'latest_snapshot_not_actionable',
            )

    async def test_predispatch_generation_fence_blocks_new_risk_off_snapshot_without_post(
        self,
    ) -> None:
        observed_at = datetime(
            2026,
            4,
            30,
            9,
            9,
            30,
            tzinfo=timezone(timedelta(hours=9)),
        )

        def snapshot_payload(*, advancers: int, decliners: int) -> dict:
            unchanged = 20
            symbol_count = advancers + unchanged + decliners
            observed_at_text = observed_at.isoformat()
            return {
                'status': 'ready',
                'as_of': observed_at_text,
                'missing_fields': [],
                'legs': {
                    'index': {'status': 'ready'},
                    'flow': {'status': 'ready'},
                    'fx': {'status': 'ready'},
                },
                'markets': {
                    'kospi': {
                        'label': 'KOSPI',
                        'index_code': '0001',
                        'value': 2673.0,
                        'change': -27.0,
                        'change_percent': -1.0,
                        'open': 2690.0,
                        'trading_value': 3_000_000,
                        'trading_value_unit': 'million_krw',
                        'upper_limit_count': 0,
                        'advancers': advancers,
                        'unchanged': unchanged,
                        'decliners': decliners,
                        'lower_limit_count': 0,
                        'breadth_raw_buckets': {
                            'upper_limit_count': 0,
                            'advancers': advancers,
                            'unchanged': unchanged,
                            'decliners': decliners,
                            'lower_limit_count': 0,
                        },
                        'breadth_completeness': {
                            'status': 'verified',
                            'expected_symbol_count': symbol_count,
                            'observed_symbol_count': symbol_count,
                            'limit_bucket_semantics': 'included_in_direction_counts',
                            'verification_basis': (
                                'kis_directional_buckets_with_same_date_completed_master_upper_bound'
                            ),
                            'master_count_relation': 'upper_bound',
                            'coverage_ratio': 1.0,
                            'expected_universe': {
                                'provider': 'kis_master',
                                'market_date': observed_at.date().isoformat(),
                                'completed_at': observed_at.replace(
                                    hour=8,
                                    minute=0,
                                    second=0,
                                ).isoformat(),
                                'symbol_count': symbol_count,
                                'relation_to_observed_count': 'upper_bound',
                                'membership_sha256': 'a' * 64,
                            },
                        },
                        'provider_business_date': observed_at.date().isoformat(),
                        'provider_session_time': observed_at.strftime('%H:%M:%S'),
                        'provider_observed_at': observed_at_text,
                        'index_leg': {
                            'status': 'ready',
                            'observation_source': 'kis_index_intraday_bar',
                            'provider_observed_at': observed_at_text,
                        },
                    },
                },
            }

        with session_scope() as session:
            session.add(
                CurrentMarketSnapshotCache(
                    cache_key='dashboard',
                    payload=snapshot_payload(advancers=650, decliners=250),
                    snapshot_status='ready',
                    stale_reason=None,
                    as_of=observed_at,
                    last_refresh_attempted_at=observed_at,
                    last_refresh_succeeded_at=observed_at,
                    last_refresh_status='success',
                    last_refresh_error=None,
                    refresh_trigger='test',
                )
            )

        open_market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T09:10:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        expected_row_bindings = {
            int(row.id): {
                'decision_snapshot_id': int(row.decision_snapshot_id),
                'row_revision_fingerprint': _row_dispatch_revision_fingerprint(row),
            }
            for row in rows
        }
        with patch.object(
            signal_decider_next_open_execution_service,
            'market_state_getter',
            return_value=open_market_state,
        ) as market_state_getter:
            healthy_generation = (
                signal_decider_next_open_execution_service.inspect_dispatch_authority_generation(
                    target_trade_date=self.target_trade_date,
                    expected_row_bindings=expected_row_bindings,
                )
            )
            self.assertEqual(healthy_generation['status'], 'valid')
            market_state_getter.return_value = MarketState(
                market='KRX',
                state='OPEN',
                is_trading_day=True,
                is_open=True,
                server_time='2026-04-30T09:10:01+09:00',
                session_open_at='2026-04-30T09:00:00+09:00',
                session_close_at='2026-04-30T15:30:00+09:00',
            )
            advanced_clock_generation = (
                signal_decider_next_open_execution_service.inspect_dispatch_authority_generation(
                    target_trade_date=self.target_trade_date,
                    expected_row_bindings=expected_row_bindings,
                )
            )
            self.assertEqual(advanced_clock_generation['status'], 'valid')
            self.assertEqual(
                advanced_clock_generation['generation_fingerprint'],
                healthy_generation['generation_fingerprint'],
            )
            with session_scope() as session:
                cache = session.execute(
                    select(CurrentMarketSnapshotCache).where(
                        CurrentMarketSnapshotCache.cache_key == 'dashboard'
                    )
                ).scalar_one()
                cache.payload = snapshot_payload(advancers=250, decliners=680)
                cache.as_of = observed_at
                cache.last_refresh_attempted_at = observed_at
                cache.last_refresh_succeeded_at = observed_at

            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_predispatch_risk_off_generation',
            )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['reason'],
            'current_live_market_risk_off',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertIn(
                'current_live_market_risk_off',
                {
                    item['code']
                    for item in row.validation_payload['blocking_reasons']
                },
            )

    async def test_dispatch_ack_generation_change_revokes_remote_acceptance(self) -> None:
        original_post_execution_plan = self.client.post_execution_plan

        async def publish_wait_before_ack(payload: dict) -> dict:
            with session_scope() as session:
                prior = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
                values = {
                    column.name: copy.deepcopy(getattr(prior, column.name))
                    for column in SignalDeciderDecisionSnapshot.__table__.columns
                    if column.name not in {'id', 'created_at', 'updated_at'}
                }
                values.update(
                    {
                        'feedback_received_at': datetime(
                            2026,
                            4,
                            29,
                            6,
                            1,
                            tzinfo=timezone.utc,
                        ),
                        'decision_action': 'wait',
                        'preferred_action': 'wait',
                        'next_day_open_action': 'wait',
                        'response_payload': {'next_day_open_action': 'wait'},
                    }
                )
                session.add(SignalDeciderDecisionSnapshot(**values))
            return await original_post_execution_plan(payload)

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=publish_wait_before_ack,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_http_generation_change',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(result['dispatch']['groups'][0]['dispatch_status'], 'revocation_pending')
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'latest_snapshot_not_actionable',
            )

    async def test_activation_transport_failure_stays_pending_and_retries_exact_activation(
        self,
    ) -> None:
        async def fail_after_activation_request(payload: dict) -> dict:
            self.client.dispatch_events.append('activation')
            self.client.activation_calls.append(copy.deepcopy(payload))
            raise SignalDeciderExecClientError(
                'activation response was lost',
                code='unavailable',
                retryable=True,
            )

        with patch.object(
            self.client,
            'post_execution_plan_activation',
            side_effect=fail_after_activation_request,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_activation_transport_failure',
            )

        self.assertEqual(self.client.dispatch_events, ['plan', 'activation'])
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(self.client.revocation_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['dispatch_status'],
            'accepted',
        )
        self.assertEqual(
            result['dispatch']['groups'][0]['activation_status'],
            'pending',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertNotEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'eligible')
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'pending',
            )
            self.assertNotIn('revocation', row.submission_payload)

        first_activation_request = copy.deepcopy(self.client.activation_calls[0])
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test_activation_transport_retry',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 2)
        self.assertEqual(self.client.activation_calls[1], first_activation_request)
        self.assertEqual(self.client.revocation_calls, [])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'activated',
            )

    async def test_activation_transport_failure_keeps_only_failed_plan_cohort_pending(
        self,
    ) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(
            snapshot_id=short_snapshot_id
        )
        original_activation = self.client.post_execution_plan_activation

        async def fail_signal_plan(payload: dict) -> dict:
            if any(
                str(item['decision_snapshot_id']) == str(self.snapshot_id)
                for item in payload['items']
            ):
                self.client.dispatch_events.append('activation')
                self.client.activation_calls.append(copy.deepcopy(payload))
                raise SignalDeciderExecClientError(
                    'signal plan activation failed',
                    code='unavailable',
                    retryable=True,
                )
            return await original_activation(payload)

        with patch.object(
            self.client,
            'post_execution_plan_activation',
            side_effect=fail_signal_plan,
        ):
            await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_activation_plan_isolation',
            )

        with session_scope() as session:
            failed_row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            unrelated_row = session.get(
                SignalDeciderExecutionPlanItem,
                short_plan_item_id,
            )
            self.assertNotEqual(failed_row.execution_status, 'revoked')
            self.assertEqual(
                failed_row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'pending',
            )
            self.assertNotEqual(unrelated_row.execution_status, 'revoked')
            self.assertEqual(
                unrelated_row.submission_payload['exec_dispatch'][
                    'contract_proof'
                ]['core_activation']['status'],
                'activated',
            )
        self.assertEqual(self.client.revocation_calls, [])

    async def test_activation_material_ack_mismatch_remains_fail_closed(
        self,
    ) -> None:
        original_activation = self.client.post_execution_plan_activation

        async def mismatched_material_ack(payload: dict) -> dict:
            response = await original_activation(payload)
            response['contract_ack']['dispatch_material_fingerprint'] = 'f' * 64
            return response

        with patch.object(
            self.client,
            'post_execution_plan_activation',
            side_effect=mismatched_material_ack,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_activation_material_ack_mismatch',
            )

        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'revocation_pending')
        self.assertEqual(group['activation_status'], 'failed_closed')
        self.assertEqual(len(self.client.revocation_calls), 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'blocked')

    async def test_restart_activates_exact_quarantined_plan_without_reposting(
        self,
    ) -> None:
        with patch.object(
            self.service,
            '_activate_core_confirmed_dispatch',
            side_effect=SystemExit('simulated crash before activation request'),
        ):
            with self.assertRaisesRegex(SystemExit, 'before activation request'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    refresh_plan=False,
                    dry_run=False,
                    force=False,
                    trigger='test_crash_before_activation',
                )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.activation_calls, [])
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'pending',
            )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test_restart_pending_activation',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'activated',
            )

    async def test_restart_replays_activation_after_remote_ack_before_local_proof(
        self,
    ) -> None:
        with patch.object(
            self.service,
            '_persist_core_activation_ack',
            side_effect=SystemExit('simulated crash after activation ACK'),
        ):
            with self.assertRaisesRegex(SystemExit, 'after activation ACK'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    refresh_plan=False,
                    dry_run=False,
                    force=False,
                    trigger='test_crash_after_activation_ack',
                )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'pending',
            )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test_restart_after_activation_ack',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 2)
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'activated',
            )

    async def test_restart_activation_persistence_contention_revokes_plan(
        self,
    ) -> None:
        with patch.object(
            self.service,
            '_activate_core_confirmed_dispatch',
            side_effect=SystemExit('simulated crash before activation request'),
        ):
            with self.assertRaisesRegex(SystemExit, 'before activation request'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    refresh_plan=False,
                    dry_run=False,
                    force=False,
                    trigger='test_crash_before_activation_persistence_busy',
                )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        with patch.object(
            restarted_service,
            '_persist_core_activation_ack',
            side_effect=_PlanRefreshRevisionConflict(
                'persistent recovered activation ACK contention'
            ),
        ) as persist_ack:
            await restarted_service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_restart_activation_persistence_busy',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(
            persist_ack.call_count,
            CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS,
        )
        self.assertEqual(len(self.client.revocation_calls), 1)
        activation = self.client.activation_calls[0]
        revocation = self.client.revocation_calls[0]
        self.assertEqual(revocation['plan_id'], activation['plan_id'])
        self.assertEqual(
            {
                (item['core_item_key'], item['execution_plan_item_id'])
                for item in revocation['items']
            },
            {
                (item['core_item_key'], item['execution_plan_item_id'])
                for item in activation['items']
            },
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'blocked')

    async def test_pre_activation_generation_contention_stays_pending(
        self,
    ) -> None:
        original_inspect = (
            signal_decider_next_open_execution_service
            .inspect_dispatch_authority_generation
        )
        inspect_call_count = 0

        def inspect_with_pre_activation_conflict(**kwargs):
            nonlocal inspect_call_count
            inspect_call_count += 1
            if inspect_call_count == 3:
                raise _PlanRefreshRevisionConflict(
                    'transient pre-activation generation contention'
                )
            return original_inspect(**kwargs)

        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=inspect_with_pre_activation_conflict,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_pre_activation_generation_contention',
            )

        self.assertEqual(inspect_call_count, 3)
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(self.client.revocation_calls, [])
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'accepted')
        self.assertEqual(group['activation_status'], 'pending')
        self.assertEqual(
            group['activation_error']['code'],
            'execution_activation_fence_busy',
        )

    async def test_pre_activation_new_generation_does_not_revoke_replacement(
        self,
    ) -> None:
        original_inspect = (
            signal_decider_next_open_execution_service
            .inspect_dispatch_authority_generation
        )
        inspect_call_count = 0

        def inspect_with_new_generation(**kwargs):
            nonlocal inspect_call_count
            inspect_call_count += 1
            if inspect_call_count == 3:
                return {
                    'status': 'valid',
                    'generation_fingerprint': 'f' * 64,
                    'reason_code': 'latest_snapshot_replaced',
                    'replacement_action': 'watch_for_trigger',
                    'replacement_actionable': True,
                }
            return original_inspect(**kwargs)

        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=inspect_with_new_generation,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_pre_activation_new_generation',
            )

        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(self.client.revocation_calls, [])
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'accepted')
        self.assertEqual(group['activation_status'], 'superseded_generation')
        self.assertEqual(
            group['activation_error']['code'],
            'execution_activation_generation_superseded',
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertNotIn(
                row.execution_status,
                {'revocation_pending', 'revocation_manual_review', 'revoked'},
            )

    async def test_post_activation_new_generation_does_not_poison_replacement(
        self,
    ) -> None:
        original_inspect = (
            signal_decider_next_open_execution_service
            .inspect_dispatch_authority_generation
        )
        inspect_call_count = 0

        def inspect_with_new_generation_after_activation(**kwargs):
            nonlocal inspect_call_count
            inspect_call_count += 1
            if inspect_call_count == 4:
                return {
                    'status': 'valid',
                    'generation_fingerprint': 'e' * 64,
                    'reason_code': 'latest_snapshot_replaced',
                    'replacement_action': 'watch_for_trigger',
                    'replacement_actionable': True,
                }
            return original_inspect(**kwargs)

        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=inspect_with_new_generation_after_activation,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_post_activation_new_generation',
            )

        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(self.client.revocation_calls, [])
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'accepted')
        self.assertEqual(group['activation_status'], 'superseded_generation')
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertNotIn(
                row.execution_status,
                {'revocation_pending', 'revocation_manual_review', 'revoked'},
            )

    async def test_transient_dispatch_ack_persistence_contention_converges(
        self,
    ) -> None:
        original_persist = self.service._persist_dispatch_success
        persist_call_count = 0

        def persist_after_one_busy_fence(**kwargs) -> dict:
            nonlocal persist_call_count
            persist_call_count += 1
            if persist_call_count == 1:
                translated_item_count = sum(
                    len(kwargs['row_links'].get(row.id, []))
                    for row in kwargs['rows']
                )
                return {
                    'dispatch_status': 'pending_remote_ack',
                    'eligible_count': len(kwargs['rows']),
                    'translated_item_count': translated_item_count,
                    'accepted_item_count': 0,
                    'rejected_item_count': 0,
                    'pending_item_count': translated_item_count,
                    'error': {
                        'code': 'dispatch_ack_authority_fence_busy',
                        'message': 'transient post-ACK authority contention',
                        'retryable': True,
                    },
                }
            return original_persist(**kwargs)

        with patch.object(
            self.service,
            '_persist_dispatch_success',
            side_effect=persist_after_one_busy_fence,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_transient_dispatch_ack_persistence_contention',
            )

        self.assertEqual(persist_call_count, 2)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(self.client.revocation_calls, [])
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'accepted')
        self.assertEqual(group['activation_status'], 'activated')

    async def test_persistent_dispatch_ack_persistence_contention_stays_quarantined(
        self,
    ) -> None:
        pending_result = {
            'dispatch_status': 'pending_remote_ack',
            'eligible_count': 1,
            'translated_item_count': 1,
            'accepted_item_count': 0,
            'rejected_item_count': 0,
            'pending_item_count': 1,
            'error': {
                'code': 'dispatch_ack_authority_fence_busy',
                'message': 'persistent post-ACK authority contention',
                'retryable': True,
            },
        }

        with (
            patch.object(
                self.service,
                '_persist_dispatch_success',
                return_value=pending_result,
            ) as persist_success,
            patch(
                'app.orders.exec_integration.asyncio.sleep',
                new_callable=AsyncMock,
            ) as sleep,
        ):
            result = (
                await self.service._persist_dispatch_success_with_revision_retry()
            )

        self.assertEqual(result, pending_result)
        self.assertEqual(
            persist_success.call_count,
            CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS,
        )
        self.assertEqual(
            sleep.await_count,
            CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS - 1,
        )

    async def test_transient_predispatch_generation_contention_converges(
        self,
    ) -> None:
        original_inspect = (
            signal_decider_next_open_execution_service
            .inspect_dispatch_authority_generation
        )
        inspect_call_count = 0

        def inspect_after_one_predispatch_conflict(**kwargs):
            nonlocal inspect_call_count
            inspect_call_count += 1
            if inspect_call_count == 1:
                raise _PlanRefreshRevisionConflict(
                    'transient predispatch generation contention'
                )
            return original_inspect(**kwargs)

        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=inspect_after_one_predispatch_conflict,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_transient_predispatch_generation_contention',
            )

        # The durable ACK can observe additional short-lived source writers
        # after the injected pre-dispatch conflict. Convergence is the
        # invariant; the exact successful-attempt number is scheduler-dependent
        # but remains bounded by the production retry budget.
        self.assertGreaterEqual(inspect_call_count, 6)
        self.assertLessEqual(
            inspect_call_count,
            5 + CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS,
        )
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(self.client.revocation_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['activation_status'],
            'activated',
        )

    async def test_persistent_predispatch_generation_contention_fails_closed(
        self,
    ) -> None:
        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=_PlanRefreshRevisionConflict(
                'persistent predispatch generation contention'
            ),
        ) as inspect_generation:
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_persistent_predispatch_generation_contention',
            )

        self.assertEqual(
            inspect_generation.call_count,
            CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS + 1,
        )
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(self.client.revocation_calls, [])
        group = result['dispatch']['groups'][0]
        self.assertEqual(
            group['dispatch_status'],
            'skipped',
        )
        self.assertEqual(
            group['authority_generation']['error_type'],
            '_PlanRefreshRevisionConflict',
        )
        self.assertEqual(
            group['authority_generation']['reason_code'],
            'dispatch_authority_generation_busy',
        )
        self.assertEqual(
            group['error']['code'],
            'dispatch_authority_fence_failed',
        )

    async def test_bounded_post_activation_generation_contention_converges(
        self,
    ) -> None:
        original_inspect = (
            signal_decider_next_open_execution_service
            .inspect_dispatch_authority_generation
        )
        inspect_call_count = 0

        def inspect_with_bounded_post_activation_conflicts(**kwargs):
            nonlocal inspect_call_count
            inspect_call_count += 1
            if inspect_call_count in {4, 5, 6}:
                raise _PlanRefreshRevisionConflict(
                    'transient post-activation generation contention'
                )
            return original_inspect(**kwargs)

        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=inspect_with_bounded_post_activation_conflicts,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_bounded_post_activation_generation_contention',
            )

        # The durable ACK can observe one or more independent, short-lived
        # source writers after the three injected post-activation conflicts.
        # Convergence is the invariant; the exact successful-attempt number is
        # intentionally scheduler-dependent but remains bounded.
        self.assertGreaterEqual(inspect_call_count, 8)
        self.assertLessEqual(
            inspect_call_count,
            7 + CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS,
        )
        self.assertEqual(self.client.revocation_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['activation_status'],
            'activated',
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'activated',
            )

    async def test_persistent_post_activation_contention_revokes_unconfirmed_plan(
        self,
    ) -> None:
        original_inspect = (
            signal_decider_next_open_execution_service
            .inspect_dispatch_authority_generation
        )

        def inspect_until_activation(**kwargs):
            if self.client.activation_calls:
                raise _PlanRefreshRevisionConflict(
                    'persistent post-activation generation contention'
                )
            return original_inspect(**kwargs)

        with patch.object(
            signal_decider_next_open_execution_service,
            'inspect_dispatch_authority_generation',
            side_effect=inspect_until_activation,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_persistent_post_activation_contention',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(len(self.client.revocation_calls), 1)
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'revocation_pending')
        self.assertEqual(group['activation_status'], 'failed_closed')
        self.assertEqual(
            group['activation_error']['code'],
            'execution_activation_ack_invalid',
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'blocked')

    async def test_transient_durable_activation_ack_contention_converges(
        self,
    ) -> None:
        original_persist = self.service._persist_core_activation_ack
        persist_call_count = 0

        def persist_after_one_conflict(**kwargs) -> None:
            nonlocal persist_call_count
            persist_call_count += 1
            if persist_call_count == 1:
                raise _PlanRefreshRevisionConflict(
                    'transient durable activation ACK contention'
                )
            original_persist(**kwargs)

        with patch.object(
            self.service,
            '_persist_core_activation_ack',
            side_effect=persist_after_one_conflict,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_transient_durable_activation_ack_contention',
            )

        self.assertEqual(persist_call_count, 2)
        self.assertEqual(self.client.revocation_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['activation_status'],
            'activated',
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['contract_proof'][
                    'core_activation'
                ]['status'],
                'activated',
            )

    async def test_durable_activation_fence_busy_revokes_activated_plan(
        self,
    ) -> None:
        with patch.object(
            self.service,
            '_persist_core_activation_ack',
            side_effect=_PlanRefreshRevisionConflict(
                'persistent durable activation ACK contention'
            ),
        ) as persist_ack:
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_durable_activation_fence_busy',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.activation_calls), 1)
        self.assertEqual(
            persist_ack.call_count,
            CORE_ACTIVATION_FENCE_RETRY_ATTEMPTS,
        )
        self.assertEqual(len(self.client.revocation_calls), 1)
        activation = self.client.activation_calls[0]
        revocation = self.client.revocation_calls[0]
        self.assertEqual(revocation['plan_id'], activation['plan_id'])
        self.assertEqual(
            {
                (item['core_item_key'], item['execution_plan_item_id'])
                for item in revocation['items']
            },
            {
                (item['core_item_key'], item['execution_plan_item_id'])
                for item in activation['items']
            },
        )
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'revocation_pending')
        self.assertEqual(
            group['activation_status'],
            'failed_closed',
        )
        self.assertEqual(
            group['activation_error']['code'],
            'execution_activation_core_persistence_busy',
        )
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'blocked')

    async def test_restart_revokes_quarantined_plan_when_core_revision_changed(
        self,
    ) -> None:
        with patch.object(
            self.service,
            '_activate_core_confirmed_dispatch',
            side_effect=SystemExit('simulated crash before activation request'),
        ):
            with self.assertRaisesRegex(SystemExit, 'before activation request'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    refresh_plan=False,
                    dry_run=False,
                    force=False,
                    trigger='test_crash_before_stale_activation',
                )

        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            row.planned_quantity = Decimal(row.planned_quantity) + Decimal('1')

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test_restart_stale_pending_activation',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.activation_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        with session_scope() as session:
            row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.eligibility_status, 'blocked')

    async def test_dispatch_ack_ignores_unrelated_summarizer_run_checkpoint(self) -> None:
        original_post_execution_plan = self.client.post_execution_plan
        unrelated_run_id = f'unrelated-summary-{uuid4().hex[:10]}'

        async def checkpoint_unrelated_run(payload: dict) -> dict:
            with session_scope() as session:
                session.add(
                    AgentContextFeedbackRun(
                        run_id=unrelated_run_id,
                        market_date=self.market_date,
                        status='running',
                        symbols_total=1,
                        completed_count=0,
                        failed_count=0,
                        request_payload={
                            'mode': 'role_sequential_runtime_buffered',
                            'roles': ['eod-summarizer'],
                            'symbols': ['999999'],
                        },
                        summary_payload={'checkpoint': 1},
                        started_at=datetime(
                            2026,
                            4,
                            29,
                            6,
                            30,
                            tzinfo=timezone.utc,
                        ),
                    )
                )
            return await original_post_execution_plan(payload)

        try:
            with patch.object(
                self.client,
                'post_execution_plan',
                side_effect=checkpoint_unrelated_run,
            ):
                result = await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    refresh_plan=False,
                    dry_run=False,
                    force=True,
                    trigger='test_unrelated_summarizer_generation',
                )
        finally:
            with session_scope() as session:
                session.execute(
                    delete(AgentContextFeedbackRun).where(
                        AgentContextFeedbackRun.run_id == unrelated_run_id
                    )
                )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.revocation_calls, [])
        self.assertEqual(
            result['dispatch']['groups'][0]['dispatch_status'],
            'accepted',
        )

    async def test_dispatch_ack_uses_wait_when_concurrent_refresh_blocks_candidate(self) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'generic_action': 'buy',
            }
        original_post_execution_plan = self.client.post_execution_plan

        async def block_row_before_ack(payload: dict) -> dict:
            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                row.eligibility_status = 'blocked'
                row.validation_payload = {
                    **(row.validation_payload or {}),
                    'blocking_reasons': [
                        {
                            'code': 'latest_snapshot_not_actionable',
                            'message': 'Concurrent refresh no longer allows this candidate.',
                        }
                    ],
                }
            return await original_post_execution_plan(payload)

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=block_row_before_ack,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_nonactionable_revision_race',
            )

        self.assertEqual(result['summary']['revoked_group_count'], 1)
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(
            self.client.revocation_calls[0]['replacement_action'],
            'wait',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revoked')

    async def test_dispatch_timeout_after_material_revision_requires_manual_review(self) -> None:
        async def mutate_row_then_timeout(payload: dict) -> dict:
            self.client.post_calls.append(payload)
            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                row.allocation_budget_krw = Decimal('875000')
                row.validation_payload = {
                    **(row.validation_payload or {}),
                    'preopen_policy_fingerprint': 'timeout-policy-revision',
                }
                row.traceability_payload = {
                    **(row.traceability_payload or {}),
                    'allocation_budget_krw': '875000',
                }
            raise SignalDeciderExecClientError(
                'exec request timed out after an ambiguous submit',
                code='unavailable',
                retryable=True,
            )

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=mutate_row_then_timeout,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_timeout_material_revision_race',
            )

        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.plan_calls), 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.allocation_budget_krw, Decimal('875000'))
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            reconciliation = row.submission_payload['dispatch_reconciliation']
            self.assertEqual(reconciliation['status'], 'manual_review_required')
            self.assertEqual(
                reconciliation['reason_code'],
                'ambiguous_remote_acceptance_after_core_revision',
            )
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'unavailable')
            self.assertEqual(
                exec_dispatch['plan_id'],
                self.client.post_calls[0]['plan_id'],
            )

    async def test_timeout_readback_revokes_old_remote_authority_after_material_revision(self) -> None:
        async def accept_old_revision_then_timeout(payload: dict) -> dict:
            self.client.post_calls.append(payload)
            self.client.plan_response = _accepted_plan_readback_for_payload(payload)
            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                row.allocation_budget_krw = Decimal('825000')
                row.validation_payload = {
                    **(row.validation_payload or {}),
                    'preopen_policy_fingerprint': 'readback-policy-revision',
                }
                row.traceability_payload = {
                    **(row.traceability_payload or {}),
                    'allocation_budget_krw': '825000',
                }
            raise SignalDeciderExecClientError(
                'exec request timed out after accepting the old revision',
                code='unavailable',
                retryable=True,
            )

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=accept_old_revision_then_timeout,
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_timeout_readback_revision_race',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.plan_calls), 1)
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        self.assertEqual(
            result['dispatch']['groups'][0]['dispatch_status'],
            'revocation_pending',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.allocation_budget_krw, Decimal('825000'))
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'planned')
            self.assertEqual(row.submission_payload['revocation']['status'], 'acknowledged')
            self.assertNotIn('exec_dispatch', row.submission_payload)

    async def test_ambiguous_material_revision_is_not_reposted_and_disabled_mode_revokes(self) -> None:
        old_readback: dict = {}

        async def timeout_after_remote_acceptance(payload: dict) -> dict:
            nonlocal old_readback
            self.client.post_calls.append(payload)
            old_readback = _accepted_plan_readback_for_payload(payload)
            self.client.plan_response = {'items': []}
            raise SignalDeciderExecClientError(
                'initial request timed out without readback visibility',
                code='unavailable',
                retryable=True,
            )

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=timeout_after_remote_acceptance,
        ):
            first_result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_initial_ambiguous_timeout',
            )
            self.assertEqual(first_result['summary']['failed_group_count'], 0)
            self.assertEqual(
                first_result['dispatch']['groups'][0]['dispatch_status'],
                'pending_remote_ack',
            )

            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                row.allocation_budget_krw = Decimal('775000')
                row.validation_payload = {
                    **(row.validation_payload or {}),
                    'preopen_policy_fingerprint': 'retry-policy-revision',
                }
                row.traceability_payload = {
                    **(row.traceability_payload or {}),
                    'allocation_budget_krw': '775000',
                }

            self.client.plan_response = old_readback
            retry_result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_duplicate_after_material_revision',
            )
            settings.signal_decider_exec_integration_mode = 'disabled'
            disabled_result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test_disable_after_material_revision',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(retry_result['summary']['dispatched_group_count'], 0)
        self.assertGreaterEqual(len(self.client.plan_calls), 2)
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(disabled_result['summary']['revoked_group_count'], 1)
        self.assertEqual(
            disabled_result['summary']['reason'],
            'execution_control_disabled',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.allocation_budget_krw, Decimal('775000'))
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(row.submission_payload['revocation']['status'], 'acknowledged')
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertTrue(
                all(
                    isinstance(link.get('exec_plan_item_id'), str)
                    for link in exec_dispatch['item_links']
                )
            )

    async def test_readback_locks_and_preserves_newer_pending_revocation_state(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test_readback_race',
        )
        stale_rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        expected_readback_bindings = {
            int(row.id): _row_readback_revision_binding(row)
            for row in stale_rows
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            item_links = copy.deepcopy(submission['exec_dispatch']['item_links'])
            submission['revision_history'] = [
                {
                    'decision_snapshot_id': str(self.snapshot_id),
                    'dispatch_evidence': {
                        'plan_id': submission['exec_dispatch']['plan_id'],
                        'item_links': item_links,
                    },
                }
            ]
            submission['revocation'] = {
                'status': 'pending',
                'reason_code': 'current_live_market_risk_off',
                'requested_at': '2026-04-30T00:05:00+00:00',
                'prior_decision_snapshot_id': str(self.snapshot_id),
                'replacement_decision_snapshot_id': 'observation:risk-off:readback-race',
                'replacement_action': 'buy',
                'replacement_actionable': False,
                'plan_id': submission['exec_dispatch']['plan_id'],
                'item_links': item_links,
            }
            submission['revocation_history'] = [
                {
                    'acknowledged_at': '2026-04-29T23:59:00+00:00',
                    'request': {'plan_id': 'prior-plan', 'items': []},
                    'response': {'plan_id': 'prior-plan', 'item_acks': []},
                }
            ]
            row.decision_agent_name = 'short-term-decider'
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'raw_action': 'watch_for_trigger',
                'handoff_intent': 'intraday_watch_buy',
            }
            row.eligibility_status = 'blocked'
            row.execution_status = 'revocation_pending'
            row.submission_payload = submission
            expected_lifecycle = {
                key: copy.deepcopy(submission[key])
                for key in ('revision_history', 'revocation', 'revocation_history')
            }

        statements: list[str] = []

        def capture_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        event.listen(engine, 'before_cursor_execute', capture_statement)
        try:
            with patch(
                'app.orders.exec_integration.signal_decider_execution_outcome_service.sync_entry_plan_items'
            ):
                self.service._persist_group_readback(
                    rows=stale_rows,
                    expected_row_bindings=expected_readback_bindings,
                    trigger='test_readback_race',
                    runtime_payload={
                        'status': 'ok',
                        'payload': {'ready': True, 'mode': 'mock'},
                        'error': None,
                    },
                    plans_result={
                        'status': 'ok',
                        'payload': {'items': []},
                        'items': [],
                        'error': None,
                    },
                    orders_result={'status': 'ok', 'items': [], 'error': None},
                    executions_result={'status': 'ok', 'items': [], 'error': None},
                    positions_result={'status': 'ok', 'items': [], 'error': None},
                )
        finally:
            event.remove(engine, 'before_cursor_execute', capture_statement)

        self.assertTrue(
            any(
                'signal_decider_execution_plan_items' in statement
                and 'FOR UPDATE' in statement.upper()
                for statement in statements
            )
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_pending')
            for key, expected in expected_lifecycle.items():
                self.assertEqual(row.submission_payload[key], expected)

    async def test_exec_readback_discards_old_plan_when_binding_changes_during_fetch(
        self,
    ) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='seed_readback_binding',
        )
        with session_scope() as session:
            before = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            old_exec_dispatch = copy.deepcopy(before.submission_payload['exec_dispatch'])

        async def replace_plan_before_persist(
            *,
            external_plan_id: str,
            execution_plan_id: str,
            symbols=None,
        ) -> dict:
            self.client.position_calls.append(
                {
                    'external_plan_id': external_plan_id,
                    'execution_plan_id': execution_plan_id,
                    'symbols': symbols,
                }
            )
            with session_scope() as session:
                row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
                submission = copy.deepcopy(row.submission_payload)
                replacement = copy.deepcopy(submission['exec_dispatch'])
                replacement['plan_id'] = 'replacement-plan-after-readback-start'
                replacement['plan_fingerprint'] = 'replacement-plan-fingerprint'
                replacement['dispatch_revision_fingerprint'] = _row_dispatch_revision_fingerprint(row)
                replacement['contract_proof']['contract_ack']['plan_id'] = replacement[
                    'plan_id'
                ]
                for index, link in enumerate(replacement['item_links'], start=1):
                    link['exec_plan_id'] = replacement['plan_id']
                    link['exec_plan_item_id'] = f'replacement-exec-item-{index}'
                submission['exec_dispatch'] = replacement
                row.submission_payload = submission
            return {'items': []}

        with patch.object(
            self.client,
            'list_positions',
            side_effect=replace_plan_before_persist,
        ):
            await self.service._refresh_exec_readback(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                symbol=None,
                trigger='test_readback_plan_replacement_race',
                runtime_payload={'status': 'ok', 'payload': {'ready': True}, 'error': None},
            )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            current = row.submission_payload['exec_dispatch']
            self.assertEqual(current['plan_id'], 'replacement-plan-after-readback-start')
            self.assertNotEqual(current['item_links'], old_exec_dispatch['item_links'])
            self.assertNotIn('readback', current)
            self.assertEqual(
                current['attempts'][-1]['status'],
                'readback_discarded_revision_mismatch',
            )
            self.assertEqual(
                current['attempts'][-1]['expected_plan_id'],
                old_exec_dispatch['plan_id'],
            )

    async def test_dispatch_mode_blocks_multiple_explicit_accounts_before_exec_http(self) -> None:
        second_account_reference = f'KIS:97{uuid4().hex[:6].upper()}-01'
        settings.signal_decider_next_open_account_targets = (
            f'Main={self.account_reference}@1500000,'
            f'Secondary={second_account_reference}@1500000'
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(self.client.post_calls, [])
        issue = next(
            item
            for item in result['integration']['issues']
            if item['code'] == 'multi_account_exec_endpoint_unsupported'
        )
        self.assertEqual(issue['configured_account_target_count'], 2)
        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertEqual(
            result['dispatch']['groups'][0]['error']['code'],
            'multi_account_exec_endpoint_unsupported',
        )

    def test_live_dispatch_status_requires_one_explicit_valid_account_target(self) -> None:
        settings.signal_decider_exec_runtime_mode = 'live'

        invalid_target_settings = (
            '',
            'Main=not-a-kis-account@1500000',
            'Synthetic=KIS:00000000-00@1500000',
        )
        for raw_targets in invalid_target_settings:
            with self.subTest(raw_targets=raw_targets):
                settings.signal_decider_next_open_account_targets = raw_targets

                status = self.service.integration_status()

                self.assertEqual(status['config_status'], 'not_configured')
                self.assertIn(
                    'live_exec_account_target_required',
                    {issue['code'] for issue in status['issues']},
                )

        settings.signal_decider_next_open_account_targets = (
            f'Main={self.account_reference}@1500000'
        )

        status = self.service.integration_status()

        self.assertEqual(status['config_status'], 'ready')
        self.assertNotIn(
            'live_exec_account_target_required',
            {issue['code'] for issue in status['issues']},
        )

    def test_paper_and_mock_dispatch_status_allow_unspecified_monitor_route(self) -> None:
        settings.signal_decider_next_open_account_targets = ''

        for runtime_mode in ('mock', 'paper'):
            with self.subTest(runtime_mode=runtime_mode):
                settings.signal_decider_exec_runtime_mode = runtime_mode

                status = self.service.integration_status()

                self.assertEqual(status['config_status'], 'ready')
                self.assertNotIn(
                    'live_exec_account_target_required',
                    {issue['code'] for issue in status['issues']},
                )

    async def test_live_dispatch_without_explicit_account_target_never_posts(self) -> None:
        settings.signal_decider_exec_runtime_mode = 'live'
        settings.signal_decider_next_open_account_targets = ''

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='live_without_account_target',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertIn(
            'live_exec_account_target_required',
            {issue['code'] for issue in result['integration']['issues']},
        )
        self.assertEqual(
            result['dispatch']['groups'][0]['error']['code'],
            'live_exec_account_target_required',
        )

    def test_live_handoff_rejects_stale_unspecified_route_even_with_configured_target(self) -> None:
        settings.signal_decider_exec_runtime_mode = 'live'
        rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        snapshots = self.service._load_snapshots(rows)
        rows[0].account_reference = 'KIS:00000000-00'
        rows[0].account_label = 'UNSPECIFIED_ROUTE'

        with self.assertRaisesRegex(
            SignalDeciderExecPayloadContractError,
            'live_exec_account_target_required',
        ):
            self.service._build_exec_plan_payload(
                rows=rows,
                snapshots=snapshots,
                trigger='live_stale_unspecified_route',
            )

    def test_disabled_mode_keeps_multi_account_planning_configuration_available(self) -> None:
        second_account_reference = f'KIS:97{uuid4().hex[:6].upper()}-01'
        settings.signal_decider_exec_integration_mode = 'disabled'
        settings.signal_decider_next_open_account_targets = (
            f'Main={self.account_reference}@1500000,'
            f'Secondary={second_account_reference}@1500000'
        )

        result = self.service.integration_status()

        self.assertEqual(result['config_status'], 'disabled')
        self.assertNotIn(
            'multi_account_exec_endpoint_unsupported',
            {issue['code'] for issue in result['issues']},
        )

    async def test_dispatch_next_open_plan_persists_compact_per_row_payload_for_multi_item_plan(self) -> None:
        bulky_decider_context = {
            'decider_input_context': {
                'items': [
                    {
                        'symbol': self.symbol,
                        'risk_context': {
                            'decider_handoff_context': {
                                'decider_input_context': {'blob': 'x' * 12000},
                            },
                        },
                    }
                ],
                'raw_request_payload': {'blob': 'y' * 12000},
            },
            'symbol_day_context': {
                'company': {'market': 'KOSPI'},
                'pre_decider_enrichment_context': {'quote': 'available'},
                'decision_input_quality_context': {
                    'schema_version': 'decision_input_quality_v1',
                    'execution_feasibility': {'status': 'usable', 'liquidity_tier': 'large_cap'},
                },
            },
        }
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.request_payload = {
                'signal_decider_policy': (
                    self._signal_decider_policy_request_fixture()
                ),
                **bulky_decider_context,
            }
            snapshot.input_feature_snapshot_payload = {'raw_feature_blob': 'z' * 12000}

        self._seed_signal_candidate(
            symbol='000660',
            name_ko='SK hynix',
            decision_run_id='exec-run-2',
            client_order_id='SDNO-20260430-000660',
            request_payload={
                'signal_decider_policy': (
                    self._signal_decider_policy_request_fixture()
                ),
                **bulky_decider_context,
            },
            input_feature_snapshot_payload={'raw_feature_blob': 'q' * 12000},
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['eligible_count'], 2)
        self.assertEqual(result['summary']['translated_item_count'], 6)
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                    SignalDeciderExecutionPlanItem.target_trade_date == self.target_trade_date,
                )
            ).scalars().all()
            self.assertEqual(len(rows), 2)
            for row in rows:
                submission_payload = row.submission_payload
                payload_text = json.dumps(submission_payload, ensure_ascii=False, sort_keys=True)
                exec_dispatch = submission_payload['exec_dispatch']
                self.assertLess(json_size_bytes(submission_payload), SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES)
                self.assertNotIn('last_request_payload', exec_dispatch)
                self.assertNotIn('last_response_payload', exec_dispatch)
                self.assertIn('last_request_summary', exec_dispatch)
                self.assertEqual(exec_dispatch['last_request_summary']['total_item_count'], 6)
                self.assertEqual(exec_dispatch['last_request_summary']['row_item_count'], 3)
                self.assertNotIn('decider_input_context', payload_text)
                self.assertNotIn('raw_request_payload', payload_text)
                self.assertNotIn('last_request_payload', payload_text)

    def test_exec_plan_wire_payload_is_bounded_for_36_candidates_and_exits_use_compact_risk_context(self) -> None:
        bulky_decider_context = {
            'decider_input_context': {
                'raw_request_payload': {'blob': 'x' * 20_000},
            },
            'symbol_day_context': {
                'company': {'market': 'KOSPI'},
                'pre_decider_enrichment_context': {
                    'schema_version': 'pre_decider_enrichment_v1',
                    'large_provider_payload': 'y' * 20_000,
                },
            },
        }
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.request_payload = {
                'signal_decider_policy': self._signal_decider_policy_request_fixture(),
                **copy.deepcopy(bulky_decider_context),
            }
            snapshot.input_feature_snapshot_payload = {'large_feature_blob': 'z' * 20_000}

        for index in range(1, 36):
            symbol = f'{900000 + index:06d}'
            self._seed_signal_candidate(
                symbol=symbol,
                name_ko=f'Payload candidate {index}',
                decision_run_id=f'wire-budget-run-{index}',
                client_order_id=f'SDNO-20260430-{symbol}',
                request_payload={
                    'signal_decider_policy': self._signal_decider_policy_request_fixture(),
                    **copy.deepcopy(bulky_decider_context),
                },
                input_feature_snapshot_payload={'large_feature_blob': 'z' * 20_000},
            )

        rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        snapshots = self.service._load_snapshots(rows)

        payload, _ = self.service._build_exec_plan_payload(
            rows=rows,
            snapshots=snapshots,
            trigger='wire_budget_regression',
        )

        self.assertEqual(len(rows), 36)
        self.assertEqual(len(payload['items']), 108)
        self.assertLessEqual(json_size_bytes(payload), EXECUTION_PLAN_DISPATCH_MAX_BYTES)
        entries = [item for item in payload['items'] if item['side'] == 'buy']
        exits = [item for item in payload['items'] if item['side'] == 'sell']
        self.assertEqual(len(entries), 36)
        self.assertEqual(len(exits), 72)
        for entry in entries:
            self.assertIn('decider_handoff_context', entry['risk_context'])
            self.assertIn('decider_handoff_context_digest', entry['risk_context'])
        for protective_exit in exits:
            risk_context = protective_exit['risk_context']
            self.assertNotIn('decider_handoff_context', risk_context)
            self.assertNotIn('validation', risk_context)
            self.assertNotIn('traceability', risk_context)
            self.assertNotIn('decision_input_quality_context', risk_context)
            self.assertNotIn('execution_feasibility_context', risk_context)
            self.assertRegex(
                risk_context['decider_handoff_context_digest'],
                r'^[0-9a-f]{64}$',
            )
            self.assertIn('account_reference', risk_context)
            self.assertIn('target_price', risk_context)
            self.assertIn('stop_loss_price', risk_context)

    def test_short_term_watch_handoff_compacts_wide_nested_decider_context(self) -> None:
        flow_short_sector_context = {
            'status': 'available',
            'investor_flow': {
                'status': 'available',
                'symbol_flow': {
                    'investor_groups': {
                        'foreign': {'net_value_krw': 1_250_000_000},
                    },
                },
                'market_flow': {
                    'investor_groups': [
                        {'name': 'foreign', 'net_value_krw': 8_500_000_000},
                    ],
                },
            },
            'short_selling': {
                'status': 'available',
                'short_volume': {'short_volume_ratio_pct': 2.1},
                'short_balance': {'short_balance_ratio_pct': 1.4},
            },
            'sector_peers': {
                'status': 'available',
                'relative_state': 'leading',
                'sector_name': 'Semiconductors',
                'peer_count': 1,
                'peers': [{'symbol': '000660', 'change_pct': 1.8}],
            },
            'market_breadth': {
                'status': 'available',
                'breadth_state': 'constructive',
            },
        }
        request_payload = {
            'symbol_day_context': {
                'company': {'market': 'KOSPI'},
                'pre_decider_enrichment_context': {
                    'schema_version': 'pre_decider_enrichment_v1',
                    'flow_short_sector_context': flow_short_sector_context,
                },
            },
            **{
                f'request_section_{index}': 'r' * 600
                for index in range(24)
            },
        }
        input_features = {
            f'feature_section_{index}': 'f' * 600
            for index in range(24)
        }
        response_payload_extra = {
            f'response_section_{index}': 'o' * 600
            for index in range(24)
        }
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload=request_payload,
            input_feature_snapshot_payload=input_features,
            response_payload_extra=response_payload_extra,
        )
        short_plan_item_id = self._seed_short_term_plan_item(
            snapshot_id=short_snapshot_id,
        )
        with session_scope() as session:
            short_row = session.get(
                SignalDeciderExecutionPlanItem,
                short_plan_item_id,
            )
            self.assertIsNotNone(short_row)
            short_row.eligibility_status = 'monitor_candidate'
            short_row.execution_status = 'awaiting_trigger'
            short_row.traceability_payload = {
                **(short_row.traceability_payload or {}),
                'signal_decider_alignment': 'wait_conflict_block',
                'alignment_gate_action': 'report_only_block_candidate',
                'alignment_gate_reason': 'signal_decider_wait_conflict',
                'alignment_signal_decider_decision_snapshot_id': 991,
                'final_enforcement_decision': 'allowed_report_only',
                'requested_quantity': 29,
                'final_quantity': 29,
                'alignment_gate': {
                    'signal_decider_alignment': 'wait_conflict_block',
                    'alignment_gate_action': 'report_only_block_candidate',
                    'signal_decider_action': 'wait',
                    'enforcement_decision': 'allowed_report_only',
                    'requested_quantity': 29,
                    'final_quantity': 29,
                },
            }
            short_snapshot = session.get(
                SignalDeciderDecisionSnapshot,
                short_snapshot_id,
            )
            self.assertIsNotNone(short_snapshot)
            short_snapshot.supporting_evidence_payload = [
                f'wide-evidence-{index}-' + ('e' * 1_000)
                for index in range(40)
            ]
            short_snapshot.data_quality_warnings_payload = [
                'Intraday order-book evidence requires a live recheck.',
            ]

        rows = [
            row
            for row in self.service._load_next_open_rows(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
            )
            if row.id == short_plan_item_id
        ]
        payload, _ = self.service._build_exec_plan_payload(
            rows=rows,
            snapshots=self.service._load_snapshots(rows),
            trigger='wide_short_term_watch_regression',
        )

        self.assertEqual(len(payload['items']), 3)
        self.assertLessEqual(
            json_size_bytes(payload),
            EXECUTION_PLAN_DISPATCH_MAX_BYTES,
        )
        entry = next(item for item in payload['items'] if item['side'] == 'buy')
        handoff = entry['risk_context']['decider_handoff_context']
        self.assertEqual(handoff['symbol'], self.symbol)
        self.assertEqual(handoff['target_trade_date'], '2026-04-30')
        self.assertEqual(handoff['source_market_date'], '2026-04-29')
        self.assertIn('dispatch_projection', handoff)
        self.assertLessEqual(
            json_size_bytes(handoff),
            EXEC_DECIDER_HANDOFF_MAX_BYTES,
        )
        self.assertRegex(
            entry['risk_context']['decider_handoff_context_digest'],
            r'^[0-9a-f]{64}$',
        )
        self.assertEqual(
            entry['risk_context']['flow_short_sector_context'],
            flow_short_sector_context,
        )
        self.assertNotIn(
            'projection_digest',
            json.dumps(entry['risk_context']['flow_short_sector_context']),
        )
        self.assertEqual(
            entry['risk_context']['signal_decider_alignment'],
            'wait_conflict_block',
        )
        self.assertEqual(
            entry['risk_context']['alignment_gate_action'],
            'report_only_block_candidate',
        )
        self.assertEqual(
            entry['risk_context']['alignment_gate_reason'],
            'signal_decider_wait_conflict',
        )
        self.assertEqual(
            entry['risk_context'][
                'alignment_signal_decider_decision_snapshot_id'
            ],
            991,
        )
        self.assertEqual(
            entry['risk_context']['final_enforcement_decision'],
            'allowed_report_only',
        )
        self.assertEqual(entry['risk_context']['requested_quantity'], '29')
        self.assertEqual(entry['risk_context']['final_quantity'], '29')
        self.assertEqual(
            entry['risk_context']['data_quality_warnings'],
            ['Intraday order-book evidence requires a live recheck.'],
        )
        activated_payload = _bind_core_confirmed_activation(
            payload,
            authority_generation={
                'status': 'valid',
                'generation_fingerprint': 'a' * 64,
            },
        )
        activated_entry = next(
            item for item in activated_payload['items'] if item['side'] == 'buy'
        )
        self.assertLessEqual(
            json_size_bytes(activated_entry['risk_context']),
            EXEC_ENTRY_RISK_CONTEXT_MAX_BYTES,
        )

    def test_entry_activation_fails_closed_when_binding_exceeds_risk_budget(self) -> None:
        rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        payload, _ = self.service._build_exec_plan_payload(
            rows=rows,
            snapshots=self.service._load_snapshots(rows),
            trigger='entry_activation_budget_regression',
        )
        entry = next(item for item in payload['items'] if item['side'] == 'buy')
        entry['risk_context']['oversized_after_activation'] = (
            'x' * EXEC_ENTRY_RISK_CONTEXT_MAX_BYTES
        )

        with self.assertRaisesRegex(
            SignalDeciderExecPayloadContractError,
            'exec_entry_risk_context exceeds bounded projection budget',
        ):
            _bind_core_confirmed_activation(
                payload,
                authority_generation={
                    'status': 'valid',
                    'generation_fingerprint': 'a' * 64,
                },
            )

    def test_protective_exit_final_risk_context_includes_trigger_within_budget(self) -> None:
        with session_scope() as session:
            snapshot = session.get(
                SignalDeciderDecisionSnapshot,
                self.snapshot_id,
            )
            # The context including trigger_rule fits just under 8 KiB, while
            # the later Core-activation binding pushes the wire value over.
            snapshot.thesis_summary = 'x' * 6_300

        rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        payload, _ = self.service._build_exec_plan_payload(
            rows=rows,
            snapshots=self.service._load_snapshots(rows),
            trigger='protective_exit_context_budget_regression',
        )
        payload = _bind_core_confirmed_activation(
            payload,
            authority_generation={
                'status': 'valid',
                'generation_fingerprint': 'a' * 64,
            },
        )

        protective_exits = [
            item for item in payload['items'] if item['side'] == 'sell'
        ]
        self.assertEqual(len(protective_exits), 2)
        self.assertEqual(
            {item['risk_context']['trigger_rule'] for item in protective_exits},
            {EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL},
        )
        for item in protective_exits:
            risk_context = item['risk_context']
            self.assertLessEqual(
                json_size_bytes(risk_context),
                EXEC_EXIT_RISK_CONTEXT_MAX_BYTES,
            )
            self.assertEqual(
                risk_context['core_plan_item_id'],
                self.plan_item_id,
            )
            self.assertEqual(
                risk_context['account_reference'],
                self.account_reference,
            )
            self.assertEqual(
                risk_context['decision_snapshot_id'],
                self.snapshot_id,
            )
            self.assertEqual(
                risk_context['_core_activation'][
                    'authority_generation_fingerprint'
                ],
                'a' * 64,
            )

    async def test_dispatch_next_open_plan_signal_agent_quantity_marks_submitted(self) -> None:
        self.client.agent_approved_quantities_by_agent['signal-decider'] = '5'

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['submitted_count'], 1)
        self.assertEqual(result['summary']['delegated_for_exec_agent_sizing_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_SUBMITTED)
            entry_link = row.submission_payload['exec_dispatch']['item_links'][0]
            self.assertEqual(entry_link['response_item']['agent_sizing']['quantity'], '5')

    async def test_cleanup_signal_decider_exec_payloads_dry_run_preserves_rows(self) -> None:
        oversized_payload = {
            'mode': 'exec_dispatch',
            'latest': {'dispatch_status': 'failed', 'attempted_at': '2026-05-22T00:00:00+00:00'},
            'exec_dispatch': {
                'plan_id': 'core-signal-decider-20260522-KIS',
                'plan_fingerprint': 'abc123',
                'dispatch_status': 'failed',
                'last_attempt_at': '2026-05-22T00:00:00+00:00',
                'last_error': {'code': 'unavailable', 'message': 'x' * 4096},
                'item_links': [
                    {
                        'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                        'intent': 'next_open_buy',
                        'side': 'buy',
                        'symbol': self.symbol,
                    }
                ],
                'last_request_payload': {
                    'plan_id': 'core-signal-decider-20260522-KIS',
                    'items': [
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                            'symbol': self.symbol,
                            'risk_context': {
                                'decider_handoff_context': {
                                    'decider_input_context': {'raw_blob': 'x' * 70000},
                                },
                            },
                        }
                    ],
                },
                'readback': {
                    'orders': {
                        'status': 'ok',
                        'count': 1,
                        'items': [{'symbol': self.symbol, 'status': 'rejected', 'raw_blob': 'y' * 70000}],
                    },
                    'derived_state': {'derived_status': 'failed'},
                },
            },
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.submission_payload = oversized_payload
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            committed_payload = row.submission_payload

        dry_run = cleanup_signal_decider_exec_submission_payloads(
            dry_run=True,
            target_trade_date=self.target_trade_date,
            min_payload_bytes=1024,
            limit=10,
        )

        self.assertEqual(dry_run['would_update_count'], 1)
        self.assertEqual(dry_run['updated_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.submission_payload, committed_payload)

        statements: list[str] = []

        def capture_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        event.listen(engine, 'before_cursor_execute', capture_statement)
        try:
            applied = cleanup_signal_decider_exec_submission_payloads(
                dry_run=False,
                target_trade_date=self.target_trade_date,
                min_payload_bytes=1024,
                limit=10,
            )
        finally:
            event.remove(engine, 'before_cursor_execute', capture_statement)

        self.assertEqual(applied['updated_count'], 1)
        self.assertTrue(
            any(
                'signal_decider_execution_plan_items' in statement
                and 'FOR UPDATE' in statement.upper()
                for statement in statements
            )
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            payload_text = json.dumps(row.submission_payload, ensure_ascii=False, sort_keys=True)
            self.assertLess(json_size_bytes(row.submission_payload), SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES)
            self.assertNotIn('last_request_payload', row.submission_payload['exec_dispatch'])
            self.assertNotIn('decider_input_context', payload_text)

    async def test_dispatch_next_open_plan_signal_rejected_agent_quantity_stays_delegated(self) -> None:
        self.client.agent_sizing_payloads_by_agent['signal-decider'] = {
            'approved': False,
            'quantity': '5',
        }

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['submitted_count'], 0)
        self.assertEqual(result['summary']['delegated_for_exec_agent_sizing_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            entry_link = row.submission_payload['exec_dispatch']['item_links'][0]
            self.assertFalse(entry_link['response_item']['agent_sizing']['approved'])
            self.assertEqual(entry_link['response_item']['agent_sizing']['quantity'], '5')

    async def test_dispatch_next_open_plan_signal_handoff_strips_decider_only_labels(self) -> None:
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.request_payload = {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'signal_decider_policy': (
                    self._signal_decider_policy_request_fixture()
                ),
                'symbol_day_context': {
                    'company': {'market': 'KOSPI'},
                },
            }
            snapshot.response_payload = {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'next_day_open_action': 'buy',
                'risk_flags': ['Size account-aware.'],
            }

        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        entry_item = self.client.post_calls[0]['items'][0]
        handoff = entry_item['risk_context']['decider_handoff_context']
        self.assertNotIn('role', handoff['decider_input_context']['request_payload'])
        self.assertNotIn('agent_name', handoff['decider_input_context']['request_payload'])
        self.assertNotIn('role', handoff['decider_output']['response_payload'])
        self.assertNotIn('agent_name', handoff['decider_output']['response_payload'])
        self.assertEqual(handoff['decider_output']['response_payload']['next_day_open_action'], 'buy')

    async def test_dispatch_next_open_plan_uses_next_open_dry_run_default(self) -> None:
        settings.signal_decider_next_open_enabled = False
        settings.signal_decider_next_open_dry_run = True

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='test',
        )

        self.assertTrue(result['request']['dry_run'])
        self.assertEqual(result['summary']['preview_group_count'], 1)
        self.assertEqual(result['dispatch']['groups'][0]['dispatch_status'], 'preview')
        self.assertEqual(len(self.client.post_calls), 0)

    async def test_dispatch_next_open_plan_refreshes_closed_market_eod_into_pending_live_cost_gate(self) -> None:
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.request_payload = {
                'signal_decider_policy': (
                    self._signal_decider_policy_request_fixture()
                ),
                'symbol_day_context': {
                    'company': {'market': 'KOSPI'},
                    'pre_decider_enrichment_context': self._closed_market_pre_decider_context(),
                },
            }

        with (
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50_000}],
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_loop_service.get_policy_prompt_context',
                return_value={'status': 'active'},
            ),
        ):
            await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=True,
                dry_run=False,
                force=True,
                trigger='test',
            )

        signal_payload = next(
            payload
            for payload in self.client.post_calls
            if payload['metadata']['decision_agent_name'] == 'signal-decider'
        )
        entry = next(item for item in signal_payload['items'] if item['side'] == 'buy')
        self.assertEqual(
            entry['risk_context']['delegated_gate_codes'],
            ['transaction_cost_evidence_pending_live_delegated_to_exec_watch'],
        )
        self.assertEqual(
            entry['risk_context']['delegated_gates'][0]['observed_value'],
            None,
        )
        self.assertEqual(
            entry['risk_context']['validation']['performance_gate']['expected_return_economics']['status'],
            'pending_live_cost_confirmation',
        )

    async def test_dispatch_next_open_plan_fails_closed_before_post_when_delegated_gate_binding_is_incomplete(
        self,
    ) -> None:
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.feedback_received_at = None
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.validation_payload = {
                **(row.validation_payload or {}),
                'pre_decider_enrichment_context': self._closed_market_pre_decider_context(),
                'warnings': [
                    {
                        'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
                        'message': 'Exec must confirm live cost before any buy.',
                    }
                ],
                'performance_gate': {
                    'expected_return_economics': {
                        'status': 'pending_live_cost_confirmation',
                        'expected_slippage_bps': None,
                        'expected_slippage_basis': 'unavailable',
                        'server_non_spread_round_trip_cost_basis': 'deferred_to_exec_final_full_cost_check',
                        'estimated_total_transaction_cost_bps': None,
                    }
                },
            }

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertEqual(result['dispatch']['groups'][0]['dispatch_status'], 'contract_error')
        self.assertEqual(
            result['dispatch']['groups'][0]['error']['code'],
            'delegated_gate_contract_invalid',
        )

    async def test_dispatch_next_open_plan_respects_execution_enabled_gate(self) -> None:
        settings.signal_decider_next_open_enabled = False
        settings.signal_decider_next_open_dry_run = False

        with patch('app.orders.exec_integration.signal_decider_next_open_execution_service.refresh_plan_items') as refresh_plan:
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=True,
                trigger='test',
            )

        refresh_plan.assert_not_called()
        self.assertFalse(result['request']['dry_run'])
        self.assertEqual(result['summary']['reason'], 'execution_disabled')
        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(len(self.client.post_calls), 0)

    async def test_disabled_mode_revokes_existing_exec_buy_authority_before_returning(self) -> None:
        accepted = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='seed_delegated_authority',
        )
        self.assertEqual(accepted['summary']['dispatched_group_count'], 1)
        self.client.post_calls.clear()
        settings.signal_decider_exec_integration_mode = 'disabled'

        self.assertTrue(
            self.service.should_manage_delegated_authority(
                target_trade_date=self.target_trade_date,
            )
        )
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='explicit_mode_disable',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(result['summary']['reason'], 'execution_control_disabled')
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
        self.assertFalse(
            self.service.should_manage_delegated_authority(
                target_trade_date=self.target_trade_date,
            )
        )

    async def test_accepted_dispatch_persists_secret_free_destination_binding(self) -> None:
        secret_token = 'exec-token-must-never-be-persisted'
        settings.signal_decider_exec_base_url = 'HTTP://EXEC-A.TEST:80/api/'
        settings.signal_decider_exec_bearer_token = secret_token
        settings.signal_decider_exec_runtime_mode = 'paper'
        self.client.base_url = settings.signal_decider_exec_base_url
        self.client.bearer_token = secret_token

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='persist_destination_binding',
        )

        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            exec_dispatch = row.submission_payload['exec_dispatch']
            destination = exec_dispatch['destination_binding']
            self.assertEqual(
                destination,
                exec_dispatch['contract_proof']['destination_binding'],
            )
            self.assertEqual(
                destination['schema_version'],
                'core_exec_destination_binding_v1',
            )
            self.assertEqual(
                destination['contract_version'],
                EXECUTION_HANDOFF_CONTRACT_VERSION,
            )
            self.assertEqual(destination['base_url'], 'http://exec-a.test:80/api')
            self.assertEqual(destination['runtime_mode'], 'paper')
            self.assertEqual(destination['dispatch_source'], 'kai-trading-core')
            self.assertEqual(destination['account_reference'], self.account_reference)
            self.assertEqual(len(destination['destination_fingerprint']), 64)
            self.assertNotIn('bearer_token', destination)
            self.assertNotIn(secret_token, json.dumps(row.submission_payload))

    def test_integration_status_never_echoes_credentials_from_unsafe_base_url(self) -> None:
        settings.signal_decider_exec_base_url = 'https://operator:secret@exec-a.test/api'

        status = self.service.integration_status()

        self.assertIsNone(status['base_url'])
        self.assertIn('unsafe_base_url', {item['code'] for item in status['issues']})
        self.assertNotIn('operator', json.dumps(status))
        self.assertNotIn('secret', json.dumps(status))

    async def test_noncanonical_v4_dispatch_source_is_not_ready_or_posted(self) -> None:
        settings.signal_decider_exec_dispatch_source = 'legacy-custom-source'

        status = self.service.integration_status()
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='invalid_dispatch_source',
        )

        self.assertEqual(status['config_status'], 'not_configured')
        self.assertEqual(status['dispatch_source'], 'kai-trading-core')
        self.assertIn(
            'invalid_dispatch_source',
            {item['code'] for item in status['issues']},
        )
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        self.assertEqual(self.client.post_calls, [])

    async def test_restart_destination_change_never_revokes_old_authority_on_new_exec(
        self,
    ) -> None:
        old_token = 'old-exec-token'
        settings.signal_decider_exec_bearer_token = old_token
        self.client.bearer_token = old_token
        accepted = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='seed_old_exec_authority',
        )
        self.assertEqual(accepted['summary']['dispatched_group_count'], 1)

        settings.signal_decider_exec_base_url = 'https://exec-b.test/runtime/'
        settings.signal_decider_exec_bearer_token = 'new-exec-token'
        settings.signal_decider_exec_integration_mode = 'disabled'
        restarted_client = FakeExecClient()
        restarted_service = SignalDeciderExecIntegrationService(
            client=restarted_client
        )

        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='restart_with_different_exec_destination',
        )

        self.assertEqual(restarted_client.revocation_calls, [])
        self.assertEqual(result['summary']['revoked_group_count'], 0)
        self.assertEqual(result['summary']['revocation_manual_review_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'exec_destination_mismatch',
            )
            self.assertEqual(
                row.submission_payload['revocation']['destination_binding'],
                row.submission_payload['exec_dispatch']['destination_binding'],
            )

    async def test_restart_runtime_change_never_revokes_old_authority(self) -> None:
        accepted = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='seed_mock_exec_authority',
        )
        self.assertEqual(accepted['summary']['dispatched_group_count'], 1)

        settings.signal_decider_exec_runtime_mode = 'live'
        settings.signal_decider_exec_integration_mode = 'disabled'
        restarted_client = FakeExecClient()
        restarted_service = SignalDeciderExecIntegrationService(
            client=restarted_client
        )

        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='restart_with_different_exec_runtime',
        )

        self.assertEqual(restarted_client.revocation_calls, [])
        self.assertEqual(result['summary']['revocation_manual_review_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'exec_destination_mismatch',
            )

    async def test_bearer_rotation_keeps_same_destination_revocation_safe(self) -> None:
        settings.signal_decider_exec_bearer_token = 'token-before-rotation'
        self.client.bearer_token = settings.signal_decider_exec_bearer_token
        accepted = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='seed_before_token_rotation',
        )
        self.assertEqual(accepted['summary']['dispatched_group_count'], 1)

        settings.signal_decider_exec_bearer_token = 'token-after-rotation'
        settings.signal_decider_exec_integration_mode = 'disabled'
        restarted_client = FakeExecClient()
        restarted_service = SignalDeciderExecIntegrationService(
            client=restarted_client
        )

        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='restart_after_token_rotation',
        )

        self.assertEqual(len(restarted_client.revocation_calls), 1)
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')

    async def test_legacy_unproven_accepted_authority_is_never_redispatched(
        self,
    ) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.execution_status = EXECUTION_STATUS_SUBMITTED
            row.submitted_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': 'legacy-accepted-plan',
                    'dispatch_status': 'accepted',
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                            'intent': 'next_open_buy',
                            'side': 'buy',
                            'exec_plan_id': 'legacy-accepted-plan',
                            'exec_plan_item_id': 'legacy-exec-item',
                            'remote_status': 'accepted',
                        }
                    ],
                },
            }

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='legacy_rollout_guard',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertFalse(result['summary']['triggered'])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'unproven_remote_acceptance',
            )

    async def test_dispatch_write_ahead_survives_crash_after_remote_ack(self) -> None:
        original_persist = self.service._persist_dispatch_success

        with patch.object(
            self.service,
            '_persist_dispatch_success',
            side_effect=SystemExit('simulated crash after remote ACK'),
        ):
            with self.assertRaisesRegex(SystemExit, 'simulated crash'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_after_remote_ack',
                )

        self.assertIsNotNone(original_persist)
        self.assertEqual(len(self.client.post_calls), 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'dispatch_pending_remote_ack')
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'pending_remote_ack')
            self.assertEqual(
                exec_dispatch['dispatch_material_fingerprint'],
                self.client.post_calls[0]['metadata'][
                    'dispatch_material_fingerprint'
                ],
            )
            self.assertEqual(
                exec_dispatch['destination_binding']['base_url'],
                'http://exec.test:80',
            )
            self.assertEqual(
                {link['core_item_key'] for link in exec_dispatch['item_links']},
                {
                    item['core_item_key']
                    for item in self.client.post_calls[0]['items']
                },
            )

    async def test_disabled_mode_recovers_pending_remote_ack_and_revokes_authority(self) -> None:
        original_post = self.client.post_execution_plan

        async def accept_and_publish_readback(payload: dict) -> dict:
            response = await original_post(payload)
            self.client.plan_response = {
                'items': [
                    {
                        **copy.deepcopy(response),
                        'external_plan_id': payload['plan_id'],
                        'status': 'accepted',
                    }
                ]
            }
            return response

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=accept_and_publish_readback,
        ), patch.object(
            self.service,
            '_persist_dispatch_success',
            side_effect=SystemExit('simulated crash after remote ACK'),
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_disabled_recovery',
                )

        settings.signal_decider_exec_integration_mode = 'disabled'
        restarted_service = SignalDeciderExecIntegrationService(client=self.client)
        self.assertTrue(
            restarted_service.should_manage_delegated_authority(
                target_trade_date=self.target_trade_date,
            )
        )

        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=False,
            trigger='disabled_write_ahead_recovery',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertGreaterEqual(len(self.client.plan_calls), 1)
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(
            self.client.revocation_calls[0]['reason_code'],
            'execution_control_disabled',
        )
        self.assertEqual(result['summary']['reason'], 'execution_control_disabled')
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'execution_control_disabled',
            )
            self.assertEqual(
                row.submission_payload['revocation']['status'],
                'acknowledged',
            )

    async def test_restart_recovers_exact_accepted_write_ahead_before_refresh(
        self,
    ) -> None:
        original_post = self.client.post_execution_plan

        async def accept_and_publish_readback(payload: dict) -> dict:
            response = await original_post(payload)
            self.client.plan_response = {
                'items': [
                    {
                        **copy.deepcopy(response),
                        'external_plan_id': payload['plan_id'],
                        'status': 'accepted',
                    }
                ]
            }
            return response

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=accept_and_publish_readback,
        ), patch.object(
            self.service,
            '_persist_dispatch_success',
            side_effect=SystemExit('simulated crash after remote ACK'),
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_success_persist',
                )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=True,
            force=False,
            trigger='restart_reconcile_write_ahead',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertGreaterEqual(len(self.client.plan_calls), 1)
        self.assertFalse(result['summary']['triggered'])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertIn(
                row.execution_status,
                {
                    EXECUTION_STATUS_SUBMITTED,
                    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
                },
            )
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

    async def test_restart_retries_exact_write_ahead_after_confirmed_not_found(
        self,
    ) -> None:
        async def crash_before_remote_send(payload: dict) -> dict:
            raise SystemExit('simulated crash before remote send')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_before_remote_send,
        ):
            with self.assertRaisesRegex(SystemExit, 'before remote send'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_remote_send',
                )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        events: list[str] = []
        original_list = self.client.list_execution_plans
        original_post = self.client.post_execution_plan

        async def synchronize_authority(**_kwargs: object) -> dict:
            events.append('authority')
            return {
                'applies': True,
                'accounts': {
                    self.account_reference: {
                        'dispatch_allowed': True,
                        'errors': [],
                    }
                },
                'errors': [],
            }

        async def record_readback(*, external_plan_id: str | None = None) -> dict:
            events.append('readback')
            return await original_list(external_plan_id=external_plan_id)

        async def record_retry(payload: dict) -> dict:
            events.append('plan')
            return await original_post(payload)

        with (
            patch.object(
                restarted_service,
                '_synchronize_open_buy_authority',
                side_effect=synchronize_authority,
            ),
            patch.object(
                self.client,
                'list_execution_plans',
                side_effect=record_readback,
            ),
            patch.object(
                self.client,
                'post_execution_plan',
                side_effect=record_retry,
            ),
        ):
            result = await restarted_service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                force=False,
                trigger='restart_retry_exact_write_ahead',
            )

        self.assertEqual(events[:3], ['authority', 'readback', 'plan'])
        self.assertEqual(len(self.client.plan_calls), 1)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

    async def test_restart_never_retries_write_ahead_after_conflicting_readback(
        self,
    ) -> None:
        async def crash_before_remote_send(payload: dict) -> dict:
            raise SystemExit('simulated crash before remote send')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_before_remote_send,
        ):
            with self.assertRaisesRegex(SystemExit, 'before remote send'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='seed_pending_write_ahead',
                )

        self.client.plan_response = {
            'items': [
                {
                    'plan_id': 'unexpected-plan',
                    'external_plan_id': 'unexpected-plan',
                    'status': 'accepted',
                }
            ]
        }
        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )

        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=False,
            trigger='conflicting_readback',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'dispatch_write_ahead_readback_conflict',
            )

    async def test_restart_does_not_retry_write_ahead_when_buy_authority_blocks(
        self,
    ) -> None:
        async def crash_before_remote_send(payload: dict) -> dict:
            raise SystemExit('simulated crash before remote send')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_before_remote_send,
        ):
            with self.assertRaisesRegex(SystemExit, 'before remote send'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_blocked_retry',
                )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        blocked_sync = {
            'applies': True,
            'accounts': {
                self.account_reference: {
                    'dispatch_allowed': False,
                    'errors': ['authority ACK unavailable'],
                }
            },
            'errors': [],
        }
        with patch.object(
            restarted_service,
            '_synchronize_open_buy_authority',
            new=AsyncMock(return_value=blocked_sync),
        ) as synchronize:
            result = await restarted_service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                force=False,
                trigger='restart_blocked_write_ahead_retry',
            )

        synchronize.assert_awaited_once()
        self.assertEqual(len(self.client.plan_calls), 1)
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(
                row.execution_status,
                'dispatch_pending_remote_ack',
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['dispatch_status'],
                'pending_remote_ack',
            )

    async def test_risk_off_buy_authority_still_dispatches_monitor_only_watch(
        self,
    ) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(
            snapshot_id=short_snapshot_id,
        )
        with session_scope() as session:
            signal_row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            signal_row.eligibility_status = 'blocked'
            signal_row.execution_status = 'blocked'
            signal_row.validation_payload = {
                **(signal_row.validation_payload or {}),
                'blocking_reasons': [
                    {
                        'code': 'test_signal_buy_block',
                        'message': 'Keep the ordinary BUY out of this watch-only group.',
                    }
                ],
            }
            watch_row = session.get(
                SignalDeciderExecutionPlanItem,
                short_plan_item_id,
            )
            watch_row.eligibility_status = 'monitor_candidate'
            watch_row.execution_status = 'awaiting_trigger'

        blocked_sync = {
            'applies': True,
            'accounts': {
                self.account_reference: {
                    'dispatch_allowed': False,
                    'fence_active': True,
                    'errors': [],
                }
            },
            'errors': [],
        }
        with (
            patch.object(
                self.service,
                '_synchronize_open_buy_authority',
                new=AsyncMock(return_value=blocked_sync),
            ) as synchronize,
            patch(
                'app.orders.next_open_execution._current_live_market_buy_authority',
                return_value={'applies': True, 'action': 'reject'},
            ),
        ):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='risk_off_monitor_only_watch',
            )

        synchronize.assert_awaited_once()
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        self.assertNotIn(
            'core_exec_buy_authority_not_acknowledged',
            {
                group.get('reason')
                for group in result['dispatch']['groups']
            },
        )
        entry = next(
            item
            for item in self.client.post_calls[0]['items']
            if item['side'] == 'buy'
        )
        self.assertEqual(entry['intent'], 'intraday_watch_buy')

    async def test_restart_never_retries_non_buy_write_ahead_as_buy(self) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.planned_side = 'sell'

        async def crash_before_remote_send(payload: dict) -> dict:
            self.assertEqual(payload['items'][0]['side'], 'buy')
            raise SystemExit('simulated legacy non-buy write-ahead crash')

        with (
            patch.object(
                self.service,
                '_row_is_dispatchable',
                return_value=True,
            ),
            patch.object(
                self.client,
                'post_execution_plan',
                side_effect=crash_before_remote_send,
            ),
        ):
            with self.assertRaisesRegex(SystemExit, 'legacy non-buy'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='seed_legacy_non_buy_write_ahead',
                )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        result = await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=False,
            trigger='restart_legacy_non_buy_write_ahead',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'dispatch_write_ahead_non_buy_invariant_violation',
            )

    async def test_restart_drops_not_found_write_ahead_when_decision_changed(
        self,
    ) -> None:
        async def crash_before_remote_send(payload: dict) -> dict:
            raise SystemExit('simulated crash before remote send')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_before_remote_send,
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_changed_decision',
                )

        with session_scope() as session:
            prior = session.get(
                SignalDeciderDecisionSnapshot,
                self.snapshot_id,
            )
            values = {
                column.name: copy.deepcopy(getattr(prior, column.name))
                for column in SignalDeciderDecisionSnapshot.__table__.columns
                if column.name not in {'id', 'created_at', 'updated_at'}
            }
            values.update(
                {
                    'feedback_received_at': datetime(
                        2026,
                        4,
                        29,
                        6,
                        1,
                        tzinfo=timezone.utc,
                    ),
                    'decision_action': 'wait',
                    'preferred_action': 'wait',
                    'next_day_open_action': 'wait',
                    'response_payload': {'next_day_open_action': 'wait'},
                }
            )
            session.add(SignalDeciderDecisionSnapshot(**values))

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=True,
            force=False,
            trigger='restart_with_changed_decision',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(self.client.revocation_calls, [])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertNotEqual(
                row.execution_status,
                'dispatch_pending_remote_ack',
            )
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(
                row.validation_payload['blocking_reasons'][0]['code'],
                'latest_snapshot_not_actionable',
            )

    async def test_concurrent_workers_converge_after_201_and_409(self) -> None:
        first_started = asyncio.Event()
        allow_first_to_finish = asyncio.Event()
        first_published = asyncio.Event()
        post_attempt_count = 0
        original_post = self.client.post_execution_plan

        async def concurrent_post(payload: dict) -> dict:
            nonlocal post_attempt_count
            post_attempt_count += 1
            if post_attempt_count == 1:
                first_started.set()
                await allow_first_to_finish.wait()
                response = await original_post(payload)
                self.client.plan_response = {
                    'items': [
                        {
                            **copy.deepcopy(response),
                            'external_plan_id': payload['plan_id'],
                            'status': 'accepted',
                        }
                    ]
                }
                first_published.set()
                return response
            allow_first_to_finish.set()
            await first_published.wait()
            raise SignalDeciderExecClientError(
                'concurrent execution plan already accepted',
                code='conflict',
                retryable=False,
                status_code=409,
            )

        second_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        first_task: asyncio.Task | None = None
        second_task: asyncio.Task | None = None
        combined_future: asyncio.Future | None = None
        try:
            with patch.object(
                self.client,
                'post_execution_plan',
                side_effect=concurrent_post,
            ):
                first_task = asyncio.create_task(
                    self.service.dispatch_next_open_plan(
                        target_trade_date=self.target_trade_date,
                        refresh_plan=False,
                        force=True,
                        trigger='concurrent_worker_one',
                    )
                )
                await asyncio.wait_for(first_started.wait(), timeout=2)
                second_task = asyncio.create_task(
                    second_service.dispatch_next_open_plan(
                        target_trade_date=self.target_trade_date,
                        refresh_plan=False,
                        force=False,
                        trigger='concurrent_worker_two',
                    )
                )
                combined_future = asyncio.gather(first_task, second_task)
                first_result, second_result = await asyncio.wait_for(
                    asyncio.shield(combined_future),
                    timeout=5,
                )
        finally:
            # A timeout must not cancel a task while its asyncio.to_thread DB
            # work continues beyond teardown. Release the controlled waits and
            # drain both tasks to their natural terminal state.
            allow_first_to_finish.set()
            first_published.set()
            tasks = [
                task
                for task in (first_task, second_task)
                if task is not None
            ]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        self.assertEqual(post_attempt_count, 2)
        self.assertEqual(self.client.revocation_calls, [])
        dispatch_statuses = {
            result['dispatch']['groups'][0]['dispatch_status']
            for result in (first_result, second_result)
        }
        self.assertIn('accepted', dispatch_statuses)
        self.assertTrue(
            dispatch_statuses.issubset(
                {'accepted', 'already_dispatched', 'pending_remote_ack'}
            ),
            dispatch_statuses,
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertTrue(_row_has_accepted_exec_dispatch(row))
            self.assertNotIn('revocation', row.submission_payload)

    async def test_restart_destination_change_quarantines_pending_write_ahead(
        self,
    ) -> None:
        with patch.object(
            self.service,
            '_persist_dispatch_success',
            side_effect=SystemExit('simulated crash after remote ACK'),
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_destination_change',
                )

        settings.signal_decider_exec_base_url = 'http://exec-b.test'
        replacement_client = FakeExecClient(base_url='http://exec-b.test')
        restarted_service = SignalDeciderExecIntegrationService(
            client=replacement_client
        )
        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=True,
            force=True,
            trigger='restart_on_different_destination',
        )

        self.assertEqual(replacement_client.plan_calls, [])
        self.assertEqual(replacement_client.post_calls, [])
        self.assertEqual(replacement_client.revocation_calls, [])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation'][
                    'reason_code'
                ],
                'exec_destination_mismatch',
            )

    async def test_duplicate_success_persistence_is_idempotent(self) -> None:
        detached_rows = self.service._load_next_open_rows(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
        )
        payload, row_links = self.service._build_exec_plan_payload(
            rows=detached_rows,
            snapshots=self.service._load_snapshots(detached_rows),
            trigger='concurrent_success_persistence',
        )
        revision_fingerprints = {
            int(row.id): _row_dispatch_revision_fingerprint(row)
            for row in detached_rows
        }
        fingerprint = _fingerprint_exec_plan(payload)
        self.service._persist_dispatch_write_ahead(
            rows=detached_rows,
            submitted_revision_fingerprints=revision_fingerprints,
            row_links=row_links,
            plan_id=payload['plan_id'],
            fingerprint=fingerprint,
            payload=payload,
            trigger='concurrent_success_persistence',
            destination_binding=self.service._destination_binding_for_payload(
                payload
            ),
            authority_generation_before=None,
        )
        response = await self.client.post_execution_plan(payload)

        first = self.service._persist_dispatch_success(
            rows=detached_rows,
            submitted_revision_fingerprints=revision_fingerprints,
            row_links=row_links,
            plan_id=payload['plan_id'],
            fingerprint=fingerprint,
            payload=payload,
            response_payload=response,
            trigger='first_success_writer',
        )
        second = self.service._persist_dispatch_success(
            rows=detached_rows,
            submitted_revision_fingerprints=revision_fingerprints,
            row_links=row_links,
            plan_id=payload['plan_id'],
            fingerprint=fingerprint,
            payload=payload,
            response_payload=response,
            trigger='second_success_writer',
        )

        self.assertEqual(first['dispatch_status'], 'accepted')
        self.assertEqual(second['dispatch_status'], 'accepted')
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertTrue(_row_has_accepted_exec_dispatch(row))
            self.assertNotIn('revocation', row.submission_payload)

    async def test_write_ahead_409_recovers_only_exact_remote_material(
        self,
    ) -> None:
        submitted_payload: dict = {}

        async def crash_before_remote_send(payload: dict) -> dict:
            submitted_payload.update(copy.deepcopy(payload))
            raise SystemExit('simulated crash before remote send')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_before_remote_send,
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='seed_pending_for_409_recovery',
                )

        exact_response = await FakeExecClient.post_execution_plan(
            self.client,
            submitted_payload,
        )
        exact_readback = {
            'items': [
                {
                    **copy.deepcopy(exact_response),
                    'external_plan_id': submitted_payload['plan_id'],
                    'status': 'accepted',
                }
            ]
        }
        self.client.post_calls.clear()
        readback_count = 0

        async def readback_after_duplicate(
            *, external_plan_id: str | None = None
        ) -> dict:
            nonlocal readback_count
            self.client.plan_calls.append(
                {'external_plan_id': external_plan_id}
            )
            readback_count += 1
            return {'items': []} if readback_count == 1 else exact_readback

        async def duplicate_conflict(payload: dict) -> dict:
            self.client.post_calls.append(copy.deepcopy(payload))
            raise SignalDeciderExecClientError(
                'execution plan already exists',
                code='conflict',
                retryable=False,
                status_code=409,
            )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        with patch.object(
            self.client,
            'list_execution_plans',
            side_effect=readback_after_duplicate,
        ), patch.object(
            self.client,
            'post_execution_plan',
            side_effect=duplicate_conflict,
        ):
            await restarted_service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                force=False,
                trigger='recover_exact_409_material',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(readback_count, 2)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertTrue(_row_has_accepted_exec_dispatch(row))
            self.assertNotIn('revocation', row.submission_payload)

    async def test_write_ahead_409_with_old_material_enters_manual_review(
        self,
    ) -> None:
        submitted_payload: dict = {}

        async def crash_before_remote_send(payload: dict) -> dict:
            submitted_payload.update(copy.deepcopy(payload))
            raise SystemExit('simulated crash before remote send')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_before_remote_send,
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='seed_pending_for_old_409',
                )

        old_response = await FakeExecClient.post_execution_plan(
            self.client,
            submitted_payload,
        )
        old_response['contract_ack']['dispatch_material_fingerprint'] = '0' * 64
        old_readback = {
            'items': [
                {
                    **copy.deepcopy(old_response),
                    'external_plan_id': submitted_payload['plan_id'],
                    'status': 'accepted',
                }
            ]
        }
        self.client.post_calls.clear()
        readback_count = 0

        async def readback_after_duplicate(
            *, external_plan_id: str | None = None
        ) -> dict:
            nonlocal readback_count
            self.client.plan_calls.append(
                {'external_plan_id': external_plan_id}
            )
            readback_count += 1
            return {'items': []} if readback_count == 1 else old_readback

        async def duplicate_conflict(payload: dict) -> dict:
            self.client.post_calls.append(copy.deepcopy(payload))
            raise SignalDeciderExecClientError(
                'plan id belongs to older dispatch material',
                code='conflict',
                retryable=False,
                status_code=409,
            )

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        with patch.object(
            self.client,
            'list_execution_plans',
            side_effect=readback_after_duplicate,
        ), patch.object(
            self.client,
            'post_execution_plan',
            side_effect=duplicate_conflict,
        ):
            await restarted_service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                force=False,
                trigger='reject_old_409_material',
            )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.revocation_calls, [])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertIn(
                row.submission_payload['dispatch_reconciliation'][
                    'reason_code'
                ],
                {
                    'ambiguous_remote_acceptance',
                    'ambiguous_remote_acceptance_after_core_revision',
                },
            )

    async def test_restart_revokes_recovered_ack_when_latest_decision_changed(
        self,
    ) -> None:
        original_post = self.client.post_execution_plan

        async def accept_and_publish_readback(payload: dict) -> dict:
            response = await original_post(payload)
            self.client.plan_response = {
                'items': [
                    {
                        **copy.deepcopy(response),
                        'external_plan_id': payload['plan_id'],
                        'status': 'accepted',
                    }
                ]
            }
            return response

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=accept_and_publish_readback,
        ), patch.object(
            self.service,
            '_persist_dispatch_success',
            side_effect=SystemExit('simulated crash after remote ACK'),
        ):
            with self.assertRaises(SystemExit):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    refresh_plan=False,
                    force=True,
                    trigger='crash_before_new_wait',
                )

        with session_scope() as session:
            prior = session.get(
                SignalDeciderDecisionSnapshot,
                self.snapshot_id,
            )
            values = {
                column.name: copy.deepcopy(getattr(prior, column.name))
                for column in SignalDeciderDecisionSnapshot.__table__.columns
                if column.name not in {'id', 'created_at', 'updated_at'}
            }
            values.update(
                {
                    'feedback_received_at': datetime(
                        2026,
                        4,
                        29,
                        6,
                        1,
                        tzinfo=timezone.utc,
                    ),
                    'decision_action': 'wait',
                    'preferred_action': 'wait',
                    'next_day_open_action': 'wait',
                    'response_payload': {'next_day_open_action': 'wait'},
                }
            )
            session.add(SignalDeciderDecisionSnapshot(**values))

        restarted_service = SignalDeciderExecIntegrationService(
            client=self.client
        )
        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=True,
            force=False,
            trigger='restart_after_new_wait',
        )

        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.revocation_calls), 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'latest_snapshot_not_actionable',
            )

    async def test_disabled_mode_reconciles_ambiguous_acceptance_before_revocation(self) -> None:
        submitted_payload: dict = {}

        async def timeout_without_visible_readback(payload: dict) -> dict:
            submitted_payload.update(copy.deepcopy(payload))
            self.client.post_calls.append(payload)
            self.client.plan_response = {'items': []}
            raise SignalDeciderExecClientError(
                'exec request timed out before ACK',
                code='unavailable',
                retryable=True,
            )

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=timeout_without_visible_readback,
        ):
            await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='seed_ambiguous_remote_acceptance',
            )

        self.assertTrue(submitted_payload)
        self.client.plan_response = _accepted_plan_readback_for_payload(
            submitted_payload
        )
        settings.signal_decider_exec_integration_mode = 'disabled'

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='disable_after_ambiguous_acceptance',
        )

        self.assertGreaterEqual(len(self.client.plan_calls), 2)
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'execution_control_disabled',
            )

    async def test_ambiguous_readback_never_crosses_exec_destination(self) -> None:
        submitted_payload: dict = {}

        async def timeout_after_send(payload: dict) -> dict:
            submitted_payload.update(copy.deepcopy(payload))
            raise SignalDeciderExecClientError(
                'exec request timed out after send',
                code='unavailable',
                retryable=True,
            )

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=timeout_after_send,
        ):
            await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                force=True,
                trigger='seed_ambiguous_destination_a',
            )

        self.assertTrue(submitted_payload)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            original_links = copy.deepcopy(
                row.submission_payload['exec_dispatch']['item_links']
            )

        settings.signal_decider_exec_integration_mode = 'disabled'
        settings.signal_decider_exec_base_url = 'http://exec-b.test'
        destination_b = FakeExecClient(base_url='http://exec-b.test')
        destination_b.plan_response = {
            'items': [
                {
                    'external_plan_id': submitted_payload['plan_id'],
                    'execution_plan_id': 'exec-plan-from-b',
                    'status': 'accepted',
                    'items': [
                        {
                            'core_item_key': item['core_item_key'],
                            'execution_plan_id': 'exec-plan-from-b',
                            'execution_plan_item_id': f'exec-b-{index}',
                            'symbol': item['symbol'],
                            'intent': item['intent'],
                            'status': 'accepted',
                        }
                        for index, item in enumerate(
                            submitted_payload['items'],
                            start=1,
                        )
                    ],
                }
            ]
        }
        restarted_service = SignalDeciderExecIntegrationService(
            client=destination_b
        )

        await restarted_service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='ambiguous_readback_on_destination_b',
        )

        self.assertEqual(destination_b.plan_calls, [])
        self.assertEqual(destination_b.order_calls, [])
        self.assertEqual(destination_b.execution_calls, [])
        self.assertEqual(destination_b.position_calls, [])
        self.assertEqual(destination_b.revocation_calls, [])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation'][
                    'reason_code'
                ],
                'exec_destination_mismatch',
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['item_links'],
                original_links,
            )

    async def test_next_open_disable_revokes_existing_exec_buy_authority_before_returning(self) -> None:
        accepted = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            force=True,
            trigger='seed_delegated_authority',
        )
        self.assertEqual(accepted['summary']['dispatched_group_count'], 1)
        self.client.post_calls.clear()
        settings.signal_decider_next_open_enabled = False

        self.assertTrue(
            self.service.should_manage_delegated_authority(
                target_trade_date=self.target_trade_date,
            )
        )
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='explicit_next_open_disable',
        )

        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(len(self.client.revocation_calls), 1)
        self.assertEqual(result['summary']['reason'], 'execution_disabled')
        self.assertEqual(result['summary']['revoked_group_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revoked')

    async def test_dispatch_next_open_plan_includes_short_term_groups_with_explicit_identity(self) -> None:
        decider_request_payload = {
            'symbol_day_context': {
                'company': {'market': 'KOSPI'},
                'pre_decider_enrichment_context': {
                    'schema_version': 'pre_decider_enrichment_v1',
                    'quote_orderbook_snapshot': {'status': 'stale_eod', 'spread_bps': 17.5},
                    'trading_value': {
                        'status': 'available',
                        'source': 'raw',
                        'raw_trading_value_krw': 9_870_000_000,
                        'trading_value_krw': 9_870_000_000,
                    },
                },
                'market_relative_context': {
                    'relative_strength': {
                        'return_percentile': 0.82,
                        'volume_percentile': 0.76,
                    },
                },
                'liquidity_context': {
                    'turnover': {
                        'status': 'available',
                        'trading_value_source': 'raw',
                        'raw_trading_value_krw': 9_870_000_000,
                    },
                },
                'decision_input_quality_context': {
                    'schema_version': 'decision_input_quality_v1',
                    'execution_feasibility': {
                        'status': 'usable',
                        'liquidity_tier': 'medium',
                        'max_safe_notional_krw': 49350000,
                        'expected_slippage_bps': 17.5,
                        'expected_slippage_basis': 'quote_spread_bps',
                        'sizing_hints': [{'code': 'max_safe_notional', 'max_safe_notional_krw': 49350000}],
                    },
                    'final_judgment': {
                        'buy_quality_score': 66,
                        'decision_band': 'small_probe',
                    },
                },
                'market_regime_context': self._market_regime_context(),
            },
            'orchestration': {'run_id': 'symbol-run-1'},
        }
        decider_input_features = {
            'technical': {'trend_state': 'uptrend'},
            'snapshots': {'market_risk': {'risk_level': 'normal'}},
        }
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload=decider_request_payload,
            input_feature_snapshot_payload=decider_input_features,
            confidence_score_10=5.2,
            response_payload_extra={
                'trigger_conditions': ['Reclaim 50500 with volume.'],
                'risk_flags': ['Opening gap risk.'],
            },
        )
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        alignment_gate = {
            'schema_version': 'signal_decider_alignment_gate_v1',
            'enforcement_mode': 'report_only',
            'signal_decider_alignment': 'wait_conflict_block',
            'alignment_gate_action': 'report_only_block_candidate',
            'alignment_gate_reason': 'Signal-decider is WAIT for the same symbol/date context.',
            'signal_decider_decision_snapshot_id': 991,
            'decision_snapshot_id': 991,
            'enforcement_decision': 'allowed_report_only',
            'requested_quantity': 29,
            'final_quantity': 29,
            'probe_quantity': 1,
        }
        with session_scope() as session:
            signal_row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            signal_row.validation_payload = {
                **(signal_row.validation_payload or {}),
                'pre_decider_enrichment_context': self._closed_market_pre_decider_context(),
                'warnings': [
                    {
                        'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
                        'message': 'Exec must confirm live cost before any buy.',
                    }
                ],
                'performance_gate': {
                    'expected_return_economics': {
                        'status': 'pending_live_cost_confirmation',
                        'expected_slippage_bps': None,
                        'expected_slippage_basis': 'unavailable',
                        'server_non_spread_round_trip_cost_basis': 'deferred_to_exec_final_full_cost_check',
                        'estimated_total_transaction_cost_bps': None,
                    }
                },
            }
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            row.validation_payload = {
                **(row.validation_payload or {}),
                'warnings': [
                    {
                        'code': 'short_term_liquidity_floor_block_delegated_to_exec_watch',
                        'message': 'Exec must confirm liquidity at trigger time.',
                    },
                    {
                        'code': 'short_term_reward_risk_ratio_block_delegated_to_exec_watch',
                        'message': 'Exec must confirm reward/risk at trigger time.',
                    },
                ],
                'short_term_entry_quality': {
                    'raw_trading_value_krw': 2_500_000_000,
                    'reward_risk_ratio': '1.50',
                },
                'alignment_gate': alignment_gate,
                'signal_decider_alignment': 'wait_conflict_block',
                'alignment_gate_action': 'report_only_block_candidate',
                'alignment_gate_reason': alignment_gate['alignment_gate_reason'],
                'alignment_signal_decider_decision_snapshot_id': 991,
            }
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'alignment_gate': alignment_gate,
                'signal_decider_alignment': 'wait_conflict_block',
                'alignment_gate_action': 'report_only_block_candidate',
                'alignment_gate_reason': alignment_gate['alignment_gate_reason'],
                'alignment_signal_decider_decision_snapshot_id': 991,
            }

        signal_rows = [
            row
            for row in self.service._load_next_open_rows(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
            )
            if row.decision_agent_name == 'signal-decider'
        ]
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(len(self.client.post_calls), 2)
        by_agent = {
            payload['metadata']['decision_agent_name']: payload
            for payload in self.client.post_calls
        }
        self.assertEqual(set(by_agent), {'signal-decider', 'short-term-decider'})
        signal_payload = by_agent['signal-decider']
        short_payload = by_agent['short-term-decider']
        base_plan_id = f'core-signal_decider-20260430-{self.account_reference.replace(":", "_").replace("-", "_")}'
        self.assertEqual(
            signal_payload['plan_id'],
            _delta_dispatch_plan_id(
                base_plan_id,
                _delta_dispatch_batch_key(signal_rows),
            ),
        )
        self.assertEqual(short_payload['metadata']['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_payload['strategy_run_id'], f'short-term-decider:2026-04-30:{self.account_reference.replace(":", "_").replace("-", "_")}')
        signal_entry = signal_payload['items'][0]
        self.assertEqual(signal_entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertNotIn('quantity', signal_entry['quantity_policy'])
        self.assertEqual(signal_entry['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(signal_entry['risk_context']['market_regime_context']['schema_version'], 'market_regime_context_v1')
        self.assertEqual(signal_entry['risk_context']['market_regime_context']['recommended_exec_action'], 'downsize')
        self.assertEqual(signal_entry['risk_context']['max_holding_close_policy']['enabled'], False)
        self.assertIsNone(signal_entry['risk_context']['max_holding_close_policy']['max_holding_trading_sessions'])
        self.assertEqual(
            signal_entry['risk_context']['delegated_gate_codes'],
            ['transaction_cost_evidence_pending_live_delegated_to_exec_watch'],
        )
        self.assertEqual(
            signal_entry['risk_context']['delegated_gates'],
            [
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
                }
            ],
        )
        self.assertIsNone(signal_entry['risk_context']['delegated_gates'][0]['observed_value'])
        self.assertLessEqual(
            datetime.fromisoformat(signal_entry['risk_context']['delegated_gates'][0]['observed_at']),
            datetime.fromisoformat(signal_entry['risk_context']['delegated_gates'][0]['expires_at']),
        )
        self.assertNotIn('max_holding_close_sell', [item['intent'] for item in signal_payload['items']])
        short_entry = short_payload['items'][0]
        self.assertEqual(short_entry['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['intent'], EXEC_INTENT_INTRADAY_WATCH_BUY)
        self.assertEqual(short_entry['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(short_entry['raw_action'], 'watch_for_trigger')
        self.assertEqual(short_entry['tactical_action'], 'watch_for_trigger')
        self.assertEqual(short_entry['generic_action'], 'monitor_candidate')
        self.assertEqual(short_entry['trigger_price'], 50500)
        self.assertEqual(short_entry['limit_price'], 51500)
        self.assertEqual(short_entry['risk_context']['agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['risk_context']['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['risk_context']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(short_entry['risk_context']['generic_action'], 'monitor_candidate')
        self.assertEqual(short_entry['risk_context']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_entry['risk_context']['alignment_gate_action'], 'report_only_block_candidate')
        self.assertEqual(short_entry['risk_context']['final_enforcement_decision'], 'allowed_report_only')
        self.assertEqual(short_entry['risk_context']['requested_quantity'], '29')
        self.assertEqual(short_entry['risk_context']['final_quantity'], '29')
        self.assertEqual(short_entry['risk_context']['alignment_signal_decider_decision_snapshot_id'], 991)
        self.assertEqual(short_entry['risk_context']['entry_execution_mode'], 'pending_trigger_limit')
        self.assertTrue(short_entry['risk_context']['monitoring_candidate'])
        self.assertEqual(short_entry['risk_context']['monitoring_intent'], EXEC_INTENT_INTRADAY_WATCH_BUY)
        self.assertEqual(short_entry['risk_context']['original_watch_trigger_price'], 50500)
        self.assertEqual(short_entry['risk_context']['original_watch_chase_limit_price'], 51500)
        self.assertNotIn('trigger_price', short_entry['risk_context'])
        self.assertNotIn('chase_limit_price', short_entry['risk_context'])
        self.assertIn('source_feedback_id', short_entry['risk_context'])
        self.assertIn('target_trade_date', short_entry['risk_context'])
        self.assertIn('source_market_date', short_entry['risk_context'])
        self.assertEqual(short_entry['risk_context']['allocation_mode'], 'specified')
        self.assertEqual(Decimal(str(short_entry['risk_context']['allocation_budget_krw'])), Decimal('1500000'))
        self.assertEqual(short_entry['risk_context']['confidence_score_10'], 5.2)
        self.assertEqual(short_entry['risk_context']['conviction_score_10'], 6.5)
        self.assertEqual(short_entry['risk_context']['decision_run_id'], 'short-run-1')
        self.assertEqual(short_entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertEqual(short_entry['quantity_policy']['final_enforcement_decision'], 'allowed_report_only')
        self.assertEqual(short_entry['quantity_policy']['requested_quantity'], '29')
        self.assertEqual(short_entry['quantity_policy']['final_quantity'], '29')
        self.assertEqual(Decimal(str(short_entry['quantity_policy']['budget_krw'])), Decimal('1500000'))
        self.assertEqual(short_entry['quantity_policy']['allocation_mode'], 'specified')
        self.assertEqual(short_entry['quantity_policy']['account_reference'], self.account_reference)
        self.assertEqual(short_entry['quantity_policy']['account_label'], 'Main')
        self.assertEqual(short_entry['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(short_entry['risk_context']['market_regime_context']['schema_version'], 'market_regime_context_v1')
        self.assertEqual(short_entry['risk_context']['market_regime_context']['recommended_exec_action'], 'downsize')
        short_items_by_intent = {item['intent']: item for item in short_payload['items']}
        self.assertEqual(
            set(short_items_by_intent),
            {EXEC_INTENT_INTRADAY_WATCH_BUY, EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL},
        )
        short_entry_key = short_entry['core_item_key']
        for exit_intent in (EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL):
            exit_policy = short_items_by_intent[exit_intent]['quantity_policy']
            self.assertEqual(exit_policy['type'], 'position_exit_from_entry')
            self.assertEqual(exit_policy['linked_entry_item_key'], short_entry_key)
            self.assertNotIn('quantity', exit_policy)
        self.assertEqual(
            short_entry['risk_context']['max_holding_close_policy'],
            {
                'enabled': True,
                'type': 'exec_agent_default',
                'intent': 'max_holding_close_sell',
                'max_holding_trading_sessions': 3,
                'reason': 'short_term_exec_agent_sizing_default',
            },
        )
        handoff = short_entry['risk_context']['decider_handoff_context']
        self.assertEqual(handoff['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(handoff['symbol'], self.symbol)
        self.assertEqual(handoff['target_trade_date'], '2026-04-30')
        self.assertEqual(handoff['source_market_date'], '2026-04-29')
        self.assertEqual(handoff['traceability']['decision_snapshot_id'], short_snapshot_id)
        self.assertEqual(handoff['traceability']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(handoff['traceability']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(handoff['traceability']['alignment_gate_action'], 'report_only_block_candidate')
        self.assertEqual(handoff['traceability']['final_enforcement_decision'], 'allowed_report_only')
        self.assertEqual(handoff['traceability']['requested_quantity'], 29)
        self.assertEqual(handoff['traceability']['final_quantity'], 29)
        self.assertEqual(handoff['decider_output']['generic_action'], 'monitor_candidate')
        self.assertEqual(handoff['decider_output']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(handoff['decider_output']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(handoff['decider_output']['final_enforcement_decision'], 'allowed_report_only')
        self.assertEqual(handoff['traceability']['core_plan_item_id'], short_entry['risk_context']['core_plan_item_id'])
        self.assertEqual(handoff['decider_input_context']['request_payload'], decider_request_payload)
        self.assertEqual(handoff['decider_input_context']['input_feature_snapshot_payload'], decider_input_features)
        self.assertEqual(
            handoff['decider_input_context']['pre_decider_enrichment_context'],
            decider_request_payload['symbol_day_context']['pre_decider_enrichment_context'],
        )
        self.assertEqual(
            handoff['decider_input_context']['decision_input_quality_context']['schema_version'],
            'decision_input_quality_v1',
        )
        self.assertEqual(
            handoff['decider_input_context']['market_regime_context']['schema_version'],
            'market_regime_context_v1',
        )
        self.assertEqual(handoff['market_regime_context']['recommended_exec_action'], 'downsize')
        self.assertEqual(
            short_entry['risk_context']['decision_input_quality_context']['schema_version'],
            'decision_input_quality_v1',
        )
        self.assertEqual(
            short_entry['risk_context']['execution_feasibility_context']['liquidity_tier'],
            'medium',
        )
        self.assertEqual(
            short_entry['risk_context']['delegated_gate_codes'],
            [
                'short_term_liquidity_floor_block_delegated_to_exec_watch',
                'short_term_reward_risk_ratio_block_delegated_to_exec_watch',
            ],
        )
        self.assertTrue(short_entry['risk_context']['delegated_gate_confirmation_required'])
        self.assertEqual(
            short_entry['risk_context']['delegated_gates'],
            [
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
                    'code': 'short_term_reward_risk_ratio_block_delegated_to_exec_watch',
                    'observed_value': '1.50',
                    'field_path': 'risk_context.validation.short_term_entry_quality.reward_risk_ratio',
                    'comparator': 'gte',
                    'threshold': '1.80',
                    'policy_version': 'core_delegated_gate_policy_v1',
                    'observed_at': '2026-04-29T06:00:00+00:00',
                    'recheck_source': 'exec_trigger_time_price_geometry_and_cost_model',
                    'expires_at': '2026-04-30T15:30:00+09:00',
                },
            ],
        )
        self.assertEqual(
            short_entry['risk_context']['execution_feasibility_context']['expected_slippage_bps'],
            17.5,
        )
        self.assertEqual(
            short_entry['risk_context']['execution_feasibility_context']['expected_slippage_basis'],
            'quote_spread_bps',
        )
        self.assertNotIn('delegated_gate_codes', handoff['traceability'])
        self.assertNotIn('delegated_gate_confirmation_required', handoff['traceability'])
        self.assertNotIn('delegated_gates', handoff['traceability'])
        for exit_intent in (EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL):
            exit_risk = short_items_by_intent[exit_intent]['risk_context']
            self.assertNotIn('delegated_gate_codes', exit_risk)
            self.assertNotIn('delegated_gate_confirmation_required', exit_risk)
            self.assertNotIn('delegated_gates', exit_risk)
        self.assertEqual(Decimal(str(handoff['decider_output']['confidence_score_10'])), Decimal('5.2'))
        self.assertIn('sizing_hints', short_entry['quantity_policy'])
        self.assertEqual(short_entry['quantity_policy']['max_safe_notional_krw'], 49350000)
        self.assertEqual(handoff['decider_output']['response_payload']['trigger_conditions'], ['Reclaim 50500 with volume.'])
        self.assertEqual(handoff['decider_output']['response_payload']['risk_flags'], ['Opening gap risk.'])
        self.assertEqual(Decimal(str(handoff['decider_output']['response_payload']['confidence_score_10'])), Decimal('5.2'))
        self.assertEqual(result['summary']['translated_item_count'], 6)
        self.assertEqual(
            {group['decision_agent_name'] for group in result['dispatch']['groups']},
            {'signal-decider', 'short-term-decider'},
        )
        short_item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        signal_item = next(item for item in result['items'] if item['decision_agent_name'] == 'signal-decider')
        self.assertEqual(signal_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(result['summary']['delegated_for_exec_agent_sizing_count'], 2)

    async def test_dispatch_next_open_plan_binds_core_downsize_quantity(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        alignment_gate = {
            'schema_version': 'signal_decider_alignment_gate_v1',
            'enforcement_mode': 'downsize_probe',
            'signal_decider_alignment': 'wait_conflict_block',
            'alignment_gate_action': 'downsize_probe',
            'alignment_gate_reason': 'WAIT conflict reduced to a deterministic probe.',
            'signal_decider_decision_snapshot_id': 991,
            'override': {'allowed': False},
            'enforcement_decision': 'downsized',
            'requested_quantity': 29,
            'final_quantity': 2,
            'probe_quantity': 2,
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            row.validation_payload = {
                **(row.validation_payload or {}),
                'alignment_gate': alignment_gate,
                'final_enforcement_decision': 'downsized',
                'requested_quantity': 29,
                'final_quantity': 2,
            }
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'alignment_gate': alignment_gate,
                'final_enforcement_decision': 'downsized',
                'requested_quantity': 29,
                'final_quantity': 2,
            }

        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        short_payload = next(
            payload
            for payload in self.client.post_calls
            if payload['metadata']['decision_agent_name'] == 'short-term-decider'
        )
        entry = next(item for item in short_payload['items'] if item['intent'] == EXEC_INTENT_INTRADAY_WATCH_BUY)
        self.assertEqual(entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertEqual(entry['quantity_policy']['max_quantity'], '2')
        self.assertNotIn('quantity', entry['quantity_policy'])
        self.assertEqual(entry['quantity_policy']['enforcement_source'], 'core_wait_conflict_alignment_gate')
        self.assertEqual(entry['quantity_policy']['requested_quantity'], '29')
        self.assertEqual(entry['quantity_policy']['final_quantity'], '2')
        self.assertEqual(entry['risk_context']['final_enforcement_decision'], 'downsized')
        self.assertEqual(entry['risk_context']['requested_quantity'], '29')
        self.assertEqual(entry['risk_context']['final_quantity'], '2')

    async def test_readback_promotes_blocked_planned_short_term_watch_to_delegated(self) -> None:
        overview, short_item, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_active',
        )

        self.assertTrue(short_item['exec_monitoring_active'])
        self.assertTrue(short_item['exec_agent_approved'])
        self.assertEqual(short_item['approval_state'], 'approved')
        self.assertEqual(short_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(short_item['eligibility_status'], 'blocked')
        self.assertEqual(short_item['validation']['blocking_reasons'][0]['code'], 'manual_recovery_gate_block')
        self.assertIn(
            'exec_watch_state_divergence',
            {warning['code'] for warning in short_item['validation']['warnings']},
        )
        self.assertEqual(short_item['remote_state']['plan_item_status_counts']['watch_monitoring_active'], 1)
        self.assertEqual(overview['summary']['core_exec_state_divergence_count'], 1)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.validation_payload['blocking_reasons'][0]['code'], 'manual_recovery_gate_block')
            self.assertEqual(row.validation_payload['warnings'][0]['code'], 'original_soft_gate_warning')
            self.assertIn(
                'exec_watch_state_divergence',
                {warning['code'] for warning in row.validation_payload['warnings']},
            )
            audit = row.submission_payload['exec_dispatch']['core_state_sync']
            self.assertTrue(audit['divergence'])
            self.assertEqual(audit['core_state_before']['eligibility_status'], 'blocked')
            self.assertEqual(audit['core_state_before']['execution_status'], 'planned')
            self.assertEqual(audit['core_state_after']['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    async def test_readback_promotes_submit_failed_short_term_watch_to_delegated(self) -> None:
        overview, short_item, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_active',
            core_execution_status='submit_failed',
        )

        self.assertTrue(short_item['exec_monitoring_active'])
        self.assertEqual(short_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(overview['summary']['core_exec_state_divergence_count'], 1)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            audit = row.submission_payload['exec_dispatch']['core_state_sync']
            self.assertEqual(audit['core_state_before']['execution_status'], 'submit_failed')
            self.assertEqual(audit['core_state_after']['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    async def test_readback_reconciles_recovered_watch_expiry_without_stale_quarantine_reason(self) -> None:
        overview, short_item, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_expired',
            response_item_status='watch_expired',
            core_execution_status='submit_failed',
            blocking_reason_code='exec_contract_quarantined',
        )

        self.assertEqual(short_item['execution_status'], 'watch_expired')
        self.assertEqual(short_item['remote_state']['derived_status'], 'cancelled')
        self.assertEqual(short_item['exec_agent_reason_code'], 'watch_expired')
        self.assertNotIn(
            'exec_contract_quarantined',
            {
                reason['code']
                for reason in short_item['validation']['blocking_reasons']
            },
        )
        self.assertIn(
            'watch_expired',
            {
                reason['code']
                for reason in short_item['validation']['blocking_reasons']
            },
        )
        self.assertEqual(overview['summary']['watch_expired_count'], 1)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(row.execution_status, 'watch_expired')
            self.assertEqual(
                row.submission_payload['exec_dispatch']['terminal_state_sync'][
                    'terminal_status'
                ],
                'watch_expired',
            )

    async def test_runtime_read_model_uses_exec_watch_status_without_broker_orders(self) -> None:
        _, _, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_pending',
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )
        short_board_item = next(
            item
            for item in board['items']
            if item['plan_item_id'] == short_plan_item_id
        )

        self.assertEqual(short_board_item['status'], 'scheduled')
        self.assertEqual(short_board_item['monitoring_state'], 'watching_fill')
        self.assertTrue(short_board_item['exec_monitoring_active'])
        self.assertTrue(short_board_item['exec_runtime_showable'])
        self.assertEqual(short_board_item['exec_runtime_state'], 'exec_approved_watch')
        self.assertEqual(short_board_item['core_state_divergence_warning']['code'], 'exec_watch_state_divergence')
        self.assertEqual(short_board_item['open_quantity'], 0)
        self.assertIsNone(short_board_item['broker_order_id'])

        with session_scope() as session:
            self.assertEqual(
                session.execute(
                    select(OrderRequest).where(OrderRequest.client_order_id == self.short_term_client_order_id)
                ).scalars().all(),
                [],
            )
            self.assertEqual(
                session.execute(
                    select(BrokerOrder).where(BrokerOrder.client_order_id == self.short_term_client_order_id)
                ).scalars().all(),
                [],
            )

    async def test_runtime_read_model_prioritizes_active_exec_watch_over_accepted_status(self) -> None:
        _, _, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_active',
            response_item_status='accepted',
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )
        short_board_item = next(
            item
            for item in board['items']
            if item['plan_item_id'] == short_plan_item_id
        )

        self.assertEqual(short_board_item['status'], 'accepted')
        self.assertEqual(short_board_item['monitoring_state'], 'watching_fill')
        self.assertTrue(short_board_item['exec_monitoring_active'])
        self.assertTrue(short_board_item['exec_runtime_showable'])
        self.assertEqual(short_board_item['exec_runtime_state'], 'exec_approved_watch')

    async def test_overview_warns_when_exec_watch_active_but_core_was_blocked_planned(self) -> None:
        overview, short_item, _ = await self._readback_stale_short_term_watch(
            exec_status='watch_buy_submitted',
        )

        self.assertEqual(overview['summary']['core_exec_state_divergence_count'], 1)
        self.assertEqual(overview['summary']['warnings'][0]['code'], 'exec_watch_state_divergence')
        self.assertEqual(overview['summary']['warnings'][0]['symbols'], [self.symbol])
        warning = short_item['core_state_divergence_warning']
        self.assertEqual(warning['code'], 'exec_watch_state_divergence')
        self.assertEqual(warning['core_state_before']['eligibility_status'], 'blocked')
        self.assertEqual(warning['core_state_before']['execution_status'], 'planned')
        self.assertEqual(warning['exec_statuses'], ['watch_buy_submitted'])

    async def test_dispatch_next_open_plan_short_term_missing_target_stop_skips_watch_handoff(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(target_price=None, stop_loss_price=None)
        self._seed_short_term_plan_item(
            snapshot_id=short_snapshot_id,
            target_price=None,
            stop_loss_price=None,
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertFalse(
            any(payload['metadata']['decision_agent_name'] == 'short-term-decider' for payload in self.client.post_calls)
        )
        short_group = next(
            group for group in result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(short_group['dispatch_status'], 'skipped')
        self.assertEqual(short_group['eligible_count'], 0)

    async def test_retryable_exec_failure_stays_pending_until_strict_not_found_retry(
        self,
    ) -> None:
        events: list[str] = []
        original_list = self.client.list_execution_plans
        original_post = self.client.post_execution_plan

        async def empty_readback(*, external_plan_id: str | None = None) -> dict:
            events.append('readback')
            return await original_list(external_plan_id=external_plan_id)

        async def retryable_failure(payload: dict) -> dict:
            events.append('failed_post')
            self.client.post_calls.append(copy.deepcopy(payload))
            raise SignalDeciderExecClientError(
                'exec returned 500 after accepting the request body',
                code='unavailable',
                retryable=True,
                status_code=500,
            )

        with patch.object(
            self.client,
            'list_execution_plans',
            side_effect=empty_readback,
        ), patch.object(
            self.client,
            'post_execution_plan',
            side_effect=retryable_failure,
        ):
            first = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='retryable_500_pending_reconciliation',
            )

        self.assertEqual(events, ['failed_post', 'readback'])
        self.assertEqual(
            first['dispatch']['groups'][0]['dispatch_status'],
            'pending_remote_ack',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'dispatch_pending_remote_ack')
            self.assertEqual(row.eligibility_status, 'eligible')
            self.assertEqual(
                row.submission_payload['exec_dispatch']['dispatch_status'],
                'pending_remote_ack',
            )
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['status'],
                'pending_remote_ack',
            )

        events.clear()

        async def accepted_retry(payload: dict) -> dict:
            events.append('post')
            return await original_post(payload)

        with patch.object(
            self.client,
            'list_execution_plans',
            side_effect=empty_readback,
        ), patch.object(
            self.client,
            'post_execution_plan',
            side_effect=accepted_retry,
        ):
            await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='retry_after_confirmed_not_found',
            )

        self.assertEqual(events[:2], ['readback', 'post'])
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

    async def test_legacy_retryable_ambiguity_is_requeued_before_remote_retry(
        self,
    ) -> None:
        async def crash_after_write_ahead(_payload: dict) -> dict:
            raise SystemExit('seed legacy retryable ambiguity')

        with patch.object(
            self.client,
            'post_execution_plan',
            side_effect=crash_after_write_ahead,
        ):
            with self.assertRaisesRegex(SystemExit, 'legacy retryable'):
                await self.service.dispatch_next_open_plan(
                    target_trade_date=self.target_trade_date,
                    account_reference=self.account_reference,
                    refresh_plan=False,
                    dry_run=False,
                    force=True,
                    trigger='seed_legacy_retryable_ambiguity',
                )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            exec_dispatch = copy.deepcopy(row.submission_payload['exec_dispatch'])
            plan_id = exec_dispatch['plan_id']
            fingerprint = exec_dispatch['plan_fingerprint']
            dispatch_revision_fingerprint = row.submission_payload[
                'exec_dispatch'
            ]['dispatch_revision_fingerprint']
            row.execution_status = 'revocation_manual_review'
            row.eligibility_status = 'blocked'
            row.submission_payload = {
                **copy.deepcopy(row.submission_payload),
                'exec_dispatch': {
                    **copy.deepcopy(row.submission_payload['exec_dispatch']),
                    'dispatch_status': 'unavailable',
                    'last_error': {
                        'code': 'unavailable',
                        'message': 'exec returned 500',
                        'retryable': True,
                        'status_code': 500,
                    },
                },
                'dispatch_reconciliation': {
                    'status': 'manual_review_required',
                    'plan_id': plan_id,
                    'plan_fingerprint': fingerprint,
                    'reason_code': 'ambiguous_remote_acceptance',
                    'submitted_dispatch_revision_fingerprint': (
                        dispatch_revision_fingerprint
                    ),
                    'current_dispatch_revision_fingerprint': (
                        dispatch_revision_fingerprint
                    ),
                    'error': {
                        'code': 'unavailable',
                        'message': 'exec returned 500',
                        'retryable': True,
                        'status_code': 500,
                    },
                },
            }

        events: list[str] = []
        observed_session_scope_kwargs: list[dict[str, object]] = []
        original_list = self.client.list_execution_plans
        original_post = self.client.post_execution_plan

        def recording_session_scope(**kwargs: object):
            observed_session_scope_kwargs.append(dict(kwargs))
            return session_scope(**kwargs)

        async def empty_readback(*, external_plan_id: str | None = None) -> dict:
            events.append('readback')
            return await original_list(external_plan_id=external_plan_id)

        async def accepted_retry(retry_payload: dict) -> dict:
            events.append('post')
            return await original_post(retry_payload)

        with patch.object(
            self.client,
            'list_execution_plans',
            side_effect=empty_readback,
        ), patch.object(
            self.client,
            'post_execution_plan',
            side_effect=accepted_retry,
        ), patch(
            'app.orders.exec_integration.session_scope',
            side_effect=recording_session_scope,
        ):
            await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='recover_legacy_retryable_ambiguity',
            )

        self.assertEqual(events[:2], ['readback', 'post'])
        self.assertIn(
            {
                'statement_timeout_ms': (
                    settings.signal_decider_db_statement_timeout_ms
                ),
                'lock_timeout_ms': settings.signal_decider_db_lock_timeout_ms,
            },
            observed_session_scope_kwargs,
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertTrue(_row_has_accepted_exec_dispatch(row))
            self.assertNotIn(
                'dispatch_reconciliation',
                row.submission_payload,
            )

    async def test_dispatch_next_open_plan_recovers_retryable_timeout_from_exec_readback(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        self.client = TimeoutAfterAcceptExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['failed_group_count'], 0)
        self.assertEqual(result['summary']['dispatched_group_count'], 2)
        self.assertEqual(len(self.client.post_calls), 2)
        self.assertEqual(len(self.client.plan_calls), 2)
        short_group = next(
            group for group in result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertTrue(short_group['recovered_from_retryable_error'])
        self.assertEqual(short_group['dispatch_status'], 'accepted')

        with session_scope() as session:
            short_row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(short_row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            exec_dispatch = short_row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertIsNone(exec_dispatch['last_error'])

    async def test_dispatch_next_open_plan_recovers_409_duplicate_from_matching_readback(self) -> None:
        self.client = DuplicateConflictAfterAcceptExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['failed_group_count'], 0)
        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.plan_calls), 1)
        group = result['dispatch']['groups'][0]
        self.assertTrue(group['recovered_from_duplicate'])
        self.assertFalse(group['recovered_from_retryable_error'])
        self.assertEqual(group['dispatch_status'], 'accepted')

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertIsNone(exec_dispatch['last_error'])
            self.assertEqual(
                {link['core_item_key'] for link in exec_dispatch['item_links']},
                {
                    f'core-plan-{self.plan_item_id}:next_open_buy',
                    f'core-plan-{self.plan_item_id}:target_sell',
                    f'core-plan-{self.plan_item_id}:stop_sell',
                },
            )

    async def test_dispatch_next_open_plan_rejects_stale_timeout_readback_without_matching_core_keys(self) -> None:
        self.client = TimeoutWithStaleSamePlanReadbackExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.plan_calls), 1)
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'unavailable')
        self.assertNotIn('recovered_from_duplicate', group)
        self.assertNotIn('recovered_from_retryable_error', group)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'unavailable')
            self.assertIsNone(exec_dispatch.get('last_success_at'))
            self.assertEqual(exec_dispatch['last_error']['code'], 'unavailable')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'ambiguous_remote_acceptance',
            )
            self.assertEqual(
                {link['core_item_key'] for link in exec_dispatch['item_links']},
                {
                    f'core-plan-{self.plan_item_id}:next_open_buy',
                    f'core-plan-{self.plan_item_id}:target_sell',
                    f'core-plan-{self.plan_item_id}:stop_sell',
                },
            )
        self.assertTrue(
            self.service.should_manage_delegated_authority(
                target_trade_date=self.target_trade_date,
            )
        )

    async def test_invalid_dispatch_ack_remains_managed_as_ambiguous_remote_acceptance(
        self,
    ) -> None:
        async def invalid_ack(payload: dict) -> dict:
            self.client.post_calls.append(payload)
            return {'plan_id': payload['plan_id'], 'status': 'accepted'}

        with patch.object(self.client, 'post_execution_plan', side_effect=invalid_ack):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                account_reference=self.account_reference,
                refresh_plan=False,
                dry_run=False,
                force=True,
                trigger='test_invalid_ack_ambiguous_acceptance',
            )

        self.assertEqual(result['dispatch']['groups'][0]['dispatch_status'], 'contract_error')
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, 'revocation_manual_review')
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'ambiguous_remote_acceptance',
            )
        self.assertTrue(
            self.service.should_manage_delegated_authority(
                target_trade_date=self.target_trade_date,
            )
        )

    async def test_dispatch_next_open_plan_short_term_idempotent_redispatch_skips_duplicate_payload(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)

        first_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test',
        )
        self.assertEqual(first_result['summary']['translated_item_count'], 6)
        self.assertEqual(first_result['summary']['dispatched_group_count'], 2)
        first_short_group = next(
            group for group in first_result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(first_short_group['dispatch_status'], 'accepted')
        first_post_call_count = len(self.client.post_calls)

        second_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test',
        )
        self.assertEqual(
            second_result['summary']['translated_item_count'],
            first_result['summary']['translated_item_count'],
        )
        self.assertEqual(second_result['summary']['dispatched_group_count'], 0)
        self.assertEqual(second_result['summary']['failed_group_count'], 0)
        self.assertEqual(second_result['summary']['skipped_group_count'], 2)
        short_group = next(
            group for group in second_result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(short_group['dispatch_status'], 'already_dispatched')
        self.assertEqual(short_group['plan_id'], first_short_group['plan_id'])
        self.assertEqual(len(self.client.post_calls), first_post_call_count)

    async def test_accepted_dispatch_proof_binds_request_and_response_material_fingerprint(
        self,
    ) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test_fingerprint_bound_proof',
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            original = copy.deepcopy(row.submission_payload)
            exec_dispatch = original['exec_dispatch']
            expected = exec_dispatch['contract_proof'][
                'dispatch_material_fingerprint'
            ]
            self.assertEqual(
                exec_dispatch['last_request_summary']['metadata'][
                    'dispatch_material_fingerprint'
                ],
                expected,
            )
            self.assertEqual(
                exec_dispatch['last_response_summary']['contract_ack'][
                    'dispatch_material_fingerprint'
                ],
                expected,
            )
            expected_item_bindings = [
                {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_id': link['exec_plan_id'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                }
                for link in exec_dispatch['item_links']
            ]
            self.assertEqual(
                exec_dispatch['contract_proof']['accepted_item_bindings'],
                expected_item_bindings,
            )
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

            for summary_key, path_key in (
                ('last_request_summary', 'metadata'),
                ('last_response_summary', 'contract_ack'),
            ):
                with self.subTest(summary_key=summary_key):
                    mismatched = copy.deepcopy(original)
                    mismatched['exec_dispatch'][summary_key][path_key][
                        'dispatch_material_fingerprint'
                    ] = 'b' * 64
                    row.submission_payload = mismatched
                    self.assertFalse(_row_has_accepted_exec_dispatch(row))
            with self.subTest(summary_key='accepted_item_bindings'):
                mismatched = copy.deepcopy(original)
                mismatched['exec_dispatch']['contract_proof'][
                    'accepted_item_bindings'
                ][0]['execution_plan_item_id'] = 'exec-item-from-other-plan'
                row.submission_payload = mismatched
                self.assertFalse(_row_has_accepted_exec_dispatch(row))
            with self.subTest(summary_key='item_links'):
                mismatched = copy.deepcopy(original)
                mismatched['exec_dispatch']['item_links'][0][
                    'exec_plan_item_id'
                ] = 'exec-item-from-other-plan'
                row.submission_payload = mismatched
                self.assertFalse(_row_has_accepted_exec_dispatch(row))
            for summary_key in ('contract_proof', 'last_response_summary'):
                with self.subTest(summary_key=summary_key, field='execution_plan_id'):
                    mismatched = copy.deepcopy(original)
                    mismatched['exec_dispatch'][summary_key][
                        'execution_plan_id'
                    ] = 'exec-plan-from-other-ack'
                    row.submission_payload = mismatched
                    self.assertFalse(_row_has_accepted_exec_dispatch(row))
            row.submission_payload = original

    async def test_accepted_dispatch_proof_binds_distinct_exec_internal_plan_id(
        self,
    ) -> None:
        internal_execution_plan_id = '8b3ea236-90b2-4ec6-a68c-f86e05499f3c'
        self.client.execution_plan_id_override = internal_execution_plan_id

        first = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test_distinct_exec_internal_plan_id',
        )

        self.assertEqual(first['summary']['dispatched_group_count'], 1)
        self.assertEqual(len(self.client.post_calls), 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            exec_dispatch = row.submission_payload['exec_dispatch']
            external_plan_id = exec_dispatch['plan_id']
            self.assertNotEqual(external_plan_id, internal_execution_plan_id)
            self.assertEqual(
                exec_dispatch['contract_proof']['execution_plan_id'],
                internal_execution_plan_id,
            )
            self.assertEqual(
                exec_dispatch['last_response_summary']['execution_plan_id'],
                internal_execution_plan_id,
            )
            self.assertEqual(
                {link['exec_plan_id'] for link in exec_dispatch['item_links']},
                {internal_execution_plan_id},
            )
            self.assertTrue(_row_has_accepted_exec_dispatch(row))
            self.assertEqual(
                self.service._fence_unproven_remote_acceptance(
                    rows=[row],
                    trigger='test_distinct_exec_internal_plan_id',
                ),
                0,
            )
        second = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test_distinct_exec_internal_plan_id_replay',
        )
        self.assertEqual(second['summary']['dispatched_group_count'], 0)
        self.assertEqual(second['summary']['skipped_group_count'], 1)
        self.assertEqual(len(self.client.post_calls), 1)

    async def test_readback_cannot_replace_ack_bound_item_identity_before_revocation(
        self,
    ) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='seed_ack_bound_identity',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = copy.deepcopy(row.submission_payload)
            original_links = copy.deepcopy(
                submission['exec_dispatch']['item_links']
            )
            submission['exec_dispatch']['readback'] = {
                'last_synced_at': '2026-04-30T00:01:00+00:00',
                'orders': {
                    'status': 'ok',
                    'count': 1,
                    'items': [
                        {
                            'id': 'stale-filled-order',
                            'execution_plan_item_id': original_links[0][
                                'exec_plan_item_id'
                            ],
                            'symbol': self.symbol,
                            'side': 'buy',
                            'status': 'filled',
                            'filled_quantity': '5',
                        }
                    ],
                },
                'executions': {'status': 'ok', 'count': 0, 'items': []},
                'positions': {'status': 'ok', 'count': 0, 'items': []},
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'open_quantity': '5',
                },
            }
            row.submission_payload = submission

        self.client.plan_response = {
            'items': [
                {
                    'external_plan_id': plan_id,
                    'items': [
                        {
                            'core_item_key': link['core_item_key'],
                            'execution_plan_id': 'exec-plan-b',
                            'execution_plan_item_id': f'exec-plan-b-item-{index}',
                            'symbol': link['symbol'],
                            'intent': link['intent'],
                            'status': 'filled',
                        }
                        for index, link in enumerate(original_links, start=1)
                    ],
                }
            ]
        }

        await self.service._refresh_exec_readback(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=None,
            trigger='mismatched_plan_readback',
            runtime_payload={
                'status': 'ok',
                'payload': {'ready': True},
                'error': None,
            },
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(
                row.submission_payload['exec_dispatch']['item_links'],
                original_links,
            )
            self.assertEqual(
                row.submission_payload['dispatch_reconciliation']['reason_code'],
                'exec_readback_identity_mismatch',
            )
            self.assertEqual(
                row.submission_payload['exec_dispatch']['attempts'][-1]['status'],
                'readback_discarded_identity_mismatch',
            )
            invalidated_readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(
                invalidated_readback['execution_plans']['status'],
                'contract_error',
            )
            self.assertEqual(invalidated_readback['orders']['status'], 'contract_error')
            self.assertEqual(invalidated_readback['orders']['items'], [])
            self.assertEqual(
                invalidated_readback['derived_state']['derived_status'],
                'unavailable',
            )

        settings.signal_decider_exec_integration_mode = 'disabled'
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='revoke_after_mismatched_readback',
        )

        self.assertEqual(result['summary']['revoked_group_count'], 1)
        self.assertEqual(len(self.client.revocation_calls), 1)
        revocation = self.client.revocation_calls[0]
        self.assertEqual(revocation['plan_id'], plan_id)
        self.assertEqual(
            {
                (
                    item['core_item_key'],
                    item['execution_plan_item_id'],
                )
                for item in revocation['items']
            },
            {
                (link['core_item_key'], link['exec_plan_item_id'])
                for link in original_links
            },
        )

    async def test_readback_rejects_same_symbol_order_with_wrong_stable_item_id(
        self,
    ) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='seed_order_identity',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {
            'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]
        }
        self.client.order_response = {
            'items': [
                {
                    'id': 'foreign-order',
                    'execution_plan_item_id': 'foreign-exec-item',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'status': 'filled',
                    'filled_quantity': '99',
                    'average_fill_price': '1',
                }
            ]
        }

        await self.service._refresh_exec_readback(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=self.symbol,
            trigger='wrong_order_identity',
            runtime_payload={'status': 'ok', 'payload': {'ready': True}, 'error': None},
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['orders']['status'], 'contract_error')
            self.assertEqual(readback['orders']['count'], 0)
            self.assertEqual(
                readback['orders']['error']['code'],
                'order_readback_identity_mismatch',
            )
            self.assertEqual(readback['derived_state']['derived_status'], 'unavailable')

    async def test_readback_rejects_same_symbol_execution_with_wrong_stable_order_id(
        self,
    ) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='seed_execution_identity',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {
            'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]
        }
        self.client.order_response = {
            'items': [
                {
                    'id': 'accepted-order',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'status': 'submitted',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'id': 'foreign-execution',
                    'execution_plan_item_id': 'exec-item-1',
                    'broker_order_id': 'foreign-order',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '99',
                    'price': '1',
                }
            ]
        }

        await self.service._refresh_exec_readback(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=self.symbol,
            trigger='wrong_execution_identity',
            runtime_payload={'status': 'ok', 'payload': {'ready': True}, 'error': None},
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['executions']['status'], 'contract_error')
            self.assertEqual(readback['executions']['count'], 0)
            self.assertEqual(
                readback['executions']['error']['code'],
                'execution_readback_identity_mismatch',
            )
            self.assertEqual(readback['derived_state']['derived_status'], 'unavailable')

    async def test_exec_overview_includes_both_deciders_without_symbol_collision(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(len(overview['items']), 2)
        self.assertEqual(overview['summary']['decision_agent_counts']['short-term-decider'], 1)
        self.assertEqual(overview['summary']['decision_agent_counts']['signal-decider'], 1)
        self.assertEqual(
            {item['decision_agent_name'] for item in overview['items']},
            {'signal-decider', 'short-term-decider'},
        )
        short_item = next(item for item in overview['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['raw_action'], 'watch_for_trigger')
        self.assertEqual(short_item['tactical_action'], 'watch_for_trigger')
        self.assertEqual(short_item['generic_action'], 'monitor_candidate')
        self.assertEqual(short_item['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(short_item['approval_state'], 'not_dispatched')
        self.assertFalse(short_item['exec_agent_approved'])
        self.assertFalse(short_item['exec_monitoring_active'])

    async def test_exec_overview_offloads_plan_read_to_threadpool(self) -> None:
        to_thread_calls = []

        async def run_inline(func, *args, **kwargs):
            to_thread_calls.append((func, kwargs))
            return func(*args, **kwargs)

        to_thread = AsyncMock(side_effect=run_inline)
        with patch('app.orders.exec_integration.asyncio.to_thread', new=to_thread):
            overview = await self.service.list_signal_decider_exec_overview(
                target_trade_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(to_thread.await_count, 2)
        offloaded_func, offloaded_kwargs = to_thread_calls[0]
        self.assertIs(offloaded_func.__self__, signal_decider_next_open_execution_service)
        self.assertEqual(offloaded_func.__name__, 'list_plan_items')
        self.assertEqual(offloaded_kwargs['target_trade_date'], self.target_trade_date)
        self.assertIsNone(offloaded_kwargs['account_reference'])
        self.assertFalse(offloaded_kwargs['refresh'])
        row_loader, row_loader_kwargs = to_thread_calls[1]
        self.assertIs(row_loader.__self__, self.service)
        self.assertEqual(row_loader.__name__, '_load_next_open_rows')
        self.assertEqual(row_loader_kwargs['target_trade_date'], self.target_trade_date)
        self.assertEqual(len(overview['items']), 1)

    async def test_exec_overview_refresh_persists_remote_readback(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        with session_scope() as session:
            dispatched_row = session.get(
                SignalDeciderExecutionPlanItem,
                self.plan_item_id,
            )
            dispatch_revision_fingerprint = dispatched_row.submission_payload[
                'exec_dispatch'
            ]['dispatch_revision_fingerprint']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                }
            ]
        }
        self.client.position_response = {
            'items': [{'symbol': self.symbol, 'quantity': '5', 'average_price': '50000'}]
        }

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(overview['summary']['remote_open_count'], 1)
        self.assertEqual(overview['items'][0]['dispatch']['status'], 'accepted')
        self.assertEqual(overview['items'][0]['remote_state']['derived_status'], 'open')
        self.assertEqual(overview['items'][0]['remote_state']['entry_filled_quantity'], '5')
        self.assertEqual(self.client.runtime_calls, 1)
        self.assertEqual(self.client.plan_calls[0]['external_plan_id'], plan_id)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['orders']['count'], 1)
            self.assertEqual(readback['derived_state']['derived_status'], 'open')
            self.assertIn('contract_proof', row.submission_payload['exec_dispatch'])
            self.assertEqual(
                row.submission_payload['exec_dispatch'][
                    'dispatch_revision_fingerprint'
                ],
                dispatch_revision_fingerprint,
            )
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

    async def test_exec_overview_contract_quarantine_overrides_immutable_accepted_item_acks(
        self,
    ) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            item_links = copy.deepcopy(
                row.submission_payload['exec_dispatch']['item_links']
            )

        self.client.plan_response = {
            'items': [
                {
                    'external_plan_id': plan_id,
                    'plan_id': plan_id,
                    'execution_plan_id': item_links[0]['exec_plan_id'],
                    'status': 'contract_quarantined',
                    'updated_at': '2026-04-30T00:00:00+00:00',
                    'reason_code': 'execution_contract_identity_invalid',
                    'reason': 'The accepted handoff was quarantined before execution.',
                    'item_acks': [
                        {
                            **copy.deepcopy(link['response_item']),
                            'status': 'accepted',
                        }
                        for link in item_links
                    ],
                }
            ]
        }
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test_contract_quarantine',
        )

        item = overview['items'][0]
        self.assertEqual(item['remote_state']['derived_status'], 'failed')
        self.assertFalse(item['exec_agent_approved'])
        self.assertEqual(item['approval_state'], 'rejected')
        self.assertEqual(
            item['exec_agent_reason_code'],
            'execution_contract_identity_invalid',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(
                row.execution_status,
                EXECUTION_STATUS_REVOCATION_PENDING,
            )
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertIn('exec_dispatch', row.submission_payload)
            self.assertEqual(
                row.submission_payload['revocation']['reason_code'],
                'exec_contract_quarantined',
            )
            self.assertEqual(
                row.submission_payload['revocation'][
                    'quarantine_observed_at'
                ],
                '2026-04-30T00:00:00+00:00',
            )
            current_states = [
                link.get('current_state')
                for link in row.submission_payload['exec_dispatch']['item_links']
            ]
            self.assertTrue(all(state['status'] == 'contract_quarantined' for state in current_states))
            self.assertTrue(all(state['terminal'] is True for state in current_states))
            self.assertTrue(
                all(
                    state['decision']['reason_code']
                    == 'execution_contract_identity_invalid'
                    and state['decision']['reason']
                    == 'The accepted handoff was quarantined before execution.'
                    for state in current_states
                )
            )
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

    async def test_contract_quarantine_retirement_requires_exact_timestamp_and_zero_exposure(
        self,
    ) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='seed_quarantine_evidence',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            item_links = copy.deepcopy(
                row.submission_payload['exec_dispatch']['item_links']
            )
            for link in item_links:
                link['current_state'] = {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_id': link['exec_plan_id'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                    'status': 'contract_quarantined',
                    'terminal': True,
                }
            zero_exposure = {
                'endpoint_statuses': {
                    endpoint: 'ok'
                    for endpoint in (
                        'plans',
                        'orders',
                        'executions',
                        'positions',
                    )
                },
                'has_plan_attributed_fill_evidence': False,
                'open_quantity': '0',
                'scoped_position_exposure_quantity': '0',
            }

            evidence = lambda state: (
                self.service._contract_quarantine_retirement_evidence(
                    row=row,
                    item_links=item_links,
                    derived_state=state,
                )
            )
            self.assertIsNone(evidence(zero_exposure))
            item_links[0]['current_state']['observed_at'] = (
                '2026-04-30T00:00:00+00:00'
            )
            self.assertIsNotNone(evidence(zero_exposure))
            blocked_states = {
                'missing_open_quantity': {
                    **zero_exposure,
                    'open_quantity': None,
                },
                'attributed_fill': {
                    **zero_exposure,
                    'has_plan_attributed_fill_evidence': True,
                },
                'open_exposure': {
                    **zero_exposure,
                    'open_quantity': '1',
                },
                'incomplete_readback': {
                    **zero_exposure,
                    'endpoint_statuses': {
                        **zero_exposure['endpoint_statuses'],
                        'executions': 'error',
                    },
                },
            }
            for case, derived_state in blocked_states.items():
                with self.subTest(case=case):
                    self.assertIsNone(evidence(derived_state))
            scoped_position_exposure = self.service._derive_remote_state(
                row=row,
                item_links=item_links,
                orders=[],
                executions=[],
                positions=[{'symbol': row.symbol, 'quantity': '5'}],
                endpoint_statuses=zero_exposure['endpoint_statuses'],
            )
            self.assertIsNone(evidence(scoped_position_exposure))
            item_links[0]['current_state']['observed_at'] = (
                '2026-04-30T00:00:00'
            )
            self.assertIsNone(evidence(zero_exposure))
            item_links[0]['current_state']['observed_at'] = (
                '2026-04-30T00:00:00+00:00'
            )
            row.execution_status = EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK
            signal_decider_next_open_execution_service.stage_exec_contract_quarantine_retirement(
                row=row,
                item_links=[item_links[0]],
                quarantine_observed_at='2026-04-30T00:00:00+00:00',
                trigger='strict_ambiguous_acceptance_readback',
                now=datetime(2026, 4, 30, 0, 0, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(
                row.execution_status,
                EXECUTION_STATUS_REVOCATION_PENDING,
            )

    async def test_exec_overview_completed_no_trade_persists_entry_terminal_reason(
        self,
    ) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            item_links = copy.deepcopy(
                row.submission_payload['exec_dispatch']['item_links']
            )

        item_states = []
        for link in item_links:
            is_entry = link.get('intent') in EXEC_ENTRY_BUY_INTENTS
            item_states.append(
                {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                    'status': (
                        'next_open_buy_expired'
                        if is_entry
                        else 'not_applicable'
                    ),
                    'updated_at': '2026-10-08T00:00:00+00:00',
                    'terminal': True,
                    'decision': (
                        {
                            'action': 'cancel',
                            'decision': 'expired',
                            'reason_code': 'missed_next_open_window',
                            'reason_family': 'execution_window',
                            'rejection_disposition': 'terminal',
                            'reason': (
                                'The next-open entry window expired before '
                                'submission.'
                            ),
                        }
                        if is_entry
                        else None
                    ),
                }
            )
        self.client.plan_response = {
            'items': [
                {
                    'external_plan_id': plan_id,
                    'plan_id': plan_id,
                    'execution_plan_id': item_links[0]['exec_plan_id'],
                    'status': 'completed_no_trade',
                    'item_acks': [
                        {
                            **copy.deepcopy(link['response_item']),
                            'status': 'accepted',
                        }
                        for link in item_links
                    ],
                    'item_status_counts': {
                        'next_open_buy_expired': 1,
                        'not_applicable': len(item_links) - 1,
                    },
                    'item_states': item_states,
                }
            ]
        }
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test_completed_no_trade',
        )

        item = overview['items'][0]
        self.assertEqual(item['remote_state']['derived_status'], 'cancelled')
        self.assertFalse(item['exec_agent_approved'])
        self.assertEqual(item['approval_state'], 'rejected')
        self.assertEqual(item['exec_agent_reason_code'], 'missed_next_open_window')
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.execution_status, EXECUTION_STATUS_SUBMIT_FAILED)
            dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(
                dispatch['terminal_state_sync']['terminal_status'],
                'next_open_buy_expired',
            )
            self.assertEqual(
                dispatch['terminal_state_sync']['decision']['reason_code'],
                'missed_next_open_window',
            )
            self.assertTrue(_row_has_accepted_exec_dispatch(row))

    async def test_exec_overview_refresh_requests_positions_for_exact_exec_plan(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(overview['items'][0]['remote_state']['derived_status'], 'accepted')
        self.assertEqual(overview['items'][0]['remote_state']['positions'], [])
        self.assertEqual(
            self.client.position_calls[-1],
            {
                'external_plan_id': plan_id,
                'execution_plan_id': plan_id,
                'symbols': [self.symbol],
            },
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['positions']['count'], 0)

    async def test_exec_overview_refresh_syncs_short_term_outcome_before_replay_selection(self) -> None:
        with session_scope() as session:
            session.execute(
                delete(
                    SignalDeciderExecutionPlanItem,
                ).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                    SignalDeciderExecutionPlanItem.decision_agent_name == 'signal-decider',
                )
            )

        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        short_group = next(
            group for group in dispatch_result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        plan_id = short_group['plan_id']

        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                }
            ]
        }
        self.client.position_response = {
            'items': [{'symbol': self.symbol, 'quantity': '5', 'average_price': '50000'}]
        }

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(len(overview['items']), 1)
        self.assertEqual(overview['summary']['remote_open_count'], 1)
        self.assertEqual(overview['items'][0]['dispatch']['status'], 'accepted')
        self.assertEqual(overview['items'][0]['remote_state']['derived_status'], 'open')
        self.assertEqual(
            len(overview['items'][0]['dispatch']['item_links']),
            3,
        )
        self.assertEqual(overview['items'][0]['remote_state']['orders'], self.client.order_response['items'])

        replay_trades = DeciderReplayRepository().list_wait_conflict_gate_trades(
            market_date_from=self.market_date,
            market_date_to=self.market_date,
            symbols=[self.symbol],
            cost_assumptions=DeciderReplayCostAssumptions(),
            gate_mode='hard_block',
            probe_quantity=1,
        )

        self.assertEqual(len(replay_trades), 1)
        self.assertEqual(replay_trades[0].symbol, self.symbol)
        self.assertEqual(replay_trades[0].entry_filled_quantity, Decimal('5'))

    async def test_dashboard_runtime_status_normalizes_exec_runtime_payload(self) -> None:
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'counts': {
                'open_orders': 2,
                'active_execution_jobs': 3,
            },
            'symbols': {
                'filled_today': ['003280', '010950', '161390'],
                'filled_today_by_side': {
                    'buy': ['010950', '161390'],
                    'sell': ['003280', '010950', '161390'],
                },
                'filled_today_counts': {
                    'all': {'symbols': 3, 'fills': 5, 'quantity': 306},
                    'buy': {'symbols': 2, 'fills': 2, 'quantity': 3},
                    'sell': {'symbols': 3, 'fills': 3, 'quantity': 153},
                },
            },
            'daily_pnl': {
                'daily_realized_pnl': '8800.0000',
                'matched_sell_quantity': 3,
                'unmatched_sell_quantity': 150,
                'sold_symbols': ['003280', '010950', '161390'],
                'sold_symbol_count': 3,
                'closed_symbols': ['010950', '161390'],
                'closed_symbol_count': 2,
                'unmatched_sell_symbols': ['003280'],
                'unmatched_sell_symbol_count': 1,
                'pnl_method': 'fifo_internal_fills',
            },
            'monthly_pnl': {
                'monthly_realized_pnl': '8800.0000',
                'matched_sell_quantity': 3,
                'unmatched_sell_quantity': 150,
                'pnl_method': 'fifo_internal_fills',
            },
            'weekly_pnl': {
                'weekly_realized_pnl': '8800.0000',
                'matched_sell_quantity': 3,
                'unmatched_sell_quantity': 150,
                'closed_trade_count': 2,
                'win_count': 2,
                'loss_count': 0,
                'pnl_method': 'fifo_internal_fills',
            },
            'actionable_alerts': {
                'status': 'ok',
                'alerts': [],
            },
            'monitoring': {
                'alive': True,
                'last_heartbeat_at': '2026-04-30T00:05:00+00:00',
            },
            'readiness': {
                'reasons': [],
            },
        }

        result = await self.service.dashboard_runtime_status(refresh=True)

        self.assertEqual(result['status'], 'ready')
        self.assertTrue(result['exec_connected'])
        self.assertTrue(result['automation_enabled'])
        self.assertTrue(result['monitoring_alive'])
        self.assertEqual(result['open_order_count'], 2)
        self.assertEqual(result['active_execution_job_count'], 3)
        self.assertEqual(result['last_heartbeat_at'], '2026-04-30T00:05:00+00:00')
        self.assertEqual(result['symbols']['filled_today_by_side']['sell'], ['003280', '010950', '161390'])
        self.assertEqual(result['daily_pnl']['sold_symbol_count'], 3)
        self.assertEqual(result['daily_pnl']['closed_symbol_count'], 2)
        self.assertEqual(result['daily_pnl']['unmatched_sell_symbols'], ['003280'])
        self.assertEqual(result['weekly_pnl']['weekly_realized_pnl'], '8800.0000')
        self.assertEqual(result['weekly_pnl']['closed_trade_count'], 2)
        self.assertEqual(result['monthly_pnl']['monthly_realized_pnl'], '8800.0000')
        self.assertEqual(result['actionable_alerts']['status'], 'ok')

    async def test_dashboard_runtime_status_surfaces_exec_offline_state(self) -> None:
        service = SignalDeciderExecIntegrationService(client=FailingRuntimeExecClient())

        result = await service.dashboard_runtime_status(refresh=True)

        self.assertEqual(result['status'], 'offline')
        self.assertFalse(result['exec_connected'])
        self.assertEqual(result['issues'][0]['code'], 'unavailable')

    async def test_dashboard_runtime_status_refresh_false_keeps_ready_config_healthy(self) -> None:
        result = await self.service.dashboard_runtime_status(refresh=False)

        self.assertEqual(result['status'], 'degraded')
        self.assertTrue(result['exec_connected'])
        self.assertFalse(result['monitoring_alive'])
        self.assertEqual(self.client.runtime_calls, 0)

    async def test_runtime_dashboard_views_merge_core_plan_and_exec_truth(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'counts': {
                'open_orders': 1,
                'active_execution_jobs': 1,
            },
        }
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                    'updated_at': '2026-04-30T00:05:00+00:00',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                    'fill_price': '50100',
                    'executed_at': '2026-04-30T00:06:00+00:00',
                }
            ]
        }
        self.client.position_response = {
            'items': [
                {
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_portfolio_holding_membership', new=AsyncMock(return_value=None)),
            patch.object(
                self.service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(
                    return_value=ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({self.symbol}),
                        membership_complete=True,
                    )
                ),
            ),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            await self.service.list_signal_decider_exec_overview(
                target_trade_date=self.target_trade_date,
                refresh=True,
                trigger='test',
            )
            board = await self.service.get_runtime_daily_board(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            detail = await self.service.get_runtime_symbol_detail(
                market_date=self.target_trade_date,
                symbol=self.symbol,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(board['runtime']['status'], 'ready')
        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['avg_fill_price'], 50100)
        self.assertEqual(board['items'][0]['execution_plan_id'], plan_id)
        self.assertEqual(board['items'][0]['current_price'], 50200)
        self.assertEqual(board['items'][0]['price_status'], 'available')
        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_activity_status'], 'available')
        self.assertEqual(today_activity['buy_activity_source'], 'executions')
        self.assertEqual(today_activity['buy_filled_quantity_today'], 5)
        self.assertEqual(today_activity['buy_avg_fill_price_today'], 50100)
        self.assertEqual(today_activity['buy_last_filled_at'], '2026-04-30T00:06:00+00:00')
        self.assertEqual(today_activity['buy_broker_order_ids'], ['order-1'])
        self.assertEqual(today_activity['sell_activity_status'], 'none')
        self.assertEqual(today_activity['sell_filled_quantity_today'], 0)
        self.assertEqual(today_activity['net_open_quantity'], 5)

        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['items'][0]['symbol'], self.symbol)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(monitored['items'][0]['planned_intents'], ['stop_sell', 'target_sell'])
        self.assertEqual(monitored['items'][0]['current_price'], 50200)
        self.assertTrue(monitored['items'][0]['exec_monitoring_active'])
        self.assertTrue(monitored['items'][0]['exec_runtime_showable'])
        self.assertTrue(monitored['items'][0]['exec_agent_approved'])
        self.assertTrue(monitored['items'][0]['held_position'])
        self.assertEqual(monitored['items'][0]['position_quantity'], 5)
        self.assertEqual(monitored['items'][0]['position_average_price'], 50100)
        self.assertIn('target_sell_monitoring', monitored['items'][0]['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', monitored['items'][0]['monitoring_statuses'])
        self.assertEqual(monitored['items'][0]['target_price'], 54000)
        self.assertEqual(monitored['items'][0]['stop_loss_price'], 47000)

        self.assertEqual(detail['summary']['items_total'], 1)
        self.assertEqual(detail['monitoring']['state'], 'watching_target_stop')
        self.assertEqual(detail['current_price'], 50200)
        self.assertEqual(detail['items'][0]['status'], 'filled')
        self.assertEqual(detail['items'][0]['remote_state']['derived_status'], 'open')

    async def test_runtime_monitored_symbols_flags_exec_approved_watch_candidate(self) -> None:
        self.client.agent_sizing_payloads_by_agent['signal-decider'] = {
            'approved': True,
            'quantity': '5',
            'notional_krw': '250000.0000',
            'action': 'approve',
            'reason_code': 'small_tactical_upside',
            'target_profile': 'tactical_2pct',
            'target_tag': 'target_profile:tactical_2pct',
            'special_tactical_scalp': True,
        }

        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_portfolio_holding_membership', new=AsyncMock(return_value=None)),
            patch.object(
                self.service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(
                    return_value=ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({self.symbol}),
                        membership_complete=True,
                    )
                ),
            ),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(monitored['summary']['items_total'], 1)
        item = monitored['items'][0]
        self.assertEqual(item['symbol'], self.symbol)
        self.assertIn(item['monitoring_state'], {'watching_fill', 'reconciling'})
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'exec_approved_watch')
        self.assertTrue(item['exec_agent_approved'])
        self.assertEqual(item['exec_agent_action'], 'approve')
        self.assertEqual(item['exec_agent_reason_code'], 'small_tactical_upside')
        self.assertEqual(item['exec_agent_target_profile'], 'tactical_2pct')
        self.assertEqual(item['exec_agent_target_tag'], 'target_profile:tactical_2pct')
        self.assertTrue(item['exec_agent_special_tactical_scalp'])
        self.assertFalse(item['held_position'])
        self.assertEqual(item['approved_quantity'], 5)
        self.assertEqual(item['approved_notional'], '250000.0000')
        self.assertIn('watch_monitoring_active', item['monitoring_statuses'])

    async def test_runtime_monitored_symbols_treats_accepted_entry_link_as_exec_approved_watch(self) -> None:
        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {
                'allowed_symbols': ['WATCHACCEPTED', 'WATCHCREATED', 'WATCHPENDING', 'WATCHQUEUED']
            }

            for entry_status in ('accepted', 'created', 'pending', 'queued'):
                with self.subTest(entry_status=entry_status):
                    symbol = f'WATCH{entry_status.upper()}'
                    accepted_watch = self.service._build_runtime_board_item(
                        item={
                            'plan_item_id': 9101,
                            'account_reference': self.account_reference,
                            'account_label': 'Main',
                            'symbol': symbol,
                            'decision_agent_name': 'short-term-decider',
                            'estimated_reference_price': 2000,
                            'planned_quantity': '10',
                            'eligibility_status': 'eligible',
                            'execution_status': 'delegated_for_exec_agent_sizing',
                            'dispatch': {
                                'status': 'accepted',
                                'plan_id': f'exec-{entry_status}-watch',
                                'item_links': [
                                    {
                                        'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                                        'side': 'buy',
                                        'symbol': symbol,
                                        'ack_status': 'accepted',
                                        'remote_status': entry_status,
                                        'remote_status_source': 'exec_item_state',
                                        'current_state': {
                                            'status': entry_status,
                                            'execution_plan_item_id': f'exec-item-watch-{entry_status}',
                                        },
                                        'response_item': {
                                            'status': 'accepted',
                                            'execution_plan_item_id': f'exec-item-watch-{entry_status}',
                                        },
                                    }
                                ],
                            },
                            'trigger_price': '2050',
                            'chase_limit_price': '2070',
                            'target_price': '2200',
                            'stop_loss_price': '1900',
                        },
                        market_date=self.target_trade_date,
                        symbol_metadata_map={symbol: {'name': 'Accepted Watch', 'market': 'KOSPI'}},
                        runtime_status='ready',
                    )

                    payload = self.service._build_runtime_monitored_symbols_payload(
                        board={
                            'market_date': self.target_trade_date.isoformat(),
                            'runtime': {'status': 'ready', 'mode': 'mock'},
                            'items': [accepted_watch],
                        },
                        refresh=False,
                        refresh_state={'requested': False, 'status': 'not_requested'},
                        served_from_cache=False,
                    )

                    item = payload['items'][0]
                    self.assertEqual(item['symbol'], symbol)
                    self.assertTrue(item['exec_runtime_showable'])
                    self.assertTrue(item['exec_monitoring_active'])
                    self.assertTrue(item['exec_agent_approved'])
                    self.assertEqual(item['exec_agent_action'], 'approve')
                    self.assertEqual(item['exec_runtime_state'], 'exec_approved_watch')
                    self.assertIsNone(item['exec_runtime_exclusion_reason'])
                    self.assertIn(entry_status, item['monitoring_statuses'])
                    self.assertIn('watch_monitoring_active', item['monitoring_statuses'])
                    self.assertNotEqual(item['exec_runtime_state'], 'core_planned_only')

    async def test_runtime_monitored_symbols_flags_core_only_and_rejected_candidates(self) -> None:
        core_only = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9001,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': 'CORE',
                'decision_agent_name': 'signal-decider',
                'estimated_reference_price': 1000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'planned',
                'dispatch': {},
                'target_price': '1100',
                'stop_loss_price': '950',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={'CORE': {'name': 'Core only', 'market': 'KOSPI'}},
            runtime_status='ready',
        )
        rejected = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9002,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': 'REJ',
                'decision_agent_name': 'signal-decider',
                'estimated_reference_price': 2000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'planned',
                'dispatch': {
                    'status': 'accepted',
                    'plan_id': 'exec-rejected',
                    'item_links': [
                        {
                            'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                            'side': 'buy',
                            'symbol': 'REJ',
                            'remote_status': 'rejected',
                            'response_item': {
                                'status': 'rejected',
                                'agent_sizing': {
                                    'approved': False,
                                    'action': 'reject',
                                    'reason_code': 'expired_setup',
                                },
                            },
                        }
                    ],
                },
                'target_price': '2200',
                'stop_loss_price': '1900',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={'REJ': {'name': 'Rejected', 'market': 'KOSPI'}},
            runtime_status='ready',
        )
        accepted_with_false_agent_decision = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9003,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': 'RNO',
                'decision_agent_name': 'short-term-decider',
                'estimated_reference_price': 3000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'delegated_for_exec_agent_sizing',
                'dispatch': {
                    'status': 'accepted',
                    'plan_id': 'exec-rejected-by-agent',
                    'item_links': [
                        {
                            'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                            'side': 'buy',
                            'symbol': 'RNO',
                            'remote_status': 'accepted',
                            'response_item': {
                                'status': 'accepted',
                                'agent_sizing': {
                                    'approved': False,
                                    'action': 'reject',
                                    'reason_code': 'liquidity_block',
                                },
                            },
                        }
                    ],
                },
                'target_price': '3300',
                'stop_loss_price': '2800',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={'RNO': {'name': 'Rejected Accepted', 'market': 'KOSPI'}},
            runtime_status='ready',
        )

        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_monitored',
                'display_price_available': False,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': []}

            payload = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [core_only, rejected, accepted_with_false_agent_decision],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
            )

        by_symbol = {item['symbol']: item for item in payload['items']}
        self.assertFalse(by_symbol['CORE']['exec_runtime_showable'])
        self.assertFalse(by_symbol['CORE']['exec_agent_approved'])
        self.assertFalse(by_symbol['CORE']['held_position'])
        self.assertEqual(by_symbol['CORE']['exec_runtime_state'], 'core_planned_only')
        self.assertEqual(by_symbol['CORE']['exec_runtime_exclusion_reason'], 'core_planned_not_exec_approved')
        self.assertFalse(by_symbol['REJ']['exec_runtime_showable'])
        self.assertFalse(by_symbol['REJ']['exec_agent_approved'])
        self.assertEqual(by_symbol['REJ']['exec_agent_action'], 'reject')
        self.assertEqual(by_symbol['REJ']['exec_agent_reason_code'], 'expired_setup')
        self.assertEqual(by_symbol['REJ']['exec_runtime_state'], 'exec_agent_rejected')
        self.assertEqual(by_symbol['REJ']['exec_runtime_exclusion_reason'], 'exec_agent_rejected')
        self.assertIn('rejected', by_symbol['REJ']['monitoring_statuses'])
        self.assertFalse(by_symbol['RNO']['exec_runtime_showable'])
        self.assertFalse(by_symbol['RNO']['exec_agent_approved'])
        self.assertEqual(by_symbol['RNO']['exec_agent_action'], 'reject')
        self.assertEqual(by_symbol['RNO']['exec_agent_reason_code'], 'liquidity_block')
        self.assertEqual(by_symbol['RNO']['exec_runtime_state'], 'exec_agent_rejected')
        self.assertEqual(by_symbol['RNO']['exec_runtime_exclusion_reason'], 'exec_agent_rejected')

    async def test_runtime_membership_does_not_infer_plan_approval(self) -> None:
        core_only = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9003,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': self.symbol,
                'decision_agent_name': 'signal-decider',
                'estimated_reference_price': 50000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'accepted',
                'dispatch': {
                    'status': 'accepted',
                    'plan_id': f'core-signal-decider-{self.target_trade_date:%Y%m%d}-KIS_00000000_00',
                },
                'target_price': '54000',
                'stop_loss_price': '47000',
                'trigger_price': '50500',
                'chase_limit_price': '51500',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={self.symbol: {'name': 'Samsung', 'market': 'KOSPI'}},
            runtime_status='ready',
        )

        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            payload = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [core_only],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_exec_monitoring_membership=(
                    ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({self.symbol}),
                        membership_complete=True,
                    )
                ),
            )

        item = payload['items'][0]
        self.assertFalse(item['exec_agent_approved'])
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'runtime_membership_confirmed')
        self.assertIsNone(item['exec_runtime_exclusion_reason'])
        self.assertIsNone(item['exec_agent_action'])
        self.assertIn('runtime_membership_active', item['monitoring_statuses'])

    async def test_runtime_membership_does_not_relabel_same_symbol_rejection(
        self,
    ) -> None:
        rejected = {
            'symbol': '005930',
            'name': 'Samsung',
            'market': 'KOSPI',
            'status': 'failed',
            'monitoring_state': 'idle',
            'planned_intents': ['target_sell'],
            'exec_monitoring_active': False,
            'exec_runtime_showable': False,
            'exec_runtime_state': 'exec_agent_rejected',
            'exec_runtime_exclusion_reason': 'exec_agent_rejected',
            'exec_agent_approved': False,
            'exec_agent_action': 'reject',
            'exec_agent_reason_code': 'expired_setup',
            'held_position': False,
            'position_quantity': 0,
            'monitoring_statuses': ['rejected'],
        }
        with patch(
            'app.orders.exec_integration.kis_realtime_price_service'
        ) as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'display_price_available': False,
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_subscribed',
            }
            payload = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [rejected],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_exec_monitoring_membership=(
                    ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({'005930'}),
                        membership_complete=True,
                    )
                ),
            )

        item = payload['items'][0]
        self.assertTrue(item['exec_monitoring_active'])
        self.assertFalse(item['exec_agent_approved'])
        self.assertEqual(item['exec_runtime_state'], 'runtime_membership_confirmed')
        self.assertIsNone(item['exec_agent_action'])
        self.assertIsNone(item['exec_agent_reason_code'])

    async def test_live_portfolio_holding_symbols_require_fresh_complete_positions(
        self,
    ) -> None:
        cases = [
            (
                'missing_holdings',
                {
                    'sync': {'status': 'stale'},
                    'freshness': {'isStale': False},
                    'accounts': [{'id': self.account_reference}],
                },
                None,
            ),
            (
                'stale_empty',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {'isStale': True},
                    'accounts': [
                        {'id': self.account_reference, 'holdings': []}
                    ],
                },
                None,
            ),
            (
                'missing_freshness_proof',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {},
                    'accounts': [
                        {'id': self.account_reference, 'holdings': []}
                    ],
                },
                None,
            ),
            (
                'malformed_holding',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {'isStale': False},
                    'accounts': [
                        {'id': self.account_reference, 'holdings': ['bad-row']}
                    ],
                },
                None,
            ),
            (
                'invalid_holding_quantity',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {'isStale': False},
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'holdings': [
                                {'symbol': self.symbol, 'quantity': 'invalid'}
                            ],
                        }
                    ],
                },
                None,
            ),
            (
                'negative_holding_quantity',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {'isStale': False},
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'holdings': [
                                {'symbol': self.symbol, 'quantity': '-1'}
                            ],
                        }
                    ],
                },
                None,
            ),
            (
                'non_krx_holding_symbol',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {'isStale': False},
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'holdings': [
                                {'symbol': 'AAPL', 'quantity': '1'}
                            ],
                        }
                    ],
                },
                None,
            ),
            (
                'unicode_digit_holding_symbol',
                {
                    'sync': {'status': 'synced'},
                    'freshness': {
                        'isStale': False,
                        'refreshInFlight': False,
                    },
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'holdings': [
                                {'symbol': '００５９３０', 'quantity': '1'}
                            ],
                        }
                    ],
                },
                None,
            ),
            (
                'refresh_in_flight',
                {
                    'asOf': '2026-04-30T00:00:00+00:00',
                    'source': 'kis_portfolio_summary_v1',
                    'accountCoverage': {
                        'status': 'complete',
                        'configuredCount': 1,
                        'returnedCount': 1,
                    },
                    'sync': {'status': 'synced'},
                    'freshness': {
                        'isStale': False,
                        'refreshInFlight': True,
                    },
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'broker': 'kis',
                            'holdings': [],
                            'holdingsCompleteness': {'status': 'complete'},
                        }
                    ],
                },
                None,
            ),
            (
                'fresh_explicit_empty',
                {
                    'asOf': '2026-04-30T00:00:00+00:00',
                    'source': 'kis_portfolio_summary_v1',
                    'accountCoverage': {
                        'status': 'complete',
                        'configuredCount': 1,
                        'returnedCount': 1,
                    },
                    'sync': {'status': 'synced'},
                    'freshness': {
                        'isStale': False,
                        'refreshInFlight': False,
                    },
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'broker': 'kis',
                            'holdings': [],
                            'holdingsCompleteness': {'status': 'complete'},
                        }
                    ],
                },
                set(),
            ),
            (
                'summary_degraded_positions_complete',
                {
                    'asOf': '2026-04-30T00:00:00+00:00',
                    'source': 'kis_portfolio_summary_v1',
                    'accountCoverage': {
                        'status': 'complete',
                        'configuredCount': 1,
                        'returnedCount': 1,
                    },
                    'sync': {'status': 'stale'},
                    'freshness': {
                        'isStale': False,
                        'refreshInFlight': False,
                    },
                    'accounts': [
                        {
                            'id': self.account_reference,
                            'broker': 'kis',
                            'holdingsCompleteness': {'status': 'complete'},
                            'holdings': [
                                {'symbol': '005930', 'quantity': 5}
                            ],
                        }
                    ],
                },
                {'005930'},
            ),
        ]

        for label, payload, expected in cases:
            with self.subTest(case=label), patch(
                'app.orders.exec_integration.portfolio_summary_service.'
                'get_portfolio_summary',
                new=AsyncMock(return_value=payload),
            ):
                service = SignalDeciderExecIntegrationService(
                    client=FakeExecClient()
                )
                actual = await service._load_live_portfolio_holding_symbols(
                    trigger='test'
                )
            self.assertEqual(actual, expected)

    async def test_portfolio_membership_preserves_positive_facts_until_exact_empty(
        self,
    ) -> None:
        def portfolio_payload(
            symbols: list[str],
            *,
            generated_at: str,
            complete: bool,
        ) -> dict:
            account = {
                'id': self.account_reference,
                'broker': 'kis',
                'holdingsCompleteness': {
                    'status': 'complete' if complete else 'incomplete',
                },
            }
            rows = [
                {'symbol': symbol, 'quantity': 1}
                for symbol in symbols
            ]
            if complete:
                account['holdings'] = rows
            else:
                account['observedHoldings'] = rows
            return {
                'asOf': generated_at,
                'source': 'kis_portfolio_summary_v1',
                'accountCoverage': {
                    'status': 'complete',
                    'configuredCount': 1,
                    'returnedCount': 1,
                },
                'sync': {'status': 'synced' if complete else 'stale'},
                'freshness': {
                    'isStale': not complete,
                    'refreshInFlight': False,
                },
                'accounts': [account],
            }

        with patch(
            'app.orders.exec_integration.portfolio_summary_service.'
            'get_portfolio_summary',
            new=AsyncMock(
                return_value=portfolio_payload(
                    ['005930'],
                    generated_at='2026-04-30T00:00:00+00:00',
                    complete=True,
                )
            ),
        ):
            complete = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.target_trade_date,
                    trigger='complete',
                )
            )

        with patch(
            'app.orders.exec_integration.portfolio_summary_service.'
            'get_portfolio_summary',
            new=AsyncMock(side_effect=RuntimeError('transport failed')),
        ):
            failed = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.target_trade_date,
                    trigger='failed',
                )
            )

        with patch(
            'app.orders.exec_integration.portfolio_summary_service.'
            'get_portfolio_summary',
            new=AsyncMock(
                return_value=portfolio_payload(
                    ['027710'],
                    generated_at='2026-04-30T00:00:01+00:00',
                    complete=False,
                )
            ),
        ):
            partial = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.target_trade_date,
                    trigger='partial',
                )
            )

        with patch(
            'app.orders.exec_integration.portfolio_summary_service.'
            'get_portfolio_summary',
            new=AsyncMock(
                return_value=portfolio_payload(
                    [],
                    generated_at='2026-04-30T00:00:02+00:00',
                    complete=True,
                )
            ),
        ):
            empty = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.target_trade_date,
                    trigger='empty',
                )
            )

        self.assertEqual(
            complete,
            PortfolioHoldingSymbolMembership(
                symbols=frozenset({'005930'}),
                membership_complete=True,
                source_version=datetime(
                    2026, 4, 30, 0, 0, tzinfo=timezone.utc
                ),
            ),
        )
        self.assertEqual(failed.symbols, frozenset({'005930'}))
        self.assertFalse(failed.membership_complete)
        self.assertEqual(
            partial.symbols,
            frozenset({'005930', '027710'}),
        )
        self.assertFalse(partial.membership_complete)
        self.assertEqual(empty.symbols, frozenset())
        self.assertTrue(empty.membership_complete)

    async def test_portfolio_membership_does_not_cross_market_dates(self) -> None:
        load = AsyncMock()
        with patch(
            'app.orders.exec_integration.portfolio_summary_service.'
            'get_portfolio_summary',
            load,
        ):
            membership = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.market_date,
                    trigger='historical',
                )
            )

        self.assertIsNone(membership)
        load.assert_not_awaited()

    async def test_equal_version_portfolio_coverage_conflict_stays_partial(
        self,
    ) -> None:
        partial_payload = {
            'asOf': '2026-04-30T00:00:00+00:00',
            'source': 'kis_portfolio_summary_v1',
            'accountCoverage': {
                'status': 'complete',
                'configuredCount': 1,
                'returnedCount': 1,
            },
            'freshness': {
                'isStale': True,
                'refreshInFlight': False,
            },
            'accounts': [
                {
                    'id': self.account_reference,
                    'broker': 'kis',
                    'holdingsCompleteness': {'status': 'incomplete'},
                    'observedHoldings': [],
                }
            ],
        }
        complete_payload = copy.deepcopy(partial_payload)
        complete_payload['freshness']['isStale'] = False
        complete_payload['accounts'][0].pop('observedHoldings')
        complete_payload['accounts'][0]['holdings'] = []
        complete_payload['accounts'][0]['holdingsCompleteness'] = {
            'status': 'complete'
        }

        with patch(
            'app.orders.exec_integration.portfolio_summary_service.'
            'get_portfolio_summary',
            new=AsyncMock(side_effect=[partial_payload, complete_payload]),
        ):
            partial = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.target_trade_date,
                    trigger='partial',
                )
            )
            conflicting_complete = (
                await self.service._load_live_portfolio_holding_membership(
                    market_date=self.target_trade_date,
                    trigger='conflicting-complete',
                )
            )

        self.assertFalse(partial.membership_complete)
        self.assertFalse(conflicting_complete.membership_complete)

    async def test_live_exec_monitoring_symbols_require_complete_current_snapshot(
        self,
    ) -> None:
        cases = [
            ('missing_contract', {}, None),
            (
                'incomplete',
                _runtime_monitored_membership_payload(
                    [],
                    membership_complete=False,
                ),
                (frozenset(), False),
            ),
            (
                'incomplete_known_domestic_symbol',
                {
                    **_runtime_monitored_membership_payload(
                        ['027710'],
                        membership_complete=False,
                    ),
                    'coverage': {
                        **_runtime_monitored_membership_payload(
                            [],
                            membership_complete=False,
                        )['coverage'],
                        'execution_scope': {
                            **_runtime_monitored_membership_payload(
                                [],
                                membership_complete=False,
                            )['coverage']['execution_scope'],
                            'status': 'available',
                            'reason': None,
                        },
                    },
                },
                (frozenset({'027710'}), False),
            ),
            (
                'wrong_market_date',
                _runtime_monitored_membership_payload(
                    [],
                    market_date=self.market_date.isoformat(),
                ),
                None,
            ),
            (
                'naive_generated_at',
                _runtime_monitored_membership_payload(
                    [],
                    generated_at='2026-04-30T00:00:00',
                ),
                None,
            ),
            (
                'future_generated_at',
                _runtime_monitored_membership_payload(
                    [],
                    generated_at='2026-04-30T00:02:00+00:00',
                ),
                None,
            ),
            (
                'stale_same_market_date_generated_at',
                _runtime_monitored_membership_payload(
                    [],
                    generated_at='2026-04-29T15:00:00+00:00',
                ),
                None,
            ),
            (
                'unavailable_coverage',
                {
                    **_runtime_monitored_membership_payload([]),
                    'coverage': {
                        **_runtime_monitored_membership_payload([])['coverage'],
                        'status': 'unavailable',
                    },
                },
                None,
            ),
            (
                'wrong_provider',
                {
                    **_runtime_monitored_membership_payload([]),
                    'coverage': {
                        **_runtime_monitored_membership_payload([])['coverage'],
                        'provider': 'mock',
                    },
                },
                None,
            ),
            (
                'scope_without_krx',
                {
                    **_runtime_monitored_membership_payload([]),
                    'coverage': {
                        **_runtime_monitored_membership_payload([])['coverage'],
                        'execution_scope': {
                            **_runtime_monitored_membership_payload([])['coverage']['execution_scope'],
                            'exchanges': ['NYSE'],
                        },
                    },
                },
                None,
            ),
            (
                'complete_scope_without_broker_proof',
                {
                    **_runtime_monitored_membership_payload([]),
                    'coverage': {
                        **_runtime_monitored_membership_payload([])['coverage'],
                        'execution_scope': {
                            **_runtime_monitored_membership_payload([])['coverage']['execution_scope'],
                            'source': None,
                            'reason': 'broker_scope_unknown',
                            'exchanges': ['KRX', 'NYSE'],
                        },
                    },
                },
                None,
            ),
            (
                'unsorted_or_duplicate_symbols',
                _runtime_monitored_membership_payload(
                    [self.symbol, '000660', self.symbol],
                ),
                None,
            ),
            (
                'complete_empty',
                _runtime_monitored_membership_payload([]),
                (frozenset(), True),
            ),
            (
                'complete_rejects_non_domestic_symbols',
                _runtime_monitored_membership_payload(
                    ['005930', 'AAPL'],
                ),
                None,
            ),
            (
                'complete_rejects_unicode_digit_symbols',
                _runtime_monitored_membership_payload(
                    ['005930', '００５９３０'],
                ),
                None,
            ),
        ]

        for label, payload, expected in cases:
            with self.subTest(case=label):
                client = FakeExecClient()
                client.runtime_membership_response = payload
                service = SignalDeciderExecIntegrationService(client=client)
                actual = await service._load_live_exec_monitoring_symbols(
                    market_date=self.target_trade_date,
                    trigger='test',
                )
            if expected is None:
                self.assertIsNone(actual)
            else:
                self.assertIsNotNone(actual)
                self.assertEqual(actual.symbols, expected[0])
                self.assertEqual(actual.membership_complete, expected[1])
                self.assertEqual(
                    actual.source_version,
                    datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc),
                )

        calls_before_historical_read = self.client.runtime_membership_calls
        historical = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.market_date,
            trigger='test-historical',
        )
        self.assertIsNone(historical)
        self.assertEqual(
            self.client.runtime_membership_calls,
            calls_before_historical_read,
        )

    async def test_delayed_older_exec_membership_cannot_replace_newer_snapshot(
        self,
    ) -> None:
        older_started = asyncio.Event()
        release_older = asyncio.Event()
        request_count = 0

        async def load_membership() -> dict:
            nonlocal request_count
            request_count += 1
            if request_count == 1:
                older_started.set()
                await release_older.wait()
                return _runtime_monitored_membership_payload(
                    ['005930'],
                    generated_at='2026-04-30T00:00:01+00:00',
                )
            return _runtime_monitored_membership_payload(
                ['027710'],
                generated_at='2026-04-30T00:00:02+00:00',
            )

        self.client.get_runtime_monitored_symbol_membership = load_membership
        older_task = asyncio.create_task(
            self.service._load_live_exec_monitoring_symbols(
                market_date=self.target_trade_date,
                trigger='older-request',
            )
        )
        await asyncio.wait_for(older_started.wait(), timeout=1)
        newer = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='newer-request',
        )
        release_older.set()
        older = await asyncio.wait_for(older_task, timeout=1)

        self.assertIsNotNone(newer)
        self.assertEqual(newer.symbols, frozenset({'027710'}))
        self.assertIsNotNone(older)
        self.assertEqual(older, newer)

    async def test_future_exec_membership_cannot_poison_source_watermark(
        self,
    ) -> None:
        self.client.runtime_membership_response = (
            _runtime_monitored_membership_payload(
                ['005930'],
                generated_at='2026-04-30T01:00:00+00:00',
            )
        )
        rejected = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='future',
        )
        self.client.runtime_membership_response = (
            _runtime_monitored_membership_payload(
                ['027710'],
                generated_at='2026-04-30T00:00:01+00:00',
            )
        )
        current = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='current',
        )

        self.assertIsNone(rejected)
        self.assertIsNotNone(current)
        self.assertEqual(current.symbols, frozenset({'027710'}))
        self.assertTrue(current.membership_complete)

    async def test_equal_version_exec_membership_conflict_fails_closed(
        self,
    ) -> None:
        self.client.runtime_membership_response = (
            _runtime_monitored_membership_payload(['005930'])
        )
        first = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='first',
        )
        self.client.runtime_membership_response = (
            _runtime_monitored_membership_payload(['027710'])
        )
        conflict = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='conflict',
        )

        self.assertIsNotNone(first)
        self.assertIsNotNone(conflict)
        self.assertEqual(
            conflict.symbols,
            frozenset({'005930', '027710'}),
        )
        self.assertFalse(conflict.membership_complete)

    async def test_newer_incomplete_exec_membership_preserves_known_symbols(
        self,
    ) -> None:
        self.client.runtime_membership_response = (
            _runtime_monitored_membership_payload(
                ['005930'],
                generated_at='2026-04-30T00:00:01+00:00',
            )
        )
        complete = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='complete',
        )
        incomplete_payload = _runtime_monitored_membership_payload(
            ['027710'],
            generated_at='2026-04-30T00:00:02+00:00',
            membership_complete=False,
        )
        incomplete_payload['coverage']['execution_scope'] = {
            **incomplete_payload['coverage']['execution_scope'],
            'status': 'available',
            'source': 'configured_broker_account',
            'reason': None,
        }
        self.client.runtime_membership_response = incomplete_payload
        incomplete = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='incomplete',
        )

        self.assertIsNotNone(complete)
        self.assertEqual(complete.symbols, frozenset({'005930'}))
        self.assertTrue(complete.membership_complete)
        self.assertIsNotNone(incomplete)
        self.assertEqual(
            incomplete.symbols,
            frozenset({'005930', '027710'}),
        )
        self.assertFalse(incomplete.membership_complete)

    async def test_exec_membership_failure_retains_symbols_without_clear_authority(
        self,
    ) -> None:
        self.client.runtime_membership_response = (
            _runtime_monitored_membership_payload(['027710'])
        )
        current = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='current',
        )
        self.client.runtime_membership_response = {}
        degraded = await self.service._load_live_exec_monitoring_symbols(
            market_date=self.target_trade_date,
            trigger='degraded',
        )

        self.assertIsNotNone(current)
        self.assertTrue(current.membership_complete)
        self.assertIsNotNone(degraded)
        self.assertEqual(degraded.symbols, current.symbols)
        self.assertFalse(degraded.membership_complete)

    async def test_runtime_monitored_publication_is_current_date_and_opt_in(self) -> None:
        payload = {
            'market_date': self.target_trade_date.isoformat(),
            'items': [],
        }
        with (
            patch.object(
                self.service,
                '_load_dashboard_cached_payload',
                return_value=payload,
            ),
            patch(
                'app.orders.exec_integration.kis_realtime_price_service'
            ) as price_service,
        ):
            price_service.observation_revision.return_value = 42
            await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='current-date',
            )
            price_service.observe_runtime_monitored_payload.assert_called_once_with(
                payload,
                trigger='current-date',
                expected_observation_revision=42,
            )

            price_service.reset_mock()
            await self.service.list_runtime_monitored_symbols(
                market_date=self.market_date,
                refresh=False,
                trigger='historical-date',
            )
            price_service.observe_runtime_monitored_payload.assert_not_called()

            await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='source-read',
                publish_realtime_observation=False,
            )
            price_service.observe_runtime_monitored_payload.assert_not_called()

    async def test_runtime_monitored_symbols_include_historical_held_position(self) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.target_trade_date = self.market_date
            row.source_market_date = date(2026, 4, 28)
            row.execution_status = EXECUTION_STATUS_SUBMITTED
            row.submitted_at = datetime(2026, 4, 29, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': 'historical-held-plan',
                    'dispatch_status': 'accepted',
                    'last_attempt_at': '2026-04-29T00:00:00+00:00',
                    'last_success_at': '2026-04-29T00:00:00+00:00',
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                            'intent': EXEC_INTENT_NEXT_OPEN_BUY,
                            'side': 'buy',
                            'symbol': self.symbol,
                            'remote_status': 'filled',
                        },
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:target_sell',
                            'intent': EXEC_INTENT_TARGET_SELL,
                            'side': 'sell',
                            'symbol': self.symbol,
                        },
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:stop_sell',
                            'intent': EXEC_INTENT_STOP_SELL,
                            'side': 'sell',
                            'symbol': self.symbol,
                        },
                    ],
                    'readback': {
                        'last_synced_at': '2026-04-29T00:07:00+00:00',
                        'runtime': {'status': 'ok', 'payload': {'ready': True}},
                        'orders': {'status': 'ok', 'count': 0, 'items': []},
                        'executions': {
                            'status': 'ok',
                            'count': 1,
                            'items': [
                                {
                                    'broker_order_id': 'historical-buy-order',
                                    'symbol': self.symbol,
                                    'side': 'buy',
                                    'quantity': '7',
                                    'fill_price': '50100',
                                    'executed_at': '2026-04-29T00:06:00+00:00',
                                }
                            ],
                        },
                        'positions': {
                            'status': 'ok',
                            'count': 1,
                            'items': [
                                {
                                    'account_reference': self.account_reference,
                                    'symbol': self.symbol,
                                    'quantity': '7',
                                    'average_price': '50100',
                                    'updated_at': '2026-04-29T00:07:00+00:00',
                                }
                            ],
                        },
                        'derived_state': {
                            'derived_status': 'open',
                            'entry_filled_quantity': '7',
                            'exit_filled_quantity': None,
                            'open_quantity': '7',
                            'closed_quantity': '0',
                            'has_plan_attributed_fill_evidence': True,
                            'endpoint_statuses': {
                                'plans': 'ok',
                                'orders': 'ok',
                                'executions': 'ok',
                                'positions': 'ok',
                            },
                        },
                    },
                },
            }

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_portfolio_holding_membership', new=AsyncMock(return_value=None)),
            patch.object(
                self.service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(
                    return_value=ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({self.symbol}),
                        membership_complete=True,
                    )
                ),
            ),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(monitored['summary']['items_total'], 1)
        item = monitored['items'][0]
        self.assertEqual(item['symbol'], self.symbol)
        self.assertIn(item['monitoring_state'], {'watching_target_stop', 'reconciling'})
        self.assertTrue(item['held_position'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['position_quantity'], 7)
        self.assertEqual(item['position_average_price'], 50100)
        self.assertIn('target_sell_monitoring', item['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', item['monitoring_statuses'])

        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_monitored',
                'display_price_available': False,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': []}

            monitored_without_live_holding = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_holding_symbols=set(),
            )

        self.assertEqual(monitored_without_live_holding['summary']['items_total'], 0)
        self.assertEqual(monitored_without_live_holding['items'], [])

    async def test_runtime_monitored_symbols_use_cached_display_fallback_when_capacity_limited(self) -> None:
        fallback_symbol = '000660'
        self._seed_signal_candidate(
            symbol=fallback_symbol,
            name_ko='SK hynix',
            decision_run_id='exec-run-2',
            client_order_id='SDNO-20260430-000660',
        )

        def realtime_fields(symbol: str) -> dict:
            if symbol == self.symbol:
                return {
                    'current_price': 50200,
                    'price_as_of': '2026-04-30T00:07:30+00:00',
                    'price_received_at': '2026-04-30T00:07:31+00:00',
                    'price_source': 'kis_realtime_ws',
                    'price_status': 'available',
                    'realtime_subscribed': True,
                    'realtime_subscription_reason': 'subscribed',
                    'display_price_available': True,
                }
            return {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'limited',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'capacity_limited',
                'display_price_available': False,
            }

        def current_quote(symbol: str) -> dict | None:
            if symbol != fallback_symbol:
                return None
            return {
                'symbol': fallback_symbol,
                'price': 182500,
                'cached_at': '2026-04-30T00:06:00+00:00',
                'as_of': '2026-04-30T00:06:00+00:00',
                'trade_date': '2026-04-30',
            }

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch('app.quotes.display_prices.load_current_quote', side_effect=current_quote),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol') as load_daily_bars,
            patch('app.brokers.kis.client.KISClient.fetch_domestic_quote', new_callable=AsyncMock) as fetch_quote,
        ):
            price_service.price_fields_for_symbol.side_effect = realtime_fields
            price_service.observe_monitored_symbols.return_value = {
                'allowed_symbols': [self.symbol],
                'limited_symbols': [fallback_symbol],
            }

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            cached_monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        by_symbol = {item['symbol']: item for item in monitored['items']}
        fallback_item = by_symbol[fallback_symbol]
        self.assertEqual(fallback_item['current_price'], 182500)
        self.assertEqual(fallback_item['price_source'], 'cached_quote')
        self.assertEqual(fallback_item['price_status'], 'fallback')
        self.assertTrue(fallback_item['display_price_available'])
        self.assertFalse(fallback_item['realtime_subscribed'])
        self.assertEqual(fallback_item['realtime_subscription_reason'], 'capacity_limited')
        self.assertEqual(monitored['summary']['desired_symbol_count'], 2)
        self.assertEqual(monitored['summary']['realtime_subscribed_count'], 1)
        self.assertEqual(monitored['summary']['capacity_limited_count'], 1)
        self.assertEqual(monitored['summary']['cached_fallback_covered_count'], 1)
        self.assertEqual(monitored['summary']['truly_unavailable_count'], 0)
        self.assertEqual(cached_monitored['summary']['cached_fallback_covered_count'], 1)
        self.assertEqual(monitored['realtime_subscription']['desired_symbol_count'], 2)
        load_daily_bars.assert_not_called()
        fetch_quote.assert_not_called()

    async def test_runtime_symbol_detail_labels_stale_cached_display_fallback(self) -> None:
        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch(
                'app.quotes.display_prices.load_current_quote',
                return_value={
                    'symbol': self.symbol,
                    'price': 49800,
                    'cached_at': '2026-04-29T06:10:00+00:00',
                    'trade_date': '2026-04-29',
                },
            ),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol') as load_daily_bars,
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_monitored',
                'display_price_available': False,
            }

            detail = await self.service.get_runtime_symbol_detail(
                market_date=self.target_trade_date,
                symbol=self.symbol,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(detail['current_price'], 49800)
        self.assertEqual(detail['price_source'], 'cached_quote')
        self.assertEqual(detail['price_status'], 'stale')
        self.assertTrue(detail['display_price_available'])
        self.assertEqual(detail['items'][0]['price_status'], 'stale')
        load_daily_bars.assert_not_called()

    async def test_runtime_monitored_payload_is_pure_and_prioritizes_symbols(self) -> None:
        board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'items': [
                {
                    'symbol': 'LOW',
                    'name': 'Low priority',
                    'market': 'KOSPI',
                    'status': 'planned',
                    'monitoring_state': 'idle',
                    'planned_intents': ['target_sell'],
                    'decision_agent_name': 'signal-decider',
                },
                {
                    'symbol': 'HIGH',
                    'name': 'Held position',
                    'market': 'KOSPI',
                    'status': 'filled',
                    'monitoring_state': 'watching_target_stop',
                    'planned_intents': ['stop_sell', 'target_sell'],
                    'open_quantity': 5,
                    'decision_agent_name': 'signal-decider',
                },
                {
                    'symbol': 'MID',
                    'name': 'Intraday watch',
                    'market': 'KOSPI',
                    'intent': 'intraday_watch_buy',
                    'status': 'accepted',
                    'monitoring_state': 'watching_fill',
                    'trigger_price': 50500,
                    'chase_limit_price': 51500,
                    'decision_agent_name': 'short-term-decider',
                },
            ],
        }
        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 100,
                'price_as_of': '2026-04-30T00:00:00+00:00',
                'price_received_at': '2026-04-30T00:00:01+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }

            payload = self.service._build_runtime_monitored_symbols_payload(
                board=board,
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_holding_symbols={'HIGH'},
                live_exec_monitoring_membership=(
                    ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset(),
                        membership_complete=True,
                    )
                ),
            )

        price_service.observe_monitored_symbols.assert_not_called()
        price_service.observe_runtime_monitored_payload.assert_not_called()
        self.assertEqual(payload['realtime_subscription']['desired_symbols'], ['HIGH', 'MID', 'LOW'])
        self.assertTrue(payload['source_coverage']['membership_complete'])
        self.assertEqual(
            payload['source_coverage']['live_exec_monitoring'],
            'complete',
        )

        incomplete_payload = self.service._build_runtime_monitored_symbols_payload(
            board={**board, 'items': []},
            refresh=False,
            refresh_state={'requested': False, 'status': 'not_requested'},
            served_from_cache=False,
            live_holding_symbols=set(),
            live_exec_monitoring_membership=None,
        )
        self.assertFalse(
            incomplete_payload['source_coverage']['membership_complete']
        )
        self.assertEqual(
            incomplete_payload['source_coverage']['live_exec_monitoring'],
            'unknown',
        )

    async def test_runtime_monitored_payload_preserves_fresh_board_coverage(self) -> None:
        generated_at = datetime(2026, 4, 30, 0, 10, tzinfo=timezone.utc)
        board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'freshness': {
                'schema_version': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
                'endpoint': 'daily_board',
                'market_date': self.target_trade_date.isoformat(),
                'status': 'fresh',
                'served_from_cache': False,
                'read_model_source': 'persisted_exec_read_model',
                'generated_at': generated_at.isoformat(),
                'cached_at': generated_at.isoformat(),
                'cache_age_seconds': 0,
                'cache_ttl_seconds': 5,
                'is_stale': False,
                'is_partial': False,
                'refreshing': False,
                'last_readback_synced_at': generated_at.isoformat(),
                'readback_age_seconds': 0,
                'readback_synced_count': 1,
                'readback_missing_count': 0,
                'overview_item_count': 1,
                'output_item_count': 1,
            },
            'items': [
                {
                    'symbol': '005930',
                    'name': 'Samsung Electronics',
                    'market': 'KOSPI',
                    'intent': 'intraday_watch_buy',
                    'status': 'accepted',
                    'monitoring_state': 'watching_fill',
                    'trigger_price': 70000,
                    'planned_intents': [],
                }
            ],
        }

        payload = self.service._build_runtime_monitored_symbols_payload(
            board=board,
            refresh=False,
            refresh_state={'requested': False, 'status': 'not_requested'},
            served_from_cache=False,
            live_holding_symbols=set(),
            live_exec_monitoring_membership=ExecRuntimeMonitoredSymbolMembership(
                symbols=frozenset(),
                membership_complete=True,
            ),
        )

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )
        self.assertEqual(payload['freshness']['status'], 'fresh')
        self.assertEqual(payload['freshness']['readback_missing_count'], 0)
        self.assertTrue(source.membership_complete)

    async def test_failed_fresh_build_refresh_is_partial(self) -> None:
        generated_at = datetime(2026, 4, 30, 0, 10, tzinfo=timezone.utc)
        freshness = self.service._build_runtime_monitored_freshness(
            source_freshness={
                'schema_version': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
                'endpoint': 'daily_board',
                'market_date': self.target_trade_date.isoformat(),
                'status': 'fresh',
                'served_from_cache': False,
                'read_model_source': 'persisted_exec_read_model',
                'generated_at': generated_at.isoformat(),
                'cached_at': generated_at.isoformat(),
                'cache_age_seconds': 0,
                'cache_ttl_seconds': 5,
                'is_stale': False,
                'is_partial': False,
                'refreshing': False,
                'readback_synced_count': 1,
                'readback_missing_count': 0,
                'overview_item_count': 1,
                'output_item_count': 1,
            },
            endpoint='monitored_symbols',
            market_date=self.target_trade_date,
            generated_at=generated_at,
            served_from_cache=False,
            runtime_status='ready',
            overview_items=[{'symbol': '005930'}],
            output_item_count=1,
            refresh_state={
                'requested': True,
                'status': 'failed',
                'in_flight': False,
            },
        )

        self.assertEqual(freshness['status'], 'partial')
        self.assertTrue(freshness['is_partial'])

    async def test_missing_upstream_freshness_cannot_authorize_exact_empty(
        self,
    ) -> None:
        payload = self.service._build_runtime_monitored_symbols_payload(
            board={
                'market_date': self.target_trade_date.isoformat(),
                'runtime': {'status': 'ready', 'mode': 'mock'},
                'items': [],
            },
            refresh=False,
            refresh_state={'requested': False, 'status': 'not_requested'},
            served_from_cache=False,
            live_portfolio_holding_membership=(
                PortfolioHoldingSymbolMembership(
                    symbols=frozenset(),
                    membership_complete=True,
                )
            ),
            live_exec_monitoring_membership=(
                ExecRuntimeMonitoredSymbolMembership(
                    symbols=frozenset(),
                    membership_complete=True,
                )
            ),
        )

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )
        self.assertEqual(payload['freshness']['status'], 'partial')
        self.assertTrue(payload['freshness']['is_partial'])
        self.assertFalse(source.membership_complete)

    async def test_partial_holdings_membership_is_merge_only_and_loads_history(
        self,
    ) -> None:
        generated_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
        with patch.object(
            self.service,
            '_load_historical_held_runtime_board_items',
            return_value=[],
        ) as load_history:
            payload = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'freshness': {
                        'schema_version': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
                        'endpoint': 'daily_board',
                        'market_date': self.target_trade_date.isoformat(),
                        'status': 'fresh',
                        'served_from_cache': False,
                        'read_model_source': 'persisted_exec_read_model',
                        'generated_at': generated_at.isoformat(),
                        'cached_at': generated_at.isoformat(),
                        'cache_age_seconds': 0,
                        'cache_ttl_seconds': 5,
                        'is_stale': False,
                        'is_partial': False,
                        'refreshing': False,
                        'readback_synced_count': 0,
                        'readback_missing_count': 0,
                        'overview_item_count': 0,
                        'output_item_count': 0,
                    },
                    'items': [],
                },
                refresh=False,
                refresh_state={
                    'requested': False,
                    'status': 'not_requested',
                },
                served_from_cache=False,
                live_portfolio_holding_membership=(
                    PortfolioHoldingSymbolMembership(
                        symbols=frozenset(),
                        membership_complete=False,
                    )
                ),
                live_exec_monitoring_membership=(
                    ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset(),
                        membership_complete=True,
                    )
                ),
            )

        self.assertEqual(payload['source_coverage']['live_holdings'], 'partial')
        self.assertFalse(payload['source_coverage']['membership_complete'])
        self.assertFalse(
            KISRealtimePriceService._runtime_monitored_source_from_payload(
                payload
            ).membership_complete
        )
        load_history.assert_called_once()
        self.assertFalse(
            load_history.call_args.kwargs[
                'live_holding_membership_complete'
            ]
        )

    async def test_monitored_cache_inherits_upstream_board_age(self) -> None:
        generated_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
        self.service.dashboard_cache_ttl_seconds = 5
        freshness = self.service._build_runtime_monitored_freshness(
            source_freshness={
                'schema_version': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
                'endpoint': 'daily_board',
                'market_date': self.target_trade_date.isoformat(),
                'status': 'fresh',
                'served_from_cache': True,
                'read_model_source': 'in_process_cache',
                'generated_at': generated_at.isoformat(),
                'cached_at': generated_at.isoformat(),
                'cache_age_seconds': 4,
                'cache_ttl_seconds': 5,
                'is_stale': False,
                'is_partial': False,
                'refreshing': False,
                'last_readback_synced_at': generated_at.isoformat(),
                'readback_age_seconds': 4,
                'readback_synced_count': 1,
                'readback_missing_count': 0,
                'overview_item_count': 1,
                'output_item_count': 1,
            },
            endpoint='monitored_symbols',
            market_date=self.target_trade_date,
            generated_at=generated_at,
            served_from_cache=False,
            runtime_status='ready',
            overview_items=[{'symbol': '005930'}],
            output_item_count=1,
            refresh_state={'requested': False, 'status': 'not_requested'},
        )
        self.service._store_dashboard_cached_payload(
            endpoint='monitored_symbols',
            market_date=self.target_trade_date,
            symbol=None,
            payload={
                'request': {
                    'market_date': self.target_trade_date.isoformat(),
                    'refresh': False,
                },
                'freshness': freshness,
                'items': [{'symbol': '005930'}],
            },
        )

        with patch(
            'app.orders.exec_integration._utcnow',
            return_value=generated_at + timedelta(seconds=2),
        ):
            cached = self.service._load_dashboard_cached_payload(
                endpoint='monitored_symbols',
                market_date=self.target_trade_date,
                symbol=None,
                refresh_state={
                    'requested': False,
                    'status': 'not_requested',
                },
                allow_stale=True,
            )

        self.assertIsNotNone(cached)
        self.assertEqual(cached['freshness']['cache_age_seconds'], 6)
        self.assertEqual(cached['freshness']['status'], 'stale')
        self.assertTrue(cached['freshness']['is_stale'])

    async def test_failed_refresh_revokes_exact_removal_authority(self) -> None:
        generated_at = datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc)
        self.service._store_dashboard_cached_payload(
            endpoint='monitored_symbols',
            market_date=self.target_trade_date,
            symbol=None,
            payload={
                'request': {
                    'market_date': self.target_trade_date.isoformat(),
                    'refresh': False,
                },
                'market_date': self.target_trade_date.isoformat(),
                'source_coverage': {'membership_complete': True},
                'freshness': {
                    'schema_version': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
                    'endpoint': 'monitored_symbols',
                    'market_date': self.target_trade_date.isoformat(),
                    'status': 'fresh',
                    'served_from_cache': False,
                    'read_model_source': 'persisted_exec_read_model',
                    'generated_at': generated_at.isoformat(),
                    'cached_at': generated_at.isoformat(),
                    'cache_age_seconds': 0,
                    'cache_ttl_seconds': 5,
                    'is_stale': False,
                    'is_partial': False,
                    'refreshing': False,
                    'readback_synced_count': 0,
                    'readback_missing_count': 0,
                    'overview_item_count': 0,
                    'output_item_count': 0,
                },
                'items': [],
            },
        )

        cached = self.service._load_dashboard_cached_payload(
            endpoint='monitored_symbols',
            market_date=self.target_trade_date,
            symbol=None,
            refresh_state={
                'requested': True,
                'status': 'failed',
                'in_flight': False,
            },
            allow_stale=True,
        )

        self.assertIsNotNone(cached)
        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            cached
        )
        self.assertEqual(cached['freshness']['status'], 'partial')
        self.assertTrue(cached['freshness']['is_partial'])
        self.assertFalse(source.membership_complete)

    async def test_runtime_monitored_payload_preserves_unmapped_live_membership(
        self,
    ) -> None:
        empty_board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'items': [],
        }
        complete_empty_exec = ExecRuntimeMonitoredSymbolMembership(
            symbols=frozenset(),
            membership_complete=True,
        )
        complete_exec_symbol = ExecRuntimeMonitoredSymbolMembership(
            symbols=frozenset({'005930'}),
            membership_complete=True,
        )

        with patch(
            'app.orders.exec_integration.kis_realtime_price_service'
        ) as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'display_price_available': False,
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_subscribed',
            }
            holding_only = self.service._build_runtime_monitored_symbols_payload(
                board=empty_board,
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_holding_symbols={'030000'},
                live_exec_monitoring_membership=complete_empty_exec,
            )
            exec_only = self.service._build_runtime_monitored_symbols_payload(
                board=empty_board,
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_holding_symbols=set(),
                live_exec_monitoring_membership=complete_exec_symbol,
            )
            holding_with_regular_watch = (
                self.service._build_runtime_monitored_symbols_payload(
                    board={
                        **empty_board,
                        'items': [
                            {
                                'symbol': '005930',
                                'name': 'Regular watch',
                                'market': 'KOSPI',
                                'intent': 'intraday_watch_buy',
                                'status': 'accepted',
                                'monitoring_state': 'watching_fill',
                                'trigger_price': 70000,
                                'planned_intents': [],
                            }
                        ],
                    },
                    refresh=False,
                    refresh_state={
                        'requested': False,
                        'status': 'not_requested',
                    },
                    served_from_cache=False,
                    live_holding_symbols={'030000'},
                    live_exec_monitoring_membership=complete_empty_exec,
                )
            )

        self.assertEqual(
            holding_only['realtime_subscription']['desired_symbols'],
            ['030000'],
        )
        self.assertEqual(
            [item['symbol'] for item in holding_only['items']],
            ['030000'],
        )
        self.assertTrue(holding_only['items'][0]['held_position'])
        self.assertEqual(
            holding_only['items'][0]['monitoring_state'],
            'reconciling',
        )
        self.assertTrue(holding_only['source_coverage']['membership_complete'])

        self.assertEqual(
            exec_only['realtime_subscription']['desired_symbols'],
            ['005930'],
        )
        self.assertEqual(
            [item['symbol'] for item in exec_only['items']],
            ['005930'],
        )
        self.assertTrue(exec_only['items'][0]['exec_monitoring_active'])
        self.assertTrue(exec_only['items'][0]['exec_runtime_showable'])
        self.assertTrue(exec_only['source_coverage']['membership_complete'])
        self.assertEqual(
            holding_with_regular_watch['realtime_subscription'][
                'desired_symbols'
            ],
            ['030000', '005930'],
        )

    async def test_complete_direct_sources_clear_stale_board_membership_facts(
        self,
    ) -> None:
        board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'items': [
                {
                    'symbol': '030000',
                    'name': 'Stale position',
                    'market': 'KOSPI',
                    'status': 'filled',
                    'monitoring_state': 'watching_target_stop',
                    'planned_intents': ['stop_sell', 'target_sell'],
                    'held_position': True,
                    'position_quantity': 5,
                    'position_average_price': 100,
                    'position_side': 'long',
                    'exec_monitoring_active': True,
                    'exec_runtime_showable': True,
                    'exec_runtime_state': 'held_position',
                    'exec_runtime_exclusion_reason': None,
                    'exec_runtime_exclusion_reasons': [],
                    'monitoring_statuses': [
                        'portfolio_holding_active',
                        'runtime_membership_active',
                        'stop_sell_monitoring',
                        'target_sell_monitoring',
                    ],
                }
            ],
        }

        payload = self.service._build_runtime_monitored_symbols_payload(
            board=board,
            refresh=False,
            refresh_state={'requested': False, 'status': 'not_requested'},
            served_from_cache=False,
            live_holding_symbols=set(),
            live_exec_monitoring_membership=ExecRuntimeMonitoredSymbolMembership(
                symbols=frozenset(),
                membership_complete=True,
            ),
        )

        item = payload['items'][0]
        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )
        self.assertFalse(item['held_position'])
        self.assertEqual(item['position_quantity'], 0)
        self.assertIsNone(item['position_average_price'])
        self.assertIsNone(item['position_side'])
        self.assertFalse(item['exec_monitoring_active'])
        self.assertFalse(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'no_runtime_membership')
        self.assertEqual(item['monitoring_state'], 'idle')
        self.assertNotIn(
            'portfolio_holding_active',
            item['monitoring_statuses'],
        )
        self.assertNotIn(
            'runtime_membership_active',
            item['monitoring_statuses'],
        )
        self.assertEqual(source.protected_symbols, ())

    async def test_existing_board_holding_outranks_regular_watch(self) -> None:
        board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'items': [
                {
                    'symbol': '005930',
                    'name': 'Regular watch',
                    'market': 'KOSPI',
                    'intent': 'intraday_watch_buy',
                    'status': 'accepted',
                    'monitoring_state': 'watching_fill',
                    'trigger_price': 70000,
                    'planned_intents': [],
                },
                {
                    'symbol': '030000',
                    'name': 'Held board row',
                    'market': 'KOSPI',
                    'status': 'planned',
                    'monitoring_state': 'idle',
                    'planned_intents': ['target_sell'],
                },
            ],
        }

        payload = self.service._build_runtime_monitored_symbols_payload(
            board=board,
            refresh=False,
            refresh_state={'requested': False, 'status': 'not_requested'},
            served_from_cache=False,
            live_holding_symbols={'030000'},
            live_exec_monitoring_membership=ExecRuntimeMonitoredSymbolMembership(
                symbols=frozenset(),
                membership_complete=True,
            ),
        )

        source = KISRealtimePriceService._runtime_monitored_source_from_payload(
            payload
        )
        self.assertEqual(
            payload['realtime_subscription']['desired_symbols'],
            ['030000', '005930'],
        )
        self.assertEqual(source.symbols, ('030000', '005930'))
        self.assertEqual(source.protected_symbols, ('030000',))

    async def test_runtime_dashboard_readback_facts_override_blocked_eligibility(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'monitoring': {
                'alive': True,
            },
        }
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                    'updated_at': '2026-04-30T00:05:00+00:00',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                    'fill_price': '50100',
                    'executed_at': '2026-04-30T00:06:00+00:00',
                }
            ]
        }
        self.client.position_response = {
            'items': [
                {
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        self.assertEqual(
            self.client.position_calls[-1],
            {
                'external_plan_id': plan_id,
                'execution_plan_id': plan_id,
                'symbols': [self.symbol],
            },
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

        remote_call_counts = (
            self.client.runtime_calls,
            len(self.client.plan_calls),
            len(self.client.order_calls),
            len(self.client.execution_calls),
            len(self.client.position_calls),
        )
        with patch.object(self.service, '_load_live_exec_monitoring_symbols', new=AsyncMock(return_value=None)):
            board = await self.service.get_runtime_daily_board(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['summary']['failed'], 0)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['open_quantity'], 5)
        self.assertIsNone(board['items'][0]['failure_reason'])
        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(
            remote_call_counts,
            (
                self.client.runtime_calls,
                len(self.client.plan_calls),
                len(self.client.order_calls),
                len(self.client.execution_calls),
                len(self.client.position_calls),
            ),
        )

    async def test_position_readback_with_mismatched_route_proof_fails_closed(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {
            'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]
        }
        self.client.order_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'status': 'filled',
                    'quantity': '5',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_pk': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                }
            ]
        }
        self.client.position_response = {
            'items': [
                {
                    'provider': 'kis',
                    'account_scope': 'b' * 24,
                    'exchange': 'KRX',
                    'symbol': self.symbol,
                    'quantity': '100',
                    'position_scope_proof': {
                        'schema_version': 'execution_position_plan_scope_v1',
                        'plan_id': plan_id,
                        'execution_plan_id': 'different-exec-plan',
                        'provider': 'kis',
                        'account_scope': 'b' * 24,
                        'exchange': 'KRX',
                    },
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='mismatched_position_scope',
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['positions']['status'], 'contract_error')
            self.assertEqual(readback['positions']['count'], 0)
            self.assertEqual(
                readback['positions']['error']['code'],
                'position_scope_proof_invalid',
            )
            self.assertEqual(
                readback['derived_state']['derived_status'],
                'unavailable',
            )
            self.assertIsNone(readback['derived_state']['open_quantity'])

    def test_position_readback_rejects_self_consistent_non_kis_route(self) -> None:
        result = self.service._validate_remote_position_scope_proofs(
            result={
                'status': 'ok',
                'payload': {},
                'error': None,
                'items': [
                    {
                        'provider': 'alpaca',
                        'account_scope': 'scope-a',
                        'exchange': 'NASDAQ',
                        'symbol': self.symbol,
                        'quantity': '5',
                        'position_scope_proof': {
                            'schema_version': 'execution_position_plan_scope_v1',
                            'plan_id': 'core-plan',
                            'execution_plan_id': 'exec-plan',
                            'provider': 'alpaca',
                            'account_scope': 'scope-a',
                            'exchange': 'NASDAQ',
                        },
                    }
                ],
            },
            external_plan_id='core-plan',
            execution_plan_id='exec-plan',
        )

        self.assertEqual(result['status'], 'contract_error')
        self.assertEqual(result['error']['code'], 'position_scope_proof_invalid')

    def test_position_readback_rejects_mixed_or_empty_account_scopes(self) -> None:
        for scopes in (('scope-a', 'scope-b'), ('scope-a', '')):
            with self.subTest(scopes=scopes):
                result = self.service._validate_remote_position_scope_proofs(
                    result={
                        'status': 'ok',
                        'payload': {},
                        'error': None,
                        'items': [
                            {
                                'provider': 'kis',
                                'account_scope': account_scope,
                                'exchange': 'KRX',
                                'symbol': self.symbol,
                                'quantity': '5',
                                'position_scope_proof': {
                                    'schema_version': 'execution_position_plan_scope_v1',
                                    'plan_id': 'core-plan',
                                    'execution_plan_id': 'exec-plan',
                                    'provider': 'kis',
                                    'account_scope': account_scope,
                                    'exchange': 'KRX',
                                },
                            }
                            for account_scope in scopes
                        ],
                    },
                    external_plan_id='core-plan',
                    execution_plan_id='exec-plan',
                )
                self.assertEqual(result['status'], 'contract_error')
                self.assertEqual(
                    result['error']['code'],
                    'position_scope_proof_invalid',
                )

    async def test_runtime_dashboard_filled_order_position_overrides_blocked_eligibility_without_execution(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'id': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'status': 'filled',
                    'quantity': '5',
                    'updated_at': '2026-04-30T00:05:00+00:00',
                }
            ]
        }
        self.client.execution_response = {'items': []}
        self.client.position_response = {
            'items': [
                {
                    'account_reference': self.account_reference,
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['summary']['failed'], 0)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['open_quantity'], 5)
        self.assertIsNone(board['items'][0]['failure_reason'])

    async def test_runtime_dashboard_position_without_plan_fill_evidence_does_not_mark_filled(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {
            'items': [
                {
                    'account_reference': self.account_reference,
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['filled'], 0)
        self.assertEqual(board['summary']['accepted'], 1)
        self.assertEqual(board['items'][0]['status'], 'accepted')
        self.assertEqual(board['items'][0]['open_quantity'], 0)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()
        self.service._dashboard_cache.clear()

        blocked_board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(blocked_board['summary']['filled'], 0)
        self.assertEqual(blocked_board['summary']['failed'], 1)
        self.assertEqual(blocked_board['items'][0]['status'], 'failed')
        self.assertEqual(blocked_board['items'][0]['open_quantity'], 0)

    async def test_runtime_dashboard_sell_fill_overrides_blocked_eligibility(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            dispatch_status='failed',
            execution_status='submit_failed',
            readback={
                'last_synced_at': '2026-04-30T05:31:00+00:00',
                'runtime': {
                    'status': 'ok',
                    'payload': {
                        'mode': 'mock',
                        'ready': True,
                        'monitoring_alive': True,
                    },
                },
                'derived_state': {
                    'derived_status': 'closed',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': '5',
                    'open_quantity': '0',
                    'closed_quantity': '5',
                },
                'orders': {
                    'items': [
                        {
                            'id': 'sell-order-1',
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'status': 'filled',
                            'updated_at': '2026-04-30T05:31:00+00:00',
                        }
                    ]
                },
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'quantity': '5',
                            'price': '51000',
                            'executed_at': '2026-04-30T05:31:00+00:00',
                        }
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['summary']['failed'], 0)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['exit_filled_quantity'], 5)
        self.assertEqual(board['items'][0]['open_quantity'], 0)
        self.assertEqual(board['items'][0]['broker_order_id'], 'sell-order-1')
        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_activity_status'], 'none')
        self.assertEqual(today_activity['buy_filled_quantity_today'], 0)
        self.assertIsNone(today_activity['buy_avg_fill_price_today'])
        self.assertIsNone(today_activity['buy_last_filled_at'])
        self.assertEqual(today_activity['buy_broker_order_ids'], [])
        self.assertEqual(today_activity['sell_activity_status'], 'available')
        self.assertEqual(today_activity['sell_activity_source'], 'executions')
        self.assertEqual(today_activity['sell_filled_quantity_today'], 5)
        self.assertEqual(today_activity['sell_avg_fill_price_today'], 51000)
        self.assertEqual(today_activity['sell_last_filled_at'], '2026-04-30T05:31:00+00:00')
        self.assertEqual(today_activity['sell_broker_order_ids'], ['sell-order-1'])
        self.assertEqual(today_activity['net_open_quantity'], 0)

    async def test_runtime_dashboard_today_activity_exposes_mixed_buy_and_sell_fills(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            dispatch_status='failed',
            execution_status='submit_failed',
            readback={
                'last_synced_at': '2026-04-30T05:31:00+00:00',
                'runtime': {
                    'status': 'ok',
                    'payload': {
                        'mode': 'mock',
                        'ready': True,
                        'monitoring_alive': True,
                    },
                },
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': '2',
                    'open_quantity': '3',
                    'closed_quantity': '2',
                },
                'orders': {
                    'items': [
                        {
                            'id': 'buy-order-1',
                            'broker_order_id': 'buy-order-1',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'status': 'filled',
                            'updated_at': '2026-04-30T00:06:30+00:00',
                        },
                        {
                            'id': 'sell-order-1',
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'status': 'filled',
                            'updated_at': '2026-04-30T05:31:30+00:00',
                        },
                    ]
                },
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'buy-order-1',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'quantity': '5',
                            'price': '50100',
                            'executed_at': '2026-04-30T00:06:00+00:00',
                        },
                        {
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'quantity': '2',
                            'price': '51200',
                            'executed_at': '2026-04-30T05:31:00+00:00',
                        },
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['partial'], 1)
        item = board['items'][0]
        self.assertEqual(item['status'], 'partial')
        self.assertEqual(item['filled_quantity'], 5)
        self.assertEqual(item['exit_filled_quantity'], 2)
        self.assertEqual(item['open_quantity'], 3)
        today_activity = item['today_activity']
        self.assertEqual(today_activity['buy_filled_quantity_today'], 5)
        self.assertEqual(today_activity['buy_avg_fill_price_today'], 50100)
        self.assertEqual(today_activity['buy_last_filled_at'], '2026-04-30T00:06:00+00:00')
        self.assertEqual(today_activity['buy_broker_order_ids'], ['buy-order-1'])
        self.assertEqual(today_activity['sell_filled_quantity_today'], 2)
        self.assertEqual(today_activity['sell_avg_fill_price_today'], 51200)
        self.assertEqual(today_activity['sell_last_filled_at'], '2026-04-30T05:31:00+00:00')
        self.assertEqual(today_activity['sell_broker_order_ids'], ['sell-order-1'])
        self.assertEqual(today_activity['net_open_quantity'], 3)

    async def test_runtime_dashboard_uses_derived_open_quantity_for_held_position_contract(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            readback={
                'last_synced_at': '2026-04-30T05:31:00+00:00',
                'runtime': {'status': 'ok', 'payload': {'mode': 'mock', 'ready': True}},
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': '2',
                    'open_quantity': '3',
                    'closed_quantity': '2',
                },
                'orders': {'items': []},
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'buy-order-1',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'quantity': '5',
                            'price': '50100',
                            'executed_at': '2026-04-30T00:06:00+00:00',
                        },
                        {
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'quantity': '2',
                            'price': '51200',
                            'executed_at': '2026-04-30T05:31:00+00:00',
                        },
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        item = board['items'][0]
        self.assertEqual(item['open_quantity'], 3)
        self.assertTrue(item['held_position'])
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'held_position')
        self.assertEqual(item['position_quantity'], 3)
        self.assertIsNone(item['position_average_price'])
        self.assertIn('target_sell_monitoring', item['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', item['monitoring_statuses'])

    async def test_runtime_dashboard_today_activity_marks_untimestamped_side_unavailable(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            readback={
                'last_synced_at': '2026-04-30T00:07:00+00:00',
                'runtime': {'status': 'ok', 'payload': {'mode': 'mock', 'ready': True}},
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': None,
                    'open_quantity': '5',
                    'closed_quantity': '0',
                },
                'orders': {'items': []},
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'buy-order-no-time',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'quantity': '5',
                            'price': '50100',
                        }
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_activity_status'], 'unavailable')
        self.assertIsNone(today_activity['buy_filled_quantity_today'])
        self.assertIsNone(today_activity['buy_avg_fill_price_today'])
        self.assertIsNone(today_activity['buy_last_filled_at'])
        self.assertEqual(today_activity['buy_broker_order_ids'], [])
        self.assertEqual(today_activity['sell_activity_status'], 'none')

    async def test_runtime_dashboard_today_activity_caps_ids_and_omits_raw_readback_blobs(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'id': f'buy-order-{index}',
                    'broker_order_id': f'buy-order-{index}',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'status': 'filled',
                    'quantity': '1',
                    'filled_quantity': '1',
                    'avg_fill_price': str(50100 + index),
                    'updated_at': '2026-04-30T00:06:30+00:00',
                    'raw_blob': 'x' * 4096,
                }
                for index in range(7)
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_id': f'buy-order-{index}',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '1',
                    'fill_price': str(50100 + index),
                    'executed_at': f'2026-04-30T00:0{index}:00+00:00',
                    'raw_blob': 'y' * 4096,
                }
                for index in range(7)
            ]
        }
        self.client.position_response = {
            'items': [
                {
                    'account_reference': self.account_reference,
                    'symbol': self.symbol,
                    'quantity': '7',
                    'average_price': '50103',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                    'raw_blob': 'z' * 4096,
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_filled_quantity_today'], 7)
        self.assertEqual(today_activity['buy_avg_fill_price_today'], 50103)
        self.assertEqual(
            today_activity['buy_broker_order_ids'],
            ['buy-order-0', 'buy-order-1', 'buy-order-2', 'buy-order-3', 'buy-order-4'],
        )
        self.assertEqual(today_activity['buy_broker_order_ids_omitted_count'], 2)
        board_payload = json.dumps(board, ensure_ascii=False, sort_keys=True)
        self.assertNotIn('raw_blob', board_payload)
        self.assertNotIn('xxxx', board_payload)
        self.assertLess(len(board_payload.encode('utf-8')), 20000)

    async def test_runtime_monitored_symbols_include_target_stop_watch_from_partial_read_model(self) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_exec_monitoring_symbols', new=AsyncMock(return_value=None)),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['summary']['watching_fill'], 1)
        self.assertEqual(monitored['freshness']['status'], 'partial')
        self.assertEqual(monitored['freshness']['readback_missing_count'], 1)
        self.assertEqual(monitored['items'][0]['symbol'], self.symbol)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_fill')
        self.assertEqual(monitored['items'][0]['planned_intents'], ['stop_sell', 'target_sell'])
        self.assertEqual(self.client.runtime_calls, 0)
        self.assertEqual(self.client.order_calls, [])
        self.assertEqual(self.client.execution_calls, [])
        self.assertEqual(self.client.position_calls, [])

    async def test_runtime_dashboard_refresh_true_returns_local_read_model_while_refresh_runs(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        original_overview = self.service.list_signal_decider_exec_overview
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_call_count = 0

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_call_count
            if kwargs.get('refresh'):
                refresh_call_count += 1
                refresh_started.set()
                await release_refresh.wait()
            return await original_overview(**kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            board = await asyncio.wait_for(
                self.service.get_runtime_daily_board(
                    market_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                ),
                timeout=1.0,
            )

            self.assertEqual(board['refresh']['status'], 'scheduled')
            self.assertEqual(board['freshness']['status'], 'refreshing')
            self.assertEqual(board['freshness']['read_model_source'], 'persisted_exec_read_model')
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)

            monitored = await asyncio.wait_for(
                self.service.list_runtime_monitored_symbols(
                    market_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                ),
                timeout=1.0,
            )

            self.assertEqual(refresh_call_count, 1)
            self.assertEqual(monitored['refresh']['status'], 'running')
            self.assertEqual(monitored['freshness']['status'], 'refreshing')
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_dashboard_background_refresh_stores_monitored_cache_with_live_holding_filter(self) -> None:
        key = self.service._dashboard_refresh_key(
            target_trade_date=self.target_trade_date,
            account_reference=None,
            symbol=None,
        )

        with (
            patch.object(
                self.service,
                'list_signal_decider_exec_overview',
                new=AsyncMock(return_value={'items': [], 'summary': {}}),
            ) as overview,
            patch.object(
                self.service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(
                    return_value=PortfolioHoldingSymbolMembership(
                        symbols=frozenset({self.symbol}),
                        membership_complete=True,
                    )
                ),
            ) as load_live_holdings,
            patch.object(self.service, '_store_dashboard_caches_from_overview') as store_caches,
        ):
            await self.service._refresh_dashboard_read_model(
                key=key,
                target_trade_date=self.target_trade_date,
                account_reference=None,
                symbol=None,
                trigger='test',
                timeout_seconds=1.0,
            )

        overview.assert_awaited_once()
        load_live_holdings.assert_awaited_once_with(
            market_date=self.target_trade_date,
            trigger='test:dashboard_background_refresh',
        )
        store_caches.assert_called_once()
        self.assertEqual(store_caches.call_args.kwargs['live_holding_symbols'], {self.symbol})
        self.assertTrue(
            store_caches.call_args.kwargs[
                'live_portfolio_holding_membership'
            ].membership_complete
        )

    async def test_old_refresh_finalizer_cannot_clobber_replacement_state(
        self,
    ) -> None:
        key = self.service._dashboard_refresh_key(
            target_trade_date=self.target_trade_date,
            account_reference=None,
            symbol=None,
        )

        async def fail() -> None:
            raise RuntimeError('old refresh failed')

        release_replacement = asyncio.Event()
        old_task = asyncio.create_task(fail())
        await asyncio.sleep(0)
        replacement = asyncio.create_task(release_replacement.wait())
        self.service._dashboard_refresh_tasks[key] = replacement
        self.service._dashboard_refresh_state[key] = {
            'status': 'running',
            'started_at': '2026-04-30T00:00:01+00:00',
        }

        self.service._finalize_dashboard_refresh_task(key, old_task)

        self.assertIs(self.service._dashboard_refresh_tasks[key], replacement)
        self.assertEqual(
            self.service._dashboard_refresh_state[key]['status'],
            'running',
        )
        release_replacement.set()
        await replacement

    async def test_background_refresh_cannot_be_overwritten_by_old_foreground_build(
        self,
    ) -> None:
        monitored_key = self.service._dashboard_cache_key(
            'monitored_symbols',
            self.target_trade_date,
            None,
        )
        monitored_lock = asyncio.Lock()
        await monitored_lock.acquire()
        self.service._dashboard_build_locks[monitored_key] = monitored_lock
        fresh_board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'items': [{'symbol': '027710'}],
        }
        fresh_monitored = {
            'market_date': self.target_trade_date.isoformat(),
            'items': [{'symbol': '027710'}],
        }

        with (
            patch.object(
                self.service,
                '_build_runtime_daily_board_payload',
                return_value=fresh_board,
            ),
            patch.object(
                self.service,
                '_build_runtime_monitored_symbols_payload',
                return_value=fresh_monitored,
            ),
        ):
            background_store = asyncio.create_task(
                self.service._store_dashboard_caches_from_overview(
                    market_date=self.target_trade_date,
                    symbol=None,
                    overview={'items': []},
                    refresh_state={'status': 'succeeded'},
                )
            )
            await asyncio.sleep(0)
            self.service._store_dashboard_cached_payload(
                endpoint='monitored_symbols',
                market_date=self.target_trade_date,
                symbol=None,
                payload={
                    'market_date': self.target_trade_date.isoformat(),
                    'items': [{'symbol': '005930'}],
                },
            )
            monitored_lock.release()
            await background_store

        self.assertEqual(
            self.service._dashboard_cache[monitored_key].payload['items'],
            [{'symbol': '027710'}],
        )

    async def test_dashboard_background_refresh_stores_monitored_cache_with_live_exec_overlay(self) -> None:
        key = self.service._dashboard_refresh_key(
            target_trade_date=self.target_trade_date,
            account_reference=None,
            symbol=None,
        )
        self.client.runtime_response = {
            'ready': True,
            'mode': 'mock',
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'liveness': {
                'autonomous': {
                    'monitoring': {
                        'status': 'running',
                        'last_success_at': '2026-04-30T00:00:00+00:00',
                        'last_cycle': {
                            'triggered_at': '2026-04-30T00:00:00+00:00',
                            'items': [
                                {
                                    'symbol': self.symbol,
                                    'intent': 'target_sell',
                                    'status': 'target_sell_monitoring',
                                    'trigger_price': '1154.1479',
                                    'observed_price': '1126',
                                    'outcome': 'waiting',
                                },
                                {
                                    'symbol': self.symbol,
                                    'intent': 'stop_sell',
                                    'status': 'stop_sell_monitoring',
                                    'trigger_price': '1097.8480',
                                    'observed_price': '1126',
                                    'outcome': 'waiting',
                                },
                            ]
                        }
                    }
                }
            },
        }
        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(
                self.service,
                '_load_live_portfolio_holding_membership',
                new=AsyncMock(return_value=None),
            ) as load_live_holdings,
            patch.object(
                self.service,
                '_load_live_exec_monitoring_symbols',
                new=AsyncMock(
                    return_value=ExecRuntimeMonitoredSymbolMembership(
                        symbols=frozenset({self.symbol}),
                        membership_complete=True,
                    )
                ),
            ),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            await self.service._refresh_dashboard_read_model(
                key=key,
                target_trade_date=self.target_trade_date,
                account_reference=None,
                symbol=None,
                trigger='test',
                timeout_seconds=1.0,
            )
            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        load_live_holdings.assert_awaited_once_with(
            market_date=self.target_trade_date,
            trigger='test:dashboard_background_refresh',
        )
        self.assertTrue(monitored['freshness']['served_from_cache'])
        item = next(candidate for candidate in monitored['items'] if candidate['symbol'] == self.symbol)
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertFalse(item['exec_agent_approved'])
        self.assertEqual(
            item['exec_runtime_state'],
            'runtime_membership_confirmed',
        )
        self.assertIsNone(item['exec_runtime_exclusion_reason'])
        self.assertIn('runtime_membership_active', item['monitoring_statuses'])
        self.assertEqual(item['target_price'], '1154.1479')
        self.assertEqual(item['stop_loss_price'], '1097.8480')
        self.assertEqual(item['current_price'], 1126)
        self.assertEqual(item['monitoring_state'], 'watching_target_stop')
        self.assertEqual(item['price_source'], 'exec_runtime_monitoring')

    async def test_scheduled_exec_readback_refresh_reuses_dashboard_background_state(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        original_overview = self.service.list_signal_decider_exec_overview
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_call_count = 0

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_call_count
            if kwargs.get('refresh'):
                refresh_call_count += 1
                refresh_started.set()
                await release_refresh.wait()
            return await original_overview(**kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            scheduled = self.service.schedule_signal_decider_exec_readback_refresh(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

            self.assertEqual(scheduled['refresh']['status'], 'scheduled')
            self.assertTrue(scheduled['refresh']['background'])
            self.assertEqual(scheduled['request']['target_trade_date'], self.target_trade_date.isoformat())
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)

            running = self.service.schedule_signal_decider_exec_readback_refresh(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

            self.assertEqual(refresh_call_count, 1)
            self.assertEqual(running['refresh']['status'], 'running')
            self.assertTrue(running['refresh']['in_flight'])
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_scheduled_exec_readback_refresh_uses_supplied_timeout(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        original_overview = self.service.list_signal_decider_exec_overview
        self.service.dashboard_refresh_timeout_seconds = 0.001
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_cancelled = False

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_cancelled
            if kwargs.get('refresh'):
                refresh_started.set()
                try:
                    await release_refresh.wait()
                except asyncio.CancelledError:
                    refresh_cancelled = True
                    raise
            return await original_overview(**kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            scheduled = self.service.schedule_signal_decider_exec_readback_refresh(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
                timeout_seconds=1.0,
            )

            self.assertEqual(scheduled['refresh']['status'], 'scheduled')
            self.assertAlmostEqual(scheduled['refresh']['timeout_seconds'], 1.0)
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)
            tasks = list(self.service._dashboard_refresh_tasks.values())
            self.assertEqual(len(tasks), 1)
            task = tasks[0]

            await asyncio.sleep(0.03)

            self.assertFalse(refresh_cancelled)
            self.assertFalse(task.done())

            release_refresh.set()
            await asyncio.wait_for(task, timeout=1.0)
            key = self.service._dashboard_refresh_key(
                target_trade_date=self.target_trade_date,
                account_reference=None,
                symbol=None,
            )
            refresh_state = self.service._dashboard_refresh_payload(
                key,
                requested=True,
            )
            self.assertEqual(refresh_state['status'], 'succeeded')
            self.assertAlmostEqual(refresh_state['timeout_seconds'], 1.0)
            self.assertIsNone(refresh_state['last_error'])
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_runtime_monitored_symbols_empty_response_does_not_wait_for_refresh(self) -> None:
        empty_market_date = date(2027, 5, 21)
        original_overview = self.service.list_signal_decider_exec_overview
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_call_count = 0

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_call_count
            if kwargs.get('refresh'):
                refresh_call_count += 1
                refresh_started.set()
                await release_refresh.wait()
                runtime_result = await self.service.runtime_status(refresh=False)
                return {
                    'request': {
                        'target_trade_date': str(kwargs.get('target_trade_date')),
                        'account_reference': kwargs.get('account_reference'),
                        'symbol': kwargs.get('symbol'),
                        'refresh': True,
                    },
                    'integration': runtime_result['integration'],
                    'runtime': runtime_result['runtime'],
                    'summary': {},
                    'items': [],
                }
            return await original_overview(**kwargs)

        async def run_inline(func, *args, **kwargs):
            return func(*args, **kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            with patch('app.orders.exec_integration.asyncio.to_thread', new=AsyncMock(side_effect=run_inline)):
                monitored = await asyncio.wait_for(
                    self.service.list_runtime_monitored_symbols(
                        market_date=empty_market_date,
                        refresh=True,
                        trigger='test',
                    ),
                    timeout=1.0,
                )

            self.assertEqual(monitored['summary']['items_total'], 0)
            self.assertEqual(monitored['items'], [])
            self.assertEqual(monitored['freshness']['status'], 'refreshing')
            self.assertEqual(monitored['refresh']['status'], 'scheduled')
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)
            self.assertEqual(refresh_call_count, 1)
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_runtime_dashboard_views_classify_cancelled_orders_before_failed(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'monitoring': {
                'alive': True,
            },
        }
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        for remote_status in ('cancelled', 'canceled'):
            with self.subTest(remote_status=remote_status):
                self.client.order_response = {
                    'items': [
                        {
                            'id': 'order-1',
                            'execution_plan_item_id': 'exec-item-1',
                            'symbol': self.symbol,
                            'status': remote_status,
                            'updated_at': '2026-04-30T00:05:00+00:00',
                        }
                    ]
                }

                await self.service.list_signal_decider_exec_overview(
                    target_trade_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                )
                board = await self.service.get_runtime_daily_board(
                    market_date=self.target_trade_date,
                    refresh=False,
                    trigger='test',
                )
                detail = await self.service.get_runtime_symbol_detail(
                    market_date=self.target_trade_date,
                    symbol=self.symbol,
                    refresh=False,
                    trigger='test',
                )

                self.assertEqual(board['summary']['cancelled'], 1)
                self.assertEqual(board['summary']['failed'], 0)
                self.assertEqual(board['items'][0]['status'], 'cancelled')
                self.assertEqual(detail['items'][0]['status'], 'cancelled')
                self.assertEqual(detail['items'][0]['remote_state']['derived_status'], 'cancelled')


if __name__ == '__main__':
    unittest.main()
