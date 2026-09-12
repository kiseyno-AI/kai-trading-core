from __future__ import annotations

import copy
import hashlib
import hmac
import json
import re
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

EXECUTION_HANDOFF_CONTRACT_VERSION = 'execution_handoff_contract_v4'
EXECUTION_HANDOFF_SOURCE = 'kai-trading-core'
LEGACY_EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION = 'signal_decider_exec_plan_v2'
EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION = 'signal_decider_exec_plan_v4'
EXECUTION_HANDOFF_CAPABILITY = 'delegated_final_full_cost_gate_recheck_v1'
EXECUTION_HANDOFF_ACK_SCHEMA_VERSION = 'execution_handoff_ack_v4'
EXECUTION_REVOCATION_SCHEMA_VERSION = 'execution_handoff_revocation_v4'
EXECUTION_REVOCATION_CAPABILITY = 'atomic_pre_submission_revocation_v1'
EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION = 'exec_agent_decider_handoff_v1'
EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION = 'execution_runtime_dashboard_read_model_v1'
EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION = 'execution_runtime_today_activity_v1'
PORTFOLIO_SUMMARY_READ_MODEL_SOURCE = 'kis_portfolio_summary_v1'
PORTFOLIO_RISK_EQUIVALENT_CONTRACT_VERSION = 'portfolio_summary_risk_equivalent_v1'

_DISPATCH_MATERIAL_FINGERPRINT_PATTERN = re.compile(r'^[0-9a-f]{64}$')

ExecutionRuntimeStatus = Literal['ready', 'degraded', 'offline', 'paused', 'kill_switched']
ExecutionDashboardEndpoint = Literal['daily_board', 'monitored_symbols', 'symbol_detail']
ExecutionDashboardFreshnessStatus = Literal['fresh', 'stale', 'partial', 'refreshing', 'unavailable']
ExecutionDashboardRefreshStatus = Literal[
    'not_requested',
    'scheduled',
    'running',
    'debounced',
    'succeeded',
    'timeout',
    'failed',
    'disabled',
    'not_configured',
]
ExecutionBoardStatus = Literal[
    'planned',
    'accepted',
    'scheduled',
    'submitted',
    'partial',
    'filled',
    'cancelled',
    'failed',
]
ExecutionMonitoringState = Literal['watching_target_stop', 'watching_fill', 'reconciling', 'idle']
ExecutionActivityStatus = Literal['available', 'partial', 'unavailable', 'none']
ExecutionPnlCoverageStatus = Literal['available', 'unavailable']
ExecutionDailyBoardActivityCoverageStatus = Literal['available', 'no_activity', 'unavailable']
ExecutionDispatchItemSide = Literal['buy', 'sell']
PortfolioSyncStatus = Literal['synced', 'stale', 'unavailable', 'not_configured']

EXECUTION_HANDOFF_CONTRACT_SURFACES = {
    'execution_plan_dispatch': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    'exec_agent_decider_handoff': EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION,
    'execution_runtime_status': EXECUTION_HANDOFF_CONTRACT_VERSION,
    'execution_runtime_dashboard_read_model': EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION,
    'execution_runtime_today_activity': EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION,
    'portfolio_risk_equivalent': PORTFOLIO_RISK_EQUIVALENT_CONTRACT_VERSION,
}

EXECUTION_HANDOFF_OWNERSHIP = {
    'core': 'Owns strategy/read-model planning, dispatch payload construction, cached dashboard read models, and portfolio summary equivalent reads.',
    'exec': 'Owns execution truth: accepted plans, order/execution/position readback, safety pause, kill switch, and runtime liveness.',
    'web': 'Consumes Core read models for display only and does not infer or submit execution commands from these payloads.',
}


class _ContractModel(BaseModel):
    model_config = ConfigDict(extra='allow')


class ExecutionPnlCoveragePayload(_ContractModel):
    status: ExecutionPnlCoverageStatus
    status_reason: str
    source: str


class ExecutionRuntimeStatusPayload(_ContractModel):
    status: ExecutionRuntimeStatus
    mode: str
    exec_connected: bool | None = None
    automation_enabled: bool | None = None
    paused: bool | None = None
    kill_switch_enabled: bool | None = None
    monitoring_alive: bool | None = None
    last_heartbeat_at: str | None = None
    open_order_count: int | None = None
    active_execution_job_count: int | None = None
    symbols: dict[str, Any] | None = None
    daily_pnl: dict[str, Any] | None = None
    weekly_pnl: dict[str, Any] | None = None
    monthly_pnl: dict[str, Any] | None = None
    pnl_coverage: dict[str, ExecutionPnlCoveragePayload] | None = None
    actionable_alerts: dict[str, Any] | None = None
    issues: list[dict[str, Any]] = Field(default_factory=list)
    core_integration_mode: str | None = None
    delegate_execution: bool | None = None


class ExecutionDashboardFreshnessPayload(_ContractModel):
    schema_version: Literal[EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION]
    endpoint: ExecutionDashboardEndpoint
    market_date: str
    status: ExecutionDashboardFreshnessStatus
    served_from_cache: bool
    read_model_source: str
    generated_at: str
    cached_at: str
    cache_age_seconds: int
    cache_ttl_seconds: int
    is_stale: bool
    is_partial: bool
    refreshing: bool
    last_readback_synced_at: str | None = None
    readback_age_seconds: int | None = None
    readback_synced_count: int
    readback_missing_count: int
    overview_item_count: int
    output_item_count: int


class ExecutionDashboardRefreshPayload(_ContractModel):
    requested: bool
    status: ExecutionDashboardRefreshStatus
    background: bool | None = None
    in_flight: bool | None = None
    debounced: bool | None = None
    timeout_seconds: float | None = None
    debounce_seconds: float | None = None
    last_trigger: str | None = None
    last_attempted_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    last_succeeded_at: str | None = None
    last_error: str | dict[str, Any] | None = None


class ExecutionTodayActivityPayload(_ContractModel):
    schema_version: Literal[EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION]
    market_date: str
    buy_activity_status: ExecutionActivityStatus
    buy_activity_source: str | None = None
    buy_filled_quantity_today: int | float | None
    buy_avg_fill_price_today: int | float | None
    buy_last_filled_at: str | None = None
    buy_broker_order_ids: list[str]
    buy_broker_order_ids_omitted_count: int
    sell_activity_status: ExecutionActivityStatus
    sell_activity_source: str | None = None
    sell_filled_quantity_today: int | float | None
    sell_avg_fill_price_today: int | float | None
    sell_last_filled_at: str | None = None
    sell_broker_order_ids: list[str]
    sell_broker_order_ids_omitted_count: int
    net_open_quantity: int | float | None


class ExecutionRuntimeExecCardPayload(_ContractModel):
    exec_monitoring_active: bool
    exec_runtime_showable: bool
    exec_runtime_state: str
    exec_runtime_exclusion_reason: str | None = None
    exec_agent_approved: bool
    exec_agent_action: str | None = None
    exec_agent_reason_code: str | None = None
    exec_agent_target_profile: str | None = None
    exec_agent_target_tag: str | None = None
    exec_agent_special_tactical_scalp: bool | None = None
    held_position: bool
    position_quantity: int | float | None
    position_average_price: int | float | None
    approved_quantity: int | float | None
    approved_notional: str | None = None
    monitoring_statuses: list[str]


class ExecutionDailyBoardRowPayload(ExecutionRuntimeExecCardPayload):
    plan_item_id: int | str | None = None
    account_reference: str | None = None
    account_label: str | None = None
    symbol: str
    name: str
    market: str | None = None
    agent_name: str
    decision_agent_name: str
    intent: str
    raw_action: str | None = None
    tactical_action: str | None = None
    generic_action: str | None = None
    handoff_intent: str | None = None
    handoff_active: bool | None = None
    approval_state: str | None = None
    status: ExecutionBoardStatus
    monitoring_state: ExecutionMonitoringState
    position_side: str | None = None
    planned_intents: list[str]
    planned_quantity: int | float | None
    filled_quantity: int | float | None
    exit_filled_quantity: int | float | None
    open_quantity: int | float | None
    planned_price: int | float | str | None = None
    avg_fill_price: int | float | None = None
    today_activity: ExecutionTodayActivityPayload
    decision_source: str
    decision_snapshot_id: int | str | None = None
    source_feedback_id: int | str | None = None
    decision_run_id: str | None = None
    policy_version: str | None = None
    execution_plan_id: str | None = None
    broker_order_id: str | None = None
    last_updated_at: str | None = None
    failure_reason: str | None = None
    target_price: int | float | str | None = None
    stop_loss_price: int | float | str | None = None
    trigger_price: int | float | str | None = None
    chase_limit_price: int | float | str | None = None
    expected_holding_period: str | None = None
    entry_execution_mode: str | None = None
    target_trade_date: str | None = None
    source_market_date: str | None = None
    current_price: int | float | None = None
    price_as_of: str | None = None
    price_received_at: str | None = None
    price_source: str | None = None
    price_status: str | None = None
    display_price_available: bool
    realtime_subscribed: bool
    realtime_subscription_reason: str | None = None


class ExecutionDailyBoardActivityCoveragePayload(_ContractModel):
    status: ExecutionDailyBoardActivityCoverageStatus
    status_reason: str
    exec_activity_detected: bool | None = None
    source: str


class ExecutionDailyBoardPayload(_ContractModel):
    request: dict[str, Any]
    runtime: ExecutionRuntimeStatusPayload
    market_date: str
    summary: dict[str, Any]
    activity_coverage: ExecutionDailyBoardActivityCoveragePayload | None = None
    freshness: ExecutionDashboardFreshnessPayload
    refresh: ExecutionDashboardRefreshPayload
    items: list[ExecutionDailyBoardRowPayload]


class ExecutionMonitoredSymbolItemPayload(ExecutionRuntimeExecCardPayload):
    symbol: str
    name: str
    market: str | None = None
    current_price: int | float | None = None
    price_as_of: str | None = None
    price_received_at: str | None = None
    price_source: str | None = None
    price_status: str | None = None
    display_price_available: bool
    realtime_subscribed: bool
    realtime_subscription_reason: str | None = None
    monitoring_state: ExecutionMonitoringState
    exec_runtime_exclusion_reasons: list[str]
    position_side: str | None = None
    planned_intents: list[str]
    tactical_actions: list[str]
    handoff_intents: list[str]
    target_price: int | float | str | None = None
    stop_loss_price: int | float | str | None = None
    trigger_price: int | float | str | None = None
    chase_limit_price: int | float | str | None = None
    expected_holding_period: str | None = None
    decision_agent_names: list[str]
    last_event_at: str | None = None
    account_references: list[str]
    status_counts: dict[str, int]


class ExecutionMonitoredSymbolsPayload(_ContractModel):
    request: dict[str, Any]
    runtime: ExecutionRuntimeStatusPayload
    market_date: str
    summary: dict[str, Any]
    realtime_subscription: dict[str, Any]
    freshness: ExecutionDashboardFreshnessPayload
    refresh: ExecutionDashboardRefreshPayload
    items: list[ExecutionMonitoredSymbolItemPayload]


class ExecutionSymbolMonitoringPayload(_ContractModel):
    state: ExecutionMonitoringState
    position_side: str | None = None
    planned_intents: list[str]
    last_event_at: str | None = None
    runtime_status: ExecutionRuntimeStatus | None = None


class ExecutionSymbolDetailPayload(_ContractModel):
    request: dict[str, Any]
    runtime: ExecutionRuntimeStatusPayload
    market_date: str
    symbol: str
    name: str
    market: str | None = None
    current_price: int | float | None = None
    price_as_of: str | None = None
    price_received_at: str | None = None
    price_source: str | None = None
    price_status: str | None = None
    display_price_available: bool
    realtime_subscribed: bool
    realtime_subscription_reason: str | None = None
    planning: dict[str, Any]
    monitoring: ExecutionSymbolMonitoringPayload
    summary: dict[str, Any]
    freshness: ExecutionDashboardFreshnessPayload
    refresh: ExecutionDashboardRefreshPayload
    items: list[ExecutionDailyBoardRowPayload]


class ExecutionPlanDispatchItemPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    core_item_key: str
    source_plan_item_id: str
    agent_name: str
    decision_agent_name: str
    symbol: str
    asset_class: str
    market: str
    exchange: str
    currency: str
    broker: str
    provider: str
    venue: str
    session_policy: dict[str, Any]
    side: ExecutionDispatchItemSide
    intent: str
    handoff_intent: Literal['next_open_buy', 'tactical_buy', 'intraday_watch_buy'] | None = None
    raw_action: Literal['buy', 'scalp_buy_candidate', 'watch_for_trigger', 'wait', 'reject', 'avoid_chase'] | None = None
    tactical_action: Literal['scalp_buy_candidate', 'watch_for_trigger', 'wait', 'reject', 'avoid_chase'] | None = None
    generic_action: Literal['buy', 'monitor_candidate', 'wait', 'reject'] | None = None
    quantity_policy: dict[str, Any]
    limit_price: int | float | str | None = None
    trigger_price: int | float | str | None = None
    time_in_force: str | None = None
    risk_context: dict[str, Any]
    decision_snapshot_id: str

    @model_validator(mode='after')
    def validate_exact_identity(self) -> 'ExecutionPlanDispatchItemPayload':
        if self.core_item_key != self.source_plan_item_id:
            raise ValueError('core_item_key and source_plan_item_id must match')
        if self.agent_name != self.decision_agent_name:
            raise ValueError('agent_name and decision_agent_name must match')
        session_policy = self.session_policy
        if (
            self.asset_class != 'stock'
            or self.market != 'KRX'
            or self.exchange != 'KRX'
            or self.currency != 'KRW'
            or self.broker != 'kis'
            or self.provider != 'kis'
            or self.venue != 'kis'
            or set(session_policy) != {
                'session',
                'timezone',
                'allow_extended_hours',
            }
            or session_policy.get('session') != 'regular'
            or session_policy.get('timezone') != 'Asia/Seoul'
            or session_policy.get('allow_extended_hours') is not False
        ):
            raise ValueError(
                'execution handoff currently supports KIS/KRX/KRW only'
            )
        return self


class ExecutionPlanDispatchMetadataPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    schema_version: Literal[EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION]
    contract_version: Literal[EXECUTION_HANDOFF_CONTRACT_VERSION]
    required_capability: Literal[EXECUTION_HANDOFF_CAPABILITY]
    agent_name: str
    decision_agent_name: str
    trigger: str
    requested_at: str
    account_reference: str | None = None
    account_label: str | None = None
    source_market_date: str
    target_trade_date: str
    dispatch_batch_type: Literal['initial', 'delta']
    dispatch_batch_key: str | None = None
    dispatch_material_fingerprint: str = Field(pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def validate_exact_binding(self) -> 'ExecutionPlanDispatchMetadataPayload':
        if self.agent_name != self.decision_agent_name:
            raise ValueError('agent_name and decision_agent_name must match')
        if self.dispatch_batch_type == 'initial' and self.dispatch_batch_key is not None:
            raise ValueError('initial dispatch must not include dispatch_batch_key')
        if self.dispatch_batch_type == 'delta' and self.dispatch_batch_key is None:
            raise ValueError('delta dispatch requires dispatch_batch_key')
        return self


class ExecutionPlanDispatchPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    plan_id: str
    source: Literal[EXECUTION_HANDOFF_SOURCE]
    mode: Literal['mock', 'paper', 'live']
    market_date: str
    strategy_run_id: str
    requested_at: str
    items: list[ExecutionPlanDispatchItemPayload] = Field(min_length=1)
    metadata: ExecutionPlanDispatchMetadataPayload

    @model_validator(mode='after')
    def validate_plan_binding(self) -> 'ExecutionPlanDispatchPayload':
        if self.market_date != self.metadata.target_trade_date:
            raise ValueError('market_date must equal metadata.target_trade_date')
        if self.requested_at != self.metadata.requested_at:
            raise ValueError('requested_at must equal metadata.requested_at')
        if any(item.decision_agent_name != self.metadata.decision_agent_name for item in self.items):
            raise ValueError('all items must match metadata.decision_agent_name')
        keys = [item.core_item_key for item in self.items]
        if len(keys) != len(set(keys)):
            raise ValueError('core_item_key values must be unique')
        entries = {
            item.core_item_key: item
            for item in self.items
            if item.side == 'buy'
        }
        if not entries:
            raise ValueError('v4 plan requires at least one protected buy entry')
        exits_by_entry = {
            entry_key: {'target_sell': [], 'stop_sell': []}
            for entry_key in entries
        }
        for entry in entries.values():
            if entry.quantity_policy.get('type') != 'exec_agent_sized':
                raise ValueError('buy quantity_policy.type must be exec_agent_sized')
        for item in self.items:
            if item.side != 'sell':
                continue
            if item.intent not in {'target_sell', 'stop_sell'} or item.trigger_price is None:
                raise ValueError('protective exits require a supported intent and trigger_price')
            linked_entry_key = item.quantity_policy.get('linked_entry_item_key')
            entry = entries.get(linked_entry_key)
            if (
                item.quantity_policy.get('type') != 'position_exit_from_entry'
                or entry is None
            ):
                raise ValueError('protective exit must reference an in-request buy')
            if (
                item.symbol != entry.symbol
                or item.agent_name != entry.agent_name
                or item.decision_agent_name != entry.decision_agent_name
                or item.decision_snapshot_id != entry.decision_snapshot_id
            ):
                raise ValueError('protective exit identity must match its linked buy')
            exits_by_entry[linked_entry_key][item.intent].append(item.core_item_key)
        if any(
            len(exits['target_sell']) != 1 or len(exits['stop_sell']) != 1
            for exits in exits_by_entry.values()
        ):
            raise ValueError('every buy requires exactly one target_sell and one stop_sell')
        return self


class ExecutionPlanDispatchAckHeader(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    schema_version: Literal[EXECUTION_HANDOFF_ACK_SCHEMA_VERSION]
    contract_version: Literal[EXECUTION_HANDOFF_CONTRACT_VERSION]
    capability: Literal[EXECUTION_HANDOFF_CAPABILITY]
    plan_id: str
    status: Literal['accepted']
    dispatch_material_fingerprint: str = Field(pattern=r'^[0-9a-f]{64}$')


class ExecutionPlanDispatchItemAck(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    core_item_key: str
    status: Literal['accepted']
    execution_plan_id: str = Field(min_length=1)
    execution_plan_item_id: str = Field(min_length=1)
    agent_sizing: dict[str, Any] | None = None


class ExecutionPlanDispatchAckPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    contract_ack: ExecutionPlanDispatchAckHeader
    item_acks: list[ExecutionPlanDispatchItemAck]
    plan_id: str = Field(min_length=1)
    execution_plan_id: str = Field(min_length=1)
    accepted_item_count: int = Field(ge=1)
    rejected_item_count: Literal[0]


class ExecutionRevocationItemPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    core_item_key: str
    execution_plan_item_id: str
    decision_snapshot_id: str


class ExecutionRevocationPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    schema_version: Literal[EXECUTION_REVOCATION_SCHEMA_VERSION]
    contract_version: Literal[EXECUTION_HANDOFF_CONTRACT_VERSION]
    required_capability: Literal[EXECUTION_REVOCATION_CAPABILITY]
    plan_id: str
    requested_at: str
    reason_code: Literal[
        'latest_snapshot_not_actionable',
        'latest_snapshot_replaced',
        'candidate_removed',
        'account_route_removed',
        'policy_scope_changed',
        'execution_control_disabled',
        'current_live_market_risk_off',
        'current_live_market_evidence_unavailable',
    ]
    replacement_decision_snapshot_id: str
    replacement_action: Literal[
        'buy',
        'scalp_buy_candidate',
        'watch_for_trigger',
        'wait',
        'reject',
        'avoid_chase',
    ]
    items: list[ExecutionRevocationItemPayload]

    @model_validator(mode='after')
    def require_unique_item_bindings(self) -> 'ExecutionRevocationPayload':
        keys = [item.core_item_key for item in self.items]
        item_ids = [item.execution_plan_item_id for item in self.items]
        if not keys or len(keys) != len(set(keys)) or len(item_ids) != len(set(item_ids)):
            raise ValueError('revocation item bindings must be non-empty and one-to-one')
        return self


class ExecutionRevocationAckHeader(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    schema_version: Literal[EXECUTION_REVOCATION_SCHEMA_VERSION]
    contract_version: Literal[EXECUTION_HANDOFF_CONTRACT_VERSION]
    capability: Literal[EXECUTION_REVOCATION_CAPABILITY]
    plan_id: str
    status: Literal['revoked', 'manual_review_required', 'not_found']


class ExecutionRevocationItemAck(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    core_item_key: str
    status: Literal['revoked', 'manual_review_required', 'preserved_protective_exit', 'not_found']
    execution_plan_id: str | None = None
    execution_plan_item_id: str


class ExecutionRevocationAckPayload(_ContractModel):
    model_config = ConfigDict(extra='forbid')

    plan_id: str
    execution_plan_id: str | None = None
    contract_ack: ExecutionRevocationAckHeader
    item_acks: list[ExecutionRevocationItemAck]
    reason_code: str
    revoked_item_count: int
    manual_review_item_count: int
    preserved_item_count: int
    idempotent_replay: bool | None = None


class ExecAgentDeciderHandoffPayload(_ContractModel):
    schema_version: Literal[EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION]
    classification: str
    symbol: str
    target_trade_date: str
    source_market_date: str
    traceability: dict[str, Any]
    decider_input_context: dict[str, Any]
    market_regime_context: dict[str, Any]
    decision_input_quality_context: dict[str, Any]
    decider_output: dict[str, Any]
    eod_pre_decider_enrichment_metadata: dict[str, Any]


class PortfolioSummarySyncPayload(_ContractModel):
    status: PortfolioSyncStatus
    lastSyncedAt: str | None = None
    source: str
    message: str | None = None


class PortfolioRiskEquivalentPayload(_ContractModel):
    contract_version: Literal[PORTFOLIO_RISK_EQUIVALENT_CONTRACT_VERSION] = Field(
        default=PORTFOLIO_RISK_EQUIVALENT_CONTRACT_VERSION,
        exclude=True,
    )
    asOf: str
    source: Literal[PORTFOLIO_SUMMARY_READ_MODEL_SOURCE]
    sync: PortfolioSummarySyncPayload
    accounts: list[dict[str, Any]]
    notices: list[str]
    freshness: dict[str, Any] | None = None


def _dump(model_type: type[_ContractModel], payload: dict[str, Any]) -> dict[str, Any]:
    return model_type.model_validate(payload).model_dump(mode='python', exclude_unset=True)


def serialize_execution_runtime_status_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionRuntimeStatusPayload, payload)


def serialize_execution_dashboard_freshness_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionDashboardFreshnessPayload, payload)


def serialize_execution_dashboard_refresh_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionDashboardRefreshPayload, payload)


def serialize_execution_today_activity_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionTodayActivityPayload, payload)


def serialize_execution_runtime_exec_card_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionRuntimeExecCardPayload, payload)


def serialize_execution_daily_board_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionDailyBoardPayload, payload)


def serialize_execution_monitored_symbols_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionMonitoredSymbolsPayload, payload)


def serialize_execution_symbol_detail_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionSymbolDetailPayload, payload)


def _canonical_dispatch_number(value: int | float | Decimal) -> str:
    if isinstance(value, float):
        number = Decimal(str(value))
    else:
        number = Decimal(value)
    if not number.is_finite():
        raise ValueError('dispatch material contains a non-finite number')
    if number == 0:
        return '0'
    return str(number.normalize()).lower()


def _canonical_dispatch_material(value: Any) -> Any:
    if value is None:
        return {'$null': True}
    if isinstance(value, bool):
        return {'$bool': value}
    if isinstance(value, (int, float, Decimal)):
        return {'$number': _canonical_dispatch_number(value)}
    if isinstance(value, str):
        return {'$string': value}
    if isinstance(value, (list, tuple)):
        return {'$array': [_canonical_dispatch_material(item) for item in value]}
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError('dispatch material object keys must be strings')
        return {
            '$object': [
                [key, _canonical_dispatch_material(value[key])]
                for key in sorted(value)
            ]
        }
    raise ValueError(
        f'dispatch material contains unsupported value type: {type(value).__name__}'
    )


def execution_plan_dispatch_material_fingerprint(payload: dict[str, Any]) -> str:
    material = copy.deepcopy(payload)
    material.pop('requested_at', None)
    metadata = material.get('metadata')
    if isinstance(metadata, dict):
        metadata.pop('requested_at', None)
        metadata.pop('trigger', None)
        metadata.pop('dispatch_material_fingerprint', None)
    canonical = _canonical_dispatch_material(material)
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def serialize_execution_plan_dispatch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    candidate = copy.deepcopy(payload)
    metadata = candidate.get('metadata')
    if isinstance(metadata, dict):
        metadata['dispatch_material_fingerprint'] = '0' * 64
    validated = _dump(ExecutionPlanDispatchPayload, candidate)
    validated['metadata']['dispatch_material_fingerprint'] = (
        execution_plan_dispatch_material_fingerprint(validated)
    )
    return _dump(ExecutionPlanDispatchPayload, validated)


def validate_execution_plan_dispatch_ack(
    payload: dict[str, Any],
    *,
    plan_id: str,
    submitted_core_item_keys: list[str],
    expected_dispatch_material_fingerprint: str,
) -> dict[str, dict[str, Any]]:
    if (
        not isinstance(expected_dispatch_material_fingerprint, str)
        or _DISPATCH_MATERIAL_FINGERPRINT_PATTERN.fullmatch(
            expected_dispatch_material_fingerprint
        )
        is None
    ):
        raise ValueError('expected dispatch material fingerprint must be 64 lowercase hex characters')
    parsed = ExecutionPlanDispatchAckPayload.model_validate(payload)
    if not hmac.compare_digest(
        parsed.contract_ack.dispatch_material_fingerprint,
        expected_dispatch_material_fingerprint,
    ):
        raise ValueError('execution handoff ACK material fingerprint mismatch')
    if parsed.contract_ack.plan_id != plan_id:
        raise ValueError('execution handoff ACK plan_id mismatch')
    submitted_keys = list(submitted_core_item_keys)
    if not submitted_keys or len(submitted_keys) != len(set(submitted_keys)):
        raise ValueError('submitted core_item_key values must be non-empty and unique')
    ack_by_key: dict[str, dict[str, Any]] = {}
    for item in parsed.item_acks:
        if item.core_item_key in ack_by_key:
            raise ValueError('execution handoff ACK contains duplicate core_item_key')
        ack_by_key[item.core_item_key] = item.model_dump(mode='python')
    if set(ack_by_key) != set(submitted_keys):
        raise ValueError('execution handoff ACK must bind every submitted core_item_key exactly once')
    if parsed.plan_id != plan_id:
        raise ValueError('execution handoff response plan_id mismatch')
    if any(
        item['execution_plan_id'] != parsed.execution_plan_id
        for item in ack_by_key.values()
    ):
        raise ValueError('execution handoff response execution_plan_id mismatch')
    execution_plan_item_ids = [
        item['execution_plan_item_id'] for item in ack_by_key.values()
    ]
    if len(execution_plan_item_ids) != len(set(execution_plan_item_ids)):
        raise ValueError(
            'execution handoff ACK execution_plan_item_id values must be unique'
        )
    if parsed.accepted_item_count != len(submitted_keys):
        raise ValueError('execution handoff accepted_item_count mismatch')
    return ack_by_key


def serialize_execution_revocation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecutionRevocationPayload, payload)


def validate_execution_revocation_ack(
    payload: dict[str, Any],
    *,
    plan_id: str,
    submitted_core_item_keys: list[str],
    expected_reason_code: str,
    submitted_items: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, dict[str, Any]]]:
    parsed = ExecutionRevocationAckPayload.model_validate(payload)
    if parsed.plan_id != plan_id or parsed.contract_ack.plan_id != plan_id:
        raise ValueError('execution revocation ACK plan_id mismatch')
    if (
        not isinstance(expected_reason_code, str)
        or not expected_reason_code
        or expected_reason_code.strip() != expected_reason_code
        or parsed.reason_code != expected_reason_code
    ):
        raise ValueError('execution revocation ACK reason_code mismatch')
    submitted_keys = list(submitted_core_item_keys)
    if not submitted_keys or len(submitted_keys) != len(set(submitted_keys)):
        raise ValueError('submitted revocation core_item_key values must be unique')
    ack_by_key: dict[str, dict[str, Any]] = {}
    for item in parsed.item_acks:
        if item.core_item_key in ack_by_key:
            raise ValueError('execution revocation ACK contains duplicate core_item_key')
        ack_by_key[item.core_item_key] = item.model_dump(mode='python')
    if set(ack_by_key) != set(submitted_keys):
        raise ValueError('execution revocation ACK must bind every submitted core_item_key exactly once')
    header_status = parsed.contract_ack.status
    statuses = [str(item['status']) for item in ack_by_key.values()]
    submitted_by_key = {
        str(item.get('core_item_key')): item
        for item in (submitted_items or [])
        if isinstance(item, dict) and item.get('core_item_key')
    }
    if submitted_items is not None and set(submitted_by_key) != set(submitted_keys):
        raise ValueError('submitted revocation item identity must bind every core_item_key')
    submitted_plan_ids = {
        str(item.get('exec_plan_id') or '').strip()
        for item in submitted_by_key.values()
    }
    if submitted_items is not None and (
        '' in submitted_plan_ids or len(submitted_plan_ids) != 1
    ):
        raise ValueError('submitted revocation plan identity must be non-empty and exact')
    expected_execution_plan_id = (
        next(iter(submitted_plan_ids)) if submitted_plan_ids else None
    )
    if expected_execution_plan_id is not None and (
        parsed.execution_plan_id != expected_execution_plan_id
        or any(
            item.get('execution_plan_id') != expected_execution_plan_id
            for item in ack_by_key.values()
        )
    ):
        raise ValueError(
            'execution revocation ACK plan identity does not match submitted plan'
        )
    for key, submitted_item in submitted_by_key.items():
        submitted_item_id = str(
            submitted_item.get('exec_plan_item_id') or ''
        ).strip()
        acknowledged_item_id = str(
            ack_by_key[key].get('execution_plan_item_id') or ''
        ).strip()
        if not submitted_item_id or acknowledged_item_id != submitted_item_id:
            raise ValueError(
                'execution revocation ACK item identity does not match submitted item'
            )
    if header_status == 'revoked':
        if any(status not in {'revoked', 'preserved_protective_exit'} for status in statuses):
            raise ValueError('revoked ACK contains an unsupported item status')
        if submitted_items is None and any(status != 'revoked' for status in statuses):
            raise ValueError('protective-exit preservation requires submitted item identity')
        for key, item in submitted_by_key.items():
            side = str(item.get('side') or '').lower()
            intent = str(item.get('intent') or '').lower()
            item_status = str(ack_by_key[key]['status'])
            if side == 'buy' or intent in {'next_open_buy', 'intraday_watch_buy'}:
                if item_status != 'revoked':
                    raise ValueError('revoked ACK requires every entry buy to be revoked')
            elif item_status == 'preserved_protective_exit' and not (
                side == 'sell' and intent in {'target_sell', 'stop_sell'}
            ):
                raise ValueError('only a target/stop protective exit may be preserved')
    if header_status == 'manual_review_required' and not any(
        status == 'manual_review_required' for status in statuses
    ):
        raise ValueError('manual-review ACK requires an explicit manual-review item')
    if parsed.execution_plan_id is not None and any(
        item['execution_plan_id'] != parsed.execution_plan_id
        for item in ack_by_key.values()
    ):
        raise ValueError('execution revocation ACK execution_plan_id mismatch')
    if parsed.revoked_item_count != sum(status == 'revoked' for status in statuses):
        raise ValueError('execution revocation revoked_item_count mismatch')
    if parsed.manual_review_item_count != sum(
        status == 'manual_review_required' for status in statuses
    ):
        raise ValueError('execution revocation manual_review_item_count mismatch')
    if parsed.preserved_item_count != sum(
        status == 'preserved_protective_exit' for status in statuses
    ):
        raise ValueError('execution revocation preserved_item_count mismatch')
    return header_status, ack_by_key


def serialize_exec_agent_decider_handoff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecAgentDeciderHandoffPayload, payload)


def serialize_portfolio_risk_equivalent_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(PortfolioRiskEquivalentPayload, payload)
