from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EXECUTION_HANDOFF_CONTRACT_VERSION = 'execution_handoff_contract_v1'
LEGACY_EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION = 'signal_decider_exec_plan_v2'
EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION = 'signal_decider_exec_plan_v3'
EXEC_AGENT_DECIDER_HANDOFF_SCHEMA_VERSION = 'exec_agent_decider_handoff_v1'
EXECUTION_RUNTIME_DASHBOARD_READ_MODEL_SCHEMA_VERSION = 'execution_runtime_dashboard_read_model_v1'
EXECUTION_RUNTIME_TODAY_ACTIVITY_SCHEMA_VERSION = 'execution_runtime_today_activity_v1'
PORTFOLIO_SUMMARY_READ_MODEL_SOURCE = 'kis_portfolio_summary_v1'
PORTFOLIO_RISK_EQUIVALENT_CONTRACT_VERSION = 'portfolio_summary_risk_equivalent_v1'

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


class ExecutionDailyBoardPayload(_ContractModel):
    request: dict[str, Any]
    runtime: ExecutionRuntimeStatusPayload
    market_date: str
    summary: dict[str, Any]
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
    core_item_key: str
    agent_name: str
    decision_agent_name: str
    symbol: str
    asset_class: str | None = None
    market: str | None = None
    exchange: str | None = None
    currency: str | None = None
    broker: str | None = None
    session_policy: dict[str, Any] | None = None
    side: ExecutionDispatchItemSide
    intent: str
    quantity_policy: dict[str, Any]
    limit_price: int | float | str | None = None
    trigger_price: int | float | str | None = None
    time_in_force: str | None = None
    risk_context: dict[str, Any]
    decision_snapshot_id: str


class ExecutionPlanDispatchPayload(_ContractModel):
    plan_id: str
    source: str
    mode: str
    market_date: str
    strategy_run_id: str
    requested_at: str
    items: list[ExecutionPlanDispatchItemPayload]
    metadata: dict[str, Any]


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


def serialize_execution_plan_dispatch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {**payload, 'items': [dict(item) for item in payload.get('items') or []]}
    metadata = normalized.get('metadata') if isinstance(normalized.get('metadata'), dict) else {}
    schema_version = str(metadata.get('schema_version') or LEGACY_EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION)
    required = ('asset_class', 'market', 'exchange', 'currency', 'broker', 'session_policy')
    for item in normalized['items']:
        market = str(item.get('market') or '').strip().upper()
        currency = str(item.get('currency') or '').strip().upper()
        is_us = market in {'US', 'NASDAQ', 'NYSE', 'NYSE_AMERICAN'} or currency == 'USD'
        missing = [field for field in required if not item.get(field)]
        if is_us and missing:
            raise ValueError(f'US execution item is missing routing fields: {", ".join(missing)}')
        if schema_version == LEGACY_EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION and missing:
            item.update(
                {
                    'asset_class': item.get('asset_class') or 'stock',
                    'market': item.get('market') or 'KRX',
                    'exchange': item.get('exchange') or 'KRX',
                    'currency': item.get('currency') or 'KRW',
                    'broker': item.get('broker') or 'kis',
                    'session_policy': item.get('session_policy')
                    or {
                        'session': 'regular',
                        'timezone': 'Asia/Seoul',
                        'allow_extended_hours': False,
                    },
                }
            )
        elif missing:
            raise ValueError(f'Execution item is missing routing fields: {", ".join(missing)}')
        if str(item.get('broker') or '').lower() != 'kis':
            raise ValueError('KIS execution handoff requires broker=kis')
        if str(item.get('currency') or '').upper() == 'USD' and str(item.get('exchange') or '').upper() not in {
            'NASD',
            'NYSE',
            'AMEX',
        }:
            raise ValueError('USD equity execution requires NASD, NYSE, or AMEX order exchange code')
    return _dump(ExecutionPlanDispatchPayload, normalized)


def serialize_exec_agent_decider_handoff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(ExecAgentDeciderHandoffPayload, payload)


def serialize_portfolio_risk_equivalent_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump(PortfolioRiskEquivalentPayload, payload)
